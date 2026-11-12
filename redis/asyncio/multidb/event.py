from typing import List

from redis.asyncio import Redis
from redis.asyncio.multidb.database import AsyncDatabase
from redis.asyncio.multidb.failure_detector import AsyncFailureDetector
from redis.event import AsyncEventListenerInterface, AsyncOnCommandsFailEvent


class AsyncActiveDatabaseChanged:
    """
    Event fired when an async active database has been changed.
    """

    def __init__(
        self,
        old_database: AsyncDatabase,
        new_database: AsyncDatabase,
        command_executor,
        **kwargs,
    ):
        self._old_database = old_database
        self._new_database = new_database
        self._command_executor = command_executor
        self._kwargs = kwargs

    @property
    def old_database(self) -> AsyncDatabase:
        return self._old_database

    @property
    def new_database(self) -> AsyncDatabase:
        return self._new_database

    @property
    def command_executor(self):
        return self._command_executor

    @property
    def kwargs(self):
        return self._kwargs


class ResubscribeOnActiveDatabaseChanged(AsyncEventListenerInterface):
    """
    Re-subscribe the currently active pub / sub to a new active database.
    """

    async def listen(self, event: AsyncActiveDatabaseChanged):
        old_pubsub = event.command_executor.active_pubsub

        if old_pubsub is not None:
            # Re-assign old channels and patterns so they will be automatically subscribed on connection.
            new_pubsub = event.new_database.client.pubsub(**event.kwargs)
            new_pubsub.channels = old_pubsub.channels
            new_pubsub.patterns = old_pubsub.patterns
            new_pubsub.shard_channels = old_pubsub.shard_channels
            await new_pubsub.on_connect(None)
            event.command_executor.active_pubsub = new_pubsub
            await old_pubsub.aclose()


class CloseConnectionOnActiveDatabaseChanged(AsyncEventListenerInterface):
    """
    Drop the free connections to the old active database and mark the in-use
    ones for reconnect, so they are not torn down mid-command.
    """

    async def listen(self, event: AsyncActiveDatabaseChanged):
        client = event.old_database.client

        if isinstance(client, Redis):
            pool = client.connection_pool
            # Mark and disconnect under one lock, so a connection acquired
            # meanwhile is either marked for reconnect or disconnected while
            # still free.
            async with pool._get_pool_lock():
                pool._set_in_maintenance(True)
                try:
                    await pool._run_proactive_reconnect_without_locking()
                finally:
                    pool._set_in_maintenance(False)
        else:
            # Snapshot the nodes: a topology refresh can change the caches
            # while this loop awaits. The seed nodes are separate instances
            # with their own connections, so they are visited too, once each.
            nodes_manager = client.nodes_manager
            nodes = {
                id(node): node
                for cache in (nodes_manager.nodes_cache, nodes_manager.startup_nodes)
                for node in cache.values()
            }
            for node in nodes.values():
                node.update_active_connections_for_reconnect()
                await node.disconnect_free_connections()


class RegisterCommandFailure(AsyncEventListenerInterface):
    """
    Event listener that registers command failures and passing it to the failure detectors.
    """

    def __init__(self, failure_detectors: List[AsyncFailureDetector]):
        self._failure_detectors = failure_detectors

    async def listen(self, event: AsyncOnCommandsFailEvent) -> None:
        for failure_detector in self._failure_detectors:
            await failure_detector.register_failure(event.exception, event.commands)
