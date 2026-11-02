from typing import List

from redis.client import Redis
from redis.event import EventListenerInterface, OnCommandsFailEvent
from redis.multidb.database import SyncDatabase
from redis.multidb.failure_detector import FailureDetector


class ActiveDatabaseChanged:
    """
    Event fired when an active database has been changed.
    """

    def __init__(
        self,
        old_database: SyncDatabase,
        new_database: SyncDatabase,
        command_executor,
        **kwargs,
    ):
        self._old_database = old_database
        self._new_database = new_database
        self._command_executor = command_executor
        self._kwargs = kwargs

    @property
    def old_database(self) -> SyncDatabase:
        return self._old_database

    @property
    def new_database(self) -> SyncDatabase:
        return self._new_database

    @property
    def command_executor(self):
        return self._command_executor

    @property
    def kwargs(self):
        return self._kwargs


class ResubscribeOnActiveDatabaseChanged(EventListenerInterface):
    """
    Re-subscribe the currently active pub / sub to a new active database.
    """

    def listen(self, event: ActiveDatabaseChanged):
        old_pubsub = event.command_executor.active_pubsub

        if old_pubsub is not None:
            # Re-assign old channels and patterns so they will be automatically subscribed on connection.
            new_pubsub = event.new_database.client.pubsub(**event.kwargs)
            new_pubsub.channels = old_pubsub.channels
            new_pubsub.patterns = old_pubsub.patterns
            new_pubsub.shard_channels = old_pubsub.shard_channels
            new_pubsub.on_connect(None)
            event.command_executor.active_pubsub = new_pubsub
            old_pubsub.close()


class CloseConnectionOnActiveDatabaseChanged(EventListenerInterface):
    """
    Drop the free connections to the old active database and mark the in-use
    ones for reconnect, so they are not torn down mid-command.
    """

    def listen(self, event: ActiveDatabaseChanged):
        client = event.old_database.client

        if isinstance(client, Redis):
            self._free_pool_connections(client.connection_pool)
        else:
            # Snapshot the nodes: a topology refresh on another thread can
            # change the cache while this loop runs.
            for node in list(client.nodes_manager.nodes_cache.values()):
                self._free_pool_connections(node.redis_connection.connection_pool)

    @staticmethod
    def _free_pool_connections(pool) -> None:
        # Mark and disconnect under one lock, so a connection acquired meanwhile
        # is either marked for reconnect or disconnected while still free. The
        # maintenance flag makes BlockingConnectionPool honour that lock too.
        set_in_maintenance = getattr(pool, "set_in_maintenance", None)
        with pool._get_pool_lock():
            if set_in_maintenance is not None:
                set_in_maintenance(True)
            try:
                pool.update_active_connections_for_reconnect()
                pool.disconnect_free_connections()
            finally:
                if set_in_maintenance is not None:
                    set_in_maintenance(False)


class RegisterCommandFailure(EventListenerInterface):
    """
    Event listener that registers command failures and passing it to the failure detectors.
    """

    def __init__(self, failure_detectors: List[FailureDetector]):
        self._failure_detectors = failure_detectors

    def listen(self, event: OnCommandsFailEvent) -> None:
        for failure_detector in self._failure_detectors:
            failure_detector.register_failure(event.exception, event.commands)
