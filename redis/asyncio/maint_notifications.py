import asyncio
import functools
import logging
from typing import TYPE_CHECKING, Any, Awaitable, Callable

from redis.asyncio.observability.recorder import (
    record_connection_handoff,
    record_connection_relaxed_timeout,
    record_maint_notification_count,
)
from redis.event import (
    MaintenanceCompletedEvent,
    MaintenanceEvent,
    MaintenanceStartedEvent,
)
from redis.maint_notifications import (
    MaintenanceNotification,
    MaintenanceState,
    MaintNotificationsConfig,
    NodeMovingNotification,
    OSSNodeMigratedNotification,
    OSSNodeMigratingNotification,
    _dispatch_maintenance_event,
    _get_maintenance_notification_name,
    _get_maintenance_notification_type,
    _should_skip_connection_timeout_update,
)
from redis.observability.attributes import get_pool_name

if TYPE_CHECKING:
    from redis.asyncio.cluster import RedisCluster
    from redis.asyncio.connection import AsyncMaintNotificationsAbstractConnection

logger = logging.getLogger(__name__)

_ScheduledCallback = Callable[..., Awaitable[None]]


def _log_task_exception(message: str, task: "asyncio.Task[Any]") -> None:
    """Task done-callback that surfaces a failed task's exception in the logs.

    Without it, an unhandled exception in a fire-and-forget task is only
    reported by asyncio as a noisy "Task exception was never retrieved"
    warning. Retrieving the exception here suppresses that warning and logs a
    meaningful error instead. Cancellation is expected and left unlogged.

    Bind ``message`` with ``functools.partial`` before passing to
    ``add_done_callback`` (which supplies ``task``).
    """
    try:
        exc = task.exception()
    except asyncio.CancelledError:
        return
    if exc:
        logger.error(message, exc_info=exc)


def add_debug_log_for_notification(
    connection: object,
    notification: str | MaintenanceNotification,
) -> None:
    if not logger.isEnabledFor(logging.DEBUG):
        return

    details = "no connection"
    try:
        extract = getattr(connection, "extract_connection_details", None)
        if callable(extract):
            details = extract()
    except (AttributeError, OSError, TypeError):
        pass

    logger.debug(
        f"Handling maintenance notification: {notification}, "
        f"with connection: {connection}, {details}",
    )


class AsyncMaintNotificationsPoolHandler:
    def __init__(
        self,
        pool: Any,
        config: MaintNotificationsConfig,
    ) -> None:
        self.pool = pool
        self.config = config
        self._processed_notifications: set[MaintenanceNotification] = set()
        self._scheduled_tasks: set[asyncio.Task[None]] = set()
        # The handoffs whose scheduled cleanup has not finished yet
        self._pending_handoffs: set[NodeMovingNotification] = set()
        self._lock = asyncio.Lock()
        self.connection: Any | None = None

    def set_connection(
        self, connection: "AsyncMaintNotificationsAbstractConnection"
    ) -> None:
        self.connection = connection

    def get_handler_for_connection(self) -> "AsyncMaintNotificationsPoolHandler":
        # Copy all data that should be shared between connections, while each
        # connection gets its own handler instance and current connection state.
        copy = AsyncMaintNotificationsPoolHandler(self.pool, self.config)
        copy._processed_notifications = self._processed_notifications
        copy._scheduled_tasks = self._scheduled_tasks
        copy._pending_handoffs = self._pending_handoffs
        copy._lock = self._lock
        copy.connection = None
        return copy

    async def remove_expired_notifications(self) -> None:
        async with self._lock:
            for notification in tuple(self._processed_notifications):
                if notification.is_expired():
                    self._processed_notifications.remove(notification)

    async def handle_notification(self, notification: MaintenanceNotification) -> None:
        await self.remove_expired_notifications()

        if isinstance(notification, NodeMovingNotification):
            await self.handle_node_moving_notification(notification)
        else:
            logger.error(f"Unhandled notification type: {notification}")

    async def handle_node_moving_notification(
        self, notification: NodeMovingNotification
    ) -> None:
        if (
            not self.config.proactive_reconnect
            and not self.config.is_relaxed_timeouts_enabled()
        ):
            return

        async with self._lock:
            if notification in self._processed_notifications:
                # nothing to do in the connection pool handling
                # the notification has already been handled or is expired
                # just return
                return
            if logger.isEnabledFor(logging.DEBUG):
                logger.debug(
                    f"Handling node MOVING notification: {notification}, "
                    f"with connection: {self.connection}, connected to ip "
                    f"{self.connection.get_resolved_ip() if self.connection else None}"
                )
            # Get the current connected address - if any
            # This is the address that is being moved
            # and we need to handle only connections
            # connected to the same address
            moving_address_src = (
                self.connection.getpeername() if self.connection else None
            )

            # Reported before the pool is relaxed: taking over a connection
            # under maintenance completes that connection's pair, and a
            # listener must see the handoff's pair open by then rather than
            # no relaxation at all.
            self._dispatch_handoff_event(MaintenanceStartedEvent, notification)

            try:
                # The async pool owns the active/free connection collections
                # and asyncio.Lock is not reentrant, so the whole MOVING pool
                # mutation has to be one pool-owned atomic operation. The
                # handler still owns the notification policy and passes the
                # already-decided inputs.
                await self.pool.apply_moving_notification(
                    notification=notification,
                    config=self.config,
                    moving_address_src=moving_address_src,
                    run_proactive_reconnect=(
                        self.config.proactive_reconnect
                        and notification.new_node_host is not None
                    ),
                )

                if (
                    self.config.proactive_reconnect
                    and notification.new_node_host is None
                ):
                    self._schedule(
                        notification.ttl / 2,
                        self.run_proactive_reconnect,
                        moving_address_src,
                    )

                self._schedule(
                    notification.ttl,
                    self.handle_node_moved_notification,
                    notification,
                )
                self._pending_handoffs.add(notification)
            except BaseException:
                # The relaxation reported above was not applied, or has no
                # cleanup to end it: close its pair
                self._dispatch_handoff_event(MaintenanceCompletedEvent, notification)
                raise

            await record_connection_handoff(
                pool_name=get_pool_name(self.pool),
            )

            self._processed_notifications.add(notification)

    def _dispatch_handoff_event(
        self,
        event_type: type[MaintenanceEvent],
        notification: NodeMovingNotification,
    ) -> None:
        """
        Report a handoff's relaxation. The events report relaxed timeouts; a
        handoff handled only for the proactive reconnect relaxes nothing.
        """
        if self.config.is_relaxed_timeouts_enabled():
            _dispatch_maintenance_event(
                self.config,
                event_type(
                    connection_pool=self.pool,
                    connection=self.connection,
                    state=MaintenanceState.MOVING,
                    notification=notification,
                    config=self.config,
                ),
            )

    async def run_proactive_reconnect(
        self, moving_address_src: str | None = None
    ) -> None:
        """
        Run proactive reconnect for the pool.
        Active connections are marked for reconnect after they complete the current command.
        Inactive connections are disconnected and will be connected on next use.
        """
        async with self._lock:
            # This delayed reconnect must be atomic for the same reason as the
            # initial MOVING mutation: a connection can move between active and
            # free lists while another task is acquiring or releasing it.
            await self.pool.run_proactive_reconnect(
                moving_address_src=moving_address_src,
            )

    async def handle_node_moved_notification(
        self, notification: NodeMovingNotification
    ) -> bool:
        """
        Handle the cleanup after a node moving notification expires.

        Returns whether the cleanup reverted the connection kwargs; it does not
        when a newer MOVING notification has superseded this one.
        """
        async with self._lock:
            return await self._revert_node_moved_notification(notification)

    async def _revert_node_moved_notification(
        self, notification: NodeMovingNotification
    ) -> bool:
        # Runs under self._lock
        notification_hash = hash(notification)

        if logger.isEnabledFor(logging.DEBUG):
            logger.debug(
                f"Reverting temporary changes related to notification: {notification}, "
                f"with connection: {self.connection}, connected to ip "
                f"{self.connection.get_resolved_ip() if self.connection else None}"
            )
        reset_relaxed_timeout = self.config.is_relaxed_timeouts_enabled()
        reset_host_address = self.config.proactive_reconnect

        # Cleanup has to reset future connection kwargs and existing
        # matching connections together under the pool lock. Splitting it
        # lets an acquire/release interleave and leaves stale MOVING state.
        reverted = await self.pool.cleanup_moving_notification(
            notification_hash=notification_hash,
            reset_relaxed_timeout=reset_relaxed_timeout,
            reset_host_address=reset_host_address,
        )

        # Discarded once the cleanup is done, so one cancelled halfway is
        # still run by cancel_scheduled_tasks(), and before the listeners run
        self._pending_handoffs.discard(notification)
        # A newer MOVING notification has superseded this one when the
        # kwargs were not reverted: the pool stays relaxed until it expires.
        if reverted:
            self._dispatch_handoff_event(MaintenanceCompletedEvent, notification)
        return reverted

    def _schedule(
        self,
        delay: float,
        callback: _ScheduledCallback,
        *args: Any,
    ) -> None:
        # Record the absolute deadline now so that any lag between create_task
        # and the task's first execution slice does not push the fire time out.
        deadline = asyncio.get_running_loop().time() + delay
        task = asyncio.create_task(self._run_after(deadline, callback, *args))
        self._scheduled_tasks.add(task)
        task.add_done_callback(self._scheduled_tasks.discard)
        task.add_done_callback(
            functools.partial(
                _log_task_exception,
                "Error handling scheduled maintenance notification",
            )
        )

    async def _run_after(
        self,
        deadline: float,
        callback: _ScheduledCallback,
        *args: Any,
    ) -> None:
        remaining = deadline - asyncio.get_running_loop().time()
        if remaining > 0:
            await asyncio.sleep(remaining)
        await callback(*args)

    async def cancel_scheduled_tasks(self) -> None:
        # Taken under the lock so a MOVING still being handled finishes, and
        # schedules its cleanup, before the snapshot. The cleanups take the
        # lock themselves, so they are awaited after it is released.
        async with self._lock:
            # A cancelled aclose() can leave handoffs whose tasks are already
            # gone: a retry still has to run their cleanups
            if not self._scheduled_tasks and not self._pending_handoffs:
                return
            pending_handoffs = tuple(self._pending_handoffs)
            tasks = tuple(self._scheduled_tasks)
            for task in tasks:
                task.cancel()
        await asyncio.gather(*tasks, return_exceptions=True)
        # The pool can be reused after aclose(): run the cancelled MOVING
        # cleanups now so the temporary host and relaxed timeouts do not stay
        # in its connection kwargs, and forget the handoffs so the server can
        # redeliver a still-active MOVING to the reused pool.
        for notification in pending_handoffs:
            async with self._lock:
                # A concurrent aclose() may have run this cleanup already
                if notification not in self._pending_handoffs:
                    continue
                reverted = await self._revert_node_moved_notification(notification)
            if not reverted:
                # Closing ends the relaxation of a superseded handoff too
                self._dispatch_handoff_event(MaintenanceCompletedEvent, notification)
            self._processed_notifications.discard(notification)


class AsyncMaintNotificationsConnectionHandler:
    def __init__(
        self,
        connection: "AsyncMaintNotificationsAbstractConnection",
        config: MaintNotificationsConfig,
    ) -> None:
        self.connection = connection
        self.config = config

    def _get_pool(self) -> Any | None:
        """
        Get the pool from the connection's pool handler, or None for
        standalone connections without a pool.
        """
        pool_handler = getattr(
            self.connection, "_maint_notifications_pool_handler", None
        )
        if pool_handler:
            return getattr(pool_handler, "pool", None)
        return None

    def _get_pool_name(self) -> str:
        """
        Get the pool name from the connection's pool handler.
        Falls back to connection representation if pool is not available.
        """
        pool = self._get_pool()
        if pool:
            return get_pool_name(pool)
        # Fallback for standalone connections without a pool
        return repr(self.connection)

    async def handle_notification(self, notification: MaintenanceNotification) -> None:
        # 1 for start, 0 for end notification type, None for unknown.
        notification_type = _get_maintenance_notification_type(notification)
        maint_notification = _get_maintenance_notification_name(notification)

        await record_maint_notification_count(
            server_address=self.connection.host,
            server_port=self.connection.port,
            network_peer_address=self.connection.host,
            network_peer_port=self.connection.port,
            maint_notification=maint_notification,
        )

        if notification_type is None:
            logger.error(f"Unhandled notification type: {notification}")
            return

        if notification_type:
            await self.handle_maintenance_start_notification(
                MaintenanceState.MAINTENANCE, notification
            )
        else:
            await self.handle_maintenance_completed_notification(
                notification=notification
            )

    async def handle_maintenance_start_notification(
        self,
        maintenance_state: MaintenanceState,
        notification: MaintenanceNotification,
    ) -> None:
        add_debug_log_for_notification(self.connection, notification)

        if _should_skip_connection_timeout_update(
            self.connection.maintenance_state, self.config
        ):
            return

        self.connection.maintenance_state = maintenance_state
        self.connection.set_tmp_settings(
            tmp_relaxed_timeout=self.config.relaxed_timeout
        )
        self.connection.update_current_socket_timeout(self.config.relaxed_timeout)
        if isinstance(notification, OSSNodeMigratingNotification):
            # add the notification id to the set of processed start maint notifications
            # this is used to skip the unrelaxing of the timeouts if we have received more than
            # one start notification before the final end notification
            self.connection.add_maint_start_notification(notification.id)

        maint_notification = _get_maintenance_notification_name(notification)
        await record_connection_relaxed_timeout(
            connection_name=self._get_pool_name(),
            maint_notification=maint_notification,
            relaxed=True,
        )

        _dispatch_maintenance_event(
            self.config,
            MaintenanceStartedEvent(
                connection_pool=self._get_pool(),
                connection=self.connection,
                state=maintenance_state,
                notification=notification,
                config=self.config,
            ),
        )

    async def handle_maintenance_completed_notification(self, **kwargs: Any) -> None:
        # Only reset timeouts if state is not MOVING and relaxed timeouts are enabled
        if _should_skip_connection_timeout_update(
            self.connection.maintenance_state, self.config
        ):
            return

        # A completion can reach a connection whose start went to another one,
        # or none at all; only a connection that was relaxed has a start event
        # to pair the completion with
        was_relaxed = self.connection.maintenance_state == MaintenanceState.MAINTENANCE

        notification = None
        if kwargs.get("notification"):
            notification = kwargs["notification"]
        add_debug_log_for_notification(
            self.connection, notification if notification else "MAINTENANCE_COMPLETED"
        )
        self.connection.reset_tmp_settings(reset_relaxed_timeout=True)
        # Maintenance completed - reset the connection
        # timeouts by providing -1 as the relaxed timeout
        self.connection.update_current_socket_timeout(-1)
        self.connection.maintenance_state = MaintenanceState.NONE
        # reset the sets that keep track of received start maint
        # notifications and skipped end maint notifications
        self.connection.reset_received_notifications()

        if notification:
            maint_notification = _get_maintenance_notification_name(notification)
            await record_connection_relaxed_timeout(
                connection_name=self._get_pool_name(),
                maint_notification=maint_notification,
                relaxed=False,
            )

        if was_relaxed:
            _dispatch_maintenance_event(
                self.config,
                MaintenanceCompletedEvent(
                    connection_pool=self._get_pool(),
                    connection=self.connection,
                    state=MaintenanceState.MAINTENANCE,
                    notification=notification,
                    config=self.config,
                ),
            )


class AsyncOSSMaintNotificationsHandler:
    """
    Cluster-wide handler for OSS (open-source) cluster maintenance notifications.

    Reacts to SMIGRATED (slot migration completed) push notifications and
    triggers topology re-initialization via nodes_manager.initialize().

    Lock discipline: _lock is held across the whole pool mutation, including
    await initialize(), so the topology refresh and the subsequent connection
    marking/disconnect happen as one atomic operation — releasing the lock
    mid-mutation would let other tasks observe partial state.

    Re-entrancy is not a deadlock risk here even though asyncio.Lock is
    non-reentrant: initialize() may dispatch commands whose responses carry new
    push notifications, but handle_notification schedules the actual handling as
    a separate background task rather than calling into it inline, so the
    re-entrant arrival never tries to re-acquire the lock on this call stack.
    The cheap _in_progress/_processed dedup that gates that scheduling runs
    without the lock — those sets are only mutated from the single event loop.

    For the same reason the sync handler's lock-ordering rule (acquire
    NodesManager._initialization_lock before the handler's _lock) does not apply
    here: because the handling never runs inline on the call stack that is inside
    initialize(), no task ever holds NodesManager._initialize_lock while waiting
    for this _lock, so the two can be acquired in this order safely. The
    divergence from the sync handler is deliberate, not drift.
    """

    def __init__(
        self,
        cluster_client: "RedisCluster",
        config: MaintNotificationsConfig,
    ) -> None:
        self.cluster_client = cluster_client
        self.config = config
        self._processed_notifications: set[MaintenanceNotification] = set()
        self._in_progress: set[MaintenanceNotification] = set()
        self._lock = asyncio.Lock()
        self._background_tasks: set[asyncio.Task] = set()

    async def remove_expired_notifications(self) -> None:
        async with self._lock:
            for n in tuple(self._processed_notifications):
                if n.is_expired():
                    self._processed_notifications.remove(n)

    async def handle_notification(self, notification: MaintenanceNotification) -> None:
        # Synchronous pre-dedup BEFORE scheduling a task.
        #
        # The same SMIGRATED notification is delivered by the server on every
        # connection, so under load this callback fires many times for the same
        # notification. Without this guard we would schedule one background task
        # per arrival - a flood of tasks that each acquire the lock and dedup,
        # saturating the single event loop. Reserving the notification in
        # _in_progress here (and skipping if already in-progress/processed) caps
        # it at exactly ONE handling task per unique notification.
        #
        # These sets are only mutated from the single event loop, so this
        # check-and-add is race-free without holding the lock.
        if (
            notification in self._in_progress
            or notification in self._processed_notifications
        ):
            return
        self._in_progress.add(notification)

        # Schedule as a background task so the parser's read path is not blocked.
        # This also breaks the inline call chain that would otherwise deadlock:
        # initialize() dispatches commands whose responses may carry more push
        # notifications; as a separate task it can run while this one awaits.
        #
        # If scheduling the task (or wiring its callbacks) fails, release the
        # in-progress reservation made above. Otherwise the notification would be
        # stuck in _in_progress forever - the dedup guard would skip it on every
        # future arrival, and _do_handle_notification's finally (which normally
        # clears it) never runs because the task was never started.
        try:
            task = asyncio.get_running_loop().create_task(
                self._do_handle_notification(notification)
            )
            self._background_tasks.add(task)
            task.add_done_callback(self._background_tasks.discard)
            task.add_done_callback(
                functools.partial(
                    _log_task_exception,
                    "Error handling maintenance notification background task",
                )
            )
        except Exception:
            self._in_progress.discard(notification)
            raise

    async def _do_handle_notification(
        self, notification: MaintenanceNotification
    ) -> None:
        try:
            if isinstance(notification, OSSNodeMigratedNotification):
                await self.handle_oss_maintenance_completed_notification(notification)
            else:
                logger.error(f"Unhandled notification type: {notification}")
        finally:
            # Release the in-progress reservation. On success the notification is
            # also in _processed_notifications (so it won't be re-handled); on
            # failure it is not, allowing a later retry.
            self._in_progress.discard(notification)

    async def handle_oss_maintenance_completed_notification(
        self, notification: OSSNodeMigratedNotification
    ) -> None:
        await self.remove_expired_notifications()

        async with self._lock:
            # handle_notification already reserved this notification in
            # _in_progress and guaranteed uniqueness; the processed check here is
            # defensive (e.g. across handler copies sharing the same sets).
            if notification in self._processed_notifications:
                return
            if logger.isEnabledFor(logging.DEBUG):
                logger.debug(f"Handling SMIGRATED notification: {notification}")

            # Extract the information about the src and destination nodes that are
            # affected by the maintenance. nodes_to_slots_mapping structure:
            # {
            #     "src_host:port": [
            #         {"dest_host:port": "slot_range"},
            #         ...
            #     ],
            #     ...
            # }
            additional_startup_nodes_info = []
            affected_nodes = set()
            for (
                src_address,
                dest_mappings,
            ) in notification.nodes_to_slots_mapping.items():
                src_host, src_port = src_address.rsplit(":", 1)
                src_node = self.cluster_client.nodes_manager.get_node(
                    host=src_host, port=int(src_port)
                )
                if src_node is not None:
                    affected_nodes.add(src_node)
                for dest_mapping in dest_mappings:
                    for dest_address in dest_mapping.keys():
                        dest_host, dest_port = dest_address.rsplit(":", 1)
                        additional_startup_nodes_info.append(
                            (dest_host, int(dest_port))
                        )
            # Updates the cluster slots cache with the new slots mapping
            # This will also update the nodes cache with the new nodes mapping
            await self.cluster_client.nodes_manager.initialize(
                additional_startup_nodes_info=additional_startup_nodes_info,
            )

            all_nodes = set(affected_nodes)
            all_nodes = all_nodes.union(
                self.cluster_client.nodes_manager.nodes_cache.values()
            )
            for current_node in all_nodes:
                handoff_recorded = False
                if current_node in affected_nodes:
                    # mark for reconnect all in-use connections to the node — this
                    # forces them to disconnect after completing their current command
                    free_set = set(current_node._free)
                    for conn in current_node._connections:
                        if conn not in free_set:
                            add_debug_log_for_notification(
                                conn, "SMIGRATED - mark for reconnect"
                            )
                            conn.mark_for_reconnect()
                    await record_connection_handoff(
                        pool_name=f"{current_node.host}:{current_node.port}"
                    )
                    handoff_recorded = True
                else:
                    if logger.isEnabledFor(logging.DEBUG):
                        logger.debug(
                            f"SMIGRATED: Node {current_node.name} not affected "
                            f"by maintenance, skipping mark for reconnect"
                        )
                if (
                    current_node
                    not in self.cluster_client.nodes_manager.nodes_cache.values()
                ):
                    task = asyncio.get_running_loop().create_task(
                        current_node.disconnect_free_connections()
                    )
                    self._background_tasks.add(task)
                    task.add_done_callback(self._background_tasks.discard)
                    task.add_done_callback(
                        functools.partial(
                            _log_task_exception,
                            "Error disconnecting free connections after "
                            "maintenance notification",
                        )
                    )
                    if not handoff_recorded:
                        await record_connection_handoff(
                            pool_name=f"{current_node.host}:{current_node.port}"
                        )

            self._processed_notifications.add(notification)
