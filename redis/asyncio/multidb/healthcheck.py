import asyncio
import copy
import inspect
import logging
import time
from abc import ABC, abstractmethod
from enum import Enum
from typing import List, Optional, Tuple, Type, Union

from redis.asyncio import Redis as AsyncRedis
from redis.asyncio.cluster import ClusterNode as AsyncClusterNode
from redis.asyncio.cluster import RedisCluster as AsyncRedisCluster
from redis.asyncio.connection import SSLConnection as AsyncSSLConnection
from redis.asyncio.http.http_client import DEFAULT_TIMEOUT, AsyncHTTPClientWrapper
from redis.backoff import NoBackoff
from redis.client import Redis as SyncRedis
from redis.cluster import RedisCluster as SyncRedisCluster
from redis.connection import SSLConnection as SyncSSLConnection
from redis.event import (
    EventDispatcher,
    EventListenerInterface,
    MaintenanceCompletedEvent,
    MaintenanceEvent,
    MaintenanceStartedEvent,
)
from redis.exceptions import ConnectionError, TimeoutError
from redis.http.http_client import HttpClient
from redis.maint_notifications import MaintenanceState, MaintNotificationsConfig
from redis.multidb.exception import UnhealthyDatabaseException
from redis.retry import Retry
from redis.typing import Number

# Type alias for async Redis clients (standalone or cluster)
AsyncRedisClientT = Union[AsyncRedis, AsyncRedisCluster]


def _get_init_params(cls: Type) -> frozenset:
    """Extract parameter names from a class's __init__ method."""
    sig = inspect.signature(cls.__init__)
    return frozenset(
        name
        for name, param in sig.parameters.items()
        if name != "self"
        and param.kind
        in (
            inspect.Parameter.POSITIONAL_OR_KEYWORD,
            inspect.Parameter.KEYWORD_ONLY,
        )
    )


def _filter_kwargs(kwargs: dict, cls: Type) -> dict:
    """Filter kwargs to only include parameters accepted by the class's __init__."""
    allowed = _get_init_params(cls)
    return {k: v for k, v in kwargs.items() if k in allowed}


def _uses_tls(client, conn_kwargs: dict) -> bool:
    """Detect whether a client's transport is TLS.

    The clients turn ``ssl=True`` into ``connection_class=SSLConnection``:
    the standalone clients hold it on their connection pool, the async
    cluster keeps it inside its connection kwargs, and the sync cluster
    keeps the plain ``ssl`` key there. None of these survive
    ``_filter_kwargs`` (``connection_class`` is not an ``__init__``
    parameter of the rebuilt clients), so TLS must be re-expressed as
    ``ssl=True`` explicitly.
    """
    if conn_kwargs.get("ssl"):
        return True
    connection_class = conn_kwargs.get("connection_class")
    if connection_class is None:
        pool = getattr(client, "connection_pool", None)
        connection_class = getattr(pool, "connection_class", None)
    return isinstance(connection_class, type) and issubclass(
        connection_class, (SyncSSLConnection, AsyncSSLConnection)
    )


DEFAULT_HEALTH_CHECK_PROBES = 3
DEFAULT_HEALTH_CHECK_INTERVAL = 5
DEFAULT_HEALTH_CHECK_TIMEOUT = 3
DEFAULT_HEALTH_CHECK_DELAY = 0.5
DEFAULT_LAG_AWARE_TOLERANCE = 5000

logger = logging.getLogger(__name__)


class HealthCheck(ABC):
    """
    Health check interface.
    """

    @property
    @abstractmethod
    def health_check_probes(self) -> int:
        """Number of probes to execute health checks."""
        pass

    @property
    @abstractmethod
    def health_check_delay(self) -> float:
        """Delay between health check probes."""
        pass

    @property
    @abstractmethod
    def health_check_timeout(self) -> float:
        """Timeout for the full health check operation (including all probes)."""
        pass

    @abstractmethod
    async def check_health(self, database, hc_client: AsyncRedisClientT) -> bool:
        """
        Function to determine the health status.

        Args:
            database: The database being checked
            hc_client: A Redis client (AsyncRedis or AsyncRedisCluster) to use for
                health checks. This client follows topology changes automatically.

        Returns:
            True if the database is healthy, False otherwise.
        """
        pass

    async def close(self) -> None:
        """
        Release resources held by this health check (clients, connections).

        The default implementation is a no-op for health checks that hold no
        resources of their own.
        """
        return None


def relaxed_health_check_budget(
    health_check: HealthCheck, relaxed_timeout: Number
) -> float:
    """
    The budget of a health check while its database is under maintenance:
    every probe allowed to take as long as the relaxed connection timeout,
    plus the delays between them.
    """
    return health_check.health_check_probes * (
        relaxed_timeout + health_check.health_check_delay
    )


def database_maint_notifications_config(client) -> Optional[MaintNotificationsConfig]:
    """
    The maintenance notifications config a database client was built with, or
    None when its notifications are off.

    The cluster clients hold the config as an attribute. The standalone clients
    keep it on their pool's notifications handler, and in their connection
    kwargs, only while the notifications are enabled: a disabled config is not
    retained, so None also stands for a client that has them disabled.
    """
    config = getattr(client, "maint_notifications_config", None)
    if config is None:
        pool = getattr(client, "connection_pool", None)
        handler = getattr(pool, "_maint_notifications_pool_handler", None)
        config = getattr(handler, "config", None)
    if config is None:
        config = client.get_connection_kwargs().get("maint_notifications_config")
    return config if isinstance(config, MaintNotificationsConfig) else None


# How often a check that has outlived its plain budget re-evaluates the
# maintenance windows relaxing it, so that a completed maintenance restores
# the plain budget without waiting out the relaxed one
MAINTENANCE_WINDOW_POLL_INTERVAL = 0.5


class _MaintenanceWindow:
    """One relaxation the probe client of a database is currently under."""

    __slots__ = ("expire_at", "relaxed_timeout")

    def __init__(self, expire_at: float, relaxed_timeout: Optional[Number]):
        self.expire_at = expire_at
        self.relaxed_timeout = relaxed_timeout


class _MaintenanceWindowTracker:
    """
    Tracks the maintenance windows the probe client of one database is in,
    keyed by the relaxed source: the pool for a handoff (MOVING), the
    connection for a migration or failover (MAINTENANCE).

    A window lasts, like the relaxation it tracks, until its completion event
    arrives: the completion notification, the handoff's TTL cleanup, or the
    disconnect of a connection still under maintenance all report one. A
    blocking relaxation (``relaxed_timeout=None``) has no timeout of its own
    to bound the budget with, so that window expires with the notification's
    time-to-live instead.
    """

    def __init__(self):
        self._windows: dict[tuple[MaintenanceState, int], _MaintenanceWindow] = {}

    @staticmethod
    def _key(event: MaintenanceEvent) -> tuple[MaintenanceState, int]:
        source = (
            event.connection_pool
            if event.state is MaintenanceState.MOVING
            else event.connection
        )
        return event.state, id(source)

    def start(self, event: MaintenanceStartedEvent) -> None:
        # A handoff is reported even when relaxation is disabled, for the
        # proactive reconnect it drives; there is nothing to relax the budget to
        if not event.config.is_relaxed_timeouts_enabled():
            return

        notification = event.notification
        expire_at = (
            notification.expire_at if notification is not None else time.monotonic()
        )
        self._windows[self._key(event)] = _MaintenanceWindow(
            expire_at, event.config.relaxed_timeout
        )

    def complete(self, event: MaintenanceCompletedEvent) -> None:
        self._windows.pop(self._key(event), None)

    def active_windows(self) -> List[_MaintenanceWindow]:
        now = time.monotonic()
        expired = [
            key
            for key, window in self._windows.items()
            if window.relaxed_timeout is None and window.expire_at <= now
        ]
        for key in expired:
            del self._windows[key]
        return list(self._windows.values())


class _MaintenanceWindowListener(EventListenerInterface):
    """Feeds the maintenance events of one probe client into its tracker."""

    def __init__(self, tracker: _MaintenanceWindowTracker):
        self._tracker = tracker

    def listen(self, event: MaintenanceEvent):
        if isinstance(event, MaintenanceStartedEvent):
            self._tracker.start(event)
        elif isinstance(event, MaintenanceCompletedEvent):
            self._tracker.complete(event)
        else:
            return
        if logger.isEnabledFor(logging.DEBUG):
            logger.debug(
                f"Health check client {type(event).__name__}: state={event.state}, "
                f"notification={event.notification}"
            )


class HealthCheckPolicy(ABC):
    """
    Health checks execution policy.
    """

    @abstractmethod
    async def execute(self, health_checks: List[HealthCheck], database) -> bool:
        """Execute health checks and return database health status."""
        pass

    @abstractmethod
    async def _execute(self, health_check: HealthCheck, database) -> bool:
        """
        Executes health check against given database.
        """
        pass

    @abstractmethod
    async def get_client(self, database) -> AsyncRedisClientT:
        """
        Get a health check client for the database.
        """
        pass

    @abstractmethod
    async def close(self) -> None:
        """Close all health check clients."""
        pass


class AbstractHealthCheckPolicy(HealthCheckPolicy):
    """
    Abstract health check policy.
    """

    def __init__(self):
        # Single client per database, keyed by database id
        self._clients: dict[int, AsyncRedisClientT] = {}
        # The maintenance windows each database's client is in, keyed by
        # database id; only present for databases with maintenance
        # notifications enabled
        self._maintenance_trackers: dict[int, _MaintenanceWindowTracker] = {}

    async def execute(self, health_checks: List[HealthCheck], database) -> bool:
        """
        Execute all health checks concurrently with individual timeouts.
        Each health check runs with its own timeout, and all run in parallel.

        All exception handling is centralized here - _execute() methods just
        propagate exceptions naturally.
        """

        # Run all health checks concurrently and collect results/exceptions
        results = await asyncio.gather(
            *[self._execute_with_budget(hc, database) for hc in health_checks],
            return_exceptions=True,
        )

        # Check results - handle exceptions and failures
        for result in results:
            if isinstance(result, Exception):
                # Any exception (including TimeoutError) makes the database unhealthy
                raise UnhealthyDatabaseException("Unhealthy database", database, result)
            elif not result:
                # Health check returned False
                return False

        return True

    async def _execute_with_budget(self, health_check: HealthCheck, database) -> bool:
        """
        Run one health check under its budget.

        The budget is ``health_check_timeout``, relaxed while the database is
        under a server maintenance: the probe client reports the maintenance
        windows it is in, and a check that runs out of the plain budget inside
        one is given the relaxed budget instead, measured from its start. The
        windows are re-evaluated while the check is relaxed, so a maintenance
        that completes restores the plain budget - a probe still hung after it
        does not get to use up the relaxed one. A stall with no notification
        behind it - an unplanned failure - keeps the plain budget.
        """
        task = asyncio.ensure_future(self._execute(health_check, database))
        start = time.monotonic()
        deadline = start + health_check.health_check_timeout

        try:
            while True:
                done, _ = await asyncio.wait(
                    {task}, timeout=max(deadline - time.monotonic(), 0)
                )
                if done:
                    return task.result()

                now = time.monotonic()
                relaxed_deadline = self._relaxed_deadline(database, health_check, start)
                if relaxed_deadline is not None and relaxed_deadline > now:
                    deadline = min(
                        relaxed_deadline, now + MAINTENANCE_WINDOW_POLL_INTERVAL
                    )
                    continue

                task.cancel()
                try:
                    await task
                except asyncio.CancelledError as exc:
                    raise asyncio.TimeoutError() from exc
                # The check completed while being cancelled
                return task.result()
        except asyncio.CancelledError:
            # Let the check's own cancellation cleanup finish before this call
            # returns, as asyncio.wait_for would: a check still winding down
            # after the policy has moved on could use a client closed since
            task.cancel()
            await asyncio.wait({task})
            if not task.cancelled():
                # Retrieve an exception the check raised while being cancelled
                task.exception()
            raise

    def _relaxed_deadline(
        self, database, health_check: HealthCheck, start: float
    ) -> Optional[float]:
        """
        The deadline the active maintenance windows relax the check to, or
        None when the database is not under maintenance.

        The tracker is looked up here rather than when the check starts: the
        first check of a database is what creates its client, and the tracker
        with it, so a maintenance announced during that check would otherwise
        go unnoticed.
        """
        tracker = self._maintenance_trackers.get(id(database))
        if tracker is None:
            return None

        deadline = None
        for window in tracker.active_windows():
            if window.relaxed_timeout is None:
                # Blocking sockets: the window's own expiry is the only bound
                candidate = window.expire_at
            else:
                candidate = start + relaxed_health_check_budget(
                    health_check, window.relaxed_timeout
                )
            if deadline is None or candidate > deadline:
                deadline = candidate

        return deadline

    def _probe_maint_notifications_config(
        self, db_id: int, client
    ) -> MaintNotificationsConfig:
        """
        The maintenance notifications config for the health check client of a
        database: a copy of the database client's config reporting the
        maintenance windows the probe enters to this policy, when the
        notifications are enabled; the database client's own config, or an
        explicitly disabled one, otherwise. The probe must never enable what
        the database client has not - without a config of its own it would
        default to ``"auto"`` on RESP3. The database's config stays untouched.
        """
        source_config = database_maint_notifications_config(client)
        if source_config is None or not source_config.enabled:
            return source_config or MaintNotificationsConfig(enabled=False)

        tracker = _MaintenanceWindowTracker()
        listener = _MaintenanceWindowListener(tracker)
        probe_config = copy.copy(source_config)
        probe_config.event_dispatcher = EventDispatcher(
            {
                MaintenanceStartedEvent: [listener],
                MaintenanceCompletedEvent: [listener],
            }
        )
        self._maintenance_trackers[db_id] = tracker
        return probe_config

    async def get_client(self, database) -> AsyncRedisClientT:
        """
        Get or create a health check client for the database.

        Creates a single client instance per database that follows topology
        changes automatically. For cluster databases, the client handles
        node discovery and slot mapping internally.
        """
        db_id = id(database)
        client = self._clients.get(db_id)

        if client is None:
            # Check for both sync and async standalone Redis clients
            if isinstance(database.client, (AsyncRedis, SyncRedis)):
                conn_kwargs = database.client.get_connection_kwargs()
                filtered_kwargs = _filter_kwargs(conn_kwargs, AsyncRedis)
                if _uses_tls(database.client, conn_kwargs):
                    # Preserve the original transport: the source kwargs carry
                    # TLS as connection_class=SSLConnection, which the filter
                    # drops, so without this the probe would speak plaintext
                    # to a TLS port and mark a healthy database unavailable.
                    filtered_kwargs["ssl"] = True
                filtered_kwargs["maint_notifications_config"] = (
                    self._probe_maint_notifications_config(db_id, database.client)
                )
                client = AsyncRedis(**filtered_kwargs)
            elif isinstance(database.client, (AsyncRedisCluster, SyncRedisCluster)):
                # Cluster client - create a single cluster client that handles
                # topology changes internally
                conn_kwargs = database.client.get_connection_kwargs().copy()
                filtered_kwargs = _filter_kwargs(conn_kwargs, AsyncRedisCluster)
                if _uses_tls(database.client, conn_kwargs):
                    # Same transport preservation as the standalone branch
                    # above; the async cluster also represents TLS as
                    # connection_class=SSLConnection in its connection kwargs.
                    filtered_kwargs["ssl"] = True
                filtered_kwargs["maint_notifications_config"] = (
                    self._probe_maint_notifications_config(db_id, database.client)
                )
                startup_nodes = database.client.startup_nodes
                if startup_nodes:
                    nodes_manager = database.client.nodes_manager
                    # The sync and async NodesManager expose this setting under
                    # different names (``_require_full_coverage`` vs
                    # ``require_full_coverage``), so resolve it defensively to
                    # support a sync RedisCluster underlying client too.
                    require_full_coverage = getattr(
                        nodes_manager,
                        "require_full_coverage",
                        getattr(nodes_manager, "_require_full_coverage", True),
                    )
                    # Seed the health-check client with every configured startup
                    # node, not just the first: if the first seed is down while
                    # another is healthy, the probe must still be able to discover
                    # the cluster instead of marking the database unhealthy.
                    client = AsyncRedisCluster(
                        startup_nodes=[
                            AsyncClusterNode(node.host, node.port)
                            for node in startup_nodes
                        ],
                        dynamic_startup_nodes=nodes_manager._dynamic_startup_nodes,
                        address_remap=nodes_manager.address_remap,
                        require_full_coverage=require_full_coverage,
                        retry=database.client.retry,
                        **filtered_kwargs,
                    )
                else:
                    raise ValueError(
                        "Cluster client has no nodes - cannot create health check client"
                    )
            else:
                raise TypeError(f"Unsupported client type: {type(database.client)}")
            self._clients[db_id] = client

        return client

    async def close(self) -> None:
        """Close all health check clients."""
        close_tasks = [
            asyncio.create_task(client.aclose()) for client in self._clients.values()
        ]

        if close_tasks:
            await asyncio.gather(*close_tasks, return_exceptions=True)

        self._clients.clear()
        self._maintenance_trackers.clear()

    @abstractmethod
    async def _execute(self, health_check: HealthCheck, database) -> bool:
        """
        Executes health check against given database.
        """
        pass


class HealthyAllPolicy(AbstractHealthCheckPolicy):
    """
    Policy that returns True if all health check probes are successful.
    """

    async def _execute(self, health_check: HealthCheck, database) -> bool:
        """
        Executes health check against given database.

        Uses a single client that handles topology changes automatically.
        """
        client = await self.get_client(database)
        probes = health_check.health_check_probes

        for attempt in range(probes):
            result = await health_check.check_health(database, client)
            if not result:
                return False

            if attempt < probes - 1:
                await asyncio.sleep(health_check.health_check_delay)

        return True


class HealthyMajorityPolicy(AbstractHealthCheckPolicy):
    """
    Policy that returns True if a majority of health check probes are successful.

    Majority means more than half must pass:
    - 3 probes: need 2+ to pass (1 failure allowed)
    - 4 probes: need 3+ to pass (1 failure allowed, tie = unhealthy)
    - 5 probes: need 3+ to pass (2 failures allowed)
    """

    async def _execute(self, health_check: HealthCheck, database) -> bool:
        """
        Executes health check against given database.

        Uses a single client that handles topology changes automatically.
        """
        probes = health_check.health_check_probes
        # Strict majority: more than half must pass
        # (probes - 1) // 2 gives the max allowed failures
        allowed_unsuccessful_probes = (probes - 1) // 2
        client = await self.get_client(database)
        last_exception = None

        for attempt in range(probes):
            try:
                result = await health_check.check_health(database, client)
                if not result:
                    # Probe failed (returned False)
                    allowed_unsuccessful_probes -= 1
                    if allowed_unsuccessful_probes < 0:
                        return False
            except Exception as e:
                # Probe failed (exception)
                last_exception = e
                allowed_unsuccessful_probes -= 1
                if allowed_unsuccessful_probes < 0:
                    raise last_exception

            if attempt < probes - 1:
                await asyncio.sleep(health_check.health_check_delay)

        return True


class HealthyAnyPolicy(AbstractHealthCheckPolicy):
    """
    Policy that returns True if at least one health check probe is successful.
    """

    async def _execute(self, health_check: HealthCheck, database) -> bool:
        """
        Executes health check against given database.

        Uses a single client that handles topology changes automatically.
        """
        probes = health_check.health_check_probes
        last_exception = None
        client = await self.get_client(database)

        for attempt in range(probes):
            try:
                result = await health_check.check_health(database, client)
                if result:
                    # At least one probe succeeded
                    return True
            except Exception as e:
                last_exception = e

            if attempt < probes - 1:
                await asyncio.sleep(health_check.health_check_delay)

        # All probes failed
        if last_exception:
            raise last_exception

        return False


class HealthCheckPolicies(Enum):
    HEALTHY_ALL = HealthyAllPolicy
    HEALTHY_MAJORITY = HealthyMajorityPolicy
    HEALTHY_ANY = HealthyAnyPolicy


DEFAULT_HEALTH_CHECK_POLICY: HealthCheckPolicies = HealthCheckPolicies.HEALTHY_ALL


class AbstractHealthCheck(HealthCheck):
    def __init__(
        self,
        health_check_probes: int = DEFAULT_HEALTH_CHECK_PROBES,
        health_check_delay: float = DEFAULT_HEALTH_CHECK_DELAY,
        health_check_timeout: float = DEFAULT_HEALTH_CHECK_TIMEOUT,
    ):
        if health_check_probes < 1:
            raise ValueError("health_check_probes must be greater than 0")
        self._health_check_probes = health_check_probes
        self._health_check_delay = health_check_delay
        self._health_check_timeout = health_check_timeout

    @property
    def health_check_probes(self) -> int:
        return self._health_check_probes

    @property
    def health_check_delay(self) -> float:
        return self._health_check_delay

    @property
    def health_check_timeout(self) -> float:
        return self._health_check_timeout

    @abstractmethod
    async def check_health(self, database, hc_client: AsyncRedisClientT) -> bool:
        pass


class PingHealthCheck(AbstractHealthCheck):
    """
    Health check based on PING command.
    """

    async def check_health(self, database, hc_client: AsyncRedisClientT) -> bool:
        if isinstance(hc_client, AsyncRedis):
            return await hc_client.execute_command("PING")
        else:
            # For a cluster checks if all nodes are healthy.
            # The health check client is created lazily, so the topology has to be
            # discovered before pinging - an uninitialized client has an empty node
            # cache, so there would be nothing to ping and the database would be
            # reported as healthy. Once discovered, this is a no-op.
            await hc_client.initialize()
            all_nodes = hc_client.get_nodes()

            if not all_nodes:
                return False

            # The nodes are pinged concurrently rather than one after another: a
            # single health_check_timeout covers the whole health check - every
            # probe of it - so a serial loop makes the budget scale with the size
            # of the cluster. Gathering keeps a probe at the cost of one round
            # trip. Exceptions are collected rather than propagated by gather
            # itself, so that a failing node does not leave its siblings running
            # unawaited; the first one is re-raised below to keep the caller's
            # error handling unchanged.
            results = await asyncio.gather(
                *(node.execute_command("PING") for node in all_nodes),
                return_exceptions=True,
            )

            for result in results:
                if isinstance(result, BaseException):
                    if isinstance(result, (ConnectionError, TimeoutError, OSError)):
                        # A failing node may have been removed or replaced in the
                        # cluster, and direct node probes bypass the client's own
                        # topology-refresh path. Closing the client resets its lazy
                        # initialization, so the next probe re-discovers the
                        # topology from the startup nodes instead of pinging the
                        # stale node forever.
                        await hc_client.aclose()
                    raise result

                if not result:
                    return False

            return True


class LagAwareHealthCheck(AbstractHealthCheck):
    """
    Health check available for Redis Enterprise deployments.
    Verify via REST API that the database is healthy based on different lags.
    """

    def __init__(
        self,
        rest_api_port: int = 9443,
        lag_aware_tolerance: int = DEFAULT_LAG_AWARE_TOLERANCE,
        http_timeout: float = DEFAULT_TIMEOUT,
        auth_basic: Optional[Tuple[str, str]] = None,
        verify_tls: bool = True,
        # TLS verification (server) options
        ca_file: Optional[str] = None,
        ca_path: Optional[str] = None,
        ca_data: Optional[Union[str, bytes]] = None,
        # Mutual TLS (client cert) options
        client_cert_file: Optional[str] = None,
        client_key_file: Optional[str] = None,
        client_key_password: Optional[str] = None,
        # Health check configuration
        health_check_probes: int = DEFAULT_HEALTH_CHECK_PROBES,
        health_check_delay: float = DEFAULT_HEALTH_CHECK_DELAY,
        health_check_timeout: float = DEFAULT_HEALTH_CHECK_TIMEOUT,
    ):
        """
        Initialize LagAwareHealthCheck with the specified parameters.

        Args:
            rest_api_port: Port number for Redis Enterprise REST API (default: 9443)
            lag_aware_tolerance: Tolerance in lag between databases in MS (default: 100)
            http_timeout: Request timeout in seconds (default: DEFAULT_TIMEOUT)
            auth_basic: Tuple of (username, password) for basic authentication
            verify_tls: Whether to verify TLS certificates (default: True)
            ca_file: Path to CA certificate file for TLS verification
            ca_path: Path to CA certificates directory for TLS verification
            ca_data: CA certificate data as string or bytes
            client_cert_file: Path to client certificate file for mutual TLS
            client_key_file: Path to client private key file for mutual TLS
            client_key_password: Password for encrypted client private key
        """
        self._http_client = AsyncHTTPClientWrapper(
            HttpClient(
                timeout=http_timeout,
                auth_basic=auth_basic,
                retry=Retry(NoBackoff(), retries=0),
                verify_tls=verify_tls,
                ca_file=ca_file,
                ca_path=ca_path,
                ca_data=ca_data,
                client_cert_file=client_cert_file,
                client_key_file=client_key_file,
                client_key_password=client_key_password,
            )
        )
        self._rest_api_port = rest_api_port
        self._lag_aware_tolerance = lag_aware_tolerance
        super().__init__(
            health_check_probes=health_check_probes,
            health_check_delay=health_check_delay,
            health_check_timeout=health_check_timeout,
        )

    async def close(self) -> None:
        """Shut down the HTTP client thread pool used for REST API calls."""
        await self._http_client.aclose()

    @staticmethod
    def _database_hosts(database) -> set[str]:
        """
        The hosts the given database is known by on the Redis Enterprise REST API.

        For a cluster client these are the endpoints the client was configured with,
        not the nodes it discovered: CLUSTER SLOTS advertises each node's own address,
        which on a public Redis Enterprise endpoint is neither the endpoint DNS name
        nor an address the bdb lists. ``startup_nodes`` is read off the client rather
        than off its nodes manager, because the latter is replaced by the discovered
        nodes when ``dynamic_startup_nodes`` is enabled, as it is by default.
        """
        if isinstance(database.client, (AsyncRedis, SyncRedis)):
            return {database.client.get_connection_kwargs()["host"]}

        # Cluster client
        return {node.host for node in database.client.startup_nodes}

    @staticmethod
    def _find_matching_bdb(bdbs, db_hosts: set[str]) -> Optional[dict]:
        """
        The bdb whose endpoints identify one of the given database hosts.

        A DNS name belongs to a single bdb, while databases hosted by the same cluster
        share the addresses their endpoints resolve to, so a match by DNS name is
        preferred over a match by address across all of the bdbs.
        """
        addr_match = None

        for bdb in bdbs:
            for endpoint in bdb["endpoints"]:
                if endpoint["dns_name"] in db_hosts:
                    return bdb

                # In case if the host was set as public IP
                if addr_match is None and any(
                    addr in db_hosts for addr in endpoint["addr"]
                ):
                    addr_match = bdb

        return addr_match

    async def check_health(self, database, hc_client: AsyncRedisClientT) -> bool:
        """
        Check database health via Redis Enterprise REST API.

        Note: The client parameter is not used for this health check as it
        relies on the REST API instead of Redis protocol. The client is
        accepted for interface compatibility.
        """
        if database.health_check_url is None:
            raise ValueError(
                "Database health check url is not set. Please check DatabaseConfig for the current database."
            )

        db_hosts = self._database_hosts(database)

        # Absolute URLs, rather than a base_url set on the HTTP client: one
        # LagAwareHealthCheck instance serves every database and MultiDBClient
        # checks them concurrently, so a base_url assignment made here would be
        # overwritten by another database's check while this one is awaiting its
        # response - sending the second request to the wrong cluster.
        base_url = f"{database.health_check_url}:{self._rest_api_port}"

        # Find bdb matching to the current database host. The per-request
        # deadline stays the client's configured http_timeout; the whole check
        # is already bounded by health_check_timeout in the policy.
        matching_bdb = self._find_matching_bdb(
            await self._http_client.get(f"{base_url}/v1/bdbs"),
            db_hosts,
        )

        if matching_bdb is None:
            logger.warning(
                "LagAwareHealthCheck failed: Couldn't find a bdb matching any of "
                f"the database hosts {sorted(db_hosts)}"
            )
            raise ValueError("Could not find a matching bdb")

        url = (
            f"{base_url}/v1/bdbs/{matching_bdb['uid']}/availability"
            f"?extend_check=lag&availability_lag_tolerance_ms={self._lag_aware_tolerance}"
        )
        await self._http_client.get(url, expect_json=False)

        # Status checked in an http client, otherwise HttpError will be raised
        return True
