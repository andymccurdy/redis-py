import asyncio
import logging
import threading
from typing import Any, Callable, Dict, Iterable, List, Literal, Optional

from redis._parsers import Encoder
from redis.asyncio.multidb.healthcheck import (
    HealthCheck,
    HealthCheckPolicy,
    database_maint_notifications_config,
    relaxed_health_check_budget,
)
from redis.background import BackgroundScheduler
from redis.backoff import NoBackoff
from redis.client import PubSubWorkerThread
from redis.commands import CoreCommands, RedisModuleCommands
from redis.exceptions import DataError, RedisClusterUnreachableError
from redis.maint_notifications import MaintNotificationsConfig
from redis.multidb.circuit import CircuitBreaker
from redis.multidb.circuit import State as CBState
from redis.multidb.command_executor import DefaultCommandExecutor
from redis.multidb.config import (
    DEFAULT_GRACE_PERIOD,
    DatabaseConfig,
    InitialHealthCheck,
    MultiDbConfig,
)
from redis.multidb.database import Database, Databases, SyncDatabase
from redis.multidb.exception import (
    InitialHealthCheckFailedError,
    NoValidDatabaseException,
    UnhealthyDatabaseException,
)
from redis.multidb.failure_detector import FailureDetector
from redis.observability.attributes import GeoFailoverReason
from redis.retry import Retry
from redis.typing import ChannelT, PubSubHandler, Subscription
from redis.utils import experimental

logger = logging.getLogger(__name__)

# The floor of the time a health check run from the calling thread is waited
# for, and the margin left on top of the largest budget a check can take
DEFAULT_SYNC_HEALTH_CHECK_TIMEOUT = 10.0
SYNC_HEALTH_CHECK_TIMEOUT_MARGIN = 1.0


@experimental
class MultiDBClient(RedisModuleCommands, CoreCommands):
    """
    Client that operates on multiple logical Redis databases.
    Should be used in Client-side geographic failover database setups.
    """

    def __init__(self, config: MultiDbConfig):
        self._databases = config.databases()
        # Removed databases, kept so that close() still closes their clients
        self._removed_databases: List[SyncDatabase] = []
        self._health_checks = (
            config.default_health_checks()
            if not config.health_checks
            else config.health_checks
        )
        self._health_check_interval = config.health_check_interval
        self._health_check_policy: HealthCheckPolicy = (
            config.health_check_policy.value()
        )
        self._failure_detectors = (
            config.default_failure_detectors()
            if not config.failure_detectors
            else config.failure_detectors
        )

        self._failover_strategy = (
            config.default_failover_strategy()
            if config.failover_strategy is None
            else config.failover_strategy
        )
        self._failover_strategy.set_databases(self._databases)
        self._auto_fallback_interval = config.auto_fallback_interval
        self._event_dispatcher = config.event_dispatcher
        self._command_retry = config.command_retry
        self._command_retry.update_supported_errors(
            (ConnectionRefusedError, RedisClusterUnreachableError)
        )
        self.command_executor = DefaultCommandExecutor(
            failure_detectors=self._failure_detectors,
            databases=self._databases,
            command_retry=self._command_retry,
            failover_strategy=self._failover_strategy,
            failover_attempts=config.failover_attempts,
            failover_delay=config.failover_delay,
            event_dispatcher=self._event_dispatcher,
            auto_fallback_interval=self._auto_fallback_interval,
        )
        self.initialized = False
        self._bg_scheduler = BackgroundScheduler()
        self._hc_lock = threading.Lock()
        self._config = config

    def __del__(self):
        try:
            self.close()
        except Exception:
            # Suppress exceptions during garbage collection.
            # close() may fail if called during interpreter shutdown
            # or while an event loop is already running.
            pass

    def initialize(self):
        """
        Perform initialization of databases to define their initial state.
        """

        # Initial databases check to define initial state.
        # Uses run_coro_sync to run in the shared background loop - this ensures
        # connection pools created during initial health check remain valid for
        # subsequent recurring health checks (they use the same event loop).
        self._bg_scheduler.run_coro_sync(
            self._perform_initial_health_check,
            timeout=self._sync_health_check_timeout(
                database for database, _ in self._databases
            ),
        )

        # Starts recurring health checks on the background.
        # Uses run_recurring_coro which shares the same event loop as run_coro_sync
        self._bg_scheduler.run_recurring_coro(
            self._health_check_interval,
            self._check_databases_health,
        )

        is_active_db_found = False

        for database, weight in self._databases:
            # Set on state changed callback for each circuit.
            database.circuit.on_state_changed(self._on_circuit_state_change_callback)

            # Set states according to a weights and circuit state
            if database.circuit.state == CBState.CLOSED and not is_active_db_found:
                # Directly set the active database during initialization
                # without recording a geo failover metric
                self.command_executor._active_database = database
                is_active_db_found = True

        if not is_active_db_found:
            raise NoValidDatabaseException(
                "Initial connection failed - no active database found"
            )

        self.initialized = True

    def get_connection_kwargs(self) -> Dict[str, Any]:
        """
        Get the connection keyword arguments of the active database's client.
        """
        return self._underlying_client().get_connection_kwargs()

    def get_encoder(self) -> Encoder:
        """
        Get the encoder of the active database's client.
        """
        return self._underlying_client().get_encoder()

    def _underlying_client(self):
        active_database = self.command_executor.active_database
        if active_database is not None:
            return active_database.client

        # Before initialization there is no active database yet, so use the
        # first configured one (databases are kept in descending weight order).
        database, _ = self._databases.get_top_n(1)[0]
        return database.client

    def get_databases(self) -> Databases:
        """
        Returns a sorted (by weight) list of all databases.
        """
        return self._databases

    def set_active_database(self, database: SyncDatabase) -> None:
        """
        Promote one of the existing databases to become an active.
        """
        exists = None

        for existing_db, _ in self._databases:
            if existing_db == database:
                exists = True
                break

        if not exists:
            raise ValueError("Given database is not a member of database list")

        self._bg_scheduler.run_coro_sync(
            self._check_db_health,
            database,
            timeout=self._sync_health_check_timeout((database,)),
        )

        if database.circuit.state == CBState.CLOSED:
            highest_weighted_db, _ = self._databases.get_top_n(1)[0]
            self.command_executor.active_database = (
                database,
                GeoFailoverReason.MANUAL,
            )
            return

        raise NoValidDatabaseException(
            "Cannot set active database, database is unhealthy"
        )

    def add_database(
        self, config: DatabaseConfig, skip_initial_health_check: bool = True
    ):
        """
        Adds a new database to the database list.

        Args:
            config: DatabaseConfig object that contains the database configuration.
            skip_initial_health_check: If True, adds the database even if it is unhealthy.
        """
        # The retry object is not used in the lower level clients, so we can safely remove it.
        # We rely on command_retry in terms of global retries.
        config.client_kwargs["retry"] = Retry(retries=0, backoff=NoBackoff())

        # Maintenance notifications are disabled by default in underlying clients,
        # but user can override this by providing their own config. A supplied
        # pool keeps its own configuration.
        if (
            not config.from_pool
            and "maint_notifications_config" not in config.client_kwargs
        ):
            config.client_kwargs["maint_notifications_config"] = (
                MaintNotificationsConfig(enabled=False)
            )

        if config.from_url:
            client = self._config.client_class.from_url(
                config.from_url, **config.client_kwargs
            )
        elif config.from_pool:
            config.from_pool.set_retry(Retry(retries=0, backoff=NoBackoff()))
            client = self._config.client_class.from_pool(
                connection_pool=config.from_pool
            )
        else:
            client = self._config.client_class(**config.client_kwargs)

        circuit = (
            config.default_circuit_breaker()
            if config.circuit is None
            else config.circuit
        )

        database = Database(
            client=client,
            circuit=circuit,
            weight=config.weight,
            health_check_url=config.health_check_url,
        )

        try:
            self._bg_scheduler.run_coro_sync(
                self._check_db_health,
                database,
                timeout=self._sync_health_check_timeout((database,)),
            )
        except UnhealthyDatabaseException:
            if not skip_initial_health_check:
                raise

        highest_weighted_db, highest_weight = self._databases.get_top_n(1)[0]
        self._databases.add(database, database.weight)
        self._change_active_database(database, highest_weighted_db)

    def _change_active_database(
        self, new_database: SyncDatabase, highest_weight_database: SyncDatabase
    ):
        if (
            new_database.weight > highest_weight_database.weight
            and new_database.circuit.state == CBState.CLOSED
        ):
            self.command_executor.active_database = (
                new_database,
                GeoFailoverReason.AUTOMATIC,
            )

    def remove_database(self, database: Database):
        """
        Removes a database from the database list.
        """
        weight = self._databases.remove(database)
        # The client is left open for in-flight commands when the database
        # was the active one, so it is closed with the others on close()
        self._removed_databases.append(database)
        highest_weighted_db, highest_weight = self._databases.get_top_n(1)[0]

        if (
            highest_weight <= weight
            and highest_weighted_db.circuit.state == CBState.CLOSED
        ):
            self.command_executor.active_database = (
                highest_weighted_db,
                GeoFailoverReason.MANUAL,
            )

    def update_database_weight(self, database: SyncDatabase, weight: float):
        """
        Updates a database from the database list.
        """
        exists = None

        for existing_db, _ in self._databases:
            if existing_db == database:
                exists = True
                break

        if not exists:
            raise ValueError("Given database is not a member of database list")

        highest_weighted_db, highest_weight = self._databases.get_top_n(1)[0]
        self._databases.update_weight(database, weight)
        database.weight = weight
        self._change_active_database(database, highest_weighted_db)

    def add_failure_detector(self, failure_detector: FailureDetector):
        """
        Adds a new failure detector to the database.
        """
        self._failure_detectors.append(failure_detector)

    def add_health_check(self, healthcheck: HealthCheck):
        """
        Adds a new health check to the database.
        """
        with self._hc_lock:
            self._health_checks.append(healthcheck)

    def execute_command(self, *args, **options):
        """
        Executes a single command and return its result.
        """
        if not self.initialized:
            self.initialize()

        return self.command_executor.execute_command(*args, **options)

    # HIMPORT is not supported on the multi-database client. A HIMPORT fieldset is
    # per-connection server session state tracked by a single client's registry;
    # there is no coherent way to keep that state consistent across independent
    # database clients through failover. ``himport_set`` is inherited from
    # ``CoreCommands`` (and the lifecycle methods would otherwise be missing entirely),
    # so override them to fail early and clearly instead of surfacing a confusing
    # ``no such fieldset`` at runtime.
    _HIMPORT_UNSUPPORTED = (
        "HIMPORT is not supported on the multi-database (Active-Active) client"
    )

    def himport_prepare(self, *args: Any, **kwargs: Any) -> Any:
        raise DataError(self._HIMPORT_UNSUPPORTED)

    def himport_set(self, *args: Any, **kwargs: Any) -> Any:
        raise DataError(self._HIMPORT_UNSUPPORTED)

    def himport_discard(self, *args: Any, **kwargs: Any) -> Any:
        raise DataError(self._HIMPORT_UNSUPPORTED)

    def himport_discard_all(self, *args: Any, **kwargs: Any) -> Any:
        raise DataError(self._HIMPORT_UNSUPPORTED)

    def pipeline(self):
        """
        Enters into pipeline mode of the client.
        """
        return Pipeline(self)

    def transaction(self, func: Callable[["Pipeline"], None], *watches, **options):
        """
        Executes callable as transaction.
        """
        if not self.initialized:
            self.initialize()

        return self.command_executor.execute_transaction(func, *watches, **options)

    def pubsub(self, **kwargs):
        """
        Return a Publish/Subscribe object. With this object, you can
        subscribe to channels and listen for messages that get published to
        them.
        """
        if not self.initialized:
            self.initialize()

        return PubSub(self, **kwargs)

    def _sync_health_check_timeout(
        self, databases: Iterable[SyncDatabase]
    ) -> Optional[float]:
        """
        How long a health check of the given databases, run from the calling
        thread, is waited for before it is given up on.

        The wait has to hold the largest budget a check can take, or a check
        that legitimately runs long would surface as a ``TimeoutError`` to the
        caller instead of as an unhealthy database. A budget is relaxed while
        the database is under a server maintenance (see
        ``relaxed_health_check_budget``), so databases with maintenance
        notifications enabled count with their relaxed budget. A blocking
        relaxed timeout (``None``) relaxes the budget up to the notification's
        time-to-live, which is not known here: the wait is then unbounded, and
        the check itself ends when that window does.
        """
        with self._hc_lock:
            health_checks = list(self._health_checks)

        timeout = DEFAULT_SYNC_HEALTH_CHECK_TIMEOUT
        for database in databases:
            config = database_maint_notifications_config(database.client)
            if not (
                config is not None
                and config.enabled
                and config.is_relaxed_timeouts_enabled()
            ):
                relaxed_timeout = None
                relaxed = False
            else:
                relaxed_timeout = config.relaxed_timeout
                relaxed = True
                if relaxed_timeout is None:
                    return None

            for health_check in health_checks:
                budget = health_check.health_check_timeout
                if relaxed:
                    budget = max(
                        budget,
                        relaxed_health_check_budget(health_check, relaxed_timeout),
                    )
                timeout = max(timeout, budget + SYNC_HEALTH_CHECK_TIMEOUT_MARGIN)

        return timeout

    async def _check_db_health(self, database: SyncDatabase) -> bool:
        """
        Runs health checks on the given database until first failure.
        """
        with self._hc_lock:
            health_checks = list(self._health_checks)

        # Health check will setup circuit state
        is_healthy = await self._health_check_policy.execute(health_checks, database)

        if not is_healthy:
            # The exception path is logged by the caller; a check that reports
            # unhealthy without raising would otherwise open the circuit silently
            logger.debug(f"Health check reported database unhealthy: {database}")
            if database.circuit.state != CBState.OPEN:
                database.circuit.state = CBState.OPEN
            return is_healthy
        elif is_healthy and database.circuit.state != CBState.CLOSED:
            database.circuit.state = CBState.CLOSED

        return is_healthy

    async def _check_databases_health(self) -> dict[Database, bool]:
        """
        Runs health checks as a recurring task.
        Runs health checks against all databases.
        """
        task_to_db: dict[asyncio.Task, Database] = {}

        self._hc_tasks = []
        for database, _ in self._databases:
            task = asyncio.create_task(self._check_db_health(database))
            task_to_db[task] = database
            self._hc_tasks.append(task)

        results = await asyncio.gather(*self._hc_tasks, return_exceptions=True)

        # Map end results to databases
        db_results = {
            task_to_db[task]: result for task, result in zip(self._hc_tasks, results)
        }

        for database, result in db_results.items():
            if isinstance(result, UnhealthyDatabaseException):
                unhealthy_db = result.database
                unhealthy_db.circuit.state = CBState.OPEN

                logger.debug(
                    "Health check failed, due to exception",
                    exc_info=result.original_exception,
                )

                db_results[unhealthy_db] = False

        return db_results

    async def _perform_initial_health_check(self):
        """
        Runs initial health check and evaluate healthiness based on initial_health_check_policy.
        """
        results = await self._check_databases_health()
        is_healthy = True

        if self._config.initial_health_check_policy == InitialHealthCheck.ALL_AVAILABLE:
            is_healthy = False not in results.values()
        elif (
            self._config.initial_health_check_policy
            == InitialHealthCheck.MAJORITY_AVAILABLE
        ):
            is_healthy = sum(results.values()) > len(results) / 2
        elif (
            self._config.initial_health_check_policy == InitialHealthCheck.ONE_AVAILABLE
        ):
            is_healthy = True in results.values()

        if not is_healthy:
            raise InitialHealthCheckFailedError(
                f"Initial health check failed. Initial health check policy: {self._config.initial_health_check_policy}"
            )

    def _on_circuit_state_change_callback(
        self, circuit: CircuitBreaker, old_state: CBState, new_state: CBState
    ):
        if new_state == CBState.HALF_OPEN:
            self._bg_scheduler.run_coro_fire_and_forget(
                self._check_db_health, circuit.database
            )
            return

        if old_state == CBState.CLOSED and new_state == CBState.OPEN:
            logger.warning(
                f"Database {circuit.database} is unreachable. Failover has been initiated."
            )

            self._bg_scheduler.run_once(
                DEFAULT_GRACE_PERIOD, _half_open_circuit, circuit
            )

        if old_state != CBState.CLOSED and new_state == CBState.CLOSED:
            logger.info(f"Database {circuit.database} is reachable again.")

    def close(self):
        """
        Closes the client and all its resources.
        """
        # Close health check policy and health checks BEFORE stopping the
        # scheduler. The policy's connection pools were created on the shared
        # health check event loop, so they must be disconnected on that same
        # loop to avoid leaking sockets/file descriptors.
        if self._bg_scheduler:
            # Each resource gets its own guard: a policy close that raises or
            # times out must not skip the health checks, which hold resources of
            # their own (the lag-aware check's HTTP thread pool).
            try:
                self._bg_scheduler.run_coro_sync(self._health_check_policy.close)
            except Exception:
                pass
            try:
                self._bg_scheduler.run_coro_sync(self._close_health_checks)
            except Exception:
                pass
            self._bg_scheduler.stop()

        # Close every database client: a failover leaves the previous active
        # database's client open, so its in-flight commands can complete. A
        # failing client must not leave the later ones open, so the first error
        # is raised only after every client was closed.
        error = None
        databases = [database for database, _ in self._databases]
        databases.extend(self._removed_databases)
        # A removed database is only kept to be closed here
        self._removed_databases.clear()
        for database in databases:
            try:
                database.client.close()
            except Exception as e:
                if error is None:
                    error = e
        if error is not None:
            raise error

    async def _close_health_checks(self):
        # Keep closing the remaining checks when one of them raises, the way the
        # async client does with gather(..., return_exceptions=True): a single
        # failing check must not strand the resources of the others.
        for health_check in self._health_checks:
            try:
                await health_check.close()
            except Exception:
                pass


def _half_open_circuit(circuit: CircuitBreaker):
    circuit.state = CBState.HALF_OPEN


class Pipeline(RedisModuleCommands, CoreCommands):
    """
    Pipeline implementation for multiple logical Redis databases.
    """

    _is_async_client: Literal[False] = False

    def __init__(self, client: MultiDBClient):
        self._command_stack = []
        self._client = client

    def get_connection_kwargs(self) -> Dict[str, Any]:
        """Get the connection keyword arguments of the parent client."""
        return self._client.get_connection_kwargs()

    def get_encoder(self) -> Encoder:
        """Get the encoder of the parent client."""
        return self._client.get_encoder()

    def __enter__(self) -> "Pipeline":
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        self.reset()

    def __del__(self):
        try:
            self.reset()
        except Exception:
            pass

    def __len__(self) -> int:
        return len(self._command_stack)

    def __bool__(self) -> bool:
        """Pipeline instances should always evaluate to True"""
        return True

    def reset(self) -> None:
        self._command_stack = []

    def close(self) -> None:
        """Close the pipeline"""
        self.reset()

    def pipeline_execute_command(self, *args, **options) -> "Pipeline":
        """
        Stage a command to be executed when execute() is next called

        Returns the current Pipeline object back so commands can be
        chained together, such as:

        pipe = pipe.set('foo', 'bar').incr('baz').decr('bang')

        At some other point, you can then run: pipe.execute(),
        which will execute all commands queued in the pipe.
        """
        self._command_stack.append((args, options))
        return self

    def execute_command(self, *args, **kwargs):
        """Adds a command to the stack"""
        return self.pipeline_execute_command(*args, **kwargs)

    # HIMPORT is unsupported on the multi-database client (see MultiDBClient). A
    # pipeline inherits ``himport_set`` from ``CoreCommands`` and would otherwise
    # queue an unprepared ``HIMPORT SET`` that fails with a confusing server-side
    # error, bypassing the client-level guard. Block all HIMPORT methods here too.
    def himport_prepare(self, *args: Any, **kwargs: Any) -> Any:
        raise DataError(MultiDBClient._HIMPORT_UNSUPPORTED)

    def himport_set(self, *args: Any, **kwargs: Any) -> Any:
        raise DataError(MultiDBClient._HIMPORT_UNSUPPORTED)

    def himport_discard(self, *args: Any, **kwargs: Any) -> Any:
        raise DataError(MultiDBClient._HIMPORT_UNSUPPORTED)

    def himport_discard_all(self, *args: Any, **kwargs: Any) -> Any:
        raise DataError(MultiDBClient._HIMPORT_UNSUPPORTED)

    def execute(self) -> List[Any]:
        """Execute all the commands in the current pipeline"""
        if not self._client.initialized:
            self._client.initialize()

        try:
            return self._client.command_executor.execute_pipeline(
                tuple(self._command_stack)
            )
        finally:
            self.reset()


class PubSub:
    """
    PubSub object for multi database client.
    """

    def __init__(self, client: MultiDBClient, **kwargs):
        """Initialize the PubSub object for a multi-database client.

        Args:
            client: MultiDBClient instance to use for pub/sub operations
            **kwargs: Additional keyword arguments to pass to the underlying pubsub implementation
        """

        self._client = client
        self._client.command_executor.pubsub(**kwargs)

    def __enter__(self) -> "PubSub":
        return self

    def __del__(self) -> None:
        try:
            # if this object went out of scope prior to shutting down
            # subscriptions, close the connection manually before
            # returning it to the connection pool
            self.reset()
        except Exception:
            pass

    def reset(self) -> None:
        return self._client.command_executor.execute_pubsub_method("reset")

    def close(self) -> None:
        self.reset()

    @property
    def subscribed(self) -> bool:
        return self._client.command_executor.active_pubsub.subscribed

    def execute_command(self, *args):
        return self._client.command_executor.execute_pubsub_method(
            "execute_command", *args
        )

    def psubscribe(
        self, *args: ChannelT | Subscription, **kwargs: PubSubHandler
    ) -> None:
        """
        Subscribe to channel patterns. Patterns supplied as keyword arguments
        expect a pattern name as the key and a callable as the value. A
        pattern's callable will be invoked automatically when a message is
        received on that pattern rather than producing a message via
        ``listen()``.
        """
        return self._client.command_executor.execute_pubsub_method(
            "psubscribe", *args, **kwargs
        )

    def punsubscribe(self, *args):
        """
        Unsubscribe from the supplied patterns. If empty, unsubscribe from
        all patterns.
        """
        return self._client.command_executor.execute_pubsub_method(
            "punsubscribe", *args
        )

    def subscribe(
        self, *args: ChannelT | Subscription, **kwargs: PubSubHandler
    ) -> None:
        """
        Subscribe to channels. Channels supplied as keyword arguments expect
        a channel name as the key and a callable as the value. A channel's
        callable will be invoked automatically when a message is received on
        that channel rather than producing a message via ``listen()`` or
        ``get_message()``.
        """
        return self._client.command_executor.execute_pubsub_method(
            "subscribe", *args, **kwargs
        )

    def unsubscribe(self, *args):
        """
        Unsubscribe from the supplied channels. If empty, unsubscribe from
        all channels
        """
        return self._client.command_executor.execute_pubsub_method("unsubscribe", *args)

    def ssubscribe(
        self, *args: ChannelT | Subscription, **kwargs: PubSubHandler
    ) -> None:
        """
        Subscribes the client to the specified shard channels.
        Channels supplied as keyword arguments expect a channel name as the key
        and a callable as the value. A channel's callable will be invoked automatically
        when a message is received on that channel rather than producing a message via
        ``listen()`` or ``get_sharded_message()``.
        """
        return self._client.command_executor.execute_pubsub_method(
            "ssubscribe", *args, **kwargs
        )

    def sunsubscribe(self, *args):
        """
        Unsubscribe from the supplied shard_channels. If empty, unsubscribe from
        all shard_channels
        """
        return self._client.command_executor.execute_pubsub_method(
            "sunsubscribe", *args
        )

    def get_message(
        self, ignore_subscribe_messages: bool = False, timeout: float = 0.0
    ):
        """
        Get the next message if one is available, otherwise None.

        If timeout is specified, the system will wait for `timeout` seconds
        before returning. Timeout should be specified as a floating point
        number, or None, to wait indefinitely.
        """
        return self._client.command_executor.execute_pubsub_method(
            "get_message",
            ignore_subscribe_messages=ignore_subscribe_messages,
            timeout=timeout,
        )

    def get_sharded_message(
        self, ignore_subscribe_messages: bool = False, timeout: float = 0.0
    ):
        """
        Get the next message if one is available in a sharded channel, otherwise None.

        If timeout is specified, the system will wait for `timeout` seconds
        before returning. Timeout should be specified as a floating point
        number, or None, to wait indefinitely.
        """
        return self._client.command_executor.execute_pubsub_method(
            "get_sharded_message",
            ignore_subscribe_messages=ignore_subscribe_messages,
            timeout=timeout,
        )

    def run_in_thread(
        self,
        sleep_time: float = 0.0,
        daemon: bool = False,
        exception_handler: Optional[Callable] = None,
        sharded_pubsub: bool = False,
    ) -> "PubSubWorkerThread":
        return self._client.command_executor.execute_pubsub_run(
            sleep_time,
            daemon=daemon,
            exception_handler=exception_handler,
            pubsub=self,
            sharded_pubsub=sharded_pubsub,
        )
