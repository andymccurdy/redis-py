import json
import logging
import os
import re
from time import monotonic, sleep
from typing import Any, Generator, Optional
from urllib.parse import urlparse

import pytest
from redis import RedisCluster

from redis.backoff import NoBackoff, ExponentialBackoff
from redis.event import EventDispatcher, EventListenerInterface
from redis.multidb.client import MultiDBClient
from redis.multidb.config import (
    DatabaseConfig,
    MultiDbConfig,
    DEFAULT_HEALTH_CHECK_INTERVAL,
)
from redis.multidb.event import ActiveDatabaseChanged
from redis.multidb.failure_detector import DEFAULT_MIN_NUM_FAILURES
from redis.asyncio.multidb.healthcheck import (
    DEFAULT_HEALTH_CHECK_DELAY,
    DEFAULT_HEALTH_CHECK_TIMEOUT,
)
from redis.backoff import ExponentialWithJitterBackoff
from redis.client import Redis
from redis.exceptions import ResponseError
from redis.maint_notifications import EndpointType, MaintNotificationsConfig
from redis.retry import Retry
from tests.test_scenario.fault_injector_client import (
    ProxyServerFaultInjector,
    REFaultInjector,
)

RELAXED_TIMEOUT = 30
CLIENT_TIMEOUT = 5

DEFAULT_ENDPOINT_NAME = "m-standard"
DEFAULT_OSS_API_ENDPOINT_NAME = "maint-notifications-oss-api"

# Bounded budget for the Active-Active readiness wait below. Long enough to outlast the
# recovery of a network failure a preceding test injected, and the propagation of the
# endpoint's new DNS record after an endpoint rebind a preceding planned maintenance
# test triggered (observed to take up to about 90 seconds), short enough that a
# genuinely dead endpoint fails the run instead of hanging it.
MULTI_DB_READY_TIMEOUT = 180
MULTI_DB_READY_INTERVAL = 1


def wait_for_databases_reachable(client_class, urls, client_kwargs):
    """Block until every Active-Active database answers PING.

    The Active-Active tests share one environment and each of them injects a network
    failure into it, so a test can start while the failure the previous one injected is
    still healing. ``MultiDBClient`` evaluates its initial health check once, and under
    the default ``ALL_AVAILABLE`` policy raises ``InitialHealthCheckFailedError`` if any
    database is unreachable at that moment - so without this wait a test fails on the
    previous test's fault instead of on anything it exercises itself.

    A plain ``PING`` is the gate rather than a faithful replay of ``PingHealthCheck``:
    the failure these tests inject takes out a whole cluster, so gross reachability is
    the signal, and the client still runs its own health check afterwards.
    """
    deadline = monotonic() + MULTI_DB_READY_TIMEOUT

    while True:
        unreachable = {}

        for url in urls:
            error = _ping_error(client_class, url, client_kwargs)

            if error is not None:
                unreachable[url] = error

        if not unreachable:
            return

        if monotonic() >= deadline:
            pytest.fail(
                f"Active-Active databases still unreachable after "
                f"{MULTI_DB_READY_TIMEOUT}s: {unreachable}"
            )

        logging.info("Waiting for Active-Active databases: %s", unreachable)
        sleep(MULTI_DB_READY_INTERVAL)


def _ping_error(client_class, url, client_kwargs):
    """Return None if the database answers PING, else a description of the failure."""
    client = None

    try:
        client = client_class.from_url(url, **client_kwargs)
        return None if client.ping() else "PING returned a falsy response"
    except Exception as error:
        return repr(error)
    finally:
        if client is not None:
            # A throwaway probe client - its pool must not outlive the check.
            try:
                client.close()
            except Exception:
                pass


def multi_db_client_kwargs(
    username: Optional[str],
    password: Optional[str],
    maint_notifications_config: Optional[MaintNotificationsConfig] = None,
) -> dict:
    """Build the client_kwargs of an Active-Active DatabaseConfig.

    Without a maintenance notifications config this is the plain credentials dict
    the Active-Active tests always used, and ``MultiDbConfig`` disables the
    notifications in the underlying clients. With one, the config is passed through
    (``MultiDbConfig`` leaves it alone when the key is present), together with what it
    needs to be exercised: RESP3, which the client requires for push notifications,
    and a finite socket timeout - the relaxed timeout a MIGRATING notification applies
    only means something when there is a stricter one to relax.
    """
    client_kwargs = {
        "username": username,
        "password": password,
        "decode_responses": True,
    }

    if maint_notifications_config is not None:
        client_kwargs["maint_notifications_config"] = maint_notifications_config
        client_kwargs["protocol"] = 3
        client_kwargs["socket_timeout"] = CLIENT_TIMEOUT

    return client_kwargs


def require_maint_notifications_supported(client_class, urls, client_kwargs):
    """Fail up front when a database does not accept ``CLIENT MAINT_NOTIFICATIONS ON``.

    An Active-Active client handshakes with both clusters, and the setting behind
    the command is cluster-level (``client_maint_notifications``), so both have to
    have it. Without this check a cluster missing it is reported by the client as
    ``InitialHealthCheckFailedError`` - which reads like a client bug - instead of
    as the infrastructure gap it is.

    Deliberately a failure and not a skip, for the same reason as
    ``require_lag_aware_credentials``: a skipped test is invisible in a scenario run.
    """
    unsupported = {}

    for url in urls:
        error = _maint_notifications_error(client_class, url, client_kwargs)

        if error is not None:
            unsupported[url] = error

    if unsupported:
        pytest.fail(
            "Maintenance notifications are rejected by the Active-Active databases "
            f"{unsupported}. Enable client_maint_notifications on every cluster of the "
            "environment (ENABLE_MAINTENANCE_NOTIFICATIONS=true in the fault injector "
            "configuration applies it to all of them)."
        )


def _maint_notifications_error(client_class, url, client_kwargs):
    """Return None if the handshake with notifications enabled succeeds, else why not.

    ``enabled=True`` makes the handshake raise ``ResponseError`` when the server
    rejects the command, unlike the ``"auto"`` default, which only logs it.
    """
    client = None
    probe_kwargs = {
        **client_kwargs,
        "protocol": 3,
        "maint_notifications_config": MaintNotificationsConfig(enabled=True),
    }

    try:
        client = client_class.from_url(url, **probe_kwargs)
        client.ping()
        return None
    except ResponseError as error:
        return repr(error)
    finally:
        if client is not None:
            # A throwaway probe client - its pool must not outlive the check.
            try:
                client.close()
            except Exception:
                pass


class CheckActiveDatabaseChangedListener(EventListenerInterface):
    def __init__(self):
        self.is_changed_flag = False

    def listen(self, event: ActiveDatabaseChanged):
        self.is_changed_flag = True


def use_mock_proxy():
    return os.getenv("REDIS_ENTERPRISE_TESTS", "true").lower() == "false"


# Module-level singleton for fault injector client used in parametrize
# This ensures we create only ONE instance that's shared between parametrize and fixture
_FAULT_INJECTOR_CLIENT_OSS_API = (
    ProxyServerFaultInjector(oss_cluster=True)
    if use_mock_proxy()
    else REFaultInjector(os.getenv("FAULT_INJECTION_API_URL", "http://127.0.0.1:20324"))
)

# Module-level singleton for fault injector client used in parametrize
# This ensures we create only ONE instance that's shared between parametrize and fixture
_FAULT_INJECTOR_STANDALONE_CLIENT = (
    ProxyServerFaultInjector(oss_cluster=False)
    if use_mock_proxy()
    else REFaultInjector(os.getenv("FAULT_INJECTION_API_URL", "http://127.0.0.1:20324"))
)


@pytest.fixture()
def endpoint_name(request):
    return request.config.getoption("--endpoint-name") or os.getenv(
        "REDIS_ENDPOINT_NAME", DEFAULT_ENDPOINT_NAME
    )


@pytest.fixture()
def cluster_endpoint_name(request):
    return request.config.getoption("--cluster-endpoint-name") or os.getenv(
        "REDIS_CLUSTER_ENDPOINT_NAME", DEFAULT_OSS_API_ENDPOINT_NAME
    )


def get_endpoints_config(endpoint_name: str):
    endpoints_config = os.getenv("REDIS_ENDPOINTS_CONFIG_PATH", None)

    if not (endpoints_config and os.path.exists(endpoints_config)):
        raise FileNotFoundError(f"Endpoints config file not found: {endpoints_config}")

    try:
        with open(endpoints_config, "r") as f:
            data = json.load(f)
            db = data[endpoint_name]
            return db
    except Exception as e:
        raise ValueError(
            f"Failed to load endpoints config file: {endpoints_config}"
        ) from e


def get_bdbs_config(endpoint_name: str):
    bdbs_config = os.getenv("REDIS_BDBS_CONFIG_PATH", None)

    if not (bdbs_config and os.path.exists(bdbs_config)):
        raise FileNotFoundError(f"BDBs config file not found: {bdbs_config}")

    try:
        with open(bdbs_config, "r") as f:
            data = json.load(f)
            dbs = data["databases"]
            for db in dbs:
                if db["name"] == endpoint_name:
                    return db
            pytest.fail(f"Failed to find bdb config for {endpoint_name}")
    except Exception as e:
        raise ValueError(f"Failed to load bdbs config file: {bdbs_config}") from e


@pytest.fixture()
def endpoints_config(endpoint_name: str):
    return get_endpoints_config(endpoint_name)


@pytest.fixture()
def maint_notifications_cluster_bdb_config(cluster_endpoint_name: str):
    """
    Get the bdb config for the cluster used in the maint notifications tests.
    This will be used to create the test database for each test.
    The bdb config is the same for all tests, but the database is created with a random name.
    """
    return get_bdbs_config(cluster_endpoint_name)


@pytest.fixture()
def fault_injector_client():
    if use_mock_proxy():
        return ProxyServerFaultInjector(oss_cluster=False)
    else:
        url = os.getenv("FAULT_INJECTION_API_URL", "http://127.0.0.1:20324")
        return REFaultInjector(url)


@pytest.fixture()
def fault_injector_client_oss_api():
    """Return the singleton instance to ensure parametrize and tests use the same client."""
    return _FAULT_INJECTOR_CLIENT_OSS_API


@pytest.fixture()
def r_multi_db(
    request,
) -> Generator[
    tuple[MultiDBClient, CheckActiveDatabaseChangedListener, dict], Any, Any
]:
    client_class = request.param.get("client_class", Redis)

    if client_class == Redis:
        endpoint_config = get_endpoints_config("re-active-active")
    else:
        endpoint_config = get_endpoints_config("re-active-active-oss-cluster")

    username = endpoint_config.get("username", None)
    password = endpoint_config.get("password", None)
    min_num_failures = request.param.get("min_num_failures", DEFAULT_MIN_NUM_FAILURES)
    command_retry = request.param.get(
        "command_retry", Retry(ExponentialBackoff(cap=0.1, base=0.01), retries=10)
    )

    # Retry configuration different for health checks as initial health check require more time in case
    # if infrastructure wasn't restored from the previous test.
    health_check_interval = request.param.get(
        "health_check_interval", DEFAULT_HEALTH_CHECK_INTERVAL
    )
    health_check_delay = request.param.get(
        "health_check_delay", DEFAULT_HEALTH_CHECK_DELAY
    )
    health_check_timeout = request.param.get(
        "health_check_timeout", DEFAULT_HEALTH_CHECK_TIMEOUT
    )
    maint_notifications_config = request.param.get("maint_notifications_config", None)
    event_dispatcher = EventDispatcher()
    listener = CheckActiveDatabaseChangedListener()
    event_dispatcher.register_listeners(
        {
            ActiveDatabaseChanged: [listener],
        }
    )
    db_configs = []

    db_config = DatabaseConfig(
        weight=1.0,
        from_url=endpoint_config["endpoints"][0],
        client_kwargs=multi_db_client_kwargs(
            username, password, maint_notifications_config
        ),
        health_check_url=extract_cluster_fqdn(endpoint_config["endpoints"][0]),
    )
    db_configs.append(db_config)

    db_config1 = DatabaseConfig(
        weight=0.9,
        from_url=endpoint_config["endpoints"][1],
        client_kwargs=multi_db_client_kwargs(
            username, password, maint_notifications_config
        ),
        health_check_url=extract_cluster_fqdn(endpoint_config["endpoints"][1]),
    )
    db_configs.append(db_config1)

    config = MultiDbConfig(
        client_class=client_class,
        databases_config=db_configs,
        command_retry=command_retry,
        min_num_failures=min_num_failures,
        health_check_probes=3,
        health_check_interval=health_check_interval,
        event_dispatcher=event_dispatcher,
        health_check_delay=health_check_delay,
        health_check_timeout=health_check_timeout,
    )

    # Before the client exists, so the initial health check it runs on first use is not
    # evaluated against a database still recovering from the previous test's fault.
    wait_for_databases_reachable(
        client_class,
        endpoint_config["endpoints"][:2],
        {"username": username, "password": password},
    )

    if maint_notifications_config is not None and (
        maint_notifications_config.enabled is True
    ):
        require_maint_notifications_supported(
            client_class,
            endpoint_config["endpoints"][:2],
            {"username": username, "password": password},
        )

    client = MultiDBClient(config)

    yield client, listener, endpoint_config

    # Without this the client keeps a health check loop thread and its event loop
    # thread running for the rest of the session, polling the databases of a test
    # that already finished.
    client.close()


def extract_cluster_fqdn(url):
    """
    Extract Cluster FQDN from Redis URL
    """
    # Parse the URL
    parsed = urlparse(url)

    # Extract hostname and port
    hostname = parsed.hostname

    # Remove the 'redis-XXXX.' prefix using regex
    # This pattern matches 'redis-' followed by digits and a dot
    cleaned_hostname = re.sub(r"^redis-\d+\.", "", hostname)

    # Reconstruct the URL
    return f"https://{cleaned_hostname}"


def _prepare_ssl_certificates(cert_chain: bool) -> dict:
    """
    Prepare SSL certificates for Redis cluster connection.

    Args:
        cert_chain: PEM-encoded certificate chain containing client cert + intermediate + CA cert.
                   This is the full certificate chain that will be used to validate the server.

    Returns:
        dict: SSL configuration kwargs for RedisCluster
    """
    certs_config_path = os.environ.get("MTLS_CONFIG_PATH", None)

    if not cert_chain:
        return {
            "ssl_cert_reqs": "none",
            "ssl_check_hostname": False,
        }

    if not certs_config_path:
        raise ValueError(
            "MTLS enabled test is triggered but MTLS_CONFIG_PATH environment variable not set"
        )

    # The cert_chain contains the full chain (client cert + intermediate + root CA)
    # Use it as CA data for validating the server's certificate
    return {
        "ssl_cert_reqs": "none",
        "ssl_keyfile": os.path.join(certs_config_path, "client.key"),
        "ssl_certfile": os.path.join(certs_config_path, "client.crt"),
    }


def _get_client_maint_notifications(
    endpoints_config,
    protocol: int = 3,
    enable_maintenance_notifications: bool = True,
    endpoint_type: Optional[EndpointType] = None,
    enable_relaxed_timeout: bool = True,
    enable_proactive_reconnect: bool = True,
    disable_retries: bool = False,
    auth_ssl_client_certs: bool = False,
    socket_timeout: Optional[float] = None,
    host_config: Optional[str] = None,
):
    """Create Redis client with maintenance notifications enabled."""

    # Get credentials from the configuration
    username = endpoints_config.get("username")
    password = endpoints_config.get("password")

    # Parse host and port from endpoints URL
    endpoints = endpoints_config.get("endpoints", [])
    if not endpoints:
        raise ValueError("No endpoints found in configuration")

    parsed = urlparse(endpoints[0])
    host = parsed.hostname if host_config is None else host_config
    port = parsed.port

    if not host:
        raise ValueError(f"Could not parse host from endpoint URL: {endpoints[0]}")

    logging.info(f"Connecting to Redis Enterprise: {host}:{port} with user: {username}")

    # Configure maintenance notifications
    maintenance_config = MaintNotificationsConfig(
        enabled=enable_maintenance_notifications,
        proactive_reconnect=enable_proactive_reconnect,
        relaxed_timeout=RELAXED_TIMEOUT if enable_relaxed_timeout else -1,
        endpoint_type=endpoint_type,
    )

    if disable_retries:
        retry = Retry(NoBackoff(), 0)
    else:
        retry = Retry(
            backoff=ExponentialWithJitterBackoff(base=0.01, cap=1), retries=10
        )

    tls_enabled = True if parsed.scheme == "rediss" else False
    logging.info(f"TLS enabled: {tls_enabled}")

    tls_kwargs = {"ssl": tls_enabled}
    if tls_enabled:
        ssl_config = _prepare_ssl_certificates(auth_ssl_client_certs)
        tls_kwargs.update(ssl_config)

    # Create Redis client with maintenance notifications config
    # This will automatically create the MaintNotificationsPoolHandler
    client = Redis(
        host=host,
        port=port,
        socket_timeout=CLIENT_TIMEOUT if socket_timeout is None else socket_timeout,
        username=username,
        password=password,
        protocol=protocol,  # RESP3 required for push notifications
        maint_notifications_config=maintenance_config,
        retry=retry,
        **tls_kwargs,
    )
    logging.info("Redis client created with maintenance notifications enabled")
    logging.info(f"Client uses Protocol: {client.connection_pool.get_protocol()}")

    return client


def get_standalone_client_maint_notifications(
    endpoints_config,
    protocol: int = 3,
    enable_maintenance_notifications: bool = True,
    endpoint_type: Optional[EndpointType] = None,
    enable_relaxed_timeout: bool = True,
    enable_proactive_reconnect: bool = True,
    disable_retries: bool = False,
    auth_ssl_client_certs: bool = False,
    socket_timeout: Optional[float] = None,
):
    """Create Redis stanalone client with maintenance notifications enabled."""
    # Get credentials from the configuration
    username = endpoints_config.get("username")
    password = endpoints_config.get("password")

    # Parse host and port from endpoints URL
    endpoints = endpoints_config.get("endpoints", [])
    if not endpoints:
        raise ValueError("No endpoints found in configuration")

    parsed = urlparse(endpoints[0])
    host = parsed.hostname
    port = parsed.port

    if not host:
        raise ValueError(f"Could not parse host from endpoint URL: {endpoints[0]}")

    logging.info(f"Connecting to Redis Enterprise: {host}:{port} with user: {username}")

    if disable_retries:
        retry = Retry(NoBackoff(), 0)
    else:
        retry = Retry(
            backoff=ExponentialWithJitterBackoff(base=0.01, cap=1), retries=10
        )

    tls_enabled = True if parsed.scheme == "rediss" else False
    logging.info(f"TLS enabled: {tls_enabled}")

    tls_kwargs = {"ssl": tls_enabled}

    if tls_enabled:
        # Prepare SSL certificate configuration
        ssl_config = _prepare_ssl_certificates(auth_ssl_client_certs)
        tls_kwargs.update(ssl_config)

    # Configure maintenance notifications
    maintenance_config = MaintNotificationsConfig(
        enabled=enable_maintenance_notifications,
        proactive_reconnect=enable_proactive_reconnect,
        relaxed_timeout=RELAXED_TIMEOUT if enable_relaxed_timeout else -1,
        endpoint_type=endpoint_type,
    )

    # Create Redis cluster client with maintenance notifications config
    client = Redis(
        host=host,
        port=port,
        socket_timeout=CLIENT_TIMEOUT if socket_timeout is None else socket_timeout,
        username=username,
        password=password,
        protocol=protocol,  # RESP3 required for push notifications
        maint_notifications_config=maintenance_config,
        retry=retry,
        **tls_kwargs,
    )
    logging.info(
        "Redis standalone client created with maintenance notifications enabled"
    )

    return client


def get_cluster_client_maint_notifications(
    endpoints_config,
    protocol: int = 3,
    enable_maintenance_notifications: bool = True,
    endpoint_type: Optional[EndpointType] = None,
    enable_relaxed_timeout: bool = True,
    enable_proactive_reconnect: bool = True,
    disable_retries: bool = False,
    auth_ssl_client_certs: bool = False,
    socket_timeout: Optional[float] = None,
    socket_connect_timeout: Optional[float] = None,
):
    """Create Redis cluster client with maintenance notifications enabled."""
    # Get credentials from the configuration
    username = endpoints_config.get("username")
    password = endpoints_config.get("password")

    # Parse host and port from endpoints URL
    endpoints = endpoints_config.get("endpoints", [])
    if not endpoints:
        raise ValueError("No endpoints found in configuration")

    parsed = urlparse(endpoints[0])
    host = parsed.hostname
    port = parsed.port

    if not host:
        raise ValueError(f"Could not parse host from endpoint URL: {endpoints[0]}")

    logging.info(f"Connecting to Redis Enterprise: {host}:{port} with user: {username}")

    if disable_retries:
        retry = Retry(NoBackoff(), 0)
    else:
        retry = Retry(
            backoff=ExponentialWithJitterBackoff(base=0.01, cap=1), retries=10
        )

    tls_enabled = True if parsed.scheme == "rediss" else False
    logging.info(f"TLS enabled: {tls_enabled}")

    tls_kwargs = {"ssl": tls_enabled}

    if tls_enabled:
        # Prepare SSL certificate configuration
        ssl_config = _prepare_ssl_certificates(auth_ssl_client_certs)
        tls_kwargs.update(ssl_config)

    # Configure maintenance notifications
    maintenance_config = MaintNotificationsConfig(
        enabled=enable_maintenance_notifications,
        proactive_reconnect=enable_proactive_reconnect,
        relaxed_timeout=RELAXED_TIMEOUT if enable_relaxed_timeout else -1,
        endpoint_type=endpoint_type,
    )

    # Create Redis cluster client with maintenance notifications config
    client = RedisCluster(
        host=host,
        port=port,
        socket_timeout=CLIENT_TIMEOUT if socket_timeout is None else socket_timeout,
        socket_connect_timeout=CLIENT_TIMEOUT
        if socket_connect_timeout is None
        else socket_connect_timeout,
        username=username,
        password=password,
        protocol=protocol,  # RESP3 required for push notifications
        maint_notifications_config=maintenance_config,
        retry=retry,
        **tls_kwargs,
    )
    logging.info("Redis cluster client created with maintenance notifications enabled")
    logging.info(
        f"Cluster working with the following nodes: {[(node.name, node.server_type) for node in client.get_nodes()]}"
    )

    return client
