import binascii
from contextlib import contextmanager
import datetime
import gc
import platform
import select
import socket
import socketserver
import ssl
import threading
from typing import List
import warnings
import weakref
from queue import LifoQueue, Queue
from time import monotonic, sleep
from unittest.mock import DEFAULT, MagicMock, Mock, call, patch

import pytest
import redis
from redis import Redis
from redis._parsers import CommandsParser
from redis.backoff import (
    AbstractBackoff,
    ExponentialBackoff,
    ExponentialWithJitterBackoff,
    NoBackoff,
)
from redis.cluster import (
    _REPLICAS_ONLY_STRATEGIES,
    _RESPONSE_POLICY_CALLBACKS,
    PRIMARY,
    PIPELINE_BLOCKED_COMMANDS,
    REDIS_CLUSTER_HASH_SLOTS,
    REPLICA,
    ClusterNode,
    ClusterPipeline,
    ExecutionStrategy,
    LoadBalancingStrategy,
    NodesManager,
    RedisCluster,
    _resolve_localhost,
    get_node_name,
    parse_cluster_shards,
    parse_cluster_shards_unified,
    parse_cluster_shards_with_str_keys,
)
from redis.cache import CacheConfig
from redis.commands.cluster import ClusterMultiKeyCommands
from redis.commands.core import HotkeysMetricsTypes
from redis.commands.metadata import (
    CommandMetadata,
    DynamicMetadataResolver,
    RequestPolicy,
    ResponsePolicy,
    StaticMetadataResolver,
)
from redis.commands.policies import DynamicPolicyResolver, StaticPolicyResolver
from redis.connection import (
    BlockingConnectionPool,
    Connection,
    ConnectionPool,
    SSLConnection,
)
from redis.crc import key_slot
from redis.credentials import StreamingCredentialProvider
from redis.event import (
    AfterSlotsCacheRefreshEvent,
    EventDispatcher,
    EventListenerInterface,
)
from redis.exceptions import (
    AskError,
    AuthenticationError,
    ClusterDownError,
    ConnectionError,
    CrossSlotTransactionError,
    DataError,
    ExecAbortError,
    MaxClientsReachedError,
    MaxConnectionsError,
    MovedError,
    NoPermissionError,
    RedisClusterException,
    RedisClusterUnreachableError,
    RedisError,
    ResponseError,
    SlotNotCoveredError,
    TimeoutError,
    TryAgainError,
)
from redis.himport import HIMPORT_SET
from redis.observability import recorder
from redis.observability.config import OTelConfig, MetricGroup
from redis.observability.metrics import RedisMetricsCollector
from redis.retry import Retry
from redis.utils import str_if_bytes
from tests.test_command_metadata import (
    CACHEABLE_KEYED,
    slot_routed_args,
    slot_routed_static_commands,
)
from tests.test_pubsub import wait_for_message

from .conftest import (
    _get_client,
    assert_resp_response,
    expects_resp2_shape,
    expects_unified_shape,
    patch_monotonic,
    skip_if_redis_enterprise,
    skip_if_server_version_lt,
    skip_unless_arch_bits,
    wait_for_command,
)

default_host = "127.0.0.1"
default_port = 7000
default_cluster_slots = [
    [0, 8191, ["127.0.0.1", 7000, "node_0"], ["127.0.0.1", 7003, "node_3"]],
    [8192, 16383, ["127.0.0.1", 7001, "node_1"], ["127.0.0.1", 7002, "node_2"]],
]


class ProxyRequestHandler(socketserver.BaseRequestHandler):
    def recv(self, sock):
        """A recv with a timeout"""
        r = select.select([sock], [], [], 0.01)
        if not r[0]:
            return None
        return sock.recv(1000)

    def handle(self):
        self.server.proxy.n_connections += 1
        conn = socket.create_connection(self.server.proxy.redis_addr)
        stop = False

        def from_server():
            # read from server and pass to client
            while not stop:
                data = self.recv(conn)
                if data is None:
                    continue
                if not data:
                    self.request.shutdown(socket.SHUT_WR)
                    return
                self.request.sendall(data)

        thread = threading.Thread(target=from_server)
        thread.start()
        try:
            while True:
                # read from client and send to server
                data = self.request.recv(1000)
                if not data:
                    return
                conn.sendall(data)
        finally:
            conn.shutdown(socket.SHUT_WR)
            stop = True  # for safety
            thread.join()
            conn.close()


class NodeProxy:
    """A class to proxy a node connection to a different port"""

    def __init__(self, addr, redis_addr):
        self.addr = addr
        self.redis_addr = redis_addr
        self.server = socketserver.ThreadingTCPServer(
            self.addr, ProxyRequestHandler, bind_and_activate=False
        )
        self.server.proxy = self
        self.server.allow_reuse_address = True
        self.server.server_bind()
        self.server.server_activate()
        self.thread = None
        self.n_connections = 0

    def start(self):
        # test that we can connect to redis
        s = socket.create_connection(self.redis_addr, timeout=2)
        s.close()
        # Start a thread with the server -- that thread will then start one
        # more thread for each request
        self.thread = threading.Thread(target=self.server.serve_forever)
        # Exit the server thread when the main thread terminates
        self.thread.daemon = True
        self.thread.start()

    def close(self):
        self.server.shutdown()


@pytest.fixture()
def slowlog(request, r):
    """
    Set the slowlog threshold to 0, and the
    max length to 128. This will force every
    command into the slowlog and allow us
    to test it
    """
    # Save old values
    current_config = r.config_get(target_nodes=r.get_primaries()[0])
    old_slower_than_value = current_config["slowlog-log-slower-than"]
    old_max_legnth_value = current_config["slowlog-max-len"]

    # Function to restore the old values
    def cleanup():
        r.config_set("slowlog-log-slower-than", old_slower_than_value)
        r.config_set("slowlog-max-len", old_max_legnth_value)

    request.addfinalizer(cleanup)

    # Set the new values
    r.config_set("slowlog-log-slower-than", 0)
    r.config_set("slowlog-max-len", 128)


def get_mocked_redis_client(
    func=None, cluster_slots_raise_error=False, *args, **kwargs
):
    """
    Return a stable RedisCluster object that have deterministic
    nodes and slots setup to remove the problem of different IP addresses
    on different installations and machines.
    """
    cluster_slots = kwargs.pop("cluster_slots", default_cluster_slots)
    coverage_res = kwargs.pop("coverage_result", "yes")
    cluster_enabled = kwargs.pop("cluster_enabled", True)
    with patch.object(Redis, "execute_command") as execute_command_mock:

        def execute_command(*_args, **_kwargs):
            if _args[0] == "CLUSTER SLOTS":
                if cluster_slots_raise_error:
                    raise ResponseError()
                else:
                    mock_cluster_slots = cluster_slots
                    return mock_cluster_slots
            elif _args[0] == "COMMAND":
                return {"get": [], "set": []}
            elif _args[0] == "INFO":
                return {"cluster_enabled": cluster_enabled}
            elif len(_args) > 1 and _args[1] == "cluster-require-full-coverage":
                return {"cluster-require-full-coverage": coverage_res}
            elif func is not None:
                return func(*args, **kwargs)
            else:
                return execute_command_mock(*_args, **_kwargs)

        execute_command_mock.side_effect = execute_command

        with patch.object(
            CommandsParser, "initialize", autospec=True
        ) as cmd_parser_initialize:

            def cmd_init_mock(self, r):
                self.commands = {
                    "get": {
                        "name": "get",
                        "arity": 2,
                        "flags": ["readonly", "fast"],
                        "first_key_pos": 1,
                        "last_key_pos": 1,
                        "step_count": 1,
                    },
                    "cluster delslots": {
                        "name": "cluster delslots",
                        "flags": ["readonly", "fast"],
                        "first_key_pos": 0,
                        "last_key_pos": 0,
                        "step_count": 0,
                    },
                    "cluster delslotsrange": {
                        "name": "cluster delslotsrange",
                        "flags": ["readonly", "fast"],
                        "first_key_pos": 0,
                        "last_key_pos": 0,
                        "step_count": 0,
                    },
                    "cluster addslots": {
                        "name": "cluster delslotsrange",
                        "flags": ["readonly", "fast"],
                        "first_key_pos": 0,
                        "last_key_pos": 0,
                        "step_count": 0,
                    },
                }

            cmd_parser_initialize.side_effect = cmd_init_mock

            # Create a subclass of RedisCluster that overrides __del__
            class MockedRedisCluster(RedisCluster):
                def __del__(self):
                    # Override to prevent connection cleanup attempts
                    pass

                @property
                def connection_pool(self):
                    # Required abstract property implementation
                    return self.nodes_manager.get_default_node().redis_connection.connection_pool

            return MockedRedisCluster(*args, **kwargs)


def mock_node_resp(node, response):
    connection = Mock()
    connection.read_response.return_value = response
    node.redis_connection.connection = connection
    return node


def mock_node_resp_func(node, func):
    connection = Mock()
    connection.read_response.side_effect = func
    node.redis_connection.connection = connection
    return node


def mock_all_nodes_resp(rc, response):
    for node in rc.get_nodes():
        mock_node_resp(node, response)
    return rc


def find_node_ip_based_on_port(cluster_client, port):
    for node in cluster_client.get_nodes():
        if node.port == port:
            return node.host


def moved_redirection_helper(request, failover=False, circular_moved=False):
    """
    Test that the client correctly handles MOVED responses in the following scenarios:
    1.	Slot migration to a different shard (failover=False, circular_moved=False) —
        a standard slot move between shards.
    2.	Failover event (failover=True, circular_moved=False) —
        the redirect target is a replica that has just been promoted to primary.
    3.	Circular MOVED (failover=False, circular_moved=True) —
        the redirect points to a node already known to be the primary of its shard.

    At first call it should return a MOVED ResponseError that will point
    the client to the next server it should talk to.

    Verify that:
    1. it tries to talk to the redirected node
    2. it updates the slot's primary to the redirected node, if required

    For a failover, also verify:
    3. the redirected node's server type updated to 'primary'
    4. the server type of the previous slot owner updated to 'replica'
    """
    rc = _get_client(RedisCluster, request, flushdb=False)
    slot = 12182
    redirect_node = None
    # Get the current primary that holds this slot
    prev_primary = rc.nodes_manager.get_node_from_slot(slot)
    if failover:
        if len(rc.nodes_manager.slots_cache[slot]) < 2:
            warnings.warn("Skipping this test since it requires to have a replica")
            return
        redirect_node = rc.nodes_manager.slots_cache[slot][1]
    elif circular_moved:
        redirect_node = prev_primary
    else:
        # Use one of the other primaries to be the redirected node
        redirect_node = rc.get_primaries()[0]
    r_host = redirect_node.host
    r_port = redirect_node.port
    with patch.object(Redis, "parse_response") as parse_response:

        def moved_redirect_effect(connection, *args, **options):
            def ok_response(connection, *args, **options):
                assert connection.host == r_host
                assert connection.port == r_port

                return "MOCK_OK"

            parse_response.side_effect = ok_response
            raise MovedError(f"{slot} {r_host}:{r_port}")

        parse_response.side_effect = moved_redirect_effect
        assert rc.execute_command("SET", "foo", "bar") == "MOCK_OK"
        slot_primary = rc.nodes_manager.slots_cache[slot][0]
        assert slot_primary == redirect_node
        if failover:
            assert rc.get_node(host=r_host, port=r_port).server_type == PRIMARY
            assert prev_primary.server_type == REPLICA
        elif circular_moved:
            fetched_node = rc.get_node(host=r_host, port=r_port)
            assert fetched_node == prev_primary
            assert fetched_node.server_type == PRIMARY


@pytest.mark.onlycluster
class TestRedisClusterObj:
    """
    Tests for the RedisCluster class
    """

    def test_from_url(self, r):
        redis_url = f"redis://{default_host}:{default_port}/0"
        with patch.object(RedisCluster, "from_url") as from_url:

            def from_url_mocked(_url, **_kwargs):
                return get_mocked_redis_client(url=_url, **_kwargs)

            from_url.side_effect = from_url_mocked
            cluster = RedisCluster.from_url(redis_url)
        assert cluster.get_node(host=default_host, port=default_port) is not None

    def test_execute_command_errors(self, r):
        """
        Test that if no key is provided then exception should be raised.
        """
        with pytest.raises(RedisClusterException) as ex:
            r.execute_command("GET")
        assert str(ex.value).startswith(
            "No way to dispatch this command to Redis Cluster. Missing key."
        )

    def test_execute_command_node_flag_primaries(self, r):
        """
        Test command execution with nodes flag PRIMARIES
        """
        primaries = r.get_primaries()
        replicas = r.get_replicas()
        mock_all_nodes_resp(r, "PONG")
        assert r.ping(target_nodes=RedisCluster.PRIMARIES) is True
        for primary in primaries:
            conn = primary.redis_connection.connection
            assert conn.read_response.called is True
        for replica in replicas:
            conn = replica.redis_connection.connection
            assert conn.read_response.called is not True

    def test_execute_command_node_flag_replicas(self, r):
        """
        Test command execution with nodes flag REPLICAS
        """
        replicas = r.get_replicas()
        if not replicas:
            r = get_mocked_redis_client(default_host, default_port)
        primaries = r.get_primaries()
        mock_all_nodes_resp(r, "PONG")
        assert r.ping(target_nodes=RedisCluster.REPLICAS) is True
        for replica in replicas:
            conn = replica.redis_connection.connection
            assert conn.read_response.called is True
        for primary in primaries:
            conn = primary.redis_connection.connection
            assert conn.read_response.called is not True

    def test_execute_command_node_flag_all_nodes(self, r):
        """
        Test command execution with nodes flag ALL_NODES
        """
        mock_all_nodes_resp(r, "PONG")
        assert r.ping(target_nodes=RedisCluster.ALL_NODES) is True
        for node in r.get_nodes():
            conn = node.redis_connection.connection
            assert conn.read_response.called is True

    def test_execute_command_node_flag_random(self, r):
        """
        Test command execution with nodes flag RANDOM
        """
        mock_all_nodes_resp(r, "PONG")
        assert r.ping(target_nodes=RedisCluster.RANDOM) is True
        called_count = 0
        for node in r.get_nodes():
            conn = node.redis_connection.connection
            if conn.read_response.called is True:
                called_count += 1
        assert called_count == 1

    def test_execute_command_default_node(self, r):
        """
        Test command execution without node flag is being executed on the
        default node
        """
        def_node = r.get_default_node()
        mock_node_resp(def_node, "PONG")
        assert r.ping() is True
        conn = def_node.redis_connection.connection
        assert conn.read_response.called

    def test_ask_redirection(self, r):
        """
        Test that the server handles ASK response.

        At first call it should return a ASK ResponseError that will point
        the client to the next server it should talk to.

        Important thing to verify is that it tries to talk to the second node.
        """
        redirect_node = r.get_nodes()[0]
        with patch.object(Redis, "parse_response") as parse_response:

            def ask_redirect_effect(connection, *args, **options):
                def ok_response(connection, *args, **options):
                    assert connection.host == redirect_node.host
                    assert connection.port == redirect_node.port

                    return "MOCK_OK"

                parse_response.side_effect = ok_response
                raise AskError(f"12182 {redirect_node.host}:{redirect_node.port}")

            parse_response.side_effect = ask_redirect_effect

            assert r.execute_command("SET", "foo", "bar") == "MOCK_OK"

    def test_himport_set_moved_after_ask_uses_moved_target(self, r):
        """An HIMPORT SET that is ASK-redirected and then hits a MOVED
        mid-exchange must retry against the MOVED target, not the stale ASK
        target.

        HIMPORT SET folds the ASK allowance into its own packed write, so the
        retry loop keeps ``asking`` set until ``_himport_execute_set`` returns.
        Regression guard: if that method raises a MOVED before returning,
        ``asking`` must still be cleared so the next iteration takes the
        moved-retry branch instead of re-sending to the old ASK node.
        """
        key = "himport:key"
        slot = r.keyslot(key)
        primary = r.nodes_manager.get_node_from_slot(slot)
        others = [n for n in r.get_primaries() if n.name != primary.name]
        if len(others) < 2:
            pytest.skip("requires at least 3 primaries")
        ask_node, moved_node = others[0], others[1]

        calls = []

        def himport_effect(redis_node, connection, *args, **kwargs):
            calls.append((connection.host, connection.port))
            if len(calls) == 1:
                raise AskError(f"{slot} {ask_node.host}:{ask_node.port}")
            if len(calls) == 2:
                # MOVED raised from within the HIMPORT SET exchange, before the
                # caller could clear ``asking``.
                raise MovedError(f"{slot} {moved_node.host}:{moved_node.port}")
            return "MOCK_OK"

        # ``determine_slot`` parses HIMPORT SET keys via COMMAND INFO, which the
        # test server may not know; pin it so the moved branch can recompute the
        # target without a live HIMPORT-capable server.
        with (
            patch.object(RedisCluster, "determine_slot", return_value=slot),
            patch.object(
                RedisCluster, "_himport_execute_set", side_effect=himport_effect
            ),
        ):
            assert (
                r._execute_command(primary, HIMPORT_SET, key, "shared", "alice")
                == "MOCK_OK"
            )

        assert calls[0] == (primary.host, primary.port)
        assert calls[1] == (ask_node.host, ask_node.port)
        # Third attempt must follow the MOVED redirect, not the stale ASK target.
        assert calls[2] == (moved_node.host, moved_node.port)

    @pytest.mark.usefixtures("no_topology_refresh_interval", "no_cluster_retry_delay")
    def test_handling_cluster_failover_to_a_replica(self, r):
        # Set the key we'll test for
        key = "key"
        r.set("key", "value")
        primary = r.get_node_from_key(key, replica=False)
        assert str_if_bytes(r.get("key")) == "value"
        # Get the current output of cluster slots
        cluster_slots = primary.redis_connection.execute_command("CLUSTER SLOTS")
        replica_host = ""
        replica_port = 0
        # Replace one of the replicas to be the new primary based on the
        # cluster slots output
        for slot_range in cluster_slots:
            primary_port = slot_range[2][1]
            if primary_port == primary.port:
                if len(slot_range) <= 3:
                    # cluster doesn't have a replica, return
                    return
                replica_host = str_if_bytes(slot_range[3][0])
                replica_port = slot_range[3][1]
                # replace replica and primary in the cluster slots output
                tmp_node = slot_range[2]
                slot_range[2] = slot_range[3]
                slot_range[3] = tmp_node
                break

        def raise_connection_error():
            raise ConnectionError("error")

        def mock_execute_command(*_args, **_kwargs):
            if _args[0] == "CLUSTER SLOTS":
                return cluster_slots
            else:
                raise Exception("Failed to mock cluster slots")

        # Mock connection error for the current primary
        mock_node_resp_func(primary, raise_connection_error)
        primary.redis_connection.set_retry(Retry(NoBackoff(), 1))

        # Mock the cluster slots response for all other nodes
        redis_mock_node = Mock()
        redis_mock_node.execute_command.side_effect = mock_execute_command
        # Mock response value for all other commands
        redis_mock_node.parse_response.return_value = "MOCK_OK"
        for node in r.get_nodes():
            if node.port != primary.port:
                node.redis_connection = redis_mock_node

        assert r.get(key) == "MOCK_OK"
        new_primary = r.get_node_from_key(key, replica=False)
        assert new_primary.host == replica_host
        assert new_primary.port == replica_port
        assert r.get_node(primary.host, primary.port).server_type == REPLICA

    def test_moved_redirection(self, request):
        """
        Test that the client handles MOVED response.
        """
        moved_redirection_helper(request, failover=False)

    def test_moved_redirection_after_failover(self, request):
        """
        Test that the client handles MOVED response after a failover.
        """
        moved_redirection_helper(request, failover=True)

    def test_moved_redirection_circular_moved(self, request):
        """
        Verify that the client does not update its slot map when receiving a circular MOVED response
        (i.e., a MOVED redirect pointing back to the same node), and retries again the same node.
        """
        moved_redirection_helper(request, failover=False, circular_moved=True)

    def test_refresh_using_specific_nodes(self, request):
        """
        Test making calls on specific nodes when the cluster has failed over to
        another node
        """
        node_7006 = ClusterNode(host=default_host, port=7006, server_type=PRIMARY)
        node_7007 = ClusterNode(host=default_host, port=7007, server_type=PRIMARY)
        with patch.object(Redis, "parse_response") as parse_response:
            with patch.object(NodesManager, "initialize", autospec=True) as initialize:
                with patch.multiple(
                    Connection, send_command=DEFAULT, connect=DEFAULT, can_read=DEFAULT
                ) as mocks:
                    # simulate 7006 as a failed node
                    def parse_response_mock(connection, command_name, **options):
                        if connection.port == 7006:
                            parse_response.failed_calls += 1
                            raise ClusterDownError(
                                "CLUSTERDOWN The cluster is "
                                "down. Use CLUSTER INFO for "
                                "more information"
                            )
                        elif connection.port == 7007:
                            parse_response.successful_calls += 1

                    def initialize_mock(self, *args, **kwargs):
                        # start with all slots mapped to 7006
                        self.nodes_cache = {node_7006.name: node_7006}
                        self.default_node = node_7006
                        self.slots_cache = {}

                        for i in range(0, 16383):
                            self.slots_cache[i] = [node_7006]

                        # After the first connection fails, a reinitialize
                        # should follow the cluster to 7007
                        def map_7007(self, *args, **kwargs):
                            self.nodes_cache = {node_7007.name: node_7007}
                            self.default_node = node_7007
                            self.slots_cache = {}

                            for i in range(0, 16383):
                                self.slots_cache[i] = [node_7007]

                        # Change initialize side effect for the second call
                        initialize.side_effect = map_7007

                    parse_response.side_effect = parse_response_mock
                    parse_response.successful_calls = 0
                    parse_response.failed_calls = 0
                    initialize.side_effect = initialize_mock
                    mocks["can_read"].return_value = False
                    mocks["send_command"].return_value = "MOCK_OK"
                    mocks["connect"].return_value = None
                    with patch.object(
                        CommandsParser, "initialize", autospec=True
                    ) as cmd_parser_initialize:

                        def cmd_init_mock(self, r):
                            self.commands = {
                                "get": {
                                    "name": "get",
                                    "arity": 2,
                                    "flags": ["readonly", "fast"],
                                    "first_key_pos": 1,
                                    "last_key_pos": 1,
                                    "step_count": 1,
                                }
                            }

                        cmd_parser_initialize.side_effect = cmd_init_mock

                        rc = _get_client(RedisCluster, request, flushdb=False)
                        assert len(rc.get_nodes()) == 1
                        assert rc.get_node(node_name=node_7006.name) is not None

                        rc.get("foo")

                        # Cluster should now point to 7007, and there should be
                        # one failed and one successful call
                        assert len(rc.get_nodes()) == 1
                        assert rc.get_node(node_name=node_7007.name) is not None
                        assert rc.get_node(node_name=node_7006.name) is None
                        assert parse_response.failed_calls == 1
                        assert parse_response.successful_calls == 1

    def test_keyslot(self, r):
        """
        Test that method will compute correct key in all supported cases
        """
        assert r.keyslot("foo") == 12182
        assert r.keyslot("{foo}bar") == 12182
        assert r.keyslot("{foo}") == 12182
        assert r.keyslot(1337) == 4314

        assert r.keyslot(125) == r.keyslot(b"125")
        assert r.keyslot(125) == r.keyslot("\x31\x32\x35")
        assert r.keyslot("大奖") == r.keyslot(b"\xe5\xa4\xa7\xe5\xa5\x96")
        assert r.keyslot("大奖") == r.keyslot(b"\xe5\xa4\xa7\xe5\xa5\x96")
        assert r.keyslot(1337.1234) == r.keyslot("1337.1234")
        assert r.keyslot(1337) == r.keyslot("1337")
        assert r.keyslot(b"abc") == r.keyslot("abc")

    def test_all_nodes(self, r):
        """
        Set a list of nodes and it should be possible to iterate over all
        """
        nodes = [node for node in r.nodes_manager.nodes_cache.values()]

        for i, node in enumerate(r.get_nodes()):
            assert node in nodes

    def test_all_nodes_masters(self, r):
        """
        Set a list of nodes with random primaries/replicas config and it shold
        be possible to iterate over all of them.
        """
        nodes = [
            node
            for node in r.nodes_manager.nodes_cache.values()
            if node.server_type == PRIMARY
        ]

        for node in r.get_primaries():
            assert node in nodes

    def test_user_on_connect_function(self, request):
        """
        Test support in passing on_connect function by the user
        """

        def on_connect(connection):
            assert connection is not None

        mock = Mock(side_effect=on_connect)

        _get_client(RedisCluster, request, redis_connect_func=mock)
        assert mock.called is True

    def test_user_connection_pool_timeout(self, request):
        """
        Test support in passing timeout value by the user when setting
        up a RedisCluster with a BlockingConnectionPool
        """

        timeout = 3
        client = _get_client(
            RedisCluster,
            request,
            timeout=timeout,
            connection_pool_class=redis.BlockingConnectionPool,
        )
        for _, node_config in client.nodes_manager.startup_nodes.items():
            assert node_config.redis_connection.connection_pool.timeout == timeout

    def test_set_default_node_success(self, r):
        """
        test successful replacement of the default cluster node
        """
        default_node = r.get_default_node()
        # get a different node
        new_def_node = None
        for node in r.get_nodes():
            if node != default_node:
                new_def_node = node
                break
        assert r.set_default_node(new_def_node) is True
        assert r.get_default_node() == new_def_node

    def test_set_default_node_failure(self, r):
        """
        test failed replacement of the default cluster node
        """
        default_node = r.get_default_node()
        new_def_node = ClusterNode("1.1.1.1", 1111)
        assert r.set_default_node(None) is False
        assert r.set_default_node(new_def_node) is False
        assert r.get_default_node() == default_node

    def test_get_node_from_key(self, r):
        """
        Test that get_node_from_key function returns the correct node
        """
        key = "bar"
        slot = r.keyslot(key)
        slot_nodes = r.nodes_manager.slots_cache.get(slot)
        primary = slot_nodes[0]
        assert r.get_node_from_key(key, replica=False) == primary
        replica = r.get_node_from_key(key, replica=True)
        if replica is not None:
            assert replica.server_type == REPLICA
            assert replica in slot_nodes

    @pytest.mark.usefixtures("no_cluster_retry_delay")
    @skip_if_redis_enterprise()
    def test_not_require_full_coverage_cluster_down_error(self, r):
        """
        When require_full_coverage is set to False (default client config) and not
        all slots are covered, if one of the nodes has 'cluster-require_full_coverage'
        config set to 'yes' some key-based commands should throw ClusterDownError
        """
        node = r.get_node_from_key("foo")
        missing_slot = r.keyslot("foo")
        assert r.set("foo", "bar") is True
        try:
            assert all(r.cluster_delslots(missing_slot))
            with pytest.raises(ClusterDownError):
                r.exists("foo")
        except ResponseError as e:
            assert "CLUSTERDOWN" in str(e)
        finally:
            try:
                # Add back the missing slot
                assert r.cluster_addslots(node, missing_slot) is True
                # Make sure we are not getting ClusterDownError anymore
                assert r.exists("foo") == 1
            except ResponseError as e:
                if f"Slot {missing_slot} is already busy" in str(e):
                    # It can happen if the test failed to delete this slot
                    pass
                else:
                    raise e

    def test_timeout_error_topology_refresh_reuse_connections(self, r):
        """
        By mucking TIMEOUT errors, we'll force the cluster topology to be reinitialized,
        and then ensure that only the impacted connection is replaced
        """
        node = r.get_node_from_key("key")
        r.set("key", "value")
        node_conn_origin = {}
        for n in r.get_nodes():
            node_conn_origin[n.name] = n.redis_connection
        real_func = r.get_redis_connection(node).parse_response

        class counter:
            def __init__(self, val=0):
                self.val = int(val)

        count = counter(0)
        with patch.object(Redis, "parse_response") as parse_response:

            def moved_redirect_effect(connection, *args, **options):
                # raise a timeout for 5 times so we'll need to reinitialize the topology
                if count.val == 4:
                    parse_response.side_effect = real_func
                count.val += 1
                raise TimeoutError()

            parse_response.side_effect = moved_redirect_effect
            assert r.get("key") == b"value"
            for node_name, conn in node_conn_origin.items():
                # all nodes' redis connection should have been reused during the
                # topology refresh
                # even the failing node doesn't need to establish a
                # new Redis connection (which is actually a new Redis Client instance)
                # but the connection pool is reused and all connections are reset and reconnected
                cur_node = r.get_node(node_name=node_name)
                assert conn == r.get_redis_connection(cur_node)

    def test_cluster_get_set_retry_object(self, request):
        retry = Retry(NoBackoff(), 2)
        r = _get_client(RedisCluster, request, retry=retry)
        assert r.retry.get_retries() == retry.get_retries()
        assert isinstance(r.retry._backoff, NoBackoff)
        for node in r.get_nodes():
            assert node.redis_connection.get_retry().get_retries() == 0
            assert isinstance(node.redis_connection.get_retry()._backoff, NoBackoff)
        rand_node = r.get_random_node()
        existing_conn = rand_node.redis_connection.connection_pool.get_connection()
        # Change retry policy
        new_retry = Retry(ExponentialBackoff(), 3)
        r.set_retry(new_retry)
        assert r.retry.get_retries() == new_retry.get_retries()
        assert isinstance(r.retry._backoff, ExponentialBackoff)
        for node in r.get_nodes():
            assert node.redis_connection.get_retry()._retries == 0
            assert isinstance(node.redis_connection.get_retry()._backoff, NoBackoff)
        assert existing_conn.retry._retries == 0
        new_conn = rand_node.redis_connection.connection_pool.get_connection()
        assert new_conn.retry._retries == 0

    def test_cluster_retry_object(self, r) -> None:
        # Test default retry
        # FIXME: Workaround for https://github.com/redis/redis-py/issues/3030
        host = r.get_default_node().host

        # test default retry config
        retry = r.retry
        assert isinstance(retry, Retry)
        assert retry.get_retries() == 10
        assert isinstance(retry._backoff, type(ExponentialWithJitterBackoff()))
        assert retry._backoff._base == 0.01
        assert retry._backoff._cap == 1
        node1_connection = r.get_node(host, 16379).redis_connection
        node2_connection = r.get_node(host, 16380).redis_connection
        assert node1_connection.get_retry()._retries == 0
        assert node2_connection.get_retry()._retries == 0

        # Test custom retry is not applied to nodes
        retry = Retry(ExponentialBackoff(10, 5), 5)
        rc_custom_retry = RedisCluster(host, 16379, retry=retry)
        assert (
            rc_custom_retry.get_node(host, 16379)
            .redis_connection.get_retry()
            .get_retries()
            == 0
        )

    def test_replace_cluster_node(self, r) -> None:
        prev_default_node = r.get_default_node()
        r.replace_default_node()
        assert r.get_default_node() != prev_default_node
        r.replace_default_node(prev_default_node)
        assert r.get_default_node() == prev_default_node

    def test_default_node_is_replaced_after_exception(self, r):
        curr_default_node = r.get_default_node()
        # CLUSTER NODES command is being executed on the default node
        nodes = r.cluster_nodes()
        assert "myself" in nodes.get(curr_default_node.name).get("flags")

        def raise_connection_error():
            raise ConnectionError("error")

        # Mock connection error for the default node
        mock_node_resp_func(curr_default_node, raise_connection_error)
        # Test that the command succeed from a different node
        nodes = r.cluster_nodes()
        assert "myself" not in nodes.get(curr_default_node.name).get("flags")
        assert r.get_default_node() != curr_default_node

    @pytest.mark.usefixtures("no_cluster_retry_delay")
    def test_address_remap(self, request, master_host):
        """Test that we can create a rediscluster object with
        a host-port remapper and map connections through proxy objects
        """

        # we remap the first n nodes
        offset = 1000
        n = 6
        hostname, master_port = master_host
        ports = [master_port + i for i in range(n)]

        def address_remap(address):
            # remap first three nodes to our local proxy
            # old = host, port
            host, port = address
            if int(port) in ports:
                host, port = "127.0.0.1", int(port) + offset
            # print(f"{old} {host, port}")
            return host, port

        # create the proxies
        proxies = [
            NodeProxy(("127.0.0.1", port + offset), (hostname, port)) for port in ports
        ]
        for p in proxies:
            p.start()
        try:
            # create cluster:
            r = _get_client(
                RedisCluster, request, flushdb=False, address_remap=address_remap
            )
            try:
                assert r.ping() is True
                assert r.set("byte_string", b"giraffe")
                assert r.get("byte_string") == b"giraffe"
            finally:
                r.close()
        finally:
            for p in proxies:
                p.close()

        # verify that the proxies were indeed used
        n_used = sum((1 if p.n_connections else 0) for p in proxies)
        assert n_used > 1

    @pytest.mark.usefixtures("no_cluster_retry_delay")
    @pytest.mark.parametrize(
        "remap_host, offset",
        # Not the ports of the other remap tests: their listeners may still be open
        [("127.0.0.1", 2000), ("localhost", 2100)],
    )
    def test_address_remap_applies_to_redirects(
        self, request, master_host, remap_host, offset
    ):
        """
        A MOVED or ASK redirect names the address the node advertises, so the
        retry must go through address_remap like the CLUSTER SLOTS addresses do.
        A remap to localhost is cached under the resolved address, so the retry
        must look the node up the same way.
        """
        n = 6
        hostname, master_port = master_host
        ports = [master_port + i for i in range(n)]

        def address_remap(address):
            host, port = address
            if int(port) in ports:
                host, port = remap_host, int(port) + offset
            return host, port

        proxies = [
            NodeProxy(("127.0.0.1", port + offset), (hostname, port)) for port in ports
        ]
        for p in proxies:
            p.start()
        try:
            r = _get_client(
                RedisCluster, request, flushdb=False, address_remap=address_remap
            )
            try:
                slot = r.keyslot("foo")
                owner = r.nodes_manager.get_node_from_slot(slot)
                other = next(p for p in r.get_primaries() if p.name != owner.name)
                # The address the redirected-to node advertises, not its proxy
                redirect_port = other.port - offset
                for redirect_error in (AskError, MovedError):
                    with patch.object(Redis, "parse_response") as parse_response:

                        def redirect_effect(connection, *args, **options):
                            def ok_response(connection, *args, **options):
                                assert (connection.host, connection.port) == (
                                    "127.0.0.1",
                                    other.port,
                                )
                                return "MOCK_OK"

                            parse_response.side_effect = ok_response
                            raise redirect_error(f"{slot} {hostname}:{redirect_port}")

                        parse_response.side_effect = redirect_effect

                        assert r.execute_command("SET", "foo", "bar") == "MOCK_OK"
            finally:
                r.close()
        finally:
            for p in proxies:
                p.close()


@pytest.mark.fixed_client
class TestRedisClusterObjMocked:
    """RedisCluster behaviour checked against mocked nodes; no server needed."""

    @pytest.mark.usefixtures("no_cluster_retry_delay")
    @pytest.mark.parametrize("error", RedisCluster.ERRORS_ALLOW_RETRY)
    def test_cluster_down_overreaches_retry_attempts(self, error):
        """
        When error that allows retry is thrown, test that we retry executing
        the command as many times as configured in cluster_error_retry_attempts
        and then raise the exception
        """
        with patch.object(RedisCluster, "_execute_command") as execute_command:

            def raise_error(target_node, *args, **kwargs):
                execute_command.failed_calls += 1
                raise error("mocked error")

            execute_command.side_effect = raise_error
            # Without this the attribute is a MagicMock, so the comparison below
            # is never an integer comparison.
            execute_command.failed_calls = 0

            rc = get_mocked_redis_client(host=default_host, port=default_port)

            with pytest.raises(error):
                rc.get("bar")
            # retry counts retries, so the first attempt is one more.
            assert execute_command.failed_calls == rc.retry.get_retries() + 1

    @pytest.mark.parametrize(
        "pool_kwargs",
        [
            {"connection_pool_class": ConnectionPool},
            {"connection_pool_class": BlockingConnectionPool, "timeout": 0.01},
        ],
        ids=["ConnectionPool", "BlockingConnectionPool"],
    )
    def test_failed_retry_checkout_keeps_previous_connection_released(
        self, pool_kwargs
    ):
        """
        A retry whose connection checkout fails must not release the previous
        attempt's connection again: that attempt already released it, and by
        then another caller may have checked it out. A second release would
        hand that caller's connection to the next one as well.
        """
        rc = get_mocked_redis_client(
            host=default_host,
            port=default_port,
            max_connections=1,
            retry=Retry(NoBackoff(), 0),
            **pool_kwargs,
        )
        pool = rc.get_node_from_key("foo").redis_connection.connection_pool
        checkout = pool.get_connection
        checkouts = []
        held_by_other_caller = []

        def get_connection():
            checkouts.append(None)
            if len(checkouts) == 2:
                # The retry: another caller gets the connection the first
                # attempt released, which leaves the pool empty.
                held_by_other_caller.append(checkout())
            return checkout()

        with (
            patch.object(Connection, "connect"),
            patch.object(Connection, "can_read", return_value=False),
            patch.object(Connection, "send_command"),
            patch.object(
                Redis,
                "parse_response",
                side_effect=TryAgainError("Multiple keys request during rehashing"),
            ),
            patch.object(pool, "get_connection", side_effect=get_connection),
        ):
            with pytest.raises(ConnectionError):
                rc.get("foo")
            assert len(held_by_other_caller) == 1

            # The other caller still holds the pool's only connection, so the
            # next checkout must not get it too
            with pytest.raises(ConnectionError):
                checkout()

    def test_empty_startup_nodes(self):
        """
        Test that exception is raised when empty providing empty startup_nodes
        """
        with pytest.raises(RedisClusterException) as ex:
            RedisCluster(startup_nodes=[])

        assert str(ex.value).startswith(
            "RedisCluster requires at least one node to discover the cluster"
        ), str_if_bytes(ex.value)

    def test_ssl_with_non_ssl_connection_class_raises(self):
        with pytest.raises(RedisClusterException) as ex:
            RedisCluster(
                host=default_host,
                port=default_port,
                ssl=True,
                connection_class=Connection,
            )

        assert "SSLConnection subclass" in str(ex.value)

    def test_unix_socket_path_raises(self):
        with pytest.raises(RedisClusterException) as ex:
            RedisCluster(
                host=default_host,
                port=default_port,
                unix_socket_path="/tmp/redis.sock",
            )

        assert "Unix Domain Socket" in str(ex.value)

    def test_unix_socket_path_none_uses_tcp(self):
        rc = get_mocked_redis_client(
            host=default_host,
            port=default_port,
            cluster_slots=default_cluster_slots,
            unix_socket_path=None,
        )

        for node in rc.nodes_manager.nodes_cache.values():
            connection = node.redis_connection.connection_pool.make_connection()
            assert isinstance(connection, Connection)

    def test_get_node_name(self):
        assert (
            get_node_name(default_host, default_port)
            == f"{default_host}:{default_port}"
        )

    def test_ssl_options_reach_node_connections(self):
        """
        TLS options accepted by Redis must not be dropped by cleanup_kwargs
        before they reach the per-node connection pools
        """
        cluster = get_mocked_redis_client(
            host=default_host,
            port=default_port,
            ssl=True,
            ssl_min_version=ssl.TLSVersion.TLSv1_3,
            ssl_ciphers="ECDHE-RSA-AES256-GCM-SHA384",
            ssl_validate_ocsp=True,
            ssl_validate_ocsp_stapled=True,
            ssl_ocsp_expected_cert="expected-cert",
        )
        node = cluster.get_node(host=default_host, port=default_port)
        connection = node.redis_connection.connection_pool.make_connection()
        assert connection.ssl_min_version == ssl.TLSVersion.TLSv1_3
        assert connection.ssl_ciphers == "ECDHE-RSA-AES256-GCM-SHA384"
        assert connection.ssl_validate_ocsp is True
        assert connection.ssl_validate_ocsp_stapled is True
        assert connection.ssl_ocsp_expected_cert == "expected-cert"

    def test_host_port_startup_node(self):
        """
        Test that it is possible to use host & port arguments as startup node
        args
        """
        cluster = get_mocked_redis_client(host=default_host, port=default_port)
        assert cluster.get_node(host=default_host, port=default_port) is not None

    @pytest.mark.parametrize(
        "read_from_replicas,load_balancing_strategy,mocks_srv_ports",
        [
            (True, None, [7001, 7002, 7001]),
            (True, LoadBalancingStrategy.ROUND_ROBIN, [7001, 7002, 7001]),
            (True, LoadBalancingStrategy.ROUND_ROBIN_REPLICAS, [7002, 7002, 7002]),
            (True, LoadBalancingStrategy.RANDOM, [7002, 7001, 7002]),
            (True, LoadBalancingStrategy.RANDOM_REPLICA, [7002, 7002, 7002]),
            (False, LoadBalancingStrategy.ROUND_ROBIN, [7001, 7002, 7001]),
            (False, LoadBalancingStrategy.ROUND_ROBIN_REPLICAS, [7002, 7002, 7002]),
            (False, LoadBalancingStrategy.RANDOM, [7002, 7001, 7002]),
            (False, LoadBalancingStrategy.RANDOM_REPLICA, [7002, 7002, 7002]),
        ],
    )
    def test_reading_with_load_balancing_strategies(
        self,
        read_from_replicas: bool,
        load_balancing_strategy: LoadBalancingStrategy,
        mocks_srv_ports: List[int],
    ):
        def _make_mock_randint():
            _state = (
                1  # Start with 1 so we have clearly different results from round robin
            )

            def _mock_randint(lower: int, upper: int) -> int:
                """
                Return a controlled sequence of numbers when called repeatedly
                """
                if lower == upper:
                    return lower

                nonlocal _state
                res = _state + lower
                _state ^= 1
                return res

            return _mock_randint

        mock_randint = _make_mock_randint()

        with patch.multiple(
            Connection,
            send_command=DEFAULT,
            read_response=DEFAULT,
            _connect=DEFAULT,
            can_read=DEFAULT,
            on_connect=DEFAULT,
        ) as mocks:
            with (
                patch.object(Redis, "parse_response") as parse_response,
                patch("random.randint", wraps=mock_randint),
            ):

                def parse_response_mock_first(connection, *args, **options):
                    # Primary
                    assert connection.port == mocks_srv_ports[0]
                    parse_response.side_effect = parse_response_mock_second
                    return "MOCK_OK"

                def parse_response_mock_second(connection, *args, **options):
                    # Replica
                    assert connection.port == mocks_srv_ports[1]
                    parse_response.side_effect = parse_response_mock_third
                    return "MOCK_OK"

                def parse_response_mock_third(connection, *args, **options):
                    # Primary
                    assert connection.port == mocks_srv_ports[2]
                    return "MOCK_OK"

                # We don't need to create a real cluster connection but we
                # do want RedisCluster.on_connect function to get called,
                # so we'll mock some of the Connection's functions to allow it
                parse_response.side_effect = parse_response_mock_first
                mocks["send_command"].return_value = True
                mocks["read_response"].return_value = "OK"
                mocks["_connect"].return_value = True
                mocks["can_read"].return_value = False
                mocks["on_connect"].return_value = True

                # Create a cluster with reading from replications
                read_cluster = get_mocked_redis_client(
                    host=default_host,
                    port=default_port,
                    read_from_replicas=read_from_replicas,
                    load_balancing_strategy=load_balancing_strategy,
                )
                assert read_cluster.read_from_replicas is read_from_replicas
                assert read_cluster.load_balancing_strategy is load_balancing_strategy
                # Check that we read from the slot's nodes in a round robin
                # matter.
                # 'foo' belongs to slot 12182 and the slot's nodes are:
                # [(127.0.0.1,7001,primary), (127.0.0.1,7002,replica)]
                read_cluster.get("foo")
                read_cluster.get("foo")
                read_cluster.get("foo")
                expected_calls_list = []
                expected_calls_list.append(call("READONLY"))
                expected_calls_list.append(call("GET", "foo", keys=["foo"]))

                if (
                    load_balancing_strategy is None
                    or load_balancing_strategy == LoadBalancingStrategy.ROUND_ROBIN
                    or load_balancing_strategy == LoadBalancingStrategy.RANDOM
                ):
                    # in the round robin and random strategies, the primary node can also receive read
                    # requests and this means that there will be second node connected
                    expected_calls_list.append(call("READONLY"))

                expected_calls_list.extend(
                    [
                        call("GET", "foo", keys=["foo"]),
                        call("GET", "foo", keys=["foo"]),
                    ]
                )

                mocks["send_command"].assert_has_calls(expected_calls_list)

    def test_startup_nodes(self):
        """
        Test that it is possible to use startup_nodes
        argument to init the cluster
        """
        port_1 = 7000
        port_2 = 7001
        startup_nodes = [
            ClusterNode(default_host, port_1),
            ClusterNode(default_host, port_2),
        ]
        cluster = get_mocked_redis_client(startup_nodes=startup_nodes)
        assert (
            cluster.get_node(host=default_host, port=port_1) is not None
            and cluster.get_node(host=default_host, port=port_2) is not None
        )

    @pytest.mark.parametrize("error", [AskError, MovedError])
    def test_redirect_without_host(self, error):
        """
        A node whose endpoint is unknown (cluster-preferred-endpoint-type
        unknown-endpoint) redirects with only a port, and the command follows
        the redirect to that port on the host of the node that sent it.
        """
        rc = get_mocked_redis_client(host=default_host, port=default_port)
        slot = rc.keyslot("foo")
        origin = rc.get_node_from_key("foo")
        target = next(node for node in rc.get_primaries() if node is not origin)
        calls = []
        redirects = []

        def parse_response(redis_node, connection, command, **kwargs):
            calls.append((redis_node, command))
            if redis_node is origin.redis_connection:
                redirects.append(error(f"{slot} :{target.port}"))
                raise redirects[-1]
            return b"value"

        with (
            patch("redis.cluster.get_connection"),
            patch.object(
                Redis, "parse_response", autospec=True, side_effect=parse_response
            ),
        ):
            assert rc.get("foo") == b"value"

        expected = [(origin.redis_connection, "GET"), (target.redis_connection, "GET")]
        if error is AskError:
            expected.insert(1, (target.redis_connection, "ASKING"))
        assert calls == expected
        assert [redirect.node_addr for redirect in redirects] == [
            (origin.host, target.port)
        ]

    @pytest.mark.parametrize("error", [AskError, MovedError])
    def test_redirect_without_host_with_address_remap(self, error):
        """
        The host filled in for a redirect without one has already been through
        address_remap, and following the redirect remaps it again. With a remap
        that only picks the port, as for a proxy, the command still reaches the
        remapped address of the node the redirect names.
        """
        offset = 1000

        def address_remap(address):
            host, port = address
            return host, port + offset

        rc = get_mocked_redis_client(
            host=default_host, port=default_port, address_remap=address_remap
        )
        slot = rc.keyslot("foo")
        origin = rc.get_node_from_key("foo")
        target = next(node for node in rc.get_primaries() if node is not origin)
        calls = []

        def parse_response(redis_node, connection, command, **kwargs):
            calls.append((redis_node, command))
            if redis_node is origin.redis_connection:
                # A node redirects to the port it advertises, not the remapped one
                raise error(f"{slot} :{target.port - offset}")
            return b"value"

        with (
            patch("redis.cluster.get_connection"),
            patch.object(
                Redis, "parse_response", autospec=True, side_effect=parse_response
            ),
        ):
            assert rc.get("foo") == b"value"

        expected = [(origin.redis_connection, "GET"), (target.redis_connection, "GET")]
        if error is AskError:
            expected.insert(1, (target.redis_connection, "ASKING"))
        assert calls == expected

    @pytest.mark.parametrize("error", [AskError, MovedError])
    def test_transaction_redirect_without_host(self, error):
        """
        A transaction redirected with only a port is retried on that port of the
        host of the node that sent the redirect.
        """
        rc = get_mocked_redis_client(host=default_host, port=default_port)
        slot = rc.keyslot("foo")
        origin = rc.get_node_from_key("foo")
        target = next(node for node in rc.get_primaries() if node is not origin)
        calls = []

        def parse_response(redis_node, connection, command, **kwargs):
            calls.append((redis_node, command))
            if redis_node is origin.redis_connection:
                if command == "_":
                    raise error(f"{slot} :{target.port}")
                if command == "EXEC":
                    raise ExecAbortError()
            if command == "EXEC":
                return [b"bar"]
            return b"QUEUED" if command == "_" else b"OK"

        with (
            patch("redis.cluster.get_connection"),
            patch.object(
                Redis, "parse_response", autospec=True, side_effect=parse_response
            ),
        ):
            with rc.pipeline(transaction=True) as pipe:
                assert pipe.get("foo").execute() == [b"bar"]

        assert calls == [
            (node.redis_connection, command)
            for node in (origin, target)
            for command in ("MULTI", "_", "EXEC")
        ]


@pytest.mark.onlycluster
class TestClusterRedisCommands:
    """
    Tests for RedisCluster unique commands
    """

    def test_case_insensitive_command_names(self, r):
        assert (
            r.cluster_response_callbacks["cluster slots"]
            == r.cluster_response_callbacks["CLUSTER SLOTS"]
        )

    @pytest.mark.parametrize(
        "load_balancing_strategy",
        [
            LoadBalancingStrategy.ROUND_ROBIN,
            LoadBalancingStrategy.ROUND_ROBIN_REPLICAS,
            LoadBalancingStrategy.RANDOM,
            LoadBalancingStrategy.RANDOM_REPLICA,
        ],
    )
    def test_get_and_set_with_load_balanced_client(
        self, request, load_balancing_strategy: LoadBalancingStrategy
    ) -> None:
        r = _get_client(
            cls=RedisCluster,
            request=request,
            load_balancing_strategy=load_balancing_strategy,
        )

        # get and set can't be tested independently of each other
        assert r.get("a") is None

        byte_string = b"value"
        assert r.set("byte_string", byte_string)

        # run the get command for the same key several times
        # to iterate over the read nodes
        assert r.get("byte_string") == byte_string
        assert r.get("byte_string") == byte_string
        assert r.get("byte_string") == byte_string

    def test_mget_nonatomic(self, r):
        assert r.mget_nonatomic([]) == []
        assert r.mget_nonatomic(["a", "b"]) == [None, None]
        r["a"] = "1"
        r["b"] = "2"
        r["c"] = "3"

        assert r.mget_nonatomic("a", "other", "b", "c") == [b"1", None, b"2", b"3"]

    def test_mset_nonatomic(self, r):
        d = {"a": b"1", "b": b"2", "c": b"3", "d": b"4"}
        assert r.mset_nonatomic(d)
        for k, v in d.items():
            assert r[k] == v

    def test_config_set(self, r):
        assert r.config_set("slowlog-log-slower-than", 0)

    def test_cluster_config_resetstat(self, r):
        r.ping(target_nodes="all")
        all_info = r.info(target_nodes="all")
        prior_commands_processed = -1
        for node_info in all_info.values():
            prior_commands_processed = node_info["total_commands_processed"]
            assert prior_commands_processed >= 1
        r.config_resetstat(target_nodes="all")
        all_info = r.info(target_nodes="all")
        for node_info in all_info.values():
            reset_commands_processed = node_info["total_commands_processed"]
            assert reset_commands_processed < prior_commands_processed

    def test_client_setname(self, r):
        node = r.get_random_node()
        r.client_setname("redis_py_test", target_nodes=node)
        client_name = r.client_getname(target_nodes=node)
        assert_resp_response(
            r, client_name, "redis_py_test", b"redis_py_test", "redis_py_test"
        )

    def test_exists(self, r):
        d = {"a": b"1", "b": b"2", "c": b"3", "d": b"4"}
        r.mset_nonatomic(d)
        assert r.exists(*d.keys()) == len(d)

    def test_delete(self, r):
        d = {"a": b"1", "b": b"2", "c": b"3", "d": b"4"}
        r.mset_nonatomic(d)
        assert r.delete(*d.keys()) == len(d)
        assert r.delete(*d.keys()) == 0

    def test_touch(self, r):
        d = {"a": b"1", "b": b"2", "c": b"3", "d": b"4"}
        r.mset_nonatomic(d)
        assert r.touch(*d.keys()) == len(d)

    def test_unlink(self, r):
        d = {"a": b"1", "b": b"2", "c": b"3", "d": b"4"}
        r.mset_nonatomic(d)
        assert r.unlink(*d.keys()) == len(d)
        # UNLINK removes the keys from the keyspace right away; only the memory
        # is reclaimed in the background
        assert r.unlink(*d.keys()) == 0

    def test_pubsub_channels_merge_results(self, r):
        nodes = r.get_nodes()
        channels = []
        pubsub_nodes = []
        i = 0
        for node in nodes:
            channel = f"foo{i}"
            # We will create different pubsub clients where each one is
            # connected to a different node
            p = r.pubsub(node)
            pubsub_nodes.append(p)
            p.subscribe(channel)
            b_channel = channel.encode("utf-8")
            channels.append(b_channel)
            # Read the subscribe ack, so the server has processed it before the
            # node is queried; the wait ends as soon as the ack arrives
            ack = wait_for_message(p, timeout=5.0)
            assert ack is not None and ack["type"] == "subscribe", ack
            # Assert that each node returns only the channel it subscribed to
            assert node.redis_connection.pubsub_channels() == [b_channel]
            i += 1
        # Assert that the cluster's pubsub_channels function returns ALL of
        # the cluster's channels
        result = r.pubsub_channels(target_nodes="all")
        result.sort()
        assert result == channels

    def test_pubsub_numsub_merge_results(self, r):
        nodes = r.get_nodes()
        pubsub_nodes = []
        channel = "foo"
        b_channel = channel.encode("utf-8")
        for node in nodes:
            # We will create different pubsub clients where each one is
            # connected to a different node
            p = r.pubsub(node)
            pubsub_nodes.append(p)
            p.subscribe(channel)
            # Read the subscribe ack, so the server has processed it before the
            # node is queried; the wait ends as soon as the ack arrives
            ack = wait_for_message(p, timeout=5.0)
            assert ack is not None and ack["type"] == "subscribe", ack
            # Assert that each node returns that only one client is subscribed
            assert node.redis_connection.pubsub_numsub(channel) == [(b_channel, 1)]
        # Assert that the cluster's pubsub_numsub function returns ALL clients
        # subscribed to this channel in the entire cluster
        assert r.pubsub_numsub(channel, target_nodes="all") == [(b_channel, len(nodes))]

    def test_pubsub_numpat_merge_results(self, r):
        nodes = r.get_nodes()
        pubsub_nodes = []
        pattern = "foo*"
        for node in nodes:
            # We will create different pubsub clients where each one is
            # connected to a different node
            p = r.pubsub(node)
            pubsub_nodes.append(p)
            p.psubscribe(pattern)
            # Read the psubscribe ack, so the server has processed it before the
            # node is queried; the wait ends as soon as the ack arrives
            ack = wait_for_message(p, timeout=5.0)
            assert ack is not None and ack["type"] == "psubscribe", ack
            # Assert that each node returns that only one client is subscribed
            assert node.redis_connection.pubsub_numpat() == 1
        # Assert that the cluster's pubsub_numsub function returns ALL clients
        # subscribed to this channel in the entire cluster
        assert r.pubsub_numpat(target_nodes="all") == len(nodes)

    @skip_if_server_version_lt("2.8.0")
    def test_cluster_pubsub_channels(self, r):
        p = r.pubsub()
        p.subscribe("foo", "bar", "baz", "quux")
        for i in range(4):
            assert wait_for_message(p, timeout=0.5)["type"] == "subscribe"
        expected = [b"bar", b"baz", b"foo", b"quux"]
        assert all(
            [channel in r.pubsub_channels(target_nodes="all") for channel in expected]
        )

    @skip_if_server_version_lt("2.8.0")
    def test_cluster_pubsub_numsub(self, r):
        p1 = r.pubsub()
        p1.subscribe("foo", "bar", "baz")
        for i in range(3):
            assert wait_for_message(p1, timeout=0.5)["type"] == "subscribe"
        p2 = r.pubsub()
        p2.subscribe("bar", "baz")
        for i in range(2):
            assert wait_for_message(p2, timeout=0.5)["type"] == "subscribe"
        p3 = r.pubsub()
        p3.subscribe("baz")
        assert wait_for_message(p3, timeout=0.5)["type"] == "subscribe"

        channels = [(b"foo", 1), (b"bar", 2), (b"baz", 3)]
        assert r.pubsub_numsub("foo", "bar", "baz", target_nodes="all") == channels

    @skip_if_redis_enterprise()
    def test_cluster_myid(self, r):
        node = r.get_random_node()
        myid = r.cluster_myid(node)
        assert len(myid) == 40

    @skip_if_redis_enterprise()
    def test_cluster_slots(self, r):
        mock_all_nodes_resp(r, default_cluster_slots)
        cluster_slots = r.cluster_slots()
        assert isinstance(cluster_slots, dict)
        assert len(default_cluster_slots) == len(cluster_slots)
        assert cluster_slots.get((0, 8191)) is not None
        assert cluster_slots.get((0, 8191)).get("primary") == ("127.0.0.1", 7000)

    @skip_if_server_version_lt("7.0.0")
    @skip_if_redis_enterprise()
    def test_cluster_shards(self, r):
        cluster_shards = r.cluster_shards()
        assert isinstance(cluster_shards, list)
        assert isinstance(cluster_shards[0], dict)
        attributes = [
            b"id",
            b"endpoint",
            b"ip",
            b"hostname",
            b"port",
            b"tls-port",
            b"role",
            b"replication-offset",
            b"health",
        ]
        if expects_unified_shape(r):
            attributes = [str_if_bytes(attribute) for attribute in attributes]
        for x in cluster_shards:
            assert_resp_response(
                r,
                list(x.keys()),
                ["slots", "nodes"],
                [b"slots", b"nodes"],
                ["slots", "nodes"],
            )
            try:
                x["nodes"]
                key = "nodes"
            except KeyError:
                key = b"nodes"
            for node in x[key]:
                for attribute in node.keys():
                    assert attribute in attributes
            slots = x["slots"] if "slots" in x else x[b"slots"]
            if expects_resp2_shape(r) or expects_unified_shape(r):
                assert all(
                    isinstance(slot, tuple)
                    and len(slot) == 2
                    and all(isinstance(bound, int) for bound in slot)
                    for slot in slots
                )
            else:
                # protocol=3 with legacy_responses=True keeps the previous RESP3
                # shape: the server's flat [start, end, ...] integer array.
                assert len(slots) % 2 == 0
                assert all(isinstance(bound, int) for bound in slots)

    @skip_if_server_version_lt("7.2.0")
    @skip_if_redis_enterprise()
    def test_cluster_myshardid(self, r):
        myshardid = r.cluster_myshardid()
        assert isinstance(myshardid, str)
        assert len(myshardid) > 0

    @skip_if_server_version_lt("7.2.0")
    @skip_if_redis_enterprise()
    def test_cluster_myshardid_decoded(self, decoded_r):
        myshardid = decoded_r.cluster_myshardid()
        assert isinstance(myshardid, str)
        assert len(myshardid) == 40

    @skip_if_redis_enterprise()
    def test_cluster_addslots(self, r):
        node = r.get_random_node()
        mock_node_resp(node, "OK")
        assert r.cluster_addslots(node, 1, 2, 3) is True

    @skip_if_server_version_lt("7.0.0")
    @skip_if_redis_enterprise()
    def test_cluster_addslotsrange(self, r):
        node = r.get_random_node()
        mock_node_resp(node, "OK")
        assert r.cluster_addslotsrange(node, 1, 5)

    @skip_if_redis_enterprise()
    def test_cluster_countkeysinslot(self, r):
        node = r.nodes_manager.get_node_from_slot(1)
        mock_node_resp(node, 2)
        assert r.cluster_countkeysinslot(1) == 2

    def test_cluster_count_failure_report(self, r):
        mock_all_nodes_resp(r, 0)
        assert r.cluster_count_failure_report("node_0") == 0

    @skip_if_redis_enterprise()
    def test_cluster_failover(self, r):
        node = r.get_random_node()
        mock_node_resp(node, "OK")
        assert r.cluster_failover(node) is True
        assert r.cluster_failover(node, "FORCE") is True
        assert r.cluster_failover(node, "TAKEOVER") is True
        with pytest.raises(RedisError):
            r.cluster_failover(node, "FORCT")

    @skip_if_redis_enterprise()
    def test_cluster_info(self, r):
        info = r.cluster_info()
        assert isinstance(info, dict)
        assert info["cluster_state"] == "ok"

    @skip_if_redis_enterprise()
    def test_cluster_keyslot(self, r):
        mock_all_nodes_resp(r, 12182)
        assert r.cluster_keyslot("foo") == 12182

    @skip_if_redis_enterprise()
    def test_cluster_meet(self, r):
        node = r.get_default_node()
        mock_node_resp(node, "OK")
        assert r.cluster_meet("127.0.0.1", 6379) is True

    @skip_if_redis_enterprise()
    def test_cluster_nodes(self, r):
        response = (
            "c8253bae761cb1ecb2b61857d85dfe455a0fec8b 172.17.0.7:7006 "
            "slave aa90da731f673a99617dfe930306549a09f83a6b 0 "
            "1447836263059 5 connected\n"
            "9bd595fe4821a0e8d6b99d70faa660638a7612b3 172.17.0.7:7008 "
            "master - 0 1447836264065 0 connected\n"
            "aa90da731f673a99617dfe930306549a09f83a6b 172.17.0.7:7003 "
            "myself,master - 0 0 2 connected 5461-10922\n"
            "1df047e5a594f945d82fc140be97a1452bcbf93e 172.17.0.7:7007 "
            "slave 19efe5a631f3296fdf21a5441680f893e8cc96ec 0 "
            "1447836262556 3 connected\n"
            "4ad9a12e63e8f0207025eeba2354bcf4c85e5b22 172.17.0.7:7005 "
            "master - 0 1447836262555 7 connected 0-5460\n"
            "19efe5a631f3296fdf21a5441680f893e8cc96ec 172.17.0.7:7004 "
            "master - 0 1447836263562 3 connected 10923-16383\n"
            "fbb23ed8cfa23f17eaf27ff7d0c410492a1093d6 172.17.0.7:7002 "
            "master,fail - 1447829446956 1447829444948 1 disconnected\n"
        )
        mock_all_nodes_resp(r, response)
        nodes = r.cluster_nodes()
        assert len(nodes) == 7
        assert nodes.get("172.17.0.7:7006") is not None
        assert (
            nodes.get("172.17.0.7:7006").get("node_id")
            == "c8253bae761cb1ecb2b61857d85dfe455a0fec8b"
        )

    @skip_if_redis_enterprise()
    def test_cluster_nodes_importing_migrating(self, r):
        response = (
            "488ead2fcce24d8c0f158f9172cb1f4a9e040fe5 127.0.0.1:16381@26381 "
            "master - 0 1648975557664 3 connected 10923-16383\n"
            "8ae2e70812db80776f739a72374e57fc4ae6f89d 127.0.0.1:16380@26380 "
            "master - 0 1648975555000 2 connected 1 5461-10922 ["
            "2-<-ed8007ccfa2d91a7b76f8e6fba7ba7e257034a16]\n"
            "ed8007ccfa2d91a7b76f8e6fba7ba7e257034a16 127.0.0.1:16379@26379 "
            "myself,master - 0 1648975556000 1 connected 0 2-5460 ["
            "2->-8ae2e70812db80776f739a72374e57fc4ae6f89d]\n"
        )
        mock_all_nodes_resp(r, response)
        nodes = r.cluster_nodes()
        assert len(nodes) == 3
        node_16379 = nodes.get("127.0.0.1:16379")
        node_16380 = nodes.get("127.0.0.1:16380")
        node_16381 = nodes.get("127.0.0.1:16381")
        assert node_16379.get("migrations") == [
            {
                "slot": "2",
                "node_id": "8ae2e70812db80776f739a72374e57fc4ae6f89d",
                "state": "migrating",
            }
        ]
        assert node_16379.get("slots") == [["0"], ["2", "5460"]]
        assert node_16380.get("migrations") == [
            {
                "slot": "2",
                "node_id": "ed8007ccfa2d91a7b76f8e6fba7ba7e257034a16",
                "state": "importing",
            }
        ]
        assert node_16380.get("slots") == [["1"], ["5461", "10922"]]
        assert node_16381.get("slots") == [["10923", "16383"]]
        assert node_16381.get("migrations") == []

    @skip_if_redis_enterprise()
    def test_cluster_replicate(self, r):
        node = r.get_random_node()
        all_replicas = r.get_replicas()
        mock_all_nodes_resp(r, "OK")
        assert r.cluster_replicate(node, "c8253bae761cb61857d") is True
        results = r.cluster_replicate(all_replicas, "c8253bae761cb61857d")
        if isinstance(results, dict):
            for res in results.values():
                assert res is True
        else:
            assert results is True

    @skip_if_redis_enterprise()
    def test_cluster_reset(self, r):
        mock_all_nodes_resp(r, "OK")
        assert r.cluster_reset() is True
        assert r.cluster_reset(False) is True
        all_results = r.cluster_reset(False, target_nodes="all")
        for res in all_results.values():
            assert res is True

    @skip_if_redis_enterprise()
    def test_cluster_save_config(self, r):
        node = r.get_random_node()
        all_nodes = r.get_nodes()
        mock_all_nodes_resp(r, "OK")
        assert r.cluster_save_config(node) is True
        all_results = r.cluster_save_config(all_nodes)
        for res in all_results.values():
            assert res is True

    @skip_if_redis_enterprise()
    def test_cluster_get_keys_in_slot(self, r):
        response = ["{foo}1", "{foo}2"]
        node = r.nodes_manager.get_node_from_slot(12182)
        mock_node_resp(node, response)
        keys = r.cluster_get_keys_in_slot(12182, 4)
        assert keys == response

    @skip_if_redis_enterprise()
    def test_cluster_set_config_epoch(self, r):
        mock_all_nodes_resp(r, "OK")
        assert r.cluster_set_config_epoch(3) is True
        all_results = r.cluster_set_config_epoch(3, target_nodes="all")
        for res in all_results.values():
            assert res is True

    @skip_if_redis_enterprise()
    def test_cluster_setslot(self, r):
        node = r.get_random_node()
        mock_node_resp(node, "OK")
        assert r.cluster_setslot(node, "node_0", 1218, "IMPORTING") is True
        assert r.cluster_setslot(node, "node_0", 1218, "NODE") is True
        assert r.cluster_setslot(node, "node_0", 1218, "MIGRATING") is True
        with pytest.raises(RedisError):
            r.cluster_failover(node, "STABLE")
        with pytest.raises(RedisError):
            r.cluster_failover(node, "STATE")

    def test_cluster_setslot_stable(self, r):
        node = r.nodes_manager.get_node_from_slot(12182)
        mock_node_resp(node, "OK")
        assert r.cluster_setslot_stable(12182) is True
        assert node.redis_connection.connection.read_response.called

    @skip_if_redis_enterprise()
    def test_cluster_replicas(self, r):
        response = [
            b"01eca22229cf3c652b6fca0d09ff6941e0d2e3 "
            b"127.0.0.1:6377@16377 slave "
            b"52611e796814b78e90ad94be9d769a4f668f9a 0 "
            b"1634550063436 4 connected",
            b"r4xfga22229cf3c652b6fca0d09ff69f3e0d4d "
            b"127.0.0.1:6378@16378 slave "
            b"52611e796814b78e90ad94be9d769a4f668f9a 0 "
            b"1634550063436 4 connected",
        ]
        mock_all_nodes_resp(r, response)
        replicas = r.cluster_replicas("52611e796814b78e90ad94be9d769a4f668f9a")
        assert replicas.get("127.0.0.1:6377") is not None
        assert replicas.get("127.0.0.1:6378") is not None
        assert (
            replicas.get("127.0.0.1:6378").get("node_id")
            == "r4xfga22229cf3c652b6fca0d09ff69f3e0d4d"
        )

    @skip_if_server_version_lt("7.0.0")
    def test_cluster_links(self, r):
        node = r.get_random_node()
        res = r.cluster_links(node)
        if expects_resp2_shape(r):
            links_to = sum(x.count(b"to") for x in res)
            links_for = sum(x.count(b"from") for x in res)
            assert links_to == links_for
            for i in range(0, len(res) - 1, 2):
                assert res[i][3] == res[i + 1][3]
        elif expects_unified_shape(r):
            links_to = len(
                list(filter(lambda x: str_if_bytes(x["direction"]) == "to", res))
            )
            links_for = len(
                list(filter(lambda x: str_if_bytes(x["direction"]) == "from", res))
            )
            assert links_to == links_for
            for i in range(0, len(res) - 1, 2):
                assert res[i]["node"] == res[i + 1]["node"]
        else:
            links_to = len(list(filter(lambda x: x[b"direction"] == b"to", res)))
            links_for = len(list(filter(lambda x: x[b"direction"] == b"from", res)))
            assert links_to == links_for
            for i in range(0, len(res) - 1, 2):
                assert res[i][b"node"] == res[i + 1][b"node"]

    def test_cluster_flshslots_not_implemented(self, r):
        with pytest.raises(NotImplementedError):
            r.cluster_flushslots()

    def test_cluster_bumpepoch_not_implemented(self, r):
        with pytest.raises(NotImplementedError):
            r.cluster_bumpepoch()

    def test_info(self, r):
        # Map keys to same slot
        r.set("x{1}", 1)
        r.set("y{1}", 2)
        r.set("z{1}", 3)
        # Get node that handles the slot
        slot = r.keyslot("x{1}")
        node = r.nodes_manager.get_node_from_slot(slot)
        # Run info on that node
        info = r.info(target_nodes=node)
        assert isinstance(info, dict)
        assert info["db0"]["keys"] == 3

    def test_info_with_custom_callback(self, decoded_r):
        decoded_r.set_response_callback("INFO", lambda response, **options: response)
        info = decoded_r.info("server", target_nodes=decoded_r.get_random_node())
        assert "redis_version" in info

    def _init_slowlog_test(self, r, node):
        slowlog_lim = r.config_get("slowlog-log-slower-than", target_nodes=node)
        assert r.config_set("slowlog-log-slower-than", 0, target_nodes=node) is True
        return slowlog_lim["slowlog-log-slower-than"]

    def _teardown_slowlog_test(self, r, node, prev_limit):
        assert (
            r.config_set("slowlog-log-slower-than", prev_limit, target_nodes=node)
            is True
        )

    def test_slowlog_get(self, r, slowlog):
        unicode_string = chr(3456) + "abcd" + chr(3421)
        node = r.get_node_from_key(unicode_string)
        slowlog_limit = self._init_slowlog_test(r, node)
        assert r.slowlog_reset(target_nodes=node)
        r.get(unicode_string)
        slowlog = r.slowlog_get(target_nodes=node)
        assert isinstance(slowlog, list)
        commands = [log["command"] for log in slowlog]

        get_command = b" ".join((b"GET", unicode_string.encode("utf-8")))
        assert get_command in commands
        assert b"SLOWLOG RESET" in commands

        # the order should be ['GET <uni string>', 'SLOWLOG RESET'],
        # but if other clients are executing commands at the same time, there
        # could be commands, before, between, or after, so just check that
        # the two we care about are in the appropriate order.
        assert commands.index(get_command) < commands.index(b"SLOWLOG RESET")

        # make sure other attributes are typed correctly
        assert isinstance(slowlog[0]["start_time"], int)
        assert isinstance(slowlog[0]["duration"], int)
        # rollback the slowlog limit to its original value
        self._teardown_slowlog_test(r, node, slowlog_limit)

    def test_slowlog_get_limit(self, r, slowlog):
        assert r.slowlog_reset()
        node = r.get_node_from_key("foo")
        slowlog_limit = self._init_slowlog_test(r, node)
        r.get("foo")
        slowlog = r.slowlog_get(1, target_nodes=node)
        assert isinstance(slowlog, list)
        # only one command, based on the number we passed to slowlog_get()
        assert len(slowlog) == 1
        self._teardown_slowlog_test(r, node, slowlog_limit)

    def test_slowlog_length(self, r, slowlog):
        r.get("foo")
        node = r.nodes_manager.get_node_from_slot(key_slot(b"foo"))
        slowlog_len = r.slowlog_len(target_nodes=node)
        assert isinstance(slowlog_len, int)

    def test_time(self, r):
        t = r.time(target_nodes=r.get_primaries()[0])
        assert len(t) == 2
        assert isinstance(t[0], int)
        assert isinstance(t[1], int)

    @skip_if_server_version_lt("4.0.0")
    @skip_if_redis_enterprise()
    def test_memory_stats(self, r):
        # put a key into the current db to make sure that "db.<current-db>"
        # has data
        r.set("foo", "bar")
        node = r.nodes_manager.get_node_from_slot(key_slot(b"foo"))
        stats = r.memory_stats(target_nodes=node)
        assert isinstance(stats, dict)
        for key, value in stats.items():
            if key.startswith("db."):
                assert not isinstance(value, list)

    @skip_if_redis_enterprise()
    def test_lastsave(self, r):
        node = r.get_primaries()[0]
        assert isinstance(r.lastsave(target_nodes=node), datetime.datetime)

    def test_cluster_echo(self, r):
        node = r.get_primaries()[0]
        assert r.echo("foo bar", target_nodes=node) == b"foo bar"

    @skip_if_server_version_lt("1.0.0")
    def test_debug_segfault(self, r):
        with pytest.raises(NotImplementedError):
            r.debug_segfault()

    def test_config_resetstat(self, r):
        node = r.get_primaries()[0]
        r.ping(target_nodes=node)
        prior_commands_processed = int(
            r.info(target_nodes=node)["total_commands_processed"]
        )
        assert prior_commands_processed >= 1
        r.config_resetstat(target_nodes=node)
        reset_commands_processed = int(
            r.info(target_nodes=node)["total_commands_processed"]
        )
        assert reset_commands_processed < prior_commands_processed

    @skip_if_server_version_lt("6.2.0")
    def test_client_trackinginfo(self, r):
        node = r.get_primaries()[0]
        res = r.client_trackinginfo(target_nodes=node)
        assert len(res) > 2
        assert "prefixes" in res or b"prefixes" in res

    @skip_if_server_version_lt("6.0.0")
    @skip_if_redis_enterprise()
    def test_client_tracking(self, r):
        # simple case - will execute on all nodes
        assert r.client_tracking_on()
        assert r.client_tracking_off()

        # id based
        node = r.get_default_node()
        # when id is provided - the command should be sent to the node that
        # owns the connection with this id
        client_id = node.redis_connection.client_id()
        assert r.client_tracking_on(clientid=client_id, target_nodes=node)
        assert r.client_tracking_off(clientid=client_id, target_nodes=node)

        # execute with client id and prefixes and bcast
        assert r.client_tracking_on(
            clientid=client_id, prefix=["foo", "bar"], bcast=True, target_nodes=node
        )

        # now with some prefixes and without bcast
        with pytest.raises(DataError):
            assert r.client_tracking_on(prefix=["foo", "bar", "blee"])

    @skip_if_server_version_lt("2.9.50")
    def test_client_pause(self, r):
        node = r.get_primaries()[0]
        assert r.client_pause(1, target_nodes=node)
        assert r.client_pause(timeout=1, target_nodes=node)
        with pytest.raises(RedisError):
            r.client_pause(timeout="not an integer", target_nodes=node)

    @skip_if_server_version_lt("6.2.0")
    @skip_if_redis_enterprise()
    def test_client_unpause(self, r):
        assert r.client_unpause()

    @skip_if_server_version_lt("5.0.0")
    def test_client_id(self, r):
        node = r.get_primaries()[0]
        assert r.client_id(target_nodes=node) > 0

    @skip_if_server_version_lt("5.0.0")
    def test_client_unblock(self, r):
        node = r.get_primaries()[0]
        myid = r.client_id(target_nodes=node)
        assert not r.client_unblock(myid, target_nodes=node)
        assert not r.client_unblock(myid, error=True, target_nodes=node)
        assert not r.client_unblock(myid, error=False, target_nodes=node)

    @skip_if_server_version_lt("6.0.0")
    def test_client_getredir(self, r):
        node = r.get_primaries()[0]
        assert isinstance(r.client_getredir(target_nodes=node), int)
        assert r.client_getredir(target_nodes=node) == -1

    @skip_if_server_version_lt("6.2.0")
    def test_client_info(self, r):
        node = r.get_primaries()[0]
        info = r.client_info(target_nodes=node)
        assert isinstance(info, dict)
        assert "addr" in info

    @skip_if_server_version_lt("2.6.9")
    def test_client_kill(self, r, r2):
        node = r.get_primaries()[0]
        r.client_setname("redis-py-c1", target_nodes="all")
        r2.client_setname("redis-py-c2", target_nodes="all")
        clients = [
            client
            for client in r.client_list(target_nodes=node)
            if client.get("name") in ["redis-py-c1", "redis-py-c2"]
        ]
        assert len(clients) == 2
        clients_by_name = {client.get("name"): client for client in clients}

        client_addr = clients_by_name["redis-py-c2"].get("addr")
        assert r.client_kill(client_addr, target_nodes=node) is True

        clients = [
            client
            for client in r.client_list(target_nodes=node)
            if client.get("name") in ["redis-py-c1", "redis-py-c2"]
        ]
        assert len(clients) == 1
        assert clients[0].get("name") == "redis-py-c1"

    @skip_if_server_version_lt("2.6.0")
    def test_cluster_bitop_not_empty_string(self, r):
        r["{foo}a"] = ""
        r.bitop("not", "{foo}r", "{foo}a")
        assert r.get("{foo}r") is None

    @skip_if_server_version_lt("2.6.0")
    def test_cluster_bitop_not(self, r):
        test_str = b"\xaa\x00\xff\x55"
        correct = ~0xAA00FF55 & 0xFFFFFFFF
        r["{foo}a"] = test_str
        r.bitop("not", "{foo}r", "{foo}a")
        assert int(binascii.hexlify(r["{foo}r"]), 16) == correct

    @skip_if_server_version_lt("2.6.0")
    def test_cluster_bitop_not_in_place(self, r):
        test_str = b"\xaa\x00\xff\x55"
        correct = ~0xAA00FF55 & 0xFFFFFFFF
        r["{foo}a"] = test_str
        r.bitop("not", "{foo}a", "{foo}a")
        assert int(binascii.hexlify(r["{foo}a"]), 16) == correct

    @skip_if_server_version_lt("2.6.0")
    def test_cluster_bitop_single_string(self, r):
        test_str = b"\x01\x02\xff"
        r["{foo}a"] = test_str
        r.bitop("and", "{foo}res1", "{foo}a")
        r.bitop("or", "{foo}res2", "{foo}a")
        r.bitop("xor", "{foo}res3", "{foo}a")
        assert r["{foo}res1"] == test_str
        assert r["{foo}res2"] == test_str
        assert r["{foo}res3"] == test_str

    @skip_if_server_version_lt("2.6.0")
    def test_cluster_bitop_string_operands(self, r):
        r["{foo}a"] = b"\x01\x02\xff\xff"
        r["{foo}b"] = b"\x01\x02\xff"
        r.bitop("and", "{foo}res1", "{foo}a", "{foo}b")
        r.bitop("or", "{foo}res2", "{foo}a", "{foo}b")
        r.bitop("xor", "{foo}res3", "{foo}a", "{foo}b")
        assert int(binascii.hexlify(r["{foo}res1"]), 16) == 0x0102FF00
        assert int(binascii.hexlify(r["{foo}res2"]), 16) == 0x0102FFFF
        assert int(binascii.hexlify(r["{foo}res3"]), 16) == 0x000000FF

    @skip_if_server_version_lt("6.2.0")
    def test_cluster_copy(self, r):
        assert r.copy("{foo}a", "{foo}b") == 0
        r.set("{foo}a", "bar")
        assert r.copy("{foo}a", "{foo}b") == 1
        assert r.get("{foo}a") == b"bar"
        assert r.get("{foo}b") == b"bar"

    @skip_if_server_version_lt("6.2.0")
    def test_cluster_copy_and_replace(self, r):
        r.set("{foo}a", "foo1")
        r.set("{foo}b", "foo2")
        assert r.copy("{foo}a", "{foo}b") == 0
        assert r.copy("{foo}a", "{foo}b", replace=True) == 1

    @skip_if_server_version_lt("6.2.0")
    def test_cluster_lmove(self, r):
        r.rpush("{foo}a", "one", "two", "three", "four")
        assert r.lmove("{foo}a", "{foo}b")
        assert r.lmove("{foo}a", "{foo}b", "right", "left")

    @skip_if_server_version_lt("6.2.0")
    def test_cluster_blmove(self, r):
        r.rpush("{foo}a", "one", "two", "three", "four")
        assert r.blmove("{foo}a", "{foo}b", 5)
        assert r.blmove("{foo}a", "{foo}b", 1, "RIGHT", "LEFT")

    @skip_if_server_version_lt("8.9.0")
    def test_cluster_lmovem(self, r):
        # source and destination collocated on the same slot via a hash tag
        r.rpush("{foo}a", "1", "2", "3", "4", "5")
        assert r.lmovem("{foo}a", "{foo}b") == [b"1"]
        assert r.lmovem(
            "{foo}a", "{foo}b", "LEFT", "LEFT", count=3, ordering="BULK"
        ) == [
            b"2",
            b"3",
            b"4",
        ]

    @skip_if_server_version_lt("8.9.0")
    def test_cluster_lmovem_crossslot(self, r):
        # source and destination on different slots -> cannot be dispatched
        with pytest.raises(RedisClusterException) as ex:
            r.lmovem("a", "b", "LEFT", "RIGHT", count=2, ordering="BULK")
        assert "all keys must map to the same key slot" in str(ex.value)

    @skip_if_server_version_lt("8.9.0")
    def test_cluster_blmovem(self, r):
        r.rpush("{foo}a", "1", "2", "3", "4", "5")
        assert r.blmovem("{foo}a", "{foo}b", 1) == [b"1"]
        assert r.blmovem(
            "{foo}a", "{foo}b", 1, "LEFT", "LEFT", count=3, ordering="BULK"
        ) == [b"2", b"3", b"4"]

    @skip_if_server_version_lt("8.9.0")
    def test_cluster_blmovem_crossslot(self, r):
        with pytest.raises(RedisClusterException) as ex:
            r.blmovem("a", "b", 1, "LEFT", "RIGHT", count=2, ordering="BULK")
        assert "all keys must map to the same key slot" in str(ex.value)

    def test_cluster_msetnx(self, r):
        d = {"{foo}a": b"1", "{foo}b": b"2", "{foo}c": b"3"}
        assert r.msetnx(d)
        d2 = {"{foo}a": b"x", "{foo}d": b"4"}
        assert not r.msetnx(d2)
        for k, v in d.items():
            assert r[k] == v
        assert r.get("{foo}d") is None

    def test_cluster_rename(self, r):
        r["{foo}a"] = "1"
        assert r.rename("{foo}a", "{foo}b")
        assert r.get("{foo}a") is None
        assert r["{foo}b"] == b"1"

    def test_cluster_renamenx(self, r):
        r["{foo}a"] = "1"
        r["{foo}b"] = "2"
        assert not r.renamenx("{foo}a", "{foo}b")
        assert r["{foo}a"] == b"1"
        assert r["{foo}b"] == b"2"

    # LIST COMMANDS
    def test_cluster_blpop(self, r):
        r.rpush("{foo}a", "1", "2")
        r.rpush("{foo}b", "3", "4")
        assert_resp_response(
            r,
            r.blpop(["{foo}b", "{foo}a"], timeout=1),
            (b"{foo}b", b"3"),
            [b"{foo}b", b"3"],
        )
        assert_resp_response(
            r,
            r.blpop(["{foo}b", "{foo}a"], timeout=1),
            (b"{foo}b", b"4"),
            [b"{foo}b", b"4"],
        )
        assert_resp_response(
            r,
            r.blpop(["{foo}b", "{foo}a"], timeout=1),
            (b"{foo}a", b"1"),
            [b"{foo}a", b"1"],
        )
        assert_resp_response(
            r,
            r.blpop(["{foo}b", "{foo}a"], timeout=1),
            (b"{foo}a", b"2"),
            [b"{foo}a", b"2"],
        )
        assert r.blpop(["{foo}b", "{foo}a"], timeout=0.1) is None
        r.rpush("{foo}c", "1")
        assert_resp_response(
            r, r.blpop("{foo}c", timeout=1), (b"{foo}c", b"1"), [b"{foo}c", b"1"]
        )

    def test_cluster_brpop(self, r):
        r.rpush("{foo}a", "1", "2")
        r.rpush("{foo}b", "3", "4")
        assert_resp_response(
            r,
            r.brpop(["{foo}b", "{foo}a"], timeout=1),
            (b"{foo}b", b"4"),
            [b"{foo}b", b"4"],
        )
        assert_resp_response(
            r,
            r.brpop(["{foo}b", "{foo}a"], timeout=1),
            (b"{foo}b", b"3"),
            [b"{foo}b", b"3"],
        )
        assert_resp_response(
            r,
            r.brpop(["{foo}b", "{foo}a"], timeout=1),
            (b"{foo}a", b"2"),
            [b"{foo}a", b"2"],
        )
        assert_resp_response(
            r,
            r.brpop(["{foo}b", "{foo}a"], timeout=1),
            (b"{foo}a", b"1"),
            [b"{foo}a", b"1"],
        )
        assert r.brpop(["{foo}b", "{foo}a"], timeout=0.1) is None
        r.rpush("{foo}c", "1")
        assert_resp_response(
            r, r.brpop("{foo}c", timeout=1), (b"{foo}c", b"1"), [b"{foo}c", b"1"]
        )

    def test_cluster_brpoplpush(self, r):
        r.rpush("{foo}a", "1", "2")
        r.rpush("{foo}b", "3", "4")
        assert r.brpoplpush("{foo}a", "{foo}b") == b"2"
        assert r.brpoplpush("{foo}a", "{foo}b") == b"1"
        assert r.brpoplpush("{foo}a", "{foo}b", timeout=0.1) is None
        assert r.lrange("{foo}a", 0, -1) == []
        assert r.lrange("{foo}b", 0, -1) == [b"1", b"2", b"3", b"4"]

    def test_cluster_brpoplpush_empty_string(self, r):
        r.rpush("{foo}a", "")
        assert r.brpoplpush("{foo}a", "{foo}b") == b""

    def test_cluster_rpoplpush(self, r):
        r.rpush("{foo}a", "a1", "a2", "a3")
        r.rpush("{foo}b", "b1", "b2", "b3")
        assert r.rpoplpush("{foo}a", "{foo}b") == b"a3"
        assert r.lrange("{foo}a", 0, -1) == [b"a1", b"a2"]
        assert r.lrange("{foo}b", 0, -1) == [b"a3", b"b1", b"b2", b"b3"]

    def test_cluster_sdiff(self, r):
        r.sadd("{foo}a", "1", "2", "3")
        assert r.sdiff("{foo}a", "{foo}b") == {b"1", b"2", b"3"}
        r.sadd("{foo}b", "2", "3")
        assert r.sdiff("{foo}a", "{foo}b") == {b"1"}

    def test_cluster_sdiffstore(self, r):
        r.sadd("{foo}a", "1", "2", "3")
        assert r.sdiffstore("{foo}c", "{foo}a", "{foo}b") == 3
        assert r.smembers("{foo}c") == {b"1", b"2", b"3"}
        r.sadd("{foo}b", "2", "3")
        assert r.sdiffstore("{foo}c", "{foo}a", "{foo}b") == 1
        assert r.smembers("{foo}c") == {b"1"}

    def test_cluster_sinter(self, r):
        r.sadd("{foo}a", "1", "2", "3")
        assert r.sinter("{foo}a", "{foo}b") == set()
        r.sadd("{foo}b", "2", "3")
        assert r.sinter("{foo}a", "{foo}b") == {b"2", b"3"}

    def test_cluster_sinterstore(self, r):
        r.sadd("{foo}a", "1", "2", "3")
        assert r.sinterstore("{foo}c", "{foo}a", "{foo}b") == 0
        assert r.smembers("{foo}c") == set()
        r.sadd("{foo}b", "2", "3")
        assert r.sinterstore("{foo}c", "{foo}a", "{foo}b") == 2
        assert r.smembers("{foo}c") == {b"2", b"3"}

    def test_cluster_smove(self, r):
        r.sadd("{foo}a", "a1", "a2")
        r.sadd("{foo}b", "b1", "b2")
        assert r.smove("{foo}a", "{foo}b", "a1")
        assert r.smembers("{foo}a") == {b"a2"}
        assert r.smembers("{foo}b") == {b"b1", b"b2", b"a1"}

    def test_cluster_sunion(self, r):
        r.sadd("{foo}a", "1", "2")
        r.sadd("{foo}b", "2", "3")
        assert r.sunion("{foo}a", "{foo}b") == {b"1", b"2", b"3"}

    def test_cluster_sunionstore(self, r):
        r.sadd("{foo}a", "1", "2")
        r.sadd("{foo}b", "2", "3")
        assert r.sunionstore("{foo}c", "{foo}a", "{foo}b") == 3
        assert r.smembers("{foo}c") == {b"1", b"2", b"3"}

    @skip_if_server_version_lt("6.2.0")
    def test_cluster_zdiff(self, r):
        r.zadd("{foo}a", {"a1": 1, "a2": 2, "a3": 3})
        r.zadd("{foo}b", {"a1": 1, "a2": 2})
        assert r.zdiff(["{foo}a", "{foo}b"]) == [b"a3"]
        response = r.zdiff(["{foo}a", "{foo}b"], withscores=True)
        assert_resp_response(
            r,
            response,
            [b"a3", b"3"],
            [[b"a3", 3.0]],
        )

    @skip_if_server_version_lt("6.2.0")
    def test_cluster_zdiffstore(self, r):
        r.zadd("{foo}a", {"a1": 1, "a2": 2, "a3": 3})
        r.zadd("{foo}b", {"a1": 1, "a2": 2})
        assert r.zdiffstore("{foo}out", ["{foo}a", "{foo}b"])
        assert r.zrange("{foo}out", 0, -1) == [b"a3"]
        response = r.zrange("{foo}out", 0, -1, withscores=True)
        assert_resp_response(r, response, [(b"a3", 3.0)], [[b"a3", 3.0]])

    @skip_if_server_version_lt("6.2.0")
    def test_cluster_zinter(self, r):
        r.zadd("{foo}a", {"a1": 1, "a2": 2, "a3": 1})
        r.zadd("{foo}b", {"a1": 2, "a2": 2, "a3": 2})
        r.zadd("{foo}c", {"a1": 6, "a3": 5, "a4": 4})
        assert r.zinter(["{foo}a", "{foo}b", "{foo}c"]) == [b"a3", b"a1"]
        # invalid aggregation
        with pytest.raises(DataError):
            r.zinter(["{foo}a", "{foo}b", "{foo}c"], aggregate="foo", withscores=True)
        assert_resp_response(
            r,
            r.zinter(["{foo}a", "{foo}b", "{foo}c"], withscores=True),
            [(b"a3", 8), (b"a1", 9)],
            [[b"a3", 8], [b"a1", 9]],
        )
        assert_resp_response(
            r,
            r.zinter(["{foo}a", "{foo}b", "{foo}c"], withscores=True, aggregate="MAX"),
            [(b"a3", 5), (b"a1", 6)],
            [[b"a3", 5], [b"a1", 6]],
        )
        assert_resp_response(
            r,
            r.zinter(["{foo}a", "{foo}b", "{foo}c"], withscores=True, aggregate="MIN"),
            [(b"a1", 1), (b"a3", 1)],
            [[b"a1", 1], [b"a3", 1]],
        )
        assert_resp_response(
            r,
            r.zinter({"{foo}a": 1, "{foo}b": 2, "{foo}c": 3}, withscores=True),
            [(b"a3", 20.0), (b"a1", 23.0)],
            [[b"a3", 20.0], [b"a1", 23.0]],
        )

    def test_cluster_zinterstore_sum(self, r):
        r.zadd("{foo}a", {"a1": 1, "a2": 1, "a3": 1})
        r.zadd("{foo}b", {"a1": 2, "a2": 2, "a3": 2})
        r.zadd("{foo}c", {"a1": 6, "a3": 5, "a4": 4})
        assert r.zinterstore("{foo}d", ["{foo}a", "{foo}b", "{foo}c"]) == 2
        assert_resp_response(
            r,
            r.zrange("{foo}d", 0, -1, withscores=True),
            [(b"a3", 8), (b"a1", 9)],
            [[b"a3", 8.0], [b"a1", 9.0]],
        )

    def test_cluster_zinterstore_max(self, r):
        r.zadd("{foo}a", {"a1": 1, "a2": 1, "a3": 1})
        r.zadd("{foo}b", {"a1": 2, "a2": 2, "a3": 2})
        r.zadd("{foo}c", {"a1": 6, "a3": 5, "a4": 4})
        assert (
            r.zinterstore("{foo}d", ["{foo}a", "{foo}b", "{foo}c"], aggregate="MAX")
            == 2
        )
        assert_resp_response(
            r,
            r.zrange("{foo}d", 0, -1, withscores=True),
            [(b"a3", 5), (b"a1", 6)],
            [[b"a3", 5.0], [b"a1", 6.0]],
        )

    def test_cluster_zinterstore_min(self, r):
        r.zadd("{foo}a", {"a1": 1, "a2": 2, "a3": 3})
        r.zadd("{foo}b", {"a1": 2, "a2": 3, "a3": 5})
        r.zadd("{foo}c", {"a1": 6, "a3": 5, "a4": 4})
        assert (
            r.zinterstore("{foo}d", ["{foo}a", "{foo}b", "{foo}c"], aggregate="MIN")
            == 2
        )
        assert_resp_response(
            r,
            r.zrange("{foo}d", 0, -1, withscores=True),
            [(b"a1", 1), (b"a3", 3)],
            [[b"a1", 1.0], [b"a3", 3.0]],
        )

    def test_cluster_zinterstore_with_weight(self, r):
        r.zadd("{foo}a", {"a1": 1, "a2": 1, "a3": 1})
        r.zadd("{foo}b", {"a1": 2, "a2": 2, "a3": 2})
        r.zadd("{foo}c", {"a1": 6, "a3": 5, "a4": 4})
        assert r.zinterstore("{foo}d", {"{foo}a": 1, "{foo}b": 2, "{foo}c": 3}) == 2
        assert_resp_response(
            r,
            r.zrange("{foo}d", 0, -1, withscores=True),
            [(b"a3", 20), (b"a1", 23)],
            [[b"a3", 20.0], [b"a1", 23.0]],
        )

    @skip_if_server_version_lt("8.7.0")
    def test_cluster_zinterstore_count(self, r):
        r.zadd("{foo}a", {"a1": 1, "a2": 1, "a3": 1})
        r.zadd("{foo}b", {"a1": 2, "a2": 2, "a3": 2})
        r.zadd("{foo}c", {"a1": 6, "a3": 5, "a4": 4})
        assert (
            r.zinterstore("{foo}d", ["{foo}a", "{foo}b", "{foo}c"], aggregate="COUNT")
            == 2
        )
        assert_resp_response(
            r,
            r.zrange("{foo}d", 0, -1, withscores=True),
            [(b"a1", 3.0), (b"a3", 3.0)],
            [[b"a1", 3.0], [b"a3", 3.0]],
        )

    @skip_if_server_version_lt("8.7.0")
    def test_cluster_zinterstore_count_with_weight(self, r):
        r.zadd("{foo}a", {"a1": 1, "a2": 1, "a3": 1})
        r.zadd("{foo}b", {"a1": 2, "a2": 2, "a3": 2})
        r.zadd("{foo}c", {"a1": 6, "a3": 5, "a4": 4})
        assert (
            r.zinterstore(
                "{foo}d",
                {"{foo}a": 1, "{foo}b": 2, "{foo}c": 3},
                aggregate="COUNT",
            )
            == 2
        )
        assert_resp_response(
            r,
            r.zrange("{foo}d", 0, -1, withscores=True),
            [(b"a1", 6.0), (b"a3", 6.0)],
            [[b"a1", 6.0], [b"a3", 6.0]],
        )

    @skip_if_server_version_lt("4.9.0")
    def test_cluster_bzpopmax(self, r):
        r.zadd("{foo}a", {"a1": 1, "a2": 2})
        r.zadd("{foo}b", {"b1": 10, "b2": 20})
        assert_resp_response(
            r,
            r.bzpopmax(["{foo}b", "{foo}a"], timeout=1),
            (b"{foo}b", b"b2", 20),
            [b"{foo}b", b"b2", 20],
        )
        assert_resp_response(
            r,
            r.bzpopmax(["{foo}b", "{foo}a"], timeout=1),
            (b"{foo}b", b"b1", 10),
            [b"{foo}b", b"b1", 10],
        )
        assert_resp_response(
            r,
            r.bzpopmax(["{foo}b", "{foo}a"], timeout=1),
            (b"{foo}a", b"a2", 2),
            [b"{foo}a", b"a2", 2],
        )
        assert_resp_response(
            r,
            r.bzpopmax(["{foo}b", "{foo}a"], timeout=1),
            (b"{foo}a", b"a1", 1),
            [b"{foo}a", b"a1", 1],
        )
        assert r.bzpopmax(["{foo}b", "{foo}a"], timeout=0.1) is None
        r.zadd("{foo}c", {"c1": 100})
        assert_resp_response(
            r,
            r.bzpopmax("{foo}c", timeout=1),
            (b"{foo}c", b"c1", 100),
            [b"{foo}c", b"c1", 100],
        )

    @skip_if_server_version_lt("4.9.0")
    def test_cluster_bzpopmin(self, r):
        r.zadd("{foo}a", {"a1": 1, "a2": 2})
        r.zadd("{foo}b", {"b1": 10, "b2": 20})
        assert_resp_response(
            r,
            r.bzpopmin(["{foo}b", "{foo}a"], timeout=1),
            (b"{foo}b", b"b1", 10),
            [b"{foo}b", b"b1", 10],
        )
        assert_resp_response(
            r,
            r.bzpopmin(["{foo}b", "{foo}a"], timeout=1),
            (b"{foo}b", b"b2", 20),
            [b"{foo}b", b"b2", 20],
        )
        assert_resp_response(
            r,
            r.bzpopmin(["{foo}b", "{foo}a"], timeout=1),
            (b"{foo}a", b"a1", 1),
            [b"{foo}a", b"a1", 1],
        )
        assert_resp_response(
            r,
            r.bzpopmin(["{foo}b", "{foo}a"], timeout=1),
            (b"{foo}a", b"a2", 2),
            [b"{foo}a", b"a2", 2],
        )
        assert r.bzpopmin(["{foo}b", "{foo}a"], timeout=0.1) is None
        r.zadd("{foo}c", {"c1": 100})
        assert_resp_response(
            r,
            r.bzpopmin("{foo}c", timeout=1),
            (b"{foo}c", b"c1", 100),
            [b"{foo}c", b"c1", 100],
        )

    @skip_if_server_version_lt("6.2.0")
    def test_cluster_zrangestore(self, r):
        r.zadd("{foo}a", {"a1": 1, "a2": 2, "a3": 3})
        assert r.zrangestore("{foo}b", "{foo}a", 0, 1)
        assert r.zrange("{foo}b", 0, -1) == [b"a1", b"a2"]
        assert r.zrangestore("{foo}b", "{foo}a", 1, 2)
        assert r.zrange("{foo}b", 0, -1) == [b"a2", b"a3"]
        assert_resp_response(
            r,
            r.zrange("{foo}b", 0, 1, withscores=True),
            [(b"a2", 2), (b"a3", 3)],
            [[b"a2", 2.0], [b"a3", 3.0]],
        )
        # reversed order
        assert r.zrangestore("{foo}b", "{foo}a", 1, 2, desc=True)
        assert r.zrange("{foo}b", 0, -1) == [b"a1", b"a2"]
        # by score
        assert r.zrangestore(
            "{foo}b", "{foo}a", 2, 1, byscore=True, offset=0, num=1, desc=True
        )
        assert r.zrange("{foo}b", 0, -1) == [b"a2"]
        # by lex
        assert r.zrangestore(
            "{foo}b", "{foo}a", "[a2", "(a3", bylex=True, offset=0, num=1
        )
        assert r.zrange("{foo}b", 0, -1) == [b"a2"]

    @skip_if_server_version_lt("6.2.0")
    def test_cluster_zunion(self, r):
        r.zadd("{foo}a", {"a1": 1, "a2": 1, "a3": 1})
        r.zadd("{foo}b", {"a1": 2, "a2": 2, "a3": 2})
        r.zadd("{foo}c", {"a1": 6, "a3": 5, "a4": 4})
        # sum
        assert r.zunion(["{foo}a", "{foo}b", "{foo}c"]) == [b"a2", b"a4", b"a3", b"a1"]
        assert_resp_response(
            r,
            r.zunion(["{foo}a", "{foo}b", "{foo}c"], withscores=True),
            [(b"a2", 3), (b"a4", 4), (b"a3", 8), (b"a1", 9)],
            [[b"a2", 3.0], [b"a4", 4.0], [b"a3", 8.0], [b"a1", 9.0]],
        )
        # max
        assert_resp_response(
            r,
            r.zunion(["{foo}a", "{foo}b", "{foo}c"], aggregate="MAX", withscores=True),
            [(b"a2", 2), (b"a4", 4), (b"a3", 5), (b"a1", 6)],
            [[b"a2", 2.0], [b"a4", 4.0], [b"a3", 5.0], [b"a1", 6.0]],
        )
        # min
        assert_resp_response(
            r,
            r.zunion(["{foo}a", "{foo}b", "{foo}c"], aggregate="MIN", withscores=True),
            [(b"a1", 1), (b"a2", 1), (b"a3", 1), (b"a4", 4)],
            [[b"a1", 1.0], [b"a2", 1.0], [b"a3", 1.0], [b"a4", 4.0]],
        )
        # with weight
        assert_resp_response(
            r,
            r.zunion({"{foo}a": 1, "{foo}b": 2, "{foo}c": 3}, withscores=True),
            [(b"a2", 5), (b"a4", 12), (b"a3", 20), (b"a1", 23)],
            [[b"a2", 5.0], [b"a4", 12.0], [b"a3", 20.0], [b"a1", 23.0]],
        )

    def test_cluster_zunionstore_sum(self, r):
        r.zadd("{foo}a", {"a1": 1, "a2": 1, "a3": 1})
        r.zadd("{foo}b", {"a1": 2, "a2": 2, "a3": 2})
        r.zadd("{foo}c", {"a1": 6, "a3": 5, "a4": 4})
        assert r.zunionstore("{foo}d", ["{foo}a", "{foo}b", "{foo}c"]) == 4
        assert_resp_response(
            r,
            r.zrange("{foo}d", 0, -1, withscores=True),
            [(b"a2", 3), (b"a4", 4), (b"a3", 8), (b"a1", 9)],
            [[b"a2", 3.0], [b"a4", 4.0], [b"a3", 8.0], [b"a1", 9.0]],
        )

    def test_cluster_zunionstore_max(self, r):
        r.zadd("{foo}a", {"a1": 1, "a2": 1, "a3": 1})
        r.zadd("{foo}b", {"a1": 2, "a2": 2, "a3": 2})
        r.zadd("{foo}c", {"a1": 6, "a3": 5, "a4": 4})
        assert (
            r.zunionstore("{foo}d", ["{foo}a", "{foo}b", "{foo}c"], aggregate="MAX")
            == 4
        )
        assert_resp_response(
            r,
            r.zrange("{foo}d", 0, -1, withscores=True),
            [(b"a2", 2), (b"a4", 4), (b"a3", 5), (b"a1", 6)],
            [[b"a2", 2.0], [b"a4", 4.0], [b"a3", 5.0], [b"a1", 6.0]],
        )

    def test_cluster_zunionstore_min(self, r):
        r.zadd("{foo}a", {"a1": 1, "a2": 2, "a3": 3})
        r.zadd("{foo}b", {"a1": 2, "a2": 2, "a3": 4})
        r.zadd("{foo}c", {"a1": 6, "a3": 5, "a4": 4})
        assert (
            r.zunionstore("{foo}d", ["{foo}a", "{foo}b", "{foo}c"], aggregate="MIN")
            == 4
        )
        assert_resp_response(
            r,
            r.zrange("{foo}d", 0, -1, withscores=True),
            [(b"a1", 1), (b"a2", 2), (b"a3", 3), (b"a4", 4)],
            [[b"a1", 1.0], [b"a2", 2.0], [b"a3", 3.0], [b"a4", 4.0]],
        )

    def test_cluster_zunionstore_with_weight(self, r):
        r.zadd("{foo}a", {"a1": 1, "a2": 1, "a3": 1})
        r.zadd("{foo}b", {"a1": 2, "a2": 2, "a3": 2})
        r.zadd("{foo}c", {"a1": 6, "a3": 5, "a4": 4})
        assert r.zunionstore("{foo}d", {"{foo}a": 1, "{foo}b": 2, "{foo}c": 3}) == 4
        assert_resp_response(
            r,
            r.zrange("{foo}d", 0, -1, withscores=True),
            [(b"a2", 5), (b"a4", 12), (b"a3", 20), (b"a1", 23)],
            [[b"a2", 5.0], [b"a4", 12.0], [b"a3", 20.0], [b"a1", 23.0]],
        )

    @skip_if_server_version_lt("8.7.0")
    def test_cluster_zunionstore_count(self, r):
        r.zadd("{foo}a", {"a1": 1, "a2": 1, "a3": 1})
        r.zadd("{foo}b", {"a1": 2, "a2": 2, "a3": 2})
        r.zadd("{foo}c", {"a1": 6, "a3": 5, "a4": 4})
        assert (
            r.zunionstore("{foo}d", ["{foo}a", "{foo}b", "{foo}c"], aggregate="COUNT")
            == 4
        )
        assert_resp_response(
            r,
            r.zrange("{foo}d", 0, -1, withscores=True),
            [(b"a4", 1.0), (b"a2", 2.0), (b"a1", 3.0), (b"a3", 3.0)],
            [[b"a4", 1.0], [b"a2", 2.0], [b"a1", 3.0], [b"a3", 3.0]],
        )

    @skip_if_server_version_lt("8.7.0")
    def test_cluster_zunionstore_count_with_weight(self, r):
        r.zadd("{foo}a", {"a1": 1, "a2": 1, "a3": 1})
        r.zadd("{foo}b", {"a1": 2, "a2": 2, "a3": 2})
        r.zadd("{foo}c", {"a1": 6, "a3": 5, "a4": 4})
        assert (
            r.zunionstore(
                "{foo}d",
                {"{foo}a": 1, "{foo}b": 2, "{foo}c": 3},
                aggregate="COUNT",
            )
            == 4
        )
        assert_resp_response(
            r,
            r.zrange("{foo}d", 0, -1, withscores=True),
            [(b"a2", 3.0), (b"a4", 3.0), (b"a1", 6.0), (b"a3", 6.0)],
            [[b"a2", 3.0], [b"a4", 3.0], [b"a1", 6.0], [b"a3", 6.0]],
        )

    @skip_if_server_version_lt("2.8.9")
    def test_cluster_pfcount(self, r):
        members = {b"1", b"2", b"3"}
        r.pfadd("{foo}a", *members)
        assert r.pfcount("{foo}a") == len(members)
        members_b = {b"2", b"3", b"4"}
        r.pfadd("{foo}b", *members_b)
        assert r.pfcount("{foo}b") == len(members_b)
        assert r.pfcount("{foo}a", "{foo}b") == len(members_b.union(members))

    @skip_if_server_version_lt("2.8.9")
    def test_cluster_pfmerge(self, r):
        mema = {b"1", b"2", b"3"}
        memb = {b"2", b"3", b"4"}
        memc = {b"5", b"6", b"7"}
        r.pfadd("{foo}a", *mema)
        r.pfadd("{foo}b", *memb)
        r.pfadd("{foo}c", *memc)
        r.pfmerge("{foo}d", "{foo}c", "{foo}a")
        assert r.pfcount("{foo}d") == 6
        r.pfmerge("{foo}d", "{foo}b")
        assert r.pfcount("{foo}d") == 7

    def test_cluster_sort_store(self, r):
        r.rpush("{foo}a", "2", "3", "1")
        assert r.sort("{foo}a", store="{foo}sorted_values") == 3
        assert r.lrange("{foo}sorted_values", 0, -1) == [b"1", b"2", b"3"]

    # GEO COMMANDS
    @skip_if_server_version_lt("6.2.0")
    def test_cluster_geosearchstore(self, r):
        values = (2.1909389952632, 41.433791470673, "place1") + (
            2.1873744593677,
            41.406342043777,
            "place2",
        )

        r.geoadd("{foo}barcelona", values)
        r.geosearchstore(
            "{foo}places_barcelona",
            "{foo}barcelona",
            longitude=2.191,
            latitude=41.433,
            radius=1000,
        )
        assert r.zrange("{foo}places_barcelona", 0, -1) == [b"place1"]

    @skip_unless_arch_bits(64)
    @skip_if_server_version_lt("6.2.0")
    def test_geosearchstore_dist(self, r):
        values = (2.1909389952632, 41.433791470673, "place1") + (
            2.1873744593677,
            41.406342043777,
            "place2",
        )

        r.geoadd("{foo}barcelona", values)
        r.geosearchstore(
            "{foo}places_barcelona",
            "{foo}barcelona",
            longitude=2.191,
            latitude=41.433,
            radius=1000,
            storedist=True,
        )
        # instead of save the geo score, the distance is saved.
        assert r.zscore("{foo}places_barcelona", "place1") == 88.05060698409301

    @skip_if_server_version_lt("3.2.0")
    def test_cluster_georadius_store(self, r):
        values = (2.1909389952632, 41.433791470673, "place1") + (
            2.1873744593677,
            41.406342043777,
            "place2",
        )

        r.geoadd("{foo}barcelona", values)
        r.georadius(
            "{foo}barcelona", 2.191, 41.433, 1000, store="{foo}places_barcelona"
        )
        assert r.zrange("{foo}places_barcelona", 0, -1) == [b"place1"]

    @skip_unless_arch_bits(64)
    @skip_if_server_version_lt("3.2.0")
    def test_cluster_georadius_store_dist(self, r):
        values = (2.1909389952632, 41.433791470673, "place1") + (
            2.1873744593677,
            41.406342043777,
            "place2",
        )

        r.geoadd("{foo}barcelona", values)
        r.georadius(
            "{foo}barcelona", 2.191, 41.433, 1000, store_dist="{foo}places_barcelona"
        )
        # instead of save the geo score, the distance is saved.
        assert r.zscore("{foo}places_barcelona", "place1") == 88.05060698409301

    def test_cluster_dbsize(self, r):
        d = {"a": b"1", "b": b"2", "c": b"3", "d": b"4"}
        assert r.mset_nonatomic(d)
        assert r.dbsize(target_nodes="primaries") == len(d)

    def test_cluster_keys(self, r):
        assert r.keys() == []
        keys_with_underscores = {b"test_a", b"test_b"}
        keys = keys_with_underscores.union({b"testc"})
        for key in keys:
            r[key] = 1
        assert (
            set(r.keys(pattern="test_*", target_nodes="primaries"))
            == keys_with_underscores
        )
        assert set(r.keys(pattern="test*", target_nodes="primaries")) == keys

    # SCAN COMMANDS
    @skip_if_server_version_lt("2.8.0")
    def test_cluster_scan(self, r):
        r.set("a", 1)
        r.set("b", 2)
        r.set("c", 3)

        for target_nodes, nodes in zip(
            ["primaries", "replicas"], [r.get_primaries(), r.get_replicas()]
        ):
            cursors, keys = r.scan(target_nodes=target_nodes)
            assert sorted(keys) == [b"a", b"b", b"c"]
            assert sorted(cursors.keys()) == sorted(node.name for node in nodes)
            assert all(cursor == 0 for cursor in cursors.values())

            cursors, keys = r.scan(match="a*", target_nodes=target_nodes)
            assert sorted(keys) == [b"a"]
            assert sorted(cursors.keys()) == sorted(node.name for node in nodes)
            assert all(cursor == 0 for cursor in cursors.values())

    @skip_if_server_version_lt("6.0.0")
    def test_cluster_scan_type(self, r):
        r.sadd("a-set", 1)
        r.sadd("b-set", 1)
        r.sadd("c-set", 1)
        r.hset("a-hash", "foo", 2)
        r.lpush("a-list", "aux", 3)

        for target_nodes, nodes in zip(
            ["primaries", "replicas"], [r.get_primaries(), r.get_replicas()]
        ):
            cursors, keys = r.scan(_type="SET", target_nodes=target_nodes)
            assert sorted(keys) == [b"a-set", b"b-set", b"c-set"]
            assert sorted(cursors.keys()) == sorted(node.name for node in nodes)
            assert all(cursor == 0 for cursor in cursors.values())

            cursors, keys = r.scan(_type="SET", match="a*", target_nodes=target_nodes)
            assert sorted(keys) == [b"a-set"]
            assert sorted(cursors.keys()) == sorted(node.name for node in nodes)
            assert all(cursor == 0 for cursor in cursors.values())

    @skip_if_server_version_lt("2.8.0")
    def test_cluster_scan_iter(self, r):
        keys_all = []
        keys_1 = []
        # one MSET per slot, pipelined per node, instead of 100 SETs
        r.mset_nonatomic({str(i): 1 for i in range(100)})
        for i in range(100):
            s = str(i)
            keys_all.append(s.encode("utf-8"))
            if s.startswith("1"):
                keys_1.append(s.encode("utf-8"))
        keys_all.sort()
        keys_1.sort()

        # the replicas are scanned below; poll until they have every key
        deadline = monotonic() + 5
        while sorted(r.scan_iter(target_nodes="replicas")) != keys_all:
            assert monotonic() < deadline, "replicas lag behind"
            sleep(0.01)

        for target_nodes in ["primaries", "replicas"]:
            keys = r.scan_iter(target_nodes=target_nodes)
            assert sorted(keys) == keys_all

            keys = r.scan_iter(match="1*", target_nodes=target_nodes)
            assert sorted(keys) == keys_1

    @skip_if_server_version_lt("8.11.0")
    def test_cluster_bless_scan(self, r):
        r.set("a", 1)
        r.set("b", 2)
        r.set("c", 3)
        assert r.bless_set("a", "NO-EVICT") == 1
        assert r.bless_set("b", "NO-EVICT") == 1
        assert r.bless_get("a") == [b"NO-EVICT"]
        assert r.bless_get("c") == []

        primaries = sorted(node.name for node in r.get_primaries())
        for kwargs in ({}, {"target_nodes": "primaries"}):
            cursors, keys = r.bless_scan(0, "NO-EVICT", **kwargs)
            assert sorted(keys) == [b"a", b"b"]
            assert sorted(cursors.keys()) == primaries
            assert all(cursor == 0 for cursor in cursors.values())

        assert r.bless_clear("a", "NO-EVICT") == 1
        cursors, keys = r.bless_scan(0, "NO-EVICT")
        assert keys == [b"b"]
        assert sorted(cursors.keys()) == primaries

    @skip_if_server_version_lt("8.11.0")
    def test_cluster_bless_scan_iter(self, r):
        keys_blessed = []
        r.mset_nonatomic({str(i): 1 for i in range(100)})
        for i in range(100):
            s = str(i)
            if s.startswith("1"):
                r.bless_set(s, "NO-EVICT")
                keys_blessed.append(s.encode("utf-8"))
        keys_blessed.sort()

        assert sorted(r.bless_scan_iter("NO-EVICT")) == keys_blessed
        # count=1 forces every primary through more than one cursor round
        assert sorted(r.bless_scan_iter("NO-EVICT", count=1)) == keys_blessed
        assert (
            sorted(r.bless_scan_iter("NO-EVICT", target_nodes="primaries"))
            == keys_blessed
        )

    def test_cluster_randomkey(self, r):
        node = r.get_node_from_key("{foo}")
        assert r.randomkey(target_nodes=node) is None
        for key in ("{foo}a", "{foo}b", "{foo}c"):
            r[key] = 1
        assert r.randomkey(target_nodes=node) in (b"{foo}a", b"{foo}b", b"{foo}c")

    @skip_if_server_version_lt("6.0.0")
    @skip_if_redis_enterprise()
    def test_acl_log(self, r, request):
        key = "{cache}:"
        node = r.get_node_from_key(key)
        username = "redis-py-user"

        def teardown():
            r.acl_deluser(username, target_nodes="primaries")

        request.addfinalizer(teardown)
        r.acl_setuser(
            username,
            enabled=True,
            reset=True,
            commands=["+get", "+set", "+select", "+cluster", "+command", "+info"],
            keys=["{cache}:*"],
            nopass=True,
            target_nodes="primaries",
        )
        r.acl_log_reset(target_nodes=node)

        user_client = _get_client(
            RedisCluster, request, flushdb=False, username=username
        )

        # Valid operation and key
        assert user_client.set("{cache}:0", 1)
        assert user_client.get("{cache}:0") == b"1"

        # Invalid key
        with pytest.raises(NoPermissionError):
            user_client.get("{cache}violated_cache:0")

        # Invalid operation
        with pytest.raises(NoPermissionError):
            user_client.hset("{cache}:0", "hkey", "hval")

        assert isinstance(r.acl_log(target_nodes=node), list)
        assert len(r.acl_log(target_nodes=node)) == 3
        assert len(r.acl_log(count=1, target_nodes=node)) == 1
        assert isinstance(r.acl_log(target_nodes=node)[0], dict)
        assert "client-info" in r.acl_log(count=1, target_nodes=node)[0]
        assert r.acl_log_reset(target_nodes=node)

    def generate_lib_code(self, lib_name):
        return f"""#!js api_version=1.0 name={lib_name}\n redis.registerFunction('foo', ()=>{{return 'bar'}})"""  # noqa

    def try_delete_libs(self, r, *lib_names):
        for lib_name in lib_names:
            try:
                r.tfunction_delete(lib_name)
            except Exception:
                pass

    @skip_if_server_version_lt("8.5.240")
    def test_hotkeys_cluster(self, r):
        """Test all HOTKEYS commands in cluster mode are raising an error"""

        with pytest.raises(NotImplementedError):
            r.hotkeys_start(count=10, metrics=[HotkeysMetricsTypes.CPU])
        with pytest.raises(NotImplementedError):
            r.hotkeys_get()
        with pytest.raises(NotImplementedError):
            r.hotkeys_reset()
        with pytest.raises(NotImplementedError):
            r.hotkeys_stop()


@pytest.mark.fixed_client
class TestClusterRedisCommandsMocked:
    """Cluster commands checked against mocked node replies; no server needed."""

    @skip_if_redis_enterprise()
    def test_cluster_delslots(self):
        cluster_slots = [
            [0, 8191, ["127.0.0.1", 7000, "node_0"]],
            [8192, 16383, ["127.0.0.1", 7001, "node_1"]],
        ]
        r = get_mocked_redis_client(
            host=default_host, port=default_port, cluster_slots=cluster_slots
        )
        mock_all_nodes_resp(r, "OK")
        node0 = r.get_node(default_host, 7000)
        node1 = r.get_node(default_host, 7001)
        assert r.cluster_delslots(0, 8192) == [True, True]
        assert node0.redis_connection.connection.read_response.called
        assert node1.redis_connection.connection.read_response.called

    @skip_if_server_version_lt("7.0.0")
    @skip_if_redis_enterprise()
    def test_cluster_delslotsrange(self):
        cluster_slots = [
            [
                0,
                8191,
                ["127.0.0.1", 7000, "node_0"],
            ],
            [
                8192,
                16383,
                ["127.0.0.1", 7001, "node_1"],
            ],
        ]
        r = get_mocked_redis_client(
            host=default_host, port=default_port, cluster_slots=cluster_slots
        )
        mock_all_nodes_resp(r, "OK")
        node = r.get_random_node()
        r.cluster_addslots(node, 1, 2, 3, 4, 5)
        assert r.cluster_delslotsrange(1, 5)

    @skip_if_redis_enterprise()
    def test_readonly(self):
        r = get_mocked_redis_client(host=default_host, port=default_port)
        mock_all_nodes_resp(r, "OK")
        assert r.readonly() is True
        all_replicas_results = r.readonly(target_nodes="replicas")
        for res in all_replicas_results.values():
            assert res is True
        for replica in r.get_replicas():
            assert replica.redis_connection.connection.read_response.called

    @skip_if_redis_enterprise()
    def test_readwrite(self):
        r = get_mocked_redis_client(host=default_host, port=default_port)
        mock_all_nodes_resp(r, "OK")
        assert r.readwrite() is True
        all_replicas_results = r.readwrite(target_nodes="replicas")
        for res in all_replicas_results.values():
            assert res is True
        for replica in r.get_replicas():
            assert replica.redis_connection.connection.read_response.called


class TestStaticMetadataRouting:
    """
    Every command ``_STATIC_COMMAND_METADATA`` routes by its keys must reach the node holding
    them.

    The static table decides the routing of ~100 commands where the shipped 7.1.0 table decided
    27, so every entry it gained is a command whose routing moved from the client's own slot
    resolution into the table. A wrong entry is close to invisible in a functional test: MOVED
    redirection still returns the right answer, at the cost of a round trip per call and a full
    topology re-discovery every ``reinitialize_steps`` MOVEDs.

    The ``movablekeys`` reads are the sharp case, and get their own test. Their keys live only in
    the ``COMMAND`` key specifications, so ``first_key_pos`` is 0; their records carry the keyed
    defaults, and the client finds the keys through ``COMMAND GETKEYS``.

    Key extraction itself is covered by ``tests/test_command_parser.py``, so it is stubbed here
    and these tests assert only the node the command is dispatched to.
    """

    MOVABLE_KEYS_READS = (
        "sintercard",
        "xread",
        "zdiff",
        "zinter",
        "zintercard",
        "zunion",
    )

    @staticmethod
    def _mocked_cluster():
        # The policy resolver is built here rather than reusing the import-time default
        # instance, which every client in the process shares along with its memo.
        return get_mocked_redis_client(
            host=default_host, port=7000, policy_resolver=StaticPolicyResolver()
        )

    @pytest.mark.fixed_client
    def test_the_shipped_default_is_this_resolver(self):
        """The routing below is the default behaviour, not one this test opted into."""
        rc = get_mocked_redis_client(host=default_host, port=7000)

        assert isinstance(rc._policy_resolver, StaticPolicyResolver)

    @pytest.mark.fixed_client
    def test_the_default_resolver_is_built_per_client(self):
        """
        Each client owns its resolver, so the memos a resolver accumulates are released with
        the client rather than retained for the life of the process. A default evaluated in
        the signature would hand every client in the process the same object.
        """
        first = get_mocked_redis_client(host=default_host, port=7000)
        second = get_mocked_redis_client(host=default_host, port=7000)

        assert first._policy_resolver is not second._policy_resolver

    @pytest.mark.fixed_client
    def test_a_pipeline_routes_through_the_clients_resolver(self):
        """
        Routing must not change just because the commands go through a pipeline. The client's
        resolver used not to reach ``ClusterPipeline``, which built its own static one, so a
        caller-supplied resolver was silently ignored for every pipelined command.
        """
        resolver = StaticPolicyResolver()
        rc = get_mocked_redis_client(
            host=default_host, port=7000, policy_resolver=resolver
        )

        with rc.pipeline() as pipe:
            assert pipe._policy_resolver is resolver
            # The execution strategy resolves through the pipeline, so assert what it reads.
            assert pipe._execution_strategy._pipe._policy_resolver is resolver

    @pytest.mark.fixed_client
    def test_a_directly_built_pipeline_still_gets_a_default(self):
        rc = get_mocked_redis_client(host=default_host, port=7000)

        pipe = ClusterPipeline(
            nodes_manager=rc.nodes_manager, commands_parser=rc.commands_parser
        )

        assert isinstance(pipe._policy_resolver, StaticPolicyResolver)

    @pytest.mark.fixed_client
    def test_routing_derives_from_a_given_metadata_resolver(self):
        """
        A policy resolver is the routing view of a metadata resolver, so a client given only a
        metadata resolver must route by it - otherwise a user who supplies live metadata would
        still route by the static table, and the two would disagree about the same command.
        """
        resolver = StaticMetadataResolver()
        rc = get_mocked_redis_client(
            host=default_host, port=7000, metadata_resolver=resolver
        )

        assert rc._metadata_resolver is resolver
        assert rc._policy_resolver._metadata_resolver is resolver

        with rc.pipeline() as pipe:
            assert pipe._metadata_resolver is resolver
            assert pipe._policy_resolver._metadata_resolver is resolver

    @pytest.mark.fixed_client
    def test_an_explicit_policy_resolver_still_wins_for_routing(self):
        """
        ``policy_resolver`` shipped in 7.1.0 as the routing extension point, so its behaviour
        must not move. Passing both is accepted rather than rejected, because a user migrating
        incrementally will legitimately do it.
        """
        policy_resolver = StaticPolicyResolver()
        metadata_resolver = StaticMetadataResolver()
        rc = get_mocked_redis_client(
            host=default_host,
            port=7000,
            policy_resolver=policy_resolver,
            metadata_resolver=metadata_resolver,
        )

        assert rc._policy_resolver is policy_resolver
        assert rc._metadata_resolver is metadata_resolver
        # Not derived from the metadata resolver, which is the point of the precedence rule.
        assert rc._policy_resolver._metadata_resolver is not metadata_resolver

    @pytest.mark.fixed_client
    def test_metadata_resolver_decides_replica_routing_with_explicit_policy_resolver(
        self,
    ):
        metadata_resolver = DynamicMetadataResolver({"core": {"set": CACHEABLE_KEYED}})
        rc = get_mocked_redis_client(
            host=default_host,
            port=7000,
            read_from_replicas=True,
            policy_resolver=StaticPolicyResolver(),
            metadata_resolver=metadata_resolver,
        )

        with (
            patch.object(rc, "determine_slot", return_value=0),
            patch.object(
                type(rc.nodes_manager), "get_node_from_slot", autospec=True
            ) as get_node,
        ):
            rc.get_nodes_from_slot("set", "set", "key", "value")

        get_node.assert_called_once_with(rc.nodes_manager, 0, True, None)

    @pytest.mark.fixed_client
    def test_split_multi_key_routing_respects_metadata_resolver(self):
        metadata_resolver = DynamicMetadataResolver({"core": {"mset": CACHEABLE_KEYED}})
        rc = get_mocked_redis_client(
            host=default_host,
            port=7000,
            read_from_replicas=True,
            policy_resolver=StaticPolicyResolver(),
            metadata_resolver=metadata_resolver,
        )
        pipe = Mock()
        pipe.execute.return_value = [True]

        with (
            patch.object(rc, "pipeline", return_value=pipe),
            patch.object(
                type(rc.nodes_manager), "get_node_from_slot", autospec=True
            ) as get_node,
        ):
            rc.mset_nonatomic({"key": "value"})

        slot = key_slot(rc.encoder.encode("key"))
        get_node.assert_called_once_with(rc.nodes_manager, slot, True, None)

    @pytest.mark.fixed_client
    def test_mixin_default_is_replica_safe_emits_deprecation_warning(self):
        from redis.commands.cluster import RedisClusterCommands

        class StandaloneClusterCommands(RedisClusterCommands):
            pass

        instance = StandaloneClusterCommands()
        with pytest.deprecated_call():
            assert instance._is_replica_safe("GET") is True
        with pytest.deprecated_call():
            assert instance._is_replica_safe("SET") is False

    @pytest.mark.fixed_client
    def test_cluster_scan_routes_to_all_primaries_by_default(self):
        rc = get_mocked_redis_client(host=default_host, port=7000)
        primaries = rc.get_primaries()
        assert len(primaries) > 1

        _, policies = rc._resolve_command_policies("scan")
        nodes = rc._determine_nodes("scan", request_policy=policies.request_policy)
        assert set(nodes) == set(primaries)

    @pytest.mark.fixed_client
    @pytest.mark.parametrize("command", ["dbsize", "keys"])
    def test_default_node_commands_route_to_default_node_by_default(self, command):
        rc = get_mocked_redis_client(host=default_host, port=7000)
        default_node = rc.get_default_node()

        _, policies = rc._resolve_command_policies(command)
        nodes = rc._determine_nodes(command, request_policy=policies.request_policy)
        assert nodes == [default_node]

    @pytest.mark.fixed_client
    @pytest.mark.parametrize("empty_targets", [[], {}])
    def test_empty_target_nodes_does_not_raise_and_falls_back(self, empty_targets):
        rc = get_mocked_redis_client(host=default_host, port=7000)
        default_node = rc.get_default_node()

        _, policies = rc._resolve_command_policies("dbsize")
        nodes = rc._determine_nodes(
            "dbsize",
            request_policy=policies.request_policy,
            nodes_flag=empty_targets,
        )
        assert nodes == [default_node]

    @pytest.mark.fixed_client
    @pytest.mark.parametrize("empty_targets", [[], {}, ""])
    def test_execute_command_empty_target_nodes_falls_back(self, empty_targets):
        rc = get_mocked_redis_client(host=default_host, port=7000)
        default_node = rc.get_default_node()

        with patch.object(
            RedisCluster, "_execute_command", return_value=100
        ) as execute:
            result = rc.execute_command("DBSIZE", target_nodes=empty_targets)

        assert result == 100
        execute.assert_called_once_with(default_node, "DBSIZE")

    @pytest.mark.fixed_client
    @pytest.mark.parametrize("invalid_targets", [False, 0, (), b""])
    def test_execute_command_rejects_invalid_empty_target_nodes(self, invalid_targets):
        rc = get_mocked_redis_client(host=default_host, port=7000)

        with pytest.raises(TypeError, match="target_nodes type"):
            rc.execute_command("DBSIZE", target_nodes=invalid_targets)

    @pytest.mark.fixed_client
    def test_command_subcommands_route_to_default_node(self):
        rc = get_mocked_redis_client(host=default_host, port=7000)
        default_node = rc.get_default_node()

        _, policies = rc._resolve_command_policies("COMMAND", "COUNT")
        nodes = rc._determine_nodes(
            "COMMAND", "COUNT", request_policy=policies.request_policy
        )
        assert nodes == [default_node]

    @pytest.mark.fixed_client
    @pytest.mark.parametrize("targets", ["nodes", "flag"])
    def test_explicit_target_nodes_do_not_apply_the_commands_aggregation(self, targets):
        """
        A response policy resolved for the whole cluster must not apply when the caller
        picked its targets, as named nodes or as a nodes flag: ``ONE_SUCCEEDED`` would
        stop the loop after the first node the caller picked, and an ``AGG_*`` would fold
        the replies of the rest into it.
        """
        resolver = DynamicMetadataResolver(
            {
                "core": {
                    "dbsize": CommandMetadata(
                        request_policy=RequestPolicy.ALL_SHARDS,
                        response_policy=ResponsePolicy.ONE_SUCCEEDED,
                    )
                }
            }
        )
        rc = get_mocked_redis_client(
            host=default_host, port=7000, metadata_resolver=resolver
        )
        nodes = rc.get_primaries()
        assert len(nodes) > 1
        target_nodes = nodes if targets == "nodes" else RedisCluster.PRIMARIES

        with patch.object(RedisCluster, "_execute_command", return_value=7) as execute:
            result = rc.execute_command("DBSIZE", target_nodes=target_nodes)

        assert execute.call_count == len(nodes)
        assert result == 7 * len(nodes)

    @pytest.mark.fixed_client
    def test_determine_nodes_raises_exception_when_no_policy_resolved(self):
        rc = get_mocked_redis_client(host=default_host, port=7000)
        with pytest.raises(RedisClusterException, match="No targets were found"):
            rc._determine_nodes("UNRECOGNIZED_COMMAND", request_policy=None)

        with rc.pipeline() as pipe:
            with pytest.raises(RedisClusterException, match="No targets were found"):
                pipe._determine_nodes("UNRECOGNIZED_COMMAND", request_policy=None)

    @pytest.mark.fixed_client
    def test_pipeline_determine_nodes_honors_resolved_metadata_policy(self):
        resolver = DynamicMetadataResolver(
            {
                "core": {
                    "dbsize": CommandMetadata(
                        request_policy=RequestPolicy.DEFAULT_KEYLESS
                    )
                }
            }
        )
        rc = get_mocked_redis_client(
            host=default_host, port=7000, metadata_resolver=resolver
        )
        selected_node = rc.get_primaries()[0]

        with rc.pipeline() as pipe:
            with patch.object(
                pipe, "get_keyless_target_node", return_value=selected_node
            ) as select_node:
                nodes = pipe._determine_nodes(
                    "dbsize", request_policy=RequestPolicy.DEFAULT_KEYLESS
                )
            assert nodes == [selected_node]
            select_node.assert_called_once_with("dbsize")

    @pytest.mark.fixed_client
    def test_keyless_routing_honors_the_public_node_selector(self):
        rc = get_mocked_redis_client(host=default_host, port=7000)
        selected_node = rc.get_primaries()[0]

        with patch.object(
            type(rc), "get_keyless_target_node", return_value=selected_node
        ) as select_node:
            nodes = rc._determine_nodes(
                "dbsize", request_policy=RequestPolicy.DEFAULT_KEYLESS
            )

        assert nodes == [selected_node]
        select_node.assert_called_once_with("dbsize")

    @pytest.mark.fixed_client
    @pytest.mark.parametrize("strategy", sorted(_REPLICAS_ONLY_STRATEGIES, key=str))
    def test_keyless_reads_honor_a_replicas_only_strategy(self, strategy):
        """
        A strategy that asks for replicas must not land a keyless read on a primary. Only
        that much of the strategy applies: the rest of it is an index into one shard's node
        list, and a keyless command has no shard, so the pick is uniform over the replicas.
        """
        rc = get_mocked_redis_client(
            host=default_host, port=7000, load_balancing_strategy=strategy
        )
        replicas = set(rc.get_replicas())
        assert replicas

        picked = {rc.get_keyless_target_node("get") for _ in range(50)}

        assert picked
        assert picked <= replicas
        assert not picked & set(rc.get_primaries())

    @pytest.mark.fixed_client
    @pytest.mark.parametrize("strategy", sorted(_REPLICAS_ONLY_STRATEGIES, key=str))
    def test_keyless_reads_fall_back_when_the_cluster_has_no_replicas(self, strategy):
        """
        A replicas-only strategy against a cluster that has none must still answer with a
        node instead of raising out of an empty choice.
        """
        rc = get_mocked_redis_client(
            host=default_host,
            port=7000,
            load_balancing_strategy=strategy,
            cluster_slots=[
                [0, 8191, ["127.0.0.1", 7000, "node_0"]],
                [8192, 16383, ["127.0.0.1", 7001, "node_1"]],
            ],
        )
        primaries = set(rc.get_primaries())
        assert not rc.get_replicas()

        picked = {rc.get_keyless_target_node("get") for _ in range(20)}

        assert picked
        assert picked <= primaries

    @pytest.mark.fixed_client
    @pytest.mark.parametrize(
        "strategy",
        [LoadBalancingStrategy.ROUND_ROBIN, LoadBalancingStrategy.RANDOM, None],
    )
    def test_keyless_reads_keep_the_whole_node_set_otherwise(self, strategy):
        """
        The two strategies that include the primary, and ``read_from_replicas`` on its own,
        keep the answer this method has given since 7.1.0: any node.
        """
        rc = get_mocked_redis_client(
            host=default_host,
            port=7000,
            read_from_replicas=True,
            load_balancing_strategy=strategy,
        )
        node = rc.get_primaries()[0]

        with patch.object(type(rc), "get_random_node", return_value=node) as any_node:
            assert rc.get_keyless_target_node("get") is node

        any_node.assert_called_once()

    @pytest.mark.fixed_client
    def test_every_load_balancing_strategy_is_classified(self):
        """
        The keyless path asks whether a strategy excludes the primary, and ``LoadBalancer``
        derives the same thing separately as its ``replicas_only`` bit. A strategy added to
        the enum and left out of the set here would silently be treated as including the
        primary, so pin the classification of all of them.
        """
        assert _REPLICAS_ONLY_STRATEGIES == {
            LoadBalancingStrategy.ROUND_ROBIN_REPLICAS,
            LoadBalancingStrategy.RANDOM_REPLICA,
        }
        assert set(LoadBalancingStrategy) - _REPLICAS_ONLY_STRATEGIES == {
            LoadBalancingStrategy.ROUND_ROBIN,
            LoadBalancingStrategy.RANDOM,
        }

    @pytest.mark.fixed_client
    def test_the_deprecated_selector_delegates_to_the_public_one(self):
        """
        The name that has been public since 7.1.0 keeps working and answers from the
        metadata resolver, which is the only thing that can send GET - a member of
        READ_COMMANDS - to a primary.
        """
        rc = get_mocked_redis_client(
            host=default_host,
            port=7000,
            read_from_replicas=True,
            metadata_resolver=DynamicMetadataResolver({"core": {}}),
        )

        with (
            patch.object(type(rc), "get_random_node") as any_node,
            pytest.warns(DeprecationWarning, match="get_keyless_target_node"),
        ):
            node = rc.get_random_primary_or_all_nodes("get")

        any_node.assert_not_called()
        assert node in rc.get_primaries()

        rc = get_mocked_redis_client(
            host=default_host,
            port=7000,
            read_from_replicas=True,
            metadata_resolver=DynamicMetadataResolver(
                {"core": {"get": CACHEABLE_KEYED}}
            ),
        )
        replica = rc.get_replicas()[0]

        with (
            patch.object(type(rc), "get_random_node", return_value=replica) as any_node,
            pytest.warns(DeprecationWarning),
        ):
            assert rc.get_random_primary_or_all_nodes("get") is replica

        any_node.assert_called_once()

    @pytest.mark.fixed_client
    def test_slot_id_commands_route_by_slot_for_any_spelling(self):
        """
        The SLOT_ID flag lookup is normalized, so a raw lowercase spelling names the same
        command as the one the command method sends - and the slot comes back as the int
        the slot map is keyed by, whichever way the caller spelled it.
        """
        rc = get_mocked_redis_client(host=default_host, port=7000)

        assert rc.determine_slot("CLUSTER COUNTKEYSINSLOT", 42) == 42
        assert rc.determine_slot("cluster countkeysinslot", "42") == 42

    @pytest.mark.fixed_client
    def test_container_command_is_dispatched_by_the_routed_name(self):
        """
        A container command passed as two words is dispatched by the name the policies were
        decided by, so the result callback registered for it fires - and receives that name
        rather than the caller's first word.
        """
        rc = get_mocked_redis_client(host=default_host, port=7000)
        seen = []
        rc.result_callbacks["COMMAND COUNT"] = lambda cmd, res, **kwargs: seen.append(
            cmd
        )

        with patch.object(RedisCluster, "_execute_command", return_value=100):
            rc.execute_command("command", "count")

        assert seen == ["COMMAND COUNT"]

    @pytest.mark.fixed_client
    def test_the_routed_name_is_the_same_with_and_without_explicit_targets(self):
        """
        The routed name is decided before the explicit-targets branch, so one command
        answers with one name either way. Without it the name kept the caller's spelling
        on that branch alone, and DBSIZE's result callback - keyed, like every other, in
        upper case - summed the replies for one of these two calls and not the other.
        """
        rc = get_mocked_redis_client(host=default_host, port=7000)
        seen = []
        rc.result_callbacks["DBSIZE"] = lambda cmd, res, **kwargs: seen.append(cmd)

        with patch.object(RedisCluster, "_execute_command", return_value=7):
            rc.execute_command("dbsize")
            rc.execute_command("dbsize", target_nodes=rc.get_primaries())

        assert seen == ["DBSIZE", "DBSIZE"]

    @pytest.mark.fixed_client
    def test_split_command_routing_applies_load_balancing_strategy(self):
        rc = get_mocked_redis_client(
            host=default_host,
            port=7000,
            load_balancing_strategy=LoadBalancingStrategy.ROUND_ROBIN_REPLICAS,
        )
        pipe = Mock()
        pipe.execute.return_value = [True, True]
        with (
            patch.object(rc._metadata_resolver, "is_replica_safe", return_value=True),
            patch.object(rc, "pipeline", return_value=pipe),
            patch.object(
                type(rc.nodes_manager), "get_node_from_slot", autospec=True
            ) as get_node,
        ):
            slots_to_args = {100: ["a", "1"], 200: ["b", "2"]}
            rc._execute_pipeline_by_slot("MSET", slots_to_args)

            get_node.assert_any_call(
                rc.nodes_manager, 100, True, LoadBalancingStrategy.ROUND_ROBIN_REPLICAS
            )
            get_node.assert_any_call(
                rc.nodes_manager, 200, True, LoadBalancingStrategy.ROUND_ROBIN_REPLICAS
            )

    @pytest.mark.fixed_client
    def test_custom_host_class_without_strategy_executes_split_slots(self):
        class ThirdPartyCluster(ClusterMultiKeyCommands):
            def __init__(self, rc):
                self.encoder = rc.encoder
                self.nodes_manager = rc.nodes_manager
                self._rc = rc

            def pipeline(self):
                return self._rc.pipeline()

        rc = get_mocked_redis_client(host=default_host, port=7000)
        custom_client = ThirdPartyCluster(rc)
        pipe = Mock()
        pipe.execute.return_value = ["OK", "OK"]
        with patch.object(custom_client, "pipeline", return_value=pipe):
            result = custom_client.mset_nonatomic({"a": "1", "b": "2"})
            assert result == ["OK", "OK"]

    @pytest.mark.fixed_client
    def test_every_node_client_gets_the_same_resolver(self):
        """
        The resolver has to reach every node's ``Redis``, because that is where the pools -
        and therefore the CSC connections - live. Asserted by identity, since two static
        resolvers give the same answers and only identity proves distribution.

        This covers the host/port branch of ``create_redis_node``; the ``from_url`` branch is
        covered by the live cluster test in ``tests/test_cache.py``.
        """
        resolver = StaticMetadataResolver()
        rc = get_mocked_redis_client(
            host=default_host, port=7000, metadata_resolver=resolver
        )

        assert rc.nodes_manager._metadata_resolver is resolver
        nodes = list(rc.nodes_manager.nodes_cache.values())
        assert len(nodes) > 1
        for node in nodes:
            pool = node.redis_connection.connection_pool
            assert pool.metadata_resolver is resolver, node.name

    @pytest.mark.fixed_client
    def test_does_not_write_the_resolver_into_the_callers_cache_config(self):
        """
        Sync parity with the standalone client, which copies the caller's ``CacheConfig``
        before injecting.

        The cluster builds one cache from ``cache_config`` and hands it to every node, so each
        node's pool sees a ``cache=`` and sets the resolver on the configuration inside it -
        which is the caller's object, because ``CacheFactory`` holds it by reference. Without
        the copy in ``NodesManager``, a ``CacheConfig`` reused across clients picks up
        whichever resolver was injected last.
        """
        # A resolver that carries nothing, so anything deciding through it reports every
        # command ineligible - which is how the injection is observable.
        empty_resolver = DynamicMetadataResolver({})
        cache_config = CacheConfig()

        rc = get_mocked_redis_client(
            host=default_host,
            port=7000,
            protocol=3,
            cache_config=cache_config,
            metadata_resolver=empty_resolver,
        )

        # The caller's object is untouched, so another client sharing it still caches GET.
        assert cache_config.is_allowed_to_cache("GET") is True

        # The cluster-wide cache decides through the resolver, on a copy of that config.
        cluster_config = rc.nodes_manager._cache.config
        assert cluster_config is not cache_config
        assert cluster_config.is_allowed_to_cache("GET") is False
        # Every other setting of the caller's config still applies to the copy.
        assert cluster_config.get_max_size() == cache_config.get_max_size()

        # And every node decides through that one copy, not through the caller's object.
        nodes = list(rc.nodes_manager.nodes_cache.values())
        assert len(nodes) > 1
        for node in nodes:
            pool = node.redis_connection.connection_pool
            assert pool.cache.config is cluster_config, node.name

    @pytest.mark.fixed_client
    def test_the_default_metadata_resolver_is_built_per_client(self):
        """Same reasoning as the policy-resolver default: the memos die with the client."""
        first = get_mocked_redis_client(host=default_host, port=7000)
        second = get_mocked_redis_client(host=default_host, port=7000)

        assert isinstance(first._metadata_resolver, StaticMetadataResolver)
        assert first._metadata_resolver is not second._metadata_resolver

    @pytest.mark.fixed_client
    def test_the_static_policy_resolver_preserves_its_metadata_resolver(self):
        """``with_fallback`` must not drop the resolver the caller configured."""
        resolver = StaticMetadataResolver()
        policy_resolver = StaticPolicyResolver(metadata_resolver=resolver)

        chained = policy_resolver.with_fallback(StaticPolicyResolver())

        assert chained._metadata_resolver is resolver

    @pytest.mark.fixed_client
    def test_the_movablekeys_reads_resolve_keyed(self):
        """
        The deterministic half of this: a keyed record is what sends the client to
        ``determine_slot``. The dispatch assertion below can only catch a wrong policy
        probabilistically, because keyless routing picks a random node.
        """
        rc = self._mocked_cluster()

        for command in self.MOVABLE_KEYS_READS:
            assert (
                rc._policy_resolver.resolve(command).request_policy
                == RequestPolicy.DEFAULT_KEYED
            ), command

    @pytest.mark.fixed_client
    def test_every_slot_routed_entry_is_dispatched_to_the_node_holding_its_key(self):
        """
        Every static-table entry that must route by its keys, not only the withheld ones.

        The table answers ~100 commands where the shipped 7.1.0 table answered 27, so each one
        is a command whose routing the table now decides. This walks the whole slot-routed set,
        so an entry added or edited later is covered without touching this test.

        Note what this alone cannot catch: the set is derived from the table, so an entry wrongly
        recorded keyless drops out of it rather than failing here. That hole is closed by
        ``TestWithheldRoutingPolicies.test_no_entry_that_takes_a_key_is_routed_keyless``, which
        asserts the classification itself. The two are only airtight together.
        """
        rc = self._mocked_cluster()
        expected_node = rc.nodes_manager.get_node_from_slot(key_slot(b"{foo}a"))
        commands = list(slot_routed_static_commands())

        assert len(commands) > 70, "the slot-routed set looks truncated"

        for command in commands:
            with (
                patch.object(
                    RedisCluster, "_get_command_keys", return_value=["{foo}a"]
                ),
                patch.object(
                    RedisCluster, "_execute_command", return_value="OK"
                ) as execute_command,
            ):
                rc.execute_command(command, *slot_routed_args(command))

            assert execute_command.call_count == 1, command
            assert execute_command.call_args[0][0] is expected_node, command


def _route_by_live_tips(r, monkeypatch):
    """Route ``r`` by the tips of the connected server instead of the shipped records."""
    monkeypatch.setattr(r, "_policy_resolver", DynamicPolicyResolver(r.commands_parser))


def _keys_on_every_shard(r, prefix):
    """One key per primary, so a command over them reaches every shard."""
    keys, shards = [], set()
    for index in range(10000):
        key = f"{{{prefix}{index}}}key"
        shard = r.get_node_from_key(key).name
        if shard not in shards:
            shards.add(shard)
            keys.append(key)
        if len(keys) == len(r.get_primaries()):
            return keys
    raise AssertionError("no key found for every shard")


@contextmanager
def _sent_commands(fail=None):
    """
    Record each command the cluster client sends to a node, still sending it to the server.

    ``fail(node, args)`` may return an error to raise instead of sending that command once.
    """
    sent = []
    send = RedisCluster._execute_command

    def execute_command(self, node, *args, **kwargs):
        sent.append((node, args, kwargs))
        error = fail(node, args) if fail is not None else None
        if error is not None:
            raise error
        return send(self, node, *args, **kwargs)

    with patch.object(RedisCluster, "_execute_command", execute_command):
        yield sent


@contextmanager
def _pipelined_commands(fail=None):
    """
    Record the commands of each pipelined write the cluster client makes to a node - one
    round trip - still sending them to the server.

    ``fail(args)`` may return an error for a command to answer with instead of sending it,
    which leaves that command for the pipeline to retry on its own.
    """
    batches = []
    write = redis.cluster.NodeCommands.write

    def record(self):
        held = []
        for c in self.commands:
            error = fail(c.args) if fail is not None else None
            if error is not None:
                c.result = error
                held.append(c)
        self.commands = [c for c in self.commands if c not in held]
        batches.append([(c.args, c.options) for c in self.commands])
        if self.commands:
            write(self)
        self.commands.extend(held)

    with patch.object(redis.cluster.NodeCommands, "write", record):
        yield batches


@contextmanager
def _busy_node(node, *command):
    """
    Keep ``node`` busy running ``command`` - a looping script or function - until a KILL stops
    it. The busy reply threshold is lowered so the node reports BUSY, and accepts the KILL,
    right away.
    """
    client = Redis(host=node.host, port=node.port)
    threshold = client.config_get("busy-reply-threshold")["busy-reply-threshold"]
    client.config_set("busy-reply-threshold", 10)

    def run():
        try:
            Redis(host=node.host, port=node.port).execute_command(*command)
        except ResponseError:
            pass

    thread = threading.Thread(target=run)
    thread.start()
    try:
        deadline = monotonic() + 5
        while monotonic() < deadline:
            try:
                client.ping()
            except ResponseError as e:
                if "BUSY" in str(e):
                    break
            sleep(0.01)
        else:
            raise AssertionError(f"{node.name} never became busy")
        yield
    finally:
        thread.join(timeout=10)
        client.config_set("busy-reply-threshold", threshold)
        client.close()


_SPIN_LIBRARY = (
    "#!lua name=spinlib\n"
    "redis.register_function{function_name='spin', "
    "callback=function() while true do end end, flags={'no-writes'}}"
)


@pytest.mark.onlycluster
@skip_if_server_version_lt("8.0.0")
@skip_if_redis_enterprise()
class TestResponsePolicies:
    """
    How the replies of a command sent to several nodes fold into the one reply the caller
    gets, through real commands that carry each policy on the server.
    """

    def test_every_response_policy_has_a_callback(self):
        assert set(_RESPONSE_POLICY_CALLBACKS) == set(ResponsePolicy)

    def test_the_policies_no_command_is_tipped_with_fold_on_their_own(self):
        """
        No server command is tipped ``agg_max`` or ``agg_logical_or``, and SCRIPT EXISTS, the
        one ``agg_logical_and`` command, is merged by its result callback, so these folds are
        exercised directly.
        """

        def fold(policy, *replies):
            return _RESPONSE_POLICY_CALLBACKS[policy](
                {f"node{index}": reply for index, reply in enumerate(replies)}
            )

        assert fold(ResponsePolicy.AGG_MAX, 4, 2, 3) == 4
        assert fold(ResponsePolicy.AGG_MAX, [1, 2], [0, 3]) == [1, 3]
        assert fold(ResponsePolicy.AGG_LOGICAL_OR, False, True) is True
        assert fold(ResponsePolicy.AGG_LOGICAL_OR, [1, 0, 0], [0, 0, 1]) == [1, 0, 1]
        folded = fold(ResponsePolicy.AGG_LOGICAL_AND, 1, 1)
        assert folded == 1 and type(folded) is int
        assert fold(ResponsePolicy.AGG_LOGICAL_AND, [1, 0, 1], [1, 1, 0]) == [1, 0, 0]

    def test_agg_sum_adds_up_every_node_s_count(self, r, monkeypatch):
        """SLOWLOG LEN: all_nodes / agg_sum."""
        _route_by_live_tips(r, monkeypatch)
        expected = sum(
            node.redis_connection.execute_command("SLOWLOG LEN")
            for node in r.get_nodes()
        )

        with _sent_commands() as sent:
            assert r.execute_command("SLOWLOG LEN") == expected

        assert {node.name for node, _, _ in sent} == {n.name for n in r.get_nodes()}

    def test_agg_min_folds_array_replies_element_wise(self, r, monkeypatch):
        """WAITAOF: all_shards / agg_min, one [numlocal, numreplicas] pair per shard."""
        _route_by_live_tips(r, monkeypatch)
        replies = [
            node.redis_connection.execute_command("WAITAOF", 0, 0, 0)
            for node in r.get_primaries()
        ]

        assert r.execute_command("WAITAOF", 0, 0, 0) == [
            min(column) for column in zip(*replies)
        ]

    def test_all_succeeded_answers_with_one_reply(self, r, monkeypatch):
        """FUNCTION LOAD: all_shards / all_succeeded, so every shard gets the library."""
        _route_by_live_tips(r, monkeypatch)
        library = "#!lua name=policylib\nredis.register_function('one', function() return 1 end)"

        try:
            loaded = r.execute_command("FUNCTION LOAD", "REPLACE", library)

            assert str_if_bytes(loaded) == "policylib"
            for node in r.get_primaries():
                assert node.redis_connection.fcall("one", 0) == 1
        finally:
            r.execute_command("FUNCTION DELETE", "policylib")

    def test_all_succeeded_fails_on_an_error(self, r, monkeypatch):
        _route_by_live_tips(r, monkeypatch)

        with pytest.raises(ResponseError, match="Library not found"):
            r.execute_command("FUNCTION DELETE", "no-such-library")

    @pytest.mark.parametrize("method", ["script_kill", "function_kill"])
    def test_one_succeeded_raises_when_no_shard_succeeds(self, r, method):
        """No shard runs a script, so every one answers NOTBUSY."""
        with _sent_commands() as sent:
            with pytest.raises(ResponseError, match="NOTBUSY"):
                getattr(r, method)()

        assert len(sent) == len(r.get_primaries())

    @pytest.mark.parametrize("method", ["script_kill", "function_kill"])
    def test_one_succeeded_reports_the_error_of_the_shard_running_the_script(
        self, r, method
    ):
        """
        The shard running a script that already wrote answers UNKILLABLE, and that is the
        error reported, not the NOTBUSY of every other shard. The UNKILLABLE is injected:
        a real one leaves the shard busy until it is shut down.
        """
        busy = r.get_primaries()[-1]

        def fail(node, args):
            if node.name == busy.name:
                return ResponseError("UNKILLABLE Sorry the script already executed")
            return None

        with _sent_commands(fail):
            with pytest.raises(ResponseError, match="UNKILLABLE"):
                getattr(r, method)()

    def test_script_kill_stops_the_script_on_whichever_shard_runs_it(self, r):
        """The shipped table tips SCRIPT KILL all_shards / one_succeeded."""
        busy = r.get_primaries()[-1]

        with _busy_node(busy, "EVAL", "while true do end", 0):
            with _sent_commands() as sent:
                assert r.script_kill() is True

        assert len(sent) == len(r.get_primaries())

    def test_function_kill_stops_the_function_on_whichever_shard_runs_it(self, r):
        busy = r.get_primaries()[-1]
        busy.redis_connection.function_load(_SPIN_LIBRARY, replace=True)

        try:
            with _busy_node(busy, "FCALL", "spin", 0):
                assert str_if_bytes(r.function_kill()) == "OK"
        finally:
            busy.redis_connection.function_delete("spinlib")

    @pytest.mark.parametrize(
        "error",
        [
            TryAgainError("TRYAGAIN"),
            AskError("1 127.0.0.1:7001"),
            ConnectionError("lost"),
        ],
    )
    def test_one_succeeded_answers_with_the_success_despite_an_error_in_doubt(
        self, r, error
    ):
        """
        As the ``one_succeeded`` tip asks, the busy shard's success is the reply even when
        another shard leaves the command in doubt - and that shard is asked first, so every
        shard after it is still asked.
        """
        busy, doubtful = r.get_primaries()[-1], r.get_primaries()[0]

        def fail(node, args):
            return error if node.name == doubtful.name else None

        with _busy_node(busy, "EVAL", "while true do end", 0):
            with _sent_commands(fail) as sent:
                assert r.script_kill() is True

        assert len(sent) == len(r.get_primaries())

    @pytest.mark.parametrize(
        "error", [TryAgainError("TRYAGAIN"), AskError("1 127.0.0.1:7001")]
    )
    def test_one_succeeded_reports_an_error_in_doubt_when_no_shard_succeeds(
        self, r, error
    ):
        """
        With no success, the error that leaves a shard in doubt is reported over the NOTBUSY
        of the others: that shard may have been running the script.
        """
        doubtful = r.get_primaries()[-1]

        def fail(node, args):
            return error if node.name == doubtful.name else None

        with _sent_commands(fail):
            with pytest.raises(type(error)):
                r.script_kill()

    @pytest.mark.parametrize("targets", ["nodes", "flag"])
    def test_the_caller_s_targets_disable_the_aggregation(
        self, r, monkeypatch, targets
    ):
        """
        WAITAOF is tipped agg_min, but the caller picked the targets - named nodes and a
        nodes flag alike - so each node's reply is kept, on the main path and in a
        pipeline.
        """
        _route_by_live_tips(r, monkeypatch)
        primaries = r.get_primaries()
        target_nodes = primaries if targets == "nodes" else r.PRIMARIES
        expected = {
            node.name: node.redis_connection.execute_command("WAITAOF", 0, 0, 0)
            for node in primaries
        }

        assert (
            r.execute_command("WAITAOF", 0, 0, 0, target_nodes=target_nodes) == expected
        )
        with r.pipeline() as pipe:
            pipe.execute_command("WAITAOF", 0, 0, 0, target_nodes=target_nodes)
            assert pipe.execute() == [expected]

    def test_a_retry_asks_only_the_nodes_that_have_not_answered(
        self, r, no_cluster_retry_delay
    ):
        """
        A node that answered is not asked again when another node fails: asking it again
        would run a write twice.
        """
        primaries = r.get_primaries()
        failed = primaries[-1]
        failures = []

        def fail(node, args):
            if node.name == failed.name and not failures:
                failures.append(node.name)
                return ConnectionError("lost")
            return None

        with _sent_commands(fail) as sent:
            assert r.execute_command("PING", target_nodes=r.PRIMARIES) is True

        assert failures
        assert sorted(node.name for node, _, _ in sent) == sorted(
            [*(node.name for node in primaries), failed.name]
        )

    def test_info_keeps_each_shard_s_reply(self, r, monkeypatch):
        """INFO: all_shards / special with no handler, so nothing is merged."""
        _route_by_live_tips(r, monkeypatch)

        info = r.info()

        assert set(info) == {node.name for node in r.get_primaries()}

    @pytest.mark.parametrize(
        "command,nodes",
        [
            (("MEMORY DOCTOR",), "get_primaries"),
            (("LATENCY DOCTOR",), "get_nodes"),
            (("LATENCY HISTOGRAM",), "get_nodes"),
        ],
    )
    def test_the_shipped_table_keeps_each_node_s_diagnostics(self, r, command, nodes):
        """The server tips them ``special``, with no merge: each node describes itself."""
        result = r.execute_command(*command)

        assert set(result) == {node.name for node in getattr(r, nodes)()}

    def test_function_stats_reports_every_primary(self, r):
        assert set(r.function_stats()) == {node.name for node in r.get_primaries()}

    def test_a_result_callback_still_merges_a_fanned_out_command(self, r):
        """DBSIZE's own callback sums the replies; the response policy does not run twice."""
        r.mset_nonatomic({key: 1 for key in _keys_on_every_shard(r, "dbsize")})
        expected = sum(node.redis_connection.dbsize() for node in r.get_primaries())

        assert (
            r.execute_command("DBSIZE", target_nodes=RedisCluster.PRIMARIES) == expected
        )

    def test_a_pipeline_folds_each_fanned_out_command_into_one_result(
        self, r, monkeypatch
    ):
        _route_by_live_tips(r, monkeypatch)
        r.mset_nonatomic({key: 1 for key in _keys_on_every_shard(r, "pipe")})
        waitaof = [
            node.redis_connection.execute_command("WAITAOF", 0, 0, 0)
            for node in r.get_primaries()
        ]
        dbsize = sum(node.redis_connection.dbsize() for node in r.get_primaries())

        with r.pipeline() as pipe:
            pipe.execute_command("WAITAOF", 0, 0, 0)
            pipe.execute_command("DBSIZE")
            pipe.execute_command("FUNCTION DELETE", "no-such-library")
            pipe.execute_command("SCRIPT KILL")
            results = pipe.execute(raise_on_error=False)

        assert results[0] == [min(column) for column in zip(*waitaof)]
        assert results[1] == dbsize
        assert isinstance(results[2], ResponseError)
        assert isinstance(results[3], ResponseError) and "NOTBUSY" in str(results[3])

    @pytest.mark.parametrize(
        "error", [ConnectionError("lost"), TryAgainError("TRYAGAIN")]
    )
    @pytest.mark.parametrize("targets", ["flag", "nodes"])
    def test_a_pipeline_retry_asks_only_the_node_that_failed(self, r, error, targets):
        """
        A fanned-out command that fails on one node is retried on that node alone, with
        the replies of the others kept: asking them again would run a write twice. The
        node is picked among the targets the caller passed, not by the command's own
        policy - DBSIZE's is the default node alone.
        """
        r.mset_nonatomic({key: 1 for key in _keys_on_every_shard(r, "pipetargets")})
        primaries = r.get_primaries()
        dbsize = sum(node.redis_connection.dbsize() for node in primaries)
        target_nodes = r.PRIMARIES if targets == "flag" else primaries
        failed = primaries[-1]
        write = redis.cluster.NodeCommands.write
        sends = {node.name: 0 for node in primaries}

        def fail_once(self):
            # The failed node's DBSIZE is not sent once, and answers with ``error`` instead
            node = r.nodes_manager.find_connection_owner(self.connection)
            held = [c for c in self.commands if c.args == ("DBSIZE",)]
            sends[node.name] += len(held)
            held = held if node.name == failed.name and sends[node.name] == 1 else []
            self.commands = [c for c in self.commands if c not in held]
            if self.commands:
                write(self)
            for c in held:
                c.result = error
                self.commands.append(c)

        with patch.object(redis.cluster.NodeCommands, "write", fail_once):
            with _sent_commands() as sent:
                with r.pipeline() as pipe:
                    pipe.execute_command("DBSIZE", target_nodes=target_nodes)
                    assert pipe.execute() == [dbsize]

        for node, _, _ in sent:
            sends[node.name] += 1
        assert sends == {**{node.name: 1 for node in primaries}, failed.name: 2}


@pytest.mark.onlycluster
@skip_if_server_version_lt("8.0.0")
class TestMultiShardRequests:
    """
    A ``MULTI_SHARD`` command is split into one command per hash slot of its keys, and the
    per-slot replies are merged into the reply of the command the caller sent.
    """

    def test_the_counts_of_a_summed_command_are_added_up(self, r):
        """The shipped table tips EXISTS and TOUCH multi_shard / agg_sum."""
        keys = _keys_on_every_shard(r, "sum")
        r.mset_nonatomic({key: 1 for key in keys})

        with _pipelined_commands() as batches, _sent_commands() as sent:
            assert r.execute_command("EXISTS", *keys, keys[0] + "missing") == len(keys)
            assert r.execute_command("TOUCH", *keys) == len(keys)

        assert sum(len(batch) for batch in batches) == 2 * len(keys)
        assert not sent

    def test_a_nodes_flag_keeps_each_primary_s_count(self, r):
        """
        EXISTS is tipped agg_sum, but ``target_nodes=PRIMARIES`` picks the targets, so
        each primary's count is kept rather than summed - the answer a list of the same
        nodes gets, and the one 8.1.0 gave. A primary that does not hold the key
        redirects to the one that does, so every primary answers 1.
        """
        r.set("{flag}foo", 1)
        expected = {node.name: 1 for node in r.get_primaries()}

        assert r.execute_command("EXISTS", "{flag}foo", target_nodes=r.PRIMARIES) == (
            expected
        )
        assert (
            r.execute_command("EXISTS", "{flag}foo", target_nodes=r.get_primaries())
            == expected
        )

    def test_the_shipped_records_keep_mget_unsplit(self, r):
        """MGET documents that keys in different slots raise on a cluster."""
        with pytest.raises(RedisClusterException, match="same key slot"):
            r.mget(_keys_on_every_shard(r, "mget"))

    def test_the_values_of_a_per_key_command_come_back_in_key_order(
        self, r, monkeypatch
    ):
        """MGET with the live multi_shard tip, each slot's command carrying its own keys."""
        _route_by_live_tips(r, monkeypatch)
        first, second, third = _keys_on_every_shard(r, "order")[:3]
        keys = [first, second, first + "x", third]
        assert r.keyslot(first) == r.keyslot(first + "x")
        r.mset_nonatomic({key: key for key in keys})

        with _pipelined_commands() as batches:
            values = r.execute_command("MGET", *keys, keys=keys)

        assert [str_if_bytes(value) for value in values] == keys
        assert [command for batch in batches for command in batch] == [
            (("MGET", first, first + "x"), {"keys": [first, first + "x"]}),
            (("MGET", second), {"keys": [second]}),
            (("MGET", third), {"keys": [third]}),
        ]

    def test_a_key_travels_with_the_arguments_of_its_step(self, r, monkeypatch):
        """MSET with the live multi_shard / all_succeeded tips: two arguments per key."""
        _route_by_live_tips(r, monkeypatch)
        keys = _keys_on_every_shard(r, "step")

        assert r.execute_command("MSET", *(arg for key in keys for arg in (key, key)))

        assert [str_if_bytes(r.get(key)) for key in keys] == keys

    def test_keys_that_share_a_slot_are_sent_as_they_are(self, r):
        keys = ["{same}1", "{same}2"]
        r.mset({key: 1 for key in keys})

        with _sent_commands() as sent:
            assert r.execute_command("EXISTS", *keys) == 2

        assert [args for _, args, _ in sent] == [("EXISTS", *keys)]

    def test_keys_that_share_a_slot_are_hashed_once(self, r):
        """The split routes the command by the slot it computed, not by its keys again."""
        keys = ["{same}1", "{same}2", "{same}3"]

        with patch.object(r, "keyslot", wraps=r.keyslot) as keyslot:
            assert r.execute_command("EXISTS", *keys) == 0
        assert keyslot.call_count == len(keys)

        with r.pipeline() as pipe:
            with patch.object(pipe, "keyslot", wraps=pipe.keyslot) as keyslot:
                pipe.execute_command("EXISTS", *keys)
                assert pipe.execute() == [0]
        assert keyslot.call_count == len(keys)

    def test_a_single_key_is_not_split(self, r):
        """It has nothing to split, so its slot is left to the keyed routing to compute."""
        with patch.object(r, "keyslot", wraps=r.keyslot) as keyslot:
            assert r._split_multi_shard_command("EXISTS", "single") is None

        keyslot.assert_not_called()

    @skip_if_server_version_lt("8.4")
    def test_keys_that_cannot_be_located_are_not_split(self, r, monkeypatch):
        """MSETEX names its key count first, so its keys are not at fixed positions."""
        _route_by_live_tips(r, monkeypatch)
        first, second = _keys_on_every_shard(r, "movable")[:2]

        with pytest.raises(RedisClusterException, match="same key slot"):
            r.execute_command("MSETEX", 2, first, "a", second, "b")

    def test_a_split_command_costs_one_round_trip_per_node(self, r):
        """Each node is sent the commands of all its slots in one pipelined write."""
        keys = [f"{{trip{index}}}key" for index in range(60)]
        r.mset_nonatomic({key: 1 for key in keys})
        nodes = {r.get_node_from_key(key).name for key in keys}
        assert len({r.keyslot(key) for key in keys}) > len(nodes)

        with _pipelined_commands() as batches, _sent_commands() as sent:
            assert r.execute_command("EXISTS", *keys) == len(keys)

        assert len(batches) == len(nodes)
        assert sum(len(batch) for batch in batches) == len(
            {r.keyslot(key) for key in keys}
        )
        assert not sent

    @pytest.mark.parametrize(
        "error",
        [
            ConnectionError("lost"),
            TryAgainError("TRYAGAIN"),
            ClusterDownError("CLUSTERDOWN The cluster is down"),
        ],
    )
    def test_a_retry_resends_only_the_slots_that_failed(self, r, monkeypatch, error):
        """
        A slot that already succeeded is not sent again: the resent DEL would find its keys
        gone and the summed count would be wrong.
        """
        _route_by_live_tips(r, monkeypatch)
        keys = _keys_on_every_shard(r, "retry")
        r.mset_nonatomic({key: 1 for key in keys})
        failed = []

        def fail(args):
            if args == ("DEL", keys[-1]) and not failed:
                failed.append(args)
                return error
            return None

        with _pipelined_commands(fail) as batches, _sent_commands() as sent:
            assert r.execute_command("DEL", *keys) == len(keys)

        assert failed
        # The failed slot reached the server only on the retry, and no other slot twice
        assert sorted(
            [
                *(args for batch in batches for args, _ in batch),
                *(args for _, args, _ in sent),
            ]
        ) == sorted(("DEL", key) for key in keys)
        assert r.exists(*keys) == 0

    def test_a_slot_without_a_known_node_is_retried(self, r, monkeypatch):
        """
        A slot the client knows no node for is resent on its own, with the retries a
        command sent on its own gets, while the other slots are sent once.
        """
        keys = _keys_on_every_shard(r, "uncovered")
        r.mset_nonatomic({key: 1 for key in keys})
        get_node_from_slot = r.nodes_manager.get_node_from_slot
        uncovered = r.keyslot(keys[-1])
        failed = []

        def lose_the_slot_once(slot, *args, **kwargs):
            if slot == uncovered and not failed:
                failed.append(slot)
                raise SlotNotCoveredError(f"Slot {slot} is not covered")
            return get_node_from_slot(slot, *args, **kwargs)

        monkeypatch.setattr(r.nodes_manager, "get_node_from_slot", lose_the_slot_once)

        with _pipelined_commands() as batches, _sent_commands() as sent:
            assert r.execute_command("EXISTS", *keys) == len(keys)

        assert failed
        assert sorted(args for batch in batches for args, _ in batch) == sorted(
            ("EXISTS", key) for key in keys[:-1]
        )
        assert [args for _, args, _ in sent] == [("EXISTS", keys[-1])]

    def test_an_error_read_from_the_pipeline_is_counted(self, r):
        """
        The pipeline counts no command's error, so the split counts the one it raises,
        on the node its slot was sent to, as a command sent on its own would.
        """
        keys = _keys_on_every_shard(r, "counted")
        error = ResponseError("OOM command not allowed")

        def fail(args):
            return error if args == ("EXISTS", keys[-1]) else None

        with _pipelined_commands(fail):
            with patch.object(r, "_record_error_metric") as record:
                with pytest.raises(ResponseError, match="OOM"):
                    r.execute_command("EXISTS", *keys)

        record.assert_called_once_with(
            error=error,
            connection=r.get_node_from_key(keys[-1]),
            is_internal=False,
        )

    def test_a_requested_refresh_runs_before_the_slots_are_routed(self, r):
        """As a command sent on its own: the slots are routed by the refreshed topology."""
        keys = _keys_on_every_shard(r, "refresh")
        events = []
        run = r.nodes_manager._run_requested_refresh
        get_node_from_slot = r.nodes_manager.get_node_from_slot

        def run_requested_refresh(*args, **kwargs):
            events.append("refresh")
            return run(*args, **kwargs)

        def route(*args, **kwargs):
            events.append("route")
            return get_node_from_slot(*args, **kwargs)

        r.nodes_manager._request_refresh()
        with (
            patch.object(
                r.nodes_manager, "_run_requested_refresh", run_requested_refresh
            ),
            patch.object(r.nodes_manager, "get_node_from_slot", route),
        ):
            assert r.execute_command("EXISTS", *keys) == 0

        assert events[0] == "refresh"
        assert "route" in events

    def test_a_pipeline_merges_a_split_command_per_slot(self, r, monkeypatch):
        _route_by_live_tips(r, monkeypatch)
        keys = _keys_on_every_shard(r, "pipesplit")
        r.mset_nonatomic({key: key for key in keys})

        with r.pipeline() as pipe:
            pipe.execute_command("MGET", *reversed(keys))
            pipe.execute_command("EXISTS", *keys)
            results = pipe.execute()

        assert [str_if_bytes(value) for value in results[0]] == keys[::-1]
        assert results[1] == len(keys)

    @pytest.mark.parametrize(
        "error", [ConnectionError("lost"), TryAgainError("TRYAGAIN")]
    )
    def test_a_pipeline_retry_resends_only_the_slots_that_failed(
        self, r, monkeypatch, error
    ):
        """
        As on the main path: the resent DEL of a slot that already succeeded would find
        its keys gone, and the summed count would be wrong.
        """
        _route_by_live_tips(r, monkeypatch)
        keys = _keys_on_every_shard(r, "piperetry")
        r.mset_nonatomic({key: 1 for key in keys})
        write = redis.cluster.NodeCommands.write
        written = []
        failed = []

        def fail_once(self):
            # The last key's slot is not sent once, and answers with ``error`` instead
            held = [c for c in self.commands if c.args == ("DEL", keys[-1])]
            held = held if not failed else []
            failed.extend(held)
            self.commands = [c for c in self.commands if c not in held]
            written.extend(c.args for c in self.commands)
            if self.commands:
                write(self)
            for c in held:
                c.result = error
                self.commands.append(c)

        with patch.object(redis.cluster.NodeCommands, "write", fail_once):
            with _sent_commands() as sent:
                with r.pipeline() as pipe:
                    pipe.execute_command("DEL", *keys)
                    assert pipe.execute() == [len(keys)]

        assert failed
        # The held slot reached the server only on the retry, and no other slot twice
        assert sorted([*written, *(args for _, args, _ in sent)]) == sorted(
            ("DEL", key) for key in keys
        )
        assert r.exists(*keys) == 0


@pytest.mark.onlycluster
@skip_if_server_version_lt("8.0.0")
class TestSpecialResponses:
    """
    A ``response_policy:special`` command is merged by a handler of its own, which applies
    only when the command was resolved for the whole cluster - never to targets the caller
    picked, as named nodes or as a nodes flag.
    """

    def test_randomkey_answers_with_a_key_from_any_shard(self, r):
        assert r.randomkey() is None
        key = next(
            f"{{only}}{index}"
            for index in range(10000)
            if r.get_node_from_key(f"{{only}}{index}") != r.get_default_node()
        )
        r.set(key, 1)

        with _sent_commands() as sent:
            assert str_if_bytes(r.randomkey()) == key

        assert {node.name for node, _, _ in sent} == {
            node.name for node in r.get_primaries()
        }

    @pytest.mark.parametrize("targets", ["nodes", "flag"])
    def test_the_caller_s_targets_keep_each_node_s_reply(self, r, targets):
        primaries = r.get_primaries()
        target_nodes = primaries if targets == "nodes" else r.PRIMARIES

        result = r.randomkey(target_nodes=target_nodes)

        assert result == {node.name: None for node in primaries}


@pytest.mark.onlycluster
class TestReadOnlyEvalSlotResolution:
    """
    EVAL_RO and EVALSHA_RO take their keys after ``numkeys`` exactly like EVAL, so their slot
    is read from the arguments rather than asked of the server - and a script with no keys runs
    on any node instead of failing to dispatch.
    """

    @skip_if_server_version_lt("7.0.0")
    def test_the_keys_are_read_from_the_arguments(self, r):
        script = "return redis.call('GET', KEYS[1])"
        r.set("{foo}a", "value")
        sha = r.script_load(script)

        with patch.object(r, "_get_command_keys", wraps=r._get_command_keys) as keys:
            assert str_if_bytes(r.eval_ro(script, 1, "{foo}a")) == "value"
            assert str_if_bytes(r.evalsha_ro(sha, 1, "{foo}a")) == "value"

        keys.assert_not_called()

    @skip_if_server_version_lt("7.0.0")
    def test_a_script_without_keys_runs_on_any_node(self, r):
        sha = r.script_load("return 1")

        assert r.eval_ro("return 1", 0) == 1
        assert r.evalsha_ro(sha, 0) == 1


class TestSlotResolutionWithoutADefaultNode:
    """
    A keyed command on a client with no default node must name that as the reason.

    ``_get_command_keys`` resolves keys through the default node, and the static metadata table
    answers most keyed reads directly, so ``_internal_execute_command`` no longer passes through
    the fallback that used to substitute keyless routing when the default node was missing.
    ``NodesManager.close`` clears the default node, so this is reachable on a closed client.

    The keys are unknown rather than absent, so the error has to say so: reporting a missing key
    for ``GET foo`` sends the caller looking for a key they did supply.

    The async stack needs no counterpart: ``AsyncCommandsParser`` holds the node it was
    initialized with and never reads the default node.
    """

    @staticmethod
    def _cluster_without_a_default_node():
        rc = get_mocked_redis_client(host=default_host, port=7000)
        # What ``NodesManager.close`` leaves behind, without closing the connections.
        rc.nodes_manager.default_node = None
        return rc

    @pytest.mark.fixed_client
    def test_get_command_keys_reports_the_missing_default_node(self):
        rc = self._cluster_without_a_default_node()

        with pytest.raises(RedisClusterException, match="no default node") as excinfo:
            rc._get_command_keys("GET", "foo")

        # The command is named, and the key is not reported as missing.
        assert "GET" in str(excinfo.value)
        assert "Missing key" not in str(excinfo.value)

    @pytest.mark.fixed_client
    def test_a_keyed_command_raises_the_cluster_error(self):
        rc = self._cluster_without_a_default_node()

        # GET is answered by the static table as DEFAULT_KEYED, so it goes straight to
        # determine_slot rather than through the fallback that guards the default node.
        with pytest.raises(RedisClusterException, match="no default node"):
            rc.execute_command("GET", "foo")


class RecordingBackoff(AbstractBackoff):
    """A backoff that returns ``step * failures`` and records every call."""

    def __init__(self, step: float = 0.5):
        self.step = step
        self.computed = []
        self.resets = 0

    def reset(self):
        self.resets += 1

    def compute(self, failures: int) -> float:
        self.computed.append(failures)
        return self.step * failures


class TestTopologyRefreshPacing:
    """
    Error-triggered topology refreshes run at most once per
    _TOPOLOGY_REFRESH_MIN_INTERVAL_SECONDS, back off after failures as the retry
    backoff says, and retries wait as long as the retry backoff says.
    """

    @staticmethod
    def _nodes_manager():
        slots = [[0, 16383, ["127.0.0.1", 7000], ["127.0.0.1", 7003]]]
        patcher, nm = TestNodesManager._refresh_with_cluster_slots([slots] * 10)
        return patcher, nm

    @pytest.mark.fixed_client
    def test_refresh_is_due_after_the_interval(self):
        patcher, nm = self._nodes_manager()
        try:
            nm._last_refresh_at = 100.0
            with patch_monotonic("redis.cluster", 100.9):
                assert nm._refresh_due() is False
            with patch_monotonic("redis.cluster", 101.0):
                assert nm._refresh_due() is True
        finally:
            patcher.stop()

    @pytest.mark.fixed_client
    def test_failed_refreshes_wait_as_long_as_the_retry_backoff_says(self):
        patcher, nm = self._nodes_manager()
        try:
            backoff = RecordingBackoff(step=1.0)
            retry = Retry(backoff, 3)
            nm._last_refresh_at = 100.0
            nm._failed_refreshes = 3
            nm._refresh_due_at = None
            with patch_monotonic("redis.cluster", 102.9):
                assert nm._refresh_due(retry) is False
            with patch_monotonic("redis.cluster", 103.0):
                assert nm._refresh_due(retry) is True
            # Computed once per refresh: a jittered backoff would otherwise
            # give a new delay on every check
            assert backoff.computed == [3]

            # The minimum interval is the floor for a shorter backoff
            short_retry = Retry(RecordingBackoff(step=0.1), 3)
            nm._failed_refreshes = 1
            nm._refresh_due_at = None
            with patch_monotonic("redis.cluster", 100.9):
                assert nm._refresh_due(short_retry) is False
            with patch_monotonic("redis.cluster", 101.0):
                assert nm._refresh_due(short_retry) is True
        finally:
            patcher.stop()

    @pytest.mark.fixed_client
    def test_due_time_of_failed_refreshes_is_not_kept_after_a_success(self):
        """
        The due time computed after 3 failed refreshes must not hold back the
        refresh after a successful one for the backoff instead of the minimum
        interval.
        """
        patcher, nm = self._nodes_manager()
        try:
            retry = Retry(RecordingBackoff(step=10.0), 3)
            nm._last_refresh_at = 100.0
            nm._failed_refreshes = 3
            nm._refresh_due_at = None
            with patch_monotonic("redis.cluster", 101.0):
                assert nm._refresh_due(retry) is False
            assert nm._refresh_due_at == 130.0

            with patch_monotonic("redis.cluster", 110.0):
                nm.initialize()
            assert nm._failed_refreshes == 0
            with patch_monotonic("redis.cluster", 110.9):
                assert nm._refresh_due(retry) is False
            with patch_monotonic("redis.cluster", 111.0):
                assert nm._refresh_due(retry) is True
        finally:
            patcher.stop()

    @pytest.mark.fixed_client
    def test_retry_policy_without_a_backoff_gives_no_delay(self):
        """
        A duck-typed retry policy, such as an async-shaped one the async client
        keeps as is, may have no backoff: retries are not delayed, and failed
        refreshes wait only the minimum interval.
        """

        class DuckTypedRetry:
            async def call_with_retry(self, do, fail):
                return await do()

            def get_retries(self):
                return 3

        retry = DuckTypedRetry()
        assert RedisCluster._retry_backoff_delay(retry, 1) == 0
        assert RedisCluster._retry_backoff_delay(retry, 2) == 0

        patcher, nm = self._nodes_manager()
        try:
            nm._last_refresh_at = 100.0
            nm._failed_refreshes = 3
            nm._refresh_due_at = None
            with patch_monotonic("redis.cluster", 100.9):
                assert nm._refresh_due(retry) is False
            with patch_monotonic("redis.cluster", 101.0):
                assert nm._refresh_due(retry) is True
        finally:
            patcher.stop()

    @pytest.mark.fixed_client
    def test_refresh_outcome_is_recorded(self):
        before = [[0, 16383, ["127.0.0.1", 7000]]]
        patcher, nm = TestNodesManager._refresh_with_cluster_slots([before, before])
        try:
            assert nm._failed_refreshes == 0
            with patch.object(
                Redis, "execute_command", side_effect=ConnectionError("down")
            ):
                with pytest.raises(RedisClusterUnreachableError) as unreachable:
                    nm.initialize()
            assert nm._failed_refreshes == 1
            # Only the message is kept, not the error and the frames it refers to
            assert nm._last_refresh_unreachable_args == unreachable.value.args
            failed_at = nm._last_refresh_at

            nm.initialize()
            assert nm._failed_refreshes == 0
            assert nm._last_refresh_at >= failed_at
            assert nm._last_refresh_unreachable_args is None
        finally:
            patcher.stop()

    @pytest.mark.fixed_client
    def test_interrupted_refresh_is_not_counted_but_waits_the_interval(self):
        """
        A refresh interrupted before it ended (a BaseException, such as a gevent
        timeout) is not a failed refresh, but the next one still waits the
        interval, so callers that keep timing out do not refresh back to back.
        Its outcome is unknown, so it leaves no record of a refresh that
        reached no node either.
        """

        class Interrupted(BaseException):
            pass

        patcher, nm = self._nodes_manager()
        try:
            nm._last_refresh_unreachable_args = ("down",)
            with (
                patch.object(Redis, "execute_command", side_effect=Interrupted()),
                patch_monotonic("redis.cluster", 200.0),
            ):
                with pytest.raises(Interrupted):
                    nm.initialize()
            assert nm._failed_refreshes == 0
            assert nm._last_refresh_at == 200.0
            assert nm._last_refresh_unreachable_args is None
            with patch_monotonic("redis.cluster", 200.9):
                assert nm._refresh_due() is False
            with patch_monotonic("redis.cluster", 201.0):
                assert nm._refresh_due() is True
        finally:
            patcher.stop()

    @pytest.mark.fixed_client
    def test_requested_refresh_is_kept_when_it_raises_another_error(self):
        """
        A requested refresh that raises an error other than a failed refresh
        (for example a reply it cannot parse) raises it, and is still asked
        for, still trying the failed node last.
        """
        patcher, nm = self._nodes_manager()
        try:
            nm._request_refresh(last_failed_node_name="127.0.0.1:7000")
            nm._last_refresh_at = nm._refresh_due_at = None
            with patch.object(
                NodesManager,
                "initialize",
                autospec=True,
                side_effect=ValueError("bad reply"),
            ):
                with pytest.raises(ValueError):
                    nm._run_requested_refresh()
            assert nm._refresh_requested is True
            assert nm._requested_last_failed_node_name == "127.0.0.1:7000"
        finally:
            patcher.stop()

    @pytest.mark.fixed_client
    def test_refresh_registers_a_snapshot_of_the_published_nodes(self):
        """
        The refresh registers the nodes it published from a snapshot: another
        thread that moves a failed node to the end of the published cache
        meanwhile, which pops it and inserts it again, cannot break it.
        """
        patcher, nm = self._nodes_manager()
        try:
            dispatch = nm._dispatch_pools_instantiated
            registered = []

            def dispatch_while_a_node_moves(nodes):
                for node in nodes:
                    # As another thread halfway through moving the node
                    moved = nm.nodes_cache.pop(node.name)
                    registered.append(node.name)
                    nm.nodes_cache[node.name] = moved
                dispatch(nodes)

            with patch.object(
                nm,
                "_dispatch_pools_instantiated",
                side_effect=dispatch_while_a_node_moves,
            ):
                nm.initialize()
            assert sorted(registered) == sorted(nm.nodes_cache)
        finally:
            patcher.stop()

    @pytest.mark.fixed_client
    def test_refresh_does_not_release_a_node_another_thread_is_moving(self):
        """
        The nodes a refresh releases are the ones missing from the topology it
        published, taken under the lock: a node of that topology that another
        thread is halfway through moving to the end of the published cache
        (popped, not inserted again yet) while the removed nodes are released
        keeps its connections.
        """
        patcher, nm = self._nodes_manager()
        try:
            release = nm._release_removed_nodes
            moved = []

            def release_while_a_node_moves(old_nodes_cache, new_node_names):
                # As another thread that popped a node of the new topology and
                # has not inserted it again yet
                name = next(iter(nm.nodes_cache))
                node = nm.nodes_cache.pop(name)
                moved.append(node)
                try:
                    release(old_nodes_cache, new_node_names)
                finally:
                    nm.nodes_cache[name] = node

            with (
                patch.object(
                    nm,
                    "_release_removed_nodes",
                    side_effect=release_while_a_node_moves,
                ),
                patch.object(NodesManager, "_free_pool_connections") as free,
            ):
                nm.initialize()
            freed = [c.args[0] for c in free.call_args_list]
            (node,) = moved
            assert node.redis_connection.connection_pool not in freed
        finally:
            patcher.stop()

    @pytest.mark.fixed_client
    def test_moving_a_failed_node_to_the_end_takes_the_lock(self):
        """
        The error paths of many threads move a failed node to the end of the
        caches at once; each move runs under the lock a refresh publishes the
        caches under.
        """

        class RecordingLock:
            def __init__(self):
                self._lock = threading.RLock()
                self.entered = 0

            def __enter__(self):
                self.entered += 1
                return self._lock.__enter__()

            def __exit__(self, *exc_info):
                return self._lock.__exit__(*exc_info)

        patcher, nm = self._nodes_manager()
        try:
            nm._lock = RecordingLock()
            nm.move_node_to_end_of_cached_nodes("127.0.0.1:7000")
            assert nm._lock.entered == 1
            assert list(nm.nodes_cache)[-1] == "127.0.0.1:7000"
        finally:
            patcher.stop()

    @pytest.mark.fixed_client
    def test_requests_are_merged_and_run_once_when_due(self):
        patcher, nm = self._nodes_manager()
        try:
            nm._last_refresh_at = nm._refresh_due_at = None
            nm._request_refresh(
                additional_startup_nodes_info=[("127.0.0.1", 7001)],
                last_failed_node_name="127.0.0.1:7000",
            )
            nm._request_refresh(
                additional_startup_nodes_info=[("127.0.0.1", 7002)],
                disconnect_startup_nodes_pools=False,
            )
            with patch.object(NodesManager, "initialize", autospec=True) as init:
                nm._run_requested_refresh()
                nm._run_requested_refresh()

            init.assert_called_once_with(
                nm,
                additional_startup_nodes_info=[
                    ("127.0.0.1", 7001),
                    ("127.0.0.1", 7002),
                ],
                disconnect_startup_nodes_pools=False,
                last_failed_node_name="127.0.0.1:7000",
            )
        finally:
            patcher.stop()

    @pytest.mark.fixed_client
    def test_request_waits_while_not_due_and_is_dropped_after_a_newer_refresh(self):
        patcher, nm = self._nodes_manager()
        try:
            nm._request_refresh()
            with patch.object(NodesManager, "initialize", autospec=True) as init:
                # The client was just created, so the interval has not passed
                nm._run_requested_refresh()
                init.assert_not_called()
                assert nm._refresh_requested is True

                # A refresh that completes after the request answers it
                nm._epoch += 1
                nm._last_refresh_at = nm._refresh_due_at = None
                nm._run_requested_refresh()
                init.assert_not_called()
                assert nm._refresh_requested is False
        finally:
            patcher.stop()

    @pytest.mark.fixed_client
    @pytest.mark.parametrize(
        "error",
        [
            RedisClusterException("All slots are not covered"),
            RedisClusterUnreachableError("down"),
        ],
    )
    def test_requested_refresh_with_an_unusable_topology_keeps_the_current_one(
        self, error
    ):
        """
        A requested refresh that reaches no node, or whose nodes answer with a
        topology that cannot replace the current one, does not fail the command
        that runs it, and is asked for again with the same arguments.
        """
        patcher, nm = self._nodes_manager()
        try:
            nm._last_refresh_at = nm._refresh_due_at = None
            nm._request_refresh(
                additional_startup_nodes_info=[("127.0.0.1", 7001)],
                last_failed_node_name="127.0.0.1:7000",
                disconnect_startup_nodes_pools=False,
            )
            with patch.object(
                NodesManager,
                "initialize",
                autospec=True,
                side_effect=error,
            ) as init:
                nm._run_requested_refresh()

            init.assert_called_once()
            assert nm._refresh_requested is True
            assert nm._requested_additional_startup_nodes == [("127.0.0.1", 7001)]
            assert nm._requested_last_failed_node_name == "127.0.0.1:7000"
            assert nm._requested_disconnect_startup_nodes_pools is False
        finally:
            patcher.stop()

    @staticmethod
    def _failing_then_ok(failures):
        calls = []

        def execute_command(target_node, *args, **kwargs):
            calls.append(target_node.name)
            if len(calls) <= failures:
                raise ConnectionError("down")
            return b"value"

        return execute_command, calls

    @staticmethod
    def _checkout_failing_then_ok(failures):
        """
        Patch the connection checkout to fail ``failures`` times with a
        connection error, then hand out a connection whose reply is b"value".
        """
        calls = []

        def get_connection(redis_node):
            calls.append(redis_node)
            if len(calls) <= failures:
                raise ConnectionError("down")
            return Mock()

        return (
            patch("redis.cluster.get_connection", side_effect=get_connection),
            patch.object(Redis, "parse_response", return_value=b"value"),
            calls,
        )

    @pytest.mark.fixed_client
    @pytest.mark.usefixtures("no_cluster_retry_delay")
    def test_connection_error_refreshes_before_the_next_attempt_when_due(self):
        rc = get_mocked_redis_client(host=default_host, port=default_port)
        rc.nodes_manager._last_refresh_at = rc.nodes_manager._refresh_due_at = None
        checkout, parse, calls = self._checkout_failing_then_ok(1)
        with (
            checkout,
            parse,
            patch.object(NodesManager, "initialize", autospec=True) as init,
        ):
            assert rc.get("foo") == b"value"

        assert len(calls) == 2
        init.assert_called_once()
        assert init.call_args.kwargs["last_failed_node_name"] is not None

    @pytest.mark.fixed_client
    @pytest.mark.usefixtures("no_cluster_retry_delay")
    def test_refresh_not_due_runs_on_the_next_command_once_due(self):
        rc = get_mocked_redis_client(host=default_host, port=default_port)
        checkout, parse, calls = self._checkout_failing_then_ok(1)
        with (
            checkout,
            parse,
            patch.object(NodesManager, "initialize", autospec=True) as init,
        ):
            # The client was just created: the retry goes out without a refresh
            assert rc.get("foo") == b"value"
            init.assert_not_called()
            assert rc.nodes_manager._refresh_requested is True

            # Once due, the next command runs the requested refresh first,
            # even though it does not fail itself
            rc.nodes_manager._last_refresh_at = rc.nodes_manager._refresh_due_at = None
            assert rc.get("foo") == b"value"
            init.assert_called_once()

    @pytest.mark.fixed_client
    @pytest.mark.parametrize("error", [ClusterDownError("down"), SlotNotCoveredError()])
    def test_cluster_down_refreshes_at_once_and_raises_a_failed_refresh(self, error):
        """
        A CLUSTERDOWN or an uncovered slot refreshes the topology at once, also
        when an error-triggered refresh is not due yet, and the command raises
        a refresh that fails.
        """
        rc = get_mocked_redis_client(host=default_host, port=default_port)
        nm = rc.nodes_manager
        # The client was just created: an error-triggered refresh is not due
        assert not nm._refresh_due(rc.retry)
        with (
            patch("redis.cluster.get_connection", return_value=Mock()),
            patch.object(Redis, "parse_response", side_effect=error),
            patch.object(
                NodesManager,
                "initialize",
                autospec=True,
                side_effect=RedisClusterUnreachableError("down"),
            ) as init,
            patch("redis.cluster.time.sleep"),
        ):
            with pytest.raises(RedisClusterUnreachableError):
                rc.get("foo")

        init.assert_called_once()
        assert nm._refresh_requested is False

    @pytest.mark.fixed_client
    def test_command_runs_on_the_current_topology_when_no_node_answers_a_refresh(
        self,
    ):
        """
        A due refresh that reaches no node (e.g. only seeds are queried, with
        dynamic_startup_nodes=False) is logged and asked for again, and the
        command that ran it still goes to its node of the current topology.
        """
        rc = get_mocked_redis_client(host=default_host, port=default_port)
        nm = rc.nodes_manager
        nm._request_refresh()
        nm._last_refresh_at = nm._refresh_due_at = None
        checkout, parse, calls = self._checkout_failing_then_ok(0)
        with (
            checkout,
            parse,
            patch.object(
                NodesManager,
                "initialize",
                autospec=True,
                side_effect=RedisClusterUnreachableError("down"),
            ) as init,
        ):
            assert rc.get("foo") == b"value"

        init.assert_called_once()
        assert len(calls) == 1
        assert nm._refresh_requested is True

    @pytest.mark.fixed_client
    @pytest.mark.usefixtures("no_cluster_retry_delay")
    @pytest.mark.parametrize("unreachable", [True, False], ids=["unreachable", "up"])
    @pytest.mark.parametrize("call", ["command", "pipeline", "transaction", "watch"])
    def test_out_of_retries_raises_unreachable_error_while_no_node_answers(
        self, call, unreachable
    ):
        """
        A command, pipeline, transaction or WATCH that runs out of retries on
        connection errors raises RedisClusterUnreachableError, with its own
        error as the cause, while the last refresh reached no node; and its own
        error otherwise.
        """
        # Transactions and WATCH retry with the backoff of their own copy of
        # the retry
        rc = get_mocked_redis_client(
            host=default_host, port=default_port, retry=Retry(NoBackoff(), 3)
        )
        if unreachable:
            rc.nodes_manager._last_refresh_unreachable_args = (
                "Redis Cluster cannot be connected",
            )
        checkout, parse, calls = self._checkout_failing_then_ok(100)
        with (
            checkout,
            parse,
            patch.object(NodesManager, "initialize", autospec=True),
            # The slot of WATCH comes from COMMAND GETKEYS otherwise, which
            # this client has no server for
            patch.object(
                ClusterPipeline, "determine_slot", return_value=rc.keyslot("foo")
            ),
        ):
            with pytest.raises(
                RedisClusterUnreachableError if unreachable else ConnectionError
            ) as raised:
                if call == "command":
                    rc.get("foo")
                elif call == "watch":
                    with rc.pipeline(transaction=True) as pipe:
                        pipe.watch("foo")
                else:
                    rc.pipeline(transaction=call == "transaction").get("foo").execute()

        assert len(calls) > 1
        if unreachable:
            assert str(raised.value) == "Redis Cluster cannot be connected"
            assert type(raised.value.__cause__) is ConnectionError
        else:
            assert type(raised.value) is ConnectionError

    @pytest.mark.fixed_client
    @pytest.mark.usefixtures("no_cluster_retry_delay", "no_topology_refresh_interval")
    @pytest.mark.parametrize("call", ["command", "pipeline", "transaction", "watch"])
    def test_retry_gives_up_once_a_refresh_since_its_error_reached_no_node(self, call):
        """
        The refresh a connection error asks for runs on the next attempt and
        reaches no node: the slot map that attempt would retry on is the one
        the error came from, so the command, pipeline, transaction or WATCH
        gives up at once with RedisClusterUnreachableError, with its own error
        as the cause, instead of using up its retries.
        """
        rc = get_mocked_redis_client(
            host=default_host,
            port=default_port,
            retry=Retry(NoBackoff(), 3),
            # A transaction asks for the refresh at this threshold
            reinitialize_steps=1,
        )
        nm = rc.nodes_manager
        checkout, parse, calls = self._checkout_failing_then_ok(100)
        with (
            checkout,
            parse,
            # Every node fails the CLUSTER SLOTS of the refresh too
            patch.object(Redis, "execute_command", side_effect=ConnectionError("down")),
            # The slot of WATCH comes from COMMAND GETKEYS otherwise, which
            # this client has no server for
            patch.object(
                ClusterPipeline, "determine_slot", return_value=rc.keyslot("foo")
            ),
        ):
            with pytest.raises(RedisClusterUnreachableError) as raised:
                if call == "command":
                    rc.get("foo")
                elif call == "watch":
                    with rc.pipeline(transaction=True) as pipe:
                        pipe.watch("foo")
                else:
                    rc.pipeline(transaction=call == "transaction").get("foo").execute()

        # One attempt failed, the refresh it asked for reached no node, and no
        # further attempt was made
        assert len(calls) == 1
        assert nm._failed_refreshes == 1
        assert "Redis Cluster cannot be connected" in str(raised.value)
        assert type(raised.value.__cause__) is ConnectionError

    @pytest.mark.fixed_client
    def test_retries_wait_as_long_as_the_retry_backoff_says(self):
        backoff = RecordingBackoff(step=0.001)
        rc = get_mocked_redis_client(
            host=default_host, port=default_port, retry=Retry(backoff, 3)
        )
        execute_command, calls = self._failing_then_ok(3)
        with (
            patch.object(RedisCluster, "_execute_command", side_effect=execute_command),
            patch("redis.cluster.time.sleep") as sleep,
        ):
            assert rc.get("foo") == b"value"

        assert len(calls) == 4
        assert [c.args[0] for c in sleep.call_args_list] == [0.001, 0.002, 0.003]
        # A new run of failures starts from a reset backoff
        assert backoff.resets == 1

    @pytest.mark.fixed_client
    @pytest.mark.parametrize("due", [True, False])
    def test_moved_threshold_asks_for_refresh_run_once_due(self, due):
        """
        At the reinitialize_steps MOVED threshold the MOVED slot is patched and
        the refresh is only asked for, also when one is due: the next command
        runs it, once due.
        """
        rc = get_mocked_redis_client(
            host=default_host, port=default_port, reinitialize_steps=1
        )
        if due:
            rc.nodes_manager._last_refresh_at = rc.nodes_manager._refresh_due_at = None
        slot = rc.keyslot("foo")
        owner = rc.get_node_from_key("foo")
        responses = [
            MovedError(f"{slot} {owner.host}:{owner.port}"),
            b"value",
            b"value",
        ]

        def parse_response(connection, command, **kwargs):
            response = responses.pop(0)
            if isinstance(response, Exception):
                raise response
            return response

        with (
            patch("redis.cluster.get_connection"),
            patch.object(Redis, "parse_response", side_effect=parse_response),
            patch.object(NodesManager, "initialize", autospec=True) as init,
            patch.object(NodesManager, "move_slot", autospec=True) as move_slot,
        ):
            assert rc.get("foo") == b"value"

            init.assert_not_called()
            move_slot.assert_called_once()
            assert rc.nodes_manager._refresh_requested
            assert rc.nodes_manager._requested_additional_startup_nodes == [
                (owner.host, owner.port)
            ]

            assert rc.get("foo") == b"value"

        if due:
            init.assert_called_once()
            assert init.call_args.kwargs["additional_startup_nodes_info"] == [
                (owner.host, owner.port)
            ]
            assert not rc.nodes_manager._refresh_requested
        else:
            init.assert_not_called()
            # Still asked for, so a later command runs it once due
            assert rc.nodes_manager._refresh_requested

    @pytest.mark.fixed_client
    @pytest.mark.parametrize("due", [True, False])
    @pytest.mark.parametrize("redirect", [True, False])
    def test_transaction_threshold_asks_for_a_refresh(self, due, redirect):
        """
        At the reinitialize_steps threshold an error in a transaction only asks
        for a refresh, also when one is due: the next attempt runs it through
        _run_requested_refresh, which keeps the current topology if the refresh
        fails. A MOVED patches the slot; a connection error asks for the failed
        node to be tried last.
        """
        rc = get_mocked_redis_client(
            host=default_host, port=default_port, reinitialize_steps=1
        )
        nm = rc.nodes_manager
        if due:
            nm._last_refresh_at = nm._refresh_due_at = None
        strategy = rc.pipeline(transaction=True)._execution_strategy
        owner = rc.get_node_from_key("foo")
        error = (
            MovedError(f"{rc.keyslot('foo')} {owner.host}:{owner.port}")
            if redirect
            else ConnectionError("down")
        )
        with (
            patch.object(NodesManager, "initialize", autospec=True) as init,
            patch.object(NodesManager, "move_slot", autospec=True) as move_slot,
        ):
            strategy._pipeline_slots.add(rc.keyslot("foo"))
            strategy._reinitialize_on_error(error, 1)

        assert move_slot.called is redirect
        init.assert_not_called()
        assert nm._refresh_requested is True
        assert rc.reinitialize_counter == 0
        assert nm._requested_last_failed_node_name == (None if redirect else owner.name)

    @pytest.mark.fixed_client
    @pytest.mark.parametrize("watch", [False, True])
    def test_transaction_attempt_runs_a_requested_refresh(self, watch):
        """
        A refresh an earlier error asked for runs when a transaction attempt
        starts, once due, so the retries do not keep hitting the stale owner.
        """
        rc = get_mocked_redis_client(host=default_host, port=default_port)
        nm = rc.nodes_manager
        nm._request_refresh()
        nm._last_refresh_at = nm._refresh_due_at = None
        strategy = rc.pipeline(transaction=True)._execution_strategy
        with (
            patch.object(NodesManager, "initialize", autospec=True) as init,
            # Stop the attempt right after its refresh check
            patch.object(
                type(strategy),
                "_get_client_and_connection_for_transaction",
                side_effect=RuntimeError("stop"),
            ),
        ):
            with pytest.raises(RuntimeError, match="stop"):
                if watch:
                    strategy._get_connection_and_send_command("WATCH", "foo")
                else:
                    strategy._execute_transaction([], raise_on_error=True)

        init.assert_called_once()
        assert nm._refresh_requested is False

    @pytest.mark.fixed_client
    def test_pipeline_retries_wait_as_long_as_the_retry_backoff_says(self):
        backoff = RecordingBackoff(step=0.001)
        rc = get_mocked_redis_client(
            host=default_host, port=default_port, retry=Retry(backoff, 2)
        )
        pipe = rc.pipeline()
        strategy = pipe._execution_strategy
        attempts = []

        def send(stack, raise_on_error=True, allow_redirections=True):
            attempts.append(1)
            if len(attempts) <= 2:
                raise ConnectionError("down")
            return [b"value"]

        with (
            patch.object(strategy, "_send_cluster_commands", side_effect=send),
            patch("redis.cluster.time.sleep") as sleep,
        ):
            assert pipe.get("foo").execute() == [b"value"]

        assert len(attempts) == 3
        assert [c.args[0] for c in sleep.call_args_list] == [0.001, 0.002]

    @pytest.mark.fixed_client
    @pytest.mark.parametrize("error", [ClusterDownError("down"), SlotNotCoveredError()])
    def test_pipeline_waits_for_the_cluster_before_retrying_a_cluster_down_error(
        self, error
    ):
        """
        A pipeline that failed with CLUSTERDOWN or an uncovered slot waits the
        fixed 0.25 seconds a command does before its retry, and the backoff
        delay after it.
        """
        rc = get_mocked_redis_client(
            host=default_host,
            port=default_port,
            retry=Retry(RecordingBackoff(step=0.001), 2),
        )
        pipe = rc.pipeline()
        strategy = pipe._execution_strategy
        attempts = []

        def send(stack, raise_on_error=True, allow_redirections=True):
            attempts.append(1)
            if len(attempts) == 1:
                raise error
            return [b"value"]

        with (
            patch.object(strategy, "_send_cluster_commands", side_effect=send),
            patch("redis.cluster.time.sleep") as sleep,
        ):
            assert pipe.get("foo").execute() == [b"value"]

        assert len(attempts) == 2
        assert [c.args[0] for c in sleep.call_args_list] == [0.25, 0.001]

    @pytest.mark.fixed_client
    @pytest.mark.usefixtures("no_cluster_retry_delay")
    @pytest.mark.parametrize("error", [ClusterDownError("down"), SlotNotCoveredError()])
    def test_pipeline_retry_asks_for_a_refresh_run_once_due(self, error):
        """
        A pipeline that failed with CLUSTERDOWN or an uncovered slot asks for a
        topology refresh, which its next attempt runs once due, instead of
        retrying on a slot map only another error could have replaced.
        """
        rc = get_mocked_redis_client(host=default_host, port=default_port)
        nm = rc.nodes_manager
        nm._last_refresh_at = nm._refresh_due_at = None
        pipe = rc.pipeline()
        strategy = pipe._execution_strategy
        attempts = []

        def send(stack, raise_on_error=True, allow_redirections=True):
            attempts.append(1)
            if len(attempts) == 1:
                raise error
            return [b"value"]

        with (
            patch.object(strategy, "_send_cluster_commands", side_effect=send),
            patch.object(NodesManager, "initialize", autospec=True) as init,
            patch("redis.cluster.time.sleep"),
        ):
            assert pipe.get("foo").execute() == [b"value"]

        assert len(attempts) == 2
        init.assert_called_once()

    @pytest.mark.fixed_client
    @pytest.mark.parametrize("due", [True, False])
    def test_pipeline_fallback_threshold_runs_or_asks_for_refresh(self, due):
        """
        At the reinitialize_steps threshold the pipeline asks for a refresh and
        runs it before resending the failed commands, once due; when it is not
        due it stays asked for instead of being dropped.
        """
        rc = get_mocked_redis_client(
            host=default_host, port=default_port, reinitialize_steps=1
        )
        nm = rc.nodes_manager
        if due:
            nm._last_refresh_at = nm._refresh_due_at = None
        owner = rc.get_node_from_key("foo")
        moved = MovedError(f"{rc.keyslot('foo')} {owner.host}:{owner.port}")
        pipe = rc.pipeline()
        with (
            patch("redis.cluster.get_connection"),
            patch.object(Redis, "parse_response", side_effect=moved),
            patch.object(NodesManager, "initialize", autospec=True) as init,
            patch.object(
                pipe, "parent_execute_command", return_value=b"value"
            ) as resend,
        ):
            assert pipe.get("foo").execute() == [b"value"]

        resend.assert_called_once()
        if due:
            init.assert_called_once()
            assert nm._refresh_requested is False
        else:
            init.assert_not_called()
            assert nm._refresh_requested is True

    @pytest.mark.fixed_client
    def test_pipeline_connection_error_tries_the_failed_node_last(self):
        """
        A node the pipeline cannot connect to moves to the end of the cached
        nodes, and the refresh asked for tries it last.
        """
        rc = get_mocked_redis_client(
            host=default_host, port=default_port, retry=Retry(NoBackoff(), 0)
        )
        nm = rc.nodes_manager
        failed = rc.get_node_from_key("foo")
        assert list(nm.nodes_cache)[-1] != failed.name
        with (
            patch("redis.cluster.get_connection", side_effect=ConnectionError("down")),
            patch.object(NodesManager, "initialize", autospec=True),
        ):
            with pytest.raises(ConnectionError):
                rc.pipeline().get("foo").execute()

        assert list(nm.nodes_cache)[-1] == failed.name
        assert nm._requested_last_failed_node_name == failed.name

    @pytest.mark.fixed_client
    def test_pubsub_topology_repair_is_a_paced_request(self):
        rc = get_mocked_redis_client(host=default_host, port=default_port)
        pubsub = rc.pubsub()
        pubsub.shard_channels = {b"channel": None}
        with patch.object(pubsub, "_submit_reconcile_work") as submit:
            pubsub._schedule_topology_repair()

        nm = rc.nodes_manager
        assert nm._refresh_requested is True
        assert nm._requested_disconnect_startup_nodes_pools is False
        (work,), _ = submit.call_args
        assert work.func == nm._run_requested_refresh
        assert work.args == (rc.retry,)


class TestPipelineNonNodeFailureErrors:
    """
    A full client pool, a server at its maxclients limit or rejected
    credentials are not node failures: commands, pipelines and transactions
    re-raise them without refreshing the topology and without a retry.
    """

    ERRORS = [
        MaxConnectionsError("pool exhausted"),
        MaxClientsReachedError("max number of clients reached"),
        AuthenticationError("invalid password"),
    ]
    IDS = ["max_connections", "max_clients_reached", "authentication"]

    @pytest.mark.fixed_client
    @pytest.mark.parametrize("transaction", [False, True], ids=["pipeline", "tx"])
    @pytest.mark.parametrize("error", ERRORS, ids=IDS)
    def test_pipeline_does_not_refresh_or_retry(self, error, transaction):
        r = get_mocked_redis_client(host=default_host, port=default_port)
        with (
            patch("redis.cluster.get_connection", side_effect=error) as checkout,
            patch.object(NodesManager, "initialize", autospec=True) as initialize,
        ):
            with pytest.raises(type(error)):
                r.pipeline(transaction=transaction).get("foo").execute()

        assert checkout.call_count == 1
        initialize.assert_not_called()

    @pytest.mark.fixed_client
    @pytest.mark.parametrize("error", ERRORS, ids=IDS)
    def test_command_does_not_refresh_or_retry(self, error):
        r = get_mocked_redis_client(host=default_host, port=default_port)
        with (
            patch("redis.cluster.get_connection", side_effect=error) as checkout,
            patch.object(NodesManager, "initialize", autospec=True) as initialize,
        ):
            with pytest.raises(type(error)):
                r.get("foo")

        assert checkout.call_count == 1
        initialize.assert_not_called()
        assert r.nodes_manager._refresh_requested is False


class TestNodesManager:
    """
    Tests for the NodesManager class
    """

    @pytest.mark.onlycluster
    def test_load_balancer(self, r):
        n_manager = r.nodes_manager
        lb = n_manager.read_load_balancer
        slot_1 = 1257
        slot_2 = 8975
        node_1 = ClusterNode(default_host, 6379, PRIMARY)
        node_2 = ClusterNode(default_host, 6378, REPLICA)
        node_3 = ClusterNode(default_host, 6377, REPLICA)
        node_4 = ClusterNode(default_host, 6376, PRIMARY)
        node_5 = ClusterNode(default_host, 6375, REPLICA)
        n_manager.slots_cache = {
            slot_1: [node_1, node_2, node_3],
            slot_2: [node_4, node_5],
        }
        primary1_name = n_manager.slots_cache[slot_1][0].name
        primary2_name = n_manager.slots_cache[slot_2][0].name
        list1_size = len(n_manager.slots_cache[slot_1])
        list2_size = len(n_manager.slots_cache[slot_2])

        # default load balancer strategy: LoadBalancerStrategy.ROUND_ROBIN
        # slot 1
        assert lb.get_server_index(primary1_name, list1_size) == 0
        assert lb.get_server_index(primary1_name, list1_size) == 1
        assert lb.get_server_index(primary1_name, list1_size) == 2
        assert lb.get_server_index(primary1_name, list1_size) == 0
        # slot 2
        assert lb.get_server_index(primary2_name, list2_size) == 0
        assert lb.get_server_index(primary2_name, list2_size) == 1
        assert lb.get_server_index(primary2_name, list2_size) == 0

        lb.reset()
        assert lb.get_server_index(primary1_name, list1_size) == 0
        assert lb.get_server_index(primary2_name, list2_size) == 0

        # reset the indexes before load balancing strategy test
        lb.reset()
        # load balancer strategy: LoadBalancerStrategy.ROUND_ROBIN_REPLICAS
        for i in [1, 2, 1]:
            srv_index = lb.get_server_index(
                primary1_name,
                list1_size,
                load_balancing_strategy=LoadBalancingStrategy.ROUND_ROBIN_REPLICAS,
            )
            assert srv_index == i

        # reset the indexes before load balancing strategy test
        lb.reset()  # reset the indexes
        # load balancer strategy: LoadBalancerStrategy.RANDOM_REPLICA
        for i in range(5):
            srv_index = lb.get_server_index(
                primary1_name,
                list1_size,
                load_balancing_strategy=LoadBalancingStrategy.RANDOM_REPLICA,
            )

            assert srv_index > 0 and srv_index <= 2

    @pytest.mark.fixed_client
    def test_init_slots_cache_not_all_slots_covered(self):
        """
        Test that if not all slots are covered it should raise an exception
        """
        # Missing slot 5460
        cluster_slots = [
            [0, 5459, ["127.0.0.1", 7000], ["127.0.0.1", 7003]],
            [5461, 10922, ["127.0.0.1", 7001], ["127.0.0.1", 7004]],
            [10923, 16383, ["127.0.0.1", 7002], ["127.0.0.1", 7005]],
        ]
        with pytest.raises(RedisClusterException) as ex:
            get_mocked_redis_client(
                host=default_host,
                port=default_port,
                cluster_slots=cluster_slots,
                require_full_coverage=True,
            )
        assert str(ex.value).startswith(
            "All slots are not covered after query all startup_nodes."
        )

    @pytest.mark.fixed_client
    def test_init_slots_cache_not_require_full_coverage_success(self):
        """
        When require_full_coverage is set to False and not all slots are
        covered the cluster client initialization should succeed
        """
        # Missing slot 5460
        cluster_slots = [
            [0, 5459, ["127.0.0.1", 7000], ["127.0.0.1", 7003]],
            [5461, 10922, ["127.0.0.1", 7001], ["127.0.0.1", 7004]],
            [10923, 16383, ["127.0.0.1", 7002], ["127.0.0.1", 7005]],
        ]

        rc = get_mocked_redis_client(
            host=default_host,
            port=default_port,
            cluster_slots=cluster_slots,
            require_full_coverage=False,
        )

        assert 5460 not in rc.nodes_manager.slots_cache

    @pytest.mark.fixed_client
    def test_init_slots_cache(self):
        """
        Test that slots cache can in initialized and all slots are covered
        """
        good_slots_resp = [
            [0, 5460, ["127.0.0.1", 7000], ["127.0.0.2", 7003]],
            [5461, 10922, ["127.0.0.1", 7001], ["127.0.0.2", 7004]],
            [10923, 16383, ["127.0.0.1", 7002], ["127.0.0.2", 7005]],
        ]

        rc = get_mocked_redis_client(
            host=default_host, port=default_port, cluster_slots=good_slots_resp
        )
        n_manager = rc.nodes_manager
        assert len(n_manager.slots_cache) == REDIS_CLUSTER_HASH_SLOTS
        for slot_info in good_slots_resp:
            all_hosts = ["127.0.0.1", "127.0.0.2"]
            all_ports = [7000, 7001, 7002, 7003, 7004, 7005]
            slot_start = slot_info[0]
            slot_end = slot_info[1]
            for i in range(slot_start, slot_end + 1):
                assert len(n_manager.slots_cache[i]) == len(slot_info[2:])
                assert n_manager.slots_cache[i][0].host in all_hosts
                assert n_manager.slots_cache[i][1].host in all_hosts
                assert n_manager.slots_cache[i][0].port in all_ports
                assert n_manager.slots_cache[i][1].port in all_ports

        assert len(n_manager.nodes_cache) == 6

    @pytest.mark.onlycluster
    def test_initialize_uses_shuffled_startup_nodes(self, r):
        startup_nodes = list(r.nodes_manager.startup_nodes.values())
        first_shuffled_node = startup_nodes[-1]
        redis_connection = first_shuffled_node.redis_connection

        with (
            patch(
                "redis.cluster.random.shuffle",
                side_effect=lambda nodes: nodes.reverse(),
            ) as shuffle,
            patch.object(
                redis_connection,
                "execute_command",
                wraps=redis_connection.execute_command,
            ) as execute_command,
        ):
            r.nodes_manager.initialize(disconnect_startup_nodes_pools=False)

        shuffle.assert_called_once()
        execute_command.assert_any_call("CLUSTER SLOTS")

    @pytest.mark.fixed_client
    @pytest.mark.skipif(
        platform.python_implementation() == "PyPy",
        reason="Immediate reclamation is a refcounting property",
    )
    def test_initialize_breaks_reference_cycle_when_a_later_startup_node_answers(
        self,
    ):
        """
        When a startup node fails but a later one answers, initialize() must
        not return while still holding the caught exception: its traceback
        refers to the initialize() frame, so the pair would stay alive, with
        the slots built by that call, until the cyclic GC runs.
        """
        failing_connection, working_connection = MagicMock(), MagicMock()
        # The constructor calls initialize(), so the failing node answers here;
        # the failure is set up below, once the manager exists.
        failing_connection.execute_command.return_value = default_cluster_slots
        working_connection.execute_command.return_value = default_cluster_slots
        nodes_manager = NodesManager(
            startup_nodes=[
                ClusterNode("127.0.0.1", 7000, redis_connection=failing_connection),
                ClusterNode("127.0.0.1", 7001, redis_connection=working_connection),
            ],
            from_url=False,
            dynamic_startup_nodes=False,
        )

        # Raise from a factory so the test itself never holds a strong
        # reference; the weakref is the only handle on the failure.
        raised_ref = []

        def fail_cluster_slots(*args, **kwargs):
            err = ConnectionError("Connection refused")
            raised_ref.append(weakref.ref(err))
            try:
                raise err
            finally:
                # Clear this frame's local too, or the traceback keeps the
                # exception alive here and the assertion measures the test
                # rather than initialize().
                err = None

        failing_connection.execute_command.side_effect = fail_cluster_slots

        gc_was_enabled = gc.isenabled()
        gc.disable()
        try:
            # Keep the startup order: the failing node is tried first.
            with patch("redis.cluster.random.shuffle"):
                nodes_manager.initialize()
            (ref,) = raised_ref
            assert ref() is None
        finally:
            if gc_was_enabled:
                gc.enable()

    @pytest.mark.fixed_client
    @pytest.mark.skipif(
        platform.python_implementation() == "PyPy",
        reason="Immediate reclamation is a refcounting property",
    )
    def test_initialize_breaks_reference_cycle_when_a_later_startup_node_fails_after_a_partial_answer(
        self,
    ):
        """
        When a startup node answers with a partial slot map and a later one
        fails, initialize() must not store the later error: nothing raises it
        once a node has answered, and its traceback refers to the
        initialize() frame, so the pair would stay alive, with the slots
        built by that call, until the cyclic GC runs.
        """
        partial_connection, failing_connection = MagicMock(), MagicMock()
        partial_slots = default_cluster_slots[:1]
        # The constructor calls initialize(), so both nodes are given an answer
        # here; the failure is set up below, once the manager exists.
        partial_connection.execute_command.return_value = partial_slots
        failing_connection.execute_command.return_value = default_cluster_slots
        nodes_manager = NodesManager(
            startup_nodes=[
                ClusterNode("127.0.0.1", 7000, redis_connection=partial_connection),
                ClusterNode("127.0.0.1", 7001, redis_connection=failing_connection),
            ],
            from_url=False,
            require_full_coverage=False,
            dynamic_startup_nodes=False,
        )

        # Raise from a factory so the test itself never holds a strong
        # reference; the weakref is the only handle on the failure.
        raised_ref = []

        def fail_cluster_slots(*args, **kwargs):
            err = ConnectionError("Connection refused")
            raised_ref.append(weakref.ref(err))
            try:
                raise err
            finally:
                # Clear this frame's local too, or the traceback keeps the
                # exception alive here and the assertion measures the test
                # rather than initialize().
                err = None

        failing_connection.execute_command.side_effect = fail_cluster_slots

        gc_was_enabled = gc.isenabled()
        gc.disable()
        try:
            # Keep the startup order: the partial node is tried first, so the
            # slots are not fully covered and the failing node is tried next.
            with patch("redis.cluster.random.shuffle"):
                nodes_manager.initialize()
            (ref,) = raised_ref
            assert ref() is None
        finally:
            if gc_was_enabled:
                gc.enable()

    @pytest.mark.fixed_client
    @pytest.mark.parametrize(
        "error_type, raised_type",
        [
            (ConnectionError, RedisClusterUnreachableError),
            (AuthenticationError, RedisClusterException),
        ],
    )
    def test_initialize_clears_exception_local_when_no_startup_node_answers(
        self, error_type, raised_type
    ):
        """
        When no startup node answers, initialize() raises the last error from
        the local that holds it. The raised error's traceback refers to the
        initialize() frame, so the frame must not keep that local after the
        raise: the pair would stay alive until the cyclic GC runs, on every
        failed refresh.
        """
        connections = [MagicMock(), MagicMock()]
        # The constructor calls initialize(), so the nodes answer here; the
        # failure is set up below, once the manager exists.
        for connection in connections:
            connection.execute_command.return_value = default_cluster_slots
        nodes_manager = NodesManager(
            startup_nodes=[
                ClusterNode("127.0.0.1", 7000, redis_connection=connections[0]),
                ClusterNode("127.0.0.1", 7001, redis_connection=connections[1]),
            ],
            from_url=False,
            dynamic_startup_nodes=False,
        )
        for connection in connections:
            connection.execute_command.side_effect = error_type("mock failure")

        with pytest.raises(RedisClusterException) as exc_info:
            nodes_manager.initialize()
        assert type(exc_info.value) is raised_type

        # Locate the initialize frame in the propagated traceback and confirm
        # its exception local was cleared, proving no exception<->frame cycle
        # survives.
        initialize_frame = None
        tb = exc_info.value.__traceback__
        while tb is not None:
            if tb.tb_frame.f_code.co_name == "initialize":
                initialize_frame = tb.tb_frame
            tb = tb.tb_next
        assert initialize_frame is not None
        # `is None` alone also holds when no such local exists, so assert the
        # name is present before asserting its value.
        assert "exception" in initialize_frame.f_locals
        assert initialize_frame.f_locals["exception"] is None

    @pytest.mark.fixed_client
    def test_init_promote_server_type_for_node_in_cache(self):
        """
        When replica is promoted to master, nodes_cache must change the server type
        accordingly
        """
        cluster_slots_before_promotion = [
            [0, 16383, ["127.0.0.1", 7000], ["127.0.0.1", 7003]]
        ]
        cluster_slots_after_promotion = [
            [0, 16383, ["127.0.0.1", 7003], ["127.0.0.1", 7004]]
        ]

        cluster_slots_results = [
            cluster_slots_before_promotion,
            cluster_slots_after_promotion,
        ]

        with patch.object(Redis, "execute_command") as execute_command_mock:

            def execute_command(*_args, **_kwargs):
                if _args[0] == "CLUSTER SLOTS":
                    mock_cluster_slots = cluster_slots_results.pop(0)
                    return mock_cluster_slots
                elif _args[0] == "COMMAND":
                    return {"get": [], "set": []}
                elif _args[0] == "INFO":
                    return {"cluster_enabled": True}
                elif len(_args) > 1 and _args[1] == "cluster-require-full-coverage":
                    return {"cluster-require-full-coverage": False}
                else:
                    return execute_command_mock(*_args, **_kwargs)

            execute_command_mock.side_effect = execute_command

            nm = NodesManager(
                startup_nodes=[ClusterNode(host=default_host, port=default_port)],
                from_url=False,
                require_full_coverage=False,
                dynamic_startup_nodes=True,
            )

            assert nm.default_node.host == "127.0.0.1"
            assert nm.default_node.port == 7000
            assert nm.default_node.server_type == PRIMARY

            nm.initialize()

            assert nm.default_node.host == "127.0.0.1"
            assert nm.default_node.port == 7003
            assert nm.default_node.server_type == PRIMARY

    @staticmethod
    def _refresh_with_cluster_slots(cluster_slots_results, **nodes_manager_kwargs):
        """
        Patch CLUSTER SLOTS to return the given responses in order and build a
        NodesManager from the first one. Returns (patcher, nodes_manager); the
        caller stops the patcher.
        """
        patcher = patch.object(Redis, "execute_command", autospec=True)
        execute_command_mock = patcher.start()

        def execute_command(self, *args, **kwargs):
            if args[0] == "CLUSTER SLOTS":
                return cluster_slots_results.pop(0)
            return None

        execute_command_mock.side_effect = execute_command
        nodes_manager_kwargs.setdefault(
            "startup_nodes", [ClusterNode(host=default_host, port=default_port)]
        )
        nodes_manager_kwargs.setdefault("require_full_coverage", False)
        nm = NodesManager(from_url=False, **nodes_manager_kwargs)
        return patcher, nm

    @staticmethod
    def _freed_pools(update_mock, disconnect_mock):
        """
        The pools whose connections were released, asserting both halves of
        the release ran on the same pools.
        """
        updated = {id(c.args[0]) for c in update_mock.call_args_list}
        disconnected = {id(c.args[0]) for c in disconnect_mock.call_args_list}
        assert updated == disconnected
        return updated

    @pytest.mark.fixed_client
    def test_refresh_keeps_connections_of_nodes_that_switch_roles(self):
        """
        A primary/replica swap is not a reason to recycle connections: both
        nodes keep their clients and pools, only their roles change.
        """
        before = [[0, 16383, ["127.0.0.1", 7000], ["127.0.0.1", 7003]]]
        after = [[0, 16383, ["127.0.0.1", 7003], ["127.0.0.1", 7000]]]
        patcher, nm = self._refresh_with_cluster_slots([before, after])
        try:
            old_clients = {
                name: node.redis_connection for name, node in nm.nodes_cache.items()
            }
            with (
                patch.object(
                    ConnectionPool,
                    "update_active_connections_for_reconnect",
                    autospec=True,
                ) as update_mock,
                patch.object(
                    ConnectionPool, "disconnect_free_connections", autospec=True
                ) as disconnect_mock,
            ):
                nm.initialize()

            assert self._freed_pools(update_mock, disconnect_mock) == set()
            for name, client in old_clients.items():
                assert nm.nodes_cache[name].redis_connection is client
            assert nm.nodes_cache["127.0.0.1:7003"].server_type == PRIMARY
            assert nm.nodes_cache["127.0.0.1:7000"].server_type == REPLICA
        finally:
            patcher.stop()

    @pytest.mark.fixed_client
    def test_refresh_releases_only_removed_nodes(self):
        """
        Only the node that left the topology has its connections released; the
        node that stays and the node that joins are not touched.
        """
        before = [[0, 16383, ["127.0.0.1", 7000], ["127.0.0.1", 7003]]]
        after = [[0, 16383, ["127.0.0.1", 7000], ["127.0.0.1", 7004]]]
        patcher, nm = self._refresh_with_cluster_slots([before, after])
        try:
            removed_pool = nm.nodes_cache[
                "127.0.0.1:7003"
            ].redis_connection.connection_pool
            kept_client = nm.nodes_cache["127.0.0.1:7000"].redis_connection
            with (
                # Query the node that stays first, so CLUSTER SLOTS is answered
                # by a node whose connections must be kept
                patch("redis.cluster.random.shuffle"),
                patch.object(
                    ConnectionPool,
                    "update_active_connections_for_reconnect",
                    autospec=True,
                ) as update_mock,
                patch.object(
                    ConnectionPool, "disconnect_free_connections", autospec=True
                ) as disconnect_mock,
            ):
                nm.initialize()

            assert self._freed_pools(update_mock, disconnect_mock) == {id(removed_pool)}
            assert "127.0.0.1:7003" not in nm.nodes_cache
            assert nm.nodes_cache["127.0.0.1:7000"].redis_connection is kept_client
            assert nm.nodes_cache["127.0.0.1:7004"].redis_connection is not None
        finally:
            patcher.stop()

    @pytest.mark.fixed_client
    @pytest.mark.parametrize("disconnect_startup_nodes_pools", [True, False])
    def test_refresh_frees_startup_node_pool_outside_topology(
        self, disconnect_startup_nodes_pools
    ):
        """
        A startup node that is not part of the topology (e.g. a load balancer
        address) has its pool freed after CLUSTER SLOTS unless the caller opts
        out; the topology nodes are never freed by it.
        """
        slots = [[0, 16383, ["127.0.0.1", 7000], ["127.0.0.1", 7003]]]
        patcher, nm = self._refresh_with_cluster_slots(
            [slots, slots], dynamic_startup_nodes=False
        )
        try:
            seed_pool = nm.startup_nodes[
                get_node_name(default_host, default_port)
            ].redis_connection.connection_pool
            with (
                patch.object(
                    ConnectionPool,
                    "update_active_connections_for_reconnect",
                    autospec=True,
                ) as update_mock,
                patch.object(
                    ConnectionPool, "disconnect_free_connections", autospec=True
                ) as disconnect_mock,
            ):
                nm.initialize(
                    disconnect_startup_nodes_pools=disconnect_startup_nodes_pools
                )

            expected = {id(seed_pool)} if disconnect_startup_nodes_pools else set()
            assert self._freed_pools(update_mock, disconnect_mock) == expected
        finally:
            patcher.stop()

    @staticmethod
    def _patch_pool_release():
        """Patch both halves of a pool release, to see which pools were freed."""
        return (
            patch.object(
                ConnectionPool, "update_active_connections_for_reconnect", autospec=True
            ),
            patch.object(ConnectionPool, "disconnect_free_connections", autospec=True),
        )

    @pytest.mark.fixed_client
    def test_mixed_refresh_releases_only_the_removed_node(self):
        """
        In one refresh a shard swaps roles, a replica leaves, another joins and
        a shard stays the same: only the node that left is released.
        """
        before = [
            [0, 8191, ["127.0.0.1", 7000], ["127.0.0.1", 7003]],
            [8192, 16383, ["127.0.0.1", 7001], ["127.0.0.1", 7004]],
        ]
        after = [
            [0, 8191, ["127.0.0.1", 7003], ["127.0.0.1", 7000]],
            [8192, 16383, ["127.0.0.1", 7001], ["127.0.0.1", 7005]],
        ]
        patcher, nm = self._refresh_with_cluster_slots([before, after])
        try:
            old_clients = {
                name: node.redis_connection for name, node in nm.nodes_cache.items()
            }
            update_patch, disconnect_patch = self._patch_pool_release()
            with (
                # Query a node that stays first, so CLUSTER SLOTS is answered by
                # a node whose connections must be kept
                patch("redis.cluster.random.shuffle"),
                update_patch as update_mock,
                disconnect_patch as disconnect_mock,
            ):
                nm.initialize()

            assert self._freed_pools(update_mock, disconnect_mock) == {
                id(old_clients["127.0.0.1:7004"].connection_pool)
            }
            for name in ("127.0.0.1:7000", "127.0.0.1:7001", "127.0.0.1:7003"):
                assert nm.nodes_cache[name].redis_connection is old_clients[name]
            assert nm.nodes_cache["127.0.0.1:7003"].server_type == PRIMARY
            assert nm.nodes_cache["127.0.0.1:7000"].server_type == REPLICA
            assert "127.0.0.1:7004" not in nm.nodes_cache
            assert nm.nodes_cache["127.0.0.1:7005"].redis_connection is not None
        finally:
            patcher.stop()

    @pytest.mark.fixed_client
    @pytest.mark.parametrize("disconnect_startup_nodes_pools", [True, False])
    def test_refresh_keeps_pools_of_startup_nodes_in_the_topology(
        self, disconnect_startup_nodes_pools
    ):
        """
        With dynamic startup nodes the startup node that answers CLUSTER SLOTS
        is a node of the topology, so its pool is kept whatever the flag says.
        """
        slots = [[0, 16383, ["127.0.0.1", 7000], ["127.0.0.1", 7003]]]
        patcher, nm = self._refresh_with_cluster_slots([slots, slots])
        try:
            update_patch, disconnect_patch = self._patch_pool_release()
            with update_patch as update_mock, disconnect_patch as disconnect_mock:
                nm.initialize(
                    disconnect_startup_nodes_pools=disconnect_startup_nodes_pools
                )

            assert self._freed_pools(update_mock, disconnect_mock) == set()
        finally:
            patcher.stop()

    @pytest.mark.fixed_client
    @pytest.mark.parametrize("disconnect_startup_nodes_pools", [True, False])
    def test_refresh_frees_clients_of_additional_startup_nodes(
        self, disconnect_startup_nodes_pools
    ):
        """
        An additional startup node gets a client only for this refresh, so its
        pool is freed afterwards even when the startup node pools are kept.
        """
        full = [[0, 16383, ["127.0.0.1", 7000]]]
        partial = [[0, 8191, ["127.0.0.1", 7000]]]
        split = [
            [0, 8191, ["127.0.0.1", 7000]],
            [8192, 16383, ["127.0.0.1", 7001]],
        ]
        # The startup node covers only half of the slots, so the additional
        # startup node is queried too
        patcher, nm = self._refresh_with_cluster_slots([full, partial, split])
        try:
            update_patch, disconnect_patch = self._patch_pool_release()
            with update_patch as update_mock, disconnect_patch as disconnect_mock:
                nm.initialize(
                    additional_startup_nodes_info=[("127.0.0.1", 7009)],
                    disconnect_startup_nodes_pools=disconnect_startup_nodes_pools,
                )

            freed = self._freed_pools(update_mock, disconnect_mock)
            assert len(freed) == 1
            assert not freed & self._current_pools(nm)
            assert "127.0.0.1:7009" not in nm.nodes_cache
        finally:
            patcher.stop()

    @pytest.mark.fixed_client
    @pytest.mark.parametrize("disconnect_startup_nodes_pools", [True, False])
    def test_failed_refresh_frees_clients_of_additional_startup_nodes(
        self, disconnect_startup_nodes_pools
    ):
        """
        A refresh that fails after querying an additional startup node still
        frees the pool of the client built for it, and keeps the topology.
        """
        full = [[0, 16383, ["127.0.0.1", 7000]]]
        partial = [[0, 8191, ["127.0.0.1", 7000]]]
        # Neither node covers every slot, so both are queried and the refresh
        # fails on the coverage check
        patcher, nm = self._refresh_with_cluster_slots(
            [full, partial, partial], require_full_coverage=True
        )
        try:
            current_pools = self._current_pools(nm)
            update_patch, disconnect_patch = self._patch_pool_release()
            with update_patch as update_mock, disconnect_patch as disconnect_mock:
                with pytest.raises(RedisClusterException):
                    nm.initialize(
                        additional_startup_nodes_info=[("127.0.0.1", 7009)],
                        disconnect_startup_nodes_pools=disconnect_startup_nodes_pools,
                    )

            freed = self._freed_pools(update_mock, disconnect_mock)
            assert len(freed) == 1
            assert not freed & current_pools
            assert self._current_pools(nm) == current_pools
        finally:
            patcher.stop()

    @pytest.mark.fixed_client
    def test_refresh_frees_client_of_additional_node_whose_query_raised(self):
        """
        The client built for an additional startup node is freed also when its
        CLUSTER SLOTS raises, not only when it answers.
        """
        partial = [[0, 8191, ["127.0.0.1", 7000]]]
        patcher, nm = self._refresh_with_cluster_slots([partial])
        try:
            current_pools = self._current_pools(nm)

            def execute_command(self, *args, **kwargs):
                if self.connection_pool.connection_kwargs["port"] == 7009:
                    raise ConnectionError("down")
                return partial

            # The patcher's mock of Redis.execute_command
            Redis.execute_command.side_effect = execute_command
            update_patch, disconnect_patch = self._patch_pool_release()
            with update_patch as update_mock, disconnect_patch as disconnect_mock:
                nm.initialize(additional_startup_nodes_info=[("127.0.0.1", 7009)])

            freed = self._freed_pools(update_mock, disconnect_mock)
            assert len(freed) == 1
            assert not freed & current_pools
            assert self._current_pools(nm) == current_pools
        finally:
            patcher.stop()

    @pytest.mark.fixed_client
    def test_additional_startup_node_that_is_a_startup_node_keeps_its_client(self):
        """
        An additional startup node that is already a startup node is queried
        through that node's client, and not again through a new client that
        would replace the live node's one.
        """
        partial = [[0, 8191, ["127.0.0.1", 7000]]]
        patcher, nm = self._refresh_with_cluster_slots([partial, partial])
        try:
            old_client = nm.nodes_cache["127.0.0.1:7000"].redis_connection
            update_patch, disconnect_patch = self._patch_pool_release()
            with (
                patch.object(
                    nm, "create_redis_node", wraps=nm.create_redis_node
                ) as create_mock,
                update_patch as update_mock,
                disconnect_patch as disconnect_mock,
            ):
                nm.initialize(additional_startup_nodes_info=[("127.0.0.1", 7000)])

            create_mock.assert_not_called()
            assert self._freed_pools(update_mock, disconnect_mock) == set()
            assert nm.nodes_cache["127.0.0.1:7000"].redis_connection is old_client
            assert nm.startup_nodes["127.0.0.1:7000"].redis_connection is old_client
        finally:
            patcher.stop()

    @pytest.mark.fixed_client
    def test_localhost_remapped_startup_node_keeps_its_client(self):
        """
        With a remap to localhost, the refresh hint of a MOVED redirect is
        localhost:port, while the startup node is named after the resolved
        address. It is the same node, so it keeps its client.
        """

        def address_remap(address):
            host, port = address
            return "localhost", port + 1000

        partial = [[0, 8191, ["127.0.0.1", 7000]]]
        patcher, nm = self._refresh_with_cluster_slots(
            [partial, partial], address_remap=address_remap
        )
        try:
            startup_node = nm.startup_nodes[get_node_name(_resolve_localhost(), 8000)]
            old_client = startup_node.redis_connection
            # The patcher's mock of Redis.execute_command
            Redis.execute_command.reset_mock()
            # The hint _execute_command passes on for a MOVED to 7000
            nm.initialize(
                additional_startup_nodes_info=[nm.remap_host_port("127.0.0.1", 7000)]
            )

            assert [c.args[0] for c in Redis.execute_command.call_args_list] == [
                old_client
            ]
            assert startup_node.redis_connection is old_client
        finally:
            patcher.stop()

    @pytest.mark.fixed_client
    def test_refresh_without_full_coverage_builds_no_node_clients(self):
        """
        A refresh that fails the full-coverage check raises before it builds
        clients for the nodes it discovered.
        """
        full = [[0, 16383, ["127.0.0.1", 7000]]]
        partial = [[0, 8191, ["127.0.0.1", 7001]]]
        patcher, nm = self._refresh_with_cluster_slots(
            [full, partial], require_full_coverage=True
        )
        try:
            current_pools = self._current_pools(nm)
            with patch.object(
                nm, "create_redis_node", wraps=nm.create_redis_node
            ) as create_mock:
                with pytest.raises(RedisClusterException):
                    nm.initialize()

            create_mock.assert_not_called()
            assert "127.0.0.1:7001" not in nm.nodes_cache
            assert self._current_pools(nm) == current_pools
        finally:
            patcher.stop()

    @staticmethod
    def _streaming_credential_provider():
        class FakeStreamingCredentialProvider(StreamingCredentialProvider):
            def __init__(self):
                self.on_next_callback = None

            def get_credentials(self):
                return "default", "token"

            def on_next(self, callback):
                self.on_next_callback = callback

            def on_error(self, callback):
                pass

            def is_streaming(self) -> bool:
                return False

        return FakeStreamingCredentialProvider()

    @staticmethod
    def _re_authed_pools(credential_provider):
        with patch.object(
            ConnectionPool, "re_auth_callback", autospec=True
        ) as re_auth_mock:
            credential_provider.on_next_callback(Mock())
        pools = [c.args[0] for c in re_auth_mock.call_args_list]
        assert len(pools) == len({id(p) for p in pools})
        return {id(p) for p in pools}

    @staticmethod
    def _current_pools(nm):
        return {
            id(node.redis_connection.connection_pool)
            for node in nm.nodes_cache.values()
        }

    @pytest.mark.fixed_client
    @pytest.mark.parametrize(
        "after",
        [
            # no topology change
            [[0, 16383, ["127.0.0.1", 7000], ["127.0.0.1", 7003]]],
            # a node joins
            [[0, 16383, ["127.0.0.1", 7000], ["127.0.0.1", 7003], ["127.0.0.1", 7004]]],
            # a node leaves
            [[0, 16383, ["127.0.0.1", 7000]]],
        ],
        ids=["unchanged", "node_added", "node_removed"],
    )
    def test_refresh_keeps_re_auth_on_every_current_pool(self, after):
        """
        A streaming credential provider keeps only the last callback, so after
        every refresh the callback must cover the pool of every current node -
        not only the pools created by that refresh - and no removed one.
        """
        before = [[0, 16383, ["127.0.0.1", 7000], ["127.0.0.1", 7003]]]
        credential_provider = self._streaming_credential_provider()
        patcher, nm = self._refresh_with_cluster_slots(
            [before, after], credential_provider=credential_provider
        )
        try:
            assert self._re_authed_pools(credential_provider) == self._current_pools(nm)

            nm.initialize()

            assert self._re_authed_pools(credential_provider) == self._current_pools(nm)
        finally:
            patcher.stop()

    @pytest.mark.fixed_client
    def test_failed_refresh_keeps_re_auth_on_every_current_pool(self):
        """
        Every client a refresh builds - for an additional startup node or a new
        node - registers its own pool with the credential provider. A refresh
        that then fails must register the current pools again, or re-auth
        would cover only those clients until the next successful refresh.
        """
        full = [[0, 16383, ["127.0.0.1", 7000], ["127.0.0.1", 7003]]]
        partial = [[0, 8000, ["127.0.0.1", 7005]]]
        credential_provider = self._streaming_credential_provider()
        patcher, nm = self._refresh_with_cluster_slots(
            [full, partial, partial, partial],
            credential_provider=credential_provider,
            require_full_coverage=True,
        )
        try:
            with pytest.raises(RedisClusterException, match="not covered"):
                nm.initialize(additional_startup_nodes_info=[("127.0.0.1", 7009)])

            assert set(nm.nodes_cache) == {"127.0.0.1:7000", "127.0.0.1:7003"}
            assert self._re_authed_pools(credential_provider) == self._current_pools(nm)
        finally:
            patcher.stop()

    @pytest.mark.fixed_client
    def test_failed_re_auth_does_not_stop_the_other_pools(self):
        """
        A pool whose re-authentication raises does not stop the remaining pools
        from being re-authenticated. The first error is raised afterwards.
        """
        slots = [[0, 16383, ["127.0.0.1", 7000], ["127.0.0.1", 7003]]]
        credential_provider = self._streaming_credential_provider()
        patcher, nm = self._refresh_with_cluster_slots(
            [slots], credential_provider=credential_provider
        )
        re_authed = []

        def re_auth_callback(pool, token):
            re_authed.append(id(pool))
            if len(re_authed) == 1:
                raise AuthenticationError("first")
            raise AuthenticationError("second")

        try:
            with patch.object(
                ConnectionPool,
                "re_auth_callback",
                autospec=True,
                side_effect=re_auth_callback,
            ):
                with pytest.raises(AuthenticationError, match="first"):
                    credential_provider.on_next_callback(Mock())

            assert set(re_authed) == self._current_pools(nm)
        finally:
            patcher.stop()

    @pytest.mark.fixed_client
    def test_lazy_node_connection_keeps_re_auth_on_every_pool(self):
        """
        Creating the client of a node added by a MOVED redirect must not narrow
        re-authentication down to that node.
        """
        slots = [[0, 16383, ["127.0.0.1", 7000], ["127.0.0.1", 7003]]]
        credential_provider = self._streaming_credential_provider()
        patcher, nm = self._refresh_with_cluster_slots(
            [slots], credential_provider=credential_provider
        )
        try:
            nm.move_slot(MovedError("0 127.0.0.1:7005"))
            new_node = nm.nodes_cache["127.0.0.1:7005"]
            assert new_node.redis_connection is None

            nm.create_redis_connections([new_node])

            assert self._re_authed_pools(credential_provider) == self._current_pools(nm)
            assert len(self._current_pools(nm)) == 3
        finally:
            patcher.stop()

    @pytest.mark.fixed_client
    def test_init_slots_cache_cluster_mode_disabled(self):
        """
        Test that creating a RedisCluster failes if one of the startup nodes
        has cluster mode disabled
        """
        with pytest.raises(RedisClusterException) as e:
            get_mocked_redis_client(
                cluster_slots_raise_error=True,
                host=default_host,
                port=default_port,
                cluster_enabled=False,
            )
        assert "Cluster mode is not enabled on this node" in str(e.value)

    @pytest.mark.fixed_client
    def test_unreachable_error_is_reserved_for_connectivity_failures(self):
        """
        RedisClusterUnreachableError is raised when no startup node could be
        contacted, but not when a node responded and rejected CLUSTER SLOTS:
        MultiDB registers the unreachable subtype as retryable, so a
        deterministic configuration error must keep surfacing as a plain
        RedisClusterException. Authentication failures subclass ConnectionError
        but cannot be repaired by a failover, so they stay plain as well.
        """
        with patch.object(NodesManager, "create_redis_node") as create_redis_node:
            create_redis_node.side_effect = ConnectionError("mock connection error")

            with pytest.raises(RedisClusterUnreachableError) as e:
                RedisCluster(startup_nodes=[ClusterNode("127.0.0.1", 7000)])
            assert "Redis Cluster cannot be connected" in str(e.value)

        with patch.object(NodesManager, "create_redis_node") as create_redis_node:
            create_redis_node.side_effect = AuthenticationError("invalid password")

            with pytest.raises(RedisClusterException) as e:
                RedisCluster(startup_nodes=[ClusterNode("127.0.0.1", 7000)])
            assert not isinstance(e.value, RedisClusterUnreachableError)
            assert "invalid password" in str(e.value)

        with pytest.raises(RedisClusterException) as e:
            get_mocked_redis_client(
                cluster_slots_raise_error=True,
                host=default_host,
                port=default_port,
                cluster_enabled=False,
            )
        assert not isinstance(e.value, RedisClusterUnreachableError)
        assert "Cluster mode is not enabled on this node" in str(e.value)

    @pytest.mark.fixed_client
    def test_empty_startup_nodes(self):
        """
        It should not be possible to create a node manager with no nodes
        specified
        """
        with pytest.raises(RedisClusterException):
            NodesManager([])

    @pytest.mark.fixed_client
    def test_wrong_startup_nodes_type(self):
        """
        If something other then a list type itteratable is provided it should
        fail
        """
        with pytest.raises(RedisClusterException):
            NodesManager({})

    @pytest.mark.fixed_client
    def test_init_slots_cache_slots_collision(self, request):
        """
        Test that if 2 nodes do not agree on the same slots setup it should
        raise an error. In this test both nodes will say that the first
        slots block should be bound to different servers.
        """
        with patch.object(NodesManager, "create_redis_node") as create_redis_node:

            def create_mocked_redis_node(host, port, **kwargs):
                """
                Helper function to return custom slots cache_data data from
                different redis nodes
                """
                if port == 7000:
                    result = [
                        [0, 5460, ["127.0.0.1", 7000], ["127.0.0.1", 7003]],
                        [5461, 10922, ["127.0.0.1", 7001], ["127.0.0.1", 7004]],
                    ]

                elif port == 7001:
                    result = [
                        [0, 5460, ["127.0.0.1", 7001], ["127.0.0.1", 7003]],
                        [5461, 10922, ["127.0.0.1", 7000], ["127.0.0.1", 7004]],
                    ]
                else:
                    result = []

                r_node = Redis(host=host, port=port)

                orig_execute_command = r_node.execute_command

                def execute_command(*args, **kwargs):
                    if args[0] == "CLUSTER SLOTS":
                        return result
                    elif args[0] == "INFO":
                        return {"cluster_enabled": True}
                    elif args[1] == "cluster-require-full-coverage":
                        return {"cluster-require-full-coverage": "yes"}
                    else:
                        return orig_execute_command(*args, **kwargs)

                r_node.execute_command = execute_command
                return r_node

            create_redis_node.side_effect = create_mocked_redis_node

            with pytest.raises(RedisClusterException) as ex:
                node_1 = ClusterNode("127.0.0.1", 7000)
                node_2 = ClusterNode("127.0.0.1", 7001)
                RedisCluster(startup_nodes=[node_1, node_2])
            assert str(ex.value).startswith(
                "startup_nodes could not agree on a valid slots cache"
            ), str(ex.value)

    @pytest.mark.fixed_client
    @pytest.mark.parametrize("host", ["", None])
    def test_cluster_one_instance(self, host):
        """
        If the cluster exists of only 1 node then there is some hacks that must
        be validated they work.
        """
        node = ClusterNode(default_host, default_port)
        cluster_slots = [[0, 16383, [host, default_port]]]
        rc = get_mocked_redis_client(startup_nodes=[node], cluster_slots=cluster_slots)

        n = rc.nodes_manager
        assert len(n.nodes_cache) == 1
        n_node = rc.get_node(node_name=node.name)
        assert n_node is not None
        assert n_node == node
        assert n_node.server_type == PRIMARY
        assert len(n.slots_cache) == REDIS_CLUSTER_HASH_SLOTS
        for i in range(0, REDIS_CLUSTER_HASH_SLOTS):
            assert n.slots_cache[i] == [n_node]

    @pytest.mark.fixed_client
    def test_init_slots_cache_without_hosts(self):
        """
        A cluster whose endpoint is unknown (cluster-preferred-endpoint-type
        unknown-endpoint) sends null hosts in CLUSTER SLOTS, and its nodes are
        reached on the host of the startup node that answered.
        """
        cluster_slots = [
            [0, 8191, [None, 7000, "node_0"], [None, 7003, "node_3"]],
            [8192, 16383, [None, 7001, "node_1"], [None, 7002, "node_2"]],
        ]
        rc = get_mocked_redis_client(
            host=default_host, port=default_port, cluster_slots=cluster_slots
        )

        n = rc.nodes_manager
        assert [node.name for node in n.slots_cache[0]] == [
            f"{default_host}:7000",
            f"{default_host}:7003",
        ]
        assert [node.name for node in n.slots_cache[16383]] == [
            f"{default_host}:7001",
            f"{default_host}:7002",
        ]
        assert len(n.nodes_cache) == 4

    @pytest.mark.fixed_client
    def test_init_with_down_node(self):
        """
        If I can't connect to one of the nodes, everything should still work.
        But if I can't connect to any of the nodes, exception should be thrown.
        """
        with patch.object(NodesManager, "create_redis_node") as create_redis_node:

            def create_mocked_redis_node(host, port, **kwargs):
                if port == 7000:
                    raise ConnectionError("mock connection error for 7000")

                r_node = Redis(host=host, port=port, decode_responses=True)

                def execute_command(*args, **kwargs):
                    if args[0] == "CLUSTER SLOTS":
                        return [
                            [0, 8191, ["127.0.0.1", 7001, "node_1"]],
                            [8192, 16383, ["127.0.0.1", 7002, "node_2"]],
                        ]
                    elif args[0] == "INFO":
                        return {"cluster_enabled": True}
                    elif args[1] == "cluster-require-full-coverage":
                        return {"cluster-require-full-coverage": "yes"}

                r_node.execute_command = execute_command

                return r_node

            create_redis_node.side_effect = create_mocked_redis_node

            node_1 = ClusterNode("127.0.0.1", 7000)
            node_2 = ClusterNode("127.0.0.1", 7001)

            # If all startup nodes fail to connect, connection error should be
            # thrown
            with pytest.raises(RedisClusterException) as e:
                RedisCluster(startup_nodes=[node_1])
            assert "Redis Cluster cannot be connected" in str(e.value)

            with patch.object(
                CommandsParser, "initialize", autospec=True
            ) as cmd_parser_initialize:

                def cmd_init_mock(self, r):
                    self.commands = {
                        "get": {
                            "name": "get",
                            "arity": 2,
                            "flags": ["readonly", "fast"],
                            "first_key_pos": 1,
                            "last_key_pos": 1,
                            "step_count": 1,
                        }
                    }

                cmd_parser_initialize.side_effect = cmd_init_mock
                # When at least one startup node is reachable, the cluster
                # initialization should succeeds
                rc = RedisCluster(startup_nodes=[node_1, node_2])
                assert rc.get_node(host=default_host, port=7001) is not None
                assert rc.get_node(host=default_host, port=7002) is not None

    @pytest.mark.fixed_client
    @pytest.mark.parametrize("dynamic_startup_nodes", [True, False])
    def test_init_slots_dynamic_startup_nodes(self, dynamic_startup_nodes):
        rc = get_mocked_redis_client(
            host="my@DNS.com",
            port=7000,
            cluster_slots=default_cluster_slots,
            dynamic_startup_nodes=dynamic_startup_nodes,
        )
        # Nodes are taken from default_cluster_slots
        discovered_nodes = [
            "127.0.0.1:7000",
            "127.0.0.1:7001",
            "127.0.0.1:7002",
            "127.0.0.1:7003",
        ]
        startup_nodes = list(rc.nodes_manager.startup_nodes.keys())
        if dynamic_startup_nodes is True:
            assert startup_nodes.sort() == discovered_nodes.sort()
        else:
            assert startup_nodes == ["my@DNS.com:7000"]

    @pytest.mark.fixed_client
    @pytest.mark.parametrize(
        "connection_pool_class", [ConnectionPool, BlockingConnectionPool]
    )
    def test_connection_pool_class(self, connection_pool_class):
        rc = get_mocked_redis_client(
            url="redis://my@DNS.com:7000",
            cluster_slots=default_cluster_slots,
            connection_pool_class=connection_pool_class,
        )

        for node in rc.nodes_manager.nodes_cache.values():
            assert isinstance(
                node.redis_connection.connection_pool, connection_pool_class
            )

    @pytest.mark.fixed_client
    @pytest.mark.parametrize(
        "connection_pool_class", [ConnectionPool, BlockingConnectionPool]
    )
    def test_connection_pool_class_with_host_and_port(self, connection_pool_class):
        rc = get_mocked_redis_client(
            host=default_host,
            port=default_port,
            cluster_slots=default_cluster_slots,
            connection_pool_class=connection_pool_class,
        )

        for node in rc.nodes_manager.nodes_cache.values():
            assert isinstance(
                node.redis_connection.connection_pool, connection_pool_class
            )

    @pytest.mark.fixed_client
    def test_connection_class_with_host_and_port(self):
        class MyConnection(Connection):
            pass

        rc = get_mocked_redis_client(
            host=default_host,
            port=default_port,
            cluster_slots=default_cluster_slots,
            connection_class=MyConnection,
        )

        for node in rc.nodes_manager.nodes_cache.values():
            pool = node.redis_connection.connection_pool
            assert pool.connection_class is MyConnection
            assert isinstance(pool.make_connection(), MyConnection)

    @pytest.mark.fixed_client
    def test_ssl_options_without_ssl_with_host_and_port(self):
        rc = get_mocked_redis_client(
            host=default_host,
            port=default_port,
            cluster_slots=default_cluster_slots,
            ssl=False,
            ssl_cert_reqs="none",
        )

        for node in rc.nodes_manager.nodes_cache.values():
            pool = node.redis_connection.connection_pool
            assert pool.connection_class is Connection
            assert isinstance(pool.make_connection(), Connection)

    @pytest.mark.fixed_client
    def test_ssl_connection_class_keeps_ssl_options_with_host_and_port(self):
        rc = get_mocked_redis_client(
            host=default_host,
            port=default_port,
            cluster_slots=default_cluster_slots,
            connection_class=SSLConnection,
            ssl_cert_reqs="none",
        )

        for node in rc.nodes_manager.nodes_cache.values():
            connection = node.redis_connection.connection_pool.make_connection()
            assert isinstance(connection, SSLConnection)
            assert connection.cert_reqs == ssl.CERT_NONE

    @pytest.mark.fixed_client
    def test_ssl_pool_default_keeps_ssl_options_with_host_and_port(self):
        class SSLConnectionPool(ConnectionPool):
            def __init__(self, connection_class=SSLConnection, **kwargs):
                super().__init__(connection_class=connection_class, **kwargs)

        rc = get_mocked_redis_client(
            host=default_host,
            port=default_port,
            cluster_slots=default_cluster_slots,
            connection_pool_class=SSLConnectionPool,
            ssl_cert_reqs="none",
        )

        for node in rc.nodes_manager.nodes_cache.values():
            connection = node.redis_connection.connection_pool.make_connection()
            assert isinstance(connection, SSLConnection)
            assert connection.cert_reqs == ssl.CERT_NONE

    @pytest.mark.fixed_client
    def test_connection_pool_with_host_and_port(self):
        pool = ConnectionPool(host=default_host, port=default_port)
        rc = get_mocked_redis_client(
            host=default_host,
            port=default_port,
            cluster_slots=default_cluster_slots,
            connection_pool=pool,
        )

        for node in rc.nodes_manager.nodes_cache.values():
            assert node.redis_connection.connection_pool is pool
            assert node.redis_connection.auto_close_connection_pool is False

    @pytest.mark.fixed_client
    def test_url_node_clients_close_their_pool(self):
        rc = get_mocked_redis_client(
            url="redis://my@DNS.com:7000",
            cluster_slots=default_cluster_slots,
        )

        for node in rc.nodes_manager.nodes_cache.values():
            assert node.redis_connection.auto_close_connection_pool is True

    @pytest.mark.fixed_client
    @pytest.mark.parametrize("queue_class", [Queue, LifoQueue])
    def test_allow_custom_queue_class(self, queue_class):
        rc = get_mocked_redis_client(
            url="redis://my@DNS.com:7000",
            cluster_slots=default_cluster_slots,
            connection_pool_class=BlockingConnectionPool,
            queue_class=queue_class,
        )

        for node in rc.nodes_manager.nodes_cache.values():
            assert node.redis_connection.connection_pool.queue_class == queue_class

    @pytest.mark.fixed_client
    def test_concurrent_initialize_exact_timing(self):
        """
        Test that exactly two concurrent initialize calls result in only
        one actual cluster slots fetch by forcing them to start simultaneously
        """
        initialization_count = {"count": 0}
        epoch_barrier = threading.Barrier(2)

        with (
            patch.object(Redis, "execute_command") as execute_command_mock,
        ):

            def execute_command(*_args, **_kwargs):
                if _args[0] == "CLUSTER SLOTS":
                    # Track how many times we actually fetch cluster slots
                    initialization_count["count"] += 1
                    return default_cluster_slots
                else:
                    return execute_command_mock(*_args, **_kwargs)

            execute_command_mock.side_effect = execute_command

            nm = NodesManager(
                startup_nodes=[ClusterNode(host=default_host, port=default_port)],
                from_url=False,
                require_full_coverage=False,
                dynamic_startup_nodes=True,
            )

            # Reset the counter after initial setup
            initialization_count["count"] = 0

            # Store the original method
            original_get_epoch = nm._get_epoch

            def mocked_get_epoch():
                """
                Mock _get_epoch to control race timing:
                1. First thread fetches epoch
                2. Both threads sync at epoch_barrier (ensures 2nd thread also fetches epoch)
                3. Both threads sync at proceed_barrier (ensures both have same epoch before lock)
                4. Both threads proceed to try to acquire _initialization_lock
                """
                epoch = original_get_epoch()
                # Signal that this thread has fetched the epoch
                epoch_barrier.wait()
                return epoch

            # Patch the instance method directly
            nm._get_epoch = mocked_get_epoch

            errors: list[Exception] = []

            def initialize_thread():
                """Call initialize to test concurrent access"""
                try:
                    nm.initialize()
                except Exception as e:
                    errors.append(e)

            # Create exactly 2 threads that will initialize at the same time
            threads: list[threading.Thread] = []
            for _ in range(2):
                threads.append(threading.Thread(target=initialize_thread))

            # Start both threads
            for t in threads:
                t.start()

            # Wait for both threads to complete
            for t in threads:
                t.join()

            # Check that no errors occurred
            assert len(errors) == 0, f"Errors occurred: {errors}"

            # Due to the _initialization_lock, only one thread should have
            # actually fetched cluster slots
            assert initialization_count["count"] == 1

            # Verify that the nodes_cache is still consistent
            assert len(nm.nodes_cache) > 0
            assert len(nm.slots_cache) > 0

    @pytest.mark.fixed_client
    def test_reentrant_initialize_is_skipped(self):
        """
        A re-entrant initialize call on the thread that is already refreshing
        the topology must be skipped instead of running a nested refresh.

        This is the shape produced by an SMIGRATED push notification that
        arrives on the CLUSTER SLOTS response and is handled inline on the
        refreshing thread: the maintenance handler calls initialize() again on
        that same thread. A nested refresh would reset() and swap the caches
        underneath the outer call, which then overwrites them with its own
        older snapshot.
        """
        initialization_count = {"count": 0}
        reentrant_calls = {"count": 0}
        # The NodesManager constructor runs its own initialize; only re-enter
        # once the instance exists and the counters have been reset.
        armed = {"on": False}

        with patch.object(Redis, "execute_command") as execute_command_mock:

            def execute_command(*_args, **_kwargs):
                if _args[0] == "CLUSTER SLOTS":
                    initialization_count["count"] += 1
                    # Re-enter initialize on this same thread, exactly once, the
                    # way an inline push notification handler would.
                    if armed["on"] and reentrant_calls["count"] == 0:
                        reentrant_calls["count"] += 1
                        nm.initialize()
                    return default_cluster_slots
                else:
                    return execute_command_mock(*_args, **_kwargs)

            execute_command_mock.side_effect = execute_command

            nm = NodesManager(
                startup_nodes=[ClusterNode(host=default_host, port=default_port)],
                from_url=False,
                require_full_coverage=False,
                dynamic_startup_nodes=True,
            )

            initialization_count["count"] = 0
            armed["on"] = True

            nm.initialize()

            # The re-entrant call must not have issued a second CLUSTER SLOTS.
            assert reentrant_calls["count"] == 1
            assert initialization_count["count"] == 1

            # The outer refresh published a consistent topology and released
            # ownership, so a later call is not affected by the guard.
            assert len(nm.nodes_cache) > 0
            assert len(nm.slots_cache) > 0
            assert nm._initializing_thread_id is None

    @pytest.mark.fixed_client
    def test_concurrent_slot_moves(self):
        # ensure multiple concurrently moved slots are processed correctly,
        # eg: not dropping updates
        r = get_mocked_redis_client(
            host=default_host,
            port=default_port,
            cluster_enabled=True,
        )
        nm = r.nodes_manager
        # Move slots 0-999 to 127.0.0.1 in concurrent threads
        num_threads = 20
        slots_per_thread = 50  # 1000 slots / 20 threads = 50 slots per thread
        errors: list[Exception] = []

        def move_slots_worker(thread_id: int):
            """Each thread moves a subset of slots to 127.0.0.1"""
            try:
                for i in range(slots_per_thread):
                    moved_error = MovedError(
                        f"{thread_id * slots_per_thread + i} 127.0.0.1:7001"
                    )
                    nm.move_slot(moved_error)
            except Exception as e:
                errors.append(e)

        # Start all threads
        threads: list[threading.Thread] = []
        for i in range(num_threads):
            threads.append(threading.Thread(target=move_slots_worker, args=(i,)))

        for t in threads:
            t.start()

        # Wait for all threads to complete
        for t in threads:
            t.join()

        # Check that no errors occurred
        assert len(errors) == 0, f"Errors occurred: {errors}"

        # Verify that all slots 0-999 are moved to 127.0.0.1:7001
        for slot_id in range(num_threads * slots_per_thread):
            assert slot_id in nm.slots_cache, f"Slot {slot_id} missing"
            slot_nodes = nm.slots_cache[slot_id]
            assert len(slot_nodes) >= 1, f"Slot {slot_id} has no nodes"
            primary_node = slot_nodes[0]
            assert primary_node.host == "127.0.0.1", (
                f"Slot {slot_id} not moved to 127.0.0.1, "
                f"current host: {primary_node.host}"
            )
            assert primary_node.port == 7001, (
                f"Slot {slot_id} not moved to port 7001, "
                f"current port: {primary_node.port}"
            )
            assert primary_node.server_type == PRIMARY

    @pytest.mark.fixed_client
    def test_concurrent_initialize_and_move_slot(self):
        # race initialize & move slot to ensure that the two operations
        # don't conflict with each other.

        with (
            patch.object(Redis, "execute_command") as execute_command_mock,
        ):
            r = get_mocked_redis_client(
                host=default_host,
                port=default_port,
                cluster_enabled=True,
            )
            nm = r.nodes_manager

            def execute_command(*_args, **_kwargs):
                if _args[0] == "CLUSTER SLOTS":
                    return default_cluster_slots
                else:
                    return execute_command_mock(*_args, **_kwargs)

            execute_command_mock.side_effect = execute_command

            errors: list[Exception] = []
            initialize_paused = threading.Event()
            allow_initialize_to_finish = threading.Event()
            original_check_slots_coverage = nm.check_slots_coverage

            def check_slots_coverage(slots_cache):
                # Pause initialization after it has rebuilt the temporary slots
                # cache, so move_slot runs during a deterministic refresh window.
                initialize_paused.set()
                if not allow_initialize_to_finish.wait(timeout=5):
                    raise TimeoutError("Timed out waiting for move_slot worker")
                return original_check_slots_coverage(slots_cache)

            def initialize_worker():
                """Reinitialize the cluster"""
                try:
                    nm.initialize()
                except Exception as e:
                    errors.append(e)

            def move_slots_worker():
                """Move slots while initialize is running"""
                for slot_id in range(10):
                    try:
                        # move slot to a mix of :7000 & :7003, which simulates failovers
                        # within the same shard. Using nodes from the same shard ensures
                        # move_slot preserves the 2-node structure (primary + replica),
                        # so both initialize() and move_slot() result in 2 nodes per slot.
                        new_slot = 7000 if slot_id % 2 == 0 else 7003
                        moved_error = MovedError(f"{slot_id} 127.0.0.1:{new_slot}")
                        nm.move_slot(moved_error)
                    except Exception as e:
                        errors.append(e)

            with patch.object(nm, "check_slots_coverage", check_slots_coverage):
                t1 = threading.Thread(target=initialize_worker)
                t2 = threading.Thread(target=move_slots_worker)

                t1.start()
                initialize_ready = False
                move_worker_started = False
                try:
                    initialize_ready = initialize_paused.wait(timeout=5)
                    if initialize_ready:
                        t2.start()
                        move_worker_started = True
                        t2.join(timeout=5)
                finally:
                    allow_initialize_to_finish.set()
                    t1.join(timeout=5)
                    if move_worker_started and t2.is_alive():
                        t2.join(timeout=5)

                assert initialize_ready
                if move_worker_started:
                    assert not t2.is_alive()
                assert not t1.is_alive()

                # check that no errors occurred
                assert len(errors) == 0, f"Errors occurred: {errors}"

                # verify data consistency
                for slot_id in range(REDIS_CLUSTER_HASH_SLOTS):
                    assert slot_id in nm.slots_cache, f"Slot {slot_id} missing"
                    slot_nodes = nm.slots_cache[slot_id]
                    assert len(slot_nodes) == 2

                    for node in slot_nodes:
                        assert node.name in nm.nodes_cache

                    # primary should be first
                    assert slot_nodes[0].server_type == PRIMARY

    def _register_listener(self, nm):
        """
        Register a tracking listener on the nodes manager's event dispatcher
        for ``AfterSlotsCacheRefreshEvent``. The returned instance must be
        kept alive by the caller for the duration of the test.
        """

        class _TrackingListener(EventListenerInterface):
            def listen(self, event: object) -> None:  # pragma: no cover
                pass

        listener = _TrackingListener()
        listener.listen = Mock()
        nm._event_dispatcher.register_listeners(
            {AfterSlotsCacheRefreshEvent: [listener]}
        )
        return listener

    @pytest.mark.fixed_client
    def test_move_slot_dispatches_event_when_slot_moves_to_new_node(self):
        """
        move_slot() must dispatch AfterSlotsCacheRefreshEvent when the
        redirected node is not currently serving the slot (new primary from a
        different shard).
        """
        r = get_mocked_redis_client(
            host=default_host,
            port=default_port,
            cluster_enabled=True,
        )
        nm = r.nodes_manager
        listener = self._register_listener(nm)
        # Default layout: slot 0 is served by 127.0.0.1:7000; redirect it to
        # 127.0.0.1:7001, which is the primary for a different shard.
        nm.move_slot(MovedError("0 127.0.0.1:7001"))

        listener.listen.assert_called_once()
        assert isinstance(listener.listen.call_args[0][0], AfterSlotsCacheRefreshEvent)

    @pytest.mark.fixed_client
    def test_move_slot_dispatches_event_on_failover(self):
        """
        move_slot() must dispatch AfterSlotsCacheRefreshEvent when the
        redirected node was a replica of the same slot and is being promoted
        (failover case).
        """
        r = get_mocked_redis_client(
            host=default_host,
            port=default_port,
            cluster_enabled=True,
        )
        nm = r.nodes_manager
        listener = self._register_listener(nm)
        # Default layout: slot 0 -> [7000 (primary), 7003 (replica)].
        # Redirect to the replica, triggering the failover branch.
        nm.move_slot(MovedError("0 127.0.0.1:7003"))

        listener.listen.assert_called_once()
        assert isinstance(listener.listen.call_args[0][0], AfterSlotsCacheRefreshEvent)

    @pytest.mark.fixed_client
    def test_move_slot_skips_dispatch_on_circular_redirect(self):
        """
        move_slot() must not dispatch AfterSlotsCacheRefreshEvent when the
        redirect points to the slot's current primary (no-op branch).
        """
        r = get_mocked_redis_client(
            host=default_host,
            port=default_port,
            cluster_enabled=True,
        )
        nm = r.nodes_manager
        listener = self._register_listener(nm)
        # Slot 0's current primary is 127.0.0.1:7000 -> no-op redirect.
        nm.move_slot(MovedError("0 127.0.0.1:7000"))

        listener.listen.assert_not_called()

    @pytest.mark.fixed_client
    def test_move_slot_remaps_redirect_address(self):
        """
        move_slot() must apply address_remap to the redirect address, which is
        the one the node advertises, and never cache the unmapped address.
        """

        def address_remap(address):
            host, port = address
            return host, port + 1000

        r = get_mocked_redis_client(
            host=default_host, port=default_port, address_remap=address_remap
        )
        nm = r.nodes_manager
        # Default layout: slot 0 -> 127.0.0.1:7000, cached as 127.0.0.1:8000.
        # Redirect to the primary of a different shard, by its advertised address.
        nm.move_slot(MovedError("0 127.0.0.1:7001"))

        assert [node.name for node in nm.slots_cache[0]] == ["127.0.0.1:8001"]
        assert "127.0.0.1:7001" not in nm.nodes_cache

        # A node not in the cache yet is added under its remapped address.
        nm.move_slot(MovedError("0 127.0.0.1:7006"))

        assert [node.name for node in nm.slots_cache[0]] == ["127.0.0.1:8006"]
        assert "127.0.0.1:7006" not in nm.nodes_cache

    @pytest.mark.fixed_client
    def test_move_slot_resolves_localhost_once(self):
        """
        A remap to localhost must be resolved once and reused, not on every
        node created or redirect looked up.
        """

        def address_remap(address):
            host, port = address
            return "localhost", port + 1000

        _resolve_localhost.cache_clear()
        try:
            with patch("socket.gethostbyname", return_value="127.0.0.1") as resolve:
                r = get_mocked_redis_client(
                    host=default_host, port=default_port, address_remap=address_remap
                )
                nm = r.nodes_manager
                nm.move_slot(MovedError("0 127.0.0.1:7001"))
                nm.move_slot(MovedError("0 127.0.0.1:7002"))

            assert [node.name for node in nm.slots_cache[0]] == ["127.0.0.1:8002"]
            resolve.assert_called_once_with("localhost")
        finally:
            _resolve_localhost.cache_clear()

    @pytest.mark.fixed_client
    def test_move_node_to_end_of_cached_nodes(self):
        """
        Test that move_node_to_end_of_cached_nodes moves a node to the end of
        startup_nodes and nodes_cache.
        """
        node1 = ClusterNode(default_host, 7000)
        node2 = ClusterNode(default_host, 7001)
        node3 = ClusterNode(default_host, 7002)

        with patch.object(NodesManager, "initialize"):
            nodes_manager = NodesManager(
                startup_nodes=[node1, node2, node3],
                require_full_coverage=False,
            )
            # Also populate nodes_cache with the same nodes
            nodes_manager.nodes_cache = {
                node1.name: node1,
                node2.name: node2,
                node3.name: node3,
            }

            # Verify initial order
            startup_node_names = list(nodes_manager.startup_nodes.keys())
            nodes_cache_names = list(nodes_manager.nodes_cache.keys())
            assert startup_node_names == [node1.name, node2.name, node3.name]
            assert nodes_cache_names == [node1.name, node2.name, node3.name]

            # Move first node to end
            nodes_manager.move_node_to_end_of_cached_nodes(node1.name)
            startup_node_names = list(nodes_manager.startup_nodes.keys())
            nodes_cache_names = list(nodes_manager.nodes_cache.keys())
            assert startup_node_names == [node2.name, node3.name, node1.name]
            assert nodes_cache_names == [node2.name, node3.name, node1.name]

            # Move middle node to end
            nodes_manager.move_node_to_end_of_cached_nodes(node3.name)
            startup_node_names = list(nodes_manager.startup_nodes.keys())
            nodes_cache_names = list(nodes_manager.nodes_cache.keys())
            assert startup_node_names == [node2.name, node1.name, node3.name]
            assert nodes_cache_names == [node2.name, node1.name, node3.name]

            # Moving last node should keep it at the end
            nodes_manager.move_node_to_end_of_cached_nodes(node3.name)
            startup_node_names = list(nodes_manager.startup_nodes.keys())
            nodes_cache_names = list(nodes_manager.nodes_cache.keys())
            assert startup_node_names == [node2.name, node1.name, node3.name]
            assert nodes_cache_names == [node2.name, node1.name, node3.name]

    @pytest.mark.fixed_client
    def test_move_node_to_end_of_cached_nodes_nonexistent(self):
        """
        Test that move_node_to_end_of_cached_nodes does nothing for a
        nonexistent node.
        """
        node1 = ClusterNode(default_host, 7000)
        node2 = ClusterNode(default_host, 7001)

        with patch.object(NodesManager, "initialize"):
            nodes_manager = NodesManager(
                startup_nodes=[node1, node2],
                require_full_coverage=False,
            )
            # Also populate nodes_cache
            nodes_manager.nodes_cache = {node1.name: node1, node2.name: node2}

            # Try to move a non-existent node - should not raise
            nodes_manager.move_node_to_end_of_cached_nodes("nonexistent:9999")
            startup_node_names = list(nodes_manager.startup_nodes.keys())
            nodes_cache_names = list(nodes_manager.nodes_cache.keys())
            assert startup_node_names == [node1.name, node2.name]
            assert nodes_cache_names == [node1.name, node2.name]

    @pytest.mark.fixed_client
    def test_move_node_to_end_of_cached_nodes_single_node(self):
        """
        Test that move_node_to_end_of_cached_nodes does nothing when there's
        only one node.
        """
        node1 = ClusterNode(default_host, 7000)

        with patch.object(NodesManager, "initialize"):
            nodes_manager = NodesManager(
                startup_nodes=[node1],
                require_full_coverage=False,
            )
            # Also populate nodes_cache
            nodes_manager.nodes_cache = {node1.name: node1}

            # Should not raise or change anything with single node
            nodes_manager.move_node_to_end_of_cached_nodes(node1.name)
            startup_node_names = list(nodes_manager.startup_nodes.keys())
            nodes_cache_names = list(nodes_manager.nodes_cache.keys())
            assert startup_node_names == [node1.name]
            assert nodes_cache_names == [node1.name]


@pytest.mark.fixed_client
class TestClusterPubSubWithMocks:
    """
    Unit tests for ClusterPubSub that do not require a running cluster.
    """

    def _make_pubsub(self, cluster_mock):
        """Create a ClusterPubSub with no node set, using the provided cluster mock."""
        from redis._parsers import Encoder
        from redis.cluster import ClusterPubSub

        cluster_mock.encoder = Encoder("utf-8", "strict", False)
        return ClusterPubSub(cluster_mock)

    def test_get_node_pubsub_uses_cluster_get_redis_connection(self):
        """
        _get_node_pubsub must route through cluster.get_redis_connection(node)
        so newly-discovered nodes are materialised via NodesManager.
        """
        cluster = Mock()
        redis_conn = Mock()
        shard_pubsub = Mock()
        redis_conn.pubsub.return_value = shard_pubsub
        cluster.get_redis_connection.return_value = redis_conn

        pubsub = self._make_pubsub(cluster)
        node = ClusterNode("127.0.0.1", 7000)

        result = pubsub._get_node_pubsub(node)

        cluster.get_redis_connection.assert_called_once_with(node)
        redis_conn.pubsub.assert_called_once_with(push_handler_func=None)
        assert result is shard_pubsub
        assert pubsub.node_pubsub_mapping[node.name] is shard_pubsub

    def test_get_node_pubsub_caches_by_node_name(self):
        """Repeated calls must not re-materialise the shard PubSub."""
        cluster = Mock()
        redis_conn = Mock()
        redis_conn.pubsub.return_value = Mock()
        cluster.get_redis_connection.return_value = redis_conn

        pubsub = self._make_pubsub(cluster)
        node = ClusterNode("127.0.0.1", 7000)

        first = pubsub._get_node_pubsub(node)
        second = pubsub._get_node_pubsub(node)

        assert first is second
        cluster.get_redis_connection.assert_called_once_with(node)
        redis_conn.pubsub.assert_called_once()

    def test_disconnect_tolerates_shard_pubsub_with_no_connection(self):
        """
        disconnect() must skip shard PubSub entries whose connection has not
        been materialised yet (pubsub.connection is None).
        """
        cluster = Mock()
        pubsub = self._make_pubsub(cluster)

        pending_pubsub = Mock()
        pending_pubsub.connection = None
        active_pubsub = Mock()
        active_pubsub.connection = Mock()

        pubsub.node_pubsub_mapping = {
            "127.0.0.1:7000": pending_pubsub,
            "127.0.0.1:7001": active_pubsub,
        }

        pubsub.disconnect()

        active_pubsub.connection.disconnect.assert_called_once()


@pytest.mark.onlycluster
class TestClusterPubSubObject:
    """
    Tests for the ClusterPubSub class
    """

    def test_init_pubsub_with_host_and_port(self, r):
        """
        Test creation of pubsub instance with passed host and port
        """
        node = r.get_default_node()
        p = r.pubsub(host=node.host, port=node.port)
        assert p.get_pubsub_node() == node

    def test_init_pubsub_with_node(self, r):
        """
        Test creation of pubsub instance with passed node
        """
        node = r.get_default_node()
        p = r.pubsub(node=node)
        assert p.get_pubsub_node() == node

    def test_init_pubusub_without_specifying_node(self, r):
        """
        Test creation of pubsub instance without specifying a node. The node
        should be determined based on the keyslot of the first command
        execution.
        """
        channel_name = "foo"
        node = r.get_node_from_key(channel_name)
        p = r.pubsub()
        assert p.get_pubsub_node() is None
        p.subscribe(channel_name)
        assert p.get_pubsub_node() == node

    def test_init_pubsub_with_a_non_existent_node(self, r):
        """
        Test creation of pubsub instance with node that doesn't exists in the
        cluster. RedisClusterException should be raised.
        """
        node = ClusterNode("1.1.1.1", 1111)
        with pytest.raises(RedisClusterException):
            r.pubsub(node)

    def test_init_pubsub_with_a_non_existent_host_port(self, r):
        """
        Test creation of pubsub instance with host and port that don't belong
        to a node in the cluster.
        RedisClusterException should be raised.
        """
        with pytest.raises(RedisClusterException):
            r.pubsub(host="1.1.1.1", port=1111)

    def test_init_pubsub_host_or_port(self, r):
        """
        Test creation of pubsub instance with host but without port, and vice
        versa. DataError should be raised.
        """
        with pytest.raises(DataError):
            r.pubsub(host="localhost")

        with pytest.raises(DataError):
            r.pubsub(port=16379)

    def test_get_redis_connection(self, r):
        """
        Test that get_redis_connection() returns the redis connection of the
        set pubsub node
        """
        node = r.get_default_node()
        p = r.pubsub(node=node)
        assert p.get_redis_connection() == node.redis_connection

    def test_disconnect_with_none_connections(self, r):
        """
        Test that disconnect() does not raise when pubsub.connection is None,
        both on the ClusterPubSub itself and on node pubsub instances in
        node_pubsub_mapping.
        """
        node = r.get_default_node()
        p = r.pubsub(node=node)
        # Ensure self.connection is None
        p.connection = None

        # Add a mock pubsub with connection=None into node_pubsub_mapping
        mock_pubsub = Mock()
        mock_pubsub.connection = None
        p.node_pubsub_mapping["fake_node"] = mock_pubsub

        # Should not raise AttributeError
        p.disconnect()

    def test_disconnect_calls_disconnect_on_existing_connections(self, r):
        """
        Test that disconnect() properly disconnects non-None connections
        on both self and node_pubsub_mapping entries.
        """
        node = r.get_default_node()
        p = r.pubsub(node=node)

        # Mock self.connection
        mock_conn = Mock()
        p.connection = mock_conn

        # Add a mock pubsub with a real connection into node_pubsub_mapping
        mock_node_conn = Mock()
        mock_node_pubsub = Mock()
        mock_node_pubsub.connection = mock_node_conn
        p.node_pubsub_mapping["fake_node"] = mock_node_pubsub

        p.disconnect()

        mock_conn.disconnect.assert_called_once()
        mock_node_conn.disconnect.assert_called_once()


@pytest.mark.fixed_client
def test_client_list_iter_blocked_on_cluster_pipeline():
    """
    client_list_iter sends the same CLIENT LIST command as client_list, which
    PIPELINE_BLOCKED_COMMANDS blocks on ClusterPipeline. client_list_iter has
    no wire-command entry of its own to add there, so it must be blocked
    explicitly too, or it would fall through to the inherited implementation
    and queue CLIENT LIST like a real pipelined command instead of raising.
    """
    r = get_mocked_redis_client(host=default_host, port=default_port)
    try:
        pipe = r.pipeline()
        with pytest.raises(RedisClusterException):
            pipe.client_list_iter()
    finally:
        r.close()


@pytest.mark.fixed_client
def test_evalsha_not_in_pipeline_blocked_commands():
    """EVALSHA must be usable in ClusterPipeline (see #2914)."""
    assert "EVALSHA" not in PIPELINE_BLOCKED_COMMANDS


@pytest.mark.fixed_client
def test_evalsha_can_be_queued_on_cluster_pipeline():
    """
    Queuing EVALSHA on a ClusterPipeline must not raise the historical
    'blocked when running redis in cluster mode' error. Slot selection for
    EVALSHA is already handled by RedisCluster.determine_slot().
    """
    r = get_mocked_redis_client(host=default_host, port=default_port)
    try:
        pipe = r.pipeline()
        sha = "a" * 40
        returned = pipe.evalsha(sha, 1, "foo", "bar")
        assert returned is pipe

        queue = pipe._execution_strategy.command_queue
        assert len(queue) == 1
        assert queue[0].args == ("EVALSHA", sha, 1, "foo", "bar")

        assert r.determine_slot("EVALSHA", sha, 1, "foo", "bar") == key_slot(b"foo")
    finally:
        r.close()


@pytest.mark.fixed_client
def test_script_queues_evalsha_on_cluster_pipeline():
    """Script must queue EVALSHA on ClusterPipeline without calling script_load."""
    r = get_mocked_redis_client(host=default_host, port=default_port)
    try:
        script = r.register_script("return 1")
        with r.pipeline() as pipe:
            script(client=pipe)
            queue = pipe._execution_strategy.command_queue
            assert len(queue) == 1
            assert queue[0].args[0] == "EVALSHA"
            assert queue[0].args[1] == script.sha
    finally:
        r.close()


@pytest.mark.fixed_client
def test_evalsha_zero_keys_pipeline_execute_skips_get_command_keys():
    """
    Sync ClusterPipeline must route EVALSHA via determine_slot, not
    COMMAND GETKEYS. Redis <7 fails on GETKEYS for EVALSHA with numkeys=0.
    """
    r = get_mocked_redis_client(host=default_host, port=default_port)
    try:
        mock_all_nodes_resp(r, 42)
        sha = "a" * 40
        with r.pipeline() as pipe:
            pipe.evalsha(sha, 0)
            with patch.object(
                pipe,
                "_get_command_keys",
                side_effect=AssertionError(
                    "COMMAND GETKEYS must not be used for EVALSHA"
                ),
            ):
                assert pipe.execute() == [42]
    finally:
        r.close()


@pytest.mark.fixed_client
def test_evalsha_zero_keys_reuses_slot_in_transaction():
    """
    Zero-key EVALSHA in a transactional pipeline must reuse one slot so
    execute() does not raise CrossSlotTransactionError.
    """
    r = get_mocked_redis_client(host=default_host, port=default_port)
    try:
        sha = "a" * 40
        with r.pipeline(transaction=True) as pipe:
            pipe.evalsha(sha, 0)
            pipe.evalsha(sha, 0)
            slots = pipe._execution_strategy._pipeline_slots
            assert len(slots) == 1

            # Keyed follow-up must retarget without needing a live COMMAND map.
            keyed_slot = key_slot(b"foo")
            with patch.object(pipe, "determine_slot", return_value=keyed_slot):
                pipe.set("foo", "bar")
            assert pipe._execution_strategy._pipeline_slots == {keyed_slot}
            assert pipe._execution_strategy._transaction_has_keyed_slot is True
    finally:
        r.close()


@pytest.mark.fixed_client
def test_evalsha_zero_keys_follows_keyed_slot_in_transaction():
    """Zero-key EVALSHA after a keyed slot is fixed reuses that slot."""
    r = get_mocked_redis_client(host=default_host, port=default_port)
    try:
        sha = "a" * 40
        with r.pipeline(transaction=True) as pipe:
            strategy = pipe._execution_strategy
            strategy._pipeline_slots = {key_slot(b"foo")}
            strategy._transaction_has_keyed_slot = True
            pipe.evalsha(sha, 0)
            assert strategy._pipeline_slots == {key_slot(b"foo")}
    finally:
        r.close()


@pytest.mark.fixed_client
def test_slotless_command_does_not_lock_keyed_slot_flag():
    """Slotless commands must not block later keyed retargeting."""
    r = get_mocked_redis_client(host=default_host, port=default_port)
    try:
        sha = "a" * 40
        with r.pipeline(transaction=True) as pipe:
            pipe.evalsha(sha, 0)
            assert pipe._execution_strategy._transaction_has_keyed_slot is False

            with patch.object(pipe, "determine_slot", return_value=None):
                pipe.execute_command("CLIENT TRACKING", "ON")
            assert pipe._execution_strategy._transaction_has_keyed_slot is False

            keyed_slot = key_slot(b"foo")
            with patch.object(pipe, "determine_slot", return_value=keyed_slot):
                pipe.set("foo", "bar")
            assert pipe._execution_strategy._pipeline_slots == {keyed_slot}
            assert pipe._execution_strategy._transaction_has_keyed_slot is True
    finally:
        r.close()


@pytest.mark.fixed_client
def test_evalsha_zero_keys_then_cross_slot_raises_at_execute():
    """
    Zero-key EVALSHA retarget must not swallow a later true cross-slot
    conflict: two keyed commands on different slots still raise at execute().
    """
    r = get_mocked_redis_client(host=default_host, port=default_port)
    try:
        sha = "a" * 40
        slot_a = key_slot(b"{foo}a")
        slot_b = key_slot(b"{bar}b")
        assert slot_a != slot_b
        with r.pipeline(transaction=True) as pipe:
            with patch.object(
                pipe, "determine_slot", side_effect=[111, slot_a, slot_b]
            ):
                pipe.evalsha(sha, 0)
                pipe.set("{foo}a", "1")
                pipe.set("{bar}b", "2")
            assert pipe._execution_strategy._pipeline_slots == {slot_a, slot_b}
            with pytest.raises(
                CrossSlotTransactionError,
                match=(
                    "All keys involved in a cluster transaction "
                    "must map to the same slot"
                ),
            ):
                pipe.execute()
    finally:
        r.close()


@pytest.mark.onlycluster
class TestClusterPipeline:
    """
    Tests for the ClusterPipeline class
    """

    def test_blocked_methods(self, r):
        """
        Currently some method calls on a Cluster pipeline
        is blocked when using in cluster mode.
        They maybe implemented in the future.
        """
        pipe = r.pipeline()

        with pytest.raises(RedisClusterException):
            pipe.load_scripts()

        with pytest.raises(RedisClusterException):
            pipe.script_load_for_pipeline(None)

        with pytest.raises(RedisClusterException):
            pipe.eval()

    def test_blocked_arguments(self, r):
        """
        Currently some arguments is blocked when using in cluster mode.
        They maybe implemented in the future.
        """
        with pytest.raises(RedisClusterException) as ex:
            r.pipeline(shard_hint=True)

        assert (
            str(ex.value).startswith("shard_hint is deprecated in cluster mode") is True
        )

    @skip_if_server_version_lt("7.0.0")
    def test_a_retry_of_the_pipeline_resends_only_the_unanswered_commands(
        self, r, no_cluster_retry_delay
    ):
        """
        An error the pipeline does not retry per command, such as CLUSTERDOWN, retries the
        whole pipeline. That retry resends only the commands no node answered: a write
        that already succeeded would otherwise run twice. The reply of an answered command
        is kept as it was parsed, so its response callback is not applied a second time.
        """
        first, second = _keys_on_every_shard(r, "stack")[:2]
        slots = r.execute_command("CLUSTER SLOTS")
        failed = []

        def fail(args):
            if args == ("INCR", second) and not failed:
                failed.append(args)
                return ClusterDownError("CLUSTERDOWN The cluster is down")
            return None

        with _pipelined_commands(fail) as batches:
            with r.pipeline() as pipe:
                pipe.execute_command("INCR", first)
                pipe.execute_command("CLUSTER SLOTS")
                pipe.execute_command("INCR", second)
                assert pipe.execute() == [1, slots, 1]

        assert failed
        # Each command reached the server once: the failed one only on the retry
        assert sorted(args for batch in batches for args, _ in batch) == sorted(
            [("INCR", first), ("CLUSTER SLOTS",), ("INCR", second)]
        )

    def test_evalsha_in_pipeline(self, r):
        """
        EVALSHA is allowed in ClusterPipeline when keys map to one slot
        and the script is already loaded on cluster primaries (#2914).
        """
        multiply = "return redis.call('GET', KEYS[1]) * ARGV[1]"
        sha = r.script_load(multiply)
        # hash tag keeps the key on a known slot for the pipeline
        r.set("{user}a", 2)
        with r.pipeline() as pipe:
            pipe.evalsha(sha, 1, "{user}a", 3)
            assert pipe.execute() == [6]

    def test_evalsha_zero_keys_in_pipeline(self, r):
        """
        EVALSHA with numkeys=0 must execute in ClusterPipeline without
        calling COMMAND GETKEYS (broken on Redis <7 for this case).
        """
        sha = r.script_load("return 42")
        with r.pipeline() as pipe:
            pipe.evalsha(sha, 0)
            assert pipe.execute() == [42]

    def test_evalsha_zero_keys_in_transaction_pipeline(self, r):
        """
        Multiple zero-key EVALSHA calls in a transactional pipeline share
        one slot and execute successfully.
        """
        sha = r.script_load("return 42")
        with r.pipeline(transaction=True) as pipe:
            pipe.evalsha(sha, 0)
            pipe.evalsha(sha, 0)
            assert pipe.execute() == [42, 42]

    def test_redis_cluster_pipeline(self, r):
        """
        Test that we can use a pipeline with the RedisCluster class
        """
        with r.pipeline() as pipe:
            pipe.set("foo", "bar")
            pipe.get("foo")
            assert pipe.execute() == [True, b"bar"]

    def test_mget_disabled(self, r):
        """
        Test that mget is disabled for ClusterPipeline
        """
        with r.pipeline() as pipe:
            with pytest.raises(RedisClusterException):
                pipe.mget(["a"])

    def test_mset_disabled(self, r):
        """
        Test that mset is disabled for ClusterPipeline
        """
        with r.pipeline() as pipe:
            with pytest.raises(RedisClusterException):
                pipe.mset({"a": 1, "b": 2})

    def test_rename_disabled(self, r):
        """
        Test that rename is disabled for ClusterPipeline
        """
        with r.pipeline(transaction=False) as pipe:
            with pytest.raises(RedisClusterException):
                pipe.rename("a", "b")

    def test_renamenx_disabled(self, r):
        """
        Test that renamenx is disabled for ClusterPipeline
        """
        with r.pipeline(transaction=False) as pipe:
            with pytest.raises(RedisClusterException):
                pipe.renamenx("a", "b")

    def test_delete_single(self, r):
        """
        Test a single delete operation
        """
        r["a"] = 1
        with r.pipeline(transaction=False) as pipe:
            pipe.delete("a")
            assert pipe.execute() == [1]

    def test_multi_delete_unsupported_cross_slot(self, r):
        """
        Test that multi delete operation is unsupported
        """
        with r.pipeline(transaction=False) as pipe:
            r["a"] = 1
            r["b"] = 2
            with pytest.raises(RedisClusterException):
                pipe.delete("a", "b")

    def test_multi_delete_supported_single_slot(self, r):
        """
        Test that multi delete operation is supported when all keys are in the same hash slot
        """
        with r.pipeline(transaction=True) as pipe:
            r["{key}:a"] = 1
            r["{key}:b"] = 2
            pipe.delete("{key}:a", "{key}:b")
            assert pipe.execute()

    def test_touch_single(self, r):
        """
        Test that touch is queued and executed with the pipeline
        """
        r["a"] = 1
        with r.pipeline(transaction=False) as pipe:
            assert pipe.touch("a") is pipe
            assert len(pipe) == 1
            assert pipe.execute() == [1]

    def test_touch_cross_slot(self, r):
        """
        Test that touch over keys in different slots is queued as one command
        and returns the summed count
        """
        r["a"] = 1
        r["b"] = 2
        assert r.keyslot("a") != r.keyslot("b")
        with r.pipeline(transaction=False) as pipe:
            pipe.touch("a", "b")
            assert len(pipe) == 1
            assert pipe.execute() == [2]

    def test_touch_not_required_from_execution_strategy_subclasses(self):
        """
        Test that a strategy that does not define touch can still be
        instantiated, and that its touch falls back to execute_command
        """
        stubs = {
            name: lambda self, *args, **kwargs: args
            for name in ExecutionStrategy.__abstractmethods__
        }
        strategy = type("StrategyWithoutTouch", (ExecutionStrategy,), stubs)()
        assert strategy.touch("a", "b") == ("TOUCH", "a", "b")

    def test_unlink_single(self, r):
        """
        Test a single unlink operation
        """
        r["a"] = 1
        with r.pipeline(transaction=False) as pipe:
            pipe.unlink("a")
            assert pipe.execute() == [1]

    def test_multi_unlink_unsupported(self, r):
        """
        Test that multi unlink operation is unsupported
        """
        with r.pipeline(transaction=False) as pipe:
            r["a"] = 1
            r["b"] = 2
            with pytest.raises(RedisClusterException):
                pipe.unlink("a", "b")

    def test_brpoplpush_disabled(self, r):
        """
        Test that brpoplpush is disabled for ClusterPipeline
        """
        with r.pipeline(transaction=False) as pipe:
            with pytest.raises(RedisClusterException):
                pipe.brpoplpush()

    def test_rpoplpush_disabled(self, r):
        """
        Test that rpoplpush is disabled for ClusterPipeline
        """
        with r.pipeline(transaction=False) as pipe:
            with pytest.raises(RedisClusterException):
                pipe.rpoplpush()

    def test_sort_disabled(self, r):
        """
        Test that sort is disabled for ClusterPipeline
        """
        with r.pipeline(transaction=False) as pipe:
            with pytest.raises(RedisClusterException):
                pipe.sort()

    def test_sdiff_disabled(self, r):
        """
        Test that sdiff is disabled for ClusterPipeline
        """
        with r.pipeline(transaction=False) as pipe:
            with pytest.raises(RedisClusterException):
                pipe.sdiff()

    def test_sdiffstore_disabled(self, r):
        """
        Test that sdiffstore is disabled for ClusterPipeline
        """
        with r.pipeline(transaction=False) as pipe:
            with pytest.raises(RedisClusterException):
                pipe.sdiffstore()

    def test_sinter_disabled(self, r):
        """
        Test that sinter is disabled for ClusterPipeline
        """
        with r.pipeline(transaction=False) as pipe:
            with pytest.raises(RedisClusterException):
                pipe.sinter()

    def test_sinterstore_disabled(self, r):
        """
        Test that sinterstore is disabled for ClusterPipeline
        """
        with r.pipeline(transaction=False) as pipe:
            with pytest.raises(RedisClusterException):
                pipe.sinterstore()

    def test_smove_disabled(self, r):
        """
        Test that move is disabled for ClusterPipeline
        """
        with r.pipeline(transaction=False) as pipe:
            with pytest.raises(RedisClusterException):
                pipe.smove()

    def test_sunion_disabled(self, r):
        """
        Test that sunion is disabled for ClusterPipeline
        """
        with r.pipeline(transaction=False) as pipe:
            with pytest.raises(RedisClusterException):
                pipe.sunion()

    def test_sunionstore_disabled(self, r):
        """
        Test that sunionstore is disabled for ClusterPipeline
        """
        with r.pipeline(transaction=False) as pipe:
            with pytest.raises(RedisClusterException):
                pipe.sunionstore()

    def test_spfmerge_disabled(self, r):
        """
        Test that spfmerge is disabled for ClusterPipeline
        """
        with r.pipeline(transaction=False) as pipe:
            with pytest.raises(RedisClusterException):
                pipe.pfmerge()

    def test_multi_key_operation_with_a_single_slot(self, r):
        """
        Test multi key operation with a single slot
        """
        pipe = r.pipeline(transaction=False)
        pipe.set("a{foo}", 1)
        pipe.set("b{foo}", 2)
        pipe.set("c{foo}", 3)
        pipe.get("a{foo}")
        pipe.get("b{foo}")
        pipe.get("c{foo}")

        res = pipe.execute()
        assert res == [True, True, True, b"1", b"2", b"3"]

    def test_multi_key_operation_with_multi_slots(self, r):
        """
        Test multi key operation with more than one slot
        """
        pipe = r.pipeline(transaction=False)
        pipe.set("a{foo}", 1)
        pipe.set("b{foo}", 2)
        pipe.set("c{foo}", 3)
        pipe.set("bar", 4)
        pipe.set("bazz", 5)
        pipe.get("a{foo}")
        pipe.get("b{foo}")
        pipe.get("c{foo}")
        pipe.get("bar")
        pipe.get("bazz")
        res = pipe.execute()
        assert res == [True, True, True, True, True, b"1", b"2", b"3", b"4", b"5"]

    def test_connection_error_not_raised(self, r):
        """
        Test that the pipeline doesn't raise an error on connection error when
        raise_on_error=False
        """
        key = "foo"
        node = r.get_node_from_key(key, False)

        def raise_connection_error():
            e = ConnectionError("error")
            return e

        with r.pipeline() as pipe:
            mock_node_resp_func(node, raise_connection_error)
            res = pipe.get(key).get(key).execute(raise_on_error=False)
            assert node.redis_connection.connection.read_response.called
            assert isinstance(res[0], ConnectionError)

    @pytest.mark.usefixtures("no_cluster_retry_delay")
    def test_connection_error_raised(self, r):
        """
        Test that the pipeline raises an error on connection error when
        raise_on_error=True
        """
        key = "foo"
        node = r.get_node_from_key(key, False)

        def raise_connection_error():
            e = ConnectionError("error")
            return e

        r.reinitialize_steps = 0
        with r.pipeline() as pipe:
            mock_node_resp_func(node, raise_connection_error)
            with pytest.raises(ConnectionError):
                pipe.get(key).get(key).execute(raise_on_error=True)

    def test_asking_error(self, r):
        """
        Test redirection on ASK error
        """
        key = "foo"
        first_node = r.get_node_from_key(key, False)
        ask_node = None
        for node in r.get_nodes():
            if node != first_node:
                ask_node = node
                break
        if ask_node is None:
            warnings.warn("skipping this test since the cluster has only one node")
            return
        ask_msg = f"{r.keyslot(key)} {ask_node.host}:{ask_node.port}"

        def raise_ask_error():
            raise AskError(ask_msg)

        with r.pipeline() as pipe:
            mock_node_resp_func(first_node, raise_ask_error)
            mock_node_resp(ask_node, "MOCK_OK")
            res = pipe.get(key).execute()
            assert first_node.redis_connection.connection.read_response.called
            assert ask_node.redis_connection.connection.read_response.called
            assert res == ["MOCK_OK"]

    def test_error_is_truncated(self, r):
        """
        Test that an error from the pipeline is truncated correctly.
        """
        key = "a" * 50
        a_value = "a" * 20
        b_value = "b" * 20

        with r.pipeline() as pipe:
            pipe.set(key, 1)
            pipe.hset(key, mapping={"field_a": a_value, "field_b": b_value})
            pipe.expire(key, 100)

            with pytest.raises(Exception) as ex:
                pipe.execute()

            expected = f"Command # 2 (HSET {key} field_a {a_value} field_b...) of pipeline caused error: "
            assert str(ex.value).startswith(expected)

    def test_return_previously_acquired_connections(self, r):
        # in order to ensure that a pipeline will make use of connections
        #   from different nodes
        assert r.keyslot("a") != r.keyslot("b")

        orig_func = redis.cluster.get_connection
        with patch("redis.cluster.get_connection") as get_connection:

            def raise_error(target_node, *args, **kwargs):
                if get_connection.call_count == 2:
                    raise ConnectionError("mocked error")
                else:
                    return orig_func(target_node, *args, **kwargs)

            get_connection.side_effect = raise_error

            r.pipeline().get("a").get("b").execute()

        # 4 = 2 get_connections per execution * 2 executions
        assert get_connection.call_count == 4
        for cluster_node in r.nodes_manager.nodes_cache.values():
            connection_pool = cluster_node.redis_connection.connection_pool
            num_of_conns = len(connection_pool._available_connections)
            assert num_of_conns == connection_pool._created_connections

    def test_empty_stack(self, r):
        """
        If pipeline is executed with no commands it should
        return a empty list.
        """
        p = r.pipeline()
        result = p.execute()
        assert result == []

    @pytest.mark.onlycluster
    def test_exec_error_in_response(self, r):
        """
        an invalid pipeline command at exec time adds the exception instance
        to the list of returned values
        """
        hashkey = "{key}"
        r[f"{hashkey}:c"] = "a"
        with r.pipeline() as pipe:
            pipe.set(f"{hashkey}:a", 1).set(f"{hashkey}:b", 2)
            pipe.lpush(f"{hashkey}:c", 3).set(f"{hashkey}:d", 4)
            result = pipe.execute(raise_on_error=False)

            assert result[0]
            assert r[f"{hashkey}:a"] == b"1"
            assert result[1]
            assert r[f"{hashkey}:b"] == b"2"

            # we can't lpush to a key that's a string value, so this should
            # be a ResponseError exception
            assert isinstance(result[2], redis.ResponseError)
            assert r[f"{hashkey}:c"] == b"a"

            # since this isn't a transaction, the other commands after the
            # error are still executed
            assert result[3]
            assert r[f"{hashkey}:d"] == b"4"

            # make sure the pipe was restored to a working state
            assert pipe.set(f"{hashkey}:z", "zzz").execute() == [True]
            assert r[f"{hashkey}:z"] == b"zzz"

    def test_exec_error_in_no_transaction_pipeline(self, r):
        r["a"] = 1
        with r.pipeline(transaction=False) as pipe:
            pipe.llen("a")
            pipe.expire("a", 100)

            with pytest.raises(redis.ResponseError) as ex:
                pipe.execute()

            assert str(ex.value).startswith(
                "Command # 1 (LLEN a) of pipeline caused error: "
            )

        assert r["a"] == b"1"

    @pytest.mark.onlycluster
    @skip_if_server_version_lt("2.0.0")
    def test_pipeline_discard(self, r):
        hashkey = "{key}"

        # empty pipeline should raise an error
        with r.pipeline() as pipe:
            pipe.set(f"{hashkey}:key", "someval")
            with pytest.raises(redis.exceptions.RedisClusterException) as ex:
                pipe.discard()

            assert str(ex.value).startswith(
                "method discard() is not supported outside of transactional context"
            )

        # setting a pipeline and discarding should do the same
        with r.pipeline() as pipe:
            pipe.set(f"{hashkey}:key", "someval")
            pipe.set(f"{hashkey}:someotherkey", "val")
            response = pipe.execute()
            pipe.set(f"{hashkey}:key", "another value!")
            with pytest.raises(redis.exceptions.RedisClusterException) as ex:
                pipe.discard()

            assert str(ex.value).startswith(
                "method discard() is not supported outside of transactional context"
            )

            pipe.set(f"{hashkey}:foo", "bar")
            response = pipe.execute()

        assert response[0]
        assert r.get(f"{hashkey}:foo") == b"bar"

    def test_connection_leak_on_non_timeout_error_during_connect(self, r):
        """
        Test that connections are not leaked when a non-TimeoutError/ConnectionError
        is raised during get_connection(). The bugfix ensures that if an error
        occurs that isn't explicitly handled, we don't leak connections.
        """
        # Ensure keys map to different nodes
        assert r.keyslot("a") != r.keyslot("b")

        orig_func = redis.cluster.get_connection
        with patch("redis.cluster.get_connection") as get_connection:

            def raise_custom_error(target_node, *args, **kwargs):
                # Raise a RuntimeError (not ConnectionError or TimeoutError)
                # on the second call (when getting second connection)
                if get_connection.call_count == 2:
                    raise RuntimeError("Some unexpected error during connection")
                else:
                    return orig_func(target_node, *args, **kwargs)

            get_connection.side_effect = raise_custom_error

            with pytest.raises(RuntimeError):
                r.pipeline().get("a").get("b").execute()

        # Verify that all connections were returned to the pool
        # (not leaked) even though a non-standard error was raised
        for cluster_node in r.nodes_manager.nodes_cache.values():
            connection_pool = cluster_node.redis_connection.connection_pool
            num_of_conns = len(connection_pool._available_connections)
            assert num_of_conns == connection_pool._created_connections, (
                f"Connection leaked: expected {connection_pool._created_connections} "
                f"available, got {num_of_conns}"
            )

    def test_dirty_connection_not_reused(self, r):
        """
        Test that dirty connections (with unread responses) are not reused.
        A dirty connection is one where we've written commands but haven't
        read all responses. If such a connection is returned to the pool,
        the next caller will read responses from the previous request.
        """
        # Ensure we're using multiple nodes to test the dirty connection scenario
        assert r.keyslot("a") != r.keyslot("b")

        # Mock the write method to raise an error after writing to only some nodes
        orig_write = redis.cluster.NodeCommands.write

        write_count = 0

        def mock_write(self):
            nonlocal write_count
            write_count += 1
            # Allow the first write to succeed
            if write_count == 1:
                return orig_write(self)
            # Simulate a failure after the first write (leaving connection dirty)
            else:
                raise RuntimeError("Simulated write error")

        # Patch Connection.disconnect so we can assert that at least one
        # connection was disconnected when the write error occurred.
        original_disconnect = Connection.disconnect
        disconnect_called = []

        def track_disconnect(self, *args):
            disconnect_called.append(True)
            return original_disconnect(self, *args)

        with patch.object(Connection, "disconnect", track_disconnect):
            with patch.object(redis.cluster.NodeCommands, "write", mock_write):
                with pytest.raises(RuntimeError):
                    r.pipeline().get("a").get("b").execute()

            # Ensure that at least one connection was disconnected as part of
            # handling the dirty connection created by the write failure.
            assert disconnect_called, (
                "Expected at least one connection to be disconnected when "
                "handling a dirty connection, but disconnect() was not called."
            )
        # After the error, verify that no connections are in the available pool
        # with dirty state (unread responses). If a connection is dirty, it should
        # have been disconnected before being returned to the pool.
        # We verify this by checking the connections can be reused successfully.
        try:
            # Try to execute a command on each connection to verify
            # they're clean (not holding responses from previous requests)
            result = r.ping()
            assert result is True
        except Exception as e:
            pytest.fail(
                f"Connection reuse after dirty state failed: {e}. "
                f"This indicates a dirty connection was returned to the pool."
            )


@pytest.mark.onlycluster
class TestReadOnlyPipeline:
    """
    Tests for ClusterPipeline class in readonly mode
    """

    def test_pipeline_readonly(self, r):
        """
        On readonly mode, we supports get related stuff only.
        """
        r.readonly(target_nodes="all")
        r.set("foo71", "a1")  # we assume this key is set on 127.0.0.1:7001
        r.zadd("foo88", {"z1": 1})  # we assume this key is set on 127.0.0.1:7002
        r.zadd("foo88", {"z2": 4})

        with r.pipeline() as readonly_pipe:
            readonly_pipe.get("foo71").zrange("foo88", 0, 5, withscores=True)
            assert_resp_response(
                r,
                readonly_pipe.execute(),
                [b"a1", [(b"z1", 1.0), (b"z2", 4)]],
                [b"a1", [[b"z1", 1.0], [b"z2", 4.0]]],
            )

    def test_moved_redirection_on_slave_with_default(self, r):
        """
        On Pipeline, we redirected once and finally get from master with
        readonly client when data is completely moved.
        """
        key = "bar"
        r.set(key, "foo")
        # set read_from_replicas to True
        r.read_from_replicas = True
        primary = r.get_node_from_key(key, False)
        replica = r.get_node_from_key(key, True)
        with r.pipeline() as readwrite_pipe:
            mock_node_resp(primary, "MOCK_FOO")
            if replica is not None:
                moved_error = f"{r.keyslot(key)} {primary.host}:{primary.port}"

                def raise_moved_error():
                    raise MovedError(moved_error)

                mock_node_resp_func(replica, raise_moved_error)
            assert readwrite_pipe.reinitialize_counter == 0
            readwrite_pipe.get(key).get(key)
            assert readwrite_pipe.execute() == ["MOCK_FOO", "MOCK_FOO"]
            if replica is not None:
                # the slot has a replica as well, so MovedError should have
                # occurred. If MovedError occurs, we should see the
                # reinitialize_counter increase.
                assert readwrite_pipe.reinitialize_counter == 1
                conn = replica.redis_connection.connection
                assert conn.read_response.called is True

    def test_readonly_pipeline_from_readonly_client(self, request):
        """
        Test that the pipeline is initialized with readonly mode if the client
        has it enabled
        """
        # Create a cluster with reading from replications
        ro = _get_client(RedisCluster, request, read_from_replicas=True)
        key = "bar"
        ro.set(key, "foo")
        with ro.pipeline() as readonly_pipe:
            mock_all_nodes_resp(ro, "MOCK_OK")
            assert readonly_pipe.read_from_replicas is True
            assert readonly_pipe.get(key).get(key).execute() == ["MOCK_OK", "MOCK_OK"]
            slot_nodes = ro.nodes_manager.slots_cache[ro.keyslot(key)]
            if len(slot_nodes) > 1:
                executed_on_replica = False
                for node in slot_nodes:
                    if node.server_type == REPLICA:
                        conn = node.redis_connection.connection
                        executed_on_replica = conn.read_response.called
                        if executed_on_replica:
                            break
                assert executed_on_replica is True

    @pytest.mark.parametrize(
        "load_balancing_strategy",
        [
            LoadBalancingStrategy.ROUND_ROBIN_REPLICAS,
            LoadBalancingStrategy.RANDOM_REPLICA,
        ],
    )
    def test_readonly_pipeline_with_reading_from_replicas_strategies(
        self, request, load_balancing_strategy: LoadBalancingStrategy
    ) -> None:
        """
        Test that the pipeline uses replicas for different replica-based
        load balancing strategies.
        """
        ro = _get_client(
            RedisCluster,
            request,
            load_balancing_strategy=load_balancing_strategy,
        )
        key = "bar"
        ro.set(key, "foo")

        with ro.pipeline() as readonly_pipe:
            mock_all_nodes_resp(ro, "MOCK_OK")
            assert readonly_pipe.load_balancing_strategy == load_balancing_strategy
            assert readonly_pipe.get(key).get(key).execute() == ["MOCK_OK", "MOCK_OK"]
            slot_nodes = ro.nodes_manager.slots_cache[ro.keyslot(key)]
            executed_on_replicas_only = True
            for node in slot_nodes:
                if node.server_type == PRIMARY:
                    conn = node.redis_connection.connection
                    if conn.read_response.called:
                        executed_on_replicas_only = False
                        break
            assert executed_on_replicas_only


@pytest.mark.onlycluster
class TestClusterMonitor:
    def test_wait_command_not_found(self, r):
        "Make sure the wait_for_command func works when command is not found"
        key = "foo"
        node = r.get_node_from_key(key)
        with r.monitor(target_node=node) as m:
            response = wait_for_command(r, m, "nothing", key=key)
            assert response is None

    def test_response_values(self, r):
        db = 0
        key = "foo"
        node = r.get_node_from_key(key)
        with r.monitor(target_node=node) as m:
            r.ping(target_nodes=node)
            response = wait_for_command(r, m, "PING", key=key)
            assert isinstance(response["time"], float)
            assert response["db"] == db
            assert response["client_type"] in ("tcp", "unix")
            assert isinstance(response["client_address"], str)
            assert isinstance(response["client_port"], str)
            assert response["command"] == "PING"

    def test_command_with_quoted_key(self, r):
        key = "{foo}1"
        node = r.get_node_from_key(key)
        with r.monitor(node) as m:
            r.get('{foo}"bar')
            response = wait_for_command(r, m, 'GET {foo}"bar', key=key)
            assert response["command"] == 'GET {foo}"bar'

    def test_command_with_binary_data(self, r):
        key = "{foo}1"
        node = r.get_node_from_key(key)
        with r.monitor(target_node=node) as m:
            byte_string = b"{foo}bar\x92"
            r.get(byte_string)
            response = wait_for_command(r, m, "GET {foo}bar\\x92", key=key)
            assert response["command"] == "GET {foo}bar\\x92"

    def test_command_with_escaped_data(self, r):
        key = "{foo}1"
        node = r.get_node_from_key(key)
        with r.monitor(target_node=node) as m:
            byte_string = b"{foo}bar\\x92"
            r.get(byte_string)
            response = wait_for_command(r, m, "GET {foo}bar\\\\x92", key=key)
            assert response["command"] == "GET {foo}bar\\\\x92"

    def test_flush(self, r):
        r.set("x", "1")
        r.set("z", "1")
        r.flushall()
        assert r.get("x") is None
        assert r.get("y") is None


@pytest.mark.onlycluster
class TestClusterMetricsRecording:
    """
    Integration tests that verify metrics are properly recorded
    from RedisCluster and delivered to the Meter through the observability recorder.

    These tests use a real Redis cluster connection but mock the OTel Meter
    to verify metrics are correctly recorded.
    """

    @pytest.fixture
    def mock_meter(self):
        """Create a mock Meter that tracks all instrument calls."""
        meter = Mock()

        # Create mock histogram for operation duration
        self.operation_duration = Mock()

        def create_histogram_side_effect(name, **kwargs):
            if name == "db.client.operation.duration":
                return self.operation_duration
            return Mock()

        meter.create_counter.return_value = Mock()
        meter.create_up_down_counter.return_value = Mock()
        meter.create_histogram.side_effect = create_histogram_side_effect

        return meter

    @pytest.fixture
    def cluster_with_otel(self, r, mock_meter):
        """
        Setup a RedisCluster with real connection and mocked OTel collector.
        Returns tuple of (cluster, operation_duration_mock).
        """

        # Reset any existing collector state
        recorder.reset_collector()

        # Create config with COMMAND group enabled
        config = OTelConfig(metric_groups=[MetricGroup.COMMAND])

        # Create collector with mocked meter
        with patch("redis.observability.metrics.OTEL_AVAILABLE", True):
            collector = RedisMetricsCollector(mock_meter, config)

        # Patch the recorder to use our collector
        with patch.object(recorder, "_metrics_collector", collector):
            # Create a new event dispatcher and attach it to the cluster
            event_dispatcher = EventDispatcher()
            r._event_dispatcher = event_dispatcher

            yield r, self.operation_duration

        # Cleanup
        recorder.reset_collector()

    def test_execute_command_records_metric(self, cluster_with_otel):
        """
        Test that execute_command records operation duration metric to Meter.
        """
        cluster, operation_duration_mock = cluster_with_otel

        # Execute a command
        cluster.set("test_key", "test_value")

        # Verify the Meter's histogram.record() was called
        operation_duration_mock.record.assert_called()

        # Get the last call arguments
        call_args = operation_duration_mock.record.call_args

        # Verify duration was recorded
        duration = call_args[0][0]
        assert isinstance(duration, float)
        assert duration >= 0

        # Verify attributes
        attrs = call_args[1]["attributes"]
        assert attrs["db.operation.name"] == "SET"
        assert "server.address" in attrs
        assert "server.port" in attrs
        assert "db.namespace" in attrs

    def test_get_command_records_metric(self, cluster_with_otel):
        """
        Test that GET command records metric with correct command name.
        """
        cluster, operation_duration_mock = cluster_with_otel

        # Execute GET command
        cluster.get("test_key")

        # Verify command name is GET
        call_args = operation_duration_mock.record.call_args
        attrs = call_args[1]["attributes"]
        assert attrs["db.operation.name"] == "GET"

    def test_multiple_commands_record_multiple_metrics(self, cluster_with_otel):
        """
        Test that multiple command executions record multiple metrics.
        """
        cluster, operation_duration_mock = cluster_with_otel

        # Execute multiple commands
        cluster.set("key1", "value1")
        cluster.get("key1")
        cluster.delete("key1")

        # Verify histogram.record() was called 3 times
        assert operation_duration_mock.record.call_count == 3

    def test_server_attributes_recorded(self, cluster_with_otel):
        """
        Test that server address, port, and db namespace are recorded.
        """
        cluster, operation_duration_mock = cluster_with_otel

        cluster.ping()

        call_args = operation_duration_mock.record.call_args
        attrs = call_args[1]["attributes"]

        # Verify server attributes are present and have valid values
        assert "server.address" in attrs
        assert isinstance(attrs["server.address"], str)
        assert len(attrs["server.address"]) > 0

        assert "server.port" in attrs
        assert isinstance(attrs["server.port"], int)
        assert attrs["server.port"] > 0

        assert "db.namespace" in attrs

    def test_duration_is_positive(self, cluster_with_otel):
        """
        Test that the recorded duration is a positive float.
        """
        cluster, operation_duration_mock = cluster_with_otel

        cluster.set("duration_test", "value")

        call_args = operation_duration_mock.record.call_args
        duration = call_args[0][0]

        assert isinstance(duration, float)
        assert duration >= 0

    def test_no_batch_size_for_single_command(self, cluster_with_otel):
        """
        Test that single commands don't include batch_size attribute.
        """
        cluster, operation_duration_mock = cluster_with_otel

        cluster.get("single_command_key")

        call_args = operation_duration_mock.record.call_args
        attrs = call_args[1]["attributes"]

        # batch_size should not be present for single commands
        assert "db.operation.batch_size" not in attrs

    def test_different_commands_record_correct_names(self, cluster_with_otel):
        """
        Test that different commands record metrics with correct command names.
        """
        cluster, operation_duration_mock = cluster_with_otel

        commands_to_test = [
            ("SET", lambda: cluster.set("cmd_test", "value")),
            ("GET", lambda: cluster.get("cmd_test")),
            ("PIPELINE", lambda: cluster.delete("cmd_test")),
            ("PING", lambda: cluster.ping()),
        ]

        for expected_cmd, cmd_func in commands_to_test:
            operation_duration_mock.reset_mock()
            cmd_func()

            call_args = operation_duration_mock.record.call_args
            attrs = call_args[1]["attributes"]
            assert attrs["db.operation.name"] == expected_cmd

    def test_command_error_records_metric_with_error_type(self, cluster_with_otel):
        """
        Test that when a command fails, the recorded metric includes error.type attribute.
        """
        cluster, operation_duration_mock = cluster_with_otel

        # Execute a command that will fail (wrong type operation)
        cluster.set("error_test_key", "string_value")

        try:
            # Try to use LPUSH on a string key - this will fail
            cluster.lpush("error_test_key", "value")
        except ResponseError:
            pass

        # Find the LPUSH event in recorded calls
        lpush_calls = [
            call_obj
            for call_obj in operation_duration_mock.record.call_args_list
            if call_obj[1]["attributes"].get("db.operation.name") == "LPUSH"
        ]

        assert len(lpush_calls) >= 1
        attrs = lpush_calls[0][1]["attributes"]
        assert "error.type" in attrs


@pytest.mark.onlycluster
class TestClusterPipelineMetricsRecording:
    """
    Integration tests that verify metrics are properly recorded
    from ClusterPipeline and delivered to the Meter through the observability recorder.

    These tests use a real Redis cluster connection but mock the OTel Meter
    to verify metrics are correctly recorded.
    """

    @pytest.fixture
    def mock_meter(self):
        """Create a mock Meter that tracks all instrument calls."""
        meter = Mock()

        # Create mock histogram for operation duration
        self.operation_duration = Mock()

        def create_histogram_side_effect(name, **kwargs):
            if name == "db.client.operation.duration":
                return self.operation_duration
            return Mock()

        meter.create_counter.return_value = Mock()
        meter.create_up_down_counter.return_value = Mock()
        meter.create_histogram.side_effect = create_histogram_side_effect

        return meter

    @pytest.fixture
    def cluster_pipeline_with_otel(self, r, mock_meter):
        """
        Setup a ClusterPipeline with real connection and mocked OTel collector.
        Returns tuple of (cluster, operation_duration_mock).
        """

        # Reset any existing collector state
        recorder.reset_collector()

        # Create config with COMMAND group enabled
        config = OTelConfig(metric_groups=[MetricGroup.COMMAND])

        # Create collector with mocked meter
        with patch("redis.observability.metrics.OTEL_AVAILABLE", True):
            collector = RedisMetricsCollector(mock_meter, config)

        # Patch the recorder to use our collector
        with patch.object(recorder, "_metrics_collector", collector):
            # Create a new event dispatcher and attach it to the cluster
            event_dispatcher = EventDispatcher()
            r._event_dispatcher = event_dispatcher

            yield r, self.operation_duration

        # Cleanup
        recorder.reset_collector()

    def test_pipeline_execute_records_metric(self, cluster_pipeline_with_otel):
        """
        Test that pipeline execute records operation duration metric to Meter.
        """
        cluster, operation_duration_mock = cluster_pipeline_with_otel

        # Execute a pipeline
        pipe = cluster.pipeline()
        pipe.set("pipe_key1", "value1")
        pipe.get("pipe_key1")
        pipe.execute()

        # Verify the Meter's histogram.record() was called
        operation_duration_mock.record.assert_called()

        # Get the last call arguments (pipeline event)
        call_args = operation_duration_mock.record.call_args

        # Verify duration was recorded
        duration = call_args[0][0]
        assert isinstance(duration, float)
        assert duration >= 0

        # Verify attributes
        attrs = call_args[1]["attributes"]
        assert attrs["db.operation.name"] == "PIPELINE"

    def test_pipeline_server_attributes_recorded(self, cluster_pipeline_with_otel):
        """
        Test that server address, port, and db namespace are recorded for pipeline.
        """
        cluster, operation_duration_mock = cluster_pipeline_with_otel

        pipe = cluster.pipeline()
        pipe.set("server_attr_key", "value")
        pipe.execute()

        # Find the PIPELINE event call
        pipeline_call = None
        for call_obj in operation_duration_mock.record.call_args_list:
            attrs = call_obj[1]["attributes"]
            if attrs.get("db.operation.name") == "PIPELINE":
                pipeline_call = call_obj
                break

        assert pipeline_call is not None
        attrs = pipeline_call[1]["attributes"]

        # Verify server attributes are present
        assert "server.address" in attrs
        assert isinstance(attrs["server.address"], str)

        assert "server.port" in attrs
        assert isinstance(attrs["server.port"], int)

        assert "db.namespace" in attrs

    def test_pipeline_duration_is_positive(self, cluster_pipeline_with_otel):
        """
        Test that the recorded duration for pipeline is a positive float.
        """
        cluster, operation_duration_mock = cluster_pipeline_with_otel

        pipe = cluster.pipeline()
        pipe.set("duration_key", "value")
        pipe.execute()

        # Find the PIPELINE event call
        pipeline_call = None
        for call_obj in operation_duration_mock.record.call_args_list:
            attrs = call_obj[1]["attributes"]
            if attrs.get("db.operation.name") == "PIPELINE":
                pipeline_call = call_obj
                break

        assert pipeline_call is not None
        duration = pipeline_call[0][0]
        assert isinstance(duration, float)
        assert duration >= 0

    def test_multiple_pipeline_executions_record_multiple_metrics(
        self, cluster_pipeline_with_otel
    ):
        """
        Test that multiple pipeline executions record multiple metrics.
        """
        cluster, operation_duration_mock = cluster_pipeline_with_otel

        # Execute first pipeline
        pipe1 = cluster.pipeline()
        pipe1.set("multi_key1", "value1")
        pipe1.execute()

        # Execute second pipeline
        pipe2 = cluster.pipeline()
        pipe2.set("multi_key2", "value2")
        pipe2.execute()

        # Count PIPELINE events
        pipeline_count = sum(
            1
            for call_obj in operation_duration_mock.record.call_args_list
            if call_obj[1]["attributes"].get("db.operation.name") == "PIPELINE"
        )

        assert pipeline_count >= 2

    def test_empty_pipeline_does_not_record_metric(self, cluster_pipeline_with_otel):
        """
        Test that an empty pipeline does not record metrics.
        """
        cluster, operation_duration_mock = cluster_pipeline_with_otel

        # Execute an empty pipeline
        pipe = cluster.pipeline()
        pipe.execute()

        # Count PIPELINE events - should be 0
        pipeline_count = sum(
            1
            for call_obj in operation_duration_mock.record.call_args_list
            if call_obj[1]["attributes"].get("db.operation.name") == "PIPELINE"
        )

        assert pipeline_count == 0

    def test_pipeline_error_records_metric_with_error(self, cluster_pipeline_with_otel):
        """
        Test that when a pipeline command fails, the recorded metric includes error.
        """
        cluster, operation_duration_mock = cluster_pipeline_with_otel

        # Set a string value
        cluster.set("pipe_error_key", "string_value")

        try:
            # Execute a pipeline with a command that will fail
            pipe = cluster.pipeline()
            pipe.lpush("pipe_error_key", "value")  # Will fail - wrong type
            pipe.execute()
        except ResponseError:
            pass

        # Find the PIPELINE event calls
        pipeline_calls = [
            call_obj
            for call_obj in operation_duration_mock.record.call_args_list
            if call_obj[1]["attributes"].get("db.operation.name") == "PIPELINE"
        ]

        # There should be at least one PIPELINE event
        assert len(pipeline_calls) >= 1

    def test_pipeline_error_records_metric_with_error_type(
        self, cluster_pipeline_with_otel
    ):
        """
        Test that when a pipeline command fails, the recorded metric includes error.type.
        """
        cluster, operation_duration_mock = cluster_pipeline_with_otel

        # Set a string value
        cluster.set("pipe_err_type_key", "string_value")

        try:
            pipe = cluster.pipeline()
            pipe.lpush("pipe_err_type_key", "value")  # Will fail - wrong type
            pipe.execute()
        except ResponseError:
            pass

        # Find PIPELINE events with error.type
        pipeline_calls = [
            call_obj
            for call_obj in operation_duration_mock.record.call_args_list
            if call_obj[1]["attributes"].get("db.operation.name") == "PIPELINE"
        ]

        # At least one PIPELINE event should exist
        assert len(pipeline_calls) >= 1

        # Check if any has error.type (the one from _raise_first_error)
        error_calls = [
            call_obj
            for call_obj in pipeline_calls
            if "error.type" in call_obj[1]["attributes"]
        ]

        # The error event should have error.type
        if error_calls:
            attrs = error_calls[0][1]["attributes"]
            assert "error.type" in attrs

    def test_pipeline_multi_node_records_multiple_metrics(
        self, cluster_pipeline_with_otel
    ):
        """
        Test that pipeline commands to multiple nodes record metrics for each node.
        """
        cluster, operation_duration_mock = cluster_pipeline_with_otel

        # Execute pipeline with keys that may go to different nodes
        pipe = cluster.pipeline()
        for i in range(10):
            pipe.set(f"multi_node_key_{i}", f"value_{i}")
        pipe.execute()

        # Find PIPELINE events
        pipeline_calls = [
            call_obj
            for call_obj in operation_duration_mock.record.call_args_list
            if call_obj[1]["attributes"].get("db.operation.name") == "PIPELINE"
        ]

        # Should have at least one PIPELINE event
        assert len(pipeline_calls) >= 1

        # Each event should have server info
        for call_obj in pipeline_calls:
            attrs = call_obj[1]["attributes"]
            assert "server.address" in attrs
            assert "server.port" in attrs
            assert "db.namespace" in attrs

    def test_pipeline_duration_recorded_per_node(self, cluster_pipeline_with_otel):
        """
        Test that pipeline duration is recorded for each node's commands.
        """
        cluster, operation_duration_mock = cluster_pipeline_with_otel

        pipe = cluster.pipeline()
        pipe.set("duration_node_key", "value")
        pipe.get("duration_node_key")
        pipe.execute()

        # Find PIPELINE events
        pipeline_calls = [
            call_obj
            for call_obj in operation_duration_mock.record.call_args_list
            if call_obj[1]["attributes"].get("db.operation.name") == "PIPELINE"
        ]

        assert len(pipeline_calls) >= 1

        # Each event should have a positive duration
        for call_obj in pipeline_calls:
            duration = call_obj[0][0]
            assert isinstance(duration, float)
            assert duration >= 0


def test_cluster_shards_slots_shape_is_protocol_agnostic():
    """CLUSTER SHARDS slots must be the same (start, end) tuples whether the reply
    arrived as RESP2 (flat shard arrays) or RESP3 (shard maps). The slots value is
    a flat ``[start, end, ...]`` integer array on both protocols; only the outer
    shard container changes, so both parse paths must pair it identically."""
    resp2 = [
        [
            b"slots",
            [0, 5460, 10000, 11000],
            b"nodes",
            [[b"id", b"abc", b"role", b"master"]],
        ]
    ]
    resp3 = [
        {
            b"slots": [0, 5460, 10000, 11000],
            b"nodes": [{b"id": b"abc", b"role": b"master"}],
        }
    ]
    expected = [(0, 5460), (10000, 11000)]

    for parser in (parse_cluster_shards_unified, parse_cluster_shards_with_str_keys):
        assert parser(resp2)[0]["slots"] == expected
        assert parser(resp3)[0]["slots"] == expected
        assert parser(resp2)[0]["slots"] == parser(resp3)[0]["slots"]

    # An empty CLUSTER SHARDS reply (node with no slots assigned) must not raise.
    assert parse_cluster_shards([]) == []
    assert parse_cluster_shards_unified([]) == []
    assert parse_cluster_shards_with_str_keys([]) == []
