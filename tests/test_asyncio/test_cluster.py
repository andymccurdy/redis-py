import asyncio
import contextlib
import binascii
import datetime
import gc
import logging
import platform
import ssl
import warnings
import weakref
from typing import (
    Any,
    Awaitable,
    Callable,
    Dict,
    List,
    Optional,
    Set,
    Tuple,
    Type,
    Union,
)
from unittest import mock
from urllib.parse import urlparse

import pytest
import pytest_asyncio
from _pytest.fixtures import FixtureRequest
from redis._parsers import AsyncCommandsParser
from redis.asyncio.client import _PIPELINE_PACK_OFFLOAD_MIN_COMMANDS
from redis.asyncio.cluster import (
    ClusterNode,
    NodesManager,
    PipelineCommand,
    RedisCluster,
)
from redis.asyncio.connection import Connection, SSLConnection, async_timeout
from redis.asyncio.retry import Retry
from redis.auth.idp import IdentityProviderInterface
from redis.auth.token import SimpleToken
from redis.auth.token_manager import (
    CredentialsListener,
    RetryPolicy,
    TokenManager,
    TokenManagerConfig,
)
from redis.backoff import (
    AbstractBackoff,
    ExponentialBackoff,
    ExponentialWithJitterBackoff,
    NoBackoff,
)
from redis.cluster import (
    _REPLICAS_ONLY_STRATEGIES,
    PIPELINE_BLOCKED_COMMANDS,
    PRIMARY,
    REPLICA,
    LoadBalancingStrategy,
    _resolve_localhost,
    get_node_name,
)
from redis.commands.cluster import AsyncClusterMultiKeyCommands
from redis.commands.core import HotkeysMetricsTypes
from redis.commands.metadata import (
    AsyncDynamicMetadataResolver,
    CommandMetadata,
    RequestPolicy,
    ResponsePolicy,
)
from redis.commands.policies import AsyncStaticPolicyResolver
from redis.crc import REDIS_CLUSTER_HASH_SLOTS, key_slot
from redis.credentials import StreamingCredentialProvider
from redis.event import (
    AfterAsyncClusterInstantiationEvent,
    AsyncAfterSlotsCacheRefreshEvent,
    AsyncEventListenerInterface,
    EventDispatcher,
    RegisterReAuthForAsyncClusterNodes,
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
    TryAgainError,
)
from redis.exceptions import TimeoutError as RedisTimeoutError
from redis.himport import HIMPORT_SET
from redis.utils import str_if_bytes
from tests.test_cluster import _SPIN_LIBRARY, _busy_node, _keys_on_every_shard
from tests.test_command_metadata import (
    CACHEABLE_KEYED,
    slot_routed_args,
    slot_routed_static_commands,
)
from tests.conftest import (
    assert_resp_response,
    expects_resp2_shape,
    expects_unified_shape,
    get_protocol_version,
    patch_monotonic,
    skip_if_redis_enterprise,
    skip_if_server_version_lt,
    skip_unless_arch_bits,
)

from unittest.mock import patch, Mock
from redis.asyncio.observability import recorder as async_recorder
from redis.observability.config import OTelConfig, MetricGroup
from redis.observability.metrics import RedisMetricsCollector

from ..ssl_utils import get_tls_certificates
from .compat import aclosing

default_host = "127.0.0.1"
default_port = 7000
default_cluster_slots = [
    [0, 8191, ["127.0.0.1", 7000, "node_0"], ["127.0.0.1", 7003, "node_3"]],
    [8192, 16383, ["127.0.0.1", 7001, "node_1"], ["127.0.0.1", 7002, "node_2"]],
]


class NodeProxy:
    """A class to proxy a node connection to a different port"""

    def __init__(self, addr, redis_addr):
        self.addr = addr
        self.redis_addr = redis_addr
        self.server = None
        self.task = None
        self.n_connections = 0

    async def start(self):
        # test that we can connect to redis
        async with async_timeout(2):
            _, redis_writer = await asyncio.open_connection(*self.redis_addr)
        redis_writer.close()
        self.server = await asyncio.start_server(
            self.handle, *self.addr, reuse_address=True
        )
        self.task = asyncio.create_task(self.server.serve_forever())

    async def handle(self, reader, writer):
        # establish connection to redis
        redis_reader, redis_writer = await asyncio.open_connection(*self.redis_addr)
        try:
            self.n_connections += 1
            pipe1 = asyncio.create_task(self.pipe(reader, redis_writer))
            pipe2 = asyncio.create_task(self.pipe(redis_reader, writer))
            await asyncio.gather(pipe1, pipe2)
        finally:
            redis_writer.close()
            await self.redis_writer.wait_closed()
            writer.close()
            await writer.wait_closed()

    async def aclose(self):
        try:
            self.task.cancel()
            await asyncio.wait_for(self.task, timeout=1)
            self.server.close()
            await self.server.wait_closed()
        except asyncio.TimeoutError:
            pass
        except asyncio.CancelledError:
            pass

    async def pipe(
        self,
        reader: asyncio.StreamReader,
        writer: asyncio.StreamWriter,
    ):
        while True:
            data = await reader.read(1000)
            if not data:
                break
            writer.write(data)
            await writer.drain()


@pytest_asyncio.fixture()
async def slowlog(r: RedisCluster) -> None:
    """
    Set the slowlog threshold to 0, and the
    max length to 128. This will force every
    command into the slowlog and allow us
    to test it
    """
    # Save old values
    current_config = await r.config_get(target_nodes=r.get_primaries()[0])
    old_slower_than_value = current_config["slowlog-log-slower-than"]
    old_max_length_value = current_config["slowlog-max-len"]

    # Set the new values
    await r.config_set("slowlog-log-slower-than", 0)
    await r.config_set("slowlog-max-len", 128)

    yield

    await r.config_set("slowlog-log-slower-than", old_slower_than_value)
    await r.config_set("slowlog-max-len", old_max_length_value)


async def get_mocked_redis_client(
    cluster_slots_raise_error=False, *args, **kwargs
) -> RedisCluster:
    """
    Return a stable RedisCluster object that have deterministic
    nodes and slots setup to remove the problem of different IP addresses
    on different installations and machines.
    """
    cluster_slots = kwargs.pop("cluster_slots", default_cluster_slots)
    coverage_res = kwargs.pop("coverage_result", "yes")
    cluster_enabled = kwargs.pop("cluster_enabled", True)
    with mock.patch.object(ClusterNode, "execute_command") as execute_command_mock:

        async def execute_command(*_args, **_kwargs):
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
            else:
                return await execute_command_mock(*_args, **_kwargs)

        execute_command_mock.side_effect = execute_command

        with mock.patch.object(
            AsyncCommandsParser, "initialize", autospec=True
        ) as cmd_parser_initialize:

            def cmd_init_mock(self, r: ClusterNode) -> None:
                self.node = r
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

            # Create a subclass of RedisCluster that overrides __del__
            class MockedRedisCluster(RedisCluster):
                def __del__(self):
                    # Override to prevent connection cleanup attempts
                    pass

                @property
                def connection_pool(self):
                    # Required abstract property implementation
                    return self.nodes_manager.get_default_node().redis_connection.connection_pool

            return await MockedRedisCluster(*args, **kwargs)


def mock_node_resp(node: ClusterNode, response: Any) -> ClusterNode:
    connection = mock.AsyncMock(spec=Connection)
    connection._resp2_fallback = False
    connection.is_connected = True
    connection.should_reconnect.return_value = False
    connection.read_response.return_value = response
    while node._free:
        node._free.pop()
    node._free.append(connection)
    return node


def mock_node_resp_exc(node: ClusterNode, exc: Exception) -> ClusterNode:
    connection = mock.AsyncMock(spec=Connection)
    connection._resp2_fallback = False
    connection.is_connected = True
    connection.should_reconnect.return_value = False
    connection.read_response.side_effect = exc
    while node._free:
        node._free.pop()
    node._free.append(connection)
    return node


def mock_all_nodes_resp(rc: RedisCluster, response: Any) -> RedisCluster:
    for node in rc.get_nodes():
        mock_node_resp(node, response)
    return rc


async def moved_redirection_helper(
    create_redis: Callable[..., RedisCluster],
    failover: bool = False,
    circular_moved=False,
) -> None:
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
    rc = await create_redis(cls=RedisCluster, flushdb=False)
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
        # Use one of the primaries to be the redirected node
        redirect_node = rc.get_primaries()[0]
    r_host = redirect_node.host
    r_port = redirect_node.port
    with mock.patch.object(
        ClusterNode, "execute_command", autospec=True
    ) as execute_command:

        def moved_redirect_effect(self, *args, **options):
            def ok_response(self, *args, **options):
                assert self.host == r_host
                assert self.port == r_port

                return "MOCK_OK"

            execute_command.side_effect = ok_response
            raise MovedError(f"{slot} {r_host}:{r_port}")

        execute_command.side_effect = moved_redirect_effect
        assert await rc.execute_command("SET", "foo", "bar") == "MOCK_OK"
        slot_primary = rc.nodes_manager.slots_cache[slot][0]
        assert slot_primary == redirect_node
        if failover:
            assert rc.get_node(host=r_host, port=r_port).server_type == PRIMARY
            assert prev_primary.server_type == REPLICA
        elif circular_moved:
            fetched_node = rc.get_node(host=r_host, port=r_port)
            assert fetched_node == prev_primary
            assert fetched_node.server_type == PRIMARY


@pytest.mark.usefixtures("no_topology_refresh_interval")
class TestMovedReinitialization:
    @pytest.mark.fixed_client
    @pytest.mark.parametrize("reinitialize_steps", [0, 1, 3])
    async def test_moved_asks_for_refresh_run_by_next_command(self, reinitialize_steps):
        r = await get_mocked_redis_client(
            host=default_host,
            port=default_port,
            reinitialize_steps=reinitialize_steps,
        )
        key = "foo"
        slot = r.keyslot(key)
        refreshes = []
        command_nodes = []

        async def execute_command(node, command, *args, **kwargs):
            if command == "CLUSTER SLOTS":
                refreshes.append(node.name)
                return cluster_slots
            assert command == "GET"
            command_nodes.append(node.port)
            if node.port != owner_port:
                raise MovedError(f"{slot} {default_host}:{owner_port}")
            return b"value"

        async with aclosing(r):
            with (
                mock.patch.object(
                    ClusterNode,
                    "execute_command",
                    autospec=True,
                    side_effect=execute_command,
                ),
                mock.patch.object(AsyncCommandsParser, "initialize", autospec=True),
            ):
                expected_refreshes = 0
                expected_counter = 0
                # Move the key back and forth, crossing the refresh threshold.
                for moved_count in range(1, 7):
                    owner_port = 7000 if moved_count % 2 else 7001
                    other_port = 7001 if owner_port == 7000 else 7000
                    cluster_slots = [
                        [0, 8191, [default_host, other_port, "other"]],
                        [8192, 16383, [default_host, owner_port, "owner"]],
                    ]
                    refresh_requested = r._initialize
                    sent_before = len(command_nodes)

                    assert await r.get(key) == b"value"
                    if refresh_requested:
                        # The command runs the refresh a MOVED asked for before
                        # it is sent, so it goes straight to the new owner
                        expected_refreshes += 1
                        assert command_nodes[sent_before:] == [owner_port]
                        assert r.nodes_manager._requested_additional_startup_nodes == ()
                    else:
                        # The MOVED patches the slot and the redirect follows
                        # it; the refresh is only asked for
                        assert command_nodes[sent_before:] == [other_port, owner_port]
                        expected_counter += 1
                    threshold_reached = bool(
                        reinitialize_steps
                        and not refresh_requested
                        and expected_counter % reinitialize_steps == 0
                    )
                    if threshold_reached:
                        expected_counter = 0
                        assert r.nodes_manager._requested_additional_startup_nodes == (
                            (default_host, owner_port),
                        )
                    assert r.get_node_from_key(key).port == owner_port
                    assert r._initialize is threshold_reached
                    assert len(refreshes) == expected_refreshes
                    assert r.reinitialize_counter == expected_counter
                if reinitialize_steps:
                    assert expected_refreshes > 0

    @pytest.mark.fixed_client
    async def test_refresh_queries_the_node_a_moved_pointed_to(self):
        """
        The node a MOVED pointed to is kept with the refresh request, so the
        refresh also queries it when no startup node answers.
        """
        r = await get_mocked_redis_client(host=default_host, port=default_port)
        moved_to = (default_host, 7006)
        queried = []

        async def execute_command(node, command, *args, **kwargs):
            assert command == "CLUSTER SLOTS"
            queried.append((node.host, node.port))
            if (node.host, node.port) != moved_to:
                raise ConnectionError("unreachable")
            return default_cluster_slots

        r.nodes_manager._requested_additional_startup_nodes = (moved_to,)
        r._initialize = True
        r.nodes_manager._last_refresh_at = r.nodes_manager._refresh_due_at = None
        async with aclosing(r):
            with mock.patch.object(
                ClusterNode,
                "execute_command",
                autospec=True,
                side_effect=execute_command,
            ):
                await r.initialize()

            assert queried[-1] == moved_to
            assert r._initialize is False
            assert r.nodes_manager._requested_additional_startup_nodes == ()


class RecordingBackoff(AbstractBackoff):
    """A backoff that returns ``step * failures`` and records every call."""

    def __init__(self, step: float = 0.5) -> None:
        self.step = step
        self.computed: List[int] = []
        self.resets = 0

    def reset(self) -> None:
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

    @pytest.mark.fixed_client
    async def test_requested_refresh_waits_until_due(self) -> None:
        r = await get_mocked_redis_client(host=default_host, port=default_port)
        r._initialize = True
        with mock.patch.object(NodesManager, "initialize", autospec=True) as init:
            # The client was just created, so the interval has not passed: the
            # current topology keeps serving and the request stays
            await r.initialize()
            init.assert_not_called()
            assert r._initialize is True

            r.nodes_manager._last_refresh_at = r.nodes_manager._refresh_due_at = None
            await r.initialize()
            init.assert_awaited_once()
            assert r._initialize is False

    @pytest.mark.fixed_client
    @pytest.mark.parametrize(
        "error",
        [
            RedisClusterException("All slots are not covered"),
            RedisClusterUnreachableError("down"),
        ],
    )
    async def test_requested_refresh_with_an_unusable_topology_keeps_the_current_one(
        self, error
    ) -> None:
        """
        A requested refresh that reaches no node, or whose nodes answer with a
        topology that cannot replace the current one, does not fail the command
        that runs it, and stays asked for.
        """
        r = await get_mocked_redis_client(host=default_host, port=default_port)
        r.nodes_manager._last_refresh_at = r.nodes_manager._refresh_due_at = None
        r._initialize = True
        with mock.patch.object(
            NodesManager, "initialize", autospec=True, side_effect=error
        ):
            assert await r.initialize() is r

        assert r._initialize is True
        assert r.nodes_manager.default_node is not None

    @pytest.mark.fixed_client
    @pytest.mark.usefixtures("no_cluster_retry_delay")
    async def test_failed_node_is_tried_last_by_a_later_refresh(self) -> None:
        """
        A connection error while the refresh is not due yet still records the
        failed node: the refresh that a later command runs queries it last, and
        clears it once it succeeds.
        """
        r = await get_mocked_redis_client(host=default_host, port=default_port)
        failed = r.get_node_from_key("foo")
        failures = [ConnectionError("Connection closed by server.")]
        queried = []

        async def execute_command(node, *args, **kwargs):
            if args[0] == "CLUSTER SLOTS":
                queried.append(node.name)
                if node.name != failed.name:
                    raise ConnectionError("down")
                return default_cluster_slots
            if failures:
                raise failures.pop(0)
            return b"value"

        with mock.patch.object(
            ClusterNode, "execute_command", autospec=True, side_effect=execute_command
        ):
            with mock.patch.object(NodesManager, "_refresh_due", return_value=False):
                # The retry runs on the current topology, no refresh yet
                assert await r.get("foo") == b"value"
            assert queried == []
            assert r._initialize is True
            assert r.nodes_manager._requested_last_failed_node_name == failed.name

            r.nodes_manager._last_refresh_at = r.nodes_manager._refresh_due_at = None
            assert await r.get("foo") == b"value"

        assert len(queried) > 1
        assert queried[-1] == failed.name
        assert failed.name not in queried[:-1]
        assert r.nodes_manager._requested_last_failed_node_name is None

    @pytest.mark.fixed_client
    async def test_requests_are_merged_until_a_refresh_answers_them(self) -> None:
        r = await get_mocked_redis_client(host=default_host, port=default_port)
        nm = r.nodes_manager
        r._request_refresh(
            additional_startup_nodes_info=[("127.0.0.1", 7001)],
            last_failed_node_name="127.0.0.1:7000",
        )
        r._request_refresh(
            additional_startup_nodes_info=[("127.0.0.1", 7001), ("127.0.0.1", 7002)]
        )

        assert r._initialize is True
        assert nm._requested_additional_startup_nodes == (
            ("127.0.0.1", 7001),
            ("127.0.0.1", 7002),
        )
        assert nm._requested_last_failed_node_name == "127.0.0.1:7000"

        async def execute_command(node, *args, **kwargs):
            return default_cluster_slots

        nm._last_refresh_at = nm._refresh_due_at = None
        with mock.patch.object(
            ClusterNode, "execute_command", autospec=True, side_effect=execute_command
        ):
            await r.initialize()

        assert r._initialize is False
        assert nm._requested_additional_startup_nodes == ()
        assert nm._requested_last_failed_node_name is None

    @pytest.mark.fixed_client
    async def test_failed_node_name_outlives_refreshes_that_do_not_answer_it(
        self,
    ) -> None:
        """
        The failed node is kept after a refresh that failed, and a node that
        failed while a refresh was running is kept for the next one.
        """
        nodes_manager = NodesManager(
            startup_nodes=[ClusterNode(default_host, 7000)],
            require_full_coverage=True,
            connection_kwargs={},
        )
        nodes_manager._requested_last_failed_node_name = "127.0.0.1:7001"
        slots = [[0, 16383, [default_host, 7000, "node_0"]]]

        async def unreachable(node, *args, **kwargs):
            raise ConnectionError("down")

        async def reachable_while_another_node_fails(node, *args, **kwargs):
            nodes_manager._requested_last_failed_node_name = "127.0.0.1:7002"
            return slots

        with mock.patch.object(
            ClusterNode, "execute_command", autospec=True, side_effect=unreachable
        ):
            with pytest.raises(RedisClusterException):
                await nodes_manager.initialize()
        assert nodes_manager._requested_last_failed_node_name == "127.0.0.1:7001"

        with mock.patch.object(
            ClusterNode,
            "execute_command",
            autospec=True,
            side_effect=reachable_while_another_node_fails,
        ):
            await nodes_manager.initialize()
        assert nodes_manager._requested_last_failed_node_name == "127.0.0.1:7002"

    @pytest.mark.fixed_client
    async def test_failed_refreshes_are_counted_and_back_off(self) -> None:
        nodes_manager = NodesManager(
            startup_nodes=[ClusterNode(default_host, 7000)],
            require_full_coverage=True,
            connection_kwargs={},
        )
        slots = [[0, 16383, [default_host, 7000, "node_0"]]]

        async def unreachable(node, *args, **kwargs):
            raise ConnectionError("down")

        async def reachable(node, *args, **kwargs):
            return slots

        with mock.patch.object(
            ClusterNode, "execute_command", autospec=True, side_effect=unreachable
        ):
            with pytest.raises(RedisClusterException):
                await nodes_manager.initialize()
        assert nodes_manager._failed_refreshes == 1

        backoff = RecordingBackoff(step=5.0)
        nodes_manager._refresh_due_at = None
        # The retry backoff gives 5 s for one failure, above the 1 s minimum
        last = nodes_manager._last_refresh_at
        retry = Retry(backoff, 3)
        with patch_monotonic("redis.asyncio.cluster", last + 4.9):
            assert nodes_manager._refresh_due(retry) is False
        with patch_monotonic("redis.asyncio.cluster", last + 5.0):
            assert nodes_manager._refresh_due(retry) is True
        assert backoff.computed == [1]

        with mock.patch.object(
            ClusterNode, "execute_command", autospec=True, side_effect=reachable
        ):
            await nodes_manager.initialize()
        assert nodes_manager._failed_refreshes == 0

    @pytest.mark.fixed_client
    async def test_cancelled_refresh_is_not_counted_but_waits_the_interval(
        self,
    ) -> None:
        """
        A refresh cancelled before it ended (for example by a timeout around the
        command that ran it) is not a failed refresh, but the next one still
        waits the interval, so callers that keep timing out do not refresh
        back to back.
        """
        nodes_manager = NodesManager(
            startup_nodes=[ClusterNode(default_host, 7000)],
            require_full_coverage=True,
            connection_kwargs={},
        )
        nodes_manager._last_refresh_unreachable_args = ("down",)
        started = asyncio.Event()

        async def hanging(node, *args, **kwargs):
            started.set()
            await asyncio.Event().wait()

        with (
            mock.patch.object(
                ClusterNode, "execute_command", autospec=True, side_effect=hanging
            ),
            patch_monotonic("redis.asyncio.cluster", 200.0),
        ):
            task = asyncio.create_task(nodes_manager.initialize())
            await started.wait()
            task.cancel()
            with pytest.raises(asyncio.CancelledError):
                await task

        assert nodes_manager._failed_refreshes == 0
        assert nodes_manager._last_refresh_at == 200.0
        assert nodes_manager._last_refresh_unreachable_args is None
        with patch_monotonic("redis.asyncio.cluster", 200.9):
            assert nodes_manager._refresh_due() is False
        with patch_monotonic("redis.asyncio.cluster", 201.0):
            assert nodes_manager._refresh_due() is True

    @pytest.mark.fixed_client
    @pytest.mark.parametrize("due", [True, False], ids=["due", "not-due"])
    @pytest.mark.parametrize("call", ["command", "pipeline", "transaction"])
    async def test_attempt_awaits_initialize_only_once_a_refresh_is_due(
        self, call, due
    ) -> None:
        """
        While a requested refresh is not due yet, command, pipeline and
        transaction attempts do not await initialize(), which would return at
        once; once it is due, the next attempt runs it.
        """
        r = await get_mocked_redis_client(host=default_host, port=default_port)
        r._request_refresh()
        if due:
            nm = r.nodes_manager
            nm._last_refresh_at = nm._refresh_due_at = None

        async def refresh(client, *args, **kwargs):
            # As a refresh that succeeds
            client._initialize = False
            return client

        async def execute_pipeline(node, commands) -> bool:
            for command in commands:
                command.result = b"value"
            return False

        with (
            mock.patch.object(
                RedisCluster, "initialize", autospec=True, side_effect=refresh
            ) as initialize,
            mock.patch.object(
                ClusterNode, "execute_command", autospec=True, return_value=b"value"
            ),
            mock.patch.object(
                ClusterNode,
                "execute_pipeline",
                autospec=True,
                side_effect=execute_pipeline,
            ),
            # Ends the transaction right after its attempt checked for a
            # refresh, without a retry
            mock.patch.object(
                ClusterNode,
                "acquire_connection",
                autospec=True,
                side_effect=AuthenticationError("invalid password"),
            ),
        ):
            if call == "command":
                await r.get("foo")
            elif call == "pipeline":
                async with r.pipeline() as pipe:
                    await pipe.get("foo").execute()
            else:
                async with r.pipeline(transaction=True) as pipe:
                    with pytest.raises(AuthenticationError):
                        await pipe.get("foo").execute()

        assert initialize.await_count == (1 if due else 0)

    @pytest.mark.fixed_client
    @pytest.mark.usefixtures("no_cluster_retry_delay")
    @pytest.mark.parametrize("fails", [False, True], ids=["succeeds", "fails"])
    async def test_default_node_is_replaced_only_after_failing_on_it(
        self, fails
    ) -> None:
        """
        While a refresh another node's error asked for is pending, a command
        that goes to the default node and succeeds leaves the default node as
        it is; one whose attempt fails on it replaces it before the retry.
        """
        r = await get_mocked_redis_client(host=default_host, port=default_port)
        default_node = r.get_default_node()
        # Pending and not due: the client was just created
        r._request_refresh(last_failed_node_name="127.0.0.1:7001")
        attempts = []

        async def execute_command(node, *args, **kwargs):
            attempts.append(node)
            if fails and len(attempts) == 1:
                raise ConnectionError("down")
            return b"PONG"

        with (
            mock.patch.object(
                RedisCluster,
                "_determine_nodes",
                autospec=True,
                side_effect=lambda client, *args, **kwargs: [client.get_default_node()],
            ),
            mock.patch.object(
                ClusterNode,
                "execute_command",
                autospec=True,
                side_effect=execute_command,
            ),
        ):
            # Routed to the default node, as a keyless command is
            assert await r.get("foo") == b"PONG"

        assert attempts[0] is default_node
        if fails:
            assert r.get_default_node() is not default_node
            assert attempts[1] is r.get_default_node()
        else:
            assert attempts == [default_node]
            assert r.get_default_node() is default_node

    @pytest.mark.fixed_client
    async def test_pending_refresh_leaves_a_targeted_default_node_in_place(
        self,
    ) -> None:
        """
        A command the caller sent to the default node, made while a refresh
        another node's error asked for is pending, does not replace the default
        node: nothing failed on it.
        """
        r = await get_mocked_redis_client(host=default_host, port=default_port)
        default_node = r.get_default_node()
        # Pending and not due: the client was just created
        r._request_refresh(last_failed_node_name="127.0.0.1:7001")
        with mock.patch.object(
            ClusterNode, "execute_command", autospec=True, return_value=b"PONG"
        ) as execute_command:
            assert await r.execute_command("PING", target_nodes=default_node) is True

        assert [c.args[0] for c in execute_command.await_args_list] == [default_node]
        assert r.get_default_node() is default_node

    @pytest.mark.fixed_client
    @pytest.mark.parametrize("answered", [True, False], ids=["answered", "pending"])
    async def test_request_answered_by_a_later_refresh_is_dropped(
        self, answered
    ) -> None:
        """
        A refresh that completes after a request, such as the one SMIGRATED runs
        on the nodes manager, answers it: the next command does not refresh
        again for it, and the nodes the request named are dropped with it.
        """
        r = await get_mocked_redis_client(host=default_host, port=default_port)
        nodes_manager = r.nodes_manager
        r._request_refresh(
            additional_startup_nodes_info=[(default_host, 7009)],
            last_failed_node_name=get_node_name(default_host, 7000),
        )
        if answered:
            nodes_manager._epoch += 1
        nodes_manager._last_refresh_at = nodes_manager._refresh_due_at = None
        with mock.patch.object(
            RedisCluster, "_refresh_topology", autospec=True
        ) as refresh:
            await r.initialize()

        assert refresh.await_count == (0 if answered else 1)
        assert r._initialize is False
        # The refresh that ran is mocked away, so it leaves what it did not
        # answer in place
        assert (nodes_manager._requested_additional_startup_nodes == ()) is answered
        assert (nodes_manager._requested_last_failed_node_name is None) is answered

    @pytest.mark.fixed_client
    @pytest.mark.usefixtures("no_cluster_retry_delay")
    @pytest.mark.parametrize("unreachable", [True, False], ids=["unreachable", "up"])
    @pytest.mark.parametrize("call", ["command", "pipeline", "transaction", "watch"])
    async def test_out_of_retries_raises_unreachable_error_while_no_node_answers(
        self, call, unreachable
    ) -> None:
        """
        A command, pipeline, transaction or WATCH that runs out of retries on
        connection errors raises RedisClusterUnreachableError, with its own
        error as the cause, while the last refresh reached no node; and its own
        error otherwise.
        """
        # Transactions and WATCH retry with the backoff of their own copy of
        # the retry
        r = await get_mocked_redis_client(
            host=default_host, port=default_port, retry=Retry(NoBackoff(), 3)
        )
        if unreachable:
            r.nodes_manager._last_refresh_unreachable_args = (
                "Redis Cluster cannot be connected",
            )
        # Transactions and WATCH check out their own connection, a pipeline
        # sends through execute_pipeline
        method = {
            "command": "execute_command",
            "pipeline": "execute_pipeline",
            "transaction": "acquire_connection",
            "watch": "acquire_connection",
        }[call]
        with (
            mock.patch.object(
                ClusterNode, method, autospec=True, side_effect=ConnectionError("down")
            ) as failing,
            # The refreshes the errors ask for would query the failing nodes
            mock.patch.object(RedisCluster, "_refresh_topology", autospec=True),
            # The slot of WATCH comes from COMMAND GETKEYS otherwise, which
            # this client has no server for
            mock.patch.object(
                RedisCluster,
                "_determine_slot",
                autospec=True,
                return_value=r.keyslot("foo"),
            ),
        ):
            with pytest.raises(
                RedisClusterUnreachableError if unreachable else ConnectionError
            ) as raised:
                if call == "command":
                    await r.get("foo")
                elif call == "watch":
                    async with r.pipeline(transaction=True) as pipe:
                        await pipe.watch("foo")
                else:
                    async with r.pipeline(transaction=call == "transaction") as pipe:
                        await pipe.get("foo").execute()

        assert failing.call_count > 1
        if unreachable:
            assert str(raised.value) == "Redis Cluster cannot be connected"
            assert type(raised.value.__cause__) is ConnectionError
        else:
            assert type(raised.value) is ConnectionError

    @pytest.mark.fixed_client
    @pytest.mark.usefixtures("no_cluster_retry_delay", "no_topology_refresh_interval")
    @pytest.mark.parametrize("call", ["command", "pipeline", "transaction", "watch"])
    async def test_retry_gives_up_once_a_refresh_since_its_error_reached_no_node(
        self, call
    ) -> None:
        """
        The refresh a connection error asks for runs on the next attempt and
        reaches no node: the slot map that attempt would retry on is the one
        the error came from, so the command, pipeline, transaction or WATCH
        gives up at once with RedisClusterUnreachableError, with its own error
        as the cause, instead of using up its retries.
        """
        r = await get_mocked_redis_client(
            host=default_host,
            port=default_port,
            retry=Retry(NoBackoff(), 3),
            # A transaction asks for the refresh at this threshold
            reinitialize_steps=1,
        )
        nodes_manager = r.nodes_manager
        # Transactions and WATCH check out their own connection, a pipeline
        # sends through execute_pipeline, and the refresh queries every node
        # with CLUSTER SLOTS through execute_command, as a command does
        method = {
            "command": "execute_command",
            "pipeline": "execute_pipeline",
            "transaction": "acquire_connection",
            "watch": "acquire_connection",
        }[call]
        with contextlib.ExitStack() as patches:
            failing = patches.enter_context(
                mock.patch.object(
                    ClusterNode,
                    "execute_command",
                    autospec=True,
                    side_effect=ConnectionError("down"),
                )
            )
            if method != "execute_command":
                failing = patches.enter_context(
                    mock.patch.object(
                        ClusterNode,
                        method,
                        autospec=True,
                        side_effect=ConnectionError("down"),
                    )
                )
            # The slot of WATCH comes from COMMAND GETKEYS otherwise, which
            # this client has no server for
            patches.enter_context(
                mock.patch.object(
                    RedisCluster,
                    "_determine_slot",
                    autospec=True,
                    return_value=r.keyslot("foo"),
                )
            )
            with pytest.raises(RedisClusterUnreachableError) as raised:
                if call == "command":
                    await r.get("foo")
                elif call == "watch":
                    async with r.pipeline(transaction=True) as pipe:
                        await pipe.watch("foo")
                else:
                    async with r.pipeline(transaction=call == "transaction") as pipe:
                        await pipe.get("foo").execute()

        # One attempt failed, the refresh it asked for reached no node, and no
        # further attempt was made
        attempts = [
            c for c in failing.call_args_list if c.args[1:2] != ("CLUSTER SLOTS",)
        ]
        assert len(attempts) == 1
        assert nodes_manager._failed_refreshes == 1
        assert "Redis Cluster cannot be connected" in str(raised.value)
        assert type(raised.value.__cause__) is ConnectionError

    @pytest.mark.fixed_client
    @pytest.mark.parametrize("error", [ClusterDownError("down"), SlotNotCoveredError()])
    async def test_cluster_down_refreshes_at_once_and_raises_a_failed_refresh(
        self, error
    ) -> None:
        """
        A CLUSTERDOWN or an uncovered slot refreshes the topology at once, also
        when an error-triggered refresh is not due yet, and the command raises
        a refresh that fails.
        """
        r = await get_mocked_redis_client(host=default_host, port=default_port)
        # The client was just created: an error-triggered refresh is not due
        assert not r.nodes_manager._refresh_due(r.retry)
        with (
            mock.patch.object(
                ClusterNode, "execute_command", autospec=True, side_effect=error
            ),
            mock.patch.object(
                NodesManager,
                "initialize",
                autospec=True,
                side_effect=RedisClusterUnreachableError("down"),
            ) as init,
            mock.patch("redis.asyncio.cluster.asyncio.sleep"),
        ):
            with pytest.raises(RedisClusterUnreachableError):
                await r.get("foo")

        init.assert_awaited_once()

    @pytest.mark.fixed_client
    async def test_failed_refresh_is_not_repeated_by_queued_tasks(self) -> None:
        """
        Tasks that queue behind a refresh that fails do not each run their own:
        the failure moves the next refresh out, so they keep the current
        topology instead of querying every startup node again in turn.
        """
        r = await get_mocked_redis_client(host=default_host, port=default_port)
        nodes = dict(r.nodes_manager.nodes_cache)
        r._initialize = True
        r.nodes_manager._last_refresh_at = r.nodes_manager._refresh_due_at = None
        initialize = NodesManager.initialize

        async def unreachable(node, *args, **kwargs):
            # Suspend, so the other tasks queue behind the running refresh
            await asyncio.sleep(0)
            raise ConnectionError("down")

        with (
            mock.patch.object(
                ClusterNode, "execute_command", autospec=True, side_effect=unreachable
            ),
            mock.patch.object(
                NodesManager, "initialize", autospec=True, side_effect=initialize
            ) as init,
        ):
            results = await asyncio.gather(
                *(r.initialize() for _ in range(5)), return_exceptions=True
            )

        assert init.await_count == 1
        # The task that ran the refresh keeps the current topology too
        assert all(res is r for res in results)
        assert r.nodes_manager.nodes_cache == nodes
        assert r._initialize is True

    @pytest.mark.fixed_client
    async def test_retries_wait_as_long_as_the_retry_backoff_says(self) -> None:
        backoff = RecordingBackoff(step=0.001)
        r = await get_mocked_redis_client(
            host=default_host, port=default_port, retry=Retry(backoff, 3)
        )
        failures = [ConnectionError("down")] * 3
        execute_command, _ = TestTopologyRefreshKeepsConnections._command_side_effect(
            failures
        )
        with (
            mock.patch.object(
                ClusterNode,
                "execute_command",
                autospec=True,
                side_effect=execute_command,
            ),
            mock.patch("redis.asyncio.cluster.asyncio.sleep") as sleep,
        ):
            assert await r.get("foo") == b"value"

        assert [c.args[0] for c in sleep.await_args_list] == [0.001, 0.002, 0.003]
        assert backoff.resets == 1

    @pytest.mark.fixed_client
    @pytest.mark.parametrize("due", [True, False])
    async def test_moved_threshold_asks_for_refresh_run_once_due(self, due) -> None:
        """
        The MOVED threshold patches the slot and only asks for a refresh, also
        when one is due: the next command runs it, once due.
        """
        r = await get_mocked_redis_client(
            host=default_host, port=default_port, reinitialize_steps=1
        )
        if due:
            r.nodes_manager._last_refresh_at = r.nodes_manager._refresh_due_at = None
        slot = r.keyslot("foo")
        owner = r.get_node_from_key("foo")
        execute_command, _ = TestTopologyRefreshKeepsConnections._command_side_effect(
            [MovedError(f"{slot} {owner.host}:{owner.port}")]
        )
        with (
            mock.patch.object(
                ClusterNode,
                "execute_command",
                autospec=True,
                side_effect=execute_command,
            ),
            mock.patch.object(
                RedisCluster, "_refresh_topology", autospec=True
            ) as refresh,
            mock.patch.object(NodesManager, "move_slot", autospec=True) as move_slot,
        ):
            assert await r.get("foo") == b"value"

            refresh.assert_not_called()
            move_slot.assert_awaited_once()
            assert r._initialize
            assert r.nodes_manager._requested_additional_startup_nodes == (
                (owner.host, owner.port),
            )

            assert await r.get("foo") == b"value"

        if due:
            refresh.assert_awaited_once()
            assert not r._initialize
        else:
            refresh.assert_not_called()
            # Still asked for, so a later command runs it once due
            assert r._initialize

    @pytest.mark.fixed_client
    async def test_refresh_running_in_another_task_is_not_waited_for(self) -> None:
        """
        Once the client has a topology, a command does not wait for a refresh
        another task is running: it runs on the current topology.
        """
        r = await get_mocked_redis_client(host=default_host, port=default_port)
        r._initialize = True
        r.nodes_manager._last_refresh_at = r.nodes_manager._refresh_due_at = None
        refresh_started = asyncio.Event()
        finish_refresh = asyncio.Event()

        async def refresh_topology(self, **kwargs):
            refresh_started.set()
            await finish_refresh.wait()

        async def execute_command(node, *args, **kwargs):
            return b"value"

        with (
            mock.patch.object(
                ClusterNode,
                "execute_command",
                autospec=True,
                side_effect=execute_command,
            ),
            mock.patch.object(
                RedisCluster,
                "_refresh_topology",
                autospec=True,
                side_effect=refresh_topology,
            ) as refresh,
        ):
            refreshing = asyncio.create_task(r.initialize())
            await refresh_started.wait()

            assert await asyncio.wait_for(r.get("foo"), timeout=1) == b"value"
            assert not refreshing.done()

            finish_refresh.set()
            await refreshing

        refresh.assert_awaited_once()
        assert r._initialize is False

    @pytest.mark.fixed_client
    async def test_first_initialize_is_waited_for(self) -> None:
        """
        The default node is published before the first initialize finishes
        (the commands parser is set up after it), so a caller that arrives
        while it runs waits for it instead of serving a half-built client.
        """
        r = await get_mocked_redis_client(host=default_host, port=default_port)
        node = r.get_default_node()
        # Closed, the client initializes again from scratch
        await r.aclose()
        refresh_started = asyncio.Event()
        finish_refresh = asyncio.Event()

        async def refresh_topology(self, **kwargs):
            self.nodes_manager.default_node = node
            refresh_started.set()
            await finish_refresh.wait()

        with mock.patch.object(
            RedisCluster,
            "_refresh_topology",
            autospec=True,
            side_effect=refresh_topology,
        ) as refresh:
            first = asyncio.create_task(r.initialize())
            await refresh_started.wait()
            second = asyncio.create_task(r.initialize())
            await asyncio.sleep(0)
            assert not second.done()

            finish_refresh.set()
            assert await first is r
            assert await second is r

        refresh.assert_awaited_once()
        assert r._initialize is False

    @pytest.mark.fixed_client
    async def test_pipeline_retries_wait_as_long_as_the_retry_backoff_says(
        self,
    ) -> None:
        backoff = RecordingBackoff(step=0.001)
        r = await get_mocked_redis_client(
            host=default_host, port=default_port, retry=Retry(backoff, 2)
        )
        attempts = []

        async def execute_pipeline(node, commands) -> bool:
            attempts.append(node.name)
            failed = len(attempts) <= 2
            for command in commands:
                command.result = ConnectionError("down") if failed else b"value"
            return failed

        with (
            mock.patch.object(
                ClusterNode,
                "execute_pipeline",
                autospec=True,
                side_effect=execute_pipeline,
            ),
            mock.patch("redis.asyncio.cluster.asyncio.sleep") as sleep,
        ):
            async with r.pipeline() as pipe:
                assert await pipe.get("foo").execute() == [b"value"]

        assert len(attempts) == 3
        assert [c.args[0] for c in sleep.await_args_list] == [0.001, 0.002]

    @pytest.mark.fixed_client
    @pytest.mark.parametrize("error", [ClusterDownError("down"), SlotNotCoveredError()])
    async def test_pipeline_waits_for_the_cluster_before_retrying_a_cluster_down_error(
        self, error
    ) -> None:
        """
        A pipeline that failed with CLUSTERDOWN or an uncovered slot waits the
        fixed 0.25 seconds a command does before its retry, and the backoff
        delay after it.
        """
        r = await get_mocked_redis_client(
            host=default_host,
            port=default_port,
            retry=Retry(RecordingBackoff(step=0.001), 2),
        )
        attempts = []

        async def execute_pipeline(node, commands) -> bool:
            attempts.append(node.name)
            if len(attempts) == 1:
                raise error
            for command in commands:
                command.result = b"value"
            return False

        with (
            mock.patch.object(
                ClusterNode,
                "execute_pipeline",
                autospec=True,
                side_effect=execute_pipeline,
            ),
            mock.patch("redis.asyncio.cluster.asyncio.sleep") as sleep,
        ):
            async with r.pipeline() as pipe:
                assert await pipe.get("foo").execute() == [b"value"]

        assert len(attempts) == 2
        assert [c.args[0] for c in sleep.await_args_list] == [0.25, 0.001]

    @pytest.mark.fixed_client
    async def test_transaction_queuing_leaves_a_pending_refresh_to_the_loop(
        self,
    ) -> None:
        """
        Queuing a transaction command runs on a private loop in a worker
        thread. A refresh an error asked for must not run there - it would do
        I/O on connections bound to the caller's loop - so it is left to the
        next command on that loop.
        """
        r = await get_mocked_redis_client(host=default_host, port=default_port)
        r._initialize = True
        r.nodes_manager._last_refresh_at = r.nodes_manager._refresh_due_at = None
        with mock.patch.object(NodesManager, "initialize", autospec=True) as init:
            pipe = r.pipeline(transaction=True)
            # GET: the only command the mocked commands parser knows
            pipe.get("{foo}:a")
            pipe.get("{foo}:b")
        init.assert_not_called()
        assert r._initialize is True
        assert len(pipe._execution_strategy._command_queue) == 2

    @pytest.mark.fixed_client
    async def test_aclose_closes_a_client_waiting_for_a_refresh(self) -> None:
        """
        An error path sets _initialize to ask for a refresh; a client closed
        before that refresh ran still closes its connections.
        """
        r = await get_mocked_redis_client(host=default_host, port=default_port)
        r._initialize = True
        with mock.patch.object(NodesManager, "aclose", autospec=True) as aclose:
            await r.aclose()

        assert [c.args[1:] for c in aclose.await_args_list] == [(), ("startup_nodes",)]

    @pytest.mark.fixed_client
    async def test_unclosed_client_waiting_for_a_refresh_warns(self) -> None:
        """
        A client an error path left waiting for a refresh is still open, so
        dropping it without closing it warns; a closed one does not.
        """
        r = await get_mocked_redis_client(host=default_host, port=default_port)
        r._initialize = True
        warn = mock.Mock()
        RedisCluster.__del__(r, _warn=warn, _grl=mock.Mock())
        warn.assert_called_once()
        assert warn.call_args.args[1] is ResourceWarning

        await r.aclose()
        warn.reset_mock()
        RedisCluster.__del__(r, _warn=warn, _grl=mock.Mock())
        warn.assert_not_called()

    @pytest.mark.fixed_client
    @pytest.mark.parametrize("due", [True, False])
    @pytest.mark.parametrize("redirect", [True, False])
    async def test_transaction_threshold_asks_for_a_refresh(
        self, due, redirect
    ) -> None:
        """
        At the reinitialize_steps threshold an error in a transaction only asks
        for a refresh, also when one is due: the next attempt runs it through
        initialize(), which keeps the current topology if the refresh fails. A
        MOVED patches the slot; a connection error asks for the failed node to
        be tried last.
        """
        r = await get_mocked_redis_client(
            host=default_host, port=default_port, reinitialize_steps=1
        )
        if due:
            r.nodes_manager._last_refresh_at = r.nodes_manager._refresh_due_at = None
        strategy = r.pipeline(transaction=True)._execution_strategy
        owner = r.get_node_from_key("foo")
        error = (
            MovedError(f"{r.keyslot('foo')} {owner.host}:{owner.port}")
            if redirect
            else ConnectionError("down")
        )
        with (
            mock.patch.object(NodesManager, "initialize", autospec=True) as init,
            mock.patch.object(NodesManager, "move_slot", autospec=True) as move_slot,
        ):
            strategy._transaction_node = owner
            await strategy._reinitialize_on_error(error, 1)

        assert move_slot.called is redirect
        init.assert_not_called()
        assert r._initialize is True
        assert r.reinitialize_counter == 0
        assert r.nodes_manager._requested_last_failed_node_name == (
            None if redirect else owner.name
        )

    @pytest.mark.fixed_client
    @pytest.mark.parametrize("watch", [False, True])
    async def test_transaction_attempt_runs_a_requested_refresh(self, watch) -> None:
        """
        A refresh an earlier error asked for runs when a transaction attempt
        starts, once due, so the retries do not keep hitting the stale owner.
        """
        r = await get_mocked_redis_client(host=default_host, port=default_port)
        r._initialize = True
        r.nodes_manager._last_refresh_at = r.nodes_manager._refresh_due_at = None
        strategy = r.pipeline(transaction=True)._execution_strategy
        with (
            mock.patch.object(
                RedisCluster, "_refresh_topology", autospec=True
            ) as refresh,
            # Stop the attempt right after its refresh check
            mock.patch.object(
                type(strategy),
                "_get_client_and_connection_for_transaction",
                side_effect=RuntimeError("stop"),
            ),
        ):
            with pytest.raises(RuntimeError, match="stop"):
                if watch:
                    await strategy._get_connection_and_send_command("WATCH", "foo")
                else:
                    await strategy._execute_transaction([], raise_on_error=True)

        refresh.assert_awaited_once()
        assert r._initialize is False

    @pytest.mark.fixed_client
    @pytest.mark.parametrize("due", [True, False])
    async def test_pubsub_topology_repair_is_paced(self, due) -> None:
        r = await get_mocked_redis_client(host=default_host, port=default_port)
        if due:
            r.nodes_manager._last_refresh_at = r.nodes_manager._refresh_due_at = None
        pubsub = r.pubsub()
        pubsub.shard_channels = {b"channel": None}
        with mock.patch.object(NodesManager, "initialize", autospec=True) as init:
            pubsub._schedule_topology_repair()
            await asyncio.gather(*pubsub._topology_repair_tasks)

        if due:
            init.assert_awaited_once()
            assert r._initialize is False
        else:
            init.assert_not_called()
            # Asked for, so the next command runs it once due
            assert r._initialize is True

    @pytest.mark.fixed_client
    async def test_failed_pubsub_topology_repair_keeps_the_request(self) -> None:
        """
        A due repair whose refresh fails leaves the request on the client, so
        the next command (or repair) runs it once the backoff allows, as on
        sync.
        """
        r = await get_mocked_redis_client(host=default_host, port=default_port)
        r.nodes_manager._last_refresh_at = r.nodes_manager._refresh_due_at = None
        pubsub = r.pubsub()
        pubsub.shard_channels = {b"channel": None}
        with mock.patch.object(
            NodesManager,
            "initialize",
            autospec=True,
            side_effect=RedisClusterUnreachableError("down"),
        ) as init:
            pubsub._schedule_topology_repair()
            await asyncio.gather(*pubsub._topology_repair_tasks, return_exceptions=True)

        init.assert_awaited_once()
        assert r._initialize is True
        # The live topology is kept
        assert r.nodes_manager.default_node is not None


@pytest.mark.usefixtures("no_topology_refresh_interval")
class TestTopologyRefreshKeepsConnections:
    """
    A topology refresh triggered by an error must not close the connections of
    the nodes that stay in the topology: other tasks are using them.
    """

    @staticmethod
    def _disconnected(disconnect_mock: mock.AsyncMock, nodes) -> List[ClusterNode]:
        """The nodes of ``nodes`` (by identity) that were disconnected."""
        disconnected = [c.args[0] for c in disconnect_mock.await_args_list]
        return [node for node in nodes if any(d is node for d in disconnected)]

    @staticmethod
    def _command_side_effect(failures: List[Exception]):
        """
        CLUSTER SLOTS returns the default topology; every other command fails
        with the next error of ``failures`` and then returns b"value".
        """
        refreshes = []

        async def execute_command(node, *args, **kwargs):
            if args[0] == "CLUSTER SLOTS":
                refreshes.append(node.name)
                return default_cluster_slots
            if failures:
                raise failures.pop(0)
            return b"value"

        return execute_command, refreshes

    @pytest.mark.fixed_client
    async def test_moved_refresh_keeps_connections(self) -> None:
        r = await get_mocked_redis_client(
            host=default_host, port=default_port, reinitialize_steps=1
        )
        slot = r.keyslot("foo")
        owner = r.get_node_from_key("foo")
        execute_command, refreshes = self._command_side_effect(
            [MovedError(f"{slot} {owner.host}:{owner.port}")]
        )
        nodes = list(r.nodes_manager.nodes_cache.values())
        with (
            mock.patch.object(
                ClusterNode,
                "execute_command",
                autospec=True,
                side_effect=execute_command,
            ),
            mock.patch.object(ClusterNode, "disconnect", autospec=True) as disconnect,
            mock.patch.object(
                AsyncCommandsParser, "initialize", autospec=True
            ) as parser_initialize,
        ):
            assert await r.get("foo") == b"value"
            # The MOVED only asks for the refresh; the next command runs it
            assert not refreshes
            assert await r.get("foo") == b"value"

        assert refreshes
        assert self._disconnected(disconnect, nodes) == []
        assert r._initialize is False
        # The command table is fetched once per client, not on every refresh
        parser_initialize.assert_not_called()
        assert r.commands_parser.node is r.nodes_manager.default_node

    @pytest.mark.fixed_client
    @pytest.mark.parametrize("error", [ClusterDownError("down"), SlotNotCoveredError()])
    async def test_cluster_down_refresh_keeps_connections(self, error) -> None:
        r = await get_mocked_redis_client(host=default_host, port=default_port)
        execute_command, refreshes = self._command_side_effect([error])
        nodes = list(r.nodes_manager.nodes_cache.values())
        with (
            mock.patch.object(
                ClusterNode,
                "execute_command",
                autospec=True,
                side_effect=execute_command,
            ),
            mock.patch.object(ClusterNode, "disconnect", autospec=True) as disconnect,
            mock.patch("redis.asyncio.cluster.asyncio.sleep"),
        ):
            assert await r.get("foo") == b"value"

        assert refreshes
        assert self._disconnected(disconnect, nodes) == []
        assert r._initialize is False

    @pytest.mark.fixed_client
    async def test_aclose_waits_for_a_cluster_down_refresh(self) -> None:
        """
        aclose() waits for the refresh a CLUSTERDOWN runs, so a refresh that
        finishes after aclose() was called does not reopen the closed client.
        """
        r = await get_mocked_redis_client(
            host=default_host, port=default_port, retry=Retry(NoBackoff(), 0)
        )
        execute_command, _ = self._command_side_effect([ClusterDownError("down")])
        started = asyncio.Event()
        release = asyncio.Event()
        initialize = NodesManager.initialize

        async def paused(nodes_manager, *args, **kwargs):
            started.set()
            await release.wait()
            await initialize(nodes_manager, *args, **kwargs)

        with (
            mock.patch.object(
                ClusterNode,
                "execute_command",
                autospec=True,
                side_effect=execute_command,
            ),
            mock.patch.object(
                NodesManager, "initialize", autospec=True, side_effect=paused
            ),
        ):
            command = asyncio.create_task(r.get("foo"))
            await started.wait()
            close = asyncio.create_task(r.aclose())
            await asyncio.sleep(0)
            assert not close.done()
            release.set()
            with pytest.raises(ClusterDownError):
                await command
            await close

        assert r.nodes_manager.default_node is None
        assert r._initialize is True

    @pytest.mark.fixed_client
    async def test_concurrent_cluster_down_errors_run_one_refresh(self) -> None:
        """
        Tasks that hit CLUSTERDOWN while another task's refresh runs wait for
        it and do not each refresh again.
        """
        r = await get_mocked_redis_client(host=default_host, port=default_port)
        failures = [ClusterDownError("down"), ClusterDownError("down")]
        refreshes = []

        async def execute_command(node, *args, **kwargs):
            if args[0] == "CLUSTER SLOTS":
                refreshes.append(node.name)
                # Slow enough for the other task to hit its error meanwhile
                await asyncio.sleep(0.05)
                return default_cluster_slots
            if failures:
                raise failures.pop(0)
            return b"value"

        with mock.patch.object(
            ClusterNode, "execute_command", autospec=True, side_effect=execute_command
        ):
            assert await asyncio.gather(r.get("foo"), r.get("bar")) == [
                b"value",
                b"value",
            ]

        assert len(refreshes) == 1

    @pytest.mark.fixed_client
    @pytest.mark.parametrize(
        "error",
        [
            MaxClientsReachedError("max number of clients reached"),
            AuthenticationError("invalid password"),
        ],
        ids=["max_clients_reached", "authentication"],
    )
    async def test_capacity_or_auth_error_is_not_a_node_failure(self, error) -> None:
        """
        A maxclients refusal or an authentication error is not a node failure:
        the node's connections are not recycled, the topology is not refreshed
        and the command is not retried.
        """
        r = await get_mocked_redis_client(host=default_host, port=default_port)
        execute_command, refreshes = self._command_side_effect([error])
        attempts = []

        async def counting_execute_command(node, *args, **kwargs):
            if args[0] != "CLUSTER SLOTS":
                attempts.append(args[0])
            return await execute_command(node, *args, **kwargs)

        with (
            mock.patch.object(
                ClusterNode,
                "execute_command",
                autospec=True,
                side_effect=counting_execute_command,
            ),
            mock.patch.object(
                ClusterNode, "update_active_connections_for_reconnect", autospec=True
            ) as update_active,
            mock.patch.object(
                ClusterNode, "disconnect_free_connections", autospec=True
            ) as disconnect_free,
        ):
            with pytest.raises(type(error)):
                await r.get("foo")

        assert attempts == ["GET"]
        assert refreshes == []
        update_active.assert_not_called()
        disconnect_free.assert_not_called()
        assert r._initialize is False

    @pytest.mark.fixed_client
    @pytest.mark.parametrize("transaction", [False, True], ids=["pipeline", "tx"])
    @pytest.mark.parametrize(
        "error",
        [
            MaxConnectionsError("pool exhausted"),
            MaxClientsReachedError("max number of clients reached"),
            AuthenticationError("invalid password"),
        ],
        ids=["max_connections", "max_clients_reached", "authentication"],
    )
    async def test_pipeline_does_not_refresh_or_retry_on_non_node_failure(
        self, error, transaction
    ) -> None:
        r = await get_mocked_redis_client(host=default_host, port=default_port)
        execute_command, refreshes = self._command_side_effect([])
        # A pipeline sends through execute_pipeline, a transaction checks out its
        # own connection
        method = "acquire_connection" if transaction else "execute_pipeline"
        with (
            mock.patch.object(
                ClusterNode, method, autospec=True, side_effect=error
            ) as failing,
            mock.patch.object(
                ClusterNode,
                "execute_command",
                autospec=True,
                side_effect=execute_command,
            ),
            mock.patch("redis.asyncio.cluster.asyncio.sleep"),
        ):
            with pytest.raises(type(error)):
                async with r.pipeline(transaction=transaction) as pipe:
                    await pipe.get("foo").execute()

        assert failing.call_count == 1
        assert refreshes == []
        assert r._initialize is False

    @pytest.mark.fixed_client
    async def test_pipeline_retry_refresh_keeps_connections(self) -> None:
        r = await get_mocked_redis_client(host=default_host, port=default_port)
        attempts = []

        async def execute_pipeline(node, commands) -> bool:
            attempts.append(node.name)
            failed = len(attempts) == 1
            for command in commands:
                command.result = ClusterDownError("down") if failed else b"value"
            return failed

        nodes = list(r.nodes_manager.nodes_cache.values())
        execute_command, refreshes = self._command_side_effect([])
        with (
            mock.patch.object(
                ClusterNode,
                "execute_pipeline",
                autospec=True,
                side_effect=execute_pipeline,
            ),
            mock.patch.object(
                ClusterNode,
                "execute_command",
                autospec=True,
                side_effect=execute_command,
            ),
            mock.patch.object(ClusterNode, "disconnect", autospec=True) as disconnect,
            mock.patch("redis.asyncio.cluster.asyncio.sleep"),
        ):
            async with r.pipeline() as pipe:
                assert await pipe.get("foo").execute() == [b"value"]

        assert len(attempts) == 2
        assert refreshes
        assert self._disconnected(disconnect, nodes) == []

    @pytest.mark.fixed_client
    @pytest.mark.usefixtures("no_cluster_retry_delay")
    @pytest.mark.parametrize("on_read", [False, True], ids=["write", "read"])
    async def test_pipeline_connection_error_tries_the_failed_node_last(
        self, on_read
    ) -> None:
        """
        A connection error in a pipeline, on the write or on a read, moves the
        failed node to the end of the cached nodes, and the refresh the retry
        runs queries it last, even when the shuffle puts it first.
        """
        r = await get_mocked_redis_client(host=default_host, port=default_port)
        nm = r.nodes_manager
        failed = r.get_node_from_key("foo")
        assert list(nm.nodes_cache)[-1] != failed.name
        attempts = []
        queried = []

        async def execute_pipeline(node, commands) -> bool:
            attempts.append(node.name)
            if len(attempts) > 1:
                for command in commands:
                    command.result = b"value"
                return False
            if not on_read:
                raise ConnectionError("down")
            for command in commands:
                command.result = ConnectionError("down")
            return True

        async def execute_command(node, *args, **kwargs):
            if args[0] == "CLUSTER SLOTS":
                queried.append(node.name)
                return default_cluster_slots
            return b"value"

        def shuffle(nodes) -> None:
            nodes.sort(key=lambda node: node.name != failed.name)

        nm._last_refresh_at = nm._refresh_due_at = None
        with (
            mock.patch("redis.asyncio.cluster.random.shuffle", side_effect=shuffle),
            mock.patch.object(
                ClusterNode,
                "execute_pipeline",
                autospec=True,
                side_effect=execute_pipeline,
            ),
            mock.patch.object(
                ClusterNode,
                "execute_command",
                autospec=True,
                side_effect=execute_command,
            ),
        ):
            async with r.pipeline() as pipe:
                assert await pipe.get("foo").execute() == [b"value"]

        assert len(attempts) == 2
        assert queried and failed.name not in queried
        assert list(nm.nodes_cache)[-1] == failed.name
        assert nm._requested_last_failed_node_name is None

    @pytest.mark.fixed_client
    @pytest.mark.usefixtures("no_cluster_retry_delay")
    async def test_pipeline_retry_waits_for_sibling_node_tasks(self) -> None:
        """
        When one node's pipeline write fails while another node's pipeline is
        still in flight, the retry starts only after the sibling settles, so
        the sibling's commands are not sent twice.
        """
        r = await get_mocked_redis_client(host=default_host, port=default_port)
        failed = r.get_node_from_key("foo")
        sibling = r.get_node_from_key("bar")
        assert failed.name != sibling.name
        attempts = []

        async def execute_pipeline(node, commands) -> bool:
            attempts.append(node.name)
            if node.name == failed.name and attempts.count(node.name) == 1:
                raise ConnectionError("down")
            if node.name == sibling.name:
                # Still reading after the failed node's write has raised
                for _ in range(5):
                    await asyncio.sleep(0)
            for command in commands:
                command.result = command.args[1].encode()
            return False

        execute_command, _ = self._command_side_effect([])
        with (
            mock.patch.object(
                ClusterNode,
                "execute_pipeline",
                autospec=True,
                side_effect=execute_pipeline,
            ),
            mock.patch.object(
                ClusterNode,
                "execute_command",
                autospec=True,
                side_effect=execute_command,
            ),
        ):
            async with r.pipeline() as pipe:
                assert await pipe.get("foo").get("bar").execute() == [b"foo", b"bar"]

        assert attempts.count(failed.name) == 2
        assert attempts.count(sibling.name) == 1

    class _ServerClosedConnection:
        """
        A free connection the server closed (a restart, an idle ``timeout``):
        its next read fails, after which it has disconnected itself, as a real
        connection does. Once disconnected, its next use reconnects and works.
        """

        # No socket to leak: keeps ClusterNode.__del__ quiet
        is_connected = False
        _resp2_fallback = False

        def __init__(self) -> None:
            self.closed_by_server = True
            self.reconnect = False
            self.failed_reads = 0

        def pack_command(self, *args):
            return b"packed"

        def pack_commands(self, commands):
            return b"packed"

        async def send_packed_command(self, *args, **kwargs) -> None:
            pass

        async def read_response(self, *args, **kwargs):
            if self.closed_by_server:
                self.failed_reads += 1
                self.closed_by_server = False
                raise ConnectionError("Connection closed by server.")
            return b"value"

        async def disconnect(self, *args, **kwargs) -> None:
            self.closed_by_server = False
            self.reconnect = False

        def mark_for_reconnect(self) -> None:
            self.reconnect = True

        def should_reconnect(self) -> bool:
            return self.reconnect

    @pytest.mark.fixed_client
    @pytest.mark.usefixtures("no_cluster_retry_delay")
    @pytest.mark.parametrize("pipeline", [False, True], ids=["command", "pipeline"])
    async def test_connection_error_reconnects_free_connections_the_server_closed(
        self, pipeline
    ) -> None:
        """
        After a node restart every free connection is closed by the server, and
        an async node does not check a free connection before handing it out.
        The first connection error marks them for reconnect, so the retry gets
        a working connection instead of the next dead one, even when the node
        holds more dead connections than there are retries.
        """
        r = await get_mocked_redis_client(host=default_host, port=default_port)
        node = r.get_node_from_key("foo")
        connections = [
            self._ServerClosedConnection() for _ in range(r.retry.get_retries() + 3)
        ]
        node._connections = list(connections)
        node._free.extend(connections)

        with mock.patch.object(NodesManager, "initialize", new=mock.AsyncMock()):
            if pipeline:
                async with r.pipeline() as pipe:
                    assert await pipe.get("foo").execute() == [b"value"]
            else:
                assert await r.get("foo") == b"value"

        # One command failed on a dead connection; the retry reconnected the
        # next one instead of failing on it
        assert sum(c.failed_reads for c in connections) == 1
        assert connections[1].closed_by_server is False
        assert connections[1].reconnect is False
        # The ones not used yet stay marked: they reconnect when acquired
        assert all(c.reconnect for c in connections[2:])

    @pytest.mark.fixed_client
    async def test_watch_reconnects_free_connections_the_server_closed(self) -> None:
        """
        A transaction's connection error marks the free connections for
        reconnect too, and WATCH reconnects a marked connection it has just
        acquired: only a connection already holding WATCH state is kept as is.
        """

        class Connection(self._ServerClosedConnection):
            host, port, db = default_host, default_port, 0

            async def send_command(self, *args, **kwargs) -> None:
                pass

        r = await get_mocked_redis_client(host=default_host, port=default_port)
        node = r.get_node_from_key("foo")
        connections = [Connection() for _ in range(r.retry.get_retries() + 3)]
        node._connections = list(connections)
        node._free.extend(connections)

        with (
            mock.patch.object(NodesManager, "initialize", new=mock.AsyncMock()),
            mock.patch("redis.asyncio.retry.sleep", new=mock.AsyncMock()),
            mock.patch.object(
                r, "_determine_slot", new=mock.AsyncMock(return_value=r.keyslot("foo"))
            ),
        ):
            async with r.pipeline(transaction=True) as pipe:
                await pipe.watch("foo")

        # WATCH failed on one dead connection; the retry reconnected the next
        assert sum(c.failed_reads for c in connections) == 1
        assert sum(c.reconnect for c in connections) == len(connections) - 2

    @pytest.mark.parametrize(
        "error, marks",
        [
            (ConnectionError("Connection closed by server."), True),
            (RedisTimeoutError("Timeout reading from socket"), False),
            (MaxClientsReachedError("max number of clients reached"), False),
            (AuthenticationError("invalid password"), False),
        ],
        ids=["connection_error", "timeout", "max_clients_reached", "authentication"],
    )
    def test_only_a_connection_error_marks_free_connections(self, error, marks) -> None:
        """
        A connection error marks the free connections for a lazy reconnect and
        leaves the in-use ones alone. A timeout does not mean the others are
        broken, and capacity or auth errors are not node failures, so those
        mark nothing.
        """
        node = ClusterNode(default_host, 7000)
        # Not connected, so the node is not reported unclosed when collected
        in_use, free = mock.Mock(is_connected=False), mock.Mock(is_connected=False)
        node._connections = [in_use, free]
        node._free.append(free)

        assert node._mark_free_connections_for_reconnect_on_error(error) is marks

        assert free.mark_for_reconnect.call_count == int(marks)
        in_use.mark_for_reconnect.assert_not_called()
        free.disconnect.assert_not_called()

    async def test_himport_prepare_connection_error_marks_free_connections(
        self,
    ) -> None:
        """
        The HIMPORT PREPARE pre-flight of a pipeline is the first I/O on the
        connection, so a connection error there marks the free connections for
        reconnect like one on the batched write.
        """
        node = ClusterNode(default_host, 7000)
        # Not connected, so the node is not reported unclosed when collected
        in_use, free = mock.Mock(is_connected=False), mock.Mock(is_connected=False)
        node._connections = [in_use, free]
        node._free.append(free)

        with (
            mock.patch.object(ClusterNode, "acquire_connection", return_value=in_use),
            mock.patch.object(
                ClusterNode, "disconnect_if_needed", new=mock.AsyncMock()
            ),
            mock.patch.object(
                ClusterNode,
                "_himport_prepare_pipeline",
                new=mock.AsyncMock(side_effect=ConnectionError("closed")),
            ),
        ):
            with pytest.raises(ConnectionError, match="closed"):
                await node.execute_pipeline([PipelineCommand(0, "GET", "key")])

        free.mark_for_reconnect.assert_called_once()
        in_use.mark_for_reconnect.assert_not_called()

    @pytest.mark.fixed_client
    @pytest.mark.parametrize(
        "error", [ConnectionError("down"), asyncio.CancelledError()]
    )
    async def test_failed_refresh_keeps_connections(self, error) -> None:
        """
        A refresh that fails or is cancelled (e.g. by a timeout around the
        command that runs it) keeps the current topology and its connections,
        and the next command retries the refresh.
        """
        r = await get_mocked_redis_client(host=default_host, port=default_port)
        nodes = dict(r.nodes_manager.nodes_cache)
        r._initialize = True
        with (
            mock.patch.object(
                NodesManager, "initialize", autospec=True, side_effect=error
            ),
            mock.patch.object(ClusterNode, "disconnect", autospec=True) as disconnect,
        ):
            with pytest.raises(type(error)):
                await r.initialize()

        assert self._disconnected(disconnect, nodes.values()) == []
        assert r.nodes_manager.nodes_cache == nodes
        assert r._initialize is True

    @pytest.mark.fixed_client
    async def test_command_runs_on_the_current_topology_when_no_node_answers_a_refresh(
        self,
    ) -> None:
        """
        A due refresh that reaches no node (e.g. only seeds are queried, with
        dynamic_startup_nodes=False) is logged and asked for again, and the
        command that ran it still goes to its node of the current topology.
        """
        r = await get_mocked_redis_client(host=default_host, port=default_port)
        owner = r.get_node_from_key("foo")
        r._initialize = True
        r.nodes_manager._last_refresh_at = r.nodes_manager._refresh_due_at = None
        with (
            mock.patch.object(
                NodesManager,
                "initialize",
                autospec=True,
                side_effect=RedisClusterUnreachableError("down"),
            ) as init,
            mock.patch.object(
                ClusterNode, "execute_command", autospec=True, return_value=b"value"
            ) as execute_command,
        ):
            assert await r.get("foo") == b"value"

        init.assert_awaited_once()
        assert [c.args[0] for c in execute_command.await_args_list] == [owner]
        assert r._initialize is True

    @pytest.mark.fixed_client
    async def test_failed_first_initialize_closes_the_client(self) -> None:
        with mock.patch.object(NodesManager, "aclose", autospec=True) as aclose:
            with pytest.raises(RedisClusterException):
                await get_mocked_redis_client(
                    host=default_host,
                    port=default_port,
                    cluster_slots_raise_error=True,
                )

        assert [c.args[1:] for c in aclose.await_args_list] == [(), ("startup_nodes",)]

    @pytest.mark.fixed_client
    async def test_refresh_queries_a_startup_node_once_when_moved_named_it(
        self,
    ) -> None:
        """
        An address a MOVED error named that is already a startup node is
        queried through that node: no temporary ClusterNode is built for it, so
        no second connection to it is opened and closed.
        """
        startup_node = ClusterNode(default_host, 7000)
        nodes_manager = NodesManager(
            startup_nodes=[startup_node],
            require_full_coverage=True,
            connection_kwargs={},
        )
        slots = [[0, 16383, [default_host, 7000, "node_0"]]]
        with (
            mock.patch.object(
                ClusterNode, "execute_command", autospec=True, return_value=slots
            ) as execute_command,
            mock.patch.object(ClusterNode, "disconnect", autospec=True) as disconnect,
        ):
            await nodes_manager.initialize(
                additional_startup_nodes_info=[(default_host, 7000)]
            )

        assert [c.args[0] for c in execute_command.await_args_list] == [startup_node]
        assert disconnect.await_args_list == []

    @pytest.mark.fixed_client
    async def test_refresh_queries_a_localhost_remapped_startup_node_once(
        self,
    ) -> None:
        """
        With a remap to localhost, the refresh hint of a MOVED redirect is
        localhost:port, while the startup node is named after the resolved
        address. It is the same node, so no temporary ClusterNode is built to
        query it a second time.
        """

        def address_remap(address):
            host, port = address
            return "localhost", port + 1000

        startup_node = ClusterNode("localhost", 8000)
        nodes_manager = NodesManager(
            startup_nodes=[startup_node],
            require_full_coverage=False,
            connection_kwargs={},
            address_remap=address_remap,
        )
        # Half of the slots, so every startup node is queried
        slots = [[0, 8191, [default_host, 7000, "node_0"]]]
        with (
            mock.patch.object(
                ClusterNode, "execute_command", autospec=True, return_value=slots
            ) as execute_command,
            mock.patch.object(ClusterNode, "disconnect", autospec=True) as disconnect,
        ):
            # The hint _execute_command passes on for a MOVED to 7000
            await nodes_manager.initialize(
                additional_startup_nodes_info=[
                    nodes_manager.remap_host_port(default_host, 7000)
                ]
            )

        assert [c.args[0] for c in execute_command.await_args_list] == [startup_node]
        assert disconnect.await_args_list == []

    @pytest.mark.fixed_client
    async def test_refresh_closes_temporary_startup_nodes(self) -> None:
        """
        An additional startup node is queried through a ClusterNode built only
        for that refresh; its connections are closed afterwards.
        """

        async def execute_command(node, *args, **kwargs):
            if node.port == 7000:
                # The startup node covers only half of the slots, so the
                # additional startup node is queried too
                return [[0, 8191, [default_host, 7000, "node_0"]]]
            return [
                [0, 8191, [default_host, 7000, "node_0"]],
                [8192, 16383, [default_host, 7001, "node_1"]],
            ]

        nodes_manager = NodesManager(
            startup_nodes=[ClusterNode(default_host, 7000)],
            require_full_coverage=True,
            connection_kwargs={},
        )
        with (
            mock.patch.object(
                ClusterNode,
                "execute_command",
                autospec=True,
                side_effect=execute_command,
            ),
            mock.patch.object(ClusterNode, "disconnect", autospec=True) as disconnect,
        ):
            await nodes_manager.initialize(
                additional_startup_nodes_info=[(default_host, 7009)]
            )

        disconnected = [c.args[0] for c in disconnect.await_args_list]
        assert [node.name for node in disconnected] == [
            get_node_name(default_host, 7009)
        ]
        assert self._disconnected(disconnect, nodes_manager.nodes_cache.values()) == []

    @pytest.mark.fixed_client
    async def test_failed_refresh_closes_temporary_startup_nodes(self) -> None:
        """
        A refresh that fails after querying an additional startup node still
        closes the connections of the ClusterNode built for it.
        """

        async def execute_command(node, *args, **kwargs):
            # Neither node covers every slot, so both are queried and the
            # refresh fails on the coverage check
            return [[0, 8191, [default_host, 7000, "node_0"]]]

        nodes_manager = NodesManager(
            startup_nodes=[ClusterNode(default_host, 7000)],
            require_full_coverage=True,
            connection_kwargs={},
        )
        with (
            mock.patch.object(
                ClusterNode,
                "execute_command",
                autospec=True,
                side_effect=execute_command,
            ),
            mock.patch.object(ClusterNode, "disconnect", autospec=True) as disconnect,
        ):
            with pytest.raises(RedisClusterException):
                await nodes_manager.initialize(
                    additional_startup_nodes_info=[(default_host, 7009)]
                )

        disconnected = [c.args[0] for c in disconnect.await_args_list]
        assert [node.name for node in disconnected] == [
            get_node_name(default_host, 7009)
        ]


@pytest.mark.fixed_client
@pytest.mark.parametrize(
    ("url", "expected_port"),
    [
        ("redis://localhost", 6379),
        ("redis://localhost:6380", 6380),
        ("redis://localhost:0", 0),
    ],
)
async def test_from_url_preserves_startup_node_port(
    url: str, expected_port: int
) -> None:
    cluster = RedisCluster.from_url(url)

    assert len(cluster.startup_nodes) == 1
    assert cluster.startup_nodes[0].port == expected_port

    await cluster.aclose()


@pytest.mark.fixed_client
def test_from_url_requires_startup_node_host() -> None:
    with pytest.raises(RedisClusterException, match="requires at least one node"):
        RedisCluster.from_url("redis://:0")


@pytest.mark.fixed_client
@pytest.mark.parametrize("port", [None, "", False, 0.0])
def test_constructor_rejects_other_falsy_ports(port: Any) -> None:
    with pytest.raises(RedisClusterException, match="requires at least one node"):
        RedisCluster(host="localhost", port=port)


@pytest.mark.onlycluster
class TestRedisClusterObj:
    """
    Tests for the RedisCluster class
    """

    async def test_startup_nodes(self) -> None:
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
        cluster = await get_mocked_redis_client(startup_nodes=startup_nodes)
        assert (
            cluster.get_node(host=default_host, port=port_1) is not None
            and cluster.get_node(host=default_host, port=port_2) is not None
        )

        await cluster.aclose()

        startup_node = ClusterNode("127.0.0.1", 16379)
        async with RedisCluster(startup_nodes=[startup_node], client_name="test") as rc:
            assert await rc.set("A", 1)
            assert await rc.get("A") == b"1"
            assert all(
                [
                    name == "test"
                    for name in (
                        await rc.client_getname(target_nodes=rc.ALL_NODES)
                    ).values()
                ]
            )

    async def test_cluster_set_get_retry_object(self, request: FixtureRequest):
        retry = Retry(NoBackoff(), 2)
        url = request.config.getoption("--redis-url")
        async with RedisCluster.from_url(url, retry=retry) as r:
            assert r.retry.get_retries() == retry.get_retries()
            assert isinstance(r.retry._backoff, NoBackoff)
            for node in r.get_nodes():
                # validate nodes lower level connections default
                # retry policy is applied
                n_retry = node.acquire_connection().retry
                assert n_retry is not None
                assert n_retry._retries == 0
                assert isinstance(n_retry._backoff, NoBackoff)
            rand_cluster_node = r.get_random_node()
            existing_conn = rand_cluster_node.acquire_connection()
            # Change retry policy
            new_retry = Retry(ExponentialBackoff(), 3)
            r.set_retry(new_retry)
            assert r.retry.get_retries() == new_retry.get_retries()
            assert isinstance(r.retry._backoff, ExponentialBackoff)
            for node in r.get_nodes():
                # validate nodes lower level connections are not affected
                n_retry = node.acquire_connection().retry
                assert n_retry is not None
                assert n_retry._retries == 0
                assert isinstance(n_retry._backoff, NoBackoff)
            assert existing_conn.retry.get_retries() == 0
            new_conn = rand_cluster_node.acquire_connection()
            assert new_conn.retry._retries == 0

    async def test_cluster_retry_object(self, request: FixtureRequest) -> None:
        url = request.config.getoption("--redis-url")
        async with RedisCluster.from_url(url) as rc_default:
            # Test default retry
            retry = rc_default.retry

            # FIXME: Workaround for https://github.com/redis/redis-py/issues/3030
            host = rc_default.get_default_node().host

            assert isinstance(retry, Retry)
            assert retry._retries == 10
            assert isinstance(retry._backoff, type(ExponentialWithJitterBackoff()))
            assert retry._backoff._base == 0.01
            assert retry._backoff._cap == 1

            # validate nodes connections are using the default retry for
            # lower level connections when client is created through 'from_url' method
            # without specified retry object
            node1_retry = rc_default.get_node(host, 16379).acquire_connection().retry
            node2_retry = rc_default.get_node(host, 16380).acquire_connection().retry
            for node_retry in (node1_retry, node2_retry):
                assert node_retry.get_retries() == 0
                assert isinstance(node_retry._backoff, NoBackoff)
                assert node_retry._supported_errors == (ConnectionError,)

        retry = Retry(ExponentialBackoff(10, 5), 5)
        async with RedisCluster.from_url(url, retry=retry) as rc_custom_retry:
            # Test custom retry
            assert rc_custom_retry.retry == retry
            # validate nodes connections are using the default retry for
            # lower level connections when client is created through 'from_url' method
            # with specified retry object
            node1_retry = rc_default.get_node(host, 16379).acquire_connection().retry
            node2_retry = rc_default.get_node(host, 16380).acquire_connection().retry
            for node_retry in (node1_retry, node2_retry):
                assert node_retry.get_retries() == 0
                assert isinstance(node_retry._backoff, NoBackoff)
                assert node_retry._supported_errors == (ConnectionError,)

        async with RedisCluster.from_url(
            url, cluster_error_retry_attempts=0
        ) as rc_no_retries:
            # Test no cluster retries
            assert rc_no_retries.retry.get_retries() == 0

        async with RedisCluster.from_url(
            url, retry=Retry(NoBackoff(), 0)
        ) as rc_no_retries:
            assert rc_no_retries.retry.get_retries() == 0

    async def test_deprecated_lib_name_lib_version(self) -> None:
        with (
            pytest.warns(
                DeprecationWarning,
                match="deprecated usage of input argument/s 'lib_name'",
            ),
            pytest.warns(
                DeprecationWarning,
                match="deprecated usage of input argument/s 'lib_version'",
            ),
        ):
            startup_nodes = [ClusterNode("127.0.0.1", 16379)]
            async with RedisCluster(
                startup_nodes=startup_nodes, lib_name="test2", lib_version="1234"
            ) as cluster:
                info = await cluster.client_info()
                assert info["lib-ver"] == "1234"
                assert info["lib-name"] == "test2"

    async def test_from_url(self, request: FixtureRequest) -> None:
        url = request.config.getoption("--redis-url")

        async with RedisCluster.from_url(url) as rc:
            await rc.set("a", 1)
            await rc.get("a") == 1

        rc = RedisCluster.from_url("rediss://localhost:16379")
        assert rc.connection_kwargs["connection_class"] is SSLConnection

    async def test_max_connections(
        self, create_redis: Callable[..., RedisCluster]
    ) -> None:
        rc = await create_redis(cls=RedisCluster, max_connections=10)
        for node in rc.get_nodes():
            assert node.max_connections == 10

        with mock.patch.object(
            Connection, "read_response", autospec=True
        ) as read_response:

            async def read_response_mocked(
                connection: Connection, *args: Any, **kwargs: Any
            ) -> Any:
                await asyncio.sleep(0.1)
                if connection.protocol == 3 and connection.handshake_metadata is None:
                    return {b"proto": 3}

            read_response.side_effect = read_response_mocked

            # return_exceptions=True waits for every ping, so no task is still
            # holding a connection when the client is closed below. (Since
            # f6bbfb45 a ping disconnects its connection in a finally block.)
            results = await asyncio.gather(
                *(rc.ping(target_nodes=RedisCluster.DEFAULT_NODE) for _ in range(11)),
                return_exceptions=True,
            )
        # the 11th ping finds no free connection; the other 10 succeed
        errors = [res for res in results if isinstance(res, BaseException)]
        assert len(errors) == 1, results
        assert isinstance(errors[0], MaxConnectionsError), results

        await rc.aclose()

    async def test_execute_command_errors(self, r: RedisCluster) -> None:
        """
        Test that if no key is provided then exception should be raised.
        """
        with pytest.raises(RedisClusterException) as ex:
            await r.execute_command("GET")
        assert str(ex.value).startswith(
            "No way to dispatch this command to Redis Cluster. Missing key."
        )

    async def test_execute_command_node_flag_primaries(self, r: RedisCluster) -> None:
        """
        Test command execution with nodes flag PRIMARIES
        """
        primaries = r.get_primaries()
        replicas = r.get_replicas()
        mock_all_nodes_resp(r, "PONG")
        assert await r.ping(target_nodes=RedisCluster.PRIMARIES) is True
        for primary in primaries:
            conn = primary._free.pop()
            assert conn.read_response.called is True
        for replica in replicas:
            conn = replica._free.pop()
            assert conn.read_response.called is not True

    async def test_execute_command_node_flag_replicas(self, r: RedisCluster) -> None:
        """
        Test command execution with nodes flag REPLICAS
        """
        replicas = r.get_replicas()
        assert len(replicas) != 0, "This test requires Cluster with 1 replica"

        primaries = r.get_primaries()
        mock_all_nodes_resp(r, "PONG")
        assert await r.ping(target_nodes=RedisCluster.REPLICAS) is True
        for replica in replicas:
            conn = replica._free.pop()
            assert conn.read_response.called is True
        for primary in primaries:
            conn = primary._free.pop()
            assert conn.read_response.called is not True

        await r.aclose()

    async def test_execute_command_node_flag_all_nodes(self, r: RedisCluster) -> None:
        """
        Test command execution with nodes flag ALL_NODES
        """
        mock_all_nodes_resp(r, "PONG")
        assert await r.ping(target_nodes=RedisCluster.ALL_NODES) is True
        for node in r.get_nodes():
            conn = node._free.pop()
            assert conn.read_response.called is True

    async def test_execute_command_node_flag_random(self, r: RedisCluster) -> None:
        """
        Test command execution with nodes flag RANDOM
        """
        mock_all_nodes_resp(r, "PONG")
        assert await r.ping(target_nodes=RedisCluster.RANDOM) is True
        called_count = 0
        for node in r.get_nodes():
            conn = node._free.pop()
            if conn.read_response.called is True:
                called_count += 1
        assert called_count == 1

    async def test_execute_command_default_node(self, r: RedisCluster) -> None:
        """
        Test command execution without node flag is being executed on the
        default node
        """
        def_node = r.get_default_node()
        mock_node_resp(def_node, "PONG")
        assert await r.ping() is True
        conn = def_node._free.pop()
        assert conn.read_response.called

    async def test_ask_redirection(self, r: RedisCluster) -> None:
        """
        Test that the server handles ASK response.

        At first call it should return a ASK ResponseError that will point
        the client to the next server it should talk to.

        Important thing to verify is that it tries to talk to the second node.
        """
        redirect_node = r.get_nodes()[0]
        with mock.patch.object(
            ClusterNode, "execute_command", autospec=True
        ) as execute_command:

            def ask_redirect_effect(self, *args, **options):
                def ok_response(self, *args, **options):
                    assert self.host == redirect_node.host
                    assert self.port == redirect_node.port

                    return "MOCK_OK"

                execute_command.side_effect = ok_response
                raise AskError(f"12182 {redirect_node.host}:{redirect_node.port}")

            execute_command.side_effect = ask_redirect_effect

            assert await r.execute_command("SET", "foo", "bar") == "MOCK_OK"

    async def test_himport_set_moved_after_ask_uses_moved_target(
        self, r: RedisCluster
    ) -> None:
        """An HIMPORT SET that is ASK-redirected and then hits a MOVED
        mid-exchange must retry against the MOVED target, not the stale ASK
        target.

        HIMPORT SET folds the ASK allowance into its own packed write, so the
        retry loop must clear ``asking`` before the exchange runs; otherwise a
        MOVED raised mid-exchange would leave ``asking`` set and shadow the
        moved-retry branch on the next iteration.
        """
        key = "himport:key"
        slot = r.keyslot(key)
        primary = r.nodes_manager.get_node_from_slot(slot)
        others = [n for n in r.get_primaries() if n.name != primary.name]
        if len(others) < 2:
            pytest.skip("requires at least 3 primaries")
        ask_node, moved_node = others[0], others[1]

        calls = []

        def himport_effect(self, *args, **kwargs):
            calls.append((self.host, self.port))
            if len(calls) == 1:
                raise AskError(f"{slot} {ask_node.host}:{ask_node.port}")
            if len(calls) == 2:
                raise MovedError(f"{slot} {moved_node.host}:{moved_node.port}")
            return "MOCK_OK"

        # ``_determine_slot`` parses HIMPORT SET keys via COMMAND INFO, which the
        # test server may not know; pin it so the moved branch can recompute the
        # target without a live HIMPORT-capable server.
        with (
            mock.patch.object(
                RedisCluster, "_determine_slot", new=mock.AsyncMock(return_value=slot)
            ),
            mock.patch.object(
                ClusterNode,
                "execute_command",
                autospec=True,
                side_effect=himport_effect,
            ),
        ):
            assert (
                await r._execute_command(primary, HIMPORT_SET, key, "shared", "alice")
                == "MOCK_OK"
            )

        assert calls[0] == (primary.host, primary.port)
        assert calls[1] == (ask_node.host, ask_node.port)
        # Third attempt must follow the MOVED redirect, not the stale ASK target.
        assert calls[2] == (moved_node.host, moved_node.port)

    async def test_moved_redirection(
        self, create_redis: Callable[..., RedisCluster]
    ) -> None:
        """
        Test that the client handles MOVED response.
        """
        await moved_redirection_helper(create_redis, failover=False)

    async def test_moved_redirection_after_failover(
        self, create_redis: Callable[..., RedisCluster]
    ) -> None:
        """
        Test that the client handles MOVED response after a failover.
        """
        await moved_redirection_helper(create_redis, failover=True)

    async def test_moved_redirection_circular_moved(
        self, create_redis: Callable[..., RedisCluster]
    ) -> None:
        """
        Verify that the client does not update its slot map when receiving a circular MOVED response
        (i.e., a MOVED redirect pointing back to the same node), and retries again the same node.
        """
        await moved_redirection_helper(
            create_redis, failover=False, circular_moved=True
        )

    async def test_refresh_using_specific_nodes(
        self, create_redis: Callable[..., RedisCluster]
    ) -> None:
        """
        Test making calls on specific nodes when the cluster has failed over to
        another node
        """
        node_7006 = ClusterNode(host=default_host, port=7006, server_type=PRIMARY)
        node_7007 = ClusterNode(host=default_host, port=7007, server_type=PRIMARY)
        with mock.patch.object(
            ClusterNode, "execute_command", autospec=True
        ) as execute_command:
            with mock.patch.object(
                NodesManager, "initialize", autospec=True
            ) as initialize:
                with mock.patch.multiple(
                    Connection,
                    send_packed_command=mock.DEFAULT,
                    connect=mock.DEFAULT,
                    can_read_destructive=mock.DEFAULT,
                ) as mocks:
                    # simulate 7006 as a failed node
                    def execute_command_mock(self, *args, **options):
                        if self.port == 7006:
                            execute_command.failed_calls += 1
                            raise ClusterDownError(
                                "CLUSTERDOWN The cluster is "
                                "down. Use CLUSTER INFO for "
                                "more information"
                            )
                        elif self.port == 7007:
                            execute_command.successful_calls += 1

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

                    execute_command.side_effect = execute_command_mock
                    execute_command.successful_calls = 0
                    execute_command.failed_calls = 0
                    initialize.side_effect = initialize_mock
                    mocks["can_read_destructive"].return_value = False
                    mocks["send_packed_command"].return_value = "MOCK_OK"
                    mocks["connect"].return_value = None
                    with mock.patch.object(
                        AsyncCommandsParser, "initialize", autospec=True
                    ) as cmd_parser_initialize:

                        def cmd_init_mock(self, r: ClusterNode) -> None:
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

                        rc = await create_redis(cls=RedisCluster, flushdb=False)
                        assert len(rc.get_nodes()) == 1
                        assert rc.get_node(node_name=node_7006.name) is not None

                        await rc.get("foo")

                        # Cluster should now point to 7007, and there should be
                        # one failed and one successful call
                        assert len(rc.get_nodes()) == 1
                        assert rc.get_node(node_name=node_7007.name) is not None
                        assert rc.get_node(node_name=node_7006.name) is None
                        assert execute_command.failed_calls == 1
                        assert execute_command.successful_calls == 1

    async def test_keyslot(self, r: RedisCluster) -> None:
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

    async def test_all_nodes(self, r: RedisCluster) -> None:
        """
        Set a list of nodes and it should be possible to iterate over all
        """
        nodes = [node for node in r.nodes_manager.nodes_cache.values()]

        for i, node in enumerate(r.get_nodes()):
            assert node in nodes

    async def test_all_nodes_masters(self, r: RedisCluster) -> None:
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

    async def test_set_default_node_success(self, r: RedisCluster) -> None:
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
        r.set_default_node(new_def_node)
        assert r.get_default_node() == new_def_node

    async def test_set_default_node_failure(self, r: RedisCluster) -> None:
        """
        test failed replacement of the default cluster node
        """
        default_node = r.get_default_node()
        new_def_node = ClusterNode("1.1.1.1", 1111)
        with pytest.raises(DataError):
            r.set_default_node(None)
        with pytest.raises(DataError):
            r.set_default_node(new_def_node)
        assert r.get_default_node() == default_node

    async def test_get_node_from_key(self, r: RedisCluster) -> None:
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

    @skip_if_redis_enterprise()
    async def test_not_require_full_coverage_cluster_down_error(
        self, r: RedisCluster
    ) -> None:
        """
        When require_full_coverage is set to False (default client config) and not
        all slots are covered, if one of the nodes has 'cluster-require_full_coverage'
        config set to 'yes' some key-based commands should throw ClusterDownError
        """
        node = r.get_node_from_key("foo")
        missing_slot = r.keyslot("foo")
        assert await r.set("foo", "bar") is True
        orig_retry = r.retry
        try:
            assert all(await r.cluster_delslots(missing_slot))
            # the slot stays uncovered, so retrying the command cannot succeed
            r.set_retry(Retry(NoBackoff(), 0))
            with pytest.raises(ClusterDownError):
                await r.exists("foo")
        except ResponseError as e:
            assert "CLUSTERDOWN" in str(e)
        finally:
            # the recovery check below may still need retries
            r.set_retry(orig_retry)
            try:
                # Add back the missing slot
                assert await r.cluster_addslots(node, missing_slot) is True
                # Make sure we are not getting ClusterDownError anymore
                assert await r.exists("foo") == 1
            except ResponseError as e:
                if f"Slot {missing_slot} is already busy" in str(e):
                    # It can happen if the test failed to delete this slot
                    pass
                else:
                    raise e

    async def test_can_run_concurrent_commands(self, request: FixtureRequest) -> None:
        url = request.config.getoption("--redis-url")
        rc = RedisCluster.from_url(url)
        assert all(
            await asyncio.gather(
                *(rc.echo("i", target_nodes=RedisCluster.ALL_NODES) for i in range(100))
            )
        )
        await rc.aclose()

    def test_replace_cluster_node(self, r: RedisCluster) -> None:
        prev_default_node = r.get_default_node()
        r.replace_default_node()
        assert r.get_default_node() != prev_default_node
        r.replace_default_node(prev_default_node)
        assert r.get_default_node() == prev_default_node

    async def test_default_node_is_replaced_after_exception(self, r):
        curr_default_node = r.get_default_node()
        # CLUSTER NODES command is being executed on the default node
        nodes = await r.cluster_nodes()
        assert "myself" in nodes.get(curr_default_node.name).get("flags")
        # Save original free connections to restore later
        original_free = list(curr_default_node._free)
        try:
            # Mock connection error for the default node
            mock_node_resp_exc(curr_default_node, ConnectionError("error"))
            # Test that the command succeed from a different node
            nodes = await r.cluster_nodes()
            assert "myself" not in nodes.get(curr_default_node.name).get("flags")
            assert r.get_default_node() != curr_default_node
        finally:
            # Restore original connections so teardown can work
            while curr_default_node._free:
                curr_default_node._free.pop()
            for conn in original_free:
                curr_default_node._free.append(conn)
            # Rollback to the old default node
            r.replace_default_node(curr_default_node)

    async def test_address_remap(self, create_redis, master_host):
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
        await asyncio.gather(*[p.start() for p in proxies])
        try:
            # create cluster:
            r = await create_redis(
                cls=RedisCluster, flushdb=False, address_remap=address_remap
            )
            try:
                assert await r.ping() is True
                assert await r.set("byte_string", b"giraffe")
                assert await r.get("byte_string") == b"giraffe"
            finally:
                await r.aclose()
        finally:
            await asyncio.gather(*[p.aclose() for p in proxies])

        # verify that the proxies were indeed used
        n_used = sum((1 if p.n_connections else 0) for p in proxies)
        assert n_used > 1

    @pytest.mark.parametrize(
        "remap_host, offset",
        # Not the ports of the other remap tests: their listeners may still be open
        [("127.0.0.1", 3000), ("localhost", 3100)],
    )
    async def test_address_remap_applies_to_redirects(
        self, create_redis, master_host, remap_host, offset
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
        await asyncio.gather(*[p.start() for p in proxies])
        try:
            r = await create_redis(
                cls=RedisCluster, flushdb=False, address_remap=address_remap
            )
            try:
                slot = r.keyslot("foo")
                owner = r.nodes_manager.get_node_from_slot(slot)
                other = next(p for p in r.get_primaries() if p.name != owner.name)
                # The address the redirected-to node advertises, not its proxy
                redirect_port = other.port - offset
                for redirect_error in (AskError, MovedError):
                    with mock.patch.object(
                        ClusterNode, "execute_command", autospec=True
                    ) as execute_command:

                        def redirect_effect(self, *args, **options):
                            def ok_response(self, *args, **options):
                                assert (self.host, self.port) == (
                                    "127.0.0.1",
                                    other.port,
                                )
                                return "MOCK_OK"

                            execute_command.side_effect = ok_response
                            raise redirect_error(f"{slot} {hostname}:{redirect_port}")

                        execute_command.side_effect = redirect_effect

                        assert await r.execute_command("SET", "foo", "bar") == "MOCK_OK"
            finally:
                await r.aclose()
        finally:
            await asyncio.gather(*[p.aclose() for p in proxies])


@pytest.mark.fixed_client
class TestRedisClusterObjMocked:
    """RedisCluster behaviour checked against mocked nodes; no server needed."""

    async def test_aclosing(self) -> None:
        cluster = await get_mocked_redis_client(host=default_host, port=default_port)
        called = 0

        async def mock_aclose():
            nonlocal called
            called += 1

        with mock.patch.object(cluster, "aclose", mock_aclose):
            async with aclosing(cluster):
                pass
            assert called == 1
        await cluster.aclose()

    async def test_close_is_aclose(self) -> None:
        """
        Test that it is possible to use host & port arguments as startup node
        args
        """
        cluster = await get_mocked_redis_client(host=default_host, port=default_port)
        called = 0

        async def mock_aclose():
            nonlocal called
            called += 1

        with mock.patch.object(cluster, "aclose", mock_aclose):
            with pytest.warns(DeprecationWarning, match=r"Use aclose\(\) instead"):
                await cluster.close()
            assert called == 1
        await cluster.aclose()

    @pytest.mark.usefixtures("no_cluster_retry_delay")
    @pytest.mark.parametrize("error", RedisCluster.ERRORS_ALLOW_RETRY)
    async def test_cluster_down_overreaches_retry_attempts(
        self,
        error: Union[Type[TimeoutError], Type[ClusterDownError], Type[ConnectionError]],
    ) -> None:
        """
        When error that allows retry is thrown, test that we retry executing
        the command as many times as configured in cluster_error_retry_attempts
        and then raise the exception
        """
        with mock.patch.object(RedisCluster, "_execute_command") as execute_command:

            def raise_error(target_node, *args, **kwargs):
                execute_command.failed_calls += 1
                raise error("mocked error")

            execute_command.side_effect = raise_error
            # Without this the attribute is a MagicMock, so the comparison below
            # is never an integer comparison.
            execute_command.failed_calls = 0

            rc = await get_mocked_redis_client(host=default_host, port=default_port)

            with pytest.raises(error):
                await rc.get("bar")
            # retry counts retries, so the first attempt is one more.
            assert execute_command.failed_calls == rc.retry.get_retries() + 1

            await rc.aclose()

    async def test_empty_startup_nodes(self) -> None:
        """
        Test that exception is raised when empty providing empty startup_nodes
        """
        with pytest.raises(RedisClusterException) as ex:
            RedisCluster(startup_nodes=[])

        assert str(ex.value).startswith(
            "RedisCluster requires at least one node to discover the cluster"
        ), str_if_bytes(ex.value)

    async def test_get_node_name(self) -> None:
        assert (
            get_node_name(default_host, default_port)
            == f"{default_host}:{default_port}"
        )

    async def test_host_port_startup_node(self) -> None:
        """
        Test that it is possible to use host & port arguments as startup node
        args
        """
        cluster = await get_mocked_redis_client(host=default_host, port=default_port)
        assert cluster.get_node(host=default_host, port=default_port) is not None

        await cluster.aclose()

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
    async def test_reading_with_load_balancing_strategies(
        self,
        read_from_replicas: bool,
        load_balancing_strategy: LoadBalancingStrategy,
        mocks_srv_ports: List[int],
    ) -> None:
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

        with mock.patch.multiple(
            Connection,
            send_command=mock.DEFAULT,
            read_response=mock.DEFAULT,
            _connect=mock.DEFAULT,
            can_read_destructive=mock.DEFAULT,
            on_connect=mock.DEFAULT,
        ) as mocks:
            with (
                mock.patch.object(
                    ClusterNode, "execute_command", autospec=True
                ) as execute_command,
                patch("random.randint", wraps=mock_randint),
            ):

                async def execute_command_mock_first(self, *args, **options):
                    await self.connection_class(**self.connection_kwargs).connect()
                    # Primary
                    assert self.port == mocks_srv_ports[0]
                    execute_command.side_effect = execute_command_mock_second
                    return "MOCK_OK"

                def execute_command_mock_second(self, *args, **options):
                    # Replica
                    assert self.port == mocks_srv_ports[1]
                    execute_command.side_effect = execute_command_mock_third
                    return "MOCK_OK"

                def execute_command_mock_third(self, *args, **options):
                    # Primary
                    assert self.port == mocks_srv_ports[2]
                    return "MOCK_OK"

                # We don't need to create a real cluster connection but we
                # do want RedisCluster.on_connect function to get called,
                # so we'll mock some of the Connection's functions to allow it
                execute_command.side_effect = execute_command_mock_first
                mocks["send_command"].return_value = True
                mocks["read_response"].return_value = "OK"
                mocks["_connect"].return_value = True
                mocks["can_read_destructive"].return_value = False
                mocks["on_connect"].return_value = True

                # Create a cluster with reading from replications
                read_cluster = await get_mocked_redis_client(
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
                await read_cluster.get("foo")
                await read_cluster.get("foo")
                await read_cluster.get("foo")
                mocks["send_command"].assert_has_calls([mock.call("READONLY")])

                await read_cluster.aclose()

    @pytest.mark.parametrize("error", [AskError, MovedError])
    async def test_redirect_without_host(self, error) -> None:
        """
        A node whose endpoint is unknown (cluster-preferred-endpoint-type
        unknown-endpoint) redirects with only a port, and the command follows
        the redirect to that port on the host of the node that sent it.
        """
        rc = await get_mocked_redis_client(host=default_host, port=default_port)
        slot = rc.keyslot("foo")
        origin = rc.get_node_from_key("foo")
        target = next(node for node in rc.get_primaries() if node is not origin)
        calls = []
        redirects = []

        async def execute_command(node, *args, **kwargs):
            calls.append((node, args[0]))
            if node is origin:
                redirects.append(error(f"{slot} :{target.port}"))
                raise redirects[-1]
            return b"value"

        with mock.patch.object(
            ClusterNode, "execute_command", autospec=True, side_effect=execute_command
        ):
            assert await rc.get("foo") == b"value"

        expected = [(origin, "GET"), (target, "GET")]
        if error is AskError:
            expected.insert(1, (target, "ASKING"))
        assert calls == expected
        assert [redirect.node_addr for redirect in redirects] == [
            (origin.host, target.port)
        ]

        await rc.aclose()

    @pytest.mark.parametrize("error", [AskError, MovedError])
    async def test_redirect_without_host_with_address_remap(self, error) -> None:
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

        rc = await get_mocked_redis_client(
            host=default_host, port=default_port, address_remap=address_remap
        )
        slot = rc.keyslot("foo")
        origin = rc.get_node_from_key("foo")
        target = next(node for node in rc.get_primaries() if node is not origin)
        calls = []

        async def execute_command(node, *args, **kwargs):
            calls.append((node, args[0]))
            if node is origin:
                # A node redirects to the port it advertises, not the remapped one
                raise error(f"{slot} :{target.port - offset}")
            return b"value"

        with mock.patch.object(
            ClusterNode, "execute_command", autospec=True, side_effect=execute_command
        ):
            assert await rc.get("foo") == b"value"

        expected = [(origin, "GET"), (target, "GET")]
        if error is AskError:
            expected.insert(1, (target, "ASKING"))
        assert calls == expected

        await rc.aclose()

    @pytest.mark.parametrize("error", [AskError, MovedError])
    async def test_transaction_redirect_without_host(self, error) -> None:
        """
        A transaction redirected with only a port is retried on that port of the
        host of the node that sent the redirect.
        """
        rc = await get_mocked_redis_client(host=default_host, port=default_port)
        slot = rc.keyslot("foo")
        origin = rc.get_node_from_key("foo")
        target = next(node for node in rc.get_primaries() if node is not origin)
        for node in (origin, target):
            connection = mock_node_resp(node, None)._free[0]
            connection.host, connection.port = node.host, node.port
        calls = []

        async def parse_response(node, connection, command, **kwargs):
            calls.append((node, command))
            if node is origin:
                if command == "_":
                    raise error(f"{slot} :{target.port}")
                if command == "EXEC":
                    raise ExecAbortError()
            if command == "EXEC":
                return [b"bar"]
            return b"QUEUED" if command == "_" else b"OK"

        with mock.patch.object(
            ClusterNode, "parse_response", autospec=True, side_effect=parse_response
        ):
            async with rc.pipeline(transaction=True) as pipe:
                pipe.get("foo")
                assert await pipe.execute() == [b"bar"]

        assert calls == [
            (node, command)
            for node in (origin, target)
            for command in ("MULTI", "_", "EXEC")
        ]

        await rc.aclose()


@pytest.mark.onlycluster
class TestClusterRedisCommands:
    """
    Tests for RedisCluster unique commands
    """

    async def _wait_for_bgsave(self, r: RedisCluster, timeout=10) -> None:
        loop = asyncio.get_running_loop()
        deadline = loop.time() + timeout
        while True:
            info = await r.info("persistence", target_nodes=r.get_default_node())
            if int(info.get("rdb_bgsave_in_progress", 0)) == 0:
                return
            if loop.time() > deadline:
                pytest.fail("Timed out waiting for BGSAVE to finish")
            await asyncio.sleep(0.05)

    @pytest.mark.parametrize(
        "load_balancing_strategy",
        [
            LoadBalancingStrategy.ROUND_ROBIN,
            LoadBalancingStrategy.ROUND_ROBIN_REPLICAS,
            LoadBalancingStrategy.RANDOM,
            LoadBalancingStrategy.RANDOM_REPLICA,
        ],
    )
    async def test_get_and_set_with_load_balanced_client(
        self, create_redis, load_balancing_strategy: LoadBalancingStrategy
    ) -> None:
        r = await create_redis(
            cls=RedisCluster,
            load_balancing_strategy=load_balancing_strategy,
        )

        # get and set can't be tested independently of each other
        assert await r.get("a") is None

        byte_string = b"value"
        assert await r.set("byte_string", byte_string)

        # run the get command for the same key several times
        # to iterate over the read nodes
        assert await r.get("byte_string") == byte_string
        assert await r.get("byte_string") == byte_string
        assert await r.get("byte_string") == byte_string

    async def test_mget_nonatomic(self, r: RedisCluster) -> None:
        assert await r.mget_nonatomic([]) == []
        assert await r.mget_nonatomic(["a", "b"]) == [None, None]
        await r.set("a", "1")
        await r.set("b", "2")
        await r.set("c", "3")

        assert await r.mget_nonatomic("a", "other", "b", "c") == [
            b"1",
            None,
            b"2",
            b"3",
        ]

    async def test_mset_nonatomic(self, r: RedisCluster) -> None:
        d = {"a": b"1", "b": b"2", "c": b"3", "d": b"4"}
        assert await r.mset_nonatomic(d)
        for k, v in d.items():
            assert await r.get(k) == v

    async def test_config_set(self, r: RedisCluster) -> None:
        assert await r.config_set("slowlog-log-slower-than", 0)

    async def test_cluster_config_resetstat(self, r: RedisCluster) -> None:
        await r.ping(target_nodes="all")
        all_info = await r.info(target_nodes="all")
        prior_commands_processed = -1
        for node_info in all_info.values():
            prior_commands_processed = node_info["total_commands_processed"]
            assert prior_commands_processed >= 1
        await r.config_resetstat(target_nodes="all")
        all_info = await r.info(target_nodes="all")
        for node_info in all_info.values():
            reset_commands_processed = node_info["total_commands_processed"]
            assert reset_commands_processed < prior_commands_processed

    async def test_client_setname(self, r: RedisCluster) -> None:
        node = r.get_random_node()
        await r.client_setname("redis_py_test", target_nodes=node)
        client_name = await r.client_getname(target_nodes=node)
        assert_resp_response(
            r, client_name, "redis_py_test", b"redis_py_test", "redis_py_test"
        )

    async def test_exists(self, r: RedisCluster) -> None:
        d = {"a": b"1", "b": b"2", "c": b"3", "d": b"4"}
        await r.mset_nonatomic(d)
        assert await r.exists(*d.keys()) == len(d)

    async def test_delete(self, r: RedisCluster) -> None:
        d = {"a": b"1", "b": b"2", "c": b"3", "d": b"4"}
        await r.mset_nonatomic(d)
        assert await r.delete(*d.keys()) == len(d)
        assert await r.delete(*d.keys()) == 0

    async def test_touch(self, r: RedisCluster) -> None:
        d = {"a": b"1", "b": b"2", "c": b"3", "d": b"4"}
        await r.mset_nonatomic(d)
        assert await r.touch(*d.keys()) == len(d)

    async def test_unlink(self, r: RedisCluster) -> None:
        d = {"a": b"1", "b": b"2", "c": b"3", "d": b"4"}
        await r.mset_nonatomic(d)
        assert await r.unlink(*d.keys()) == len(d)
        # UNLINK removes the keys from the keyspace right away; only the memory
        # is reclaimed in the background
        assert await r.unlink(*d.keys()) == 0

    async def test_initialize_before_execute_multi_key_command(
        self, request: FixtureRequest
    ) -> None:
        # Test for issue https://github.com/redis/redis-py/issues/2437
        url = request.config.getoption("--redis-url")
        r = RedisCluster.from_url(url)
        assert 0 == await r.exists("a", "b", "c")
        await r.aclose()

    @skip_if_redis_enterprise()
    async def test_cluster_myid(self, r: RedisCluster) -> None:
        node = r.get_random_node()
        myid = await r.cluster_myid(node)
        assert len(myid) == 40

    @skip_if_server_version_lt("7.2.0")
    @skip_if_redis_enterprise()
    async def test_cluster_myshardid(self, r: RedisCluster) -> None:
        node = r.get_random_node()
        myshardid = await r.cluster_myshardid(node)
        assert len(myshardid) == 40

    @skip_if_server_version_lt("7.2.0")
    @skip_if_redis_enterprise()
    async def test_cluster_myshardid_decoded(self, decoded_r: RedisCluster) -> None:
        node = decoded_r.get_random_node()
        myshardid = await decoded_r.cluster_myshardid(node)
        assert isinstance(myshardid, str)
        assert len(myshardid) == 40

    @skip_if_redis_enterprise()
    async def test_cluster_slots(self, r: RedisCluster) -> None:
        mock_all_nodes_resp(r, default_cluster_slots)
        cluster_slots = await r.cluster_slots()
        assert isinstance(cluster_slots, dict)
        assert len(default_cluster_slots) == len(cluster_slots)
        assert cluster_slots.get((0, 8191)) is not None
        assert cluster_slots.get((0, 8191)).get("primary") == ("127.0.0.1", 7000)

    @skip_if_redis_enterprise()
    async def test_cluster_addslots(self, r: RedisCluster) -> None:
        node = r.get_random_node()
        mock_node_resp(node, "OK")
        assert await r.cluster_addslots(node, 1, 2, 3) is True

    @skip_if_server_version_lt("7.0.0")
    @skip_if_redis_enterprise()
    async def test_cluster_addslotsrange(self, r: RedisCluster):
        node = r.get_random_node()
        mock_node_resp(node, "OK")
        assert await r.cluster_addslotsrange(node, 1, 5)

    @skip_if_redis_enterprise()
    async def test_cluster_countkeysinslot(self, r: RedisCluster) -> None:
        node = r.nodes_manager.get_node_from_slot(1)
        mock_node_resp(node, 2)
        assert await r.cluster_countkeysinslot(1) == 2

    async def test_cluster_count_failure_report(self, r: RedisCluster) -> None:
        mock_all_nodes_resp(r, 0)
        assert await r.cluster_count_failure_report("node_0") == 0

    @skip_if_redis_enterprise()
    async def test_cluster_failover(self, r: RedisCluster) -> None:
        node = r.get_random_node()
        mock_node_resp(node, "OK")
        assert await r.cluster_failover(node) is True
        assert await r.cluster_failover(node, "FORCE") is True
        assert await r.cluster_failover(node, "TAKEOVER") is True
        with pytest.raises(RedisError):
            await r.cluster_failover(node, "FORCT")

    @skip_if_redis_enterprise()
    async def test_cluster_info(self, r: RedisCluster) -> None:
        info = await r.cluster_info()
        assert isinstance(info, dict)
        assert info["cluster_state"] == "ok"

    @skip_if_redis_enterprise()
    async def test_cluster_keyslot(self, r: RedisCluster) -> None:
        mock_all_nodes_resp(r, 12182)
        assert await r.cluster_keyslot("foo") == 12182

    @skip_if_redis_enterprise()
    async def test_cluster_meet(self, r: RedisCluster) -> None:
        node = r.get_default_node()
        mock_node_resp(node, "OK")
        assert await r.cluster_meet("127.0.0.1", 6379) is True

    @skip_if_redis_enterprise()
    async def test_cluster_nodes(self, r: RedisCluster) -> None:
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
        nodes = await r.cluster_nodes()
        assert len(nodes) == 7
        assert nodes.get("172.17.0.7:7006") is not None
        assert (
            nodes.get("172.17.0.7:7006").get("node_id")
            == "c8253bae761cb1ecb2b61857d85dfe455a0fec8b"
        )

    @skip_if_redis_enterprise()
    async def test_cluster_nodes_importing_migrating(self, r: RedisCluster) -> None:
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
        nodes = await r.cluster_nodes()
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
    async def test_cluster_replicate(self, r: RedisCluster) -> None:
        node = r.get_random_node()
        all_replicas = r.get_replicas()
        mock_all_nodes_resp(r, "OK")
        assert await r.cluster_replicate(node, "c8253bae761cb61857d") is True
        results = await r.cluster_replicate(all_replicas, "c8253bae761cb61857d")
        if isinstance(results, dict):
            for res in results.values():
                assert res is True
        else:
            assert results is True

    @skip_if_redis_enterprise()
    async def test_cluster_reset(self, r: RedisCluster) -> None:
        mock_all_nodes_resp(r, "OK")
        assert await r.cluster_reset() is True
        assert await r.cluster_reset(False) is True
        all_results = await r.cluster_reset(False, target_nodes="all")
        for res in all_results.values():
            assert res is True

    @skip_if_redis_enterprise()
    async def test_cluster_save_config(self, r: RedisCluster) -> None:
        node = r.get_random_node()
        all_nodes = r.get_nodes()
        mock_all_nodes_resp(r, "OK")
        assert await r.cluster_save_config(node) is True
        all_results = await r.cluster_save_config(all_nodes)
        for res in all_results.values():
            assert res is True

    @skip_if_redis_enterprise()
    async def test_cluster_get_keys_in_slot(self, r: RedisCluster) -> None:
        response = ["{foo}1", "{foo}2"]
        node = r.nodes_manager.get_node_from_slot(12182)
        mock_node_resp(node, response)
        keys = await r.cluster_get_keys_in_slot(12182, 4)
        assert keys == response

    @skip_if_redis_enterprise()
    async def test_cluster_set_config_epoch(self, r: RedisCluster) -> None:
        mock_all_nodes_resp(r, "OK")
        assert await r.cluster_set_config_epoch(3) is True
        all_results = await r.cluster_set_config_epoch(3, target_nodes="all")
        for res in all_results.values():
            assert res is True

    @skip_if_redis_enterprise()
    async def test_cluster_setslot(self, r: RedisCluster) -> None:
        node = r.get_random_node()
        mock_node_resp(node, "OK")
        assert await r.cluster_setslot(node, "node_0", 1218, "IMPORTING") is True
        assert await r.cluster_setslot(node, "node_0", 1218, "NODE") is True
        assert await r.cluster_setslot(node, "node_0", 1218, "MIGRATING") is True
        with pytest.raises(RedisError):
            await r.cluster_failover(node, "STABLE")
        with pytest.raises(RedisError):
            await r.cluster_failover(node, "STATE")

    async def test_cluster_setslot_stable(self, r: RedisCluster) -> None:
        node = r.nodes_manager.get_node_from_slot(12182)
        mock_node_resp(node, "OK")
        assert await r.cluster_setslot_stable(12182) is True
        assert node._free.pop().read_response.called

    @skip_if_redis_enterprise()
    async def test_cluster_replicas(self, r: RedisCluster) -> None:
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
        replicas = await r.cluster_replicas("52611e796814b78e90ad94be9d769a4f668f9a")
        assert replicas.get("127.0.0.1:6377") is not None
        assert replicas.get("127.0.0.1:6378") is not None
        assert (
            replicas.get("127.0.0.1:6378").get("node_id")
            == "r4xfga22229cf3c652b6fca0d09ff69f3e0d4d"
        )

    @skip_if_server_version_lt("7.0.0")
    async def test_cluster_links(self, r: RedisCluster):
        node = r.get_random_node()
        res = await r.cluster_links(node)
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

    @skip_if_redis_enterprise()
    async def test_bgsave(self, r: RedisCluster) -> None:
        await self._wait_for_bgsave(r)
        assert await r.bgsave()
        await self._wait_for_bgsave(r)
        assert await r.bgsave(True)
        await self._wait_for_bgsave(r)

    async def test_info(self, r: RedisCluster) -> None:
        # Map keys to same slot
        await r.set("x{1}", 1)
        await r.set("y{1}", 2)
        await r.set("z{1}", 3)
        # Get node that handles the slot
        slot = r.keyslot("x{1}")
        node = r.nodes_manager.get_node_from_slot(slot)
        # Run info on that node
        info = await r.info(target_nodes=node)
        assert isinstance(info, dict)
        assert info["db0"]["keys"] == 3

    async def test_info_with_custom_callback(self, decoded_r: RedisCluster) -> None:
        decoded_r.set_response_callback("INFO", lambda response: response)
        info = await decoded_r.info("server", target_nodes=decoded_r.get_random_node())
        assert "redis_version" in info

    async def _init_slowlog_test(self, r: RedisCluster, node: ClusterNode) -> str:
        slowlog_lim = await r.config_get("slowlog-log-slower-than", target_nodes=node)
        assert (
            await r.config_set("slowlog-log-slower-than", 0, target_nodes=node) is True
        )
        return slowlog_lim["slowlog-log-slower-than"]

    async def _teardown_slowlog_test(
        self, r: RedisCluster, node: ClusterNode, prev_limit: str
    ) -> None:
        assert (
            await r.config_set("slowlog-log-slower-than", prev_limit, target_nodes=node)
            is True
        )

    async def test_slowlog_get(
        self, r: RedisCluster, slowlog: Optional[List[Dict[str, Union[int, bytes]]]]
    ) -> None:
        unicode_string = chr(3456) + "abcd" + chr(3421)
        node = r.get_node_from_key(unicode_string)
        slowlog_limit = await self._init_slowlog_test(r, node)
        assert await r.slowlog_reset(target_nodes=node)
        await r.get(unicode_string)
        slowlog = await r.slowlog_get(target_nodes=node)
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
        await self._teardown_slowlog_test(r, node, slowlog_limit)

    async def test_slowlog_get_limit(
        self, r: RedisCluster, slowlog: Optional[List[Dict[str, Union[int, bytes]]]]
    ) -> None:
        assert await r.slowlog_reset()
        node = r.get_node_from_key("foo")
        slowlog_limit = await self._init_slowlog_test(r, node)
        await r.get("foo")
        slowlog = await r.slowlog_get(1, target_nodes=node)
        assert isinstance(slowlog, list)
        # only one command, based on the number we passed to slowlog_get()
        assert len(slowlog) == 1
        await self._teardown_slowlog_test(r, node, slowlog_limit)

    async def test_slowlog_length(self, r: RedisCluster, slowlog: None) -> None:
        await r.get("foo")
        node = r.nodes_manager.get_node_from_slot(key_slot(b"foo"))
        slowlog_len = await r.slowlog_len(target_nodes=node)
        assert isinstance(slowlog_len, int)

    async def test_time(self, r: RedisCluster) -> None:
        t = await r.time(target_nodes=r.get_primaries()[0])
        assert len(t) == 2
        assert isinstance(t[0], int)
        assert isinstance(t[1], int)

    @skip_if_server_version_lt("4.0.0")
    @skip_if_redis_enterprise()
    async def test_memory_malloc_stats(self, r: RedisCluster) -> None:
        assert await r.memory_malloc_stats()

    @skip_if_server_version_lt("4.0.0")
    @skip_if_redis_enterprise()
    async def test_memory_stats(self, r: RedisCluster) -> None:
        # put a key into the current db to make sure that "db.<current-db>"
        # has data
        await r.set("foo", "bar")
        node = r.nodes_manager.get_node_from_slot(key_slot(b"foo"))
        stats = await r.memory_stats(target_nodes=node)
        assert isinstance(stats, dict)
        for key, value in stats.items():
            if key.startswith("db."):
                assert not isinstance(value, list)

    @skip_if_server_version_lt("4.0.0")
    async def test_memory_help(self, r: RedisCluster) -> None:
        with pytest.raises(NotImplementedError):
            await r.memory_help()

    @skip_if_server_version_lt("4.0.0")
    async def test_memory_doctor(self, r: RedisCluster) -> None:
        with pytest.raises(NotImplementedError):
            await r.memory_doctor()

    @skip_if_redis_enterprise()
    async def test_lastsave(self, r: RedisCluster) -> None:
        node = r.get_primaries()[0]
        assert isinstance(await r.lastsave(target_nodes=node), datetime.datetime)

    async def test_cluster_echo(self, r: RedisCluster) -> None:
        node = r.get_primaries()[0]
        assert await r.echo("foo bar", target_nodes=node) == b"foo bar"

    @skip_if_server_version_lt("1.0.0")
    async def test_debug_segfault(self, r: RedisCluster) -> None:
        with pytest.raises(NotImplementedError):
            await r.debug_segfault()

    async def test_config_resetstat(self, r: RedisCluster) -> None:
        node = r.get_primaries()[0]
        await r.ping(target_nodes=node)
        prior_commands_processed = int(
            (await r.info(target_nodes=node))["total_commands_processed"]
        )
        assert prior_commands_processed >= 1
        await r.config_resetstat(target_nodes=node)
        reset_commands_processed = int(
            (await r.info(target_nodes=node))["total_commands_processed"]
        )
        assert reset_commands_processed < prior_commands_processed

    @skip_if_server_version_lt("6.2.0")
    async def test_client_trackinginfo(self, r: RedisCluster) -> None:
        node = r.get_primaries()[0]
        res = await r.client_trackinginfo(target_nodes=node)
        assert len(res) > 2
        assert "prefixes" in res or b"prefixes" in res

    @skip_if_server_version_lt("2.9.50")
    async def test_client_pause(self, r: RedisCluster) -> None:
        node = r.get_primaries()[0]
        assert await r.client_pause(1, target_nodes=node)
        assert await r.client_pause(timeout=1, target_nodes=node)
        with pytest.raises(RedisError):
            await r.client_pause(timeout="not an integer", target_nodes=node)

    @skip_if_server_version_lt("6.2.0")
    @skip_if_redis_enterprise()
    async def test_client_unpause(self, r: RedisCluster) -> None:
        assert await r.client_unpause()

    @skip_if_server_version_lt("5.0.0")
    async def test_client_id(self, r: RedisCluster) -> None:
        node = r.get_primaries()[0]
        assert await r.client_id(target_nodes=node) > 0

    @skip_if_server_version_lt("5.0.0")
    async def test_client_unblock(self, r: RedisCluster) -> None:
        node = r.get_primaries()[0]
        myid = await r.client_id(target_nodes=node)
        assert not await r.client_unblock(myid, target_nodes=node)
        assert not await r.client_unblock(myid, error=True, target_nodes=node)
        assert not await r.client_unblock(myid, error=False, target_nodes=node)

    @skip_if_server_version_lt("6.0.0")
    async def test_client_getredir(self, r: RedisCluster) -> None:
        node = r.get_primaries()[0]
        assert isinstance(await r.client_getredir(target_nodes=node), int)
        assert await r.client_getredir(target_nodes=node) == -1

    @skip_if_server_version_lt("6.2.0")
    async def test_client_info(self, r: RedisCluster) -> None:
        node = r.get_primaries()[0]
        info = await r.client_info(target_nodes=node)
        assert isinstance(info, dict)
        assert "addr" in info

    @skip_if_server_version_lt("2.6.9")
    async def test_client_kill(
        self, r: RedisCluster, create_redis: Callable[..., RedisCluster]
    ) -> None:
        node = r.get_primaries()[0]
        r2 = await create_redis(cls=RedisCluster, flushdb=False)
        await r.client_setname("redis-py-c1", target_nodes="all")
        await r2.client_setname("redis-py-c2", target_nodes="all")
        clients = [
            client
            for client in await r.client_list(target_nodes=node)
            if client.get("name") in ["redis-py-c1", "redis-py-c2"]
        ]
        assert len(clients) == 2
        clients_by_name = {client.get("name"): client for client in clients}

        client_addr = clients_by_name["redis-py-c2"].get("addr")
        assert await r.client_kill(client_addr, target_nodes=node) is True

        clients = [
            client
            for client in await r.client_list(target_nodes=node)
            if client.get("name") in ["redis-py-c1", "redis-py-c2"]
        ]
        assert len(clients) == 1
        assert clients[0].get("name") == "redis-py-c1"
        await r2.aclose()

    @skip_if_server_version_lt("2.6.0")
    async def test_cluster_bitop_not_empty_string(self, r: RedisCluster) -> None:
        await r.set("{foo}a", "")
        await r.bitop("not", "{foo}r", "{foo}a")
        assert await r.get("{foo}r") is None

    @skip_if_server_version_lt("2.6.0")
    async def test_cluster_bitop_not(self, r: RedisCluster) -> None:
        test_str = b"\xaa\x00\xff\x55"
        correct = ~0xAA00FF55 & 0xFFFFFFFF
        await r.set("{foo}a", test_str)
        await r.bitop("not", "{foo}r", "{foo}a")
        assert int(binascii.hexlify(await r.get("{foo}r")), 16) == correct

    @skip_if_server_version_lt("2.6.0")
    async def test_cluster_bitop_not_in_place(self, r: RedisCluster) -> None:
        test_str = b"\xaa\x00\xff\x55"
        correct = ~0xAA00FF55 & 0xFFFFFFFF
        await r.set("{foo}a", test_str)
        await r.bitop("not", "{foo}a", "{foo}a")
        assert int(binascii.hexlify(await r.get("{foo}a")), 16) == correct

    @skip_if_server_version_lt("2.6.0")
    async def test_cluster_bitop_single_string(self, r: RedisCluster) -> None:
        test_str = b"\x01\x02\xff"
        await r.set("{foo}a", test_str)
        await r.bitop("and", "{foo}res1", "{foo}a")
        await r.bitop("or", "{foo}res2", "{foo}a")
        await r.bitop("xor", "{foo}res3", "{foo}a")
        assert await r.get("{foo}res1") == test_str
        assert await r.get("{foo}res2") == test_str
        assert await r.get("{foo}res3") == test_str

    @skip_if_server_version_lt("2.6.0")
    async def test_cluster_bitop_string_operands(self, r: RedisCluster) -> None:
        await r.set("{foo}a", b"\x01\x02\xff\xff")
        await r.set("{foo}b", b"\x01\x02\xff")
        await r.bitop("and", "{foo}res1", "{foo}a", "{foo}b")
        await r.bitop("or", "{foo}res2", "{foo}a", "{foo}b")
        await r.bitop("xor", "{foo}res3", "{foo}a", "{foo}b")
        assert int(binascii.hexlify(await r.get("{foo}res1")), 16) == 0x0102FF00
        assert int(binascii.hexlify(await r.get("{foo}res2")), 16) == 0x0102FFFF
        assert int(binascii.hexlify(await r.get("{foo}res3")), 16) == 0x000000FF

    @skip_if_server_version_lt("6.2.0")
    async def test_cluster_copy(self, r: RedisCluster) -> None:
        assert await r.copy("{foo}a", "{foo}b") == 0
        await r.set("{foo}a", "bar")
        assert await r.copy("{foo}a", "{foo}b") == 1
        assert await r.get("{foo}a") == b"bar"
        assert await r.get("{foo}b") == b"bar"

    @skip_if_server_version_lt("6.2.0")
    async def test_cluster_copy_and_replace(self, r: RedisCluster) -> None:
        await r.set("{foo}a", "foo1")
        await r.set("{foo}b", "foo2")
        assert await r.copy("{foo}a", "{foo}b") == 0
        assert await r.copy("{foo}a", "{foo}b", replace=True) == 1

    @skip_if_server_version_lt("6.2.0")
    async def test_cluster_lmove(self, r: RedisCluster) -> None:
        await r.rpush("{foo}a", "one", "two", "three", "four")
        assert await r.lmove("{foo}a", "{foo}b")
        assert await r.lmove("{foo}a", "{foo}b", "right", "left")

    @skip_if_server_version_lt("6.2.0")
    async def test_cluster_blmove(self, r: RedisCluster) -> None:
        await r.rpush("{foo}a", "one", "two", "three", "four")
        assert await r.blmove("{foo}a", "{foo}b", 5)
        assert await r.blmove("{foo}a", "{foo}b", 1, "RIGHT", "LEFT")

    @skip_if_server_version_lt("8.9.0")
    async def test_cluster_lmovem(self, r: RedisCluster) -> None:
        # source and destination collocated on the same slot via a hash tag
        await r.rpush("{foo}a", "1", "2", "3", "4", "5")
        assert await r.lmovem("{foo}a", "{foo}b") == [b"1"]
        assert await r.lmovem(
            "{foo}a", "{foo}b", "LEFT", "LEFT", count=3, ordering="BULK"
        ) == [b"2", b"3", b"4"]

    @skip_if_server_version_lt("8.9.0")
    async def test_cluster_lmovem_crossslot(self, r: RedisCluster) -> None:
        # source and destination on different slots -> cannot be dispatched
        with pytest.raises(RedisClusterException) as ex:
            await r.lmovem("a", "b", "LEFT", "RIGHT", count=2, ordering="BULK")
        assert "all keys must map to the same key slot" in str(ex.value)

    @skip_if_server_version_lt("8.9.0")
    async def test_cluster_blmovem(self, r: RedisCluster) -> None:
        await r.rpush("{foo}a", "1", "2", "3", "4", "5")
        assert await r.blmovem("{foo}a", "{foo}b", 1) == [b"1"]
        assert await r.blmovem(
            "{foo}a", "{foo}b", 1, "LEFT", "LEFT", count=3, ordering="BULK"
        ) == [b"2", b"3", b"4"]

    @skip_if_server_version_lt("8.9.0")
    async def test_cluster_blmovem_crossslot(self, r: RedisCluster) -> None:
        with pytest.raises(RedisClusterException) as ex:
            await r.blmovem("a", "b", 1, "LEFT", "RIGHT", count=2, ordering="BULK")
        assert "all keys must map to the same key slot" in str(ex.value)

    async def test_cluster_msetnx(self, r: RedisCluster) -> None:
        d = {"{foo}a": b"1", "{foo}b": b"2", "{foo}c": b"3"}
        assert await r.msetnx(d)
        d2 = {"{foo}a": b"x", "{foo}d": b"4"}
        assert not await r.msetnx(d2)
        for k, v in d.items():
            assert await r.get(k) == v
        assert await r.get("{foo}d") is None

    async def test_cluster_rename(self, r: RedisCluster) -> None:
        await r.set("{foo}a", "1")
        assert await r.rename("{foo}a", "{foo}b")
        assert await r.get("{foo}a") is None
        assert await r.get("{foo}b") == b"1"

    async def test_cluster_renamenx(self, r: RedisCluster) -> None:
        await r.set("{foo}a", "1")
        await r.set("{foo}b", "2")
        assert not await r.renamenx("{foo}a", "{foo}b")
        assert await r.get("{foo}a") == b"1"
        assert await r.get("{foo}b") == b"2"

    # LIST COMMANDS
    async def test_cluster_blpop(self, r: RedisCluster) -> None:
        await r.rpush("{foo}a", "1", "2")
        await r.rpush("{foo}b", "3", "4")
        assert_resp_response(
            r,
            await r.blpop(["{foo}b", "{foo}a"], timeout=1),
            (b"{foo}b", b"3"),
            [b"{foo}b", b"3"],
        )
        assert_resp_response(
            r,
            await r.blpop(["{foo}b", "{foo}a"], timeout=1),
            (b"{foo}b", b"4"),
            [b"{foo}b", b"4"],
        )
        assert_resp_response(
            r,
            await r.blpop(["{foo}b", "{foo}a"], timeout=1),
            (b"{foo}a", b"1"),
            [b"{foo}a", b"1"],
        )
        assert_resp_response(
            r,
            await r.blpop(["{foo}b", "{foo}a"], timeout=1),
            (b"{foo}a", b"2"),
            [b"{foo}a", b"2"],
        )
        assert await r.blpop(["{foo}b", "{foo}a"], timeout=0.1) is None
        await r.rpush("{foo}c", "1")
        assert_resp_response(
            r, await r.blpop("{foo}c", timeout=1), (b"{foo}c", b"1"), [b"{foo}c", b"1"]
        )

    async def test_cluster_brpop(self, r: RedisCluster) -> None:
        await r.rpush("{foo}a", "1", "2")
        await r.rpush("{foo}b", "3", "4")
        assert_resp_response(
            r,
            await r.brpop(["{foo}b", "{foo}a"], timeout=1),
            (b"{foo}b", b"4"),
            [b"{foo}b", b"4"],
        )
        assert_resp_response(
            r,
            await r.brpop(["{foo}b", "{foo}a"], timeout=1),
            (b"{foo}b", b"3"),
            [b"{foo}b", b"3"],
        )
        assert_resp_response(
            r,
            await r.brpop(["{foo}b", "{foo}a"], timeout=1),
            (b"{foo}a", b"2"),
            [b"{foo}a", b"2"],
        )
        assert_resp_response(
            r,
            await r.brpop(["{foo}b", "{foo}a"], timeout=1),
            (b"{foo}a", b"1"),
            [b"{foo}a", b"1"],
        )
        assert await r.brpop(["{foo}b", "{foo}a"], timeout=0.1) is None
        await r.rpush("{foo}c", "1")
        assert_resp_response(
            r, await r.brpop("{foo}c", timeout=1), (b"{foo}c", b"1"), [b"{foo}c", b"1"]
        )

    async def test_cluster_brpoplpush(self, r: RedisCluster) -> None:
        await r.rpush("{foo}a", "1", "2")
        await r.rpush("{foo}b", "3", "4")
        assert await r.brpoplpush("{foo}a", "{foo}b") == b"2"
        assert await r.brpoplpush("{foo}a", "{foo}b") == b"1"
        assert await r.brpoplpush("{foo}a", "{foo}b", timeout=0.1) is None
        assert await r.lrange("{foo}a", 0, -1) == []
        assert await r.lrange("{foo}b", 0, -1) == [b"1", b"2", b"3", b"4"]

    async def test_cluster_brpoplpush_empty_string(self, r: RedisCluster) -> None:
        await r.rpush("{foo}a", "")
        assert await r.brpoplpush("{foo}a", "{foo}b") == b""

    async def test_cluster_rpoplpush(self, r: RedisCluster) -> None:
        await r.rpush("{foo}a", "a1", "a2", "a3")
        await r.rpush("{foo}b", "b1", "b2", "b3")
        assert await r.rpoplpush("{foo}a", "{foo}b") == b"a3"
        assert await r.lrange("{foo}a", 0, -1) == [b"a1", b"a2"]
        assert await r.lrange("{foo}b", 0, -1) == [b"a3", b"b1", b"b2", b"b3"]

    async def test_cluster_sdiff(self, r: RedisCluster) -> None:
        await r.sadd("{foo}a", "1", "2", "3")
        assert await r.sdiff("{foo}a", "{foo}b") == {b"1", b"2", b"3"}
        await r.sadd("{foo}b", "2", "3")
        assert await r.sdiff("{foo}a", "{foo}b") == {b"1"}

    async def test_cluster_sdiffstore(self, r: RedisCluster) -> None:
        await r.sadd("{foo}a", "1", "2", "3")
        assert await r.sdiffstore("{foo}c", "{foo}a", "{foo}b") == 3
        assert await r.smembers("{foo}c") == {b"1", b"2", b"3"}
        await r.sadd("{foo}b", "2", "3")
        assert await r.sdiffstore("{foo}c", "{foo}a", "{foo}b") == 1
        assert await r.smembers("{foo}c") == {b"1"}

    async def test_cluster_sinter(self, r: RedisCluster) -> None:
        await r.sadd("{foo}a", "1", "2", "3")
        assert await r.sinter("{foo}a", "{foo}b") == set()
        await r.sadd("{foo}b", "2", "3")
        assert await r.sinter("{foo}a", "{foo}b") == {b"2", b"3"}

    async def test_cluster_sinterstore(self, r: RedisCluster) -> None:
        await r.sadd("{foo}a", "1", "2", "3")
        assert await r.sinterstore("{foo}c", "{foo}a", "{foo}b") == 0
        assert await r.smembers("{foo}c") == set()
        await r.sadd("{foo}b", "2", "3")
        assert await r.sinterstore("{foo}c", "{foo}a", "{foo}b") == 2
        assert await r.smembers("{foo}c") == {b"2", b"3"}

    async def test_cluster_smove(self, r: RedisCluster) -> None:
        await r.sadd("{foo}a", "a1", "a2")
        await r.sadd("{foo}b", "b1", "b2")
        assert await r.smove("{foo}a", "{foo}b", "a1")
        assert await r.smembers("{foo}a") == {b"a2"}
        assert await r.smembers("{foo}b") == {b"b1", b"b2", b"a1"}

    async def test_cluster_sunion(self, r: RedisCluster) -> None:
        await r.sadd("{foo}a", "1", "2")
        await r.sadd("{foo}b", "2", "3")
        assert set(await r.sunion("{foo}a", "{foo}b")) == {b"1", b"2", b"3"}

    async def test_cluster_sunionstore(self, r: RedisCluster) -> None:
        await r.sadd("{foo}a", "1", "2")
        await r.sadd("{foo}b", "2", "3")
        assert await r.sunionstore("{foo}c", "{foo}a", "{foo}b") == 3
        assert set(await r.smembers("{foo}c")) == {b"1", b"2", b"3"}

    @skip_if_server_version_lt("6.2.0")
    async def test_cluster_zdiff(self, r: RedisCluster) -> None:
        await r.zadd("{foo}a", {"a1": 1, "a2": 2, "a3": 3})
        await r.zadd("{foo}b", {"a1": 1, "a2": 2})
        assert await r.zdiff(["{foo}a", "{foo}b"]) == [b"a3"]
        response = await r.zdiff(["{foo}a", "{foo}b"], withscores=True)
        assert_resp_response(r, response, [b"a3", b"3"], [[b"a3", 3.0]])

    @skip_if_server_version_lt("6.2.0")
    async def test_cluster_zdiffstore(self, r: RedisCluster) -> None:
        await r.zadd("{foo}a", {"a1": 1, "a2": 2, "a3": 3})
        await r.zadd("{foo}b", {"a1": 1, "a2": 2})
        assert await r.zdiffstore("{foo}out", ["{foo}a", "{foo}b"])
        assert await r.zrange("{foo}out", 0, -1) == [b"a3"]
        response = await r.zrange("{foo}out", 0, -1, withscores=True)
        assert_resp_response(r, response, [(b"a3", 3.0)], [[b"a3", 3.0]])

    @skip_if_server_version_lt("6.2.0")
    async def test_cluster_zinter(self, r: RedisCluster) -> None:
        await r.zadd("{foo}a", {"a1": 1, "a2": 2, "a3": 1})
        await r.zadd("{foo}b", {"a1": 2, "a2": 2, "a3": 2})
        await r.zadd("{foo}c", {"a1": 6, "a3": 5, "a4": 4})
        assert await r.zinter(["{foo}a", "{foo}b", "{foo}c"]) == [b"a3", b"a1"]
        # invalid aggregation
        with pytest.raises(DataError):
            await r.zinter(
                ["{foo}a", "{foo}b", "{foo}c"], aggregate="foo", withscores=True
            )
        # aggregate with SUM
        response = await r.zinter(["{foo}a", "{foo}b", "{foo}c"], withscores=True)
        assert_resp_response(
            r, response, [(b"a3", 8), (b"a1", 9)], [[b"a3", 8], [b"a1", 9]]
        )
        # aggregate with MAX
        response = await r.zinter(
            ["{foo}a", "{foo}b", "{foo}c"], aggregate="MAX", withscores=True
        )
        assert_resp_response(
            r, response, [(b"a3", 5), (b"a1", 6)], [[b"a3", 5], [b"a1", 6]]
        )
        # aggregate with MIN
        response = await r.zinter(
            ["{foo}a", "{foo}b", "{foo}c"], aggregate="MIN", withscores=True
        )
        assert_resp_response(
            r, response, [(b"a1", 1), (b"a3", 1)], [[b"a1", 1], [b"a3", 1]]
        )
        # with weights
        res = await r.zinter({"{foo}a": 1, "{foo}b": 2, "{foo}c": 3}, withscores=True)
        assert_resp_response(
            r, res, [(b"a3", 20), (b"a1", 23)], [[b"a3", 20], [b"a1", 23]]
        )

    async def test_cluster_zinterstore_sum(self, r: RedisCluster) -> None:
        await r.zadd("{foo}a", {"a1": 1, "a2": 1, "a3": 1})
        await r.zadd("{foo}b", {"a1": 2, "a2": 2, "a3": 2})
        await r.zadd("{foo}c", {"a1": 6, "a3": 5, "a4": 4})
        assert await r.zinterstore("{foo}d", ["{foo}a", "{foo}b", "{foo}c"]) == 2
        assert_resp_response(
            r,
            await r.zrange("{foo}d", 0, -1, withscores=True),
            [(b"a3", 8), (b"a1", 9)],
            [[b"a3", 8.0], [b"a1", 9.0]],
        )

    async def test_cluster_zinterstore_max(self, r: RedisCluster) -> None:
        await r.zadd("{foo}a", {"a1": 1, "a2": 1, "a3": 1})
        await r.zadd("{foo}b", {"a1": 2, "a2": 2, "a3": 2})
        await r.zadd("{foo}c", {"a1": 6, "a3": 5, "a4": 4})
        assert (
            await r.zinterstore(
                "{foo}d", ["{foo}a", "{foo}b", "{foo}c"], aggregate="MAX"
            )
            == 2
        )
        assert_resp_response(
            r,
            await r.zrange("{foo}d", 0, -1, withscores=True),
            [(b"a3", 5), (b"a1", 6)],
            [[b"a3", 5.0], [b"a1", 6.0]],
        )

    async def test_cluster_zinterstore_min(self, r: RedisCluster) -> None:
        await r.zadd("{foo}a", {"a1": 1, "a2": 2, "a3": 3})
        await r.zadd("{foo}b", {"a1": 2, "a2": 3, "a3": 5})
        await r.zadd("{foo}c", {"a1": 6, "a3": 5, "a4": 4})
        assert (
            await r.zinterstore(
                "{foo}d", ["{foo}a", "{foo}b", "{foo}c"], aggregate="MIN"
            )
            == 2
        )
        assert_resp_response(
            r,
            await r.zrange("{foo}d", 0, -1, withscores=True),
            [(b"a1", 1), (b"a3", 3)],
            [[b"a1", 1.0], [b"a3", 3.0]],
        )

    async def test_cluster_zinterstore_with_weight(self, r: RedisCluster) -> None:
        await r.zadd("{foo}a", {"a1": 1, "a2": 1, "a3": 1})
        await r.zadd("{foo}b", {"a1": 2, "a2": 2, "a3": 2})
        await r.zadd("{foo}c", {"a1": 6, "a3": 5, "a4": 4})
        assert (
            await r.zinterstore("{foo}d", {"{foo}a": 1, "{foo}b": 2, "{foo}c": 3}) == 2
        )
        assert_resp_response(
            r,
            await r.zrange("{foo}d", 0, -1, withscores=True),
            [(b"a3", 20), (b"a1", 23)],
            [[b"a3", 20.0], [b"a1", 23.0]],
        )

    @skip_if_server_version_lt("8.7.0")
    async def test_cluster_zinterstore_count(self, r: RedisCluster) -> None:
        await r.zadd("{foo}a", {"a1": 1, "a2": 1, "a3": 1})
        await r.zadd("{foo}b", {"a1": 2, "a2": 2, "a3": 2})
        await r.zadd("{foo}c", {"a1": 6, "a3": 5, "a4": 4})
        assert (
            await r.zinterstore(
                "{foo}d", ["{foo}a", "{foo}b", "{foo}c"], aggregate="COUNT"
            )
            == 2
        )
        assert_resp_response(
            r,
            await r.zrange("{foo}d", 0, -1, withscores=True),
            [(b"a1", 3.0), (b"a3", 3.0)],
            [[b"a1", 3.0], [b"a3", 3.0]],
        )

    @skip_if_server_version_lt("8.7.0")
    async def test_cluster_zinterstore_count_with_weight(self, r: RedisCluster) -> None:
        await r.zadd("{foo}a", {"a1": 1, "a2": 1, "a3": 1})
        await r.zadd("{foo}b", {"a1": 2, "a2": 2, "a3": 2})
        await r.zadd("{foo}c", {"a1": 6, "a3": 5, "a4": 4})
        assert (
            await r.zinterstore(
                "{foo}d",
                {"{foo}a": 1, "{foo}b": 2, "{foo}c": 3},
                aggregate="COUNT",
            )
            == 2
        )
        assert_resp_response(
            r,
            await r.zrange("{foo}d", 0, -1, withscores=True),
            [(b"a1", 6.0), (b"a3", 6.0)],
            [[b"a1", 6.0], [b"a3", 6.0]],
        )

    @skip_if_server_version_lt("4.9.0")
    async def test_cluster_bzpopmax(self, r: RedisCluster) -> None:
        await r.zadd("{foo}a", {"a1": 1, "a2": 2})
        await r.zadd("{foo}b", {"b1": 10, "b2": 20})
        assert_resp_response(
            r,
            await r.bzpopmax(["{foo}b", "{foo}a"], timeout=1),
            (b"{foo}b", b"b2", 20),
            [b"{foo}b", b"b2", 20],
        )
        assert_resp_response(
            r,
            await r.bzpopmax(["{foo}b", "{foo}a"], timeout=1),
            (b"{foo}b", b"b1", 10),
            [b"{foo}b", b"b1", 10],
        )
        assert_resp_response(
            r,
            await r.bzpopmax(["{foo}b", "{foo}a"], timeout=1),
            (b"{foo}a", b"a2", 2),
            [b"{foo}a", b"a2", 2],
        )
        assert_resp_response(
            r,
            await r.bzpopmax(["{foo}b", "{foo}a"], timeout=1),
            (b"{foo}a", b"a1", 1),
            [b"{foo}a", b"a1", 1],
        )
        assert await r.bzpopmax(["{foo}b", "{foo}a"], timeout=0.1) is None
        await r.zadd("{foo}c", {"c1": 100})
        assert_resp_response(
            r,
            await r.bzpopmax("{foo}c", timeout=1),
            (b"{foo}c", b"c1", 100),
            [b"{foo}c", b"c1", 100],
        )

    @skip_if_server_version_lt("4.9.0")
    async def test_cluster_bzpopmin(self, r: RedisCluster) -> None:
        await r.zadd("{foo}a", {"a1": 1, "a2": 2})
        await r.zadd("{foo}b", {"b1": 10, "b2": 20})
        assert_resp_response(
            r,
            await r.bzpopmin(["{foo}b", "{foo}a"], timeout=1),
            (b"{foo}b", b"b1", 10),
            [b"{foo}b", b"b1", 10],
        )
        assert_resp_response(
            r,
            await r.bzpopmin(["{foo}b", "{foo}a"], timeout=1),
            (b"{foo}b", b"b2", 20),
            [b"{foo}b", b"b2", 20],
        )
        assert_resp_response(
            r,
            await r.bzpopmin(["{foo}b", "{foo}a"], timeout=1),
            (b"{foo}a", b"a1", 1),
            [b"{foo}a", b"a1", 1],
        )
        assert_resp_response(
            r,
            await r.bzpopmin(["{foo}b", "{foo}a"], timeout=1),
            (b"{foo}a", b"a2", 2),
            [b"{foo}a", b"a2", 2],
        )
        assert await r.bzpopmin(["{foo}b", "{foo}a"], timeout=0.1) is None
        await r.zadd("{foo}c", {"c1": 100})
        assert_resp_response(
            r,
            await r.bzpopmin("{foo}c", timeout=1),
            (b"{foo}c", b"c1", 100),
            [b"{foo}c", b"c1", 100],
        )

    @skip_if_server_version_lt("6.2.0")
    async def test_cluster_zrangestore(self, r: RedisCluster) -> None:
        await r.zadd("{foo}a", {"a1": 1, "a2": 2, "a3": 3})
        assert await r.zrangestore("{foo}b", "{foo}a", 0, 1)
        assert await r.zrange("{foo}b", 0, -1) == [b"a1", b"a2"]
        assert await r.zrangestore("{foo}b", "{foo}a", 1, 2)
        assert await r.zrange("{foo}b", 0, -1) == [b"a2", b"a3"]
        assert_resp_response(
            r,
            await r.zrange("{foo}b", 0, -1, withscores=True),
            [(b"a2", 2), (b"a3", 3)],
            [[b"a2", 2.0], [b"a3", 3.0]],
        )
        # reversed order
        assert await r.zrangestore("{foo}b", "{foo}a", 1, 2, desc=True)
        assert await r.zrange("{foo}b", 0, -1) == [b"a1", b"a2"]
        # by score
        assert await r.zrangestore(
            "{foo}b", "{foo}a", 2, 1, byscore=True, offset=0, num=1, desc=True
        )
        assert await r.zrange("{foo}b", 0, -1) == [b"a2"]
        # by lex
        assert await r.zrangestore(
            "{foo}b", "{foo}a", "[a2", "(a3", bylex=True, offset=0, num=1
        )
        assert await r.zrange("{foo}b", 0, -1) == [b"a2"]

    @skip_if_server_version_lt("6.2.0")
    async def test_cluster_zunion(self, r: RedisCluster) -> None:
        await r.zadd("{foo}a", {"a1": 1, "a2": 1, "a3": 1})
        await r.zadd("{foo}b", {"a1": 2, "a2": 2, "a3": 2})
        await r.zadd("{foo}c", {"a1": 6, "a3": 5, "a4": 4})
        # sum
        assert await r.zunion(["{foo}a", "{foo}b", "{foo}c"]) == [
            b"a2",
            b"a4",
            b"a3",
            b"a1",
        ]
        assert_resp_response(
            r,
            await r.zunion(["{foo}a", "{foo}b", "{foo}c"], withscores=True),
            [(b"a2", 3), (b"a4", 4), (b"a3", 8), (b"a1", 9)],
            [[b"a2", 3.0], [b"a4", 4.0], [b"a3", 8.0], [b"a1", 9.0]],
        )
        # max
        assert_resp_response(
            r,
            await r.zunion(
                ["{foo}a", "{foo}b", "{foo}c"], aggregate="MAX", withscores=True
            ),
            [(b"a2", 2), (b"a4", 4), (b"a3", 5), (b"a1", 6)],
            [[b"a2", 2.0], [b"a4", 4.0], [b"a3", 5.0], [b"a1", 6.0]],
        )
        # min
        assert_resp_response(
            r,
            await r.zunion(
                ["{foo}a", "{foo}b", "{foo}c"], aggregate="MIN", withscores=True
            ),
            [(b"a1", 1), (b"a2", 1), (b"a3", 1), (b"a4", 4)],
            [[b"a1", 1.0], [b"a2", 1.0], [b"a3", 1.0], [b"a4", 4.0]],
        )
        # with weight
        assert_resp_response(
            r,
            await r.zunion({"{foo}a": 1, "{foo}b": 2, "{foo}c": 3}, withscores=True),
            [(b"a2", 5), (b"a4", 12), (b"a3", 20), (b"a1", 23)],
            [[b"a2", 5.0], [b"a4", 12.0], [b"a3", 20.0], [b"a1", 23.0]],
        )

    async def test_cluster_zunionstore_sum(self, r: RedisCluster) -> None:
        await r.zadd("{foo}a", {"a1": 1, "a2": 1, "a3": 1})
        await r.zadd("{foo}b", {"a1": 2, "a2": 2, "a3": 2})
        await r.zadd("{foo}c", {"a1": 6, "a3": 5, "a4": 4})
        assert await r.zunionstore("{foo}d", ["{foo}a", "{foo}b", "{foo}c"]) == 4
        assert_resp_response(
            r,
            await r.zrange("{foo}d", 0, -1, withscores=True),
            [(b"a2", 3), (b"a4", 4), (b"a3", 8), (b"a1", 9)],
            [[b"a2", 3.0], [b"a4", 4.0], [b"a3", 8.0], [b"a1", 9.0]],
        )

    async def test_cluster_zunionstore_max(self, r: RedisCluster) -> None:
        await r.zadd("{foo}a", {"a1": 1, "a2": 1, "a3": 1})
        await r.zadd("{foo}b", {"a1": 2, "a2": 2, "a3": 2})
        await r.zadd("{foo}c", {"a1": 6, "a3": 5, "a4": 4})
        assert (
            await r.zunionstore(
                "{foo}d", ["{foo}a", "{foo}b", "{foo}c"], aggregate="MAX"
            )
            == 4
        )
        assert_resp_response(
            r,
            await r.zrange("{foo}d", 0, -1, withscores=True),
            [(b"a2", 2), (b"a4", 4), (b"a3", 5), (b"a1", 6)],
            [[b"a2", 2.0], [b"a4", 4.0], [b"a3", 5.0], [b"a1", 6.0]],
        )

    async def test_cluster_zunionstore_min(self, r: RedisCluster) -> None:
        await r.zadd("{foo}a", {"a1": 1, "a2": 2, "a3": 3})
        await r.zadd("{foo}b", {"a1": 2, "a2": 2, "a3": 4})
        await r.zadd("{foo}c", {"a1": 6, "a3": 5, "a4": 4})
        assert (
            await r.zunionstore(
                "{foo}d", ["{foo}a", "{foo}b", "{foo}c"], aggregate="MIN"
            )
            == 4
        )
        assert_resp_response(
            r,
            await r.zrange("{foo}d", 0, -1, withscores=True),
            [(b"a1", 1), (b"a2", 2), (b"a3", 3), (b"a4", 4)],
            [[b"a1", 1.0], [b"a2", 2.0], [b"a3", 3.0], [b"a4", 4.0]],
        )

    async def test_cluster_zunionstore_with_weight(self, r: RedisCluster) -> None:
        await r.zadd("{foo}a", {"a1": 1, "a2": 1, "a3": 1})
        await r.zadd("{foo}b", {"a1": 2, "a2": 2, "a3": 2})
        await r.zadd("{foo}c", {"a1": 6, "a3": 5, "a4": 4})
        assert (
            await r.zunionstore("{foo}d", {"{foo}a": 1, "{foo}b": 2, "{foo}c": 3}) == 4
        )
        assert_resp_response(
            r,
            await r.zrange("{foo}d", 0, -1, withscores=True),
            [(b"a2", 5), (b"a4", 12), (b"a3", 20), (b"a1", 23)],
            [[b"a2", 5.0], [b"a4", 12.0], [b"a3", 20.0], [b"a1", 23.0]],
        )

    @skip_if_server_version_lt("8.7.0")
    async def test_cluster_zunionstore_count(self, r: RedisCluster) -> None:
        await r.zadd("{foo}a", {"a1": 1, "a2": 1, "a3": 1})
        await r.zadd("{foo}b", {"a1": 2, "a2": 2, "a3": 2})
        await r.zadd("{foo}c", {"a1": 6, "a3": 5, "a4": 4})
        assert (
            await r.zunionstore(
                "{foo}d", ["{foo}a", "{foo}b", "{foo}c"], aggregate="COUNT"
            )
            == 4
        )
        assert_resp_response(
            r,
            await r.zrange("{foo}d", 0, -1, withscores=True),
            [(b"a4", 1.0), (b"a2", 2.0), (b"a1", 3.0), (b"a3", 3.0)],
            [[b"a4", 1.0], [b"a2", 2.0], [b"a1", 3.0], [b"a3", 3.0]],
        )

    @skip_if_server_version_lt("8.7.0")
    async def test_cluster_zunionstore_count_with_weight(self, r: RedisCluster) -> None:
        await r.zadd("{foo}a", {"a1": 1, "a2": 1, "a3": 1})
        await r.zadd("{foo}b", {"a1": 2, "a2": 2, "a3": 2})
        await r.zadd("{foo}c", {"a1": 6, "a3": 5, "a4": 4})
        assert (
            await r.zunionstore(
                "{foo}d",
                {"{foo}a": 1, "{foo}b": 2, "{foo}c": 3},
                aggregate="COUNT",
            )
            == 4
        )
        assert_resp_response(
            r,
            await r.zrange("{foo}d", 0, -1, withscores=True),
            [(b"a2", 3.0), (b"a4", 3.0), (b"a1", 6.0), (b"a3", 6.0)],
            [[b"a2", 3.0], [b"a4", 3.0], [b"a1", 6.0], [b"a3", 6.0]],
        )

    @skip_if_server_version_lt("2.8.9")
    async def test_cluster_pfcount(self, r: RedisCluster) -> None:
        members = {b"1", b"2", b"3"}
        await r.pfadd("{foo}a", *members)
        assert await r.pfcount("{foo}a") == len(members)
        members_b = {b"2", b"3", b"4"}
        await r.pfadd("{foo}b", *members_b)
        assert await r.pfcount("{foo}b") == len(members_b)
        assert await r.pfcount("{foo}a", "{foo}b") == len(members_b.union(members))

    @skip_if_server_version_lt("2.8.9")
    async def test_cluster_pfmerge(self, r: RedisCluster) -> None:
        mema = {b"1", b"2", b"3"}
        memb = {b"2", b"3", b"4"}
        memc = {b"5", b"6", b"7"}
        await r.pfadd("{foo}a", *mema)
        await r.pfadd("{foo}b", *memb)
        await r.pfadd("{foo}c", *memc)
        await r.pfmerge("{foo}d", "{foo}c", "{foo}a")
        assert await r.pfcount("{foo}d") == 6
        await r.pfmerge("{foo}d", "{foo}b")
        assert await r.pfcount("{foo}d") == 7

    async def test_cluster_sort_store(self, r: RedisCluster) -> None:
        await r.rpush("{foo}a", "2", "3", "1")
        assert await r.sort("{foo}a", store="{foo}sorted_values") == 3
        assert await r.lrange("{foo}sorted_values", 0, -1) == [b"1", b"2", b"3"]

    # GEO COMMANDS
    @skip_if_server_version_lt("6.2.0")
    async def test_cluster_geosearchstore(self, r: RedisCluster) -> None:
        values = (2.1909389952632, 41.433791470673, "place1") + (
            2.1873744593677,
            41.406342043777,
            "place2",
        )

        await r.geoadd("{foo}barcelona", values)
        await r.geosearchstore(
            "{foo}places_barcelona",
            "{foo}barcelona",
            longitude=2.191,
            latitude=41.433,
            radius=1000,
        )
        assert await r.zrange("{foo}places_barcelona", 0, -1) == [b"place1"]

    @skip_unless_arch_bits(64)
    @skip_if_server_version_lt("6.2.0")
    async def test_geosearchstore_dist(self, r: RedisCluster) -> None:
        values = (2.1909389952632, 41.433791470673, "place1") + (
            2.1873744593677,
            41.406342043777,
            "place2",
        )

        await r.geoadd("{foo}barcelona", values)
        await r.geosearchstore(
            "{foo}places_barcelona",
            "{foo}barcelona",
            longitude=2.191,
            latitude=41.433,
            radius=1000,
            storedist=True,
        )
        # instead of save the geo score, the distance is saved.
        assert await r.zscore("{foo}places_barcelona", "place1") == 88.05060698409301

    @skip_if_server_version_lt("3.2.0")
    async def test_cluster_georadius_store(self, r: RedisCluster) -> None:
        values = (2.1909389952632, 41.433791470673, "place1") + (
            2.1873744593677,
            41.406342043777,
            "place2",
        )

        await r.geoadd("{foo}barcelona", values)
        await r.georadius(
            "{foo}barcelona", 2.191, 41.433, 1000, store="{foo}places_barcelona"
        )
        assert await r.zrange("{foo}places_barcelona", 0, -1) == [b"place1"]

    @skip_unless_arch_bits(64)
    @skip_if_server_version_lt("3.2.0")
    async def test_cluster_georadius_store_dist(self, r: RedisCluster) -> None:
        values = (2.1909389952632, 41.433791470673, "place1") + (
            2.1873744593677,
            41.406342043777,
            "place2",
        )

        await r.geoadd("{foo}barcelona", values)
        await r.georadius(
            "{foo}barcelona", 2.191, 41.433, 1000, store_dist="{foo}places_barcelona"
        )
        # instead of save the geo score, the distance is saved.
        assert await r.zscore("{foo}places_barcelona", "place1") == 88.05060698409301

    async def test_cluster_dbsize(self, r: RedisCluster) -> None:
        d = {"a": b"1", "b": b"2", "c": b"3", "d": b"4"}
        assert await r.mset_nonatomic(d)
        assert await r.dbsize(target_nodes="primaries") == len(d)

    async def test_cluster_keys(self, r: RedisCluster) -> None:
        assert await r.keys() == []
        keys_with_underscores = {b"test_a", b"test_b"}
        keys = keys_with_underscores.union({b"testc"})
        for key in keys:
            await r.set(key, 1)
        assert (
            set(await r.keys(pattern="test_*", target_nodes="primaries"))
            == keys_with_underscores
        )
        assert set(await r.keys(pattern="test*", target_nodes="primaries")) == keys

    # SCAN COMMANDS
    @skip_if_server_version_lt("2.8.0")
    async def test_cluster_scan(self, r: RedisCluster) -> None:
        await r.set("a", 1)
        await r.set("b", 2)
        await r.set("c", 3)

        for target_nodes, nodes in zip(
            ["primaries", "replicas"], [r.get_primaries(), r.get_replicas()]
        ):
            cursors, keys = await r.scan(target_nodes=target_nodes)
            assert sorted(keys) == [b"a", b"b", b"c"]
            assert sorted(cursors.keys()) == sorted(node.name for node in nodes)
            assert all(cursor == 0 for cursor in cursors.values())

            cursors, keys = await r.scan(match="a*", target_nodes=target_nodes)
            assert sorted(keys) == [b"a"]
            assert sorted(cursors.keys()) == sorted(node.name for node in nodes)
            assert all(cursor == 0 for cursor in cursors.values())

    @skip_if_server_version_lt("6.0.0")
    async def test_cluster_scan_type(self, r: RedisCluster) -> None:
        await r.sadd("a-set", 1)
        await r.sadd("b-set", 1)
        await r.sadd("c-set", 1)
        await r.hset("a-hash", "foo", 2)
        await r.lpush("a-list", "aux", 3)

        for target_nodes, nodes in zip(
            ["primaries", "replicas"], [r.get_primaries(), r.get_replicas()]
        ):
            cursors, keys = await r.scan(_type="SET", target_nodes=target_nodes)
            assert sorted(keys) == [b"a-set", b"b-set", b"c-set"]
            assert sorted(cursors.keys()) == sorted(node.name for node in nodes)
            assert all(cursor == 0 for cursor in cursors.values())

            cursors, keys = await r.scan(
                _type="SET", match="a*", target_nodes=target_nodes
            )
            assert sorted(keys) == [b"a-set"]
            assert sorted(cursors.keys()) == sorted(node.name for node in nodes)
            assert all(cursor == 0 for cursor in cursors.values())

    @skip_if_server_version_lt("2.8.0")
    async def test_cluster_scan_iter(self, r: RedisCluster) -> None:
        keys_all = []
        keys_1 = []
        # one MSET per slot, pipelined per node, instead of 100 SETs
        await r.mset_nonatomic({str(i): 1 for i in range(100)})
        for i in range(100):
            s = str(i)
            keys_all.append(s.encode("utf-8"))
            if s.startswith("1"):
                keys_1.append(s.encode("utf-8"))
        keys_all.sort()
        keys_1.sort()

        # the replicas are scanned below; poll until they have every key
        deadline = asyncio.get_running_loop().time() + 5
        while (
            sorted([k async for k in r.scan_iter(target_nodes="replicas")]) != keys_all
        ):
            assert asyncio.get_running_loop().time() < deadline, "replicas lag behind"
            await asyncio.sleep(0.01)

        for target_nodes in ["primaries", "replicas"]:
            keys = [key async for key in r.scan_iter(target_nodes=target_nodes)]
            assert sorted(keys) == keys_all

            keys = [
                key async for key in r.scan_iter(match="1*", target_nodes=target_nodes)
            ]
            assert sorted(keys) == keys_1

    @skip_if_server_version_lt("8.11.0")
    async def test_cluster_bless_scan(self, r: RedisCluster) -> None:
        await r.set("a", 1)
        await r.set("b", 2)
        await r.set("c", 3)
        assert await r.bless_set("a", "NO-EVICT") == 1
        assert await r.bless_set("b", "NO-EVICT") == 1
        assert await r.bless_get("a") == [b"NO-EVICT"]
        assert await r.bless_get("c") == []

        primaries = sorted(node.name for node in r.get_primaries())
        for kwargs in ({}, {"target_nodes": "primaries"}):
            cursors, keys = await r.bless_scan(0, "NO-EVICT", **kwargs)
            assert sorted(keys) == [b"a", b"b"]
            assert sorted(cursors.keys()) == primaries
            assert all(cursor == 0 for cursor in cursors.values())

        assert await r.bless_clear("a", "NO-EVICT") == 1
        cursors, keys = await r.bless_scan(0, "NO-EVICT")
        assert keys == [b"b"]
        assert sorted(cursors.keys()) == primaries

    @skip_if_server_version_lt("8.11.0")
    async def test_cluster_bless_scan_iter(self, r: RedisCluster) -> None:
        keys_blessed = []
        await r.mset_nonatomic({str(i): 1 for i in range(100)})
        for i in range(100):
            s = str(i)
            if s.startswith("1"):
                await r.bless_set(s, "NO-EVICT")
                keys_blessed.append(s.encode("utf-8"))
        keys_blessed.sort()

        assert sorted([k async for k in r.bless_scan_iter("NO-EVICT")]) == keys_blessed
        # count=1 forces every primary through more than one cursor round
        assert (
            sorted([k async for k in r.bless_scan_iter("NO-EVICT", count=1)])
            == keys_blessed
        )
        assert (
            sorted(
                [
                    k
                    async for k in r.bless_scan_iter(
                        "NO-EVICT", target_nodes="primaries"
                    )
                ]
            )
            == keys_blessed
        )

    async def test_cluster_randomkey(self, r: RedisCluster) -> None:
        node = r.get_node_from_key("{foo}")
        assert await r.randomkey(target_nodes=node) is None
        for key in ("{foo}a", "{foo}b", "{foo}c"):
            await r.set(key, 1)
        assert await r.randomkey(target_nodes=node) in (b"{foo}a", b"{foo}b", b"{foo}c")

    @skip_if_server_version_lt("6.0.0")
    @skip_if_redis_enterprise()
    async def test_acl_log(
        self, r: RedisCluster, create_redis: Callable[..., RedisCluster]
    ) -> None:
        key = "{cache}:"
        node = r.get_node_from_key(key)
        username = "redis-py-user"

        await r.acl_setuser(
            username,
            enabled=True,
            reset=True,
            commands=["+get", "+set", "+select", "+cluster", "+command", "+info"],
            keys=["{cache}:*"],
            nopass=True,
            target_nodes="primaries",
        )
        await r.acl_log_reset(target_nodes=node)

        user_client = await create_redis(
            cls=RedisCluster, flushdb=False, username=username
        )

        # Valid operation and key
        assert await user_client.set("{cache}:0", 1)
        assert await user_client.get("{cache}:0") == b"1"

        # Invalid key
        with pytest.raises(NoPermissionError):
            await user_client.get("{cache}violated_cache:0")

        # Invalid operation
        with pytest.raises(NoPermissionError):
            await user_client.hset("{cache}:0", "hkey", "hval")

        assert isinstance(await r.acl_log(target_nodes=node), list)
        assert len(await r.acl_log(target_nodes=node)) == 3
        assert len(await r.acl_log(count=1, target_nodes=node)) == 1
        assert isinstance((await r.acl_log(target_nodes=node))[0], dict)
        assert "client-info" in (await r.acl_log(count=1, target_nodes=node))[0]
        assert await r.acl_log_reset(target_nodes=node)

        await r.acl_deluser(username, target_nodes="primaries")

        await user_client.aclose()

    @skip_if_server_version_lt("8.5.240")
    async def test_hotkeys_cluster(self, r: RedisCluster) -> None:
        """Test all HOTKEYS commands in cluster are raising an error"""

        with pytest.raises(NotImplementedError):
            await r.hotkeys_start(count=10, metrics=[HotkeysMetricsTypes.CPU])
        with pytest.raises(NotImplementedError):
            await r.hotkeys_get()
        with pytest.raises(NotImplementedError):
            await r.hotkeys_reset()
        with pytest.raises(NotImplementedError):
            await r.hotkeys_stop()


@pytest.mark.fixed_client
class TestClusterRedisCommandsMocked:
    """Cluster commands checked against mocked node replies; no server needed."""

    @skip_if_redis_enterprise()
    async def test_cluster_delslots(self) -> None:
        cluster_slots = [
            [0, 8191, ["127.0.0.1", 7000, "node_0"]],
            [8192, 16383, ["127.0.0.1", 7001, "node_1"]],
        ]
        r = await get_mocked_redis_client(
            host=default_host, port=default_port, cluster_slots=cluster_slots
        )
        mock_all_nodes_resp(r, "OK")
        node0 = r.get_node(default_host, 7000)
        node1 = r.get_node(default_host, 7001)
        assert await r.cluster_delslots(0, 8192) == [True, True]
        assert node0._free.pop().read_response.called
        assert node1._free.pop().read_response.called

        await r.aclose()

    @skip_if_server_version_lt("7.0.0")
    @skip_if_redis_enterprise()
    async def test_cluster_delslotsrange(self):
        r = await get_mocked_redis_client(host=default_host, port=default_port)
        mock_all_nodes_resp(r, "OK")
        node = r.get_random_node()
        await r.cluster_addslots(node, 1, 2, 3, 4, 5)
        assert await r.cluster_delslotsrange(1, 5)
        assert node._free.pop().read_response.called
        await r.aclose()

    @skip_if_redis_enterprise()
    async def test_readonly(self) -> None:
        r = await get_mocked_redis_client(host=default_host, port=default_port)
        mock_all_nodes_resp(r, "OK")
        assert await r.readonly() is True
        all_replicas_results = await r.readonly(target_nodes="replicas")
        for res in all_replicas_results.values():
            assert res is True
        for replica in r.get_replicas():
            assert replica._free.pop().read_response.called

        await r.aclose()

    @skip_if_redis_enterprise()
    async def test_readwrite(self) -> None:
        r = await get_mocked_redis_client(host=default_host, port=default_port)
        mock_all_nodes_resp(r, "OK")
        assert await r.readwrite() is True
        all_replicas_results = await r.readwrite(target_nodes="replicas")
        for res in all_replicas_results.values():
            assert res is True
        for replica in r.get_replicas():
            assert replica._free.pop().read_response.called

        await r.aclose()


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

    Key extraction itself is covered by ``tests/test_asyncio/test_command_parser.py``, so it is
    stubbed here and these tests assert only the node the command is dispatched to.
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
    async def _mocked_cluster() -> RedisCluster:
        # The policy resolver is built here rather than reusing the import-time default
        # instance, which every client in the process shares along with its memo.
        return await get_mocked_redis_client(
            host=default_host, port=7000, policy_resolver=AsyncStaticPolicyResolver()
        )

    @pytest.mark.fixed_client
    async def test_the_shipped_default_is_this_resolver(self) -> None:
        """The routing below is the default behaviour, not one this test opted into."""
        rc = await get_mocked_redis_client(host=default_host, port=7000)

        assert isinstance(rc._policy_resolver, AsyncStaticPolicyResolver)

        await rc.aclose()

    @pytest.mark.fixed_client
    async def test_the_default_resolver_is_built_per_client(self) -> None:
        """
        Each client owns its resolver, so the memos a resolver accumulates are released with
        the client rather than retained for the life of the process. A default evaluated in
        the signature would hand every client in the process the same object.
        """
        first = await get_mocked_redis_client(host=default_host, port=7000)
        second = await get_mocked_redis_client(host=default_host, port=7000)

        assert first._policy_resolver is not second._policy_resolver

        await first.aclose()
        await second.aclose()

    @pytest.mark.fixed_client
    async def test_a_pipeline_routes_through_the_clients_resolver(self) -> None:
        """
        Sync parity: routing must not change just because the commands go through a pipeline.
        The async pipeline holds the client and reads its resolver, so this needs no
        propagation of its own - unlike the sync stack, where ``pipeline()`` has to pass it.
        """
        resolver = AsyncStaticPolicyResolver()
        rc = await get_mocked_redis_client(
            host=default_host, port=7000, policy_resolver=resolver
        )

        assert rc.pipeline().cluster_client._policy_resolver is resolver

        await rc.aclose()

    @pytest.mark.fixed_client
    async def test_metadata_resolver_decides_replica_routing_with_explicit_policy_resolver(
        self,
    ) -> None:
        metadata_resolver = AsyncDynamicMetadataResolver(
            {"core": {"set": CACHEABLE_KEYED}}
        )
        rc = await get_mocked_redis_client(
            host=default_host,
            port=7000,
            read_from_replicas=True,
            policy_resolver=AsyncStaticPolicyResolver(),
            metadata_resolver=metadata_resolver,
        )

        with (
            mock.patch.object(rc, "_determine_slot", return_value=0),
            mock.patch.object(
                type(rc.nodes_manager), "get_node_from_slot", autospec=True
            ) as get_node,
        ):
            await rc.get_nodes_from_slot("set", "key", "value")

        get_node.assert_called_once_with(rc.nodes_manager, 0, True, None)
        await rc.aclose()

    @pytest.mark.fixed_client
    async def test_split_multi_key_routing_respects_metadata_resolver(
        self,
    ) -> None:
        metadata_resolver = AsyncDynamicMetadataResolver(
            {"core": {"mset": CACHEABLE_KEYED}}
        )
        rc = await get_mocked_redis_client(
            host=default_host,
            port=7000,
            read_from_replicas=True,
            policy_resolver=AsyncStaticPolicyResolver(),
            metadata_resolver=metadata_resolver,
        )
        pipe = mock.MagicMock()
        pipe.execute = mock.AsyncMock(return_value=[True])

        with (
            mock.patch.object(rc, "pipeline", return_value=pipe),
            mock.patch.object(
                type(rc.nodes_manager), "get_node_from_slot", autospec=True
            ) as get_node,
        ):
            await rc.mset_nonatomic({"key": "value"})

        slot = key_slot(rc.encoder.encode("key"))
        get_node.assert_called_once_with(rc.nodes_manager, slot, True, None)
        await rc.aclose()

    @pytest.mark.fixed_client
    async def test_mixin_default_is_replica_safe_emits_deprecation_warning(
        self,
    ) -> None:
        from redis.commands.cluster import AsyncRedisClusterCommands

        class StandaloneAsyncClusterCommands(AsyncRedisClusterCommands):
            pass

        instance = StandaloneAsyncClusterCommands()
        with pytest.deprecated_call():
            assert await instance._is_replica_safe("GET") is True
        with pytest.deprecated_call():
            assert await instance._is_replica_safe("SET") is False

    @pytest.mark.fixed_client
    async def test_cluster_scan_routes_to_all_primaries_by_default(self) -> None:
        rc = await get_mocked_redis_client(host=default_host, port=7000)
        primaries = rc.get_primaries()
        assert len(primaries) > 1

        _, policies = await rc._resolve_command_policies("scan")
        nodes = await rc._determine_nodes(
            "scan", request_policy=policies.request_policy
        )
        assert set(nodes) == set(primaries)
        await rc.aclose()

    @pytest.mark.fixed_client
    @pytest.mark.parametrize("command", ["dbsize", "keys"])
    async def test_default_node_commands_route_to_default_node_by_default(
        self, command: str
    ) -> None:
        rc = await get_mocked_redis_client(host=default_host, port=7000)
        default_node = rc.get_default_node()

        _, policies = await rc._resolve_command_policies(command)
        nodes = await rc._determine_nodes(
            command, request_policy=policies.request_policy
        )
        assert nodes == [default_node]
        await rc.aclose()

    @pytest.mark.fixed_client
    @pytest.mark.parametrize("empty_targets", [[], {}])
    async def test_empty_target_nodes_does_not_raise_and_falls_back(
        self, empty_targets: Any
    ) -> None:
        rc = await get_mocked_redis_client(host=default_host, port=7000)
        default_node = rc.get_default_node()

        _, policies = await rc._resolve_command_policies("dbsize")
        nodes = await rc._determine_nodes(
            "dbsize",
            request_policy=policies.request_policy,
            node_flag=empty_targets,
        )
        assert nodes == [default_node]
        await rc.aclose()

    @pytest.mark.fixed_client
    @pytest.mark.parametrize("empty_targets", [[], {}, ""])
    async def test_execute_command_empty_target_nodes_falls_back(
        self, empty_targets: Any
    ) -> None:
        rc = await get_mocked_redis_client(host=default_host, port=7000)
        default_node = rc.get_default_node()

        with mock.patch.object(
            RedisCluster,
            "_execute_command",
            new=mock.AsyncMock(return_value=100),
        ) as execute:
            result = await rc.execute_command("DBSIZE", target_nodes=empty_targets)

        assert result == 100
        execute.assert_awaited_once_with(default_node, "DBSIZE")
        await rc.aclose()

    @pytest.mark.fixed_client
    @pytest.mark.parametrize("invalid_targets", [False, 0, (), b""])
    async def test_execute_command_rejects_invalid_empty_target_nodes(
        self, invalid_targets: Any
    ) -> None:
        rc = await get_mocked_redis_client(host=default_host, port=7000)

        with pytest.raises(TypeError, match="target_nodes type"):
            await rc.execute_command("DBSIZE", target_nodes=invalid_targets)

        await rc.aclose()

    @pytest.mark.fixed_client
    async def test_command_subcommands_route_to_default_node(self) -> None:
        rc = await get_mocked_redis_client(host=default_host, port=7000)
        default_node = rc.get_default_node()

        _, policies = await rc._resolve_command_policies("COMMAND", "COUNT")
        nodes = await rc._determine_nodes(
            "COMMAND", "COUNT", request_policy=policies.request_policy
        )
        assert nodes == [default_node]
        await rc.aclose()

    @pytest.mark.fixed_client
    async def test_command_subcommands_case_insensitive_routing(self) -> None:
        rc = await get_mocked_redis_client(host=default_host, port=7000)
        default_node = rc.get_default_node()

        _, policies = await rc._resolve_command_policies("command", "count")
        nodes = await rc._determine_nodes(
            "command", "count", request_policy=policies.request_policy
        )
        assert nodes == [default_node]

        with mock.patch.object(
            ClusterNode, "execute_command", new=mock.AsyncMock(return_value=100)
        ) as exec_mock:
            res = await rc.execute_command("command", "count")
            assert res == 100
            exec_mock.assert_called_once_with("command", "count", asking=False)
        await rc.aclose()

    @pytest.mark.fixed_client
    @pytest.mark.parametrize("targets", ["nodes", "flag"])
    async def test_explicit_target_nodes_do_not_apply_the_commands_aggregation(
        self, targets: str
    ) -> None:
        """
        A response policy resolved for the whole cluster must not apply when the caller
        picked its targets, as named nodes or as a nodes flag. RANDOMKEY has no result
        callback, so the replies are handed to the response-policy callback, which must
        leave them keyed by node.
        """
        resolver = AsyncDynamicMetadataResolver(
            {
                "core": {
                    "randomkey": CommandMetadata(
                        request_policy=RequestPolicy.ALL_SHARDS,
                        response_policy=ResponsePolicy.ONE_SUCCEEDED,
                    )
                }
            }
        )
        rc = await get_mocked_redis_client(
            host=default_host, port=7000, metadata_resolver=resolver
        )
        nodes = rc.get_primaries()
        assert len(nodes) > 1
        target_nodes = nodes if targets == "nodes" else RedisCluster.PRIMARIES

        with mock.patch.object(
            ClusterNode, "execute_command", new=mock.AsyncMock(return_value="k")
        ) as execute:
            result = await rc.execute_command("RANDOMKEY", target_nodes=target_nodes)

        assert execute.call_count == len(nodes)
        assert result == {node.name: "k" for node in nodes}
        await rc.aclose()

    @pytest.mark.fixed_client
    async def test_determine_nodes_raises_exception_when_no_policy_resolved(
        self,
    ) -> None:
        rc = await get_mocked_redis_client(host=default_host, port=7000)
        with pytest.raises(RedisClusterException, match="No targets were found"):
            await rc._determine_nodes("UNRECOGNIZED_COMMAND", request_policy=None)
        await rc.aclose()

    @pytest.mark.fixed_client
    async def test_split_command_routing_applies_load_balancing_strategy(self) -> None:
        rc = await get_mocked_redis_client(
            host=default_host,
            port=7000,
            load_balancing_strategy=LoadBalancingStrategy.ROUND_ROBIN_REPLICAS,
        )
        pipe = Mock()
        pipe.execute = mock.AsyncMock(return_value=["OK", "OK"])
        with (
            mock.patch.object(
                rc._metadata_resolver, "is_replica_safe", return_value=True
            ),
            mock.patch.object(rc, "pipeline", return_value=pipe),
            mock.patch.object(
                type(rc.nodes_manager), "get_node_from_slot", autospec=True
            ) as get_node,
        ):
            slots_to_args = {100: ["a", "1"], 200: ["b", "2"]}
            await rc._execute_pipeline_by_slot("MSET", slots_to_args)

            get_node.assert_any_call(
                rc.nodes_manager, 100, True, LoadBalancingStrategy.ROUND_ROBIN_REPLICAS
            )
            get_node.assert_any_call(
                rc.nodes_manager, 200, True, LoadBalancingStrategy.ROUND_ROBIN_REPLICAS
            )
        await rc.aclose()

    @pytest.mark.fixed_client
    async def test_custom_host_class_without_strategy_executes_split_slots(
        self,
    ) -> None:
        class ThirdPartyAsyncCluster(AsyncClusterMultiKeyCommands):
            def __init__(self, rc):
                self.encoder = rc.encoder
                self.nodes_manager = rc.nodes_manager
                self._rc = rc
                self._initialize = False

            def pipeline(self):
                return self._rc.pipeline()

        rc = await get_mocked_redis_client(host=default_host, port=7000)
        custom_client = ThirdPartyAsyncCluster(rc)
        pipe = Mock()
        pipe.execute = mock.AsyncMock(return_value=["OK", "OK"])
        with patch.object(custom_client, "pipeline", return_value=pipe):
            result = await custom_client.mset_nonatomic({"a": "1", "b": "2"})
            assert result == ["OK", "OK"]
        await rc.aclose()

    @pytest.mark.fixed_client
    async def test_keyless_routing_honors_the_public_node_selector(self) -> None:
        rc = await get_mocked_redis_client(host=default_host, port=7000)
        selected_node = rc.get_primaries()[0]

        with mock.patch.object(
            type(rc),
            "get_keyless_target_node",
            new=mock.AsyncMock(return_value=selected_node),
        ) as select_node:
            nodes = await rc._determine_nodes(
                "dbsize", request_policy=RequestPolicy.DEFAULT_KEYLESS
            )

        assert nodes == [selected_node]
        select_node.assert_called_once_with("dbsize")
        await rc.aclose()

    @pytest.mark.fixed_client
    @pytest.mark.parametrize("strategy", sorted(_REPLICAS_ONLY_STRATEGIES, key=str))
    async def test_keyless_reads_honor_a_replicas_only_strategy(self, strategy) -> None:
        """
        A strategy that asks for replicas must not land a keyless read on a primary. Only
        that much of the strategy applies: the rest of it is an index into one shard's node
        list, and a keyless command has no shard, so the pick is uniform over the replicas.
        """
        rc = await get_mocked_redis_client(
            host=default_host, port=7000, load_balancing_strategy=strategy
        )
        replicas = set(rc.get_replicas())
        assert replicas

        picked = {await rc.get_keyless_target_node("get") for _ in range(50)}

        assert picked
        assert picked <= replicas
        assert not picked & set(rc.get_primaries())
        await rc.aclose()

    @pytest.mark.fixed_client
    @pytest.mark.parametrize("strategy", sorted(_REPLICAS_ONLY_STRATEGIES, key=str))
    async def test_keyless_reads_fall_back_when_the_cluster_has_no_replicas(
        self, strategy
    ) -> None:
        """
        A replicas-only strategy against a cluster that has none must still answer with a
        node instead of raising out of an empty choice.
        """
        rc = await get_mocked_redis_client(
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

        picked = {await rc.get_keyless_target_node("get") for _ in range(20)}

        assert picked
        assert picked <= primaries
        await rc.aclose()

    @pytest.mark.fixed_client
    @pytest.mark.parametrize(
        "strategy",
        [LoadBalancingStrategy.ROUND_ROBIN, LoadBalancingStrategy.RANDOM, None],
    )
    async def test_keyless_reads_keep_the_whole_node_set_otherwise(
        self, strategy
    ) -> None:
        """
        The two strategies that include the primary, and ``read_from_replicas`` on its own,
        keep the answer this method has given since 7.1.0: any node.
        """
        rc = await get_mocked_redis_client(
            host=default_host,
            port=7000,
            read_from_replicas=True,
            load_balancing_strategy=strategy,
        )
        node = rc.get_primaries()[0]

        with mock.patch.object(
            type(rc), "get_random_node", return_value=node
        ) as any_node:
            assert await rc.get_keyless_target_node("get") is node

        any_node.assert_called_once()
        await rc.aclose()

    @pytest.mark.fixed_client
    async def test_slot_id_commands_route_by_slot_for_any_spelling(self) -> None:
        """
        The SLOT_ID flag lookup is normalized, so a raw lowercase spelling names the same
        command as the one the command method sends - and the slot comes back as the int
        the slot map is keyed by, whichever way the caller spelled it.
        """
        rc = await get_mocked_redis_client(host=default_host, port=7000)

        assert await rc._determine_slot("CLUSTER COUNTKEYSINSLOT", 42) == 42
        assert await rc._determine_slot("cluster countkeysinslot", "42") == 42
        await rc.aclose()

    @pytest.mark.fixed_client
    async def test_container_command_is_dispatched_by_the_routed_name(self) -> None:
        """
        A container command passed as two words is dispatched by the name the policies were
        decided by, so the result callback registered for it fires - and receives that name
        rather than the caller's first word.
        """
        rc = await get_mocked_redis_client(host=default_host, port=7000)
        seen = []
        rc.result_callbacks["COMMAND COUNT"] = lambda cmd, res, **kwargs: seen.append(
            cmd
        )

        with mock.patch.object(
            ClusterNode, "execute_command", new=mock.AsyncMock(return_value=100)
        ):
            await rc.execute_command("command", "count")

        assert seen == ["COMMAND COUNT"]
        await rc.aclose()

    @pytest.mark.fixed_client
    async def test_the_routed_name_is_the_same_with_and_without_explicit_targets(
        self,
    ) -> None:
        """
        The routed name is decided before the explicit-targets branch, so one command
        answers with one name either way. Without it the name kept the caller's spelling
        on that branch alone, and DBSIZE's result callback - keyed, like every other, in
        upper case - summed the replies for one of these two calls and not the other.
        """
        rc = await get_mocked_redis_client(host=default_host, port=7000)
        seen = []
        rc.result_callbacks["DBSIZE"] = lambda cmd, res, **kwargs: seen.append(cmd)

        with mock.patch.object(
            ClusterNode, "execute_command", new=mock.AsyncMock(return_value=7)
        ):
            await rc.execute_command("dbsize")
            await rc.execute_command("dbsize", target_nodes=rc.get_primaries())

        assert seen == ["DBSIZE", "DBSIZE"]
        await rc.aclose()

    @pytest.mark.fixed_client
    async def test_the_deprecated_selector_stays_synchronous(self) -> None:
        """
        The signature it has carried since 7.1.0: called, not awaited, and answering with
        a node rather than a coroutine - from inside a running event loop and from a plain
        sync frame alike.
        """
        rc = await get_mocked_redis_client(host=default_host, port=7000)

        with pytest.warns(DeprecationWarning, match="get_keyless_target_node"):
            in_loop = rc.get_random_primary_or_all_nodes("get")

        assert in_loop in rc.get_nodes()

        with pytest.warns(DeprecationWarning):
            off_loop = await asyncio.to_thread(
                rc.get_random_primary_or_all_nodes, "get"
            )

        assert off_loop in rc.get_nodes()
        await rc.aclose()

    @pytest.mark.fixed_client
    async def test_the_deprecated_selector_answers_from_the_metadata_resolver(
        self,
    ) -> None:
        """
        Not from ``READ_COMMANDS``: GET is a member of it, so a resolver that does not report
        the command replica safe is the only thing that can send this to a primary.
        """
        rc = await get_mocked_redis_client(
            host=default_host,
            port=7000,
            read_from_replicas=True,
            metadata_resolver=AsyncDynamicMetadataResolver({"core": {}}),
        )

        with (
            mock.patch.object(type(rc), "get_random_node") as any_node,
            pytest.warns(DeprecationWarning),
        ):
            node = rc.get_random_primary_or_all_nodes("get")

        any_node.assert_not_called()
        assert node in rc.get_primaries()
        await rc.aclose()

        rc = await get_mocked_redis_client(
            host=default_host,
            port=7000,
            read_from_replicas=True,
            metadata_resolver=AsyncDynamicMetadataResolver(
                {"core": {"get": CACHEABLE_KEYED}}
            ),
        )
        replica = rc.get_replicas()[0]

        with (
            mock.patch.object(
                type(rc), "get_random_node", return_value=replica
            ) as any_node,
            pytest.warns(DeprecationWarning),
        ):
            assert rc.get_random_primary_or_all_nodes("get") is replica

        any_node.assert_called_once()
        await rc.aclose()

    @pytest.mark.fixed_client
    async def test_the_movablekeys_reads_resolve_keyed(self) -> None:
        """
        The deterministic half of this: a keyed record is what sends the client to
        ``_determine_slot``. The dispatch assertion below can only catch a wrong policy
        probabilistically, because keyless routing picks a random node.
        """
        rc = await self._mocked_cluster()

        for command in self.MOVABLE_KEYS_READS:
            resolved = await rc._policy_resolver.resolve(command)
            assert resolved.request_policy == RequestPolicy.DEFAULT_KEYED, command

        await rc.aclose()

    @pytest.mark.fixed_client
    async def test_every_slot_routed_entry_is_dispatched_to_the_node_holding_its_key(
        self,
    ) -> None:
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
        rc = await self._mocked_cluster()
        expected_node = rc.nodes_manager.get_node_from_slot(key_slot(b"{foo}a"))
        commands = list(slot_routed_static_commands())

        assert len(commands) > 70, "the slot-routed set looks truncated"

        for command in commands:
            with (
                mock.patch.object(
                    rc.commands_parser, "get_keys", return_value=["{foo}a"]
                ),
                mock.patch.object(
                    RedisCluster, "_execute_command", return_value="OK"
                ) as execute_command,
            ):
                await rc.execute_command(command, *slot_routed_args(command))

            assert execute_command.call_count == 1, command
            assert execute_command.call_args[0][0] is expected_node, command

        await rc.aclose()


async def _route_by_live_tips(r: RedisCluster, monkeypatch) -> None:
    """Route ``r`` by the tips of the connected server instead of the shipped records."""
    records = await r.commands_parser.get_commands_metadata_cache()
    monkeypatch.setattr(
        r,
        "_policy_resolver",
        AsyncStaticPolicyResolver(
            metadata_resolver=AsyncDynamicMetadataResolver(records)
        ),
    )


@contextlib.contextmanager
def _sent_commands(
    fail: Optional[Callable[[ClusterNode, tuple], Optional[Exception]]] = None,
):
    """Async mirror of ``tests.test_cluster._sent_commands``."""
    sent: List[Any] = []
    send = RedisCluster._execute_command

    async def execute_command(
        self: RedisCluster, node: ClusterNode, *args: Any, **kwargs: Any
    ) -> Any:
        sent.append((node, args, kwargs))
        error = fail(node, args) if fail is not None else None
        if error is not None:
            raise error
        return await send(self, node, *args, **kwargs)

    with mock.patch.object(RedisCluster, "_execute_command", execute_command):
        yield sent


@contextlib.contextmanager
def _pipelined_commands(
    fail: Optional[Callable[[tuple], Optional[Exception]]] = None,
):
    """Async mirror of ``tests.test_cluster._pipelined_commands``."""
    batches: List[List[Tuple[tuple, Dict[str, Any]]]] = []
    execute_pipeline = ClusterNode.execute_pipeline

    async def record(self: ClusterNode, commands: List[Any]) -> bool:
        held = []
        for c in commands:
            error = fail(c.args) if fail is not None else None
            if error is not None:
                c.result = error
                held.append(c)
        rest = [c for c in commands if c not in held]
        batches.append([(c.args, c.kwargs) for c in rest])
        ret = await execute_pipeline(self, rest) if rest else False
        return ret or bool(held)

    with mock.patch.object(ClusterNode, "execute_pipeline", record):
        yield batches


@pytest.mark.onlycluster
@skip_if_server_version_lt("8.0.0")
@skip_if_redis_enterprise()
class TestResponsePolicies:
    """
    Async mirror of ``tests.test_cluster.TestResponsePolicies``. The folds are module-level
    and shared by both stacks, so the sync suite covers the ones no command is tipped with.
    """

    async def test_agg_sum_adds_up_every_node_s_count(
        self, r: RedisCluster, monkeypatch
    ) -> None:
        await _route_by_live_tips(r, monkeypatch)
        expected = 0
        for node in r.get_nodes():
            expected += await node.execute_command("SLOWLOG LEN")

        with _sent_commands() as sent:
            assert await r.execute_command("SLOWLOG LEN") == expected

        assert {node.name for node, _, _ in sent} == {n.name for n in r.get_nodes()}

    async def test_agg_min_folds_array_replies_element_wise(
        self, r: RedisCluster, monkeypatch
    ) -> None:
        await _route_by_live_tips(r, monkeypatch)
        replies = [
            await node.execute_command("WAITAOF", 0, 0, 0) for node in r.get_primaries()
        ]

        assert await r.execute_command("WAITAOF", 0, 0, 0) == [
            min(column) for column in zip(*replies)
        ]

    async def test_all_succeeded_answers_with_one_reply(
        self, r: RedisCluster, monkeypatch
    ) -> None:
        await _route_by_live_tips(r, monkeypatch)
        library = "#!lua name=policylib\nredis.register_function('one', function() return 1 end)"

        try:
            loaded = await r.execute_command("FUNCTION LOAD", "REPLACE", library)

            assert str_if_bytes(loaded) == "policylib"
            for node in r.get_primaries():
                assert await node.execute_command("FCALL", "one", 0) == 1
        finally:
            await r.execute_command("FUNCTION DELETE", "policylib")

    async def test_all_succeeded_fails_on_an_error(
        self, r: RedisCluster, monkeypatch
    ) -> None:
        await _route_by_live_tips(r, monkeypatch)

        with pytest.raises(ResponseError, match="Library not found"):
            await r.execute_command("FUNCTION DELETE", "no-such-library")

    @pytest.mark.parametrize("method", ["script_kill", "function_kill"])
    async def test_one_succeeded_raises_when_no_shard_succeeds(
        self, r: RedisCluster, method: str
    ) -> None:
        with _sent_commands() as sent:
            with pytest.raises(ResponseError, match="NOTBUSY"):
                await getattr(r, method)()

        assert len(sent) == len(r.get_primaries())

    async def test_script_kill_stops_the_script_on_whichever_shard_runs_it(
        self, r: RedisCluster
    ) -> None:
        busy = r.get_primaries()[-1]

        with _busy_node(busy, "EVAL", "while true do end", 0):
            with _sent_commands() as sent:
                assert await r.script_kill() is True

        assert len(sent) == len(r.get_primaries())

    async def test_function_kill_stops_the_function_on_whichever_shard_runs_it(
        self, r: RedisCluster
    ) -> None:
        busy = r.get_primaries()[-1]
        await busy.execute_command("FUNCTION LOAD", "REPLACE", _SPIN_LIBRARY)

        try:
            with _busy_node(busy, "FCALL", "spin", 0):
                assert str_if_bytes(await r.function_kill()) == "OK"
        finally:
            await busy.execute_command("FUNCTION DELETE", "spinlib")

    @pytest.mark.parametrize(
        "error",
        [
            TryAgainError("TRYAGAIN"),
            AskError("1 127.0.0.1:7001"),
            ConnectionError("lost"),
        ],
    )
    async def test_one_succeeded_answers_with_the_success_despite_an_error_in_doubt(
        self, r: RedisCluster, error: Exception
    ) -> None:
        busy, doubtful = r.get_primaries()[-1], r.get_primaries()[0]

        def fail(node: ClusterNode, args: tuple) -> Optional[Exception]:
            return error if node.name == doubtful.name else None

        with _busy_node(busy, "EVAL", "while true do end", 0):
            with _sent_commands(fail) as sent:
                assert await r.script_kill() is True

        assert len(sent) == len(r.get_primaries())

    @pytest.mark.parametrize(
        "error", [TryAgainError("TRYAGAIN"), AskError("1 127.0.0.1:7001")]
    )
    async def test_one_succeeded_reports_an_error_in_doubt_when_no_shard_succeeds(
        self, r: RedisCluster, error: Exception
    ) -> None:
        doubtful = r.get_primaries()[-1]

        def fail(node: ClusterNode, args: tuple) -> Optional[Exception]:
            return error if node.name == doubtful.name else None

        with _sent_commands(fail):
            with pytest.raises(type(error)):
                await r.script_kill()

    @pytest.mark.parametrize("targets", ["nodes", "flag"])
    async def test_the_caller_s_targets_disable_the_aggregation(
        self, r: RedisCluster, monkeypatch, targets: str
    ) -> None:
        await _route_by_live_tips(r, monkeypatch)
        primaries = r.get_primaries()
        target_nodes = primaries if targets == "nodes" else r.PRIMARIES
        expected = {}
        for node in primaries:
            expected[node.name] = await node.execute_command("WAITAOF", 0, 0, 0)

        assert (
            await r.execute_command("WAITAOF", 0, 0, 0, target_nodes=target_nodes)
            == expected
        )
        async with r.pipeline() as pipe:
            pipe.execute_command("WAITAOF", 0, 0, 0, target_nodes=target_nodes)
            assert await pipe.execute() == [expected]

    async def test_a_retry_asks_only_the_nodes_that_have_not_answered(
        self, r: RedisCluster, no_cluster_retry_delay
    ) -> None:
        primaries = r.get_primaries()
        failed = primaries[-1]
        failures: List[str] = []

        def fail(node: ClusterNode, args: tuple) -> Optional[Exception]:
            if node.name == failed.name and not failures:
                failures.append(node.name)
                return ConnectionError("lost")
            return None

        with _sent_commands(fail) as sent:
            assert await r.execute_command("PING", target_nodes=r.PRIMARIES) is True

        assert failures
        assert sorted(node.name for node, _, _ in sent) == sorted(
            [*(node.name for node in primaries), failed.name]
        )

    async def test_info_keeps_each_shard_s_reply(
        self, r: RedisCluster, monkeypatch
    ) -> None:
        await _route_by_live_tips(r, monkeypatch)

        info = await r.info()

        assert set(info) == {node.name for node in r.get_primaries()}

    @pytest.mark.parametrize(
        "command,nodes",
        [
            (("MEMORY DOCTOR",), "get_primaries"),
            (("LATENCY DOCTOR",), "get_nodes"),
            (("LATENCY HISTOGRAM",), "get_nodes"),
        ],
    )
    async def test_the_shipped_table_keeps_each_node_s_diagnostics(
        self, r: RedisCluster, command: tuple[str, ...], nodes: str
    ) -> None:
        result = await r.execute_command(*command)

        assert set(result) == {node.name for node in getattr(r, nodes)()}

    async def test_function_stats_reports_every_primary(self, r: RedisCluster) -> None:
        assert set(await r.function_stats()) == {
            node.name for node in r.get_primaries()
        }

    async def test_a_result_callback_still_merges_a_fanned_out_command(
        self, r: RedisCluster
    ) -> None:
        await r.mset_nonatomic({key: 1 for key in _keys_on_every_shard(r, "dbsize")})
        expected = 0
        for node in r.get_primaries():
            expected += await node.execute_command("DBSIZE")

        assert (
            await r.execute_command("DBSIZE", target_nodes=RedisCluster.PRIMARIES)
            == expected
        )

    async def test_a_pipeline_folds_each_fanned_out_command_into_one_result(
        self, r: RedisCluster, monkeypatch
    ) -> None:
        await _route_by_live_tips(r, monkeypatch)
        await r.mset_nonatomic({key: 1 for key in _keys_on_every_shard(r, "pipe")})
        waitaof = [
            await node.execute_command("WAITAOF", 0, 0, 0) for node in r.get_primaries()
        ]
        dbsize = 0
        for node in r.get_primaries():
            dbsize += await node.execute_command("DBSIZE")

        async with r.pipeline() as pipe:
            pipe.execute_command("WAITAOF", 0, 0, 0)
            pipe.execute_command("DBSIZE")
            pipe.execute_command("FUNCTION DELETE", "no-such-library")
            pipe.execute_command("SCRIPT KILL")
            results = await pipe.execute(raise_on_error=False)

        assert results[0] == [min(column) for column in zip(*waitaof)]
        assert results[1] == dbsize
        assert isinstance(results[2], ResponseError)
        assert isinstance(results[3], ResponseError) and "NOTBUSY" in str(results[3])

    @pytest.mark.parametrize(
        "error", [ConnectionError("lost"), TryAgainError("TRYAGAIN")]
    )
    @pytest.mark.parametrize("targets", ["flag", "nodes"])
    async def test_a_pipeline_retry_asks_only_the_node_that_failed(
        self, r: RedisCluster, error: Exception, targets: str
    ) -> None:
        """
        A connection error retries the pipeline, and TRYAGAIN the one command, and both
        ask only the node that failed, with the replies of the others kept: asking them
        again would run a write twice. The node is picked among the targets the caller
        passed, not by the command's own policy - DBSIZE's is the default node alone,
        which holds only some of the keys.
        """
        await r.mset_nonatomic(
            {key: 1 for key in _keys_on_every_shard(r, "pipetargets")}
        )
        primaries = r.get_primaries()
        dbsize = 0
        for node in primaries:
            dbsize += await node.execute_command("DBSIZE")
        target_nodes = r.PRIMARIES if targets == "flag" else primaries
        failed = primaries[-1]
        execute_pipeline = ClusterNode.execute_pipeline
        sends = {node.name: 0 for node in primaries}

        async def fail_once(self: ClusterNode, commands: List[Any]) -> bool:
            # The failed node's DBSIZE is not sent once, and answers with ``error`` instead
            held = [c for c in commands if c.args == ("DBSIZE",)]
            sends[self.name] += len(held)
            held = held if self.name == failed.name and sends[self.name] == 1 else []
            rest = [c for c in commands if c not in held]
            ret = await execute_pipeline(self, rest) if rest else False
            for c in held:
                c.result = error
            return ret or bool(held)

        with mock.patch.object(ClusterNode, "execute_pipeline", fail_once):
            with _sent_commands() as sent:
                async with r.pipeline() as pipe:
                    pipe.execute_command("DBSIZE", target_nodes=target_nodes)
                    assert await pipe.execute() == [dbsize]

        for node, _, _ in sent:
            sends[node.name] += 1
        assert sends == {**{node.name: 1 for node in primaries}, failed.name: 2}


@pytest.mark.onlycluster
@skip_if_server_version_lt("8.0.0")
class TestMultiShardRequests:
    """Async mirror of ``tests.test_cluster.TestMultiShardRequests``."""

    async def test_the_counts_of_a_summed_command_are_added_up(
        self, r: RedisCluster
    ) -> None:
        keys = _keys_on_every_shard(r, "sum")
        await r.mset_nonatomic({key: 1 for key in keys})

        with _pipelined_commands() as batches, _sent_commands() as sent:
            assert await r.execute_command("EXISTS", *keys, keys[0] + "missing") == len(
                keys
            )
            assert await r.execute_command("TOUCH", *keys) == len(keys)

        assert sum(len(batch) for batch in batches) == 2 * len(keys)
        assert not sent

    async def test_a_nodes_flag_keeps_each_primary_s_count(
        self, r: RedisCluster
    ) -> None:
        await r.set("{flag}foo", 1)
        expected = {node.name: 1 for node in r.get_primaries()}

        assert (
            await r.execute_command("EXISTS", "{flag}foo", target_nodes=r.PRIMARIES)
            == expected
        )
        assert (
            await r.execute_command(
                "EXISTS", "{flag}foo", target_nodes=r.get_primaries()
            )
            == expected
        )

    async def test_the_shipped_records_keep_mget_unsplit(self, r: RedisCluster) -> None:
        with pytest.raises(RedisClusterException, match="same key slot"):
            await r.mget(_keys_on_every_shard(r, "mget"))

    async def test_the_values_of_a_per_key_command_come_back_in_key_order(
        self, r: RedisCluster, monkeypatch
    ) -> None:
        await _route_by_live_tips(r, monkeypatch)
        first, second, third = _keys_on_every_shard(r, "order")[:3]
        keys = [first, second, first + "x", third]
        assert r.keyslot(first) == r.keyslot(first + "x")
        await r.mset_nonatomic({key: key for key in keys})

        with _pipelined_commands() as batches:
            values = await r.execute_command("MGET", *keys, keys=keys)

        assert [str_if_bytes(value) for value in values] == keys
        assert sorted(
            (args, kwargs["keys"]) for batch in batches for args, kwargs in batch
        ) == sorted(
            [
                (("MGET", first, first + "x"), [first, first + "x"]),
                (("MGET", second), [second]),
                (("MGET", third), [third]),
            ]
        )

    async def test_a_key_travels_with_the_arguments_of_its_step(
        self, r: RedisCluster, monkeypatch
    ) -> None:
        await _route_by_live_tips(r, monkeypatch)
        keys = _keys_on_every_shard(r, "step")

        assert await r.execute_command(
            "MSET", *(arg for key in keys for arg in (key, key))
        )

        assert [str_if_bytes(await r.get(key)) for key in keys] == keys

    async def test_keys_that_share_a_slot_are_sent_as_they_are(
        self, r: RedisCluster
    ) -> None:
        keys = ["{same}1", "{same}2"]
        await r.mset({key: 1 for key in keys})

        with _sent_commands() as sent:
            assert await r.execute_command("EXISTS", *keys) == 2

        assert [args for _, args, _ in sent] == [("EXISTS", *keys)]

    async def test_keys_that_share_a_slot_are_hashed_once(
        self, r: RedisCluster
    ) -> None:
        keys = ["{same}1", "{same}2", "{same}3"]

        with mock.patch.object(r, "keyslot", wraps=r.keyslot) as keyslot:
            assert await r.execute_command("EXISTS", *keys) == 0
        assert keyslot.call_count == len(keys)

        with mock.patch.object(r, "keyslot", wraps=r.keyslot) as keyslot:
            async with r.pipeline() as pipe:
                pipe.execute_command("EXISTS", *keys)
                assert await pipe.execute() == [0]
        assert keyslot.call_count == len(keys)

    @skip_if_server_version_lt("8.4")
    async def test_keys_that_cannot_be_located_are_not_split(
        self, r: RedisCluster, monkeypatch
    ) -> None:
        await _route_by_live_tips(r, monkeypatch)
        first, second = _keys_on_every_shard(r, "movable")[:2]

        with pytest.raises(RedisClusterException, match="same key slot"):
            await r.execute_command("MSETEX", 2, first, "a", second, "b")

    @pytest.mark.parametrize(
        "error",
        [
            ConnectionError("lost"),
            TryAgainError("TRYAGAIN"),
            ClusterDownError("CLUSTERDOWN The cluster is down"),
        ],
    )
    async def test_a_retry_resends_only_the_slots_that_failed(
        self, r: RedisCluster, monkeypatch, error: Exception
    ) -> None:
        await _route_by_live_tips(r, monkeypatch)
        keys = _keys_on_every_shard(r, "retry")
        await r.mset_nonatomic({key: 1 for key in keys})
        failed: List[tuple] = []

        def fail(args: tuple) -> Optional[Exception]:
            if args == ("DEL", keys[-1]) and not failed:
                failed.append(args)
                return error
            return None

        with _pipelined_commands(fail) as batches, _sent_commands() as sent:
            assert await r.execute_command("DEL", *keys) == len(keys)

        assert failed
        # The failed slot reached the server only on the retry, and no other slot twice
        assert sorted(
            [
                *(args for batch in batches for args, _ in batch),
                *(args for _, args, _ in sent),
            ]
        ) == sorted(("DEL", key) for key in keys)
        assert await r.exists(*keys) == 0

    async def test_a_slot_without_a_known_node_is_retried(
        self, r: RedisCluster, monkeypatch
    ) -> None:
        keys = _keys_on_every_shard(r, "uncovered")
        await r.mset_nonatomic({key: 1 for key in keys})
        get_node_from_slot = NodesManager.get_node_from_slot
        uncovered = r.keyslot(keys[-1])
        failed: List[int] = []

        def lose_the_slot_once(
            self: NodesManager, slot: int, *args: Any, **kwargs: Any
        ) -> ClusterNode:
            if slot == uncovered and not failed:
                failed.append(slot)
                raise SlotNotCoveredError(f"Slot {slot} is not covered")
            return get_node_from_slot(self, slot, *args, **kwargs)

        # NodesManager has __slots__, so the method is patched on the class
        monkeypatch.setattr(NodesManager, "get_node_from_slot", lose_the_slot_once)

        with _pipelined_commands() as batches, _sent_commands() as sent:
            assert await r.execute_command("EXISTS", *keys) == len(keys)

        assert failed
        assert sorted(args for batch in batches for args, _ in batch) == sorted(
            ("EXISTS", key) for key in keys[:-1]
        )
        assert [args for _, args, _ in sent] == [("EXISTS", keys[-1])]

    async def test_an_error_read_from_the_pipeline_is_counted(
        self, r: RedisCluster
    ) -> None:
        keys = _keys_on_every_shard(r, "counted")
        error = ResponseError("OOM command not allowed")

        def fail(args: tuple) -> Optional[Exception]:
            return error if args == ("EXISTS", keys[-1]) else None

        with _pipelined_commands(fail):
            with mock.patch.object(r, "_record_error_metric") as record:
                with pytest.raises(ResponseError, match="OOM"):
                    await r.execute_command("EXISTS", *keys)

        record.assert_called_once_with(
            error=error,
            connection=r.get_node_from_key(keys[-1]),
            is_internal=False,
        )

    async def test_a_retry_does_not_resend_a_slot_that_answered(
        self, r: RedisCluster, monkeypatch
    ) -> None:
        """
        A write lost on one node retries the pipeline, which resends only the commands
        no node answered: the DEL of a missing key answered 0, which is not resent.
        """
        await _route_by_live_tips(r, monkeypatch)
        keys = _keys_on_every_shard(r, "answered")
        await r.mset_nonatomic({key: 1 for key in keys[1:]})
        lost = r.get_node_from_key(keys[-1]).name
        assert r.get_node_from_key(keys[0]).name != lost
        execute_pipeline = ClusterNode.execute_pipeline
        written: List[tuple] = []
        failed: List[str] = []

        async def lose_a_write_once(self: ClusterNode, commands: List[Any]) -> bool:
            if self.name == lost and not failed:
                failed.append(self.name)
                raise ConnectionError("lost")
            written.extend(c.args for c in commands)
            return await execute_pipeline(self, commands)

        with mock.patch.object(ClusterNode, "execute_pipeline", lose_a_write_once):
            assert await r.execute_command("DEL", *keys) == len(keys) - 1

        assert failed
        assert sorted(written) == sorted(("DEL", key) for key in keys)

    async def test_a_split_command_costs_one_round_trip_per_node(
        self, r: RedisCluster, monkeypatch
    ) -> None:
        """
        Each node is sent the commands of all its slots in one pipelined write, on one
        connection, so keys over more slots than a node has connections do not exhaust
        its pool with ``MaxConnectionsError``.
        """
        keys = [f"{{conn{index}}}key" for index in range(60)]
        await r.mset_nonatomic({key: 1 for key in keys})
        slots_per_node: Dict[str, Set[int]] = {}
        for key in keys:
            node = r.get_node_from_key(key).name
            slots_per_node.setdefault(node, set()).add(r.keyslot(key))
        assert min(len(slots) for slots in slots_per_node.values()) > 1
        for node in r.get_primaries():
            monkeypatch.setattr(node, "max_connections", 1)

        with _pipelined_commands() as batches, _sent_commands() as sent:
            assert await r.execute_command("EXISTS", *keys) == len(keys)

        assert len(batches) == len(slots_per_node)
        assert sum(len(batch) for batch in batches) == len(
            {r.keyslot(key) for key in keys}
        )
        assert not sent

    async def test_a_pipeline_retry_of_a_split_command_holds_one_connection(
        self, r: RedisCluster, monkeypatch
    ) -> None:
        """
        The slots of a split command that failed in the pipeline are resent one at a time,
        so a node with more of them than connections does not raise ``MaxConnectionsError``.
        """
        keys = [f"{{conn{index}}}key" for index in range(60)]
        await r.mset_nonatomic({key: 1 for key in keys})
        for node in r.get_primaries():
            monkeypatch.setattr(node, "max_connections", 1)
        failed: Set[str] = set()

        async def fail_once(self: ClusterNode, commands: List[Any]) -> bool:
            # Every slot answers TRYAGAIN once, so each is resent individually
            failed.add(self.name)
            for c in commands:
                c.result = TryAgainError("TRYAGAIN")
            return True

        with mock.patch.object(ClusterNode, "execute_pipeline", fail_once):
            with _sent_commands() as sent:
                async with r.pipeline() as pipe:
                    pipe.execute_command("EXISTS", *keys)
                    assert await pipe.execute() == [len(keys)]

        assert failed
        assert len(sent) == len({r.keyslot(key) for key in keys})

    async def test_a_pipeline_merges_a_split_command_per_slot(
        self, r: RedisCluster, monkeypatch
    ) -> None:
        await _route_by_live_tips(r, monkeypatch)
        keys = _keys_on_every_shard(r, "pipesplit")
        await r.mset_nonatomic({key: key for key in keys})

        async with r.pipeline() as pipe:
            pipe.execute_command("MGET", *reversed(keys))
            pipe.execute_command("EXISTS", *keys)
            results = await pipe.execute()

        assert [str_if_bytes(value) for value in results[0]] == keys[::-1]
        assert results[1] == len(keys)

    @pytest.mark.parametrize(
        "error", [ConnectionError("lost"), TryAgainError("TRYAGAIN")]
    )
    async def test_a_pipeline_retry_resends_only_the_slots_that_failed(
        self, r: RedisCluster, monkeypatch, error: Exception
    ) -> None:
        await _route_by_live_tips(r, monkeypatch)
        keys = _keys_on_every_shard(r, "piperetry")
        await r.mset_nonatomic({key: 1 for key in keys})
        execute_pipeline = ClusterNode.execute_pipeline
        written: List[tuple] = []
        failed: List[Any] = []

        async def fail_once(self: ClusterNode, commands: List[Any]) -> bool:
            # The last key's slot is not sent once, and answers with ``error`` instead
            held = [c for c in commands if c.args == ("DEL", keys[-1])]
            held = held if not failed else []
            failed.extend(held)
            rest = [c for c in commands if c not in held]
            written.extend(c.args for c in rest)
            ret = await execute_pipeline(self, rest) if rest else False
            for c in held:
                c.result = error
            return ret or bool(held)

        with mock.patch.object(ClusterNode, "execute_pipeline", fail_once):
            with _sent_commands() as sent:
                async with r.pipeline() as pipe:
                    pipe.execute_command("DEL", *keys)
                    assert await pipe.execute() == [len(keys)]

        assert failed
        # The held slot reached the server only on the retry, and no other slot twice
        assert sorted([*written, *(args for _, args, _ in sent)]) == sorted(
            ("DEL", key) for key in keys
        )
        assert await r.exists(*keys) == 0


@pytest.mark.onlycluster
@skip_if_server_version_lt("8.0.0")
class TestSpecialResponses:
    """Async mirror of ``tests.test_cluster.TestSpecialResponses``."""

    async def test_randomkey_answers_with_a_key_from_any_shard(
        self, r: RedisCluster
    ) -> None:
        assert await r.randomkey() is None
        key = next(
            f"{{only}}{index}"
            for index in range(10000)
            if r.get_node_from_key(f"{{only}}{index}") != r.get_default_node()
        )
        await r.set(key, 1)

        with _sent_commands() as sent:
            assert str_if_bytes(await r.randomkey()) == key

        assert {node.name for node, _, _ in sent} == {
            node.name for node in r.get_primaries()
        }

    @pytest.mark.parametrize("targets", ["nodes", "flag"])
    async def test_the_caller_s_targets_keep_each_node_s_reply(
        self, r: RedisCluster, targets: str
    ) -> None:
        primaries = r.get_primaries()
        target_nodes = primaries if targets == "nodes" else r.PRIMARIES

        result = await r.randomkey(target_nodes=target_nodes)

        assert result == {node.name: None for node in primaries}


@pytest.mark.onlycluster
class TestReadOnlyEvalSlotResolution:
    """Async mirror of ``tests.test_cluster.TestReadOnlyEvalSlotResolution``."""

    @skip_if_server_version_lt("7.0.0")
    async def test_the_keys_are_read_from_the_arguments(self, r: RedisCluster) -> None:
        script = "return redis.call('GET', KEYS[1])"
        await r.set("{foo}a", "value")
        sha = await r.script_load(script)

        with mock.patch.object(
            r.commands_parser, "get_keys", wraps=r.commands_parser.get_keys
        ) as get_keys:
            assert str_if_bytes(await r.eval_ro(script, 1, "{foo}a")) == "value"
            assert str_if_bytes(await r.evalsha_ro(sha, 1, "{foo}a")) == "value"

        get_keys.assert_not_called()

    @skip_if_server_version_lt("7.0.0")
    async def test_a_script_without_keys_runs_on_any_node(
        self, r: RedisCluster
    ) -> None:
        sha = await r.script_load("return 1")

        assert await r.eval_ro("return 1", 0) == 1
        assert await r.evalsha_ro(sha, 0) == 1


class TestNodesManager:
    """
    Tests for the NodesManager class
    """

    @pytest.mark.fixed_client
    async def test_get_node_from_slot_with_missing_slot(self) -> None:
        nodes_manager = NodesManager([], False, {})

        with pytest.raises(SlotNotCoveredError):
            nodes_manager.get_node_from_slot(42)

    @pytest.mark.onlycluster
    async def test_load_balancer(self, r: RedisCluster) -> None:
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
        lb.reset()
        # load balancer strategy: LoadBalancerStrategy.RANDOM_REPLICA
        for i in range(5):
            srv_index = lb.get_server_index(
                primary1_name,
                list1_size,
                load_balancing_strategy=LoadBalancingStrategy.RANDOM_REPLICA,
            )

            assert srv_index > 0 and srv_index <= 2

    @pytest.mark.fixed_client
    async def test_init_slots_cache_not_all_slots_covered(self) -> None:
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
            rc = await get_mocked_redis_client(
                host=default_host,
                port=default_port,
                cluster_slots=cluster_slots,
                require_full_coverage=True,
            )
            await rc.aclose()
        assert str(ex.value).startswith(
            "All slots are not covered after query all startup_nodes."
        )

    @pytest.mark.fixed_client
    async def test_init_slots_cache_not_require_full_coverage_success(self) -> None:
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

        rc = await get_mocked_redis_client(
            host=default_host,
            port=default_port,
            cluster_slots=cluster_slots,
            require_full_coverage=False,
        )

        assert 5460 not in rc.nodes_manager.slots_cache

        await rc.aclose()

    @pytest.mark.fixed_client
    async def test_init_slots_cache(self) -> None:
        """
        Test that slots cache can in initialized and all slots are covered
        """
        good_slots_resp = [
            [0, 5460, ["127.0.0.1", 7000], ["127.0.0.2", 7003]],
            [5461, 10922, ["127.0.0.1", 7001], ["127.0.0.2", 7004]],
            [10923, 16383, ["127.0.0.1", 7002], ["127.0.0.2", 7005]],
        ]

        rc = await get_mocked_redis_client(
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
                for node in n_manager.slots_cache[i]:
                    assert node is n_manager.nodes_cache[node.name]

        assert len(n_manager.nodes_cache) == 6

        await rc.aclose()

    @pytest.mark.fixed_client
    async def test_initialize_reuses_nodes_cache_nodes_in_slots_cache(
        self,
    ) -> None:
        """
        Test that a topology refresh keeps slots_cache pointed at nodes_cache
        nodes instead of temporary nodes built from CLUSTER SLOTS.
        """
        first_slots = [[0, 16383, [default_host, 7000, "node_0"]]]
        second_slots = [
            [0, 8191, [default_host, 7000, "node_0"]],
            [8192, 16383, [default_host, 7001, "node_1"]],
        ]
        cluster_slots = [first_slots]

        async def mocked_execute_command(self, *args, **kwargs):
            if args[0] == "CLUSTER SLOTS":
                return cluster_slots[0]
            return None

        with mock.patch.object(
            ClusterNode, "execute_command", autospec=True
        ) as execute_command:
            execute_command.side_effect = mocked_execute_command
            nodes_manager = NodesManager(
                startup_nodes=[ClusterNode(default_host, 7000)],
                require_full_coverage=True,
                connection_kwargs={},
            )

            await nodes_manager.initialize()
            first_node = nodes_manager.nodes_cache[get_node_name(default_host, 7000)]
            assert nodes_manager.slots_cache[0][0] is first_node
            assert nodes_manager.slots_cache[0] is nodes_manager.slots_cache[16383]

            cluster_slots[0] = second_slots
            await nodes_manager.initialize()

            assert (
                nodes_manager.nodes_cache[get_node_name(default_host, 7000)]
                is first_node
            )
            assert nodes_manager.slots_cache[0][0] is first_node
            assert nodes_manager.slots_cache[0] is nodes_manager.slots_cache[8191]
            second_node = nodes_manager.nodes_cache[get_node_name(default_host, 7001)]
            assert nodes_manager.slots_cache[8192][0] is second_node
            assert nodes_manager.slots_cache[8192] is nodes_manager.slots_cache[16383]
            assert nodes_manager.slots_cache[0] is not nodes_manager.slots_cache[8192]
            for nodes in nodes_manager.slots_cache.values():
                for node in nodes:
                    assert node is nodes_manager.nodes_cache[node.name]

    @pytest.mark.onlycluster
    async def test_initialize_uses_shuffled_startup_nodes(
        self, r: RedisCluster
    ) -> None:
        startup_nodes = list(r.nodes_manager.startup_nodes.values())
        first_shuffled_node = startup_nodes[-1]
        original_execute_command = ClusterNode.execute_command
        executed_nodes = []

        async def execute_command(node, *args, **kwargs):
            executed_nodes.append((node, args))
            return await original_execute_command(node, *args, **kwargs)

        with (
            mock.patch(
                "redis.asyncio.cluster.random.shuffle",
                side_effect=lambda nodes: nodes.reverse(),
            ) as shuffle,
            mock.patch.object(
                ClusterNode,
                "execute_command",
                autospec=True,
                side_effect=execute_command,
            ),
        ):
            await r.nodes_manager.initialize()

        shuffle.assert_called_once()
        assert any(
            node is first_shuffled_node and args == ("CLUSTER SLOTS",)
            for node, args in executed_nodes
        )

    @pytest.mark.fixed_client
    @pytest.mark.skipif(
        platform.python_implementation() == "PyPy",
        reason="Immediate reclamation is a refcounting property",
    )
    async def test_initialize_breaks_reference_cycle_when_a_later_startup_node_answers(
        self,
    ) -> None:
        """
        When a startup node fails but a later one answers, initialize() must
        not return while still holding the caught exception: its traceback
        refers to the initialize() frame, so the pair would stay alive, with
        the slots built by that call, until the cyclic GC runs.
        """
        # Raise from the mocked command so the test itself never holds a
        # strong reference; the weakref is the only handle on the failure.
        raised_ref = []

        async def mocked_execute_command(self, *args, **kwargs):
            if self.port == 7000:
                err = ConnectionError("Connection refused")
                raised_ref.append(weakref.ref(err))
                try:
                    raise err
                finally:
                    # Clear this frame's local too, or the traceback keeps the
                    # exception alive here and the assertion measures the test
                    # rather than initialize().
                    err = None
            return default_cluster_slots

        nodes_manager = NodesManager(
            [ClusterNode("127.0.0.1", 7000), ClusterNode("127.0.0.1", 7001)],
            False,
            {},
        )

        gc_was_enabled = gc.isenabled()
        gc.disable()
        try:
            with (
                # Keep the startup order: the failing node is tried first.
                mock.patch("redis.asyncio.cluster.random.shuffle"),
                mock.patch.object(
                    ClusterNode,
                    "execute_command",
                    autospec=True,
                    side_effect=mocked_execute_command,
                ),
            ):
                await nodes_manager.initialize()
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
    async def test_initialize_breaks_reference_cycle_when_a_later_startup_node_fails_after_a_partial_answer(
        self,
    ) -> None:
        """
        When a startup node answers with a partial slot map and a later one
        fails, initialize() must not store the later error: nothing raises it
        once a node has answered, and its traceback refers to the
        initialize() frame, so the pair would stay alive, with the slots
        built by that call, until the cyclic GC runs.
        """
        # Raise from the mocked command so the test itself never holds a
        # strong reference; the weakref is the only handle on the failure.
        raised_ref = []

        async def mocked_execute_command(self, *args, **kwargs):
            if self.port == 7001:
                err = ConnectionError("Connection refused")
                raised_ref.append(weakref.ref(err))
                try:
                    raise err
                finally:
                    # Clear this frame's local too, or the traceback keeps the
                    # exception alive here and the assertion measures the test
                    # rather than initialize().
                    err = None
            return default_cluster_slots[:1]

        nodes_manager = NodesManager(
            [ClusterNode("127.0.0.1", 7000), ClusterNode("127.0.0.1", 7001)],
            False,
            {},
        )

        gc_was_enabled = gc.isenabled()
        gc.disable()
        try:
            with (
                # Keep the startup order: the partial node is tried first, so
                # the slots are not fully covered and the failing node is
                # tried next.
                mock.patch("redis.asyncio.cluster.random.shuffle"),
                mock.patch.object(
                    ClusterNode,
                    "execute_command",
                    autospec=True,
                    side_effect=mocked_execute_command,
                ),
            ):
                await nodes_manager.initialize()
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
    async def test_initialize_clears_exception_local_when_no_startup_node_answers(
        self, error_type: Type[Exception], raised_type: Type[Exception]
    ) -> None:
        """
        When no startup node answers, initialize() raises the last error from
        the local that holds it. The raised error's traceback refers to the
        initialize() frame, so the frame must not keep that local after the
        raise: the pair would stay alive until the cyclic GC runs, on every
        failed refresh.
        """

        async def mocked_execute_command(self, *args, **kwargs):
            raise error_type("mock failure")

        nodes_manager = NodesManager(
            [ClusterNode("127.0.0.1", 7000), ClusterNode("127.0.0.1", 7001)],
            False,
            {},
        )

        with mock.patch.object(
            ClusterNode,
            "execute_command",
            autospec=True,
            side_effect=mocked_execute_command,
        ):
            with pytest.raises(RedisClusterException) as exc_info:
                await nodes_manager.initialize()
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
    async def test_init_slots_cache_cluster_mode_disabled(self) -> None:
        """
        Test that creating a RedisCluster failes if one of the startup nodes
        has cluster mode disabled
        """
        with pytest.raises(RedisClusterException) as e:
            rc = await get_mocked_redis_client(
                cluster_slots_raise_error=True,
                host=default_host,
                port=default_port,
                cluster_enabled=False,
            )
            await rc.aclose()
        assert "Cluster mode is not enabled on this node" in str(e.value)

    @pytest.mark.fixed_client
    async def test_unreachable_error_is_reserved_for_connectivity_failures(
        self,
    ) -> None:
        """
        RedisClusterUnreachableError is raised when no startup node could be
        contacted, but not when a node responded and rejected CLUSTER SLOTS:
        MultiDB registers the unreachable subtype as retryable, so a
        deterministic configuration error must keep surfacing as a plain
        RedisClusterException. Authentication failures subclass ConnectionError
        but cannot be repaired by a failover, so they stay plain as well.
        """
        with mock.patch.object(
            ClusterNode, "execute_command", autospec=True
        ) as execute_command:

            async def mocked_execute_command(self, *args, **kwargs):
                raise ConnectionError("mock connection error")

            execute_command.side_effect = mocked_execute_command

            with pytest.raises(RedisClusterUnreachableError) as e:
                async with RedisCluster(startup_nodes=[ClusterNode("127.0.0.1", 7000)]):
                    ...
            assert "Redis Cluster cannot be connected" in str(e.value)

        with mock.patch.object(
            ClusterNode, "execute_command", autospec=True
        ) as execute_command:

            async def mocked_execute_command_auth(self, *args, **kwargs):
                raise AuthenticationError("invalid password")

            execute_command.side_effect = mocked_execute_command_auth

            with pytest.raises(RedisClusterException) as e:
                async with RedisCluster(startup_nodes=[ClusterNode("127.0.0.1", 7000)]):
                    ...
            assert not isinstance(e.value, RedisClusterUnreachableError)
            assert "invalid password" in str(e.value)

        with pytest.raises(RedisClusterException) as e:
            rc = await get_mocked_redis_client(
                cluster_slots_raise_error=True,
                host=default_host,
                port=default_port,
                cluster_enabled=False,
            )
            await rc.aclose()
        assert not isinstance(e.value, RedisClusterUnreachableError)
        assert "Cluster mode is not enabled on this node" in str(e.value)

    @pytest.mark.fixed_client
    async def test_empty_startup_nodes(self) -> None:
        """
        It should not be possible to create a node manager with no nodes
        specified
        """
        with pytest.raises(RedisClusterException):
            await NodesManager([], False, {}).initialize()

    @pytest.mark.fixed_client
    async def test_wrong_startup_nodes_type(self) -> None:
        """
        If something other then a list type itteratable is provided it should
        fail
        """
        with pytest.raises(RedisClusterException):
            await NodesManager({}, False, {}).initialize()

    @pytest.mark.fixed_client
    async def test_init_slots_cache_slots_collision(self) -> None:
        """
        Test that if 2 nodes do not agree on the same slots setup it should
        raise an error. In this test both nodes will say that the first
        slots block should be bound to different servers.
        """
        with mock.patch.object(
            ClusterNode, "execute_command", autospec=True
        ) as execute_command:

            async def mocked_execute_command(self, *args, **kwargs):
                """
                Helper function to return custom slots cache data from
                different redis nodes
                """
                if self.port == 7000:
                    result = [
                        [0, 5460, ["127.0.0.1", 7000], ["127.0.0.1", 7003]],
                        [5461, 10922, ["127.0.0.1", 7001], ["127.0.0.1", 7004]],
                    ]

                elif self.port == 7001:
                    result = [
                        [0, 5460, ["127.0.0.1", 7001], ["127.0.0.1", 7003]],
                        [5461, 10922, ["127.0.0.1", 7000], ["127.0.0.1", 7004]],
                    ]
                else:
                    result = []

                if args[0] == "CLUSTER SLOTS":
                    return result
                elif args[0] == "INFO":
                    return {"cluster_enabled": True}
                elif args[1] == "cluster-require-full-coverage":
                    return {"cluster-require-full-coverage": "yes"}

            execute_command.side_effect = mocked_execute_command

            with pytest.raises(RedisClusterException) as ex:
                node_1 = ClusterNode("127.0.0.1", 7000)
                node_2 = ClusterNode("127.0.0.1", 7001)
                async with RedisCluster(startup_nodes=[node_1, node_2]):
                    ...
            assert str(ex.value).startswith(
                "startup_nodes could not agree on a valid slots cache"
            ), str(ex.value)

    @pytest.mark.fixed_client
    @pytest.mark.parametrize("host", ["", None])
    async def test_cluster_one_instance(self, host) -> None:
        """
        If the cluster exists of only 1 node then there is some hacks that must
        be validated they work.
        """
        node = ClusterNode(default_host, default_port)
        cluster_slots = [[0, 16383, [host, default_port]]]
        rc = await get_mocked_redis_client(
            startup_nodes=[node], cluster_slots=cluster_slots
        )

        n = rc.nodes_manager
        assert len(n.nodes_cache) == 1
        n_node = rc.get_node(node_name=node.name)
        assert n_node is not None
        assert n_node == node
        assert n_node.server_type == PRIMARY
        assert len(n.slots_cache) == REDIS_CLUSTER_HASH_SLOTS
        for i in range(0, REDIS_CLUSTER_HASH_SLOTS):
            assert n.slots_cache[i] == [n_node]

        await rc.aclose()

    @pytest.mark.fixed_client
    async def test_init_slots_cache_without_hosts(self) -> None:
        """
        A cluster whose endpoint is unknown (cluster-preferred-endpoint-type
        unknown-endpoint) sends null hosts in CLUSTER SLOTS, and its nodes are
        reached on the host of the startup node that answered.
        """
        cluster_slots = [
            [0, 8191, [None, 7000, "node_0"], [None, 7003, "node_3"]],
            [8192, 16383, [None, 7001, "node_1"], [None, 7002, "node_2"]],
        ]
        rc = await get_mocked_redis_client(
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

        await rc.aclose()

    @pytest.mark.fixed_client
    async def test_init_with_down_node(self) -> None:
        """
        If I can't connect to one of the nodes, everything should still work.
        But if I can't connect to any of the nodes, exception should be thrown.
        """
        with mock.patch.object(
            ClusterNode, "execute_command", autospec=True
        ) as execute_command:

            async def mocked_execute_command(self, *args, **kwargs):
                if self.port == 7000:
                    raise ConnectionError("mock connection error for 7000")

                if args[0] == "CLUSTER SLOTS":
                    return [
                        [0, 8191, ["127.0.0.1", 7001, "node_1"]],
                        [8192, 16383, ["127.0.0.1", 7002, "node_2"]],
                    ]
                elif args[0] == "INFO":
                    return {"cluster_enabled": True}
                elif args[1] == "cluster-require-full-coverage":
                    return {"cluster-require-full-coverage": "yes"}

            execute_command.side_effect = mocked_execute_command

            node_1 = ClusterNode("127.0.0.1", 7000)
            node_2 = ClusterNode("127.0.0.1", 7001)

            # If all startup nodes fail to connect, connection error should be
            # thrown
            with pytest.raises(RedisClusterException) as e:
                async with RedisCluster(startup_nodes=[node_1]):
                    ...
            assert "Redis Cluster cannot be connected" in str(e.value)

            with mock.patch.object(
                AsyncCommandsParser, "initialize", autospec=True
            ) as cmd_parser_initialize:

                def cmd_init_mock(self, r: ClusterNode) -> None:
                    self.commands = {
                        "GET": {
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
                async with RedisCluster(startup_nodes=[node_1, node_2]) as rc:
                    assert rc.get_node(host=default_host, port=7001) is not None
                    assert rc.get_node(host=default_host, port=7002) is not None

    @pytest.mark.fixed_client
    @pytest.mark.parametrize("dynamic_startup_nodes", [True, False])
    async def test_init_slots_dynamic_startup_nodes(self, dynamic_startup_nodes):
        rc = await get_mocked_redis_client(
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
            assert sorted(startup_nodes) == sorted(discovered_nodes)
        else:
            assert startup_nodes == ["my@DNS.com:7000"]

    @pytest.mark.fixed_client
    async def test_move_node_to_end_of_cached_nodes(self) -> None:
        """
        Test that move_node_to_end_of_cached_nodes moves a node to the end of
        startup_nodes and nodes_cache.
        """
        node1 = ClusterNode(default_host, 7000)
        node2 = ClusterNode(default_host, 7001)
        node3 = ClusterNode(default_host, 7002)

        nodes_manager = NodesManager(
            startup_nodes=[node1, node2, node3],
            require_full_coverage=False,
            connection_kwargs={},
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
    async def test_set_nodes_disconnects_removed_node_connections_after_use(
        self,
    ) -> None:
        """
        Test removed nodes let in-flight commands finish, then disconnect.
        """

        class FakeConnection:
            def __init__(self) -> None:
                self.disconnect_called = False
                self.is_connected = True
                self.reconnect = False
                self.response_ready = asyncio.Event()
                self.send_called = asyncio.Event()
                self._resp2_fallback = False

            def pack_command(self, *args):
                return b"packed"

            async def send_packed_command(self, *args) -> None:
                self.send_called.set()

            async def read_response(self, *args, **kwargs):
                await self.response_ready.wait()
                return b"OK"

            async def disconnect(self) -> None:
                self.disconnect_called = True
                self.is_connected = False
                self.reconnect = False

            def mark_for_reconnect(self) -> None:
                self.reconnect = True

            def should_reconnect(self) -> bool:
                return self.reconnect

        node = ClusterNode(default_host, 7000)
        active_conn = FakeConnection()
        free_conn = FakeConnection()
        node._connections = [active_conn, free_conn]
        node._free.append(active_conn)
        node._free.append(free_conn)

        nodes_manager = NodesManager(
            startup_nodes=[node],
            require_full_coverage=False,
            connection_kwargs={},
        )
        nodes_manager.nodes_cache = {node.name: node}

        command = asyncio.create_task(node.execute_command("GET", "key"))
        await active_conn.send_called.wait()

        nodes_manager.set_nodes(nodes_manager.nodes_cache, {}, remove_old=True)
        tasks = tuple(nodes_manager._background_tasks)
        assert len(tasks) == 1
        await asyncio.gather(*tasks)

        assert nodes_manager.nodes_cache == {}
        assert free_conn.disconnect_called is True
        assert free_conn.is_connected is False
        assert active_conn.reconnect is True
        assert active_conn.disconnect_called is False
        assert active_conn.is_connected is True

        active_conn.response_ready.set()
        assert await command == b"OK"

        assert active_conn.disconnect_called is True
        assert active_conn.is_connected is False
        assert active_conn in node._free
        assert free_conn.reconnect is False
        assert active_conn.reconnect is False

    @pytest.mark.fixed_client
    async def test_refresh_keeps_connections_of_nodes_that_switch_roles(
        self,
    ) -> None:
        """
        A primary/replica swap is not a reason to recycle connections: both
        nodes keep their ClusterNode objects and connections, only their roles
        change.
        """
        before = [
            [0, 16383, [default_host, 7000, "node_0"], [default_host, 7003, "node_3"]]
        ]
        after = [
            [0, 16383, [default_host, 7003, "node_3"], [default_host, 7000, "node_0"]]
        ]
        cluster_slots = [before]

        async def mocked_execute_command(self, *args, **kwargs):
            if args[0] == "CLUSTER SLOTS":
                return cluster_slots[0]
            return None

        with mock.patch.object(
            ClusterNode, "execute_command", autospec=True
        ) as execute_command:
            execute_command.side_effect = mocked_execute_command
            nodes_manager = NodesManager(
                startup_nodes=[ClusterNode(default_host, 7000)],
                require_full_coverage=True,
                connection_kwargs={},
            )
            await nodes_manager.initialize()

            old_nodes = dict(nodes_manager.nodes_cache)
            connections = []
            for node in old_nodes.values():
                in_use, free = Mock(is_connected=False), Mock(is_connected=False)
                node._connections = [in_use, free]
                node._free.append(free)
                connections.extend((in_use, free))

            cluster_slots[0] = after
            with mock.patch.object(
                ClusterNode, "update_active_connections_for_reconnect", autospec=True
            ) as update_active:
                await nodes_manager.initialize()

        update_active.assert_not_called()
        for connection in connections:
            connection.mark_for_reconnect.assert_not_called()
            connection.disconnect.assert_not_called()
        for name, node in old_nodes.items():
            assert nodes_manager.nodes_cache[name] is node
        primary = nodes_manager.nodes_cache[get_node_name(default_host, 7003)]
        replica = nodes_manager.nodes_cache[get_node_name(default_host, 7000)]
        assert primary.server_type == PRIMARY
        assert replica.server_type == REPLICA
        assert nodes_manager.slots_cache[0] == [primary, replica]

    @pytest.mark.fixed_client
    async def test_initialize_registers_re_auth_once_before_querying_nodes(
        self,
    ) -> None:
        """
        The re-auth registration happens once per refresh, before any node is
        queried - not once per startup node tried - and the event holds the
        live nodes cache, so it covers the nodes the refresh adds.
        """
        slots = [[0, 16383, [default_host, 7000, "node_0"]]]
        calls = []

        async def mocked_execute_command(self, *args, **kwargs):
            calls.append(("query", self.port))
            if self.port == 7001:
                raise ConnectionError("down")
            if args[0] == "CLUSTER SLOTS":
                return slots
            return None

        event_dispatcher = EventDispatcher()
        dispatched = []
        original_dispatch = event_dispatcher.dispatch

        def dispatch(event):
            if isinstance(event, AfterAsyncClusterInstantiationEvent):
                calls.append(("dispatch", None))
                dispatched.append(event)
            return original_dispatch(event)

        event_dispatcher.dispatch = dispatch

        with (
            mock.patch.object(
                ClusterNode, "execute_command", autospec=True
            ) as execute_command,
            mock.patch("redis.asyncio.cluster.random.shuffle"),
        ):
            execute_command.side_effect = mocked_execute_command
            nodes_manager = NodesManager(
                startup_nodes=[
                    ClusterNode(default_host, 7001),
                    ClusterNode(default_host, 7000),
                ],
                require_full_coverage=True,
                connection_kwargs={},
                event_dispatcher=event_dispatcher,
            )
            await nodes_manager.initialize()

        assert len(dispatched) == 1
        assert calls[0] == ("dispatch", None)
        assert dispatched[0].nodes is nodes_manager.nodes_cache
        assert list(dispatched[0].nodes) == [get_node_name(default_host, 7000)]

    @pytest.mark.fixed_client
    async def test_first_initialize_keeps_token_renewal_running(self, caplog) -> None:
        """
        The first connection's handshake starts a streaming credential
        provider, whose first renewal is cancelled for good if no re-auth
        callback is registered by then. With ``block_for_initial=True`` the
        handshake waits for that renewal, so the registration must come before
        any node is queried.
        """
        now = datetime.datetime.now(datetime.timezone.utc).timestamp() * 1000
        token = SimpleToken("value", now + 60000, now, {"oid": "test"})
        idp = mock.Mock(spec=IdentityProviderInterface)
        idp.request_token.return_value = token
        token_manager = TokenManager(
            idp, TokenManagerConfig(0.9, 0, 1000, RetryPolicy(1, 10))
        )

        class EntraIdLikeCredentialProvider(StreamingCredentialProvider):
            """Starts streaming on the first credentials call, as redis-entraid."""

            def __init__(self) -> None:
                self.listener = CredentialsListener()
                self.streaming = False

            def get_credentials(self):
                raise NotImplementedError

            async def get_credentials_async(self):
                if not self.streaming:
                    await token_manager.start_async(
                        self.listener, block_for_initial=True, skip_initial=True
                    )
                    self.streaming = True
                return "oid", "value"

            def on_next(self, callback) -> None:
                self.listener.on_next = callback

            def on_error(self, callback) -> None:
                self.listener.on_error = callback

            def is_streaming(self) -> bool:
                return self.streaming

        provider = EntraIdLikeCredentialProvider()
        slots = [[0, 16383, [default_host, 7000, "node_0"]]]

        async def mocked_execute_command(self, *args, **kwargs):
            # The handshake of the node's first connection
            await provider.get_credentials_async()
            if args[0] == "CLUSTER SLOTS":
                return slots
            return None

        try:
            with (
                caplog.at_level(logging.INFO, logger="redis.auth.token_manager"),
                mock.patch.object(
                    ClusterNode, "execute_command", autospec=True
                ) as execute_command,
            ):
                execute_command.side_effect = mocked_execute_command
                nodes_manager = NodesManager(
                    startup_nodes=[ClusterNode(default_host, 7000)],
                    require_full_coverage=True,
                    connection_kwargs={"credential_provider": provider},
                )
                await nodes_manager.initialize()
        finally:
            token_manager.stop()

        assert provider.listener.on_next is not None
        assert "Renewal cancelled" not in caplog.text
        assert "Next token renewal scheduled" in caplog.text

    @staticmethod
    def _streaming_credential_provider():
        class FakeStreamingCredentialProvider(StreamingCredentialProvider):
            def __init__(self) -> None:
                self.on_next_callback = None

            def get_credentials(self):
                return "default", "token"

            def on_next(self, callback) -> None:
                self.on_next_callback = callback

            def on_error(self, callback) -> None:
                pass

            def is_streaming(self) -> bool:
                return False

        return FakeStreamingCredentialProvider()

    @staticmethod
    def _re_auth_connection(
        marked: bool, error: Optional[Exception] = None, connected: bool = True
    ):
        connection = mock.AsyncMock(spec=Connection)
        connection.retry = Retry(NoBackoff(), 0)
        connection.should_reconnect = mock.Mock(return_value=marked)
        connection.is_connected = connected
        if error is not None:
            connection.send_command.side_effect = error
        return connection

    @pytest.mark.fixed_client
    async def test_re_auth_skips_free_connections_that_are_not_usable(
        self,
    ) -> None:
        """
        A free connection that is disconnected, or marked for reconnect after a
        connection error (its socket may be dead), would raise if
        re-authenticated and stop the token renewal. It is skipped - it
        authenticates with fresh credentials when it reconnects on acquire -
        and stays in the free queue.
        """
        node = ClusterNode(default_host, 7000)
        marked = self._re_auth_connection(True, ConnectionError("dead socket"))
        # Disconnected and not marked, as after a timeout or the connection
        # error of a pipeline: sending AUTH would open a socket to a node
        # that may be down
        disconnected = self._re_auth_connection(
            False, ConnectionError("node down"), connected=False
        )
        healthy = self._re_auth_connection(False)
        node._free.extend([marked, disconnected, healthy])
        token = mock.Mock()
        token.try_get.return_value = "oid"
        token.get_value.return_value = "value"

        await node.re_auth_callback(token)

        marked.send_command.assert_not_called()
        disconnected.send_command.assert_not_called()
        healthy.send_command.assert_awaited_once_with("AUTH", "oid", "value")
        healthy.read_response.assert_awaited_once()
        assert list(node._free) == [marked, disconnected, healthy]

    @pytest.mark.fixed_client
    async def test_failed_re_auth_returns_connections_to_the_free_queue(
        self,
    ) -> None:
        """A re-authentication that raises leaves no connection out of _free."""
        node = ClusterNode(default_host, 7000)
        failing = self._re_auth_connection(False, ConnectionError("down"))
        other = self._re_auth_connection(False)
        node._free.extend([failing, other])

        with pytest.raises(ConnectionError):
            await node.re_auth_callback(mock.Mock())

        other.send_command.assert_awaited_once()
        assert sorted(map(id, node._free)) == sorted(map(id, [failing, other]))

    @pytest.mark.fixed_client
    async def test_re_auth_stops_at_authentication_error(self) -> None:
        """
        A rejected token is rejected by every connection, so the first
        AuthenticationError stops the re-authentication. Every connection
        stays in the free queue.
        """
        node = ClusterNode(default_host, 7000)
        failing = self._re_auth_connection(False)
        failing.retry = Retry(NoBackoff(), 3)
        failing.read_response.side_effect = AuthenticationError("expired")
        other = self._re_auth_connection(False)
        node._free.extend([failing, other])

        with pytest.raises(AuthenticationError, match="expired"):
            await node.re_auth_callback(mock.Mock())

        failing.read_response.assert_awaited_once()
        other.send_command.assert_not_awaited()
        assert sorted(map(id, node._free)) == sorted(map(id, [failing, other]))

    @pytest.mark.fixed_client
    async def test_re_auth_survives_topology_refresh_during_iteration(
        self,
    ) -> None:
        """
        A refresh that adds and removes nodes while the nodes are being
        re-authenticated neither breaks the loop nor re-authenticates the
        removed node.
        """
        re_authed = []
        nodes: Dict[str, Any] = {}

        class FakeNode:
            def __init__(self, name: str) -> None:
                self.name = name

            async def re_auth_callback(self, token) -> None:
                re_authed.append(self.name)
                if self.name == "a":
                    # A topology refresh runs while this node re-authenticates
                    del nodes["c"]
                    nodes["d"] = FakeNode("d")
                await asyncio.sleep(0)

        for name in ("a", "b", "c"):
            nodes[name] = FakeNode(name)

        credential_provider = self._streaming_credential_provider()
        RegisterReAuthForAsyncClusterNodes().listen(
            AfterAsyncClusterInstantiationEvent(nodes, credential_provider)
        )

        await credential_provider.on_next_callback(Mock())

        assert re_authed == ["a", "b"]

    @pytest.mark.fixed_client
    async def test_failed_re_auth_does_not_stop_the_other_nodes(self) -> None:
        """
        A node whose re-authentication raises does not stop the remaining nodes
        from being re-authenticated. The first error is raised afterwards.
        """
        re_authed = []

        class FakeNode:
            def __init__(self, name: str) -> None:
                self.name = name

            async def re_auth_callback(self, token) -> None:
                re_authed.append(self.name)
                if self.name != "c":
                    raise AuthenticationError(self.name)

        nodes = {name: FakeNode(name) for name in ("a", "b", "c")}

        credential_provider = self._streaming_credential_provider()
        RegisterReAuthForAsyncClusterNodes().listen(
            AfterAsyncClusterInstantiationEvent(nodes, credential_provider)
        )

        with pytest.raises(AuthenticationError, match="a"):
            await credential_provider.on_next_callback(Mock())

        assert re_authed == ["a", "b", "c"]

    @pytest.mark.fixed_client
    async def test_move_node_to_end_of_cached_nodes_nonexistent(self) -> None:
        """
        Test that move_node_to_end_of_cached_nodes does nothing for a
        nonexistent node.
        """
        node1 = ClusterNode(default_host, 7000)
        node2 = ClusterNode(default_host, 7001)

        nodes_manager = NodesManager(
            startup_nodes=[node1, node2],
            require_full_coverage=False,
            connection_kwargs={},
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
    async def test_move_node_to_end_of_cached_nodes_single_node(self) -> None:
        """
        Test that move_node_to_end_of_cached_nodes does nothing when there's
        only one node.
        """
        node1 = ClusterNode(default_host, 7000)

        nodes_manager = NodesManager(
            startup_nodes=[node1],
            require_full_coverage=False,
            connection_kwargs={},
        )
        # Also populate nodes_cache
        nodes_manager.nodes_cache = {node1.name: node1}

        # Should not raise or change anything with single node
        nodes_manager.move_node_to_end_of_cached_nodes(node1.name)
        startup_node_names = list(nodes_manager.startup_nodes.keys())
        nodes_cache_names = list(nodes_manager.nodes_cache.keys())
        assert startup_node_names == [node1.name]
        assert nodes_cache_names == [node1.name]

    def _register_listener(self, nm):
        """
        Register a tracking async listener on the nodes manager's event
        dispatcher for ``AsyncAfterSlotsCacheRefreshEvent``. The returned
        instance must be kept alive by the caller for the duration of the
        test.
        """

        class _TrackingListener(AsyncEventListenerInterface):
            async def listen(self, event: object) -> None:  # pragma: no cover
                pass

        listener = _TrackingListener()
        listener.listen = mock.AsyncMock()
        nm._event_dispatcher.register_listeners(
            {AsyncAfterSlotsCacheRefreshEvent: [listener]}
        )
        return listener

    @pytest.mark.fixed_client
    async def test_move_slot_dispatches_event_when_slot_moves_to_new_node(
        self,
    ) -> None:
        """
        move_slot() must dispatch AsyncAfterSlotsCacheRefreshEvent when the
        redirected node is not currently serving the slot (new primary from a
        different shard).
        """
        r = await get_mocked_redis_client(host=default_host, port=default_port)
        nm = r.nodes_manager
        listener = self._register_listener(nm)
        # Default layout: slot 0 is served by 127.0.0.1:7000; redirect it to
        # 127.0.0.1:7001, which is the primary for a different shard.
        await nm.move_slot(MovedError("0 127.0.0.1:7001"))

        listener.listen.assert_called_once()
        assert isinstance(
            listener.listen.call_args[0][0], AsyncAfterSlotsCacheRefreshEvent
        )
        await r.aclose()

    @pytest.mark.fixed_client
    async def test_move_slot_dispatches_event_on_failover(self) -> None:
        """
        move_slot() must dispatch AsyncAfterSlotsCacheRefreshEvent when the
        redirected node was a replica of the same slot and is being promoted
        (failover case).
        """
        r = await get_mocked_redis_client(host=default_host, port=default_port)
        nm = r.nodes_manager
        listener = self._register_listener(nm)
        # Default layout: slot 0 -> [7000 (primary), 7003 (replica)].
        # Redirect to the replica, triggering the failover branch.
        await nm.move_slot(MovedError("0 127.0.0.1:7003"))

        listener.listen.assert_called_once()
        assert isinstance(
            listener.listen.call_args[0][0], AsyncAfterSlotsCacheRefreshEvent
        )
        await r.aclose()

    @pytest.mark.fixed_client
    async def test_move_slot_skips_dispatch_on_circular_redirect(self) -> None:
        """
        move_slot() must not dispatch AsyncAfterSlotsCacheRefreshEvent when
        the redirect points to the slot's current primary (no-op branch).
        """
        r = await get_mocked_redis_client(host=default_host, port=default_port)
        nm = r.nodes_manager
        listener = self._register_listener(nm)
        # Slot 0's current primary is 127.0.0.1:7000 -> no-op redirect.
        await nm.move_slot(MovedError("0 127.0.0.1:7000"))

        listener.listen.assert_not_called()
        await r.aclose()

    @pytest.mark.fixed_client
    async def test_move_slot_remaps_redirect_address(self) -> None:
        """
        move_slot() must apply address_remap to the redirect address, which is
        the one the node advertises, and never cache the unmapped address.
        """

        def address_remap(address):
            host, port = address
            return host, port + 1000

        r = await get_mocked_redis_client(
            host=default_host, port=default_port, address_remap=address_remap
        )
        nm = r.nodes_manager
        # Default layout: slot 0 -> 127.0.0.1:7000, cached as 127.0.0.1:8000.
        # Redirect to the primary of a different shard, by its advertised address.
        await nm.move_slot(MovedError("0 127.0.0.1:7001"))

        assert [node.name for node in nm.slots_cache[0]] == ["127.0.0.1:8001"]
        assert "127.0.0.1:7001" not in nm.nodes_cache

        # A node not in the cache yet is added under its remapped address.
        await nm.move_slot(MovedError("0 127.0.0.1:7006"))

        assert [node.name for node in nm.slots_cache[0]] == ["127.0.0.1:8006"]
        assert "127.0.0.1:7006" not in nm.nodes_cache
        await r.aclose()

    @pytest.mark.fixed_client
    async def test_move_slot_resolves_localhost_once(self) -> None:
        """
        A remap to localhost must be resolved once and reused, not on every
        node created or redirect looked up.
        """

        def address_remap(address):
            host, port = address
            return "localhost", port + 1000

        _resolve_localhost.cache_clear()
        try:
            with mock.patch(
                "socket.gethostbyname", return_value="127.0.0.1"
            ) as resolve:
                r = await get_mocked_redis_client(
                    host=default_host, port=default_port, address_remap=address_remap
                )
                nm = r.nodes_manager
                await nm.move_slot(MovedError("0 127.0.0.1:7001"))
                await nm.move_slot(MovedError("0 127.0.0.1:7002"))

            assert [node.name for node in nm.slots_cache[0]] == ["127.0.0.1:8002"]
            resolve.assert_called_once_with("localhost")
            await r.aclose()
        finally:
            _resolve_localhost.cache_clear()


@pytest.mark.fixed_client
class TestClusterNodeConnectionHandling:
    """Tests for ClusterNode connection handling methods."""

    class WriteFailingConnection:
        def __init__(self, **kwargs: Any) -> None:
            self.host = kwargs["host"]
            self.port = kwargs["port"]
            self.db = kwargs.get("db", 0)
            self.is_connected = False
            self.send_count = 0

        def should_reconnect(self) -> bool:
            return False

        async def disconnect(self) -> None:
            self.is_connected = False

        def pack_command(self, *args: Any) -> bytes:
            return b"packed-command"

        def pack_commands(self, commands: Any) -> List[bytes]:
            list(commands)
            return [b"packed-commands"]

        async def send_packed_command(self, command: Any) -> None:
            self.send_count += 1
            raise ConnectionError("simulated write failure")

    async def test_execute_command_releases_connection_after_send_error(self) -> None:
        node = ClusterNode(
            default_host,
            7000,
            max_connections=1,
            connection_class=self.WriteFailingConnection,
        )

        with pytest.raises(ConnectionError, match="simulated write failure"):
            await node.execute_command("GET", "key")

        assert len(node._connections) == 1
        assert len(node._free) == 1
        connection = node._connections[0]
        assert node._free[0] is connection

        with pytest.raises(ConnectionError, match="simulated write failure"):
            await node.execute_command("GET", "key")

        assert len(node._connections) == 1
        assert len(node._free) == 1
        assert node._free[0] is connection
        assert connection.send_count == 2

    async def test_execute_pipeline_releases_connection_after_send_error(self) -> None:
        node = ClusterNode(
            default_host,
            7000,
            max_connections=1,
            connection_class=self.WriteFailingConnection,
        )

        with pytest.raises(ConnectionError, match="simulated write failure"):
            await node.execute_pipeline([PipelineCommand(0, "GET", "key")])

        assert len(node._connections) == 1
        assert len(node._free) == 1
        connection = node._connections[0]
        assert node._free[0] is connection

        with pytest.raises(ConnectionError, match="simulated write failure"):
            await node.execute_pipeline([PipelineCommand(0, "GET", "key")])

        assert len(node._connections) == 1
        assert len(node._free) == 1
        assert node._free[0] is connection
        assert connection.send_count == 2

    @pytest.mark.parametrize("large", [False, True])
    async def test_execute_pipeline_offloads_only_large_batches(self, large) -> None:
        """
        A batch at the offload threshold is packed in a worker thread, so a large
        cluster pipeline does not block the event loop (#3416). Smaller batches
        are packed inline.
        """
        node = ClusterNode(default_host, 7000)
        connection = mock.MagicMock(spec=Connection)
        connection.is_connected = False
        connection.should_reconnect.return_value = False
        connection.pack_commands.return_value = [b"packed"]
        node._connections.append(connection)
        node._free.append(connection)
        count = _PIPELINE_PACK_OFFLOAD_MIN_COMMANDS if large else 2
        commands = [PipelineCommand(i, "GET", "key") for i in range(count)]
        to_thread = mock.AsyncMock(return_value=[b"offloaded"])

        with (
            mock.patch.object(
                ClusterNode,
                "parse_response",
                new=mock.AsyncMock(return_value=b"value"),
            ),
            mock.patch("redis.asyncio.client.asyncio.to_thread", new=to_thread),
        ):
            assert await node.execute_pipeline(commands) is False

        if large:
            to_thread.assert_awaited_once_with(
                connection.pack_commands, [cmd.args for cmd in commands]
            )
            connection.pack_commands.assert_not_called()
            connection.send_packed_command.assert_awaited_once_with([b"offloaded"])
        else:
            to_thread.assert_not_called()
            connection.pack_commands.assert_called_once_with(
                [cmd.args for cmd in commands]
            )
            connection.send_packed_command.assert_awaited_once_with([b"packed"])
        assert [cmd.result for cmd in commands] == [b"value"] * count
        assert node._free[0] is connection

    async def test_execute_command_releases_connection_after_disconnect_error(
        self,
    ) -> None:
        node = ClusterNode(
            default_host,
            7000,
            max_connections=1,
            connection_class=self.WriteFailingConnection,
        )

        with mock.patch.object(
            ClusterNode,
            "disconnect_if_needed",
            autospec=True,
            side_effect=ConnectionError("simulated disconnect failure"),
        ):
            with pytest.raises(ConnectionError, match="simulated disconnect failure"):
                await node.execute_command("GET", "key")

        assert len(node._connections) == 1
        assert len(node._free) == 1
        assert node._free[0] is node._connections[0]

    async def test_execute_pipeline_releases_connection_after_disconnect_error(
        self,
    ) -> None:
        node = ClusterNode(
            default_host,
            7000,
            max_connections=1,
            connection_class=self.WriteFailingConnection,
        )

        with mock.patch.object(
            ClusterNode,
            "disconnect_if_needed",
            autospec=True,
            side_effect=ConnectionError("simulated disconnect failure"),
        ):
            with pytest.raises(ConnectionError, match="simulated disconnect failure"):
                await node.execute_pipeline([PipelineCommand(0, "GET", "key")])

        assert len(node._connections) == 1
        assert len(node._free) == 1
        assert node._free[0] is node._connections[0]

    async def test_update_active_connections_for_reconnect(self) -> None:
        """
        Test that update_active_connections_for_reconnect marks in-use connections.
        """
        node = ClusterNode(default_host, 7000)

        # Create mock connections
        conn1 = mock.AsyncMock(spec=Connection)
        conn2 = mock.AsyncMock(spec=Connection)
        conn3 = mock.AsyncMock(spec=Connection)
        conn1.is_connected = False
        conn2.is_connected = False
        conn3.is_connected = False

        # Add all connections to _connections
        node._connections = [conn1, conn2, conn3]
        # Only conn1 is free, conn2 and conn3 are "in-use"
        node._free.append(conn1)

        # Mark active connections for reconnect
        node.update_active_connections_for_reconnect()

        # conn1 is free, should NOT be marked
        conn1.mark_for_reconnect.assert_not_called()
        # conn2 and conn3 are in-use, should be marked
        conn2.mark_for_reconnect.assert_called_once()
        conn3.mark_for_reconnect.assert_called_once()

    async def test_disconnect_free_connections(self) -> None:
        """
        Test that disconnect_free_connections disconnects all free connections.
        """
        node = ClusterNode(default_host, 7000)

        # Create mock connections
        conn1 = mock.AsyncMock(spec=Connection)
        conn2 = mock.AsyncMock(spec=Connection)
        conn3 = mock.AsyncMock(spec=Connection)
        conn1.is_connected = False
        conn2.is_connected = False
        conn3.is_connected = False

        # Add all connections to _connections
        node._connections = [conn1, conn2, conn3]
        # conn1 and conn2 are free, conn3 is "in-use"
        node._free.append(conn1)
        node._free.append(conn2)

        # Disconnect free connections
        await node.disconnect_free_connections()

        # conn1 and conn2 should be disconnected
        conn1.disconnect.assert_called_once()
        conn2.disconnect.assert_called_once()
        # conn3 is in-use, should NOT be disconnected
        conn3.disconnect.assert_not_called()

        # Connections should still be in _free (not removed)
        assert conn1 in node._free
        assert conn2 in node._free

    async def test_disconnect_free_connections_empty(self) -> None:
        """
        Test that disconnect_free_connections handles empty _free gracefully.
        """
        node = ClusterNode(default_host, 7000)

        # No free connections
        assert len(node._free) == 0

        # Should not raise
        await node.disconnect_free_connections()

    async def test_disconnect_free_connections_not_acquirable_while_closing(
        self,
    ) -> None:
        """
        Test that a connection being disconnected cannot be acquired until its
        disconnect completes, and that it is back in _free afterwards.
        """
        node = ClusterNode(default_host, 7000, max_connections=2)
        closing = mock.AsyncMock(spec=Connection)
        released = mock.AsyncMock(spec=Connection)
        node._connections = [closing, released]
        node._free.append(closing)

        disconnect_started = asyncio.Event()
        finish_disconnect = asyncio.Event()

        async def slow_disconnect() -> None:
            disconnect_started.set()
            await finish_disconnect.wait()

        closing.disconnect.side_effect = slow_disconnect

        task = asyncio.create_task(node.disconnect_free_connections())
        await disconnect_started.wait()

        # The node is at max_connections and the only free one is closing
        with pytest.raises(MaxConnectionsError):
            node.acquire_connection()
        # A connection released meanwhile is acquirable
        node._free.append(released)
        assert node.acquire_connection() is released
        node._free.append(released)

        finish_disconnect.set()
        await task

        assert list(node._free) == [released, closing]

    async def test_execute_command_reuses_closed_remarked_connection_inline(
        self,
    ) -> None:
        class RaceConnection:
            def __init__(self, **kwargs: Any) -> None:
                self.host = kwargs["host"]
                self.port = kwargs["port"]
                self.db = kwargs.get("db", 0)
                self.is_connected = True
                self._reconnect = False
                self.send_started = asyncio.Event()
                self.response_ready = asyncio.Event()
                self.disconnect_started = asyncio.Event()
                self.allow_disconnect = asyncio.Event()
                self.disconnect_calls = 0
                self._resp2_fallback = False

            def pack_command(self, *args: Any) -> bytes:
                return b"packed-command"

            async def send_packed_command(self, command: Any) -> None:
                self.send_started.set()

            async def read_response(self, *args: Any, **kwargs: Any) -> bytes:
                await self.response_ready.wait()
                return b"value"

            def mark_for_reconnect(self) -> None:
                self._reconnect = True

            def should_reconnect(self) -> bool:
                return self._reconnect

            def reset_should_reconnect(self) -> None:
                self._reconnect = False

            async def disconnect(self) -> None:
                self.disconnect_calls += 1
                self.reset_should_reconnect()

                if not self.is_connected:
                    return

                self.disconnect_started.set()
                await self.allow_disconnect.wait()
                self.is_connected = False

        node = ClusterNode(
            default_host,
            7000,
            max_connections=1,
            connection_class=RaceConnection,
        )
        connection = RaceConnection(host=default_host, port=7000)
        node._connections = [connection]
        node._free.append(connection)

        async def remark_during_disconnect() -> None:
            await connection.send_started.wait()
            node.update_active_connections_for_reconnect()
            connection.response_ready.set()

            await connection.disconnect_started.wait()
            node.update_active_connections_for_reconnect()
            connection.allow_disconnect.set()

        remark_task = asyncio.create_task(remark_during_disconnect())

        assert await node.execute_command("GET", "key") == b"value"
        await remark_task

        assert node.acquire_connection() is connection
        assert node._background_tasks == set()
        assert connection.should_reconnect() is False
        assert connection.disconnect_calls == 1

    async def test_release_with_reconnect_flag(self) -> None:
        """
        Test that release() disconnects a connection marked for reconnect before
        adding it to _free.
        """

        class FakeConnection:
            def __init__(self) -> None:
                self.disconnect_called = False
                self.is_connected = True

            def should_reconnect(self) -> bool:
                return True

            async def disconnect(self) -> None:
                self.disconnect_called = True
                self.is_connected = False

        node = ClusterNode(default_host, 7000)
        conn = FakeConnection()

        node._connections = [conn]

        # Release schedules disconnect first because release() is synchronous.
        node.release(conn)

        assert conn not in node._free
        await asyncio.gather(*node._background_tasks)

        assert conn.disconnect_called is True
        assert conn.is_connected is False
        assert conn in node._free

    async def test_release_without_reconnect_flag(self) -> None:
        """
        Test that release() adds connection to _free without disconnect.
        """

        class FakeConnection:
            def __init__(self) -> None:
                self.disconnect_called = False
                self.is_connected = False

            def should_reconnect(self) -> bool:
                return False

            async def disconnect(self) -> None:
                self.disconnect_called = True

        node = ClusterNode(default_host, 7000)
        conn = FakeConnection()

        node._connections = [conn]

        # Release the connection
        node.release(conn)

        # Connection should NOT be disconnected but added to _free
        assert conn.disconnect_called is False
        assert conn in node._free

    async def test_release_debug_logs_disconnect_task_exception(self, caplog) -> None:
        """
        Test that release() consumes background disconnect errors.
        """

        class FakeConnection:
            is_connected = True

            def should_reconnect(self) -> bool:
                return True

            def extract_connection_details(self) -> str:
                # release() renders the connection into its debug log before
                # scheduling the disconnect, so the double must expose this.
                return "fake connection details"

            async def disconnect(self) -> None:
                raise RuntimeError("simulated disconnect failure")

        node = ClusterNode(default_host, 7000)
        conn = FakeConnection()
        node._connections = [conn]

        with caplog.at_level(logging.DEBUG, logger="redis.asyncio.cluster"):
            node.release(conn)
            assert len(node._background_tasks) == 1
            (task,) = tuple(node._background_tasks)
            await task
            await asyncio.sleep(0)

        assert node._background_tasks == set()
        assert conn not in node._free
        assert conn not in node._connections
        assert any(
            "disconnecting released cluster connection failed" in rec.message
            and rec.levelno == logging.DEBUG
            for rec in caplog.records
        )

    async def test_disconnect_if_needed_disconnects_when_reconnect_needed(
        self,
    ) -> None:
        """
        Test that disconnect_if_needed() disconnects a connection marked for reconnect.
        This implements lazy disconnect to avoid race conditions.
        """
        node = ClusterNode(default_host, 7000)

        # Create a mock connection marked for reconnect
        conn = mock.AsyncMock(spec=Connection)
        conn.should_reconnect.return_value = True

        # disconnect_if_needed should disconnect the connection
        await node.disconnect_if_needed(conn)

        conn.disconnect.assert_called_once()

    async def test_disconnect_if_needed_skips_when_no_reconnect_needed(self) -> None:
        """
        Test that disconnect_if_needed() does not disconnect if no reconnect needed.
        """
        node = ClusterNode(default_host, 7000)

        # Create a mock connection NOT marked for reconnect
        conn = mock.AsyncMock(spec=Connection)
        conn.should_reconnect.return_value = False

        # disconnect_if_needed should not disconnect
        await node.disconnect_if_needed(conn)

        conn.disconnect.assert_not_called()


@pytest.mark.fixed_client
class TestClusterConnectionErrorHandling:
    """Tests for cluster connection error handling behavior."""

    @pytest.mark.usefixtures("no_topology_refresh_interval", "no_cluster_retry_delay")
    async def test_connection_error_calls_move_node_to_end_of_cached_nodes(
        self,
    ) -> None:
        """
        Test that ConnectionError triggers move_node_to_end_of_cached_nodes
        instead of pop.
        """
        with mock.patch.object(
            NodesManager, "move_node_to_end_of_cached_nodes", autospec=True
        ) as move_node_to_end_of_cached_nodes:
            with mock.patch.object(ClusterNode, "execute_command") as execute_command:

                async def execute_command_mock(*args, **kwargs):
                    if args[0] == "CLUSTER SLOTS":
                        return default_cluster_slots
                    elif args[0] == "COMMAND":
                        return {"get": [], "set": []}
                    elif args[0] == "INFO":
                        return {"cluster_enabled": True}
                    elif len(args) > 1 and args[1] == "cluster-require-full-coverage":
                        return {"cluster-require-full-coverage": "yes"}
                    elif args[0] == "GET":
                        raise ConnectionError("Connection failed")
                    return None

                execute_command.side_effect = execute_command_mock

                with mock.patch.object(
                    AsyncCommandsParser, "initialize", autospec=True
                ) as cmd_parser_initialize:

                    def cmd_init_mock(self, node: Optional[ClusterNode] = None) -> None:
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

                    rc = await RedisCluster(host=default_host, port=7000)
                    initialize_calls = []
                    original_initialize = NodesManager.initialize

                    async def initialize(nodes_manager, *args, **kwargs):
                        if nodes_manager is rc.nodes_manager:
                            initialize_calls.append(
                                nodes_manager._requested_last_failed_node_name
                            )
                        return await original_initialize(nodes_manager, *args, **kwargs)

                    with mock.patch.object(
                        NodesManager,
                        "initialize",
                        autospec=True,
                        side_effect=initialize,
                    ):
                        with pytest.raises(ConnectionError):
                            await rc.get("foo")

                    # Verify move_node_to_end_of_cached_nodes was called
                    move_node_to_end_of_cached_nodes.assert_called()
                    # The refresh knew which node failed, to try it last
                    assert any(name is not None for name in initialize_calls)

    @pytest.mark.usefixtures("no_topology_refresh_interval", "no_cluster_retry_delay")
    async def test_connection_error_handles_node_connections(self) -> None:
        """
        A ConnectionError closes only the failed connection: the node's in-use
        connections are not recycled and its free ones are not closed at once;
        they are only marked for a lazy reconnect on acquire.
        """
        with mock.patch.object(
            ClusterNode,
            "update_active_connections_for_reconnect",
            autospec=True,
        ) as update_active:
            with mock.patch.object(
                ClusterNode, "disconnect_free_connections", autospec=True
            ) as disconnect_free:
                with mock.patch.object(
                    ClusterNode, "execute_command"
                ) as execute_command:

                    async def execute_command_mock(*args, **kwargs):
                        if args[0] == "CLUSTER SLOTS":
                            return default_cluster_slots
                        elif args[0] == "COMMAND":
                            return {"get": [], "set": []}
                        elif args[0] == "INFO":
                            return {"cluster_enabled": True}
                        elif (
                            len(args) > 1 and args[1] == "cluster-require-full-coverage"
                        ):
                            return {"cluster-require-full-coverage": "yes"}
                        elif args[0] == "GET":
                            raise ConnectionError("Connection failed")
                        return None

                    execute_command.side_effect = execute_command_mock

                    with mock.patch.object(
                        AsyncCommandsParser, "initialize", autospec=True
                    ) as cmd_parser_initialize:

                        def cmd_init_mock(
                            self, node: Optional[ClusterNode] = None
                        ) -> None:
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

                        rc = await RedisCluster(host=default_host, port=7000)
                        with pytest.raises(ConnectionError):
                            await rc.get("foo")

                        update_active.assert_not_called()
                        disconnect_free.assert_not_called()


@pytest.mark.onlycluster
class TestClusterPipeline:
    """Tests for the ClusterPipeline class."""

    @pytest.mark.parametrize("dynamic_startup_nodes", [False, True])
    @pytest.mark.parametrize("use_default_node", [False, True])
    async def test_lazy_transaction_does_not_reuse_private_loop_connections(
        self,
        r: RedisCluster,
        request,
        dynamic_startup_nodes,
        use_default_node,
    ) -> None:
        default_node = r.get_default_node()
        key = next(
            f"lazy-transaction-{i}"
            for i in range(1000)
            if (r.get_node_from_key(f"lazy-transaction-{i}") == default_node)
            == use_default_node
        )
        client = RedisCluster.from_url(
            request.config.getoption("--redis-url"),
            protocol=get_protocol_version(r),
            dynamic_startup_nodes=dynamic_startup_nodes,
        )
        try:
            pipe = client.pipeline(transaction=True)
            assert pipe.set(key, "value") is pipe
            nodes = (
                list(client.nodes_manager.startup_nodes.values()) + client.get_nodes()
            )
            assert all(
                not connection.is_connected
                for node in nodes
                for connection in node._connections
            )
            assert await pipe.execute() == [True]
            assert await client.get(key) == b"value"
            assert await client.ping(target_nodes=RedisCluster.PRIMARIES)
            # Queuing again must preserve the caller's connections and WATCH state.
            await pipe.watch(key)
            assert await pipe.get(key) == b"value"
            pipe.multi()
            assert pipe.set(key, "updated") is pipe
            assert await pipe.execute() == [True]
            assert await client.get(key) == b"updated"
            # Queuing movablekeys commands (COMMAND GETKEYS) on an initialized
            # transaction pipeline must neither reuse caller-loop sockets on the
            # private worker loop nor leave private-loop sockets open afterward.
            zset_a = f"{{{key}}}:zset-a"
            zset_b = f"{{{key}}}:zset-b"
            zset_out = f"{{{key}}}:zset-out"
            await client.zadd(zset_a, {"member": 1})
            await client.zadd(zset_b, {"member": 2})
            assert pipe.zunionstore(zset_out, [zset_a, zset_b]) is pipe
            assert await pipe.execute() == [1]
            assert await client.zscore(zset_out, "member") == 3.0
            await client.delete(key, zset_a, zset_b, zset_out)
        finally:
            await client.aclose()

    async def test_blocked_arguments(self, r: RedisCluster) -> None:
        """Test handling for blocked pipeline arguments."""

        with pytest.raises(RedisClusterException) as ex:
            r.pipeline(shard_hint=True)

        assert str(ex.value) == "shard_hint is deprecated in cluster mode"

    async def test_blocked_methods(self, r: RedisCluster) -> None:
        """Test handling for blocked pipeline commands."""
        pipeline = r.pipeline()
        for command in PIPELINE_BLOCKED_COMMANDS:
            command = command.replace(" ", "_").lower()
            if command == "mset_nonatomic":
                continue

            with pytest.raises(RedisClusterException) as exc:
                getattr(pipeline, command)()

            assert str(exc.value) == (
                f"ERROR: Calling pipelined function {command} is blocked "
                "when running redis in cluster mode..."
            )

    async def test_evalsha_in_pipeline(self, r: RedisCluster) -> None:
        """
        EVALSHA is allowed in ClusterPipeline when keys map to one slot
        and the script is already loaded on cluster primaries (#2914).
        """
        multiply = "return redis.call('GET', KEYS[1]) * ARGV[1]"
        sha = await r.script_load(multiply)
        await r.set("{user}a", 2)
        async with r.pipeline() as pipe:
            pipe.evalsha(sha, 1, "{user}a", 3)
            assert await pipe.execute() == [6]

    @skip_if_server_version_lt("7.2.0")
    async def test_zrank_withscore_decode_responses(self, create_redis) -> None:
        """
        A cluster pipeline reads decode_responses from its cluster client, so
        custom-cast ZRANK/ZREVRANK scores come back as str, as on RESP2.
        """
        r = await create_redis(cls=RedisCluster, decode_responses=True)
        identity = lambda x: x  # noqa: E731
        await r.zadd("a", {"a1": 1, "a2": 2.5})
        async with r.pipeline() as pipe:
            pipe.zrank("a", "a2", withscore=True, score_cast_func=identity)
            pipe.zrevrank("a", "a2", withscore=True, score_cast_func=identity)
            result = await pipe.execute()
        assert_resp_response(
            r,
            result,
            [[1, "2.5"], [0, "2.5"]],
            [[1, 2.5], [0, 2.5]],
        )

    async def test_evalsha_zero_keys_in_pipeline(self, r: RedisCluster) -> None:
        """
        EVALSHA with numkeys=0 must execute in ClusterPipeline without
        calling COMMAND GETKEYS (broken on Redis <7 for this case).
        """
        sha = await r.script_load("return 42")
        async with r.pipeline() as pipe:
            pipe.evalsha(sha, 0)
            assert await pipe.execute() == [42]

    async def test_evalsha_zero_keys_in_transaction_pipeline(
        self, r: RedisCluster
    ) -> None:
        """
        Multiple zero-key EVALSHA calls in a transactional pipeline share
        one slot and execute successfully.
        """
        sha = await r.script_load("return 42")
        async with r.pipeline(transaction=True) as pipe:
            pipe.evalsha(sha, 0)
            pipe.evalsha(sha, 0)
            assert await pipe.execute() == [42, 42]

    async def test_empty_stack(self, r: RedisCluster) -> None:
        """If a pipeline is executed with no commands it should return a empty list."""
        p = r.pipeline()
        result = await p.execute()
        assert result == []

    async def test_redis_cluster_pipeline(self, r: RedisCluster) -> None:
        """Test that we can use a pipeline with the RedisCluster class"""
        result = await (
            r.pipeline()
            .set("A", 1)
            .get("A")
            .hset("K", "F", "V")
            .hgetall("K")
            .mset_nonatomic({"A": 2, "B": 3})
            .get("A")
            .get("B")
            .delete("A", "B", "K")
            .execute()
        )
        assert result == [True, b"1", 1, {b"F": b"V"}, True, True, b"2", b"3", 1, 1, 1]

    async def test_cluster_pipeline_execution_zero_cluster_err_retries(
        self, r: RedisCluster
    ) -> None:
        """
        Test that we can run successfully cluster pipeline execute at least once when
        cluster_error_retry_attempts is set to 0
        """
        r.cluster_error_retry_attempts = 0
        result = await r.pipeline().set("A", 1).get("A").delete("A").execute()
        assert result == [True, b"1", 1]

    async def test_multi_key_operation_with_a_single_slot(
        self, r: RedisCluster
    ) -> None:
        """Test multi key operation with a single slot."""
        pipe = r.pipeline()
        pipe.set("a{foo}", 1)
        pipe.set("b{foo}", 2)
        pipe.set("c{foo}", 3)
        pipe.get("a{foo}")
        pipe.get("b{foo}")
        pipe.get("c{foo}")

        res = await pipe.execute()
        assert res == [True, True, True, b"1", b"2", b"3"]

    async def test_multi_key_operation_with_multi_slots(self, r: RedisCluster) -> None:
        """Test multi key operation with more than one slot."""
        pipe = r.pipeline()
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
        res = await pipe.execute()
        assert res == [True, True, True, True, True, b"1", b"2", b"3", b"4", b"5"]

    @pytest.mark.usefixtures("no_topology_refresh_interval")
    async def test_cluster_down_error(self, r: RedisCluster) -> None:
        """
        Test that the pipeline retries the specified in retry object times before raising
        an error.
        """
        key = "foo"
        node = r.get_node_from_key(key, False)

        parse_response_orig = node.parse_response
        with mock.patch.object(
            ClusterNode, "parse_response", autospec=True
        ) as parse_response_mock:

            async def parse_response(
                self, connection: Connection, command: str, **kwargs: Any
            ) -> Any:
                response = await parse_response_orig(connection, command, **kwargs)
                if command == "GET":
                    # A real CLUSTERDOWN is a reply that was read off the socket;
                    # read the GET reply first so the connection, which the retry
                    # keeps, is not left holding it
                    raise ClusterDownError("error")
                return response

            parse_response_mock.side_effect = parse_response

            # every attempt fails, so one retry is enough to test the count
            r.set_retry(Retry(NoBackoff(), 1))
            # The first GET fails, then each retry refreshes the topology
            # (CLUSTER SLOTS; the command table is fetched only once per client)
            # and sends the GET again
            async with r.pipeline() as pipe:
                with pytest.raises(ClusterDownError):
                    await pipe.get(key).execute()
                assert node.parse_response.await_count == 2 * r.retry.get_retries() + 1

    async def test_connection_error_not_raised(self, r: RedisCluster) -> None:
        """Test ConnectionError handling with raise_on_error=False."""
        key = "foo"
        node = r.get_node_from_key(key, False)

        parse_response_orig = node.parse_response
        with mock.patch.object(
            ClusterNode, "parse_response", autospec=True
        ) as parse_response_mock:

            async def parse_response(
                self, connection: Connection, command: str, **kwargs: Any
            ) -> Any:
                if command == "GET":
                    raise ConnectionError("error")
                return await parse_response_orig(connection, command, **kwargs)

            parse_response_mock.side_effect = parse_response

            async with r.pipeline() as pipe:
                res = await pipe.get(key).get(key).execute(raise_on_error=False)
                assert node.parse_response.await_count
                assert isinstance(res[0], ConnectionError)

    async def test_connection_error_raised(self, r: RedisCluster) -> None:
        """Test ConnectionError handling with raise_on_error=True."""
        key = "foo"
        node = r.get_node_from_key(key, False)

        parse_response_orig = node.parse_response
        with mock.patch.object(
            ClusterNode, "parse_response", autospec=True
        ) as parse_response_mock:

            async def parse_response(
                self, connection: Connection, command: str, **kwargs: Any
            ) -> Any:
                if command == "GET":
                    raise ConnectionError("error")
                return await parse_response_orig(connection, command, **kwargs)

            parse_response_mock.side_effect = parse_response

            # every GET fails, so skip the futile retries
            r.set_retry(Retry(NoBackoff(), 0))
            async with r.pipeline() as pipe:
                with pytest.raises(ConnectionError):
                    await pipe.get(key).get(key).execute(raise_on_error=True)

    async def test_asking_error(self, r: RedisCluster) -> None:
        """Test AskError handling."""
        key = "foo"
        first_node = r.get_node_from_key(key, False)
        ask_node = None
        for node in r.get_nodes():
            if node != first_node:
                ask_node = node
                break
        ask_msg = f"{r.keyslot(key)} {ask_node.host}:{ask_node.port}"

        async with r.pipeline() as pipe:
            mock_node_resp_exc(first_node, AskError(ask_msg))
            mock_node_resp(ask_node, "MOCK_OK")
            res = await pipe.get(key).execute()
            assert first_node._free.pop().read_response.await_count
            assert ask_node._free.pop().read_response.await_count
            assert res == ["MOCK_OK"]

    async def test_error_is_truncated(self, r) -> None:
        """
        Test that an error from the pipeline is truncated correctly.
        """
        key = "a" * 50
        a_value = "a" * 20
        b_value = "b" * 20

        async with r.pipeline() as pipe:
            pipe.set(key, 1)
            pipe.hset(key, mapping={"field_a": a_value, "field_b": b_value})
            pipe.expire(key, 100)

            with pytest.raises(Exception) as ex:
                await pipe.execute()

            expected = f"Command # 2 (HSET {key} field_a {a_value} field_b...) of pipeline caused error: "
            assert str(ex.value).startswith(expected)

    async def test_moved_redirection_on_slave_with_default(
        self, r: RedisCluster
    ) -> None:
        """Test MovedError handling."""
        key = "foo"
        await r.set("foo", "bar")
        # set read_from_replicas to True
        r.read_from_replicas = True
        primary = r.get_node_from_key(key, False)
        moved_error = f"{r.keyslot(key)} {primary.host}:{primary.port}"

        parse_response_orig = primary.parse_response
        with mock.patch.object(
            ClusterNode, "parse_response", autospec=True
        ) as parse_response_mock:

            async def parse_response(
                self, connection: Connection, command: str, **kwargs: Any
            ) -> Any:
                if command == "GET" and self.port != primary.port:
                    raise MovedError(moved_error)

                return await parse_response_orig(connection, command, **kwargs)

            parse_response_mock.side_effect = parse_response

            async with r.pipeline() as readwrite_pipe:
                assert r.reinitialize_counter == 0
                readwrite_pipe.get(key).get(key)
                assert r.reinitialize_counter == 0
                assert await readwrite_pipe.execute() == [b"bar", b"bar"]

    async def test_readonly_pipeline_from_readonly_client(
        self, r: RedisCluster
    ) -> None:
        """Test that the pipeline uses replicas for read_from_replicas clients."""
        # Create a cluster with reading from replications
        r.read_from_replicas = True
        key = "bar"
        await r.set(key, "foo")

        async with r.pipeline() as pipe:
            mock_all_nodes_resp(r, "MOCK_OK")
            assert await pipe.get(key).get(key).execute() == ["MOCK_OK", "MOCK_OK"]
            slot_nodes = r.nodes_manager.slots_cache[r.keyslot(key)]
            executed_on_replica = False
            for node in slot_nodes:
                if node.server_type == REPLICA:
                    if node._free.pop().read_response.await_count:
                        executed_on_replica = True
                        break
            assert executed_on_replica

    @pytest.mark.parametrize(
        "load_balancing_strategy",
        [
            LoadBalancingStrategy.ROUND_ROBIN_REPLICAS,
            LoadBalancingStrategy.RANDOM_REPLICA,
        ],
    )
    async def test_readonly_pipeline_with_reading_from_replicas_strategies(
        self, r: RedisCluster, load_balancing_strategy: LoadBalancingStrategy
    ) -> None:
        """
        Test that the pipeline uses replicas for different replica-based
        load balancing strategies.
        """
        # Set the load balancing strategy
        r.load_balancing_strategy = load_balancing_strategy
        key = "bar"
        await r.set(key, "foo")

        async with r.pipeline() as pipe:
            mock_all_nodes_resp(r, "MOCK_OK")
            assert await pipe.get(key).get(key).execute() == ["MOCK_OK", "MOCK_OK"]
            slot_nodes = r.nodes_manager.slots_cache[r.keyslot(key)]
            executed_on_replicas_only = True
            for node in slot_nodes:
                if node.server_type == PRIMARY:
                    if node._free.pop().read_response.await_count > 0:
                        executed_on_replicas_only = False
                        break
            assert executed_on_replicas_only

    async def test_can_run_concurrent_pipelines(
        self, create_redis: Callable[..., RedisCluster]
    ) -> None:
        """Test that the pipeline can be used concurrently."""
        r = await create_redis(max_connections=1000)
        await asyncio.gather(
            *(self.test_redis_cluster_pipeline(r) for i in range(100)),
            *(self.test_multi_key_operation_with_a_single_slot(r) for i in range(100)),
            *(self.test_multi_key_operation_with_multi_slots(r) for i in range(100)),
        )

    @pytest.mark.onlycluster
    async def test_pipeline_with_default_node_error_command(self, create_redis):
        """
        Test that the default node is being replaced when it raises a relevant exception
        """
        r = await create_redis(cls=RedisCluster, flushdb=False)
        curr_default_node = r.get_default_node()
        err = ConnectionError("error")
        cmd_count = await r.command_count()
        mock_node_resp_exc(curr_default_node, err)
        async with r.pipeline(transaction=False) as pipe:
            pipe.command_count()
            result = await pipe.execute(raise_on_error=False)
            assert result[0] == err
            assert r.get_default_node() != curr_default_node
            pipe.command_count()
            result = await pipe.execute(raise_on_error=False)
            assert result[0] == cmd_count


@pytest.mark.fixed_client
class TestClusterPipelineMocked:
    """ClusterPipeline behaviour checked against mocked nodes; no server needed."""

    @pytest.mark.parametrize("initialized", [False, True])
    @pytest.mark.parametrize("slot_error", [False, True])
    @pytest.mark.parametrize("disconnect_error", [False, True])
    async def test_transaction_closes_only_connections_initialized_on_private_loop(
        self, initialized, slot_error, disconnect_error
    ) -> None:
        client = RedisCluster(host=default_host, port=default_port)
        startup_node = next(iter(client.nodes_manager.startup_nodes.values()))
        # Equal addresses need not mean identical node objects or connection pools.
        cached_node = ClusterNode(default_host, default_port)
        connections = []
        disconnect_loops = []
        initialize_loops = []
        for node in (startup_node, cached_node):
            connection = mock.AsyncMock(spec=Connection)
            connection.is_connected = True

            async def disconnect(connection=connection):
                disconnect_loops.append(asyncio.get_running_loop())
                connection.is_connected = False
                if (
                    disconnect_error
                    and not initialized
                    and connection.disconnect.await_count == 1
                ):
                    raise TimeoutError("close timed out")

            connection.disconnect.side_effect = disconnect
            node._connections.append(connection)
            connections.append(connection)

        def set_topology():
            client.commands_parser.node = cached_node
            client.nodes_manager.default_node = cached_node
            client.nodes_manager.nodes_cache[cached_node.name] = cached_node
            client._initialize = False
            client._topology_ready = True

        async def initialize(self):
            initialize_loops.append(asyncio.get_running_loop())
            set_topology()
            return self

        async def determine_slot(self, *args):
            if slot_error:
                raise RedisClusterException("slot resolution failed")
            return 123

        if initialized:
            set_topology()
        try:
            with (
                mock.patch.object(RedisCluster, "initialize", initialize),
                mock.patch.object(RedisCluster, "_determine_slot", determine_slot),
            ):
                pipe = client.pipeline(transaction=True)
                if slot_error:
                    with pytest.raises(
                        RedisClusterException, match="slot resolution failed"
                    ):
                        pipe.get("key")
                else:
                    assert pipe.get("key") is pipe
                    assert pipe._execution_strategy._command_queue[0].args == (
                        "GET",
                        "key",
                    )

            assert client.get_default_node() is cached_node
            assert len(initialize_loops) == (0 if initialized else 1)
            assert len(disconnect_loops) == (0 if initialized else 2)
            for connection in connections:
                assert connection.is_connected is initialized
            if not initialized:
                assert all(loop is initialize_loops[0] for loop in disconnect_loops)
        finally:
            await client.aclose()

    @pytest.mark.parametrize("caller_connected", [False, True])
    @pytest.mark.parametrize("slot_error", [False, True])
    @pytest.mark.parametrize("disconnect_error", [False, True])
    async def test_transaction_movablekeys_isolates_caller_and_private_loop_connections(
        self, caller_connected, slot_error, disconnect_error
    ) -> None:
        client = RedisCluster(host=default_host, port=default_port, max_connections=3)
        parser_node = ClusterNode(
            default_host, default_port, **client.nodes_manager.connection_kwargs
        )
        caller_loop = asyncio.get_running_loop()
        parser_io_nodes = []
        parser_io_loops = []
        disconnect_nodes = []
        disconnect_loops = []
        private_connections = []

        for _ in range(parser_node.max_connections):
            connection = mock.AsyncMock(spec=Connection)
            connection.is_connected = caller_connected
            connection.should_reconnect.return_value = False
            connection.pack_command.return_value = [b"COMMAND GETKEYS"]
            connection._resp2_fallback = False
            connection.read_response.return_value = ["{tag}a", "{tag}b"]

            async def disconnect_caller(connection=connection):
                connection.is_connected = False

            connection.disconnect.side_effect = disconnect_caller
            parser_node._connections.append(connection)
        parser_node._free.extend(reversed(parser_node._connections))
        caller_connections = list(parser_node._connections)
        caller_free = list(parser_node._free)
        connections_pool = parser_node._connections
        free_pool = parser_node._free

        def make_private_connection(**kwargs):
            connection = mock.AsyncMock(spec=Connection)
            connection._resp2_fallback = False
            connection.is_connected = False
            connection.should_reconnect.return_value = False

            async def send_packed_command(*args, **kwargs):
                parser_io_nodes.append(client.commands_parser.node)
                parser_io_loops.append(asyncio.get_running_loop())
                connection.is_connected = True

            async def disconnect():
                disconnect_loops.append(asyncio.get_running_loop())
                connection.is_connected = False
                if disconnect_error:
                    raise TimeoutError("close timed out")

            connection.send_packed_command.side_effect = send_packed_command
            connection.disconnect.side_effect = disconnect
            connection.pack_command.return_value = [b"COMMAND GETKEYS"]
            if slot_error:
                connection.read_response.side_effect = RedisClusterException(
                    "slot resolution failed"
                )
            else:
                connection.read_response.return_value = ["{tag}a", "{tag}b"]
            private_connections.append(connection)
            return connection

        node_disconnect = ClusterNode.disconnect

        async def disconnect_node(node):
            disconnect_nodes.append(node)
            # Restore the caller's parser before cleanup, even if cleanup raises.
            assert client.commands_parser.node is parser_node
            await node_disconnect(node)

        client.nodes_manager.connection_kwargs["connection_class"] = (
            make_private_connection
        )
        parser_node.connection_class = make_private_connection
        client.nodes_manager.default_node = parser_node
        client.nodes_manager.nodes_cache[parser_node.name] = parser_node
        client._initialize = False
        client._topology_ready = True
        client.commands_parser.node = parser_node
        client.commands_parser.commands = {
            "zunionstore": {"name": "zunionstore", "flags": ["movablekeys"]}
        }

        try:
            with mock.patch.object(ClusterNode, "disconnect", disconnect_node):
                pipe = client.pipeline(transaction=True)
                if slot_error:
                    with pytest.raises(
                        RedisClusterException, match="slot resolution failed"
                    ):
                        pipe.zunionstore("{tag}out", ["{tag}a", "{tag}b"])
                    assert not pipe._execution_strategy._command_queue
                else:
                    assert pipe.zunionstore("{tag}out", ["{tag}a", "{tag}b"]) is pipe
                    assert pipe._execution_strategy._command_queue[0].args == (
                        "ZUNIONSTORE",
                        "{tag}out",
                        2,
                        "{tag}a",
                        "{tag}b",
                    )

            assert client.commands_parser.node is parser_node
            assert client.get_default_node() is parser_node
            assert parser_node._connections is connections_pool
            assert parser_node._connections == caller_connections
            assert parser_node._free is free_pool
            assert list(parser_node._free) == caller_free
            assert parser_node._free.maxlen == parser_node.max_connections
            for connection in caller_connections:
                assert connection.is_connected is caller_connected
                connection.send_packed_command.assert_not_awaited()
                connection.disconnect.assert_not_awaited()

            assert len(parser_io_nodes) == 1
            temporary_node = parser_io_nodes[0]
            assert temporary_node is not parser_node
            assert temporary_node.host == parser_node.host
            assert temporary_node.port == parser_node.port
            assert temporary_node.max_connections == parser_node.max_connections
            assert temporary_node.connection_class is make_private_connection
            assert len(disconnect_nodes) == 1
            assert disconnect_nodes[0] is temporary_node
            assert len(parser_io_loops) == 1
            assert parser_io_loops[0] is not caller_loop
            assert disconnect_loops == parser_io_loops
            assert len(private_connections) == 1
            private_connections[0].disconnect.assert_awaited_once()
            assert not private_connections[0].is_connected
        finally:
            await client.aclose()

    async def test_async_script_queues_evalsha_on_cluster_pipeline(self) -> None:
        """AsyncScript must queue EVALSHA on ClusterPipeline without dropping it."""
        r = await get_mocked_redis_client(host=default_host, port=default_port)
        try:
            script = r.register_script("return 1")
            async with r.pipeline() as pipe:
                await script(client=pipe)
                queue = pipe._execution_strategy._command_queue
                assert len(queue) == 1
                assert queue[0].args[0] == "EVALSHA"
                assert queue[0].args[1] == script.sha
        finally:
            await r.aclose()

    async def test_client_list_iter_blocked_on_cluster_pipeline(self) -> None:
        """
        client_list_iter sends the same CLIENT LIST command as client_list,
        which PIPELINE_BLOCKED_COMMANDS blocks on ClusterPipeline.
        client_list_iter has no wire-command entry of its own to add there,
        so it must be blocked explicitly too, or it would fall through to
        the inherited implementation and queue CLIENT LIST like a real
        pipelined command instead of raising.
        """
        r = await get_mocked_redis_client(host=default_host, port=default_port)
        try:
            pipe = r.pipeline()
            with pytest.raises(RedisClusterException):
                pipe.client_list_iter()
        finally:
            await r.aclose()

    async def test_evalsha_not_blocked_on_cluster_pipeline(self) -> None:
        """EVALSHA must be usable on async ClusterPipeline (see #2914)."""
        assert "EVALSHA" not in PIPELINE_BLOCKED_COMMANDS
        r = await get_mocked_redis_client(host=default_host, port=default_port)
        try:
            pipe = r.pipeline()
            sha = "a" * 40
            returned = pipe.evalsha(sha, 1, "foo", "bar")
            assert returned is pipe
            queue = pipe._execution_strategy._command_queue
            assert len(queue) == 1
            assert queue[0].args == ("EVALSHA", sha, 1, "foo", "bar")
        finally:
            await r.aclose()

    async def test_evalsha_zero_keys_follows_keyed_slot_in_transaction(self) -> None:
        """Zero-key EVALSHA after a keyed slot is fixed reuses that slot."""
        r = await get_mocked_redis_client(host=default_host, port=default_port)
        try:
            sha = "a" * 40
            async with r.pipeline(transaction=True) as pipe:
                strategy = pipe._execution_strategy
                strategy._pipeline_slots = {key_slot(b"foo")}
                strategy._transaction_has_keyed_slot = True
                pipe.evalsha(sha, 0)
                assert strategy._pipeline_slots == {key_slot(b"foo")}
        finally:
            await r.aclose()

    async def test_evalsha_zero_keys_pipeline_execute(self) -> None:
        """Async ClusterPipeline must execute EVALSHA with numkeys=0."""
        r = await get_mocked_redis_client(host=default_host, port=default_port)
        try:
            mock_all_nodes_resp(r, 42)
            sha = "a" * 40
            async with r.pipeline() as pipe:
                pipe.evalsha(sha, 0)
                assert await pipe.execute() == [42]
        finally:
            await r.aclose()

    async def test_redirect_retry_error_without_args_is_raised(self) -> None:
        """An argless error from the redirect retry keeps its type when raised."""
        r = await get_mocked_redis_client(
            host=default_host, port=default_port, retry=Retry(NoBackoff(), 0)
        )
        try:
            key = "foo"
            first_node = r.get_node_from_key(key)
            ask_node = next(n for n in r.get_nodes() if n != first_node)
            ask_msg = f"{r.keyslot(key)} {ask_node.host}:{ask_node.port}"
            mock_node_resp_exc(first_node, AskError(ask_msg))
            # Exhaust the redirect target's pool so the retry raises an
            # argless MaxConnectionsError() from acquire_connection().
            ask_node._free.clear()
            ask_node.max_connections = 0

            async with r.pipeline() as pipe:
                with pytest.raises(MaxConnectionsError) as ex:
                    await pipe.get(key).execute()
            assert str(ex.value) == (
                "Command # 1 (GET foo) of pipeline caused error: MaxConnectionsError"
            )
        finally:
            await r.aclose()

    async def test_evalsha_zero_keys_reuses_slot_in_transaction(self) -> None:
        """Zero-key EVALSHA in a transactional async pipeline reuses one slot."""
        r = await get_mocked_redis_client(host=default_host, port=default_port)
        try:
            sha = "a" * 40
            async with r.pipeline(transaction=True) as pipe:
                pipe.evalsha(sha, 0)
                pipe.evalsha(sha, 0)
                slots = pipe._execution_strategy._pipeline_slots
                assert len(slots) == 1

                keyed_slot = key_slot(b"foo")

                async def _fake_determine_slot(*_args, **_kwargs):
                    return keyed_slot

                with mock.patch.object(
                    pipe.cluster_client,
                    "_determine_slot",
                    side_effect=_fake_determine_slot,
                ):
                    pipe.set("foo", "bar")
                assert pipe._execution_strategy._pipeline_slots == {keyed_slot}
                assert pipe._execution_strategy._transaction_has_keyed_slot is True
        finally:
            await r.aclose()

    async def test_evalsha_zero_keys_then_cross_slot_raises_at_execute(self) -> None:
        """
        Zero-key EVALSHA retarget must not swallow a later true cross-slot
        conflict: two keyed commands on different slots still raise at execute().
        """
        r = await get_mocked_redis_client(host=default_host, port=default_port)
        try:
            sha = "a" * 40
            slot_a = key_slot(b"{foo}a")
            slot_b = key_slot(b"{bar}b")
            assert slot_a != slot_b
            async with r.pipeline(transaction=True) as pipe:
                slots = iter([111, slot_a, slot_b])

                async def _fake_determine_slot(*_args, **_kwargs):
                    return next(slots)

                with mock.patch.object(
                    pipe.cluster_client,
                    "_determine_slot",
                    side_effect=_fake_determine_slot,
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
                    await pipe.execute()
        finally:
            await r.aclose()

    async def test_pipeline_nodes_manager_property(self) -> None:
        """
        Test that ClusterPipeline exposes nodes_manager property
        that delegates to the cluster client's nodes_manager.
        """
        r = await get_mocked_redis_client(host=default_host, port=default_port)
        try:
            pipeline = r.pipeline()
            # Verify that nodes_manager property exists and returns the same object
            # as the cluster client's nodes_manager
            assert pipeline.nodes_manager is r.nodes_manager
            # Verify that we can access nodes_manager attributes
            assert pipeline.nodes_manager.default_node is not None
        finally:
            await r.aclose()

    async def test_pipeline_set_response_callback(self) -> None:
        """
        Test that ClusterPipeline exposes set_response_callback method
        that delegates to the cluster client's set_response_callback.
        """
        r = await get_mocked_redis_client(host=default_host, port=default_port)
        try:
            pipeline = r.pipeline()

            # Define a custom callback
            def custom_callback(response):
                return f"custom_{response}"

            # Set the callback via the pipeline
            pipeline.set_response_callback("CUSTOM_CMD", custom_callback)

            # Verify that the callback was set on the cluster client
            assert "CUSTOM_CMD" in r.response_callbacks
            assert r.response_callbacks["CUSTOM_CMD"] is custom_callback
        finally:
            await r.aclose()

    async def test_slotless_command_does_not_lock_keyed_slot_flag(self) -> None:
        """Slotless commands must not block later keyed retargeting."""
        r = await get_mocked_redis_client(host=default_host, port=default_port)
        try:
            sha = "a" * 40
            async with r.pipeline(transaction=True) as pipe:
                pipe.evalsha(sha, 0)
                assert pipe._execution_strategy._transaction_has_keyed_slot is False

                async def _no_slot(*_args, **_kwargs):
                    return None

                with mock.patch.object(
                    pipe.cluster_client,
                    "_determine_slot",
                    side_effect=_no_slot,
                ):
                    pipe.execute_command("CLIENT TRACKING", "ON")
                assert pipe._execution_strategy._transaction_has_keyed_slot is False

                keyed_slot = key_slot(b"foo")

                async def _fake_determine_slot(*_args, **_kwargs):
                    return keyed_slot

                with mock.patch.object(
                    pipe.cluster_client,
                    "_determine_slot",
                    side_effect=_fake_determine_slot,
                ):
                    pipe.set("foo", "bar")
                assert pipe._execution_strategy._pipeline_slots == {keyed_slot}
                assert pipe._execution_strategy._transaction_has_keyed_slot is True
        finally:
            await r.aclose()

    @pytest.mark.parametrize("large", [False, True])
    async def test_transaction_offloads_only_large_batches(self, large) -> None:
        """
        A transaction whose MULTI ... EXEC batch reaches the offload threshold is
        packed in a worker thread, so it does not block the event loop (#3416).
        Smaller transactions are packed inline.
        """
        r = await get_mocked_redis_client(host=default_host, port=default_port)
        try:
            node = r.get_node(host=default_host, port=7000)
            connection = mock.MagicMock(spec=Connection)
            connection.should_reconnect.return_value = False
            connection.pack_commands.return_value = [b"packed"]
            # Stop the attempt right after the batch is packed
            connection.send_packed_command.side_effect = RuntimeError("stop")
            # MULTI and EXEC bring the large batch up to the threshold
            count = _PIPELINE_PACK_OFFLOAD_MIN_COMMANDS - 2 if large else 2
            stack = [PipelineCommand(i, "SET", "key", i) for i in range(count)]
            strategy = r.pipeline(transaction=True)._execution_strategy
            to_thread = mock.AsyncMock(return_value=[b"offloaded"])

            with (
                mock.patch.object(
                    type(strategy),
                    "_get_client_and_connection_for_transaction",
                    return_value=(node, connection),
                ),
                mock.patch("redis.asyncio.client.asyncio.to_thread", new=to_thread),
            ):
                with pytest.raises(RuntimeError, match="stop"):
                    await strategy._execute_transaction(stack, raise_on_error=True)

            if large:
                to_thread.assert_awaited_once()
                assert to_thread.await_args.args[0] == connection.pack_commands
                packed = to_thread.await_args.args[1]
                connection.pack_commands.assert_not_called()
                connection.send_packed_command.assert_awaited_once_with([b"offloaded"])
            else:
                to_thread.assert_not_called()
                connection.pack_commands.assert_called_once()
                packed = connection.pack_commands.call_args.args[0]
                connection.send_packed_command.assert_awaited_once_with([b"packed"])
            assert packed[0] == ("MULTI",)
            assert packed[1:-1] == [cmd.args for cmd in stack]
            assert packed[-1] == ("EXEC",)
        finally:
            await r.aclose()

    async def test_transaction_keeps_held_connection_when_slot_owner_changes(
        self,
    ) -> None:
        """
        If the slot map changes while a transaction holds a connection, the
        held (possibly WATCHing) connection must be kept, and reset() must
        return it to the pool of the node that owns it, not the newly
        resolved slot owner (#4253).
        """
        r = await get_mocked_redis_client(host=default_host, port=default_port)
        try:
            original_node = r.get_node(host=default_host, port=7000)
            new_node = r.get_node(host=default_host, port=7001)
            assert original_node is not None and new_node is not None
            assert original_node.name != new_node.name

            def _seed_connection(node: ClusterNode) -> Connection:
                node._free.clear()
                conn = mock.AsyncMock(spec=Connection)
                conn.host = node.host
                conn.port = node.port
                conn.should_reconnect.return_value = False
                node._connections.append(conn)
                node._free.append(conn)
                return conn

            original_conn = _seed_connection(original_node)
            new_conn = _seed_connection(new_node)

            slot = 0
            r.nodes_manager.slots_cache[slot] = [original_node]

            pipe = r.pipeline(transaction=True)
            strategy = pipe._execution_strategy
            # Simulate a mid-WATCH hold without talking to Redis
            strategy._pipeline_slots = {slot}
            strategy._watching = True
            node, conn = strategy._get_client_and_connection_for_transaction()
            assert node is original_node
            assert conn is original_conn
            assert original_conn not in original_node._free

            # A concurrent topology refresh retargets the slot
            r.nodes_manager.slots_cache[slot] = [new_node]

            node, conn = strategy._get_client_and_connection_for_transaction()
            assert node is original_node
            assert conn is original_conn
            assert strategy._transaction_node is original_node
            assert strategy._transaction_connection is original_conn
            assert new_conn in new_node._free

            strategy._watching = False
            await pipe.reset()
            assert strategy._transaction_connection is None
            assert strategy._transaction_node is None
            assert original_conn in original_node._free
            assert original_conn not in new_node._free
        finally:
            await r.aclose()


@pytest.mark.onlycluster
@pytest.mark.ssl
class TestSSL:
    """
    Tests for SSL connections.

    This relies on the --redis-ssl-url for building the client and connecting to the
    appropriate port.
    """

    @pytest_asyncio.fixture()
    def create_client(self, request: FixtureRequest) -> Callable[..., RedisCluster]:
        ssl_url = request.config.option.redis_ssl_url
        ssl_host, ssl_port = urlparse(ssl_url)[1].split(":")
        self.client_cert, self.client_key, self.ca_cert = get_tls_certificates(
            "cluster"
        )

        async def _create_client(mocked: bool = True, **kwargs: Any) -> RedisCluster:
            if mocked:
                with mock.patch.object(
                    ClusterNode, "execute_command", autospec=True
                ) as execute_command_mock:

                    async def execute_command(self, *args, **kwargs):
                        if args[0] == "INFO":
                            return {"cluster_enabled": True}
                        if args[0] == "CLUSTER SLOTS":
                            return [[0, 16383, [ssl_host, ssl_port, "ssl_node"]]]
                        if args[0] == "COMMAND":
                            return {
                                "ping": {
                                    "name": "ping",
                                    "arity": -1,
                                    "flags": ["stale", "fast"],
                                    "first_key_pos": 0,
                                    "last_key_pos": 0,
                                    "step_count": 0,
                                }
                            }
                        raise NotImplementedError()

                    execute_command_mock.side_effect = execute_command

                    rc = await RedisCluster(host=ssl_host, port=ssl_port, **kwargs)

                assert len(rc.get_nodes()) == 1
                node = rc.get_default_node()
                assert node.port == int(ssl_port)
                return rc

            return await RedisCluster(host=ssl_host, port=ssl_port, **kwargs)

        return _create_client

    async def test_ssl_connection_without_ssl(
        self, create_client: Callable[..., Awaitable[RedisCluster]]
    ) -> None:
        with pytest.raises(RedisClusterException) as e:
            await create_client(mocked=False, ssl=False)
        e = e.value.__cause__
        assert "Connection closed by server" in str(e)

    async def test_ssl_with_invalid_cert(
        self, create_client: Callable[..., Awaitable[RedisCluster]]
    ) -> None:
        with pytest.raises(RedisClusterException) as e:
            await create_client(mocked=False, ssl=True)
        e = e.value.__cause__.__context__
        assert "SSL: CERTIFICATE_VERIFY_FAILED" in str(e)

    async def test_ssl_connection(
        self, create_client: Callable[..., Awaitable[RedisCluster]]
    ) -> None:
        async with await create_client(ssl=True, ssl_cert_reqs="none") as rc:
            assert await rc.ping()

    async def test_ssl_connection_tls12_custom_ciphers_invalid(
        self, create_client: Callable[..., Awaitable[RedisCluster]]
    ) -> None:
        async with await create_client(
            ssl=True,
            ssl_cert_reqs="none",
            ssl_min_version=ssl.TLSVersion.TLSv1_2,
            ssl_ciphers="foo:bar",
        ) as rc:
            # The refresh the connection error asked for reaches no node either,
            # so the command raises that, with its own connection error as the cause
            with pytest.raises(RedisClusterUnreachableError) as e:
                assert await rc.ping()
            assert "Redis Cluster cannot be connected" in str(e.value)
            assert "No cipher can be selected" in str(e.value.__cause__)

    async def test_ssl_connection_tls13_custom_ciphers(
        self, create_client: Callable[..., Awaitable[RedisCluster]]
    ) -> None:
        # TLSv1.3 suites cannot be set through the cipher list, so a list of only
        # TLSv1.3 suites leaves no cipher to select
        ssl_ciphers = "TLS_AES_256_GCM_SHA384:TLS_CHACHA20_POLY1305_SHA256"
        async with await create_client(
            ssl=True,
            ssl_cert_reqs="none",
            ssl_min_version=ssl.TLSVersion.TLSv1_2,
            ssl_ciphers=ssl_ciphers,
        ) as rc:
            # The refresh the connection error asked for reaches no node either,
            # so the command raises that, with its own connection error as the cause
            with pytest.raises(RedisClusterUnreachableError) as e:
                assert await rc.ping()
            assert "Redis Cluster cannot be connected" in str(e.value)
            assert "No cipher can be selected" in str(e.value.__cause__)

    async def test_validating_self_signed_certificate(
        self, create_client: Callable[..., Awaitable[RedisCluster]]
    ) -> None:
        # ssl_check_hostname=False is used to avoid hostname verification
        # in the test environment, where the server certificate is self-signed
        # and does not match the hostname that is extracted for the cluster.
        # Cert hostname is 'localhost' in the cluster initialization when using
        # 'localhost' it gets transformed into 127.0.0.1
        # In production code, ssl_check_hostname should be set to True
        # to ensure proper hostname verification.
        async with await create_client(
            ssl=True,
            ssl_ca_certs=self.ca_cert,
            ssl_cert_reqs="required",
            ssl_certfile=self.client_cert,
            ssl_keyfile=self.client_key,
            ssl_check_hostname=False,
        ) as rc:
            assert await rc.ping()

    async def test_validating_self_signed_string_certificate(
        self, create_client: Callable[..., Awaitable[RedisCluster]]
    ) -> None:
        with open(self.ca_cert) as f:
            cert_data = f.read()

        # ssl_check_hostname=False is used to avoid hostname verification
        # in the test environment, where the server certificate is self-signed
        # and does not match the hostname that is extracted for the cluster.
        # Cert hostname is 'localhost' in the cluster initialization when using
        # 'localhost' it gets transformed into 127.0.0.1
        # In production code, ssl_check_hostname should be set to True
        # to ensure proper hostname verification.
        async with await create_client(
            ssl=True,
            ssl_ca_data=cert_data,
            ssl_cert_reqs="required",
            ssl_check_hostname=False,
            ssl_certfile=self.client_cert,
            ssl_keyfile=self.client_key,
        ) as rc:
            assert await rc.ping()


@pytest.mark.onlycluster
class TestAsyncClusterMetricsRecording:
    """
    Integration tests that verify metrics are properly recorded
    from async RedisCluster and delivered to the Meter through the observability recorder.

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
        meter.create_observable_gauge.return_value = Mock()

        return meter

    @pytest.fixture
    async def cluster_with_otel(self, r, mock_meter):
        """
        Setup a RedisCluster with real connection and mocked OTel collector.
        Returns tuple of (cluster, operation_duration_mock).
        """

        # Reset any existing collector state
        async_recorder.reset_collector()

        # Create config with COMMAND group enabled
        config = OTelConfig(metric_groups=[MetricGroup.COMMAND])

        # Create collector with mocked meter
        with patch("redis.observability.metrics.OTEL_AVAILABLE", True):
            collector = RedisMetricsCollector(mock_meter, config)

        # Patch the recorder to use our collector
        with patch.object(async_recorder, "_async_metrics_collector", collector):
            # Create a new event dispatcher and attach it to the cluster
            event_dispatcher = EventDispatcher()
            r._event_dispatcher = event_dispatcher

            yield r, self.operation_duration

        # Cleanup
        async_recorder.reset_collector()

    async def test_execute_command_records_metric(self, cluster_with_otel):
        """
        Test that execute_command records operation duration metric to Meter.
        """
        cluster, operation_duration_mock = cluster_with_otel

        # Execute a command
        await cluster.set("test_key", "test_value")

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

    async def test_get_command_records_metric(self, cluster_with_otel):
        """
        Test that GET command records metric with correct command name.
        """
        cluster, operation_duration_mock = cluster_with_otel

        # Execute GET command
        await cluster.get("test_key")

        # Verify command name is GET
        call_args = operation_duration_mock.record.call_args
        attrs = call_args[1]["attributes"]
        assert attrs["db.operation.name"] == "GET"

    async def test_multiple_commands_record_multiple_metrics(self, cluster_with_otel):
        """
        Test that multiple command executions record multiple metrics.
        """
        cluster, operation_duration_mock = cluster_with_otel

        # Execute multiple commands
        await cluster.set("key1", "value1")
        await cluster.get("key1")
        await cluster.delete("key1")

        # Verify histogram.record() was called 3 times
        assert operation_duration_mock.record.call_count == 3

    async def test_server_attributes_recorded(self, cluster_with_otel):
        """
        Test that server address, port, and db namespace are recorded.
        """
        cluster, operation_duration_mock = cluster_with_otel

        await cluster.ping()

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

    async def test_duration_is_positive(self, cluster_with_otel):
        """
        Test that the recorded duration is a positive float.
        """
        cluster, operation_duration_mock = cluster_with_otel

        await cluster.set("duration_test", "value")

        call_args = operation_duration_mock.record.call_args
        duration = call_args[0][0]

        assert isinstance(duration, float)
        assert duration >= 0

    async def test_no_batch_size_for_single_command(self, cluster_with_otel):
        """
        Test that single commands don't include batch_size attribute.
        """
        cluster, operation_duration_mock = cluster_with_otel

        await cluster.get("single_command_key")

        call_args = operation_duration_mock.record.call_args
        attrs = call_args[1]["attributes"]

        # batch_size should not be present for single commands
        assert "db.operation.batch.size" not in attrs

    async def test_command_error_records_metric_with_error_type(
        self, cluster_with_otel
    ):
        """
        Test that when a command fails, the recorded metric includes error.type attribute.
        """
        cluster, operation_duration_mock = cluster_with_otel

        # Execute a command that will fail (wrong type operation)
        await cluster.set("error_test_key", "string_value")

        try:
            # Try to use LPUSH on a string key - this will fail
            await cluster.lpush("error_test_key", "value")
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
class TestAsyncClusterPipelineMetricsRecording:
    """
    Integration tests that verify metrics are properly recorded
    from async ClusterPipeline and delivered to the Meter through the observability recorder.

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
        meter.create_observable_gauge.return_value = Mock()

        return meter

    @pytest.fixture
    async def cluster_pipeline_with_otel(self, r, mock_meter):
        """
        Setup a ClusterPipeline with real connection and mocked OTel collector.
        Returns tuple of (cluster, operation_duration_mock).
        """

        # Reset any existing collector state
        async_recorder.reset_collector()

        # Create config with COMMAND group enabled
        config = OTelConfig(metric_groups=[MetricGroup.COMMAND])

        # Create collector with mocked meter
        with patch("redis.observability.metrics.OTEL_AVAILABLE", True):
            collector = RedisMetricsCollector(mock_meter, config)

        # Patch the recorder to use our collector
        with patch.object(async_recorder, "_async_metrics_collector", collector):
            # Create a new event dispatcher and attach it to the cluster
            event_dispatcher = EventDispatcher()
            r._event_dispatcher = event_dispatcher

            yield r, self.operation_duration

        # Cleanup
        async_recorder.reset_collector()

    async def test_pipeline_execute_records_metric(self, cluster_pipeline_with_otel):
        """
        Test that pipeline execute records operation duration metric to Meter.
        """
        cluster, operation_duration_mock = cluster_pipeline_with_otel

        # Execute a pipeline
        pipe = cluster.pipeline()
        pipe.set("pipe_key1", "value1")
        pipe.get("pipe_key1")
        await pipe.execute()

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

    async def test_pipeline_server_attributes_recorded(
        self, cluster_pipeline_with_otel
    ):
        """
        Test that server address, port, and db namespace are recorded for pipeline.
        """
        cluster, operation_duration_mock = cluster_pipeline_with_otel

        pipe = cluster.pipeline()
        pipe.set("server_attr_key", "value")
        await pipe.execute()

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

    async def test_pipeline_duration_is_positive(self, cluster_pipeline_with_otel):
        """
        Test that the recorded duration for pipeline is a positive float.
        """
        cluster, operation_duration_mock = cluster_pipeline_with_otel

        pipe = cluster.pipeline()
        pipe.set("duration_key", "value")
        await pipe.execute()

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

    async def test_multiple_pipeline_executions_record_multiple_metrics(
        self, cluster_pipeline_with_otel
    ):
        """
        Test that multiple pipeline executions record multiple metrics.
        """
        cluster, operation_duration_mock = cluster_pipeline_with_otel

        # First pipeline
        pipe1 = cluster.pipeline()
        pipe1.set("multi_pipe_key1", "value1")
        await pipe1.execute()

        # Second pipeline
        pipe2 = cluster.pipeline()
        pipe2.set("multi_pipe_key2", "value2")
        pipe2.get("multi_pipe_key2")
        await pipe2.execute()

        # Find all PIPELINE event calls
        pipeline_calls = [
            call_obj
            for call_obj in operation_duration_mock.record.call_args_list
            if call_obj[1]["attributes"].get("db.operation.name") == "PIPELINE"
        ]

        # Should have at least 2 pipeline events
        assert len(pipeline_calls) >= 2


@pytest.mark.onlycluster
@pytest.mark.skipif(
    'not config.REDIS_INFO.get("cluster_enabled", False)',
    reason="Requires Redis Cluster",
)
class TestClusterPubSub:
    """
    Test ClusterPubSub with shard channels functionality
    """

    async def wait_for_message(
        self, pubsub, timeout=0.5, ignore_subscribe_messages=False, sharded=False
    ):
        """Helper method to wait for a message with timeout.

        Args:
            pubsub: The PubSub instance
            timeout: Timeout in seconds
            ignore_subscribe_messages: Whether to ignore subscribe messages
            sharded: If True, use get_sharded_message() instead of get_message()
        """
        import asyncio

        now = asyncio.get_running_loop().time()
        end_time = now + timeout
        while now < end_time:
            if sharded:
                message = await pubsub.get_sharded_message(
                    ignore_subscribe_messages=ignore_subscribe_messages,
                    timeout=0.01,
                )
            else:
                message = await pubsub.get_message(
                    ignore_subscribe_messages=ignore_subscribe_messages
                )
            if message is not None:
                return message
            await asyncio.sleep(0.01)
            now = asyncio.get_running_loop().time()
        return None

    def make_message(self, type, channel, data, pattern=None):
        """Helper method to create expected message format"""
        return {
            "type": type,
            "pattern": pattern and pattern.encode("utf-8") or None,
            "channel": channel and channel.encode("utf-8") or None,
            "data": data.encode("utf-8") if isinstance(data, str) else data,
        }

    @skip_if_server_version_lt("7.0.0")
    async def test_cluster_pubsub_creation(self, r):
        """Test basic ClusterPubSub creation"""
        pubsub = r.pubsub()
        assert pubsub is not None
        assert hasattr(pubsub, "ssubscribe")
        assert hasattr(pubsub, "sunsubscribe")
        assert hasattr(pubsub, "get_sharded_message")
        await pubsub.aclose()

    @skip_if_server_version_lt("7.0.0")
    async def test_cluster_pubsub_with_node(self, r):
        """Test ClusterPubSub creation with specific node"""
        nodes = r.get_nodes()
        if nodes:
            node = nodes[0]
            pubsub = r.pubsub(node=node)
            assert pubsub.node == node
            await pubsub.aclose()

    @skip_if_server_version_lt("7.0.0")
    async def test_cluster_pubsub_with_host_port(self, r):
        """Test ClusterPubSub creation with host and port"""
        nodes = r.get_nodes()
        if nodes:
            node = nodes[0]
            pubsub = r.pubsub(host=node.host, port=node.port)
            assert pubsub.node.host == node.host
            assert pubsub.node.port == node.port
            await pubsub.aclose()

    @skip_if_server_version_lt("7.0.0")
    async def test_shard_channel_subscribe_unsubscribe(self, r):
        """Test shard channel subscribe and unsubscribe"""
        pubsub = r.pubsub()

        try:
            # Test channels that map to different nodes
            channels = ["shard_test_1", "shard_test_2", "shard_test_3"]

            # Subscribe to shard channels
            await pubsub.ssubscribe(*channels)

            # Verify subscription messages - one ssubscribe confirmation per channel
            received_channels = set()
            for _ in range(len(channels)):
                msg = await self.wait_for_message(pubsub, timeout=1.0, sharded=True)
                assert msg is not None, "Expected subscription confirmation message"
                assert msg["type"] == "ssubscribe"
                assert msg["channel"].decode() in channels
                received_channels.add(msg["channel"].decode())

            # Verify we got confirmations for all channels
            assert received_channels == set(channels)

            # Unsubscribe from shard channels
            await pubsub.sunsubscribe(*channels)

        finally:
            await pubsub.aclose()

    @skip_if_server_version_lt("7.0.0")
    async def test_shard_channel_attributes(self, r):
        """Test shard channel attributes"""
        pubsub = r.pubsub()

        try:
            # Initially no shard channels
            assert not pubsub.shard_channels
            assert not pubsub.pending_unsubscribe_shard_channels

            # Subscribe to a shard channel
            await pubsub.ssubscribe("test_shard_attr")

            # Should have shard channel information
            # Note: The exact behavior may depend on implementation details

        finally:
            await pubsub.aclose()

    @skip_if_server_version_lt("7.0.0")
    async def test_shard_channel_with_handler(self, r):
        """Test shard channel subscription with message handler"""
        pubsub = r.pubsub()

        try:
            received_messages = []

            def message_handler(message):
                received_messages.append(message)

            # Subscribe with handler
            await pubsub.ssubscribe(test_handler_channel=message_handler)

            # This test verifies that the handler mechanism is properly set up
            # Actual message delivery testing would require a live Redis cluster

        finally:
            await pubsub.aclose()

    @skip_if_server_version_lt("7.0.0")
    async def test_invalid_node_raises_exception(self, r):
        """Test that invalid node raises appropriate exception"""
        with pytest.raises(RedisClusterException):
            r.pubsub(host="invalid_host", port=9999)

    @skip_if_server_version_lt("7.0.0")
    async def test_partial_host_port_raises_exception(self, r):
        """Test that providing only host or port raises DataError"""
        with pytest.raises(DataError):
            r.pubsub(host="localhost")  # Missing port

        with pytest.raises(DataError):
            r.pubsub(port=7000)  # Missing host

    @skip_if_server_version_lt("7.0.0")
    async def test_pubsub_without_specifying_node(self, r):
        """
        Test creation of pubsub instance without specifying a node. The node
        should be determined based on the keyslot of the first command
        execution.
        """
        channel_name = "foo"
        node = r.get_node_from_key(channel_name)
        p = r.pubsub()
        try:
            assert p.get_pubsub_node() is None
            await p.subscribe(channel_name)
            assert p.get_pubsub_node() == node
        finally:
            await p.aclose()

    @skip_if_server_version_lt("7.0.0")
    async def test_get_pubsub_node(self, r):
        """Test get_pubsub_node returns the correct node"""
        nodes = r.get_nodes()
        if nodes:
            node = nodes[0]
            p = r.pubsub(node=node)
            try:
                assert p.get_pubsub_node() == node
            finally:
                await p.aclose()

    @skip_if_server_version_lt("7.0.0")
    async def test_get_sharded_message_with_publish(self, r):
        """
        Test get_sharded_message returns published messages correctly.
        Validates that sharded message retrieval works end-to-end.
        """
        pubsub = r.pubsub()
        channel = "test-channel:{0}"

        try:
            # Subscribe to the shard channel
            await pubsub.ssubscribe(channel)

            # Read subscription confirmation using sharded message retrieval
            msg = await self.wait_for_message(pubsub, timeout=1.0, sharded=True)
            assert msg is not None
            assert msg["type"] == "ssubscribe"

            # Publish a message using execute_command directly (spublish not on RedisCluster)
            await r.execute_command("SPUBLISH", channel, "test message")

            # Read the published message using get_sharded_message
            msg = await pubsub.get_sharded_message(timeout=1.0)
            # May need to retry a few times
            for _ in range(10):
                if msg is not None and msg.get("type") == "smessage":
                    break
                await asyncio.sleep(0.1)
                msg = await pubsub.get_sharded_message(timeout=0.1)

            assert msg is not None
            assert msg["type"] == "smessage"
            assert msg["channel"] == channel.encode()
            assert msg["data"] == b"test message"
        finally:
            await pubsub.aclose()

    @skip_if_server_version_lt("7.0.0")
    async def test_get_sharded_message_multiple_channels(self, r):
        """
        Test get_sharded_message with multiple channels on potentially different nodes.
        Validates round-robin message retrieval across nodes.
        """
        pubsub = r.pubsub()
        channel1 = "test-channel:{0}"
        channel2 = "test-channel:{6}"

        try:
            # Subscribe to both channels
            await pubsub.ssubscribe(channel1, channel2)

            # Read subscription confirmations using sharded retrieval
            for _ in range(2):
                msg = await self.wait_for_message(pubsub, timeout=1.0, sharded=True)
                assert msg is not None

            # Publish messages to both channels using execute_command
            await r.execute_command("SPUBLISH", channel1, "msg1")
            await r.execute_command("SPUBLISH", channel2, "msg2")

            # Read messages using get_sharded_message
            messages = []
            for _ in range(10):
                msg = await pubsub.get_sharded_message(timeout=0.2)
                if msg and msg.get("type") == "smessage":
                    messages.append(msg)
                if len(messages) >= 2:
                    break

            assert len(messages) == 2
            channels_received = {msg["channel"] for msg in messages}
            assert channel1.encode() in channels_received
            assert channel2.encode() in channels_received
        finally:
            await pubsub.aclose()

    @skip_if_server_version_lt("7.0.0")
    async def test_get_sharded_message_timeout_returns_none(self, r):
        """
        Test that get_sharded_message with timeout returns None when no message
        arrives within the timeout period.
        """
        pubsub = r.pubsub()
        channel = "test-channel:{0}"

        try:
            await pubsub.ssubscribe(channel)
            # Read subscription confirmation using sharded retrieval
            msg = await self.wait_for_message(pubsub, timeout=1.0, sharded=True)
            assert msg is not None

            # Call get_sharded_message with a short timeout - should return None
            import time

            start = time.monotonic()
            msg = await pubsub.get_sharded_message(timeout=0.1)
            elapsed = time.monotonic() - start

            assert msg is None
            # Verify timeout was approximately respected (allow some slack)
            assert elapsed < 1.0  # generous: a stalled runner must not fail it
        finally:
            await pubsub.aclose()

    @skip_if_server_version_lt("7.0.0")
    async def test_get_sharded_message_timeout_zero_returns_immediately(self, r):
        """
        Test that get_sharded_message(timeout=0) returns immediately without blocking.
        """
        pubsub = r.pubsub()
        channel = "test-channel:{0}"

        try:
            await pubsub.ssubscribe(channel)
            # Read subscription confirmation using sharded retrieval
            msg = await self.wait_for_message(pubsub, timeout=1.0, sharded=True)
            assert msg is not None

            # get_sharded_message with timeout=0 should return immediately
            import time

            start = time.monotonic()
            msg = await pubsub.get_sharded_message(timeout=0)
            elapsed = time.monotonic() - start

            assert msg is None
            assert elapsed < 1.0  # generous: a stalled runner must not fail it
        finally:
            await pubsub.aclose()

    @skip_if_server_version_lt("7.0.0")
    async def test_generator_handles_concurrent_mapping_changes(self, r):
        """
        Test that the generator properly handles mapping changes during iteration.
        This validates the fix for RuntimeError: dictionary changed size during iteration.
        """
        pubsub = r.pubsub()
        channel1 = "test-channel:{0}"
        channel2 = "test-channel:{6}"

        try:
            # Subscribe to first channel
            await pubsub.ssubscribe(channel1)
            msg = await self.wait_for_message(pubsub, timeout=1.0, sharded=True)
            assert msg is not None

            # Get initial mapping size (cluster pubsub only)
            assert hasattr(pubsub, "node_pubsub_mapping"), "Test requires ClusterPubSub"
            initial_size = len(pubsub.node_pubsub_mapping)

            # Subscribe to second channel (modifies mapping during potential iteration)
            await pubsub.ssubscribe(channel2)
            msg = await self.wait_for_message(pubsub, timeout=1.0, sharded=True)
            assert msg is not None

            # Verify mapping was updated
            assert len(pubsub.node_pubsub_mapping) >= initial_size

            # Publish and read messages - should not raise RuntimeError
            await r.execute_command("SPUBLISH", channel1, "msg1")
            await r.execute_command("SPUBLISH", channel2, "msg2")

            messages_received = 0
            for _ in range(10):
                msg = await pubsub.get_sharded_message(timeout=0.2)
                if msg and msg.get("type") == "smessage":
                    messages_received += 1
                if messages_received >= 2:
                    break

            assert messages_received == 2
        finally:
            await pubsub.aclose()

    @skip_if_server_version_lt("7.0.0")
    async def test_get_redis_connection(self, r):
        """
        Test that get_redis_connection() returns the pubsub's dedicated
        connection after subscribing to a channel.
        """
        node = r.get_default_node()
        p = r.pubsub(node=node)
        try:
            # Before subscribing, connection should be None
            assert p.get_redis_connection() is None

            # Subscribe to establish the dedicated pubsub connection
            await p.subscribe("test-channel")

            # Now get_redis_connection() should return the dedicated connection
            connection = p.get_redis_connection()
            assert connection is not None
            # The connection should be from the node
            assert connection.host == node.host
            assert connection.port == node.port
        finally:
            await p.aclose()

    @skip_if_server_version_lt("7.0.0")
    async def test_init_pubsub_with_non_existent_node(self, r):
        """
        Test creation of pubsub instance with node that doesn't exist in the
        cluster. RedisClusterException should be raised.
        """
        from redis.cluster import ClusterNode

        node = ClusterNode("1.1.1.1", 1111)
        with pytest.raises(RedisClusterException):
            r.pubsub(node=node)

    @skip_if_server_version_lt("7.0.0")
    async def test_pubsub_channels_merge_results(self, r):
        """
        Test that pubsub_channels merges results from all nodes.
        """
        nodes = r.get_nodes()
        channels = []
        pubsub_nodes = []
        i = 0
        try:
            for node in nodes:
                channel = f"foo{i}"
                # Create pubsub clients connected to different nodes
                p = r.pubsub(node=node)
                pubsub_nodes.append(p)
                await p.subscribe(channel)
                b_channel = channel.encode("utf-8")
                channels.append(b_channel)
                # Read the subscribe ack, so the server has processed SUBSCRIBE
                # before PUBSUB runs; the wait ends as soon as the ack arrives
                assert await self.wait_for_message(p, timeout=5.0) is not None
                i += 1

            # Assert that the cluster's pubsub_channels function returns ALL channels
            result = await r.pubsub_channels(target_nodes="all")
            result.sort()
            channels.sort()
            assert result == channels
        finally:
            for p in pubsub_nodes:
                await p.aclose()

    @skip_if_server_version_lt("7.0.0")
    async def test_pubsub_numsub_merge_results(self, r):
        """
        Test that pubsub_numsub merges subscription counts from all nodes.
        """
        nodes = r.get_nodes()
        pubsub_nodes = []
        channel = "foo"
        b_channel = channel.encode("utf-8")
        try:
            for node in nodes:
                # Create pubsub clients connected to different nodes
                p = r.pubsub(node=node)
                pubsub_nodes.append(p)
                await p.subscribe(channel)
                # Read the subscribe ack, so the server has processed SUBSCRIBE
                # before PUBSUB runs; the wait ends as soon as the ack arrives
                assert await self.wait_for_message(p, timeout=5.0) is not None

            # Assert cluster's pubsub_numsub returns ALL clients
            result = await r.pubsub_numsub(channel, target_nodes="all")
            assert result == [(b_channel, len(nodes))]
        finally:
            for p in pubsub_nodes:
                await p.aclose()

    @skip_if_server_version_lt("7.0.0")
    async def test_pubsub_numpat_merge_results(self, r):
        """
        Test that pubsub_numpat merges pattern subscription counts from all nodes.
        """
        nodes = r.get_nodes()
        pubsub_nodes = []
        pattern = "foo*"
        try:
            for node in nodes:
                # Create pubsub clients connected to different nodes
                p = r.pubsub(node=node)
                pubsub_nodes.append(p)
                await p.psubscribe(pattern)
                # Read the subscribe ack, so the server has processed SUBSCRIBE
                # before PUBSUB runs; the wait ends as soon as the ack arrives
                assert await self.wait_for_message(p, timeout=5.0) is not None

            # Assert cluster's pubsub_numpat returns ALL pattern subscriptions
            result = await r.pubsub_numpat(target_nodes="all")
            assert result == len(nodes)
        finally:
            for p in pubsub_nodes:
                await p.aclose()

    @skip_if_server_version_lt("7.0.0")
    async def test_shard_channel_message_handler_with_message(self, r):
        """
        Test shard channel subscription with message handler receives actual messages.
        This verifies the handler callback is executed when messages arrive.
        """
        pubsub = r.pubsub(ignore_subscribe_messages=True)
        channel = "test-handler-channel:{0}"
        received_messages = []

        def message_handler(message):
            received_messages.append(message)

        try:
            # Subscribe with handler
            await pubsub.ssubscribe(**{channel: message_handler})

            # Publish a message
            await r.spublish(channel, "test message")

            # Read message using get_sharded_message - handler should be called
            for _ in range(10):
                msg = await pubsub.get_sharded_message(timeout=0.2)
                # When handler is set, get_sharded_message returns None
                # but the handler receives the message
                if received_messages:
                    break

            # Verify handler received the message
            assert msg is None
            assert len(received_messages) == 1
            assert received_messages[0]["type"] == "smessage"
            assert received_messages[0]["channel"] == channel.encode()
            assert received_messages[0]["data"] == b"test message"
        finally:
            await pubsub.aclose()

    @skip_if_server_version_lt("7.0.0")
    async def test_handler_may_ssubscribe_from_inside_the_handler(self, r):
        """
        A message handler is awaited inside get_sharded_message, so an
        ssubscribe from inside it re-acquires the per-node I/O lock.
        asyncio.Lock is not reentrant, so this used to hang the reader task
        permanently and silently; wait_for keeps a regression from hanging CI.
        """
        first = "handler-resub-a:{0}"
        second = "handler-resub-b:{0}"
        pubsub = r.pubsub()
        resubscribed = asyncio.Event()

        async def handler(message):
            if not resubscribed.is_set():
                await pubsub.ssubscribe(second)
                resubscribed.set()

        async def wait_for_ssubscribe_ack(s_channel):
            # ssubscribe only writes the command, it does not wait for the
            # server's confirmation, and spublish travels on a separate
            # connection. Publishing before the confirmation has been read can
            # therefore reach the node first and report zero receivers, so wait
            # for the ack before asserting on the delivery count.
            async with async_timeout(10):
                while True:
                    message = await pubsub.get_sharded_message(timeout=0.05)
                    if (
                        message is not None
                        and str_if_bytes(message["type"]) == "ssubscribe"
                        and message["channel"] == s_channel.encode()
                    ):
                        return

        try:
            await pubsub.ssubscribe(**{first: handler})
            await wait_for_ssubscribe_ack(first)

            assert await r.spublish(first, "go") == 1
            async with async_timeout(10):
                while not resubscribed.is_set():
                    await pubsub.get_sharded_message(timeout=0.05)
            assert resubscribed.is_set()

            await wait_for_ssubscribe_ack(second)
            assert await r.spublish(second, "hi") == 1
            delivered = []
            async with async_timeout(10):
                while not delivered:
                    msg = await pubsub.get_sharded_message(timeout=0.05)
                    if msg is not None and msg["type"] == "smessage":
                        delivered.append(msg)
            assert delivered[0]["channel"] == second.encode()
        finally:
            await pubsub.aclose()


@pytest.mark.fixed_client
class TestClusterPubSubWithMocks:
    """
    Unit tests for async ClusterPubSub that do not require a running cluster.
    """

    def _make_pubsub(self, cluster_mock, node=None):
        """Create a ClusterPubSub with the provided cluster mock."""
        from redis._parsers import Encoder
        from redis.asyncio.cluster import ClusterPubSub

        cluster_mock.encoder = Encoder("utf-8", "strict", False)
        return ClusterPubSub(cluster_mock, node=node)

    async def test_init_with_node_uses_adapter(self) -> None:
        """
        __init__ with a node must wrap it in _ClusterNodePoolAdapter instead
        of creating a detached ConnectionPool.
        """
        from redis.asyncio.cluster import _ClusterNodePoolAdapter

        node = ClusterNode("127.0.0.1", 7000)
        cluster = Mock()
        cluster.get_node.return_value = node

        pubsub = self._make_pubsub(cluster, node=node)

        assert isinstance(pubsub.connection_pool, _ClusterNodePoolAdapter)
        assert pubsub.connection_pool._node is node
        assert pubsub.connection_pool.connection_kwargs is node.connection_kwargs

    async def test_init_without_node_has_no_connection_pool(self) -> None:
        """
        __init__ without a node must defer connection_pool creation until
        the first command selects a node.
        """
        cluster = Mock()
        pubsub = self._make_pubsub(cluster)

        assert pubsub.node is None
        assert pubsub.connection_pool is None

    async def test_subscribe_initializes_cluster(self) -> None:
        client = await get_mocked_redis_client(host=default_host, port=default_port)
        slots_cache = client.nodes_manager.slots_cache
        client.nodes_manager.slots_cache = {}
        # As a client that has not initialized yet
        client._initialize = True
        client._topology_ready = False
        client.initialize = mock.AsyncMock(
            side_effect=lambda: client.nodes_manager.slots_cache.update(slots_cache)
        )
        pubsub = client.pubsub()

        with mock.patch(
            "redis.asyncio.client.PubSub.execute_command", new=mock.AsyncMock()
        ):
            await pubsub.subscribe("channel")

        assert pubsub.node in client.get_nodes()

    @pytest.mark.parametrize("method", ["ssubscribe", "sunsubscribe"])
    async def test_empty_shard_command_does_not_initialize_cluster(
        self, method
    ) -> None:
        cluster = Mock()
        cluster._initialize = True
        cluster.initialize = mock.AsyncMock()
        cluster.read_from_replicas = False
        cluster.load_balancing_strategy = None
        pubsub = self._make_pubsub(cluster)

        await getattr(pubsub, method)()

        cluster.initialize.assert_not_awaited()

    async def test_get_node_pubsub_uses_adapter(self) -> None:
        """
        _get_node_pubsub must build a PubSub backed by a
        _ClusterNodePoolAdapter wrapping the ClusterNode.
        """
        from redis.asyncio.cluster import _ClusterNodePoolAdapter

        cluster = Mock()
        pubsub = self._make_pubsub(cluster)
        node = ClusterNode("127.0.0.1", 7000)

        shard_pubsub = pubsub._get_node_pubsub(node)

        assert isinstance(shard_pubsub.connection_pool, _ClusterNodePoolAdapter)
        assert shard_pubsub.connection_pool._node is node
        assert pubsub.node_pubsub_mapping[node.name] is shard_pubsub

    async def test_get_node_pubsub_caches_by_node_name(self) -> None:
        """Repeated calls must not re-materialise the shard PubSub."""
        cluster = Mock()
        pubsub = self._make_pubsub(cluster)
        node = ClusterNode("127.0.0.1", 7000)

        first = pubsub._get_node_pubsub(node)
        second = pubsub._get_node_pubsub(node)

        assert first is second
