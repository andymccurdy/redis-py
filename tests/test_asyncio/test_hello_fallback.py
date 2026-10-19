"""Async HELLO fallback, negotiated response shapes, and pipeline regressions."""

from unittest import mock

import pytest
import redis.asyncio as redis

from redis._parsers import _AsyncHiredisParser, _AsyncRESP2Parser, _AsyncRESP3Parser
from redis._parsers.response_callbacks import get_response_callbacks
from redis.asyncio.cluster import ClusterNode, PipelineCommand, TransactionStrategy
from redis.asyncio.connection import Connection, ConnectionPool
from redis.credentials import CredentialProvider
from redis.exceptions import (
    AuthenticationError,
    AuthenticationWrongNumberOfArgsError,
    ResponseError,
)
from redis.maint_notifications import MaintNotificationsConfig
from redis.utils import HIREDIS_AVAILABLE


PARSERS = [
    _AsyncRESP3Parser,
    pytest.param(
        _AsyncHiredisParser,
        marks=pytest.mark.skipif(not HIREDIS_AVAILABLE, reason="hiredis not installed"),
    ),
]


class HelloRejectingConnection(Connection):
    async def send_command(self, *args, **kwargs):
        if args[0] == "HELLO":
            self._reject_hello = True
            return
        return await super().send_command(*args, **kwargs)

    async def read_response(self, **kwargs):
        if getattr(self, "_reject_hello", False):
            self._reject_hello = False
            raise ResponseError("unknown command 'HELLO'")
        return await super().read_response(**kwargs)


class TestHelloFallback:
    def _make_connection(self, **kwargs):
        conn = Connection(driver_info=None, **kwargs)
        conn._parser.on_connect = mock.Mock()
        conn.send_command = mock.AsyncMock()
        return conn

    @pytest.mark.parametrize("parser_class", PARSERS)
    @pytest.mark.parametrize("password", [None, "secret"])
    @pytest.mark.parametrize("enabled", [False, "auto"])
    async def test_parser_and_maintenance_after_fallback(
        self, parser_class, password, enabled
    ):
        conn = self._make_connection(
            parser_class=parser_class,
            password=password,
            maint_notifications_config=MaintNotificationsConfig(enabled=enabled),
        )
        parser = conn._parser
        exceptions = dict(parser.EXCEPTION_CLASSES)
        parser.EXCEPTION_CLASSES = exceptions
        conn.read_response = mock.AsyncMock(
            side_effect=[ResponseError("unknown command 'HELLO'"), b"OK"]
        )
        with mock.patch.object(_AsyncRESP2Parser, "on_connect"):
            await conn.on_connect_check_health()
        if parser_class is _AsyncHiredisParser:
            assert conn._parser is parser
        else:
            assert isinstance(conn._parser, _AsyncRESP2Parser)
        assert conn._parser.EXCEPTION_CLASSES is exceptions
        assert conn.get_protocol() == 2
        assert conn.handshake_metadata == {}
        assert not any(
            c.args[:2] == ("CLIENT", "MAINT_NOTIFICATIONS")
            for c in conn.send_command.call_args_list
        )
        if password:
            assert conn.send_command.call_args.args == ("AUTH", "secret")

    @pytest.mark.parametrize("protocol,enabled", [(3, False), (None, True)])
    async def test_explicit_resp3_requirements_reject_fallback(self, protocol, enabled):
        conn = self._make_connection(
            protocol=protocol,
            maint_notifications_config=MaintNotificationsConfig(enabled=enabled),
        )
        error = ResponseError("unknown command 'HELLO'")
        conn.read_response = mock.AsyncMock(side_effect=error)
        with pytest.raises(ResponseError) as exc:
            await conn.on_connect_check_health()
        assert exc.value is error
        assert conn.get_protocol() == 3

    @pytest.mark.parametrize(
        "error",
        [
            ResponseError("ERR unknown user"),
            ResponseError("ERR unknown subcommand 'AUTH'"),
            ResponseError("ERR failure NOPROTO"),
            ResponseError("NOAUTH Authentication required"),
            AuthenticationError("WRONGPASS invalid credentials"),
        ],
    )
    async def test_unrelated_errors_do_not_fall_back(self, error):
        conn = self._make_connection(password="secret")
        conn.read_response = mock.AsyncMock(side_effect=error)
        with pytest.raises(type(error)) as exc:
            await conn.on_connect_check_health()
        assert exc.value is error
        assert conn.get_protocol() == 3

    @pytest.mark.parametrize("protocol", [None, 2])
    async def test_credentials_are_fetched_once(self, protocol):
        provider = mock.Mock(spec=CredentialProvider)
        provider.get_credentials_async = mock.AsyncMock(return_value=("secret",))
        conn = self._make_connection(protocol=protocol, credential_provider=provider)
        responses = [b"OK"]
        if protocol is None:
            responses.insert(0, ResponseError("NOPROTO unsupported protocol version"))
        conn.read_response = mock.AsyncMock(side_effect=responses)
        await conn.on_connect_check_health()
        provider.get_credentials_async.assert_awaited_once_with()
        assert conn.send_command.call_args.args == ("AUTH", "secret")

    async def test_old_server_retries_single_argument_auth(self):
        conn = self._make_connection(username="user", password="secret")
        conn.read_response = mock.AsyncMock(
            side_effect=[
                ResponseError("unknown command 'HELLO'"),
                AuthenticationWrongNumberOfArgsError(),
                b"OK",
            ]
        )
        await conn.on_connect_check_health()
        assert conn.send_command.call_args_list[1].args == ("AUTH", "user", "secret")
        assert conn.send_command.call_args_list[2].args == ("AUTH", "secret")

    async def test_invalid_fallback_auth_fails(self):
        conn = self._make_connection(password="secret")
        conn.read_response = mock.AsyncMock(
            side_effect=[
                ResponseError("unknown command 'HELLO'"),
                b"invalid",
            ]
        )
        with pytest.raises(AuthenticationError):
            await conn.on_connect_check_health()

    async def test_noproto_preserves_username(self):
        conn = self._make_connection(username="user", password="secret")
        conn.read_response = mock.AsyncMock(
            side_effect=[ResponseError("NOPROTO unsupported protocol version"), b"OK"]
        )
        await conn.on_connect_check_health()
        assert conn.send_command.call_args.args == ("AUTH", "user", "secret")

    @pytest.mark.parametrize("message", ["ERR unknown command", "NOPROTO"])
    async def test_proxy_errors_without_details_allow_fallback(self, message):
        conn = self._make_connection()
        conn.read_response = mock.AsyncMock(side_effect=ResponseError(message))
        await conn.on_connect_check_health()
        assert conn.get_protocol() == 2

    @pytest.mark.parametrize("password", [None, "secret"])
    async def test_success_assigns_handshake_metadata(self, password):
        conn = self._make_connection(password=password)
        metadata = {b"proto": 3, b"server": b"redis", b"version": b"8.0.0"}
        conn.read_response = mock.AsyncMock(return_value=metadata)
        await conn.on_connect_check_health()
        assert conn.handshake_metadata is metadata
        assert conn.get_protocol() == 3

    @pytest.mark.parametrize("legacy_responses", [True, False])
    async def test_cluster_node_uses_negotiated_callbacks(self, legacy_responses):
        conn = self._make_connection(legacy_responses=legacy_responses)
        conn.protocol = 2
        conn.read_response = mock.AsyncMock(return_value=[b"field", b"value"])
        node = ClusterNode(
            "127.0.0.1",
            6379,
            response_callbacks=get_response_callbacks(None, legacy_responses),
        )
        assert await node.parse_response(conn, "HGETALL") == {b"field": b"value"}

    @pytest.mark.parametrize("legacy_responses", [True, False])
    async def test_cluster_transaction_uses_negotiated_callbacks(
        self, legacy_responses
    ):
        conn = self._make_connection(legacy_responses=legacy_responses)
        conn.protocol = 2
        conn.send_packed_command = mock.AsyncMock()
        node = mock.Mock(spec=ClusterNode)
        node.parse_response = mock.AsyncMock(
            side_effect=[b"OK", b"QUEUED", [[b"field", b"value"]]]
        )
        node.disconnect_if_needed = mock.AsyncMock()
        node._himport_prepare_pipeline = mock.AsyncMock()
        strategy = object.__new__(TransactionStrategy)
        strategy._pipeline_slots = {0}
        strategy._watching = False
        strategy._pipe = mock.Mock()
        strategy._pipe.cluster_client.response_callbacks = get_response_callbacks(
            None, legacy_responses
        )
        strategy._command_queue = [PipelineCommand(0, "HGETALL", "key", keys=["key"])]
        strategy._get_client_and_connection_for_transaction = mock.Mock(
            return_value=(node, conn)
        )
        strategy._raise_first_error = mock.AsyncMock()
        assert await strategy._execute_transaction(strategy._command_queue, True) == [
            {b"field": b"value"}
        ]


@pytest.mark.onlynoncluster
@pytest.mark.fixed_client
@pytest.mark.parametrize("parser_class", PARSERS)
@pytest.mark.parametrize("legacy_responses", [True, False])
@pytest.mark.parametrize("transaction", [None, False, True])
async def test_fallback_client_and_pipeline(
    request, parser_class, legacy_responses, transaction
):
    url = request.config.getoption("--redis-url")
    pool = ConnectionPool.from_url(
        url,
        connection_class=HelloRejectingConnection,
        parser_class=parser_class,
        legacy_responses=legacy_responses,
        driver_info=None,
    )
    client = redis.Redis(connection_pool=pool)
    control = redis.Redis.from_url(url, protocol=2, legacy_responses=legacy_responses)
    key = "async-hello-fallback-hash"
    try:
        await control.delete(key)
        await control.hset(key, mapping={"field": "value"})
        expected = [{b"field": b"value"}, await control.config_get("timeout")]
        if transaction is None:
            assert [
                await client.hgetall(key),
                await client.config_get("timeout"),
            ] == expected
        else:
            async with client.pipeline(transaction=transaction) as pipe:
                assert (
                    await pipe.hgetall(key).config_get("timeout").execute() == expected
                )
        assert pool._available_connections[-1].get_protocol() == 2
        await pool.disconnect()
        assert await client.hgetall(key) == expected[0]

        async def custom_callback(response):
            return tuple(response)

        client.set_response_callback("HGETALL", custom_callback)
        async with client.pipeline(transaction=bool(transaction)) as pipe:
            assert await pipe.hgetall(key).execute() == [(b"field", b"value")]
    finally:
        await control.delete(key)
        await control.aclose()
        await client.aclose()
        await pool.disconnect()
