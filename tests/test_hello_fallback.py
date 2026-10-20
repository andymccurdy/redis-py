"""Tests for HELLO fallback to RESP2 when server/proxy doesn't support it.

See https://github.com/redis/redis-py/issues/4089
"""

import logging
from unittest import mock

import pytest
import redis

from redis._parsers import _HiredisParser, _RESP2Parser, _RESP3Parser
from redis._parsers.response_callbacks import (
    _get_response_callback,
    get_response_callbacks,
)
from redis.cache import CacheConfig
from redis.client import CaseInsensitiveDict
from redis.cluster import PipelineCommand, TransactionStrategy
from redis.connection import BlockingConnectionPool, Connection, ConnectionPool
from redis.credentials import CredentialProvider
from redis.exceptions import AuthenticationError, ResponseError
from redis.maint_notifications import MaintNotificationsConfig
from redis.utils import HIREDIS_AVAILABLE


PARSERS = [
    _RESP3Parser,
    pytest.param(
        _HiredisParser,
        marks=pytest.mark.skipif(not HIREDIS_AVAILABLE, reason="hiredis not installed"),
    ),
]


class HelloRejectingConnection(Connection):
    """Reject HELLO locally and send subsequent commands to the RESP2 server."""

    def send_command(self, *args, **kwargs):
        if args[0] == "HELLO":
            self._reject_hello = True
            return
        return super().send_command(*args, **kwargs)

    def read_response(self, **kwargs):
        if getattr(self, "_reject_hello", False):
            self._reject_hello = False
            raise ResponseError("unknown command 'HELLO'")
        return super().read_response(**kwargs)


class TestHelloFallback:
    """Test RESP3 HELLO fallback behavior."""

    def _make_connection(self, protocol=None, password="secret", **kwargs):
        """Create a Connection with mocked I/O."""
        conn = Connection(
            password=password, protocol=protocol, driver_info=None, **kwargs
        )
        conn._parser.on_connect = mock.Mock()
        conn.send_command = mock.Mock()
        return conn

    def test_fallback_single_password_one_round_trip(self):
        """When user only configured a password, fallback should send a
        SINGLE-ARG AUTH directly (no two-arg attempt with 'default')."""
        conn = self._make_connection(protocol=None, password="secret")

        # First read_response: HELLO rejected
        # Second read_response: AUTH OK (single round trip)
        conn.read_response = mock.Mock(
            side_effect=[
                ResponseError("ERR unknown command 'HELLO'"),
                "OK",
            ]
        )

        conn.on_connect_check_health()

        assert conn.protocol == 2
        assert conn.handshake_metadata == {}
        calls = conn.send_command.call_args_list
        # HELLO 3 AUTH default secret
        assert calls[0][0][0] == "HELLO"
        assert "default" in calls[0][0]
        # Fallback AUTH should be SINGLE-ARG (just password)
        assert calls[1][0] == ("AUTH", "secret")
        # No third call needed
        assert len(calls) == 2

    def test_fallback_username_password_two_round_trips_on_old_server(self):
        """When user explicitly configured username+password and server
        is Redis < 6.0, fallback first tries two-arg AUTH then degrades
        to single-arg AUTH (matches issue #1274 behavior)."""
        from redis.exceptions import AuthenticationWrongNumberOfArgsError

        conn = self._make_connection(
            protocol=None, username="myuser", password="secret"
        )

        conn.read_response = mock.Mock(
            side_effect=[
                ResponseError("ERR unknown command 'HELLO'"),
                AuthenticationWrongNumberOfArgsError(),
                "OK",
            ]
        )

        conn.on_connect_check_health()

        assert conn.protocol == 2
        calls = conn.send_command.call_args_list
        # HELLO 3 AUTH myuser secret
        assert calls[0][0][0] == "HELLO"
        # Fallback AUTH myuser secret (two-arg, using ORIGINAL creds)
        assert calls[1][0] == ("AUTH", "myuser", "secret")
        # Final fallback AUTH secret (single-arg)
        assert calls[2][0] == ("AUTH", "secret")

    def test_fallback_on_noproto_default_protocol(self):
        """Default protocol should fall back on NOPROTO error."""
        conn = self._make_connection(protocol=None)

        conn.read_response = mock.Mock(
            side_effect=[
                ResponseError("NOPROTO sorry this proto is not supported"),
                "OK",
            ]
        )

        conn.on_connect_check_health()

        assert conn.protocol == 2
        assert conn.handshake_metadata == {}

    def test_no_fallback_on_explicit_protocol_3(self):
        """Explicit protocol=3 should NOT fall back — raise the error."""
        conn = self._make_connection(protocol=3)

        conn.read_response = mock.Mock(
            side_effect=ResponseError("ERR unknown command 'HELLO'")
        )

        with pytest.raises(ResponseError, match="unknown command"):
            conn.on_connect_check_health()

    def test_no_fallback_on_wrongpass(self):
        """WRONGPASS errors should never be swallowed, even with default protocol."""
        conn = self._make_connection(protocol=None)

        conn.read_response = mock.Mock(
            side_effect=ResponseError("WRONGPASS invalid username-password pair")
        )

        with pytest.raises(ResponseError, match="WRONGPASS"):
            conn.on_connect_check_health()

    def test_no_fallback_with_maint_notifications_enabled_true(self):
        """When maintenance notifications enabled=True, fail loudly."""
        maint_config = mock.Mock()
        maint_config.enabled = True

        conn = self._make_connection(
            protocol=None, maint_notifications_config=maint_config
        )

        conn.read_response = mock.Mock(
            side_effect=ResponseError("ERR unknown command 'HELLO'")
        )

        with pytest.raises(ResponseError, match="unknown command"):
            conn.on_connect_check_health()

    def test_fallback_emits_warning(self, caplog):
        """Fallback should log a warning."""
        conn = self._make_connection(protocol=None)

        conn.read_response = mock.Mock(
            side_effect=[
                ResponseError("ERR unknown command 'HELLO'"),
                "OK",
            ]
        )

        with caplog.at_level(logging.WARNING, logger="redis.connection"):
            conn.on_connect_check_health()

        assert "does not support HELLO" in caplog.text
        assert "RESP3" in caplog.text or "RESP" in caplog.text
        assert "protocol=2" in caplog.text

    def test_handshake_metadata_is_dict_after_fallback(self):
        """handshake_metadata should be {} (not None) after fallback to
        prevent AttributeError in downstream code like CacheProxyConnection."""
        conn = self._make_connection(protocol=None)

        conn.read_response = mock.Mock(
            side_effect=[
                ResponseError("ERR unknown command 'HELLO'"),
                "OK",
            ]
        )

        conn.on_connect_check_health()

        assert conn.handshake_metadata is not None
        assert isinstance(conn.handshake_metadata, dict)

    def test_no_auth_branch_fallback(self):
        """Branch 3: RESP3 + no credentials should also fall back."""
        conn = self._make_connection(protocol=None, password=None)

        conn.read_response = mock.Mock(
            side_effect=ResponseError("ERR unknown command 'HELLO'")
        )

        conn.on_connect_check_health()

        assert conn.protocol == 2
        assert conn.handshake_metadata == {}

    def test_explicit_resp2_path_uses_helper(self):
        """The standalone RESP2 path (protocol=2) should authenticate
        through the same _do_resp2_auth helper using ORIGINAL creds."""
        conn = self._make_connection(protocol=2, password="secret")

        conn.read_response = mock.Mock(return_value="OK")

        conn.on_connect_check_health()

        # Should not have sent HELLO at all
        calls = conn.send_command.call_args_list
        assert all(c[0][0] != "HELLO" for c in calls)
        # Should have sent single-arg AUTH (no default injection for RESP2)
        assert calls[0][0] == ("AUTH", "secret")

    def test_handshake_metadata_assigned_on_success(self):
        """On HELLO success, handshake_metadata should be assigned the
        response dict (not left as None). Regression test for async parity."""
        conn = self._make_connection(protocol=None, password="secret")

        hello_response = {b"server": b"redis", b"version": b"7.0.0", b"proto": 3}
        conn.read_response = mock.Mock(return_value=hello_response)

        conn.on_connect_check_health()

        assert conn.handshake_metadata is hello_response
        assert conn.handshake_metadata.get(b"proto") == 3

    def test_handshake_metadata_assigned_no_auth_success(self):
        """Branch 3 (RESP3 + no credentials): on HELLO success,
        handshake_metadata should be the response dict."""
        conn = self._make_connection(protocol=None, password=None)

        hello_response = {b"server": b"redis", b"version": b"7.4.0", b"proto": 3}
        conn.read_response = mock.Mock(return_value=hello_response)

        conn.on_connect_check_health()

        assert conn.handshake_metadata is hello_response
        assert conn.protocol == 3

    @pytest.mark.parametrize("parser_class", PARSERS)
    @pytest.mark.parametrize("password", [None, "secret"])
    @pytest.mark.parametrize("enabled", [False, "auto"])
    def test_parser_and_maintenance_after_fallback(
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
        conn.read_response = mock.Mock(
            side_effect=[ResponseError("unknown command 'HELLO'"), b"OK"]
        )
        with mock.patch.object(_RESP2Parser, "on_connect"):
            conn.on_connect_check_health()
        if parser_class is _HiredisParser:
            assert conn._parser is parser
        else:
            assert isinstance(conn._parser, _RESP2Parser)
        assert conn._parser.EXCEPTION_CLASSES is exceptions
        assert conn.get_protocol() == 2
        assert not any(
            c.args[:2] == ("CLIENT", "MAINT_NOTIFICATIONS")
            for c in conn.send_command.call_args_list
        )

    @pytest.mark.parametrize(
        "message",
        [
            "ERR unknown user",
            "ERR unknown subcommand 'AUTH'",
            "ERR failure NOPROTO",
            "NOAUTH Authentication required",
        ],
    )
    def test_unrelated_errors_do_not_fall_back(self, message):
        conn = self._make_connection()
        error = ResponseError(message)
        conn.read_response = mock.Mock(side_effect=error)
        with pytest.raises(ResponseError) as exc:
            conn.on_connect_check_health()
        assert exc.value is error
        assert conn.get_protocol() == 3

    @pytest.mark.parametrize("protocol", [None, 2])
    def test_credentials_are_fetched_once(self, protocol):
        provider = mock.Mock(spec=CredentialProvider)
        provider.get_credentials.return_value = ("secret",)
        conn = self._make_connection(
            protocol=protocol, password=None, credential_provider=provider
        )
        responses = [b"OK"]
        if protocol is None:
            responses.insert(0, ResponseError("NOPROTO unsupported protocol version"))
        conn.read_response = mock.Mock(side_effect=responses)
        conn.on_connect_check_health()
        provider.get_credentials.assert_called_once_with()
        assert conn.send_command.call_args.args == ("AUTH", "secret")

    def test_invalid_fallback_auth_fails(self):
        conn = self._make_connection()
        conn.read_response = mock.Mock(
            side_effect=[ResponseError("unknown command 'HELLO'"), b"invalid"]
        )
        with pytest.raises(AuthenticationError):
            conn.on_connect_check_health()

    def test_noproto_preserves_username(self):
        conn = self._make_connection(username="user", password="secret")
        conn.read_response = mock.Mock(
            side_effect=[ResponseError("NOPROTO unsupported protocol version"), b"OK"]
        )
        conn.on_connect_check_health()
        assert conn.send_command.call_args.args == ("AUTH", "user", "secret")

    @pytest.mark.parametrize("message", ["ERR unknown command", "NOPROTO"])
    def test_proxy_errors_without_details_allow_fallback(self, message):
        conn = self._make_connection(password=None)
        conn.read_response = mock.Mock(side_effect=ResponseError(message))
        conn.on_connect_check_health()
        assert conn.get_protocol() == 2

    @pytest.mark.parametrize("pool_class", [ConnectionPool, BlockingConnectionPool])
    @pytest.mark.parametrize("parser_class", PARSERS)
    def test_caching_rejects_fallback(self, pool_class, parser_class):
        pool = pool_class(
            cache_config=CacheConfig(), parser_class=parser_class, driver_info=None
        )
        proxy = pool.make_connection()
        conn = proxy._conn
        parser = conn._parser
        parser.on_connect = mock.Mock()
        conn.send_command = mock.Mock()
        conn.read_response = mock.Mock(
            side_effect=ResponseError("unknown command 'HELLO'")
        )
        try:
            with pytest.raises(
                ResponseError, match="Client-side caching requires RESP3"
            ):
                conn.on_connect_check_health()
            assert conn.get_protocol() == 3
            assert conn._parser is parser
            assert conn.handshake_metadata is None
            assert conn.send_command.call_count == 1
        finally:
            pool.disconnect()


@pytest.mark.parametrize("legacy_responses", [True, False])
def test_callbacks_are_selected_per_connection(legacy_responses):
    client = redis.Redis(legacy_responses=legacy_responses)
    conn = Connection(protocol=None, legacy_responses=legacy_responses)
    callbacks = client.response_callbacks.copy()
    conn.protocol = 2
    callback = _get_response_callback(client.response_callbacks, "hgetall", conn)
    assert callback([b"field", b"value"]) == {b"field": b"value"}
    conn.protocol = 3
    assert _get_response_callback(
        client.response_callbacks, "hgetall", conn
    ) is callbacks.get("HGETALL")
    assert client.response_callbacks == callbacks

    custom = mock.Mock(return_value="custom")
    client.set_response_callback("HGETALL", custom)
    conn.protocol = 2
    assert _get_response_callback(client.response_callbacks, "hgetall", conn) is custom

    # RESP2 adds callbacks for commands that return native types on RESP3.
    callbacks = CaseInsensitiveDict(get_response_callbacks(None, legacy_responses))
    explicit = get_response_callbacks(2, legacy_responses)
    for command, callback in explicit.items():
        for name in (
            command,
            command.lower(),
            command.encode(),
            command.lower().encode(),
        ):
            assert _get_response_callback(callbacks, name, conn) is callback


@pytest.mark.parametrize("legacy_responses", [True, False])
@pytest.mark.parametrize("as_bytes", [True, False])
@pytest.mark.parametrize("command", ["CONFIG GET", "XREAD"])
def test_fallback_command_names_and_custom_callbacks(
    legacy_responses, as_bytes, command
):
    conn = Connection(legacy_responses=legacy_responses)
    conn.protocol = 2
    client = redis.Redis(legacy_responses=legacy_responses)
    if command == "CONFIG GET":
        wire = [b"timeout", b"0"]
        expected = {"timeout": "0"}
    else:
        wire = [[b"stream", [[b"1-0", [b"field", b"value"]]]]]
        entries = [(b"1-0", {b"field": b"value"})]
        expected = [[b"stream", entries]] if legacy_responses else {b"stream": entries}
    conn.read_response = mock.Mock(return_value=wire)
    for name in (command, command.lower()):
        name = name.encode() if as_bytes else name
        assert client.parse_response(conn, name) == expected
    custom = mock.Mock(return_value="custom")
    client.set_response_callback(command, custom)
    assert client.parse_response(conn, name) == "custom"
    custom.assert_called_once_with(wire)


@pytest.mark.parametrize("mapping_type", [dict, CaseInsensitiveDict])
def test_original_callback_keys_are_preserved(mapping_type):
    conn = Connection()
    conn.protocol = 2
    for name in ("config get", b"CONFIG GET"):
        callbacks = mapping_type(get_response_callbacks(None, True))
        custom = mock.Mock()
        callbacks[name] = custom
        assert _get_response_callback(callbacks, name, conn) is custom


@pytest.mark.parametrize("legacy_responses", [True, False])
def test_cluster_transaction_uses_negotiated_callbacks(legacy_responses):
    conn = Connection(legacy_responses=legacy_responses)
    conn.protocol = 2
    conn.send_packed_command = mock.Mock()
    conn.read_response = mock.Mock(
        side_effect=[b"OK", b"QUEUED", [[b"field", b"value"]]]
    )
    node = redis.Redis(legacy_responses=legacy_responses)
    strategy = object.__new__(TransactionStrategy)
    strategy._pipeline_slots = {0}
    strategy._pipe = mock.Mock(cluster_response_callbacks={})
    strategy._command_queue = [PipelineCommand(("HGETALL", "key"), {"keys": ["key"]})]
    strategy._get_client_and_connection_for_transaction = mock.Mock(
        return_value=(node, conn)
    )
    strategy._himport_prepare_pipeline = mock.Mock()
    strategy._raise_first_error = mock.Mock()
    assert strategy._execute_transaction(strategy._command_queue, True) == [
        {b"field": b"value"}
    ]


@pytest.mark.onlynoncluster
@pytest.mark.fixed_client
@pytest.mark.parametrize("parser_class", PARSERS)
@pytest.mark.parametrize("legacy_responses", [True, False])
@pytest.mark.parametrize("transaction", [None, False, True])
def test_fallback_client_and_pipeline(
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
    key = "hello-fallback-hash"
    try:
        control.delete(key)
        control.hset(key, mapping={"field": "value"})
        expected = [{b"field": b"value"}, control.config_get("timeout")]
        expected.append(expected[1])
        if transaction is None:
            assert [
                client.hgetall(key),
                client.config_get("timeout"),
                client.execute_command(b"CONFIG GET", "timeout"),
            ] == expected
        else:
            with client.pipeline(transaction=transaction) as pipe:
                assert (
                    pipe.hgetall(key)
                    .config_get("timeout")
                    .execute_command(b"CONFIG GET", "timeout")
                    .execute()
                    == expected
                )
        assert pool._available_connections[-1].get_protocol() == 2
        pool.disconnect()
        assert client.hgetall(key) == expected[0]
        client.set_response_callback("HGETALL", tuple)
        with client.pipeline(transaction=bool(transaction)) as pipe:
            assert pipe.hgetall(key).execute() == [(b"field", b"value")]
    finally:
        control.delete(key)
        control.close()
        client.close()
        pool.disconnect()
