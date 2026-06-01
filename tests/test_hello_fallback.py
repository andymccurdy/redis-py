"""Tests for HELLO fallback to RESP2 when server/proxy doesn't support it.

See https://github.com/redis/redis-py/issues/4089
"""

import logging
from unittest import mock

import pytest

from redis.connection import Connection
from redis.exceptions import AuthenticationError, ResponseError


class TestHelloFallback:
    """Test RESP3 HELLO fallback behavior."""

    def _make_connection(self, protocol=None, password="secret", **kwargs):
        """Create a Connection with mocked I/O."""
        conn = Connection(password=password, protocol=protocol, **kwargs)
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
