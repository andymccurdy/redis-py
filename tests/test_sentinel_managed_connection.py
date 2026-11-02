import itertools
import socket
import time

import pytest

import redis
from redis.retry import Retry
from redis.sentinel import SentinelConnectionPool, SentinelManagedConnection
from redis.backoff import NoBackoff
from redis.utils import SENTINEL
from unittest import mock


@pytest.mark.fixed_client
def test_connect_retry_on_timeout_error(master_host):
    """Test that the _connect function is retried in case of a timeout"""
    connection_pool = mock.Mock()
    connection_pool.get_master_address = mock.Mock(
        return_value=(master_host[0], master_host[1])
    )
    conn = SentinelManagedConnection(
        retry_on_timeout=True,
        retry=Retry(NoBackoff(), 3),
        connection_pool=connection_pool,
    )
    origin_connect = conn._connect
    conn._connect = mock.Mock()

    def mock_connect():
        # connect only on the last retry
        if conn._connect.call_count <= 2:
            raise socket.timeout
        else:
            return origin_connect()

    conn._connect.side_effect = mock_connect
    conn.connect()
    assert conn._connect.call_count == 3
    assert connection_pool.get_master_address.call_count == 3
    conn.disconnect()


@pytest.mark.fixed_client
class TestSentinelManagedConnectionReadResponseTimeout:
    """
    Tests for timeout parameter propagation in SentinelManagedConnection.read_response().
    """

    def test_read_response_accepts_timeout_parameter(self, master_host):
        """
        Test that SentinelManagedConnection.read_response() accepts a timeout parameter.
        """
        connection_pool = mock.Mock()
        connection_pool.get_master_address = mock.Mock(
            return_value=(master_host[0], master_host[1])
        )
        connection_pool.is_master = True
        conn = SentinelManagedConnection(connection_pool=connection_pool)

        # Mock the parent class's read_response to verify timeout is passed
        with mock.patch.object(
            SentinelManagedConnection.__bases__[0],
            "read_response",
            return_value=b"OK",
        ) as mock_read_response:
            conn.read_response(timeout=0.5)
            mock_read_response.assert_called_once_with(
                disable_decoding=False,
                timeout=0.5,
                disconnect_on_error=True,
                push_request=False,
            )

    def test_read_response_timeout_default_is_sentinel(self, master_host):
        """
        Test that the default timeout value is SENTINEL (not modified).
        """
        connection_pool = mock.Mock()
        connection_pool.get_master_address = mock.Mock(
            return_value=(master_host[0], master_host[1])
        )
        connection_pool.is_master = True
        conn = SentinelManagedConnection(connection_pool=connection_pool)

        # Mock the parent class's read_response to verify default timeout
        with mock.patch.object(
            SentinelManagedConnection.__bases__[0],
            "read_response",
            return_value=b"OK",
        ) as mock_read_response:
            conn.read_response()
            mock_read_response.assert_called_once_with(
                disable_decoding=False,
                timeout=SENTINEL,
                disconnect_on_error=True,
                push_request=False,
            )

    def test_read_response_timeout_none_passed_through(self, master_host):
        """
        Test that timeout=None is passed through (for blocking behavior).
        """
        connection_pool = mock.Mock()
        connection_pool.get_master_address = mock.Mock(
            return_value=(master_host[0], master_host[1])
        )
        connection_pool.is_master = True
        conn = SentinelManagedConnection(connection_pool=connection_pool)

        # Mock the parent class's read_response to verify timeout=None is passed
        with mock.patch.object(
            SentinelManagedConnection.__bases__[0],
            "read_response",
            return_value=b"OK",
        ) as mock_read_response:
            conn.read_response(timeout=None)
            mock_read_response.assert_called_once_with(
                disable_decoding=False,
                timeout=None,
                disconnect_on_error=True,
                push_request=False,
            )

    def test_read_response_timeout_zero_passed_through(self, master_host):
        """
        Test that timeout=0 is passed through (for non-blocking behavior).
        """
        connection_pool = mock.Mock()
        connection_pool.get_master_address = mock.Mock(
            return_value=(master_host[0], master_host[1])
        )
        connection_pool.is_master = True
        conn = SentinelManagedConnection(connection_pool=connection_pool)

        # Mock the parent class's read_response to verify timeout=0 is passed
        with mock.patch.object(
            SentinelManagedConnection.__bases__[0],
            "read_response",
            return_value=b"OK",
        ) as mock_read_response:
            conn.read_response(timeout=0)
            mock_read_response.assert_called_once_with(
                disable_decoding=False,
                timeout=0,
                disconnect_on_error=True,
                push_request=False,
            )

    def test_read_response_all_parameters_passed_through(self, master_host):
        """
        Test that all parameters including timeout are correctly passed to parent.
        """
        connection_pool = mock.Mock()
        connection_pool.get_master_address = mock.Mock(
            return_value=(master_host[0], master_host[1])
        )
        connection_pool.is_master = True
        conn = SentinelManagedConnection(connection_pool=connection_pool)

        # Mock the parent class's read_response to verify all params
        with mock.patch.object(
            SentinelManagedConnection.__bases__[0],
            "read_response",
            return_value=b"OK",
        ) as mock_read_response:
            conn.read_response(
                disable_decoding=True,
                timeout=1.5,
                disconnect_on_error=True,
                push_request=True,
            )
            mock_read_response.assert_called_once_with(
                disable_decoding=True,
                timeout=1.5,
                disconnect_on_error=True,
                push_request=True,
            )


@pytest.mark.fixed_client
class TestSentinelManagedConnectionDisconnectOnError:
    """
    Tests for the disconnect-on-error behaviour of
    SentinelManagedConnection.read_response().

    These assert on the socket state and on the next reply rather than on the
    arguments forwarded to the base class, so they observe what the flag
    actually does.
    """

    def _connect(self, master_host):
        connection_pool = mock.Mock()
        connection_pool.get_master_address = mock.Mock(
            return_value=(master_host[0], master_host[1])
        )
        connection_pool.is_master = True
        connection_pool.check_connection = False
        conn = SentinelManagedConnection(connection_pool=connection_pool)
        conn.connect()
        return conn

    def test_base_exception_during_read_disconnects(self, master_host):
        """
        A BaseException raised at the socket read leaves the reply unread, so
        the connection must be closed instead of being reused. See #1128.
        """
        conn = self._connect(master_host)
        try:
            conn.send_command("PING")
            with mock.patch.object(
                conn._parser, "read_response", side_effect=KeyboardInterrupt
            ):
                with pytest.raises(KeyboardInterrupt):
                    conn.read_response()
            assert conn._sock is None
        finally:
            conn.disconnect()

    def test_reply_after_interrupted_read_is_not_stale(self, master_host):
        """
        The next command on a reused connection must get its own reply, not the
        reply left in the socket buffer by the interrupted one.
        """
        conn = self._connect(master_host)
        try:
            conn.send_command("ECHO", "interrupted")
            with mock.patch.object(
                conn._parser, "read_response", side_effect=KeyboardInterrupt
            ):
                with pytest.raises(KeyboardInterrupt):
                    conn.read_response()

            conn.send_command("PING")
            assert conn.read_response() == b"PONG"
        finally:
            conn.disconnect()

    def test_interrupted_transaction_does_not_recycle_a_desynced_connection(
        self, master_host
    ):
        """
        _execute_transaction reads the MULTI reply, one reply per queued
        command, then EXEC. A BaseException part way through that loop strands
        the rest: Retry does not treat it as retryable, execute() catches only
        Exception, and reset() disconnects only while watching, so the
        connection is released straight back to the pool. Whatever comes out of
        the pool next must not be holding those unread replies.
        """
        # The real pool class, so the release path under test is the production
        # one. Only discover_master is reached, so no live Sentinel is needed.
        sentinel_manager = mock.Mock()
        sentinel_manager.discover_master = mock.Mock(
            return_value=(master_host[0], master_host[1])
        )
        pool = SentinelConnectionPool("mymaster", sentinel_manager)

        conn = pool.get_connection()
        pipe = redis.Redis(connection_pool=pool).pipeline(transaction=True)
        # Hand the pipeline the connection we hold, so it is the pool's own
        # checked-out connection that gets released at the end of execute().
        pipe.connection = conn
        pipe.echo("first").echo("second").echo("third")

        real_read = conn._parser.read_response
        reads = itertools.count()

        def interrupt_third_read(*args, **kwargs):
            # MULTI, then "first" - the interrupt lands with "second", "third"
            # and the EXEC reply still queued in the socket.
            if next(reads) == 2:
                raise KeyboardInterrupt
            return real_read(*args, **kwargs)

        with mock.patch.object(
            conn._parser, "read_response", side_effect=interrupt_third_read
        ):
            with pytest.raises(KeyboardInterrupt):
                pipe.execute()

        reused = pool.get_connection()
        assert reused is conn
        try:
            reused.send_command("ECHO", "after")
            assert reused.read_response() == b"after"
        finally:
            reused.disconnect()

    def test_explicit_disconnect_on_error_false_is_honoured(self, master_host):
        """
        PubSub reads pass disconnect_on_error=False on purpose; an explicit
        False must still keep the connection open.
        """
        conn = self._connect(master_host)
        try:
            conn.send_command("PING")
            with mock.patch.object(
                conn._parser, "read_response", side_effect=KeyboardInterrupt
            ):
                with pytest.raises(KeyboardInterrupt):
                    conn.read_response(disconnect_on_error=False)
            assert conn._sock is not None
        finally:
            conn.disconnect()


DEMOTION_ERROR = (
    "UNBLOCKED force unblock from blocking operation, "
    "instance state changed (master -> replica?)"
)


@pytest.mark.fixed_client
class TestSentinelManagedConnectionDemotedMaster:
    """
    A master that becomes a replica, for instance during a coordinated
    failover, answers every client blocked on BLPOP/BRPOP/XREAD... with an
    UNBLOCKED error and closes the connection. For a master connection this
    means the same as READONLY: the master moved.
    """

    def _connect(self, master_host):
        connection_pool = mock.Mock()
        connection_pool.get_master_address = mock.Mock(
            return_value=(master_host[0], master_host[1])
        )
        connection_pool.is_master = True
        connection_pool.check_connection = False
        conn = SentinelManagedConnection(connection_pool=connection_pool)
        conn.connect()
        return conn

    def test_unblocked_by_demotion_is_a_connection_error(self, master_host):
        conn = self._connect(master_host)
        try:
            conn.send_command("PING")
            with mock.patch.object(
                conn._parser,
                "read_response",
                return_value=redis.ResponseError(DEMOTION_ERROR),
            ):
                with pytest.raises(redis.ConnectionError) as e:
                    conn.read_response()
            assert str(e.value) == "The previous master is now a slave"
            assert conn._sock is None
        finally:
            conn.disconnect()

    def test_unblocked_by_demotion_on_a_replica_connection_is_raised(self, master_host):
        conn = self._connect(master_host)
        conn.connection_pool.is_master = False
        try:
            conn.send_command("PING")
            with mock.patch.object(
                conn._parser,
                "read_response",
                return_value=redis.ResponseError(DEMOTION_ERROR),
            ):
                with pytest.raises(redis.ResponseError) as e:
                    conn.read_response()
            assert not isinstance(e.value, redis.ConnectionError)
            assert conn._sock is not None
        finally:
            conn.disconnect()

    @pytest.mark.onlynoncluster
    def test_client_unblock_error_is_not_a_demotion(self, master_host):
        """
        CLIENT UNBLOCK ... ERROR also answers UNBLOCKED, with another message:
        the server did not change role, so the error must reach the caller.
        """
        conn = self._connect(master_host)
        admin = redis.Redis(host=master_host[0], port=master_host[1])
        try:
            conn.send_command("CLIENT", "ID")
            client_id = conn.read_response()
            conn.send_command("BLPOP", "redis-py:unblocked-test", 5)
            unblocked = False
            for _ in range(50):
                if admin.client_unblock(client_id, error=True):
                    unblocked = True
                    break
                time.sleep(0.01)
            assert unblocked
            with pytest.raises(redis.ResponseError) as e:
                conn.read_response()
            assert str(e.value).startswith("UNBLOCKED client unblocked")
            assert not isinstance(e.value, redis.ConnectionError)
        finally:
            admin.close()
            conn.disconnect()
