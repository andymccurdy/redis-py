import os
import signal
import socket
import threading
import time
from typing import List, Union
from unittest.mock import MagicMock, patch

import pytest

from redis import Redis
from redis._defaults import DEFAULT_SOCKET_CONNECT_TIMEOUT, DEFAULT_SOCKET_TIMEOUT
from redis.cache import CacheConfig, CacheEntry, CacheEntryStatus, CacheKey
from redis._parsers.resp3 import _RESP3Parser
from redis.connection import (
    AbstractConnection,
    Connection,
    ConnectionPool,
    BlockingConnectionPool,
    MaintenanceState,
)
from redis.event import (
    EventDispatcher,
    EventListenerInterface,
    MaintenanceCompletedEvent,
    MaintenanceStartedEvent,
)
from redis.exceptions import ConnectionError as RedisConnectionError
from redis.exceptions import ResponseError
from redis.maint_notifications import (
    EndpointType,
    MaintNotificationsConfig,
    NodeMigratingNotification,
    NodeMigratedNotification,
    NodeFailingOverNotification,
    NodeFailedOverNotification,
    MaintNotificationsPoolHandler,
    NodeMovingNotification,
    OSSMaintNotificationsHandler,
    OSSNodeMigratedNotification,
    _build_moving_connection_kwargs,
)


AFTER_MOVING_ADDRESS = "1.2.3.4:6379"
DEFAULT_ADDRESS = "12.45.34.56:6379"
MOVING_TIMEOUT = 1

MOVING_NOTIFICATION = NodeMovingNotification(
    id=1,
    new_node_host=AFTER_MOVING_ADDRESS.split(":")[0],
    new_node_port=int(AFTER_MOVING_ADDRESS.split(":")[1]),
    ttl=MOVING_TIMEOUT,
)

MOVING_NONE_NOTIFICATION = NodeMovingNotification(
    id=1,
    new_node_host=None,
    new_node_port=None,
    ttl=MOVING_TIMEOUT,
)


class Helpers:
    """Helper class containing static methods for validation in maintenance notifications tests."""

    @staticmethod
    def validate_in_use_connections_state(
        in_use_connections: List[AbstractConnection],
        expected_state=MaintenanceState.NONE,
        expected_should_reconnect: Union[bool, str] = True,
        expected_host_address=DEFAULT_ADDRESS.split(":")[0],
        expected_socket_timeout=DEFAULT_SOCKET_TIMEOUT,
        expected_socket_connect_timeout=DEFAULT_SOCKET_CONNECT_TIMEOUT,
        expected_orig_host_address=DEFAULT_ADDRESS.split(":")[0],
        expected_orig_socket_timeout=DEFAULT_SOCKET_TIMEOUT,
        expected_orig_socket_connect_timeout=DEFAULT_SOCKET_CONNECT_TIMEOUT,
        expected_current_socket_timeout=DEFAULT_SOCKET_TIMEOUT,
        expected_current_peername=DEFAULT_ADDRESS.split(":")[0],
    ):
        """Helper method to validate state of in-use connections."""

        # validate in use connections are still working with set flag for reconnect
        # and timeout is updated
        for connection in in_use_connections:
            if expected_should_reconnect != "any":
                assert connection.should_reconnect() == expected_should_reconnect
            assert connection.host == expected_host_address
            assert connection.socket_timeout == expected_socket_timeout
            assert connection.socket_connect_timeout == expected_socket_connect_timeout
            assert connection.orig_host_address == expected_orig_host_address
            assert connection.orig_socket_timeout == expected_orig_socket_timeout
            assert (
                connection.orig_socket_connect_timeout
                == expected_orig_socket_connect_timeout
            )
            conn_socket = connection._get_socket()
            if conn_socket is not None:
                assert conn_socket.gettimeout() == expected_current_socket_timeout
                assert conn_socket.connected is True
                if expected_current_peername != "any":
                    assert conn_socket.getpeername()[0] == expected_current_peername
            assert connection.maintenance_state == expected_state

    @staticmethod
    def validate_free_connections_state(
        pool,
        should_be_connected_count=0,
        connected_to_tmp_address=False,
        tmp_address=AFTER_MOVING_ADDRESS.split(":")[0],
        expected_state=MaintenanceState.MOVING,
        expected_host_address=DEFAULT_ADDRESS.split(":")[0],
        expected_socket_timeout=DEFAULT_SOCKET_TIMEOUT,
        expected_socket_connect_timeout=DEFAULT_SOCKET_CONNECT_TIMEOUT,
        expected_orig_host_address=DEFAULT_ADDRESS.split(":")[0],
        expected_orig_socket_timeout=DEFAULT_SOCKET_TIMEOUT,
        expected_orig_socket_connect_timeout=DEFAULT_SOCKET_CONNECT_TIMEOUT,
    ):
        """Helper method to validate state of free/available connections."""

        if isinstance(pool, BlockingConnectionPool):
            free_connections = [conn for conn in pool.pool.queue if conn is not None]
        elif isinstance(pool, ConnectionPool):
            free_connections = pool._available_connections
        else:
            raise ValueError(f"Unsupported pool type: {type(pool)}")

        connected_count = 0
        for connection in free_connections:
            assert connection.should_reconnect() is False
            assert connection.host == expected_host_address
            assert connection.socket_timeout == expected_socket_timeout
            assert connection.socket_connect_timeout == expected_socket_connect_timeout
            assert connection.orig_host_address == expected_orig_host_address
            assert connection.orig_socket_timeout == expected_orig_socket_timeout
            assert (
                connection.orig_socket_connect_timeout
                == expected_orig_socket_connect_timeout
            )
            assert connection.maintenance_state == expected_state
            if expected_state == MaintenanceState.NONE:
                assert connection.maintenance_notification_hash is None

            conn_socket = connection._get_socket()
            if conn_socket is not None:
                assert conn_socket.connected is True
                if connected_to_tmp_address and tmp_address != "any":
                    assert conn_socket.getpeername()[0] == tmp_address
                connected_count += 1
        assert connected_count == should_be_connected_count

    @staticmethod
    def validate_conn_kwargs(
        pool,
        expected_maintenance_state,
        expected_maintenance_notification_hash,
        expected_host_address,
        expected_port,
        expected_socket_timeout,
        expected_socket_connect_timeout,
        expected_orig_host_address,
        expected_orig_socket_timeout,
        expected_orig_socket_connect_timeout,
    ):
        """Helper method to validate connection kwargs."""
        assert pool.connection_kwargs["maintenance_state"] == expected_maintenance_state
        assert (
            pool.connection_kwargs["maintenance_notification_hash"]
            == expected_maintenance_notification_hash
        )
        assert pool.connection_kwargs["host"] == expected_host_address
        assert pool.connection_kwargs["port"] == expected_port
        assert pool.connection_kwargs["socket_timeout"] == expected_socket_timeout
        assert (
            pool.connection_kwargs["socket_connect_timeout"]
            == expected_socket_connect_timeout
        )
        assert (
            pool.connection_kwargs.get("orig_host_address", None)
            == expected_orig_host_address
        )
        assert (
            pool.connection_kwargs.get("orig_socket_timeout", None)
            == expected_orig_socket_timeout
        )
        assert (
            pool.connection_kwargs.get("orig_socket_connect_timeout", None)
            == expected_orig_socket_connect_timeout
        )


class MockSocket:
    """Mock socket that simulates Redis protocol responses."""

    def __init__(self):
        self.connected = False
        self.address = None
        self.sent_data = []
        self.closed = False
        self.command_count = 0
        self.pending_responses = []
        # Track socket timeout changes for maintenance notifications validation
        self.timeout = None
        self.thread_timeouts = {}  # Track last applied timeout per thread
        self.moving_sent = False

    def connect(self, address):
        """Simulate socket connection."""
        self.connected = True
        self.address = address

    def send(self, data):
        """Simulate sending data to Redis."""
        if self.closed:
            raise ConnectionError("Socket is closed")
        self.sent_data.append(data)

        # Analyze the command and prepare appropriate response
        if b"HELLO" in data:
            response = b"%7\r\n$6\r\nserver\r\n$5\r\nredis\r\n$7\r\nversion\r\n$5\r\n7.4.0\r\n$5\r\nproto\r\n:3\r\n$2\r\nid\r\n:1\r\n$4\r\nmode\r\n$10\r\nstandalone\r\n$4\r\nrole\r\n$6\r\nmaster\r\n$7\r\nmodules\r\n*0\r\n"
            self.pending_responses.append(response)
        elif b"MAINT_NOTIFICATIONS" in data and b"internal-ip" in data:
            # Simulate error response - activate it only for internal-ip tests
            response = b"+ERROR\r\n"
            self.pending_responses.append(response)
        elif b"SET" in data:
            response = b"+OK\r\n"

            # Check if this is a key that should trigger a push message
            if b"key_receive_migrating_" in data or b"key_receive_migrating" in data:
                # MIGRATING push message before SET key_receive_migrating_X response
                # Format: >3\r\n$9\r\nMIGRATING\r\n:1\r\n:10\r\n (3 elements: MIGRATING, id, ttl)
                migrating_push = ">3\r\n$9\r\nMIGRATING\r\n:1\r\n:10\r\n"
                response = migrating_push.encode() + response
            elif b"key_receive_migrated_" in data or b"key_receive_migrated" in data:
                # MIGRATED push message before SET key_receive_migrated_X response
                # Format: >2\r\n$8\r\nMIGRATED\r\n:1\r\n (2 elements: MIGRATED, id)
                migrated_push = ">2\r\n$8\r\nMIGRATED\r\n:1\r\n"
                response = migrated_push.encode() + response
            elif (
                b"key_receive_failing_over_" in data
                or b"key_receive_failing_over" in data
            ):
                # FAILING_OVER push message before SET key_receive_failing_over_X response
                # Format: >3\r\n$12\r\nFAILING_OVER\r\n:1\r\n:10\r\n (3 elements: FAILING_OVER, id, ttl)
                failing_over_push = ">3\r\n$12\r\nFAILING_OVER\r\n:1\r\n:10\r\n"

                response = failing_over_push.encode() + response
            elif (
                b"key_receive_failed_over_" in data
                or b"key_receive_failed_over" in data
            ):
                # FAILED_OVER push message before SET key_receive_failed_over_X response
                # Format: >2\r\n$11\r\nFAILED_OVER\r\n:1\r\n (2 elements: FAILED_OVER, id)
                failed_over_push = ">2\r\n$11\r\nFAILED_OVER\r\n:1\r\n"
                response = failed_over_push.encode() + response
            elif b"key_receive_moving_none_" in data:
                # MOVING push message before SET key_receive_moving_none_X response
                # Format: >4\r\n$6\r\nMOVING\r\n:1\r\n:1\r\n+null\r\n (4 elements: MOVING, id, ttl, null)
                # Note: Using + instead of $ to send as simple string instead of bulk string
                moving_push = f">4\r\n$6\r\nMOVING\r\n:1\r\n:{MOVING_TIMEOUT}\r\n_\r\n"
                response = moving_push.encode() + response
            elif b"key_receive_moving_" in data:
                # MOVING push message before SET key_receive_moving_X response
                # Format: >4\r\n$6\r\nMOVING\r\n:1\r\n:1\r\n+1.2.3.4:6379\r\n (4 elements: MOVING, id, ttl, host:port)
                # Note: Using + instead of $ to send as simple string instead of bulk string
                moving_push = f">4\r\n$6\r\nMOVING\r\n:1\r\n:{MOVING_TIMEOUT}\r\n+{AFTER_MOVING_ADDRESS}\r\n"
                response = moving_push.encode() + response

            self.pending_responses.append(response)
        elif b"GET" in data:
            # Extract key and provide appropriate response
            if b"hello" in data:
                response = b"$5\r\nworld\r\n"
                self.pending_responses.append(response)
            # Handle specific keys used in tests
            elif b"key_receive_moving_0" in data:
                self.pending_responses.append(b"$8\r\nvalue3_0\r\n")
            elif b"key_receive_migrated_0" in data:
                self.pending_responses.append(b"$13\r\nmigrated_value\r\n")
            elif b"key_receive_migrating" in data:
                self.pending_responses.append(b"$6\r\nvalue2\r\n")
            elif b"key_receive_migrated" in data:
                self.pending_responses.append(b"$6\r\nvalue3\r\n")
            elif b"key_receive_failing_over" in data:
                self.pending_responses.append(b"$6\r\nvalue4\r\n")
            elif b"key_receive_failed_over" in data:
                self.pending_responses.append(b"$6\r\nvalue5\r\n")
            elif b"key1" in data:
                self.pending_responses.append(b"$6\r\nvalue1\r\n")
            else:
                self.pending_responses.append(b"$-1\r\n")  # NULL response
        else:
            self.pending_responses.append(b"+OK\r\n")  # Default response

        self.command_count += 1
        return len(data)

    def sendall(self, data):
        """Simulate sending all data to Redis."""
        return self.send(data)

    def recv(self, bufsize):
        """Simulate receiving data from Redis."""
        if self.closed:
            raise ConnectionError("Socket is closed")

        # Use pending responses that were prepared when commands were sent
        if self.pending_responses:
            response = self.pending_responses.pop(0)
            if b"MOVING" in response:
                self.moving_sent = True
            return response[:bufsize]  # Respect buffer size
        else:
            # No data available - this should block or raise an exception
            # For can_read checks, we should indicate no data is available
            import errno

            raise BlockingIOError(errno.EAGAIN, "Resource temporarily unavailable")

    def recv_into(self, buffer, nbytes=0):
        """
        Receive data from Redis and write it into the provided buffer.
        Returns the number of bytes written.

        This method is used by the hiredis parser for efficient data reading.
        """
        if self.closed:
            raise ConnectionError("Socket is closed")

        # Use pending responses that were prepared when commands were sent
        if self.pending_responses:
            response = self.pending_responses.pop(0)
            if b"MOVING" in response:
                self.moving_sent = True

            # Determine how many bytes to write
            if nbytes == 0:
                nbytes = len(buffer)

            # Write data into the buffer (up to nbytes or response length)
            bytes_to_write = min(len(response), nbytes, len(buffer))
            buffer[:bytes_to_write] = response[:bytes_to_write]

            return bytes_to_write
        else:
            # No data available - this should block or raise an exception
            # For can_read checks, we should indicate no data is available
            import errno

            raise BlockingIOError(errno.EAGAIN, "Resource temporarily unavailable")

    def fileno(self):
        """Return a fake file descriptor for select/poll operations."""
        return 1  # Fake file descriptor

    def close(self):
        """Simulate closing the socket."""
        self.closed = True
        self.connected = False
        self.address = None
        self.timeout = None
        self.thread_timeouts = {}

    def settimeout(self, timeout):
        """Simulate setting socket timeout and track changes per thread."""
        self.timeout = timeout

        # Track last applied timeout with thread_id information added
        thread_id = threading.current_thread().ident
        self.thread_timeouts[thread_id] = timeout

    def gettimeout(self):
        """Simulate getting socket timeout."""
        return self.timeout

    def setsockopt(self, level, optname, value):
        """Simulate setting socket options."""
        pass

    def setblocking(self, blocking):
        pass

    def getpeername(self):
        """Simulate getting peer name."""
        return self.address

    def getsockname(self):
        """Simulate getting socket name."""
        return (self.address.split(":")[0], 12345)

    def shutdown(self, how):
        """Simulate socket shutdown."""
        pass


class _FakeTimer:
    """Stands in for ``threading.Timer``; runs only when the test advances time."""

    def __init__(self, test, interval, function, args=None, kwargs=None):
        self._test = test
        self.interval = interval
        self.function = function
        self.args = args or ()
        self.kwargs = kwargs or {}
        self.due = None

    def start(self):
        self.due = self._test._timer_clock + self.interval
        self._test._pending_timers.append(self)


class _ThreadingWithFakeTimer:
    """The ``threading`` module, with ``Timer`` replaced by ``_FakeTimer``."""

    def __init__(self, test):
        self._test = test

    def Timer(self, interval, function, args=None, kwargs=None):
        return _FakeTimer(self._test, interval, function, args, kwargs)

    def __getattr__(self, name):
        return getattr(threading, name)


class TestMaintenanceNotificationsBase:
    """Base class for maintenance notifications handling tests."""

    def setup_method(self):
        """Set up test fixtures with mocked sockets."""
        self.mock_sockets = []
        self.original_socket = socket.socket

        # Mock socket creation to return our mock sockets
        def mock_socket_factory(*args, **kwargs):
            mock_sock = MockSocket()
            self.mock_sockets.append(mock_sock)
            return mock_sock

        self.socket_patcher = patch("socket.socket", side_effect=mock_socket_factory)
        self.socket_patcher.start()

        # Mock select.select to simulate data availability for reading
        def mock_select(rlist, wlist, xlist, timeout=0):
            # Check if any of the sockets in rlist have data available
            ready_sockets = []
            for sock in rlist:
                if hasattr(sock, "connected") and sock.connected and not sock.closed:
                    # Only return socket as ready if it actually has data to read
                    if hasattr(sock, "pending_responses") and sock.pending_responses:
                        ready_sockets.append(sock)
                    # Don't return socket as ready just because it received commands
                    # Only when there are actual responses available
            return (ready_sockets, [], [])

        self.select_patcher = patch("select.select", side_effect=mock_select)
        self.select_patcher.start()

        # The handlers schedule MOVING follow-ups with threading.Timer. Record
        # them instead, so tests run them with _advance_timers, not sleeps.
        self._timer_clock = 0.0
        self._pending_timers = []
        self.timer_patcher = patch(
            "redis.maint_notifications.threading", _ThreadingWithFakeTimer(self)
        )
        self.timer_patcher.start()

        # Create maintenance notifications config
        self.config = MaintNotificationsConfig(
            enabled=True, proactive_reconnect=True, relaxed_timeout=30
        )

    def teardown_method(self):
        """Clean up test fixtures."""
        self.socket_patcher.stop()
        self.select_patcher.stop()
        self.timer_patcher.stop()

    def _advance_timers(self, seconds):
        """Move the fake timer clock forward and run the timers that come due.

        Timers run in due order, on the calling thread. A timer scheduled by
        another timer's callback runs too if it is due by the new time. The
        handlers' duplicate detection still expires on real time, so a test
        that resends the same MOVING after advancing would see it deduped.
        """
        self._timer_clock += seconds
        while True:
            due = [t for t in self._pending_timers if t.due <= self._timer_clock]
            if not due:
                return
            timer = min(due, key=lambda t: t.due)
            self._pending_timers.remove(timer)
            timer.function(*timer.args, **timer.kwargs)

    def _get_client(
        self,
        pool_class,
        connection_class=Connection,
        enable_cache=False,
        max_connections=10,
        maint_notifications_config=None,
        client_name=None,
        db=0,
    ):
        """Helper method to create a pool and Redis client with maintenance notifications configuration.

        Args:
            pool_class: The connection pool class (ConnectionPool or BlockingConnectionPool)
            max_connections: Maximum number of connections in the pool (default: 10)
            maint_notifications_config: Optional MaintNotificationsConfig to use. If not provided,
                                    uses self.config from setup_method (default: None)
            setup_pool_handler: Whether to set up pool handler for moving notifications (default: False)

        Returns:
            test_redis_client
        """
        config = (
            maint_notifications_config
            if maint_notifications_config is not None
            else self.config
        )
        pool_kwargs = {}
        if enable_cache:
            pool_kwargs = {"cache_config": CacheConfig()}

        if client_name is not None:
            pool_kwargs["client_name"] = client_name

        test_pool = pool_class(
            connection_class=connection_class,
            host=DEFAULT_ADDRESS.split(":")[0],
            port=int(DEFAULT_ADDRESS.split(":")[1]),
            max_connections=max_connections,
            protocol=3,  # Required for maintenance notifications
            db=db,
            maint_notifications_config=config,
            **pool_kwargs,
        )
        test_redis_client = Redis(connection_pool=test_pool)

        return test_redis_client


@pytest.mark.fixed_client
class TestMaintenanceNotificationsHandshake(TestMaintenanceNotificationsBase):
    """Integration tests for maintenance notifications handling with real connection pool."""

    def test_handshake_success_when_enabled(self):
        """Test that handshake is performed correctly."""
        maint_notifications_config = MaintNotificationsConfig(
            enabled=True, endpoint_type=EndpointType.EXTERNAL_IP
        )
        test_redis_client = self._get_client(
            ConnectionPool, maint_notifications_config=maint_notifications_config
        )

        try:
            # Perform Redis operations that should work with our improved mock responses
            result_set = test_redis_client.set("hello", "world")
            result_get = test_redis_client.get("hello")

            # Verify operations completed successfully
            assert result_set is True
            assert result_get == b"world"

        finally:
            test_redis_client.close()

    def test_handshake_success_when_auto_and_command_not_supported(self):
        """Test that when maintenance notifications are set to 'auto', the client gracefully handles unsupported MAINT_NOTIFICATIONS commands and normal Redis operations succeed."""
        maint_notifications_config = MaintNotificationsConfig(
            enabled="auto", endpoint_type=EndpointType.INTERNAL_IP
        )
        test_redis_client = self._get_client(
            ConnectionPool, maint_notifications_config=maint_notifications_config
        )

        try:
            # Perform Redis operations that should work with our improved mock responses
            result_set = test_redis_client.set("hello", "world")
            result_get = test_redis_client.get("hello")

            # Verify operations completed successfully
            assert result_set is True
            assert result_get == b"world"

        finally:
            test_redis_client.close()

    def test_handshake_failure_when_enabled(self):
        """Test that handshake is performed correctly."""
        maint_notifications_config = MaintNotificationsConfig(
            enabled=True, endpoint_type=EndpointType.INTERNAL_IP
        )
        test_redis_client = self._get_client(
            ConnectionPool, maint_notifications_config=maint_notifications_config
        )
        try:
            with pytest.raises(ResponseError):
                # handshake should fail
                # socket mock will return error when enabling maint notifications
                # for internal-ip
                test_redis_client.set("hello", "world")

        finally:
            test_redis_client.close()

    def test_handshake_pipelines_full_tail_with_client_name_and_db(self):
        """The whole handshake tail -- CLIENT MAINT_NOTIFICATIONS, SETNAME, both
        SETINFO commands and SELECT -- is pipelined (all sent, then the replies read
        back in send order).

        Uses enabled="auto" with an internal-ip endpoint so the server rejects
        MAINT_NOTIFICATIONS and its error reply is swallowed *mid-tail*. Because the
        maint command is first, a following command only succeeds if the swallow
        consumed exactly one reply and the remaining SETNAME/SETINFO/SELECT replies
        stayed aligned -- this is the reply/command-ordering contract the whole
        optimization depends on.
        """
        maint_notifications_config = MaintNotificationsConfig(
            enabled="auto", endpoint_type=EndpointType.INTERNAL_IP
        )
        test_redis_client = self._get_client(
            ConnectionPool,
            maint_notifications_config=maint_notifications_config,
            client_name="myclient",
            db=5,
        )
        try:
            # Post-handshake commands succeed -> replies stayed aligned even though
            # the swallowed MAINT_NOTIFICATIONS error sits first in the tail.
            assert test_redis_client.set("hello", "world") is True
            assert test_redis_client.get("hello") == b"world"

            # HELLO first (its own round-trip), then the tail commands in send order.
            handshake_sock = next(
                s for s in self.mock_sockets if any(b"HELLO" in w for w in s.sent_data)
            )
            wire = b"".join(handshake_sock.sent_data)
            assert (
                wire.index(b"HELLO")
                < wire.index(b"MAINT_NOTIFICATIONS")
                < wire.index(b"SETNAME")
                < wire.index(b"LIB-NAME")
                < wire.index(b"LIB-VER")
                < wire.index(b"SELECT")
            )
        finally:
            test_redis_client.close()


@pytest.mark.fixed_client
class TestMaintenanceNotificationsHandlingSingleProxy(TestMaintenanceNotificationsBase):
    """Integration tests for maintenance notifications handling with real connection pool."""

    def _validate_connection_handlers(self, conn, pool_handler, config):
        """Helper method to validate connection handlers are properly set."""
        # Test that the node moving handler function is correctly set
        parser_handler = conn._parser.node_moving_push_handler_func
        assert parser_handler is not None
        assert hasattr(parser_handler, "__self__")
        assert hasattr(parser_handler, "__func__")
        assert parser_handler.__self__.connection is conn
        assert parser_handler.__self__.pool is pool_handler.pool
        assert parser_handler.__self__._lock is pool_handler._lock
        assert (
            parser_handler.__self__._processed_notifications
            is pool_handler._processed_notifications
        )
        assert parser_handler.__func__ is pool_handler.handle_notification.__func__

        # Test that the maintenance handler function is correctly set
        maintenance_handler = conn._parser.maintenance_push_handler_func
        assert maintenance_handler is not None
        assert hasattr(maintenance_handler, "__self__")
        assert hasattr(maintenance_handler, "__func__")
        # The maintenance handler should be bound to the connection's
        # maintenance notification connection handler
        assert (
            maintenance_handler.__self__ is conn._maint_notifications_connection_handler
        )
        assert (
            maintenance_handler.__func__
            is conn._maint_notifications_connection_handler.handle_notification.__func__
        )

        # Validate that the connection's maintenance handler has the same config object
        assert conn._maint_notifications_connection_handler.config is config

    def _validate_current_timeout(self, expected_timeout, error_msg=None):
        """Helper method to validate the current timeout for the calling thread."""
        actual_timeout = None
        # Get the actual thread ID from the current thread
        current_thread_id = threading.current_thread().ident
        for sock in self.mock_sockets:
            if current_thread_id in sock.thread_timeouts:
                actual_timeout = sock.thread_timeouts[current_thread_id]
                break

        assert actual_timeout == expected_timeout, (
            f"{error_msg or ''}"
            f"Expected timeout ({expected_timeout}), "
            f"but found timeout: {actual_timeout}. "
            f"All thread timeouts: {[sock.thread_timeouts for sock in self.mock_sockets]}",
        )

    def _validate_disconnected(self, expected_count):
        """Helper method to validate all socket timeouts"""
        disconnected_sockets_count = 0
        for sock in self.mock_sockets:
            if sock.closed:
                disconnected_sockets_count += 1
        assert disconnected_sockets_count == expected_count

    def _validate_connected(self, expected_count):
        """Helper method to validate all socket timeouts"""
        connected_sockets_count = 0
        for sock in self.mock_sockets:
            if sock.connected:
                connected_sockets_count += 1
        assert connected_sockets_count == expected_count

    def test_client_initialization(self):
        """Test that Redis client is created with maintenance notifications configuration."""
        # Create a pool and Redis client with maintenance notifications

        test_redis_client = Redis(
            protocol=3,  # Required for maintenance notifications
            maint_notifications_config=self.config,
        )

        pool_handler = test_redis_client.connection_pool.connection_kwargs.get(
            "maint_notifications_pool_handler"
        )
        assert pool_handler is not None
        assert pool_handler.config == self.config

        conn = test_redis_client.connection_pool.get_connection()

        assert conn.should_reconnect() is False
        assert conn.orig_host_address == "localhost"
        assert conn.orig_socket_timeout == DEFAULT_SOCKET_TIMEOUT

        self._validate_connection_handlers(conn, pool_handler, self.config)

    def test_moving_update_with_disabled_relaxed_timeout_preserves_timeouts(self):
        config = MaintNotificationsConfig(
            enabled=True,
            proactive_reconnect=True,
            relaxed_timeout=-1,
        )
        pool = ConnectionPool(
            host=DEFAULT_ADDRESS.split(":")[0],
            port=int(DEFAULT_ADDRESS.split(":")[1]),
            protocol=3,
            maint_notifications_config=config,
        )
        connection = Connection(
            host=DEFAULT_ADDRESS.split(":")[0],
            port=int(DEFAULT_ADDRESS.split(":")[1]),
            protocol=3,
            maint_notifications_config=config,
        )

        pool.update_connection_settings(
            connection,
            state=MaintenanceState.MOVING,
            maintenance_notification_hash=hash(MOVING_NOTIFICATION),
            host_address=AFTER_MOVING_ADDRESS.split(":")[0],
            relaxed_timeout=config.relaxed_timeout,
            update_notification_hash=True,
        )

        assert connection.maintenance_state == MaintenanceState.MOVING
        assert connection.maintenance_notification_hash == hash(MOVING_NOTIFICATION)
        assert connection.host == AFTER_MOVING_ADDRESS.split(":")[0]
        assert connection.socket_timeout == DEFAULT_SOCKET_TIMEOUT
        assert connection.socket_connect_timeout == DEFAULT_SOCKET_CONNECT_TIMEOUT

    def test_maint_handler_init_for_existing_connections(self):
        """Test that maintenance notification handlers are properly set on existing and new connections
        when configuration is enabled after client creation."""

        # Create a Redis client with disabled maintenance notifications configuration
        disabled_config = MaintNotificationsConfig(enabled=False)
        test_redis_client = Redis(
            protocol=3,  # Required for maintenance notifications
            maint_notifications_config=disabled_config,
        )

        # Extract an existing connection before enabling maintenance notifications
        existing_conn = test_redis_client.connection_pool.get_connection()

        # Verify that maintenance notifications are initially disabled
        assert existing_conn._parser.node_moving_push_handler_func is None
        assert existing_conn._maint_notifications_connection_handler is None
        assert existing_conn._parser.maintenance_push_handler_func is None

        # Create a new enabled configuration and set up pool handler
        enabled_config = MaintNotificationsConfig(
            enabled=True, proactive_reconnect=True, relaxed_timeout=30
        )
        test_redis_client.connection_pool.update_maint_notifications_config(
            enabled_config
        )

        pool_handler = (
            test_redis_client.connection_pool._maint_notifications_pool_handler
        )
        # Validate the existing connection after enabling maintenance notifications
        # Both existing and new connections should now have full handler setup
        self._validate_connection_handlers(existing_conn, pool_handler, enabled_config)

        # Create a new connection and validate it has full handlers
        new_conn = test_redis_client.connection_pool.get_connection()
        self._validate_connection_handlers(new_conn, pool_handler, enabled_config)
        self._validate_connection_handlers(existing_conn, pool_handler, enabled_config)

        # Clean up connections
        test_redis_client.connection_pool.release(existing_conn)
        test_redis_client.connection_pool.release(new_conn)

    def test_update_oss_handler_wires_existing_connections(self):
        """update_maint_notifications_config wires an OSS cluster handler onto every
        existing connection in the pool.

        Each connection's parser receives the OSS cluster and maintenance push
        handlers and the connection references the handler. In-use connections are
        marked for reconnect; free connections are wired then disconnected.
        """
        config = MaintNotificationsConfig(enabled=True)
        pool = ConnectionPool(
            host=DEFAULT_ADDRESS.split(":")[0],
            port=int(DEFAULT_ADDRESS.split(":")[1]),
            protocol=3,
            maint_notifications_config=config,
        )
        free_connection = pool.make_connection()
        in_use_connection = pool.make_connection()
        pool._available_connections.append(free_connection)
        pool._in_use_connections.add(in_use_connection)

        oss_handler = OSSMaintNotificationsHandler(MagicMock(), config)

        pool.update_maint_notifications_config(
            config, oss_cluster_maint_notifications_handler=oss_handler
        )

        # In-use connection: parser has the OSS + maintenance push handlers wired
        # and the connection is marked for reconnect.
        assert in_use_connection._oss_cluster_maint_notifications_handler is oss_handler
        in_use_oss_func = in_use_connection._parser.oss_cluster_maint_push_handler_func
        assert in_use_oss_func is not None
        assert in_use_oss_func.__func__ is oss_handler.handle_notification.__func__
        assert in_use_connection._parser.maintenance_push_handler_func is not None
        assert in_use_connection.should_reconnect()

        # Free connection — the idle OSS path, wired then disconnected.
        assert free_connection._oss_cluster_maint_notifications_handler is oss_handler
        assert free_connection._parser.oss_cluster_maint_push_handler_func is not None
        assert free_connection._parser.maintenance_push_handler_func is not None

        # Existing connections must not retain the orphaned pool-handler binding.
        # A default (enabled) pool wires node_moving on each connection at
        # __init__; switching to OSS mode must clear it so no existing connection
        # is configured with both the node-moving and OSS cluster handlers.
        assert in_use_connection._parser.node_moving_push_handler_func is None
        assert in_use_connection._maint_notifications_pool_handler is None
        assert free_connection._parser.node_moving_push_handler_func is None
        assert free_connection._maint_notifications_pool_handler is None

        # OSS cluster mode and pool-handler mode are mutually exclusive. The pool
        # was created with the default (enabled) config, which wires a pool
        # handler in __init__; switching to OSS mode must clear it from both the
        # pool attribute and the shared connection kwargs so future connections
        # are not configured with both handlers.
        assert pool._maint_notifications_pool_handler is None
        assert "maint_notifications_pool_handler" not in pool.connection_kwargs
        assert (
            pool.connection_kwargs.get("oss_cluster_maint_notifications_handler")
            is oss_handler
        )

        # A connection created after the switch gets only the OSS cluster handler.
        new_connection = pool.make_connection()
        assert new_connection._maint_notifications_pool_handler is None
        assert new_connection._parser.node_moving_push_handler_func is None
        assert new_connection._oss_cluster_maint_notifications_handler is oss_handler
        assert new_connection._parser.oss_cluster_maint_push_handler_func is not None

    def test_update_config_on_oss_pool_updates_oss_handler(self):
        """A config-only update on a pool already in OSS cluster mode must update
        the OSS handler's config instead of being silently discarded.

        OSS cluster mode and pool-handler mode are mutually exclusive, so the
        update must not create a pool handler; it must apply the new config to
        the existing OSS handler and propagate it to connection kwargs and to
        existing connections.
        """
        config = MaintNotificationsConfig(enabled=True)
        pool = ConnectionPool(
            host=DEFAULT_ADDRESS.split(":")[0],
            port=int(DEFAULT_ADDRESS.split(":")[1]),
            protocol=3,
            maint_notifications_config=config,
        )
        free_connection = pool.make_connection()
        pool._available_connections.append(free_connection)

        oss_handler = OSSMaintNotificationsHandler(MagicMock(), config)
        pool.update_maint_notifications_config(
            config, oss_cluster_maint_notifications_handler=oss_handler
        )

        # Now issue a config-only update (no OSS handler passed).
        new_config = MaintNotificationsConfig(
            enabled=True, proactive_reconnect=True, relaxed_timeout=30
        )
        pool.update_maint_notifications_config(new_config)

        # The new config must land on the existing OSS handler, and the pool must
        # remain in OSS mode without an orphaned pool handler.
        assert oss_handler.config is new_config
        assert pool._oss_cluster_maint_notifications_handler is oss_handler
        assert pool._maint_notifications_pool_handler is None
        assert "maint_notifications_pool_handler" not in pool.connection_kwargs
        assert pool.connection_kwargs.get("maint_notifications_config") is new_config

        # Existing free connections must receive the new config, not the stale one.
        assert free_connection.maint_notifications_config is new_config

        # Disabling an already-enabled OSS pool is not allowed and must not be
        # silently accepted by the config-only update path.
        with pytest.raises(
            ValueError, match="Cannot disable maintenance notifications"
        ):
            pool.update_maint_notifications_config(
                MaintNotificationsConfig(enabled=False)
            )
        assert oss_handler.config is new_config

    def test_smigrated_failure_releases_in_progress_for_retry(self):
        """A SMIGRATED handling that raises must not leave the notification wedged
        in _in_progress.

        The in-progress reservation dedupes concurrent redeliveries of the same
        notification (arriving on different connections). If handling raises
        (e.g. nodes_manager.initialize() fails because startup nodes are briefly
        unreachable mid-migration), the notification must be released so a later
        redelivery can retry — otherwise the dedup guard skips it forever and the
        topology is never refreshed from that notification.
        """
        config = MaintNotificationsConfig(enabled=True)
        cluster_client = MagicMock()
        cluster_client.nodes_manager.nodes_cache = {}
        handler = OSSMaintNotificationsHandler(cluster_client, config)

        notification = OSSNodeMigratedNotification(
            id=5,
            nodes_to_slots_mapping={
                f"{DEFAULT_ADDRESS}": [{AFTER_MOVING_ADDRESS: "0-100"}]
            },
        )

        # First delivery: initialize() fails, so handling raises.
        cluster_client.nodes_manager.initialize.side_effect = RedisConnectionError(
            "startup nodes unreachable"
        )
        with pytest.raises(RedisConnectionError):
            handler.handle_oss_maintenance_completed_notification(notification)

        # The notification is released from _in_progress and was not marked
        # processed, so a redelivery is allowed to retry.
        assert notification not in handler._in_progress
        assert notification not in handler._processed_notifications

        # Second delivery (retry): initialize() now succeeds, so handling
        # completes and the notification is marked processed exactly once.
        cluster_client.nodes_manager.initialize.side_effect = None
        cluster_client.nodes_manager.get_node.return_value = None
        handler.handle_oss_maintenance_completed_notification(notification)

        assert notification not in handler._in_progress
        assert notification in handler._processed_notifications

    @pytest.mark.parametrize("pool_class", [ConnectionPool, BlockingConnectionPool])
    def test_connection_pool_creation_with_maintenance_notifications(self, pool_class):
        """Test that connection pools are created with maintenance notifications configuration."""
        # Create a pool and Redis client with maintenance notifications
        max_connections = 3 if pool_class == BlockingConnectionPool else 10
        test_redis_client = self._get_client(
            pool_class, max_connections=max_connections
        )
        test_pool = test_redis_client.connection_pool

        try:
            assert (
                test_pool.connection_kwargs.get("maint_notifications_config")
                == self.config
            )
            # Pool should have maintenance notifications enabled
            assert test_pool.maint_notifications_enabled() is True

            # Create and set a pool handler
            test_pool.update_maint_notifications_config(self.config)
            pool_handler = test_pool._maint_notifications_pool_handler

            # Validate that the handler is properly set on the pool
            assert (
                test_pool.connection_kwargs.get("maint_notifications_pool_handler")
                == pool_handler
            )
            assert (
                test_pool.connection_kwargs.get("maint_notifications_config")
                == pool_handler.config
            )

            # Verify that the pool handler has the correct configuration
            assert pool_handler.pool == test_pool
            assert pool_handler.config == self.config

        finally:
            if hasattr(test_pool, "disconnect"):
                test_pool.disconnect()

    @pytest.mark.parametrize("pool_class", [ConnectionPool, BlockingConnectionPool])
    @pytest.mark.parametrize("enabled, raises", [(True, False), (False, True)])
    def test_pool_get_connection_handles_pending_push_after_reconnect(
        self, pool_class, enabled, raises
    ):
        max_connections = 3 if pool_class == BlockingConnectionPool else 10
        pool = pool_class(
            host=DEFAULT_ADDRESS.split(":")[0],
            port=int(DEFAULT_ADDRESS.split(":")[1]),
            max_connections=max_connections,
            protocol=3,
            maint_notifications_config=MaintNotificationsConfig(enabled=enabled),
        )
        connection = MagicMock()
        connection.pid = pool.pid
        connection.can_read.side_effect = [RedisConnectionError("closed"), True]
        connection.should_reconnect.return_value = False

        if pool_class == BlockingConnectionPool:
            pool.pool.get_nowait()
            pool.pool.put_nowait(connection)
            pool._connections.append(connection)
        else:
            pool._available_connections.append(connection)

        returned_connection = None
        try:
            if raises:
                with pytest.raises(RedisConnectionError, match="Connection not ready"):
                    pool.get_connection()
            else:
                returned_connection = pool.get_connection()
                assert returned_connection is connection

            assert connection.connect.call_count == 2
            connection.disconnect.assert_called_once()
        finally:
            if returned_connection is connection:
                pool.release(connection)
            pool.disconnect()

    @pytest.mark.parametrize("pool_class", [ConnectionPool, BlockingConnectionPool])
    def test_redis_operations_with_mock_sockets(self, pool_class):
        """
        Test basic Redis operations work with mocked sockets and proper response parsing.
        Basically with test - the mocked socket is validated.
        """
        # Create a pool and Redis client with maintenance notifications
        test_redis_client = self._get_client(pool_class, max_connections=5)

        try:
            # Perform Redis operations that should work with our improved mock responses
            result_set = test_redis_client.set("hello", "world")
            result_get = test_redis_client.get("hello")

            # Verify operations completed successfully
            assert result_set is True
            assert result_get == b"world"

            # Verify socket interactions
            assert len(self.mock_sockets) >= 1
            assert self.mock_sockets[0].connected
            assert len(self.mock_sockets[0].sent_data) >= 2  # HELLO, SET, GET commands

            # Verify that the connection has maintenance notification handler
            connection = test_redis_client.connection_pool.get_connection()
            assert hasattr(connection, "_maint_notifications_connection_handler")
            test_redis_client.connection_pool.release(connection)

        finally:
            if hasattr(test_redis_client.connection_pool, "disconnect"):
                test_redis_client.connection_pool.disconnect()

    def test_pool_handler_with_migrating_notification(self):
        """Test that pool handler correctly handles migrating notifications."""
        # Create a pool and Redis client with maintenance notifications
        test_redis_client = self._get_client(ConnectionPool)
        test_pool = test_redis_client.connection_pool

        try:
            # Create and set a pool handler
            pool_handler = MaintNotificationsPoolHandler(test_pool, self.config)

            # Create a migrating notification (not handled by pool handler)
            migrating_notification = NodeMigratingNotification(id=1, ttl=5)

            # Mock the required functions
            with (
                patch.object(
                    pool_handler, "remove_expired_notifications"
                ) as mock_remove_expired,
                patch.object(
                    pool_handler, "handle_node_moving_notification"
                ) as mock_handle_moving,
                patch("redis.maint_notifications.logger.error") as mock_logging_error,
            ):
                # Pool handler should return None for migrating notifications (not its responsibility)
                pool_handler.handle_notification(migrating_notification)

                # Validate that remove_expired_notifications has been called once
                mock_remove_expired.assert_called_once()

                # Validate that handle_node_moving_notification hasn't been called
                mock_handle_moving.assert_not_called()

                # Validate that logging.error has been called once
                mock_logging_error.assert_called_once()

        finally:
            if hasattr(test_pool, "disconnect"):
                test_pool.disconnect()

    @pytest.mark.parametrize("pool_class", [ConnectionPool, BlockingConnectionPool])
    def test_migration_related_notifications_handling_integration(self, pool_class):
        """
        Test full integration of migration-related notifications (MIGRATING/MIGRATED) handling.

        This test validates the complete migration lifecycle:
        1. Executes 5 Redis commands sequentially
        2. Injects MIGRATING push message before command 2 (SET key_receive_migrating)
        3. Validates socket timeout is updated to relaxed value (30s) after MIGRATING
        4. Executes commands 3-4 while timeout remains relaxed
        5. Injects MIGRATED push message before command 5 (SET key_receive_migrated)
        6. Validates socket timeout is restored after MIGRATED
        7. Tests both ConnectionPool and BlockingConnectionPool implementations
        8. Uses proper RESP3 push message format for realistic protocol simulation
        """
        # Create a pool and Redis client with maintenance notifications
        test_redis_client = self._get_client(pool_class, max_connections=10)

        try:
            # Command 1: Initial command
            key1 = "key1"
            value1 = "value1"
            result1 = test_redis_client.set(key1, value1)

            # Validate Command 1 result
            assert result1 is True, "Command 1 (SET key1) failed"

            # Command 2: This SET command will receive MIGRATING push message before response
            key_migrating = "key_receive_migrating"
            value_migrating = "value2"
            result2 = test_redis_client.set(key_migrating, value_migrating)

            # Validate Command 2 result
            assert result2 is True, "Command 2 (SET key_receive_migrating) failed"

            # Step 4: Validate timeout was updated to relaxed value after MIGRATING
            self._validate_current_timeout(30, "Right after MIGRATING is received. ")

            # Command 3: Another command while timeout is still relaxed
            result3 = test_redis_client.get(key1)

            # Validate Command 3 result
            expected_value3 = value1.encode()
            assert result3 == expected_value3, (
                f"Command 3 (GET key1) failed. Expected {expected_value3}, got {result3}"
            )

            # Command 4: Execute command (step 5)
            result4 = test_redis_client.get(key_migrating)

            # Validate Command 4 result
            expected_value4 = value_migrating.encode()
            assert result4 == expected_value4, (
                f"Command 4 (GET key_receive_migrating) failed. Expected {expected_value4}, got {result4}"
            )

            # Step 6: Validate socket timeout is still relaxed during commands 3-4
            self._validate_current_timeout(
                30,
                "Execute a command with a connection extracted from the pool (after it has received MIGRATING)",
            )

            # Command 5: This SET command will receive
            # MIGRATED push message before actual response
            key_migrated = "key_receive_migrated"
            value_migrated = "value3"
            result5 = test_redis_client.set(key_migrated, value_migrated)

            # Validate Command 5 result
            assert result5 is True, "Command 5 (SET key_receive_migrated) failed"

            # Step 8: Validate socket timeout is reversed back to original after MIGRATED
            self._validate_current_timeout(DEFAULT_SOCKET_TIMEOUT)

            # Verify maintenance notifications were processed correctly
            # The key is that we have at least 1 socket and all operations succeeded
            assert len(self.mock_sockets) >= 1, (
                f"Expected at least 1 socket for operations, got {len(self.mock_sockets)}"
            )

        finally:
            if hasattr(test_redis_client.connection_pool, "disconnect"):
                test_redis_client.connection_pool.disconnect()

    @pytest.mark.parametrize("pool_class", [ConnectionPool, BlockingConnectionPool])
    def test_migrating_notification_with_disabled_relaxed_timeout(self, pool_class):
        """
        Test maintenance notifications handling when relaxed timeout is disabled.

        This test validates that when relaxed_timeout is disabled (-1):
        1. MIGRATING, MIGRATED, FAILING_OVER, and FAILED_OVER notifications are received and processed
        2. No timeout updates are applied to connections
        3. Socket timeouts remain unchanged during all maintenance notifications
        4. Tests both ConnectionPool and BlockingConnectionPool implementations
        5. Tests the complete lifecycle: MIGRATING -> MIGRATED -> FAILING_OVER -> FAILED_OVER
        """
        # Create config with disabled relaxed timeout
        disabled_config = MaintNotificationsConfig(
            enabled=True,
            relaxed_timeout=-1,  # This means the relaxed timeout is Disabled
        )

        # Create a pool and Redis client with disabled relaxed timeout config
        test_redis_client = self._get_client(
            pool_class, max_connections=5, maint_notifications_config=disabled_config
        )

        try:
            # Command 1: Initial command
            key1 = "key1"
            value1 = "value1"
            result1 = test_redis_client.set(key1, value1)

            # Validate Command 1 result
            assert result1 is True, "Command 1 (SET key1) failed"

            # Command 2: This SET command will receive MIGRATING push message before response
            key_migrating = "key_receive_migrating"
            value_migrating = "value2"
            result2 = test_redis_client.set(key_migrating, value_migrating)

            # Validate Command 2 result
            assert result2 is True, "Command 2 (SET key_receive_migrating) failed"

            # Validate timeout was NOT updated (relaxed is disabled)
            # Should remain at the default timeout, not relaxed to 30s
            self._validate_current_timeout(DEFAULT_SOCKET_TIMEOUT)

            # Command 3: Another command to verify timeout remains unchanged
            result3 = test_redis_client.get(key1)

            # Validate Command 3 result
            expected_value3 = value1.encode()
            assert result3 == expected_value3, (
                f"Command 3 (GET key1) failed. Expected: {expected_value3}, Got: {result3}"
            )

            # Command 4: This SET command will receive MIGRATED push message before response
            key_migrated = "key_receive_migrated"
            value_migrated = "value3"
            result4 = test_redis_client.set(key_migrated, value_migrated)

            # Validate Command 4 result
            assert result4 is True, "Command 4 (SET key_receive_migrated) failed"

            # Validate timeout is still NOT updated after MIGRATED (relaxed is disabled)
            self._validate_current_timeout(DEFAULT_SOCKET_TIMEOUT)

            # Command 5: This SET command will receive FAILING_OVER push message before response
            key_failing_over = "key_receive_failing_over"
            value_failing_over = "value4"
            result5 = test_redis_client.set(key_failing_over, value_failing_over)

            # Validate Command 5 result
            assert result5 is True, "Command 5 (SET key_receive_failing_over) failed"

            # Validate timeout is still NOT updated after FAILING_OVER (relaxed is disabled)
            self._validate_current_timeout(DEFAULT_SOCKET_TIMEOUT)

            # Command 6: Another command to verify timeout remains unchanged during failover
            result6 = test_redis_client.get(key_failing_over)

            # Validate Command 6 result
            expected_value6 = value_failing_over.encode()
            assert result6 == expected_value6, (
                f"Command 6 (GET key_receive_failing_over) failed. Expected: {expected_value6}, Got: {result6}"
            )

            # Command 7: This SET command will receive FAILED_OVER push message before response
            key_failed_over = "key_receive_failed_over"
            value_failed_over = "value5"
            result7 = test_redis_client.set(key_failed_over, value_failed_over)

            # Validate Command 7 result
            assert result7 is True, "Command 7 (SET key_receive_failed_over) failed"

            # Validate timeout is still NOT updated after FAILED_OVER (relaxed is disabled)
            self._validate_current_timeout(DEFAULT_SOCKET_TIMEOUT)

            # Command 8: Final command to verify timeout remains unchanged after all notifications
            result8 = test_redis_client.get(key_failed_over)

            # Validate Command 8 result
            expected_value8 = value_failed_over.encode()
            assert result8 == expected_value8, (
                f"Command 8 (GET key_receive_failed_over) failed. Expected: {expected_value8}, Got: {result8}"
            )

            # Verify maintenance notifications were processed correctly
            # The key is that we have at least 1 socket and all operations succeeded
            assert len(self.mock_sockets) >= 1, (
                f"Expected at least 1 socket for operations, got {len(self.mock_sockets)}"
            )

        finally:
            if hasattr(test_redis_client.connection_pool, "disconnect"):
                test_redis_client.connection_pool.disconnect()

    @pytest.mark.parametrize("pool_class", [ConnectionPool, BlockingConnectionPool])
    def test_failing_over_related_notifications_handling_integration(self, pool_class):
        """
        Test full integration of failing-over-related notifications (FAILING_OVER/FAILED_OVER) handling.

        This test validates the complete FAILING_OVER -> FAILED_OVER lifecycle:
        1. Executes 5 Redis commands sequentially
        2. Injects FAILING_OVER push message before command 2 (SET key_receive_failing_over)
        3. Validates socket timeout is updated to relaxed value (30s) after FAILING_OVER
        4. Executes commands 3-4 while timeout remains relaxed
        5. Injects FAILED_OVER push message before command 5 (SET key_receive_failed_over)
        6. Validates socket timeout is restored after FAILED_OVER
        7. Tests both ConnectionPool and BlockingConnectionPool implementations
        8. Uses proper RESP3 push message format for realistic protocol simulation
        """
        # Create a pool and Redis client with maintenance notifications
        test_redis_client = self._get_client(pool_class, max_connections=10)

        try:
            # Command 1: Initial command
            key1 = "key1"
            value1 = "value1"
            result1 = test_redis_client.set(key1, value1)

            # Validate Command 1 result
            assert result1 is True, "Command 1 (SET key1) failed"

            # Command 2: This SET command will receive FAILING_OVER push message before response
            key_failing_over = "key_receive_failing_over"
            value_failing_over = "value4"
            result2 = test_redis_client.set(key_failing_over, value_failing_over)

            # Validate Command 2 result
            assert result2 is True, "Command 2 (SET key_receive_failing_over) failed"

            # Step 4: Validate timeout was updated to relaxed value after MIGRATING
            self._validate_current_timeout(30, "Right after FAILING_OVER is received. ")

            # Command 3: Another command while timeout is still relaxed
            result3 = test_redis_client.get(key1)

            # Validate Command 3 result
            expected_value3 = value1.encode()
            assert result3 == expected_value3, (
                f"Command 3 (GET key1) failed. Expected {expected_value3}, got {result3}"
            )

            # Command 4: Execute command (step 5)
            result4 = test_redis_client.get(key_failing_over)

            # Validate Command 4 result
            expected_value4 = value_failing_over.encode()
            assert result4 == expected_value4, (
                f"Command 4 (GET key_receive_failing_over) failed. Expected {expected_value4}, got {result4}"
            )

            # Step 6: Validate socket timeout is still relaxed during commands 3-4
            self._validate_current_timeout(
                30,
                "Execute a command with a connection extracted from the pool (after it has received FAILING_OVER)",
            )

            # Command 5: This SET command will receive
            # FAILED_OVER push message before actual response
            key_failed_over = "key_receive_failed_over"
            value_migrated = "value3"
            result5 = test_redis_client.set(key_failed_over, value_migrated)

            # Validate Command 5 result
            assert result5 is True, "Command 5 (SET key_receive_failed_over) failed"

            # Step 8: Validate socket timeout is reversed back to original after FAILED_OVER
            self._validate_current_timeout(DEFAULT_SOCKET_TIMEOUT)

            # Verify maintenance notifications were processed correctly
            # The key is that we have at least 1 socket and all operations succeeded
            assert len(self.mock_sockets) >= 1, (
                f"Expected at least 1 socket for operations, got {len(self.mock_sockets)}"
            )

        finally:
            if hasattr(test_redis_client.connection_pool, "disconnect"):
                test_redis_client.connection_pool.disconnect()

    @pytest.mark.parametrize("pool_class", [ConnectionPool, BlockingConnectionPool])
    def test_moving_related_notifications_handling_integration(self, pool_class):
        """
        Test full integration of moving-related notifications (MOVING) handling with Redis commands.

        This test validates the complete MOVING notification lifecycle:
        1. Creates multiple connections in the pool
        2. Executes a Redis command that triggers a MOVING push message
        3. Validates that pool configuration is updated with temporary
           address and timeout - for new connections creation
        4. Validates that existing connections are marked for disconnection
        5. Tests both ConnectionPool and BlockingConnectionPool implementations
        """
        # Create a pool and Redis client with maintenance notifications and pool handler
        test_redis_client = self._get_client(pool_class, max_connections=10)

        try:
            # Create several connections and return them in the pool
            connections = []
            for _ in range(10):
                connection = test_redis_client.connection_pool.get_connection()
                connections.append(connection)

            for connection in connections:
                test_redis_client.connection_pool.release(connection)

            # Take 5 connections to be "in use"
            in_use_connections = []
            for _ in range(5):
                connection = test_redis_client.connection_pool.get_connection()
                in_use_connections.append(connection)

            # Validate all connections are connected prior MOVING notification
            self._validate_disconnected(0)

            # Run command that will receive and handle MOVING notification
            key_moving = "key_receive_moving_0"
            value_moving = "value3_0"
            # the connection used for the command is expected to be reconnected to the new address
            # before it is returned to the pool
            result2 = test_redis_client.set(key_moving, value_moving)

            # Validate Command 2 result
            assert result2 is True, "Command 2 (SET key_receive_moving) failed"

            # Validate pool and connections settings were updated according to MOVING notification
            expected_notification_hash = hash(MOVING_NOTIFICATION)

            Helpers.validate_conn_kwargs(
                pool=test_redis_client.connection_pool,
                expected_maintenance_state=MaintenanceState.MOVING,
                expected_maintenance_notification_hash=expected_notification_hash,
                expected_host_address=AFTER_MOVING_ADDRESS.split(":")[0],
                expected_port=int(DEFAULT_ADDRESS.split(":")[1]),
                expected_socket_timeout=self.config.relaxed_timeout,
                expected_socket_connect_timeout=self.config.relaxed_timeout,
                expected_orig_host_address=DEFAULT_ADDRESS.split(":")[0],
                expected_orig_socket_timeout=DEFAULT_SOCKET_TIMEOUT,
                expected_orig_socket_connect_timeout=DEFAULT_SOCKET_CONNECT_TIMEOUT,
            )
            self._validate_disconnected(5)
            self._validate_connected(6)
            Helpers.validate_in_use_connections_state(
                in_use_connections,
                expected_state=MaintenanceState.MOVING,
                expected_host_address=AFTER_MOVING_ADDRESS.split(":")[0],
                expected_socket_timeout=self.config.relaxed_timeout,
                expected_socket_connect_timeout=self.config.relaxed_timeout,
                expected_orig_host_address=DEFAULT_ADDRESS.split(":")[0],
                expected_orig_socket_timeout=DEFAULT_SOCKET_TIMEOUT,
                expected_orig_socket_connect_timeout=DEFAULT_SOCKET_CONNECT_TIMEOUT,
                expected_current_socket_timeout=self.config.relaxed_timeout,
                expected_current_peername=DEFAULT_ADDRESS.split(":")[
                    0
                ],  # the in use connections reconnect when they complete their current task
            )
            Helpers.validate_free_connections_state(
                pool=test_redis_client.connection_pool,
                expected_state=MaintenanceState.MOVING,
                expected_host_address=AFTER_MOVING_ADDRESS.split(":")[0],
                expected_socket_timeout=self.config.relaxed_timeout,
                expected_socket_connect_timeout=self.config.relaxed_timeout,
                expected_orig_host_address=DEFAULT_ADDRESS.split(":")[0],
                expected_orig_socket_timeout=DEFAULT_SOCKET_TIMEOUT,
                expected_orig_socket_connect_timeout=DEFAULT_SOCKET_CONNECT_TIMEOUT,
                should_be_connected_count=1,
                connected_to_tmp_address=True,
            )
            # Wait for MOVING timeout to expire and the moving completed handler to run
            self._advance_timers(MOVING_TIMEOUT + 0.5)

            Helpers.validate_in_use_connections_state(
                in_use_connections,
                expected_state=MaintenanceState.NONE,
                expected_host_address=DEFAULT_ADDRESS.split(":")[0],
                expected_socket_timeout=DEFAULT_SOCKET_TIMEOUT,
                expected_socket_connect_timeout=DEFAULT_SOCKET_CONNECT_TIMEOUT,
                expected_orig_host_address=DEFAULT_ADDRESS.split(":")[0],
                expected_orig_socket_timeout=DEFAULT_SOCKET_TIMEOUT,
                expected_orig_socket_connect_timeout=DEFAULT_SOCKET_CONNECT_TIMEOUT,
                expected_current_socket_timeout=DEFAULT_SOCKET_TIMEOUT,
                expected_current_peername=DEFAULT_ADDRESS.split(":")[0],
            )
            Helpers.validate_conn_kwargs(
                pool=test_redis_client.connection_pool,
                expected_maintenance_state=MaintenanceState.NONE,
                expected_maintenance_notification_hash=None,
                expected_host_address=DEFAULT_ADDRESS.split(":")[0],
                expected_port=int(DEFAULT_ADDRESS.split(":")[1]),
                expected_socket_timeout=DEFAULT_SOCKET_TIMEOUT,
                expected_socket_connect_timeout=DEFAULT_SOCKET_CONNECT_TIMEOUT,
                expected_orig_host_address=DEFAULT_ADDRESS.split(":")[0],
                expected_orig_socket_timeout=DEFAULT_SOCKET_TIMEOUT,
                expected_orig_socket_connect_timeout=DEFAULT_SOCKET_CONNECT_TIMEOUT,
            )
            Helpers.validate_free_connections_state(
                pool=test_redis_client.connection_pool,
                expected_host_address=DEFAULT_ADDRESS.split(":")[0],
                expected_socket_timeout=DEFAULT_SOCKET_TIMEOUT,
                expected_socket_connect_timeout=DEFAULT_SOCKET_CONNECT_TIMEOUT,
                expected_orig_host_address=DEFAULT_ADDRESS.split(":")[0],
                expected_orig_socket_timeout=DEFAULT_SOCKET_TIMEOUT,
                expected_orig_socket_connect_timeout=DEFAULT_SOCKET_CONNECT_TIMEOUT,
                should_be_connected_count=1,
                connected_to_tmp_address=True,
                expected_state=MaintenanceState.NONE,
            )
        finally:
            if hasattr(test_redis_client.connection_pool, "disconnect"):
                test_redis_client.connection_pool.disconnect()

    @pytest.mark.parametrize("pool_class", [ConnectionPool, BlockingConnectionPool])
    def test_moving_none_notifications_handling_integration(self, pool_class):
        """
        Test full integration of moving-related notifications (MOVING) handling with Redis commands.

        This test validates the complete MOVING notification lifecycle,
        when the push notification doesn't contain host and port:
        1. Creates multiple connections in the pool
        2. Executes a Redis command that triggers a MOVING with "null" push message
        3. Validates that pool configuration is updated with temporary
           address and timeout - for new connections creation
        4. Validates that existing connections are marked for disconnection after ttl/2 seconds
        5. Tests both ConnectionPool and BlockingConnectionPool implementations
        """
        # Create a pool and Redis client with maintenance notifications and pool handler
        test_redis_client = self._get_client(pool_class, max_connections=10)

        try:
            # Create several connections and return them in the pool
            connections = []
            for _ in range(10):
                connection = test_redis_client.connection_pool.get_connection()
                connections.append(connection)

            for connection in connections:
                test_redis_client.connection_pool.release(connection)

            # Take 5 connections to be "in use"
            in_use_connections = []
            for _ in range(5):
                connection = test_redis_client.connection_pool.get_connection()
                in_use_connections.append(connection)

            # Validate all connections are connected prior MOVING notification
            self._validate_disconnected(0)

            # Run command that will receive and handle MOVING notification
            key_moving = "key_receive_moving_none_0"
            value_moving = "value3_0"

            # the connection used for the command is expected to be reconnected to the new address
            # before it is returned to the pool
            result2 = test_redis_client.set(key_moving, value_moving)

            # Validate Command 2 result
            assert result2 is True, "Command 2 (SET key_receive_moving) failed"

            # Validate pool and connections settings were updated according to MOVING notification
            Helpers.validate_conn_kwargs(
                pool=test_redis_client.connection_pool,
                expected_maintenance_state=MaintenanceState.MOVING,
                expected_maintenance_notification_hash=hash(MOVING_NONE_NOTIFICATION),
                expected_host_address=DEFAULT_ADDRESS.split(":")[0],
                expected_port=int(DEFAULT_ADDRESS.split(":")[1]),
                expected_socket_timeout=self.config.relaxed_timeout,
                expected_socket_connect_timeout=self.config.relaxed_timeout,
                expected_orig_host_address=DEFAULT_ADDRESS.split(":")[0],
                expected_orig_socket_timeout=DEFAULT_SOCKET_TIMEOUT,
                expected_orig_socket_connect_timeout=DEFAULT_SOCKET_CONNECT_TIMEOUT,
            )
            self._validate_disconnected(0)
            self._validate_connected(10)
            Helpers.validate_in_use_connections_state(
                in_use_connections,
                expected_should_reconnect=False,
                expected_state=MaintenanceState.MOVING,
                expected_host_address=DEFAULT_ADDRESS.split(":")[0],
                expected_socket_timeout=self.config.relaxed_timeout,
                expected_socket_connect_timeout=self.config.relaxed_timeout,
                expected_orig_host_address=DEFAULT_ADDRESS.split(":")[0],
                expected_orig_socket_timeout=DEFAULT_SOCKET_TIMEOUT,
                expected_orig_socket_connect_timeout=DEFAULT_SOCKET_CONNECT_TIMEOUT,
                expected_current_socket_timeout=self.config.relaxed_timeout,
                expected_current_peername=DEFAULT_ADDRESS.split(":")[
                    0
                ],  # the in use connections reconnect when they complete their current task
            )
            Helpers.validate_free_connections_state(
                pool=test_redis_client.connection_pool,
                expected_state=MaintenanceState.MOVING,
                expected_host_address=DEFAULT_ADDRESS.split(":")[0],
                expected_socket_timeout=self.config.relaxed_timeout,
                expected_socket_connect_timeout=self.config.relaxed_timeout,
                expected_orig_host_address=DEFAULT_ADDRESS.split(":")[0],
                expected_orig_socket_timeout=DEFAULT_SOCKET_TIMEOUT,
                expected_orig_socket_connect_timeout=DEFAULT_SOCKET_CONNECT_TIMEOUT,
                should_be_connected_count=5,
                connected_to_tmp_address=False,
            )
            # Wait for half of MOVING timeout to expire and the proactive reconnect to run
            self._advance_timers(MOVING_TIMEOUT / 2 + 0.2)
            Helpers.validate_in_use_connections_state(
                in_use_connections,
                expected_should_reconnect=True,
                expected_state=MaintenanceState.MOVING,
                expected_host_address=DEFAULT_ADDRESS.split(":")[0],
                expected_socket_timeout=self.config.relaxed_timeout,
                expected_socket_connect_timeout=self.config.relaxed_timeout,
                expected_orig_host_address=DEFAULT_ADDRESS.split(":")[0],
                expected_orig_socket_timeout=DEFAULT_SOCKET_TIMEOUT,
                expected_orig_socket_connect_timeout=DEFAULT_SOCKET_CONNECT_TIMEOUT,
                expected_current_socket_timeout=self.config.relaxed_timeout,
                expected_current_peername=DEFAULT_ADDRESS.split(":")[
                    0
                ],  # the in use connections reconnect when they complete their current task
            )
            self._validate_disconnected(5)
            self._validate_connected(5)

            # Wait for MOVING timeout to expire and the moving completed handler to run
            self._advance_timers(MOVING_TIMEOUT / 2 + 0.2)
            Helpers.validate_in_use_connections_state(
                in_use_connections,
                expected_state=MaintenanceState.NONE,
                expected_host_address=DEFAULT_ADDRESS.split(":")[0],
                expected_socket_timeout=DEFAULT_SOCKET_TIMEOUT,
                expected_socket_connect_timeout=DEFAULT_SOCKET_CONNECT_TIMEOUT,
                expected_orig_host_address=DEFAULT_ADDRESS.split(":")[0],
                expected_orig_socket_timeout=DEFAULT_SOCKET_TIMEOUT,
                expected_orig_socket_connect_timeout=DEFAULT_SOCKET_CONNECT_TIMEOUT,
                expected_current_socket_timeout=DEFAULT_SOCKET_TIMEOUT,
                expected_current_peername=DEFAULT_ADDRESS.split(":")[0],
            )
            Helpers.validate_conn_kwargs(
                pool=test_redis_client.connection_pool,
                expected_maintenance_state=MaintenanceState.NONE,
                expected_maintenance_notification_hash=None,
                expected_host_address=DEFAULT_ADDRESS.split(":")[0],
                expected_port=int(DEFAULT_ADDRESS.split(":")[1]),
                expected_socket_timeout=DEFAULT_SOCKET_TIMEOUT,
                expected_socket_connect_timeout=DEFAULT_SOCKET_CONNECT_TIMEOUT,
                expected_orig_host_address=DEFAULT_ADDRESS.split(":")[0],
                expected_orig_socket_timeout=DEFAULT_SOCKET_TIMEOUT,
                expected_orig_socket_connect_timeout=DEFAULT_SOCKET_CONNECT_TIMEOUT,
            )
            Helpers.validate_free_connections_state(
                pool=test_redis_client.connection_pool,
                expected_host_address=DEFAULT_ADDRESS.split(":")[0],
                expected_socket_timeout=DEFAULT_SOCKET_TIMEOUT,
                expected_socket_connect_timeout=DEFAULT_SOCKET_CONNECT_TIMEOUT,
                expected_orig_host_address=DEFAULT_ADDRESS.split(":")[0],
                expected_orig_socket_timeout=DEFAULT_SOCKET_TIMEOUT,
                expected_orig_socket_connect_timeout=DEFAULT_SOCKET_CONNECT_TIMEOUT,
                should_be_connected_count=0,
                connected_to_tmp_address=True,
                expected_state=MaintenanceState.NONE,
            )
        finally:
            if hasattr(test_redis_client.connection_pool, "disconnect"):
                test_redis_client.connection_pool.disconnect()

    @pytest.mark.parametrize("pool_class", [ConnectionPool, BlockingConnectionPool])
    def test_create_new_conn_while_moving_not_expired(self, pool_class):
        """
        Test creating new connections while MOVING notification is active (not expired).

        This test validates that:
        1. After MOVING notification is processed, new connections are created with temporary address
        2. New connections inherit the relaxed timeout settings
        3. Pool configuration is properly applied to newly created connections
        """
        # Create a pool and Redis client with maintenance notifications and pool handler
        test_redis_client = self._get_client(pool_class, max_connections=10)

        try:
            # Create several connections and return them in the pool
            connections = []
            for _ in range(5):
                connection = test_redis_client.connection_pool.get_connection()
                connections.append(connection)

            for connection in connections:
                test_redis_client.connection_pool.release(connection)

            # Take 3 connections to be "in use"
            in_use_connections = []
            for _ in range(3):
                connection = test_redis_client.connection_pool.get_connection()
                in_use_connections.append(connection)

            # Validate all connections are connected prior MOVING notification
            self._validate_disconnected(0)

            # Run command that will receive and handle MOVING notification
            key_moving = "key_receive_moving_0"
            value_moving = "value3_0"
            result = test_redis_client.set(key_moving, value_moving)

            # Validate command result
            assert result is True, "SET key_receive_moving command failed"

            # Validate pool and connections settings were updated according to MOVING notification
            Helpers.validate_conn_kwargs(
                pool=test_redis_client.connection_pool,
                expected_maintenance_state=MaintenanceState.MOVING,
                expected_maintenance_notification_hash=hash(MOVING_NOTIFICATION),
                expected_orig_host_address=DEFAULT_ADDRESS.split(":")[0],
                expected_port=int(DEFAULT_ADDRESS.split(":")[1]),
                expected_orig_socket_timeout=DEFAULT_SOCKET_TIMEOUT,
                expected_orig_socket_connect_timeout=DEFAULT_SOCKET_CONNECT_TIMEOUT,
                expected_host_address=AFTER_MOVING_ADDRESS.split(":")[0],
                expected_socket_timeout=self.config.relaxed_timeout,
                expected_socket_connect_timeout=self.config.relaxed_timeout,
            )

            # Now get several more connections to force creation of new ones
            # This should create new connections with the temporary address
            old_connections = []
            for _ in range(2):
                connection = test_redis_client.connection_pool.get_connection()
                old_connections.append(connection)

            new_connection = test_redis_client.connection_pool.get_connection()

            # Validate that new connections are created with temporary address and relaxed timeout
            # and when connecting those configs are used
            # get_connection() returns a connection that is already connected
            assert new_connection.host == AFTER_MOVING_ADDRESS.split(":")[0]
            assert new_connection.socket_timeout is self.config.relaxed_timeout
            # New connections should be connected to the temporary address
            assert new_connection._get_socket() is not None
            assert new_connection._get_socket().connected is True
            assert (
                new_connection._get_socket().getpeername()[0]
                == AFTER_MOVING_ADDRESS.split(":")[0]
            )
            assert (
                new_connection._get_socket().gettimeout() == self.config.relaxed_timeout
            )

        finally:
            if hasattr(test_redis_client.connection_pool, "disconnect"):
                test_redis_client.connection_pool.disconnect()

    @pytest.mark.parametrize("pool_class", [ConnectionPool, BlockingConnectionPool])
    def test_create_new_conn_after_moving_expires(self, pool_class):
        """
        Test creating new connections after MOVING notification expires.

        This test validates that:
        1. After MOVING timeout expires, new connections use original address
        2. Pool configuration is reset to original values
        3. New connections don't inherit temporary settings
        """
        # Create a pool and Redis client with maintenance notifications and pool handler
        test_redis_client = self._get_client(pool_class, max_connections=10)

        try:
            # Create several connections and return them in the pool
            connections = []
            for _ in range(5):
                connection = test_redis_client.connection_pool.get_connection()
                connections.append(connection)

            for connection in connections:
                test_redis_client.connection_pool.release(connection)

            # Take 3 connections to be "in use"
            in_use_connections = []
            for _ in range(3):
                connection = test_redis_client.connection_pool.get_connection()
                in_use_connections.append(connection)

            # Run command that will receive and handle MOVING notification
            key_moving = "key_receive_moving_0"
            value_moving = "value3_0"
            result = test_redis_client.set(key_moving, value_moving)

            # Validate command result
            assert result is True, "SET key_receive_moving command failed"

            # Wait for MOVING timeout to expire
            self._advance_timers(MOVING_TIMEOUT + 0.5)

            # Now get several new connections after expiration
            old_connections = []
            for _ in range(2):
                connection = test_redis_client.connection_pool.get_connection()
                old_connections.append(connection)

            new_connection = test_redis_client.connection_pool.get_connection()

            # Validate that new connections are created with original address (no temporary settings)
            assert new_connection.orig_host_address == DEFAULT_ADDRESS.split(":")[0]
            assert new_connection.orig_socket_timeout == DEFAULT_SOCKET_TIMEOUT
            # New connections should be connected to the original address
            assert new_connection._get_socket() is not None
            assert new_connection._get_socket().connected is True
            assert new_connection._get_socket().gettimeout() == DEFAULT_SOCKET_TIMEOUT

        finally:
            if hasattr(test_redis_client.connection_pool, "disconnect"):
                test_redis_client.connection_pool.disconnect()

    @pytest.mark.parametrize("pool_class", [ConnectionPool, BlockingConnectionPool])
    def test_receive_migrated_after_moving(self, pool_class):
        """
        Test receiving MIGRATED notification after MOVING notification.

        This test validates the complete MOVING -> MIGRATED lifecycle:
        1. MOVING notification is processed and temporary settings are applied
        2. MIGRATED notification is received during command execution
        3. Temporary settings are cleared after MIGRATED
        4. Pool configuration is restored to original values

        Note: When MIGRATED comes after MOVING and MOVING hasn't yet expired,
        it should not decrease timeouts (future refactoring consideration).
        """
        # Create a pool and Redis client with maintenance notifications and pool handler
        test_redis_client = self._get_client(pool_class, max_connections=10)

        try:
            # Create several connections and return them in the pool
            connections = []
            for _ in range(5):
                connection = test_redis_client.connection_pool.get_connection()
                connections.append(connection)

            for connection in connections:
                test_redis_client.connection_pool.release(connection)

            # Take 3 connections to be "in use"
            in_use_connections = []
            for _ in range(3):
                connection = test_redis_client.connection_pool.get_connection()
                in_use_connections.append(connection)

            # Validate all connections are connected prior MOVING notification
            self._validate_disconnected(0)

            # Step 1: Run command that will receive and handle MOVING notification
            key_moving = "key_receive_moving_0"
            value_moving = "value3_0"
            result_moving = test_redis_client.set(key_moving, value_moving)

            # Validate MOVING command result
            assert result_moving is True, "SET key_receive_moving command failed"

            # Validate pool and connections settings were updated according to MOVING notification
            Helpers.validate_conn_kwargs(
                pool=test_redis_client.connection_pool,
                expected_maintenance_state=MaintenanceState.MOVING,
                expected_maintenance_notification_hash=hash(MOVING_NOTIFICATION),
                expected_orig_host_address=DEFAULT_ADDRESS.split(":")[0],
                expected_port=int(DEFAULT_ADDRESS.split(":")[1]),
                expected_orig_socket_timeout=DEFAULT_SOCKET_TIMEOUT,
                expected_orig_socket_connect_timeout=DEFAULT_SOCKET_CONNECT_TIMEOUT,
                expected_host_address=AFTER_MOVING_ADDRESS.split(":")[0],
                expected_socket_timeout=self.config.relaxed_timeout,
                expected_socket_connect_timeout=self.config.relaxed_timeout,
            )

            # TODO validate current socket timeout

            # Step 2: Run command that will receive and handle MIGRATED notification
            # This should clear the temporary settings
            key_migrated = "key_receive_migrated_0"
            value_migrated = "migrated_value"
            result_migrated = test_redis_client.set(key_migrated, value_migrated)

            # Validate MIGRATED command result
            assert result_migrated is True, "SET key_receive_migrated command failed"

            # Step 3: Validate that MIGRATED notification was processed but MOVING settings remain
            # (MIGRATED doesn't automatically clear MOVING settings - they are separate notifications)
            # MOVING settings should still be active
            # MOVING timeout should still be active
            Helpers.validate_conn_kwargs(
                pool=test_redis_client.connection_pool,
                expected_maintenance_state=MaintenanceState.MOVING,
                expected_maintenance_notification_hash=hash(MOVING_NOTIFICATION),
                expected_orig_host_address=DEFAULT_ADDRESS.split(":")[0],
                expected_port=int(DEFAULT_ADDRESS.split(":")[1]),
                expected_orig_socket_timeout=DEFAULT_SOCKET_TIMEOUT,
                expected_orig_socket_connect_timeout=DEFAULT_SOCKET_CONNECT_TIMEOUT,
                expected_host_address=AFTER_MOVING_ADDRESS.split(":")[0],
                expected_socket_timeout=self.config.relaxed_timeout,
                expected_socket_connect_timeout=self.config.relaxed_timeout,
            )

            # Step 4: Create new connections after MIGRATED to verify they still use MOVING settings
            # (since MOVING settings are still active)
            new_connections = []
            for _ in range(2):
                connection = test_redis_client.connection_pool.get_connection()
                new_connections.append(connection)

            # Validate that new connections are created with MOVING settings (still active)
            for connection in new_connections:
                assert connection.host == AFTER_MOVING_ADDRESS.split(":")[0]
                # Note: New connections may not inherit the exact relaxed timeout value
                # but they should have the temporary host address
                # New connections should be connected
                if connection._get_socket() is not None:
                    assert connection._get_socket().connected is True

            # Release the new connections
            for connection in new_connections:
                test_redis_client.connection_pool.release(connection)

            # Validate free connections state with MOVING settings still active
            # Note: We'll validate with the pool's current settings rather than individual connection settings
            # since new connections may have different timeout values but still use the temporary address

        finally:
            if hasattr(test_redis_client.connection_pool, "disconnect"):
                test_redis_client.connection_pool.disconnect()

    @pytest.mark.parametrize("pool_class", [ConnectionPool, BlockingConnectionPool])
    def test_overlapping_moving_notifications(self, pool_class):
        """
        Test handling of overlapping/duplicate MOVING notifications (e.g., two MOVING notifications before the first expires).
        Ensures that the second MOVING notification updates the pool and connections as expected, and that expiry/cleanup works.
        """
        global AFTER_MOVING_ADDRESS
        test_redis_client = self._get_client(pool_class, max_connections=5)
        try:
            # Create and release some connections
            in_use_connections = []
            for _ in range(3):
                in_use_connections.append(
                    test_redis_client.connection_pool.get_connection()
                )

            for conn in in_use_connections:
                test_redis_client.connection_pool.release(conn)

            # Take 2 connections to be in use
            in_use_connections = []
            for _ in range(2):
                conn = test_redis_client.connection_pool.get_connection()
                in_use_connections.append(conn)

            # Trigger first MOVING notification
            key_moving1 = "key_receive_moving_0"
            value_moving1 = "value3_0"
            result1 = test_redis_client.set(key_moving1, value_moving1)
            assert result1 is True
            Helpers.validate_conn_kwargs(
                pool=test_redis_client.connection_pool,
                expected_maintenance_state=MaintenanceState.MOVING,
                expected_maintenance_notification_hash=hash(MOVING_NOTIFICATION),
                expected_host_address=AFTER_MOVING_ADDRESS.split(":")[0],
                expected_port=int(DEFAULT_ADDRESS.split(":")[1]),
                expected_socket_timeout=self.config.relaxed_timeout,
                expected_socket_connect_timeout=self.config.relaxed_timeout,
                expected_orig_host_address=DEFAULT_ADDRESS.split(":")[0],
                expected_orig_socket_timeout=DEFAULT_SOCKET_TIMEOUT,
                expected_orig_socket_connect_timeout=DEFAULT_SOCKET_CONNECT_TIMEOUT,
            )
            # Validate all connections reflect the first MOVING notification
            Helpers.validate_in_use_connections_state(
                in_use_connections,
                expected_state=MaintenanceState.MOVING,
                expected_host_address=AFTER_MOVING_ADDRESS.split(":")[0],
                expected_socket_timeout=self.config.relaxed_timeout,
                expected_socket_connect_timeout=self.config.relaxed_timeout,
                expected_orig_host_address=DEFAULT_ADDRESS.split(":")[0],
                expected_orig_socket_timeout=DEFAULT_SOCKET_TIMEOUT,
                expected_orig_socket_connect_timeout=DEFAULT_SOCKET_CONNECT_TIMEOUT,
                expected_current_socket_timeout=self.config.relaxed_timeout,
                expected_current_peername=DEFAULT_ADDRESS.split(":")[0],
            )
            Helpers.validate_free_connections_state(
                pool=test_redis_client.connection_pool,
                should_be_connected_count=1,
                connected_to_tmp_address=True,
                expected_state=MaintenanceState.MOVING,
                expected_host_address=AFTER_MOVING_ADDRESS.split(":")[0],
                expected_socket_timeout=self.config.relaxed_timeout,
                expected_socket_connect_timeout=self.config.relaxed_timeout,
                expected_orig_host_address=DEFAULT_ADDRESS.split(":")[0],
                expected_orig_socket_timeout=DEFAULT_SOCKET_TIMEOUT,
                expected_orig_socket_connect_timeout=DEFAULT_SOCKET_CONNECT_TIMEOUT,
            )
            # Reconnect in use connections
            for conn in in_use_connections:
                conn.disconnect()
                conn.connect()

            # Before the first MOVING expires, trigger a second MOVING notification (simulate new address)
            # Validate the orig properties are not changed!
            second_moving_address = "5.6.7.8:6380"
            orig_after_moving = AFTER_MOVING_ADDRESS
            # Temporarily modify the global constant for this test
            AFTER_MOVING_ADDRESS = second_moving_address
            second_moving_notification = NodeMovingNotification(
                id=1,
                new_node_host=second_moving_address.split(":")[0],
                new_node_port=int(second_moving_address.split(":")[1]),
                ttl=MOVING_TIMEOUT,
            )
            try:
                key_moving2 = "key_receive_moving_1"
                value_moving2 = "value3_1"
                result2 = test_redis_client.set(key_moving2, value_moving2)
                assert result2 is True
                Helpers.validate_conn_kwargs(
                    pool=test_redis_client.connection_pool,
                    expected_maintenance_state=MaintenanceState.MOVING,
                    expected_maintenance_notification_hash=hash(
                        second_moving_notification
                    ),
                    expected_host_address=second_moving_address.split(":")[0],
                    expected_port=int(DEFAULT_ADDRESS.split(":")[1]),
                    expected_socket_timeout=self.config.relaxed_timeout,
                    expected_socket_connect_timeout=self.config.relaxed_timeout,
                    expected_orig_host_address=DEFAULT_ADDRESS.split(":")[0],
                    expected_orig_socket_timeout=DEFAULT_SOCKET_TIMEOUT,
                    expected_orig_socket_connect_timeout=DEFAULT_SOCKET_CONNECT_TIMEOUT,
                )
                # Validate all connections reflect the second MOVING notification
                Helpers.validate_in_use_connections_state(
                    in_use_connections,
                    expected_state=MaintenanceState.MOVING,
                    expected_host_address=second_moving_address.split(":")[0],
                    expected_socket_timeout=self.config.relaxed_timeout,
                    expected_socket_connect_timeout=self.config.relaxed_timeout,
                    expected_orig_host_address=DEFAULT_ADDRESS.split(":")[0],
                    expected_orig_socket_timeout=DEFAULT_SOCKET_TIMEOUT,
                    expected_orig_socket_connect_timeout=DEFAULT_SOCKET_CONNECT_TIMEOUT,
                    expected_current_socket_timeout=self.config.relaxed_timeout,
                    expected_current_peername=orig_after_moving.split(":")[0],
                )
                Helpers.validate_free_connections_state(
                    test_redis_client.connection_pool,
                    should_be_connected_count=1,
                    connected_to_tmp_address=True,
                    tmp_address=second_moving_address.split(":")[0],
                    expected_state=MaintenanceState.MOVING,
                    expected_host_address=second_moving_address.split(":")[0],
                    expected_socket_timeout=self.config.relaxed_timeout,
                    expected_socket_connect_timeout=self.config.relaxed_timeout,
                    expected_orig_host_address=DEFAULT_ADDRESS.split(":")[0],
                    expected_orig_socket_timeout=DEFAULT_SOCKET_TIMEOUT,
                    expected_orig_socket_connect_timeout=DEFAULT_SOCKET_CONNECT_TIMEOUT,
                )
            finally:
                AFTER_MOVING_ADDRESS = orig_after_moving

            # Wait for both MOVING timeouts to expire
            self._advance_timers(MOVING_TIMEOUT + 0.5)
            Helpers.validate_conn_kwargs(
                pool=test_redis_client.connection_pool,
                expected_maintenance_state=MaintenanceState.NONE,
                expected_maintenance_notification_hash=None,
                expected_host_address=DEFAULT_ADDRESS.split(":")[0],
                expected_port=int(DEFAULT_ADDRESS.split(":")[1]),
                expected_socket_timeout=DEFAULT_SOCKET_TIMEOUT,
                expected_socket_connect_timeout=DEFAULT_SOCKET_CONNECT_TIMEOUT,
                expected_orig_host_address=DEFAULT_ADDRESS.split(":")[0],
                expected_orig_socket_timeout=DEFAULT_SOCKET_TIMEOUT,
                expected_orig_socket_connect_timeout=DEFAULT_SOCKET_CONNECT_TIMEOUT,
            )
        finally:
            if hasattr(test_redis_client.connection_pool, "disconnect"):
                test_redis_client.connection_pool.disconnect()

    @pytest.mark.parametrize("pool_class", [ConnectionPool, BlockingConnectionPool])
    def test_thread_safety_concurrent_notification_handling(self, pool_class):
        """
        Test thread-safety under concurrent maintenance notification handling.
        Simulates multiple threads triggering MOVING notifications and performing operations concurrently.
        """
        import threading

        test_redis_client = self._get_client(pool_class, max_connections=5)
        results = []
        errors = []

        def worker(idx):
            try:
                key = f"key_receive_moving_{idx}"
                value = f"value3_{idx}"
                result = test_redis_client.set(key, value)
                results.append(result)
            except Exception as e:
                errors.append(e)

        threads = [threading.Thread(target=worker, args=(i,)) for i in range(5)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()
        assert all(results), f"Not all threads succeeded: {results}"
        assert not errors, f"Errors occurred in threads: {errors}"
        # After all threads, MOVING notification should have been handled safely
        Helpers.validate_conn_kwargs(
            pool=test_redis_client.connection_pool,
            expected_maintenance_state=MaintenanceState.MOVING,
            expected_maintenance_notification_hash=hash(MOVING_NOTIFICATION),
            expected_host_address=AFTER_MOVING_ADDRESS.split(":")[0],
            expected_port=int(DEFAULT_ADDRESS.split(":")[1]),
            expected_socket_timeout=self.config.relaxed_timeout,
            expected_socket_connect_timeout=self.config.relaxed_timeout,
            expected_orig_host_address=DEFAULT_ADDRESS.split(":")[0],
            expected_orig_socket_timeout=DEFAULT_SOCKET_TIMEOUT,
            expected_orig_socket_connect_timeout=DEFAULT_SOCKET_CONNECT_TIMEOUT,
        )

        if hasattr(test_redis_client.connection_pool, "disconnect"):
            test_redis_client.connection_pool.disconnect()

    @pytest.mark.parametrize(
        "pool_class,enable_cache",
        [
            (ConnectionPool, False),
            (ConnectionPool, True),
            (BlockingConnectionPool, False),
            (BlockingConnectionPool, True),
        ],
    )
    def test_moving_migrating_migrated_moved_state_transitions(
        self, pool_class, enable_cache
    ):
        """
        Test moving configs are not lost if the per connection notifications get picked up after moving is handled.
        Sequence of notifications: MOVING, MIGRATING, MIGRATED, FAILING_OVER, FAILED_OVER, MOVED.
        Note: FAILING_OVER and FAILED_OVER notifications do not change the connection state when already in MOVING state.
        Checks the state after each notification for all connections and for new connections created during each state.
        """
        # Setup
        test_redis_client = self._get_client(
            pool_class,
            max_connections=5,
            enable_cache=enable_cache,
        )
        pool = test_redis_client.connection_pool

        # Create and release some connections
        in_use_connections = []
        for _ in range(3):
            in_use_connections.append(pool.get_connection())

        pool_handler = in_use_connections[0]._maint_notifications_pool_handler

        while len(in_use_connections) > 0:
            pool.release(in_use_connections.pop())

        # Take 2 connections to be in use
        in_use_connections = []
        for _ in range(2):
            conn = pool.get_connection()
            in_use_connections.append(conn)

        # 1. MOVING notification
        tmp_address = "22.23.24.25"
        moving_notification = NodeMovingNotification(
            id=1, new_node_host=tmp_address, new_node_port=6379, ttl=1
        )
        pool_handler.handle_notification(moving_notification)

        Helpers.validate_in_use_connections_state(
            in_use_connections,
            expected_state=MaintenanceState.MOVING,
            expected_host_address=tmp_address,
            expected_socket_timeout=self.config.relaxed_timeout,
            expected_socket_connect_timeout=self.config.relaxed_timeout,
            expected_orig_host_address=DEFAULT_ADDRESS.split(":")[0],
            expected_orig_socket_timeout=DEFAULT_SOCKET_TIMEOUT,
            expected_orig_socket_connect_timeout=DEFAULT_SOCKET_CONNECT_TIMEOUT,
            expected_current_socket_timeout=self.config.relaxed_timeout,
            expected_current_peername=DEFAULT_ADDRESS.split(":")[0],
        )
        Helpers.validate_free_connections_state(
            pool=pool,
            should_be_connected_count=0,
            connected_to_tmp_address=False,
            expected_state=MaintenanceState.MOVING,
            expected_host_address=tmp_address,
            expected_socket_timeout=self.config.relaxed_timeout,
            expected_socket_connect_timeout=self.config.relaxed_timeout,
            expected_orig_host_address=DEFAULT_ADDRESS.split(":")[0],
            expected_orig_socket_timeout=DEFAULT_SOCKET_TIMEOUT,
            expected_orig_socket_connect_timeout=DEFAULT_SOCKET_CONNECT_TIMEOUT,
        )

        # 2. MIGRATING notification (simulate direct connection handler call)
        for conn in in_use_connections:
            conn._maint_notifications_connection_handler.handle_notification(
                NodeMigratingNotification(id=2, ttl=1)
            )
        Helpers.validate_in_use_connections_state(
            in_use_connections,
            expected_state=MaintenanceState.MOVING,
            expected_host_address=tmp_address,
            expected_socket_timeout=self.config.relaxed_timeout,
            expected_socket_connect_timeout=self.config.relaxed_timeout,
            expected_orig_host_address=DEFAULT_ADDRESS.split(":")[0],
            expected_orig_socket_timeout=DEFAULT_SOCKET_TIMEOUT,
            expected_orig_socket_connect_timeout=DEFAULT_SOCKET_CONNECT_TIMEOUT,
            expected_current_socket_timeout=self.config.relaxed_timeout,
            expected_current_peername=DEFAULT_ADDRESS.split(":")[0],
        )

        # 3. MIGRATED notification (simulate direct connection handler call)
        for conn in in_use_connections:
            conn._maint_notifications_connection_handler.handle_notification(
                NodeMigratedNotification(id=2)
            )
        # State should not change for connections that are in MOVING state
        Helpers.validate_in_use_connections_state(
            in_use_connections,
            expected_state=MaintenanceState.MOVING,
            expected_host_address=tmp_address,
            expected_socket_timeout=self.config.relaxed_timeout,
            expected_socket_connect_timeout=self.config.relaxed_timeout,
            expected_orig_host_address=DEFAULT_ADDRESS.split(":")[0],
            expected_orig_socket_timeout=DEFAULT_SOCKET_TIMEOUT,
            expected_orig_socket_connect_timeout=DEFAULT_SOCKET_CONNECT_TIMEOUT,
            expected_current_socket_timeout=self.config.relaxed_timeout,
            expected_current_peername=DEFAULT_ADDRESS.split(":")[0],
        )

        # 4. FAILING_OVER notification (simulate direct connection handler call)
        for conn in in_use_connections:
            conn._maint_notifications_connection_handler.handle_notification(
                NodeFailingOverNotification(id=3, ttl=1)
            )
        # State should not change for connections that are in MOVING state
        Helpers.validate_in_use_connections_state(
            in_use_connections,
            expected_state=MaintenanceState.MOVING,
            expected_host_address=tmp_address,
            expected_socket_timeout=self.config.relaxed_timeout,
            expected_socket_connect_timeout=self.config.relaxed_timeout,
            expected_orig_host_address=DEFAULT_ADDRESS.split(":")[0],
            expected_orig_socket_timeout=DEFAULT_SOCKET_TIMEOUT,
            expected_orig_socket_connect_timeout=DEFAULT_SOCKET_CONNECT_TIMEOUT,
            expected_current_socket_timeout=self.config.relaxed_timeout,
            expected_current_peername=DEFAULT_ADDRESS.split(":")[0],
        )

        # 5. FAILED_OVER notification (simulate direct connection handler call)
        for conn in in_use_connections:
            conn._maint_notifications_connection_handler.handle_notification(
                NodeFailedOverNotification(id=3)
            )
        # State should not change for connections that are in MOVING state
        Helpers.validate_in_use_connections_state(
            in_use_connections,
            expected_state=MaintenanceState.MOVING,
            expected_host_address=tmp_address,
            expected_socket_timeout=self.config.relaxed_timeout,
            expected_socket_connect_timeout=self.config.relaxed_timeout,
            expected_orig_host_address=DEFAULT_ADDRESS.split(":")[0],
            expected_orig_socket_timeout=DEFAULT_SOCKET_TIMEOUT,
            expected_orig_socket_connect_timeout=DEFAULT_SOCKET_CONNECT_TIMEOUT,
            expected_current_socket_timeout=self.config.relaxed_timeout,
            expected_current_peername=DEFAULT_ADDRESS.split(":")[0],
        )

        # 6. MOVED notification (simulate timer expiry)
        pool_handler.handle_node_moved_notification(moving_notification)
        Helpers.validate_in_use_connections_state(
            in_use_connections,
            expected_state=MaintenanceState.NONE,
            expected_host_address=DEFAULT_ADDRESS.split(":")[0],
            expected_socket_timeout=DEFAULT_SOCKET_TIMEOUT,
            expected_socket_connect_timeout=DEFAULT_SOCKET_CONNECT_TIMEOUT,
            expected_orig_host_address=DEFAULT_ADDRESS.split(":")[0],
            expected_orig_socket_timeout=DEFAULT_SOCKET_TIMEOUT,
            expected_orig_socket_connect_timeout=DEFAULT_SOCKET_CONNECT_TIMEOUT,
            expected_current_socket_timeout=DEFAULT_SOCKET_TIMEOUT,
            expected_current_peername=DEFAULT_ADDRESS.split(":")[0],
        )
        Helpers.validate_free_connections_state(
            pool=pool,
            should_be_connected_count=0,
            connected_to_tmp_address=False,
            expected_state=MaintenanceState.NONE,
            expected_host_address=DEFAULT_ADDRESS.split(":")[0],
            expected_socket_timeout=DEFAULT_SOCKET_TIMEOUT,
            expected_socket_connect_timeout=DEFAULT_SOCKET_CONNECT_TIMEOUT,
            expected_orig_host_address=DEFAULT_ADDRESS.split(":")[0],
            expected_orig_socket_timeout=DEFAULT_SOCKET_TIMEOUT,
            expected_orig_socket_connect_timeout=DEFAULT_SOCKET_CONNECT_TIMEOUT,
        )
        # New connection after MOVED
        new_conn_none = pool.get_connection()
        assert new_conn_none.maintenance_state == MaintenanceState.NONE
        pool.release(new_conn_none)
        # Cleanup
        for conn in in_use_connections:
            pool.release(conn)
        if hasattr(pool, "disconnect"):
            pool.disconnect()


@pytest.mark.fixed_client
class TestPendingPushNotificationsOnIdleConnection(TestMaintenanceNotificationsBase):
    """
    A connection idle through a maintenance has the notifications, and possibly
    the server's close, waiting in its buffer when the pool hands it out again.
    """

    MOVING_PUSH = (
        f">4\r\n$6\r\nMOVING\r\n:1\r\n:{MOVING_TIMEOUT}\r\n+{AFTER_MOVING_ADDRESS}\r\n"
    ).encode()
    MIGRATING_PUSH = b">3\r\n$9\r\nMIGRATING\r\n:1\r\n:10\r\n"
    INVALIDATE_PUSH = b">2\r\n$10\r\ninvalidate\r\n*1\r\n$3\r\nkey\r\n"
    # An empty read is how the socket reports the server's close
    SERVER_CLOSE = b""
    CACHE_KEY = CacheKey(command="GET", redis_keys=("key",))

    def _idle_connection(self, pool_class, enable_cache=False):
        """A pool with one connection that has been used once and released."""
        # The pure-Python parser: the hiredis one checks socket readiness with
        # poll(), which the mock socket's fake file descriptor cannot answer.
        pool = pool_class(
            host=DEFAULT_ADDRESS.split(":")[0],
            port=int(DEFAULT_ADDRESS.split(":")[1]),
            protocol=3,
            parser_class=_RESP3Parser,
            maint_notifications_config=self.config,
            cache_config=CacheConfig() if enable_cache else None,
        )
        connection = pool.get_connection()
        connection.send_command("SET", "key", "value")
        connection.read_response()
        pool.release(connection)
        return pool, connection

    @pytest.mark.parametrize("pool_class", [ConnectionPool, BlockingConnectionPool])
    def test_pending_pushes_on_alive_connection_are_applied(self, pool_class):
        pool, connection = self._idle_connection(pool_class)
        sock = connection._sock
        sock.pending_responses.append(self.MOVING_PUSH)

        try:
            assert pool.get_connection() is connection

            assert connection._sock is sock
            assert connection.maintenance_state == MaintenanceState.MOVING
            assert connection.host == AFTER_MOVING_ADDRESS.split(":")[0]
            assert (
                pool.connection_kwargs.get("maintenance_state")
                == MaintenanceState.MOVING
            )
        finally:
            pool.release(connection)
            pool.disconnect()

    @pytest.mark.parametrize("pool_class", [ConnectionPool, BlockingConnectionPool])
    def test_pending_pushes_on_closed_connection_are_discarded(self, pool_class):
        """
        The server closes the connections of a moved endpoint once the MOVING
        time-to-live expires, so notifications buffered ahead of the server's
        close describe a maintenance that is over: nothing is applied, and the
        pool reconnects through the configured address before the command is
        sent, instead of letting the command fail on the dead socket.
        """
        pool, connection = self._idle_connection(pool_class)
        sock = connection._sock
        sock.pending_responses.extend(
            [self.MIGRATING_PUSH, self.MOVING_PUSH, self.SERVER_CLOSE]
        )

        try:
            assert pool.get_connection() is connection

            assert sock.closed
            assert connection._sock is not sock
            assert connection._sock.connected
            # Reconnected through the configured address, not the MOVING target
            assert connection._sock.address == (
                DEFAULT_ADDRESS.split(":")[0],
                int(DEFAULT_ADDRESS.split(":")[1]),
            )
            assert connection.maintenance_state == MaintenanceState.NONE
            assert connection.socket_timeout == connection.orig_socket_timeout
            assert (
                pool.connection_kwargs.get("maintenance_state", MaintenanceState.NONE)
                == MaintenanceState.NONE
            )
            assert not pool._maint_notifications_pool_handler._processed_notifications
        finally:
            pool.release(connection)
            pool.disconnect()

    @pytest.mark.parametrize("pool_class", [ConnectionPool, BlockingConnectionPool])
    @pytest.mark.parametrize(
        "stale_reply",
        [b"+OK\r\n", b"_\r\n", b"-MOVED 1337 1.2.3.4:6379\r\n"],
        ids=["reply", "null_reply", "error_reply"],
    )
    def test_stale_reply_on_idle_connection_reconnects(self, pool_class, stale_reply):
        """
        A reply left unread by an earlier command is not a push notification: the
        connection is dirty, and the pool reconnects it rather than letting the
        next command read the stale reply - as it did before the notifications.
        The pushes read ahead of it are discarded with it.
        """
        pool, connection = self._idle_connection(pool_class)
        sock = connection._sock
        sock.pending_responses.extend([self.MOVING_PUSH, stale_reply])

        try:
            assert pool.get_connection() is connection

            assert sock.closed
            assert connection._sock is not sock
            assert connection._sock.connected
            assert connection.maintenance_state == MaintenanceState.NONE
            assert not pool._maint_notifications_pool_handler._processed_notifications
        finally:
            pool.release(connection)
            pool.disconnect()

    @pytest.mark.parametrize("pool_class", [ConnectionPool, BlockingConnectionPool])
    def test_partial_push_frame_on_idle_connection_reconnects(self, pool_class):
        """
        A push frame only partly received must not block the checkout: the
        read is bounded, and running out of it reconnects the connection.
        """
        pool, connection = self._idle_connection(pool_class)
        sock = connection._sock
        # The frame header and first element, with the rest never arriving
        sock.pending_responses.append(b">4\r\n$6\r\nMOVING\r\n")

        try:
            assert pool.get_connection() is connection

            assert sock.closed
            assert connection._sock is not sock
            assert connection._sock.connected
            assert connection.maintenance_state == MaintenanceState.NONE
            assert not pool._maint_notifications_pool_handler._processed_notifications
        finally:
            pool.release(connection)
            pool.disconnect()

    @pytest.mark.parametrize("pool_class", [ConnectionPool, BlockingConnectionPool])
    def test_drain_running_out_of_its_deadline_reconnects(self, pool_class):
        """
        The deadline spans the whole drain, not each frame: pushes still
        pending when it is up are discarded and the connection reconnected.
        """
        pool, connection = self._idle_connection(pool_class)
        sock = connection._sock
        sock.pending_responses.extend([self.MIGRATING_PUSH, self.MOVING_PUSH])

        try:
            with patch("redis.connection.PENDING_PUSH_NOTIFICATIONS_READ_TIMEOUT", 0):
                assert pool.get_connection() is connection

            assert sock.closed
            assert connection._sock is not sock
            assert connection.maintenance_state == MaintenanceState.NONE
            assert not pool._maint_notifications_pool_handler._processed_notifications
        finally:
            pool.release(connection)
            pool.disconnect()

    @pytest.mark.parametrize("pool_class", [ConnectionPool, BlockingConnectionPool])
    def test_handlers_are_restored_after_drain(self, pool_class):
        """A push arriving with the next command is handled as usual."""
        pool, connection = self._idle_connection(pool_class)
        connection._sock.pending_responses.extend(
            [self.MIGRATING_PUSH, self.SERVER_CLOSE]
        )

        try:
            assert pool.get_connection() is connection
            assert connection.maintenance_state == MaintenanceState.NONE

            # The mock socket queues a MIGRATING push ahead of this SET's reply
            connection.send_command("SET", "key_receive_migrating", "value")
            connection.read_response()

            assert connection.maintenance_state == MaintenanceState.MAINTENANCE
        finally:
            pool.release(connection)
            pool.disconnect()

    def _cached_idle_connection(self, pool_class):
        """
        A client-side caching pool with one idle connection and a cached GET
        the server's invalidation of ``key`` applies to.
        """
        pool, connection = self._idle_connection(pool_class, enable_cache=True)
        assert pool.cache.set(
            CacheEntry(
                cache_key=self.CACHE_KEY,
                cache_value=b"value",
                status=CacheEntryStatus.VALID,
                connection_ref=connection,
            )
        )
        return pool, connection

    @pytest.mark.parametrize("pool_class", [ConnectionPool, BlockingConnectionPool])
    def test_pending_pushes_with_cache_on_alive_connection_are_applied(
        self, pool_class
    ):
        """
        With client-side caching the pool used to leave the pending data for
        the next command to read; the notifications are now applied on checkout
        the same way, and the invalidations buffered with them are applied too.
        """
        pool, connection = self._cached_idle_connection(pool_class)
        sock = connection._conn._sock
        sock.pending_responses.extend([self.MOVING_PUSH, self.INVALIDATE_PUSH])

        try:
            assert pool.get_connection() is connection

            assert connection._conn._sock is sock
            assert connection.maintenance_state == MaintenanceState.MOVING
            assert connection.host == AFTER_MOVING_ADDRESS.split(":")[0]
            assert pool.cache.get(self.CACHE_KEY) is None
        finally:
            pool.release(connection)
            pool.disconnect()

    @pytest.mark.parametrize("pool_class", [ConnectionPool, BlockingConnectionPool])
    def test_pending_pushes_with_cache_on_closed_connection_are_discarded(
        self, pool_class
    ):
        """
        The reconnect of a closed connection flushes the cache, which covers
        the invalidations discarded with the stale notifications.
        """
        pool, connection = self._cached_idle_connection(pool_class)
        sock = connection._conn._sock
        sock.pending_responses.extend(
            [self.MOVING_PUSH, self.INVALIDATE_PUSH, self.SERVER_CLOSE]
        )

        try:
            assert pool.get_connection() is connection

            assert sock.closed
            assert connection._conn._sock is not sock
            assert connection._conn._sock.connected
            assert connection.maintenance_state == MaintenanceState.NONE
            assert connection.host == DEFAULT_ADDRESS.split(":")[0]
            assert pool.cache.get(self.CACHE_KEY) is None
        finally:
            pool.release(connection)
            pool.disconnect()

    @pytest.mark.parametrize("pool_class", [ConnectionPool, BlockingConnectionPool])
    def test_closed_cached_connection_in_maintenance_completes_as_the_proxy(
        self, pool_class
    ):
        """
        The maintenance handlers are bound to the cache proxy, so the relaxation
        a closed connection ends is reported with the proxy as its source: by
        the proxy's own disconnect, not by the wrapped connection failing the
        drain and disconnecting itself. The close arrives inside a frame, which
        is where the read - rather than the readiness check - runs into it.
        """
        events = []

        class Listener(EventListenerInterface):
            def listen(self, event):
                events.append(event)

        self.config.event_dispatcher = EventDispatcher(
            {MaintenanceCompletedEvent: [Listener()]}
        )
        pool, connection = self._cached_idle_connection(pool_class)
        connection.maintenance_state = MaintenanceState.MAINTENANCE
        connection._processed_start_maint_notifications.add(1)
        connection._conn._sock.pending_responses.extend(
            [b">4\r\n$6\r\nMOVING\r\n", self.SERVER_CLOSE]
        )

        try:
            assert pool.get_connection() is connection

            assert connection.maintenance_state == MaintenanceState.NONE
            assert not connection._processed_start_maint_notifications
            assert [event.connection for event in events] == [connection]
        finally:
            pool.release(connection)
            pool.disconnect()


@pytest.mark.fixed_client
class TestMaintenanceNotificationsHandlingMultipleProxies(
    TestMaintenanceNotificationsBase
):
    """Integration tests for maintenance notifications handling with real connection pool."""

    def setup_method(self):
        """Set up test fixtures with mocked sockets."""
        super().setup_method()
        self.orig_host = "test.address.com"

        ips = ["1.2.3.4", "5.6.7.8", "9.10.11.12"]
        ips = ips * 3

        # Mock socket creation to return our mock sockets
        def mock_socket_getaddrinfo(host, port, family=0, type=0, proto=0, flags=0):
            if host == self.orig_host:
                ip_address = ips.pop(0)
            else:
                ip_address = host

            # Return the standard getaddrinfo format
            # (family, type, proto, canonname, sockaddr)
            return [
                (
                    socket.AF_INET,
                    socket.SOCK_STREAM,
                    socket.IPPROTO_TCP,
                    "",
                    (ip_address, port),
                )
            ]

        self.getaddrinfo_patcher = patch(
            "socket.getaddrinfo", side_effect=mock_socket_getaddrinfo
        )
        self.getaddrinfo_patcher.start()

    def teardown_method(self):
        """Clean up test fixtures."""
        super().teardown_method()
        self.getaddrinfo_patcher.stop()

    @pytest.mark.parametrize("pool_class", [ConnectionPool, BlockingConnectionPool])
    def test_migrating_after_moving_multiple_proxies(self, pool_class):
        """ """
        # Setup

        pool = pool_class(
            host=self.orig_host,
            port=12345,
            max_connections=10,
            protocol=3,  # Required for maintenance notifications
            maint_notifications_config=self.config,
        )

        pool_handler = pool._maint_notifications_pool_handler

        # Create and release some connections
        key1 = "1.2.3.4"
        key2 = "5.6.7.8"
        key3 = "9.10.11.12"
        in_use_connections = {key1: [], key2: [], key3: []}
        # Create 7 connections
        for _ in range(7):
            conn = pool.get_connection()
            in_use_connections[conn.getpeername()].append(conn)

        for _, conns in in_use_connections.items():
            while len(conns) > 1:
                pool.release(conns.pop())

        # Send MOVING notification to con with ip = key1
        conn = in_use_connections[key1][0]
        pool_handler.set_connection(conn)
        new_ip = "13.14.15.16"
        pool_handler.handle_notification(
            NodeMovingNotification(
                id=1, new_node_host=new_ip, new_node_port=6379, ttl=1
            )
        )

        # validate in use connection and ip1
        Helpers.validate_in_use_connections_state(
            in_use_connections[key1],
            expected_state=MaintenanceState.MOVING,
            expected_host_address=new_ip,
            expected_socket_timeout=self.config.relaxed_timeout,
            expected_socket_connect_timeout=self.config.relaxed_timeout,
            expected_orig_host_address=self.orig_host,
            expected_orig_socket_timeout=DEFAULT_SOCKET_TIMEOUT,
            expected_orig_socket_connect_timeout=DEFAULT_SOCKET_CONNECT_TIMEOUT,
            expected_current_socket_timeout=self.config.relaxed_timeout,
            expected_current_peername=key1,
        )
        # validate free connections for ip1
        changed_free_connections = 0
        free_connections = pool._get_free_connections()

        for conn in free_connections:
            if conn.host == new_ip:
                changed_free_connections += 1
                assert conn.maintenance_state == MaintenanceState.MOVING
                assert conn.host == new_ip
                assert conn.socket_timeout == self.config.relaxed_timeout
                assert conn.socket_connect_timeout == self.config.relaxed_timeout
                assert conn.orig_host_address == self.orig_host
                assert conn.orig_socket_timeout == DEFAULT_SOCKET_TIMEOUT
                assert (
                    conn.orig_socket_connect_timeout == DEFAULT_SOCKET_CONNECT_TIMEOUT
                )
            else:
                assert conn.maintenance_state == MaintenanceState.NONE
                assert conn.host == self.orig_host
                assert conn.socket_timeout == DEFAULT_SOCKET_TIMEOUT
                assert conn.socket_connect_timeout == DEFAULT_SOCKET_CONNECT_TIMEOUT
                assert conn.orig_host_address == self.orig_host
                assert conn.orig_socket_timeout == DEFAULT_SOCKET_TIMEOUT
                assert (
                    conn.orig_socket_connect_timeout == DEFAULT_SOCKET_CONNECT_TIMEOUT
                )
        assert changed_free_connections == 2
        assert len(free_connections) == 4

        # Send second MOVING notification to con with ip = key2
        conn = in_use_connections[key2][0]
        pool_handler.set_connection(conn)
        new_ip_2 = "17.18.19.20"
        pool_handler.handle_notification(
            NodeMovingNotification(
                id=2, new_node_host=new_ip_2, new_node_port=6379, ttl=2
            )
        )

        # validate in use connection and ip2
        Helpers.validate_in_use_connections_state(
            in_use_connections[key2],
            expected_state=MaintenanceState.MOVING,
            expected_host_address=new_ip_2,
            expected_socket_timeout=self.config.relaxed_timeout,
            expected_socket_connect_timeout=self.config.relaxed_timeout,
            expected_orig_host_address=self.orig_host,
            expected_orig_socket_timeout=DEFAULT_SOCKET_TIMEOUT,
            expected_orig_socket_connect_timeout=DEFAULT_SOCKET_CONNECT_TIMEOUT,
            expected_current_socket_timeout=self.config.relaxed_timeout,
            expected_current_peername=key2,
        )
        # validate free connections for ip2
        changed_free_connections = 0
        free_connections = pool._get_free_connections()

        for conn in free_connections:
            if conn.host == new_ip_2:
                changed_free_connections += 1
                assert conn.maintenance_state == MaintenanceState.MOVING
                assert conn.host == new_ip_2
                assert conn.socket_timeout == self.config.relaxed_timeout
                assert conn.socket_connect_timeout == self.config.relaxed_timeout
                assert conn.orig_host_address == self.orig_host
                assert conn.orig_socket_timeout == DEFAULT_SOCKET_TIMEOUT
                assert (
                    conn.orig_socket_connect_timeout == DEFAULT_SOCKET_CONNECT_TIMEOUT
                )
            # here I can't validate the other connections since some of
            # them are in MOVING state from the first notification
            # and some are in NONE state
        assert changed_free_connections == 1

        # MIGRATING notification on connection that has already been marked as MOVING
        conn = in_use_connections[key2][0]
        conn_notification_handler = conn._maint_notifications_connection_handler
        conn_notification_handler.handle_notification(
            NodeMigratingNotification(id=3, ttl=1)
        )
        # validate connection does not lose its MOVING state
        assert conn.maintenance_state == MaintenanceState.MOVING
        # MIGRATED notification
        conn_notification_handler.handle_notification(NodeMigratedNotification(id=3))
        # validate connection does not lose its MOVING state and relaxed timeout
        assert conn.maintenance_state == MaintenanceState.MOVING
        assert conn.socket_timeout == self.config.relaxed_timeout

        # Send Migrating notification to con with ip = key3
        conn = in_use_connections[key3][0]
        conn_notification_handler = conn._maint_notifications_connection_handler
        conn_notification_handler.handle_notification(
            NodeMigratingNotification(id=3, ttl=1)
        )
        # validate connection is in MIGRATING state
        assert conn.maintenance_state == MaintenanceState.MAINTENANCE

        assert conn.socket_timeout == self.config.relaxed_timeout

        # Send MIGRATED notification to con with ip = key3
        conn_notification_handler.handle_notification(NodeMigratedNotification(id=3))
        # validate connection is in MOVING state
        assert conn.maintenance_state == MaintenanceState.NONE
        assert conn.socket_timeout == DEFAULT_SOCKET_TIMEOUT

        # expire only the first MOVING notifications
        self._advance_timers(1.3)
        # validate only the connections affected by the first MOVING notification
        # have lost their MOVING state
        Helpers.validate_in_use_connections_state(
            in_use_connections[key1],
            expected_state=MaintenanceState.NONE,
            expected_host_address=self.orig_host,
            expected_socket_timeout=DEFAULT_SOCKET_TIMEOUT,
            expected_socket_connect_timeout=DEFAULT_SOCKET_CONNECT_TIMEOUT,
            expected_orig_host_address=self.orig_host,
            expected_orig_socket_timeout=DEFAULT_SOCKET_TIMEOUT,
            expected_orig_socket_connect_timeout=DEFAULT_SOCKET_CONNECT_TIMEOUT,
            expected_current_socket_timeout=DEFAULT_SOCKET_TIMEOUT,
            expected_current_peername=key1,
        )
        Helpers.validate_in_use_connections_state(
            in_use_connections[key2],
            expected_state=MaintenanceState.MOVING,
            expected_host_address=new_ip_2,
            expected_socket_timeout=self.config.relaxed_timeout,
            expected_socket_connect_timeout=self.config.relaxed_timeout,
            expected_orig_host_address=self.orig_host,
            expected_orig_socket_timeout=DEFAULT_SOCKET_TIMEOUT,
            expected_orig_socket_connect_timeout=DEFAULT_SOCKET_CONNECT_TIMEOUT,
            expected_current_socket_timeout=self.config.relaxed_timeout,
            expected_current_peername=key2,
        )
        Helpers.validate_in_use_connections_state(
            in_use_connections[key3],
            expected_state=MaintenanceState.NONE,
            expected_should_reconnect=False,
            expected_host_address=self.orig_host,
            expected_socket_timeout=DEFAULT_SOCKET_TIMEOUT,
            expected_socket_connect_timeout=DEFAULT_SOCKET_CONNECT_TIMEOUT,
            expected_orig_host_address=self.orig_host,
            expected_orig_socket_timeout=DEFAULT_SOCKET_TIMEOUT,
            expected_orig_socket_connect_timeout=DEFAULT_SOCKET_CONNECT_TIMEOUT,
            expected_current_socket_timeout=DEFAULT_SOCKET_TIMEOUT,
            expected_current_peername=key3,
        )
        # TODO validate free connections

        # expire the second MOVING notifications
        self._advance_timers(1)
        # validate all connections have lost their MOVING state
        Helpers.validate_in_use_connections_state(
            [
                *in_use_connections[key1],
                *in_use_connections[key2],
                *in_use_connections[key3],
            ],
            expected_state=MaintenanceState.NONE,
            expected_should_reconnect="any",
            expected_host_address=self.orig_host,
            expected_socket_timeout=DEFAULT_SOCKET_TIMEOUT,
            expected_socket_connect_timeout=DEFAULT_SOCKET_CONNECT_TIMEOUT,
            expected_orig_host_address=self.orig_host,
            expected_orig_socket_timeout=DEFAULT_SOCKET_TIMEOUT,
            expected_orig_socket_connect_timeout=DEFAULT_SOCKET_CONNECT_TIMEOUT,
            expected_current_socket_timeout=DEFAULT_SOCKET_TIMEOUT,
            expected_current_peername="any",
        )
        # TODO validate free connections


def _moving_pool(pool_class=ConnectionPool, **config_kwargs):
    config_kwargs.setdefault("enabled", True)
    config_kwargs.setdefault("proactive_reconnect", True)
    config_kwargs.setdefault("relaxed_timeout", 10)
    return pool_class(
        protocol=3, maint_notifications_config=MaintNotificationsConfig(**config_kwargs)
    )


@pytest.mark.fixed_client
@pytest.mark.parametrize("new_node_host", ["1.2.3.4", None])
def test_pool_close_cancels_moving_notification_timers(new_node_host):
    pool = _moving_pool()
    handler = pool._maint_notifications_pool_handler
    handler.handle_node_moved_notification = MagicMock()
    handler.run_proactive_reconnect = MagicMock()
    notification = NodeMovingNotification(
        id=1, new_node_host=new_node_host, new_node_port=6379, ttl=0.2
    )

    handler.handle_notification(notification)
    pool.close()
    # close() runs the cleanup itself; the cancelled timer does not run it again
    handler.handle_node_moved_notification.assert_called_once_with(notification)
    time.sleep(0.4)

    handler.handle_node_moved_notification.assert_called_once_with(notification)
    if new_node_host is None:
        handler.run_proactive_reconnect.assert_not_called()


@pytest.mark.fixed_client
@pytest.mark.parametrize("new_node_host", ["1.2.3.4", None])
def test_pool_reused_after_close_mid_moving_has_reverted_kwargs(new_node_host):
    pool = ConnectionPool(
        host="localhost",
        socket_timeout=3,
        socket_connect_timeout=4,
        protocol=3,
        maint_notifications_config=MaintNotificationsConfig(
            enabled=True, proactive_reconnect=True, relaxed_timeout=10
        ),
    )
    original = dict(pool.connection_kwargs)
    notification = NodeMovingNotification(
        id=1, new_node_host=new_node_host, new_node_port=6379, ttl=60
    )

    pool._maint_notifications_pool_handler.handle_notification(notification)
    assert pool.connection_kwargs["socket_timeout"] == 10
    if new_node_host is not None:
        assert pool.connection_kwargs["host"] == new_node_host
    pool.close()

    for key in ("host", "socket_timeout", "socket_connect_timeout"):
        assert pool.connection_kwargs[key] == original[key]
    assert pool.connection_kwargs["maintenance_state"] == MaintenanceState.NONE
    assert pool.connection_kwargs["maintenance_notification_hash"] is None
    # A connection made after the pool is reused does not inherit the MOVING state
    connection = pool.make_connection()
    assert connection.host == "localhost"
    assert connection.socket_timeout == 3
    assert connection.socket_connect_timeout == 4


@pytest.mark.fixed_client
def test_moving_redelivered_after_close_is_applied_again():
    pool = ConnectionPool(
        host="localhost",
        socket_timeout=3,
        protocol=3,
        maint_notifications_config=MaintNotificationsConfig(
            enabled=True, proactive_reconnect=True, relaxed_timeout=10
        ),
    )
    handler = pool._maint_notifications_pool_handler
    notification = NodeMovingNotification(
        id=1, new_node_host="1.2.3.4", new_node_port=6379, ttl=60
    )

    handler.handle_notification(notification)
    pool.close()
    assert pool.connection_kwargs["host"] == "localhost"

    # The server redelivers the still-active MOVING to the reused pool
    handler.handle_notification(notification)

    assert pool.connection_kwargs["host"] == "1.2.3.4"
    assert pool.connection_kwargs["socket_timeout"] == 10
    assert handler._pending_handoffs == {notification}
    pool.close()


@pytest.mark.fixed_client
def test_pool_disconnect_does_not_cancel_moving_notification_timers():
    pool = _moving_pool()
    handler = pool._maint_notifications_pool_handler
    moved = threading.Event()
    handler.handle_node_moved_notification = MagicMock(
        side_effect=lambda _: moved.set()
    )
    notification = NodeMovingNotification(
        id=1, new_node_host="1.2.3.4", new_node_port=6379, ttl=0.2
    )

    handler.handle_notification(notification)
    pool.disconnect()

    assert moved.wait(timeout=10)
    handler.handle_node_moved_notification.assert_called_once_with(notification)


@pytest.mark.fixed_client
def test_timer_firing_during_close_does_not_run_its_callback():
    pool = _moving_pool()
    handler = pool._maint_notifications_pool_handler
    callback = MagicMock()

    test_thread = threading.current_thread()
    timer_waiting = threading.Event()

    class SignallingLock:
        def __init__(self, lock):
            self._lock = lock

        def __enter__(self):
            if threading.current_thread() is not test_thread:
                timer_waiting.set()
            return self._lock.__enter__()

        def __exit__(self, *exc_info):
            return self._lock.__exit__(*exc_info)

    handler._lock = SignallingLock(handler._lock)

    with handler._lock:
        handler._schedule(0.01, callback)
        (timer,) = handler._scheduled_timers
        # The timer fires and waits for the lock held here.
        assert timer_waiting.wait(timeout=10)
        pool.close()
    timer.join(timeout=10)

    callback.assert_not_called()
    assert handler._scheduled_timers == set()


@pytest.mark.fixed_client
def test_pool_close_completes_superseded_handoff_event():
    events = []

    class Listener(EventListenerInterface):
        def listen(self, event):
            events.append(event)

    pool = _moving_pool(
        event_dispatcher=EventDispatcher(
            {
                MaintenanceStartedEvent: [Listener()],
                MaintenanceCompletedEvent: [Listener()],
            }
        )
    )
    first = NodeMovingNotification(
        id=1, new_node_host="1.2.3.4", new_node_port=6379, ttl=60
    )
    second = NodeMovingNotification(
        id=2, new_node_host="5.6.7.8", new_node_port=6379, ttl=60
    )
    handler = pool._maint_notifications_pool_handler
    handler.handle_notification(first)
    handler.handle_notification(second)
    assert [type(e) for e in events] == [MaintenanceStartedEvent] * 2

    pool.close()

    # closing ends the relaxation of every pending handoff, also of the one
    # a newer MOVING superseded, so each started event gets its pair
    completed = [e for e in events if isinstance(e, MaintenanceCompletedEvent)]
    assert {e.notification for e in completed} == {first, second}
    assert len(completed) == 2


@pytest.mark.fixed_client
def test_pool_close_completes_pending_handoff_event():
    events = []

    class Listener(EventListenerInterface):
        def listen(self, event):
            events.append(event)

    pool = _moving_pool(
        event_dispatcher=EventDispatcher(
            {
                MaintenanceStartedEvent: [Listener()],
                MaintenanceCompletedEvent: [Listener()],
            }
        )
    )
    notification = NodeMovingNotification(
        id=1, new_node_host="1.2.3.4", new_node_port=6379, ttl=1
    )

    pool._maint_notifications_pool_handler.handle_notification(notification)
    assert [type(e) for e in events] == [MaintenanceStartedEvent]

    pool.close()

    assert [type(e) for e in events] == [
        MaintenanceStartedEvent,
        MaintenanceCompletedEvent,
    ]
    assert events[1].notification is notification
    assert events[1].connection_pool is pool


@pytest.mark.fixed_client
def test_pool_close_retried_after_failed_cleanup_runs_it_again():
    """
    A MOVING cleanup that fails halfway, as when a connection is disconnected
    under it, leaves the handoff pending: a retried close() runs it again and
    reports its completion once.
    """
    events = []

    class Listener(EventListenerInterface):
        def listen(self, event):
            events.append(event)

    pool = _moving_pool(
        event_dispatcher=EventDispatcher(
            {
                MaintenanceStartedEvent: [Listener()],
                MaintenanceCompletedEvent: [Listener()],
            }
        )
    )
    handler = pool._maint_notifications_pool_handler
    notification = NodeMovingNotification(
        id=1, new_node_host="1.2.3.4", new_node_port=6379, ttl=60
    )
    handler.handle_notification(notification)
    update_connections_settings = pool.update_connections_settings
    calls = []

    def failing_once(**kwargs):
        calls.append(kwargs)
        if len(calls) == 1:
            raise OSError(9, "Bad file descriptor")
        return update_connections_settings(**kwargs)

    with patch.object(pool, "update_connections_settings", new=failing_once):
        with pytest.raises(OSError):
            pool.close()
        pool.close()

    assert len(calls) == 2
    assert calls[1]["matching_notification_hash"] == hash(notification)
    assert handler._pending_handoffs == set()
    completed = [e for e in events if isinstance(e, MaintenanceCompletedEvent)]
    assert [e.notification for e in completed] == [notification]


@pytest.mark.fixed_client
def test_completed_listener_closing_the_pool_reports_completion_once():
    """
    A completion listener that closes the pool re-enters close() while the
    handoff is being cleaned up; the reentrant close must not run the cleanup
    again and report a second completion.
    """
    events = []
    pool_ref = []

    class ClosingListener(EventListenerInterface):
        def listen(self, event):
            events.append(event)
            if isinstance(event, MaintenanceCompletedEvent):
                pool_ref[0].close()

    pool = _moving_pool(
        event_dispatcher=EventDispatcher(
            {
                MaintenanceStartedEvent: [ClosingListener()],
                MaintenanceCompletedEvent: [ClosingListener()],
            }
        )
    )
    pool_ref.append(pool)
    notification = NodeMovingNotification(
        id=1, new_node_host="1.2.3.4", new_node_port=6379, ttl=60
    )
    pool._maint_notifications_pool_handler.handle_notification(notification)

    pool.close()

    assert [type(e) for e in events] == [
        MaintenanceStartedEvent,
        MaintenanceCompletedEvent,
    ]


@pytest.mark.fixed_client
def test_completed_listener_closing_the_pool_mid_close_reports_each_handoff_once():
    """
    close() walks a snapshot of the pending handoffs. A completion listener that
    closes the pool while the first one is cleaned up re-enters close(), which
    settles the rest; the outer walk must skip them instead of cleaning them up
    and reporting their completion a second time.
    """
    events = []
    pool_ref = []

    class ClosingListener(EventListenerInterface):
        def listen(self, event):
            events.append(event)
            if isinstance(event, MaintenanceCompletedEvent) and (
                sum(isinstance(e, MaintenanceCompletedEvent) for e in events) == 1
            ):
                pool_ref[0].close()

    pool = _moving_pool(
        event_dispatcher=EventDispatcher(
            {
                MaintenanceStartedEvent: [ClosingListener()],
                MaintenanceCompletedEvent: [ClosingListener()],
            }
        )
    )
    pool_ref.append(pool)
    first = NodeMovingNotification(
        id=1, new_node_host="1.2.3.4", new_node_port=6379, ttl=60
    )
    second = NodeMovingNotification(
        id=2, new_node_host="5.6.7.8", new_node_port=6379, ttl=60
    )
    handler = pool._maint_notifications_pool_handler
    handler.handle_notification(first)
    handler.handle_notification(second)

    pool.close()

    completed = [e for e in events if isinstance(e, MaintenanceCompletedEvent)]
    assert sorted(e.notification.id for e in completed) == [1, 2]


@pytest.mark.fixed_client
def test_pool_close_disconnects_even_when_moving_cleanup_fails():
    pool = _moving_pool()
    handler = pool._maint_notifications_pool_handler
    notification = NodeMovingNotification(
        id=1, new_node_host="1.2.3.4", new_node_port=6379, ttl=60
    )
    handler.handle_notification(notification)
    disconnect = MagicMock()

    failing_reset = patch.object(
        pool,
        "update_connections_settings",
        side_effect=OSError(9, "Bad file descriptor"),
    )
    with failing_reset, patch.object(pool, "disconnect", new=disconnect):
        with pytest.raises(OSError):
            pool.close()

    # the pool is closed regardless; the handoff stays for a retried close()
    disconnect.assert_called_once_with()
    assert handler._pending_handoffs == {notification}


@pytest.mark.fixed_client
def test_pool_reset_in_forked_child_gives_the_handler_fresh_state():
    pool = _moving_pool()
    handler = pool._maint_notifications_pool_handler
    notification = NodeMovingNotification(
        id=1, new_node_host="1.2.3.4", new_node_port=6379, ttl=60
    )
    handler.handle_notification(notification)
    inherited_lock = handler._lock
    assert handler._scheduled_timers and handler._pending_handoffs
    assert pool.connection_kwargs["host"] == "1.2.3.4"

    # What _checkpid() sees in a forked child
    pool.pid = -1
    pool._checkpid()

    assert handler._lock is not inherited_lock
    assert handler._scheduled_timers == set()
    assert handler._pending_handoffs == set()
    # The child starts without the temporary host and relaxed timeout
    assert pool.connection_kwargs.get("host") != "1.2.3.4"
    assert pool.connection_kwargs.get("socket_timeout") != 10
    # Connections made in the child share the fresh state
    copy = handler.get_handler_for_connection()
    assert copy._lock is handler._lock
    pool.close()


@pytest.mark.fixed_client
def test_pool_reset_in_forked_child_restores_moving_kwargs_without_pending_handoff():
    """
    The process forks after the MOVING kwargs were applied to the pool but
    before the handoff was registered: there is no handoff whose cleanup would
    restore them, so the reset has to.
    """
    pool = ConnectionPool(
        host="10.0.0.1",
        socket_timeout=5,
        protocol=3,
        maint_notifications_config=MaintNotificationsConfig(
            enabled=True, proactive_reconnect=True, relaxed_timeout=10
        ),
    )
    handler = pool._maint_notifications_pool_handler
    notification = NodeMovingNotification(
        id=1, new_node_host="1.2.3.4", new_node_port=6379, ttl=60
    )
    pool.update_connection_kwargs(
        **_build_moving_connection_kwargs(notification, handler.config)
    )
    assert pool.connection_kwargs["host"] == "1.2.3.4"
    assert not handler._pending_handoffs

    # What _checkpid() sees in a forked child
    pool.pid = -1
    pool._checkpid()

    assert pool.connection_kwargs["host"] == "10.0.0.1"
    assert pool.connection_kwargs["socket_timeout"] == 5
    assert pool.connection_kwargs["maintenance_state"] == MaintenanceState.NONE
    pool.close()


@pytest.mark.fixed_client
@pytest.mark.skipif(not hasattr(os, "fork"), reason="requires os.fork")
def test_pool_close_in_forked_child_does_not_block_on_inherited_handler_lock():
    """
    A parent thread holds the handler lock when the process forks: the child
    inherits it locked and no thread there will ever release it. close() in the
    child must reset the pool before it takes that lock.
    """
    pool = _moving_pool()
    handler = pool._maint_notifications_pool_handler
    holding = threading.Event()
    release = threading.Event()

    def hold_lock():
        with handler._lock:
            holding.set()
            release.wait(30)

    holder = threading.Thread(target=hold_lock, daemon=True)
    holder.start()
    assert holding.wait(5)

    pid = os.fork()
    if pid == 0:  # pragma: no cover - runs in the child
        status = 1
        try:
            signal.alarm(5)
            pool.close()
            status = 0
        finally:
            os._exit(status)
    try:
        _, wait_status = os.waitpid(pid, 0)
    finally:
        release.set()
        holder.join(5)

    assert os.WIFEXITED(wait_status) and os.WEXITSTATUS(wait_status) == 0


@pytest.mark.fixed_client
@pytest.mark.skipif(not hasattr(os, "fork"), reason="requires os.fork")
@pytest.mark.parametrize("pool_class", [ConnectionPool, BlockingConnectionPool])
def test_pool_close_in_forked_child_does_not_block_on_inherited_pool_lock(
    pool_class,
):
    """
    A parent thread holds the pool lock while a handoff is pending when the
    process forks: the child inherits the lock held and no thread there will
    ever release it, yet resetting the handler takes it to revert the handoff.
    """
    pool = _moving_pool(pool_class)
    handler = pool._maint_notifications_pool_handler
    handler.handle_notification(
        NodeMovingNotification(
            id=1, new_node_host="1.2.3.4", new_node_port=6379, ttl=60
        )
    )
    assert handler._pending_handoffs
    holding = threading.Event()
    release = threading.Event()

    def hold_lock():
        with pool._lock:
            holding.set()
            release.wait(30)

    holder = threading.Thread(target=hold_lock, daemon=True)
    holder.start()
    assert holding.wait(5)

    pid = os.fork()
    if pid == 0:  # pragma: no cover - runs in the child
        status = 1
        try:
            signal.alarm(5)
            pool.close()
            status = 0
        finally:
            os._exit(status)
    try:
        _, wait_status = os.waitpid(pid, 0)
    finally:
        release.set()
        holder.join(5)
        # The handoff started a non-daemon timer in the parent.
        pool.close()

    assert os.WIFEXITED(wait_status) and os.WEXITSTATUS(wait_status) == 0


@pytest.mark.fixed_client
@pytest.mark.skipif(not hasattr(os, "fork"), reason="requires os.fork")
def test_blocking_pool_fork_in_maintenance_does_not_stay_in_maintenance():
    """
    The process forks while a parent thread is applying a MOVING, with the
    pool flagged as in maintenance: no thread in the child will ever clear the
    flag, and a later checkout would nest on the pool lock and leak it.
    """
    pool = _moving_pool(BlockingConnectionPool)
    pool.set_in_maintenance(True)

    pid = os.fork()
    if pid == 0:  # pragma: no cover - runs in the child
        status = 1
        try:
            signal.alarm(5)
            pool._checkpid()
            free = pool._lock.acquire(blocking=False)
            if free:
                pool._lock.release()
            if free and not pool._in_maintenance and not pool._locked:
                status = 0
        finally:
            os._exit(status)
    _, wait_status = os.waitpid(pid, 0)
    pool.set_in_maintenance(False)
    pool.disconnect()

    assert os.WIFEXITED(wait_status) and os.WEXITSTATUS(wait_status) == 0
