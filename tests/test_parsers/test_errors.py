import socket
from unittest.mock import patch

import pytest
from redis.backoff import NoBackoff
from redis.client import Redis
from redis.exceptions import ExternalAuthProviderError
from redis.retry import Retry


class MockSocket:
    """Mock socket that simulates Redis protocol responses."""

    def __init__(self):
        self.sent_data = []
        self.closed = False
        self.pending_responses = []

    def connect(self, address):
        pass

    def send(self, data):
        """Simulate sending data to Redis."""
        if self.closed:
            raise ConnectionError("Socket is closed")
        self.sent_data.append(data)

        # Analyze the command and prepare appropriate response
        if b"HELLO" in data:
            response = b"%7\r\n$6\r\nserver\r\n$5\r\nredis\r\n$7\r\nversion\r\n$5\r\n7.4.0\r\n$5\r\nproto\r\n:3\r\n$2\r\nid\r\n:1\r\n$4\r\nmode\r\n$10\r\nstandalone\r\n$4\r\nrole\r\n$6\r\nmaster\r\n$7\r\nmodules\r\n*0\r\n"
            self.pending_responses.append(response)
        elif b"SET" in data:
            response = b"+OK\r\n"
            self.pending_responses.append(response)
        elif b"GET" in data:
            # Extract key and provide appropriate response
            if b"hello" in data:
                response = b"$5\r\nworld\r\n"
                self.pending_responses.append(response)
            # Handle specific keys used in tests
            elif b"ldap_error" in data:
                self.pending_responses.append(b"-ERR problem with LDAP service\r\n")
            else:
                self.pending_responses.append(b"$-1\r\n")  # NULL response
        else:
            self.pending_responses.append(b"+OK\r\n")  # Default response

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
        self.address = None
        self.timeout = None

    def settimeout(self, timeout):
        pass

    def setsockopt(self, level, optname, value):
        pass

    def setblocking(self, blocking):
        pass

    def shutdown(self, how):
        pass


@pytest.mark.fixed_client
class TestErrorParsing:
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

    def teardown_method(self):
        """Clean up test fixtures."""
        self.socket_patcher.stop()
        self.select_patcher.stop()

    @pytest.mark.parametrize("protocol_version", [2, 3])
    def test_external_auth_provider_error(self, protocol_version):
        # ExternalAuthProviderError subclasses ConnectionError, so skip the retries
        client = Redis(
            protocol=protocol_version,
            retry=Retry(NoBackoff(), 0),
        )
        client.set("hello", "world")

        with pytest.raises(ExternalAuthProviderError):
            client.get("ldap_error")
