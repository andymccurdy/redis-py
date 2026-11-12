import errno
import io
import socket
from io import SEEK_END
from typing import Optional, Union

from ..exceptions import ConnectionError, TimeoutError
from ..utils import SENTINEL, SSL_AVAILABLE, is_os_level_timeout

NONBLOCKING_EXCEPTION_ERROR_NUMBERS = {BlockingIOError: errno.EWOULDBLOCK}

if SSL_AVAILABLE:
    import ssl

    if hasattr(ssl, "SSLWantReadError"):
        NONBLOCKING_EXCEPTION_ERROR_NUMBERS[ssl.SSLWantReadError] = 2
        NONBLOCKING_EXCEPTION_ERROR_NUMBERS[ssl.SSLWantWriteError] = 2
    else:
        NONBLOCKING_EXCEPTION_ERROR_NUMBERS[ssl.SSLError] = 2

NONBLOCKING_EXCEPTIONS = tuple(NONBLOCKING_EXCEPTION_ERROR_NUMBERS.keys())

SERVER_CLOSED_CONNECTION_ERROR = "Connection closed by server."

SYM_CRLF = b"\r\n"


class SocketBuffer:
    def __init__(
        self, socket: socket.socket, socket_read_size: int, socket_timeout: float
    ):
        self._sock = socket
        self.socket_read_size = socket_read_size
        self.socket_timeout = socket_timeout
        self._buffer = io.BytesIO()

    def _live_buffer(self) -> io.BytesIO:
        """
        The read buffer, or a ConnectionError when the connection is already gone.

        ``close()`` closes the buffer before dropping it, and it can run while another
        thread reads: the multi-database client closes connections from its health
        check thread when a database is taken out of service, so a reader finds either
        a closed ``BytesIO`` or ``None``. The connection is gone either way, so report
        it the way every other teardown here is reported and let the retry layers act
        on it, instead of surfacing ``ValueError: I/O operation on closed file`` to the
        caller of the command.
        """
        buffer = self._buffer

        if buffer is None or buffer.closed:
            raise ConnectionError(SERVER_CLOSED_CONNECTION_ERROR)

        return buffer

    def unread_bytes(self) -> int:
        """
        Remaining unread length of buffer
        """
        buffer = self._live_buffer()

        try:
            pos = buffer.tell()
            end = buffer.seek(0, SEEK_END)
            buffer.seek(pos)
        except ValueError:
            # Closed between the check above and here.
            raise ConnectionError(SERVER_CLOSED_CONNECTION_ERROR) from None

        return end - pos

    def _read_from_socket(
        self,
        length: Optional[int] = None,
        timeout: Union[float, object] = SENTINEL,
        raise_on_timeout: Optional[bool] = True,
    ) -> bool:
        sock = self._sock
        socket_read_size = self.socket_read_size
        marker = 0
        custom_timeout = timeout is not SENTINEL

        buf = self._live_buffer()

        if sock is None:
            raise ConnectionError(SERVER_CLOSED_CONNECTION_ERROR)

        try:
            current_pos = buf.tell()
            buf.seek(0, SEEK_END)
        except ValueError:
            raise ConnectionError(SERVER_CLOSED_CONNECTION_ERROR) from None

        if custom_timeout:
            sock.settimeout(timeout)
        try:
            while True:
                data = sock.recv(socket_read_size)
                # an empty string indicates the server shutdown the socket
                if isinstance(data, bytes) and len(data) == 0:
                    raise ConnectionError(SERVER_CLOSED_CONNECTION_ERROR)
                buf.write(data)
                data_length = len(data)
                marker += data_length

                if length is not None and length > marker:
                    continue
                return True
        except socket.timeout as e:
            if is_os_level_timeout(e):
                # ETIMEDOUT from the OS (e.g. after a network change): the
                # connection is dead, not slow. Reporting it as a timeout - or
                # as "no data" when polling - would hide a broken connection.
                raise ConnectionError(f"Error while reading from socket: {e.args}")
            if raise_on_timeout:
                raise TimeoutError("Timeout reading from socket")
            return False
        except NONBLOCKING_EXCEPTIONS as ex:
            # if we're in nonblocking mode and the recv raises a
            # blocking error, simply return False indicating that
            # there's no data to be read. otherwise raise the
            # original exception.
            allowed = NONBLOCKING_EXCEPTION_ERROR_NUMBERS.get(ex.__class__, -1)
            if ex.errno == allowed:
                if not raise_on_timeout:
                    return False
                if timeout == 0:
                    raise TimeoutError("Timeout reading from socket")
            raise ConnectionError(f"Error while reading from socket: {ex.args}")
        except ValueError:
            # The buffer was closed by another thread while this read was in flight.
            raise ConnectionError(SERVER_CLOSED_CONNECTION_ERROR) from None
        finally:
            try:
                buf.seek(current_pos)
            except ValueError:
                # Closed by another thread while recv was blocked. Whatever the body
                # of the read raised is the outcome to report, so this stays quiet
                # rather than replacing it - the next read raises ConnectionError.
                pass
            if custom_timeout:
                try:
                    sock.settimeout(self.socket_timeout)
                except OSError:
                    # Same window as the seek above: the close that dropped the
                    # buffer closed the socket too, so there is nothing left to
                    # restore the timeout on. Staying quiet keeps the outcome the
                    # body reported instead of replacing it with EBADF.
                    pass

    def can_read(self, timeout: float = 0) -> bool:
        return bool(self.unread_bytes()) or self._read_from_socket(
            timeout=timeout, raise_on_timeout=False
        )

    def read(self, length: int, timeout: Union[float, object] = SENTINEL) -> bytes:
        length = length + 2  # make sure to read the \r\n terminator
        buf = self._live_buffer()
        try:
            # BufferIO will return less than requested if buffer is short
            data = buf.read(length)
            missing = length - len(data)
            if missing:
                # fill up the buffer and read the remainder
                self._read_from_socket(length=missing, timeout=timeout)
                data += buf.read(missing)
        except ValueError:
            raise ConnectionError(SERVER_CLOSED_CONNECTION_ERROR) from None
        return data[:-2]

    def readline(self, timeout: Union[float, object] = SENTINEL) -> bytes:
        buf = self._live_buffer()
        try:
            data = buf.readline()
            while not data.endswith(SYM_CRLF):
                # there's more data in the socket that we need
                self._read_from_socket(timeout=timeout)
                data += buf.readline()
        except ValueError:
            raise ConnectionError(SERVER_CLOSED_CONNECTION_ERROR) from None

        return data[:-2]

    def get_pos(self) -> int:
        """
        Get current read position
        """
        try:
            return self._live_buffer().tell()
        except ValueError:
            raise ConnectionError(SERVER_CLOSED_CONNECTION_ERROR) from None

    def rewind(self, pos: int) -> None:
        """
        Rewind the buffer to a specific position, to re-start reading
        """
        buffer = self._buffer

        # Best effort: the caller is unwinding a read that already failed, and a
        # buffer closed by another thread has nothing to rewind. Raising here would
        # replace the exception the caller is propagating.
        if buffer is None:
            return

        try:
            buffer.seek(pos)
        except ValueError:
            pass

    def purge(self) -> None:
        """
        After a successful read, purge the read part of buffer
        """
        try:
            self._purge()
        except (ConnectionError, ValueError):
            # Closed by another thread while the response was being read. The
            # response is already parsed, so there is nothing to report and nothing
            # left to purge.
            return

    def _purge(self) -> None:
        unread = self.unread_bytes()

        # Only if we have read all of the buffer do we truncate, to
        # reduce the amount of memory thrashing.  This heuristic
        # can be changed or removed later.
        if unread > 0:
            return

        # Bind the buffer once: another thread's ``close()`` can drop
        # ``self._buffer`` to None between the read above and the truncate below,
        # and the resulting AttributeError would escape ``purge()``'s best effort
        # wrapper. A local reference to an already closed buffer raises
        # ValueError, which ``purge()`` handles.
        buffer = self._live_buffer()

        if unread > 0:
            # move unread data to the front
            view = buffer.getbuffer()
            view[:unread] = view[-unread:]
        buffer.truncate(unread)
        buffer.seek(0)

    def close(self) -> None:
        try:
            self._buffer.close()
        except Exception:
            # issue #633 suggests the purge/close somehow raised a
            # BadFileDescriptor error. Perhaps the client ran out of
            # memory or something else? It's probably OK to ignore
            # any error being raised from purge/close since we're
            # removing the reference to the instance below.
            pass
        self._buffer = None
        self._sock = None
