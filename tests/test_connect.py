import re
import socket
import socketserver
import ssl
import threading

import pytest
from redis.connection import Connection, SSLConnection, UnixDomainSocketConnection
from redis.exceptions import ConnectionError, RedisError

from .ssl_utils import CertificateType, get_tls_certificates

_CLIENT_NAME = "test-suite-client"
_CMD_SEP = b"\r\n"
_SUCCESS_RESP = b"+OK" + _CMD_SEP
_ERROR_RESP = b"-ERR" + _CMD_SEP
# Minimal RESP3 map response for ``HELLO 3`` containing only ``proto: 3`` —
# enough to satisfy ``Connection.on_connect`` when the default protocol
# resolves to RESP3.
_HELLO_RESP3 = b"%1\r\n$5\r\nproto\r\n:3\r\n"
_SUPPORTED_CMDS = {
    f"CLIENT SETNAME {_CLIENT_NAME}": _SUCCESS_RESP,
    "HELLO 3": _HELLO_RESP3,
}


@pytest.fixture
def tcp_address():
    with socket.socket() as sock:
        sock.bind(("127.0.0.1", 0))
        return sock.getsockname()


@pytest.fixture
def uds_address(tmpdir):
    return tmpdir / "uds.sock"


@pytest.mark.fixed_client
def test_tcp_connect(tcp_address):
    host, port = tcp_address
    conn = Connection(host=host, port=port, client_name=_CLIENT_NAME, socket_timeout=10)
    _assert_connect(conn, tcp_address)


@pytest.mark.fixed_client
def test_uds_connect(uds_address):
    path = str(uds_address)
    conn = UnixDomainSocketConnection(path, client_name=_CLIENT_NAME, socket_timeout=10)
    _assert_connect(conn, path)


@pytest.mark.fixed_client
@pytest.mark.ssl
@pytest.mark.parametrize(
    "ssl_min_version",
    [
        ssl.TLSVersion.TLSv1_2,
        pytest.param(
            ssl.TLSVersion.TLSv1_3,
            marks=pytest.mark.skipif(not ssl.HAS_TLSv1_3, reason="requires TLSv1.3"),
        ),
    ],
)
def test_tcp_ssl_connect(tcp_address, ssl_min_version):
    host, port = tcp_address

    # in order to have working hostname verification, we need to use "localhost"
    # as redis host as the server certificate is self-signed and only valid for "localhost"
    host = "localhost"
    server_certs = get_tls_certificates(cert_type=CertificateType.server)

    conn = SSLConnection(
        host=host,
        port=port,
        ssl_check_hostname=True,
        client_name=_CLIENT_NAME,
        ssl_ca_certs=server_certs.ca_certfile,
        socket_timeout=10,
        ssl_min_version=ssl_min_version,
    )
    _assert_connect(
        conn, tcp_address, certfile=server_certs.certfile, keyfile=server_certs.keyfile
    )


@pytest.mark.fixed_client
@pytest.mark.ssl
@pytest.mark.parametrize(
    "ssl_ciphers,connects",
    [
        ("ECDHE-RSA-AES128-GCM-SHA256", True),
        ("ECDHE-RSA-AES256-GCM-SHA384", True),
        # the server certificate is RSA, so an ECDSA-only list cannot connect
        ("ECDHE-ECDSA-AES256-GCM-SHA384", False),
    ],
)
def test_tcp_ssl_tls12_custom_ciphers(tcp_address, ssl_ciphers, connects):
    host, port = tcp_address

    # in order to have working hostname verification, we need to use "localhost"
    # as redis host as the server certificate is self-signed and only valid for "localhost"
    host = "localhost"

    server_certs = get_tls_certificates(cert_type=CertificateType.server)

    conn = SSLConnection(
        host=host,
        port=port,
        client_name=_CLIENT_NAME,
        ssl_ca_certs=server_certs.ca_certfile,
        socket_timeout=10,
        ssl_min_version=ssl.TLSVersion.TLSv1_2,
        ssl_ciphers=ssl_ciphers,
    )

    # pin the server to TLS 1.2: under TLS 1.3 the cipher list is not used at
    # all, so only then does the list decide whether the handshake succeeds
    def assert_connect():
        _assert_connect(
            conn,
            tcp_address,
            certfile=server_certs.certfile,
            keyfile=server_certs.keyfile,
            maximum_ssl_version=ssl.TLSVersion.TLSv1_2,
        )

    if connects:
        assert_connect()
    else:
        with pytest.raises(ConnectionError):
            assert_connect()


"""
Addresses bug CAE-333 which uncovered that the init method of the base
class did override the initialization of the socket_timeout parameter.
"""


@pytest.mark.fixed_client
def test_unix_socket_with_timeout():
    conn = UnixDomainSocketConnection(socket_timeout=1000)

    # Check if the base class defaults were taken over.
    assert conn.db == 0

    # Verify if the timeout and the path is set correctly.
    assert conn.socket_timeout == 1000
    assert conn.path == ""


@pytest.mark.fixed_client
@pytest.mark.ssl
@pytest.mark.skipif(not ssl.HAS_TLSv1_3, reason="requires TLSv1.3")
def test_tcp_ssl_version_mismatch(tcp_address):
    host, port = tcp_address
    certfile, keyfile, _ = get_tls_certificates(cert_type=CertificateType.server)
    conn = SSLConnection(
        host=host,
        port=port,
        client_name=_CLIENT_NAME,
        ssl_ca_certs=certfile,
        socket_timeout=3,
        ssl_min_version=ssl.TLSVersion.TLSv1_3,
    )
    with pytest.raises(RedisError):
        _assert_connect(
            conn,
            tcp_address,
            certfile=certfile,
            keyfile=keyfile,
            maximum_ssl_version=ssl.TLSVersion.TLSv1_2,
        )


def _assert_connect(conn, server_address, **tcp_kw):
    if isinstance(server_address, str):
        if not _RedisUDSServer:
            pytest.skip("Unix domain sockets are not supported on this platform")
        server = _RedisUDSServer(server_address, _RedisRequestHandler)
    else:
        server = _RedisTCPServer(server_address, _RedisRequestHandler, **tcp_kw)
    with server as aserver:
        # the default 0.5 s poll interval would delay every shutdown
        t = threading.Thread(
            target=aserver.serve_forever, kwargs={"poll_interval": 0.05}
        )
        t.start()
        try:
            aserver.wait_online()
            conn.connect()
            conn.disconnect()
        finally:
            aserver.stop()
            t.join(timeout=5)


class _RedisTCPServer(socketserver.TCPServer):
    def __init__(
        self,
        *args,
        certfile=None,
        keyfile=None,
        minimum_ssl_version=ssl.TLSVersion.TLSv1_2,
        maximum_ssl_version=ssl.TLSVersion.TLSv1_3,
        **kw,
    ) -> None:
        self._ready_event = threading.Event()
        self._stop_requested = False
        self._certfile = certfile
        self._keyfile = keyfile
        self._minimum_ssl_version = minimum_ssl_version
        self._maximum_ssl_version = maximum_ssl_version
        super().__init__(*args, **kw)

    def service_actions(self):
        self._ready_event.set()

    def wait_online(self):
        self._ready_event.wait()

    def stop(self):
        self._stop_requested = True
        self.shutdown()

    def is_serving(self):
        return not self._stop_requested

    def get_request(self):
        if self._certfile is None:
            return super().get_request()
        newsocket, fromaddr = self.socket.accept()
        context = ssl.create_default_context(ssl.Purpose.CLIENT_AUTH)
        context.load_cert_chain(certfile=self._certfile, keyfile=self._keyfile)
        context.minimum_version = self._minimum_ssl_version
        context.maximum_version = self._maximum_ssl_version
        connstream = context.wrap_socket(newsocket, server_side=True)
        return connstream, fromaddr


if hasattr(socketserver, "UnixStreamServer"):

    class _RedisUDSServer(socketserver.UnixStreamServer):
        def __init__(self, *args, **kw) -> None:
            self._ready_event = threading.Event()
            self._stop_requested = False
            super().__init__(*args, **kw)

        def service_actions(self):
            self._ready_event.set()

        def wait_online(self):
            self._ready_event.wait()

        def stop(self):
            self._stop_requested = True
            self.shutdown()

        def is_serving(self):
            return not self._stop_requested

else:
    _RedisUDSServer = None


class _RedisRequestHandler(socketserver.StreamRequestHandler):
    def setup(self):
        pass

    def finish(self):
        pass

    def handle(self):
        buffer = b""
        command = None
        command_ptr = None
        fragment_length = None
        while self.server.is_serving() or buffer:
            try:
                buffer += self.request.recv(1024)
            except socket.timeout:
                continue
            if not buffer:
                continue
            parts = re.split(_CMD_SEP, buffer)
            buffer = parts[-1]
            for fragment in parts[:-1]:
                fragment = fragment.decode()

                if fragment.startswith("*") and command is None:
                    command = [None for _ in range(int(fragment[1:]))]
                    command_ptr = 0
                    fragment_length = None
                    continue

                if fragment.startswith("$") and command[command_ptr] is None:
                    fragment_length = int(fragment[1:])
                    continue

                assert len(fragment) == fragment_length
                command[command_ptr] = fragment
                command_ptr += 1

                if command_ptr < len(command):
                    continue

                command = " ".join(command)
                resp = _SUPPORTED_CMDS.get(command, _ERROR_RESP)
                self.request.sendall(resp)
                command = None
