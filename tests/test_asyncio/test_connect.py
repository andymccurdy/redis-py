import asyncio
import re
import socket
import ssl

import pytest
from redis.asyncio.connection import (
    Connection,
    SSLConnection,
    UnixDomainSocketConnection,
)
from redis.exceptions import ConnectionError

from ..ssl_utils import CertificateType, get_tls_certificates

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
async def test_tcp_connect(tcp_address):
    host, port = tcp_address
    conn = Connection(host=host, port=port, client_name=_CLIENT_NAME, socket_timeout=10)
    await _assert_connect(conn, tcp_address)


@pytest.mark.fixed_client
async def test_uds_connect(uds_address):
    path = str(uds_address)
    conn = UnixDomainSocketConnection(
        path=path, client_name=_CLIENT_NAME, socket_timeout=10
    )
    await _assert_connect(conn, path)


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
async def test_tcp_ssl_tls12_custom_ciphers(tcp_address, ssl_ciphers, connects):
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
    connect = _assert_connect(
        conn,
        tcp_address,
        certfile=server_certs.certfile,
        keyfile=server_certs.keyfile,
        maximum_ssl_version=ssl.TLSVersion.TLSv1_2,
    )
    if connects:
        await connect
    else:
        with pytest.raises(ConnectionError):
            await connect
    await conn.disconnect()


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
async def test_tcp_ssl_connect(tcp_address, ssl_min_version):
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
        ssl_min_version=ssl_min_version,
    )
    await _assert_connect(
        conn,
        tcp_address,
        certfile=server_certs.certfile,
        keyfile=server_certs.keyfile,
    )
    await conn.disconnect()


@pytest.mark.fixed_client
@pytest.mark.ssl
@pytest.mark.skipif(not ssl.HAS_TLSv1_3, reason="requires TLSv1.3")
async def test_tcp_ssl_version_mismatch(tcp_address):
    host, port = tcp_address
    certfile, keyfile, _ = get_tls_certificates(cert_type=CertificateType.server)
    conn = SSLConnection(
        host=host,
        port=port,
        client_name=_CLIENT_NAME,
        ssl_ca_certs=certfile,
        socket_timeout=1,
        ssl_min_version=ssl.TLSVersion.TLSv1_3,
    )
    with pytest.raises(ConnectionError):
        await _assert_connect(
            conn,
            tcp_address,
            certfile=certfile,
            keyfile=keyfile,
            maximum_ssl_version=ssl.TLSVersion.TLSv1_2,
        )
    await conn.disconnect()


async def _assert_connect(
    conn,
    server_address,
    certfile=None,
    keyfile=None,
    minimum_ssl_version=ssl.TLSVersion.TLSv1_2,
    maximum_ssl_version=ssl.TLSVersion.TLSv1_3,
):
    stop_event = asyncio.Event()
    finished = asyncio.Event()

    async def _handler(reader, writer):
        try:
            return await _redis_request_handler(reader, writer, stop_event)
        finally:
            writer.close()
            await writer.wait_closed()
            finished.set()

    if isinstance(server_address, str):
        server = await asyncio.start_unix_server(_handler, path=server_address)
    elif certfile:
        host, port = server_address
        context = ssl.create_default_context(ssl.Purpose.CLIENT_AUTH)
        context.minimum_version = minimum_ssl_version
        context.maximum_version = maximum_ssl_version
        context.load_cert_chain(certfile=certfile, keyfile=keyfile)
        server = await asyncio.start_server(_handler, host=host, port=port, ssl=context)
    else:
        host, port = server_address
        server = await asyncio.start_server(_handler, host=host, port=port)

    async with server as aserver:
        await aserver.start_serving()
        try:
            await conn.connect()
            await conn.disconnect()
        except ConnectionError:
            finished.set()
            raise
        finally:
            stop_event.set()
            aserver.close()
            await aserver.wait_closed()
            await finished.wait()


async def _redis_request_handler(reader, writer, stop_event):
    command = None
    command_ptr = None
    fragment_length = None
    while not stop_event.is_set():
        buffer = await reader.read(1024)
        if not buffer:
            break
        parts = re.split(_CMD_SEP, buffer)
        for fragment in parts:
            fragment = fragment.decode()
            if not fragment:
                continue

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
            writer.write(resp)
            await writer.drain()
            command = None
