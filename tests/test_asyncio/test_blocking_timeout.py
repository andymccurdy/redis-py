"""Unit tests for async blocking-command timeout mapping.

These do not require a live Redis server.
"""

import math
from unittest.mock import AsyncMock, MagicMock

import pytest

import redis.asyncio as redis


@pytest.mark.asyncio
async def test_async_parse_response_maps_zero_blocking_timeout_to_math_inf():
    """Redis timeout=0 must block indefinitely on async reads.

    Async Connection.read_response treats:
      - None as \"use socket_timeout\"
      - math.inf as \"no timeout\"
    so parse_response must map _blocking_timeout=0 to math.inf, not None.
    """
    conn = MagicMock()
    conn.read_response = AsyncMock(return_value=None)

    client = redis.Redis(connection_pool=MagicMock())
    result = await client.parse_response(conn, "BLPOP", _blocking_timeout=0)

    assert result is None
    conn.read_response.assert_awaited_once_with(timeout=math.inf)


@pytest.mark.asyncio
async def test_async_parse_response_forwards_positive_blocking_timeout():
    conn = MagicMock()
    conn.read_response = AsyncMock(return_value=None)

    client = redis.Redis(connection_pool=MagicMock())
    await client.parse_response(conn, "BLPOP", _blocking_timeout=5)

    conn.read_response.assert_awaited_once_with(timeout=5)


@pytest.mark.asyncio
async def test_async_parse_response_without_blocking_timeout_uses_default():
    conn = MagicMock()
    conn.read_response = AsyncMock(return_value=b"OK")

    client = redis.Redis(connection_pool=MagicMock())
    # Use a command without a response callback so the raw value is returned.
    result = await client.parse_response(conn, "CUSTOMCMD")

    assert result == b"OK"
    # No override: default None -> read_response falls back to socket_timeout.
    conn.read_response.assert_awaited_once_with(timeout=None)
