import pytest

from redis.asyncio.http.http_client import AsyncHTTPClientWrapper
from redis.http.http_client import HttpClient


class TestAsyncHTTPClientWrapper:
    def test_close_shuts_down_executor(self):
        wrapper = AsyncHTTPClientWrapper(HttpClient(timeout=1.0))
        assert wrapper._executor._shutdown is False
        wrapper.close()
        assert wrapper._executor._shutdown is True

    @pytest.mark.asyncio
    async def test_aclose_shuts_down_executor(self):
        wrapper = AsyncHTTPClientWrapper(HttpClient(timeout=1.0))
        assert wrapper._executor._shutdown is False
        await wrapper.aclose()
        assert wrapper._executor._shutdown is True
