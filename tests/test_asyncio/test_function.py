import pytest
import pytest_asyncio
from tests.conftest import skip_if_server_version_lt


@pytest.mark.onlynoncluster
@skip_if_server_version_lt("7.0.0")
class TestFunction:
    @pytest_asyncio.fixture(autouse=True)
    async def reset_functions(self, r):
        await r.function_flush()
        yield
        await r.function_flush()

    @pytest.mark.parametrize("withcode", [False, True])
    async def test_function_list_without_library_filter(self, r, withcode):
        await r.function_load(
            "#!lua name=mylib\n"
            "redis.register_function('myfunc', function() return 1 end)"
        )
        await r.function_load(
            "#!lua name=mylib2\n"
            "redis.register_function('hello', function() return 2 end)"
        )
        libraries = await r.function_list(withcode=withcode)
        assert len(libraries) == 2
        assert await r.function_list(library=None, withcode=withcode) == libraries
        assert len(await r.function_list(library="mylib", withcode=withcode)) == 1
        assert await r.function_list(library="", withcode=withcode) == []
