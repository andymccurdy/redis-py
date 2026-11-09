import pytest
import pytest_asyncio
import redis.asyncio as redis
from redis.exceptions import DataError


@pytest.mark.onlynoncluster
class TestEncoding:
    @pytest_asyncio.fixture()
    async def r(self, create_redis):
        redis = await create_redis(decode_responses=True)
        yield redis
        await redis.flushall()

    @pytest_asyncio.fixture()
    async def r_no_decode(self, create_redis):
        redis = await create_redis(decode_responses=False)
        yield redis
        await redis.flushall()

    async def test_simple_encoding(self, r_no_decode: redis.Redis):
        unicode_string = chr(3456) + "abcd" + chr(3421)
        await r_no_decode.set("unicode-string", unicode_string.encode("utf-8"))
        cached_val = await r_no_decode.get("unicode-string")
        assert isinstance(cached_val, bytes)
        assert unicode_string == cached_val.decode("utf-8")

    async def test_simple_encoding_and_decoding(self, r: redis.Redis):
        unicode_string = chr(3456) + "abcd" + chr(3421)
        await r.set("unicode-string", unicode_string)
        cached_val = await r.get("unicode-string")
        assert isinstance(cached_val, str)
        assert unicode_string == cached_val

    async def test_memoryview_encoding(self, r_no_decode: redis.Redis):
        unicode_string = chr(3456) + "abcd" + chr(3421)
        unicode_string_view = memoryview(unicode_string.encode("utf-8"))
        await r_no_decode.set("unicode-string-memoryview", unicode_string_view)
        cached_val = await r_no_decode.get("unicode-string-memoryview")
        # The cached value won't be a memoryview because it's a copy from Redis
        assert isinstance(cached_val, bytes)
        assert unicode_string == cached_val.decode("utf-8")

    async def test_memoryview_encoding_and_decoding(self, r: redis.Redis):
        unicode_string = chr(3456) + "abcd" + chr(3421)
        unicode_string_view = memoryview(unicode_string.encode("utf-8"))
        await r.set("unicode-string-memoryview", unicode_string_view)
        cached_val = await r.get("unicode-string-memoryview")
        assert isinstance(cached_val, str)
        assert unicode_string == cached_val

    async def test_list_encoding(self, r: redis.Redis):
        unicode_string = chr(3456) + "abcd" + chr(3421)
        result = [unicode_string, unicode_string, unicode_string]
        await r.rpush("a", *result)
        assert await r.lrange("a", 0, -1) == result


@pytest.mark.onlynoncluster
class TestEncodingErrors:
    async def test_ignore(self, create_redis):
        r = await create_redis(decode_responses=True, encoding_errors="ignore")
        await r.set("a", b"foo\xff")
        assert await r.get("a") == "foo"

    async def test_replace(self, create_redis):
        r = await create_redis(decode_responses=True, encoding_errors="replace")
        await r.set("a", b"foo\xff")
        assert await r.get("a") == "foo\ufffd"


@pytest.mark.onlynoncluster
class TestMemoryviewsAreNotPacked:
    async def test_memoryviews_are_not_packed(self, r):
        arg = memoryview(b"some_arg")
        arg_list = ["SOME_COMMAND", arg]
        c = r.connection or await r.connection_pool.get_connection()
        cmd = c.pack_command(*arg_list)
        assert cmd[1] is arg
        cmds = c.pack_commands([arg_list, arg_list])
        assert cmds[1] is arg
        assert cmds[3] is arg


class TestCommandsAreNotEncoded:
    @pytest_asyncio.fixture()
    async def r(self, create_redis):
        redis = await create_redis(encoding="utf-16")
        yield redis
        await redis.flushall()

    @pytest.mark.xfail
    async def test_basic_command(self, r: redis.Redis):
        await r.set("hello", "world")


class TestInvalidUserInput:
    async def test_boolean_fails(self, r: redis.Redis):
        with pytest.raises(DataError):
            await r.set("a", True)  # type: ignore

    async def test_none_fails(self, r: redis.Redis):
        with pytest.raises(DataError):
            await r.set("a", None)  # type: ignore

    async def test_user_type_fails(self, r: redis.Redis):
        class Foo:
            def __str__(self):
                return "Foo"

        with pytest.raises(DataError):
            await r.set("a", Foo())  # type: ignore
