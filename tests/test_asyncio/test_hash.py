import asyncio
import math
import time
from datetime import datetime, timedelta
from unittest import mock

import pytest

from redis import exceptions
from redis.commands.core import HashDataPersistOptions
from tests.conftest import skip_if_server_version_lt
from tests.test_asyncio.test_utils import redis_server_time


async def _wait_for_fields_expired(r, key, *fields, timeout=2.0):
    """Poll until every field is gone, so short TTLs need no fixed sleep."""
    deadline = time.monotonic() + timeout
    for field in fields:
        while await r.hexists(key, field):
            assert time.monotonic() < deadline, f"{field!r} did not expire"
            await asyncio.sleep(0.01)


@skip_if_server_version_lt("7.3.240")
async def test_hexpire_basic(r):
    await r.delete("test:hash")
    await r.hset("test:hash", mapping={"field1": "value1", "field2": "value2"})
    assert await r.hexpire("test:hash", 1, "field1") == [1]
    await asyncio.sleep(1.1)
    assert await r.hexists("test:hash", "field1") is False
    assert await r.hexists("test:hash", "field2") is True


@skip_if_server_version_lt("7.3.240")
async def test_hexpire_with_timedelta(r):
    await r.delete("test:hash")
    await r.hset("test:hash", mapping={"field1": "value1", "field2": "value2"})
    assert await r.hexpire("test:hash", timedelta(seconds=10), "field1") == [1]
    # test_hexpire_basic waits for a real expiry; here the stored TTL is enough
    assert 0 < (await r.hpttl("test:hash", "field1"))[0] <= 10000
    assert await r.httl("test:hash", "field2") == [-1]


@skip_if_server_version_lt("7.3.240")
async def test_hexpire_conditions(r):
    await r.delete("test:hash")
    await r.hset("test:hash", mapping={"field1": "value1"})
    assert await r.hexpire("test:hash", 20, "field1", xx=True) == [0]
    assert await r.hexpire("test:hash", 20, "field1", nx=True) == [1]
    assert await r.hexpire("test:hash", 10, "field1", xx=True) == [1]
    assert await r.hexpire("test:hash", 20, "field1", nx=True) == [0]
    # the xx call lowered the TTL and the last nx call left it alone
    assert 0 < (await r.hpttl("test:hash", "field1"))[0] <= 10000
    await r.hexpire("test:hash", 20, "field1")
    assert await r.hexpire("test:hash", 10, "field1", gt=True) == [0]
    assert await r.hexpire("test:hash", 10, "field1", lt=True) == [1]
    assert 0 < (await r.hpttl("test:hash", "field1"))[0] <= 10000


@skip_if_server_version_lt("7.3.240")
async def test_hexpire_nonexistent_key_or_field(r):
    await r.delete("test:hash")
    assert await r.hexpire("test:hash", 1, "field1") == [-2]
    await r.hset("test:hash", "field1", "value1")
    assert await r.hexpire("test:hash", 1, "nonexistent_field") == [-2]


@skip_if_server_version_lt("7.3.240")
async def test_hexpire_multiple_fields(r):
    await r.delete("test:hash")
    await r.hset(
        "test:hash",
        mapping={"field1": "value1", "field2": "value2", "field3": "value3"},
    )
    assert await r.hexpire("test:hash", 10, "field1", "field2") == [1, 1]
    ttl1, ttl2, ttl3 = await r.hpttl("test:hash", "field1", "field2", "field3")
    assert 0 < ttl1 <= 10000
    assert 0 < ttl2 <= 10000
    assert ttl3 == -1


@skip_if_server_version_lt("7.3.240")
async def test_hpexpire_basic(r):
    await r.delete("test:hash")
    await r.hset("test:hash", mapping={"field1": "value1", "field2": "value2"})
    assert await r.hpexpire("test:hash", 50, "field1") == [1]
    await _wait_for_fields_expired(r, "test:hash", "field1")
    assert await r.hexists("test:hash", "field2") is True


@skip_if_server_version_lt("7.3.240")
async def test_hpexpire_with_timedelta(r):
    await r.delete("test:hash")
    await r.hset("test:hash", mapping={"field1": "value1", "field2": "value2"})
    assert await r.hpexpire("test:hash", timedelta(milliseconds=50), "field1") == [1]
    await _wait_for_fields_expired(r, "test:hash", "field1")
    assert await r.hexists("test:hash", "field2") is True


@skip_if_server_version_lt("7.3.240")
async def test_hpexpire_timedelta_is_exact(r):
    await r.delete("test:hash")
    await r.hset("test:hash", "field1", "value1")
    with mock.patch.object(
        r, "execute_command", mock.AsyncMock(return_value=[1])
    ) as cmd:
        await r.hpexpire("test:hash", timedelta(milliseconds=1001), "field1")
    assert cmd.call_args.args == ("HPEXPIRE", "test:hash", 1001, "FIELDS", 1, "field1")
    assert await r.hpexpire("test:hash", timedelta(milliseconds=1001), "field1") == [1]
    assert 0 < (await r.hpttl("test:hash", "field1"))[0] <= 1001


@skip_if_server_version_lt("7.3.240")
async def test_hpexpire_conditions(r):
    await r.delete("test:hash")
    await r.hset("test:hash", mapping={"field1": "value1"})
    assert await r.hpexpire("test:hash", 1500, "field1", xx=True) == [0]
    assert await r.hpexpire("test:hash", 1500, "field1", nx=True) == [1]
    assert await r.hpexpire("test:hash", 1500, "field1", nx=True) == [0]
    # lower the TTL last, so the field cannot expire before the nx check
    assert await r.hpexpire("test:hash", 50, "field1", xx=True) == [1]
    await _wait_for_fields_expired(r, "test:hash", "field1")
    await r.hset("test:hash", "field1", "value1")
    await r.hpexpire("test:hash", 1500, "field1")
    assert await r.hpexpire("test:hash", 50, "field1", gt=True) == [0]
    assert await r.hpexpire("test:hash", 50, "field1", lt=True) == [1]
    await _wait_for_fields_expired(r, "test:hash", "field1")


@skip_if_server_version_lt("7.3.240")
async def test_hpexpire_nonexistent_key_or_field(r):
    await r.delete("test:hash")
    assert await r.hpexpire("test:hash", 500, "field1") == [-2]
    await r.hset("test:hash", "field1", "value1")
    assert await r.hpexpire("test:hash", 500, "nonexistent_field") == [-2]


@skip_if_server_version_lt("7.3.240")
async def test_hpexpire_multiple_fields(r):
    await r.delete("test:hash")
    await r.hset(
        "test:hash",
        mapping={"field1": "value1", "field2": "value2", "field3": "value3"},
    )
    assert await r.hpexpire("test:hash", 50, "field1", "field2") == [1, 1]
    await _wait_for_fields_expired(r, "test:hash", "field1", "field2")
    assert await r.hexists("test:hash", "field3") is True


@skip_if_server_version_lt("7.3.240")
async def test_hexpireat_basic(r):
    await r.delete("test:hash")
    await r.hset("test:hash", mapping={"field1": "value1", "field2": "value2"})
    exp_time = math.ceil((datetime.now() + timedelta(seconds=10)).timestamp())
    assert await r.hexpireat("test:hash", exp_time, "field1") == [1]
    # the stored expire time is exact; test_hexpireat_conditions covers deletion
    assert await r.hexpiretime("test:hash", "field1", "field2") == [exp_time, -1]


@skip_if_server_version_lt("7.3.240")
async def test_hexpireat_with_datetime(r):
    await r.delete("test:hash")
    await r.hset("test:hash", mapping={"field1": "value1", "field2": "value2"})
    exp_time = (datetime.now() + timedelta(seconds=10)).replace(microsecond=0)
    assert await r.hexpireat("test:hash", exp_time, "field1") == [1]
    assert await r.hexpiretime("test:hash", "field1", "field2") == [
        int(exp_time.timestamp()),
        -1,
    ]


@skip_if_server_version_lt("7.3.240")
async def test_hexpireat_conditions(r):
    await r.delete("test:hash")
    await r.hset("test:hash", mapping={"field1": "value1"})
    future_exp_time = int((datetime.now() + timedelta(seconds=2)).timestamp())
    past_exp_time = int((datetime.now() - timedelta(seconds=1)).timestamp())
    assert await r.hexpireat("test:hash", future_exp_time, "field1", xx=True) == [0]
    assert await r.hexpireat("test:hash", future_exp_time, "field1", nx=True) == [1]
    assert await r.hexpireat("test:hash", past_exp_time, "field1", gt=True) == [0]
    assert await r.hexpireat("test:hash", past_exp_time, "field1", lt=True) == [2]
    assert await r.hexists("test:hash", "field1") is False


@skip_if_server_version_lt("7.3.240")
async def test_hexpireat_nonexistent_key_or_field(r):
    await r.delete("test:hash")
    future_exp_time = int((datetime.now() + timedelta(seconds=1)).timestamp())
    assert await r.hexpireat("test:hash", future_exp_time, "field1") == [-2]
    await r.hset("test:hash", "field1", "value1")
    assert await r.hexpireat("test:hash", future_exp_time, "nonexistent_field") == [-2]


@skip_if_server_version_lt("7.3.240")
async def test_hexpireat_multiple_fields(r):
    await r.delete("test:hash")
    await r.hset(
        "test:hash",
        mapping={"field1": "value1", "field2": "value2", "field3": "value3"},
    )
    exp_time = math.ceil((datetime.now() + timedelta(seconds=10)).timestamp())
    assert await r.hexpireat("test:hash", exp_time, "field1", "field2") == [1, 1]
    assert await r.hexpiretime("test:hash", "field1", "field2", "field3") == [
        exp_time,
        exp_time,
        -1,
    ]


@skip_if_server_version_lt("7.3.240")
async def test_hpexpireat_basic(r):
    await r.delete("test:hash")
    await r.hset("test:hash", mapping={"field1": "value1", "field2": "value2"})
    exp_time = int((datetime.now() + timedelta(seconds=10)).timestamp() * 1000)
    assert await r.hpexpireat("test:hash", exp_time, "field1") == [1]
    # the stored expire time is exact; test_hpexpireat_conditions covers deletion
    assert await r.hpexpiretime("test:hash", "field1", "field2") == [exp_time, -1]


@skip_if_server_version_lt("7.3.240")
async def test_hpexpireat_with_datetime(r):
    await r.delete("test:hash")
    await r.hset("test:hash", mapping={"field1": "value1", "field2": "value2"})
    exp_time = datetime.now() + timedelta(seconds=10)
    assert await r.hpexpireat("test:hash", exp_time, "field1") == [1]
    assert await r.hpexpiretime("test:hash", "field1", "field2") == [
        int(exp_time.timestamp() * 1000),
        -1,
    ]


@skip_if_server_version_lt("7.3.240")
async def test_hpexpireat_conditions(r):
    await r.delete("test:hash")
    await r.hset("test:hash", mapping={"field1": "value1"})
    future_exp_time = int(
        (datetime.now() + timedelta(milliseconds=500)).timestamp() * 1000
    )
    past_exp_time = int(
        (datetime.now() - timedelta(milliseconds=500)).timestamp() * 1000
    )
    assert await r.hpexpireat("test:hash", future_exp_time, "field1", xx=True) == [0]
    assert await r.hpexpireat("test:hash", future_exp_time, "field1", nx=True) == [1]
    assert await r.hpexpireat("test:hash", past_exp_time, "field1", gt=True) == [0]
    assert await r.hpexpireat("test:hash", past_exp_time, "field1", lt=True) == [2]
    assert await r.hexists("test:hash", "field1") is False


@skip_if_server_version_lt("7.3.240")
async def test_hpexpireat_nonexistent_key_or_field(r):
    await r.delete("test:hash")
    future_exp_time = int(
        (datetime.now() + timedelta(milliseconds=500)).timestamp() * 1000
    )
    assert await r.hpexpireat("test:hash", future_exp_time, "field1") == [-2]
    await r.hset("test:hash", "field1", "value1")
    assert await r.hpexpireat("test:hash", future_exp_time, "nonexistent_field") == [-2]


@skip_if_server_version_lt("7.3.240")
async def test_hpexpireat_multiple_fields(r):
    await r.delete("test:hash")
    await r.hset(
        "test:hash",
        mapping={"field1": "value1", "field2": "value2", "field3": "value3"},
    )
    exp_time = int((datetime.now() + timedelta(seconds=10)).timestamp() * 1000)
    assert await r.hpexpireat("test:hash", exp_time, "field1", "field2") == [1, 1]
    assert await r.hpexpiretime("test:hash", "field1", "field2", "field3") == [
        exp_time,
        exp_time,
        -1,
    ]


@skip_if_server_version_lt("7.3.240")
async def test_hpersist_multiple_fields_mixed_conditions(r):
    await r.delete("test:hash")
    await r.hset("test:hash", mapping={"field1": "value1", "field2": "value2"})
    await r.hexpire("test:hash", 5000, "field1")
    assert await r.hpersist("test:hash", "field1", "field2", "field3") == [1, -1, -2]


@skip_if_server_version_lt("7.3.240")
async def test_hexpiretime_multiple_fields_mixed_conditions(r):
    await r.delete("test:hash")
    await r.hset("test:hash", mapping={"field1": "value1", "field2": "value2"})
    future_time = int((datetime.now() + timedelta(minutes=30)).timestamp())
    await r.hexpireat("test:hash", future_time, "field1")
    result = await r.hexpiretime("test:hash", "field1", "field2", "field3")
    assert future_time - 10 < result[0] <= future_time
    assert result[1:] == [-1, -2]


@skip_if_server_version_lt("7.3.240")
async def test_hpexpiretime_multiple_fields_mixed_conditions(r):
    await r.delete("test:hash")
    await r.hset("test:hash", mapping={"field1": "value1", "field2": "value2"})
    future_time = int((datetime.now() + timedelta(minutes=30)).timestamp())
    await r.hexpireat("test:hash", future_time, "field1")
    result = await r.hpexpiretime("test:hash", "field1", "field2", "field3")
    assert future_time * 1000 - 10000 < result[0] <= future_time * 1000
    assert result[1:] == [-1, -2]


@skip_if_server_version_lt("7.3.240")
async def test_ttl_multiple_fields_mixed_conditions(r):
    await r.delete("test:hash")
    await r.hset("test:hash", mapping={"field1": "value1", "field2": "value2"})
    future_time = int((datetime.now() + timedelta(minutes=30)).timestamp())
    await r.hexpireat("test:hash", future_time, "field1")
    result = await r.httl("test:hash", "field1", "field2", "field3")
    assert 30 * 60 - 10 < result[0] <= 30 * 60
    assert result[1:] == [-1, -2]


@skip_if_server_version_lt("7.3.240")
async def test_pttl_multiple_fields_mixed_conditions(r):
    await r.delete("test:hash")
    await r.hset("test:hash", mapping={"field1": "value1", "field2": "value2"})
    future_time = int((datetime.now() + timedelta(minutes=30)).timestamp())
    await r.hexpireat("test:hash", future_time, "field1")
    result = await r.hpttl("test:hash", "field1", "field2", "field3")
    assert 30 * 60000 - 10000 < result[0] <= 30 * 60000
    assert result[1:] == [-1, -2]


@skip_if_server_version_lt("7.9.0")
async def test_hgetdel(r):
    await r.delete("test:hash")
    await r.hset("test:hash", "foo", "bar", mapping={"1": 1, "2": 2})
    assert await r.hgetdel("test:hash", "foo", "1") == [b"bar", b"1"]
    assert await r.hget("test:hash", "foo") is None
    assert await r.hget("test:hash", "1") is None
    assert await r.hget("test:hash", "2") == b"2"
    assert await r.hgetdel("test:hash", "foo", "1") == [None, None]
    assert await r.hget("test:hash", "2") == b"2"

    with pytest.raises(exceptions.DataError):
        await r.hgetdel("test:hash")


@skip_if_server_version_lt("7.9.0")
async def test_hgetex_no_expiration(r):
    await r.delete("test:hash")
    await r.hset(
        "b", "foo", "bar", mapping={"1": 1, "2": 2, "3": "three", "4": b"four"}
    )

    assert await r.hgetex("b", "foo", "1", "4") == [b"bar", b"1", b"four"]
    assert await r.hgetex("b", "foo") == [b"bar"]
    assert await r.httl("b", "foo", "1", "4") == [-1, -1, -1]


@skip_if_server_version_lt("7.9.0")
async def test_hgetex_expiration_configs(r):
    await r.delete("test:hash")
    await r.hset(
        "test:hash", "foo", "bar", mapping={"1": 1, "3": "three", "4": b"four"}
    )

    test_keys = ["foo", "1", "4"]
    # test get with multiple fields with expiration set through 'ex'
    assert await r.hgetex("test:hash", *test_keys, ex=10) == [
        b"bar",
        b"1",
        b"four",
    ]
    ttls = await r.httl("test:hash", *test_keys)
    for ttl in ttls:
        assert pytest.approx(ttl) == 10

    # test get with multiple fields removing expiration settings with 'persist'
    assert await r.hgetex("test:hash", *test_keys, persist=True) == [
        b"bar",
        b"1",
        b"four",
    ]
    assert await r.httl("test:hash", *test_keys) == [-1, -1, -1]

    # test get with multiple fields with expiration set through 'px'
    assert await r.hgetex("test:hash", *test_keys, px=6000) == [
        b"bar",
        b"1",
        b"four",
    ]
    ttls = await r.httl("test:hash", *test_keys)
    for ttl in ttls:
        assert pytest.approx(ttl) == 6

    # test get single field with expiration set through 'pxat'
    expire_at = await redis_server_time(r) + timedelta(minutes=1)
    assert await r.hgetex("test:hash", "foo", pxat=expire_at) == [b"bar"]
    assert (await r.httl("test:hash", "foo"))[0] <= 61

    # test get single field with expiration set through 'exat'
    expire_at = await redis_server_time(r) + timedelta(seconds=10)
    assert await r.hgetex("test:hash", "foo", exat=expire_at) == [b"bar"]
    assert (await r.httl("test:hash", "foo"))[0] <= 10


@skip_if_server_version_lt("7.9.0")
async def test_hgetex_validate_expired_fields_removed(r):
    await r.delete("test:hash")
    await r.hset(
        "test:hash", "foo", "bar", mapping={"1": 1, "3": "three", "4": b"four"}
    )

    # test get multiple fields with expiration set
    # validate that expired fields are removed
    assert await r.hgetex("test:hash", "foo", "1", "3", px=50) == [
        b"bar",
        b"1",
        b"three",
    ]
    await _wait_for_fields_expired(r, "test:hash", "foo", "1", "3")
    assert await r.hgetex("test:hash", "foo", "1", "3") == [None, None, None]
    assert await r.httl("test:hash", "foo", "1", "3") == [-2, -2, -2]
    assert await r.hgetex("test:hash", "4") == [b"four"]


@skip_if_server_version_lt("7.9.0")
async def test_hgetex_invalid_inputs(r):
    with pytest.raises(exceptions.DataError):
        await r.hgetex("b", "foo", ex=10, persist=True)

    with pytest.raises(exceptions.DataError):
        await r.hgetex("b", "foo", ex=10.0, persist=True)

    with pytest.raises(exceptions.DataError):
        await r.hgetex("b", "foo", ex=10, px=6000)

    with pytest.raises(exceptions.DataError):
        await r.hgetex("b", ex=10)


@skip_if_server_version_lt("7.9.0")
async def test_hsetex_no_expiration(r):
    await r.delete("test:hash")

    # # set items from mapping without expiration
    assert await r.hsetex("test:hash", None, None, mapping={"1": 1, "4": b"four"}) == 1
    assert await r.httl("test:hash", "foo", "1", "4") == [-2, -1, -1]
    assert await r.hgetex("test:hash", "foo", "1") == [None, b"1"]


@skip_if_server_version_lt("7.9.0")
async def test_hsetex_expiration_ex_and_keepttl(r):
    await r.delete("test:hash")

    # set items from key/value provided
    # combined with mapping and items with expiration - testing ex field
    assert (
        await r.hsetex(
            "test:hash",
            "foo",
            "bar",
            mapping={"1": 1, "2": "2"},
            items=["i1", 11, "i2", 22],
            ex=10,
        )
        == 1
    )
    test_keys = ["foo", "1", "2", "i1", "i2"]
    ttls = await r.httl("test:hash", *test_keys)
    for ttl in ttls:
        assert pytest.approx(ttl) == 10

    assert await r.hgetex("test:hash", *test_keys) == [
        b"bar",
        b"1",
        b"2",
        b"11",
        b"22",
    ]
    await asyncio.sleep(0.01)
    # validate keepttl: the TTL keeps counting down instead of being cleared
    assert await r.hsetex("test:hash", "foo", "bar1", keepttl=True) == 1
    assert 0 < (await r.hpttl("test:hash", "foo"))[0] < 10000


@skip_if_server_version_lt("7.9.0")
async def test_hsetex_expiration_px(r):
    await r.delete("test:hash")
    # set items from key/value provided and mapping
    # with expiration - testing px field
    assert (
        await r.hsetex("test:hash", "foo", "bar", mapping={"1": 1, "2": "2"}, px=60000)
        == 1
    )
    test_keys = ["foo", "1", "2"]
    ttls = await r.httl("test:hash", *test_keys)
    for ttl in ttls:
        assert pytest.approx(ttl) == 60

    assert await r.hgetex("test:hash", *test_keys) == [b"bar", b"1", b"2"]


@skip_if_server_version_lt("7.9.0")
async def test_hsetex_expiration_pxat_and_fnx(r):
    await r.delete("test:hash")
    assert (
        await r.hsetex("test:hash", "foo", "bar", mapping={"1": 1, "2": "2"}, ex=30)
        == 1
    )

    expire_at = await redis_server_time(r) + timedelta(minutes=1)
    assert (
        await r.hsetex(
            "test:hash",
            "foo",
            "bar1",
            mapping={"new": "ok"},
            pxat=expire_at,
            data_persist_option=HashDataPersistOptions.FNX,
        )
        == 0
    )
    ttls = await r.httl("test:hash", "foo", "new")
    assert ttls[0] <= 30
    assert ttls[1] == -2

    assert await r.hgetex("test:hash", "foo", "1", "new") == [b"bar", b"1", None]
    assert (
        await r.hsetex(
            "test:hash",
            "foo_new",
            "bar1",
            mapping={"new": "ok"},
            pxat=expire_at,
            data_persist_option=HashDataPersistOptions.FNX,
        )
        == 1
    )
    ttls = await r.httl("test:hash", "foo", "new")
    for ttl in ttls:
        assert ttl <= 61
    assert await r.hgetex("test:hash", "foo", "foo_new", "new") == [
        b"bar",
        b"bar1",
        b"ok",
    ]


@skip_if_server_version_lt("7.9.0")
async def test_hsetex_expiration_exat_and_fxx(r):
    await r.delete("test:hash")
    assert (
        await r.hsetex("test:hash", "foo", "bar", mapping={"1": 1, "2": "2"}, ex=30)
        == 1
    )

    expire_at = await redis_server_time(r) + timedelta(seconds=10)
    assert (
        await r.hsetex(
            "test:hash",
            "foo",
            "bar1",
            mapping={"new": "ok"},
            exat=expire_at,
            data_persist_option=HashDataPersistOptions.FXX,
        )
        == 0
    )
    ttls = await r.httl("test:hash", "foo", "new")
    assert 10 < ttls[0] <= 30
    assert ttls[1] == -2

    assert await r.hgetex("test:hash", "foo", "1", "new") == [b"bar", b"1", None]
    assert (
        await r.hsetex(
            "test:hash",
            "foo",
            "bar1",
            mapping={"1": "new_value"},
            exat=expire_at,
            data_persist_option=HashDataPersistOptions.FXX,
        )
        == 1
    )
    assert await r.hgetex("test:hash", "foo", "1") == [b"bar1", b"new_value"]


@skip_if_server_version_lt("7.9.0")
async def test_hsetex_invalid_inputs(r):
    with pytest.raises(exceptions.DataError):
        await r.hsetex("b", "foo", "bar", ex=10.0)

    with pytest.raises(exceptions.DataError):
        await r.hsetex("b", None, None)

    with pytest.raises(exceptions.DataError):
        await r.hsetex("b", "foo", "bar", items=["i1", 11, "i2"], px=6000)

    with pytest.raises(exceptions.DataError):
        await r.hsetex("b", "foo", "bar", ex=10, keepttl=True)
