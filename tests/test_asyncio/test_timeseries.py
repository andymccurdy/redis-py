import time

import pytest
import pytest_asyncio
import redis.asyncio as redis
from redis.utils import SENTINEL
from tests.conftest import (
    assert_resp_response,
    expected_response_shape,
    expects_resp2_shape,
    is_resp2_connection,
    skip_if_server_version_gte,
    skip_if_server_version_lt,
    skip_ifmodversion_lt,
)
from tests.test_timeseries import _assert_nrange_rows

KEY1 = "key1"
KEY2 = "key2"
KEY3 = "key3"
KEY4 = "key4"
KEY5 = "key5"


@pytest_asyncio.fixture()
async def decoded_r(create_redis, stack_url):
    return await create_redis(decode_responses=True, url=stack_url)


@pytest.mark.redismod
async def test_create(decoded_r: redis.Redis):
    assert await decoded_r.ts().create(KEY1)
    assert await decoded_r.ts().create(KEY2, retention_msecs=5)
    assert await decoded_r.ts().create(KEY3, labels={"Redis": "Labs"})
    assert await decoded_r.ts().create(
        KEY4, retention_msecs=20, labels={"Time": "Series"}
    )
    info = await decoded_r.ts().info(KEY4)
    assert_resp_response(
        decoded_r, 20, info.get("retention_msecs"), info.get("retentionTime")
    )
    assert "Series" == info["labels"]["Time"]

    # Test for a chunk size of 128 Bytes
    assert await decoded_r.ts().create("time-serie-1", chunk_size=128)
    info = await decoded_r.ts().info("time-serie-1")
    assert_resp_response(decoded_r, 128, info.get("chunk_size"), info.get("chunkSize"))


@pytest.mark.redismod
@skip_ifmodversion_lt("1.4.0", "timeseries")
async def test_create_duplicate_policy(decoded_r: redis.Redis):
    # Test for duplicate policy
    for duplicate_policy in ["block", "last", "first", "min", "max"]:
        ts_name = f"time-serie-ooo-{duplicate_policy}"
        assert await decoded_r.ts().create(ts_name, duplicate_policy=duplicate_policy)
        info = await decoded_r.ts().info(ts_name)
        assert_resp_response(
            decoded_r,
            duplicate_policy,
            info.get("duplicate_policy"),
            info.get("duplicatePolicy"),
        )


@pytest.mark.redismod
async def test_alter(decoded_r: redis.Redis):
    assert await decoded_r.ts().create(KEY1)
    res = await decoded_r.ts().info(KEY1)
    assert_resp_response(
        decoded_r, 0, res.get("retention_msecs"), res.get("retentionTime")
    )
    assert await decoded_r.ts().alter(KEY1, retention_msecs=10)
    res = await decoded_r.ts().info(KEY1)
    assert {} == (await decoded_r.ts().info(KEY1))["labels"]
    info = await decoded_r.ts().info(KEY1)
    assert_resp_response(
        decoded_r, 10, info.get("retention_msecs"), info.get("retentionTime")
    )
    assert await decoded_r.ts().alter(KEY1, labels={"Time": "Series"})
    res = await decoded_r.ts().info(KEY1)
    assert "Series" == (await decoded_r.ts().info(KEY1))["labels"]["Time"]
    info = await decoded_r.ts().info(KEY1)
    assert_resp_response(
        decoded_r, 10, info.get("retention_msecs"), info.get("retentionTime")
    )
    assert await decoded_r.ts().alter(KEY1, labels={})
    assert {} == (await decoded_r.ts().info(KEY1))["labels"]


@pytest.mark.redismod
@skip_ifmodversion_lt("1.4.0", "timeseries")
@skip_if_server_version_lt("7.9.0")
async def test_alter_duplicate_policy(decoded_r: redis.Redis):
    assert await decoded_r.ts().create(KEY1)
    info = await decoded_r.ts().info(KEY1)
    assert_resp_response(
        decoded_r, "block", info.get("duplicate_policy"), info.get("duplicatePolicy")
    )
    assert await decoded_r.ts().alter(KEY1, duplicate_policy="min")
    info = await decoded_r.ts().info(KEY1)
    assert_resp_response(
        decoded_r, "min", info.get("duplicate_policy"), info.get("duplicatePolicy")
    )


@pytest.mark.redismod
@skip_ifmodversion_lt("1.4.0", "timeseries")
@skip_if_server_version_gte("7.9.0")
async def test_alter_duplicate_policy_prior_redis_8(decoded_r: redis.Redis):
    assert await decoded_r.ts().create(KEY1)
    info = await decoded_r.ts().info(KEY1)
    assert_resp_response(
        decoded_r, None, info.get("duplicate_policy"), info.get("duplicatePolicy")
    )
    assert await decoded_r.ts().alter(KEY1, duplicate_policy="min")
    info = await decoded_r.ts().info(KEY1)
    assert_resp_response(
        decoded_r, "min", info.get("duplicate_policy"), info.get("duplicatePolicy")
    )


@pytest.mark.redismod
async def test_add(decoded_r: redis.Redis):
    assert 1 == await decoded_r.ts().add(KEY1, 1, 1)
    assert 2 == await decoded_r.ts().add(KEY2, 2, 3, retention_msecs=10)
    assert 3 == await decoded_r.ts().add(KEY3, 3, 2, labels={"Redis": "Labs"})
    assert 4 == await decoded_r.ts().add(
        KEY4, 4, 2, retention_msecs=10, labels={"Redis": "Labs", "Time": "Series"}
    )
    res = await decoded_r.ts().add(KEY5, "*", 1)
    assert abs(time.time() - round(float(res) / 1000)) < 1.0

    info = await decoded_r.ts().info(KEY4)
    assert_resp_response(
        decoded_r, 10, info.get("retention_msecs"), info.get("retentionTime")
    )
    assert "Labs" == info["labels"]["Redis"]

    # Test for a chunk size of 128 Bytes on TS.ADD
    assert await decoded_r.ts().add("time-serie-1", 1, 10.0, chunk_size=128)
    info = await decoded_r.ts().info("time-serie-1")
    assert_resp_response(decoded_r, 128, info.get("chunk_size"), info.get("chunkSize"))


@pytest.mark.redismod
@skip_ifmodversion_lt("1.4.0", "timeseries")
async def test_add_duplicate_policy(decoded_r: redis.Redis):
    # Test for duplicate policy BLOCK
    assert 1 == await decoded_r.ts().add("time-serie-add-ooo-block", 1, 5.0)
    with pytest.raises(Exception):
        await decoded_r.ts().add(
            "time-serie-add-ooo-block", 1, 5.0, on_duplicate="block"
        )

    # Test for duplicate policy LAST
    assert 1 == await decoded_r.ts().add("time-serie-add-ooo-last", 1, 5.0)
    assert 1 == await decoded_r.ts().add(
        "time-serie-add-ooo-last", 1, 10.0, on_duplicate="last"
    )
    res = await decoded_r.ts().get("time-serie-add-ooo-last")
    assert res is not None
    assert 10.0 == res[1]

    # Test for duplicate policy FIRST
    assert 1 == await decoded_r.ts().add("time-serie-add-ooo-first", 1, 5.0)
    assert 1 == await decoded_r.ts().add(
        "time-serie-add-ooo-first", 1, 10.0, on_duplicate="first"
    )
    res = await decoded_r.ts().get("time-serie-add-ooo-first")
    assert res is not None
    assert 5.0 == res[1]

    # Test for duplicate policy MAX
    assert 1 == await decoded_r.ts().add("time-serie-add-ooo-max", 1, 5.0)
    assert 1 == await decoded_r.ts().add(
        "time-serie-add-ooo-max", 1, 10.0, on_duplicate="max"
    )
    res = await decoded_r.ts().get("time-serie-add-ooo-max")
    assert res is not None
    assert 10.0 == res[1]

    # Test for duplicate policy MIN
    assert 1 == await decoded_r.ts().add("time-serie-add-ooo-min", 1, 5.0)
    assert 1 == await decoded_r.ts().add(
        "time-serie-add-ooo-min", 1, 10.0, on_duplicate="min"
    )
    res = await decoded_r.ts().get("time-serie-add-ooo-min")
    assert res is not None
    assert 5.0 == res[1]


@pytest.mark.redismod
async def test_madd(decoded_r: redis.Redis):
    await decoded_r.ts().create("a")
    assert [1, 2, 3] == await decoded_r.ts().madd(
        [("a", 1, 5), ("a", 2, 10), ("a", 3, 15)]
    )


@pytest.mark.redismod
async def test_incrby_decrby(decoded_r: redis.Redis):
    # explicit, increasing timestamps give each call its own sample
    for ts in range(1, 6):
        assert await decoded_r.ts().incrby(KEY1, 1, timestamp=ts)
    assert 5 == (await decoded_r.ts().get(KEY1))[1]
    for ts in range(6, 11):
        assert await decoded_r.ts().decrby(KEY1, 1, timestamp=ts)
    assert 0 == (await decoded_r.ts().get(KEY1))[1]

    assert await decoded_r.ts().incrby(KEY2, 1.5, timestamp=5)
    assert_resp_response(decoded_r, await decoded_r.ts().get(KEY2), (5, 1.5), [5, 1.5])
    assert await decoded_r.ts().incrby(KEY2, 2.25, timestamp=7)
    assert_resp_response(
        decoded_r, await decoded_r.ts().get(KEY2), (7, 3.75), [7, 3.75]
    )
    assert await decoded_r.ts().decrby(KEY2, 1.5, timestamp=15)
    assert_resp_response(
        decoded_r, await decoded_r.ts().get(KEY2), (15, 2.25), [15, 2.25]
    )

    # Test for a chunk size of 128 Bytes on TS.INCRBY
    assert await decoded_r.ts().incrby("time-serie-1", 10, chunk_size=128)
    info = await decoded_r.ts().info("time-serie-1")
    assert_resp_response(decoded_r, 128, info.get("chunk_size"), info.get("chunkSize"))

    # Test for a chunk size of 128 Bytes on TS.DECRBY
    assert await decoded_r.ts().decrby("time-serie-2", 10, chunk_size=128)
    info = await decoded_r.ts().info("time-serie-2")
    assert_resp_response(decoded_r, 128, info.get("chunk_size"), info.get("chunkSize"))


@pytest.mark.redismod
async def test_create_and_delete_rule(decoded_r: redis.Redis):
    # test rule creation
    time = 100
    await decoded_r.ts().create(KEY1)
    await decoded_r.ts().create(KEY2)
    await decoded_r.ts().createrule(KEY1, KEY2, "avg", 100)
    pipe = decoded_r.pipeline(transaction=False)
    for i in range(50):
        pipe.execute_command("TS.ADD", KEY1, time + i * 2, 1)
        pipe.execute_command("TS.ADD", KEY1, time + i * 2 + 1, 2)
    await pipe.execute()
    await decoded_r.ts().add(KEY1, time * 2, 1.5)
    assert round((await decoded_r.ts().get(KEY2))[1], 5) == 1.5
    info = await decoded_r.ts().info(KEY1)
    if is_resp2_connection(decoded_r):
        assert info.rules[0][1] == 100
    else:
        assert info["rules"][KEY2][0] == 100

    # test rule deletion
    await decoded_r.ts().deleterule(KEY1, KEY2)
    info = await decoded_r.ts().info(KEY1)
    assert not info["rules"]


@pytest.mark.redismod
@skip_ifmodversion_lt("1.10.0", "timeseries")
async def test_del_range(decoded_r: redis.Redis):
    try:
        await decoded_r.ts().delete("test", 0, 100)
    except Exception as e:
        assert e.__str__() != ""

    pipe = decoded_r.pipeline(transaction=False)
    for i in range(100):
        pipe.execute_command("TS.ADD", KEY1, i, i % 7)
    await pipe.execute()
    assert 22 == await decoded_r.ts().delete(KEY1, 0, 21)
    assert [] == await decoded_r.ts().range(KEY1, 0, 21)
    assert_resp_response(
        decoded_r,
        await decoded_r.ts().range(KEY1, 22, 22),
        [(22, 1.0)],
        [[22, 1.0]],
    )


@pytest.mark.redismod
async def test_range(decoded_r: redis.Redis):
    pipe = decoded_r.pipeline(transaction=False)
    for i in range(100):
        pipe.execute_command("TS.ADD", KEY1, i, i % 7)
    await pipe.execute()
    assert 100 == len(await decoded_r.ts().range(KEY1, 0, 200))
    pipe = decoded_r.pipeline(transaction=False)
    for i in range(100):
        pipe.execute_command("TS.ADD", KEY1, i + 200, i % 7)
    await pipe.execute()
    assert 200 == len(await decoded_r.ts().range(KEY1, 0, 500))
    # last sample isn't returned
    assert 20 == len(
        await decoded_r.ts().range(
            KEY1, 0, 500, aggregation_type="avg", bucket_size_msec=10
        )
    )
    assert 10 == len(await decoded_r.ts().range(KEY1, 0, 500, count=10))


@pytest.mark.redismod
@skip_ifmodversion_lt("1.10.0", "timeseries")
async def test_range_advanced(decoded_r: redis.Redis):
    pipe = decoded_r.pipeline(transaction=False)
    for i in range(100):
        pipe.execute_command("TS.ADD", KEY1, i, i % 7)
        pipe.execute_command("TS.ADD", KEY1, i + 200, i % 7)
    await pipe.execute()

    assert 2 == len(
        await decoded_r.ts().range(
            KEY1,
            0,
            500,
            filter_by_ts=[i for i in range(10, 20)],
            filter_by_min_value=1,
            filter_by_max_value=2,
        )
    )
    res = await decoded_r.ts().range(
        KEY1, 0, 10, aggregation_type="count", bucket_size_msec=10, align="+"
    )
    assert_resp_response(decoded_r, res, [(0, 10.0), (10, 1.0)], [[0, 10.0], [10, 1.0]])
    res = await decoded_r.ts().range(
        KEY1, 0, 10, aggregation_type="count", bucket_size_msec=10, align=5
    )
    assert_resp_response(decoded_r, res, [(0, 5.0), (5, 6.0)], [[0, 5.0], [5, 6.0]])
    res = await decoded_r.ts().range(
        KEY1, 0, 10, aggregation_type="twa", bucket_size_msec=10
    )
    assert_resp_response(decoded_r, res, [(0, 2.55), (10, 3.0)], [[0, 2.55], [10, 3.0]])


@pytest.mark.redismod
@skip_ifmodversion_lt("1.10.0", "timeseries")
async def test_rev_range(decoded_r: redis.Redis):
    pipe = decoded_r.pipeline(transaction=False)
    for i in range(100):
        pipe.execute_command("TS.ADD", KEY1, i, i % 7)
    await pipe.execute()
    assert 100 == len(await decoded_r.ts().range(KEY1, 0, 200))
    pipe = decoded_r.pipeline(transaction=False)
    for i in range(100):
        pipe.execute_command("TS.ADD", KEY1, i + 200, i % 7)
    await pipe.execute()
    assert 200 == len(await decoded_r.ts().range(KEY1, 0, 500))
    # first sample isn't returned
    assert 20 == len(
        await decoded_r.ts().revrange(
            KEY1, 0, 500, aggregation_type="avg", bucket_size_msec=10
        )
    )
    assert 10 == len(await decoded_r.ts().revrange(KEY1, 0, 500, count=10))
    assert 2 == len(
        await decoded_r.ts().revrange(
            KEY1,
            0,
            500,
            filter_by_ts=[i for i in range(10, 20)],
            filter_by_min_value=1,
            filter_by_max_value=2,
        )
    )
    assert_resp_response(
        decoded_r,
        await decoded_r.ts().revrange(
            KEY1, 0, 10, aggregation_type="count", bucket_size_msec=10, align="+"
        ),
        [(10, 1.0), (0, 10.0)],
        [[10, 1.0], [0, 10.0]],
    )
    assert_resp_response(
        decoded_r,
        await decoded_r.ts().revrange(
            KEY1, 0, 10, aggregation_type="count", bucket_size_msec=10, align=1
        ),
        [(1, 10.0), (0, 1.0)],
        [[1, 10.0], [0, 1.0]],
    )


@pytest.mark.redismod
@skip_if_server_version_lt("8.9.0")
async def test_read(decoded_r: redis.Redis):
    await decoded_r.ts().create(1)
    await decoded_r.ts().add(1, 100, 1.0)
    await decoded_r.ts().add(1, 200, 2.0)
    await decoded_r.ts().add(1, 300, 3.0)

    # Read everything at or after the cursor. TS.READ always returns the same
    # unified sample shape (list of [timestamp, value]) regardless of protocol.
    assert await decoded_r.ts().read(1, 0) == [[100, 1.0], [200, 2.0], [300, 3.0]]

    # The cursor is inclusive.
    assert await decoded_r.ts().read(1, 200) == [[200, 2.0], [300, 3.0]]


@pytest.mark.redismod
@skip_if_server_version_lt("8.9.0")
async def test_read_max_count(decoded_r: redis.Redis):
    await decoded_r.ts().create(1)
    await decoded_r.ts().add(1, 100, 1.0)
    await decoded_r.ts().add(1, 200, 2.0)
    await decoded_r.ts().add(1, 300, 3.0)

    # Bounded paging: read the oldest max_count, then page from last_ts + 1.
    assert await decoded_r.ts().read(1, "-", max_count=2) == [[100, 1.0], [200, 2.0]]
    assert await decoded_r.ts().read(1, 201, max_count=2) == [[300, 3.0]]


@pytest.mark.redismod
@skip_if_server_version_lt("8.9.0")
async def test_read_sentinels(decoded_r: redis.Redis):
    await decoded_r.ts().create(1)
    await decoded_r.ts().add(1, 100, 1.0)
    await decoded_r.ts().add(1, 200, 2.0)
    await decoded_r.ts().add(1, 300, 3.0)

    # `+` resolves to the latest sample, inclusive; returned even without BLOCK.
    assert await decoded_r.ts().read(1, "+") == [[300, 3.0]]

    # `-` reads from the earliest sample.
    assert len(await decoded_r.ts().read(1, "-")) == 3


@pytest.mark.redismod
@skip_if_server_version_lt("8.9.0")
async def test_read_empty(decoded_r: redis.Redis):
    await decoded_r.ts().create(1)
    await decoded_r.ts().add(1, 100, 1.0)

    # A cursor past the newest sample yields an empty (successful) reply.
    assert [] == await decoded_r.ts().read(1, 301)
    # A missing key is also an empty reply, not an error.
    assert [] == await decoded_r.ts().read("missing", 0)


@pytest.mark.redismod
@skip_if_server_version_lt("8.9.0")
async def test_read_block(decoded_r: redis.Redis):
    await decoded_r.ts().create(1)
    await decoded_r.ts().add(1, 100, 1.0)
    await decoded_r.ts().add(1, 200, 2.0)
    await decoded_r.ts().add(1, 300, 3.0)

    # min_count is already met, so the blocking call returns immediately.
    res = await decoded_r.ts().read(1, 0, block_milliseconds=1000, block_min_count=1)
    assert len(res) == 3

    # min_count cannot be reached; after the timeout the available samples flush.
    res = await decoded_r.ts().read(1, 101, block_milliseconds=100, block_min_count=10)
    assert res == [[200, 2.0], [300, 3.0]]

    # A blocking timeout with nothing available is a successful empty reply.
    assert [] == await decoded_r.ts().read(
        1, 301, block_milliseconds=100, block_min_count=1
    )


@pytest.mark.redismod
async def test_range_filter_by_value_requires_both_bounds(decoded_r: redis.Redis):
    await decoded_r.ts().add(1, 100, 1)
    await decoded_r.ts().add(1, 200, 5)

    for cmd in (decoded_r.ts().range, decoded_r.ts().revrange):
        with pytest.raises(
            redis.DataError,
            match="filter_by_min_value and filter_by_max_value must be set together",
        ):
            await cmd(1, 0, 500, filter_by_min_value=1)
        with pytest.raises(
            redis.DataError,
            match="filter_by_min_value and filter_by_max_value must be set together",
        ):
            await cmd(1, 0, 500, filter_by_max_value=5)
        # Both bounds still emit FILTER_BY_VALUE and filter server-side.
        assert 1 == len(
            await cmd(1, 0, 500, filter_by_min_value=1, filter_by_max_value=1)
        )


@pytest.mark.redismod
@skip_if_server_version_lt("8.9.0")
async def test_read_block_min_count_requires_milliseconds(decoded_r: redis.Redis):
    # BLOCK is all-or-nothing: min_count without milliseconds is invalid usage.
    with pytest.raises(
        redis.DataError, match="block_min_count requires block_milliseconds"
    ):
        await decoded_r.ts().read(1, 0, block_min_count=5)


@pytest.mark.onlynoncluster
@pytest.mark.redismod
async def test_multi_range(decoded_r: redis.Redis):
    await decoded_r.ts().create(KEY1, labels={"Test": "This", "team": "ny"})
    await decoded_r.ts().create(
        KEY2, labels={"Test": "This", "Taste": "That", "team": "sf"}
    )
    pipe = decoded_r.pipeline(transaction=False)
    for i in range(100):
        pipe.execute_command("TS.ADD", KEY1, i, i % 7)
        pipe.execute_command("TS.ADD", KEY2, i, i % 11)
    await pipe.execute()

    res = await decoded_r.ts().mrange(0, 200, filters=["Test=This"])
    assert res is not None
    assert 2 == len(res)
    if expects_resp2_shape(decoded_r):
        assert 100 == len(res[0][KEY1][1])

        res = await decoded_r.ts().mrange(0, 200, filters=["Test=This"], count=10)
        assert res is not None
        assert 10 == len(res[0][KEY1][1])

        pipe = decoded_r.pipeline(transaction=False)
        for i in range(100):
            pipe.execute_command("TS.ADD", KEY1, i + 200, i % 7)
        await pipe.execute()
        res = await decoded_r.ts().mrange(
            0, 500, filters=["Test=This"], aggregation_type="avg", bucket_size_msec=10
        )
        assert res is not None
        assert 2 == len(res)
        assert 20 == len(res[0][KEY1][1])

        # test withlabels
        assert {} == res[0][KEY1][0]
        res = await decoded_r.ts().mrange(
            0, 200, filters=["Test=This"], with_labels=True
        )
        assert res is not None
        assert {"Test": "This", "team": "ny"} == res[0][KEY1][0]
    else:
        assert 100 == len(res[KEY1][2])

        res = await decoded_r.ts().mrange(0, 200, filters=["Test=This"], count=10)
        assert res is not None
        assert 10 == len(res[KEY1][2])

        pipe = decoded_r.pipeline(transaction=False)
        for i in range(100):
            pipe.execute_command("TS.ADD", KEY1, i + 200, i % 7)
        await pipe.execute()
        res = await decoded_r.ts().mrange(
            0, 500, filters=["Test=This"], aggregation_type="avg", bucket_size_msec=10
        )
        assert res is not None
        assert 2 == len(res)
        assert 20 == len(res[KEY1][2])

        # test withlabels
        assert {} == res[KEY1][0]
        res = await decoded_r.ts().mrange(
            0, 200, filters=["Test=This"], with_labels=True
        )
        assert res is not None
        assert {"Test": "This", "team": "ny"} == res[KEY1][0]


def _mrange_returned_keys(decoded_r, res):
    """Return the set of series key names in a TS.MRANGE/MREVRANGE reply,
    normalizing the RESP2 (list of single-key dicts) and RESP3/unified
    (dict keyed by name) shapes."""
    if expects_resp2_shape(decoded_r):
        return {next(iter(entry)) for entry in res}
    return set(res.keys())


@pytest.mark.onlynoncluster
@pytest.mark.redismod
@skip_if_server_version_lt("8.9.0")
async def test_mrange_exclude_empty(decoded_r: redis.Redis):
    await decoded_r.ts().create("s", labels={"sensor": "1", "type": "demo"})
    await decoded_r.ts().create("t", labels={"sensor": "1", "type": "demo"})
    await decoded_r.ts().create("u", labels={"sensor": "1", "type": "demo"})
    await decoded_r.ts().madd(
        [
            ("s", 100, 100),
            ("t", 100, 100),
            ("s", 200, 200),
            ("t", 300, 300),
            ("s", 400, 400),
            ("t", 400, 400),
            ("u", 2000, 2000),
        ]
    )

    # Without EXCLUDEEMPTY, "u" matches the filter but has no samples in range.
    res = await decoded_r.ts().mrange("-", 500, filters=["sensor=1"])
    assert {"s", "t", "u"} == _mrange_returned_keys(decoded_r, res)

    # With EXCLUDEEMPTY, "u" is omitted from the top-level reply.
    res = await decoded_r.ts().mrange(
        "-", 500, filters=["sensor=1"], exclude_empty=True
    )
    assert {"s", "t"} == _mrange_returned_keys(decoded_r, res)

    # Composing with WITHLABELS should not change the exclusion behavior.
    res = await decoded_r.ts().mrange(
        "-", 500, filters=["sensor=1"], with_labels=True, exclude_empty=True
    )
    assert {"s", "t"} == _mrange_returned_keys(decoded_r, res)

    # Composing with AGGREGATION should still omit the empty series.
    res = await decoded_r.ts().mrange(
        "-",
        500,
        filters=["sensor=1"],
        aggregation_type="min",
        bucket_size_msec=100,
        exclude_empty=True,
    )
    assert {"s", "t"} == _mrange_returned_keys(decoded_r, res)

    # When every matching series is empty, no series is reported (an empty
    # top-level reply; shape is [] in RESP2 and {} in RESP3).
    res = await decoded_r.ts().mrange(1, 50, filters=["sensor=1"], exclude_empty=True)
    assert 0 == len(res)


@pytest.mark.onlynoncluster
@pytest.mark.redismod
@skip_if_server_version_lt("8.9.0")
async def test_mrevrange_exclude_empty(decoded_r: redis.Redis):
    await decoded_r.ts().create("s", labels={"sensor": "1", "type": "demo"})
    await decoded_r.ts().create("t", labels={"sensor": "1", "type": "demo"})
    await decoded_r.ts().create("u", labels={"sensor": "1", "type": "demo"})
    await decoded_r.ts().madd(
        [
            ("s", 100, 100),
            ("t", 100, 100),
            ("s", 200, 200),
            ("t", 300, 300),
            ("s", 400, 400),
            ("t", 400, 400),
            ("u", 2000, 2000),
        ]
    )

    res = await decoded_r.ts().mrevrange("-", 500, filters=["sensor=1"])
    assert {"s", "t", "u"} == _mrange_returned_keys(decoded_r, res)

    res = await decoded_r.ts().mrevrange(
        "-", 500, filters=["sensor=1"], exclude_empty=True
    )
    assert {"s", "t"} == _mrange_returned_keys(decoded_r, res)

    # All matching series empty -> empty top-level reply ([] in RESP2, {} in RESP3).
    res = await decoded_r.ts().mrevrange(
        1, 50, filters=["sensor=1"], exclude_empty=True
    )
    assert 0 == len(res)


@pytest.mark.onlynoncluster
@pytest.mark.redismod
async def test_mrange_exclude_empty_with_groupby_raises(decoded_r: redis.Redis):
    # EXCLUDEEMPTY is mutually exclusive with GROUPBY. This is validated
    # client-side, so it does not require server support.
    with pytest.raises(
        redis.DataError, match="EXCLUDEEMPTY is not allowed with GROUPBY"
    ):
        await decoded_r.ts().mrange(
            "-",
            500,
            filters=["sensor=1"],
            groupby="type",
            reduce="max",
            exclude_empty=True,
        )
    with pytest.raises(
        redis.DataError, match="EXCLUDEEMPTY is not allowed with GROUPBY"
    ):
        await decoded_r.ts().mrevrange(
            "-",
            500,
            filters=["sensor=1"],
            groupby="type",
            reduce="max",
            exclude_empty=True,
        )


@pytest.mark.onlynoncluster
@pytest.mark.redismod
@skip_ifmodversion_lt("1.10.0", "timeseries")
async def test_multi_range_advanced(decoded_r: redis.Redis):
    await decoded_r.ts().create(KEY1, labels={"Test": "This", "team": "ny"})
    await decoded_r.ts().create(
        KEY2, labels={"Test": "This", "Taste": "That", "team": "sf"}
    )
    pipe = decoded_r.pipeline(transaction=False)
    for i in range(100):
        pipe.execute_command("TS.ADD", KEY1, i, i % 7)
        pipe.execute_command("TS.ADD", KEY2, i, i % 11)
    await pipe.execute()

    # test with selected labels
    res = await decoded_r.ts().mrange(
        0, 200, filters=["Test=This"], select_labels=["team"]
    )
    assert res is not None
    if expects_resp2_shape(decoded_r):
        assert {"team": "ny"} == res[0][KEY1][0]
        assert {"team": "sf"} == res[1][KEY2][0]

        # test with filterby
        res = await decoded_r.ts().mrange(
            0,
            200,
            filters=["Test=This"],
            filter_by_ts=[i for i in range(10, 20)],
            filter_by_min_value=1,
            filter_by_max_value=2,
        )
        assert res is not None
        assert [(15, 1.0), (16, 2.0)] == res[0][KEY1][1]

        # test groupby
        res = await decoded_r.ts().mrange(
            0, 3, filters=["Test=This"], groupby="Test", reduce="sum"
        )
        assert res is not None
        assert [(0, 0.0), (1, 2.0), (2, 4.0), (3, 6.0)] == res[0]["Test=This"][1]
        res = await decoded_r.ts().mrange(
            0, 3, filters=["Test=This"], groupby="Test", reduce="max"
        )
        assert res is not None
        assert [(0, 0.0), (1, 1.0), (2, 2.0), (3, 3.0)] == res[0]["Test=This"][1]
        res = await decoded_r.ts().mrange(
            0, 3, filters=["Test=This"], groupby="team", reduce="min"
        )
        assert res is not None
        assert 2 == len(res)
        assert [(0, 0.0), (1, 1.0), (2, 2.0), (3, 3.0)] == res[0]["team=ny"][1]
        assert [(0, 0.0), (1, 1.0), (2, 2.0), (3, 3.0)] == res[1]["team=sf"][1]

        # test align
        res = await decoded_r.ts().mrange(
            0,
            10,
            filters=["team=ny"],
            aggregation_type="count",
            bucket_size_msec=10,
            align="-",
        )
        assert res is not None
        assert [(0, 10.0), (10, 1.0)] == res[0][KEY1][1]
        res = await decoded_r.ts().mrange(
            0,
            10,
            filters=["team=ny"],
            aggregation_type="count",
            bucket_size_msec=10,
            align=5,
        )
        assert res is not None
        assert [(0, 5.0), (5, 6.0)] == res[0][KEY1][1]
    else:
        # ``expected_response_shape`` is ``"unified"`` for
        # ``legacy_responses=False`` (any protocol) and ``"legacy_resp3"``
        # for ``protocol=3`` with ``legacy_responses=True``. Unified parsers
        # keep samples at index 2; legacy RESP3 preserves the wire layout,
        # appending an extra ``sources`` element under GROUPBY which pushes
        # samples to index 3.
        groupby_samples_idx = (
            2 if expected_response_shape(decoded_r) == "unified" else 3
        )
        assert {"team": "ny"} == res[KEY1][0]
        assert {"team": "sf"} == res[KEY2][0]

        # test with filterby
        res = await decoded_r.ts().mrange(
            0,
            200,
            filters=["Test=This"],
            filter_by_ts=[i for i in range(10, 20)],
            filter_by_min_value=1,
            filter_by_max_value=2,
        )
        assert res is not None
        assert [[15, 1.0], [16, 2.0]] == res[KEY1][2]

        # test groupby
        res = await decoded_r.ts().mrange(
            0, 3, filters=["Test=This"], groupby="Test", reduce="sum"
        )
        assert res is not None
        assert [[0, 0.0], [1, 2.0], [2, 4.0], [3, 6.0]] == res["Test=This"][
            groupby_samples_idx
        ]
        res = await decoded_r.ts().mrange(
            0, 3, filters=["Test=This"], groupby="Test", reduce="max"
        )
        assert res is not None
        assert [[0, 0.0], [1, 1.0], [2, 2.0], [3, 3.0]] == res["Test=This"][
            groupby_samples_idx
        ]
        res = await decoded_r.ts().mrange(
            0, 3, filters=["Test=This"], groupby="team", reduce="min"
        )
        assert res is not None
        assert 2 == len(res)

        assert [[0, 0.0], [1, 1.0], [2, 2.0], [3, 3.0]] == res["team=ny"][
            groupby_samples_idx
        ]
        assert [[0, 0.0], [1, 1.0], [2, 2.0], [3, 3.0]] == res["team=sf"][
            groupby_samples_idx
        ]

        # test align
        res = await decoded_r.ts().mrange(
            0,
            10,
            filters=["team=ny"],
            aggregation_type="count",
            bucket_size_msec=10,
            align="-",
        )
        assert res is not None
        assert [[0, 10.0], [10, 1.0]] == res[KEY1][2]
        res = await decoded_r.ts().mrange(
            0,
            10,
            filters=["team=ny"],
            aggregation_type="count",
            bucket_size_msec=10,
            align=5,
        )
        assert res is not None
        assert [[0, 5.0], [5, 6.0]] == res[KEY1][2]


@pytest.mark.onlynoncluster
@pytest.mark.redismod
@skip_ifmodversion_lt("1.10.0", "timeseries")
async def test_multi_reverse_range(decoded_r: redis.Redis):
    await decoded_r.ts().create(KEY1, labels={"Test": "This", "team": "ny"})
    await decoded_r.ts().create(
        KEY2, labels={"Test": "This", "Taste": "That", "team": "sf"}
    )
    pipe = decoded_r.pipeline(transaction=False)
    for i in range(100):
        pipe.execute_command("TS.ADD", KEY1, i, i % 7)
        pipe.execute_command("TS.ADD", KEY2, i, i % 11)
    await pipe.execute()

    # ``expected_response_shape`` is ``"unified"`` for
    # ``legacy_responses=False`` (any protocol) and ``"legacy_resp3"`` for
    # ``protocol=3`` with ``legacy_responses=True``. The unified shape
    # always emits ``[labels, [], samples]`` (samples at index 2); legacy
    # RESP3 preserves the wire layout, appending an extra ``sources``
    # element under GROUPBY which pushes samples to index 3.
    groupby_samples_idx = 2 if expected_response_shape(decoded_r) == "unified" else 3

    res = await decoded_r.ts().mrange(0, 200, filters=["Test=This"])
    assert res is not None
    assert 2 == len(res)
    if expects_resp2_shape(decoded_r):
        assert 100 == len(res[0][KEY1][1])

        res = await decoded_r.ts().mrange(0, 200, filters=["Test=This"], count=10)
        assert res is not None
        assert 10 == len(res[0][KEY1][1])

        pipe = decoded_r.pipeline(transaction=False)
        for i in range(100):
            pipe.execute_command("TS.ADD", KEY1, i + 200, i % 7)
        await pipe.execute()
        res = await decoded_r.ts().mrevrange(
            0, 500, filters=["Test=This"], aggregation_type="avg", bucket_size_msec=10
        )
        assert res is not None
        assert 2 == len(res)
        assert 20 == len(res[0][KEY1][1])
        assert {} == res[0][KEY1][0]

        # test withlabels
        res = await decoded_r.ts().mrevrange(
            0, 200, filters=["Test=This"], with_labels=True
        )
        assert res is not None
        assert {"Test": "This", "team": "ny"} == res[0][KEY1][0]

        # test with selected labels
        res = await decoded_r.ts().mrevrange(
            0, 200, filters=["Test=This"], select_labels=["team"]
        )
        assert res is not None
        assert {"team": "ny"} == res[0][KEY1][0]
        assert {"team": "sf"} == res[1][KEY2][0]

        # test filterby
        res = await decoded_r.ts().mrevrange(
            0,
            200,
            filters=["Test=This"],
            filter_by_ts=[i for i in range(10, 20)],
            filter_by_min_value=1,
            filter_by_max_value=2,
        )
        assert res is not None
        assert [(16, 2.0), (15, 1.0)] == res[0][KEY1][1]

        # test groupby
        res = await decoded_r.ts().mrevrange(
            0, 3, filters=["Test=This"], groupby="Test", reduce="sum"
        )
        assert res is not None
        assert [(3, 6.0), (2, 4.0), (1, 2.0), (0, 0.0)] == res[0]["Test=This"][1]
        res = await decoded_r.ts().mrevrange(
            0, 3, filters=["Test=This"], groupby="Test", reduce="max"
        )
        assert res is not None
        assert [(3, 3.0), (2, 2.0), (1, 1.0), (0, 0.0)] == res[0]["Test=This"][1]
        res = await decoded_r.ts().mrevrange(
            0, 3, filters=["Test=This"], groupby="team", reduce="min"
        )
        assert res is not None
        assert 2 == len(res)
        assert [(3, 3.0), (2, 2.0), (1, 1.0), (0, 0.0)] == res[0]["team=ny"][1]
        assert [(3, 3.0), (2, 2.0), (1, 1.0), (0, 0.0)] == res[1]["team=sf"][1]

        # test align
        res = await decoded_r.ts().mrevrange(
            0,
            10,
            filters=["team=ny"],
            aggregation_type="count",
            bucket_size_msec=10,
            align="-",
        )
        assert res is not None
        assert [(10, 1.0), (0, 10.0)] == res[0][KEY1][1]
        res = await decoded_r.ts().mrevrange(
            0,
            10,
            filters=["team=ny"],
            aggregation_type="count",
            bucket_size_msec=10,
            align=1,
        )
        assert res is not None
        assert [(1, 10.0), (0, 1.0)] == res[0][KEY1][1]
    else:
        assert 100 == len(res[KEY1][2])

        res = await decoded_r.ts().mrange(0, 200, filters=["Test=This"], count=10)
        assert res is not None
        assert 10 == len(res[KEY1][2])

        pipe = decoded_r.pipeline(transaction=False)
        for i in range(100):
            pipe.execute_command("TS.ADD", KEY1, i + 200, i % 7)
        await pipe.execute()
        res = await decoded_r.ts().mrevrange(
            0, 500, filters=["Test=This"], aggregation_type="avg", bucket_size_msec=10
        )
        assert res is not None
        assert 2 == len(res)
        assert 20 == len(res[KEY1][2])
        assert {} == res[KEY1][0]

        # test withlabels
        res = await decoded_r.ts().mrevrange(
            0, 200, filters=["Test=This"], with_labels=True
        )
        assert res is not None
        assert {"Test": "This", "team": "ny"} == res[KEY1][0]

        # test with selected labels
        res = await decoded_r.ts().mrevrange(
            0, 200, filters=["Test=This"], select_labels=["team"]
        )
        assert res is not None
        assert {"team": "ny"} == res[KEY1][0]
        assert {"team": "sf"} == res[KEY2][0]

        # test filterby
        res = await decoded_r.ts().mrevrange(
            0,
            200,
            filters=["Test=This"],
            filter_by_ts=[i for i in range(10, 20)],
            filter_by_min_value=1,
            filter_by_max_value=2,
        )
        assert res is not None
        assert [[16, 2.0], [15, 1.0]] == res[KEY1][2]

        # test groupby
        res = await decoded_r.ts().mrevrange(
            0, 3, filters=["Test=This"], groupby="Test", reduce="sum"
        )
        assert res is not None
        assert [[3, 6.0], [2, 4.0], [1, 2.0], [0, 0.0]] == res["Test=This"][
            groupby_samples_idx
        ]
        res = await decoded_r.ts().mrevrange(
            0, 3, filters=["Test=This"], groupby="Test", reduce="max"
        )
        assert res is not None
        assert [[3, 3.0], [2, 2.0], [1, 1.0], [0, 0.0]] == res["Test=This"][
            groupby_samples_idx
        ]
        res = await decoded_r.ts().mrevrange(
            0, 3, filters=["Test=This"], groupby="team", reduce="min"
        )
        assert res is not None
        assert 2 == len(res)
        assert [[3, 3.0], [2, 2.0], [1, 1.0], [0, 0.0]] == res["team=ny"][
            groupby_samples_idx
        ]
        assert [[3, 3.0], [2, 2.0], [1, 1.0], [0, 0.0]] == res["team=sf"][
            groupby_samples_idx
        ]

        # test align
        res = await decoded_r.ts().mrevrange(
            0,
            10,
            filters=["team=ny"],
            aggregation_type="count",
            bucket_size_msec=10,
            align="-",
        )
        assert res is not None
        assert [[10, 1.0], [0, 10.0]] == res[KEY1][2]
        res = await decoded_r.ts().mrevrange(
            0,
            10,
            filters=["team=ny"],
            aggregation_type="count",
            bucket_size_msec=10,
            align=1,
        )
        assert res is not None
        assert [[1, 10.0], [0, 1.0]] == res[KEY1][2]


@pytest.mark.redismod
async def test_get(decoded_r: redis.Redis):
    name = "test"
    await decoded_r.ts().create(name)
    assert not await decoded_r.ts().get(name)
    await decoded_r.ts().add(name, 2, 3)
    assert 2 == (await decoded_r.ts().get(name))[0]
    await decoded_r.ts().add(name, 3, 4)
    assert 4 == (await decoded_r.ts().get(name))[1]


@pytest.mark.onlynoncluster
@pytest.mark.redismod
async def test_mget(decoded_r: redis.Redis):
    await decoded_r.ts().create(KEY1, labels={"Test": "This"})
    await decoded_r.ts().create(KEY2, labels={"Test": "This", "Taste": "That"})
    act_res = await decoded_r.ts().mget(["Test=This"])
    exp_res = [{KEY1: [{}, None, None]}, {KEY2: [{}, None, None]}]
    exp_res_resp3 = {KEY1: [{}, []], KEY2: [{}, []]}
    assert_resp_response(decoded_r, act_res, exp_res, exp_res_resp3)
    await decoded_r.ts().add(KEY1, "*", 15)
    await decoded_r.ts().add(KEY2, "*", 25)
    res = await decoded_r.ts().mget(["Test=This"])
    assert res is not None
    if expects_resp2_shape(decoded_r):
        assert 15 == res[0][KEY1][2]
        assert 25 == res[1][KEY2][2]
    else:
        assert 15 == res[KEY1][1][1]
        assert 25 == res[KEY2][1][1]
    res = await decoded_r.ts().mget(["Taste=That"])
    assert res is not None
    if expects_resp2_shape(decoded_r):
        assert 25 == res[0][KEY2][2]
    else:
        assert 25 == res[KEY2][1][1]

    # test with_labels
    if expects_resp2_shape(decoded_r):
        assert {} == res[0][KEY2][0]
    else:
        assert {} == res[KEY2][0]
    res = await decoded_r.ts().mget(["Taste=That"], with_labels=True)
    assert res is not None
    if expects_resp2_shape(decoded_r):
        assert {"Taste": "That", "Test": "This"} == res[0][KEY2][0]
    else:
        assert {"Taste": "That", "Test": "This"} == res[KEY2][0]


@pytest.mark.redismod
async def test_info(decoded_r: redis.Redis):
    await decoded_r.ts().create(
        KEY1, retention_msecs=5, labels={"currentLabel": "currentData"}
    )
    info = await decoded_r.ts().info(KEY1)
    assert_resp_response(
        decoded_r, 5, info.get("retention_msecs"), info.get("retentionTime")
    )
    assert info["labels"]["currentLabel"] == "currentData"


@pytest.mark.redismod
@pytest.mark.fixed_client
@pytest.mark.parametrize(
    "protocol", [pytest.param(2, id="resp2"), pytest.param(SENTINEL, id="default")]
)
async def test_info_without_decode_responses(create_redis, stack_url, protocol):
    kwargs = {"decode_responses": False, "url": stack_url}
    if protocol is not SENTINEL:
        kwargs["protocol"] = protocol
    r = await create_redis(**kwargs)
    await r.ts().create(KEY1, retention_msecs=5, labels={"currentLabel": "currentData"})
    await r.ts().add(KEY1, 1, 1.5)
    info = await r.ts().info(KEY1)
    assert info.total_samples == 1
    assert info.retention_msecs == 5
    assert info.labels == {"currentLabel": "currentData"}


@pytest.mark.redismod
@skip_ifmodversion_lt("1.4.0", "timeseries")
@skip_if_server_version_lt("7.9.0")
async def test_info_duplicate_policy(decoded_r: redis.Redis):
    await decoded_r.ts().create(
        KEY1, retention_msecs=5, labels={"currentLabel": "currentData"}
    )
    info = await decoded_r.ts().info(KEY1)
    assert_resp_response(
        decoded_r, "block", info.get("duplicate_policy"), info.get("duplicatePolicy")
    )

    await decoded_r.ts().create("time-serie-2", duplicate_policy="min")
    info = await decoded_r.ts().info("time-serie-2")
    assert_resp_response(
        decoded_r, "min", info.get("duplicate_policy"), info.get("duplicatePolicy")
    )


@pytest.mark.redismod
@skip_ifmodversion_lt("1.4.0", "timeseries")
@skip_if_server_version_gte("7.9.0")
async def test_info_duplicate_policy_prior_redis_8(decoded_r: redis.Redis):
    await decoded_r.ts().create(
        KEY1, retention_msecs=5, labels={"currentLabel": "currentData"}
    )
    info = await decoded_r.ts().info(KEY1)
    assert_resp_response(
        decoded_r, None, info.get("duplicate_policy"), info.get("duplicatePolicy")
    )

    await decoded_r.ts().create("time-serie-2", duplicate_policy="min")
    info = await decoded_r.ts().info("time-serie-2")
    assert_resp_response(
        decoded_r, "min", info.get("duplicate_policy"), info.get("duplicatePolicy")
    )


@pytest.mark.onlynoncluster
@pytest.mark.redismod
async def test_query_index(decoded_r: redis.Redis):
    await decoded_r.ts().create(KEY1, labels={"Test": "This"})
    await decoded_r.ts().create(KEY2, labels={"Test": "This", "Taste": "That"})
    assert 2 == len(await decoded_r.ts().queryindex(["Test=This"]))
    assert 1 == len(await decoded_r.ts().queryindex(["Taste=That"]))
    assert_resp_response(
        decoded_r,
        await decoded_r.ts().queryindex(["Taste=That"]),
        [KEY2],
        [KEY2],
    )


@pytest.mark.onlynoncluster
@pytest.mark.redismod
@skip_if_server_version_lt("8.9.0")
async def test_query_labels(decoded_r: redis.Redis):
    await decoded_r.ts().create(
        1, labels={"type": "sensor", "location": "LivingRoom", "sensortype": "temp"}
    )
    await decoded_r.ts().create(
        2, labels={"type": "sensor", "location": "Kitchen", "sensortype": "temp"}
    )
    await decoded_r.ts().create(3, labels={"type": "gauge", "location": "BedRoom"})

    # LABELS mode with a filter returns the union of label names across the
    # matching series, including the label used in the filter itself.
    labels = await decoded_r.ts().querylabels(filters=["type=sensor"])
    assert isinstance(labels, set)
    assert sorted(labels) == ["location", "sensortype", "type"]

    # Omitting the filter queries all indexed series.
    assert sorted(await decoded_r.ts().querylabels()) == [
        "location",
        "sensortype",
        "type",
    ]

    # A filter that matches nothing is a normal empty reply, not an error.
    assert await decoded_r.ts().querylabels(filters=["type=missing"]) == set()

    # `filters` accepts any iterable, not just a list (a tuple and a single-pass
    # generator both work).
    assert sorted(await decoded_r.ts().querylabels(filters=("type=sensor",))) == [
        "location",
        "sensortype",
        "type",
    ]
    assert sorted(
        await decoded_r.ts().querylabels(filters=(f for f in ["type=sensor"]))
    ) == ["location", "sensortype", "type"]


@pytest.mark.onlynoncluster
@pytest.mark.redismod
@skip_if_server_version_lt("8.9.0")
async def test_query_label_values(decoded_r: redis.Redis):
    await decoded_r.ts().create(1, labels={"type": "sensor", "location": "LivingRoom"})
    await decoded_r.ts().create(2, labels={"type": "sensor", "location": "Kitchen"})
    await decoded_r.ts().create(3, labels={"type": "gauge", "location": "BedRoom"})

    # VALUES mode returns the deduplicated union of a label's values.
    values = await decoded_r.ts().querylabels("location", filters=["type=sensor"])
    assert isinstance(values, set)
    assert sorted(values) == ["Kitchen", "LivingRoom"]

    # Omitting the filter collects values across all indexed series.
    assert sorted(await decoded_r.ts().querylabels("location")) == [
        "BedRoom",
        "Kitchen",
        "LivingRoom",
    ]

    # A label carried by no matching series yields an empty reply.
    assert (
        await decoded_r.ts().querylabels("nonexistent", filters=["type=sensor"])
        == set()
    )

    # Values are byte-exact strings and are never coerced to numbers.
    await decoded_r.ts().create(4, labels={"type": "sensor", "code": "123"})
    assert await decoded_r.ts().querylabels("code", filters=["type=sensor"]) == {"123"}


@pytest.mark.onlynoncluster
@pytest.mark.redismod
@skip_if_server_version_lt("8.9.0")
async def test_query_labels_empty_filters_raises(decoded_r: redis.Redis):
    # An explicitly empty filter collection is a local usage error; pass None
    # (omit the argument) to query all indexed series instead.
    with pytest.raises(redis.DataError):
        await decoded_r.ts().querylabels(filters=[])
    with pytest.raises(redis.DataError):
        await decoded_r.ts().querylabels("location", filters=[])


@pytest.mark.onlynoncluster
@pytest.mark.redismod
@skip_if_server_version_lt("8.9.0")
async def test_query_labels_server_errors(decoded_r: redis.Redis):
    # Server-side filter parsing errors surface unchanged as ResponseError.
    with pytest.raises(redis.ResponseError):
        await decoded_r.ts().querylabels("location", filters=["badexpr"])


@pytest.mark.redismod
async def test_uncompressed(decoded_r: redis.Redis):
    await decoded_r.ts().create("compressed")
    await decoded_r.ts().create("uncompressed", uncompressed=True)
    # Append the samples with a single TS.MADD instead of 2000 individual TS.ADD
    # round trips, which are slow enough to hit the test timeout when the target
    # deployment is remote.
    samples = []
    for i in range(1000):
        samples.append(("compressed", i, i))
        samples.append(("uncompressed", i, i))
    assert len(await decoded_r.ts().madd(samples)) == 2000
    compressed_info = await decoded_r.ts().info("compressed")
    uncompressed_info = await decoded_r.ts().info("uncompressed")
    if is_resp2_connection(decoded_r):
        assert compressed_info.memory_usage < uncompressed_info.memory_usage
    else:
        assert compressed_info["memoryUsage"] < uncompressed_info["memoryUsage"]


@pytest.mark.redismod
@skip_ifmodversion_lt("1.12.0", "timeseries")
async def test_create_with_insertion_filters(decoded_r: redis.Redis):
    await decoded_r.ts().create(
        "time-series-1",
        duplicate_policy="last",
        ignore_max_time_diff=5,
        ignore_max_val_diff=10.0,
    )
    assert 1000 == await decoded_r.ts().add("time-series-1", 1000, 1.0)
    assert 1010 == await decoded_r.ts().add("time-series-1", 1010, 11.0)
    assert 1010 == await decoded_r.ts().add("time-series-1", 1013, 10.0)
    assert 1020 == await decoded_r.ts().add("time-series-1", 1020, 11.5)
    assert 1021 == await decoded_r.ts().add("time-series-1", 1021, 22.0)

    data_points = await decoded_r.ts().range("time-series-1", "-", "+")
    assert_resp_response(
        decoded_r,
        data_points,
        [(1000, 1.0), (1010, 11.0), (1020, 11.5), (1021, 22.0)],
        [[1000, 1.0], [1010, 11.0], [1020, 11.5], [1021, 22.0]],
    )


@pytest.mark.redismod
@skip_ifmodversion_lt("1.12.0", "timeseries")
async def test_alter_with_insertion_filters(decoded_r: redis.Redis):
    assert 1000 == await decoded_r.ts().add("time-series-1", 1000, 1.0)
    assert 1010 == await decoded_r.ts().add("time-series-1", 1010, 11.0)
    assert 1013 == await decoded_r.ts().add("time-series-1", 1013, 10.0)

    await decoded_r.ts().alter(
        "time-series-1",
        duplicate_policy="last",
        ignore_max_time_diff=5,
        ignore_max_val_diff=10.0,
    )

    assert 1013 == await decoded_r.ts().add("time-series-1", 1015, 11.5)

    data_points = await decoded_r.ts().range("time-series-1", "-", "+")
    assert_resp_response(
        decoded_r,
        data_points,
        [(1000, 1.0), (1010, 11.0), (1013, 10.0)],
        [[1000, 1.0], [1010, 11.0], [1013, 10.0]],
    )


@pytest.mark.redismod
@skip_ifmodversion_lt("1.12.0", "timeseries")
async def test_add_with_insertion_filters(decoded_r: redis.Redis):
    assert 1000 == await decoded_r.ts().add(
        "time-series-1",
        1000,
        1.0,
        duplicate_policy="last",
        ignore_max_time_diff=5,
        ignore_max_val_diff=10.0,
    )

    assert 1000 == await decoded_r.ts().add("time-series-1", 1004, 3.0)

    data_points = await decoded_r.ts().range("time-series-1", "-", "+")
    assert_resp_response(decoded_r, data_points, [(1000, 1.0)], [[1000, 1.0]])


@pytest.mark.redismod
@skip_ifmodversion_lt("1.12.0", "timeseries")
async def test_incrby_with_insertion_filters(decoded_r: redis.Redis):
    assert 1000 == await decoded_r.ts().incrby(
        "time-series-1",
        1.0,
        timestamp=1000,
        duplicate_policy="last",
        ignore_max_time_diff=5,
        ignore_max_val_diff=10.0,
    )

    assert 1000 == await decoded_r.ts().incrby("time-series-1", 3.0, timestamp=1000)

    data_points = await decoded_r.ts().range("time-series-1", "-", "+")
    assert_resp_response(decoded_r, data_points, [(1000, 1.0)], [[1000, 1.0]])

    assert 1000 == await decoded_r.ts().incrby("time-series-1", 10.1, timestamp=1000)

    data_points = await decoded_r.ts().range("time-series-1", "-", "+")
    assert_resp_response(decoded_r, data_points, [(1000, 11.1)], [[1000, 11.1]])


@pytest.mark.redismod
@skip_ifmodversion_lt("1.12.0", "timeseries")
async def test_decrby_with_insertion_filters(decoded_r: redis.Redis):
    assert 1000 == await decoded_r.ts().decrby(
        "time-series-1",
        1.0,
        timestamp=1000,
        duplicate_policy="last",
        ignore_max_time_diff=5,
        ignore_max_val_diff=10.0,
    )

    assert 1000 == await decoded_r.ts().decrby("time-series-1", 3.0, timestamp=1000)

    data_points = await decoded_r.ts().range("time-series-1", "-", "+")
    assert_resp_response(decoded_r, data_points, [(1000, -1.0)], [[1000, -1.0]])

    assert 1000 == await decoded_r.ts().decrby("time-series-1", 10.1, timestamp=1000)

    data_points = await decoded_r.ts().range("time-series-1", "-", "+")
    assert_resp_response(decoded_r, data_points, [(1000, -11.1)], [[1000, -11.1]])


@pytest.mark.redismod
@skip_ifmodversion_lt("1.12.0", "timeseries")
async def test_labels_with_insertion_filters_and_on_duplicate(decoded_r: redis.Redis):
    ts = decoded_r.ts()
    filters = {
        "duplicate_policy": "last",
        "ignore_max_time_diff": 5,
        "ignore_max_val_diff": 10.0,
    }
    labels = {"sensor": "1"}

    assert await ts.create("create", labels=labels, **filters)
    assert labels == (await ts.info("create"))["labels"]
    assert 1000 == await ts.add("create", 1000, 1.0)
    assert 1000 == await ts.add("create", 1004, 3.0)

    assert await ts.alter("create", labels={}, **filters)
    assert {} == (await ts.info("create"))["labels"]
    assert 1000 == await ts.add("create", 1003, 2.0)

    assert 1000 == await ts.add("add", 1000, 1.0, labels=labels, **filters)
    assert labels == (await ts.info("add"))["labels"]
    assert 1000 == await ts.add("add", 1004, 3.0)

    assert 1000 == await ts.add("add-dup", 1000, 1.0)
    assert 1000 == await ts.add(
        "add-dup", 1000, 2.0, labels=labels, on_duplicate="last"
    )
    assert {} == (await ts.info("add-dup"))["labels"]

    assert 1000 == await ts.add(
        "add-new", 1000, 1.0, labels=labels, on_duplicate="last"
    )
    assert labels == (await ts.info("add-new"))["labels"]

    assert 1000 == await ts.incrby(
        "incrby", 1.0, timestamp=1000, labels=labels, **filters
    )
    assert labels == (await ts.info("incrby"))["labels"]

    assert 1000 == await ts.decrby(
        "decrby", 1.0, timestamp=1000, labels=labels, **filters
    )
    assert labels == (await ts.info("decrby"))["labels"]


@pytest.mark.redismod
@skip_if_server_version_lt("8.5.0")
async def test_range_with_count_nan_count_all_aggregators(decoded_r: redis.Redis):
    await decoded_r.ts().create(
        "temperature:2:32",
    )

    # Fill with values
    assert await decoded_r.ts().add("temperature:2:32", 1000, "NaN") == 1000
    assert await decoded_r.ts().add("temperature:2:32", 1003, 25) == 1003
    assert await decoded_r.ts().add("temperature:2:32", 1005, "NaN") == 1005
    assert await decoded_r.ts().add("temperature:2:32", 1006, "NaN") == 1006

    # Ensure we count only NaN values
    data_points = await decoded_r.ts().range(
        "temperature:2:32",
        1000,
        1006,
        aggregation_type="countNan",
        bucket_size_msec=1000,
    )
    assert_resp_response(
        decoded_r,
        data_points,
        [(1000, 3)],
        [[1000, 3]],
    )

    # Ensure we count ALL values
    data_points = await decoded_r.ts().range(
        "temperature:2:32",
        1000,
        1006,
        aggregation_type="countAll",
        bucket_size_msec=1000,
    )
    assert_resp_response(
        decoded_r,
        data_points,
        [(1000, 4)],
        [[1000, 4]],
    )


@pytest.mark.redismod
@skip_if_server_version_lt("8.5.0")
async def test_rev_range_with_count_nan_count_all_aggregators(decoded_r: redis.Redis):
    await decoded_r.ts().create(
        "temperature:2:32",
    )

    # Fill with values
    assert await decoded_r.ts().add("temperature:2:32", 1000, "NaN") == 1000
    assert await decoded_r.ts().add("temperature:2:32", 1003, 25) == 1003
    assert await decoded_r.ts().add("temperature:2:32", 1005, "NaN") == 1005
    assert await decoded_r.ts().add("temperature:2:32", 1006, "NaN") == 1006

    # Ensure we count only NaN values
    data_points = await decoded_r.ts().revrange(
        "temperature:2:32",
        1000,
        1006,
        aggregation_type="countNan",
        bucket_size_msec=1000,
    )
    assert_resp_response(
        decoded_r,
        data_points,
        [(1000, 3)],
        [[1000, 3]],
    )

    # Ensure we count ALL values
    data_points = await decoded_r.ts().revrange(
        "temperature:2:32",
        1000,
        1006,
        aggregation_type="countAll",
        bucket_size_msec=1000,
    )
    assert_resp_response(
        decoded_r,
        data_points,
        [(1000, 4)],
        [[1000, 4]],
    )


@pytest.mark.redismod
@skip_if_server_version_lt("8.5.0")
async def test_mrange_with_count_nan_count_all_aggregators(decoded_r: redis.Redis):
    await decoded_r.ts().create(
        "temperature:A",
        labels={"type": "temperature", "name": "A"},
    )
    await decoded_r.ts().create(
        "temperature:B",
        labels={"type": "temperature", "name": "B"},
    )

    # Fill with values
    assert await decoded_r.ts().madd(
        [("temperature:A", 1000, "NaN"), ("temperature:A", 1001, 27)]
    )
    assert await decoded_r.ts().madd(
        [("temperature:B", 1000, "NaN"), ("temperature:B", 1001, 28)]
    )

    # Ensure we count only NaN values
    data_points = await decoded_r.ts().mrange(
        1000,
        1001,
        aggregation_type="countNan",
        bucket_size_msec=1000,
        filters=["type=temperature"],
    )
    assert_resp_response(
        decoded_r,
        data_points,
        [
            {"temperature:A": [{}, [(1000, 1.0)]]},
            {"temperature:B": [{}, [(1000, 1.0)]]},
        ],
        {
            "temperature:A": [{}, {"aggregators": ["countnan"]}, [[1000, 1.0]]],
            "temperature:B": [{}, {"aggregators": ["countnan"]}, [[1000, 1.0]]],
        },
        {
            "temperature:A": [
                {},
                {"aggregators": ["countnan"]},
                [[1000, 1.0]],
            ],
            "temperature:B": [
                {},
                {"aggregators": ["countnan"]},
                [[1000, 1.0]],
            ],
        },
    )

    # Ensure we count ALL values
    data_points = await decoded_r.ts().mrange(
        1000,
        1001,
        aggregation_type="countAll",
        bucket_size_msec=1000,
        filters=["type=temperature"],
    )
    assert_resp_response(
        decoded_r,
        data_points,
        [
            {"temperature:A": [{}, [(1000, 2.0)]]},
            {"temperature:B": [{}, [(1000, 2.0)]]},
        ],
        {
            "temperature:A": [{}, {"aggregators": ["countall"]}, [[1000, 2.0]]],
            "temperature:B": [{}, {"aggregators": ["countall"]}, [[1000, 2.0]]],
        },
        {
            "temperature:A": [
                {},
                {"aggregators": ["countall"]},
                [[1000, 2.0]],
            ],
            "temperature:B": [
                {},
                {"aggregators": ["countall"]},
                [[1000, 2.0]],
            ],
        },
    )


@pytest.mark.redismod
@skip_if_server_version_lt("8.5.0")
async def test_mrevrange_with_count_nan_count_all_aggregators(decoded_r: redis.Redis):
    await decoded_r.ts().create(
        "temperature:A",
        labels={"type": "temperature", "name": "A"},
    )
    await decoded_r.ts().create(
        "temperature:B",
        labels={"type": "temperature", "name": "B"},
    )

    # Fill with values
    assert await decoded_r.ts().madd(
        [("temperature:A", 1000, "NaN"), ("temperature:A", 1001, 27)]
    )
    assert await decoded_r.ts().madd(
        [("temperature:B", 1000, "NaN"), ("temperature:B", 1001, 28)]
    )

    # Ensure we count only NaN values
    data_points = await decoded_r.ts().mrevrange(
        1000,
        1001,
        aggregation_type="countNan",
        bucket_size_msec=1000,
        filters=["type=temperature"],
    )
    assert_resp_response(
        decoded_r,
        data_points,
        [
            {"temperature:A": [{}, [(1000, 1.0)]]},
            {"temperature:B": [{}, [(1000, 1.0)]]},
        ],
        {
            "temperature:A": [{}, {"aggregators": ["countnan"]}, [[1000, 1.0]]],
            "temperature:B": [{}, {"aggregators": ["countnan"]}, [[1000, 1.0]]],
        },
        {
            "temperature:A": [
                {},
                {"aggregators": ["countnan"]},
                [[1000, 1.0]],
            ],
            "temperature:B": [
                {},
                {"aggregators": ["countnan"]},
                [[1000, 1.0]],
            ],
        },
    )

    # Ensure we count ALL values
    data_points = await decoded_r.ts().mrevrange(
        1000,
        1001,
        aggregation_type="countAll",
        bucket_size_msec=1000,
        filters=["type=temperature"],
    )
    assert_resp_response(
        decoded_r,
        data_points,
        [
            {"temperature:A": [{}, [(1000, 2.0)]]},
            {"temperature:B": [{}, [(1000, 2.0)]]},
        ],
        {
            "temperature:A": [{}, {"aggregators": ["countall"]}, [[1000, 2.0]]],
            "temperature:B": [{}, {"aggregators": ["countall"]}, [[1000, 2.0]]],
        },
        {
            "temperature:A": [
                {},
                {"aggregators": ["countall"]},
                [[1000, 2.0]],
            ],
            "temperature:B": [
                {},
                {"aggregators": ["countall"]},
                [[1000, 2.0]],
            ],
        },
    )


@pytest.mark.redismod
@skip_if_server_version_lt("8.8.0")
async def test_range_multiple_aggregators(decoded_r: redis.Redis):
    """Test TS.RANGE with multiple aggregators (Redis 8.8+)."""
    await decoded_r.ts().create("ts:multi_agg")
    await decoded_r.ts().add("ts:multi_agg", 1000, 10)
    await decoded_r.ts().add("ts:multi_agg", 1001, 20)
    await decoded_r.ts().add("ts:multi_agg", 1002, 30)

    result = await decoded_r.ts().range(
        "ts:multi_agg",
        1000,
        1002,
        aggregation_type=["min", "max", "avg"],
        bucket_size_msec=10,
    )
    assert len(result) == 1
    assert result[0][0] == 1000  # timestamp
    assert result[0][1] == 10.0  # min
    assert result[0][2] == 30.0  # max
    assert result[0][3] == 20.0  # avg


@pytest.mark.redismod
@skip_if_server_version_lt("8.8.0")
async def test_revrange_multiple_aggregators(decoded_r: redis.Redis):
    """Test TS.REVRANGE with multiple aggregators (Redis 8.8+)."""
    await decoded_r.ts().create("ts:multi_agg")
    await decoded_r.ts().add("ts:multi_agg", 1000, 10)
    await decoded_r.ts().add("ts:multi_agg", 1001, 20)
    await decoded_r.ts().add("ts:multi_agg", 1002, 30)

    result = await decoded_r.ts().revrange(
        "ts:multi_agg",
        1000,
        1002,
        aggregation_type=["min", "max", "avg"],
        bucket_size_msec=10,
    )
    assert len(result) == 1
    assert result[0][0] == 1000  # timestamp
    assert result[0][1] == 10.0  # min
    assert result[0][2] == 30.0  # max
    assert result[0][3] == 20.0  # avg


@pytest.mark.redismod
@skip_if_server_version_lt("8.8.0")
async def test_mrange_multiple_aggregators(decoded_r: redis.Redis):
    """Test TS.MRANGE with multiple aggregators (Redis 8.8+)."""
    await decoded_r.ts().create("ts:multi_agg_a", labels={"type": "test_multi_agg"})
    await decoded_r.ts().add("ts:multi_agg_a", 1000, 10)
    await decoded_r.ts().add("ts:multi_agg_a", 1001, 20)

    result = await decoded_r.ts().mrange(
        1000,
        1001,
        filters=["type=test_multi_agg"],
        aggregation_type=["min", "max"],
        bucket_size_msec=10,
    )
    if expects_resp2_shape(decoded_r):
        assert len(result) == 1
        assert "ts:multi_agg_a" in result[0]
        samples = result[0]["ts:multi_agg_a"][1]
    else:
        assert "ts:multi_agg_a" in result
        samples = result["ts:multi_agg_a"][2]
    assert len(samples) == 1
    assert samples[0][0] == 1000  # timestamp
    assert samples[0][1] == 10.0  # min
    assert samples[0][2] == 20.0  # max


@pytest.mark.redismod
@skip_if_server_version_lt("8.8.0")
async def test_mrevrange_multiple_aggregators(decoded_r: redis.Redis):
    """Test TS.MREVRANGE with multiple aggregators (Redis 8.8+)."""
    await decoded_r.ts().create("ts:multi_agg_b", labels={"type": "test_multi_agg_rev"})
    await decoded_r.ts().add("ts:multi_agg_b", 1000, 10)
    await decoded_r.ts().add("ts:multi_agg_b", 1001, 20)

    result = await decoded_r.ts().mrevrange(
        1000,
        1001,
        filters=["type=test_multi_agg_rev"],
        aggregation_type=["min", "max"],
        bucket_size_msec=10,
    )
    if expects_resp2_shape(decoded_r):
        assert len(result) == 1
        assert "ts:multi_agg_b" in result[0]
        samples = result[0]["ts:multi_agg_b"][1]
    else:
        assert "ts:multi_agg_b" in result
        samples = result["ts:multi_agg_b"][2]
    assert len(samples) == 1
    assert samples[0][0] == 1000  # timestamp
    assert samples[0][1] == 10.0  # min
    assert samples[0][2] == 20.0  # max


@pytest.mark.redismod
async def test_mrange_groupby_multiple_aggregators_raises(decoded_r: redis.Redis):
    """Test that GROUPBY with multiple aggregators raises DataError."""
    await decoded_r.ts().create("ts:gb_test", labels={"type": "test_gb"})

    with pytest.raises(redis.DataError, match="GROUPBY is not allowed"):
        await decoded_r.ts().mrange(
            0,
            100,
            filters=["type=test_gb"],
            aggregation_type=["min", "max"],
            bucket_size_msec=10,
            groupby="type",
            reduce="max",
        )


@pytest.mark.redismod
async def test_mrevrange_groupby_multiple_aggregators_raises(decoded_r: redis.Redis):
    """Test that GROUPBY with multiple aggregators raises DataError."""
    await decoded_r.ts().create("ts:gb_test2", labels={"type": "test_gb2"})

    with pytest.raises(redis.DataError, match="GROUPBY is not allowed"):
        await decoded_r.ts().mrevrange(
            0,
            100,
            filters=["type=test_gb2"],
            aggregation_type=["min", "max"],
            bucket_size_msec=10,
            groupby="type",
            reduce="max",
        )


@pytest.mark.redismod
@skip_if_server_version_lt("8.9.0")
async def test_nrange_raw_rows_and_missing_cells(decoded_r: redis.Redis):
    nan = float("nan")
    await decoded_r.ts().add("{s}:a", 10, 1.0)
    await decoded_r.ts().add("{s}:a", 20, 2.0)
    await decoded_r.ts().add("{s}:b", 20, 3.0)
    await decoded_r.ts().add("{s}:b", 30, 4.0)

    # Forward: one row per distinct timestamp, values follow key order,
    # missing cells are NaN.
    res = await decoded_r.ts().nrange(["{s}:a", "{s}:b"], from_time="-", to_time="+")
    _assert_nrange_rows(res, [[10, [1.0, nan]], [20, [2.0, 3.0]], [30, [nan, 4.0]]])
    assert all(len(row[1]) == 2 for row in res)


@pytest.mark.redismod
@skip_if_server_version_lt("8.9.0")
async def test_nrange_preserves_duplicate_keys(decoded_r: redis.Redis):
    await decoded_r.ts().add("{s}:a", 10, 1.0)
    await decoded_r.ts().add("{s}:a", 20, 2.0)

    # Duplicate keys produce repeated value columns and are not deduplicated.
    res = await decoded_r.ts().nrange(["{s}:a", "{s}:a"], from_time="-", to_time="+")
    _assert_nrange_rows(res, [[10, [1.0, 1.0]], [20, [2.0, 2.0]]])


@pytest.mark.redismod
@skip_if_server_version_lt("8.9.0")
async def test_nrange_aggregation_one_per_key(decoded_r: redis.Redis):
    for ts, val in [(0, 1.0), (1, 2.0), (10, 3.0), (11, 4.0)]:
        await decoded_r.ts().add("{s}:a", ts, val)
    for ts, val in [(0, 5.0), (1, 6.0), (10, 7.0), (11, 8.0)]:
        await decoded_r.ts().add("{s}:b", ts, val)

    # One aggregator per key: max for {s}:a, min for {s}:b.
    res = await decoded_r.ts().nrange(
        ["{s}:a", "{s}:b"],
        from_time=0,
        to_time=20,
        aggregators=["max", "min"],
        bucket_size_msec=10,
    )
    _assert_nrange_rows(res, [[0, [2.0, 5.0]], [10, [4.0, 7.0]]])


@pytest.mark.redismod
@skip_if_server_version_lt("8.9.0")
async def test_nrange_multiple_aggregators_per_key_layout(decoded_r: redis.Redis):
    # A per-key spec may list several aggregators. That key then contributes one
    # value per aggregator (in spec order), and each row's value array is the
    # per-key blocks concatenated in key order: here [a_avg, a_max, b_sum].
    # key a: bucket [0,10) avg=2 max=3; bucket [10,20) avg=15 max=20
    for ts, val in [(0, 1.0), (5, 3.0), (10, 10.0), (15, 20.0)]:
        await decoded_r.ts().add("{s}:a", ts, val)
    # key b: bucket [0,10) sum=10; bucket [10,20) sum=7
    for ts, val in [(0, 5.0), (5, 5.0), (10, 7.0)]:
        await decoded_r.ts().add("{s}:b", ts, val)

    res = await decoded_r.ts().nrange(
        ["{s}:a", "{s}:b"],
        from_time=0,
        to_time=20,
        aggregators=["avg,max", "sum"],
        bucket_size_msec=10,
    )
    _assert_nrange_rows(res, [[0, [2.0, 3.0, 10.0]], [10, [15.0, 20.0, 7.0]]])
    # 2 columns for key a (avg, max) + 1 for key b (sum).
    assert all(len(row[1]) == 3 for row in res)


@pytest.mark.redismod
@skip_if_server_version_lt("8.9.0")
async def test_nrange_count_limits_rows(decoded_r: redis.Redis):
    for ts in range(5):
        await decoded_r.ts().add("{s}:a", ts, ts)
    res = await decoded_r.ts().nrange(["{s}:a"], from_time="-", to_time="+", count=2)
    assert len(res) == 2


@pytest.mark.redismod
@skip_if_server_version_lt("8.9.0")
async def test_nrange_empty_result(decoded_r: redis.Redis):
    await decoded_r.ts().create("{s}:a")
    assert await decoded_r.ts().nrange(["{s}:a"], from_time="-", to_time="+") == []


@pytest.mark.redismod
@skip_if_server_version_lt("8.9.0")
async def test_nrange_single_aggregator_multi_key_raises(decoded_r: redis.Redis):
    # A single aggregator is NOT broadcast across keys (RedisTimeSeries PR
    # #2079): with more than one key, each needs its own spec token. Raised
    # client-side before any server round trip.
    with pytest.raises(redis.DataError, match="one aggregation spec per key"):
        await decoded_r.ts().nrange(
            ["{s}:a", "{s}:b"],
            from_time=0,
            to_time=20,
            aggregators="max",
            bucket_size_msec=10,
        )


@pytest.mark.redismod
@skip_if_server_version_lt("8.9.0")
async def test_nrange_aggregator_count_mismatch_raises(decoded_r: redis.Redis):
    # A spec list whose length differs from the key count is invalid.
    with pytest.raises(redis.DataError, match="one aggregation spec per key"):
        await decoded_r.ts().nrange(
            ["{s}:a", "{s}:b"],
            from_time=0,
            to_time=1,
            aggregators=["min"],
            bucket_size_msec=10,
        )


@pytest.mark.redismod
@skip_if_server_version_lt("8.9.0")
async def test_nrange_empty_keys_raises(decoded_r: redis.Redis):
    with pytest.raises(redis.DataError, match="At least one key"):
        await decoded_r.ts().nrange([], from_time=0, to_time=1)


@pytest.mark.redismod
@skip_if_server_version_lt("8.9.0")
async def test_nrevrange_reverse_order(decoded_r: redis.Redis):
    nan = float("nan")
    await decoded_r.ts().add("{s}:a", 10, 1.0)
    await decoded_r.ts().add("{s}:a", 20, 2.0)
    await decoded_r.ts().add("{s}:b", 20, 3.0)
    await decoded_r.ts().add("{s}:b", 30, 4.0)

    # Reverse: rows in decreasing-timestamp order, same NaN cells.
    res = await decoded_r.ts().nrevrange(["{s}:a", "{s}:b"], from_time="-", to_time="+")
    _assert_nrange_rows(res, [[30, [nan, 4.0]], [20, [2.0, 3.0]], [10, [1.0, nan]]])


@pytest.mark.redismod
@skip_if_server_version_lt("8.9.0")
async def test_nrevrange_aggregation_one_per_key(decoded_r: redis.Redis):
    for ts, val in [(0, 1.0), (1, 2.0), (10, 3.0), (11, 4.0)]:
        await decoded_r.ts().add("{s}:a", ts, val)
    for ts, val in [(0, 5.0), (1, 6.0), (10, 7.0), (11, 8.0)]:
        await decoded_r.ts().add("{s}:b", ts, val)

    # One aggregator per key, rows in decreasing-timestamp order.
    res = await decoded_r.ts().nrevrange(
        ["{s}:a", "{s}:b"],
        from_time=0,
        to_time=20,
        aggregators=["max", "min"],
        bucket_size_msec=10,
    )
    _assert_nrange_rows(res, [[10, [4.0, 7.0]], [0, [2.0, 5.0]]])


@pytest.mark.redismod
@skip_if_server_version_lt("8.9.0")
async def test_nrevrange_count_keeps_highest_timestamps(decoded_r: redis.Redis):
    for ts in range(5):
        await decoded_r.ts().add("{s}:a", ts, ts)
    # COUNT is applied after the merge in decreasing-timestamp order, so the
    # highest timestamps are kept (the opposite end from nrange).
    res = await decoded_r.ts().nrange(["{s}:a"], from_time="-", to_time="+", count=2)
    _assert_nrange_rows(res, [[0, [0.0]], [1, [1.0]]])
    res = await decoded_r.ts().nrevrange(["{s}:a"], from_time="-", to_time="+", count=2)
    _assert_nrange_rows(res, [[4, [4.0]], [3, [3.0]]])
