import math
import time

import pytest
import redis
from redis.commands.timeseries.commands import TimeSeriesCommands
from redis.commands.timeseries.info import TSInfo
from redis.utils import SENTINEL

from .conftest import (
    _get_client,
    assert_resp_response,
    expected_response_shape,
    expects_resp2_shape,
    is_resp2_connection,
    skip_if_server_version_gte,
    skip_if_server_version_lt,
    skip_ifmodversion_lt,
)


@pytest.fixture()
def decoded_r(request, stack_url):
    with _get_client(
        redis.Redis, request, decode_responses=True, from_url=stack_url
    ) as client:
        yield client


@pytest.fixture
def client(decoded_r):
    decoded_r.flushdb()
    return decoded_r


@pytest.mark.redismod
def test_create(client):
    assert client.ts().create(1)
    assert client.ts().create(2, retention_msecs=5)
    assert client.ts().create(3, labels={"Redis": "Labs"})
    assert client.ts().create(4, retention_msecs=20, labels={"Time": "Series"})
    info = client.ts().info(4)
    assert_resp_response(
        client, 20, info.get("retention_msecs"), info.get("retentionTime")
    )
    assert "Series" == info["labels"]["Time"]

    # Test for a chunk size of 128 Bytes
    assert client.ts().create("time-serie-1", chunk_size=128)
    info = client.ts().info("time-serie-1")
    assert_resp_response(client, 128, info.get("chunk_size"), info.get("chunkSize"))


@pytest.mark.redismod
@skip_ifmodversion_lt("1.4.0", "timeseries")
def test_create_duplicate_policy(client):
    # Test for duplicate policy
    for duplicate_policy in ["block", "last", "first", "min", "max"]:
        ts_name = f"time-serie-ooo-{duplicate_policy}"
        assert client.ts().create(ts_name, duplicate_policy=duplicate_policy)
        info = client.ts().info(ts_name)
        assert_resp_response(
            client,
            duplicate_policy,
            info.get("duplicate_policy"),
            info.get("duplicatePolicy"),
        )


@pytest.mark.redismod
def test_alter(client):
    assert client.ts().create(1)
    info = client.ts().info(1)
    assert_resp_response(
        client, 0, info.get("retention_msecs"), info.get("retentionTime")
    )
    assert client.ts().alter(1, retention_msecs=10)
    assert {} == client.ts().info(1)["labels"]
    info = client.ts().info(1)
    assert_resp_response(
        client, 10, info.get("retention_msecs"), info.get("retentionTime")
    )
    assert client.ts().alter(1, labels={"Time": "Series"})
    assert "Series" == client.ts().info(1)["labels"]["Time"]
    info = client.ts().info(1)
    assert_resp_response(
        client, 10, info.get("retention_msecs"), info.get("retentionTime")
    )
    assert client.ts().alter(1, labels={})
    assert {} == client.ts().info(1)["labels"]


@pytest.mark.redismod
@skip_ifmodversion_lt("1.4.0", "timeseries")
@skip_if_server_version_gte("7.9.0")
def test_alter_duplicate_policy_prior_redis_8(client):
    assert client.ts().create(1)
    info = client.ts().info(1)
    assert_resp_response(
        client, None, info.get("duplicate_policy"), info.get("duplicatePolicy")
    )
    assert client.ts().alter(1, duplicate_policy="min")
    info = client.ts().info(1)
    assert_resp_response(
        client, "min", info.get("duplicate_policy"), info.get("duplicatePolicy")
    )


@pytest.mark.redismod
@skip_ifmodversion_lt("1.4.0", "timeseries")
@skip_if_server_version_lt("7.9.0")
def test_alter_duplicate_policy(client):
    assert client.ts().create(1)
    info = client.ts().info(1)
    assert_resp_response(
        client, "block", info.get("duplicate_policy"), info.get("duplicatePolicy")
    )
    assert client.ts().alter(1, duplicate_policy="min")
    info = client.ts().info(1)
    assert_resp_response(
        client, "min", info.get("duplicate_policy"), info.get("duplicatePolicy")
    )


@pytest.mark.redismod
def test_add(client):
    assert 1 == client.ts().add(1, 1, 1)
    assert 2 == client.ts().add(2, 2, 3, retention_msecs=10)
    assert 3 == client.ts().add(3, 3, 2, labels={"Redis": "Labs"})
    assert 4 == client.ts().add(
        4, 4, 2, retention_msecs=10, labels={"Redis": "Labs", "Time": "Series"}
    )

    assert abs(time.time() - float(client.ts().add(5, "*", 1)) / 1000) < 1.0

    info = client.ts().info(4)
    assert_resp_response(
        client, 10, info.get("retention_msecs"), info.get("retentionTime")
    )
    assert "Labs" == info["labels"]["Redis"]

    # Test for a chunk size of 128 Bytes on TS.ADD
    assert client.ts().add("time-serie-1", 1, 10.0, chunk_size=128)
    info = client.ts().info("time-serie-1")
    assert_resp_response(client, 128, info.get("chunk_size"), info.get("chunkSize"))


@pytest.mark.redismod
@skip_ifmodversion_lt("1.4.0", "timeseries")
def test_add_on_duplicate(client):
    # Test for duplicate policy BLOCK
    assert 1 == client.ts().add("time-serie-add-ooo-block", 1, 5.0)
    with pytest.raises(Exception):
        client.ts().add("time-serie-add-ooo-block", 1, 5.0, on_duplicate="block")

    # Test for duplicate policy LAST
    assert 1 == client.ts().add("time-serie-add-ooo-last", 1, 5.0)
    assert 1 == client.ts().add("time-serie-add-ooo-last", 1, 10.0, on_duplicate="last")
    assert 10.0 == client.ts().get("time-serie-add-ooo-last")[1]

    # Test for duplicate policy FIRST
    assert 1 == client.ts().add("time-serie-add-ooo-first", 1, 5.0)
    assert 1 == client.ts().add(
        "time-serie-add-ooo-first", 1, 10.0, on_duplicate="first"
    )
    assert 5.0 == client.ts().get("time-serie-add-ooo-first")[1]

    # Test for duplicate policy MAX
    assert 1 == client.ts().add("time-serie-add-ooo-max", 1, 5.0)
    assert 1 == client.ts().add("time-serie-add-ooo-max", 1, 10.0, on_duplicate="max")
    assert 10.0 == client.ts().get("time-serie-add-ooo-max")[1]

    # Test for duplicate policy MIN
    assert 1 == client.ts().add("time-serie-add-ooo-min", 1, 5.0)
    assert 1 == client.ts().add("time-serie-add-ooo-min", 1, 10.0, on_duplicate="min")
    assert 5.0 == client.ts().get("time-serie-add-ooo-min")[1]


@pytest.mark.redismod
def test_madd(client):
    client.ts().create("a")
    assert [1, 2, 3] == client.ts().madd([("a", 1, 5), ("a", 2, 10), ("a", 3, 15)])


@pytest.mark.redismod
def test_madd_missing_timeseries(client):
    response = client.ts().madd([("a", 1, 5), ("a", 2, 10)])
    assert isinstance(response, list)
    assert len(response) == 2
    assert isinstance(response[0], redis.ResponseError)
    assert isinstance(response[1], redis.ResponseError)


@pytest.mark.redismod
def test_incrby_decrby(client):
    # explicit, increasing timestamps give each call its own sample
    for ts in range(1, 6):
        assert client.ts().incrby(1, 1, timestamp=ts)
    assert 5 == client.ts().get(1)[1]
    for ts in range(6, 11):
        assert client.ts().decrby(1, 1, timestamp=ts)
    assert 0 == client.ts().get(1)[1]

    assert client.ts().incrby(2, 1.5, timestamp=5)
    assert_resp_response(client, client.ts().get(2), (5, 1.5), [5, 1.5])
    assert client.ts().incrby(2, 2.25, timestamp=7)
    assert_resp_response(client, client.ts().get(2), (7, 3.75), [7, 3.75])
    assert client.ts().decrby(2, 1.5, timestamp=15)
    assert_resp_response(client, client.ts().get(2), (15, 2.25), [15, 2.25])

    # Test for a chunk size of 128 Bytes on TS.INCRBY
    assert client.ts().incrby("time-serie-1", 10, chunk_size=128)
    info = client.ts().info("time-serie-1")
    assert_resp_response(client, 128, info.get("chunk_size"), info.get("chunkSize"))

    # Test for a chunk size of 128 Bytes on TS.DECRBY
    assert client.ts().decrby("time-serie-2", 10, chunk_size=128)
    info = client.ts().info("time-serie-2")
    assert_resp_response(client, 128, info.get("chunk_size"), info.get("chunkSize"))


@pytest.mark.redismod
def test_create_and_delete_rule(client):
    # test rule creation
    time = 100
    client.ts().create(1)
    client.ts().create(2)
    client.ts().createrule(1, 2, "avg", 100)
    pipe = client.pipeline(transaction=False)
    for i in range(50):
        pipe.execute_command("TS.ADD", 1, time + i * 2, 1)
        pipe.execute_command("TS.ADD", 1, time + i * 2 + 1, 2)
    pipe.execute()
    client.ts().add(1, time * 2, 1.5)
    assert round(client.ts().get(2)[1], 5) == 1.5
    info = client.ts().info(1)
    if is_resp2_connection(client):
        assert info.rules[0][1] == 100
    else:
        assert info["rules"]["2"][0] == 100

    # test rule deletion
    client.ts().deleterule(1, 2)
    info = client.ts().info(1)
    assert not info["rules"]


@pytest.mark.redismod
@skip_ifmodversion_lt("1.10.0", "timeseries")
def test_del_range(client):
    try:
        client.ts().delete("test", 0, 100)
    except redis.ResponseError as e:
        assert "key does not exist" in str(e)

    pipe = client.pipeline(transaction=False)
    for i in range(100):
        pipe.execute_command("TS.ADD", 1, i, i % 7)
    pipe.execute()
    assert 22 == client.ts().delete(1, 0, 21)
    assert [] == client.ts().range(1, 0, 21)
    assert_resp_response(client, client.ts().range(1, 22, 22), [(22, 1.0)], [[22, 1.0]])


@pytest.mark.redismod
def test_range(client):
    pipe = client.pipeline(transaction=False)
    for i in range(100):
        pipe.execute_command("TS.ADD", 1, i, i % 7)
    pipe.execute()
    assert 100 == len(client.ts().range(1, 0, 200))
    pipe = client.pipeline(transaction=False)
    for i in range(100):
        pipe.execute_command("TS.ADD", 1, i + 200, i % 7)
    pipe.execute()
    assert 200 == len(client.ts().range(1, 0, 500))
    # last sample isn't returned
    assert 20 == len(
        client.ts().range(1, 0, 500, aggregation_type="avg", bucket_size_msec=10)
    )
    assert 10 == len(client.ts().range(1, 0, 500, count=10))


@pytest.mark.redismod
@skip_ifmodversion_lt("1.10.0", "timeseries")
def test_range_advanced(client):
    pipe = client.pipeline(transaction=False)
    for i in range(100):
        pipe.execute_command("TS.ADD", 1, i, i % 7)
        pipe.execute_command("TS.ADD", 1, i + 200, i % 7)
    pipe.execute()

    assert 2 == len(
        client.ts().range(
            1,
            0,
            500,
            filter_by_ts=[i for i in range(10, 20)],
            filter_by_min_value=1,
            filter_by_max_value=2,
        )
    )
    res = client.ts().range(
        1, 0, 10, aggregation_type="count", bucket_size_msec=10, align="+"
    )
    assert_resp_response(client, res, [(0, 10.0), (10, 1.0)], [[0, 10.0], [10, 1.0]])
    res = client.ts().range(
        1, 0, 10, aggregation_type="count", bucket_size_msec=10, align=5
    )
    assert_resp_response(client, res, [(0, 5.0), (5, 6.0)], [[0, 5.0], [5, 6.0]])
    res = client.ts().range(1, 0, 10, aggregation_type="twa", bucket_size_msec=10)
    assert_resp_response(client, res, [(0, 2.55), (10, 3.0)], [[0, 2.55], [10, 3.0]])


@pytest.mark.onlynoncluster
@pytest.mark.redismod
@skip_ifmodversion_lt("1.8.0", "timeseries")
def test_range_latest(client: redis.Redis):
    timeseries = client.ts()
    timeseries.create("t1")
    timeseries.create("t2")
    timeseries.createrule("t1", "t2", aggregation_type="sum", bucket_size_msec=10)
    timeseries.add("t1", 1, 1)
    timeseries.add("t1", 2, 3)
    timeseries.add("t1", 11, 7)
    timeseries.add("t1", 13, 1)
    assert_resp_response(
        client,
        timeseries.range("t1", 0, 20),
        [(1, 1.0), (2, 3.0), (11, 7.0), (13, 1.0)],
        [[1, 1.0], [2, 3.0], [11, 7.0], [13, 1.0]],
    )
    assert_resp_response(client, timeseries.range("t2", 0, 10), [(0, 4.0)], [[0, 4.0]])
    res = timeseries.range("t2", 0, 10, latest=True)
    assert_resp_response(client, res, [(0, 4.0), (10, 8.0)], [[0, 4.0], [10, 8.0]])
    assert_resp_response(
        client, timeseries.range("t2", 0, 9, latest=True), [(0, 4.0)], [[0, 4.0]]
    )


@pytest.mark.redismod
@skip_ifmodversion_lt("1.8.0", "timeseries")
def test_range_bucket_timestamp(client: redis.Redis):
    timeseries = client.ts()
    timeseries.create("t1")
    timeseries.add("t1", 15, 1)
    timeseries.add("t1", 17, 4)
    timeseries.add("t1", 51, 3)
    timeseries.add("t1", 73, 5)
    timeseries.add("t1", 75, 3)
    assert_resp_response(
        client,
        timeseries.range(
            "t1", 0, 100, align=0, aggregation_type="max", bucket_size_msec=10
        ),
        [(10, 4.0), (50, 3.0), (70, 5.0)],
        [[10, 4.0], [50, 3.0], [70, 5.0]],
    )
    assert_resp_response(
        client,
        timeseries.range(
            "t1",
            0,
            100,
            align=0,
            aggregation_type="max",
            bucket_size_msec=10,
            bucket_timestamp="+",
        ),
        [(20, 4.0), (60, 3.0), (80, 5.0)],
        [[20, 4.0], [60, 3.0], [80, 5.0]],
    )


@pytest.mark.redismod
@skip_ifmodversion_lt("1.8.0", "timeseries")
def test_range_empty(client: redis.Redis):
    timeseries = client.ts()
    timeseries.create("t1")
    timeseries.add("t1", 15, 1)
    timeseries.add("t1", 17, 4)
    timeseries.add("t1", 51, 3)
    timeseries.add("t1", 73, 5)
    timeseries.add("t1", 75, 3)
    assert_resp_response(
        client,
        timeseries.range(
            "t1", 0, 100, align=0, aggregation_type="max", bucket_size_msec=10
        ),
        [(10, 4.0), (50, 3.0), (70, 5.0)],
        [[10, 4.0], [50, 3.0], [70, 5.0]],
    )
    res = timeseries.range(
        "t1", 0, 100, align=0, aggregation_type="max", bucket_size_msec=10, empty=True
    )
    for i in range(len(res)):
        if math.isnan(res[i][1]):
            res[i] = (res[i][0], None)
    resp2_expected = [
        (10, 4.0),
        (20, None),
        (30, None),
        (40, None),
        (50, 3.0),
        (60, None),
        (70, 5.0),
    ]
    resp3_expected = [
        [10, 4.0],
        (20, None),
        (30, None),
        (40, None),
        [50, 3.0],
        (60, None),
        [70, 5.0],
    ]
    assert_resp_response(client, res, resp2_expected, resp3_expected)


@pytest.mark.redismod
@skip_ifmodversion_lt("1.10.0", "timeseries")
def test_rev_range(client):
    pipe = client.pipeline(transaction=False)
    for i in range(100):
        pipe.execute_command("TS.ADD", 1, i, i % 7)
    pipe.execute()
    assert 100 == len(client.ts().range(1, 0, 200))
    pipe = client.pipeline(transaction=False)
    for i in range(100):
        pipe.execute_command("TS.ADD", 1, i + 200, i % 7)
    pipe.execute()
    assert 200 == len(client.ts().range(1, 0, 500))
    # first sample isn't returned
    assert 20 == len(
        client.ts().revrange(1, 0, 500, aggregation_type="avg", bucket_size_msec=10)
    )
    assert 10 == len(client.ts().revrange(1, 0, 500, count=10))
    assert 2 == len(
        client.ts().revrange(
            1,
            0,
            500,
            filter_by_ts=[i for i in range(10, 20)],
            filter_by_min_value=1,
            filter_by_max_value=2,
        )
    )
    assert_resp_response(
        client,
        client.ts().revrange(
            1, 0, 10, aggregation_type="count", bucket_size_msec=10, align="+"
        ),
        [(10, 1.0), (0, 10.0)],
        [[10, 1.0], [0, 10.0]],
    )
    assert_resp_response(
        client,
        client.ts().revrange(
            1, 0, 10, aggregation_type="count", bucket_size_msec=10, align=1
        ),
        [(1, 10.0), (0, 1.0)],
        [[1, 10.0], [0, 1.0]],
    )
    assert_resp_response(
        client,
        client.ts().revrange(1, 0, 10, aggregation_type="twa", bucket_size_msec=10),
        [(10, 3.0), (0, 2.55)],
        [[10, 3.0], [0, 2.55]],
    )


@pytest.mark.onlynoncluster
@pytest.mark.redismod
@skip_ifmodversion_lt("1.8.0", "timeseries")
def test_revrange_latest(client: redis.Redis):
    timeseries = client.ts()
    timeseries.create("t1")
    timeseries.create("t2")
    timeseries.createrule("t1", "t2", aggregation_type="sum", bucket_size_msec=10)
    timeseries.add("t1", 1, 1)
    timeseries.add("t1", 2, 3)
    timeseries.add("t1", 11, 7)
    timeseries.add("t1", 13, 1)
    res = timeseries.revrange("t2", 0, 10)
    assert_resp_response(client, res, [(0, 4.0)], [[0, 4.0]])
    res = timeseries.revrange("t2", 0, 10, latest=True)
    assert_resp_response(client, res, [(10, 8.0), (0, 4.0)], [[10, 8.0], [0, 4.0]])
    res = timeseries.revrange("t2", 0, 9, latest=True)
    assert_resp_response(client, res, [(0, 4.0)], [[0, 4.0]])


@pytest.mark.redismod
@skip_ifmodversion_lt("1.8.0", "timeseries")
def test_revrange_bucket_timestamp(client: redis.Redis):
    timeseries = client.ts()
    timeseries.create("t1")
    timeseries.add("t1", 15, 1)
    timeseries.add("t1", 17, 4)
    timeseries.add("t1", 51, 3)
    timeseries.add("t1", 73, 5)
    timeseries.add("t1", 75, 3)
    assert_resp_response(
        client,
        timeseries.revrange(
            "t1", 0, 100, align=0, aggregation_type="max", bucket_size_msec=10
        ),
        [(70, 5.0), (50, 3.0), (10, 4.0)],
        [[70, 5.0], [50, 3.0], [10, 4.0]],
    )
    assert_resp_response(
        client,
        timeseries.range(
            "t1",
            0,
            100,
            align=0,
            aggregation_type="max",
            bucket_size_msec=10,
            bucket_timestamp="+",
        ),
        [(20, 4.0), (60, 3.0), (80, 5.0)],
        [[20, 4.0], [60, 3.0], [80, 5.0]],
    )


@pytest.mark.redismod
@skip_ifmodversion_lt("1.8.0", "timeseries")
def test_revrange_empty(client: redis.Redis):
    timeseries = client.ts()
    timeseries.create("t1")
    timeseries.add("t1", 15, 1)
    timeseries.add("t1", 17, 4)
    timeseries.add("t1", 51, 3)
    timeseries.add("t1", 73, 5)
    timeseries.add("t1", 75, 3)
    assert_resp_response(
        client,
        timeseries.revrange(
            "t1", 0, 100, align=0, aggregation_type="max", bucket_size_msec=10
        ),
        [(70, 5.0), (50, 3.0), (10, 4.0)],
        [[70, 5.0], [50, 3.0], [10, 4.0]],
    )
    res = timeseries.revrange(
        "t1", 0, 100, align=0, aggregation_type="max", bucket_size_msec=10, empty=True
    )
    for i in range(len(res)):
        if math.isnan(res[i][1]):
            res[i] = (res[i][0], None)
    resp2_expected = [
        (70, 5.0),
        (60, None),
        (50, 3.0),
        (40, None),
        (30, None),
        (20, None),
        (10, 4.0),
    ]
    resp3_expected = [
        [70, 5.0],
        (60, None),
        [50, 3.0],
        (40, None),
        (30, None),
        (20, None),
        [10, 4.0],
    ]
    assert_resp_response(client, res, resp2_expected, resp3_expected)


@pytest.mark.redismod
@skip_if_server_version_lt("8.9.0")
def test_read(client):
    client.ts().create(1)
    client.ts().add(1, 100, 1.0)
    client.ts().add(1, 200, 2.0)
    client.ts().add(1, 300, 3.0)

    # Read everything at or after the cursor. TS.READ always returns the same
    # unified sample shape (list of [timestamp, value]) regardless of protocol.
    assert client.ts().read(1, 0) == [[100, 1.0], [200, 2.0], [300, 3.0]]

    # The cursor is inclusive.
    assert client.ts().read(1, 200) == [[200, 2.0], [300, 3.0]]


@pytest.mark.redismod
@skip_if_server_version_lt("8.9.0")
def test_read_max_count(client):
    client.ts().create(1)
    client.ts().add(1, 100, 1.0)
    client.ts().add(1, 200, 2.0)
    client.ts().add(1, 300, 3.0)

    # Bounded paging: read the oldest max_count, then page from last_ts + 1.
    assert client.ts().read(1, "-", max_count=2) == [[100, 1.0], [200, 2.0]]
    assert client.ts().read(1, 201, max_count=2) == [[300, 3.0]]


@pytest.mark.redismod
@skip_if_server_version_lt("8.9.0")
def test_read_sentinels(client):
    client.ts().create(1)
    client.ts().add(1, 100, 1.0)
    client.ts().add(1, 200, 2.0)
    client.ts().add(1, 300, 3.0)

    # `+` resolves to the latest sample, inclusive; returned even without BLOCK.
    assert client.ts().read(1, "+") == [[300, 3.0]]

    # `-` reads from the earliest sample.
    assert len(client.ts().read(1, "-")) == 3


@pytest.mark.redismod
@skip_if_server_version_lt("8.9.0")
def test_read_empty(client):
    client.ts().create(1)
    client.ts().add(1, 100, 1.0)

    # A cursor past the newest sample yields an empty (successful) reply.
    assert [] == client.ts().read(1, 301)
    # A missing key is also an empty reply, not an error.
    assert [] == client.ts().read("missing", 0)


@pytest.mark.redismod
@skip_if_server_version_lt("8.9.0")
def test_read_block(client):
    client.ts().create(1)
    client.ts().add(1, 100, 1.0)
    client.ts().add(1, 200, 2.0)
    client.ts().add(1, 300, 3.0)

    # min_count is already met, so the blocking call returns immediately.
    res = client.ts().read(1, 0, block_milliseconds=1000, block_min_count=1)
    assert 3 == len(res)

    # min_count cannot be reached; after the timeout the available samples flush.
    res = client.ts().read(1, 101, block_milliseconds=100, block_min_count=10)
    assert res == [[200, 2.0], [300, 3.0]]

    # A blocking timeout with nothing available is a successful empty reply.
    assert [] == client.ts().read(1, 301, block_milliseconds=100, block_min_count=1)


@pytest.mark.redismod
def test_range_filter_by_value_requires_both_bounds(client):
    client.ts().add(1, 100, 1)
    client.ts().add(1, 200, 5)

    for cmd in (client.ts().range, client.ts().revrange):
        with pytest.raises(
            redis.exceptions.DataError,
            match="filter_by_min_value and filter_by_max_value must be set together",
        ):
            cmd(1, 0, 500, filter_by_min_value=1)
        with pytest.raises(
            redis.exceptions.DataError,
            match="filter_by_min_value and filter_by_max_value must be set together",
        ):
            cmd(1, 0, 500, filter_by_max_value=5)
        # Both bounds still emit FILTER_BY_VALUE and filter server-side.
        assert 1 == len(cmd(1, 0, 500, filter_by_min_value=1, filter_by_max_value=1))


@pytest.mark.redismod
@skip_if_server_version_lt("8.9.0")
def test_read_block_min_count_requires_milliseconds(client):
    # BLOCK is all-or-nothing: min_count without milliseconds is invalid usage.
    with pytest.raises(
        redis.exceptions.DataError, match="block_min_count requires block_milliseconds"
    ):
        client.ts().read(1, 0, block_min_count=5)


@pytest.mark.onlynoncluster
@pytest.mark.redismod
def test_mrange(client):
    client.ts().create(1, labels={"Test": "This", "team": "ny"})
    client.ts().create(2, labels={"Test": "This", "Taste": "That", "team": "sf"})
    pipe = client.pipeline(transaction=False)
    for i in range(100):
        pipe.execute_command("TS.ADD", 1, i, i % 7)
        pipe.execute_command("TS.ADD", 2, i, i % 11)
    pipe.execute()

    res = client.ts().mrange(0, 200, filters=["Test=This"])
    assert 2 == len(res)
    if expects_resp2_shape(client):
        assert 100 == len(res[0]["1"][1])

        res = client.ts().mrange(0, 200, filters=["Test=This"], count=10)
        assert 10 == len(res[0]["1"][1])

        pipe = client.pipeline(transaction=False)
        for i in range(100):
            pipe.execute_command("TS.ADD", 1, i + 200, i % 7)
        pipe.execute()
        res = client.ts().mrange(
            0, 500, filters=["Test=This"], aggregation_type="avg", bucket_size_msec=10
        )
        assert 2 == len(res)
        assert 20 == len(res[0]["1"][1])

        # test withlabels
        assert {} == res[0]["1"][0]
        res = client.ts().mrange(0, 200, filters=["Test=This"], with_labels=True)
        assert {"Test": "This", "team": "ny"} == res[0]["1"][0]
    else:
        assert 100 == len(res["1"][2])

        res = client.ts().mrange(0, 200, filters=["Test=This"], count=10)
        assert 10 == len(res["1"][2])

        pipe = client.pipeline(transaction=False)
        for i in range(100):
            pipe.execute_command("TS.ADD", 1, i + 200, i % 7)
        pipe.execute()
        res = client.ts().mrange(
            0, 500, filters=["Test=This"], aggregation_type="avg", bucket_size_msec=10
        )
        assert 2 == len(res)
        assert 20 == len(res["1"][2])

        # test withlabels
        assert {} == res["1"][0]
        res = client.ts().mrange(0, 200, filters=["Test=This"], with_labels=True)
        assert {"Test": "This", "team": "ny"} == res["1"][0]


def _mrange_returned_keys(client, res):
    """Return the set of series key names in a TS.MRANGE/MREVRANGE reply,
    normalizing the RESP2 (list of single-key dicts) and RESP3/unified
    (dict keyed by name) shapes."""
    if expects_resp2_shape(client):
        return {next(iter(entry)) for entry in res}
    return set(res.keys())


@pytest.mark.onlynoncluster
@pytest.mark.redismod
@skip_if_server_version_lt("8.9.0")
def test_mrange_exclude_empty(client):
    client.ts().create("s", labels={"sensor": "1", "type": "demo"})
    client.ts().create("t", labels={"sensor": "1", "type": "demo"})
    client.ts().create("u", labels={"sensor": "1", "type": "demo"})
    client.ts().madd(
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
    res = client.ts().mrange("-", 500, filters=["sensor=1"])
    assert {"s", "t", "u"} == _mrange_returned_keys(client, res)

    # With EXCLUDEEMPTY, "u" is omitted from the top-level reply.
    res = client.ts().mrange("-", 500, filters=["sensor=1"], exclude_empty=True)
    assert {"s", "t"} == _mrange_returned_keys(client, res)

    # Composing with WITHLABELS should not change the exclusion behavior.
    res = client.ts().mrange(
        "-", 500, filters=["sensor=1"], with_labels=True, exclude_empty=True
    )
    assert {"s", "t"} == _mrange_returned_keys(client, res)

    # Composing with AGGREGATION should still omit the empty series.
    res = client.ts().mrange(
        "-",
        500,
        filters=["sensor=1"],
        aggregation_type="min",
        bucket_size_msec=100,
        exclude_empty=True,
    )
    assert {"s", "t"} == _mrange_returned_keys(client, res)

    # When every matching series is empty, no series is reported (an empty
    # top-level reply; shape is [] in RESP2 and {} in RESP3).
    res = client.ts().mrange(1, 50, filters=["sensor=1"], exclude_empty=True)
    assert 0 == len(res)


@pytest.mark.onlynoncluster
@pytest.mark.redismod
@skip_if_server_version_lt("8.9.0")
def test_mrevrange_exclude_empty(client):
    client.ts().create("s", labels={"sensor": "1", "type": "demo"})
    client.ts().create("t", labels={"sensor": "1", "type": "demo"})
    client.ts().create("u", labels={"sensor": "1", "type": "demo"})
    client.ts().madd(
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

    res = client.ts().mrevrange("-", 500, filters=["sensor=1"])
    assert {"s", "t", "u"} == _mrange_returned_keys(client, res)

    res = client.ts().mrevrange("-", 500, filters=["sensor=1"], exclude_empty=True)
    assert {"s", "t"} == _mrange_returned_keys(client, res)

    # All matching series empty -> empty top-level reply ([] in RESP2, {} in RESP3).
    res = client.ts().mrevrange(1, 50, filters=["sensor=1"], exclude_empty=True)
    assert 0 == len(res)


@pytest.mark.onlynoncluster
@pytest.mark.redismod
def test_mrange_exclude_empty_with_groupby_raises(client):
    # EXCLUDEEMPTY is mutually exclusive with GROUPBY. This is validated
    # client-side, so it does not require server support.
    with pytest.raises(
        redis.DataError, match="EXCLUDEEMPTY is not allowed with GROUPBY"
    ):
        client.ts().mrange(
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
        client.ts().mrevrange(
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
def test_multi_range_advanced(client):
    client.ts().create(1, labels={"Test": "This", "team": "ny"})
    client.ts().create(2, labels={"Test": "This", "Taste": "That", "team": "sf"})
    pipe = client.pipeline(transaction=False)
    for i in range(100):
        pipe.execute_command("TS.ADD", 1, i, i % 7)
        pipe.execute_command("TS.ADD", 2, i, i % 11)
    pipe.execute()

    # test with selected labels
    res = client.ts().mrange(0, 200, filters=["Test=This"], select_labels=["team"])
    if expects_resp2_shape(client):
        assert {"team": "ny"} == res[0]["1"][0]
        assert {"team": "sf"} == res[1]["2"][0]

        # test with filterby
        res = client.ts().mrange(
            0,
            200,
            filters=["Test=This"],
            filter_by_ts=[i for i in range(10, 20)],
            filter_by_min_value=1,
            filter_by_max_value=2,
        )
        assert [(15, 1.0), (16, 2.0)] == res[0]["1"][1]

        # test groupby
        res = client.ts().mrange(
            0, 3, filters=["Test=This"], groupby="Test", reduce="sum"
        )
        assert [(0, 0.0), (1, 2.0), (2, 4.0), (3, 6.0)] == res[0]["Test=This"][1]
        res = client.ts().mrange(
            0, 3, filters=["Test=This"], groupby="Test", reduce="max"
        )
        assert [(0, 0.0), (1, 1.0), (2, 2.0), (3, 3.0)] == res[0]["Test=This"][1]
        res = client.ts().mrange(
            0, 3, filters=["Test=This"], groupby="team", reduce="min"
        )
        assert 2 == len(res)
        assert [(0, 0.0), (1, 1.0), (2, 2.0), (3, 3.0)] == res[0]["team=ny"][1]
        assert [(0, 0.0), (1, 1.0), (2, 2.0), (3, 3.0)] == res[1]["team=sf"][1]

        # test align
        res = client.ts().mrange(
            0,
            10,
            filters=["team=ny"],
            aggregation_type="count",
            bucket_size_msec=10,
            align="-",
        )
        assert [(0, 10.0), (10, 1.0)] == res[0]["1"][1]
        res = client.ts().mrange(
            0,
            10,
            filters=["team=ny"],
            aggregation_type="count",
            bucket_size_msec=10,
            align=5,
        )
        assert [(0, 5.0), (5, 6.0)] == res[0]["1"][1]
    else:
        # ``expected_response_shape`` is ``"unified"`` for
        # ``legacy_responses=False`` (any protocol) and ``"legacy_resp3"``
        # for ``protocol=3`` with ``legacy_responses=True``. Unified parsers
        # keep samples at index 2; legacy RESP3 preserves the wire layout,
        # appending an extra ``sources`` element under GROUPBY which pushes
        # samples to index 3.
        groupby_samples_idx = 2 if expected_response_shape(client) == "unified" else 3
        assert {"team": "ny"} == res["1"][0]
        assert {"team": "sf"} == res["2"][0]

        # test with filterby
        res = client.ts().mrange(
            0,
            200,
            filters=["Test=This"],
            filter_by_ts=[i for i in range(10, 20)],
            filter_by_min_value=1,
            filter_by_max_value=2,
        )
        assert [[15, 1.0], [16, 2.0]] == res["1"][2]

        # test groupby
        res = client.ts().mrange(
            0, 3, filters=["Test=This"], groupby="Test", reduce="sum"
        )
        assert [[0, 0.0], [1, 2.0], [2, 4.0], [3, 6.0]] == res["Test=This"][
            groupby_samples_idx
        ]
        res = client.ts().mrange(
            0, 3, filters=["Test=This"], groupby="Test", reduce="max"
        )
        assert [[0, 0.0], [1, 1.0], [2, 2.0], [3, 3.0]] == res["Test=This"][
            groupby_samples_idx
        ]
        res = client.ts().mrange(
            0, 3, filters=["Test=This"], groupby="team", reduce="min"
        )
        assert 2 == len(res)
        assert [[0, 0.0], [1, 1.0], [2, 2.0], [3, 3.0]] == res["team=ny"][
            groupby_samples_idx
        ]
        assert [[0, 0.0], [1, 1.0], [2, 2.0], [3, 3.0]] == res["team=sf"][
            groupby_samples_idx
        ]

        # test align
        res = client.ts().mrange(
            0,
            10,
            filters=["team=ny"],
            aggregation_type="count",
            bucket_size_msec=10,
            align="-",
        )
        assert [[0, 10.0], [10, 1.0]] == res["1"][2]
        res = client.ts().mrange(
            0,
            10,
            filters=["team=ny"],
            aggregation_type="count",
            bucket_size_msec=10,
            align=5,
        )
        assert [[0, 5.0], [5, 6.0]] == res["1"][2]


@pytest.mark.onlynoncluster
@pytest.mark.redismod
@skip_ifmodversion_lt("1.8.0", "timeseries")
def test_mrange_latest(client: redis.Redis):
    timeseries = client.ts()
    timeseries.create("t1")
    timeseries.create("t2", labels={"is_compaction": "true"})
    timeseries.create("t3")
    timeseries.create("t4", labels={"is_compaction": "true"})
    timeseries.createrule("t1", "t2", aggregation_type="sum", bucket_size_msec=10)
    timeseries.createrule("t3", "t4", aggregation_type="sum", bucket_size_msec=10)
    timeseries.add("t1", 1, 1)
    timeseries.add("t1", 2, 3)
    timeseries.add("t1", 11, 7)
    timeseries.add("t1", 13, 1)
    timeseries.add("t3", 1, 1)
    timeseries.add("t3", 2, 3)
    timeseries.add("t3", 11, 7)
    timeseries.add("t3", 13, 1)
    assert_resp_response(
        client,
        client.ts().mrange(0, 10, filters=["is_compaction=true"], latest=True),
        [{"t2": [{}, [(0, 4.0), (10, 8.0)]]}, {"t4": [{}, [(0, 4.0), (10, 8.0)]]}],
        {
            "t2": [{}, {"aggregators": []}, [[0, 4.0], [10, 8.0]]],
            "t4": [{}, {"aggregators": []}, [[0, 4.0], [10, 8.0]]],
        },
        {
            "t2": [{}, {"aggregators": []}, [[0, 4.0], [10, 8.0]]],
            "t4": [{}, {"aggregators": []}, [[0, 4.0], [10, 8.0]]],
        },
    )


@pytest.mark.onlynoncluster
@pytest.mark.redismod
@skip_ifmodversion_lt("1.10.0", "timeseries")
def test_multi_reverse_range(client):
    client.ts().create(1, labels={"Test": "This", "team": "ny"})
    client.ts().create(2, labels={"Test": "This", "Taste": "That", "team": "sf"})
    pipe = client.pipeline(transaction=False)
    for i in range(100):
        pipe.execute_command("TS.ADD", 1, i, i % 7)
        pipe.execute_command("TS.ADD", 2, i, i % 11)
    pipe.execute()

    # ``expected_response_shape`` is ``"unified"`` for
    # ``legacy_responses=False`` (any protocol) and ``"legacy_resp3"`` for
    # ``protocol=3`` with ``legacy_responses=True``. The unified shape
    # always emits ``[labels, [], samples]`` (samples at index 2); legacy
    # RESP3 preserves the wire layout, appending an extra ``sources``
    # element under GROUPBY which pushes samples to index 3.
    groupby_samples_idx = 2 if expected_response_shape(client) == "unified" else 3

    res = client.ts().mrange(0, 200, filters=["Test=This"])
    assert 2 == len(res)
    if expects_resp2_shape(client):
        assert 100 == len(res[0]["1"][1])
    else:
        assert 100 == len(res["1"][2])

    res = client.ts().mrange(0, 200, filters=["Test=This"], count=10)
    if expects_resp2_shape(client):
        assert 10 == len(res[0]["1"][1])
    else:
        assert 10 == len(res["1"][2])

    pipe = client.pipeline(transaction=False)
    for i in range(100):
        pipe.execute_command("TS.ADD", 1, i + 200, i % 7)
    pipe.execute()
    res = client.ts().mrevrange(
        0, 500, filters=["Test=This"], aggregation_type="avg", bucket_size_msec=10
    )
    assert 2 == len(res)
    if expects_resp2_shape(client):
        assert 20 == len(res[0]["1"][1])
        assert {} == res[0]["1"][0]
    else:
        assert 20 == len(res["1"][2])
        assert {} == res["1"][0]

    # test withlabels
    res = client.ts().mrevrange(0, 200, filters=["Test=This"], with_labels=True)
    if expects_resp2_shape(client):
        assert {"Test": "This", "team": "ny"} == res[0]["1"][0]
    else:
        assert {"Test": "This", "team": "ny"} == res["1"][0]

    # test with selected labels
    res = client.ts().mrevrange(0, 200, filters=["Test=This"], select_labels=["team"])
    if expects_resp2_shape(client):
        assert {"team": "ny"} == res[0]["1"][0]
        assert {"team": "sf"} == res[1]["2"][0]
    else:
        assert {"team": "ny"} == res["1"][0]
        assert {"team": "sf"} == res["2"][0]

    # test filterby
    res = client.ts().mrevrange(
        0,
        200,
        filters=["Test=This"],
        filter_by_ts=[i for i in range(10, 20)],
        filter_by_min_value=1,
        filter_by_max_value=2,
    )
    if expects_resp2_shape(client):
        assert [(16, 2.0), (15, 1.0)] == res[0]["1"][1]
    else:
        assert [[16, 2.0], [15, 1.0]] == res["1"][2]

    # test groupby
    res = client.ts().mrevrange(
        0, 3, filters=["Test=This"], groupby="Test", reduce="sum"
    )
    if expects_resp2_shape(client):
        assert [(3, 6.0), (2, 4.0), (1, 2.0), (0, 0.0)] == res[0]["Test=This"][1]
    else:
        assert [[3, 6.0], [2, 4.0], [1, 2.0], [0, 0.0]] == res["Test=This"][
            groupby_samples_idx
        ]
    res = client.ts().mrevrange(
        0, 3, filters=["Test=This"], groupby="Test", reduce="max"
    )
    if expects_resp2_shape(client):
        assert [(3, 3.0), (2, 2.0), (1, 1.0), (0, 0.0)] == res[0]["Test=This"][1]
    else:
        assert [[3, 3.0], [2, 2.0], [1, 1.0], [0, 0.0]] == res["Test=This"][
            groupby_samples_idx
        ]
    res = client.ts().mrevrange(
        0, 3, filters=["Test=This"], groupby="team", reduce="min"
    )
    assert 2 == len(res)
    if expects_resp2_shape(client):
        assert [(3, 3.0), (2, 2.0), (1, 1.0), (0, 0.0)] == res[0]["team=ny"][1]
        assert [(3, 3.0), (2, 2.0), (1, 1.0), (0, 0.0)] == res[1]["team=sf"][1]
    else:
        assert [[3, 3.0], [2, 2.0], [1, 1.0], [0, 0.0]] == res["team=ny"][
            groupby_samples_idx
        ]
        assert [[3, 3.0], [2, 2.0], [1, 1.0], [0, 0.0]] == res["team=sf"][
            groupby_samples_idx
        ]

    # test align
    res = client.ts().mrevrange(
        0,
        10,
        filters=["team=ny"],
        aggregation_type="count",
        bucket_size_msec=10,
        align="-",
    )
    if expects_resp2_shape(client):
        assert [(10, 1.0), (0, 10.0)] == res[0]["1"][1]
    else:
        assert [[10, 1.0], [0, 10.0]] == res["1"][2]
    res = client.ts().mrevrange(
        0,
        10,
        filters=["team=ny"],
        aggregation_type="count",
        bucket_size_msec=10,
        align=1,
    )
    if expects_resp2_shape(client):
        assert [(1, 10.0), (0, 1.0)] == res[0]["1"][1]
    else:
        assert [[1, 10.0], [0, 1.0]] == res["1"][2]


@pytest.mark.onlynoncluster
@pytest.mark.redismod
@skip_ifmodversion_lt("1.8.0", "timeseries")
def test_mrevrange_latest(client: redis.Redis):
    timeseries = client.ts()
    timeseries.create("t1")
    timeseries.create("t2", labels={"is_compaction": "true"})
    timeseries.create("t3")
    timeseries.create("t4", labels={"is_compaction": "true"})
    timeseries.createrule("t1", "t2", aggregation_type="sum", bucket_size_msec=10)
    timeseries.createrule("t3", "t4", aggregation_type="sum", bucket_size_msec=10)
    timeseries.add("t1", 1, 1)
    timeseries.add("t1", 2, 3)
    timeseries.add("t1", 11, 7)
    timeseries.add("t1", 13, 1)
    timeseries.add("t3", 1, 1)
    timeseries.add("t3", 2, 3)
    timeseries.add("t3", 11, 7)
    timeseries.add("t3", 13, 1)
    assert_resp_response(
        client,
        client.ts().mrevrange(0, 10, filters=["is_compaction=true"], latest=True),
        [{"t2": [{}, [(10, 8.0), (0, 4.0)]]}, {"t4": [{}, [(10, 8.0), (0, 4.0)]]}],
        {
            "t2": [{}, {"aggregators": []}, [[10, 8.0], [0, 4.0]]],
            "t4": [{}, {"aggregators": []}, [[10, 8.0], [0, 4.0]]],
        },
        {
            "t2": [{}, {"aggregators": []}, [[10, 8.0], [0, 4.0]]],
            "t4": [{}, {"aggregators": []}, [[10, 8.0], [0, 4.0]]],
        },
    )


@pytest.mark.redismod
def test_get(client):
    name = "test"
    client.ts().create(name)
    assert not client.ts().get(name)
    client.ts().add(name, 2, 3)
    assert 2 == client.ts().get(name)[0]
    client.ts().add(name, 3, 4)
    assert 4 == client.ts().get(name)[1]


@pytest.mark.onlynoncluster
@pytest.mark.redismod
@skip_ifmodversion_lt("1.8.0", "timeseries")
def test_get_latest(client: redis.Redis):
    timeseries = client.ts()
    timeseries.create("t1")
    timeseries.create("t2")
    timeseries.createrule("t1", "t2", aggregation_type="sum", bucket_size_msec=10)
    timeseries.add("t1", 1, 1)
    timeseries.add("t1", 2, 3)
    timeseries.add("t1", 11, 7)
    timeseries.add("t1", 13, 1)
    assert_resp_response(client, timeseries.get("t2"), (0, 4.0), [0, 4.0])
    assert_resp_response(
        client, timeseries.get("t2", latest=True), (10, 8.0), [10, 8.0]
    )


@pytest.mark.onlynoncluster
@pytest.mark.redismod
def test_mget(client):
    client.ts().create(1, labels={"Test": "This"})
    client.ts().create(2, labels={"Test": "This", "Taste": "That"})
    act_res = client.ts().mget(["Test=This"])
    exp_res = [{"1": [{}, None, None]}, {"2": [{}, None, None]}]
    exp_res_resp3 = {"1": [{}, []], "2": [{}, []]}
    assert_resp_response(client, act_res, exp_res, exp_res_resp3)
    client.ts().add(1, "*", 15)
    client.ts().add(2, "*", 25)
    res = client.ts().mget(["Test=This"])
    if expects_resp2_shape(client):
        assert 15 == res[0]["1"][2]
        assert 25 == res[1]["2"][2]
    else:
        assert 15 == res["1"][1][1]
        assert 25 == res["2"][1][1]
    res = client.ts().mget(["Taste=That"])
    if expects_resp2_shape(client):
        assert 25 == res[0]["2"][2]
    else:
        assert 25 == res["2"][1][1]

    # test with_labels
    if expects_resp2_shape(client):
        assert {} == res[0]["2"][0]
    else:
        assert {} == res["2"][0]
    res = client.ts().mget(["Taste=That"], with_labels=True)
    if expects_resp2_shape(client):
        assert {"Taste": "That", "Test": "This"} == res[0]["2"][0]
    else:
        assert {"Taste": "That", "Test": "This"} == res["2"][0]


@pytest.mark.onlynoncluster
@pytest.mark.redismod
@skip_ifmodversion_lt("1.8.0", "timeseries")
def test_mget_latest(client: redis.Redis):
    timeseries = client.ts()
    timeseries.create("t1")
    timeseries.create("t2", labels={"is_compaction": "true"})
    timeseries.createrule("t1", "t2", aggregation_type="sum", bucket_size_msec=10)
    timeseries.add("t1", 1, 1)
    timeseries.add("t1", 2, 3)
    timeseries.add("t1", 11, 7)
    timeseries.add("t1", 13, 1)
    res = timeseries.mget(filters=["is_compaction=true"])
    assert_resp_response(client, res, [{"t2": [{}, 0, 4.0]}], {"t2": [{}, [0, 4.0]]})
    res = timeseries.mget(filters=["is_compaction=true"], latest=True)
    assert_resp_response(client, res, [{"t2": [{}, 10, 8.0]}], {"t2": [{}, [10, 8.0]]})


@pytest.mark.redismod
def test_info(client):
    client.ts().create(1, retention_msecs=5, labels={"currentLabel": "currentData"})
    info = client.ts().info(1)
    assert_resp_response(
        client, 5, info.get("retention_msecs"), info.get("retentionTime")
    )
    assert info["labels"]["currentLabel"] == "currentData"


@pytest.mark.fixed_client
def test_info_resp3_bytes_keys():
    # Server-free: without decode_responses the RESP3 map has bytes keys and
    # must parse like the RESP2 flat list.
    resp2 = TSInfo(
        [
            b"totalSamples",
            1,
            b"retentionTime",
            5,
            b"chunkSize",
            4096,
            b"duplicatePolicy",
            b"block",
            b"labels",
            [[b"currentLabel", b"currentData"]],
        ]
    )
    resp3 = TSInfo(
        {
            b"totalSamples": 1,
            b"retentionTime": 5,
            b"chunkSize": 4096,
            b"duplicatePolicy": b"block",
            b"labels": {b"currentLabel": b"currentData"},
        }
    )
    for info in (resp2, resp3):
        assert info.total_samples == 1
        assert info.retention_msecs == 5
        assert info.chunk_size == 4096
        assert info.duplicate_policy == "block"
        assert info.labels == {"currentLabel": "currentData"}


@pytest.mark.fixed_client
def test_info_resp3_keeps_null_labels():
    # Server-free: a decoded RESP3 map keeps a literal "null" label as is.
    info = TSInfo({"labels": {"null": "null", "currentLabel": "currentData"}})
    assert info.labels == {"null": "null", "currentLabel": "currentData"}


@pytest.mark.redismod
@pytest.mark.fixed_client
@pytest.mark.parametrize(
    "protocol", [pytest.param(2, id="resp2"), pytest.param(SENTINEL, id="default")]
)
def test_info_without_decode_responses(request, stack_url, protocol):
    kwargs = {"decode_responses": False, "from_url": stack_url}
    if protocol is not SENTINEL:
        kwargs["protocol"] = protocol
    client = _get_client(redis.Redis, request, **kwargs)
    client.flushdb()
    client.ts().create(1, retention_msecs=5, labels={"currentLabel": "currentData"})
    client.ts().add(1, 1, 1.5)
    info = client.ts().info(1)
    assert info.total_samples == 1
    assert info.retention_msecs == 5
    assert info.labels == {"currentLabel": "currentData"}


@pytest.mark.redismod
@skip_ifmodversion_lt("1.4.0", "timeseries")
@skip_if_server_version_lt("7.9.0")
def test_info_duplicate_policy(client):
    client.ts().create(1, retention_msecs=5, labels={"currentLabel": "currentData"})
    info = client.ts().info(1)
    assert_resp_response(
        client, "block", info.get("duplicate_policy"), info.get("duplicatePolicy")
    )

    client.ts().create("time-serie-2", duplicate_policy="min")
    info = client.ts().info("time-serie-2")
    assert_resp_response(
        client, "min", info.get("duplicate_policy"), info.get("duplicatePolicy")
    )


@pytest.mark.redismod
@skip_ifmodversion_lt("1.4.0", "timeseries")
@skip_if_server_version_gte("7.9.0")
def test_info_duplicate_policy_prior_redis_8(client):
    client.ts().create(1, retention_msecs=5, labels={"currentLabel": "currentData"})
    info = client.ts().info(1)
    assert_resp_response(
        client, None, info.get("duplicate_policy"), info.get("duplicatePolicy")
    )

    client.ts().create("time-serie-2", duplicate_policy="min")
    info = client.ts().info("time-serie-2")
    assert_resp_response(
        client, "min", info.get("duplicate_policy"), info.get("duplicatePolicy")
    )


@pytest.mark.redismod
@pytest.mark.onlynoncluster
def test_query_index(client):
    client.ts().create(1, labels={"Test": "This"})
    client.ts().create(2, labels={"Test": "This", "Taste": "That"})
    assert 2 == len(client.ts().queryindex(["Test=This"]))
    assert 1 == len(client.ts().queryindex(["Taste=That"]))
    assert_resp_response(client, client.ts().queryindex(["Taste=That"]), [2], ["2"])


@pytest.mark.redismod
@pytest.mark.onlynoncluster
@skip_if_server_version_lt("8.9.0")
def test_query_labels(client):
    client.ts().create(
        1, labels={"type": "sensor", "location": "LivingRoom", "sensortype": "temp"}
    )
    client.ts().create(
        2, labels={"type": "sensor", "location": "Kitchen", "sensortype": "temp"}
    )
    client.ts().create(3, labels={"type": "gauge", "location": "BedRoom"})

    # LABELS mode with a filter returns the union of label names across the
    # matching series, including the label used in the filter itself.
    labels = client.ts().querylabels(filters=["type=sensor"])
    assert isinstance(labels, set)
    assert sorted(labels) == ["location", "sensortype", "type"]

    # Omitting the filter queries all indexed series.
    assert sorted(client.ts().querylabels()) == [
        "location",
        "sensortype",
        "type",
    ]

    # A filter that matches nothing is a normal empty reply, not an error.
    assert client.ts().querylabels(filters=["type=missing"]) == set()

    # `filters` accepts any iterable, not just a list (a tuple and a single-pass
    # generator both work).
    assert sorted(client.ts().querylabels(filters=("type=sensor",))) == [
        "location",
        "sensortype",
        "type",
    ]
    assert sorted(client.ts().querylabels(filters=(f for f in ["type=sensor"]))) == [
        "location",
        "sensortype",
        "type",
    ]


@pytest.mark.redismod
@pytest.mark.onlynoncluster
@skip_if_server_version_lt("8.9.0")
def test_query_label_values(client):
    client.ts().create(1, labels={"type": "sensor", "location": "LivingRoom"})
    client.ts().create(2, labels={"type": "sensor", "location": "Kitchen"})
    client.ts().create(3, labels={"type": "gauge", "location": "BedRoom"})

    # VALUES mode returns the deduplicated union of a label's values.
    values = client.ts().querylabels("location", filters=["type=sensor"])
    assert isinstance(values, set)
    assert sorted(values) == ["Kitchen", "LivingRoom"]

    # Omitting the filter collects values across all indexed series.
    assert sorted(client.ts().querylabels("location")) == [
        "BedRoom",
        "Kitchen",
        "LivingRoom",
    ]

    # A label carried by no matching series yields an empty reply.
    assert client.ts().querylabels("nonexistent", filters=["type=sensor"]) == set()

    # Values are byte-exact strings and are never coerced to numbers.
    client.ts().create(4, labels={"type": "sensor", "code": "123"})
    assert client.ts().querylabels("code", filters=["type=sensor"]) == {"123"}


@pytest.mark.redismod
@pytest.mark.onlynoncluster
@skip_if_server_version_lt("8.9.0")
def test_query_labels_empty_filters_raises(client):
    # An explicitly empty filter collection is a local usage error; pass None
    # (omit the argument) to query all indexed series instead.
    with pytest.raises(redis.DataError):
        client.ts().querylabels(filters=[])
    with pytest.raises(redis.DataError):
        client.ts().querylabels("location", filters=[])


@pytest.mark.redismod
@pytest.mark.onlynoncluster
@skip_if_server_version_lt("8.9.0")
def test_query_labels_server_errors(client):
    # Server-side filter parsing errors surface unchanged as ResponseError.
    with pytest.raises(redis.ResponseError):
        client.ts().querylabels("location", filters=["badexpr"])


@pytest.mark.redismod
def test_pipeline(client):
    pipeline = client.ts().pipeline()
    pipeline.create("with_pipeline")
    for i in range(100):
        pipeline.add("with_pipeline", i, 1.1 * i)
    pipeline.execute()

    info = client.ts().info("with_pipeline")
    assert_resp_response(
        client, 99, info.get("last_timestamp"), info.get("lastTimestamp")
    )
    assert_resp_response(
        client, 100, info.get("total_samples"), info.get("totalSamples")
    )
    assert client.ts().get("with_pipeline")[1] == 99 * 1.1


@pytest.mark.redismod
def test_uncompressed(client):
    client.ts().create("compressed")
    client.ts().create("uncompressed", uncompressed=True)
    # Append the samples with a single TS.MADD instead of 2000 individual TS.ADD
    # round trips, which are slow enough to hit the test timeout when the target
    # deployment is remote.
    samples = []
    for i in range(1000):
        samples.append(("compressed", i, i))
        samples.append(("uncompressed", i, i))
    assert len(client.ts().madd(samples)) == 2000
    compressed_info = client.ts().info("compressed")
    uncompressed_info = client.ts().info("uncompressed")
    if is_resp2_connection(client):
        assert compressed_info.memory_usage < uncompressed_info.memory_usage
    else:
        assert compressed_info["memoryUsage"] < uncompressed_info["memoryUsage"]


@pytest.mark.redismod
@skip_ifmodversion_lt("1.12.0", "timeseries")
def test_create_with_insertion_filters(client):
    client.ts().create(
        "time-series-1",
        duplicate_policy="last",
        ignore_max_time_diff=5,
        ignore_max_val_diff=10.0,
    )
    assert 1000 == client.ts().add("time-series-1", 1000, 1.0)
    assert 1010 == client.ts().add("time-series-1", 1010, 11.0)
    assert 1010 == client.ts().add("time-series-1", 1013, 10.0)
    assert 1020 == client.ts().add("time-series-1", 1020, 11.5)
    assert 1021 == client.ts().add("time-series-1", 1021, 22.0)

    data_points = client.ts().range("time-series-1", "-", "+")
    assert_resp_response(
        client,
        data_points,
        [(1000, 1.0), (1010, 11.0), (1020, 11.5), (1021, 22.0)],
        [[1000, 1.0], [1010, 11.0], [1020, 11.5], [1021, 22.0]],
    )


@pytest.mark.redismod
@skip_ifmodversion_lt("1.12.0", "timeseries")
def test_create_with_insertion_filters_other_duplicate_policy(client):
    client.ts().create(
        "time-series-1",
        ignore_max_time_diff=5,
        ignore_max_val_diff=10.0,
    )
    assert 1000 == client.ts().add("time-series-1", 1000, 1.0)
    assert 1010 == client.ts().add("time-series-1", 1010, 11.0)
    # Still accepted because the duplicate_policy is not `last`.
    assert 1013 == client.ts().add("time-series-1", 1013, 10.0)

    data_points = client.ts().range("time-series-1", "-", "+")
    assert_resp_response(
        client,
        data_points,
        [(1000, 1.0), (1010, 11.0), (1013, 10)],
        [[1000, 1.0], [1010, 11.0], [1013, 10]],
    )


@pytest.mark.redismod
@skip_ifmodversion_lt("1.12.0", "timeseries")
def test_alter_with_insertion_filters(client):
    assert 1000 == client.ts().add("time-series-1", 1000, 1.0)
    assert 1010 == client.ts().add("time-series-1", 1010, 11.0)
    assert 1013 == client.ts().add("time-series-1", 1013, 10.0)

    client.ts().alter(
        "time-series-1",
        duplicate_policy="last",
        ignore_max_time_diff=5,
        ignore_max_val_diff=10.0,
    )

    assert 1013 == client.ts().add("time-series-1", 1015, 11.5)

    data_points = client.ts().range("time-series-1", "-", "+")
    assert_resp_response(
        client,
        data_points,
        [(1000, 1.0), (1010, 11.0), (1013, 10.0)],
        [[1000, 1.0], [1010, 11.0], [1013, 10.0]],
    )


@pytest.mark.redismod
@skip_ifmodversion_lt("1.12.0", "timeseries")
def test_add_with_insertion_filters(client):
    assert 1000 == client.ts().add(
        "time-series-1",
        1000,
        1.0,
        duplicate_policy="last",
        ignore_max_time_diff=5,
        ignore_max_val_diff=10.0,
    )

    assert 1000 == client.ts().add("time-series-1", 1004, 3.0)

    data_points = client.ts().range("time-series-1", "-", "+")
    assert_resp_response(client, data_points, [(1000, 1.0)], [[1000, 1.0]])


@pytest.mark.redismod
@skip_ifmodversion_lt("1.12.0", "timeseries")
def test_incrby_with_insertion_filters(client):
    assert 1000 == client.ts().incrby(
        "time-series-1",
        1.0,
        timestamp=1000,
        duplicate_policy="last",
        ignore_max_time_diff=5,
        ignore_max_val_diff=10.0,
    )

    assert 1000 == client.ts().incrby("time-series-1", 3.0, timestamp=1000)

    data_points = client.ts().range("time-series-1", "-", "+")
    assert_resp_response(client, data_points, [(1000, 1.0)], [[1000, 1.0]])

    assert 1000 == client.ts().incrby("time-series-1", 10.1, timestamp=1000)

    data_points = client.ts().range("time-series-1", "-", "+")
    assert_resp_response(client, data_points, [(1000, 11.1)], [[1000, 11.1]])


@pytest.mark.redismod
@skip_ifmodversion_lt("1.12.0", "timeseries")
def test_decrby_with_insertion_filters(client):
    assert 1000 == client.ts().decrby(
        "time-series-1",
        1.0,
        timestamp=1000,
        duplicate_policy="last",
        ignore_max_time_diff=5,
        ignore_max_val_diff=10.0,
    )

    assert 1000 == client.ts().decrby("time-series-1", 3.0, timestamp=1000)

    data_points = client.ts().range("time-series-1", "-", "+")
    assert_resp_response(client, data_points, [(1000, -1.0)], [[1000, -1.0]])

    assert 1000 == client.ts().decrby("time-series-1", 10.1, timestamp=1000)

    data_points = client.ts().range("time-series-1", "-", "+")
    assert_resp_response(client, data_points, [(1000, -11.1)], [[1000, -11.1]])


@pytest.mark.redismod
@skip_ifmodversion_lt("1.12.0", "timeseries")
def test_madd_with_insertion_filters(client):
    client.ts().create(
        "time-series-1",
        duplicate_policy="last",
        ignore_max_time_diff=5,
        ignore_max_val_diff=10.0,
    )
    assert 1010 == client.ts().add("time-series-1", 1010, 1.0)
    assert [1010, 1010, 1020, 1021] == client.ts().madd(
        [
            ("time-series-1", 1011, 11.0),
            ("time-series-1", 1013, 10.0),
            ("time-series-1", 1020, 2.0),
            ("time-series-1", 1021, 22.0),
        ]
    )

    data_points = client.ts().range("time-series-1", "-", "+")
    assert_resp_response(
        client,
        data_points,
        [(1010, 1.0), (1020, 2.0), (1021, 22.0)],
        [[1010, 1.0], [1020, 2.0], [1021, 22.0]],
    )


@pytest.mark.redismod
@skip_ifmodversion_lt("1.12.0", "timeseries")
def test_labels_with_insertion_filters_and_on_duplicate(client):
    ts = client.ts()
    filters = {
        "duplicate_policy": "last",
        "ignore_max_time_diff": 5,
        "ignore_max_val_diff": 10.0,
    }
    labels = {"sensor": "1"}

    assert ts.create("create", labels=labels, **filters)
    assert labels == ts.info("create")["labels"]
    assert 1000 == ts.add("create", 1000, 1.0)
    assert 1000 == ts.add("create", 1004, 3.0)

    assert ts.alter("create", labels={}, **filters)
    assert {} == ts.info("create")["labels"]
    assert 1000 == ts.add("create", 1003, 2.0)

    assert 1000 == ts.add("add", 1000, 1.0, labels=labels, **filters)
    assert labels == ts.info("add")["labels"]
    assert 1000 == ts.add("add", 1004, 3.0)

    assert 1000 == ts.add("add-dup", 1000, 1.0)
    assert 1000 == ts.add("add-dup", 1000, 2.0, labels=labels, on_duplicate="last")
    assert {} == ts.info("add-dup")["labels"]

    assert 1000 == ts.add("add-new", 1000, 1.0, labels=labels, on_duplicate="last")
    assert labels == ts.info("add-new")["labels"]

    assert 1000 == ts.incrby("incrby", 1.0, timestamp=1000, labels=labels, **filters)
    assert labels == ts.info("incrby")["labels"]

    assert 1000 == ts.decrby("decrby", 1.0, timestamp=1000, labels=labels, **filters)
    assert labels == ts.info("decrby")["labels"]


@pytest.mark.redismod
@skip_if_server_version_lt("8.5.0")
def test_range_with_count_nan_count_all_aggregators(client):
    client.ts().create(
        "temperature:2:32",
    )

    # Fill with values
    assert client.ts().add("temperature:2:32", 1000, "NaN") == 1000
    assert client.ts().add("temperature:2:32", 1003, 25) == 1003
    assert client.ts().add("temperature:2:32", 1005, "NaN") == 1005
    assert client.ts().add("temperature:2:32", 1006, "NaN") == 1006

    # Ensure we count only NaN values
    data_points = client.ts().range(
        "temperature:2:32",
        1000,
        1006,
        aggregation_type="countNan",
        bucket_size_msec=1000,
    )
    assert_resp_response(
        client,
        data_points,
        [(1000, 3)],
        [[1000, 3]],
    )

    # Ensure we count ALL values
    data_points = client.ts().range(
        "temperature:2:32",
        1000,
        1006,
        aggregation_type="countAll",
        bucket_size_msec=1000,
    )
    assert_resp_response(
        client,
        data_points,
        [(1000, 4)],
        [[1000, 4]],
    )


@pytest.mark.redismod
@skip_if_server_version_lt("8.5.0")
def test_rev_range_with_count_nan_count_all_aggregators(client):
    client.ts().create(
        "temperature:2:32",
    )

    # Fill with values
    assert client.ts().add("temperature:2:32", 1000, "NaN") == 1000
    assert client.ts().add("temperature:2:32", 1003, 25) == 1003
    assert client.ts().add("temperature:2:32", 1005, "NaN") == 1005
    assert client.ts().add("temperature:2:32", 1006, "NaN") == 1006

    # Ensure we count only NaN values
    data_points = client.ts().revrange(
        "temperature:2:32",
        1000,
        1006,
        aggregation_type="countNan",
        bucket_size_msec=1000,
    )
    assert_resp_response(
        client,
        data_points,
        [(1000, 3)],
        [[1000, 3]],
    )

    # Ensure we count ALL values
    data_points = client.ts().revrange(
        "temperature:2:32",
        1000,
        1006,
        aggregation_type="countAll",
        bucket_size_msec=1000,
    )
    assert_resp_response(
        client,
        data_points,
        [(1000, 4)],
        [[1000, 4]],
    )


@pytest.mark.redismod
@skip_if_server_version_lt("8.5.0")
def test_mrange_with_count_nan_count_all_aggregators(client):
    client.ts().create(
        "temperature:A",
        labels={"type": "temperature", "name": "A"},
    )
    client.ts().create(
        "temperature:B",
        labels={"type": "temperature", "name": "B"},
    )

    # Fill with values
    assert client.ts().madd(
        [("temperature:A", 1000, "NaN"), ("temperature:A", 1001, 27)]
    )
    assert client.ts().madd(
        [("temperature:B", 1000, "NaN"), ("temperature:B", 1001, 28)]
    )

    # Ensure we count only NaN values
    data_points = client.ts().mrange(
        1000,
        1001,
        aggregation_type="countNan",
        bucket_size_msec=1000,
        filters=["type=temperature"],
    )
    assert_resp_response(
        client,
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
    data_points = client.ts().mrange(
        1000,
        1001,
        aggregation_type="countAll",
        bucket_size_msec=1000,
        filters=["type=temperature"],
    )
    assert_resp_response(
        client,
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
def test_mrevrange_with_count_nan_count_all_aggregators(client):
    client.ts().create(
        "temperature:A",
        labels={"type": "temperature", "name": "A"},
    )
    client.ts().create(
        "temperature:B",
        labels={"type": "temperature", "name": "B"},
    )

    # Fill with values
    assert client.ts().madd(
        [("temperature:A", 1000, "NaN"), ("temperature:A", 1001, 27)]
    )
    assert client.ts().madd(
        [("temperature:B", 1000, "NaN"), ("temperature:B", 1001, 28)]
    )

    # Ensure we count only NaN values
    data_points = client.ts().mrevrange(
        1000,
        1001,
        aggregation_type="countNan",
        bucket_size_msec=1000,
        filters=["type=temperature"],
    )
    assert_resp_response(
        client,
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
    data_points = client.ts().mrevrange(
        1000,
        1001,
        aggregation_type="countAll",
        bucket_size_msec=1000,
        filters=["type=temperature"],
    )
    assert_resp_response(
        client,
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
def test_range_multiple_aggregators(client):
    """Test TS.RANGE with multiple aggregators (Redis 8.8+)."""
    client.ts().create("ts:multi_agg")
    client.ts().add("ts:multi_agg", 1000, 10)
    client.ts().add("ts:multi_agg", 1001, 20)
    client.ts().add("ts:multi_agg", 1002, 30)

    result = client.ts().range(
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
def test_revrange_multiple_aggregators(client):
    """Test TS.REVRANGE with multiple aggregators (Redis 8.8+)."""
    client.ts().create("ts:multi_agg")
    client.ts().add("ts:multi_agg", 1000, 10)
    client.ts().add("ts:multi_agg", 1001, 20)
    client.ts().add("ts:multi_agg", 1002, 30)

    result = client.ts().revrange(
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
def test_mrange_multiple_aggregators(client):
    """Test TS.MRANGE with multiple aggregators (Redis 8.8+)."""
    client.ts().create("ts:multi_agg_a", labels={"type": "test_multi_agg"})
    client.ts().add("ts:multi_agg_a", 1000, 10)
    client.ts().add("ts:multi_agg_a", 1001, 20)

    result = client.ts().mrange(
        1000,
        1001,
        filters=["type=test_multi_agg"],
        aggregation_type=["min", "max"],
        bucket_size_msec=10,
    )
    if expects_resp2_shape(client):
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
def test_mrevrange_multiple_aggregators(client):
    """Test TS.MREVRANGE with multiple aggregators (Redis 8.8+)."""
    client.ts().create("ts:multi_agg_b", labels={"type": "test_multi_agg_rev"})
    client.ts().add("ts:multi_agg_b", 1000, 10)
    client.ts().add("ts:multi_agg_b", 1001, 20)

    result = client.ts().mrevrange(
        1000,
        1001,
        filters=["type=test_multi_agg_rev"],
        aggregation_type=["min", "max"],
        bucket_size_msec=10,
    )
    if expects_resp2_shape(client):
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
def test_mrange_groupby_multiple_aggregators_raises(client):
    """Test that GROUPBY with multiple aggregators raises DataError."""
    client.ts().create("ts:gb_test", labels={"type": "test_gb"})

    with pytest.raises(redis.exceptions.DataError, match="GROUPBY is not allowed"):
        client.ts().mrange(
            0,
            100,
            filters=["type=test_gb"],
            aggregation_type=["min", "max"],
            bucket_size_msec=10,
            groupby="type",
            reduce="max",
        )


@pytest.mark.redismod
def test_mrevrange_groupby_multiple_aggregators_raises(client):
    """Test that GROUPBY with multiple aggregators raises DataError."""
    client.ts().create("ts:gb_test2", labels={"type": "test_gb2"})

    with pytest.raises(redis.exceptions.DataError, match="GROUPBY is not allowed"):
        client.ts().mrevrange(
            0,
            100,
            filters=["type=test_gb2"],
            aggregation_type=["min", "max"],
            bucket_size_msec=10,
            groupby="type",
            reduce="max",
        )


def _assert_nrange_rows(actual, expected):
    """Compare TS.NRANGE responses treating NaN cells as equal by position."""
    assert len(actual) == len(expected), (actual, expected)
    for (a_ts, a_vals), (e_ts, e_vals) in zip(actual, expected):
        assert a_ts == e_ts
        assert len(a_vals) == len(e_vals)
        for a, e in zip(a_vals, e_vals):
            if isinstance(e, float) and math.isnan(e):
                assert math.isnan(a), (actual, expected)
            else:
                assert a == e, (actual, expected)


@pytest.mark.redismod
@skip_if_server_version_lt("8.9.0")
def test_nrange_raw_rows_and_missing_cells(client):
    nan = float("nan")
    client.ts().add("{s}:a", 10, 1.0)
    client.ts().add("{s}:a", 20, 2.0)
    client.ts().add("{s}:b", 20, 3.0)
    client.ts().add("{s}:b", 30, 4.0)

    # Forward: one row per distinct timestamp, values follow key order,
    # missing cells are NaN.
    res = client.ts().nrange(["{s}:a", "{s}:b"], from_time="-", to_time="+")
    _assert_nrange_rows(res, [[10, [1.0, nan]], [20, [2.0, 3.0]], [30, [nan, 4.0]]])
    assert all(len(row[1]) == 2 for row in res)


@pytest.mark.redismod
@skip_if_server_version_lt("8.9.0")
def test_nrange_preserves_duplicate_keys(client):
    client.ts().add("{s}:a", 10, 1.0)
    client.ts().add("{s}:a", 20, 2.0)

    # Duplicate keys produce repeated value columns and are not deduplicated.
    res = client.ts().nrange(["{s}:a", "{s}:a"], from_time="-", to_time="+")
    _assert_nrange_rows(res, [[10, [1.0, 1.0]], [20, [2.0, 2.0]]])


@pytest.mark.redismod
@skip_if_server_version_lt("8.9.0")
def test_nrange_aggregation_one_per_key(client):
    for ts, val in [(0, 1.0), (1, 2.0), (10, 3.0), (11, 4.0)]:
        client.ts().add("{s}:a", ts, val)
    for ts, val in [(0, 5.0), (1, 6.0), (10, 7.0), (11, 8.0)]:
        client.ts().add("{s}:b", ts, val)

    # One aggregator per key: max for {s}:a, min for {s}:b.
    res = client.ts().nrange(
        ["{s}:a", "{s}:b"],
        from_time=0,
        to_time=20,
        aggregators=["max", "min"],
        bucket_size_msec=10,
    )
    _assert_nrange_rows(res, [[0, [2.0, 5.0]], [10, [4.0, 7.0]]])


@pytest.mark.redismod
@skip_if_server_version_lt("8.9.0")
def test_nrange_multiple_aggregators_per_key_layout(client):
    # A per-key spec may list several aggregators. That key then contributes one
    # value per aggregator (in spec order), and each row's value array is the
    # per-key blocks concatenated in key order: here [a_avg, a_max, b_sum].
    # key a: bucket [0,10) avg=2 max=3; bucket [10,20) avg=15 max=20
    for ts, val in [(0, 1.0), (5, 3.0), (10, 10.0), (15, 20.0)]:
        client.ts().add("{s}:a", ts, val)
    # key b: bucket [0,10) sum=10; bucket [10,20) sum=7
    for ts, val in [(0, 5.0), (5, 5.0), (10, 7.0)]:
        client.ts().add("{s}:b", ts, val)

    res = client.ts().nrange(
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
def test_nrange_count_limits_rows(client):
    for ts in range(5):
        client.ts().add("{s}:a", ts, ts)
    assert len(client.ts().nrange(["{s}:a"], from_time="-", to_time="+", count=2)) == 2


@pytest.mark.redismod
@skip_if_server_version_lt("8.9.0")
def test_nrange_empty_result(client):
    client.ts().create("{s}:a")
    assert client.ts().nrange(["{s}:a"], from_time="-", to_time="+") == []


@pytest.mark.redismod
@skip_if_server_version_lt("8.9.0")
def test_nrange_single_aggregator_multi_key_raises(client):
    # A single aggregator is NOT broadcast across keys (RedisTimeSeries PR
    # #2079): with more than one key, each needs its own spec token. Raised
    # client-side before any server round trip.
    with pytest.raises(
        redis.exceptions.DataError, match="one aggregation spec per key"
    ):
        client.ts().nrange(
            ["{s}:a", "{s}:b"],
            from_time=0,
            to_time=20,
            aggregators="max",
            bucket_size_msec=10,
        )


@pytest.mark.redismod
@skip_if_server_version_lt("8.9.0")
def test_nrange_aggregator_count_mismatch_raises(client):
    # A spec list whose length differs from the key count is invalid.
    with pytest.raises(
        redis.exceptions.DataError, match="one aggregation spec per key"
    ):
        client.ts().nrange(
            ["{s}:a", "{s}:b"],
            from_time=0,
            to_time=1,
            aggregators=["min"],
            bucket_size_msec=10,
        )


@pytest.mark.fixed_client
def test_nrange_aggregation_builds_one_spec_token_per_key():
    # Server-free: each per-key spec is its own token and may hold multiple
    # comma-separated aggregators -> AGGREGATION avg,max sum 1000 for two keys
    # (RedisTimeSeries PR #2079; no broadcast, no single comma-joined token).
    params = []
    TimeSeriesCommands._append_n_aggregation(params, ["avg,max", "sum"], 1000, 2)
    assert params == ["AGGREGATION", "avg,max", "sum", 1000]


@pytest.mark.fixed_client
def test_nrange_aggregation_single_key_single_spec():
    # Server-free: a bare string is the single-key spec (may be comma-joined).
    params = []
    TimeSeriesCommands._append_n_aggregation(params, "avg,max", 1000, 1)
    assert params == ["AGGREGATION", "avg,max", 1000]


@pytest.mark.redismod
@skip_if_server_version_lt("8.9.0")
def test_nrange_empty_keys_raises(client):
    with pytest.raises(redis.exceptions.DataError, match="At least one key"):
        client.ts().nrange([], from_time=0, to_time=1)


@pytest.mark.redismod
@skip_if_server_version_lt("8.9.0")
def test_nrevrange_reverse_order(client):
    nan = float("nan")
    client.ts().add("{s}:a", 10, 1.0)
    client.ts().add("{s}:a", 20, 2.0)
    client.ts().add("{s}:b", 20, 3.0)
    client.ts().add("{s}:b", 30, 4.0)

    # Reverse: rows in decreasing-timestamp order, same NaN cells.
    res = client.ts().nrevrange(["{s}:a", "{s}:b"], from_time="-", to_time="+")
    _assert_nrange_rows(res, [[30, [nan, 4.0]], [20, [2.0, 3.0]], [10, [1.0, nan]]])


@pytest.mark.redismod
@skip_if_server_version_lt("8.9.0")
def test_nrevrange_aggregation_one_per_key(client):
    for ts, val in [(0, 1.0), (1, 2.0), (10, 3.0), (11, 4.0)]:
        client.ts().add("{s}:a", ts, val)
    for ts, val in [(0, 5.0), (1, 6.0), (10, 7.0), (11, 8.0)]:
        client.ts().add("{s}:b", ts, val)

    # One aggregator per key, rows in decreasing-timestamp order.
    res = client.ts().nrevrange(
        ["{s}:a", "{s}:b"],
        from_time=0,
        to_time=20,
        aggregators=["max", "min"],
        bucket_size_msec=10,
    )
    _assert_nrange_rows(res, [[10, [4.0, 7.0]], [0, [2.0, 5.0]]])


@pytest.mark.redismod
@skip_if_server_version_lt("8.9.0")
def test_nrevrange_count_keeps_highest_timestamps(client):
    for ts in range(5):
        client.ts().add("{s}:a", ts, ts)
    # COUNT is applied after the merge in decreasing-timestamp order, so the
    # highest timestamps are kept (the opposite end from nrange).
    res = client.ts().nrange(["{s}:a"], from_time="-", to_time="+", count=2)
    _assert_nrange_rows(res, [[0, [0.0]], [1, [1.0]]])
    res = client.ts().nrevrange(["{s}:a"], from_time="-", to_time="+", count=2)
    _assert_nrange_rows(res, [[4, [4.0]], [3, [3.0]]])
