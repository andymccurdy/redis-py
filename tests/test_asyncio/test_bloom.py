from math import inf, isnan

import pytest
import pytest_asyncio
import redis.asyncio as redis
from redis.exceptions import RedisError
from tests.conftest import (
    assert_resp_response,
    expected_response_shape,
    skip_ifmodversion_lt,
)


@pytest_asyncio.fixture()
async def decoded_r(create_redis, stack_url):
    return await create_redis(decode_responses=True, url=stack_url)


def intlist(obj):
    return [int(v) for v in obj]


@pytest.mark.redismod
async def test_create(decoded_r: redis.Redis):
    """Test CREATE/RESERVE calls"""
    assert await decoded_r.bf().create("bloom", 0.01, 1000)
    assert await decoded_r.bf().create("bloom_e", 0.01, 1000, expansion=1)
    assert await decoded_r.bf().create("bloom_ns", 0.01, 1000, noScale=True)
    assert await decoded_r.cf().create("cuckoo", 1000)
    assert await decoded_r.cf().create("cuckoo_e", 1000, expansion=1)
    assert await decoded_r.cf().create("cuckoo_bs", 1000, bucket_size=4)
    assert await decoded_r.cf().create("cuckoo_mi", 1000, max_iterations=10)
    assert await decoded_r.cms().initbydim("cmsDim", 100, 5)
    assert await decoded_r.cms().initbyprob("cmsProb", 0.01, 0.01)
    assert await decoded_r.topk().reserve("topk", 5, 100, 5, 0.9)


@pytest.mark.experimental
@pytest.mark.redismod
async def test_tdigest_create(decoded_r: redis.Redis):
    assert await decoded_r.tdigest().create("tDigest", 100)


@pytest.mark.redismod
async def test_bf_add(decoded_r: redis.Redis):
    assert await decoded_r.bf().create("bloom", 0.01, 1000)
    assert 1 == await decoded_r.bf().add("bloom", "foo")
    assert 0 == await decoded_r.bf().add("bloom", "foo")
    assert [0] == intlist(await decoded_r.bf().madd("bloom", "foo"))
    assert [0, 1] == await decoded_r.bf().madd("bloom", "foo", "bar")
    assert [0, 0, 1] == await decoded_r.bf().madd("bloom", "foo", "bar", "baz")
    assert 1 == await decoded_r.bf().exists("bloom", "foo")
    assert 0 == await decoded_r.bf().exists("bloom", "noexist")
    assert [1, 0] == intlist(await decoded_r.bf().mexists("bloom", "foo", "noexist"))


@pytest.mark.redismod
async def test_bf_insert(decoded_r: redis.Redis):
    assert await decoded_r.bf().create("bloom", 0.01, 1000)
    assert [1] == intlist(await decoded_r.bf().insert("bloom", ["foo"]))
    assert [0, 1] == intlist(await decoded_r.bf().insert("bloom", ["foo", "bar"]))
    assert [1] == intlist(await decoded_r.bf().insert("captest", ["foo"], capacity=10))
    assert [1] == intlist(await decoded_r.bf().insert("errtest", ["foo"], error=0.01))
    assert 1 == await decoded_r.bf().exists("bloom", "foo")
    assert 0 == await decoded_r.bf().exists("bloom", "noexist")
    assert [1, 0] == intlist(await decoded_r.bf().mexists("bloom", "foo", "noexist"))
    info = await decoded_r.bf().info("bloom")
    assert_resp_response(
        decoded_r,
        2,
        info.get("insertedNum"),
        info.get("Number of items inserted"),
        info.get("insertedNum"),
    )
    assert_resp_response(
        decoded_r,
        1000,
        info.get("capacity"),
        info.get("Capacity"),
        info.get("capacity"),
    )
    assert_resp_response(
        decoded_r,
        1,
        info.get("filterNum"),
        info.get("Number of filters"),
        info.get("filterNum"),
    )


@pytest.mark.redismod
async def test_bf_false_flags_are_not_sent(decoded_r: redis.Redis):
    # noScale=False and noCreate=False must behave like omitting the flag,
    # not send NONSCALING / NOCREATE.
    assert await decoded_r.bf().create("bloom", 0.01, 1, noScale=False)
    assert [1, 1, 1] == intlist(await decoded_r.bf().madd("bloom", "a", "b", "c"))
    assert [1, 1, 1] == intlist(
        await decoded_r.bf().insert(
            "bloom_ins", ["a", "b", "c"], capacity=1, noScale=False
        )
    )
    assert [1] == intlist(
        await decoded_r.bf().insert("bloom_new", ["a"], noCreate=False)
    )
    assert [1] == await decoded_r.cf().insert("cuckoo_new", ["a"], nocreate=False)
    assert [1] == await decoded_r.cf().insertnx("cuckoo_nx_new", ["a"], nocreate=False)


@pytest.mark.redismod
async def test_bf_scandump_and_loadchunk(decoded_r: redis.Redis):
    # Store a filter
    await decoded_r.bf().create("myBloom", "0.0001", "1000")

    # test is probabilistic and might fail. It is OK to change variables if
    # certain to not break anything
    async def do_verify():
        pipe = decoded_r.pipeline(transaction=False)
        bf = pipe.bf()
        for x in range(1000):
            bf.add("myBloom", x)
            bf.exists("myBloom", x)
            bf.exists("myBloom", f"nonexist_{x}")
        replies = await pipe.execute()
        res = 0
        for exists, false_positive in zip(replies[1::3], replies[2::3]):
            assert exists
            res += false_positive
        assert res < 5

    await do_verify()
    cmds = []

    cur = await decoded_r.bf().scandump("myBloom", 0)
    first = cur[0]
    cmds.append(cur)

    while True:
        cur = await decoded_r.bf().scandump("myBloom", first)
        first = cur[0]
        if first == 0:
            break
        else:
            cmds.append(cur)
    prev_info = await decoded_r.bf().execute_command("bf.debug", "myBloom")

    # Remove the filter
    await decoded_r.bf().client.delete("myBloom")

    # Now, load all the commands:
    for cmd in cmds:
        await decoded_r.bf().loadchunk("myBloom", *cmd)

    cur_info = await decoded_r.bf().execute_command("bf.debug", "myBloom")
    assert prev_info == cur_info
    await do_verify()

    await decoded_r.bf().client.delete("myBloom")


@pytest.mark.redismod
async def test_cf_scandump_and_loadchunk(decoded_r: redis.Redis):
    # The client decodes responses, but the dumped chunks are binary.
    await decoded_r.cf().create("myCuckoo", 1000)
    for x in range(100):
        await decoded_r.cf().add("myCuckoo", x)

    chunks = []
    it = 0
    while True:
        it, data = await decoded_r.cf().scandump("myCuckoo", it)
        if it == 0:
            break
        assert isinstance(data, bytes)
        chunks.append((it, data))

    await decoded_r.delete("myCuckoo")
    for it, data in chunks:
        await decoded_r.cf().loadchunk("myCuckoo", it, data)

    assert all(await decoded_r.cf().mexists("myCuckoo", *range(100)))


@pytest.mark.redismod
async def test_bf_info(decoded_r: redis.Redis):
    expansion = 4
    # Store a filter
    await decoded_r.bf().create("nonscaling", "0.0001", "1000", noScale=True)
    info = await decoded_r.bf().info("nonscaling")
    assert_resp_response(
        decoded_r,
        None,
        info.get("expansionRate"),
        info.get("Expansion rate"),
        info.get("expansionRate"),
    )

    await decoded_r.bf().create("expanding", "0.0001", "1000", expansion=expansion)
    info = await decoded_r.bf().info("expanding")
    assert_resp_response(
        decoded_r,
        4,
        info.get("expansionRate"),
        info.get("Expansion rate"),
        info.get("expansionRate"),
    )

    try:
        # noScale mean no expansion
        await decoded_r.bf().create(
            "myBloom", "0.0001", "1000", expansion=expansion, noScale=True
        )
        assert False
    except RedisError:
        assert True


@pytest.mark.redismod
async def test_bf_card(decoded_r: redis.Redis):
    # return 0 if the key does not exist
    assert await decoded_r.bf().card("not_exist") == 0

    # Store a filter
    assert await decoded_r.bf().add("bf1", "item_foo") == 1
    assert await decoded_r.bf().card("bf1") == 1

    # Error when key is of a type other than Bloom filtedecoded_r.
    with pytest.raises(redis.ResponseError):
        await decoded_r.set("setKey", "value")
        await decoded_r.bf().card("setKey")


@pytest.mark.redismod
async def test_cf_add_and_insert(decoded_r: redis.Redis):
    assert await decoded_r.cf().create("cuckoo", 1000)
    assert await decoded_r.cf().add("cuckoo", "filter")
    assert not await decoded_r.cf().addnx("cuckoo", "filter")
    assert 1 == await decoded_r.cf().addnx("cuckoo", "newItem")
    assert [1] == await decoded_r.cf().insert("captest", ["foo"])
    assert [1] == await decoded_r.cf().insert("captest", ["foo"], capacity=1000)
    assert [1] == await decoded_r.cf().insertnx("captest", ["bar"])
    assert [1] == await decoded_r.cf().insertnx("captest", ["food"], nocreate="1")
    assert [0, 0, 1] == await decoded_r.cf().insertnx("captest", ["foo", "bar", "baz"])
    assert [0] == await decoded_r.cf().insertnx("captest", ["bar"], capacity=1000)
    assert [1] == await decoded_r.cf().insert("empty1", ["foo"], capacity=1000)
    assert [1] == await decoded_r.cf().insertnx("empty2", ["bar"], capacity=1000)
    info = await decoded_r.cf().info("captest")
    assert_resp_response(
        decoded_r,
        5,
        info.get("insertedNum"),
        info.get("Number of items inserted"),
        info.get("insertedNum"),
    )
    assert_resp_response(
        decoded_r,
        0,
        info.get("deletedNum"),
        info.get("Number of items deleted"),
        info.get("deletedNum"),
    )
    assert_resp_response(
        decoded_r,
        1,
        info.get("filterNum"),
        info.get("Number of filters"),
        info.get("filterNum"),
    )


@pytest.mark.redismod
async def test_cf_exists_and_del(decoded_r: redis.Redis):
    assert await decoded_r.cf().create("cuckoo", 1000)
    assert await decoded_r.cf().add("cuckoo", "filter")
    assert await decoded_r.cf().exists("cuckoo", "filter")
    assert not await decoded_r.cf().exists("cuckoo", "notexist")
    assert 1 == await decoded_r.cf().count("cuckoo", "filter")
    assert 0 == await decoded_r.cf().count("cuckoo", "notexist")
    assert await decoded_r.cf().delete("cuckoo", "filter")
    assert 0 == await decoded_r.cf().count("cuckoo", "filter")


@pytest.mark.redismod
async def test_cms(decoded_r: redis.Redis):
    assert await decoded_r.cms().initbydim("dim", 1000, 5)
    assert await decoded_r.cms().initbyprob("prob", 0.01, 0.01)
    assert await decoded_r.cms().incrby("dim", ["foo"], [5])
    assert [0] == await decoded_r.cms().query("dim", "notexist")
    assert [5] == await decoded_r.cms().query("dim", "foo")
    assert [10, 15] == await decoded_r.cms().incrby("dim", ["foo", "bar"], [5, 15])
    assert [10, 15] == await decoded_r.cms().query("dim", "foo", "bar")
    info = await decoded_r.cms().info("dim")
    assert info["width"]
    assert 1000 == info["width"]
    assert 5 == info["depth"]
    assert 25 == info["count"]


@pytest.mark.onlynoncluster
@pytest.mark.redismod
async def test_cms_merge(decoded_r: redis.Redis):
    assert await decoded_r.cms().initbydim("A", 1000, 5)
    assert await decoded_r.cms().initbydim("B", 1000, 5)
    assert await decoded_r.cms().initbydim("C", 1000, 5)
    assert await decoded_r.cms().incrby("A", ["foo", "bar", "baz"], [5, 3, 9])
    assert await decoded_r.cms().incrby("B", ["foo", "bar", "baz"], [2, 3, 1])
    assert [5, 3, 9] == await decoded_r.cms().query("A", "foo", "bar", "baz")
    assert [2, 3, 1] == await decoded_r.cms().query("B", "foo", "bar", "baz")
    assert await decoded_r.cms().merge("C", 2, ["A", "B"])
    assert [7, 6, 10] == await decoded_r.cms().query("C", "foo", "bar", "baz")
    assert await decoded_r.cms().merge("C", 2, ["A", "B"], ["1", "2"])
    assert [9, 9, 11] == await decoded_r.cms().query("C", "foo", "bar", "baz")
    assert await decoded_r.cms().merge("C", 2, ["A", "B"], ["2", "3"])
    assert [16, 15, 21] == await decoded_r.cms().query("C", "foo", "bar", "baz")


@pytest.mark.redismod
@skip_ifmodversion_lt("8.11.0", "bf")
async def test_cms_cell_size(decoded_r: redis.Redis):
    for cell_size in (1, 2, 4, 8):
        assert await decoded_r.cms().initbydim(
            f"cell_dim{cell_size}", 1000, 5, cell_size=cell_size
        )
        assert (
            cell_size
            == (await decoded_r.cms().info(f"cell_dim{cell_size}"))["cell_size"]
        )
        assert await decoded_r.cms().initbyprob(
            f"cell_prob{cell_size}", 0.01, 0.01, cell_size=cell_size
        )
        assert (
            cell_size
            == (await decoded_r.cms().info(f"cell_prob{cell_size}"))["cell_size"]
        )

    # the server uses 4-byte counters when CELL_SIZE is omitted
    assert await decoded_r.cms().initbydim("cell_default", 1000, 5)
    info = await decoded_r.cms().info("cell_default")
    assert 1000 == info["width"]
    assert 5 == info["depth"]
    assert 0 == info["count"]
    assert 4 == info["cell_size"]

    # only 1, 2, 4 and 8 are valid cell sizes
    with pytest.raises(redis.ResponseError):
        await decoded_r.cms().initbydim("cell_bad", 1000, 5, cell_size=3)
    with pytest.raises(redis.ResponseError):
        await decoded_r.cms().initbyprob("cell_bad", 0.01, 0.01, cell_size=16)


@pytest.mark.redismod
async def test_topk(decoded_r: redis.Redis):
    # test list with empty buckets
    assert await decoded_r.topk().reserve("topk", 3, 50, 4, 0.9)
    assert [
        None,
        None,
        None,
        "A",
        "C",
        "D",
        None,
        None,
        "E",
        None,
        "B",
        "C",
        None,
        None,
        None,
        "D",
        None,
    ] == await decoded_r.topk().add(
        "topk",
        "A",
        "B",
        "C",
        "D",
        "E",
        "A",
        "A",
        "B",
        "C",
        "G",
        "D",
        "B",
        "D",
        "A",
        "E",
        "E",
        1,
    )
    assert [1, 1, 0, 0, 1, 0, 0] == await decoded_r.topk().query(
        "topk", "A", "B", "C", "D", "E", "F", "G"
    )
    with pytest.deprecated_call():
        assert [4, 3, 2, 3, 3, 0, 1] == await decoded_r.topk().count(
            "topk", "A", "B", "C", "D", "E", "F", "G"
        )

    # test full list
    assert await decoded_r.topk().reserve("topklist", 3, 50, 3, 0.9)
    assert await decoded_r.topk().add(
        "topklist",
        "A",
        "B",
        "C",
        "D",
        "E",
        "A",
        "A",
        "B",
        "C",
        "G",
        "D",
        "B",
        "D",
        "A",
        "E",
        "E",
    )
    assert ["A", "B", "E"] == await decoded_r.topk().list("topklist")
    res = await decoded_r.topk().list("topklist", withcount=True)
    assert ["A", 4, "B", 3, "E", 3] == res
    info = await decoded_r.topk().info("topklist")
    assert 3 == info["k"]
    assert 50 == info["width"]
    assert 3 == info["depth"]
    assert 0.9 == round(float(info["decay"]), 1)


@pytest.mark.redismod
async def test_topk_list_with_special_words(decoded_r: redis.Redis):
    # test list with empty buckets
    assert await decoded_r.topk().reserve("topklist:specialwords", 5, 20, 4, 0.9)
    assert await decoded_r.topk().add(
        "topklist:specialwords",
        "infinity",
        "B",
        "nan",
        "D",
        "-infinity",
        "infinity",
        "infinity",
        "B",
        "nan",
        "G",
        "D",
        "B",
        "D",
        "infinity",
        "-infinity",
        "-infinity",
    )
    assert ["infinity", "B", "D", "-infinity", "nan"] == await decoded_r.topk().list(
        "topklist:specialwords"
    )


@pytest.mark.redismod
async def test_topk_incrby(decoded_r: redis.Redis):
    await decoded_r.flushdb()
    assert await decoded_r.topk().reserve("topk", 3, 10, 3, 1)
    assert [None, None, None] == await decoded_r.topk().incrby(
        "topk", ["bar", "baz", "42"], [3, 6, 2]
    )
    res = await decoded_r.topk().incrby("topk", ["42", "xyzzy"], [8, 4])
    assert [None, "bar"] == res
    with pytest.deprecated_call():
        assert [3, 6, 10, 4, 0] == await decoded_r.topk().count(
            "topk", "bar", "baz", "42", "xyzzy", 4
        )


@pytest.mark.experimental
@pytest.mark.redismod
async def test_tdigest_reset(decoded_r: redis.Redis):
    assert await decoded_r.tdigest().create("tDigest", 10)
    # reset on empty histogram
    assert await decoded_r.tdigest().reset("tDigest")
    # insert data-points into sketch
    assert await decoded_r.tdigest().add("tDigest", list(range(10)))

    assert await decoded_r.tdigest().reset("tDigest")
    # assert we have 0 unmerged nodes
    info = await decoded_r.tdigest().info("tDigest")
    assert_resp_response(
        decoded_r,
        0,
        info.get("unmerged_nodes"),
        info.get("Unmerged nodes"),
        info.get("unmerged_nodes"),
    )


@pytest.mark.onlynoncluster
@pytest.mark.redismod
async def test_tdigest_merge(decoded_r: redis.Redis):
    assert await decoded_r.tdigest().create("to-tDigest", 10)
    assert await decoded_r.tdigest().create("from-tDigest", 10)
    # insert data-points into sketch
    assert await decoded_r.tdigest().add("from-tDigest", [1.0] * 10)
    assert await decoded_r.tdigest().add("to-tDigest", [2.0] * 10)
    # merge from-tdigest into to-tdigest
    assert await decoded_r.tdigest().merge("to-tDigest", 1, "from-tDigest")
    # we should now have 110 weight on to-histogram
    info = await decoded_r.tdigest().info("to-tDigest")
    if expected_response_shape(decoded_r) == "legacy_resp3":
        # Native RESP3 map from the server (no module callback registered for
        # ``protocol=3`` with ``legacy_responses=True``); keys are title-case.
        assert 20 == float(info["Merged weight"]) + float(info["Unmerged weight"])
    else:
        # ``TDigestInfo`` wrapper exposes snake_case keys for both the
        # ``legacy_resp2`` and ``unified`` shapes.
        assert 20 == float(info["merged_weight"]) + float(info["unmerged_weight"])
    # test override
    assert await decoded_r.tdigest().create("from-override", 10)
    assert await decoded_r.tdigest().create("from-override-2", 10)
    assert await decoded_r.tdigest().add("from-override", [3.0] * 10)
    assert await decoded_r.tdigest().add("from-override-2", [4.0] * 10)
    assert await decoded_r.tdigest().merge(
        "to-tDigest", 2, "from-override", "from-override-2", override=True
    )
    assert 3.0 == await decoded_r.tdigest().min("to-tDigest")
    assert 4.0 == await decoded_r.tdigest().max("to-tDigest")


@pytest.mark.experimental
@pytest.mark.redismod
async def test_tdigest_min_and_max(decoded_r: redis.Redis):
    assert await decoded_r.tdigest().create("tDigest", 100)
    # insert data-points into sketch
    assert await decoded_r.tdigest().add("tDigest", [1, 2, 3])
    # min/max
    assert 3 == await decoded_r.tdigest().max("tDigest")
    assert 1 == await decoded_r.tdigest().min("tDigest")


@pytest.mark.experimental
@pytest.mark.redismod
@skip_ifmodversion_lt("2.4.0", "bf")
async def test_tdigest_quantile(decoded_r: redis.Redis):
    assert await decoded_r.tdigest().create("tDigest", 500)
    # insert data-points into sketch
    assert await decoded_r.tdigest().add(
        "tDigest", list([x * 0.01 for x in range(1, 10000)])
    )
    # assert min min/max have same result as quantile 0 and 1
    assert (
        await decoded_r.tdigest().max("tDigest")
        == (await decoded_r.tdigest().quantile("tDigest", 1))[0]
    )
    assert (
        await decoded_r.tdigest().min("tDigest")
        == (await decoded_r.tdigest().quantile("tDigest", 0.0))[0]
    )

    assert 1.0 == round((await decoded_r.tdigest().quantile("tDigest", 0.01))[0], 2)
    assert 99.0 == round((await decoded_r.tdigest().quantile("tDigest", 0.99))[0], 2)

    # test multiple quantiles
    assert await decoded_r.tdigest().create("t-digest", 100)
    assert await decoded_r.tdigest().add("t-digest", [1, 2, 3, 4, 5])
    res = await decoded_r.tdigest().quantile("t-digest", 0.5, 0.8)
    assert [3.0, 5.0] == res


@pytest.mark.experimental
@pytest.mark.redismod
async def test_tdigest_cdf(decoded_r: redis.Redis):
    assert await decoded_r.tdigest().create("tDigest", 100)
    # insert data-points into sketch
    assert await decoded_r.tdigest().add("tDigest", list(range(1, 10)))
    assert 0.1 == round((await decoded_r.tdigest().cdf("tDigest", 1.0))[0], 1)
    assert 0.9 == round((await decoded_r.tdigest().cdf("tDigest", 9.0))[0], 1)
    res = await decoded_r.tdigest().cdf("tDigest", 1.0, 9.0)
    assert [0.1, 0.9] == [round(x, 1) for x in res]


@pytest.mark.experimental
@pytest.mark.redismod
@skip_ifmodversion_lt("2.4.0", "bf")
async def test_tdigest_trimmed_mean(decoded_r: redis.Redis):
    assert await decoded_r.tdigest().create("tDigest", 100)
    # insert data-points into sketch
    assert await decoded_r.tdigest().add("tDigest", list(range(1, 10)))
    assert 5 == await decoded_r.tdigest().trimmed_mean("tDigest", 0.1, 0.9)
    assert 4.5 == await decoded_r.tdigest().trimmed_mean("tDigest", 0.4, 0.5)


@pytest.mark.experimental
@pytest.mark.redismod
async def test_tdigest_rank(decoded_r: redis.Redis):
    assert await decoded_r.tdigest().create("t-digest", 500)
    assert await decoded_r.tdigest().add("t-digest", list(range(0, 20)))
    assert -1 == (await decoded_r.tdigest().rank("t-digest", -1))[0]
    assert 0 == (await decoded_r.tdigest().rank("t-digest", 0))[0]
    assert 10 == (await decoded_r.tdigest().rank("t-digest", 10))[0]
    assert [-1, 20, 9] == await decoded_r.tdigest().rank("t-digest", -20, 20, 9)


@pytest.mark.experimental
@pytest.mark.redismod
async def test_tdigest_revrank(decoded_r: redis.Redis):
    assert await decoded_r.tdigest().create("t-digest", 500)
    assert await decoded_r.tdigest().add("t-digest", list(range(0, 20)))
    assert -1 == (await decoded_r.tdigest().revrank("t-digest", 20))[0]
    assert 19 == (await decoded_r.tdigest().revrank("t-digest", 0))[0]
    assert [-1, 19, 9] == await decoded_r.tdigest().revrank("t-digest", 21, 0, 10)


@pytest.mark.experimental
@pytest.mark.redismod
async def test_tdigest_byrank(decoded_r: redis.Redis):
    assert await decoded_r.tdigest().create("t-digest", 500)
    assert await decoded_r.tdigest().add("t-digest", list(range(1, 11)))
    assert 1 == (await decoded_r.tdigest().byrank("t-digest", 0))[0]
    assert 10 == (await decoded_r.tdigest().byrank("t-digest", 9))[0]
    assert (await decoded_r.tdigest().byrank("t-digest", 100))[0] == inf
    with pytest.raises(redis.ResponseError):
        (await decoded_r.tdigest().byrank("t-digest", -1))[0]


@pytest.mark.experimental
@pytest.mark.redismod
async def test_tdigest_byrevrank(decoded_r: redis.Redis):
    assert await decoded_r.tdigest().create("t-digest", 500)
    assert await decoded_r.tdigest().add("t-digest", list(range(1, 11)))
    assert 10 == (await decoded_r.tdigest().byrevrank("t-digest", 0))[0]
    assert 1 == (await decoded_r.tdigest().byrevrank("t-digest", 9))[0]
    assert (await decoded_r.tdigest().byrevrank("t-digest", 100))[0] == -inf
    with pytest.raises(redis.ResponseError):
        (await decoded_r.tdigest().byrevrank("t-digest", -1))[0]


@pytest.mark.fixed_client
@pytest.mark.experimental
@pytest.mark.redismod
@pytest.mark.parametrize("protocol", [2, 3])
async def test_tdigest_unified_responses_are_floats(create_redis, stack_url, protocol):
    client = await create_redis(
        protocol=protocol,
        legacy_responses=False,
        decode_responses=True,
        url=stack_url,
    )
    tdigest = client.tdigest()
    assert await tdigest.create("t-digest", 100)
    assert await tdigest.add("t-digest", [1, 2, 3, 4, 5])
    assert await tdigest.create("empty", 100)
    # Redis replies with doubles, which RESP2 sends as bulk strings.
    results = [
        (await tdigest.quantile("t-digest", 0.5, 0.9), [3.0, 5.0]),
        (await tdigest.cdf("t-digest", 1, 5), [0.1, 0.9]),
        (await tdigest.byrank("t-digest", 0, 2, 100), [1.0, 3.0, inf]),
        (await tdigest.byrevrank("t-digest", 0, 100), [5.0, -inf]),
    ]
    for result, expected in results:
        assert result == expected
        assert all(type(value) is float for value in result)
    # An empty sketch answers nan, as a float as well
    for result in (
        await tdigest.quantile("empty", 0.5),
        await tdigest.cdf("empty", 1),
        await tdigest.byrank("empty", 0),
        await tdigest.byrevrank("empty", 0),
    ):
        assert len(result) == 1
        assert type(result[0]) is float and isnan(result[0])


@pytest.mark.fixed_client
@pytest.mark.redismod
@pytest.mark.parametrize("protocol", [2, 3])
async def test_topk_info_unified_responses(create_redis, stack_url, protocol):
    client = await create_redis(
        protocol=protocol,
        legacy_responses=False,
        decode_responses=True,
        url=stack_url,
    )
    assert await client.topk().reserve("topk", 3, 50, 4, 0.9)
    info = await client.topk().info("topk")
    assert (info["k"], info["width"], info["depth"]) == (3, 50, 4)
    # The decay is a double, which RESP2 sends as a bulk string.
    assert info["decay"] == 0.9
    assert type(info["decay"]) is float
