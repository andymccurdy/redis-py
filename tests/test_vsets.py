import json
import random
from unittest.mock import patch
import numpy as np
import pytest
import redis
from redis._parsers import CommandsParser
from redis.cluster import NodesManager
from redis.commands.vectorset.commands import QuantizationOptions

from .conftest import (
    _get_client,
    skip_if_redis_enterprise,
    skip_if_server_version_lt,
)


@pytest.fixture
def d_client(request):
    r = _get_client(redis.Redis, request, decode_responses=True)

    r.flushdb()
    return r


@pytest.fixture
def client(request):
    r = _get_client(redis.Redis, request, decode_responses=False)

    r.flushdb()
    return r


@skip_if_server_version_lt("7.9.0")
def test_add_elem_with_values(d_client):
    float_array = [1, 4.32, 0.11]
    resp = d_client.vset().vadd("myset", float_array, "elem1")
    assert resp == 1

    emb = d_client.vset().vemb("myset", "elem1")
    assert _validate_quantization(float_array, emb, tolerance=0.1)

    with pytest.raises(redis.DataError):
        d_client.vset().vadd("myset_invalid_data", None, "elem1")

    with pytest.raises(redis.DataError):
        d_client.vset().vadd("myset_invalid_data", [12, 45], None, reduce_dim=3)


@skip_if_server_version_lt("7.9.0")
def test_add_elem_with_vector(d_client):
    float_array = [1, 4.32, 0.11]
    # Convert the list of floats to a byte array in fp32 format
    byte_array = _to_fp32_blob_array(float_array)
    resp = d_client.vset().vadd("myset", byte_array, "elem1")
    assert resp == 1

    emb = d_client.vset().vemb("myset", "elem1")
    assert _validate_quantization(float_array, emb, tolerance=0.1)


@skip_if_server_version_lt("7.9.0")
def test_add_elem_reduced_dim(d_client):
    float_array = [1, 4.32, 0.11, 0.5, 0.9]
    resp = d_client.vset().vadd("myset", float_array, "elem1", reduce_dim=3)
    assert resp == 1

    dim = d_client.vset().vdim("myset")
    assert dim == 3


@skip_if_server_version_lt("7.9.0")
def test_add_elem_cas(d_client):
    float_array = [1, 4.32, 0.11, 0.5, 0.9]
    resp = d_client.vset().vadd("myset", vector=float_array, element="elem1", cas=True)
    assert resp == 1

    emb = d_client.vset().vemb("myset", "elem1")
    assert _validate_quantization(float_array, emb, tolerance=0.1)


@skip_if_server_version_lt("7.9.0")
def test_add_elem_no_quant(d_client):
    float_array = [1, 4.32, 0.11, 0.5, 0.9]
    resp = d_client.vset().vadd(
        "myset",
        vector=float_array,
        element="elem1",
        quantization=QuantizationOptions.NOQUANT,
    )
    assert resp == 1

    emb = d_client.vset().vemb("myset", "elem1")
    assert _validate_quantization(float_array, emb, tolerance=0.0)


@skip_if_server_version_lt("7.9.0")
def test_add_elem_bin_quant(d_client):
    float_array = [1, 4.32, 0.0, 0.05, -2.9]
    resp = d_client.vset().vadd(
        "myset",
        vector=float_array,
        element="elem1",
        quantization=QuantizationOptions.BIN,
    )
    assert resp == 1

    emb = d_client.vset().vemb("myset", "elem1")
    expected_array = [1, 1, -1, 1, -1]
    assert _validate_quantization(expected_array, emb, tolerance=0.0)


@skip_if_server_version_lt("7.9.0")
def test_add_elem_q8_quant(d_client):
    float_array = [1, 4.32, 10.0, -21, -2.9]
    resp = d_client.vset().vadd(
        "myset",
        vector=float_array,
        element="elem1",
        quantization=QuantizationOptions.BIN,
    )
    assert resp == 1

    emb = d_client.vset().vemb("myset", "elem1")
    expected_array = [1, 1, 1, -1, -1]
    assert _validate_quantization(expected_array, emb, tolerance=0.0)


@skip_if_server_version_lt("7.9.0")
def test_add_elem_ef(d_client):
    d_client.vset().vadd("myset", vector=[5, 55, 65, -20, 30], element="elem1")
    d_client.vset().vadd("myset", vector=[-40, -40.32, 10.0, -4, 2.9], element="elem2")

    float_array = [1, 4.32, 10.0, -21, -2.9]
    resp = d_client.vset().vadd("myset", float_array, "elem3", ef=1)
    assert resp == 1

    emb = d_client.vset().vemb("myset", "elem3")
    assert _validate_quantization(float_array, emb, tolerance=0.1)

    sim = d_client.vset().vsim("myset", input="elem3", with_scores=True)
    assert len(sim) == 3


@skip_if_server_version_lt("7.9.0")
def test_add_elem_with_attr(d_client):
    float_array = [1, 4.32, 10.0, -21, -2.9]
    attrs_dict = {"key1": "value1", "key2": "value2"}
    resp = d_client.vset().vadd(
        "myset",
        vector=float_array,
        element="elem3",
        attributes=attrs_dict,
    )
    assert resp == 1

    emb = d_client.vset().vemb("myset", "elem3")
    assert _validate_quantization(float_array, emb, tolerance=0.1)

    attr_saved = d_client.vset().vgetattr("myset", "elem3")
    assert attr_saved == attrs_dict

    resp = d_client.vset().vadd(
        "myset",
        vector=float_array,
        element="elem4",
        attributes={},
    )
    assert resp == 1

    emb = d_client.vset().vemb("myset", "elem4")
    assert _validate_quantization(float_array, emb, tolerance=0.1)

    attr_saved = d_client.vset().vgetattr("myset", "elem4")
    assert attr_saved is None

    resp = d_client.vset().vadd(
        "myset",
        vector=float_array,
        element="elem5",
        attributes=json.dumps(attrs_dict),
    )
    assert resp == 1

    emb = d_client.vset().vemb("myset", "elem5")
    assert _validate_quantization(float_array, emb, tolerance=0.1)

    attr_saved = d_client.vset().vgetattr("myset", "elem5")
    assert attr_saved == attrs_dict


@skip_if_server_version_lt("7.9.0")
@pytest.mark.parametrize(
    "attributes, expected",
    [
        ({}, None),
        ("{}", None),
        ("", None),
        (None, {"source": "old"}),
        ({"source": "new"}, {"source": "new"}),
    ],
)
def test_add_elem_updates_attributes(d_client, attributes, expected):
    vset = d_client.vset()
    vset.vadd("myset", [1, 0], "elem", attributes={"source": "old"})

    vset.vadd("myset", [0, 1], "elem", attributes=attributes)

    assert vset.vgetattr("myset", "elem") == expected


@skip_if_server_version_lt("7.9.0")
def test_add_elem_with_numlinks(d_client):
    elements_count = 100
    vector_dim = 10
    pipe = d_client.pipeline(transaction=False)
    for i in range(elements_count):
        float_array = [random.randint(0, 10) for x in range(vector_dim)]
        pipe.vset().vadd(
            "myset",
            float_array,
            f"elem{i}",
            numlinks=8,
        )
    pipe.execute()

    float_array = [1, 4.32, 0.11, 0.5, 0.9, 0.1, 0.2, 0.3, 0.4, 0.5]
    resp = d_client.vset().vadd("myset", float_array, "elem_numlinks", numlinks=8)
    assert resp == 1

    emb = d_client.vset().vemb("myset", "elem_numlinks")
    assert _validate_quantization(float_array, emb, tolerance=0.5)

    numlinks_all_layers = d_client.vset().vlinks("myset", "elem_numlinks")
    for neighbours_list_for_layer in numlinks_all_layers:
        assert len(neighbours_list_for_layer) <= 8


@skip_if_server_version_lt("7.9.0")
def test_vsim_count(d_client):
    elements_count = 30
    vector_dim = 800
    pipe = d_client.pipeline(transaction=False)
    for i in range(elements_count):
        float_array = [random.uniform(0, 10) for x in range(vector_dim)]
        pipe.vset().vadd(
            "myset",
            float_array,
            f"elem{i}",
            numlinks=64,
        )
    pipe.execute()

    vsim = d_client.vset().vsim("myset", input="elem1")
    assert len(vsim) == 10
    assert isinstance(vsim, list)
    assert isinstance(vsim[0], str)

    vsim = d_client.vset().vsim("myset", input="elem1", count=5)
    assert len(vsim) == 5
    assert isinstance(vsim, list)
    assert isinstance(vsim[0], str)

    vsim = d_client.vset().vsim("myset", input="elem1", count=50)
    assert len(vsim) == 30
    assert isinstance(vsim, list)
    assert isinstance(vsim[0], str)

    vsim = d_client.vset().vsim("myset", input="elem1", count=15)
    assert len(vsim) == 15
    assert isinstance(vsim, list)
    assert isinstance(vsim[0], str)


@skip_if_server_version_lt("7.9.0")
def test_vsim_with_scores(d_client):
    elements_count = 20
    vector_dim = 50
    pipe = d_client.pipeline(transaction=False)
    for i in range(elements_count):
        float_array = [random.uniform(0, 10) for x in range(vector_dim)]
        pipe.vset().vadd(
            "myset",
            float_array,
            f"elem{i}",
            numlinks=64,
        )
    pipe.execute()

    vsim = d_client.vset().vsim("myset", input="elem1", with_scores=True)
    assert len(vsim) == 10
    assert isinstance(vsim, dict)
    assert isinstance(vsim["elem1"], float)
    assert 0 <= vsim["elem1"] <= 1


@skip_if_server_version_lt("8.2.0")
def test_vsim_with_attribs_attribs_set(d_client):
    elements_count = 5
    vector_dim = 10
    attrs_dict = {"key1": "value1", "key2": "value2"}
    pipe = d_client.pipeline(transaction=False)
    for i in range(elements_count):
        float_array = [random.uniform(0, 5) for x in range(vector_dim)]
        pipe.vset().vadd(
            "myset",
            float_array,
            f"elem{i}",
            numlinks=64,
            attributes=attrs_dict if i % 2 == 0 else None,
        )
    pipe.execute()

    vsim = d_client.vset().vsim("myset", input="elem1", with_attribs=True)
    assert len(vsim) == 5
    assert isinstance(vsim, dict)
    assert vsim["elem1"] is None
    assert vsim["elem2"] == attrs_dict


@skip_if_server_version_lt("8.2.0")
def test_vsim_with_scores_and_attribs_attribs_set(d_client):
    elements_count = 5
    vector_dim = 10
    attrs_dict = {"key1": "value1", "key2": "value2"}
    pipe = d_client.pipeline(transaction=False)
    for i in range(elements_count):
        float_array = [random.uniform(0, 5) for x in range(vector_dim)]
        pipe.vset().vadd(
            "myset",
            float_array,
            f"elem{i}",
            numlinks=64,
            attributes=attrs_dict if i % 2 == 0 else None,
        )
    pipe.execute()

    vsim = d_client.vset().vsim(
        "myset", input="elem1", with_scores=True, with_attribs=True
    )
    assert len(vsim) == 5
    assert isinstance(vsim, dict)
    assert isinstance(vsim["elem1"], dict)
    assert "score" in vsim["elem1"]
    assert "attributes" in vsim["elem1"]
    assert isinstance(vsim["elem1"]["score"], float)
    assert vsim["elem1"]["attributes"] is None

    assert isinstance(vsim["elem2"], dict)
    assert "score" in vsim["elem2"]
    assert "attributes" in vsim["elem2"]
    assert isinstance(vsim["elem2"]["score"], float)
    assert vsim["elem2"]["attributes"] == attrs_dict


@skip_if_server_version_lt("8.2.0")
def test_vsim_with_attribs_attribs_not_set(d_client):
    elements_count = 20
    vector_dim = 50
    pipe = d_client.pipeline(transaction=False)
    for i in range(elements_count):
        float_array = [random.uniform(0, 10) for x in range(vector_dim)]
        pipe.vset().vadd(
            "myset",
            float_array,
            f"elem{i}",
            numlinks=64,
        )
    pipe.execute()

    vsim = d_client.vset().vsim("myset", input="elem1", with_attribs=True)
    assert len(vsim) == 10
    assert isinstance(vsim, dict)
    assert vsim["elem1"] is None


@skip_if_server_version_lt("7.9.0")
def test_vsim_with_different_vector_input_types(d_client):
    elements_count = 10
    vector_dim = 5
    pipe = d_client.pipeline(transaction=False)
    for i in range(elements_count):
        float_array = [random.uniform(0, 10) for x in range(vector_dim)]
        attributes = {"index": i, "elem_name": f"elem_{i}"}
        pipe.vset().vadd(
            "myset",
            float_array,
            f"elem_{i}",
            numlinks=4,
            attributes=attributes,
        )
    pipe.execute()
    sim = d_client.vset().vsim("myset", input="elem_1")
    assert len(sim) == 10
    assert isinstance(sim, list)

    float_array = [1, 4.32, 0.0, 0.05, -2.9]
    sim_to_float_array = d_client.vset().vsim("myset", input=float_array)
    assert len(sim_to_float_array) == 10
    assert isinstance(sim_to_float_array, list)

    fp32_vector = _to_fp32_blob_array(float_array)
    sim_to_fp32_vector = d_client.vset().vsim("myset", input=fp32_vector)
    assert len(sim_to_fp32_vector) == 10
    assert isinstance(sim_to_fp32_vector, list)
    assert sim_to_float_array == sim_to_fp32_vector

    with pytest.raises(redis.DataError):
        d_client.vset().vsim("myset", input=None)


@skip_if_server_version_lt("7.9.0")
def test_vsim_unexisting(d_client):
    float_array = [1, 4.32, 0.11, 0.5, 0.9]
    d_client.vset().vadd("myset", vector=float_array, element="elem1", cas=True)

    with pytest.raises(redis.ResponseError):
        d_client.vset().vsim("myset", input="elem_not_existing")

    sim = d_client.vset().vsim("myset_not_existing", input="elem1")
    assert sim == []

    sim = d_client.vset().vsim("myset_not_existing", input="elem1", with_scores=True)
    assert sim == {}

    sim = d_client.vset().vsim("myset_not_existing", input="elem1", with_attribs=True)
    assert sim == {}

    sim = d_client.vset().vsim(
        "myset_not_existing", input="elem1", with_scores=True, with_attribs=True
    )
    assert sim == {}


@skip_if_server_version_lt("7.9.0")
def test_vsim_with_filter(d_client):
    elements_count = 50
    vector_dim = 800
    pipe = d_client.pipeline(transaction=False)
    for i in range(elements_count):
        float_array = [random.uniform(10, 20) for x in range(vector_dim)]
        attributes = {"index": i, "elem_name": f"elem_{i}"}
        pipe.vset().vadd(
            "myset",
            float_array,
            f"elem_{i}",
            numlinks=4,
            attributes=attributes,
        )
    pipe.execute()
    float_array = [-random.uniform(10, 20) for x in range(vector_dim)]
    attributes = {"index": elements_count, "elem_name": "elem_special"}
    d_client.vset().vadd(
        "myset",
        float_array,
        "elem_special",
        numlinks=4,
        attributes=attributes,
    )
    sim = d_client.vset().vsim("myset", input="elem_1", filter=".index > 10")
    assert len(sim) == 10
    assert isinstance(sim, list)
    for elem in sim:
        assert int(elem.split("_")[1]) > 10

    sim = d_client.vset().vsim(
        "myset",
        input="elem_1",
        filter=".index > 10 and .index < 15 and .elem_name in ['elem_12', 'elem_17']",
    )
    assert len(sim) == 1
    assert isinstance(sim, list)
    assert sim[0] == "elem_12"

    sim = d_client.vset().vsim(
        "myset",
        input="elem_1",
        filter=".index > 25 and .elem_name in ['elem_12', 'elem_17', 'elem_19']",
        ef=100,
    )
    assert len(sim) == 0
    assert isinstance(sim, list)

    sim = d_client.vset().vsim(
        "myset",
        input="elem_1",
        filter=".index > 28 and .elem_name in ['elem_12', 'elem_17', 'elem_special']",
        filter_ef=1,
    )
    assert len(sim) == 0, (
        f"Expected 0 results, but got {len(sim)} with filter_ef=1, sim: {sim}"
    )
    assert isinstance(sim, list)

    sim = d_client.vset().vsim(
        "myset",
        input="elem_1",
        filter=".index > 28 and .elem_name in ['elem_12', 'elem_17', 'elem_special']",
        filter_ef=500,
    )
    assert len(sim) == 1
    assert isinstance(sim, list)


@skip_if_server_version_lt("7.9.0")
def test_vsim_truth_no_thread_enabled(d_client):
    elements_count = 100
    vector_dim = 50
    pipe = d_client.pipeline(transaction=False)
    for i in range(1, elements_count + 1):
        float_array = [i * vector_dim for _ in range(vector_dim)]
        pipe.vset().vadd("myset", float_array, f"elem_{i}")
    pipe.execute()

    d_client.vset().vadd("myset", [-22 for _ in range(vector_dim)], "elem_man_2")

    sim_without_truth = d_client.vset().vsim(
        "myset", input="elem_man_2", with_scores=True, count=30
    )
    sim_truth = d_client.vset().vsim(
        "myset", input="elem_man_2", with_scores=True, count=30, truth=True
    )

    assert len(sim_without_truth) == 30
    assert len(sim_truth) == 30

    assert isinstance(sim_without_truth, dict)
    assert isinstance(sim_truth, dict)

    sim_no_thread = d_client.vset().vsim(
        "myset", input="elem_man_2", with_scores=True, no_thread=True
    )

    assert len(sim_no_thread) == 10
    assert isinstance(sim_no_thread, dict)


@skip_if_server_version_lt("8.2.0")
def test_vsim_epsilon(d_client):
    d_client.vset().vadd("myset", [2, 1, 1], "a")
    d_client.vset().vadd("myset", [2, 0, 1], "b")
    d_client.vset().vadd("myset", [2, 0, 0], "c")
    d_client.vset().vadd("myset", [2, 0, 2], "d")
    d_client.vset().vadd("myset", [-2, -1, -1], "e")

    res1 = d_client.vset().vsim("myset", [2, 1, 1])
    assert 5 == len(res1)

    res2 = d_client.vset().vsim("myset", [2, 1, 1], epsilon=0.5)
    assert 4 == len(res2)


@skip_if_server_version_lt("7.9.0")
def test_vdim(d_client):
    float_array = [1, 4.32, 0.11, 0.5, 0.9, 0.1, 0.2]
    d_client.vset().vadd("myset", float_array, "elem1")

    dim = d_client.vset().vdim("myset")
    assert dim == len(float_array)

    d_client.vset().vadd("myset_reduced", float_array, "elem1", reduce_dim=4)
    reduced_dim = d_client.vset().vdim("myset_reduced")
    assert reduced_dim == 4

    with pytest.raises(redis.ResponseError):
        d_client.vset().vdim("myset_unexisting")


@skip_if_server_version_lt("7.9.0")
def test_vcard(d_client):
    n = 20
    pipe = d_client.pipeline(transaction=False)
    for i in range(n):
        float_array = [random.uniform(0, 10) for x in range(1, 8)]
        pipe.vset().vadd("myset", float_array, f"elem{i}")
    pipe.execute()

    card = d_client.vset().vcard("myset")
    assert card == n

    with pytest.raises(redis.ResponseError):
        d_client.vset().vdim("myset_unexisting")


@skip_if_server_version_lt("7.9.0")
def test_vrem(d_client):
    n = 3
    pipe = d_client.pipeline(transaction=False)
    for i in range(n):
        float_array = [random.uniform(0, 10) for x in range(1, 8)]
        pipe.vset().vadd("myset", float_array, f"elem{i}")
    pipe.execute()

    resp = d_client.vset().vrem("myset", "elem2")
    assert resp == 1

    card = d_client.vset().vcard("myset")
    assert card == n - 1

    resp = d_client.vset().vrem("myset", "elem2")
    assert resp == 0

    card = d_client.vset().vcard("myset")
    assert card == n - 1

    resp = d_client.vset().vrem("myset_unexisting", "elem1")
    assert resp == 0


@pytest.mark.fixed_client
@pytest.mark.parametrize("client_factory", ["direct", "from_url", "pool", "cluster"])
def test_vemb_raw_default_decode_responses(client_factory):
    if client_factory == "direct":
        r = redis.Redis()
    elif client_factory == "from_url":
        r = redis.Redis.from_url("redis://localhost:6379")
    elif client_factory == "pool":
        r = redis.Redis(connection_pool=redis.ConnectionPool())
    else:
        with (
            patch.object(NodesManager, "initialize", autospec=True),
            patch.object(CommandsParser, "initialize", autospec=True),
        ):
            r = redis.RedisCluster(host="localhost")

    response = [b"embedding"]
    try:
        with patch.object(
            r, "execute_command", return_value=response
        ) as execute_command:
            assert r.vset().vemb("vectors", "item", raw=True) is response
        execute_command.assert_called_once_with(
            "VEMB", "vectors", "item", "RAW", RESP3=True, NEVER_DECODE=True, RAW=True
        )
    finally:
        r.close()


@skip_if_server_version_lt("7.9.0")
def test_vemb_bin_quantization(d_client):
    e = [1, 4.32, 0.0, 0.05, -2.9]
    d_client.vset().vadd(
        "myset",
        e,
        "elem",
        quantization=QuantizationOptions.BIN,
    )
    emb_no_quant = d_client.vset().vemb("myset", "elem")
    assert emb_no_quant == [1, 1, -1, 1, -1]

    emb_no_quant_raw = d_client.vset().vemb("myset", "elem", raw=True)
    assert emb_no_quant_raw["quantization"] == "bin"
    assert isinstance(emb_no_quant_raw["raw"], bytes)
    assert isinstance(emb_no_quant_raw["l2"], float)
    assert "range" not in emb_no_quant_raw


@skip_if_server_version_lt("7.9.0")
def test_vemb_q8_quantization(d_client):
    e = [1, 10.32, 0.0, 2.05, -12.5]
    d_client.vset().vadd("myset", e, "elem", quantization=QuantizationOptions.Q8)

    emb_q8_quant = d_client.vset().vemb("myset", "elem")
    assert _validate_quantization(e, emb_q8_quant, tolerance=0.1)

    emb_q8_quant_raw = d_client.vset().vemb("myset", "elem", raw=True)
    assert emb_q8_quant_raw["quantization"] == "int8"
    assert isinstance(emb_q8_quant_raw["raw"], bytes)
    assert isinstance(emb_q8_quant_raw["l2"], float)
    assert isinstance(emb_q8_quant_raw["range"], float)


@skip_if_server_version_lt("7.9.0")
def test_vemb_no_quantization(d_client):
    e = [1, 10.32, 0.0, 2.05, -12.5]
    d_client.vset().vadd("myset", e, "elem", quantization=QuantizationOptions.NOQUANT)

    emb_no_quant = d_client.vset().vemb("myset", "elem")
    assert _validate_quantization(e, emb_no_quant, tolerance=0.1)

    emb_no_quant_raw = d_client.vset().vemb("myset", "elem", raw=True)
    assert emb_no_quant_raw["quantization"] == "f32"
    assert isinstance(emb_no_quant_raw["raw"], bytes)
    assert isinstance(emb_no_quant_raw["l2"], float)
    assert "range" not in emb_no_quant_raw


@skip_if_server_version_lt("7.9.0")
def test_vemb_default_quantization(d_client):
    e = [1, 5.32, 0.0, 0.25, -5]
    d_client.vset().vadd("myset", vector=e, element="elem")

    emb_default_quant = d_client.vset().vemb("myset", "elem")
    assert _validate_quantization(e, emb_default_quant, tolerance=0.1)

    emb_default_quant_raw = d_client.vset().vemb("myset", "elem", raw=True)
    assert emb_default_quant_raw["quantization"] == "int8"
    assert isinstance(emb_default_quant_raw["raw"], bytes)
    assert isinstance(emb_default_quant_raw["l2"], float)
    assert isinstance(emb_default_quant_raw["range"], float)


@skip_if_server_version_lt("7.9.0")
def test_vemb_fp32_quantization(d_client):
    float_array_fp32 = [1, 4.32, 0.11]
    # Convert the list of floats to a byte array in fp32 format
    byte_array = _to_fp32_blob_array(float_array_fp32)
    d_client.vset().vadd("myset", byte_array, "elem")

    emb_fp32_quant = d_client.vset().vemb("myset", "elem")
    assert _validate_quantization(float_array_fp32, emb_fp32_quant, tolerance=0.1)

    emb_fp32_quant_raw = d_client.vset().vemb("myset", "elem", raw=True)
    assert emb_fp32_quant_raw["quantization"] == "int8"
    assert isinstance(emb_fp32_quant_raw["raw"], bytes)
    assert isinstance(emb_fp32_quant_raw["l2"], float)
    assert isinstance(emb_fp32_quant_raw["range"], float)


@skip_if_server_version_lt("7.9.0")
def test_vemb_unexisting(d_client):
    emb_not_existing = d_client.vset().vemb("not_existing", "elem")
    assert emb_not_existing is None

    e = [1, 5.32, 0.0, 0.25, -5]
    d_client.vset().vadd("myset", vector=e, element="elem")
    emb_elem_not_existing = d_client.vset().vemb("myset", "not_existing")
    assert emb_elem_not_existing is None


@skip_if_server_version_lt("7.9.0")
def test_vemb_unified_responses_return_floats_on_both_protocols(request):
    vector = [1, 0.5, 0]
    embs = {}
    for protocol in (2, 3):
        r = _get_client(
            redis.Redis,
            request,
            decode_responses=True,
            protocol=protocol,
            legacy_responses=False,
        )
        r.delete("myset")
        r.vset().vadd("myset", vector, "elem", quantization=QuantizationOptions.NOQUANT)
        embs[protocol] = r.vset().vemb("myset", "elem")

    assert all(isinstance(value, float) for value in embs[2] + embs[3])
    assert embs[2] == embs[3] == [1.0, 0.5, 0.0]


@skip_if_server_version_lt("7.9.0")
def test_vlinks(d_client):
    elements_count = 100
    vector_dim = 800
    pipe = d_client.pipeline(transaction=False)
    for i in range(elements_count):
        float_array = [random.uniform(0, 10) for x in range(vector_dim)]
        pipe.vset().vadd(
            "myset",
            float_array,
            f"elem{i}",
            numlinks=8,
        )
    pipe.execute()

    element_links_all_layers = d_client.vset().vlinks("myset", "elem1")
    assert len(element_links_all_layers) >= 1
    for neighbours_list_for_layer in element_links_all_layers:
        assert isinstance(neighbours_list_for_layer, list)
        for neighbour in neighbours_list_for_layer:
            assert isinstance(neighbour, str)

    elem_links_all_layers_with_scores = d_client.vset().vlinks(
        "myset", "elem1", with_scores=True
    )
    assert len(elem_links_all_layers_with_scores) >= 1
    for neighbours_dict_for_layer in elem_links_all_layers_with_scores:
        assert isinstance(neighbours_dict_for_layer, dict)
        for neighbour_key, score_value in neighbours_dict_for_layer.items():
            assert isinstance(neighbour_key, str)
            assert isinstance(score_value, float)

    float_array = [0.75, 0.25, 0.5, 0.1, 0.9]
    d_client.vset().vadd("myset_one_elem_only", float_array, "elem1")
    elem_no_neighbours_with_scores = d_client.vset().vlinks(
        "myset_one_elem_only", "elem1", with_scores=True
    )
    assert len(elem_no_neighbours_with_scores) >= 1
    for neighbours_dict_for_layer in elem_no_neighbours_with_scores:
        assert isinstance(neighbours_dict_for_layer, dict)
        assert len(neighbours_dict_for_layer) == 0

    elem_no_neighbours_no_scores = d_client.vset().vlinks(
        "myset_one_elem_only", "elem1"
    )
    assert len(elem_no_neighbours_no_scores) >= 1
    for neighbours_list_for_layer in elem_no_neighbours_no_scores:
        assert isinstance(neighbours_list_for_layer, list)
        assert len(neighbours_list_for_layer) == 0

    unexisting_element_links = d_client.vset().vlinks("myset", "unexisting_elem")
    assert unexisting_element_links is None

    unexisting_vset_links = d_client.vset().vlinks("myset_unexisting", "elem1")
    assert unexisting_vset_links is None

    unexisting_element_links = d_client.vset().vlinks(
        "myset", "unexisting_elem", with_scores=True
    )
    assert unexisting_element_links is None

    unexisting_vset_links = d_client.vset().vlinks(
        "myset_unexisting", "elem1", with_scores=True
    )
    assert unexisting_vset_links is None


@skip_if_server_version_lt("7.9.0")
def test_vinfo(d_client):
    elements_count = 100
    vector_dim = 800
    pipe = d_client.pipeline(transaction=False)
    for i in range(elements_count):
        float_array = [random.uniform(0, 10) for x in range(vector_dim)]
        pipe.vset().vadd(
            "myset",
            float_array,
            f"elem{i}",
            numlinks=8,
            quantization=QuantizationOptions.BIN,
        )
    pipe.execute()

    vset_info = d_client.vset().vinfo("myset")
    assert vset_info["quant-type"] == "bin"
    assert vset_info["vector-dim"] == vector_dim
    assert vset_info["size"] == elements_count
    assert vset_info["max-level"] > 0
    assert vset_info["hnsw-max-node-uid"] == elements_count

    unexisting_vset_info = d_client.vset().vinfo("myset_unexisting")
    assert unexisting_vset_info is None


@skip_if_server_version_lt("7.9.0")
def test_vset_vget_attributes(d_client):
    float_array = [1, 4.32, 0.11]
    attributes = {"key1": "value1", "key2": "value2"}

    # validate vgetattrs when no attributes are set with vadd
    resp = d_client.vset().vadd("myset", float_array, "elem1")
    assert resp == 1

    attrs = d_client.vset().vgetattr("myset", "elem1")
    assert attrs is None

    # validate vgetattrs when attributes are set with vadd
    resp = d_client.vset().vadd(
        "myset_with_attrs", float_array, "elem1", attributes=attributes
    )
    assert resp == 1

    attrs = d_client.vset().vgetattr("myset_with_attrs", "elem1")
    assert attrs == attributes

    # Set attributes and get attributes
    resp = d_client.vset().vsetattr("myset", "elem1", attributes)
    assert resp == 1
    attr_saved = d_client.vset().vgetattr("myset", "elem1")
    assert attr_saved == attributes

    # Set attributes to None
    resp = d_client.vset().vsetattr("myset", "elem1", None)
    assert resp == 1
    attr_saved = d_client.vset().vgetattr("myset", "elem1")
    assert attr_saved is None

    # Set attributes to empty dict
    resp = d_client.vset().vsetattr("myset", "elem1", {})
    assert resp == 1
    attr_saved = d_client.vset().vgetattr("myset", "elem1")
    assert attr_saved is None

    # Set attributes provided as string
    resp = d_client.vset().vsetattr("myset", "elem1", json.dumps(attributes))
    assert resp == 1
    attr_saved = d_client.vset().vgetattr("myset", "elem1")
    assert attr_saved == attributes

    # Set attributes to unexisting element
    resp = d_client.vset().vsetattr("myset", "elem2", attributes)
    assert resp == 0
    attr_saved = d_client.vset().vgetattr("myset", "elem2")
    assert attr_saved is None

    # Set attributes to unexisting vset
    resp = d_client.vset().vsetattr("myset_unexisting", "elem1", attributes)
    assert resp == 0
    attr_saved = d_client.vset().vgetattr("myset_unexisting", "elem1")
    assert attr_saved is None


@skip_if_server_version_lt("7.9.0")
def test_vrandmember(d_client):
    elements = ["elem1", "elem2", "elem3"]
    pipe = d_client.pipeline(transaction=False)
    for elem in elements:
        float_array = [random.uniform(0, 10) for x in range(1, 8)]
        pipe.vset().vadd("myset", float_array, element=elem)
    pipe.execute()

    random_member = d_client.vset().vrandmember("myset")
    assert random_member in elements

    members_list = d_client.vset().vrandmember("myset", count=2)
    assert len(members_list) == 2
    assert all(member in elements for member in members_list)

    # Test with count greater than the number of elements
    members_list = d_client.vset().vrandmember("myset", count=10)
    assert len(members_list) == len(elements)
    assert all(member in elements for member in members_list)

    # Test with negative count
    members_list = d_client.vset().vrandmember("myset", count=-2)
    assert len(members_list) == 2
    assert all(member in elements for member in members_list)

    # Test with count equal to the number of elements
    members_list = d_client.vset().vrandmember("myset", count=len(elements))
    assert len(members_list) == len(elements)
    assert all(member in elements for member in members_list)

    # Test with count equal to 0
    members_list = d_client.vset().vrandmember("myset", count=0)
    assert members_list == []

    # Test with count equal to 1
    members_list = d_client.vset().vrandmember("myset", count=1)
    assert len(members_list) == 1
    assert members_list[0] in elements

    # Test with count equal to -1
    members_list = d_client.vset().vrandmember("myset", count=-1)
    assert len(members_list) == 1
    assert members_list[0] in elements

    # Test with unexisting vset & without count
    members_list = d_client.vset().vrandmember("myset_unexisting")
    assert members_list is None

    # Test with unexisting vset & count
    members_list = d_client.vset().vrandmember("myset_unexisting", count=5)
    assert members_list == []


@skip_if_server_version_lt("8.2.0")
def test_8_2_new_vset_features_without_decoding_responces(client):
    # test vadd
    elements = ["elem1", "elem2", "elem3"]
    attrs_dict = {"key1": "value1", "key2": "value2"}
    for elem in elements:
        float_array = [random.uniform(0.5, 10) for x in range(0, 8)]
        resp = client.vset().vadd(
            "myset", float_array, element=elem, attributes=attrs_dict
        )
        assert resp == 1

    # test vsim with attributes
    vsim_with_attribs = client.vset().vsim("myset", input="elem1", with_attribs=True)
    assert len(vsim_with_attribs) == 3
    assert isinstance(vsim_with_attribs, dict)
    assert isinstance(vsim_with_attribs[b"elem1"], dict)
    assert vsim_with_attribs[b"elem1"] == attrs_dict

    # test vsim with score and attributes
    vsim_with_scores_and_attribs = client.vset().vsim(
        "myset", input="elem1", with_scores=True, with_attribs=True
    )
    assert len(vsim_with_scores_and_attribs) == 3
    assert isinstance(vsim_with_scores_and_attribs, dict)
    assert isinstance(vsim_with_scores_and_attribs[b"elem1"], dict)
    assert "score" in vsim_with_scores_and_attribs[b"elem1"]
    assert "attributes" in vsim_with_scores_and_attribs[b"elem1"]
    assert isinstance(vsim_with_scores_and_attribs[b"elem1"]["score"], float)
    assert isinstance(vsim_with_scores_and_attribs[b"elem1"]["attributes"], dict)
    assert vsim_with_scores_and_attribs[b"elem1"]["attributes"] == attrs_dict


@skip_if_server_version_lt("7.9.0")
def test_vset_commands_without_decoding_responces(client):
    # test vadd
    elements = ["elem1", "elem2", "elem3"]
    attrs_dict = {"key1": "value1", "key2": "value2"}
    for elem in elements:
        float_array = [random.uniform(0.5, 10) for x in range(0, 8)]
        resp = client.vset().vadd(
            "myset", float_array, element=elem, attributes=attrs_dict
        )
        assert resp == 1

    # test vemb
    emb = client.vset().vemb("myset", "elem1")
    assert len(emb) == 8
    assert isinstance(emb, list)
    assert all(isinstance(x, (int, float)) for x in emb), (
        f"Expected numeric values, got {emb}"
    )

    emb_raw = client.vset().vemb("myset", "elem1", raw=True)
    assert emb_raw["quantization"] == b"int8"
    assert isinstance(emb_raw["raw"], bytes)
    assert isinstance(emb_raw["l2"], float)
    assert isinstance(emb_raw["range"], float)

    # test vsim
    vsim = client.vset().vsim("myset", input="elem1")
    assert len(vsim) == 3
    assert isinstance(vsim, list)
    assert isinstance(vsim[0], bytes)

    # test vsim with scores
    vsim_with_scores = client.vset().vsim("myset", input="elem1", with_scores=True)
    assert len(vsim_with_scores) == 3
    assert isinstance(vsim_with_scores, dict)
    assert isinstance(vsim_with_scores[b"elem1"], float)

    # test vlinks - no scores
    element_links_all_layers = client.vset().vlinks("myset", "elem1")
    assert len(element_links_all_layers) >= 1
    for neighbours_list_for_layer in element_links_all_layers:
        assert isinstance(neighbours_list_for_layer, list)
        for neighbour in neighbours_list_for_layer:
            assert isinstance(neighbour, bytes)
    # test vlinks with scores
    elem_links_all_layers_with_scores = client.vset().vlinks(
        "myset", "elem1", with_scores=True
    )
    assert len(elem_links_all_layers_with_scores) >= 1
    for neighbours_dict_for_layer in elem_links_all_layers_with_scores:
        assert isinstance(neighbours_dict_for_layer, dict)
        for neighbour_key, score_value in neighbours_dict_for_layer.items():
            assert isinstance(neighbour_key, bytes)
            assert isinstance(score_value, float)

    # test vinfo
    vset_info = client.vset().vinfo("myset")
    assert vset_info[b"quant-type"] == b"int8"
    assert vset_info[b"vector-dim"] == 8
    assert vset_info[b"size"] == len(elements)
    assert vset_info[b"max-level"] >= 0
    assert vset_info[b"hnsw-max-node-uid"] == len(elements)

    # test vgetattr
    attributes = {"key1": "value1", "key2": "value2"}
    client.vset().vsetattr("myset", "elem1", attributes)
    attrs = client.vset().vgetattr("myset", "elem1")
    assert attrs == attributes

    # test vrandmember
    random_member = client.vset().vrandmember("myset")
    assert isinstance(random_member, bytes)
    assert random_member.decode("utf-8") in elements

    members_list = client.vset().vrandmember("myset", count=2)
    assert len(members_list) == 2
    assert all(member.decode("utf-8") in elements for member in members_list)


def _to_fp32_blob_array(float_array):
    """
    Convert a list of floats to a byte array in fp32 format.
    """
    # Convert the list of floats to a NumPy array with dtype np.float32
    arr = np.array(float_array, dtype=np.float32)
    # Convert the NumPy array to a byte array
    byte_array = arr.tobytes()
    return byte_array


def _validate_quantization(original, quantized, tolerance=0.1):
    original = np.array(original, dtype=np.float32)
    quantized = np.array(quantized, dtype=np.float32)

    max_diff = np.max(np.abs(original - quantized))
    if max_diff > tolerance:
        return False
    else:
        return True


@skip_if_server_version_lt("8.4.0")
@skip_if_redis_enterprise()
def test_vrange_basic(d_client):
    """Test basic VRANGE functionality with lexicographical ordering."""
    # Add elements with different names
    elements = ["apple", "banana", "cherry", "date", "elderberry"]
    pipe = d_client.pipeline(transaction=False)
    for elem in elements:
        pipe.vset().vadd("myset", [1.0, 2.0, 3.0], elem)
    pipe.execute()

    # Test full range
    result = d_client.vset().vrange("myset", "-", "+")
    assert result == elements
    assert len(result) == 5

    # Test inclusive range
    result = d_client.vset().vrange("myset", "[banana", "[date")
    assert result == ["banana", "cherry", "date"]

    # Test exclusive range
    result = d_client.vset().vrange("myset", "(banana", "(date")
    assert result == ["cherry"]


@skip_if_server_version_lt("8.4.0")
@skip_if_redis_enterprise()
def test_vrange_with_count(d_client):
    """Test VRANGE with count parameter."""
    # Add elements
    elements = ["a", "b", "c", "d", "e", "f", "g"]
    pipe = d_client.pipeline(transaction=False)
    for elem in elements:
        pipe.vset().vadd("myset", [1.0, 2.0], elem)
    pipe.execute()

    # Test with positive count
    result = d_client.vset().vrange("myset", "-", "+", count=3)
    assert len(result) == 3
    assert result == ["a", "b", "c"]

    # Test with count larger than set size
    result = d_client.vset().vrange("myset", "-", "+", count=100)
    assert len(result) == 7
    assert result == elements

    # Test with count = 0
    result = d_client.vset().vrange("myset", "-", "+", count=0)
    assert result == []

    # Test with negative count (should return all)
    result = d_client.vset().vrange("myset", "-", "+", count=-1)
    assert len(result) == 7
    assert result == elements


@skip_if_server_version_lt("8.4.0")
@skip_if_redis_enterprise()
def test_vrange_iteration(d_client):
    """Test VRANGE for stateless iteration."""
    # Add elements
    elements = [f"elem{i:03d}" for i in range(20)]
    pipe = d_client.pipeline(transaction=False)
    for elem in elements:
        pipe.vset().vadd("myset", [1.0, 2.0], elem)
    pipe.execute()

    # Iterate through all elements, 5 at a time
    all_results = []
    start = "-"
    while True:
        result = d_client.vset().vrange("myset", start, "+", count=5)
        if not result:
            break
        all_results.extend(result)
        # Continue from the last element (exclusive)
        start = f"({result[-1]}"

    assert len(all_results) == 20
    assert all_results == elements


@skip_if_server_version_lt("8.4.0")
@skip_if_redis_enterprise()
def test_vrange_empty_key(d_client):
    """Test VRANGE on non-existent key."""
    result = d_client.vset().vrange("nonexistent", "-", "+")
    assert result == []

    result = d_client.vset().vrange("nonexistent", "[a", "[z", count=10)
    assert result == []


@skip_if_server_version_lt("8.4.0")
@skip_if_redis_enterprise()
def test_vrange_special_characters(d_client):
    """Test VRANGE with elements containing special characters."""
    # Add elements with special characters
    elements = ["a:1", "a:2", "b:1", "b:2", "c:1"]
    pipe = d_client.pipeline(transaction=False)
    for elem in elements:
        pipe.vset().vadd("myset", [1.0, 2.0], elem)
    pipe.execute()

    # Test range with prefix
    result = d_client.vset().vrange("myset", "[a:", "[a:9")
    assert result == ["a:1", "a:2"]

    result = d_client.vset().vrange("myset", "[b:", "[b:9")
    assert result == ["b:1", "b:2"]


@skip_if_server_version_lt("8.4.0")
@skip_if_redis_enterprise()
def test_vrange_single_element(d_client):
    """Test VRANGE with a single element."""
    d_client.vset().vadd("myset", [1.0, 2.0], "single")

    result = d_client.vset().vrange("myset", "-", "+")
    assert result == ["single"]

    result = d_client.vset().vrange("myset", "[single", "[single")
    assert result == ["single"]

    result = d_client.vset().vrange("myset", "(single", "+")
    assert result == []


@skip_if_server_version_lt("8.4.0")
@skip_if_redis_enterprise()
def test_vrange_lexicographical_order(d_client):
    """Test that VRANGE returns elements in correct lexicographical order."""
    # Add elements in random order
    elements = ["zebra", "apple", "mango", "banana", "cherry"]
    pipe = d_client.pipeline(transaction=False)
    for elem in elements:
        pipe.vset().vadd("myset", [1.0, 2.0], elem)
    pipe.execute()

    # Should return in sorted order
    result = d_client.vset().vrange("myset", "-", "+")
    expected = sorted(elements)
    assert result == expected


@skip_if_server_version_lt("8.4.0")
@skip_if_redis_enterprise()
def test_vrange_numeric_strings(d_client):
    """Test VRANGE with numeric string elements."""
    # Add numeric strings (lexicographical order, not numeric)
    elements = ["1", "10", "2", "20", "3"]
    pipe = d_client.pipeline(transaction=False)
    for elem in elements:
        pipe.vset().vadd("myset", [1.0, 2.0], elem)
    pipe.execute()

    # Lexicographical order: "1", "10", "2", "20", "3"
    result = d_client.vset().vrange("myset", "-", "+")
    expected = sorted(elements)  # ["1", "10", "2", "20", "3"]
    assert result == expected

    # Range from "1" to "2" (inclusive)
    result = d_client.vset().vrange("myset", "[1", "[2")
    assert result == ["1", "10", "2"]
