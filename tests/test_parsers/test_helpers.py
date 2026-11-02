import inspect
import math

import pytest

from redis._parsers.helpers import (
    pairs_to_dict,
    pairs_to_dict_typed,
    parse_acl_log,
    parse_acl_log_resp3_to_resp2_legacy,
    parse_client_info,
    parse_client_list,
    parse_command,
    parse_function_stats_resp3_to_resp2_legacy,
    parse_function_stats_unified,
    parse_info,
    parse_memory_stats_unified,
    parse_sentinel_masters_resp3,
    zmpop_resp3_to_resp2_legacy,
    zpop_score_pairs,
    zpop_score_pairs_resp3_to_resp2_legacy,
    zset_score_for_rank_resp3_to_resp2_legacy,
    zset_score_pairs_resp3_to_resp2_legacy,
    zset_score_pairs_resp3_to_resp2_legacy_flat,
)


@pytest.mark.fixed_client
def test_parse_info():
    info_output = """
# Modules
module:name=search,ver=999999,api=1,filters=0,usedby=[],using=[ReJSON],options=[handle-io-errors]

# search_fields_statistics
search_fields_text:Text=3
search_fields_tag:Tag=2,Sortable=1

# search_version
search_version:99.99.99
search_redis_version:7.2.2 - oss

# search_runtime_configurations
search_query_timeout_ms:500
    """
    info = parse_info(info_output)

    assert isinstance(info["modules"], list)
    assert isinstance(info["modules"][0], dict)
    assert info["modules"][0]["name"] == "search"

    assert isinstance(info["search_fields_text"], dict)
    assert info["search_fields_text"]["Text"] == 3

    assert isinstance(info["search_fields_tag"], dict)
    assert info["search_fields_tag"]["Tag"] == 2
    assert info["search_fields_tag"]["Sortable"] == 1

    assert info["search_version"] == "99.99.99"
    assert info["search_redis_version"] == "7.2.2 - oss"
    assert info["search_query_timeout_ms"] == 500


@pytest.mark.fixed_client
def test_parse_info_list():
    info_output = """
list_one:a,
list_two:a b,,c,10,1.1
    """
    info = parse_info(info_output)

    assert isinstance(info["list_one"], list)
    assert info["list_one"] == ["a"]

    assert isinstance(info["list_two"], list)
    assert info["list_two"] == ["a b", "c", 10, 1.1]


@pytest.mark.fixed_client
def test_parse_info_list_dict_mixed():
    info_output = """
list_one:a,b=1
list_two:a b=foo,,c,d=bar,e,
    """
    info = parse_info(info_output)

    assert isinstance(info["list_one"], dict)
    assert info["list_one"] == {"a": True, "b": 1}

    assert isinstance(info["list_two"], dict)
    assert info["list_two"] == {"a b": "foo", "c": True, "d": "bar", "e": True}


@pytest.mark.fixed_client
def test_parse_client_list():
    response = "id=7 addr=/tmp/redis sock/redis.sock:0 fd=9 name=test=_complex_[name] age=-1 idle=0 cmd=client|list user=default lib-name=go-redis(,go1.24.4) lib-ver="
    expected = [
        {
            "id": "7",
            "addr": "/tmp/redis sock/redis.sock:0",
            "fd": "9",
            "name": "test=_complex_[name]",
            "age": "-1",
            "idle": "0",
            "cmd": "client|list",
            "user": "default",
            "lib-name": "go-redis(,go1.24.4)",
            "lib-ver": "",
        }
    ]
    clients = parse_client_list(response)
    assert clients == expected


@pytest.mark.fixed_client
def test_parse_client_list_as_iter():
    # ``as_iter=True`` (client_list_iter's opt-in path) must yield the same
    # records as the eager list form, just lazily and one at a time.
    response = "id=1 addr=127.0.0.1:1\nid=2 addr=127.0.0.1:2"
    result = parse_client_list(response, as_iter=True)
    assert inspect.isgenerator(result)
    assert list(result) == parse_client_list(response)


@pytest.mark.fixed_client
def test_parse_client_info():
    # A CLIENT INFO value can contain both a space (a unix-socket addr such as
    # "/tmp/redis sock/redis.sock") and an "=" (a client name like
    # "test=_complex_[name]"), the same tricky data parse_client_list handles.
    # Int-typed fields are additionally coerced to int.
    info = (
        "id=7 addr=/tmp/redis sock/redis.sock:0 name=test=_complex_[name] "
        "age=-1 db=0 lib-ver="
    )
    assert parse_client_info(info) == {
        "id": 7,
        "addr": "/tmp/redis sock/redis.sock:0",
        "name": "test=_complex_[name]",
        "age": -1,
        "db": 0,
        "lib-ver": "",
    }


@pytest.mark.fixed_client
def test_parse_client_info_empty():
    # An empty or whitespace-only client-info stays an empty dict rather than
    # raising. ACL LOG entries omit client-info as "" (see parse_acl_log), so
    # this path must not crash.
    assert parse_client_info("") == {}
    assert parse_client_info("   ") == {}


@pytest.mark.fixed_client
def test_parse_client_list_shares_guard():
    # parse_client_list and parse_client_info parse the same server blob and
    # now share one tokenizer, so the leading-token guard added for CLIENT INFO
    # also protects CLIENT LIST. A line whose first token has no "=" (only
    # possible with malformed input) is skipped instead of raising KeyError.
    assert parse_client_list("junk id=3") == [{"id": "3"}]
    # Consecutive spaces inside a value round-trip verbatim: a unix-socket
    # path can contain two or more spaces in a row, and CLIENT LIST on master
    # preserves them, so the shared tokenizer must not collapse them.
    assert parse_client_list("id=1 addr=/tmp/a  b/r.sock:0 fd=9") == [
        {"id": "1", "addr": "/tmp/a  b/r.sock:0", "fd": "9"}
    ]


@pytest.mark.fixed_client
def test_parse_command_preserves_acl_categories():
    response = [
        [
            b"get",
            2,
            [b"readonly", b"fast"],
            1,
            1,
            1,
            [b"@read", b"@string", b"@fast"],
            [b"request_policy:all_shards"],
            [],
            [],
        ]
    ]

    command = parse_command(response)["get"]

    assert command["flags"] == ["readonly", "fast"]
    assert command["acl_categories"] == ["@read", "@string", "@fast"]


@pytest.mark.fixed_client
def test_zpop_score_pairs_resp2_always_pairs_scores():
    response = [b"member1", b"1", b"member2", b"2.5"]

    assert zpop_score_pairs(response) == [(b"member1", 1.0), (b"member2", 2.5)]


@pytest.mark.fixed_client
def test_parse_sentinel_masters_resp3_returns_master_dict():
    response = [
        {
            b"name": b"redis-py-test",
            b"ip": b"127.0.0.1",
            b"port": b"6379",
            b"flags": b"master",
            b"num-other-sentinels": b"1",
        }
    ]

    masters = parse_sentinel_masters_resp3(response)

    assert set(masters) == {"redis-py-test"}
    assert masters["redis-py-test"]["flags"] == {"master"}
    assert masters["redis-py-test"]["is_master"] is True
    assert masters["redis-py-test"]["is_sdown"] is False


@pytest.mark.fixed_client
def test_parse_acl_log_resp3_legacy_decodes_string_values():
    # On a RESP3 connection with the default legacy_responses=True, each ACL
    # LOG entry arrives as a map. The scalar string fields (reason, context,
    # object, username) are bulk strings and must be decoded to ``str`` so the
    # result matches what parse_acl_log() produces from a RESP2 connection.
    client_info = b"id=3 addr=127.0.0.1:52654 name= age=0 user=someuser"
    resp3_entry = {
        b"count": 1,
        b"reason": b"auth",
        b"context": b"toplevel",
        b"object": b"AUTH",
        b"username": b"someuser",
        b"age-seconds": b"8.038",
        b"client-info": client_info,
        b"entry-id": 0,
        b"timestamp-created": 1700000000000,
        b"timestamp-last-updated": 1700000000000,
    }

    parsed = parse_acl_log_resp3_to_resp2_legacy([resp3_entry])[0]

    assert parsed["reason"] == "auth"
    assert parsed["context"] == "toplevel"
    assert parsed["object"] == "AUTH"
    assert parsed["username"] == "someuser"

    # Must match the RESP2-wire legacy shape exactly (the default-config promise).
    resp2_entry = [
        b"count",
        1,
        b"reason",
        b"auth",
        b"context",
        b"toplevel",
        b"object",
        b"AUTH",
        b"username",
        b"someuser",
        b"age-seconds",
        b"8.038",
        b"client-info",
        client_info,
        b"entry-id",
        0,
        b"timestamp-created",
        1700000000000,
        b"timestamp-last-updated",
        1700000000000,
    ]
    assert parse_acl_log([resp2_entry]) == [parsed]


# FUNCTION STATS while a function runs. RESP2 sends the reply, the running
# script and the stats of each engine as flat lists; RESP3 sends them as maps.
_FUNCTION_STATS_RESP2 = [
    b"running_script",
    [b"name", b"busy", b"command", [b"FCALL", b"busy", b"0"], b"duration_ms", 500],
    b"engines",
    [b"LUA", [b"libraries_count", 1, b"functions_count", 1]],
]
_FUNCTION_STATS_RESP3 = {
    b"running_script": {
        b"name": b"busy",
        b"command": [b"FCALL", b"busy", b"0"],
        b"duration_ms": 500,
    },
    b"engines": {b"LUA": {b"libraries_count": 1, b"functions_count": 1}},
}


@pytest.mark.fixed_client
def test_parse_function_stats_unified_gives_the_resp3_shape():
    assert parse_function_stats_unified(_FUNCTION_STATS_RESP2) == _FUNCTION_STATS_RESP3
    assert parse_function_stats_unified(_FUNCTION_STATS_RESP3) == _FUNCTION_STATS_RESP3


@pytest.mark.fixed_client
def test_parse_function_stats_resp3_to_resp2_legacy_gives_the_resp2_shape():
    assert (
        parse_function_stats_resp3_to_resp2_legacy(_FUNCTION_STATS_RESP3)
        == _FUNCTION_STATS_RESP2
    )


@pytest.mark.fixed_client
def test_parse_memory_stats_unified_converts_doubles_to_floats():
    # RESP2 sends the doubles of MEMORY STATS as bulk strings.
    stats = parse_memory_stats_unified(
        [
            b"peak.allocated",
            1024,
            b"db.0",
            [b"overhead.hashtable.main", 72, b"overhead.hashtable.expires", 0],
            b"dataset.percentage",
            b"95.5",
            b"fragmentation",
            b"1",
        ]
    )
    assert stats == {
        "peak.allocated": 1024,
        "db.0": {"overhead.hashtable.main": 72, "overhead.hashtable.expires": 0},
        "dataset.percentage": 95.5,
        "fragmentation": 1.0,
    }
    assert type(stats["fragmentation"]) is float
    assert type(stats["peak.allocated"]) is int
    # decode_responses=True gives str values
    assert parse_memory_stats_unified(["fragmentation", "0.5"]) == {
        "fragmentation": 0.5
    }
    # a value that isn't a number is left as it is
    assert parse_memory_stats_unified([b"allocator", b"jemalloc"]) == {
        "allocator": b"jemalloc"
    }


# pairs_to_dict / pairs_to_dict_typed turn a flat [k1, v1, k2, v2] reply into a
# dict and back the response callbacks. These cover their behavior (decoding
# flags, type coercion and its fallback) without a live server; they were
# previously only exercised indirectly through integration tests.


@pytest.mark.fixed_client
def test_pairs_to_dict_none_returns_empty_dict():
    assert pairs_to_dict(None) == {}


@pytest.mark.fixed_client
def test_pairs_to_dict_empty_list_returns_empty_dict():
    assert pairs_to_dict([]) == {}


@pytest.mark.fixed_client
def test_pairs_to_dict_basic_pairs_preserve_types():
    # Without any decode flag, keys and values are passed through as-is.
    assert pairs_to_dict([b"a", b"1", b"b", b"2"]) == {b"a": b"1", b"b": b"2"}


@pytest.mark.fixed_client
def test_pairs_to_dict_decode_keys_only():
    # Keys are decoded to str; values are left untouched.
    assert pairs_to_dict([b"a", b"1"], decode_keys=True) == {"a": b"1"}


@pytest.mark.fixed_client
def test_pairs_to_dict_decode_string_values_only():
    # Values are decoded to str; keys are left untouched.
    assert pairs_to_dict([b"a", b"1"], decode_string_values=True) == {b"a": "1"}


@pytest.mark.fixed_client
def test_pairs_to_dict_decode_keys_and_values():
    result = pairs_to_dict(
        [b"a", b"1", b"b", b"2"], decode_keys=True, decode_string_values=True
    )
    assert result == {"a": "1", "b": "2"}


@pytest.mark.fixed_client
def test_pairs_to_dict_non_bytes_values_pass_through_when_decoding():
    # str_if_bytes only decodes bytes; ints and strs are returned unchanged.
    assert pairs_to_dict([b"n", 5], decode_string_values=True) == {b"n": 5}


@pytest.mark.fixed_client
def test_pairs_to_dict_typed_empty_list_returns_empty_dict():
    assert pairs_to_dict_typed([], {}) == {}


@pytest.mark.fixed_client
def test_pairs_to_dict_typed_coerces_value_for_known_key():
    result = pairs_to_dict_typed(["count", "5"], {"count": int})
    assert result == {"count": 5}
    assert isinstance(result["count"], int)


@pytest.mark.fixed_client
def test_pairs_to_dict_typed_uncoercible_value_falls_back_to_original():
    # int("abc") raises, so the raw value is kept instead of blowing up.
    assert pairs_to_dict_typed(["count", "abc"], {"count": int}) == {"count": "abc"}


@pytest.mark.fixed_client
def test_pairs_to_dict_typed_unknown_key_is_left_unchanged():
    assert pairs_to_dict_typed(["name", "redis"], {"count": int}) == {"name": "redis"}


@pytest.mark.fixed_client
def test_pairs_to_dict_typed_mixed_typed_and_untyped_keys():
    result = pairs_to_dict_typed(
        ["count", "5", "ratio", "1.5", "name", "redis"],
        {"count": int, "ratio": float},
    )
    assert result == {"count": 5, "ratio": 1.5, "name": "redis"}


# Expected values are what Redis 8 sends for these scores on a RESP2 connection.
@pytest.mark.fixed_client
@pytest.mark.parametrize(
    "score, expected",
    [
        (0.1, b"0.1"),
        (1 / 3, b"0.3333333333333333"),
        (2.5, b"2.5"),
        (-2.5, b"-2.5"),
        (1.0, b"1"),
        (-3.0, b"-3"),
        (0.0, b"0"),
        (-0.0, b"-0"),
        (1e16, b"10000000000000000"),
        (2.0**62, b"4611686018427387904"),
        (-(2.0**62), b"-4611686018427387904"),
        (5e18, b"5e+18"),
        (1e20, b"1e+20"),
        (1.5e300, b"1.5e+300"),
        (0.00001, b"0.00001"),
        (1e-7, b"1e-7"),
        (1.2345e-8, b"1.2345e-8"),
        (float("inf"), b"inf"),
        (float("-inf"), b"-inf"),
    ],
)
def test_resp3_to_resp2_legacy_score_matches_resp2_wire(score, expected):
    identity = {"withscores": True, "score_cast_func": lambda x: x}
    assert zset_score_pairs_resp3_to_resp2_legacy([[b"m", score]], **identity) == [
        (b"m", expected)
    ]
    assert zmpop_resp3_to_resp2_legacy([b"key", [[b"m", score]]]) == [
        b"key",
        [[b"m", expected]],
    ]


@pytest.mark.fixed_client
@pytest.mark.parametrize("score", [0.1, -0.0, float("inf")])
def test_resp3_to_resp2_legacy_default_float_cast(score):
    def check(value):
        assert type(value) is float
        assert math.copysign(1.0, value) == math.copysign(1.0, score)
        assert value == score

    for options in (
        {"withscores": True},
        {"withscores": True, "score_cast_func": float},
    ):
        [(member, value)] = zset_score_pairs_resp3_to_resp2_legacy(
            [[b"m", score]], **options
        )
        check(value)
    [rank, value] = zset_score_for_rank_resp3_to_resp2_legacy(
        [3, score], withscore=True
    )
    check(value)
    [(member, value)] = zpop_score_pairs_resp3_to_resp2_legacy([b"m", score])
    check(value)
    [(member, value)] = zpop_score_pairs_resp3_to_resp2_legacy([[b"m", score]])
    check(value)


@pytest.mark.fixed_client
def test_resp3_to_resp2_legacy_decoded_scores_are_str():
    # With decode_responses=True members arrive as str, and RESP2 would
    # return the score text as str too.
    identity = lambda x: x  # noqa: E731
    options = {"withscores": True, "score_cast_func": identity}
    assert zset_score_pairs_resp3_to_resp2_legacy([["m", 2.5]], **options) == [
        ("m", "2.5")
    ]
    assert zset_score_pairs_resp3_to_resp2_legacy_flat([["m", 1.0]], **options) == [
        "m",
        "1",
    ]
    assert zmpop_resp3_to_resp2_legacy(["key", [["m", 0.1]]]) == [
        "key",
        [["m", "0.1"]],
    ]
    assert zpop_score_pairs_resp3_to_resp2_legacy(
        ["m", 2.5], score_cast_func=identity
    ) == [("m", "2.5")]
    assert zpop_score_pairs_resp3_to_resp2_legacy(
        [["m", 2.5]], score_cast_func=identity
    ) == [("m", "2.5")]
    assert zset_score_for_rank_resp3_to_resp2_legacy(
        [3, 2.5], withscore=True, score_cast_func=identity, decode_responses=True
    ) == [3, "2.5"]
    # Without decoding the scores stay bytes.
    assert zset_score_for_rank_resp3_to_resp2_legacy(
        [3, 2.5], withscore=True, score_cast_func=identity
    ) == [3, b"2.5"]
    assert zmpop_resp3_to_resp2_legacy([b"key", [[b"m", 0.1]]]) == [
        b"key",
        [[b"m", b"0.1"]],
    ]
