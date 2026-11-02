"""Response-callback dictionaries and the protocol/legacy selector.

This module is the single source of truth for the mapping between Redis
command names and the Python-side callbacks that post-process raw parser
output into user-facing values.

Six dictionaries are defined:

* ``_RedisCallbacks`` — entries that produce the same Python value
  regardless of wire protocol or legacy-response selection.
* ``_RedisCallbacksRESP2`` — RESP2 wire, legacy Python shapes.
* ``_RedisCallbacksRESP3`` — RESP3 wire, Python shapes from the previous
  RESP3 callbacks.
* ``_RedisCallbacksRESP2Unified`` — RESP2 wire, unified Python shapes
  (``legacy_responses=False``).
* ``_RedisCallbacksRESP3Unified`` — RESP3 wire, unified Python shapes
  (``legacy_responses=False``).
* ``_RedisCallbacksRESP3toRESP2Legacy`` — RESP3 wire converted back to the
  legacy RESP2 Python shapes for users who keep ``legacy_responses=True``
  on a RESP3 connection.

``get_response_callbacks`` merges ``_RedisCallbacks`` with the appropriate
protocol-specific overlay. Callers wrap the returned dict in
``CaseInsensitiveDict``.
"""

from typing import Any, Callable, Optional

from redis.himport import (
    HIMPORT_DISCARD,
    HIMPORT_DISCARDALL,
    HIMPORT_PREPARE,
    HIMPORT_SET,
)
from redis.utils import str_if_bytes

from .helpers import (
    bool_ok,
    bzpop_score_resp3_to_resp2_legacy,
    bzpop_score_unified,
    float_or_none,
    hrandfield_resp3_to_resp2_legacy,
    hrandfield_unified,
    pairs_to_dict,
    parse_acl_getuser,
    parse_acl_getuser_resp3_to_resp2_legacy,
    parse_acl_getuser_unified,
    parse_acl_log,
    parse_acl_log_resp3_to_resp2_legacy,
    parse_acl_log_resp3_unified,
    parse_arinfo,
    parse_client_info,
    parse_client_kill,
    parse_client_list,
    parse_client_trackinginfo_resp3_to_resp2_legacy,
    parse_client_trackinginfo_unified,
    parse_cluster_info,
    parse_cluster_links_resp3_to_resp2_legacy,
    parse_cluster_links_unified,
    parse_cluster_nodes,
    parse_command,
    parse_command_resp3,
    parse_command_unified,
    parse_config_get,
    parse_config_get_resp3_to_resp2_legacy,
    parse_debug_object,
    parse_function_list_resp3_to_resp2_legacy,
    parse_function_list_unified,
    parse_function_stats_resp3_to_resp2_legacy,
    parse_function_stats_unified,
    parse_geopos_resp3_to_resp2_legacy,
    parse_geopos_unified,
    parse_geosearch_generic,
    parse_geosearch_generic_unified,
    parse_hscan,
    parse_increx_unified,
    parse_info,
    parse_lcs_idx_resp3_to_resp2_legacy,
    parse_lcs_idx_unified,
    parse_list_of_dicts,
    parse_list_of_dicts_resp3,
    parse_memory_stats,
    parse_memory_stats_resp3,
    parse_memory_stats_unified,
    parse_pubsub_numsub,
    parse_scan,
    parse_sentinel_get_master,
    parse_sentinel_master,
    parse_sentinel_master_resp3_to_resp2_legacy,
    parse_sentinel_master_unified,
    parse_sentinel_master_unified_resp3,
    parse_sentinel_masters,
    parse_sentinel_masters_resp3,
    parse_sentinel_masters_resp3_to_resp2_legacy,
    parse_sentinel_masters_unified,
    parse_sentinel_masters_unified_resp3,
    parse_sentinel_slaves_and_sentinels,
    parse_sentinel_slaves_and_sentinels_resp3,
    parse_sentinel_slaves_and_sentinels_resp3_to_resp2_legacy,
    parse_sentinel_slaves_and_sentinels_unified,
    parse_sentinel_slaves_and_sentinels_unified_resp3,
    parse_sentinel_state_resp3,
    parse_set_result,
    parse_slowlog_get,
    parse_stralgo,
    parse_stralgo_resp3_unified,
    parse_stralgo_unified,
    parse_stream_list,
    parse_xautoclaim,
    parse_xclaim,
    parse_xinfo_stream,
    parse_xpending,
    parse_xread,
    parse_xread_resp3,
    parse_xread_resp3_to_resp2_legacy,
    parse_xread_unified,
    parse_zadd,
    parse_zmscore,
    parse_zscan,
    parse_zscan_unified,
    sort_return_tuples,
    string_keys_to_dict,
    timestamp_to_datetime,
    zmpop_resp3_to_resp2_legacy,
    zmpop_unified,
    zpop_score_pairs,
    zpop_score_pairs_resp3_to_resp2_legacy,
    zpop_score_pairs_resp3_unified,
    zpop_score_pairs_unified,
    zset_score_for_rank,
    zset_score_for_rank_resp3,
    zset_score_for_rank_resp3_to_resp2_legacy,
    zset_score_for_rank_unified,
    zset_score_pairs,
    zset_score_pairs_resp3,
    zset_score_pairs_resp3_to_resp2_legacy,
    zset_score_pairs_resp3_to_resp2_legacy_flat,
    zset_score_pairs_unified,
)

_RedisCallbacks = {
    **string_keys_to_dict(
        "AUTH COPY EXPIRE EXPIREAT HEXISTS HMSET MOVE MSETNX PERSIST PSETEX "
        "PEXPIRE PEXPIREAT RENAMENX SETEX SETNX SMOVE",
        bool,
    ),
    **string_keys_to_dict("HINCRBYFLOAT INCRBYFLOAT", float),
    **string_keys_to_dict(
        "ASKING FLUSHALL FLUSHDB LSET LTRIM MSET PFMERGE READONLY READWRITE "
        "RENAME SAVE SELECT SHUTDOWN SLAVEOF SWAPDB WATCH UNWATCH",
        bool_ok,
    ),
    **string_keys_to_dict("XREAD XREADGROUP", parse_xread),
    **string_keys_to_dict(
        "GEORADIUS GEORADIUSBYMEMBER GEOSEARCH",
        parse_geosearch_generic,
    ),
    **string_keys_to_dict("XRANGE XREVRANGE", parse_stream_list),
    "ACL GETUSER": parse_acl_getuser,
    "ACL LOAD": bool_ok,
    "ACL LOG": parse_acl_log,
    "ACL SETUSER": bool_ok,
    "ACL SAVE": bool_ok,
    "BLESS SCAN": parse_scan,
    "CLIENT INFO": parse_client_info,
    "CLIENT KILL": parse_client_kill,
    "CLIENT LIST": parse_client_list,
    "CLIENT PAUSE": bool_ok,
    "CLIENT SETINFO": bool_ok,
    "CLIENT SETNAME": bool_ok,
    "CLIENT UNBLOCK": bool,
    "CLUSTER ADDSLOTS": bool_ok,
    "CLUSTER ADDSLOTSRANGE": bool_ok,
    "CLUSTER DELSLOTS": bool_ok,
    "CLUSTER DELSLOTSRANGE": bool_ok,
    "CLUSTER FAILOVER": bool_ok,
    "CLUSTER FORGET": bool_ok,
    "CLUSTER INFO": parse_cluster_info,
    "CLUSTER MEET": bool_ok,
    "CLUSTER NODES": parse_cluster_nodes,
    "CLUSTER REPLICAS": parse_cluster_nodes,
    "CLUSTER REPLICATE": bool_ok,
    "CLUSTER RESET": bool_ok,
    "CLUSTER SAVECONFIG": bool_ok,
    "CLUSTER SET-CONFIG-EPOCH": bool_ok,
    "CLUSTER SETSLOT": bool_ok,
    "CLUSTER SLAVES": parse_cluster_nodes,
    "COMMAND": parse_command,
    "CONFIG RESETSTAT": bool_ok,
    "CONFIG SET": bool_ok,
    "FUNCTION DELETE": bool_ok,
    "FUNCTION FLUSH": bool_ok,
    "FUNCTION RESTORE": bool_ok,
    "GEODIST": float_or_none,
    HIMPORT_PREPARE: bool_ok,
    HIMPORT_SET: bool_ok,
    HIMPORT_DISCARD: int,
    HIMPORT_DISCARDALL: int,
    "HSCAN": parse_hscan,
    "INFO": parse_info,
    "LASTSAVE": timestamp_to_datetime,
    "MEMORY PURGE": bool_ok,
    "MODULE LOAD": bool,
    "MODULE UNLOAD": bool,
    "PING": lambda r: str_if_bytes(r) == "PONG",
    "PUBSUB NUMSUB": parse_pubsub_numsub,
    "PUBSUB SHARDNUMSUB": parse_pubsub_numsub,
    "QUIT": bool_ok,
    "SET": parse_set_result,
    "SCAN": parse_scan,
    "SCRIPT EXISTS": lambda r: list(map(bool, r)),
    "SCRIPT FLUSH": bool_ok,
    "SCRIPT KILL": bool_ok,
    "SCRIPT LOAD": str_if_bytes,
    "SENTINEL CKQUORUM": bool_ok,
    "SENTINEL FAILOVER": bool_ok,
    "SENTINEL FLUSHCONFIG": bool_ok,
    "SENTINEL GET-MASTER-ADDR-BY-NAME": parse_sentinel_get_master,
    "SENTINEL MONITOR": bool_ok,
    "SENTINEL RESET": bool_ok,
    "SENTINEL REMOVE": bool_ok,
    "SENTINEL SET": bool_ok,
    "SLOWLOG GET": parse_slowlog_get,
    "SLOWLOG RESET": bool_ok,
    "SORT": sort_return_tuples,
    "SSCAN": parse_scan,
    "TIME": lambda x: (int(x[0]), int(x[1])),
    "XAUTOCLAIM": parse_xautoclaim,
    "XCLAIM": parse_xclaim,
    "XGROUP CREATE": bool_ok,
    "XGROUP DESTROY": bool,
    "XGROUP SETID": bool_ok,
    "ARINFO": parse_arinfo,
    "XINFO STREAM": parse_xinfo_stream,
    "XPENDING": parse_xpending,
    "ZSCAN": parse_zscan,
}


_RedisCallbacksRESP2 = {
    **string_keys_to_dict(
        "SDIFF SINTER SMEMBERS SUNION", lambda r: r and set(r) or set()
    ),
    **string_keys_to_dict(
        "ZINTER ZRANGE ZRANGEBYSCORE ZREVRANGE ZREVRANGEBYSCORE ZUNION",
        zset_score_pairs,
    ),
    **string_keys_to_dict("ZPOPMAX ZPOPMIN", zpop_score_pairs),
    **string_keys_to_dict(
        "ZREVRANK ZRANK",
        zset_score_for_rank,
    ),
    **string_keys_to_dict("ZINCRBY ZSCORE", float_or_none),
    **string_keys_to_dict("BGREWRITEAOF BGSAVE", lambda r: True),
    **string_keys_to_dict("BLPOP BRPOP", lambda r: r and tuple(r) or None),
    **string_keys_to_dict(
        "BZPOPMAX BZPOPMIN", lambda r: r and (r[0], r[1], float(r[2])) or None
    ),
    "ACL CAT": lambda r: list(map(str_if_bytes, r)),
    "ACL GENPASS": str_if_bytes,
    "ACL HELP": lambda r: list(map(str_if_bytes, r)),
    "ACL LIST": lambda r: list(map(str_if_bytes, r)),
    "ACL USERS": lambda r: list(map(str_if_bytes, r)),
    "ACL WHOAMI": str_if_bytes,
    "CLIENT GETNAME": str_if_bytes,
    "CLIENT TRACKINGINFO": lambda r: list(map(str_if_bytes, r)),
    "CLUSTER GETKEYSINSLOT": lambda r: list(map(str_if_bytes, r)),
    "COMMAND GETKEYS": lambda r: list(map(str_if_bytes, r)),
    "CONFIG GET": parse_config_get,
    "DEBUG OBJECT": parse_debug_object,
    "GEOHASH": lambda r: list(map(str_if_bytes, r)),
    "GEOPOS": lambda r: list(
        map(lambda ll: (float(ll[0]), float(ll[1])) if ll is not None else None, r)
    ),
    "HGETALL": lambda r: r and pairs_to_dict(r) or {},
    "HOTKEYS GET": lambda r: None if r is None else [pairs_to_dict(m) for m in r],
    "MEMORY STATS": parse_memory_stats,
    "MODULE LIST": lambda r: [pairs_to_dict(m) for m in r],
    "RESET": str_if_bytes,
    "SENTINEL MASTER": parse_sentinel_master,
    "SENTINEL MASTERS": parse_sentinel_masters,
    "SENTINEL SENTINELS": parse_sentinel_slaves_and_sentinels,
    "SENTINEL SLAVES": parse_sentinel_slaves_and_sentinels,
    "STRALGO": parse_stralgo,
    "XINFO CONSUMERS": parse_list_of_dicts,
    "XINFO GROUPS": parse_list_of_dicts,
    "ZADD": parse_zadd,
    "ZMSCORE": parse_zmscore,
}


_RedisCallbacksRESP3 = {
    **string_keys_to_dict(
        "SDIFF SINTER SMEMBERS SUNION", lambda r: r and set(r) or set()
    ),
    **string_keys_to_dict(
        "ZRANGE ZINTER ZPOPMAX ZPOPMIN HGETALL XREADGROUP",
        lambda r, **kwargs: r,
    ),
    **string_keys_to_dict(
        "ZRANGE ZRANGEBYSCORE ZREVRANGE ZREVRANGEBYSCORE ZUNION",
        zset_score_pairs_resp3,
    ),
    **string_keys_to_dict(
        "ZREVRANK ZRANK",
        zset_score_for_rank_resp3,
    ),
    **string_keys_to_dict("XREAD XREADGROUP", parse_xread_resp3),
    "ACL LOG": lambda r: (
        [
            {str_if_bytes(key): str_if_bytes(value) for key, value in x.items()}
            for x in r
        ]
        if isinstance(r, list)
        else bool_ok(r)
    ),
    "COMMAND": parse_command_resp3,
    "CONFIG GET": parse_config_get_resp3_to_resp2_legacy,
    "MEMORY STATS": parse_memory_stats_resp3,
    "SENTINEL MASTER": parse_sentinel_state_resp3,
    "SENTINEL MASTERS": parse_sentinel_masters_resp3,
    "SENTINEL SENTINELS": parse_sentinel_slaves_and_sentinels_resp3,
    "SENTINEL SLAVES": parse_sentinel_slaves_and_sentinels_resp3,
    "STRALGO": lambda r, **options: (
        {str_if_bytes(key): str_if_bytes(value) for key, value in r.items()}
        if isinstance(r, dict)
        else str_if_bytes(r)
    ),
    "XINFO CONSUMERS": parse_list_of_dicts_resp3,
    "XINFO GROUPS": parse_list_of_dicts_resp3,
}


# RESP2 wire, unified response shapes (``legacy_responses=False``).
_RedisCallbacksRESP2Unified: dict[str, Callable[..., Any]] = {
    **_RedisCallbacksRESP2,
    **string_keys_to_dict(
        "ZDIFF ZINTER ZRANGE ZRANGEBYSCORE ZREVRANGE ZREVRANGEBYSCORE ZUNION",
        zset_score_pairs_unified,
    ),
    **string_keys_to_dict(
        "ZPOPMAX ZPOPMIN",
        zpop_score_pairs_unified,
    ),
    **string_keys_to_dict(
        "ZREVRANK ZRANK",
        zset_score_for_rank_unified,
    ),
    **string_keys_to_dict(
        "BZPOPMAX BZPOPMIN",
        bzpop_score_unified,
    ),
    **string_keys_to_dict("XREAD XREADGROUP", parse_xread_unified),
    **string_keys_to_dict(
        "GEORADIUS GEORADIUSBYMEMBER GEOSEARCH",
        parse_geosearch_generic_unified,
    ),
    **string_keys_to_dict("BLPOP BRPOP", lambda r: r or None),
    **string_keys_to_dict("ZMPOP BZMPOP", zmpop_unified),
    "ZSCAN": parse_zscan_unified,
    "ZRANDMEMBER": zset_score_pairs_unified,
    "HRANDFIELD": hrandfield_unified,
    "ACL GETUSER": parse_acl_getuser_unified,
    "CLIENT TRACKINGINFO": parse_client_trackinginfo_unified,
    "CLUSTER GETKEYSINSLOT": lambda r, **kwargs: r,
    "CLUSTER LINKS": parse_cluster_links_unified,
    "COMMAND": parse_command_unified,
    "COMMAND GETKEYS": lambda r, **kwargs: r,
    "FUNCTION LIST": parse_function_list_unified,
    "FUNCTION STATS": parse_function_stats_unified,
    "GEOPOS": parse_geopos_unified,
    "INCREX": parse_increx_unified,
    "LCS": parse_lcs_idx_unified,
    "MEMORY STATS": parse_memory_stats_unified,
    "SENTINEL MASTER": parse_sentinel_master_unified,
    "SENTINEL MASTERS": parse_sentinel_masters_unified,
    "SENTINEL SENTINELS": parse_sentinel_slaves_and_sentinels_unified,
    "SENTINEL SLAVES": parse_sentinel_slaves_and_sentinels_unified,
    "STRALGO": parse_stralgo_unified,
}


# RESP3 wire, unified response shapes (``legacy_responses=False``).
_RedisCallbacksRESP3Unified: dict[str, Callable[..., Any]] = {
    **_RedisCallbacksRESP3,
    **string_keys_to_dict(
        "ZDIFF ZINTER ZRANGE ZRANGEBYSCORE ZREVRANGE ZREVRANGEBYSCORE ZUNION",
        zset_score_pairs_resp3,
    ),
    **string_keys_to_dict(
        "ZPOPMAX ZPOPMIN",
        zpop_score_pairs_resp3_unified,
    ),
    **string_keys_to_dict(
        "BZPOPMAX BZPOPMIN",
        bzpop_score_unified,
    ),
    **string_keys_to_dict("XREAD XREADGROUP", parse_xread_unified),
    **string_keys_to_dict(
        "GEORADIUS GEORADIUSBYMEMBER GEOSEARCH",
        parse_geosearch_generic_unified,
    ),
    **string_keys_to_dict("BGREWRITEAOF BGSAVE", lambda r: True),
    "ZSCAN": parse_zscan_unified,
    "ZRANDMEMBER": zset_score_pairs_resp3,
    "ACL CAT": lambda r: list(map(str_if_bytes, r)),
    "ACL GENPASS": str_if_bytes,
    "ACL HELP": lambda r: list(map(str_if_bytes, r)),
    "ACL LIST": lambda r: list(map(str_if_bytes, r)),
    "ACL LOG": parse_acl_log_resp3_unified,
    "ACL USERS": lambda r: list(map(str_if_bytes, r)),
    "ACL WHOAMI": str_if_bytes,
    "CLIENT GETNAME": str_if_bytes,
    "CLIENT TRACKINGINFO": parse_client_trackinginfo_unified,
    "CLUSTER LINKS": parse_cluster_links_unified,
    "COMMAND": parse_command_unified,
    "DEBUG OBJECT": parse_debug_object,
    "FUNCTION LIST": parse_function_list_unified,
    "FUNCTION STATS": parse_function_stats_unified,
    "GEOHASH": lambda r: list(map(str_if_bytes, r)),
    "LCS": parse_lcs_idx_unified,
    "RESET": str_if_bytes,
    "SENTINEL MASTER": parse_sentinel_master_unified_resp3,
    "SENTINEL MASTERS": parse_sentinel_masters_unified_resp3,
    "SENTINEL SENTINELS": parse_sentinel_slaves_and_sentinels_unified_resp3,
    "SENTINEL SLAVES": parse_sentinel_slaves_and_sentinels_unified_resp3,
    "STRALGO": parse_stralgo_resp3_unified,
}


# RESP3 wire converted back to the legacy RESP2 Python shapes. Only the
# entries needed to undo RESP3-side differences are listed; everything
# else falls through to ``_RedisCallbacks``. Scores are re-encoded to bytes
# (``str`` when the client decodes responses) before being passed to
# ``score_cast_func`` so the callable observes the same input type it would
# on a RESP2 connection.
_RedisCallbacksRESP3toRESP2Legacy: dict[str, Callable[..., Any]] = {
    **string_keys_to_dict(
        "SDIFF SINTER SMEMBERS SUNION", lambda r: r and set(r) or set()
    ),
    **string_keys_to_dict(
        "ZINTER ZRANGE ZRANGEBYSCORE ZREVRANGE ZREVRANGEBYSCORE ZUNION",
        zset_score_pairs_resp3_to_resp2_legacy,
    ),
    "ZDIFF": zset_score_pairs_resp3_to_resp2_legacy_flat,
    "ZRANDMEMBER": zset_score_pairs_resp3_to_resp2_legacy_flat,
    **string_keys_to_dict(
        "ZPOPMAX ZPOPMIN",
        zpop_score_pairs_resp3_to_resp2_legacy,
    ),
    **string_keys_to_dict(
        "BZPOPMAX BZPOPMIN",
        bzpop_score_resp3_to_resp2_legacy,
    ),
    **string_keys_to_dict(
        "ZREVRANK ZRANK",
        zset_score_for_rank_resp3_to_resp2_legacy,
    ),
    **string_keys_to_dict("BGREWRITEAOF BGSAVE", lambda r: True),
    **string_keys_to_dict("XREAD XREADGROUP", parse_xread_resp3_to_resp2_legacy),
    **string_keys_to_dict("BLPOP BRPOP", lambda r: r and tuple(r) or None),
    **string_keys_to_dict("ZMPOP BZMPOP", zmpop_resp3_to_resp2_legacy),
    "HRANDFIELD": hrandfield_resp3_to_resp2_legacy,
    "ACL CAT": lambda r: list(map(str_if_bytes, r)),
    "ACL GENPASS": str_if_bytes,
    "ACL GETUSER": parse_acl_getuser_resp3_to_resp2_legacy,
    "ACL HELP": lambda r: list(map(str_if_bytes, r)),
    "ACL LIST": lambda r: list(map(str_if_bytes, r)),
    "ACL LOG": parse_acl_log_resp3_to_resp2_legacy,
    "ACL USERS": lambda r: list(map(str_if_bytes, r)),
    "ACL WHOAMI": str_if_bytes,
    "CLIENT GETNAME": str_if_bytes,
    "CLIENT TRACKINGINFO": parse_client_trackinginfo_resp3_to_resp2_legacy,
    "CLUSTER GETKEYSINSLOT": lambda r: list(map(str_if_bytes, r)),
    "CLUSTER LINKS": parse_cluster_links_resp3_to_resp2_legacy,
    "COMMAND GETKEYS": lambda r: list(map(str_if_bytes, r)),
    "CONFIG GET": parse_config_get_resp3_to_resp2_legacy,
    "DEBUG OBJECT": parse_debug_object,
    "FUNCTION LIST": parse_function_list_resp3_to_resp2_legacy,
    "FUNCTION STATS": parse_function_stats_resp3_to_resp2_legacy,
    "GEOHASH": lambda r: list(map(str_if_bytes, r)),
    "GEOPOS": parse_geopos_resp3_to_resp2_legacy,
    "LCS": parse_lcs_idx_resp3_to_resp2_legacy,
    "MEMORY STATS": parse_memory_stats_resp3,
    "RESET": str_if_bytes,
    "SENTINEL MASTER": parse_sentinel_master_resp3_to_resp2_legacy,
    "SENTINEL MASTERS": parse_sentinel_masters_resp3_to_resp2_legacy,
    "SENTINEL SENTINELS": parse_sentinel_slaves_and_sentinels_resp3_to_resp2_legacy,
    "SENTINEL SLAVES": parse_sentinel_slaves_and_sentinels_resp3_to_resp2_legacy,
    "XINFO CONSUMERS": parse_list_of_dicts_resp3,
    "XINFO GROUPS": parse_list_of_dicts_resp3,
}


def get_response_callbacks(
    user_protocol: Optional[int],
    legacy_responses: bool,
) -> dict[str, Callable[..., Any]]:
    """Return the merged callback dict for the given (protocol, legacy)
    combination.

    ``user_protocol`` is the value the user supplied to the client
    constructor (``None`` means "not specified"). ``legacy_responses``
    defaults to ``True`` and selects today's RESP2-style Python shapes
    even when the wire protocol is RESP3.

    Callers wrap the returned dict in ``CaseInsensitiveDict``.
    """
    callbacks: dict[str, Callable[..., Any]] = dict(_RedisCallbacks)
    if legacy_responses:
        if user_protocol is None:
            callbacks.update(_RedisCallbacksRESP3toRESP2Legacy)
        elif user_protocol in (3, "3"):
            callbacks.update(_RedisCallbacksRESP3)
        else:
            callbacks.update(_RedisCallbacksRESP2)
    else:
        if user_protocol is None or user_protocol in (3, "3"):
            callbacks.update(_RedisCallbacksRESP3Unified)
        else:
            callbacks.update(_RedisCallbacksRESP2Unified)
    return callbacks
