import datetime
import math

from redis.utils import str_if_bytes


def timestamp_to_datetime(response):
    "Converts a unix timestamp to a Python datetime object"
    if not response:
        return None
    try:
        response = int(response)
    except ValueError:
        return None
    return datetime.datetime.fromtimestamp(response)


def parse_debug_object(response):
    "Parse the results of Redis's DEBUG OBJECT command into a Python dict"
    # The 'type' of the object is the first item in the response, but isn't
    # prefixed with a name
    response = str_if_bytes(response)
    response = "type:" + response
    response = dict(kv.split(":") for kv in response.split())

    # parse some expected int values from the string response
    # note: this cmd isn't spec'd so these may not appear in all redis versions
    int_fields = ("refcount", "serializedlength", "lru", "lru_seconds_idle")
    for field in int_fields:
        if field in response:
            response[field] = int(response[field])

    return response


def decode_info(response, encoder):
    """Decode a raw INFO reply, replacing any bytes the encoder can't decode"""
    try:
        return encoder.decode(response)
    except UnicodeDecodeError:
        return response.decode(encoder.encoding, errors="replace")


def parse_info(response, **options):
    """Parse the result of Redis's INFO command into a Python dict"""
    info = {}
    response = str_if_bytes(response)

    def get_value(value):
        if "," not in value and "=" not in value:
            try:
                if "." in value:
                    return float(value)
                else:
                    return int(value)
            except ValueError:
                return value
        elif "=" not in value:
            return [get_value(v) for v in value.split(",") if v]
        else:
            sub_dict = {}
            for item in value.split(","):
                if not item:
                    continue
                if "=" in item:
                    k, v = item.rsplit("=", 1)
                    sub_dict[k] = get_value(v)
                else:
                    sub_dict[item] = True
            return sub_dict

    for line in response.splitlines():
        if line and not line.startswith("#"):
            if line.find(":") != -1:
                # Split, the info fields keys and values.
                # Note that the value may contain ':'. but the 'host:'
                # pseudo-command is the only case where the key contains ':'
                key, value = line.split(":", 1)
                if key == "cmdstat_host":
                    key, value = line.rsplit(":", 1)

                if key == "module":
                    # Hardcode a list for key 'modules' since there could be
                    # multiple lines that started with 'module'
                    info.setdefault("modules", []).append(get_value(value))
                else:
                    info[key] = get_value(value)
            else:
                # if the line isn't splittable, append it to the "__raw__" key
                info.setdefault("__raw__", []).append(line)

    return info


def parse_memory_stats(response, **kwargs):
    """Parse the results of MEMORY STATS"""
    stats = pairs_to_dict(response, decode_keys=True, decode_string_values=True)
    for key, value in stats.items():
        if key.startswith("db.") and isinstance(value, list):
            stats[key] = pairs_to_dict(
                value, decode_keys=True, decode_string_values=True
            )
    return stats


def parse_memory_stats_unified(response, **kwargs):
    """Parse MEMORY STATS for unified RESP2 output.

    Unified responses decode structural keys and use native numeric values.
    The RESP2 reply holds the ratios and percentages, such as
    ``fragmentation``, as bulk strings, while RESP3 sends doubles. Convert
    them to ``float`` so both wires give the same values; the integer
    replies, and any string that isn't a number, are left as they are.
    """
    stats = pairs_to_dict(response, decode_keys=True)
    for key, value in stats.items():
        if key.startswith("db.") and isinstance(value, list):
            stats[key] = pairs_to_dict(value, decode_keys=True)
        elif isinstance(value, (bytes, str)):
            try:
                stats[key] = float(value)
            except ValueError:
                pass
    return stats


def parse_memory_stats_resp3(response, **kwargs):
    """Parse the results of MEMORY STATS on RESP3 wire.

    Each entry arrives as a top-level ``dict`` instead of a flat list of
    pairs; decode the keys to ``str`` and recurse into the per-database
    ``db.*`` sub-dicts so the Python shape matches what
    :func:`parse_memory_stats` produces from RESP2 wire.
    """
    stats = {str_if_bytes(key): value for key, value in response.items()}
    for key, value in stats.items():
        if key.startswith("db.") and isinstance(value, dict):
            stats[key] = {str_if_bytes(k): v for k, v in value.items()}
    return stats


def parse_list_of_dicts_resp3(response, **kwargs):
    """Parse list-of-maps responses on RESP3 wire (e.g. ``XINFO`` family).

    Each list entry arrives as a ``dict`` with bytes keys; decode the
    keys to ``str`` so the Python shape matches what
    :func:`parse_list_of_dicts` produces from RESP2 wire.
    """
    return [{str_if_bytes(key): value for key, value in x.items()} for x in response]


SENTINEL_STATE_TYPES = {
    "can-failover-its-master": int,
    "config-epoch": int,
    "down-after-milliseconds": int,
    "failover-timeout": int,
    "info-refresh": int,
    "last-hello-message": int,
    "last-ok-ping-reply": int,
    "last-ping-reply": int,
    "last-ping-sent": int,
    "master-link-down-time": int,
    "master-port": int,
    "num-other-sentinels": int,
    "num-slaves": int,
    "o-down-time": int,
    "pending-commands": int,
    "parallel-syncs": int,
    "port": int,
    "quorum": int,
    "role-reported-time": int,
    "s-down-time": int,
    "slave-priority": int,
    "slave-repl-offset": int,
    "voted-leader-epoch": int,
}


_SENTINEL_DERIVED_BOOLEANS = (
    ("is_master", "master"),
    ("is_slave", "slave"),
    ("is_sdown", "s_down"),
    ("is_odown", "o_down"),
    ("is_sentinel", "sentinel"),
    ("is_disconnected", "disconnected"),
    ("is_master_down", "master_down"),
)


def _add_derived_sentinel_booleans(result, flags):
    """Set ``is_master`` / ``is_slave`` / ``is_sdown`` / ``is_odown`` /
    ``is_sentinel`` / ``is_disconnected`` / ``is_master_down`` on
    ``result`` based on membership in the ``flags`` set.
    """
    for name, flag in _SENTINEL_DERIVED_BOOLEANS:
        result[name] = flag in flags


def parse_sentinel_state(item):
    result = pairs_to_dict_typed(item, SENTINEL_STATE_TYPES)
    flags = set(result["flags"].split(","))
    _add_derived_sentinel_booleans(result, flags)
    return result


def parse_sentinel_master(response, **options):
    return parse_sentinel_state(map(str_if_bytes, response))


def parse_sentinel_state_resp3(response, **options):
    result = {}
    for key in response:
        str_key = str_if_bytes(key)
        try:
            value = SENTINEL_STATE_TYPES[str_key](str_if_bytes(response[key]))
            result[str_key] = value
        except Exception:
            result[str_key] = str_if_bytes(response[key])
    flags = set(result["flags"].split(","))
    result["flags"] = flags
    _add_derived_sentinel_booleans(result, flags)
    return result


def parse_sentinel_masters(response, **options):
    result = {}
    for item in response:
        state = parse_sentinel_state(map(str_if_bytes, item))
        result[state["name"]] = state
    return result


def parse_sentinel_masters_resp3(response, **options):
    result = {}
    for master in response:
        state = parse_sentinel_state_resp3(master)
        result[state["name"]] = state
    return result


def parse_sentinel_slaves_and_sentinels(response, **options):
    return [parse_sentinel_state(map(str_if_bytes, item)) for item in response]


def parse_sentinel_slaves_and_sentinels_resp3(response, **options):
    return [parse_sentinel_state_resp3(item, **options) for item in response]


def _flatten_resp3_state_pairs(state):
    """Yield key/value pairs from a RESP3 sentinel-state map as a flat
    iterable suitable for ``parse_sentinel_state``.
    """
    for key, value in state.items():
        yield key
        yield value


def parse_sentinel_master_resp3_to_resp2_legacy(response, **options):
    return parse_sentinel_state(map(str_if_bytes, _flatten_resp3_state_pairs(response)))


def parse_sentinel_masters_resp3_to_resp2_legacy(response, **options):
    result = {}
    for master in response:
        state = parse_sentinel_state(
            map(str_if_bytes, _flatten_resp3_state_pairs(master))
        )
        result[state["name"]] = state
    return result


def parse_sentinel_slaves_and_sentinels_resp3_to_resp2_legacy(response, **options):
    return [
        parse_sentinel_state(map(str_if_bytes, _flatten_resp3_state_pairs(item)))
        for item in response
    ]


def parse_sentinel_master_unified(response, **options):
    state = parse_sentinel_state(map(str_if_bytes, response))
    state["flags"] = set(state["flags"].split(","))
    return state


def parse_sentinel_masters_unified(response, **options):
    result = {}
    for item in response:
        state = parse_sentinel_state(map(str_if_bytes, item))
        state["flags"] = set(state["flags"].split(","))
        result[state["name"]] = state
    return result


def parse_sentinel_slaves_and_sentinels_unified(response, **options):
    out = []
    for item in response:
        state = parse_sentinel_state(map(str_if_bytes, item))
        state["flags"] = set(state["flags"].split(","))
        out.append(state)
    return out


def parse_sentinel_master_unified_resp3(response, **options):
    state = parse_sentinel_state_resp3(response, **options)
    _add_derived_sentinel_booleans(state, state["flags"])
    return state


def parse_sentinel_masters_unified_resp3(response, **options):
    result = {}
    for master in response:
        state = parse_sentinel_state_resp3(master)
        _add_derived_sentinel_booleans(state, state["flags"])
        result[state["name"]] = state
    return result


def parse_sentinel_slaves_and_sentinels_unified_resp3(response, **options):
    out = []
    for item in response:
        state = parse_sentinel_state_resp3(item, **options)
        _add_derived_sentinel_booleans(state, state["flags"])
        out.append(state)
    return out


def parse_sentinel_get_master(response, **options):
    return response and (response[0], int(response[1])) or None


def pairs_to_dict(response, decode_keys=False, decode_string_values=False):
    """Create a dict given a list of key/value pairs"""
    if response is None:
        return {}
    if decode_keys or decode_string_values:
        # the iter form is faster, but I don't know how to make that work
        # with a str_if_bytes() map
        keys = response[::2]
        if decode_keys:
            keys = map(str_if_bytes, keys)
        values = response[1::2]
        if decode_string_values:
            values = map(str_if_bytes, values)
        return dict(zip(keys, values))
    else:
        it = iter(response)
        return dict(zip(it, it))


def pairs_to_dict_typed(response, type_info):
    it = iter(response)
    result = {}
    for key, value in zip(it, it):
        if key in type_info:
            try:
                value = type_info[key](value)
            except Exception:
                # if for some reason the value can't be coerced, just use
                # the string value
                pass
        result[key] = value
    return result


def _wrap_score_cast_func(score_cast_func):
    """Wrap score_cast_func to handle scientific notation in RESP2 byte strings.

    Redis returns scores as byte strings in RESP2, and large numbers may use
    scientific notation (e.g., b'1.7732526297292595e+18'). Python's int() cannot
    parse scientific notation directly.  Rather than unconditionally routing
    through float() (which would change the input type for every custom
    callable), we try the original function first and only fall back to
    converting through float() on ValueError.
    """
    if score_cast_func is float:
        return score_cast_func

    def _safe_cast(x):
        try:
            return score_cast_func(x)
        except (ValueError, TypeError):
            return score_cast_func(float(x))

    return _safe_cast


def zset_score_pairs(response, **options):
    """
    If ``withscores`` is specified in the options, return the response as
    a list of (value, score) pairs
    """
    if not response or not options.get("withscores"):
        return response
    score_cast_func = _wrap_score_cast_func(options.get("score_cast_func", float))
    it = iter(response)
    return list(zip(it, map(score_cast_func, it)))


def zpop_score_pairs(response, **options):
    """RESP2-wire ZPOPMAX/ZPOPMIN -> legacy ``list[(member, score), ...]``.

    ZPOPMAX/ZPOPMIN always include scores, so this parser intentionally
    does not depend on a ``withscores`` option.
    """
    if not response:
        return response
    score_cast_func = _wrap_score_cast_func(options.get("score_cast_func", float))
    it = iter(response)
    return list(zip(it, map(score_cast_func, it)))


def zset_score_for_rank(response, **options):
    """
    If ``withscores`` is specified in the options, return the response as
    a [value, score] pair
    """
    if not response or not options.get("withscore"):
        return response
    score_cast_func = _wrap_score_cast_func(options.get("score_cast_func", float))
    return [response[0], score_cast_func(response[1])]


def zset_score_pairs_resp3(response, **options):
    """
    If ``withscores`` is specified in the options, return the response as
    a list of [value, score] pairs
    """
    if not response or not options.get("withscores"):
        return response
    score_cast_func = options.get("score_cast_func", float)
    return [[name, score_cast_func(val)] for name, val in response]


def zset_score_for_rank_resp3(response, **options):
    """
    If ``withscores`` is specified in the options, return the response as
    a [value, score] pair
    """
    if not response or not options.get("withscore"):
        return response
    score_cast_func = options.get("score_cast_func", float)
    return [response[0], score_cast_func(response[1])]


def _score_to_resp2_bytes(value):
    """Re-encode a score back to the bytes form Redis returns on the RESP2
    wire so that custom ``score_cast_func`` callables observe the same
    input type they would receive on a RESP2 connection when the wire
    protocol is RESP3 but legacy response shapes are requested.
    """
    if isinstance(value, bytes):
        return value
    if isinstance(value, str):
        return value.encode()
    if isinstance(value, bool):
        return b"1" if value else b"0"
    return _redis_double_to_string(float(value)).encode()


def _score_to_resp2_reply(value, decode):
    """Like ``_score_to_resp2_bytes`` but returns ``str`` when the client
    decodes responses, as the RESP2 parser does with ``decode_responses=True``.
    """
    if not decode:
        return _score_to_resp2_bytes(value)
    if isinstance(value, str):
        return value
    if isinstance(value, bytes):
        return value.decode()
    if isinstance(value, bool):
        return "1" if value else "0"
    return _redis_double_to_string(float(value))


def _redis_double_to_string(value):
    """Format a double the way Redis does in replies (``d2string``).

    Redis writes integral values in the 64-bit range as plain integers and
    everything else with the shortest round-trip digits (``fpconv_dtoa``),
    e.g. ``0.1`` and not ``0.10000000000000001``.
    """
    if math.isinf(value):
        return "inf" if value > 0 else "-inf"
    if value == 0:
        return "-0" if math.copysign(1.0, value) < 0 else "0"
    if -(2**62) <= value <= 2**62 and value.is_integer():
        return str(int(value))
    # repr() gives the shortest digits that round-trip; lay them out with
    # the same rules as fpconv's emit_digits().
    text = repr(value)
    sign = ""
    if text[0] == "-":
        sign, text = "-", text[1:]
    mantissa, _, exponent = text.partition("e")
    int_part, _, frac = mantissa.partition(".")
    if not exponent:
        # repr() is plain decimal; Redis prints it the same way unless it
        # has 7+ fractional digits and a decimal exponent of 4 or more.
        if int_part != "0":
            exp = len(int_part) - 1
        else:
            exp = len(frac) - len(frac.lstrip("0")) + 1
        if len(frac) < 7 or exp < 4:
            return sign + text
    digits = (int_part + frac).lstrip("0")
    k = int(exponent or 0) - len(frac)
    stripped = digits.rstrip("0")
    k += len(digits) - len(stripped)
    digits = stripped
    ndigits = len(digits)
    exp = abs(k + ndigits - 1)
    if k >= 0 and exp < ndigits + 7:
        out = digits + "0" * k
    elif k < 0 and (k > -7 or exp < 4):
        offset = ndigits + k
        if offset <= 0:
            out = "0." + "0" * -offset + digits
        else:
            out = digits[:offset] + "." + digits[offset:]
    else:
        out = digits[0] + ("." + digits[1:] if ndigits > 1 else "")
        out += f"e{'-' if k + ndigits - 1 < 0 else '+'}{exp}"
    return sign + out


def zset_score_pairs_resp3_to_resp2_legacy(response, **options):
    """Convert RESP3 nested ``[[member, score], ...]`` to today's RESP2
    ``list[(member, score)]`` shape: tuples instead of lists, scores
    re-encoded to bytes (``str`` when the client decodes responses) before
    being passed to ``score_cast_func`` so the cast receives the same input
    as on a RESP2 connection.
    """
    if not response or not options.get("withscores"):
        return response
    score_cast_func = options.get("score_cast_func", float)
    if score_cast_func is float:
        return [(member, float(score)) for member, score in response]
    score_cast_func = _wrap_score_cast_func(score_cast_func)
    # Members are str exactly when the client decodes responses.
    decode = isinstance(response[0][0], str)
    return [
        (member, score_cast_func(_score_to_resp2_reply(score, decode)))
        for member, score in response
    ]


def zset_score_pairs_resp3_to_resp2_legacy_flat(response, **options):
    """Convert RESP3 nested ``[[member, score], ...]`` to the flat raw RESP2
    wire shape ``[member, score_bytes, ...]`` used by ZDIFF in v8.0.0b1
    (scores are ``str`` when the client decodes responses).

    ZDIFF historically did not propagate ``withscores`` to the response
    callback, so the legacy RESP2 callback was a no-op and the raw flat
    wire response was returned to the user. This helper reproduces that
    shape on RESP3 wires so ``legacy_responses=True`` keeps emitting the
    same Python value regardless of the underlying protocol.
    """
    if not response or not options.get("withscores"):
        return response
    decode = isinstance(response[0][0], str)
    flat = []
    for member, score in response:
        flat.append(member)
        flat.append(_score_to_resp2_reply(score, decode))
    return flat


def zset_score_for_rank_resp3_to_resp2_legacy(response, **options):
    """RESP3-wire ZRANK/ZREVRANK WITHSCORE → legacy RESP2 ``[rank, score]``.

    The shape ``[rank, score]`` is identical between RESP2 and RESP3; only
    the score is re-encoded to bytes (``str`` when ``decode_responses`` is
    passed as true) before being passed to ``score_cast_func`` so the cast
    observes the same input type it would on a RESP2 connection.
    """
    if not response or not options.get("withscore"):
        return response
    score_cast_func = options.get("score_cast_func", float)
    if score_cast_func is float:
        return [response[0], float(response[1])]
    score_cast_func = _wrap_score_cast_func(score_cast_func)
    # The reply has no member to tell whether the client decodes responses,
    # so ZRANK/ZREVRANK pass ``decode_responses`` in the options.
    decode = options.get("decode_responses", False)
    return [response[0], score_cast_func(_score_to_resp2_reply(response[1], decode))]


def zset_score_pairs_unified(response, **options):
    """RESP2-wire WITHSCORES → unified ``list[[member, score], ...]``.

    Normalises RESP2 byte-string scores through ``float`` before applying
    ``score_cast_func`` so the cast receives the same input type as on a
    RESP3 connection.
    """
    if not response or not options.get("withscores"):
        return response
    score_cast_func = _wrap_score_cast_func(options.get("score_cast_func", float))
    it = iter(response)
    return [[val, score_cast_func(float(score))] for val, score in zip(it, it)]


def zset_score_for_rank_unified(response, **options):
    """RESP2-wire ZRANK/ZREVRANK WITHSCORE → unified ``[rank, score]``.

    Normalises the RESP2 byte-string score through ``float`` before
    applying ``score_cast_func`` so the cast receives the same input type
    as on a RESP3 connection.
    """
    if not response or not options.get("withscore"):
        return response
    score_cast_func = _wrap_score_cast_func(options.get("score_cast_func", float))
    return [response[0], score_cast_func(float(response[1]))]


def zpop_score_pairs_unified(response, **options):
    """RESP2-wire ZPOPMAX/ZPOPMIN → unified ``list[[member, score], ...]``.

    ZPOPMAX/ZPOPMIN always include scores; no ``withscores`` gate is
    required. Scores are normalised through ``float`` before applying
    ``score_cast_func`` for parity with RESP3.
    """
    if not response:
        return response
    score_cast_func = _wrap_score_cast_func(options.get("score_cast_func", float))
    it = iter(response)
    return [[val, score_cast_func(float(score))] for val, score in zip(it, it)]


def zpop_score_pairs_resp3_unified(response, **options):
    """RESP3-wire ZPOPMAX/ZPOPMIN → unified ``list[[member, score], ...]``.

    Without ``count`` RESP3 returns a flat ``[member, score]``; with
    ``count`` it returns a nested ``[[member, score], ...]``. Both shapes
    are normalised to a nested list with ``score_cast_func`` applied.
    """
    if not response:
        return response
    score_cast_func = options.get("score_cast_func", float)
    if isinstance(response[0], list):
        return [[name, score_cast_func(val)] for name, val in response]
    return [[response[0], score_cast_func(response[1])]]


def zpop_score_pairs_resp3_to_resp2_legacy(response, **options):
    """RESP3-wire ZPOPMAX/ZPOPMIN → legacy RESP2 ``list[(member, score), ...]``.

    Both RESP3 shapes (flat without ``count``; nested with ``count``) are
    converted to a list of tuples. Scores are re-encoded to bytes (``str``
    when the client decodes responses) before being passed to
    ``score_cast_func`` so the cast observes the same input type it would
    on a RESP2 connection.
    """
    if not response:
        return response
    score_cast_func = options.get("score_cast_func", float)
    if score_cast_func is float:
        if isinstance(response[0], list):
            return [(member, float(score)) for member, score in response]
        return [(response[0], float(response[1]))]
    score_cast_func = _wrap_score_cast_func(score_cast_func)
    if isinstance(response[0], list):
        decode = isinstance(response[0][0], str)
        return [
            (member, score_cast_func(_score_to_resp2_reply(score, decode)))
            for member, score in response
        ]
    decode = isinstance(response[0], str)
    return [(response[0], score_cast_func(_score_to_resp2_reply(response[1], decode)))]


def bzpop_score_unified(response, **options):
    """BZPOPMAX/BZPOPMIN → unified ``[key, member, score]``.

    Works for both RESP2 (bytes score) and RESP3 (float score) wire shapes.
    """
    if not response:
        return None
    return [response[0], response[1], float(response[2])]


def bzpop_score_resp3_to_resp2_legacy(response, **options):
    """RESP3-wire BZPOPMAX/BZPOPMIN → legacy RESP2 ``(key, member, score)``.

    Matches the v8.0.0b1 RESP2-wire callback shape (tuple, ``float`` score).
    """
    if not response:
        return None
    return (response[0], response[1], float(response[2]))


def zmpop_resp3_to_resp2_legacy(response, **options):
    """RESP3-wire ZMPOP/BZMPOP → legacy RESP2 ``[name, [[member, b"score"], ...]]``.

    Re-encodes RESP3 native float scores back to the bytes form Redis
    returns on the RESP2 wire (``str`` when the client decodes responses)
    so callers observe today's RESP2 raw shape.
    """
    if not response:
        return response
    decode = isinstance(response[0], str)
    return [
        response[0],
        [
            [member, _score_to_resp2_reply(score, decode)]
            for member, score in response[1]
        ],
    ]


def zmpop_unified(response, **options):
    """ZMPOP/BZMPOP → unified ``[name, [[member, float_score], ...]]``.

    Used for the ``legacy_responses=False`` overlay on RESP2 wire to mirror
    RESP3's native float-score shape.
    """
    if not response:
        return response
    return [
        response[0],
        [[member, float(score)] for member, score in response[1]],
    ]


def hrandfield_unified(response, **options):
    """RESP2-wire HRANDFIELD WITHVALUES → unified ``list[[field, value], ...]``.

    Plain (no-values) responses — flat list of fields — pass through.
    The ``withvalues`` option (forwarded by the command method) selects the
    pairing branch so the no-values flat result is never misread.
    """
    if not response or not options.get("withvalues"):
        return response
    if isinstance(response[0], list):
        return response
    it = iter(response)
    return [[field, value] for field, value in zip(it, it)]


def hrandfield_resp3_to_resp2_legacy(response, **options):
    """RESP3-wire HRANDFIELD WITHVALUES → legacy RESP2 flat ``[field, value, ...]``.

    Plain (no-values) responses — flat list of fields — pass through.
    """
    if not response or not options.get("withvalues"):
        return response
    if not isinstance(response[0], list):
        return response
    flat = []
    for field, value in response:
        flat.append(field)
        flat.append(value)
    return flat


def parse_geopos_unified(response, **options):
    """GEOPOS → unified ``list[list[float, float] | None]``.

    Used for the ``legacy_responses=False`` overlay on RESP2 wire to mirror
    RESP3's native ``list[list]`` shape.
    """
    return [[float(ll[0]), float(ll[1])] if ll is not None else None for ll in response]


def parse_geopos_resp3_to_resp2_legacy(response, **options):
    """RESP3-wire GEOPOS → legacy RESP2 ``list[tuple(float, float) | None]``.

    Matches today's RESP2-wire callback shape (tuple coordinates).
    """
    return [(float(ll[0]), float(ll[1])) if ll is not None else None for ll in response]


def parse_lcs_idx_unified(response, **options):
    """LCS with IDX → unified ``dict``.

    Used for the ``legacy_responses=False`` overlay on RESP2 wire to mirror
    RESP3's native ``dict`` shape. Non-IDX responses (``bytes`` / ``int``)
    pass through unchanged.
    """
    if isinstance(response, list):
        it = iter(response)
        return {str_if_bytes(key): value for key, value in zip(it, it)}
    if isinstance(response, dict):
        return {str_if_bytes(key): value for key, value in response.items()}
    return response


def parse_lcs_idx_resp3_to_resp2_legacy(response, **options):
    """RESP3-wire LCS with IDX → legacy RESP2 flat list shape.

    Reproduces today's RESP2 raw output (``[b"matches", [...], b"len", n]``).
    Non-IDX responses pass through unchanged.
    """
    if not isinstance(response, dict):
        return response
    out: list = []
    for key, value in response.items():
        out.append(key)
        out.append(value)
    return out


def parse_client_trackinginfo_unified(response, **options):
    """CLIENT TRACKINGINFO → unified ``dict[str, Any]``.

    Accepts either RESP2's flat ``[label, value, ...]`` list or RESP3's
    native ``dict`` and returns a ``dict`` with ``str`` keys.
    """
    if isinstance(response, dict):
        data = {str_if_bytes(key): value for key, value in response.items()}
    else:
        data = {
            str_if_bytes(key): value
            for key, value in zip(response[::2], response[1::2])
        }
    if "flags" in data:
        data["flags"] = [str_if_bytes(flag) for flag in data["flags"]]
    if "prefixes" in data:
        data["prefixes"] = [str_if_bytes(prefix) for prefix in data["prefixes"]]
    return data


def parse_client_trackinginfo_resp3_to_resp2_legacy(response, **options):
    """RESP3-wire CLIENT TRACKINGINFO → legacy RESP2 flat ``list``.

    Mirrors today's RESP2-wire callback (``list(map(str_if_bytes, r))``):
    labels are decoded to ``str`` while values are preserved as-is.
    """
    if not isinstance(response, dict):
        return list(map(str_if_bytes, response))
    out: list = []
    for key, value in response.items():
        out.append(str_if_bytes(key))
        out.append(value)
    return out


def sort_return_tuples(response, **options):
    """
    If ``groups`` is specified, return the response as a list of
    n-element tuples with n being the value found in options['groups']
    """
    if not response or not options.get("groups"):
        return response
    n = options["groups"]
    return list(zip(*[response[i::n] for i in range(n)]))


def parse_stream_list(response, **options):
    if response is None:
        return None
    data = []
    for r in response:
        if r is not None:
            if "claim_min_idle_time" in options:
                data.append((r[0], pairs_to_dict(r[1]), *r[2:]))
            else:
                data.append((r[0], pairs_to_dict(r[1])))
        else:
            data.append((None, None))
    return data


def pairs_to_dict_with_str_keys(response):
    return pairs_to_dict(response, decode_keys=True)


def parse_list_of_dicts(response):
    return list(map(pairs_to_dict_with_str_keys, response))


def parse_xclaim(response, **options):
    if options.get("parse_justid", False):
        return response
    return parse_stream_list(response)


def parse_xautoclaim(response, **options):
    if options.get("parse_justid", False):
        return response[1]
    response[1] = parse_stream_list(response[1])
    return response


def parse_arinfo(response, **options):
    if isinstance(response, list):
        return pairs_to_dict(response, decode_keys=True)
    return {str_if_bytes(k): v for k, v in response.items()}


def parse_xinfo_stream(response, **options):
    if isinstance(response, list):
        data = pairs_to_dict(response, decode_keys=True)
    else:
        data = {str_if_bytes(k): v for k, v in response.items()}
    if not options.get("full", False):
        first = data.get("first-entry")
        if first is not None and first[0] is not None:
            data["first-entry"] = (first[0], pairs_to_dict(first[1]))
        last = data["last-entry"]
        if last is not None and last[0] is not None:
            data["last-entry"] = (last[0], pairs_to_dict(last[1]))
    else:
        data["entries"] = {_id: pairs_to_dict(entry) for _id, entry in data["entries"]}
        if len(data["groups"]) > 0 and isinstance(data["groups"][0], list):
            data["groups"] = [
                pairs_to_dict(group, decode_keys=True) for group in data["groups"]
            ]
            for g in data["groups"]:
                if g["consumers"] and g["consumers"][0] is not None:
                    g["consumers"] = [
                        pairs_to_dict(c, decode_keys=True) for c in g["consumers"]
                    ]
        else:
            data["groups"] = [
                {str_if_bytes(k): v for k, v in group.items()}
                for group in data["groups"]
            ]
            for g in data["groups"]:
                if g["consumers"] and g["consumers"][0] is not None:
                    g["consumers"] = [
                        {str_if_bytes(k): v for k, v in c.items()}
                        for c in g["consumers"]
                    ]
    return data


def parse_xread(response, **options):
    if response is None:
        return []
    return [[r[0], parse_stream_list(r[1], **options)] for r in response]


def parse_xread_resp3(response, **options):
    if response is None:
        return {}
    return {
        key: [parse_stream_list(value, **options)] for key, value in response.items()
    }


def parse_xread_unified(response, **options):
    """XREAD/XREADGROUP → unified ``dict[stream, list[tuple[id, dict]]]``.

    Accepts either RESP2 (``list[[stream, entries]]``) or RESP3
    (``dict[stream, entries]``) wire shape. Empty result is ``{}``.
    """
    if not response:
        return {}
    if isinstance(response, dict):
        return {
            key: parse_stream_list(value, **options) for key, value in response.items()
        }
    return {
        stream: parse_stream_list(entries, **options) for stream, entries in response
    }


def parse_xread_resp3_to_resp2_legacy(response, **options):
    """RESP3-wire XREAD/XREADGROUP → legacy RESP2 ``list[[stream, entries]]``.

    Empty result ``{}`` is converted to ``[]`` to match today's RESP2 shape.
    """
    if not response:
        return []
    return [
        [key, parse_stream_list(value, **options)] for key, value in response.items()
    ]


def parse_xpending(response, **options):
    if options.get("parse_detail", False):
        return parse_xpending_range(response)
    consumers = [{"name": n, "pending": int(p)} for n, p in response[3] or []]
    return {
        "pending": response[0],
        "min": response[1],
        "max": response[2],
        "consumers": consumers,
    }


def parse_xpending_range(response):
    k = ("message_id", "consumer", "time_since_delivered", "times_delivered")
    return [dict(zip(k, r)) for r in response]


def float_or_none(response):
    if response is None:
        return None
    return float(response)


def bool_ok(response, **options):
    return str_if_bytes(response) == "OK"


def parse_zadd(response, **options):
    if response is None:
        return None
    if options.get("as_score"):
        return float(response)
    return int(response)


def _parse_client_info_fields(value):
    """Tokenize a single ``key=value`` client-info blob into a dict.

    The server emits this space-separated format for each client in
    ``CLIENT INFO`` and ``CLIENT LIST``, and inside ``ACL LOG`` entries.
    Two quirks have to be preserved: a value may contain ``=`` (a client
    name set to ``foo=bar``), so only the first ``=`` splits key from value;
    and a value may contain spaces (a Unix-socket ``addr``/``laddr`` path
    such as ``/tmp/redis sock/redis.sock``), so a token with no ``=`` is
    reattached to the previous value. ``last_key is None`` guards the leading
    and empty/whitespace-only cases, which yield an empty dict rather than
    raising.
    """
    fields = {}
    last_key = None
    for token in value.split(" "):
        if "=" in token:
            key, val = token.split("=", 1)
            fields[key] = val
            last_key = key
        elif last_key is not None:
            # A token without ``=`` continues the previous value, since a
            # Unix-socket path may contain spaces. Empty tokens (from two or
            # more consecutive spaces in the path) are reattached the same
            # way, so a run of spaces round-trips verbatim instead of being
            # collapsed. Tokens before the first ``key=`` (``last_key is
            # None``, i.e. a leading space or an empty/whitespace-only blob)
            # are dropped, yielding an empty dict rather than raising.
            fields[last_key] += " " + token
    return fields


def parse_client_list(response, **options):
    if options.pop("as_iter", False):
        return _client_list_iter(response)
    clients = []
    for c in str_if_bytes(response).splitlines():
        client_dict = _parse_client_info_fields(c)
        if client_dict:
            clients.append(client_dict)
    return clients


def _client_list_iter(response):
    """
    Yield one CLIENT LIST record at a time instead of building the full
    ``list[dict]`` up front, so a caller processing records one at a time
    doesn't need all of them held in memory simultaneously. The full reply
    is still buffered off the socket before this runs - that part is
    unavoidable given how RESP bulk-string framing (and hiredis) works -
    so this only trims the per-record dict/string overhead, not the raw
    reply size.

    Line boundaries are scanned for lazily (rather than via ``splitlines()``)
    so the generator never materializes a list of all lines either - that
    list would otherwise be held for the generator's whole lifetime, right
    alongside the decoded reply, defeating the point of iterating.
    """
    text = str_if_bytes(response)
    # With decode_responses=False, str_if_bytes decodes a full copy of the
    # reply. response is a parameter of this generator, so it stays alive
    # in this frame for as long as the generator does; drop it once decoded
    # so the original buffer doesn't sit alongside the decoded one for the
    # whole iteration.
    response = None
    end = len(text)
    start = 0
    while start < end:
        newline = text.find("\n", start)
        if newline == -1:
            newline = end
        client_dict = _parse_client_info_fields(text[start:newline])
        start = newline + 1
        if client_dict:
            yield client_dict


def parse_config_get(response, **options):
    response = [str_if_bytes(i) if i is not None else None for i in response]
    return response and pairs_to_dict(response) or {}


def parse_config_get_resp3_to_resp2_legacy(response, **options):
    """RESP3-wire CONFIG GET → today's RESP2 ``dict[str, str]`` shape.

    On RESP3 the server returns a map; convert both keys and values via
    ``str_if_bytes`` so callers using ``r.config_get()["timeout"]``
    keep working when the wire is RESP3 with ``legacy_responses=True``.
    """
    if not response:
        return {}
    return {
        str_if_bytes(key) if key is not None else None: (
            str_if_bytes(value) if value is not None else None
        )
        for key, value in response.items()
    }


def parse_scan(response, **options):
    cursor, r = response
    return int(cursor), r


def parse_hscan(response, **options):
    cursor, r = response
    no_values = options.get("no_values", False)
    if no_values:
        payload = r or []
    else:
        payload = r and pairs_to_dict(r) or {}
    return int(cursor), payload


def parse_zscan(response, **options):
    score_cast_func = _wrap_score_cast_func(options.get("score_cast_func", float))
    cursor, r = response
    it = iter(r)
    return int(cursor), list(zip(it, map(score_cast_func, it)))


def parse_zscan_unified(response, **options):
    score_cast_func = _wrap_score_cast_func(options.get("score_cast_func", float))
    cursor, r = response
    it = iter(r)
    return int(cursor), [
        [value, score_cast_func(float(score))] for value, score in zip(it, it)
    ]


def parse_zmscore(response, **options):
    # zmscore: list of scores (double precision floating point number) or nil
    return [float(score) if score is not None else None for score in response]


def parse_increx_unified(response, **options):
    """RESP2-wire INCREX -> unified ``[value, increment]``.

    With ``BYFLOAT`` the RESP2 reply holds both numbers as bulk strings,
    while RESP3 sends doubles. Convert the strings to ``float`` so both
    wires give the same values; the integer replies of the default and
    ``BYINT`` forms are left as they are.
    """
    return [float(x) if isinstance(x, (bytes, str)) else x for x in response]


def parse_slowlog_get(response, **options):
    space = " " if options.get("decode_responses", False) else b" "

    def parse_item(item):
        result = {"id": item[0], "start_time": int(item[1]), "duration": int(item[2])}
        # Redis Enterprise injects another entry at index [3], which has
        # the complexity info (i.e. the value N in case the command has
        # an O(N) complexity) instead of the command.
        if isinstance(item[3], list):
            result["command"] = space.join(item[3])

            # These fields are optional, depends on environment.
            if len(item) >= 6:
                result["client_address"] = item[4]
                result["client_name"] = item[5]
        else:
            result["complexity"] = item[3]
            result["command"] = space.join(item[4])

            # These fields are optional, depends on environment.
            if len(item) >= 7:
                result["client_address"] = item[5]
                result["client_name"] = item[6]

        return result

    return [parse_item(item) for item in response]


def parse_stralgo(response, **options):
    """
    Parse the response from `STRALGO` command.
    Without modifiers the returned value is string.
    When LEN is given the command returns the length of the result
    (i.e integer).
    When IDX is given the command returns a dictionary with the LCS
    length and all the ranges in both the strings, start and end
    offset for each string, where there are matches.
    When WITHMATCHLEN is given, each array representing a match will
    also have the length of the match at the beginning of the array.
    """
    if options.get("len", False):
        return int(response)
    if options.get("idx", False):
        if options.get("withmatchlen", False):
            matches = [
                [(int(match[-1]))] + list(map(tuple, match[:-1]))
                for match in response[1]
            ]
        else:
            matches = [list(map(tuple, match)) for match in response[1]]
        return {
            str_if_bytes(response[0]): matches,
            str_if_bytes(response[2]): int(response[3]),
        }
    return str_if_bytes(response)


def parse_stralgo_unified(response, **options):
    """
    Parse STRALGO into the approved unified shape.

    The legacy parser returns tuple ranges for IDX responses. Unified
    responses use list ranges so RESP2 and RESP3 produce the same value.
    """
    if options.get("len", False):
        return int(response)
    if options.get("idx", False):
        if options.get("withmatchlen", False):
            matches = [
                [int(match[-1])] + [list(m) for m in match[:-1]]
                for match in response[1]
            ]
        else:
            matches = [[list(m) for m in match] for match in response[1]]
        return {
            str_if_bytes(response[0]): matches,
            str_if_bytes(response[2]): int(response[3]),
        }
    return str_if_bytes(response)


def parse_stralgo_resp3_unified(response, **options):
    """Parse RESP3 STRALGO into the same value as ``parse_stralgo_unified``."""
    if options.get("len", False):
        return int(response)
    if options.get("idx", False):
        if not isinstance(response, dict):
            return str_if_bytes(response)
        raw_matches = response.get("matches", response.get(b"matches", []))
        raw_len = response.get("len", response.get(b"len", 0))
        if options.get("withmatchlen", False):
            matches = [
                [int(match[-1])] + [list(m) for m in match[:-1]]
                for match in raw_matches
            ]
        else:
            matches = [[list(m) for m in match] for match in raw_matches]
        return {"matches": matches, "len": int(raw_len)}
    return str_if_bytes(response)


def parse_cluster_info(response, **options):
    response = str_if_bytes(response)
    return dict(line.split(":") for line in response.splitlines() if line)


def _parse_node_line(line):
    line_items = line.split(" ")
    node_id, addr, flags, master_id, ping, pong, epoch, connected = line.split(" ")[:8]
    ip = addr.split("@")[0]
    hostname = addr.split("@")[1].split(",")[1] if "@" in addr and "," in addr else ""
    node_dict = {
        "node_id": node_id,
        "hostname": hostname,
        "flags": flags,
        "master_id": master_id,
        "last_ping_sent": ping,
        "last_pong_rcvd": pong,
        "epoch": epoch,
        "slots": [],
        "migrations": [],
        "connected": True if connected == "connected" else False,
    }
    if len(line_items) >= 9:
        slots, migrations = _parse_slots(line_items[8:])
        node_dict["slots"], node_dict["migrations"] = slots, migrations
    return ip, node_dict


def _parse_slots(slot_ranges):
    slots, migrations = [], []
    for s_range in slot_ranges:
        if "->-" in s_range:
            slot_id, dst_node_id = s_range[1:-1].split("->-", 1)
            migrations.append(
                {"slot": slot_id, "node_id": dst_node_id, "state": "migrating"}
            )
        elif "-<-" in s_range:
            slot_id, src_node_id = s_range[1:-1].split("-<-", 1)
            migrations.append(
                {"slot": slot_id, "node_id": src_node_id, "state": "importing"}
            )
        else:
            s_range = [sl for sl in s_range.split("-")]
            slots.append(s_range)

    return slots, migrations


def parse_cluster_nodes(response, **options):
    """
    @see: https://redis.io/commands/cluster-nodes  # string / bytes
    @see: https://redis.io/commands/cluster-replicas # list of string / bytes
    """
    if isinstance(response, (str, bytes)):
        response = response.splitlines()
    return dict(_parse_node_line(str_if_bytes(node)) for node in response)


def parse_geosearch_generic(response, **options):
    """
    Parse the response of 'GEOSEARCH', GEORADIUS' and 'GEORADIUSBYMEMBER'
    commands according to 'withdist', 'withhash' and 'withcoord' labels.
    """
    try:
        if options["store"] or options["store_dist"]:
            # `store` and `store_dist` can't be combined
            # with other command arguments.
            # relevant to 'GEORADIUS' and 'GEORADIUSBYMEMBER'
            return response
    except KeyError:  # it means the command was sent via execute_command
        return response

    if not isinstance(response, list):
        response_list = [response]
    else:
        response_list = response

    if not options["withdist"] and not options["withcoord"] and not options["withhash"]:
        # just a bunch of places
        return response_list

    cast = {
        "withdist": float,
        "withcoord": lambda ll: (float(ll[0]), float(ll[1])),
        "withhash": int,
    }

    # zip all output results with each casting function to get
    # the properly native Python value.
    f = [lambda x: x]
    f += [cast[o] for o in ["withdist", "withhash", "withcoord"] if options[o]]
    return [list(map(lambda fv: fv[0](fv[1]), zip(f, r))) for r in response_list]


def parse_geosearch_generic_unified(response, **options):
    """
    Parse GEOSEARCH/GEORADIUS responses using tuple coordinates.
    """
    try:
        if options["store"] or options["store_dist"]:
            return response
    except KeyError:
        return response

    response_list = response if isinstance(response, list) else [response]

    if not options["withdist"] and not options["withcoord"] and not options["withhash"]:
        return response_list

    cast = {
        "withdist": float,
        "withcoord": lambda ll: (float(ll[0]), float(ll[1])),
        "withhash": int,
    }
    funcs = [lambda x: x]
    funcs += [cast[o] for o in ["withdist", "withhash", "withcoord"] if options[o]]
    return [list(map(lambda fv: fv[0](fv[1]), zip(funcs, r))) for r in response_list]


def parse_command(response, **options):
    commands = {}
    for command in response:
        cmd_dict = {}
        cmd_name = str_if_bytes(command[0])
        cmd_dict["name"] = cmd_name
        cmd_dict["arity"] = int(command[1])
        cmd_dict["flags"] = [str_if_bytes(flag) for flag in command[2]]
        cmd_dict["first_key_pos"] = command[3]
        cmd_dict["last_key_pos"] = command[4]
        cmd_dict["step_count"] = command[5]
        if len(command) > 6:
            cmd_dict["acl_categories"] = [
                str_if_bytes(category) for category in command[6]
            ]
        if len(command) > 7:
            cmd_dict["tips"] = command[7]
            cmd_dict["key_specifications"] = command[8]
            cmd_dict["subcommands"] = command[9]
        commands[cmd_name] = cmd_dict
    return commands


def parse_command_unified(response, **options):
    commands = {}
    for command in response:
        cmd_dict = {}
        cmd_name = str_if_bytes(command[0])
        cmd_dict["name"] = cmd_name
        cmd_dict["arity"] = int(command[1])
        cmd_dict["flags"] = {str_if_bytes(flag) for flag in command[2]}
        cmd_dict["first_key_pos"] = command[3]
        cmd_dict["last_key_pos"] = command[4]
        cmd_dict["step_count"] = command[5]
        if len(command) > 6:
            cmd_dict["acl_categories"] = {str_if_bytes(c) for c in command[6]}
        if len(command) > 7:
            cmd_dict["tips"] = command[7]
            cmd_dict["key_specifications"] = command[8]
            cmd_dict["subcommands"] = command[9]
        commands[cmd_name] = cmd_dict
    return commands


def parse_command_resp3(response, **options):
    commands = {}
    for command in response:
        cmd_dict = {}
        cmd_name = str_if_bytes(command[0])
        cmd_dict["name"] = cmd_name
        cmd_dict["arity"] = command[1]
        cmd_dict["flags"] = {str_if_bytes(flag) for flag in command[2]}
        cmd_dict["first_key_pos"] = command[3]
        cmd_dict["last_key_pos"] = command[4]
        cmd_dict["step_count"] = command[5]
        cmd_dict["acl_categories"] = command[6]
        if len(command) > 7:
            cmd_dict["tips"] = command[7]
            cmd_dict["key_specifications"] = command[8]
            cmd_dict["subcommands"] = command[9]

        commands[cmd_name] = cmd_dict
    return commands


def parse_pubsub_numsub(response, **options):
    return list(zip(response[0::2], response[1::2]))


def parse_client_kill(response, **options):
    if isinstance(response, int):
        return response
    return str_if_bytes(response) == "OK"


def parse_acl_getuser(response, **options):
    if response is None:
        return None
    if isinstance(response, list):
        data = pairs_to_dict(response, decode_keys=True)
    else:
        data = {str_if_bytes(key): value for key, value in response.items()}

    # convert everything but user-defined data in 'keys' to native strings
    data["flags"] = list(map(str_if_bytes, data["flags"]))
    data["passwords"] = list(map(str_if_bytes, data["passwords"]))
    data["commands"] = str_if_bytes(data["commands"])
    if isinstance(data["keys"], str) or isinstance(data["keys"], bytes):
        data["keys"] = list(str_if_bytes(data["keys"]).split(" "))
    if data["keys"] == [""]:
        data["keys"] = []
    if "channels" in data:
        if isinstance(data["channels"], str) or isinstance(data["channels"], bytes):
            data["channels"] = list(str_if_bytes(data["channels"]).split(" "))
        if data["channels"] == [""]:
            data["channels"] = []
    if "selectors" in data:
        if data["selectors"] != [] and isinstance(data["selectors"][0], list):
            data["selectors"] = [
                list(map(str_if_bytes, selector)) for selector in data["selectors"]
            ]
        elif data["selectors"] != []:
            data["selectors"] = [
                {str_if_bytes(k): str_if_bytes(v) for k, v in selector.items()}
                for selector in data["selectors"]
            ]

    # split 'commands' into separate 'categories' and 'commands' lists
    commands, categories = [], []
    for command in data["commands"].split(" "):
        categories.append(command) if "@" in command else commands.append(command)

    data["commands"] = commands
    data["categories"] = categories
    data["enabled"] = "on" in data["flags"]
    return data


def parse_acl_log(response, **options):
    if response is None:
        return None
    if isinstance(response, list):
        data = []
        for log in response:
            log_data = pairs_to_dict(log, True, True)
            client_info = log_data.get("client-info", "")
            log_data["client-info"] = parse_client_info(client_info)

            # float() is lossy comparing to the "double" in C
            log_data["age-seconds"] = float(log_data["age-seconds"])
            data.append(log_data)
    else:
        data = bool_ok(response)
    return data


def parse_acl_log_resp3_to_resp2_legacy(response, **options):
    """RESP3-wire ACL LOG → today's RESP2 parsed shape.

    Each log entry arrives as a ``dict`` on RESP3 wire instead of a flat
    list of pairs; convert ``client-info`` from a string blob into the
    parsed ``dict`` and ``age-seconds`` to ``float`` so the Python shape
    matches what :func:`parse_acl_log` produces from RESP2 wire.

    Also used as the unified callback (Set D): the legacy and unified
    shapes coincide for ACL LOG.
    """
    if response is None:
        return None
    if not isinstance(response, list):
        return bool_ok(response)
    data = []
    for log in response:
        if isinstance(log, dict):
            log_data = {str_if_bytes(k): str_if_bytes(v) for k, v in log.items()}
        else:
            log_data = pairs_to_dict(log, True, True)
        client_info = log_data.get("client-info", "")
        log_data["client-info"] = parse_client_info(client_info)
        log_data["age-seconds"] = float(log_data["age-seconds"])
        data.append(log_data)
    return data


def parse_acl_log_resp3_unified(response, **options):
    """Parse RESP3 ACL LOG into the approved unified shape."""
    if response is None:
        return None
    if not isinstance(response, list):
        return bool_ok(response)
    data = []
    for entry in response:
        if isinstance(entry, dict):
            log_data = {str_if_bytes(k): v for k, v in entry.items()}
        else:
            log_data = pairs_to_dict(entry, True, True)
        if "age-seconds" in log_data:
            log_data["age-seconds"] = float(log_data["age-seconds"])
        if "client-info" in log_data:
            log_data["client-info"] = parse_client_info(log_data["client-info"])
        for key, value in list(log_data.items()):
            if key not in ("age-seconds", "client-info"):
                log_data[key] = str_if_bytes(value)
        data.append(log_data)
    return data


def parse_acl_getuser_unified(response, **options):
    """ACL GETUSER → unified shape with selectors as ``list[dict]``.

    On RESP2 wire each selector arrives as a flat ``[k, v, k, v, …]``
    list; pair them into dicts to match the RESP3 wire shape.
    """
    data = parse_acl_getuser(response, **options)
    if data is None:
        return data
    selectors = data.get("selectors")
    if selectors and isinstance(selectors[0], list):
        data["selectors"] = [
            dict(zip(selector[0::2], selector[1::2])) for selector in selectors
        ]
    return data


def parse_acl_getuser_resp3_to_resp2_legacy(response, **options):
    """RESP3-wire ACL GETUSER → today's RESP2 selectors as flat lists.

    Each selector arrives as a ``dict`` on RESP3 wire; flatten back to
    the interleaved ``[k, v, k, v, …]`` form produced by RESP2 wire.
    """
    data = parse_acl_getuser(response, **options)
    if data is None:
        return data
    selectors = data.get("selectors")
    if selectors and isinstance(selectors[0], dict):
        data["selectors"] = [
            [item for kv in selector.items() for item in kv] for selector in selectors
        ]
    return data


def parse_client_info(value):
    """
    Parsing client-info in ACL Log in following format.
    "key1=value1 key2=value2 key3=value3"
    """
    client_info = _parse_client_info_fields(str_if_bytes(value).strip())

    # Those fields are defined as int in networking.c
    for int_key in {
        "id",
        "age",
        "idle",
        "db",
        "sub",
        "psub",
        "multi",
        "qbuf",
        "qbuf-free",
        "obl",
        "argv-mem",
        "oll",
        "omem",
        "tot-mem",
    }:
        if int_key in client_info:
            client_info[int_key] = int(client_info[int_key])
    return client_info


def parse_set_result(response, **options):
    """
    Handle SET result since GET argument is available since Redis 6.2.
    Parsing SET result into:
    - BOOL
    - String when GET argument is used
    """
    if options.get("get"):
        # Redis will return a getCommand result.
        # See `setGenericCommand` in t_string.c
        return response
    return response and str_if_bytes(response) == "OK"


def parse_function_list_unified(response, **options):
    """FUNCTION LIST → unified ``list[dict]`` with bytes keys.

    Accepts either RESP2 wire (``list[list]`` of flat ``[k, v, k, v, …]``
    pairs, with the nested ``b"functions"`` value also a flat list of
    flat lists) or RESP3 wire (``list[dict]`` already in nested-map
    form). Both are normalised to ``list[dict]``.
    """
    if response is None:
        return None
    result = []
    for lib in response:
        if isinstance(lib, dict):
            result.append(lib)
            continue
        lib_dict = pairs_to_dict(lib)
        func_key = b"functions" if b"functions" in lib_dict else "functions"
        if func_key in lib_dict:
            functions = lib_dict[func_key]
            lib_dict[func_key] = [
                func if isinstance(func, dict) else pairs_to_dict(func)
                for func in functions
            ]
        result.append(lib_dict)
    return result


def parse_function_list_resp3_to_resp2_legacy(response, **options):
    """RESP3-wire FUNCTION LIST → today's RESP2 ``list[list]`` shape.

    Each library and each nested function arrives as a ``dict``; flatten
    them back to interleaved ``[k, v, k, v, …]`` lists so the Python
    shape matches what RESP2 wire produces natively.
    """
    if response is None:
        return None
    result = []
    for lib in response:
        if not isinstance(lib, dict):
            result.append(lib)
            continue
        flat = []
        for key, value in lib.items():
            flat.append(key)
            if key == b"functions" or key == "functions":
                flat.append(
                    [
                        [item for kv in func.items() for item in kv]
                        if isinstance(func, dict)
                        else func
                        for func in value
                    ]
                )
            else:
                flat.append(value)
        result.append(flat)
    return result


def parse_function_stats_unified(response, **options):
    """FUNCTION STATS → unified nested ``dict`` with bytes keys.

    Accepts either RESP2 wire (a flat ``[k, v, k, v, …]`` list whose
    ``running_script`` value is a flat list or ``None`` and whose
    ``engines`` value pairs each engine name with a flat list of its
    stats) or RESP3 wire (nested maps). Both are normalised to nested
    dicts.
    """
    if response is None or isinstance(response, dict):
        return response
    stats = {}
    it = iter(response)
    for key, value in zip(it, it):
        if isinstance(value, list):
            name = str_if_bytes(key)
            if name == "running_script":
                value = pairs_to_dict(value)
            elif name == "engines":
                engines = iter(value)
                value = {
                    engine: pairs_to_dict(engine_stats)
                    for engine, engine_stats in zip(engines, engines)
                }
        stats[key] = value
    return stats


def parse_function_stats_resp3_to_resp2_legacy(response, **options):
    """RESP3-wire FUNCTION STATS → today's RESP2 flat ``list`` shape.

    The reply, the running script and the stats of each engine each arrive
    as a ``dict``; flatten them back to interleaved ``[k, v, k, v, …]``
    lists so the Python shape matches what RESP2 wire produces natively.
    """
    if not isinstance(response, dict):
        return response
    flat = []
    for key, value in response.items():
        if isinstance(value, dict):
            if str_if_bytes(key) == "engines":
                engines = []
                for engine, engine_stats in value.items():
                    if isinstance(engine_stats, dict):
                        engine_stats = [
                            item for kv in engine_stats.items() for item in kv
                        ]
                    engines.append(engine)
                    engines.append(engine_stats)
                value = engines
            else:
                value = [item for kv in value.items() for item in kv]
        flat.append(key)
        flat.append(value)
    return flat


def parse_cluster_links_unified(response, **options):
    """CLUSTER LINKS → unified ``list[dict]`` with string keys.

    Accepts either RESP2 wire (``list[list]`` of flat pairs) or RESP3
    wire (``list[dict]``). Both are normalised to ``list[dict]``.
    """
    if response is None:
        return None
    return [
        {str_if_bytes(k): v for k, v in item.items()}
        if isinstance(item, dict)
        else pairs_to_dict(item, decode_keys=True)
        for item in response
    ]


def parse_cluster_links_resp3_to_resp2_legacy(response, **options):
    """RESP3-wire CLUSTER LINKS → today's RESP2 ``list[list]`` shape.

    Each link arrives as a ``dict`` with bytes keys; flatten back to
    interleaved ``[k, v, k, v, …]`` lists so the Python shape matches
    what RESP2 wire produces natively.
    """
    if response is None:
        return None
    return [
        [item for kv in link.items() for item in kv] if isinstance(link, dict) else link
        for link in response
    ]


def string_keys_to_dict(key_string, callback):
    return dict.fromkeys(key_string.split(), callback)


# The command-to-callback mapping dictionaries (``_RedisCallbacks``,
# ``_RedisCallbacksRESP2``, ``_RedisCallbacksRESP3``, …) and the
# ``get_response_callbacks`` selector live in
# ``redis/_parsers/response_callbacks.py``. They are re-exported below for
# backward compatibility so existing imports of the form
# ``from redis._parsers.helpers import _RedisCallbacks`` keep working. The
# import is placed at module bottom to avoid a circular import (the
# response_callbacks module imports parser helpers defined above).
# isort: off
from .response_callbacks import (  # noqa: E402, F401
    _RedisCallbacks,
    _RedisCallbacksRESP2,
    _RedisCallbacksRESP2Unified,
    _RedisCallbacksRESP3,
    _RedisCallbacksRESP3Unified,
    _RedisCallbacksRESP3toRESP2Legacy,
    get_response_callbacks,
)
# isort: on
