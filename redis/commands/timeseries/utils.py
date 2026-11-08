from ..helpers import nativestr


def list_to_dict(aList):
    return {nativestr(aList[i][0]): nativestr(aList[i][1]) for i in range(len(aList))}


def _pairs_to_dict(pairs):
    """Convert a list of [key, value] pairs to a dict without forcing str."""
    return {pairs[i][0]: pairs[i][1] for i in range(len(pairs))}


def _nativestr_dict(d):
    """Apply ``nativestr`` to every key and string-typed value of ``d``.

    Used by the RESP3-to-RESP2-legacy adapters so labels coming from a
    RESP3 native map match today's RESP2 ``list_to_dict`` semantics.
    """
    return {
        nativestr(k): nativestr(v) if isinstance(v, (bytes, str)) else v
        for k, v in d.items()
    }


def parse_query_labels(response, **kwargs):
    """Normalize the TS.QUERYLABELS reply to a ``set`` of unique strings.

    The reply is a flat array under RESP2 and a set under RESP3; both are
    surfaced as a Python ``set`` so the collection type is identical across
    protocols and reflects that the server returns a unique, unordered
    collection. Elements are kept byte-exact (never coerced or normalized) and
    the server already deduplicates them. A missing reply maps to an empty
    ``set`` so an empty result is a normal success rather than an error.
    """
    if response is None:
        return set()
    return set(response)


def parse_range(response, **kwargs):
    """Parse range response. Used by TS.RANGE and TS.REVRANGE (legacy shape)."""
    if not response:
        return []
    # Multi-aggregator: samples have >2 elements [timestamp, val1, val2, ...]
    if len(response[0]) > 2:
        return [tuple([r[0]] + [float(v) for v in r[1:]]) for r in response]
    return [tuple((r[0], float(r[1]))) for r in response]


def parse_range_unified(response, **kwargs):
    """Unified parser for TS.RANGE / TS.REVRANGE.

    Returns ``list[list]`` rather than ``list[tuple]`` so the unified
    shape is symmetric with the RESP3 wire format.
    """
    if not response:
        return []
    if len(response[0]) > 2:
        return [[r[0]] + [float(v) for v in r[1:]] for r in response]
    return [[r[0], float(r[1])] for r in response]


def parse_n_range(response, **kwargs):
    """Parse the TS.NRANGE / TS.NREVRANGE response.

    The wire shape is ``[[timestamp, [value_0, value_1, ...]], ...]`` where the
    value array preserves input key order. Its length equals the number of
    queried keys, except when a per-key AGGREGATION spec lists multiple
    aggregators: each such key then contributes one value per aggregator (in
    spec order), concatenated in key order. Rows are returned in server order
    (never re-sorted or reversed), and a missing raw sample or missing
    aggregation bucket is kept as ``float('nan')`` rather than converted to
    ``None``.
    """
    if not response:
        return []
    return [[int(row[0]), [float(v) for v in row[1]]] for row in response]


def parse_get(response):
    """Parse get response. Used by TS.GET (legacy shape)."""
    if not response:
        return None
    return int(response[0]), float(response[1])


def parse_get_unified(response, **kwargs):
    """Unified parser for TS.GET. Returns ``[int, float]``."""
    if not response:
        return None
    return [int(response[0]), float(response[1])]


def parse_m_get(response):
    """Parse multi get response (RESP2 wire)."""
    res = []
    for item in response:
        if not item[2]:
            res.append({nativestr(item[0]): [list_to_dict(item[1]), None, None]})
        else:
            res.append(
                {
                    nativestr(item[0]): [
                        list_to_dict(item[1]),
                        int(item[2][0]),
                        float(item[2][1]),
                    ]
                }
            )
    return sorted(res, key=lambda d: list(d.keys()))


def parse_m_get_unified(response, **kwargs):
    """Unified parser for TS.MGET.

    Emits ``{key: [labels_dict, sample]}`` where ``sample`` is
    ``[int, float]`` or ``[]`` when no sample exists. Handles both wire
    formats: the RESP2 wire arrives as a list of ``[key, label_pairs,
    sample]`` triples; the RESP3 wire is already keyed by name.
    """
    if isinstance(response, dict):
        res = {}
        for key, item in response.items():
            sample = item[1] if len(item) > 1 else []
            if not sample:
                res[key] = [item[0], []]
            else:
                res[key] = [item[0], [int(sample[0]), float(sample[1])]]
        return res
    res = {}
    for item in response:
        if not item[2]:
            res[item[0]] = [_pairs_to_dict(item[1]), []]
        else:
            res[item[0]] = [
                _pairs_to_dict(item[1]),
                [int(item[2][0]), float(item[2][1])],
            ]
    return res


def parse_m_get_resp3_to_resp2_legacy(response, **kwargs):
    """RESP3 wire → today's RESP2 legacy shape for TS.MGET."""
    res = []
    for key, item in response.items():
        labels = _nativestr_dict(item[0]) if item[0] else {}
        sample = item[1] if len(item) > 1 else []
        if not sample:
            res.append({nativestr(key): [labels, None, None]})
        else:
            res.append({nativestr(key): [labels, int(sample[0]), float(sample[1])]})
    return sorted(res, key=lambda d: list(d.keys()))


def parse_m_range(response, **kwargs):
    """Parse multi range response (RESP2 wire)."""
    res = []
    for item in response:
        res.append({nativestr(item[0]): [list_to_dict(item[1]), parse_range(item[2])]})
    return sorted(res, key=lambda d: list(d.keys()))


def _m_range_metadata(aggregation_type=None):
    if aggregation_type is None:
        # Aggregators are empty when TS.MRANGE/TS.MREVRANGE is called without
        # AGGREGATION; this mirrors RESP3 metadata such as {"aggregators": []}.
        return {"aggregators": []}
    if isinstance(aggregation_type, list):
        aggregators = aggregation_type
    else:
        aggregators = [aggregation_type]
    return {"aggregators": [nativestr(agg).lower() for agg in aggregators]}


def _nativestr_metadata(metadata):
    """Apply ``nativestr`` to RESP3 metadata so it matches the RESP2 shape."""
    return {
        nativestr(k): [nativestr(v) for v in values] for k, values in metadata.items()
    }


def parse_m_range_unified(response, **kwargs):
    """Unified parser for TS.MRANGE / TS.MREVRANGE.

    Emits ``{key: [labels_dict, metadata, samples]}`` regardless of wire
    format. RESP2 has no metadata element on the wire, so the command options
    are used to synthesize the same ``{"aggregators": ...}`` structure that
    RESP3 returns.
    """
    if isinstance(response, dict):
        res = {}
        for key, item in response.items():
            metadata = _nativestr_metadata(item[1]) if len(item) > 2 else []
            res[key] = [item[0], metadata, parse_range_unified(item[-1])]
        return res
    res = {}
    metadata = _m_range_metadata(kwargs.get("aggregation_type"))
    for item in response:
        res[item[0]] = [
            _pairs_to_dict(item[1]),
            metadata,
            parse_range_unified(item[2]),
        ]
    return res


def parse_m_range_resp3_to_resp2_legacy(response, **kwargs):
    """RESP3 wire → today's RESP2 legacy shape for TS.MRANGE / TS.MREVRANGE."""
    res = []
    for key, item in response.items():
        labels = _nativestr_dict(item[0]) if item[0] else {}
        samples = item[-1]
        res.append({nativestr(key): [labels, parse_range(samples)]})
    return sorted(res, key=lambda d: list(d.keys()))
