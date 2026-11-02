import itertools
import time
from typing import Any, Dict, List, Optional, Set, Union

from redis._parsers.helpers import pairs_to_dict
from redis.asyncio.client import Pipeline as AsyncioPipeline
from redis.client import NEVER_DECODE, Pipeline
from redis.commands.search.hybrid_query import (
    CombineResultsMethod,
    HybridCursorQuery,
    HybridPostProcessingConfig,
    HybridQuery,
)
from redis.commands.search.hybrid_result import HybridCursorResult, HybridResult
from redis.typing import KeyT
from redis.utils import (
    check_protocol_version,
    decode_field_value,
    deprecated_function,
    experimental_method,
    str_if_bytes,
)

from ..helpers import get_legacy_responses, get_protocol_version
from .aggregation import (
    AggregateRequest,
    AggregateResult,
    Cursor,
)
from .document import Document
from .field import Field
from .index_definition import IndexDefinition
from .profile_information import ProfileInformation
from .query import Query
from .result import Result
from .suggestion import SuggestionParser

NUMERIC = "NUMERIC"

CREATE_CMD = "FT.CREATE"
ALTER_CMD = "FT.ALTER"
SEARCH_CMD = "FT.SEARCH"
ADD_CMD = "FT.ADD"
ADDHASH_CMD = "FT.ADDHASH"
DROPINDEX_CMD = "FT.DROPINDEX"
EXPLAIN_CMD = "FT.EXPLAIN"
EXPLAINCLI_CMD = "FT.EXPLAINCLI"
DEL_CMD = "FT.DEL"
AGGREGATE_CMD = "FT.AGGREGATE"
PROFILE_CMD = "FT.PROFILE"
CURSOR_CMD = "FT.CURSOR"
SPELLCHECK_CMD = "FT.SPELLCHECK"
DICT_ADD_CMD = "FT.DICTADD"
DICT_DEL_CMD = "FT.DICTDEL"
DICT_DUMP_CMD = "FT.DICTDUMP"
MGET_CMD = "FT.MGET"
CONFIG_CMD = "FT.CONFIG"
TAGVALS_CMD = "FT.TAGVALS"
ALIAS_ADD_CMD = "FT.ALIASADD"
ALIAS_UPDATE_CMD = "FT.ALIASUPDATE"
ALIAS_DEL_CMD = "FT.ALIASDEL"
ALIAS_LIST_CMD = "FT.ALIASLIST"
INFO_CMD = "FT.INFO"
SUGADD_COMMAND = "FT.SUGADD"
SUGDEL_COMMAND = "FT.SUGDEL"
SUGLEN_COMMAND = "FT.SUGLEN"
SUGGET_COMMAND = "FT.SUGGET"
SYNUPDATE_CMD = "FT.SYNUPDATE"
SYNDUMP_CMD = "FT.SYNDUMP"
HYBRID_CMD = "FT.HYBRID"

NOOFFSETS = "NOOFFSETS"
NOFIELDS = "NOFIELDS"
NOHL = "NOHL"
NOFREQS = "NOFREQS"
MAXTEXTFIELDS = "MAXTEXTFIELDS"
TEMPORARY = "TEMPORARY"
STOPWORDS = "STOPWORDS"
SKIPINITIALSCAN = "SKIPINITIALSCAN"
WITHSCORES = "WITHSCORES"
FUZZY = "FUZZY"
WITHPAYLOADS = "WITHPAYLOADS"


# ``execute_command`` returns the pipeline itself when a command is queued.
_PIPELINE_TYPES = (Pipeline, AsyncioPipeline)


class SearchCommands:
    """Search commands."""

    # Commands whose parsers require a ``query`` kwarg.  When invoked as a
    # pipeline response-callback the kwarg is carried inside the options
    # dict that ``execute_command`` stored earlier.  If the key is absent
    # (e.g. a raw ``execute_command("FT.SEARCH", ...)`` call) return the
    # response unparsed so we don't crash.
    _QUERY_REQUIRED_CMDS = frozenset(
        {SEARCH_CMD, AGGREGATE_CMD, CURSOR_CMD, HYBRID_CMD, PROFILE_CMD}
    )

    def _init_module_callbacks(self):
        """Build the per-protocol module callback maps.

        Called from ``Search.__init__``, ``Pipeline.__init__`` and
        ``AsyncPipeline.__init__`` so the mapping lives in a single place
        rather than being duplicated across all three classes.
        """
        # ``protocol=2`` + ``legacy_responses=True``: original RESP2 wire
        # parsers preserving the v5 Python shapes exactly.
        self._RESP2_MODULE_CALLBACKS = {
            INFO_CMD: self._parse_info,
            SEARCH_CMD: self._parse_search,
            HYBRID_CMD: self._parse_hybrid_search,
            AGGREGATE_CMD: self._parse_aggregate,
            PROFILE_CMD: self._parse_profile,
            SPELLCHECK_CMD: self._parse_spellcheck,
            CONFIG_CMD: self._parse_config_get,
            SYNDUMP_CMD: self._parse_syndump,
            ALIAS_LIST_CMD: self._parse_aliaslist,
        }
        # Explicit ``protocol=3`` + ``legacy_responses=True`` keeps the
        # pre-existing native RESP3 surface.  SEARCH and HYBRID both use
        # ``NEVER_DECODE`` (their results may contain binary field values),
        # so their native callbacks decode only the structural keys and keep
        # field values as bytes.  FT.PROFILE stays on the old direct
        # ``_parse_results`` special case and is not registered as a callback.
        self._RESP3_MODULE_CALLBACKS = {
            SEARCH_CMD: self._parse_search_resp3_native,
            HYBRID_CMD: self._parse_hybrid_search_resp3_native,
            ALIAS_LIST_CMD: self._parse_aliaslist,
        }
        # ``protocol=None`` + ``legacy_responses=True`` (the v8 default):
        # the wire is RESP3 but the Python surface mirrors RESP2 legacy
        # objects (``Result``, ``AggregateResult``, ``(result, profile)``
        # tuple, ...).
        self._RESP3_TO_RESP2_LEGACY_MODULE_CALLBACKS = {
            INFO_CMD: self._parse_info_resp3_to_legacy,
            SEARCH_CMD: self._parse_search_resp3,
            HYBRID_CMD: self._parse_hybrid_search_resp3,
            AGGREGATE_CMD: self._parse_aggregate_resp3,
            PROFILE_CMD: self._parse_profile_resp3,
            SPELLCHECK_CMD: self._parse_spellcheck_resp3,
            CONFIG_CMD: self._parse_config_get_resp3_to_legacy,
            SYNDUMP_CMD: self._parse_syndump_resp3,
            ALIAS_LIST_CMD: self._parse_aliaslist,
        }
        # Search pipelines historically returned raw wire responses in
        # legacy mode.  The default connection now uses RESP3 on the wire,
        # so these callbacks adapt only the default legacy pipeline case
        # back to the raw RESP2 pipeline shapes users saw prior v8.0.
        self._RESP3_TO_RESP2_LEGACY_PIPELINE_CALLBACKS = {
            SEARCH_CMD: self._pipeline_parse_search_resp3_to_legacy,
            HYBRID_CMD: self._pipeline_parse_hybrid_search_resp3_to_legacy,
            ALIAS_LIST_CMD: self._parse_aliaslist,
        }
        # ``protocol=2`` + ``legacy_responses=True`` pipelines otherwise return
        # raw wire responses.  ``FT.ALIASLIST`` is a new command with no pre-v8
        # raw pipeline shape to preserve, so it is still normalized to a ``set``
        # to match the direct-call behavior and the documented return type.
        self._RESP2_LEGACY_PIPELINE_CALLBACKS = {
            ALIAS_LIST_CMD: self._parse_aliaslist,
        }
        # ``legacy_responses=False`` + RESP2 wire: enhanced RESP2 parsers
        # producing the unified shape (``attributes`` as list of dicts,
        # command-specific value normalisation where the approved shape
        # requires it).
        self._RESP2_UNIFIED_MODULE_CALLBACKS = {
            INFO_CMD: self._parse_info_unified,
            SEARCH_CMD: self._parse_search,
            HYBRID_CMD: self._parse_hybrid_search_unified,
            AGGREGATE_CMD: self._parse_aggregate,
            PROFILE_CMD: self._parse_profile_unified,
            SPELLCHECK_CMD: self._parse_spellcheck,
            CONFIG_CMD: self._parse_config_get_unified,
            SYNDUMP_CMD: self._parse_syndump_unified,
            ALIAS_LIST_CMD: self._parse_aliaslist,
        }
        # ``legacy_responses=False`` + RESP3 wire: keeps the native RESP3
        # shape for commands whose unified shape diverges from the
        # RESP3-to-RESP2-legacy adapter (``FT.INFO`` keeps the native
        # nested dict, ``FT.PROFILE`` keeps profile data as a dict).
        self._RESP3_UNIFIED_MODULE_CALLBACKS = dict(
            self._RESP3_TO_RESP2_LEGACY_MODULE_CALLBACKS
        )
        self._RESP3_UNIFIED_MODULE_CALLBACKS[INFO_CMD] = self._parse_info_resp3
        self._RESP3_UNIFIED_MODULE_CALLBACKS[CONFIG_CMD] = self._parse_config_get_resp3
        self._RESP3_UNIFIED_MODULE_CALLBACKS[PROFILE_CMD] = (
            self._parse_profile_resp3_unified
        )
        self._RESP3_UNIFIED_MODULE_CALLBACKS[HYBRID_CMD] = (
            self._parse_hybrid_search_resp3_unified
        )

    def _parse_results(self, cmd, res, **kwargs):
        if cmd in self._QUERY_REQUIRED_CMDS and "query" not in kwargs:
            return res
        protocol = get_protocol_version(self.client)
        legacy = get_legacy_responses(self.client)
        if legacy:
            if protocol in (3, "3"):
                if cmd == PROFILE_CMD:
                    return ProfileInformation(res)
                cb = self._RESP3_MODULE_CALLBACKS.get(cmd)
            elif check_protocol_version(protocol, 3):
                cb = self._RESP3_TO_RESP2_LEGACY_MODULE_CALLBACKS.get(cmd)
            else:
                cb = self._RESP2_MODULE_CALLBACKS.get(cmd)
        else:
            if check_protocol_version(protocol, 3):
                cb = self._RESP3_UNIFIED_MODULE_CALLBACKS.get(cmd)
            else:
                cb = self._RESP2_UNIFIED_MODULE_CALLBACKS.get(cmd)
        if cb is None:
            return res
        return cb(res, **kwargs)

    @staticmethod
    def _resp3_get(mapping, key, default=None):
        if not isinstance(mapping, dict):
            return default
        return mapping.get(key, mapping.get(key.encode(), default))

    @staticmethod
    def _flatten_resp3_mapping(mapping):
        if not isinstance(mapping, dict):
            return mapping
        flat = []
        for key, value in mapping.items():
            flat.append(str_if_bytes(key))
            flat.append(value)
        return flat

    def _pipeline_parse_search_resp3_to_legacy(self, res, **kwargs):
        """Convert RESP3 FT.SEARCH pipeline output to raw RESP2 pipeline shape."""
        query = kwargs.get("query")
        if query is None or not isinstance(res, dict):
            return res

        output = [self._resp3_get(res, "total_results", 0)]
        for item in self._resp3_get(res, "results", []):
            output.append(self._resp3_get(item, "id"))
            if query._with_scores:
                output.append(self._resp3_get(item, "score"))
            if query._with_payloads:
                output.append(self._resp3_get(item, "payload"))
            if not query._no_content:
                output.append(
                    self._flatten_resp3_mapping(
                        self._resp3_get(item, "extra_attributes", {})
                    )
                )
        return output

    def _pipeline_parse_hybrid_search_resp3_to_legacy(self, res, **kwargs):
        """Convert RESP3 FT.HYBRID pipeline output to raw RESP2 pipeline shape."""
        if not isinstance(res, dict):
            return res
        res = {str_if_bytes(key): value for key, value in res.items()}
        if "cursor" in kwargs:
            return ["SEARCH", res.get("SEARCH"), "VSIM", res.get("VSIM")]

        results = [
            self._flatten_resp3_mapping(item) if isinstance(item, dict) else item
            for item in res.get("results", [])
        ]
        return [
            "total_results",
            res.get("total_results", 0),
            "results",
            results,
            "warnings",
            res.get("warnings", []),
            "execution_time",
            res.get("execution_time", 0),
        ]

    # ---- RESP2 legacy parsers ----

    def _parse_info(self, res, **kwargs):
        it = map(str_if_bytes, res)
        return dict(zip(it, it))

    def _parse_aliaslist(self, res, **kwargs):
        # RESP2 replies with an array and RESP3 with a set; both are decoded
        # into a list/set by the parsers, so normalize to an unordered ``set``.
        # Alias names are user data, so they honor ``decode_responses`` (``str``
        # when decoding is enabled, ``bytes`` when it is not) — matching the
        # passthrough behavior of FT.TAGVALS and FT.DICTDUMP rather than being
        # force-decoded like the structural map keys handled by the other
        # search parsers.
        return set(res) if res else set()

    def _parse_search(self, res, **kwargs):
        return Result(
            res,
            not kwargs["query"]._no_content,
            duration=kwargs["duration"],
            has_payload=kwargs["query"]._with_payloads,
            with_scores=kwargs["query"]._with_scores,
            field_encodings=kwargs["query"]._return_fields_decode_as,
        )

    def _parse_hybrid_search(self, res, **kwargs):
        res_dict = pairs_to_dict(res, decode_keys=True)
        if "cursor" in kwargs:
            return HybridCursorResult(
                search_cursor_id=int(res_dict["SEARCH"]),
                vsim_cursor_id=int(res_dict["VSIM"]),
            )

        results: List[Dict[str, Any]] = []
        # the original results are a list of lists
        # we convert them to a list of dicts
        for res_item in res_dict["results"]:
            item_dict = pairs_to_dict(res_item, decode_keys=True)
            results.append(item_dict)

        return HybridResult(
            total_results=int(res_dict["total_results"]),
            results=results,
            warnings=res_dict["warnings"],
            execution_time=float(res_dict["execution_time"]),
        )

    def _parse_aggregate(self, res, **kwargs):
        return self._get_aggregate_result(res, kwargs["query"], kwargs["has_cursor"])

    def _parse_profile(self, res, **kwargs):
        query = kwargs["query"]
        if isinstance(query, AggregateRequest):
            result = self._get_aggregate_result(res[0], query, query._cursor)
        else:
            result = Result(
                res[0],
                not query._no_content,
                duration=kwargs["duration"],
                has_payload=query._with_payloads,
                with_scores=query._with_scores,
                field_encodings=query._return_fields_decode_as,
            )

        return result, ProfileInformation(res[1])

    def _parse_spellcheck(self, res, **kwargs):
        corrections = {}
        if res == 0:
            return corrections

        for _correction in res:
            if isinstance(_correction, int) and _correction == 0:
                continue

            if len(_correction) != 3:
                continue
            if not _correction[2]:
                continue
            if not _correction[2][0]:
                continue

            # For spellcheck output
            # 1)  1) "TERM"
            #     2) "{term1}"
            #     3)  1)  1)  "{score1}"
            #             2)  "{suggestion1}"
            #         2)  1)  "{score2}"
            #             2)  "{suggestion2}"
            #
            # Following dictionary will be made
            # corrections = {
            #     '{term1}': [
            #         {'score': '{score1}', 'suggestion': '{suggestion1}'},
            #         {'score': '{score2}', 'suggestion': '{suggestion2}'}
            #     ]
            # }
            corrections[_correction[1]] = [
                {"score": _item[0], "suggestion": _item[1]} for _item in _correction[2]
            ]

        return corrections

    def _parse_config_get(self, res, **kwargs):
        return {kvs[0]: kvs[1] for kvs in res} if res else {}

    def _parse_syndump(self, res, **kwargs):
        return {res[i]: res[i + 1] for i in range(0, len(res), 2)}

    # ---- RESP2 unified parsers (legacy_responses=False) ----

    # Uppercase FT.INFO attribute keys that are followed by a value
    # (key-value pairs in the RESP2 flat list). Lowercase keys, such as
    # ``identifier``, ``type`` or the vector keys ``algorithm`` and
    # ``compression``, are always followed by a value; flags are uppercase.
    _INFO_ATTR_PAIR_KEYS = frozenset({"WEIGHT", "SEPARATOR", "PHONETIC", "M"})

    @staticmethod
    def _normalize_info_attribute(attr_list):
        """Convert a RESP2 flat attribute list into a RESP3-style dict.

        RESP2 format: ``[identifier, name, attribute, alias, type, TEXT,
        WEIGHT, 1, SORTABLE, NOSTEM]``.
        RESP3 format: ``{"identifier": name, "attribute": alias, "type":
        "TEXT", "WEIGHT": "1", "flags": ["SORTABLE", "NOSTEM"]}``.

        A key is followed by a value if it is lowercase or listed in
        ``_INFO_ATTR_PAIR_KEYS``; any other key is a flag.
        """
        result = {}
        flags = []
        pair_keys = SearchCommands._INFO_ATTR_PAIR_KEYS
        i = 0
        while i < len(attr_list):
            key = str_if_bytes(attr_list[i])
            if (
                isinstance(key, str)
                and (key.islower() or key in pair_keys)
                and i + 1 < len(attr_list)
            ):
                result[key] = str_if_bytes(attr_list[i + 1])
                i += 2
            else:
                flags.append(key)
                i += 1
        result["flags"] = flags
        return result

    def _parse_info_unified(self, res, **kwargs):
        """Parse FT.INFO into the unified shape with ``attributes`` as a
        list of dicts so RESP2 output matches RESP3 output.
        """
        it = map(str_if_bytes, res)
        info = dict(zip(it, it))
        if "attributes" in info and isinstance(info["attributes"], list):
            info["attributes"] = [
                self._normalize_info_attribute(attr) if isinstance(attr, list) else attr
                for attr in info["attributes"]
            ]
        return info

    def _parse_hybrid_search_unified(self, res, **kwargs):
        res_dict = pairs_to_dict(res, decode_keys=True)
        if "cursor" in kwargs:
            return HybridCursorResult(
                search_cursor_id=int(res_dict["SEARCH"]),
                vsim_cursor_id=int(res_dict["VSIM"]),
            )

        field_encodings = self._hybrid_field_encodings(**kwargs)

        results: List[Dict[str, Any]] = []
        for res_item in res_dict["results"]:
            item_dict = pairs_to_dict(res_item, decode_keys=True)
            results.append(
                {
                    key: self._decode_hybrid_field_value(value, key, field_encodings)
                    for key, value in item_dict.items()
                }
            )

        return HybridResult(
            total_results=int(res_dict["total_results"]),
            results=results,
            warnings=res_dict["warnings"],
            execution_time=float(res_dict["execution_time"]),
        )

    def _parse_profile_unified(self, res, **kwargs):
        """Parse FT.PROFILE into ``(result, ProfileInformation)`` with
        the profile_data normalised to a dict on >= 7.9.0 servers.
        """
        query = kwargs["query"]
        if isinstance(query, AggregateRequest):
            result = self._get_aggregate_result(res[0], query, query._cursor)
        else:
            result = Result(
                res[0],
                not query._no_content,
                duration=kwargs["duration"],
                has_payload=query._with_payloads,
                with_scores=query._with_scores,
                field_encodings=query._return_fields_decode_as,
            )

        profile_data = res[1]
        # >= 7.9.0 servers return a flat ``[key, value, ...]`` list at the
        # top level; convert to dict to match the RESP3 profile shape.
        # < 7.9.0 servers return a list-of-pairs whose first element is
        # itself a list — leave as-is.
        if (
            isinstance(profile_data, list)
            and profile_data
            and isinstance(profile_data[0], (str, bytes))
        ):
            profile_data = pairs_to_dict(profile_data, decode_keys=True)

        return result, ProfileInformation(profile_data)

    def _parse_config_get_unified(self, res, **kwargs):
        if not res:
            return {}
        return {str_if_bytes(kvs[0]): str_if_bytes(kvs[1]) for kvs in res}

    def _parse_syndump_unified(self, res, **kwargs):
        if not res:
            return {}
        return {
            str_if_bytes(res[i]): [str_if_bytes(s) for s in res[i + 1]]
            if isinstance(res[i + 1], list)
            else str_if_bytes(res[i + 1])
            for i in range(0, len(res), 2)
        }

    # ---- RESP3 shared result parsers ----

    def _parse_search_resp3(self, res, **kwargs):
        """Parse RESP3 FT.SEARCH response into a Result object."""
        query = kwargs.get("query")
        return Result.from_resp3(
            res,
            duration=kwargs.get("duration", 0),
            with_scores=getattr(query, "_with_scores", False),
            field_encodings=getattr(query, "_return_fields_decode_as", None),
        )

    def _parse_search_resp3_native(self, res, **kwargs):
        """Normalise the RESP3 FT.SEARCH map while preserving its native shape.

        ``protocol=3`` + ``legacy_responses=True`` keeps the RESP3 dict
        surface.  FT.SEARCH uses ``NEVER_DECODE`` (its results may contain
        binary field values such as vector embeddings), so the wire hands
        back raw bytes.  Decode the structural keys and the field values here
        instead of at the wire: text values are decoded leniently while
        binary fields flagged via ``return_field(..., decode_field=False)``
        are preserved as raw bytes.  This keeps the previously decoded native
        surface intact and no longer crashes on non-UTF-8 field data.
        """
        if not isinstance(res, dict):
            return res
        query = kwargs.get("query")
        field_encodings = getattr(query, "_return_fields_decode_as", None)
        res = {str_if_bytes(k): v for k, v in res.items()}
        if "results" in res:
            results = []
            for item in res["results"]:
                if not isinstance(item, dict):
                    results.append(item)
                    continue
                item = {str_if_bytes(k): v for k, v in item.items()}
                if "id" in item:
                    item["id"] = str_if_bytes(item["id"])
                extra = item.get("extra_attributes")
                if isinstance(extra, dict):
                    decoded = {}
                    for k, v in extra.items():
                        k = str_if_bytes(k)
                        decoded[k] = decode_field_value(v, k, field_encodings)
                    item["extra_attributes"] = decoded
                results.append(item)
            res["results"] = results
        if "warning" in res:
            res["warning"] = [str_if_bytes(w) for w in res["warning"]]
        return res

    def _parse_aggregate_resp3(self, res, **kwargs):
        """Parse RESP3 FT.AGGREGATE response into an AggregateResult object."""
        query = kwargs.get("query")
        has_cursor = kwargs.get("has_cursor", False)

        # When has_cursor is True, RESP3 returns [data_dict, cursor_id].
        cursor_id = 0
        if has_cursor and isinstance(res, list):
            data = res[0]
            cursor_id = res[1] if len(res) > 1 else 0
        else:
            data = res

        if data is None:
            data = {}
        # On RESP3 connections with decode_responses=False the server's map
        # keys arrive as bytes, so normalise structural keys to strings
        # before lookup.  Mirrors ``Result.from_resp3``.
        data = {str_if_bytes(k): v for k, v in data.items()}

        warnings = [str_if_bytes(w) for w in data.get("warning", [])]
        total = data.get("total_results", 0)

        rows = []
        for result_item in data.get("results", []):
            result_item = {str_if_bytes(k): v for k, v in result_item.items()}
            extra_attrs = result_item.get("extra_attributes", {})
            # Convert dict to flat list [key, value, key, value, ...]
            # to match RESP2 row format consumers expect.
            flat = []
            for k, v in extra_attrs.items():
                flat.append(k)
                flat.append(v)
            rows.append(flat)

        cursor = None
        if has_cursor:
            if isinstance(query, Cursor):
                query.cid = cursor_id
                cursor = query
            else:
                cursor = Cursor(cursor_id)

        return AggregateResult(rows, cursor, None, total=total, warnings=warnings)

    # ---- RESP3 HYBRID parsers ----

    def _parse_hybrid_search_resp3(self, res, **kwargs):
        """Parse RESP3 FT.HYBRID response into HybridResult/HybridCursorResult.

        Top-level keys are normalised to strings.  Values are preserved
        as delivered by the wire (bytes when ``NEVER_DECODE`` is set,
        strings otherwise) so byte/str semantics match the RESP2 legacy
        parser.
        """
        res = {str_if_bytes(k): v for k, v in res.items()}
        if "cursor" in kwargs:
            return HybridCursorResult(
                search_cursor_id=int(res["SEARCH"]),
                vsim_cursor_id=int(res["VSIM"]),
            )

        results: List[Dict[str, Any]] = []
        for res_item in res.get("results", []):
            if isinstance(res_item, dict):
                results.append({str_if_bytes(k): v for k, v in res_item.items()})
            else:
                results.append(pairs_to_dict(res_item, decode_keys=True))

        return HybridResult(
            total_results=int(res.get("total_results", 0)),
            results=results,
            warnings=res.get("warnings", []),
            execution_time=float(res.get("execution_time", 0)),
        )

    def _parse_hybrid_search_resp3_unified(self, res, **kwargs):
        """Parse RESP3 FT.HYBRID into the approved unified HybridResult."""
        res = {str_if_bytes(k): v for k, v in res.items()}
        if "cursor" in kwargs:
            return HybridCursorResult(
                search_cursor_id=int(res["SEARCH"]),
                vsim_cursor_id=int(res["VSIM"]),
            )

        field_encodings = self._hybrid_field_encodings(**kwargs)

        results: List[Dict[str, Any]] = []
        for res_item in res.get("results", []):
            if isinstance(res_item, dict):
                results.append(
                    {
                        str_if_bytes(key): self._decode_hybrid_field_value(
                            value, str_if_bytes(key), field_encodings
                        )
                        for key, value in res_item.items()
                    }
                )
            else:
                item_dict = pairs_to_dict(res_item, decode_keys=True)
                results.append(
                    {
                        key: self._decode_hybrid_field_value(
                            value, key, field_encodings
                        )
                        for key, value in item_dict.items()
                    }
                )

        return HybridResult(
            total_results=int(res.get("total_results", 0)),
            results=results,
            warnings=res.get("warnings", []),
            execution_time=float(res.get("execution_time", 0)),
        )

    @staticmethod
    def _hybrid_field_encodings(**kwargs):
        encodings = {}
        for source_name in ("query", "post_processing"):
            source = kwargs.get(source_name)
            source_encodings = getattr(source, "_return_fields_decode_as", None)
            if source_encodings:
                encodings.update(source_encodings)
        return encodings or None

    @staticmethod
    def _decode_hybrid_field_value(value, key, field_encodings):
        if not field_encodings or key not in field_encodings:
            return value
        return decode_field_value(value, key, field_encodings)

    def _parse_hybrid_search_resp3_native(self, res, **kwargs):
        """Normalise RESP3 FT.HYBRID map keys while preserving native shape.

        ``protocol=3`` + ``legacy_responses=True`` keeps the RESP3 dict
        surface, but HYBRID uses ``NEVER_DECODE`` so result values mirror
        legacy RESP2 bytes. Decode only structural keys so callers can use
        the same native RESP3 key names as before.
        """
        res = {str_if_bytes(k): v for k, v in res.items()}
        if "cursor" in kwargs:
            return res

        if "results" in res:
            res["results"] = [
                {str_if_bytes(k): v for k, v in item.items()}
                if isinstance(item, dict)
                else pairs_to_dict(item, decode_keys=True)
                for item in res["results"]
            ]
        if "warnings" in res:
            res["warnings"] = [str_if_bytes(w) for w in res["warnings"]]
        return res

    # ---- RESP3 spellcheck parser ----

    def _parse_spellcheck_resp3(self, res, **kwargs):
        """Parse RESP3 FT.SPELLCHECK response into unified format.

        RESP3 format:
            {"results": {"term": [{"suggestion": score}, ...], ...}}
        Unified format (matches RESP2 parsed output):
            {"term": [{"score": score_str, "suggestion": suggestion}, ...], ...}
        """
        if not isinstance(res, dict):
            return self._parse_spellcheck(res, **kwargs)
        # On RESP3 connections with decode_responses=False the server's map
        # keys arrive as bytes, so normalise the structural ``results`` key
        # to a string before lookup.  Mirrors ``Result.from_resp3``.
        res = {str_if_bytes(k): v for k, v in res.items()}
        corrections = {}
        results = res.get("results", {})
        for term, suggestions in results.items():
            if not suggestions:
                continue
            term_corrections = []
            for suggestion_dict in suggestions:
                for suggestion, score in suggestion_dict.items():
                    # Normalize score to match RESP2's string form: RESP3
                    # returns a float (e.g. ``0.0``) but RESP2 returns the
                    # string ``"0"``.
                    score_str = str(score)
                    if score_str.endswith(".0"):
                        score_str = score_str[:-2]
                    # Preserve ``suggestion`` as-is so it keeps the
                    # ``decode_responses`` shape RESP2 would produce
                    # (``str`` when decoded, ``bytes`` otherwise).
                    term_corrections.append(
                        {"score": score_str, "suggestion": suggestion}
                    )
            if term_corrections:
                corrections[term] = term_corrections
        return corrections

    # ---- RESP3 profile parsers ----

    def _extract_resp3_profile_parts(self, res, **kwargs):
        """Extract ``(result, profile_data_dict)`` from a RESP3 FT.PROFILE
        response.  ``profile_data_dict`` has its keys/values normalised
        to strings but is otherwise left as the native RESP3 dict.
        """
        query = kwargs["query"]
        # RESP3 returns a dict with "Results" and "Profile" keys.  Handle
        # both decoded (str) and raw (bytes) keys.  Use ``is not None`` to
        # avoid dropping falsy values such as empty dicts/lists.
        results_data = res.get("Results")
        if results_data is None:
            results_data = res.get(b"Results")
        if results_data is None:
            results_data = res.get("results")
        if results_data is None:
            results_data = res.get(b"results")
        if results_data is None:
            results_data = res.get(0)
        profile_data = res.get("Profile")
        if profile_data is None:
            profile_data = res.get(b"Profile")
        if profile_data is None:
            profile_data = res.get("profile")
        if profile_data is None:
            profile_data = res.get(b"profile")
        if profile_data is None:
            profile_data = res.get(1)
        # On older servers (pre MOD-6816, e.g. Redis 7.2/7.4) the "Results"
        # value is a bare list of result-item dicts, not the wrapper dict
        # ``{"total_results": N, "results": [...], "warning": [...]}``.
        # Wrap the list so downstream parsers receive the expected format.
        if isinstance(results_data, list):
            results_data = {
                "total_results": len(results_data),
                "results": results_data,
            }
        if isinstance(query, AggregateRequest):
            result = self._parse_aggregate_resp3(
                results_data, query=query, has_cursor=bool(query._cursor)
            )
        else:
            result = Result.from_resp3(
                results_data,
                duration=kwargs.get("duration", 0),
                with_scores=getattr(query, "_with_scores", False),
                field_encodings=getattr(query, "_return_fields_decode_as", None),
            )
        profile_data = self._to_string_recursive(profile_data)
        return result, profile_data

    def _parse_profile_resp3(self, res, **kwargs):
        """Parse RESP3 FT.PROFILE response into ``(result, ProfileInformation)``.

        RESP3 format (aligned, RediSearch >= MOD-6816):
            {"Results": {search/aggregate result dict},
             "Profile": {profile information dict}}

        Older RediSearch versions may return a list (same as RESP2) even
        when the connection uses RESP3.  In that case we delegate to the
        RESP2 ``_parse_profile`` parser.
        """
        if isinstance(res, list):
            return self._parse_profile(res, **kwargs)

        result, profile_data = self._extract_resp3_profile_parts(res, **kwargs)
        # Convert the RESP3 profile dict to the RESP2 list shape so
        # consumers see the same structure as the RESP2 wire path.
        # Post-7.9.0 servers return a top-level ``{"Shards": ...,
        # "Coordinator": ...}`` dict which RESP2 wires as a flat
        # alternating ``[key, value, key, value]`` list.  Pre-7.9.0
        # servers return a nested list-of-pairs.
        if isinstance(profile_data, dict):
            flat_top = "Shards" in profile_data or "Coordinator" in profile_data
            profile_data = self._resp3_profile_dict_to_list(
                profile_data, flat_top=flat_top
            )
        return result, ProfileInformation(profile_data)

    def _parse_profile_resp3_unified(self, res, **kwargs):
        """Parse RESP3 FT.PROFILE for the unified shape.

        Redis < 7.9.0 returns RESP2 profile data as nested list-of-pairs,
        while RESP3 returns the same data as a dict.  Convert that pre-7.9
        RESP3 dict back to the RESP2 list shape so the unified surface is
        protocol-independent.  Redis >= 7.9.0 coordinator profiles contain
        ``Shards``/``Coordinator`` keys and stay as dicts, matching the
        RESP2 unified parser.
        """
        if isinstance(res, list):
            return self._parse_profile_unified(res, **kwargs)

        result, profile_data = self._extract_resp3_profile_parts(res, **kwargs)
        if isinstance(profile_data, dict) and not (
            "Shards" in profile_data or "Coordinator" in profile_data
        ):
            profile_data = self._resp3_profile_dict_to_list(profile_data)
        return result, ProfileInformation(profile_data)

    @staticmethod
    def _resp3_profile_dict_to_list(data, flat_top=False):
        """Convert a RESP3 profile dict into the RESP2 wire list shape.

        Pre-7.9.0 servers serialise the RESP2 profile as a list of
        ``[key, value]`` pairs at the top level, with nested structures
        as flat alternating key-value lists.  Post-7.9.0 servers serialise
        the top level itself as a flat ``[key, value, key, value]`` list
        (with ``"Shards"``/``"Coordinator"`` keys).  ``flat_top`` selects
        between these two top-level shapes; nested dicts always use the
        flat alternating form.

        Key structural difference: when a dict value is a list of dicts
        (e.g. ``"Child iterators": [{...}, {...}]``), RESP2 expands each
        dict as a separate sibling element in the parent flat list rather
        than keeping them nested inside a single value.
        """

        def _is_list_of_dicts(obj):
            return isinstance(obj, list) and obj and isinstance(obj[0], dict)

        def _convert(obj, top_level=False):
            if isinstance(obj, dict):
                if top_level and not flat_top:
                    result = []
                    for k, v in obj.items():
                        entry = [k]
                        if _is_list_of_dicts(v):
                            for item in v:
                                entry.append(_convert(item))
                        else:
                            entry.append(_convert(v))
                        result.append(entry)
                    return result
                else:
                    result = []
                    for k, v in obj.items():
                        result.append(k)
                        if _is_list_of_dicts(v):
                            for item in v:
                                result.append(_convert(item))
                        else:
                            result.append(_convert(v))
                    return result
            elif isinstance(obj, list):
                return [_convert(item) for item in obj]
            return obj

        return _convert(data, top_level=True)

    # ---- RESP3 structural parsers ----

    @staticmethod
    def _to_string_recursive(obj):
        """Recursively convert bytes keys/values to strings in nested
        structures.  Non-bytes scalars (int, float, None, bool) pass
        through unchanged.
        """
        if isinstance(obj, bytes):
            return str_if_bytes(obj)
        if isinstance(obj, dict):
            return {
                str_if_bytes(k) if isinstance(k, bytes) else k: (
                    SearchCommands._to_string_recursive(v)
                )
                for k, v in obj.items()
            }
        if isinstance(obj, list):
            return [SearchCommands._to_string_recursive(item) for item in obj]
        return obj

    def _parse_info_resp3(self, res, **kwargs):
        """Parse RESP3 FT.INFO response, normalising bytes to strings."""
        return self._to_string_recursive(res)

    @staticmethod
    def _flatten_info_attribute(attr_dict):
        """Convert a RESP3-style attribute dict back into the RESP2 flat
        list (key-value pairs followed by bare flag tokens).
        """
        flat = []
        flags = attr_dict.get("flags") or []
        for k, v in attr_dict.items():
            if k == "flags":
                continue
            flat.append(k)
            flat.append(v)
        flat.extend(flags)
        return flat

    def _parse_info_resp3_to_legacy(self, res, **kwargs):
        """Parse RESP3 FT.INFO into the legacy RESP2 flat shape so
        ``legacy_responses=True`` on a RESP3 wire matches RESP2 output.
        """
        info = self._to_string_recursive(res)
        attrs = info.get("attributes")
        if isinstance(attrs, list):
            info["attributes"] = [
                self._flatten_info_attribute(attr) if isinstance(attr, dict) else attr
                for attr in attrs
            ]
        return info

    def _parse_config_get_resp3(self, res, **kwargs):
        """Parse RESP3 FT.CONFIG GET response, normalising bytes to strings."""
        if not res:
            return {}
        return {str_if_bytes(k): str_if_bytes(v) for k, v in res.items()}

    def _parse_config_get_resp3_to_legacy(self, res, **kwargs):
        """Parse RESP3 FT.CONFIG GET back to the RESP2 legacy dict shape."""
        return dict(res) if res else {}

    def _parse_syndump_resp3(self, res, **kwargs):
        """Parse RESP3 FT.SYNDUMP response, normalising bytes to strings."""
        if not res:
            return {}
        return self._to_string_recursive(res)

    def batch_indexer(self, chunk_size=100):
        """
        Create a new batch indexer from the client with a given chunk size
        """
        return self.BatchIndexer(self, chunk_size=chunk_size)

    def create_index(
        self,
        fields: List[Field],
        no_term_offsets: bool = False,
        no_field_flags: bool = False,
        stopwords: Optional[List[str]] = None,
        definition: Optional[IndexDefinition] = None,
        max_text_fields=False,
        temporary=None,
        no_highlight: bool = False,
        no_term_frequencies: bool = False,
        skip_initial_scan: bool = False,
    ):
        """
        Creates the search index. The index must not already exist.

        For more information, see https://redis.io/commands/ft.create/

        Args:
            fields: A list of Field objects.
            no_term_offsets: If `true`, term offsets will not be saved in the index.
            no_field_flags: If true, field flags that allow searching in specific fields
                            will not be saved.
            stopwords: If provided, the index will be created with this custom stopword
                       list. The list can be empty.
            definition: If provided, the index will be created with this custom index
                        definition.
            max_text_fields: If true, indexes will be encoded as if there were more than
                             32 text fields, allowing for additional fields beyond 32.
            temporary: Creates a lightweight temporary index which will expire after the
                       specified period of inactivity. The internal idle timer is reset
                       whenever the index is searched or added to.
            no_highlight: If true, disables highlighting support. Also implied by
                          `no_term_offsets`.
            no_term_frequencies: If true, term frequencies will not be saved in the
                                 index.
            skip_initial_scan: If true, the initial scan and indexing will be skipped.

        """
        args = [CREATE_CMD, self.index_name]
        if definition is not None:
            args += definition.args
        if max_text_fields:
            args.append(MAXTEXTFIELDS)
        if temporary is not None and isinstance(temporary, int):
            args.append(TEMPORARY)
            args.append(temporary)
        if no_term_offsets:
            args.append(NOOFFSETS)
        if no_highlight:
            args.append(NOHL)
        if no_field_flags:
            args.append(NOFIELDS)
        if no_term_frequencies:
            args.append(NOFREQS)
        if skip_initial_scan:
            args.append(SKIPINITIALSCAN)
        if stopwords is not None and isinstance(stopwords, (list, tuple, set)):
            args += [STOPWORDS, len(stopwords)]
            if len(stopwords) > 0:
                args += list(stopwords)

        args.append("SCHEMA")
        try:
            args += list(itertools.chain(*(f.redis_args() for f in fields)))
        except TypeError:
            args += fields.redis_args()

        return self.execute_command(*args)

    def alter_schema_add(self, fields: Union[Field, List[Field]]):
        """
        Alter the existing search index by adding new fields. The index
        must already exist.

        ### Parameters:

        - **fields**: a list of Field objects to add for the index

        For more information see `FT.ALTER <https://redis.io/commands/ft.alter>`_.
        """  # noqa

        args = [ALTER_CMD, self.index_name, "SCHEMA", "ADD"]
        try:
            args += list(itertools.chain(*(f.redis_args() for f in fields)))
        except TypeError:
            args += fields.redis_args()

        return self.execute_command(*args)

    def dropindex(self, delete_documents: bool = False):
        """
        Drop the index if it exists.
        Replaced `drop_index` in RediSearch 2.0.
        Default behavior was changed to not delete the indexed documents.

        ### Parameters:

        - **delete_documents**: If `True`, all documents will be deleted.

        For more information see `FT.DROPINDEX <https://redis.io/commands/ft.dropindex>`_.
        """  # noqa
        args = [DROPINDEX_CMD, self.index_name]

        delete_str = (
            "DD"
            if isinstance(delete_documents, bool) and delete_documents is True
            else ""
        )

        if delete_str:
            args.append(delete_str)

        return self.execute_command(*args)

    def _add_document(
        self,
        doc_id,
        conn=None,
        nosave=False,
        score=1.0,
        payload=None,
        replace=False,
        partial=False,
        language=None,
        no_create=False,
        **fields,
    ):
        """
        Internal add_document used for both batch and single doc indexing
        """

        if partial or no_create:
            replace = True

        args = [ADD_CMD, self.index_name, doc_id, score]
        if nosave:
            args.append("NOSAVE")
        if payload is not None:
            args.append("PAYLOAD")
            args.append(payload)
        if replace:
            args.append("REPLACE")
            if partial:
                args.append("PARTIAL")
            if no_create:
                args.append("NOCREATE")
        if language:
            args += ["LANGUAGE", language]
        args.append("FIELDS")
        args += list(itertools.chain(*fields.items()))

        if conn is not None:
            return conn.execute_command(*args)

        return self.execute_command(*args)

    def _add_document_hash(
        self, doc_id, conn=None, score=1.0, language=None, replace=False
    ):
        """
        Internal add_document_hash used for both batch and single doc indexing
        """

        args = [ADDHASH_CMD, self.index_name, doc_id, score]

        if replace:
            args.append("REPLACE")

        if language:
            args += ["LANGUAGE", language]

        if conn is not None:
            return conn.execute_command(*args)

        return self.execute_command(*args)

    @deprecated_function(
        version="2.0.0", reason="deprecated since redisearch 2.0, call hset instead"
    )
    def add_document(
        self,
        doc_id: str,
        nosave: bool = False,
        score: float = 1.0,
        payload: Optional[bool] = None,
        replace: bool = False,
        partial: bool = False,
        language: Optional[str] = None,
        no_create: bool = False,
        **fields: List[str],
    ):
        """
        Add a single document to the index.

        Args:

            doc_id: the id of the saved document.
            nosave: if set to true, we just index the document, and don't
                      save a copy of it. This means that searches will just
                      return ids.
            score: the document ranking, between 0.0 and 1.0
            payload: optional inner-index payload we can save for fast
                     access in scoring functions
            replace: if True, and the document already is in the index,
                     we perform an update and reindex the document
            partial: if True, the fields specified will be added to the
                       existing document.
                       This has the added benefit that any fields specified
                       with `no_index`
                       will not be reindexed again. Implies `replace`
            language: Specify the language used for document tokenization.
            no_create: if True, the document is only updated and reindexed
                         if it already exists.
                         If the document does not exist, an error will be
                         returned. Implies `replace`
            fields: kwargs dictionary of the document fields to be saved
                    and/or indexed.
                    NOTE: Geo points should be encoded as strings of "lon,lat"
        """  # noqa
        return self._add_document(
            doc_id,
            conn=None,
            nosave=nosave,
            score=score,
            payload=payload,
            replace=replace,
            partial=partial,
            language=language,
            no_create=no_create,
            **fields,
        )

    @deprecated_function(
        version="2.0.0", reason="deprecated since redisearch 2.0, call hset instead"
    )
    def add_document_hash(self, doc_id, score=1.0, language=None, replace=False):
        """
        Add a hash document to the index.

        ### Parameters

        - **doc_id**: the document's id. This has to be an existing HASH key
                      in Redis that will hold the fields the index needs.
        - **score**:  the document ranking, between 0.0 and 1.0
        - **replace**: if True, and the document already is in the index, we
                      perform an update and reindex the document
        - **language**: Specify the language used for document tokenization.
        """  # noqa
        return self._add_document_hash(
            doc_id, conn=None, score=score, language=language, replace=replace
        )

    @deprecated_function(version="2.0.0", reason="deprecated since redisearch 2.0")
    def delete_document(self, doc_id, conn=None, delete_actual_document=False):
        """
        Delete a document from index
        Returns 1 if the document was deleted, 0 if not

        ### Parameters

        - **delete_actual_document**: if set to True, RediSearch also delete
                                      the actual document if it is in the index
        """  # noqa
        args = [DEL_CMD, self.index_name, doc_id]
        if delete_actual_document:
            args.append("DD")

        if conn is not None:
            return conn.execute_command(*args)

        return self.execute_command(*args)

    def load_document(self, id, field_encodings: Optional[Dict[str, Any]] = None):
        """
        Load a single document by id

        - **field_encodings**: optional dict mapping field names to encodings.
          If a field's encoding is ``None`` the raw bytes value is preserved
          (useful for binary data such as vectors).
        """
        fields = self.client.hgetall(id)
        fields = {
            str_if_bytes(k): decode_field_value(v, str_if_bytes(k), field_encodings)
            for k, v in fields.items()
        }

        try:
            del fields["id"]
        except KeyError:
            pass

        return Document(id=id, **fields)

    @deprecated_function(version="2.0.0", reason="deprecated since redisearch 2.0")
    def get(self, *ids):
        """
        Returns the full contents of multiple documents.

        ### Parameters

        - **ids**: the ids of the saved documents.

        """

        return self.execute_command(MGET_CMD, self.index_name, *ids)

    def info(self):
        """
        Get info and stats about the current index, including the number of
        documents, memory consumption, etc

        For more information see `FT.INFO <https://redis.io/commands/ft.info>`_.
        """

        res = self.execute_command(INFO_CMD, self.index_name)
        if isinstance(res, _PIPELINE_TYPES):
            return res
        return self._parse_results(INFO_CMD, res)

    def get_params_args(
        self, query_params: Optional[Dict[str, Union[str, int, float, bytes]]]
    ):
        if query_params is None:
            return []
        args = []
        if len(query_params) > 0:
            args.append("PARAMS")
            args.append(len(query_params) * 2)
            for key, value in query_params.items():
                args.append(key)
                args.append(value)
        return args

    def _mk_query_args(
        self, query, query_params: Optional[Dict[str, Union[str, int, float, bytes]]]
    ):
        args = [self.index_name]

        if isinstance(query, str):
            # convert the query from a text to a query object
            query = Query(query)
        if not isinstance(query, Query):
            raise ValueError(f"Bad query type {type(query)}")

        args += query.get_args()
        args += self.get_params_args(query_params)

        return args, query

    def search(
        self,
        query: Union[str, Query],
        query_params: Union[Dict[str, Union[str, int, float, bytes]], None] = None,
    ):
        """
        Search the index for a given query, and return a result of documents

        ### Parameters

        - **query**: the search query. Either a text for simple queries with
                     default parameters, or a Query object for complex queries.
                     See RediSearch's documentation on query format

        For more information see `FT.SEARCH <https://redis.io/commands/ft.search>`_.
        """  # noqa
        args, query = self._mk_query_args(query, query_params=query_params)
        st = time.monotonic()

        # FT.SEARCH results may contain binary field values (e.g. vector
        # embeddings), so always request raw bytes from the wire and let the
        # search-layer parsers decode per-field.  Mirrors ``hybrid_search``.
        options = {NEVER_DECODE: True}
        if isinstance(self, Pipeline):
            options["query"] = query
            options["duration"] = 0

        res = self.execute_command(SEARCH_CMD, *args, **options)

        if isinstance(res, Pipeline):
            return res

        return self._parse_results(
            SEARCH_CMD, res, query=query, duration=(time.monotonic() - st) * 1000.0
        )

    @experimental_method()
    def hybrid_search(
        self,
        query: HybridQuery,
        combine_method: Optional[CombineResultsMethod] = None,
        post_processing: Optional[HybridPostProcessingConfig] = None,
        params_substitution: Optional[Dict[str, Union[str, int, float, bytes]]] = None,
        timeout: Optional[int] = None,
        cursor: Optional[HybridCursorQuery] = None,
    ) -> Union[HybridResult, HybridCursorResult, Pipeline]:
        """
        Execute a hybrid search using both text and vector queries

        Args:
            - **query**: HybridQuery object
                        Contains the text and vector queries
            - **combine_method**: CombineResultsMethod object
                        Contains the combine method and parameters
            - **post_processing**: HybridPostProcessingConfig object
                        Contains the post processing configuration
            - **params_substitution**: Dict[str, Union[str, int, float, bytes]]
                        Contains the parameters substitution
            - **timeout**: int - contains the timeout in milliseconds
            - **cursor**: HybridCursorQuery object - contains the cursor configuration.
                        ``WITHCURSOR`` for ``FT.HYBRID`` is exposed only by
                        Redis versions 8.4 to 8.10.


        For more information see `FT.SEARCH <https://redis.io/commands/ft.hybrid>`.
        """
        index = self.index_name
        options = {}
        pieces = [HYBRID_CMD, index]
        pieces.extend(query.get_args())
        if combine_method:
            pieces.extend(combine_method.get_args())
        if post_processing:
            pieces.extend(post_processing.build_args())
            options["post_processing"] = post_processing
        if params_substitution:
            pieces.extend(self.get_params_args(params_substitution))
        if timeout is not None:
            pieces.extend(("TIMEOUT", timeout))
        if cursor:
            options["cursor"] = True
            pieces.extend(cursor.build_args())

        # Preserve HYBRID result values as bytes by default, matching the
        # legacy RESP2 Search surface; selected LOAD fields can opt into
        # decoding through HybridPostProcessingConfig.load(..., decode_field=True).
        options[NEVER_DECODE] = True
        options["query"] = query

        res = self.execute_command(*pieces, **options)

        if isinstance(res, Pipeline):
            return res

        return self._parse_results(HYBRID_CMD, res, **options)

    def explain(
        self,
        query: Union[str, Query],
        query_params: Optional[Dict[str, Union[str, int, float, bytes]]] = None,
    ):
        """Returns the execution plan for a complex query.

        For more information see `FT.EXPLAIN <https://redis.io/commands/ft.explain>`_.
        """  # noqa
        args, query_text = self._mk_query_args(query, query_params=query_params)
        return self.execute_command(EXPLAIN_CMD, *args)

    def explain_cli(self, query: Union[str, Query]):  # noqa
        raise NotImplementedError("EXPLAINCLI will not be implemented.")

    def aggregate(
        self,
        query: Union[AggregateRequest, Cursor],
        query_params: Optional[Dict[str, Union[str, int, float, bytes]]] = None,
    ):
        """
        Issue an aggregation query.

        ### Parameters

        **query**: This can be either an `AggregateRequest`, or a `Cursor`

        An `AggregateResult` object is returned. You can access the rows from
        its `rows` property, which will always yield the rows of the result.

        For more information see `FT.AGGREGATE <https://redis.io/commands/ft.aggregate>`_.
        """  # noqa
        if isinstance(query, AggregateRequest):
            has_cursor = bool(query._cursor)
            cmd = [AGGREGATE_CMD, self.index_name] + query.build_args()
        elif isinstance(query, Cursor):
            has_cursor = True
            cmd = [CURSOR_CMD, "READ", self.index_name] + query.build_args()
        else:
            raise ValueError("Bad query", query)
        cmd += self.get_params_args(query_params)

        options = {}
        if isinstance(self, Pipeline):
            options["query"] = query
            options["has_cursor"] = has_cursor
        raw = self.execute_command(*cmd, **options)
        if isinstance(raw, _PIPELINE_TYPES):
            return raw
        return self._parse_results(
            AGGREGATE_CMD, raw, query=query, has_cursor=has_cursor
        )

    def _get_aggregate_result(
        self, raw: List, query: Union[AggregateRequest, Cursor], has_cursor: bool
    ):
        if has_cursor:
            if isinstance(query, Cursor):
                query.cid = raw[1]
                cursor = query
            else:
                cursor = Cursor(raw[1])
            raw = raw[0]
        else:
            cursor = None

        if isinstance(query, AggregateRequest) and query._with_schema:
            schema = raw[0]
            rows = raw[2:]
        else:
            schema = None
            rows = raw[1:]

        return AggregateResult(rows, cursor, schema)

    def profile(
        self,
        query: Union[Query, AggregateRequest],
        limited: bool = False,
        query_params: Optional[Dict[str, Union[str, int, float, bytes]]] = None,
    ) -> Union[
        tuple[Union[Result, AggregateResult], ProfileInformation],
        ProfileInformation,
    ]:
        """
        Performs a search or aggregate command and collects performance
        information. Search results honor the query's field encodings. For binary
        fields, use ``decode_responses=False`` and ``Query.return_field`` with
        ``decode_field=False``.

        ### Parameters

        **query**: This can be either an `AggregateRequest` or `Query`.
        **limited**: If set to True, removes details of reader iterator.
        **query_params**: Define one or more value parameters.
        Each parameter has a name and a value.

        """
        st = time.monotonic()
        cmd = [PROFILE_CMD, self.index_name, ""]
        if limited:
            cmd.append("LIMITED")
        cmd.append("QUERY")

        if isinstance(query, AggregateRequest):
            cmd[2] = "AGGREGATE"
            cmd += query.build_args()
        elif isinstance(query, Query):
            cmd[2] = "SEARCH"
            cmd += query.get_args()
            cmd += self.get_params_args(query_params)
        else:
            raise ValueError("Must provide AggregateRequest object or Query object.")

        options = {}
        if isinstance(self, Pipeline):
            options["query"] = query
            options["duration"] = 0
        res = self.execute_command(*cmd, **options)
        if isinstance(res, _PIPELINE_TYPES):
            return res

        return self._parse_results(
            PROFILE_CMD, res, query=query, duration=(time.monotonic() - st) * 1000.0
        )

    def spellcheck(self, query, distance=None, include=None, exclude=None):
        """
        Issue a spellcheck query

        Args:

            query: search query.
            distance: the maximal Levenshtein distance for spelling
                       suggestions (default: 1, max: 4).
            include: specifies an inclusion custom dictionary.
            exclude: specifies an exclusion custom dictionary.

        For more information see `FT.SPELLCHECK <https://redis.io/commands/ft.spellcheck>`_.
        """  # noqa
        cmd = [SPELLCHECK_CMD, self.index_name, query]
        if distance:
            cmd.extend(["DISTANCE", distance])

        if include:
            cmd.extend(["TERMS", "INCLUDE", include])

        if exclude:
            cmd.extend(["TERMS", "EXCLUDE", exclude])

        res = self.execute_command(*cmd)

        if isinstance(res, _PIPELINE_TYPES):
            return res
        return self._parse_results(SPELLCHECK_CMD, res)

    def dict_add(self, name: str, *terms: List[str]):
        """Adds terms to a dictionary.

        ### Parameters

        - **name**: Dictionary name.
        - **terms**: List of items for adding to the dictionary.

        For more information see `FT.DICTADD <https://redis.io/commands/ft.dictadd>`_.
        """  # noqa
        cmd = [DICT_ADD_CMD, name]
        cmd.extend(terms)
        return self.execute_command(*cmd)

    def dict_del(self, name: str, *terms: List[str]):
        """Deletes terms from a dictionary.

        ### Parameters

        - **name**: Dictionary name.
        - **terms**: List of items for removing from the dictionary.

        For more information see `FT.DICTDEL <https://redis.io/commands/ft.dictdel>`_.
        """  # noqa
        cmd = [DICT_DEL_CMD, name]
        cmd.extend(terms)
        return self.execute_command(*cmd)

    def dict_dump(self, name: str):
        """Dumps all terms in the given dictionary.

        ### Parameters

        - **name**: Dictionary name.

        For more information see `FT.DICTDUMP <https://redis.io/commands/ft.dictdump>`_.
        """  # noqa
        cmd = [DICT_DUMP_CMD, name]
        return self.execute_command(*cmd)

    @deprecated_function(
        version="8.0.0",
        reason="deprecated since Redis 8.0, call config_set from core module instead",
    )
    def config_set(self, option: str, value: str) -> bool:
        """Set runtime configuration option.

        ### Parameters

        - **option**: the name of the configuration option.
        - **value**: a value for the configuration option.

        For more information see `FT.CONFIG SET <https://redis.io/commands/ft.config-set>`_.
        """  # noqa
        cmd = [CONFIG_CMD, "SET", option, value]
        raw = self.execute_command(*cmd)
        if isinstance(raw, _PIPELINE_TYPES):
            return raw
        return raw == "OK"

    @deprecated_function(
        version="8.0.0",
        reason="deprecated since Redis 8.0, call config_get from core module instead",
    )
    def config_get(self, option: str) -> str:
        """Get runtime configuration option value.

        ### Parameters

        - **option**: the name of the configuration option.

        For more information see `FT.CONFIG GET <https://redis.io/commands/ft.config-get>`_.
        """  # noqa
        cmd = [CONFIG_CMD, "GET", option]
        res = self.execute_command(*cmd)
        if isinstance(res, _PIPELINE_TYPES):
            return res
        return self._parse_results(CONFIG_CMD, res)

    def tagvals(self, tagfield: str):
        """
        Return a list of all possible tag values

        ### Parameters

        - **tagfield**: Tag field name

        For more information see `FT.TAGVALS <https://redis.io/commands/ft.tagvals>`_.
        """  # noqa

        return self.execute_command(TAGVALS_CMD, self.index_name, tagfield)

    def aliasadd(self, alias: KeyT):
        """
        Alias a search index - will fail if alias already exists

        ### Parameters

        - **alias**: Name of the alias to create

        For more information see `FT.ALIASADD <https://redis.io/commands/ft.aliasadd>`_.
        """  # noqa

        return self.execute_command(ALIAS_ADD_CMD, alias, self.index_name)

    def aliasupdate(self, alias: KeyT):
        """
        Updates an alias - will fail if alias does not already exist

        ### Parameters

        - **alias**: Name of the alias to create

        For more information see `FT.ALIASUPDATE <https://redis.io/commands/ft.aliasupdate>`_.
        """  # noqa

        return self.execute_command(ALIAS_UPDATE_CMD, alias, self.index_name)

    def aliasdel(self, alias: KeyT):
        """
        Removes an alias to a search index

        ### Parameters

        - **alias**: Name of the alias to delete

        For more information see `FT.ALIASDEL <https://redis.io/commands/ft.aliasdel>`_.
        """  # noqa
        return self.execute_command(ALIAS_DEL_CMD, alias)

    def aliaslist(self) -> Set[str | bytes]:
        """
        List all aliases associated with the current index as an unordered set.

        The index must be the name of an index created with ``FT.CREATE``; an
        alias name is not accepted as a substitute. Returns an empty set when
        the index exists but has no aliases. Alias names honor
        ``decode_responses`` (``str`` when decoding is enabled, ``bytes``
        otherwise).

        For more information see `FT.ALIASLIST <https://redis.io/commands/ft.aliaslist>`_.
        """  # noqa
        res = self.execute_command(ALIAS_LIST_CMD, self.index_name)
        if isinstance(res, Pipeline):
            return res
        return self._parse_results(ALIAS_LIST_CMD, res)

    def sugadd(self, key, *suggestions, **kwargs):
        """
        Add suggestion terms to the AutoCompleter engine. Each suggestion has
        a score and string.
        If kwargs["increment"] is true and the terms are already in the
        server's dictionary, we increment their scores.

        For more information see `FT.SUGADD <https://redis.io/commands/ft.sugadd/>`_.
        """  # noqa
        # If Transaction is not False it will MULTI/EXEC which will error
        pipe = self.pipeline(transaction=False)
        for sug in suggestions:
            args = [SUGADD_COMMAND, key, sug.string, sug.score]
            if kwargs.get("increment"):
                args.append("INCR")
            if sug.payload:
                args.append("PAYLOAD")
                args.append(sug.payload)

            pipe.execute_command(*args)

        return pipe.execute()[-1]

    def suglen(self, key: str) -> int:
        """
        Return the number of entries in the AutoCompleter index.

        For more information see `FT.SUGLEN <https://redis.io/commands/ft.suglen>`_.
        """  # noqa
        return self.execute_command(SUGLEN_COMMAND, key)

    def sugdel(self, key: str, string: str) -> int:
        """
        Delete a string from the AutoCompleter index.
        Returns 1 if the string was found and deleted, 0 otherwise.

        For more information see `FT.SUGDEL <https://redis.io/commands/ft.sugdel>`_.
        """  # noqa
        return self.execute_command(SUGDEL_COMMAND, key, string)

    def sugget(
        self,
        key: str,
        prefix: str,
        fuzzy: bool = False,
        num: int = 10,
        with_scores: bool = False,
        with_payloads: bool = False,
    ) -> List[SuggestionParser]:
        """
        Get a list of suggestions from the AutoCompleter, for a given prefix.

        Parameters:

        prefix : str
            The prefix we are searching. **Must be valid ascii or utf-8**
        fuzzy : bool
            If set to true, the prefix search is done in fuzzy mode.
            **NOTE**: Running fuzzy searches on short (<3 letters) prefixes
            can be very
            slow, and even scan the entire index.
        with_scores : bool
            If set to true, we also return the (refactored) score of
            each suggestion.
            This is normally not needed, and is NOT the original score
            inserted into the index.
        with_payloads : bool
            Return suggestion payloads
        num : int
            The maximum number of results we return. Note that we might
            return less. The algorithm trims irrelevant suggestions.

        Returns:

        list:
             A list of Suggestion objects. If with_scores was False, the
             score of all suggestions is 1.

        For more information see `FT.SUGGET <https://redis.io/commands/ft.sugget>`_.
        """  # noqa
        args = [SUGGET_COMMAND, key, prefix, "MAX", num]
        if fuzzy:
            args.append(FUZZY)
        if with_scores:
            args.append(WITHSCORES)
        if with_payloads:
            args.append(WITHPAYLOADS)

        res = self.execute_command(*args)
        if isinstance(res, _PIPELINE_TYPES):
            return res
        results = []
        if not res:
            return results

        parser = SuggestionParser(with_scores, with_payloads, res)
        return [s for s in parser]

    def synupdate(self, groupid: str, skipinitial: bool = False, *terms: List[str]):
        """
        Updates a synonym group.
        The command is used to create or update a synonym group with
        additional terms.
        Only documents which were indexed after the update will be affected.

        Parameters:

        groupid :
            Synonym group id.
        skipinitial : bool
            If set to true, we do not scan and index.
        terms :
            The terms.

        For more information see `FT.SYNUPDATE <https://redis.io/commands/ft.synupdate>`_.
        """  # noqa
        cmd = [SYNUPDATE_CMD, self.index_name, groupid]
        if skipinitial:
            cmd.extend(["SKIPINITIALSCAN"])
        cmd.extend(terms)
        return self.execute_command(*cmd)

    def syndump(self):
        """
        Dumps the contents of a synonym group.

        The command is used to dump the synonyms data structure.
        Returns a list of synonym terms and their synonym group ids.

        For more information see `FT.SYNDUMP <https://redis.io/commands/ft.syndump>`_.
        """  # noqa
        res = self.execute_command(SYNDUMP_CMD, self.index_name)
        if isinstance(res, _PIPELINE_TYPES):
            return res
        return self._parse_results(SYNDUMP_CMD, res)


class AsyncSearchCommands(SearchCommands):
    async def info(self):
        """
        Get info and stats about the current index, including the number of
        documents, memory consumption, etc

        For more information see `FT.INFO <https://redis.io/commands/ft.info>`_.
        """

        res = await self.execute_command(INFO_CMD, self.index_name)
        if isinstance(res, _PIPELINE_TYPES):
            return res
        return self._parse_results(INFO_CMD, res)

    async def aliaslist(self) -> Set[str | bytes]:
        """
        List all aliases associated with the current index as an unordered set.

        The index must be the name of an index created with ``FT.CREATE``; an
        alias name is not accepted as a substitute. Returns an empty set when
        the index exists but has no aliases. Alias names honor
        ``decode_responses`` (``str`` when decoding is enabled, ``bytes``
        otherwise).

        For more information see `FT.ALIASLIST <https://redis.io/commands/ft.aliaslist>`_.
        """  # noqa
        res = await self.execute_command(ALIAS_LIST_CMD, self.index_name)
        if isinstance(res, _PIPELINE_TYPES):
            return res
        return self._parse_results(ALIAS_LIST_CMD, res)

    async def search(
        self,
        query: Union[str, Query],
        query_params: Optional[Dict[str, Union[str, int, float, bytes]]] = None,
    ):
        """
        Search the index for a given query, and return a result of documents

        ### Parameters

        - **query**: the search query. Either a text for simple queries with
                     default parameters, or a Query object for complex queries.
                     See RediSearch's documentation on query format

        For more information see `FT.SEARCH <https://redis.io/commands/ft.search>`_.
        """  # noqa
        args, query = self._mk_query_args(query, query_params=query_params)
        st = time.monotonic()

        # FT.SEARCH results may contain binary field values (e.g. vector
        # embeddings), so always request raw bytes from the wire and let the
        # search-layer parsers decode per-field.  Mirrors ``hybrid_search``.
        options = {NEVER_DECODE: True}
        if isinstance(self, Pipeline):
            options["query"] = query
            options["duration"] = 0

        res = await self.execute_command(SEARCH_CMD, *args, **options)

        if isinstance(res, _PIPELINE_TYPES):
            return res

        return self._parse_results(
            SEARCH_CMD, res, query=query, duration=(time.monotonic() - st) * 1000.0
        )

    @experimental_method()
    async def hybrid_search(
        self,
        query: HybridQuery,
        combine_method: Optional[CombineResultsMethod] = None,
        post_processing: Optional[HybridPostProcessingConfig] = None,
        params_substitution: Optional[Dict[str, Union[str, int, float, bytes]]] = None,
        timeout: Optional[int] = None,
        cursor: Optional[HybridCursorQuery] = None,
    ) -> Union[HybridResult, HybridCursorResult, Pipeline]:
        """
        Execute a hybrid search using both text and vector queries

        Args:
            - **query**: HybridQuery object
                        Contains the text and vector queries
            - **combine_method**: CombineResultsMethod object
                        Contains the combine method and parameters
            - **post_processing**: HybridPostProcessingConfig object
                        Contains the post processing configuration
            - **params_substitution**: Dict[str, Union[str, int, float, bytes]]
                        Contains the parameters substitution
            - **timeout**: int - contains the timeout in milliseconds
            - **cursor**: HybridCursorQuery object - contains the cursor configuration.
                        ``WITHCURSOR`` for ``FT.HYBRID`` is exposed only by
                        Redis versions 8.4 to 8.10.


        For more information see `FT.SEARCH <https://redis.io/commands/ft.hybrid>`.
        """
        index = self.index_name
        options = {}
        pieces = [HYBRID_CMD, index]
        pieces.extend(query.get_args())
        if combine_method:
            pieces.extend(combine_method.get_args())
        if post_processing:
            pieces.extend(post_processing.build_args())
            options["post_processing"] = post_processing
        if params_substitution:
            pieces.extend(self.get_params_args(params_substitution))
        if timeout is not None:
            pieces.extend(("TIMEOUT", timeout))
        if cursor:
            options["cursor"] = True
            pieces.extend(cursor.build_args())

        # Preserve HYBRID result values as bytes by default, matching the
        # legacy RESP2 Search surface; selected LOAD fields can opt into
        # decoding through HybridPostProcessingConfig.load(..., decode_field=True).
        options[NEVER_DECODE] = True
        options["query"] = query

        res = await self.execute_command(*pieces, **options)

        if isinstance(res, _PIPELINE_TYPES):
            return res

        return self._parse_results(HYBRID_CMD, res, **options)

    async def aggregate(
        self,
        query: Union[AggregateResult, Cursor],
        query_params: Optional[Dict[str, Union[str, int, float, bytes]]] = None,
    ):
        """
        Issue an aggregation query.

        ### Parameters

        **query**: This can be either an `AggregateRequest`, or a `Cursor`

        An `AggregateResult` object is returned. You can access the rows from
        its `rows` property, which will always yield the rows of the result.

        For more information see `FT.AGGREGATE <https://redis.io/commands/ft.aggregate>`_.
        """  # noqa
        if isinstance(query, AggregateRequest):
            has_cursor = bool(query._cursor)
            cmd = [AGGREGATE_CMD, self.index_name] + query.build_args()
        elif isinstance(query, Cursor):
            has_cursor = True
            cmd = [CURSOR_CMD, "READ", self.index_name] + query.build_args()
        else:
            raise ValueError("Bad query", query)
        cmd += self.get_params_args(query_params)

        options = {}
        if isinstance(self, Pipeline):
            options["query"] = query
            options["has_cursor"] = has_cursor
        raw = await self.execute_command(*cmd, **options)
        if isinstance(raw, _PIPELINE_TYPES):
            return raw
        return self._parse_results(
            AGGREGATE_CMD, raw, query=query, has_cursor=has_cursor
        )

    async def spellcheck(self, query, distance=None, include=None, exclude=None):
        """
        Issue a spellcheck query

        ### Parameters

        **query**: search query.
        **distance***: the maximal Levenshtein distance for spelling
                       suggestions (default: 1, max: 4).
        **include**: specifies an inclusion custom dictionary.
        **exclude**: specifies an exclusion custom dictionary.

        For more information see `FT.SPELLCHECK <https://redis.io/commands/ft.spellcheck>`_.
        """  # noqa
        cmd = [SPELLCHECK_CMD, self.index_name, query]
        if distance:
            cmd.extend(["DISTANCE", distance])

        if include:
            cmd.extend(["TERMS", "INCLUDE", include])

        if exclude:
            cmd.extend(["TERMS", "EXCLUDE", exclude])

        res = await self.execute_command(*cmd)

        if isinstance(res, _PIPELINE_TYPES):
            return res
        return self._parse_results(SPELLCHECK_CMD, res)

    @deprecated_function(
        version="8.0.0",
        reason="deprecated since Redis 8.0, call config_set from core module instead",
    )
    async def config_set(self, option: str, value: str) -> bool:
        """Set runtime configuration option.

        ### Parameters

        - **option**: the name of the configuration option.
        - **value**: a value for the configuration option.

        For more information see `FT.CONFIG SET <https://redis.io/commands/ft.config-set>`_.
        """  # noqa
        cmd = [CONFIG_CMD, "SET", option, value]
        raw = await self.execute_command(*cmd)
        if isinstance(raw, _PIPELINE_TYPES):
            return raw
        return raw == "OK"

    @deprecated_function(
        version="8.0.0",
        reason="deprecated since Redis 8.0, call config_get from core module instead",
    )
    async def config_get(self, option: str) -> str:
        """Get runtime configuration option value.

        ### Parameters

        - **option**: the name of the configuration option.

        For more information see `FT.CONFIG GET <https://redis.io/commands/ft.config-get>`_.
        """  # noqa
        cmd = [CONFIG_CMD, "GET", option]
        res = {}
        res = await self.execute_command(*cmd)
        if isinstance(res, _PIPELINE_TYPES):
            return res
        return self._parse_results(CONFIG_CMD, res)

    async def load_document(self, id, field_encodings: Optional[Dict[str, Any]] = None):
        """
        Load a single document by id

        - **field_encodings**: optional dict mapping field names to encodings.
          If a field's encoding is ``None`` the raw bytes value is preserved
          (useful for binary data such as vectors).
        """
        fields = await self.client.hgetall(id)
        fields = {
            str_if_bytes(k): decode_field_value(v, str_if_bytes(k), field_encodings)
            for k, v in fields.items()
        }

        try:
            del fields["id"]
        except KeyError:
            pass

        return Document(id=id, **fields)

    async def sugadd(self, key, *suggestions, **kwargs):
        """
        Add suggestion terms to the AutoCompleter engine. Each suggestion has
        a score and string.
        If kwargs["increment"] is true and the terms are already in the
        server's dictionary, we increment their scores.

        For more information see `FT.SUGADD <https://redis.io/commands/ft.sugadd>`_.
        """  # noqa
        # If Transaction is not False it will MULTI/EXEC which will error
        pipe = self.pipeline(transaction=False)
        for sug in suggestions:
            args = [SUGADD_COMMAND, key, sug.string, sug.score]
            if kwargs.get("increment"):
                args.append("INCR")
            if sug.payload:
                args.append("PAYLOAD")
                args.append(sug.payload)

            pipe.execute_command(*args)

        return (await pipe.execute())[-1]

    async def sugget(
        self,
        key: str,
        prefix: str,
        fuzzy: bool = False,
        num: int = 10,
        with_scores: bool = False,
        with_payloads: bool = False,
    ) -> List[SuggestionParser]:
        """
        Get a list of suggestions from the AutoCompleter, for a given prefix.

        Parameters:

        prefix : str
            The prefix we are searching. **Must be valid ascii or utf-8**
        fuzzy : bool
            If set to true, the prefix search is done in fuzzy mode.
            **NOTE**: Running fuzzy searches on short (<3 letters) prefixes
            can be very
            slow, and even scan the entire index.
        with_scores : bool
            If set to true, we also return the (refactored) score of
            each suggestion.
            This is normally not needed, and is NOT the original score
            inserted into the index.
        with_payloads : bool
            Return suggestion payloads
        num : int
            The maximum number of results we return. Note that we might
            return less. The algorithm trims irrelevant suggestions.

        Returns:

        list:
             A list of Suggestion objects. If with_scores was False, the
             score of all suggestions is 1.

        For more information see `FT.SUGGET <https://redis.io/commands/ft.sugget>`_.
        """  # noqa
        args = [SUGGET_COMMAND, key, prefix, "MAX", num]
        if fuzzy:
            args.append(FUZZY)
        if with_scores:
            args.append(WITHSCORES)
        if with_payloads:
            args.append(WITHPAYLOADS)

        ret = await self.execute_command(*args)
        if isinstance(ret, _PIPELINE_TYPES):
            return ret
        results = []
        if not ret:
            return results

        parser = SuggestionParser(with_scores, with_payloads, ret)
        return [s for s in parser]
