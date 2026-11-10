"""Unit tests for the RESP3 search response parsers.

These tests exercise the pure-Python parsing logic and do not require a
running Redis server, so they live in the ``fixed_client`` test group.
"""

from unittest.mock import MagicMock

import pytest

from redis.commands.search import Search
from redis.commands.search.aggregation import AggregateRequest
from redis.commands.search.query import Query
from redis.commands.search.result import Result


@pytest.mark.fixed_client
class TestResultFromResp3:
    def test_str_keys_are_parsed(self):
        """Baseline: a response with native ``str`` keys parses correctly."""
        res = {
            "total_results": 2,
            "results": [
                {
                    "id": "doc1",
                    "score": 1.5,
                    "extra_attributes": {"field1": "value1"},
                },
                {
                    "id": "doc2",
                    "score": 0.5,
                    "extra_attributes": {"field1": "value2"},
                },
            ],
            "warning": ["a warning"],
        }

        result = Result.from_resp3(res, with_scores=True)

        assert result.total == 2
        assert result.warnings == ["a warning"]
        assert [d.id for d in result.docs] == ["doc1", "doc2"]
        assert [d.score for d in result.docs] == [1.5, 0.5]
        assert result.docs[0].field1 == "value1"

    def test_bytes_keys_are_parsed(self):
        """Regression test for #4107.

        On a RESP3 connection opened with ``decode_responses=False`` the map
        keys returned by the server are ``bytes``.  Before the fix
        :meth:`Result.from_resp3` looked up ``"total_results"``/``"results"``
        as ``str`` keys, missed them entirely and returned an empty result.
        """
        res = {
            b"total_results": 2,
            b"results": [
                {
                    b"id": b"doc1",
                    b"score": 1.5,
                    b"extra_attributes": {b"field1": b"value1"},
                },
                {
                    b"id": b"doc2",
                    b"score": 0.5,
                    b"extra_attributes": {b"field1": b"value2"},
                },
            ],
            b"warning": [b"a warning"],
        }

        result = Result.from_resp3(res, with_scores=True)

        assert result.total == 2
        assert result.warnings == ["a warning"]
        assert [d.id for d in result.docs] == ["doc1", "doc2"]
        assert [d.score for d in result.docs] == [1.5, 0.5]
        assert result.docs[0].field1 == "value1"
        assert result.docs[1].field1 == "value2"

    def test_mixed_str_and_bytes_keys(self):
        """Outer ``bytes`` keys with inner ``str`` keys (and vice-versa) parse.

        Different parser layers (hiredis vs. the pure-python parser, RESP2 vs.
        RESP3) can produce mixed maps in practice, so each level is normalised
        independently.
        """
        res = {
            b"total_results": 1,
            b"results": [
                {"id": "doc1", "extra_attributes": {"f": "v"}},
            ],
            b"warning": [],
        }

        result = Result.from_resp3(res)

        assert result.total == 1
        assert result.docs[0].id == "doc1"
        assert result.docs[0].f == "v"

    def test_empty_or_none_response(self):
        """``None`` and an empty dict both yield an empty result."""
        for empty in (None, {}, {b"total_results": 0, b"results": []}):
            r = Result.from_resp3(empty)
            assert r.total == 0
            assert r.docs == []
            assert r.warnings == []


@pytest.mark.fixed_client
class TestResultFieldsNamedLikeDocumentAttributes:
    """A document field named ``payload`` or ``score`` used to raise
    ``TypeError: Document() got multiple values for keyword argument``."""

    def test_resp2_field_named_payload(self):
        res = [1, b"doc1", [b"payload", b"hello", b"title", b"t"]]

        result = Result(res, hascontent=True)

        assert result.docs[0].id == "doc1"
        assert result.docs[0].payload == "hello"
        assert result.docs[0].title == "t"

    def test_resp2_field_named_score_with_scores(self):
        res = [1, b"doc1", b"1.5", [b"score", b"10"]]

        result = Result(res, hascontent=True, with_scores=True)

        assert result.docs[0].score == 1.5

    def test_resp2_field_named_payload_with_payloads(self):
        res = [1, b"doc1", b"server-payload", [b"payload", b"field-value"]]

        result = Result(res, hascontent=True, has_payload=True)

        assert result.docs[0].payload == "server-payload"

    def test_resp2_field_named_score_without_scores(self):
        res = [1, b"doc1", [b"score", b"10"]]

        result = Result(res, hascontent=True)

        assert result.docs[0].score == "10"
        assert result.docs[0].payload is None

    def test_resp3_field_named_payload(self):
        res = {
            "total_results": 1,
            "results": [
                {"id": "doc1", "extra_attributes": {"payload": "hello"}},
            ],
        }

        result = Result.from_resp3(res)

        assert result.docs[0].id == "doc1"
        assert result.docs[0].payload == "hello"

    def test_resp3_field_named_score_with_scores(self):
        res = {
            "total_results": 1,
            "results": [
                {"id": "doc1", "score": 1.5, "extra_attributes": {"score": "10"}},
            ],
        }

        result = Result.from_resp3(res, with_scores=True)

        assert result.docs[0].score == 1.5

    def test_resp3_with_payloads(self):
        res = {
            "total_results": 1,
            "results": [
                {
                    "id": "doc1",
                    "payload": b"server-payload",
                    "extra_attributes": {"payload": "field-value"},
                },
            ],
        }

        query = Query("*").with_payloads()

        result = _make_search()._parse_search_resp3(res, query=query)

        assert result.docs[0].payload == "server-payload"

    def test_resp3_profile_with_payloads(self):
        res = {
            "Results": {
                "total_results": 1,
                "results": [
                    {
                        "id": "doc1",
                        "payload": b"server-payload",
                        "extra_attributes": {"payload": "field-value"},
                    },
                ],
            },
            "Profile": {"Shards": [], "Coordinator": {}},
        }

        query = Query("*").with_payloads()

        result, _ = _make_search()._parse_profile_resp3(res, query=query)

        assert result.docs[0].payload == "server-payload"


def _make_search():
    """Build a ``Search`` instance whose parsers can be exercised without
    talking to Redis."""
    client = MagicMock()
    return Search(client)


@pytest.mark.fixed_client
class TestParseAggregateResp3:
    """Regression tests for ``SearchCommands._parse_aggregate_resp3``.

    Mirrors :class:`TestResultFromResp3` but for the FT.AGGREGATE /
    FT.CURSOR READ / FT.PROFILE AGGREGATE callback.  Before the fix, a
    byte-keyed RESP3 map (the shape produced when the wire is RESP3 and
    the client was opened with ``decode_responses=False``) parsed to an
    empty :class:`AggregateResult` with ``total == 0`` and no rows.
    """

    def _request(self):
        return AggregateRequest("*")

    def test_str_keys_are_parsed(self):
        s = _make_search()
        res = {
            "total_results": 1,
            "warning": ["w"],
            "results": [
                {"extra_attributes": {"parent": "redis", "n": "3"}},
            ],
        }
        out = s._parse_aggregate_resp3(res, query=self._request())
        assert out.total == 1
        assert out.warnings == ["w"]
        assert out.rows == [["parent", "redis", "n", "3"]]

    def test_bytes_keys_are_parsed(self):
        """The map keys arrive as ``bytes`` on RESP3 with decode_responses=False.

        Without the fix, ``data.get("total_results", 0)`` and
        ``data.get("results", [])`` both miss, so the result has
        ``total == 0`` and ``rows == []``.  Inner ``extra_attributes``
        content stays as ``bytes`` because it is document data, not a
        structural key.
        """
        res = {
            b"total_results": 1,
            b"warning": [b"w"],
            b"results": [
                {b"extra_attributes": {b"parent": b"redis", b"n": b"3"}},
            ],
        }
        out = _make_search()._parse_aggregate_resp3(res, query=self._request())
        assert out.total == 1
        assert out.warnings == ["w"]
        # Content values stay as bytes (matches RESP2 with
        # decode_responses=False); only structural keys are normalised.
        assert out.rows == [[b"parent", b"redis", b"n", b"3"]]

    def test_with_cursor_bytes_keys(self):
        """``has_cursor=True`` wraps the payload in ``[data_dict, cursor_id]``;
        the inner dict still needs structural key normalisation."""
        res = [
            {
                b"total_results": 2,
                b"warning": [],
                b"results": [
                    {b"extra_attributes": {b"a": b"1"}},
                    {b"extra_attributes": {b"a": b"2"}},
                ],
            },
            42,
        ]
        s = _make_search()
        out = s._parse_aggregate_resp3(res, query=self._request(), has_cursor=True)
        assert out.total == 2
        assert out.cursor is not None
        assert out.cursor.cid == 42
        assert out.rows == [[b"a", b"1"], [b"a", b"2"]]

    def test_empty_or_none_response(self):
        """``None`` and an empty dict both yield an empty AggregateResult."""
        s = _make_search()
        for empty in (None, {}, {b"total_results": 0, b"results": []}):
            out = s._parse_aggregate_resp3(empty, query=self._request())
            assert out.total == 0
            assert out.rows == []
            assert out.warnings == []


@pytest.mark.fixed_client
class TestParseSpellcheckResp3:
    """Regression tests for ``SearchCommands._parse_spellcheck_resp3``.

    Before the fix, a byte-keyed RESP3 map produced an empty ``{}``
    because ``res.get("results", {})`` looked up the ``str`` key
    ``"results"`` against a ``bytes``-keyed dict.
    """

    def test_str_keys_are_parsed(self):
        s = _make_search()
        res = {
            "results": {
                "impornant": [{"important": 0.5}, {"impotent": 0.25}],
            }
        }
        out = s._parse_spellcheck_resp3(res)
        assert list(out.keys()) == ["impornant"]
        assert out["impornant"] == [
            {"score": "0.5", "suggestion": "important"},
            {"score": "0.25", "suggestion": "impotent"},
        ]

    def test_bytes_keys_are_parsed(self):
        """The outer ``results`` structural key arrives as ``bytes`` on
        RESP3 with decode_responses=False.  The fix normalises only that
        structural key; inner term keys and suggestion values mirror
        RESP2's behaviour and keep their wire type (bytes when
        undecoded)."""
        s = _make_search()
        res = {
            b"results": {
                b"impornant": [{b"important": 0.5}],
            }
        }
        out = s._parse_spellcheck_resp3(res)
        # Before the fix this returned ``{}``.
        assert out  # not empty
        # The term key mirrors the RESP2 form (bytes when decode is off).
        assert list(out.keys()) == [b"impornant"]
        suggestions = list(out.values())[0]
        assert suggestions[0]["score"] == "0.5"
        # And the suggestion stays in its wire form (bytes when
        # ``decode_responses=False``) so the legacy path matches RESP2.
        assert suggestions[0]["suggestion"] == b"important"

    def test_no_corrections_returns_empty(self):
        """Term with no candidate suggestions still yields ``{}``."""
        s = _make_search()
        assert s._parse_spellcheck_resp3({b"results": {b"vlis": []}}) == {}
        assert s._parse_spellcheck_resp3({"results": {"vlis": []}}) == {}

    def test_non_dict_falls_through_to_resp2_parser(self):
        """A non-dict response (e.g. ``0`` from RESP2) still goes through
        the legacy :meth:`_parse_spellcheck` path."""
        s = _make_search()
        # ``_parse_spellcheck`` short-circuits on ``res == 0``.
        assert s._parse_spellcheck_resp3(0) == {}
