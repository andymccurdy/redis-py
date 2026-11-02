import bz2
import csv
import os
import random
import time
from io import TextIOWrapper
from unittest import mock

import numpy as np
import pytest
from redis import ResponseError
import redis

import redis.commands.search.aggregation as aggregations
from redis.commands.search.hybrid_query import (
    CombinationMethods,
    CombineResultsMethod,
    HybridCursorQuery,
    HybridFilter,
    HybridPostProcessingConfig,
    HybridQuery,
    HybridSearchQuery,
    HybridVsimQuery,
    VectorSearchMethods,
)
from redis.commands.search.hybrid_result import HybridCursorResult
import redis.commands.search.reducers as reducers
from redis.commands.json.path import Path
from redis.commands.search import Search
from redis.commands.search.field import (
    GeoField,
    GeoShapeField,
    NumericField,
    TagField,
    TextField,
    VectorField,
)
from redis.commands.search.index_definition import IndexDefinition, IndexType
from redis.commands.search.query import (
    GeoFilter,
    NumericFilter,
    Query,
    SortbyField,
)
from redis.commands.search.result import Result
from redis.commands.search.suggestion import Suggestion
from redis.utils import SENTINEL, safe_str

from .conftest import (
    _get_client,
    expects_resp2_shape,
    expects_resp3_shape,
    expects_unified_shape,
    get_protocol_version,
    skip_if_redis_enterprise,
    skip_if_server_version_gte,
    skip_if_server_version_lt,
    skip_ifmodversion_lt,
)

WILL_PLAY_TEXT = os.path.abspath(
    os.path.join(os.path.dirname(__file__), "testdata", "will_play_text.csv.bz2")
)

TITLES_CSV = os.path.abspath(
    os.path.join(os.path.dirname(__file__), "testdata", "titles.csv")
)


def _assert_search_result(client, result, expected_doc_ids):
    """
    Make sure the result of a geo search is as expected, taking into account the RESP
    version being used.
    """
    if expects_resp2_shape(client) or expects_unified_shape(client):
        assert set([doc.id for doc in result.docs]) == set(expected_doc_ids)
    elif expects_resp3_shape(client):
        assert set([doc["id"] for doc in result["results"]]) == set(expected_doc_ids)


def _search_total(client, result):
    """
    Return the number of matched documents in a search result, taking into
    account the RESP version being used.
    """
    if expects_resp2_shape(client) or expects_unified_shape(client):
        return result.total
    return result["total_results"]


# Languages RediSearch analyses with a Snowball stemmer.  For each case the
# document stores ``doc_word`` (inside ``text``) and the query uses a different
# surface form that shares the same stem in that language but NOT in English --
# so a match proves the language stemmer is applied rather than a literal match.
NON_ENGLISH_STEMMING_CASES = [
    # (language, text, query, doc_word)
    ("german", "Die Kinder spielen im Garten", "Kind", "Kinder"),
    ("french", "Les chevaux courent vite", "cheval", "chevaux"),
    ("spanish", "Nosotros hablamos mucho", "hablar", "hablamos"),
    ("greek", "Οι άνθρωποι περπατούν", "άνθρωπος", "άνθρωποι"),
]


class SearchTestsBase:
    @staticmethod
    def waitForIndex(env, idx, timeout=None):
        delay = 0.02
        while True:
            try:
                res = env.execute_command("FT.INFO", idx)
                # ``execute_command`` bypasses the search module's
                # callbacks, so the response is the raw wire shape.
                # With ``decode_responses=False`` the structural keys
                # arrive as bytes; accept both forms.
                try:
                    i = res.index("indexing")
                except ValueError:
                    i = res.index(b"indexing")
                if int(res[i + 1]) == 0:
                    break
            except ValueError:
                break
            except AttributeError:
                # RESP3 dict response.  Keys may be ``str`` or ``bytes``
                # depending on ``decode_responses``.
                indexing = res.get("indexing")
                if indexing is None:
                    indexing = res.get(b"indexing")
                try:
                    if int(indexing) == 0:
                        break
                except (TypeError, ValueError):
                    break
            except ResponseError:
                # index doesn't exist yet
                # continue to sleep and try again
                pass

            time.sleep(delay)
            if timeout is not None:
                timeout -= delay
                if timeout <= 0:
                    break

    @staticmethod
    def getClient(client):
        """
        Gets a client client attached to an index name which is ready to be
        created
        """
        return client

    @staticmethod
    def createIndex(client, num_docs=100, definition=None):
        try:
            client.create_index(
                (
                    TextField("play", weight=5.0),
                    TextField("txt"),
                    NumericField("chapter"),
                ),
                definition=definition,
            )
        except redis.ResponseError:
            client.dropindex(delete_documents=True)
            return SearchTestsBase.createIndex(
                client, num_docs=num_docs, definition=definition
            )

        chapters = {}
        bzfp = TextIOWrapper(bz2.BZ2File(WILL_PLAY_TEXT), encoding="utf8")

        r = csv.reader(bzfp, delimiter=";")
        for n, line in enumerate(r):
            play, chapter, _, text = line[1], line[2], line[4], line[5]

            key = f"{play}:{chapter}".lower()
            d = chapters.setdefault(key, {})
            d["play"] = play
            d["txt"] = d.get("txt", "") + " " + text
            d["chapter"] = int(chapter or 0)
            if len(chapters) == num_docs:
                break

        indexer = client.batch_indexer(chunk_size=50)
        assert isinstance(indexer, Search.BatchIndexer)
        assert 50 == indexer.chunk_size

        # one round trip for all documents, not one HSET each
        pipe = indexer.client.client.pipeline(transaction=False)
        for key, doc in chapters.items():
            pipe.hset(key, mapping=doc)
        pipe.execute()
        indexer.commit()

    @pytest.fixture
    def client(self, request, stack_url):
        r = _get_client(redis.Redis, request, decode_responses=True, from_url=stack_url)
        r.flushdb()
        return r


class TestBaseSearchFunctionality(SearchTestsBase):
    @pytest.mark.redismod
    # FT.DEL is not available on Redis Enterprise's search module.
    @skip_if_redis_enterprise()
    def test_client(self, client):
        num_docs = 500
        self.createIndex(client.ft(), num_docs=num_docs)
        self.waitForIndex(client, getattr(client.ft(), "index_name", "idx"))
        # verify info
        info = client.ft().info()
        for k in [
            "index_name",
            "index_options",
            "attributes",
            "num_docs",
            "max_doc_id",
            "num_terms",
            "num_records",
            "inverted_sz_mb",
            "offset_vectors_sz_mb",
            "doc_table_size_mb",
            "key_table_size_mb",
            "records_per_doc_avg",
            "bytes_per_record_avg",
            "offsets_per_term_avg",
            "offset_bits_per_record_avg",
        ]:
            assert k in info

        assert client.ft().index_name == info["index_name"]
        assert num_docs == int(info["num_docs"])

        res = client.ft().search("henry iv")
        if expects_resp2_shape(client) or expects_unified_shape(client):
            assert isinstance(res, Result)
            assert 225 == res.total
            assert 10 == len(res.docs)
            assert res.duration > 0

            for doc in res.docs:
                assert doc.id
                assert doc["id"]
                assert doc.play == "Henry IV"
                assert doc["play"] == "Henry IV"
                assert len(doc.txt) > 0

            # test no content
            res = client.ft().search(Query("king").no_content())
            assert 194 == res.total
            assert 10 == len(res.docs)
            for doc in res.docs:
                assert "txt" not in doc.__dict__
                assert "play" not in doc.__dict__

            # test verbatim vs no verbatim
            total = client.ft().search(Query("kings").no_content()).total
            vtotal = client.ft().search(Query("kings").no_content().verbatim()).total
            assert total > vtotal

            # test in fields
            txt_total = (
                client.ft()
                .search(Query("henry").no_content().limit_fields("txt"))
                .total
            )
            play_total = (
                client.ft()
                .search(Query("henry").no_content().limit_fields("play"))
                .total
            )
            both_total = (
                client.ft()
                .search(Query("henry").no_content().limit_fields("play", "txt"))
                .total
            )
            assert 129 == txt_total
            assert 494 == play_total
            assert 494 == both_total

            # test load_document
            doc = client.ft().load_document("henry vi part 3:62")
            assert doc is not None
            assert "henry vi part 3:62" == doc.id
            assert doc.play == "Henry VI Part 3"
            assert len(doc.txt) > 0

            # test in-keys
            ids = [x.id for x in client.ft().search(Query("henry")).docs]
            assert 10 == len(ids)
            subset = ids[:5]
            docs = client.ft().search(Query("henry").limit_ids(*subset))
            assert len(subset) == docs.total
            ids = [x.id for x in docs.docs]
            assert set(ids) == set(subset)

            # test slop and in order
            assert 193 == client.ft().search(Query("henry king")).total
            assert 3 == client.ft().search(Query("henry king").slop(0).in_order()).total
            assert (
                52 == client.ft().search(Query("king henry").slop(0).in_order()).total
            )
            assert 53 == client.ft().search(Query("henry king").slop(0)).total
            assert 167 == client.ft().search(Query("henry king").slop(100)).total

            # test delete document
            client.hset("doc-5ghs2", mapping={"play": "Death of a Salesman"})
            res = client.ft().search(Query("death of a salesman"))
            assert 1 == res.total

            assert 1 == client.ft().delete_document("doc-5ghs2")
            res = client.ft().search(Query("death of a salesman"))
            assert 0 == res.total
            assert 0 == client.ft().delete_document("doc-5ghs2")

            client.hset("doc-5ghs2", mapping={"play": "Death of a Salesman"})
            res = client.ft().search(Query("death of a salesman"))
            assert 1 == res.total
            client.ft().delete_document("doc-5ghs2")
        elif expects_resp3_shape(client):
            assert isinstance(res, dict)
            assert 225 == res["total_results"]
            assert 10 == len(res["results"])

            for doc in res["results"]:
                assert doc["id"]
                assert doc["extra_attributes"]["play"] == "Henry IV"
                assert len(doc["extra_attributes"]["txt"]) > 0

            # test no content
            res = client.ft().search(Query("king").no_content())
            assert 194 == res["total_results"]
            assert 10 == len(res["results"])
            for doc in res["results"]:
                assert "extra_attributes" not in doc.keys()

            # test verbatim vs no verbatim
            total = client.ft().search(Query("kings").no_content())["total_results"]
            vtotal = client.ft().search(Query("kings").no_content().verbatim())[
                "total_results"
            ]
            assert total > vtotal

            # test in fields
            txt_total = client.ft().search(
                Query("henry").no_content().limit_fields("txt")
            )["total_results"]
            play_total = client.ft().search(
                Query("henry").no_content().limit_fields("play")
            )["total_results"]
            both_total = client.ft().search(
                Query("henry").no_content().limit_fields("play", "txt")
            )["total_results"]
            assert 129 == txt_total
            assert 494 == play_total
            assert 494 == both_total

            # test load_document
            doc = client.ft().load_document("henry vi part 3:62")
            assert doc is not None
            assert "henry vi part 3:62" == doc.id
            assert doc.play == "Henry VI Part 3"
            assert len(doc.txt) > 0

            # test in-keys
            ids = [x["id"] for x in client.ft().search(Query("henry"))["results"]]
            assert 10 == len(ids)
            subset = ids[:5]
            docs = client.ft().search(Query("henry").limit_ids(*subset))
            assert len(subset) == docs["total_results"]
            ids = [x["id"] for x in docs["results"]]
            assert set(ids) == set(subset)

            # test slop and in order
            assert 193 == client.ft().search(Query("henry king"))["total_results"]
            assert (
                3
                == client.ft().search(Query("henry king").slop(0).in_order())[
                    "total_results"
                ]
            )
            assert (
                52
                == client.ft().search(Query("king henry").slop(0).in_order())[
                    "total_results"
                ]
            )
            assert (
                53 == client.ft().search(Query("henry king").slop(0))["total_results"]
            )
            assert (
                167
                == client.ft().search(Query("henry king").slop(100))["total_results"]
            )

            # test delete document
            client.hset("doc-5ghs2", mapping={"play": "Death of a Salesman"})
            res = client.ft().search(Query("death of a salesman"))
            assert 1 == res["total_results"]

            assert 1 == client.ft().delete_document("doc-5ghs2")
            res = client.ft().search(Query("death of a salesman"))
            assert 0 == res["total_results"]
            assert 0 == client.ft().delete_document("doc-5ghs2")

            client.hset("doc-5ghs2", mapping={"play": "Death of a Salesman"})
            res = client.ft().search(Query("death of a salesman"))
            assert 1 == res["total_results"]
            client.ft().delete_document("doc-5ghs2")

    @pytest.mark.redismod
    @pytest.mark.onlynoncluster
    @skip_if_server_version_gte("7.9.0")
    def test_scores(self, client):
        client.ft().create_index((TextField("txt"),))

        client.hset("doc1", mapping={"txt": "foo baz"})
        client.hset("doc2", mapping={"txt": "foo bar"})

        q = Query("foo ~bar").with_scores()
        res = client.ft().search(q)
        if expects_resp2_shape(client) or expects_unified_shape(client):
            assert 2 == res.total
            assert "doc2" == res.docs[0].id
            assert 3.0 == res.docs[0].score
            assert "doc1" == res.docs[1].id
        elif expects_resp3_shape(client):
            assert 2 == res["total_results"]
            assert "doc2" == res["results"][0]["id"]
            assert 3.0 == res["results"][0]["score"]
            assert "doc1" == res["results"][1]["id"]

    @pytest.mark.redismod
    @pytest.mark.onlynoncluster
    @skip_if_server_version_lt("7.9.0")
    def test_scores_with_new_default_scorer(self, client):
        client.ft().create_index((TextField("txt"),))

        client.hset("doc1", mapping={"txt": "foo baz"})
        client.hset("doc2", mapping={"txt": "foo bar"})

        q = Query("foo ~bar").with_scores()
        res = client.ft().search(q)
        if expects_resp2_shape(client) or expects_unified_shape(client):
            assert 2 == res.total
            assert "doc2" == res.docs[0].id
            assert 0.87 == pytest.approx(res.docs[0].score, 0.01)
            assert "doc1" == res.docs[1].id
        elif expects_resp3_shape(client):
            assert 2 == res["total_results"]
            assert "doc2" == res["results"][0]["id"]
            assert 0.87 == pytest.approx(res["results"][0]["score"], 0.01)
            assert "doc1" == res["results"][1]["id"]

    @pytest.mark.redismod
    def test_stopwords(self, client):
        client.ft().create_index((TextField("txt"),), stopwords=["foo", "bar", "baz"])
        client.hset("doc1", mapping={"txt": "foo bar"})
        client.hset("doc2", mapping={"txt": "hello world"})
        self.waitForIndex(client, getattr(client.ft(), "index_name", "idx"))

        q1 = Query("foo bar").no_content()
        q2 = Query("foo bar hello world").no_content()
        res1, res2 = client.ft().search(q1), client.ft().search(q2)
        if expects_resp2_shape(client) or expects_unified_shape(client):
            assert 0 == res1.total
            assert 1 == res2.total
        elif expects_resp3_shape(client):
            assert 0 == res1["total_results"]
            assert 1 == res2["total_results"]

    @pytest.mark.redismod
    def test_filters(self, client):
        client.ft().create_index(
            (TextField("txt"), NumericField("num"), GeoField("loc"))
        )
        client.hset(
            "doc1", mapping={"txt": "foo bar", "num": 3.141, "loc": "-0.441,51.458"}
        )
        client.hset("doc2", mapping={"txt": "foo baz", "num": 2, "loc": "-0.1,51.2"})

        self.waitForIndex(client, getattr(client.ft(), "index_name", "idx"))
        # Test numerical filter
        q1 = Query("foo").add_filter(NumericFilter("num", 0, 2)).no_content()
        q2 = (
            Query("foo")
            .add_filter(NumericFilter("num", 2, NumericFilter.INF, minExclusive=True))
            .no_content()
        )
        res1, res2 = client.ft().search(q1), client.ft().search(q2)
        if expects_resp2_shape(client) or expects_unified_shape(client):
            assert 1 == res1.total
            assert 1 == res2.total
            assert "doc2" == res1.docs[0].id
            assert "doc1" == res2.docs[0].id
        elif expects_resp3_shape(client):
            assert 1 == res1["total_results"]
            assert 1 == res2["total_results"]
            assert "doc2" == res1["results"][0]["id"]
            assert "doc1" == res2["results"][0]["id"]

        # Test geo filter
        q1 = Query("foo").add_filter(GeoFilter("loc", -0.44, 51.45, 10)).no_content()
        q2 = Query("foo").add_filter(GeoFilter("loc", -0.44, 51.45, 100)).no_content()
        res1, res2 = client.ft().search(q1), client.ft().search(q2)

        if expects_resp2_shape(client) or expects_unified_shape(client):
            assert 1 == res1.total
            assert 2 == res2.total
            assert "doc1" == res1.docs[0].id

            # Sort results, after RDB reload order may change
            res = [res2.docs[0].id, res2.docs[1].id]
            res.sort()
            assert ["doc1", "doc2"] == res
        elif expects_resp3_shape(client):
            assert 1 == res1["total_results"]
            assert 2 == res2["total_results"]
            assert "doc1" == res1["results"][0]["id"]

            # Sort results, after RDB reload order may change
            res = [res2["results"][0]["id"], res2["results"][1]["id"]]
            res.sort()
            assert ["doc1", "doc2"] == res

    @pytest.mark.redismod
    def test_sort_by(self, client):
        client.ft().create_index((TextField("txt"), NumericField("num", sortable=True)))
        client.hset("doc1", mapping={"txt": "foo bar", "num": 1})
        client.hset("doc2", mapping={"txt": "foo baz", "num": 2})
        client.hset("doc3", mapping={"txt": "foo qux", "num": 3})

        # Test sort
        q1 = Query("foo").sort_by("num", asc=True).no_content()
        q2 = Query("foo").sort_by("num", asc=False).no_content()
        res1, res2 = client.ft().search(q1), client.ft().search(q2)

        if expects_resp2_shape(client) or expects_unified_shape(client):
            assert 3 == res1.total
            assert "doc1" == res1.docs[0].id
            assert "doc2" == res1.docs[1].id
            assert "doc3" == res1.docs[2].id
            assert 3 == res2.total
            assert "doc1" == res2.docs[2].id
            assert "doc2" == res2.docs[1].id
            assert "doc3" == res2.docs[0].id
        elif expects_resp3_shape(client):
            assert 3 == res1["total_results"]
            assert "doc1" == res1["results"][0]["id"]
            assert "doc2" == res1["results"][1]["id"]
            assert "doc3" == res1["results"][2]["id"]
            assert 3 == res2["total_results"]
            assert "doc1" == res2["results"][2]["id"]
            assert "doc2" == res2["results"][1]["id"]
            assert "doc3" == res2["results"][0]["id"]

    @pytest.mark.redismod
    @skip_ifmodversion_lt("2.0.0", "search")
    def test_drop_index(self, client):
        """
        Ensure the index gets dropped by data remains by default
        """
        for keep_docs in [[True, {}], [False, {"name": "haveit"}]]:
            idx = "HaveIt"
            index = self.getClient(client)
            index.hset("index:haveit", mapping={"name": "haveit"})
            idef = IndexDefinition(prefix=["index:"])
            index.ft(idx).create_index((TextField("name"),), definition=idef)
            self.waitForIndex(index, idx)
            index.ft(idx).dropindex(delete_documents=keep_docs[0])
            i = index.hgetall("index:haveit")
            assert i == keep_docs[1]

    @pytest.mark.redismod
    def test_example(self, client):
        # Creating the index definition and schema
        client.ft().create_index((TextField("title", weight=5.0), TextField("body")))

        # Indexing a document
        client.hset(
            "doc1",
            mapping={
                "title": "RediSearch",
                "body": "Redisearch impements a search engine on top of redis",
            },
        )

        # Searching with complex parameters:
        q = Query("search engine").verbatim().no_content().paging(0, 5)

        res = client.ft().search(q)
        assert res is not None

    @pytest.mark.redismod
    @skip_if_redis_enterprise()
    def test_auto_complete(self, client):
        with open(TITLES_CSV) as f:
            suggestions = [
                Suggestion(row[0], score=float(row[1])) for row in csv.reader(f)
            ]

        # sugadd pipelines its suggestions and returns the last reply, which is
        # the dictionary size, so batching keeps the size check per batch
        ft = client.ft()
        n = 0
        for i in range(0, len(suggestions), 500):
            batch = suggestions[i : i + 500]
            n += len(batch)
            assert n == ft.sugadd("ac", *batch)

        assert n == client.ft().suglen("ac")
        ret = client.ft().sugget("ac", "bad", with_scores=True)
        assert 2 == len(ret)
        assert "badger" == ret[0].string
        assert isinstance(ret[0].score, float)
        assert 1.0 != ret[0].score
        assert "badalte rishtey" == ret[1].string
        assert isinstance(ret[1].score, float)
        assert 1.0 != ret[1].score

        ret = client.ft().sugget("ac", "bad", fuzzy=True, num=10)
        assert 10 == len(ret)
        assert 1.0 == ret[0].score
        strs = {x.string for x in ret}

        for sug in strs:
            assert 1 == client.ft().sugdel("ac", sug)
        # make sure a second delete returns 0
        for sug in strs:
            assert 0 == client.ft().sugdel("ac", sug)

        # make sure they were actually deleted
        ret2 = client.ft().sugget("ac", "bad", fuzzy=True, num=10)
        for sug in ret2:
            assert sug.string not in strs

        # Test with payload
        client.ft().sugadd("ac", Suggestion("pay1", payload="pl1"))
        client.ft().sugadd("ac", Suggestion("pay2", payload="pl2"))
        client.ft().sugadd("ac", Suggestion("pay3", payload="pl3"))

        sugs = client.ft().sugget("ac", "pay", with_payloads=True, with_scores=True)
        assert 3 == len(sugs)
        for sug in sugs:
            assert sug.payload
            assert sug.payload.startswith("pl")

    @pytest.mark.redismod
    def test_no_index(self, client):
        client.ft().create_index(
            (
                TextField("field"),
                TextField("text", no_index=True, sortable=True),
                NumericField("numeric", no_index=True, sortable=True),
                GeoField("geo", no_index=True, sortable=True),
                TagField("tag", no_index=True, sortable=True),
            )
        )

        client.hset(
            "doc1",
            mapping={
                "field": "aaa",
                "text": "1",
                "numeric": "1",
                "geo": "1,1",
                "tag": "1",
            },
        )
        client.hset(
            "doc2",
            mapping={
                "field": "aab",
                "text": "2",
                "numeric": "2",
                "geo": "2,2",
                "tag": "2",
            },
        )
        self.waitForIndex(client, getattr(client.ft(), "index_name", "idx"))

        if expects_resp2_shape(client) or expects_unified_shape(client):
            res = client.ft().search(Query("@text:aa*"))
            assert 0 == res.total

            res = client.ft().search(Query("@field:aa*"))
            assert 2 == res.total

            res = client.ft().search(Query("*").sort_by("text", asc=False))
            assert 2 == res.total
            assert "doc2" == res.docs[0].id

            res = client.ft().search(Query("*").sort_by("text", asc=True))
            assert "doc1" == res.docs[0].id

            res = client.ft().search(Query("*").sort_by("numeric", asc=True))
            assert "doc1" == res.docs[0].id

            res = client.ft().search(Query("*").sort_by("geo", asc=True))
            assert "doc1" == res.docs[0].id

            res = client.ft().search(Query("*").sort_by("tag", asc=True))
            assert "doc1" == res.docs[0].id
        elif expects_resp3_shape(client):
            res = client.ft().search(Query("@text:aa*"))
            assert 0 == res["total_results"]

            res = client.ft().search(Query("@field:aa*"))
            assert 2 == res["total_results"]

            res = client.ft().search(Query("*").sort_by("text", asc=False))
            assert 2 == res["total_results"]
            assert "doc2" == res["results"][0]["id"]

            res = client.ft().search(Query("*").sort_by("text", asc=True))
            assert "doc1" == res["results"][0]["id"]

            res = client.ft().search(Query("*").sort_by("numeric", asc=True))
            assert "doc1" == res["results"][0]["id"]

            res = client.ft().search(Query("*").sort_by("geo", asc=True))
            assert "doc1" == res["results"][0]["id"]

            res = client.ft().search(Query("*").sort_by("tag", asc=True))
            assert "doc1" == res["results"][0]["id"]

        # Ensure exception is raised for non-indexable, non-sortable fields
        with pytest.raises(Exception):
            TextField("name", no_index=True, sortable=False)
        with pytest.raises(Exception):
            NumericField("name", no_index=True, sortable=False)
        with pytest.raises(Exception):
            GeoField("name", no_index=True, sortable=False)
        with pytest.raises(Exception):
            TagField("name", no_index=True, sortable=False)

    @pytest.mark.redismod
    def test_explain(self, client):
        client.ft().create_index((TextField("f1"), TextField("f2"), TextField("f3")))
        res = client.ft().explain("@f3:f3_val @f2:f2_val @f1:f1_val")
        assert res

    @pytest.mark.redismod
    def test_explaincli(self, client):
        with pytest.raises(NotImplementedError):
            client.ft().explain_cli("foo")

    @pytest.mark.redismod
    def test_summarize(self, client):
        self.createIndex(client.ft())
        self.waitForIndex(client, getattr(client.ft(), "index_name", "idx"))

        q = Query('"king henry"').paging(0, 1)
        q.highlight(fields=("play", "txt"), tags=("<b>", "</b>"))
        q.summarize("txt")

        if expects_resp2_shape(client) or expects_unified_shape(client):
            doc = sorted(client.ft().search(q).docs)[0]
            assert "<b>Henry</b> IV" == doc.play
            assert (
                "ACT I SCENE I. London. The palace. Enter <b>KING</b> <b>HENRY</b>, LORD JOHN OF LANCASTER, the EARL of WESTMORELAND, SIR... "  # noqa
                == doc.txt
            )

            q = Query('"king henry"').paging(0, 1).summarize().highlight()

            doc = sorted(client.ft().search(q).docs)[0]
            assert "<b>Henry</b> ... " == doc.play
            assert (
                "ACT I SCENE I. London. The palace. Enter <b>KING</b> <b>HENRY</b>, LORD JOHN OF LANCASTER, the EARL of WESTMORELAND, SIR... "  # noqa
                == doc.txt
            )
        elif expects_resp3_shape(client):
            doc = sorted(client.ft().search(q)["results"])[0]
            assert "<b>Henry</b> IV" == doc["extra_attributes"]["play"]
            assert (
                "ACT I SCENE I. London. The palace. Enter <b>KING</b> <b>HENRY</b>, LORD JOHN OF LANCASTER, the EARL of WESTMORELAND, SIR... "  # noqa
                == doc["extra_attributes"]["txt"]
            )

            q = Query('"king henry"').paging(0, 1).summarize().highlight()

            doc = sorted(client.ft().search(q)["results"])[0]
            assert "<b>Henry</b> ... " == doc["extra_attributes"]["play"]
            assert (
                "ACT I SCENE I. London. The palace. Enter <b>KING</b> <b>HENRY</b>, LORD JOHN OF LANCASTER, the EARL of WESTMORELAND, SIR... "  # noqa
                == doc["extra_attributes"]["txt"]
            )

    @pytest.mark.redismod
    @skip_ifmodversion_lt("2.0.0", "search")
    def test_alias(self, client):
        index1 = self.getClient(client)
        index2 = self.getClient(client)

        def1 = IndexDefinition(prefix=["index1:"])
        def2 = IndexDefinition(prefix=["index2:"])

        ftindex1 = index1.ft("testAlias")
        ftindex2 = index2.ft("testAlias2")
        ftindex1.create_index((TextField("name"),), definition=def1)
        ftindex2.create_index((TextField("name"),), definition=def2)

        index1.hset("index1:lonestar", mapping={"name": "lonestar"})
        index2.hset("index2:yogurt", mapping={"name": "yogurt"})

        if expects_resp2_shape(client) or expects_unified_shape(client):
            res = ftindex1.search("*").docs[0]
            assert "index1:lonestar" == res.id

            # create alias and check for results
            ftindex1.aliasadd("spaceballs")
            alias_client = self.getClient(client).ft("spaceballs")
            res = alias_client.search("*").docs[0]
            assert "index1:lonestar" == res.id

            # Throw an exception when trying to add an alias that already exists
            with pytest.raises(Exception):
                ftindex2.aliasadd("spaceballs")

            # update alias and ensure new results
            ftindex2.aliasupdate("spaceballs")
            alias_client2 = self.getClient(client).ft("spaceballs")

            res = alias_client2.search("*").docs[0]
            assert "index2:yogurt" == res.id
        elif expects_resp3_shape(client):
            res = ftindex1.search("*")["results"][0]
            assert "index1:lonestar" == res["id"]

            # create alias and check for results
            ftindex1.aliasadd("spaceballs")
            alias_client = self.getClient(client).ft("spaceballs")
            res = alias_client.search("*")["results"][0]
            assert "index1:lonestar" == res["id"]

            # Throw an exception when trying to add an alias that already exists
            with pytest.raises(Exception):
                ftindex2.aliasadd("spaceballs")

            # update alias and ensure new results
            ftindex2.aliasupdate("spaceballs")
            alias_client2 = self.getClient(client).ft("spaceballs")

            res = alias_client2.search("*")["results"][0]
            assert "index2:yogurt" == res["id"]

        ftindex2.aliasdel("spaceballs")
        with pytest.raises(Exception):
            alias_client2.search("*").docs[0]

    @pytest.mark.redismod
    @pytest.mark.xfail(strict=False)
    def test_alias_basic(self, client):
        # Creating a client with one index
        index1 = self.getClient(client).ft("testAlias")

        index1.create_index((TextField("txt"),))
        index1.client.hset("doc1", mapping={"txt": "text goes here"})

        index2 = self.getClient(client).ft("testAlias2")
        index2.create_index((TextField("txt"),))
        index2.client.hset("doc2", mapping={"txt": "text goes here"})

        # add the actual alias and check
        index1.aliasadd("myalias")
        alias_client = self.getClient(client).ft("myalias")
        if expects_resp2_shape(client) or expects_unified_shape(client):
            res = sorted(alias_client.search("*").docs, key=lambda x: x.id)
            assert "doc1" == res[0].id

            # Throw an exception when trying to add an alias that already exists
            with pytest.raises(Exception):
                index2.aliasadd("myalias")

            # update the alias and ensure we get doc2
            index2.aliasupdate("myalias")
            alias_client2 = self.getClient(client).ft("myalias")
            res = sorted(alias_client2.search("*").docs, key=lambda x: x.id)
            assert "doc1" == res[0].id
        elif expects_resp3_shape(client):
            res = sorted(alias_client.search("*")["results"], key=lambda x: x["id"])
            assert "doc1" == res[0]["id"]

            # Throw an exception when trying to add an alias that already exists
            with pytest.raises(Exception):
                index2.aliasadd("myalias")

            # update the alias and ensure we get doc2
            index2.aliasupdate("myalias")
            alias_client2 = self.getClient(client).ft("myalias")
            res = sorted(alias_client2.search("*")["results"], key=lambda x: x["id"])
            assert "doc1" == res[0]["id"]

        # delete the alias and expect an error if we try to query again
        index2.aliasdel("myalias")
        with pytest.raises(Exception):
            _ = alias_client2.search("*").docs[0]

    @pytest.mark.redismod
    @skip_if_server_version_lt("8.9.0")
    def test_aliaslist(self, client):
        index = client.ft("aliaslistidx")
        index.create_index((TextField("txt"),))

        # An existing index with no aliases returns an empty set, not an error.
        assert index.aliaslist() == set()

        # Aliases are returned as an unordered collection.
        index.aliasadd("alias1")
        index.aliasadd("alias2")
        aliases = index.aliaslist()
        assert isinstance(aliases, set)
        assert aliases == {"alias1", "alias2"}

        # FT.ALIASUPDATE moving an alias to another index removes it from the
        # previous index's listing.
        index2 = client.ft("aliaslistidx2")
        index2.create_index((TextField("txt"),))
        index2.aliasupdate("alias1")
        assert index.aliaslist() == {"alias2"}
        assert index2.aliaslist() == {"alias1"}

        # FT.ALIASDEL removes the alias from the listing.
        index.aliasdel("alias2")
        assert index.aliaslist() == set()

        # A missing index and an alias name supplied as the index both fail
        # with the index-not-found error; the client must not resolve aliases.
        with pytest.raises(redis.ResponseError):
            client.ft("nonexistent_aliaslist_index").aliaslist()
        with pytest.raises(redis.ResponseError):
            client.ft("alias1").aliaslist()

    @pytest.mark.redismod
    @skip_if_server_version_lt("8.9.0")
    @skip_if_redis_enterprise()
    def test_aliaslist_decode_responses_false(self, request, stack_url):
        # ``decode_responses`` is not part of the CI protocol/legacy matrix,
        # so pin only that here and let the harness supply the protocol x
        # legacy combinations (as it does for ``test_aliaslist`` above).
        client = _get_client(
            redis.Redis, request, decode_responses=False, from_url=stack_url
        )
        index = client.ft("aliaslistbytesidx")
        index.create_index((TextField("txt"),))

        # ``aliasadd`` accepts a ``KeyT`` (str | bytes | memoryview); mix a
        # ``str`` and a ``bytes`` alias to exercise the widened input type.
        index.aliasadd("alias1")
        index.aliasadd(b"alias2")

        # Alias names are user data, so with ``decode_responses=False`` they
        # are returned as ``bytes`` (honoring the flag, like FT.TAGVALS /
        # FT.DICTDUMP) rather than being force-decoded to ``str``.
        aliases = index.aliaslist()
        assert isinstance(aliases, set)
        assert aliases == {b"alias1", b"alias2"}

    @pytest.mark.redismod
    def test_textfield_sortable_nostem(self, client):
        # Creating the index definition with sortable and no_stem
        client.ft().create_index((TextField("txt", sortable=True, no_stem=True),))

        # Now get the index info to confirm its contents
        response = client.ft().info()
        if expects_resp2_shape(client):
            assert "SORTABLE" in response["attributes"][0]
            assert "NOSTEM" in response["attributes"][0]
        elif expects_resp3_shape(client) or expects_unified_shape(client):
            assert "SORTABLE" in response["attributes"][0]["flags"]
            assert "NOSTEM" in response["attributes"][0]["flags"]

    @pytest.mark.redismod
    def test_alter_schema_add(self, client):
        # Creating the index definition and schema
        client.ft().create_index(TextField("title"))

        # Using alter to add a field
        client.ft().alter_schema_add(TextField("body"))

        # Indexing a document
        client.hset(
            "doc1",
            mapping={"title": "MyTitle", "body": "Some content only in the body"},
        )

        # Searching with parameter only in the body (the added field)
        q = Query("only in the body")

        # Ensure we find the result searching on the added body field
        res = client.ft().search(q)
        if expects_resp2_shape(client) or expects_unified_shape(client):
            assert 1 == res.total
        elif expects_resp3_shape(client):
            assert 1 == res["total_results"]

    @pytest.mark.redismod
    def test_spell_check(self, client):
        client.ft().create_index((TextField("f1"), TextField("f2")))

        client.hset(
            "doc1", mapping={"f1": "some valid content", "f2": "this is sample text"}
        )
        client.hset("doc2", mapping={"f1": "very important", "f2": "lorem ipsum"})
        self.waitForIndex(client, getattr(client.ft(), "index_name", "idx"))

        if expects_resp2_shape(client) or expects_unified_shape(client):
            # test spellcheck
            res = client.ft().spellcheck("impornant")
            assert "important" == res["impornant"][0]["suggestion"]

            res = client.ft().spellcheck("contnt")
            assert "content" == res["contnt"][0]["suggestion"]

            # test spellcheck with Levenshtein distance
            res = client.ft().spellcheck("vlis")
            assert res == {}
            res = client.ft().spellcheck("vlis", distance=2)
            assert "valid" == res["vlis"][0]["suggestion"]

            # test spellcheck include
            client.ft().dict_add("dict", "lore", "lorem", "lorm")
            res = client.ft().spellcheck("lorm", include="dict")
            assert len(res["lorm"]) == 3
            assert (
                res["lorm"][0]["suggestion"],
                res["lorm"][1]["suggestion"],
                res["lorm"][2]["suggestion"],
            ) == ("lorem", "lore", "lorm")
            assert (res["lorm"][0]["score"], res["lorm"][1]["score"]) == ("0.5", "0")

            # test spellcheck exclude
            res = client.ft().spellcheck("lorm", exclude="dict")
            assert res == {}
        elif expects_resp3_shape(client):
            # test spellcheck
            res = client.ft().spellcheck("impornant")
            assert "important" in res["results"]["impornant"][0].keys()

            res = client.ft().spellcheck("contnt")
            assert "content" in res["results"]["contnt"][0].keys()

            # test spellcheck with Levenshtein distance
            res = client.ft().spellcheck("vlis")
            assert res == {"results": {"vlis": []}}
            res = client.ft().spellcheck("vlis", distance=2)
            assert "valid" in res["results"]["vlis"][0].keys()

            # test spellcheck include
            client.ft().dict_add("dict", "lore", "lorem", "lorm")
            res = client.ft().spellcheck("lorm", include="dict")
            assert len(res["results"]["lorm"]) == 3
            assert "lorem" in res["results"]["lorm"][0].keys()
            assert "lore" in res["results"]["lorm"][1].keys()
            assert "lorm" in res["results"]["lorm"][2].keys()
            assert (
                res["results"]["lorm"][0]["lorem"],
                res["results"]["lorm"][1]["lore"],
            ) == (0.5, 0)

            # test spellcheck exclude
            res = client.ft().spellcheck("lorm", exclude="dict")
            assert res == {"results": {}}

    @pytest.mark.redismod
    def test_dict_operations(self, client):
        client.ft().create_index((TextField("f1"), TextField("f2")))
        # Add three items
        res = client.ft().dict_add("custom_dict", "item1", "item2", "item3")
        assert 3 == res

        # Remove one item
        res = client.ft().dict_del("custom_dict", "item2")
        assert 1 == res

        # Dump dict and inspect content
        res = client.ft().dict_dump("custom_dict")
        assert res == ["item1", "item3"]

        # Remove rest of the items before reload
        client.ft().dict_del("custom_dict", *res)

    @pytest.mark.redismod
    def test_phonetic_matcher(self, client):
        client.ft().create_index((TextField("name"),))
        client.hset("doc1", mapping={"name": "Jon"})
        client.hset("doc2", mapping={"name": "John"})

        res = client.ft().search(Query("Jon"))
        if expects_resp2_shape(client) or expects_unified_shape(client):
            assert 1 == len(res.docs)
            assert "Jon" == res.docs[0].name
        elif expects_resp3_shape(client):
            assert 1 == res["total_results"]
            assert "Jon" == res["results"][0]["extra_attributes"]["name"]

        # Drop and create index with phonetic matcher
        client.flushdb()

        client.ft().create_index((TextField("name", phonetic_matcher="dm:en"),))
        client.hset("doc1", mapping={"name": "Jon"})
        client.hset("doc2", mapping={"name": "John"})

        res = client.ft().search(Query("Jon"))
        if expects_resp2_shape(client) or expects_unified_shape(client):
            assert 2 == len(res.docs)
            assert ["John", "Jon"] == sorted(d.name for d in res.docs)
        elif expects_resp3_shape(client):
            assert 2 == res["total_results"]
            assert ["John", "Jon"] == sorted(
                d["extra_attributes"]["name"] for d in res["results"]
            )

    @pytest.mark.redismod
    def test_get(self, client):
        client.ft().create_index((TextField("f1"), TextField("f2")))

        assert [None] == client.ft().get("doc1")
        assert [None, None] == client.ft().get("doc2", "doc1")

        client.hset(
            "doc1",
            mapping={"f1": "some valid content dd1", "f2": "this is sample text f1"},
        )
        client.hset(
            "doc2",
            mapping={"f1": "some valid content dd2", "f2": "this is sample text f2"},
        )

        assert [
            ["f1", "some valid content dd2", "f2", "this is sample text f2"]
        ] == client.ft().get("doc2")
        assert [
            ["f1", "some valid content dd1", "f2", "this is sample text f1"],
            ["f1", "some valid content dd2", "f2", "this is sample text f2"],
        ] == client.ft().get("doc1", "doc2")

    @pytest.mark.redismod
    @skip_ifmodversion_lt("2.0.0", "search")
    def test_index_definition(self, client):
        """
        Create definition and test its args
        """
        with pytest.raises(RuntimeError):
            IndexDefinition(prefix=["hset:", "henry"], index_type="json")

        definition = IndexDefinition(
            prefix=["hset:", "henry"],
            filter="@f1==32",
            language="English",
            language_field="play",
            score_field="chapter",
            score=0.5,
            payload_field="txt",
            index_type=IndexType.JSON,
        )

        assert [
            "ON",
            "JSON",
            "PREFIX",
            2,
            "hset:",
            "henry",
            "FILTER",
            "@f1==32",
            "LANGUAGE_FIELD",
            "play",
            "LANGUAGE",
            "English",
            "SCORE_FIELD",
            "chapter",
            "SCORE",
            0.5,
            "PAYLOAD_FIELD",
            "txt",
        ] == definition.args

        self.createIndex(client.ft(), num_docs=500, definition=definition)

    @pytest.mark.fixed_client
    @pytest.mark.parametrize(
        "prefix",
        [
            "my:prefix:",
            b"my:prefix:",
            bytearray(b"my:prefix:"),
            memoryview(b"my:prefix:"),
        ],
    )
    def test_index_definition_scalar_prefix(self, prefix):
        definition = IndexDefinition(prefix=prefix, score=None)

        assert ["PREFIX", 1, prefix] == definition.args

    @pytest.mark.fixed_client
    @pytest.mark.parametrize("prefix", [["hset:", "henry"], ("hset:", "henry")])
    def test_index_definition_prefix_list_or_tuple(self, prefix):
        definition = IndexDefinition(prefix=prefix, score=None)

        assert ["PREFIX", 2, "hset:", "henry"] == definition.args

    @pytest.mark.redismod
    @pytest.mark.onlynoncluster
    @skip_if_redis_enterprise()
    @skip_if_server_version_gte("7.9.0")
    def test_expire(self, client):
        client.ft().create_index((TextField("txt", sortable=True),), temporary=4)
        ttl = client.execute_command("ft.debug", "TTL", "idx")
        assert ttl > 2

        # the TTL has seconds granularity, so one second is enough to see it drop
        time.sleep(1.1)
        assert client.execute_command("ft.debug", "TTL", "idx") < ttl

    @pytest.mark.redismod
    def test_skip_initial_scan(self, client):
        client.hset("doc1", "foo", "bar")
        q = Query("@foo:bar")

        client.ft().create_index((TextField("foo"),), skip_initial_scan=True)
        res = client.ft().search(q)
        if expects_resp2_shape(client) or expects_unified_shape(client):
            assert res.total == 0
        elif expects_resp3_shape(client):
            assert res["total_results"] == 0

    @pytest.mark.redismod
    def test_summarize_disabled_nooffset(self, client):
        client.ft().create_index((TextField("txt"),), no_term_offsets=True)
        client.hset("doc1", mapping={"txt": "foo bar"})
        with pytest.raises(Exception):
            client.ft().search(Query("foo").summarize(fields=["txt"]))

    @pytest.mark.redismod
    def test_summarize_disabled_nohl(self, client):
        client.ft().create_index((TextField("txt"),), no_highlight=True)
        client.hset("doc1", mapping={"txt": "foo bar"})
        with pytest.raises(Exception):
            client.ft().search(Query("foo").summarize(fields=["txt"]))

    @pytest.mark.redismod
    def test_max_text_fields(self, client):
        # Creating the index definition
        client.ft().create_index((TextField("f0"),))
        for x in range(1, 32):
            client.ft().alter_schema_add((TextField(f"f{x}"),))

        # Should be too many indexes
        with pytest.raises(redis.ResponseError):
            client.ft().alter_schema_add((TextField(f"f{x}"),))

        client.ft().dropindex()
        # Creating the index definition
        client.ft().create_index((TextField("f0"),), max_text_fields=True)
        # Fill the index with fields
        for x in range(1, 50):
            client.ft().alter_schema_add((TextField(f"f{x}"),))

    @pytest.mark.redismod
    @skip_ifmodversion_lt("2.0.0", "search")
    def test_create_client_definition(self, client):
        """
        Create definition with no index type provided,
        and use hset to test the client definition (the default is HASH).
        """
        definition = IndexDefinition(prefix=["hset:", "henry"])
        self.createIndex(client.ft(), num_docs=500, definition=definition)

        info = client.ft().info()
        assert 494 == int(info["num_docs"])

        client.ft().client.hset("hset:1", "f1", "v1")
        info = client.ft().info()
        assert 495 == int(info["num_docs"])

    @pytest.mark.redismod
    @skip_ifmodversion_lt("2.0.0", "search")
    def test_create_client_definition_hash(self, client):
        """
        IndexType.HASH sends an explicit ON HASH, which the server accepts and
        indexes like the default index type; hset tests the client definition.
        """
        definition = IndexDefinition(
            prefix=["hset:", "henry"], index_type=IndexType.HASH, score=None
        )
        assert definition.args == ["ON", "HASH", "PREFIX", 2, "hset:", "henry"]
        default = IndexDefinition(prefix=["hset:", "henry"], score=None)
        assert default.args == ["PREFIX", 2, "hset:", "henry"]

        client.ft().create_index((TextField("f1"),), definition=definition)
        self.waitForIndex(client, getattr(client.ft(), "index_name", "idx"))

        client.hset("hset:1", "f1", "v1")
        # outside the definition's prefixes, so not indexed
        client.hset("other:1", "f1", "v1")
        info = client.ft().info()
        assert 1 == int(info["num_docs"])

    @pytest.mark.redismod
    @skip_ifmodversion_lt("2.2.0", "search")
    def test_create_client_definition_json(self, client):
        """
        Create definition with IndexType.JSON as index type (ON JSON),
        and use json client to test it.
        """
        definition = IndexDefinition(prefix=["king:"], index_type=IndexType.JSON)
        client.ft().create_index((TextField("$.name"),), definition=definition)

        client.json().set("king:1", Path.root_path(), {"name": "henry"})
        client.json().set("king:2", Path.root_path(), {"name": "james"})

        res = client.ft().search("henry")
        if expects_resp2_shape(client) or expects_unified_shape(client):
            assert res.docs[0].id == "king:1"
            assert res.docs[0].payload is None
            assert res.docs[0].json == '{"name":"henry"}'
            assert res.total == 1
        elif expects_resp3_shape(client):
            assert res["results"][0]["id"] == "king:1"
            assert res["results"][0]["extra_attributes"]["$"] == '{"name":"henry"}'
            assert res["total_results"] == 1

    @pytest.mark.redismod
    @skip_ifmodversion_lt("2.2.0", "search")
    def test_fields_as_name(self, client):
        # create index
        SCHEMA = (
            TextField("$.name", sortable=True, as_name="name"),
            NumericField("$.age", as_name="just_a_number"),
        )
        definition = IndexDefinition(index_type=IndexType.JSON)
        client.ft().create_index(SCHEMA, definition=definition)

        # insert json data
        res = client.json().set("doc:1", Path.root_path(), {"name": "Jon", "age": 25})
        assert res

        res = client.ft().search(Query("Jon").return_fields("name", "just_a_number"))
        if expects_resp2_shape(client) or expects_unified_shape(client):
            assert 1 == len(res.docs)
            assert "doc:1" == res.docs[0].id
            assert "Jon" == res.docs[0].name
            assert "25" == res.docs[0].just_a_number
        elif expects_resp3_shape(client):
            assert 1 == len(res["results"])
            assert "doc:1" == res["results"][0]["id"]
            assert "Jon" == res["results"][0]["extra_attributes"]["name"]
            assert "25" == res["results"][0]["extra_attributes"]["just_a_number"]

    @pytest.mark.redismod
    def test_casesensitive(self, client):
        # create index
        SCHEMA = (TagField("t", case_sensitive=False),)
        client.ft().create_index(SCHEMA)
        client.ft().client.hset("1", "t", "HELLO")
        client.ft().client.hset("2", "t", "hello")

        res = client.ft().search("@t:{HELLO}")

        if expects_resp2_shape(client) or expects_unified_shape(client):
            assert 2 == len(res.docs)
            assert "1" == res.docs[0].id
            assert "2" == res.docs[1].id
        elif expects_resp3_shape(client):
            assert 2 == len(res["results"])
            assert "1" == res["results"][0]["id"]
            assert "2" == res["results"][1]["id"]

        # create casesensitive index
        client.ft().dropindex()
        SCHEMA = (TagField("t", case_sensitive=True),)
        client.ft().create_index(SCHEMA)
        self.waitForIndex(client, getattr(client.ft(), "index_name", "idx"))

        res = client.ft().search("@t:{HELLO}")
        if expects_resp2_shape(client) or expects_unified_shape(client):
            assert 1 == len(res.docs)
            assert "1" == res.docs[0].id
        elif expects_resp3_shape(client):
            assert 1 == len(res["results"])
            assert "1" == res["results"][0]["id"]

    @pytest.mark.redismod
    @skip_ifmodversion_lt("2.2.0", "search")
    def test_search_return_fields(self, client):
        res = client.json().set(
            "doc:1",
            Path.root_path(),
            {"t": "riceratops", "t2": "telmatosaurus", "n": 9072, "flt": 97.2},
        )
        assert res

        # create index on
        definition = IndexDefinition(index_type=IndexType.JSON)
        SCHEMA = (TextField("$.t"), NumericField("$.flt"))
        client.ft().create_index(SCHEMA, definition=definition)
        self.waitForIndex(client, getattr(client.ft(), "index_name", "idx"))

        if expects_resp2_shape(client) or expects_unified_shape(client):
            total = (
                client.ft().search(Query("*").return_field("$.t", as_field="txt")).docs
            )
            assert 1 == len(total)
            assert "doc:1" == total[0].id
            assert "riceratops" == total[0].txt

            total = (
                client.ft().search(Query("*").return_field("$.t2", as_field="txt")).docs
            )
            assert 1 == len(total)
            assert "doc:1" == total[0].id
            assert "telmatosaurus" == total[0].txt
        elif expects_resp3_shape(client):
            total = client.ft().search(Query("*").return_field("$.t", as_field="txt"))
            assert 1 == len(total["results"])
            assert "doc:1" == total["results"][0]["id"]
            assert "riceratops" == total["results"][0]["extra_attributes"]["txt"]

            total = client.ft().search(Query("*").return_field("$.t2", as_field="txt"))
            assert 1 == len(total["results"])
            assert "doc:1" == total["results"][0]["id"]
            assert "telmatosaurus" == total["results"][0]["extra_attributes"]["txt"]

    @pytest.mark.redismod
    def test_binary_and_text_fields(self, client):
        fake_vec = np.array([0.1, 0.2, 0.3, 0.4], dtype=np.float32)

        index_name = "mixed_index"
        mixed_data = {"first_name": "🐍python", "vector_emb": fake_vec.tobytes()}
        client.hset(f"{index_name}:1", mapping=mixed_data)

        schema = (
            TagField("first_name"),
            VectorField(
                "embeddings_bio",
                algorithm="HNSW",
                attributes={
                    "TYPE": "FLOAT32",
                    "DIM": 4,
                    "DISTANCE_METRIC": "COSINE",
                },
            ),
        )

        client.ft(index_name).create_index(
            fields=schema,
            definition=IndexDefinition(
                prefix=[f"{index_name}:"], index_type=IndexType.HASH
            ),
        )

        self.waitForIndex(client, index_name)

        query = (
            Query("*")
            .return_field("vector_emb", decode_field=False)
            .return_field("first_name")
        )
        result = client.ft(index_name).search(query=query, query_params={})

        if expects_resp3_shape(client):
            # protocol=3 + legacy_responses=True returns the native RESP3 dict;
            # text fields are decoded while the binary field is kept as bytes.
            results = result["results"]
            assert len(results) > 0, f"Returned search results are empty: {result}"
            attributes = results[0]["extra_attributes"]
        else:
            docs = result.docs
            assert len(docs) > 0, f"Returned search results are empty: {result}"
            attributes = docs[0]

        decoded_vec_from_search_results = np.frombuffer(
            attributes["vector_emb"], dtype=np.float32
        )

        assert np.array_equal(decoded_vec_from_search_results, fake_vec), (
            "The vectors are not equal"
        )

        assert attributes["first_name"] == mixed_data["first_name"], (
            "The text field is not decoded correctly"
        )

    @pytest.mark.fixed_client
    def test_return_field_encoding_is_keyed_by_alias(self):
        # The server returns an aliased field under its alias, so that is the
        # name the response is looked up under when decoding.
        query = Query("*").return_field(
            "$.vector_emb", as_field="vector_emb", decode_field=False
        )
        assert query._return_fields_decode_as == {"vector_emb": None}
        assert query.get_args()[:4] == ["*", "RETURN", 3, "$.vector_emb"]

        raw = b"\x00\x01\x82\xff"
        res = Result(
            [1, "doc:1", ["vector_emb", raw]],
            True,
            field_encodings=query._return_fields_decode_as,
        )
        assert res.docs[0].vector_emb == raw

        res = Result.from_resp3(
            {
                "total_results": 1,
                "results": [{"id": "doc:1", "extra_attributes": {"vector_emb": raw}}],
            },
            field_encodings=query._return_fields_decode_as,
        )
        assert res.docs[0].vector_emb == raw

    @pytest.mark.redismod
    def test_binary_field_survives_alias_with_decode_field_false(self, client):
        # Regression test: return_field() used to key the decode-encoding map
        # by the field identifier instead of the AS alias, so an aliased
        # binary field was decoded (and silently truncated) instead of being
        # kept raw. Covered against a real server across the protocol /
        # legacy_responses matrix that CI runs this suite under.
        fake_vec = np.array([0.1, 0.2, 0.3, 0.4], dtype=np.float32)

        index_name = "aliased_binary_index"
        client.hset(f"{index_name}:1", mapping={"vector_emb": fake_vec.tobytes()})

        client.ft(index_name).create_index(
            fields=(
                VectorField(
                    "vector_emb",
                    algorithm="HNSW",
                    attributes={
                        "TYPE": "FLOAT32",
                        "DIM": 4,
                        "DISTANCE_METRIC": "COSINE",
                    },
                ),
            ),
            definition=IndexDefinition(
                prefix=[f"{index_name}:"], index_type=IndexType.HASH
            ),
        )
        self.waitForIndex(client, index_name)

        query = Query("*").return_field(
            "vector_emb", as_field="emb", decode_field=False
        )
        result = client.ft(index_name).search(query=query, query_params={})

        if expects_resp3_shape(client):
            results = result["results"]
            assert len(results) > 0, f"Returned search results are empty: {result}"
            attributes = results[0]["extra_attributes"]
        else:
            docs = result.docs
            assert len(docs) > 0, f"Returned search results are empty: {result}"
            attributes = docs[0]

        decoded_vec = np.frombuffer(attributes["emb"], dtype=np.float32)
        assert np.array_equal(decoded_vec, fake_vec), (
            "The aliased binary field was not preserved intact"
        )

    @pytest.mark.redismod
    def test_synupdate(self, client):
        definition = IndexDefinition(index_type=IndexType.HASH)
        client.ft().create_index(
            (TextField("title"), TextField("body")), definition=definition
        )

        client.ft().synupdate("id1", True, "boy", "child", "offspring")
        client.hset("doc1", mapping={"title": "he is a baby", "body": "this is a test"})

        client.ft().synupdate("id1", True, "baby")
        client.hset(
            "doc2", mapping={"title": "he is another baby", "body": "another test"}
        )

        res = client.ft().search(Query("child").expander("SYNONYM"))
        if expects_resp2_shape(client) or expects_unified_shape(client):
            assert res.docs[0].id == "doc2"
            assert res.docs[0].title == "he is another baby"
            assert res.docs[0].body == "another test"
        elif expects_resp3_shape(client):
            assert res["results"][0]["id"] == "doc2"
            assert (
                res["results"][0]["extra_attributes"]["title"] == "he is another baby"
            )
            assert res["results"][0]["extra_attributes"]["body"] == "another test"

    @pytest.mark.redismod
    def test_syndump(self, client):
        definition = IndexDefinition(index_type=IndexType.HASH)
        client.ft().create_index(
            (TextField("title"), TextField("body")), definition=definition
        )

        client.ft().synupdate("id1", False, "boy", "child", "offspring")
        client.ft().synupdate("id2", False, "baby", "child")
        client.ft().synupdate("id3", False, "tree", "wood")
        res = client.ft().syndump()
        assert res == {
            "boy": ["id1"],
            "tree": ["id3"],
            "wood": ["id3"],
            "child": ["id1", "id2"],
            "baby": ["id2"],
            "offspring": ["id1"],
        }

    @pytest.mark.redismod
    def test_expire_while_search(self, client: redis.Redis):
        client.ft().create_index((TextField("txt"),))
        client.hset("hset:1", "txt", "a")
        client.hset("hset:2", "txt", "b")
        client.hset("hset:3", "txt", "c")
        # Keep searching until the expiration window of hset:2 has passed. The
        # loop is bounded by elapsed time rather than by a fixed iteration count,
        # so it covers the same window without depending on how long a single
        # search round trip takes.
        if expects_resp2_shape(client) or expects_unified_shape(client):
            assert 3 == client.ft().search(Query("*")).total
            client.pexpire("hset:2", 300)
            deadline = time.monotonic() + 0.5
            while time.monotonic() < deadline:
                client.ft().search(Query("*")).docs[1]
            # wait for the expired document to leave the index
            deadline = time.monotonic() + 5
            while client.ft().search(Query("*")).total != 2:
                assert time.monotonic() < deadline, "hset:2 was not expired"
                time.sleep(0.01)
        elif expects_resp3_shape(client):
            assert 3 == client.ft().search(Query("*"))["total_results"]
            client.pexpire("hset:2", 300)
            deadline = time.monotonic() + 0.5
            while time.monotonic() < deadline:
                client.ft().search(Query("*"))["results"][1]
            # wait for the expired document to leave the index
            deadline = time.monotonic() + 5
            while client.ft().search(Query("*"))["total_results"] != 2:
                assert time.monotonic() < deadline, "hset:2 was not expired"
                time.sleep(0.01)

    @pytest.mark.redismod
    @pytest.mark.experimental
    def test_withsuffixtrie(self, client: redis.Redis):
        # create index
        assert client.ft().create_index((TextField("txt"),))
        self.waitForIndex(client, getattr(client.ft(), "index_name", "idx"))
        if expects_resp2_shape(client):
            info = client.ft().info()
            assert "WITHSUFFIXTRIE" not in info["attributes"][0]
            assert client.ft().dropindex()

            # create withsuffixtrie index (text fields)
            assert client.ft().create_index(TextField("t", withsuffixtrie=True))
            self.waitForIndex(client, getattr(client.ft(), "index_name", "idx"))
            info = client.ft().info()
            assert "WITHSUFFIXTRIE" in info["attributes"][0]
            assert client.ft().dropindex()

            # create withsuffixtrie index (tag field)
            assert client.ft().create_index(TagField("t", withsuffixtrie=True))
            self.waitForIndex(client, getattr(client.ft(), "index_name", "idx"))
            info = client.ft().info()
            assert "WITHSUFFIXTRIE" in info["attributes"][0]
        elif expects_resp3_shape(client) or expects_unified_shape(client):
            info = client.ft().info()
            assert "WITHSUFFIXTRIE" not in info["attributes"][0]["flags"]
            assert client.ft().dropindex()

            # create withsuffixtrie index (text fields)
            assert client.ft().create_index(TextField("t", withsuffixtrie=True))
            self.waitForIndex(client, getattr(client.ft(), "index_name", "idx"))
            info = client.ft().info()
            assert "WITHSUFFIXTRIE" in info["attributes"][0]["flags"]
            assert client.ft().dropindex()

            # create withsuffixtrie index (tag field)
            assert client.ft().create_index(TagField("t", withsuffixtrie=True))
            self.waitForIndex(client, getattr(client.ft(), "index_name", "idx"))
            info = client.ft().info()
            assert "WITHSUFFIXTRIE" in info["attributes"][0]["flags"]

    @pytest.mark.redismod
    def test_query_timeout(self, r: redis.Redis):
        q1 = Query("foo").timeout(5000)
        assert q1.get_args() == ["foo", "TIMEOUT", 5000, "DIALECT", 2, "LIMIT", 0, 10]
        q1 = Query("foo").timeout(0)
        assert q1.get_args() == ["foo", "TIMEOUT", 0, "DIALECT", 2, "LIMIT", 0, 10]
        q2 = Query("foo").timeout("not_a_number")
        with pytest.raises(redis.ResponseError):
            r.ft().search(q2)

    def _create_search_timeout_index(self, client):
        client.ft().create_index(
            (TextField("description"),),
            definition=IndexDefinition(prefix=["search-timeout-item:"]),
        )
        SearchTestsBase.waitForIndex(client, "idx")
        for i in range(5):
            client.hset(f"search-timeout-item:{i}", "description", "red shoes")

    def _search_with_forced_timeout(self, client, query):
        # Whether a real query outlives its TIMEOUT depends on the dataset and
        # on how fast the server is, so force the timeout instead:
        # ``_FT.DEBUG FT.SEARCH ... TIMEOUT_AFTER_N 0`` runs the wrapped
        # FT.SEARCH and has it time out before the first result. search()
        # still builds the arguments and parses the reply.
        ft = client.ft()
        execute_command = ft.execute_command

        def execute_with_forced_timeout(*args, **options):
            return execute_command(
                "_FT.DEBUG",
                *args,
                "TIMEOUT_AFTER_N",
                0,
                "DEBUG_PARAMS_COUNT",
                2,
                **options,
            )

        ft.execute_command = execute_with_forced_timeout
        return ft.search(query)

    @pytest.mark.redismod
    # The Redis Enterprise skip and the 8.0.0 gate come from the _FT.DEBUG
    # timeout hook, not from TIMEOUT itself: Redis Enterprise does not expose
    # the hook, and the 7.x stack images predate it.
    @skip_if_redis_enterprise()
    @skip_if_server_version_lt("8.0.0")
    def test_search_query_with_timeout(self, client):
        self._create_search_timeout_index(client)

        query = Query("red").timeout(1)
        res = self._search_with_forced_timeout(client, query)

        if expects_resp3_shape(client):
            warnings = res.get("warning", [])
            total = res["total_results"]
        else:
            warnings = res.warnings
            total = res.total

        # A timed-out search still returns a well-formed (partial) result.
        assert isinstance(total, int) and total >= 0

        if int(get_protocol_version(client)) == 3:
            # Only the RESP3 wire carries the server timeout warning.
            assert any(
                "Timeout limit was reached" in safe_str(warning) for warning in warnings
            )
        else:
            # The RESP2 wire does not carry the warning field on FT.SEARCH.
            assert warnings == []

    @pytest.mark.redismod
    # The Redis Enterprise skip comes from the _FT.DEBUG timeout hook, which
    # Redis Enterprise does not expose, not from the fail policy itself.
    @skip_if_redis_enterprise()
    @skip_if_server_version_lt("8.9.0")
    def test_search_query_with_timeout_fail_policy(self, client):
        self._create_search_timeout_index(client)

        query = Query("red").timeout(1)

        # ``search-on-timeout`` controls whether a timed-out query returns the
        # partial results collected so far (``return``, the default) or fails
        # the command (``fail``). The server refuses TIMEOUT_AFTER_N with
        # ``fail`` while search worker threads are enabled, so turn them off.
        # Capture the original values so they are always restored, even if
        # the assertion below raises.
        original = client.config_get("search-on-timeout")["search-on-timeout"]
        original_workers = client.config_get("search-workers")["search-workers"]
        try:
            assert client.config_set("search-workers", 0)
            assert client.config_set("search-on-timeout", "fail")
            # With the ``fail`` policy the server aborts the timed-out search
            # instead of returning partial results.
            with pytest.raises(redis.ResponseError, match="Timeout limit was reached"):
                self._search_with_forced_timeout(client, query)
        finally:
            client.config_set("search-on-timeout", original)
            client.config_set("search-workers", original_workers)

    @pytest.mark.redismod
    @skip_if_server_version_lt("7.2.0")
    @skip_ifmodversion_lt("2.8.4", "search")
    def test_geoshape(self, client: redis.Redis):
        client.ft().create_index(GeoShapeField("geom", GeoShapeField.FLAT))
        self.waitForIndex(client, getattr(client.ft(), "index_name", "idx"))
        client.hset("small", "geom", "POLYGON((1 1, 1 100, 100 100, 100 1, 1 1))")
        client.hset("large", "geom", "POLYGON((1 1, 1 200, 200 200, 200 1, 1 1))")
        q1 = Query("@geom:[WITHIN $poly]").dialect(3)
        qp1 = {"poly": "POLYGON((0 0, 0 150, 150 150, 150 0, 0 0))"}
        q2 = Query("@geom:[CONTAINS $poly]").dialect(3)
        qp2 = {"poly": "POLYGON((2 2, 2 50, 50 50, 50 2, 2 2))"}
        result = client.ft().search(q1, query_params=qp1)
        _assert_search_result(client, result, ["small"])
        result = client.ft().search(q2, query_params=qp2)
        _assert_search_result(client, result, ["small", "large"])

    @pytest.mark.redismod
    @skip_if_server_version_lt("7.4.0")
    @skip_ifmodversion_lt("2.10.0", "search")
    def test_search_missing_fields(self, client):
        definition = IndexDefinition(prefix=["property:"], index_type=IndexType.HASH)

        fields = [
            TextField("title", sortable=True),
            TagField("features", index_missing=True),
            TextField("description", index_missing=True),
        ]

        client.ft().create_index(fields, definition=definition)

        # All fields present
        client.hset(
            "property:1",
            mapping={
                "title": "Luxury Villa in Malibu",
                "features": "pool,sea view,modern",
                "description": "A stunning modern villa overlooking the Pacific Ocean.",
            },
        )

        # Missing features
        client.hset(
            "property:2",
            mapping={
                "title": "Downtown Flat",
                "description": "Modern flat in central Paris with easy access to metro.",
            },
        )

        # Missing description
        client.hset(
            "property:3",
            mapping={
                "title": "Beachfront Bungalow",
                "features": "beachfront,sun deck",
            },
        )

        with pytest.raises(redis.exceptions.ResponseError):
            client.ft().search(
                Query("ismissing(@title)").return_field("id").no_content()
            )

        res = client.ft().search(
            Query("ismissing(@features)").return_field("id").no_content()
        )
        _assert_search_result(client, res, ["property:2"])

        res = client.ft().search(
            Query("-ismissing(@features)").return_field("id").no_content()
        )
        _assert_search_result(client, res, ["property:1", "property:3"])

        res = client.ft().search(
            Query("ismissing(@description)").return_field("id").no_content()
        )
        _assert_search_result(client, res, ["property:3"])

        res = client.ft().search(
            Query("-ismissing(@description)").return_field("id").no_content()
        )
        _assert_search_result(client, res, ["property:1", "property:2"])

    @pytest.mark.redismod
    @skip_if_server_version_lt("7.4.0")
    @skip_ifmodversion_lt("2.10.0", "search")
    def test_create_index_empty_or_missing_fields_with_sortable(self, client):
        definition = IndexDefinition(prefix=["property:"], index_type=IndexType.HASH)

        fields = [
            TextField("title", sortable=True, index_empty=True),
            TagField("features", index_missing=True, sortable=True),
            TextField("description", no_index=True, sortable=True),
        ]

        client.ft().create_index(fields, definition=definition)

    @pytest.mark.redismod
    @skip_if_server_version_lt("7.4.0")
    @skip_ifmodversion_lt("2.10.0", "search")
    def test_search_empty_fields(self, client):
        definition = IndexDefinition(prefix=["property:"], index_type=IndexType.HASH)

        fields = [
            TextField("title", sortable=True),
            TagField("features", index_empty=True),
            TextField("description", index_empty=True),
        ]

        client.ft().create_index(fields, definition=definition)

        # All fields present
        client.hset(
            "property:1",
            mapping={
                "title": "Luxury Villa in Malibu",
                "features": "pool,sea view,modern",
                "description": "A stunning modern villa overlooking the Pacific Ocean.",
            },
        )

        # Empty features
        client.hset(
            "property:2",
            mapping={
                "title": "Downtown Flat",
                "features": "",
                "description": "Modern flat in central Paris with easy access to metro.",
            },
        )

        # Empty description
        client.hset(
            "property:3",
            mapping={
                "title": "Beachfront Bungalow",
                "features": "beachfront,sun deck",
                "description": "",
            },
        )

        with pytest.raises(redis.exceptions.ResponseError) as e:
            client.ft().search(Query("@title:''").return_field("id").no_content())
        assert "Use `INDEXEMPTY` in field creation" in e.value.args[0]

        res = client.ft().search(
            Query("@features:{$empty}").return_field("id").no_content(),
            query_params={"empty": ""},
        )
        _assert_search_result(client, res, ["property:2"])

        res = client.ft().search(
            Query("-@features:{$empty}").return_field("id").no_content(),
            query_params={"empty": ""},
        )
        _assert_search_result(client, res, ["property:1", "property:3"])

        res = client.ft().search(
            Query("@description:''").return_field("id").no_content()
        )
        _assert_search_result(client, res, ["property:3"])

        res = client.ft().search(
            Query("-@description:''").return_field("id").no_content()
        )
        _assert_search_result(client, res, ["property:1", "property:2"])

    @pytest.mark.redismod
    @skip_if_server_version_lt("7.4.0")
    @skip_ifmodversion_lt("2.10.0", "search")
    def test_special_characters_in_fields(self, client):
        definition = IndexDefinition(prefix=["resource:"], index_type=IndexType.HASH)

        fields = [
            TagField("uuid"),
            TagField("tags", separator="|"),
            TextField("description"),
            NumericField("rating"),
        ]

        client.ft().create_index(fields, definition=definition)

        client.hset(
            "resource:1",
            mapping={
                "uuid": "123e4567-e89b-12d3-a456-426614174000",
                "tags": "finance|crypto|$btc|blockchain",
                "description": "Analysis of blockchain technologies & Bitcoin's potential.",
                "rating": 5,
            },
        )

        client.hset(
            "resource:2",
            mapping={
                "uuid": "987e6543-e21c-12d3-a456-426614174999",
                "tags": "health|well-being|fitness|new-year's-resolutions",
                "description": "Health trends for the new year, including fitness regimes.",
                "rating": 4,
            },
        )

        # no need to escape - when using params
        res = client.ft().search(
            Query("@uuid:{$uuid}"),
            query_params={"uuid": "123e4567-e89b-12d3-a456-426614174000"},
        )
        _assert_search_result(client, res, ["resource:1"])

        # with double quotes exact match no need to escape the - even without params
        res = client.ft().search(
            Query('@uuid:{"123e4567-e89b-12d3-a456-426614174000"}')
        )
        _assert_search_result(client, res, ["resource:1"])

        res = client.ft().search(Query('@tags:{"new-year\'s-resolutions"}'))
        _assert_search_result(client, res, ["resource:2"])

        # possible to search numeric fields by single value
        res = client.ft().search(Query("@rating:[4]"))
        _assert_search_result(client, res, ["resource:2"])

        # some chars still need escaping
        res = client.ft().search(Query(r"@tags:{\$btc}"))
        _assert_search_result(client, res, ["resource:1"])

    @pytest.mark.redismod
    @skip_ifmodversion_lt("2.4.3", "search")
    def test_vector_search_with_default_dialect(self, client):
        client.ft().create_index(
            (
                VectorField(
                    "v", "HNSW", {"TYPE": "FLOAT32", "DIM": 2, "DISTANCE_METRIC": "L2"}
                ),
            )
        )

        client.hset("a", "v", "aaaaaaaa")
        client.hset("b", "v", "aaaabaaa")
        client.hset("c", "v", "aaaaabaa")

        query = "*=>[KNN 2 @v $vec]"
        q = Query(query)

        assert "DIALECT" in q.get_args()
        assert 2 in q.get_args()

        res = client.ft().search(q, query_params={"vec": "aaaaaaaa"})
        if expects_resp2_shape(client) or expects_unified_shape(client):
            assert res.total == 2
        elif expects_resp3_shape(client):
            assert res["total_results"] == 2

    @pytest.mark.redismod
    @skip_ifmodversion_lt("2.4.3", "search")
    def test_search_query_with_different_dialects(self, client):
        client.ft().create_index(
            (TextField("name"), TextField("lastname")),
            definition=IndexDefinition(prefix=["test:"]),
        )

        client.hset("test:1", "name", "James")
        client.hset("test:1", "lastname", "Brown")

        # Query with default DIALECT 2
        query = "@name: James Brown"
        q = Query(query)
        res = client.ft().search(q)
        if expects_resp2_shape(client) or expects_unified_shape(client):
            assert res.total == 1
        elif expects_resp3_shape(client):
            assert res["total_results"] == 1

        # Query with explicit DIALECT 1
        query = "@name: James Brown"
        q = Query(query).dialect(1)
        res = client.ft().search(q)
        if expects_resp2_shape(client) or expects_unified_shape(client):
            assert res.total == 0
        elif expects_resp3_shape(client):
            assert res["total_results"] == 0

    @pytest.mark.redismod
    @skip_if_server_version_lt("7.9.0")
    def test_info_exposes_search_info(self, client):
        assert len(client.info("search")) > 0


class TestSearchLanguages(SearchTestsBase):
    """Functional coverage for indexing and searching text in languages other
    than English: Snowball stemming for several languages, query-time language
    selection, the Chinese (friso) tokenizer and per-document ``LANGUAGE_FIELD``
    routing."""

    @pytest.mark.redismod
    @pytest.mark.parametrize(
        "language, text, query, doc_word",
        NON_ENGLISH_STEMMING_CASES,
    )
    def test_search_non_english_stemming(self, client, language, text, query, doc_word):
        """A query for a different inflection of ``doc_word`` matches when the
        document is indexed with its own language stemmer, but not when the same
        text is indexed as English -- proving the language setting is what drives
        the stem match."""
        # Index the document under its own language.
        lang_def = IndexDefinition(prefix=["lang:"], language=language)
        client.ft("idx_lang").create_index((TextField("txt"),), definition=lang_def)
        client.hset("lang:1", mapping={"txt": text})

        # Index the same text as English as a negative control.
        en_def = IndexDefinition(prefix=["en:"], language="english")
        client.ft("idx_en").create_index((TextField("txt"),), definition=en_def)
        client.hset("en:1", mapping={"txt": text})

        self.waitForIndex(client, "idx_lang")
        self.waitForIndex(client, "idx_en")

        # Same query in both cases -- only the index language differs.
        q = Query(query).language(language).no_content()
        assert _search_total(client, client.ft("idx_lang").search(q)) == 1
        assert _search_total(client, client.ft("idx_en").search(q)) == 0

    @pytest.mark.redismod
    @skip_if_server_version_lt("8.9.0")
    def test_search_stemming_indonesian(self, client):
        """The Indonesian stemmer reduces "membaca" to its root "baca", so a
        query for the stem matches the indexed document.  The Indonesian
        stemmer is only available on newer server versions."""
        definition = IndexDefinition(prefix=["doc:"], language="indonesian")
        client.ft("idx_id").create_index((TextField("content"),), definition=definition)
        client.hset("doc:1", mapping={"content": "mereka membaca buku di perpustakaan"})
        self.waitForIndex(client, "idx_id")

        q = Query("baca").language("indonesian").no_content()
        assert _search_total(client, client.ft("idx_id").search(q)) == 1

    @pytest.mark.redismod
    def test_search_query_language(self, client):
        """``Query.language()`` controls how the *query* is stemmed.  The German
        stemmer reduces "Kindern" to "kind" (matching the indexed document's
        stem), while English analysis leaves it as "kindern" (no match).  The
        query word is deliberately absent from the document text so the match
        can only come from stemming, not from the indexed raw term."""
        definition = IndexDefinition(prefix=["doc:"], language="german")
        client.ft().create_index((TextField("txt"),), definition=definition)
        client.hset("doc:1", mapping={"txt": "Die Kinder spielen im Garten"})
        self.waitForIndex(client, "idx")

        matched = client.ft().search(Query("Kindern").language("german").no_content())
        assert _search_total(client, matched) == 1

        missed = client.ft().search(Query("Kindern").language("english").no_content())
        assert _search_total(client, missed) == 0

    @pytest.mark.redismod
    def test_search_chinese_tokenization(self, client):
        """Chinese text has no whitespace word boundaries; only the ``chinese``
        tokenizer (friso) segments it into terms so an inner term can be
        matched.  Indexed as English the text stays a single token."""
        text = "我喜欢编程"  # "I like programming"
        term = "编程"  # "programming"

        cn_def = IndexDefinition(prefix=["cn:"], language="chinese")
        client.ft("idx_cn").create_index((TextField("txt"),), definition=cn_def)
        client.hset("cn:1", mapping={"txt": text})

        en_def = IndexDefinition(prefix=["en:"], language="english")
        client.ft("idx_en").create_index((TextField("txt"),), definition=en_def)
        client.hset("en:1", mapping={"txt": text})

        self.waitForIndex(client, "idx_cn")
        self.waitForIndex(client, "idx_en")

        cn_res = client.ft("idx_cn").search(
            Query(term).language("chinese").no_content()
        )
        assert _search_total(client, cn_res) == 1

        en_res = client.ft("idx_en").search(Query(term).no_content())
        assert _search_total(client, en_res) == 0

    @pytest.mark.redismod
    def test_search_language_field(self, client):
        """``LANGUAGE_FIELD`` selects the stemmer per document from a document
        field, so the German document's "Kinder" is stemmed to "kind" while the
        English document is analysed with the English stemmer."""
        definition = IndexDefinition(prefix=["doc:"], language_field="__lang")
        client.ft().create_index((TextField("txt"),), definition=definition)
        client.hset("doc:de", mapping={"txt": "Die Kinder spielen", "__lang": "german"})
        client.hset("doc:en", mapping={"txt": "the children play", "__lang": "english"})
        self.waitForIndex(client, "idx")

        # "Kind" only reaches the German document, whose "Kinder" stems to "kind".
        res = client.ft().search(Query("Kind").language("german").no_content())
        assert _search_total(client, res) == 1
        _assert_search_result(client, res, ["doc:de"])


class TestScorers(SearchTestsBase):
    @pytest.mark.redismod
    @pytest.mark.onlynoncluster
    # NOTE(imalinovskyi): This test contains hardcoded scores valid only for RediSearch 2.8+
    @skip_ifmodversion_lt("2.8.0", "search")
    @skip_if_server_version_gte("7.9.0")
    def test_scorer(self, client):
        client.ft().create_index((TextField("description"),))

        client.hset(
            "doc1",
            mapping={"description": "The quick brown fox jumps over the lazy dog"},
        )
        client.hset(
            "doc2",
            mapping={
                "description": "Quick alice was beginning to get very tired of sitting by her quick sister on the bank, and of having nothing to do."  # noqa
            },
        )

        # default scorer is TFIDF
        if expects_resp2_shape(client) or expects_unified_shape(client):
            res = client.ft().search(Query("quick").with_scores())
            assert 1.0 == res.docs[0].score
            res = client.ft().search(Query("quick").scorer("TFIDF").with_scores())
            assert 1.0 == res.docs[0].score
            res = client.ft().search(
                Query("quick").scorer("TFIDF.DOCNORM").with_scores()
            )
            assert 0.14285714285714285 == res.docs[0].score
            res = client.ft().search(Query("quick").scorer("BM25").with_scores())
            assert 0.22471909420069797 == res.docs[0].score
            res = client.ft().search(Query("quick").scorer("DISMAX").with_scores())
            assert 2.0 == res.docs[0].score
            res = client.ft().search(Query("quick").scorer("DOCSCORE").with_scores())
            assert 1.0 == res.docs[0].score
            res = client.ft().search(Query("quick").scorer("HAMMING").with_scores())
            assert 0.0 == res.docs[0].score
        elif expects_resp3_shape(client):
            res = client.ft().search(Query("quick").with_scores())
            assert 1.0 == res["results"][0]["score"]
            res = client.ft().search(Query("quick").scorer("TFIDF").with_scores())
            assert 1.0 == res["results"][0]["score"]
            res = client.ft().search(
                Query("quick").scorer("TFIDF.DOCNORM").with_scores()
            )
            assert 0.14285714285714285 == res["results"][0]["score"]
            res = client.ft().search(Query("quick").scorer("BM25").with_scores())
            assert 0.22471909420069797 == res["results"][0]["score"]
            res = client.ft().search(Query("quick").scorer("DISMAX").with_scores())
            assert 2.0 == res["results"][0]["score"]
            res = client.ft().search(Query("quick").scorer("DOCSCORE").with_scores())
            assert 1.0 == res["results"][0]["score"]
            res = client.ft().search(Query("quick").scorer("HAMMING").with_scores())
            assert 0.0 == res["results"][0]["score"]

    @pytest.mark.redismod
    @pytest.mark.onlynoncluster
    @skip_if_server_version_lt("7.9.0")
    def test_scorer_with_new_default_scorer(self, client):
        client.ft().create_index((TextField("description"),))

        client.hset(
            "doc1",
            mapping={"description": "The quick brown fox jumps over the lazy dog"},
        )
        client.hset(
            "doc2",
            mapping={
                "description": "Quick alice was beginning to get very tired of sitting by her quick sister on the bank, and of having nothing to do."  # noqa
            },
        )

        # default scorer is BM25STD
        if expects_resp2_shape(client) or expects_unified_shape(client):
            res = client.ft().search(Query("quick").with_scores())
            assert 0.23 == pytest.approx(res.docs[0].score, 0.05)
            res = client.ft().search(Query("quick").scorer("TFIDF").with_scores())
            assert 1.0 == res.docs[0].score
            res = client.ft().search(
                Query("quick").scorer("TFIDF.DOCNORM").with_scores()
            )
            assert 0.14285714285714285 == res.docs[0].score
            res = client.ft().search(Query("quick").scorer("BM25").with_scores())
            assert 0.22471909420069797 == res.docs[0].score
            res = client.ft().search(Query("quick").scorer("DISMAX").with_scores())
            assert 2.0 == res.docs[0].score
            res = client.ft().search(Query("quick").scorer("DOCSCORE").with_scores())
            assert 1.0 == res.docs[0].score
            res = client.ft().search(Query("quick").scorer("HAMMING").with_scores())
            assert 0.0 == res.docs[0].score
        elif expects_resp3_shape(client):
            res = client.ft().search(Query("quick").with_scores())
            assert 0.23 == pytest.approx(res["results"][0]["score"], 0.05)
            res = client.ft().search(Query("quick").scorer("TFIDF").with_scores())
            assert 1.0 == res["results"][0]["score"]
            res = client.ft().search(
                Query("quick").scorer("TFIDF.DOCNORM").with_scores()
            )
            assert 0.14285714285714285 == res["results"][0]["score"]
            res = client.ft().search(Query("quick").scorer("BM25").with_scores())
            assert 0.22471909420069797 == res["results"][0]["score"]
            res = client.ft().search(Query("quick").scorer("DISMAX").with_scores())
            assert 2.0 == res["results"][0]["score"]
            res = client.ft().search(Query("quick").scorer("DOCSCORE").with_scores())
            assert 1.0 == res["results"][0]["score"]
            res = client.ft().search(Query("quick").scorer("HAMMING").with_scores())
            assert 0.0 == res["results"][0]["score"]


class TestConfig(SearchTestsBase):
    @pytest.mark.redismod
    @pytest.mark.onlynoncluster
    @skip_ifmodversion_lt("2.2.0", "search")
    @skip_if_server_version_gte("7.9.0")
    def test_config(self, client):
        assert client.ft().config_set("TIMEOUT", "100")
        with pytest.raises(redis.ResponseError):
            client.ft().config_set("TIMEOUT", "null")
        res = client.ft().config_get("*")
        assert "100" == res["TIMEOUT"]
        res = client.ft().config_get("TIMEOUT")
        assert "100" == res["TIMEOUT"]

    @pytest.mark.redismod
    @pytest.mark.onlynoncluster
    @skip_if_server_version_lt("7.9.0")
    # Redis Enterprise rejects CONFIG SET for the removed FT.CONFIG "timeout" param.
    @skip_if_redis_enterprise()
    def test_config_with_removed_ftconfig(self, client):
        assert client.config_set("timeout", "100")
        with pytest.raises(redis.ResponseError):
            client.config_set("timeout", "null")
        res = client.config_get("*")
        assert "100" == res["timeout"]
        res = client.config_get("timeout")
        assert "100" == res["timeout"]

    @pytest.mark.redismod
    @pytest.mark.onlynoncluster
    @skip_ifmodversion_lt("2.4.3", "search")
    def test_dialect_config(self, client):
        assert client.ft().config_get("DEFAULT_DIALECT")
        client.ft().config_set("DEFAULT_DIALECT", 2)
        assert client.ft().config_get("DEFAULT_DIALECT") == {"DEFAULT_DIALECT": "2"}
        with pytest.raises(redis.ResponseError):
            client.ft().config_set("DEFAULT_DIALECT", 0)

    @pytest.mark.redismod
    @skip_ifmodversion_lt("2.4.3", "search")
    def test_dialect(self, client):
        client.ft().create_index(
            (
                TagField("title"),
                TextField("t1"),
                TextField("t2"),
                NumericField("num"),
                VectorField(
                    "v",
                    "HNSW",
                    {"TYPE": "FLOAT32", "DIM": 1, "DISTANCE_METRIC": "COSINE"},
                ),
            )
        )
        client.hset("h", "t1", "hello")
        with pytest.raises(redis.ResponseError) as err:
            client.ft().explain(Query("(*)").dialect(1))
        assert "Syntax error" in str(err.value)
        assert "WILDCARD" in client.ft().explain(Query("(*)"))

        with pytest.raises(redis.ResponseError) as err:
            client.ft().explain(Query("$hello").dialect(1))
        assert "Syntax error" in str(err.value)
        q = Query("$hello")
        expected = "UNION {\n  hello\n  +hello(expanded)\n}\n"
        assert expected in client.ft().explain(q, query_params={"hello": "hello"})

        expected = "NUMERIC {0.000000 <= @num <= 10.000000}\n"
        assert expected in client.ft().explain(Query("@title:(@num:[0 10])").dialect(1))
        with pytest.raises(redis.ResponseError) as err:
            client.ft().explain(Query("@title:(@num:[0 10])"))
        assert "Syntax error" in str(err.value)


# Documents shared by the COLLECT reducer tests. Some fruits deliberately omit
# ``origin`` so the sparse-projection behavior can be exercised.
COLLECT_FRUITS = {
    "fruit:apple": {"color": "red", "name": "apple", "sweetness": 4, "origin": "usa"},
    "fruit:strawberry": {"color": "red", "name": "strawberry", "sweetness": 3},
    "fruit:cherry": {
        "color": "red",
        "name": "cherry",
        "sweetness": 5,
        "origin": "turkey",
    },
    "fruit:banana": {
        "color": "yellow",
        "name": "banana",
        "sweetness": 4,
        "origin": "ecuador",
    },
    "fruit:lemon": {"color": "yellow", "name": "lemon", "sweetness": 2},
}


def collect_entry_to_dict(entry):
    """Normalize a single COLLECT entry to a dict.

    RESP3 entries are maps already; RESP2 entries are flat
    ``[key, value, key, value, ...]`` arrays.
    """
    if isinstance(entry, dict):
        return entry
    return {entry[i]: entry[i + 1] for i in range(0, len(entry), 2)}


def collect_groups(client, res, alias, group_field="color"):
    """Return ``{group_value: [entry_dict, ...]}`` from an FT.AGGREGATE COLLECT
    result across all response shapes.

    ``legacy_resp3`` returns a dict of results; the other shapes return an
    ``AggregateResult`` whose rows are flat ``[key, value, ...]`` lists.
    """
    if expects_resp3_shape(client):
        rows = [result["extra_attributes"] for result in res["results"]]
    else:
        rows = [{row[i]: row[i + 1] for i in range(0, len(row), 2)} for row in res.rows]
    return {
        attrs[group_field]: [collect_entry_to_dict(e) for e in attrs[alias]]
        for attrs in rows
    }


class TestAggregations(SearchTestsBase):
    @pytest.fixture
    def collect_index(self, client):
        """Enable the preview feature and index the shared fruit documents.

        Restores ``search-enable-unstable-features`` to its original value on
        teardown so the preview flag does not leak into other tests.
        """
        config = client.config_get("search-enable-unstable-features")
        original = config["search-enable-unstable-features"]
        client.config_set("search-enable-unstable-features", "yes")
        client.ft().create_index(
            (
                TextField("color"),
                TextField("name"),
                NumericField("sweetness"),
                TextField("origin"),
            )
        )
        for key, mapping in COLLECT_FRUITS.items():
            client.hset(key, mapping=mapping)
        self.waitForIndex(client, "idx")
        yield
        client.config_set("search-enable-unstable-features", original)

    @pytest.mark.redismod
    @pytest.mark.onlynoncluster
    def test_aggregations_groupby(self, client):
        # Creating the index definition and schema
        client.ft().create_index(
            (
                NumericField("random_num"),
                TextField("title"),
                TextField("body"),
                TextField("parent"),
            )
        )

        # Indexing a document
        client.hset(
            "search",
            mapping={
                "title": "RediSearch",
                "body": "Redisearch impements a search engine on top of redis",
                "parent": "redis",
                "random_num": 10,
            },
        )
        client.hset(
            "ai",
            mapping={
                "title": "RedisAI",
                "body": "RedisAI executes Deep Learning/Machine Learning models and managing their data.",  # noqa
                "parent": "redis",
                "random_num": 3,
            },
        )
        client.hset(
            "json",
            mapping={
                "title": "RedisJson",
                "body": "RedisJSON implements ECMA-404 The JSON Data Interchange Standard as a native data type.",  # noqa
                "parent": "redis",
                "random_num": 8,
            },
        )

        if expects_resp2_shape(client) or expects_unified_shape(client):
            req = aggregations.AggregateRequest("redis").group_by(
                "@parent", reducers.count()
            )

            res = client.ft().aggregate(req).rows[0]
            assert res[1] == "redis"
            assert res[3] == "3"

            req = aggregations.AggregateRequest("redis").group_by(
                "@parent", reducers.count_distinct("@title")
            )

            res = client.ft().aggregate(req).rows[0]
            assert res[1] == "redis"
            assert res[3] == "3"

            req = aggregations.AggregateRequest("redis").group_by(
                "@parent", reducers.count_distinctish("@title")
            )

            res = client.ft().aggregate(req).rows[0]
            assert res[1] == "redis"
            assert res[3] == "3"

            req = aggregations.AggregateRequest("redis").group_by(
                "@parent", reducers.sum("@random_num")
            )

            res = client.ft().aggregate(req).rows[0]
            assert res[1] == "redis"
            assert res[3] == "21"  # 10+8+3

            req = aggregations.AggregateRequest("redis").group_by(
                "@parent", reducers.min("@random_num")
            )

            res = client.ft().aggregate(req).rows[0]
            assert res[1] == "redis"
            assert res[3] == "3"  # min(10,8,3)

            req = aggregations.AggregateRequest("redis").group_by(
                "@parent", reducers.max("@random_num")
            )

            res = client.ft().aggregate(req).rows[0]
            assert res[1] == "redis"
            assert res[3] == "10"  # max(10,8,3)

            req = aggregations.AggregateRequest("redis").group_by(
                "@parent", reducers.avg("@random_num")
            )

            res = client.ft().aggregate(req).rows[0]
            assert res[1] == "redis"
            index = res.index("__generated_aliasavgrandom_num")
            assert res[index + 1] == "7"  # (10+3+8)/3

            req = aggregations.AggregateRequest("redis").group_by(
                "@parent", reducers.stddev("random_num")
            )

            res = client.ft().aggregate(req).rows[0]
            assert res[1] == "redis"
            assert res[3] == "3.60555127546"

            req = aggregations.AggregateRequest("redis").group_by(
                "@parent", reducers.quantile("@random_num", 0.5)
            )

            res = client.ft().aggregate(req).rows[0]
            assert res[1] == "redis"
            assert res[3] == "8"  # median of 3,8,10

            req = aggregations.AggregateRequest("redis").group_by(
                "@parent", reducers.tolist("@title")
            )

            res = client.ft().aggregate(req).rows[0]
            assert res[1] == "redis"
            assert set(res[3]) == {"RediSearch", "RedisAI", "RedisJson"}

            req = aggregations.AggregateRequest("redis").group_by(
                "@parent", reducers.first_value("@title").alias("first")
            )

            res = client.ft().aggregate(req).rows[0]
            assert res == ["parent", "redis", "first", "RediSearch"]

            req = aggregations.AggregateRequest("redis").group_by(
                "@parent", reducers.random_sample("@title", 2).alias("random")
            )

            res = client.ft().aggregate(req).rows[0]
            assert res[1] == "redis"
            assert res[2] == "random"
            assert len(res[3]) == 2
            assert res[3][0] in ["RediSearch", "RedisAI", "RedisJson"]
        elif expects_resp3_shape(client):
            req = aggregations.AggregateRequest("redis").group_by(
                "@parent", reducers.count()
            )

            res = client.ft().aggregate(req)["results"][0]
            assert res["extra_attributes"]["parent"] == "redis"
            assert res["extra_attributes"]["__generated_aliascount"] == "3"

            req = aggregations.AggregateRequest("redis").group_by(
                "@parent", reducers.count_distinct("@title")
            )

            res = client.ft().aggregate(req)["results"][0]
            assert res["extra_attributes"]["parent"] == "redis"
            assert (
                res["extra_attributes"]["__generated_aliascount_distincttitle"] == "3"
            )

            req = aggregations.AggregateRequest("redis").group_by(
                "@parent", reducers.count_distinctish("@title")
            )

            res = client.ft().aggregate(req)["results"][0]
            assert res["extra_attributes"]["parent"] == "redis"
            assert (
                res["extra_attributes"]["__generated_aliascount_distinctishtitle"]
                == "3"
            )

            req = aggregations.AggregateRequest("redis").group_by(
                "@parent", reducers.sum("@random_num")
            )

            res = client.ft().aggregate(req)["results"][0]
            assert res["extra_attributes"]["parent"] == "redis"
            assert res["extra_attributes"]["__generated_aliassumrandom_num"] == "21"

            req = aggregations.AggregateRequest("redis").group_by(
                "@parent", reducers.min("@random_num")
            )

            res = client.ft().aggregate(req)["results"][0]
            assert res["extra_attributes"]["parent"] == "redis"
            assert res["extra_attributes"]["__generated_aliasminrandom_num"] == "3"

            req = aggregations.AggregateRequest("redis").group_by(
                "@parent", reducers.max("@random_num")
            )

            res = client.ft().aggregate(req)["results"][0]
            assert res["extra_attributes"]["parent"] == "redis"
            assert res["extra_attributes"]["__generated_aliasmaxrandom_num"] == "10"

            req = aggregations.AggregateRequest("redis").group_by(
                "@parent", reducers.avg("@random_num")
            )

            res = client.ft().aggregate(req)["results"][0]
            assert res["extra_attributes"]["parent"] == "redis"
            assert res["extra_attributes"]["__generated_aliasavgrandom_num"] == "7"

            req = aggregations.AggregateRequest("redis").group_by(
                "@parent", reducers.stddev("random_num")
            )

            res = client.ft().aggregate(req)["results"][0]
            assert res["extra_attributes"]["parent"] == "redis"
            assert (
                res["extra_attributes"]["__generated_aliasstddevrandom_num"]
                == "3.60555127546"
            )

            req = aggregations.AggregateRequest("redis").group_by(
                "@parent", reducers.quantile("@random_num", 0.5)
            )

            res = client.ft().aggregate(req)["results"][0]
            assert res["extra_attributes"]["parent"] == "redis"
            assert (
                res["extra_attributes"]["__generated_aliasquantilerandom_num,0.5"]
                == "8"
            )

            req = aggregations.AggregateRequest("redis").group_by(
                "@parent", reducers.tolist("@title")
            )

            res = client.ft().aggregate(req)["results"][0]
            assert res["extra_attributes"]["parent"] == "redis"
            assert set(res["extra_attributes"]["__generated_aliastolisttitle"]) == {
                "RediSearch",
                "RedisAI",
                "RedisJson",
            }

            req = aggregations.AggregateRequest("redis").group_by(
                "@parent", reducers.first_value("@title").alias("first")
            )

            res = client.ft().aggregate(req)["results"][0]
            assert res["extra_attributes"] == {"parent": "redis", "first": "RediSearch"}

            req = aggregations.AggregateRequest("redis").group_by(
                "@parent", reducers.random_sample("@title", 2).alias("random")
            )

            res = client.ft().aggregate(req)["results"][0]
            assert res["extra_attributes"]["parent"] == "redis"
            assert "random" in res["extra_attributes"].keys()
            assert len(res["extra_attributes"]["random"]) == 2
            assert res["extra_attributes"]["random"][0] in [
                "RediSearch",
                "RedisAI",
                "RedisJson",
            ]

    @pytest.mark.redismod
    @pytest.mark.onlynoncluster
    @skip_if_server_version_lt("8.9.0")
    def test_aggregations_collect_sortby(self, client, collect_index):
        # COLLECT projects the named fields per group, ordered by SORTBY.
        req = (
            aggregations.AggregateRequest("*")
            .load("*")
            .group_by(
                "@color",
                reducers.collect(
                    ["@name", "@sweetness"],
                    sort_by=[aggregations.Desc("@sweetness")],
                ).alias("fruits"),
            )
        )
        groups = collect_groups(client, client.ft().aggregate(req), "fruits")

        assert groups["red"] == [
            {"name": "cherry", "sweetness": "5"},
            {"name": "apple", "sweetness": "4"},
            {"name": "strawberry", "sweetness": "3"},
        ]
        assert groups["yellow"] == [
            {"name": "banana", "sweetness": "4"},
            {"name": "lemon", "sweetness": "2"},
        ]

    @pytest.mark.redismod
    @pytest.mark.onlynoncluster
    @skip_if_server_version_lt("8.9.0")
    def test_aggregations_collect_sortby_ascending(self, client, collect_index):
        # An ``Asc`` directive orders the collected entries in ascending order.
        req = (
            aggregations.AggregateRequest("*")
            .load("*")
            .group_by(
                "@color",
                reducers.collect(
                    ["@name"], sort_by=[aggregations.Asc("@sweetness")]
                ).alias("fruits"),
            )
        )
        groups = collect_groups(client, client.ft().aggregate(req), "fruits")

        assert groups["red"] == [
            {"name": "strawberry"},
            {"name": "apple"},
            {"name": "cherry"},
        ]
        assert groups["yellow"] == [{"name": "lemon"}, {"name": "banana"}]

    @pytest.mark.redismod
    @pytest.mark.onlynoncluster
    @skip_if_server_version_lt("8.9.0")
    def test_aggregations_collect_limit_top_n(self, client, collect_index):
        # SORTBY + LIMIT is a bounded top-N selection with an offset.
        req = (
            aggregations.AggregateRequest("*")
            .load("*")
            .group_by(
                "@color",
                reducers.collect(
                    ["@name"],
                    sort_by=[aggregations.Desc("@sweetness")],
                    limit=(1, 1),
                ).alias("fruits"),
            )
        )
        groups = collect_groups(client, client.ft().aggregate(req), "fruits")

        # red desc: cherry, apple, strawberry -> skip 1, take 1 -> apple
        assert groups["red"] == [{"name": "apple"}]
        # yellow desc: banana, lemon -> skip 1, take 1 -> lemon
        assert groups["yellow"] == [{"name": "lemon"}]

    @pytest.mark.redismod
    @pytest.mark.onlynoncluster
    @skip_if_server_version_lt("8.9.0")
    def test_aggregations_collect_multiple_sort_keys(self, client, collect_index):
        # Multiple sort keys are applied left to right.
        req = (
            aggregations.AggregateRequest("*")
            .load("*")
            .group_by(
                "@color",
                reducers.collect(
                    ["@name", "@sweetness"],
                    sort_by=[
                        aggregations.Desc("@sweetness"),
                        aggregations.Asc("@name"),
                    ],
                ).alias("fruits"),
            )
        )
        groups = collect_groups(client, client.ft().aggregate(req), "fruits")

        assert groups["red"] == [
            {"name": "cherry", "sweetness": "5"},
            {"name": "apple", "sweetness": "4"},
            {"name": "strawberry", "sweetness": "3"},
        ]
        assert groups["yellow"] == [
            {"name": "banana", "sweetness": "4"},
            {"name": "lemon", "sweetness": "2"},
        ]

    @pytest.mark.redismod
    @pytest.mark.onlynoncluster
    @skip_if_server_version_lt("8.9.0")
    def test_aggregations_collect_sparse_projection(self, client, collect_index):
        # A field absent from a row is omitted from that entry (not NULL).
        req = (
            aggregations.AggregateRequest("*")
            .load("*")
            .group_by(
                "@color",
                reducers.collect(
                    ["@name", "@origin"],
                    sort_by=[aggregations.Desc("@sweetness")],
                ).alias("fruits"),
            )
        )
        groups = collect_groups(client, client.ft().aggregate(req), "fruits")

        # strawberry and lemon have no ``origin`` -> the key is omitted.
        assert groups["red"] == [
            {"name": "cherry", "origin": "turkey"},
            {"name": "apple", "origin": "usa"},
            {"name": "strawberry"},
        ]
        assert groups["yellow"] == [
            {"name": "banana", "origin": "ecuador"},
            {"name": "lemon"},
        ]

    @pytest.mark.redismod
    @pytest.mark.onlynoncluster
    @skip_if_server_version_lt("8.9.0")
    def test_aggregations_collect_key_field(self, client, collect_index):
        # ``@__key`` can be projected when carried into the pipeline via LOAD.
        req = (
            aggregations.AggregateRequest("*")
            .load("@__key", "@name")
            .group_by(
                "@color",
                reducers.collect(
                    ["@name", "@__key"], sort_by=[aggregations.Asc("@name")]
                ).alias("fruits"),
            )
        )
        groups = collect_groups(client, client.ft().aggregate(req), "fruits")

        assert groups["red"] == [
            {"name": "apple", "__key": "fruit:apple"},
            {"name": "cherry", "__key": "fruit:cherry"},
            {"name": "strawberry", "__key": "fruit:strawberry"},
        ]
        assert groups["yellow"] == [
            {"name": "banana", "__key": "fruit:banana"},
            {"name": "lemon", "__key": "fruit:lemon"},
        ]

    @pytest.mark.redismod
    @pytest.mark.onlynoncluster
    @skip_if_server_version_lt("8.9.0")
    def test_aggregations_collect_fields_all(self, client, collect_index):
        # FIELDS * is stage-local: after GROUPBY only the group key is present.
        req = (
            aggregations.AggregateRequest("*")
            .load("*")
            .group_by("@color", reducers.collect("*").alias("fruits"))
        )
        groups = collect_groups(client, client.ft().aggregate(req), "fruits")

        assert len(groups["red"]) == 3
        assert len(groups["yellow"]) == 2
        assert all(entry == {"color": "red"} for entry in groups["red"])
        assert all(entry == {"color": "yellow"} for entry in groups["yellow"])

    @pytest.mark.fixed_client
    @pytest.mark.parametrize(
        "bad_fields",
        [[], (), "", "   ", ["   "], ["name", ""], ["name", "   "]],
    )
    def test_aggregations_collect_invalid_fields(self, bad_fields):
        # Empty or blank field selectors are local API misuse and are rejected
        # client-side with a ValueError before any command is sent. A blank
        # entry anywhere in the list is rejected too, not just an empty list.
        with pytest.raises(ValueError):
            reducers.collect(bad_fields)

    @pytest.mark.redismod
    @pytest.mark.onlynoncluster
    @skip_if_server_version_lt("8.9.0")
    def test_aggregations_collect_single_str_field(self, client, collect_index):
        # A single field may be passed as a bare string; the client wraps it as
        # a one-field projection.
        req = (
            aggregations.AggregateRequest("*")
            .load("*")
            .group_by(
                "@color",
                reducers.collect(
                    "@name", sort_by=[aggregations.Asc("@sweetness")]
                ).alias("fruits"),
            )
        )
        groups = collect_groups(client, client.ft().aggregate(req), "fruits")

        assert groups["red"] == [
            {"name": "strawberry"},
            {"name": "apple"},
            {"name": "cherry"},
        ]
        assert groups["yellow"] == [{"name": "lemon"}, {"name": "banana"}]

    @pytest.mark.redismod
    @pytest.mark.onlynoncluster
    @skip_if_server_version_lt("8.9.0")
    def test_aggregations_collect_field_names_without_prefix(
        self, client, collect_index
    ):
        # Field and sort names may be supplied without a leading "@"; the client
        # normalizes them on the wire (the server requires the prefix).
        req = (
            aggregations.AggregateRequest("*")
            .load("*")
            .group_by(
                "@color",
                reducers.collect(
                    ["name", "sweetness"],
                    sort_by=[aggregations.Desc("sweetness")],
                ).alias("fruits"),
            )
        )
        groups = collect_groups(client, client.ft().aggregate(req), "fruits")

        assert groups["red"] == [
            {"name": "cherry", "sweetness": "5"},
            {"name": "apple", "sweetness": "4"},
            {"name": "strawberry", "sweetness": "3"},
        ]
        assert groups["yellow"] == [
            {"name": "banana", "sweetness": "4"},
            {"name": "lemon", "sweetness": "2"},
        ]

    @pytest.mark.redismod
    def test_aggregations_sort_by_and_limit(self, client):
        client.ft().create_index((TextField("t1"), TextField("t2")))

        client.ft().client.hset("doc1", mapping={"t1": "a", "t2": "b"})
        client.ft().client.hset("doc2", mapping={"t1": "b", "t2": "a"})

        if expects_resp2_shape(client) or expects_unified_shape(client):
            # test sort_by using SortDirection
            req = aggregations.AggregateRequest("*").sort_by(
                aggregations.Asc("@t2"), aggregations.Desc("@t1")
            )
            res = client.ft().aggregate(req)
            assert res.rows[0] == ["t2", "a", "t1", "b"]
            assert res.rows[1] == ["t2", "b", "t1", "a"]

            # test sort_by without SortDirection
            req = aggregations.AggregateRequest("*").sort_by("@t1")
            res = client.ft().aggregate(req)
            assert res.rows[0] == ["t1", "a"]
            assert res.rows[1] == ["t1", "b"]

            # test sort_by with max
            req = aggregations.AggregateRequest("*").sort_by("@t1", max=1)
            res = client.ft().aggregate(req)
            assert len(res.rows) == 1

            # test limit
            req = aggregations.AggregateRequest("*").sort_by("@t1").limit(1, 1)
            res = client.ft().aggregate(req)
            assert len(res.rows) == 1
            assert res.rows[0] == ["t1", "b"]
        elif expects_resp3_shape(client):
            # test sort_by using SortDirection
            req = aggregations.AggregateRequest("*").sort_by(
                aggregations.Asc("@t2"), aggregations.Desc("@t1")
            )
            res = client.ft().aggregate(req)["results"]
            assert res[0]["extra_attributes"] == {"t2": "a", "t1": "b"}
            assert res[1]["extra_attributes"] == {"t2": "b", "t1": "a"}

            # test sort_by without SortDirection
            req = aggregations.AggregateRequest("*").sort_by("@t1")
            res = client.ft().aggregate(req)["results"]
            assert res[0]["extra_attributes"] == {"t1": "a"}
            assert res[1]["extra_attributes"] == {"t1": "b"}

            # test sort_by with max
            req = aggregations.AggregateRequest("*").sort_by("@t1", max=1)
            res = client.ft().aggregate(req)
            assert len(res["results"]) == 1

            # test limit
            req = aggregations.AggregateRequest("*").sort_by("@t1").limit(1, 1)
            res = client.ft().aggregate(req)
            assert len(res["results"]) == 1
            assert res["results"][0]["extra_attributes"] == {"t1": "b"}

    @pytest.mark.redismod
    def test_aggregations_load(self, client):
        client.ft().create_index((TextField("t1"), TextField("t2")))

        client.ft().client.hset("doc1", mapping={"t1": "hello", "t2": "world"})

        if expects_resp2_shape(client) or expects_unified_shape(client):
            # load t1
            req = aggregations.AggregateRequest("*").load("t1")
            res = client.ft().aggregate(req)
            assert res.rows[0] == ["t1", "hello"]

            # load t2
            req = aggregations.AggregateRequest("*").load("t2")
            res = client.ft().aggregate(req)
            assert res.rows[0] == ["t2", "world"]

            # load all
            req = aggregations.AggregateRequest("*").load()
            res = client.ft().aggregate(req)
            assert res.rows[0] == ["t1", "hello", "t2", "world"]
        elif expects_resp3_shape(client):
            # load t1
            req = aggregations.AggregateRequest("*").load("t1")
            res = client.ft().aggregate(req)
            assert res["results"][0]["extra_attributes"] == {"t1": "hello"}

            # load t2
            req = aggregations.AggregateRequest("*").load("t2")
            res = client.ft().aggregate(req)
            assert res["results"][0]["extra_attributes"] == {"t2": "world"}

            # load all
            req = aggregations.AggregateRequest("*").load()
            res = client.ft().aggregate(req)
            assert res["results"][0]["extra_attributes"] == {
                "t1": "hello",
                "t2": "world",
            }

    @pytest.mark.redismod
    def test_aggregations_apply(self, client):
        client.ft().create_index(
            (
                TextField("PrimaryKey", sortable=True),
                NumericField("CreatedDateTimeUTC", sortable=True),
            )
        )

        client.ft().client.hset(
            "doc1",
            mapping={
                "PrimaryKey": "9::362330",
                "CreatedDateTimeUTC": "637387878524969984",
            },
        )
        client.ft().client.hset(
            "doc2",
            mapping={
                "PrimaryKey": "9::362329",
                "CreatedDateTimeUTC": "637387875859270016",
            },
        )

        req = aggregations.AggregateRequest("*").apply(
            CreatedDateTimeUTC="@CreatedDateTimeUTC * 10"
        )
        res = client.ft().aggregate(req)
        if expects_resp2_shape(client) or expects_unified_shape(client):
            res_set = {res.rows[0][1], res.rows[1][1]}
            assert res_set == {"6373878785249699840", "6373878758592700416"}
        elif expects_resp3_shape(client):
            res_set = {
                res["results"][0]["extra_attributes"]["CreatedDateTimeUTC"],
                res["results"][1]["extra_attributes"]["CreatedDateTimeUTC"],
            }
            assert res_set == {"6373878785249699840", "6373878758592700416"}

    @pytest.mark.redismod
    def test_aggregations_filter(self, client):
        client.ft().create_index(
            (TextField("name", sortable=True), NumericField("age", sortable=True))
        )

        client.ft().client.hset("doc1", mapping={"name": "bar", "age": "25"})
        client.ft().client.hset("doc2", mapping={"name": "foo", "age": "19"})

        for dialect in [1, 2]:
            req = (
                aggregations.AggregateRequest("*")
                .filter("@name=='foo' && @age < 20")
                .dialect(dialect)
            )
            res = client.ft().aggregate(req)
            if expects_resp2_shape(client) or expects_unified_shape(client):
                assert len(res.rows) == 1
                assert res.rows[0] == ["name", "foo", "age", "19"]

                req = (
                    aggregations.AggregateRequest("*")
                    .filter("@age > 15")
                    .sort_by("@age")
                    .dialect(dialect)
                )
                res = client.ft().aggregate(req)
                assert len(res.rows) == 2
                assert res.rows[0] == ["age", "19"]
                assert res.rows[1] == ["age", "25"]
            elif expects_resp3_shape(client):
                assert len(res["results"]) == 1
                assert res["results"][0]["extra_attributes"] == {
                    "name": "foo",
                    "age": "19",
                }

                req = (
                    aggregations.AggregateRequest("*")
                    .filter("@age > 15")
                    .sort_by("@age")
                    .dialect(dialect)
                )
                res = client.ft().aggregate(req)
                assert len(res["results"]) == 2
                assert res["results"][0]["extra_attributes"] == {"age": "19"}
                assert res["results"][1]["extra_attributes"] == {"age": "25"}

    @pytest.mark.redismod
    @skip_ifmodversion_lt("2.10.05", "search")
    def test_aggregations_add_scores(self, client):
        client.ft().create_index(
            (
                TextField("name", sortable=True, weight=5.0),
                NumericField("age", sortable=True),
            )
        )

        client.hset("doc1", mapping={"name": "bar", "age": "25"})
        client.hset("doc2", mapping={"name": "foo", "age": "19"})

        req = aggregations.AggregateRequest("*").add_scores()
        res = client.ft().aggregate(req)

        if isinstance(res, dict):
            assert len(res["results"]) == 2
            assert res["results"][0]["extra_attributes"] == {"__score": "0.2"}
            assert res["results"][1]["extra_attributes"] == {"__score": "0.2"}
        else:
            assert len(res.rows) == 2
            assert res.rows[0] == ["__score", "0.2"]
            assert res.rows[1] == ["__score", "0.2"]

    @pytest.mark.redismod
    @skip_ifmodversion_lt("2.10.05", "search")
    async def test_aggregations_hybrid_scoring(self, client):
        client.ft().create_index(
            (
                TextField("name", sortable=True, weight=5.0),
                TextField("description", sortable=True, weight=5.0),
                VectorField(
                    "vector",
                    "HNSW",
                    {"TYPE": "FLOAT32", "DIM": 2, "DISTANCE_METRIC": "COSINE"},
                ),
            )
        )

        client.hset(
            "doc1",
            mapping={
                "name": "cat book",
                "description": "an animal book about cats",
                "vector": np.array([0.1, 0.2]).astype(np.float32).tobytes(),
            },
        )
        client.hset(
            "doc2",
            mapping={
                "name": "dog book",
                "description": "an animal book about dogs",
                "vector": np.array([0.2, 0.1]).astype(np.float32).tobytes(),
            },
        )

        query_string = "(@description:animal)=>[KNN 3 @vector $vec_param AS dist]"
        req = (
            aggregations.AggregateRequest(query_string)
            .scorer("BM25")
            .add_scores()
            .apply(hybrid_score="@__score + @dist")
            .load("*")
            .dialect(4)
        )

        res = client.ft().aggregate(
            req,
            query_params={
                "vec_param": np.array([0.11, 0.21]).astype(np.float32).tobytes()
            },
        )

        if isinstance(res, dict):
            assert len(res["results"]) == 2
        else:
            assert len(res.rows) == 2
            for row in res.rows:
                len(row) == 6


class TestSearchWithJsonIndex(SearchTestsBase):
    @pytest.mark.redismod
    @skip_ifmodversion_lt("2.2.0", "search")
    def test_create_json_with_alias(self, client):
        """
        Create definition with IndexType.JSON as index type (ON JSON) with two
        fields with aliases, and use json client to test it.
        """
        definition = IndexDefinition(prefix=["king:"], index_type=IndexType.JSON)
        client.ft().create_index(
            (TextField("$.name", as_name="name"), NumericField("$.num", as_name="num")),
            definition=definition,
        )

        client.json().set("king:1", Path.root_path(), {"name": "henry", "num": 42})
        client.json().set("king:2", Path.root_path(), {"name": "james", "num": 3.14})

        if expects_resp2_shape(client) or expects_unified_shape(client):
            res = client.ft().search("@name:henry")
            assert res.docs[0].id == "king:1"
            assert res.docs[0].json == '{"name":"henry","num":42}'
            assert res.total == 1

            res = client.ft().search("@num:[0 10]")
            assert res.docs[0].id == "king:2"
            assert res.docs[0].json == '{"name":"james","num":3.14}'
            assert res.total == 1
        elif expects_resp3_shape(client):
            res = client.ft().search("@name:henry")
            assert res["results"][0]["id"] == "king:1"
            assert (
                res["results"][0]["extra_attributes"]["$"]
                == '{"name":"henry","num":42}'
            )
            assert res["total_results"] == 1

            res = client.ft().search("@num:[0 10]")
            assert res["results"][0]["id"] == "king:2"
            assert (
                res["results"][0]["extra_attributes"]["$"]
                == '{"name":"james","num":3.14}'
            )
            assert res["total_results"] == 1

        # Tests returns an error if path contain special characters (user should
        # use an alias)
        with pytest.raises(Exception):
            client.ft().search("@$.name:henry")

    @pytest.mark.redismod
    @skip_ifmodversion_lt("2.2.0", "search")
    def test_json_with_multipath(self, client):
        """
        Create definition with IndexType.JSON as index type (ON JSON),
        and use json client to test it.
        """
        definition = IndexDefinition(prefix=["king:"], index_type=IndexType.JSON)
        client.ft().create_index(
            (TagField("$..name", as_name="name")), definition=definition
        )

        client.json().set(
            "king:1",
            Path.root_path(),
            {"name": "henry", "country": {"name": "england"}},
        )

        if expects_resp2_shape(client) or expects_unified_shape(client):
            res = client.ft().search("@name:{henry}")
            assert res.docs[0].id == "king:1"
            assert res.docs[0].json == '{"name":"henry","country":{"name":"england"}}'
            assert res.total == 1

            res = client.ft().search("@name:{england}")
            assert res.docs[0].id == "king:1"
            assert res.docs[0].json == '{"name":"henry","country":{"name":"england"}}'
            assert res.total == 1
        elif expects_resp3_shape(client):
            res = client.ft().search("@name:{henry}")
            assert res["results"][0]["id"] == "king:1"
            assert (
                res["results"][0]["extra_attributes"]["$"]
                == '{"name":"henry","country":{"name":"england"}}'
            )
            assert res["total_results"] == 1

            res = client.ft().search("@name:{england}")
            assert res["results"][0]["id"] == "king:1"
            assert (
                res["results"][0]["extra_attributes"]["$"]
                == '{"name":"henry","country":{"name":"england"}}'
            )
            assert res["total_results"] == 1

    @pytest.mark.redismod
    @skip_ifmodversion_lt("2.2.0", "search")
    def test_json_with_jsonpath(self, client):
        definition = IndexDefinition(index_type=IndexType.JSON)
        client.ft().create_index(
            (
                TextField('$["prod:name"]', as_name="name"),
                TextField("$.prod:name", as_name="name_unsupported"),
            ),
            definition=definition,
        )

        client.json().set("doc:1", Path.root_path(), {"prod:name": "RediSearch"})

        if expects_resp2_shape(client) or expects_unified_shape(client):
            # query for a supported field succeeds
            res = client.ft().search(Query("@name:RediSearch"))
            assert res.total == 1
            assert res.docs[0].id == "doc:1"
            assert res.docs[0].json == '{"prod:name":"RediSearch"}'

            # query for an unsupported field
            res = client.ft().search("@name_unsupported:RediSearch")
            assert res.total == 1

            # return of a supported field succeeds
            res = client.ft().search(Query("@name:RediSearch").return_field("name"))
            assert res.total == 1
            assert res.docs[0].id == "doc:1"
            assert res.docs[0].name == "RediSearch"
        elif expects_resp3_shape(client):
            # query for a supported field succeeds
            res = client.ft().search(Query("@name:RediSearch"))
            assert res["total_results"] == 1
            assert res["results"][0]["id"] == "doc:1"
            assert (
                res["results"][0]["extra_attributes"]["$"]
                == '{"prod:name":"RediSearch"}'
            )

            # query for an unsupported field
            res = client.ft().search("@name_unsupported:RediSearch")
            assert res["total_results"] == 1

            # return of a supported field succeeds
            res = client.ft().search(Query("@name:RediSearch").return_field("name"))
            assert res["total_results"] == 1
            assert res["results"][0]["id"] == "doc:1"
            assert res["results"][0]["extra_attributes"]["name"] == "RediSearch"


class TestProfile(SearchTestsBase):
    @pytest.mark.redismod
    @pytest.mark.onlynoncluster
    @pytest.mark.fixed_client
    @skip_if_redis_enterprise()
    @skip_if_server_version_lt("6.3.0")
    @pytest.mark.parametrize(
        "protocol,legacy_responses",
        [(None, True), (None, False), (2, True), (2, False), (3, False)],
    )
    @pytest.mark.parametrize("limited", [False, True])
    @pytest.mark.parametrize("with_scores", [False, True])
    def test_profile_field_encodings(
        self, request, stack_url, protocol, legacy_responses, limited, with_scores
    ):
        options = {"decode_responses": False, "legacy_responses": legacy_responses}
        if protocol is not None:
            options["protocol"] = protocol
        client = _get_client(redis.Redis, request, from_url=stack_url, **options)
        client.flushdb()
        client.ft().create_index((TextField("title"),))
        binary = b"\x00\x01\x82\xff"
        client.hset(
            "doc:1", mapping={"title": "hello", "vector": binary, "label": b"caf\xe9"}
        )
        self.waitForIndex(client, "idx")
        query = (
            Query("hello")
            .return_field("vector", as_field="embedding", decode_field=False)
            .return_field("label", encoding="latin-1")
            .return_field("title")
        )
        if with_scores:
            query.with_scores()

        result, profile = client.ft().profile(query, limited=limited)

        assert result.total == 1
        assert result.docs[0].embedding == binary
        assert result.docs[0].label == "café"
        assert result.docs[0].title == "hello"
        if with_scores:
            assert isinstance(result.docs[0].score, float)
        assert profile.info

    @pytest.mark.redismod
    @pytest.mark.onlynoncluster
    @skip_if_redis_enterprise()
    @skip_if_server_version_gte("7.9.0")
    @skip_if_server_version_lt("6.3.0")
    def test_profile(self, client):
        client.ft().create_index((TextField("t"),))
        client.ft().client.hset("1", "t", "hello")
        client.ft().client.hset("2", "t", "world")

        # check using Query
        q = Query("hello|world").no_content()
        if expects_resp2_shape(client) or expects_unified_shape(client):
            res, det = client.ft().profile(q)
            det = det.info

            assert isinstance(det, list)
            assert len(res.docs) == 2  # check also the search result

            # check using AggregateRequest
            req = (
                aggregations.AggregateRequest("*")
                .load("t")
                .apply(prefix="startswith(@t, 'hel')")
            )
            res, det = client.ft().profile(req)
            det = det.info
            assert isinstance(det, list)
            assert len(res.rows) == 2  # check also the search result
        elif expects_resp3_shape(client):
            res = client.ft().profile(q)
            res = res.info

            assert isinstance(res, dict)
            assert len(res["results"]) == 2  # check also the search result

            # check using AggregateRequest
            req = (
                aggregations.AggregateRequest("*")
                .load("t")
                .apply(prefix="startswith(@t, 'hel')")
            )
            res = client.ft().profile(req)
            res = res.info

            assert isinstance(res, dict)
            assert len(res["results"]) == 2  # check also the search result

    @pytest.mark.redismod
    @pytest.mark.onlynoncluster
    @skip_if_redis_enterprise()
    @skip_if_server_version_lt("7.9.0")
    def test_profile_with_coordinator(self, client):
        client.ft().create_index((TextField("t"),))
        client.ft().client.hset("1", "t", "hello")
        client.ft().client.hset("2", "t", "world")

        # check using Query
        q = Query("hello|world").no_content()
        if expects_resp2_shape(client):
            res, det = client.ft().profile(q)
            det = det.info

            assert isinstance(det, list)
            assert len(res.docs) == 2  # check also the search result

            # check using AggregateRequest
            req = (
                aggregations.AggregateRequest("*")
                .load("t")
                .apply(prefix="startswith(@t, 'hel')")
            )
            res, det = client.ft().profile(req)
            det = det.info

            assert isinstance(det, list)
            assert det[0] == "Shards"
            assert det[2] == "Coordinator"
            assert len(res.rows) == 2  # check also the search result
        elif expects_unified_shape(client):
            res, det = client.ft().profile(q)
            det = det.info

            assert isinstance(det, dict)
            assert len(res.docs) == 2  # check also the search result

            # check using AggregateRequest
            req = (
                aggregations.AggregateRequest("*")
                .load("t")
                .apply(prefix="startswith(@t, 'hel')")
            )
            res, det = client.ft().profile(req)
            det = det.info

            assert isinstance(det, dict)
            assert len(res.rows) == 2  # check also the search result
        elif expects_resp3_shape(client):
            res = client.ft().profile(q)
            res = res.info

            assert isinstance(res, dict)
            assert len(res["Results"]["results"]) == 2  # check also the search result

            # check using AggregateRequest
            req = (
                aggregations.AggregateRequest("*")
                .load("t")
                .apply(prefix="startswith(@t, 'hel')")
            )
            res = client.ft().profile(req)
            res = res.info

            assert isinstance(res, dict)
            assert len(res["Results"]["results"]) == 2  # check also the search result

    @pytest.mark.redismod
    @pytest.mark.onlynoncluster
    @skip_if_server_version_gte("7.9.0")
    @skip_if_server_version_lt("6.3.0")
    def test_profile_limited(self, client):
        client.ft().create_index((TextField("t"),))
        client.ft().client.hset("1", "t", "hello")
        client.ft().client.hset("2", "t", "hell")
        client.ft().client.hset("3", "t", "help")
        client.ft().client.hset("4", "t", "helowa")

        q = Query("%hell% hel*")
        if expects_resp2_shape(client) or expects_unified_shape(client):
            res, det = client.ft().profile(q, limited=True)
            det = det.info
            assert det[4][1][7][9] == "The number of iterators in the union is 3"
            assert det[4][1][8][9] == "The number of iterators in the union is 4"
            assert det[4][1][1] == "INTERSECT"
            assert len(res.docs) == 3  # check also the search result
        elif expects_resp3_shape(client):
            res = client.ft().profile(q, limited=True)
            res = res.info
            iterators_profile = res["profile"]["Iterators profile"]
            assert (
                iterators_profile[0]["Child iterators"][0]["Child iterators"]
                == "The number of iterators in the union is 3"
            )
            assert (
                iterators_profile[0]["Child iterators"][1]["Child iterators"]
                == "The number of iterators in the union is 4"
            )
            assert iterators_profile[0]["Type"] == "INTERSECT"
            assert len(res["results"]) == 3  # check also the search result

    @pytest.mark.redismod
    @skip_ifmodversion_lt("2.4.3", "search")
    @skip_if_server_version_gte("7.9.0")
    @skip_if_server_version_lt("6.3.0")
    def test_profile_query_params(self, client):
        client.ft().create_index(
            (
                VectorField(
                    "v", "HNSW", {"TYPE": "FLOAT32", "DIM": 2, "DISTANCE_METRIC": "L2"}
                ),
            )
        )
        client.hset("a", "v", "aaaaaaaa")
        client.hset("b", "v", "aaaabaaa")
        client.hset("c", "v", "aaaaabaa")
        query = "*=>[KNN 2 @v $vec]"
        q = Query(query).return_field("__v_score").sort_by("__v_score", True)
        if expects_resp2_shape(client) or expects_unified_shape(client):
            res, det = client.ft().profile(q, query_params={"vec": "aaaaaaaa"})
            det = det.info
            assert det[4][1][5] == 2.0
            assert det[4][1][1] == "VECTOR"
            assert res.total == 2
            assert "a" == res.docs[0].id
            assert "0" == res.docs[0].__getattribute__("__v_score")
        elif expects_resp3_shape(client):
            res = client.ft().profile(q, query_params={"vec": "aaaaaaaa"})
            res = res.info
            assert res["profile"]["Iterators profile"][0]["Counter"] == 2
            assert res["profile"]["Iterators profile"][0]["Type"] == "VECTOR"
            assert res["total_results"] == 2
            assert "a" == res["results"][0]["id"]
            assert "0" == res["results"][0]["extra_attributes"]["__v_score"]


class TestDifferentFieldTypesSearch(SearchTestsBase):
    @pytest.mark.redismod
    @skip_ifmodversion_lt("2.4.3", "search")
    def test_vector_field(self, client):
        client.flushdb()
        client.ft().create_index(
            (
                VectorField(
                    "v", "HNSW", {"TYPE": "FLOAT32", "DIM": 2, "DISTANCE_METRIC": "L2"}
                ),
            )
        )
        client.hset("a", "v", "aaaaaaaa")
        client.hset("b", "v", "aaaabaaa")
        client.hset("c", "v", "aaaaabaa")

        query = "*=>[KNN 2 @v $vec]"
        q = Query(query).return_field("__v_score").sort_by("__v_score", True)
        res = client.ft().search(q, query_params={"vec": "aaaaaaaa"})

        if expects_resp2_shape(client) or expects_unified_shape(client):
            assert "a" == res.docs[0].id
            assert "0" == res.docs[0].__getattribute__("__v_score")
        elif expects_resp3_shape(client):
            assert "a" == res["results"][0]["id"]
            assert "0" == res["results"][0]["extra_attributes"]["__v_score"]

    @pytest.mark.redismod
    @skip_ifmodversion_lt("2.4.3", "search")
    def test_vector_field_error(self, r):
        r.flushdb()

        # sortable tag
        with pytest.raises(Exception):
            r.ft().create_index((VectorField("v", "HNSW", {}, sortable=True),))

        # not supported algorithm
        with pytest.raises(Exception):
            r.ft().create_index((VectorField("v", "SORT", {}),))

    @pytest.mark.fixed_client
    def test_vector_field_rerank(self):
        # Pure serialization check: VectorField builds the FT.CREATE args with
        # no server round-trip, so this test needs no Redis and is not gated.
        # RERANK is a boolean key-value attribute for HNSW vector fields on
        # disk-backed (Flex / Auto-Tiering) deployments, where it is mandatory.
        # It toggles the exact FP32 rerank pass over the approximate candidates
        # returned by the on-disk graph traversal. It flows through the generic
        # ``attributes`` dict as the string "TRUE"/"FALSE" (a bare flag is
        # rejected by the server, and Python bools are rejected by the client
        # encoder), and the attribute-count token accounts for the extra pair.
        field = VectorField(
            "v",
            "HNSW",
            {"TYPE": "FLOAT32", "DIM": 128, "DISTANCE_METRIC": "L2", "RERANK": "TRUE"},
        )
        assert field.args[0] == "VECTOR"
        assert field.args[1] == "HNSW"
        assert field.args[2] == 8  # 4 attribute pairs -> 8 tokens
        assert "RERANK" in field.args
        assert "TRUE" in field.args

        # MS2 also accepts RERANK FALSE (opt out of the rerank pass).
        field = VectorField(
            "v",
            "HNSW",
            {"TYPE": "FLOAT32", "DIM": 128, "DISTANCE_METRIC": "L2", "RERANK": "FALSE"},
        )
        assert field.args[2] == 8
        assert "RERANK" in field.args
        assert "FALSE" in field.args

    @pytest.mark.redismod
    @skip_if_server_version_lt("8.11.0")
    def test_hnsw_sq8_compression(self, client):
        client.ft().create_index(
            (
                VectorField(
                    "sq8",
                    "HNSW",
                    {
                        "TYPE": "FLOAT32",
                        "DIM": 8,
                        "DISTANCE_METRIC": "L2",
                        "COMPRESSION": "SQ8",
                        "TRAINING_THRESHOLD": 0,
                    },
                ),
                VectorField(
                    "sq8_default",
                    "HNSW",
                    {
                        "TYPE": "FLOAT16",
                        "DIM": 8,
                        "DISTANCE_METRIC": "L2",
                        "COMPRESSION": "SQ8",
                    },
                ),
                VectorField(
                    "plain",
                    "HNSW",
                    {"TYPE": "FLOAT32", "DIM": 8, "DISTANCE_METRIC": "L2"},
                    index_missing=True,
                ),
            )
        )

        for i in range(20):
            vec = np.array([float(i + j) for j in range(8)], dtype=np.float32)
            client.hset(
                f"doc{i}",
                mapping={
                    "sq8": vec.tobytes(),
                    "sq8_default": vec.astype(np.float16).tobytes(),
                },
            )

        query = Query("*=>[KNN 5 @sq8 $vec as score]").no_content()
        query_params = {"vec": np.arange(8, dtype=np.float32).tobytes()}
        res = client.ft().search(query, query_params=query_params)
        if expects_resp2_shape(client) or expects_unified_shape(client):
            assert res.total == 5
            assert "doc0" == res.docs[0].id
        elif expects_resp3_shape(client):
            assert res["total_results"] == 5
            assert "doc0" == res["results"][0]["id"]

        attrs = client.ft().info()["attributes"]
        if expects_resp2_shape(client):
            sq8, sq8_default, plain = (dict(zip(a[::2], a[1::2])) for a in attrs)
            assert "INDEXMISSING" in attrs[2]
        else:
            sq8, sq8_default, plain = attrs
            assert plain["flags"] == ["INDEXMISSING"]
            assert sq8["flags"] == []

        assert sq8["algorithm"] == "HNSW"
        assert sq8["dim"] == 8
        assert sq8["M"] == 16
        assert sq8["compression"] == "SQ8"
        # An explicit zero is kept and not replaced by the default.
        assert sq8["training_threshold"] == 0
        assert sq8_default["compression"] == "SQ8"
        assert sq8_default["training_threshold"] == 10240
        assert "compression" not in plain
        assert "training_threshold" not in plain

    @pytest.mark.redismod
    @skip_if_server_version_lt("8.11.0")
    def test_hnsw_sq8_compression_errors(self, client):
        with pytest.raises(ResponseError, match="FLOAT32 and FLOAT16"):
            client.ft("idx_int8").create_index(
                (
                    VectorField(
                        "v",
                        "HNSW",
                        {
                            "TYPE": "INT8",
                            "DIM": 8,
                            "DISTANCE_METRIC": "L2",
                            "COMPRESSION": "SQ8",
                        },
                    ),
                )
            )

        with pytest.raises(ResponseError, match="compression was not requested"):
            client.ft("idx_no_compression").create_index(
                (
                    VectorField(
                        "v",
                        "HNSW",
                        {
                            "TYPE": "FLOAT32",
                            "DIM": 8,
                            "DISTANCE_METRIC": "L2",
                            "TRAINING_THRESHOLD": 1024,
                        },
                    ),
                )
            )

    @pytest.mark.fixed_client
    def test_normalize_info_attribute(self):
        # Pure parsing check of the RESP2 -> unified FT.INFO attribute
        # conversion, using the flat lists that RediSearch sends on RESP2.
        normalize = Search._normalize_info_attribute
        assert normalize(
            [
                "identifier", "v", "attribute", "v", "type", "VECTOR",
                "algorithm", "HNSW", "data_type", "FLOAT32", "dim", 4,
                "distance_metric", "L2", "M", 16, "ef_construction", 200,
                "ef_runtime", 10, "compression", "SQ8",
                "training_threshold", 0, "INDEXMISSING",
            ]
        ) == {
            "identifier": "v",
            "attribute": "v",
            "type": "VECTOR",
            "algorithm": "HNSW",
            "data_type": "FLOAT32",
            "dim": 4,
            "distance_metric": "L2",
            "M": 16,
            "ef_construction": 200,
            "ef_runtime": 10,
            "compression": "SQ8",
            "training_threshold": 0,
            "flags": ["INDEXMISSING"],
        }  # fmt: skip
        assert normalize(
            [
                b"identifier", b"g", b"attribute", b"g", b"type", b"GEOSHAPE",
                b"coord_system", b"FLAT", b"INDEXMISSING",
            ]
        ) == {
            "identifier": "g",
            "attribute": "g",
            "type": "GEOSHAPE",
            "coord_system": "FLAT",
            "flags": ["INDEXMISSING"],
        }  # fmt: skip
        assert normalize(
            [
                "identifier", "t", "attribute", "t", "type", "TAG",
                "SEPARATOR", ";", "CASESENSITIVE", "SORTABLE", "UNF",
            ]
        ) == {
            "identifier": "t",
            "attribute": "t",
            "type": "TAG",
            "SEPARATOR": ";",
            "flags": ["CASESENSITIVE", "SORTABLE", "UNF"],
        }  # fmt: skip

    @pytest.mark.redismod
    @skip_ifmodversion_lt("2.4.3", "search")
    def test_text_params(self, client):
        client.flushdb()
        client.ft().create_index((TextField("name"),))

        client.hset("doc1", mapping={"name": "Alice"})
        client.hset("doc2", mapping={"name": "Bob"})
        client.hset("doc3", mapping={"name": "Carol"})

        params_dict = {"name1": "Alice", "name2": "Bob"}
        q = Query("@name:($name1 | $name2 )")
        res = client.ft().search(q, query_params=params_dict)
        if expects_resp2_shape(client) or expects_unified_shape(client):
            assert 2 == res.total
            assert "doc1" == res.docs[0].id
            assert "doc2" == res.docs[1].id
        elif expects_resp3_shape(client):
            assert 2 == res["total_results"]
            assert "doc1" == res["results"][0]["id"]
            assert "doc2" == res["results"][1]["id"]

    @pytest.mark.redismod
    @skip_ifmodversion_lt("2.4.3", "search")
    def test_numeric_params(self, client):
        client.flushdb()
        client.ft().create_index((NumericField("numval"),))

        client.hset("doc1", mapping={"numval": 101})
        client.hset("doc2", mapping={"numval": 102})
        client.hset("doc3", mapping={"numval": 103})

        params_dict = {"min": 101, "max": 102}
        q = Query("@numval:[$min $max]")
        res = client.ft().search(q, query_params=params_dict)

        if expects_resp2_shape(client) or expects_unified_shape(client):
            assert 2 == res.total
            assert "doc1" == res.docs[0].id
            assert "doc2" == res.docs[1].id
        elif expects_resp3_shape(client):
            assert 2 == res["total_results"]
            assert "doc1" == res["results"][0]["id"]
            assert "doc2" == res["results"][1]["id"]

    @pytest.mark.redismod
    @skip_ifmodversion_lt("2.4.3", "search")
    def test_geo_params(self, client):
        client.ft().create_index(GeoField("g"))
        client.hset("doc1", mapping={"g": "29.69465, 34.95126"})
        client.hset("doc2", mapping={"g": "29.69350, 34.94737"})
        client.hset("doc3", mapping={"g": "29.68746, 34.94882"})

        params_dict = {
            "lat": "34.95126",
            "lon": "29.69465",
            "radius": 1000,
            "units": "km",
        }
        q = Query("@g:[$lon $lat $radius $units]")
        res = client.ft().search(q, query_params=params_dict)
        _assert_search_result(client, res, ["doc1", "doc2", "doc3"])

    @pytest.mark.redismod
    @skip_if_server_version_lt("7.4.0")
    @skip_ifmodversion_lt("2.10.0", "search")
    def test_geoshapes_query_intersects_and_disjoint(self, client):
        client.ft().create_index((GeoShapeField("g", coord_system=GeoShapeField.FLAT)))
        client.hset("doc_point1", mapping={"g": "POINT (10 10)"})
        client.hset("doc_point2", mapping={"g": "POINT (50 50)"})
        client.hset(
            "doc_polygon1", mapping={"g": "POLYGON ((20 20, 25 35, 35 25, 20 20))"}
        )
        client.hset(
            "doc_polygon2",
            mapping={"g": "POLYGON ((60 60, 65 75, 70 70, 65 55, 60 60))"},
        )

        intersection = client.ft().search(
            Query("@g:[intersects $shape]").dialect(3),
            query_params={"shape": "POLYGON((15 15, 75 15, 50 70, 20 40, 15 15))"},
        )
        _assert_search_result(client, intersection, ["doc_point2", "doc_polygon1"])

        disjunction = client.ft().search(
            Query("@g:[disjoint $shape]").dialect(3),
            query_params={"shape": "POLYGON((15 15, 75 15, 50 70, 20 40, 15 15))"},
        )
        _assert_search_result(client, disjunction, ["doc_point1", "doc_polygon2"])

    @pytest.mark.redismod
    @skip_ifmodversion_lt("2.10.0", "search")
    def test_geoshapes_query_contains_and_within(self, client):
        client.ft().create_index((GeoShapeField("g", coord_system=GeoShapeField.FLAT)))
        client.hset("doc_point1", mapping={"g": "POINT (10 10)"})
        client.hset("doc_point2", mapping={"g": "POINT (50 50)"})
        client.hset(
            "doc_polygon1", mapping={"g": "POLYGON ((20 20, 25 35, 35 25, 20 20))"}
        )
        client.hset(
            "doc_polygon2",
            mapping={"g": "POLYGON ((60 60, 65 75, 70 70, 65 55, 60 60))"},
        )

        contains_a = client.ft().search(
            Query("@g:[contains $shape]").dialect(3),
            query_params={"shape": "POINT(25 25)"},
        )
        _assert_search_result(client, contains_a, ["doc_polygon1"])

        contains_b = client.ft().search(
            Query("@g:[contains $shape]").dialect(3),
            query_params={"shape": "POLYGON((24 24, 24 26, 25 25, 24 24))"},
        )
        _assert_search_result(client, contains_b, ["doc_polygon1"])

        within = client.ft().search(
            Query("@g:[within $shape]").dialect(3),
            query_params={"shape": "POLYGON((15 15, 75 15, 50 70, 20 40, 15 15))"},
        )
        _assert_search_result(client, within, ["doc_point2", "doc_polygon1"])

    @pytest.mark.redismod
    @skip_if_server_version_lt("7.9.0")
    def test_vector_search_with_int8_type(self, client):
        client.ft().create_index(
            (
                VectorField(
                    "v", "FLAT", {"TYPE": "INT8", "DIM": 2, "DISTANCE_METRIC": "L2"}
                ),
            )
        )

        a = [1.5, 10]
        b = [123, 100]
        c = [1, 1]

        client.hset("a", "v", np.array(a, dtype=np.int8).tobytes())
        client.hset("b", "v", np.array(b, dtype=np.int8).tobytes())
        client.hset("c", "v", np.array(c, dtype=np.int8).tobytes())

        query = Query("*=>[KNN 2 @v $vec as score]").no_content()
        query_params = {"vec": np.array(a, dtype=np.int8).tobytes()}

        assert 2 in query.get_args()

        res = client.ft().search(query, query_params=query_params)
        if expects_resp2_shape(client) or expects_unified_shape(client):
            assert res.total == 2
        elif expects_resp3_shape(client):
            assert res["total_results"] == 2

    @pytest.mark.redismod
    @skip_if_server_version_lt("7.9.0")
    def test_vector_search_with_uint8_type(self, client):
        client.ft().create_index(
            (
                VectorField(
                    "v", "FLAT", {"TYPE": "UINT8", "DIM": 2, "DISTANCE_METRIC": "L2"}
                ),
            )
        )

        a = [1.5, 10]
        b = [123, 100]
        c = [1, 1]

        client.hset("a", "v", np.array(a, dtype=np.uint8).tobytes())
        client.hset("b", "v", np.array(b, dtype=np.uint8).tobytes())
        client.hset("c", "v", np.array(c, dtype=np.uint8).tobytes())

        query = Query("*=>[KNN 2 @v $vec as score]").no_content()
        query_params = {"vec": np.array(a, dtype=np.uint8).tobytes()}

        assert 2 in query.get_args()

        res = client.ft().search(query, query_params=query_params)
        if expects_resp2_shape(client) or expects_unified_shape(client):
            assert res.total == 2
        elif expects_resp3_shape(client):
            assert res["total_results"] == 2


class TestPipeline(SearchTestsBase):
    @pytest.mark.redismod
    @skip_if_redis_enterprise()
    def test_search_commands_in_pipeline(self, client):
        p = client.ft().pipeline()
        p.create_index((TextField("txt"),))
        p.hset("doc1", mapping={"txt": "foo bar"})
        p.hset("doc2", mapping={"txt": "foo bar"})
        q = Query("foo bar").with_payloads()
        p.search(q)
        res = p.execute()
        if expects_resp2_shape(client):
            assert res[:3] == ["OK", True, True]
            assert 2 == res[3][0]
            assert "doc1" == res[3][1]
            assert "doc2" == res[3][4]
            assert res[3][5] is None
            assert res[3][3] == res[3][6] == ["txt", "foo bar"]
        elif expects_unified_shape(client):
            assert res[:3] == ["OK", True, True]
            assert isinstance(res[3], Result)
            assert 2 == res[3].total
            assert "doc1" == res[3].docs[0].id
            assert "doc2" == res[3].docs[1].id
            assert res[3].docs[0].payload is None
            assert res[3].docs[0].txt == res[3].docs[1].txt == "foo bar"
        elif expects_resp3_shape(client):
            assert res[:3] == ["OK", True, True]
            assert 2 == res[3]["total_results"]
            assert "doc1" == res[3]["results"][0]["id"]
            assert "doc2" == res[3]["results"][1]["id"]
            assert res[3]["results"][0]["payload"] is None
            assert (
                res[3]["results"][0]["extra_attributes"]
                == res[3]["results"][1]["extra_attributes"]
                == {"txt": "foo bar"}
            )

    @pytest.mark.redismod
    def test_parsed_commands_return_the_pipeline(self, client):
        p = client.ft().pipeline()
        assert all(res is p for res in _queue_parsed_commands(p))
        p = client.pipeline()
        assert all(res is p for res in _queue_parsed_commands(p.ft()))

    @pytest.mark.redismod
    @pytest.mark.onlynoncluster
    @skip_if_redis_enterprise()
    def test_parsed_commands_execute_in_pipeline(self, client):
        client.ft().create_index((TextField("txt"),))
        client.hset("doc1", mapping={"txt": "foo bar"})
        client.ft().synupdate("id1", False, "foo", "bar")
        client.ft().sugadd("ac", Suggestion("hello"))
        p = client.ft().pipeline()
        p.info()
        p.aggregate(aggregations.AggregateRequest("*"))
        p.profile(Query("foo"))
        p.spellcheck("fo")
        p.syndump()
        p.sugget("ac", "he")
        assert len(p.execute()) == 6

    @pytest.mark.redismod
    @skip_if_redis_enterprise()
    def test_search_in_explicit_multi_matches_transaction(self, client):
        client.ft().create_index((TextField("txt"),))
        client.hset("doc1", mapping={"txt": "foo bar"})
        with client.ft().pipeline() as p:
            p.search(Query("foo bar"))
            expected = p.execute()
        with client.ft().pipeline(transaction=False) as p:
            p.multi()
            p.search(Query("foo bar"))
            res = p.execute()
        assert _search_docs(res[0]) == _search_docs(expected[0])

    @pytest.mark.redismod
    @skip_if_redis_enterprise()
    def test_search_through_pipeline_ft_is_decoded(self, client):
        client.ft().create_index((TextField("txt"),))
        client.hset("doc1", mapping={"txt": "foo bar"})
        with client.pipeline() as p:
            p.ft().search(Query("foo bar"))
            res = p.execute()
        assert not _contains_bytes(res[0])

    @pytest.mark.redismod
    @skip_if_redis_enterprise()
    @skip_if_server_version_lt("8.9.0")
    def test_aliaslist_in_pipeline(self, client):
        index = client.ft("aliaslistpipeidx")
        index.create_index((TextField("txt"),))

        # An empty listing normalizes to ``set()`` through the pipeline, not
        # the raw ``[]`` wire response.
        p = index.pipeline()
        p.aliaslist()
        res = p.execute()
        assert isinstance(res[0], set)
        assert res[0] == set()

        # Aliases are returned as an unordered set, matching the direct call.
        index.aliasadd("alias1")
        index.aliasadd("alias2")
        p = index.pipeline()
        p.aliaslist()
        res = p.execute()
        assert isinstance(res[0], set)
        assert res[0] == {"alias1", "alias2"}

    @pytest.mark.redismod
    @skip_if_server_version_lt("8.4.0")
    def test_hybrid_search_query_with_pipeline(self, client):
        p = client.ft().pipeline()
        p.create_index(
            (
                TextField("txt"),
                VectorField(
                    "embedding",
                    "FLAT",
                    {"TYPE": "FLOAT32", "DIM": 4, "DISTANCE_METRIC": "L2"},
                ),
            )
        )

        p.hset(
            "doc1",
            mapping={
                "txt": "foo bar",
                "embedding": np.array([1, 2, 3, 4], dtype=np.float32).tobytes(),
            },
        )
        p.hset(
            "doc2",
            mapping={
                "txt": "foo bar",
                "embedding": np.array([1, 2, 2, 3], dtype=np.float32).tobytes(),
            },
        )

        # set search query
        search_query = HybridSearchQuery("foo")

        vsim_query = HybridVsimQuery(
            vector_field_name="@embedding",
            vector_data="$vec",
        )

        hybrid_query = HybridQuery(
            search_query,
            vsim_query,
        )

        p.hybrid_search(
            query=hybrid_query,
            params_substitution={
                "vec": np.array([2, 2, 3, 3], dtype=np.float32).tobytes()
            },
        )
        res = p.execute()

        # the default results count limit is 10
        assert res[:3] == ["OK", 2, 2]
        hybrid_search_res = res[3]
        if expects_resp2_shape(client):
            # it doesn't get parsed to object in pipeline
            assert hybrid_search_res[0] == "total_results"
            assert hybrid_search_res[1] == 2
            assert hybrid_search_res[2] == "results"
            assert len(hybrid_search_res[3]) == 2
            assert hybrid_search_res[4] == "warnings"
            assert hybrid_search_res[5] == []
            assert hybrid_search_res[6] == "execution_time"
            assert float(hybrid_search_res[7]) > 0
        elif expects_unified_shape(client):
            assert hybrid_search_res.total_results == 2
            assert len(hybrid_search_res.results) == 2
            assert hybrid_search_res.warnings == []
            assert hybrid_search_res.execution_time > 0
        elif expects_resp3_shape(client):
            assert hybrid_search_res["total_results"] == 2
            assert len(hybrid_search_res["results"]) == 2
            assert hybrid_search_res["warnings"] == []
            assert hybrid_search_res["execution_time"] > 0


class TestSearchWithVamana(SearchTestsBase):
    @pytest.mark.redismod
    @skip_ifmodversion_lt("2.4.3", "search")
    @skip_if_server_version_lt("8.1.224")
    def test_svs_vamana_l2_distance_metric(self, client):
        client.ft().create_index(
            (
                VectorField(
                    "v",
                    "SVS-VAMANA",
                    {"TYPE": "FLOAT32", "DIM": 3, "DISTANCE_METRIC": "L2"},
                ),
            )
        )

        # L2 distance test vectors
        vectors = [[1.0, 0.0, 0.0], [2.0, 0.0, 0.0], [0.0, 1.0, 0.0], [5.0, 0.0, 0.0]]

        pipe = client.pipeline(transaction=False)
        for i, vec in enumerate(vectors):
            pipe.hset(f"doc{i}", "v", np.array(vec, dtype=np.float32).tobytes())
        pipe.execute()

        query = Query("*=>[KNN 3 @v $vec as score]").sort_by("score").no_content()
        query_params = {"vec": np.array(vectors[0], dtype=np.float32).tobytes()}

        res = client.ft().search(query, query_params=query_params)
        if expects_resp2_shape(client) or expects_unified_shape(client):
            assert res.total == 3
            assert "doc0" == res.docs[0].id
        elif expects_resp3_shape(client):
            assert res["total_results"] == 3
            assert "doc0" == res["results"][0]["id"]

    @pytest.mark.redismod
    @skip_ifmodversion_lt("2.4.3", "search")
    @skip_if_server_version_lt("8.1.224")
    def test_svs_vamana_cosine_distance_metric(self, client):
        client.ft().create_index(
            (
                VectorField(
                    "v",
                    "SVS-VAMANA",
                    {"TYPE": "FLOAT32", "DIM": 3, "DISTANCE_METRIC": "COSINE"},
                ),
            )
        )

        vectors = [
            [1.0, 0.0, 0.0],
            [0.707, 0.707, 0.0],
            [0.0, 1.0, 0.0],
            [-1.0, 0.0, 0.0],
        ]

        pipe = client.pipeline(transaction=False)
        for i, vec in enumerate(vectors):
            pipe.hset(f"doc{i}", "v", np.array(vec, dtype=np.float32).tobytes())
        pipe.execute()

        query = Query("*=>[KNN 3 @v $vec as score]").sort_by("score").no_content()
        query_params = {"vec": np.array(vectors[0], dtype=np.float32).tobytes()}

        res = client.ft().search(query, query_params=query_params)
        if expects_resp2_shape(client) or expects_unified_shape(client):
            assert res.total == 3
            assert "doc0" == res.docs[0].id
        elif expects_resp3_shape(client):
            assert res["total_results"] == 3
            assert "doc0" == res["results"][0]["id"]

    @pytest.mark.redismod
    @skip_ifmodversion_lt("2.4.3", "search")
    @skip_if_server_version_lt("8.1.224")
    def test_svs_vamana_ip_distance_metric(self, client):
        client.ft().create_index(
            (
                VectorField(
                    "v",
                    "SVS-VAMANA",
                    {"TYPE": "FLOAT32", "DIM": 3, "DISTANCE_METRIC": "IP"},
                ),
            )
        )

        vectors = [[1.0, 2.0, 3.0], [2.0, 1.0, 1.0], [3.0, 3.0, 3.0], [0.1, 0.1, 0.1]]

        pipe = client.pipeline(transaction=False)
        for i, vec in enumerate(vectors):
            pipe.hset(f"doc{i}", "v", np.array(vec, dtype=np.float32).tobytes())
        pipe.execute()

        query = Query("*=>[KNN 3 @v $vec as score]").sort_by("score").no_content()
        query_params = {"vec": np.array(vectors[0], dtype=np.float32).tobytes()}

        res = client.ft().search(query, query_params=query_params)
        if expects_resp2_shape(client) or expects_unified_shape(client):
            assert res.total == 3
            assert "doc2" == res.docs[0].id
        elif expects_resp3_shape(client):
            assert res["total_results"] == 3
            assert "doc2" == res["results"][0]["id"]

    @pytest.mark.redismod
    @skip_ifmodversion_lt("2.4.3", "search")
    @skip_if_server_version_lt("8.1.224")
    def test_svs_vamana_basic_functionality(self, client):
        client.ft().create_index(
            (
                VectorField(
                    "v",
                    "SVS-VAMANA",
                    {"TYPE": "FLOAT32", "DIM": 4, "DISTANCE_METRIC": "L2"},
                ),
            )
        )

        vectors = [
            [1.0, 2.0, 3.0, 4.0],
            [2.0, 3.0, 4.0, 5.0],
            [3.0, 4.0, 5.0, 6.0],
            [10.0, 11.0, 12.0, 13.0],
        ]

        pipe = client.pipeline(transaction=False)
        for i, vec in enumerate(vectors):
            pipe.hset(f"doc{i}", "v", np.array(vec, dtype=np.float32).tobytes())
        pipe.execute()

        query = "*=>[KNN 3 @v $vec]"
        q = Query(query).return_field("__v_score").sort_by("__v_score", True)
        res = client.ft().search(
            q, query_params={"vec": np.array(vectors[0], dtype=np.float32).tobytes()}
        )

        if expects_resp2_shape(client) or expects_unified_shape(client):
            assert res.total == 3
            assert "doc0" == res.docs[0].id  # Should be closest to itself
            assert "0" == res.docs[0].__getattribute__("__v_score")
        elif expects_resp3_shape(client):
            assert res["total_results"] == 3
            assert "doc0" == res["results"][0]["id"]
            assert "0" == res["results"][0]["extra_attributes"]["__v_score"]

    @pytest.mark.redismod
    @skip_ifmodversion_lt("2.4.3", "search")
    @skip_if_server_version_lt("8.1.224")
    def test_svs_vamana_float16_type(self, client):
        client.ft().create_index(
            (
                VectorField(
                    "v",
                    "SVS-VAMANA",
                    {"TYPE": "FLOAT16", "DIM": 4, "DISTANCE_METRIC": "L2"},
                ),
            )
        )

        vectors = [[1.5, 2.5, 3.5, 4.5], [2.5, 3.5, 4.5, 5.5], [3.5, 4.5, 5.5, 6.5]]

        pipe = client.pipeline(transaction=False)
        for i, vec in enumerate(vectors):
            pipe.hset(f"doc{i}", "v", np.array(vec, dtype=np.float16).tobytes())
        pipe.execute()

        query = Query("*=>[KNN 2 @v $vec as score]").no_content()
        query_params = {"vec": np.array(vectors[0], dtype=np.float16).tobytes()}

        res = client.ft().search(query, query_params=query_params)
        if expects_resp2_shape(client) or expects_unified_shape(client):
            assert res.total == 2
            assert "doc0" == res.docs[0].id
        elif expects_resp3_shape(client):
            assert res["total_results"] == 2
            assert "doc0" == res["results"][0]["id"]

    @pytest.mark.redismod
    @skip_ifmodversion_lt("2.4.3", "search")
    @skip_if_server_version_lt("8.1.224")
    def test_svs_vamana_float32_type(self, client):
        client.ft().create_index(
            (
                VectorField(
                    "v",
                    "SVS-VAMANA",
                    {"TYPE": "FLOAT32", "DIM": 4, "DISTANCE_METRIC": "L2"},
                ),
            )
        )

        vectors = [[1.0, 2.0, 3.0, 4.0], [2.0, 3.0, 4.0, 5.0], [3.0, 4.0, 5.0, 6.0]]

        pipe = client.pipeline(transaction=False)
        for i, vec in enumerate(vectors):
            pipe.hset(f"doc{i}", "v", np.array(vec, dtype=np.float32).tobytes())
        pipe.execute()

        query = Query("*=>[KNN 2 @v $vec as score]").no_content()
        query_params = {"vec": np.array(vectors[0], dtype=np.float32).tobytes()}

        res = client.ft().search(query, query_params=query_params)
        if expects_resp2_shape(client) or expects_unified_shape(client):
            assert res.total == 2
            assert "doc0" == res.docs[0].id
        elif expects_resp3_shape(client):
            assert res["total_results"] == 2
            assert "doc0" == res["results"][0]["id"]

    @pytest.mark.redismod
    @skip_ifmodversion_lt("2.4.3", "search")
    @skip_if_server_version_lt("8.1.224")
    def test_svs_vamana_vector_search_with_default_dialect(self, client):
        client.ft().create_index(
            (
                VectorField(
                    "v",
                    "SVS-VAMANA",
                    {"TYPE": "FLOAT32", "DIM": 2, "DISTANCE_METRIC": "L2"},
                ),
            )
        )

        client.hset("a", "v", "aaaaaaaa")
        client.hset("b", "v", "aaaabaaa")
        client.hset("c", "v", "aaaaabaa")

        query = "*=>[KNN 2 @v $vec]"
        q = Query(query).return_field("__v_score").sort_by("__v_score", True)
        res = client.ft().search(q, query_params={"vec": "aaaaaaaa"})

        if expects_resp2_shape(client) or expects_unified_shape(client):
            assert res.total == 2
        elif expects_resp3_shape(client):
            assert res["total_results"] == 2

    @pytest.mark.fixed_client
    def test_svs_vamana_vector_field_basic(self):
        # builds the field arguments only, so it needs no server or module
        field = VectorField(
            "v",
            "SVS-VAMANA",
            {"TYPE": "FLOAT32", "DIM": 128, "DISTANCE_METRIC": "COSINE"},
        )

        # Check that the field was created successfully
        assert field.name == "v"
        assert field.args[0] == "VECTOR"
        assert field.args[1] == "SVS-VAMANA"
        assert field.args[2] == 6
        assert "TYPE" in field.args
        assert "FLOAT32" in field.args
        assert "DIM" in field.args
        assert 128 in field.args
        assert "DISTANCE_METRIC" in field.args
        assert "COSINE" in field.args

    @pytest.mark.redismod
    @skip_ifmodversion_lt("2.4.3", "search")
    @skip_if_server_version_lt("8.1.224")
    def test_svs_vamana_lvq8_compression(self, client):
        client.ft().create_index(
            (
                VectorField(
                    "v",
                    "SVS-VAMANA",
                    {
                        "TYPE": "FLOAT32",
                        "DIM": 8,
                        "DISTANCE_METRIC": "L2",
                        "COMPRESSION": "LVQ8",
                        "TRAINING_THRESHOLD": 1024,
                    },
                ),
            )
        )

        vectors = []
        pipe = client.pipeline(transaction=False)
        for i in range(20):
            vec = [float(i + j) for j in range(8)]
            vectors.append(vec)
            pipe.hset(f"doc{i}", "v", np.array(vec, dtype=np.float32).tobytes())
        pipe.execute()

        query = Query("*=>[KNN 5 @v $vec as score]").no_content()
        query_params = {"vec": np.array(vectors[0], dtype=np.float32).tobytes()}

        res = client.ft().search(query, query_params=query_params)
        if expects_resp2_shape(client) or expects_unified_shape(client):
            assert res.total == 5
            assert "doc0" == res.docs[0].id
        elif expects_resp3_shape(client):
            assert res["total_results"] == 5
            assert "doc0" == res["results"][0]["id"]

    @pytest.mark.redismod
    @skip_ifmodversion_lt("2.4.3", "search")
    @skip_if_server_version_lt("8.1.224")
    def test_svs_vamana_compression_with_both_vector_types(self, client):
        # Test FLOAT16 with LVQ8
        client.ft("idx16").create_index(
            (
                VectorField(
                    "v16",
                    "SVS-VAMANA",
                    {
                        "TYPE": "FLOAT16",
                        "DIM": 8,
                        "DISTANCE_METRIC": "L2",
                        "COMPRESSION": "LVQ8",
                        "TRAINING_THRESHOLD": 1024,
                    },
                ),
            )
        )

        # Test FLOAT32 with LVQ8
        client.ft("idx32").create_index(
            (
                VectorField(
                    "v32",
                    "SVS-VAMANA",
                    {
                        "TYPE": "FLOAT32",
                        "DIM": 8,
                        "DISTANCE_METRIC": "L2",
                        "COMPRESSION": "LVQ8",
                        "TRAINING_THRESHOLD": 1024,
                    },
                ),
            )
        )

        # Add data to both indices
        pipe = client.pipeline(transaction=False)
        for i in range(15):
            vec = [float(i + j) for j in range(8)]
            pipe.hset(f"doc16_{i}", "v16", np.array(vec, dtype=np.float16).tobytes())
            pipe.hset(f"doc32_{i}", "v32", np.array(vec, dtype=np.float32).tobytes())
        pipe.execute()

        # Test both indices
        query = Query("*=>[KNN 3 @v16 $vec as score]").no_content()
        res16 = client.ft("idx16").search(
            query,
            query_params={
                "vec": np.array(
                    [1.0, 2.0, 3.0, 4.0, 5.0, 6.0, 7.0, 8.0], dtype=np.float16
                ).tobytes()
            },
        )

        query = Query("*=>[KNN 3 @v32 $vec as score]").no_content()
        res32 = client.ft("idx32").search(
            query,
            query_params={
                "vec": np.array(
                    [1.0, 2.0, 3.0, 4.0, 5.0, 6.0, 7.0, 8.0], dtype=np.float32
                ).tobytes()
            },
        )

        if expects_resp2_shape(client) or expects_unified_shape(client):
            assert res16.total == 3
            assert res32.total == 3
        elif expects_resp3_shape(client):
            assert res16["total_results"] == 3
            assert res32["total_results"] == 3

    @pytest.mark.fixed_client
    def test_svs_vamana_build_parameters_args(self):
        # Each build parameter is forwarded as given; the server accepting them
        # all together is test_svs_vamana_all_build_parameters_combined.
        attributes = {
            "TYPE": "FLOAT32",
            "DIM": 6,
            "DISTANCE_METRIC": "L2",
            "CONSTRUCTION_WINDOW_SIZE": 300,
            "GRAPH_MAX_DEGREE": 64,
            "SEARCH_WINDOW_SIZE": 20,
            "EPSILON": 0.05,
        }
        field = VectorField("v", "SVS-VAMANA", attributes)
        expected = ["VECTOR", "SVS-VAMANA", 2 * len(attributes)]
        for name, value in attributes.items():
            expected += [name, value]
        assert field.args == expected

    @pytest.mark.redismod
    @skip_ifmodversion_lt("2.4.3", "search")
    @skip_if_server_version_lt("8.1.224")
    def test_svs_vamana_all_build_parameters_combined(self, client):
        client.ft().create_index(
            (
                VectorField(
                    "v",
                    "SVS-VAMANA",
                    {
                        "TYPE": "FLOAT32",
                        "DIM": 8,
                        "DISTANCE_METRIC": "IP",
                        "CONSTRUCTION_WINDOW_SIZE": 250,
                        "GRAPH_MAX_DEGREE": 48,
                        "SEARCH_WINDOW_SIZE": 15,
                        "EPSILON": 0.02,
                    },
                ),
            )
        )

        vectors = []
        pipe = client.pipeline(transaction=False)
        for i in range(35):
            vec = [float(i + j) for j in range(8)]
            vectors.append(vec)
            pipe.hset(f"doc{i}", "v", np.array(vec, dtype=np.float32).tobytes())
        pipe.execute()

        query = Query("*=>[KNN 7 @v $vec as score]").no_content()
        query_params = {"vec": np.array(vectors[0], dtype=np.float32).tobytes()}

        res = client.ft().search(query, query_params=query_params)
        if expects_resp2_shape(client) or expects_unified_shape(client):
            assert res.total == 7
            doc_ids = [doc.id for doc in res.docs]
            assert len(doc_ids) == 7
        elif expects_resp3_shape(client):
            assert res["total_results"] == 7
            doc_ids = [doc["id"] for doc in res["results"]]
            assert len(doc_ids) == 7

    @pytest.mark.redismod
    @skip_ifmodversion_lt("2.4.3", "search")
    @skip_if_server_version_lt("8.1.224")
    def test_svs_vamana_comprehensive_configuration(self, client):
        client.flushdb()
        client.ft().create_index(
            (
                VectorField(
                    "v",
                    "SVS-VAMANA",
                    {
                        "TYPE": "FLOAT16",
                        "DIM": 32,
                        "DISTANCE_METRIC": "COSINE",
                        "COMPRESSION": "LVQ8",
                        "CONSTRUCTION_WINDOW_SIZE": 400,
                        "GRAPH_MAX_DEGREE": 96,
                        "SEARCH_WINDOW_SIZE": 25,
                        "EPSILON": 0.03,
                        "TRAINING_THRESHOLD": 2048,
                    },
                ),
            )
        )

        vectors = []
        pipe = client.pipeline(transaction=False)
        for i in range(60):
            vec = [float(i + j) for j in range(32)]
            vectors.append(vec)
            pipe.hset(f"doc{i}", "v", np.array(vec, dtype=np.float16).tobytes())
        pipe.execute()

        query = Query("*=>[KNN 10 @v $vec as score]").no_content()
        query_params = {"vec": np.array(vectors[0], dtype=np.float16).tobytes()}

        res = client.ft().search(query, query_params=query_params)
        if expects_resp2_shape(client) or expects_unified_shape(client):
            assert res.total == 10
            assert "doc0" == res.docs[0].id
        elif expects_resp3_shape(client):
            assert res["total_results"] == 10
            assert "doc0" == res["results"][0]["id"]

    @pytest.mark.redismod
    @skip_ifmodversion_lt("2.4.3", "search")
    @skip_if_server_version_lt("8.1.224")
    def test_svs_vamana_hybrid_text_vector_search(self, client):
        client.flushdb()
        client.ft().create_index(
            (
                TextField("title"),
                TextField("content"),
                VectorField(
                    "embedding",
                    "SVS-VAMANA",
                    {
                        "TYPE": "FLOAT32",
                        "DIM": 6,
                        "DISTANCE_METRIC": "COSINE",
                        "SEARCH_WINDOW_SIZE": 20,
                    },
                ),
            )
        )

        docs = [
            {
                "title": "AI Research",
                "content": "machine learning algorithms",
                "embedding": [1.0, 2.0, 3.0, 4.0, 5.0, 6.0],
            },
            {
                "title": "Data Science",
                "content": "statistical analysis methods",
                "embedding": [2.0, 3.0, 4.0, 5.0, 6.0, 7.0],
            },
            {
                "title": "Deep Learning",
                "content": "neural network architectures",
                "embedding": [3.0, 4.0, 5.0, 6.0, 7.0, 8.0],
            },
            {
                "title": "Computer Vision",
                "content": "image processing techniques",
                "embedding": [10.0, 11.0, 12.0, 13.0, 14.0, 15.0],
            },
        ]

        pipe = client.pipeline(transaction=False)
        for i, doc in enumerate(docs):
            pipe.hset(
                f"doc{i}",
                mapping={
                    "title": doc["title"],
                    "content": doc["content"],
                    "embedding": np.array(doc["embedding"], dtype=np.float32).tobytes(),
                },
            )
        pipe.execute()

        # Hybrid query - text filter + vector similarity
        query = "(@title:AI|@content:machine)=>[KNN 2 @embedding $vec]"
        q = (
            Query(query)
            .return_field("__embedding_score")
            .sort_by("__embedding_score", True)
        )
        res = client.ft().search(
            q,
            query_params={
                "vec": np.array(
                    [1.0, 2.0, 3.0, 4.0, 5.0, 6.0], dtype=np.float32
                ).tobytes()
            },
        )

        if expects_resp2_shape(client) or expects_unified_shape(client):
            assert res.total >= 1
            doc_ids = [doc.id for doc in res.docs]
            assert "doc0" in doc_ids
        elif expects_resp3_shape(client):
            assert res["total_results"] >= 1
            doc_ids = [doc["id"] for doc in res["results"]]
            assert "doc0" in doc_ids

    @pytest.mark.redismod
    @skip_ifmodversion_lt("2.4.3", "search")
    @skip_if_server_version_lt("8.1.224")
    def test_svs_vamana_large_dimension_vectors(self, client):
        client.flushdb()
        client.ft().create_index(
            (
                VectorField(
                    "v",
                    "SVS-VAMANA",
                    {
                        "TYPE": "FLOAT32",
                        "DIM": 512,
                        "DISTANCE_METRIC": "L2",
                        "CONSTRUCTION_WINDOW_SIZE": 300,
                        "GRAPH_MAX_DEGREE": 64,
                    },
                ),
            )
        )

        vectors = []
        pipe = client.pipeline(transaction=False)
        for i in range(10):
            vec = [float(i + j) for j in range(512)]
            vectors.append(vec)
            pipe.hset(f"doc{i}", "v", np.array(vec, dtype=np.float32).tobytes())
        pipe.execute()

        query = Query("*=>[KNN 5 @v $vec as score]").no_content()
        query_params = {"vec": np.array(vectors[0], dtype=np.float32).tobytes()}

        res = client.ft().search(query, query_params=query_params)
        if expects_resp2_shape(client) or expects_unified_shape(client):
            assert res.total == 5
            assert "doc0" == res.docs[0].id
        elif expects_resp3_shape(client):
            assert res["total_results"] == 5
            assert "doc0" == res["results"][0]["id"]

    @pytest.mark.redismod
    @skip_ifmodversion_lt("2.4.3", "search")
    @skip_if_server_version_lt("8.1.224")
    def test_svs_vamana_training_threshold_behavior(self, client):
        client.ft().create_index(
            (
                VectorField(
                    "v",
                    "SVS-VAMANA",
                    {
                        "TYPE": "FLOAT32",
                        "DIM": 8,
                        "DISTANCE_METRIC": "L2",
                        "COMPRESSION": "LVQ8",
                        "TRAINING_THRESHOLD": 1024,
                    },
                ),
            )
        )

        vectors = []
        for i in range(20):
            vec = [float(i + j) for j in range(8)]
            vectors.append(vec)
            client.hset(f"doc{i}", "v", np.array(vec, dtype=np.float32).tobytes())

            if i >= 5:
                query = Query("*=>[KNN 3 @v $vec as score]").no_content()
                query_params = {"vec": np.array(vectors[0], dtype=np.float32).tobytes()}
                res = client.ft().search(query, query_params=query_params)

                if expects_resp2_shape(client) or expects_unified_shape(client):
                    assert res.total >= 1
                elif expects_resp3_shape(client):
                    assert res["total_results"] >= 1

    @pytest.mark.redismod
    @skip_ifmodversion_lt("2.4.3", "search")
    @skip_if_server_version_lt("8.1.224")
    def test_svs_vamana_different_k_values(self, client):
        client.ft().create_index(
            (
                VectorField(
                    "v",
                    "SVS-VAMANA",
                    {
                        "TYPE": "FLOAT32",
                        "DIM": 6,
                        "DISTANCE_METRIC": "L2",
                        "SEARCH_WINDOW_SIZE": 15,
                    },
                ),
            )
        )

        vectors = []
        pipe = client.pipeline(transaction=False)
        for i in range(25):
            vec = [float(i + j) for j in range(6)]
            vectors.append(vec)
            pipe.hset(f"doc{i}", "v", np.array(vec, dtype=np.float32).tobytes())
        pipe.execute()

        for k in [1, 3, 5, 10, 15]:
            query = Query(f"*=>[KNN {k} @v $vec as score]").no_content()
            query_params = {"vec": np.array(vectors[0], dtype=np.float32).tobytes()}
            res = client.ft().search(query, query_params=query_params)

            if expects_resp2_shape(client) or expects_unified_shape(client):
                assert res.total == k
                assert "doc0" == res.docs[0].id
            elif expects_resp3_shape(client):
                assert res["total_results"] == k
                assert "doc0" == res["results"][0]["id"]

    @pytest.mark.redismod
    @skip_ifmodversion_lt("2.4.3", "search")
    @skip_if_server_version_lt("8.1.224")
    def test_svs_vamana_vector_field_error(self, client):
        # sortable tag
        with pytest.raises(Exception):
            client.ft().create_index(
                (VectorField("v", "SVS-VAMANA", {}, sortable=True),)
            )

        # no_index tag
        with pytest.raises(Exception):
            client.ft().create_index(
                (VectorField("v", "SVS-VAMANA", {}, no_index=True),)
            )

    @pytest.mark.redismod
    @skip_ifmodversion_lt("2.4.3", "search")
    @skip_if_server_version_lt("8.1.224")
    def test_svs_vamana_vector_search_with_parameters(self, client):
        client.ft().create_index(
            (
                VectorField(
                    "v",
                    "SVS-VAMANA",
                    {
                        "TYPE": "FLOAT32",
                        "DIM": 4,
                        "DISTANCE_METRIC": "L2",
                        "CONSTRUCTION_WINDOW_SIZE": 200,
                        "GRAPH_MAX_DEGREE": 64,
                        "SEARCH_WINDOW_SIZE": 40,
                        "EPSILON": 0.01,
                    },
                ),
            )
        )

        # Create test vectors
        vectors = [
            [1.0, 2.0, 3.0, 4.0],
            [2.0, 3.0, 4.0, 5.0],
            [3.0, 4.0, 5.0, 6.0],
            [4.0, 5.0, 6.0, 7.0],
            [5.0, 6.0, 7.0, 8.0],
        ]

        for i, vec in enumerate(vectors):
            client.hset(f"doc{i}", "v", np.array(vec, dtype=np.float32).tobytes())

        query = Query("*=>[KNN 3 @v $vec as score]").no_content()
        query_params = {"vec": np.array(vectors[0], dtype=np.float32).tobytes()}

        res = client.ft().search(query, query_params=query_params)
        if expects_resp2_shape(client) or expects_unified_shape(client):
            assert res.total == 3
            assert "doc0" == res.docs[0].id
        elif expects_resp3_shape(client):
            assert res["total_results"] == 3
            assert "doc0" == res["results"][0]["id"]

    @pytest.mark.redismod
    @skip_ifmodversion_lt("2.4.3", "search")
    @skip_if_server_version_lt("8.1.224")
    def test_svs_vamana_vector_search_with_parameters_leanvec(self, client):
        client.ft().create_index(
            (
                VectorField(
                    "v",
                    "SVS-VAMANA",
                    {
                        "TYPE": "FLOAT32",
                        "DIM": 8,
                        "DISTANCE_METRIC": "L2",
                        "COMPRESSION": "LeanVec8x8",  # LeanVec compression required for REDUCE
                        "CONSTRUCTION_WINDOW_SIZE": 200,
                        "GRAPH_MAX_DEGREE": 32,
                        "SEARCH_WINDOW_SIZE": 15,
                        "EPSILON": 0.01,
                        "TRAINING_THRESHOLD": 1024,
                        "REDUCE": 4,  # Half of DIM (8/2 = 4)
                    },
                ),
            )
        )

        # Create test vectors (8-dimensional to match DIM)
        vectors = [
            [1.0, 2.0, 3.0, 4.0, 5.0, 6.0, 7.0, 8.0],
            [2.0, 3.0, 4.0, 5.0, 6.0, 7.0, 8.0, 9.0],
            [3.0, 4.0, 5.0, 6.0, 7.0, 8.0, 9.0, 10.0],
            [4.0, 5.0, 6.0, 7.0, 8.0, 9.0, 10.0, 11.0],
            [5.0, 6.0, 7.0, 8.0, 9.0, 10.0, 11.0, 12.0],
        ]

        for i, vec in enumerate(vectors):
            client.hset(f"doc{i}", "v", np.array(vec, dtype=np.float32).tobytes())

        query = Query("*=>[KNN 3 @v $vec as score]").no_content()
        query_params = {"vec": np.array(vectors[0], dtype=np.float32).tobytes()}

        res = client.ft().search(query, query_params=query_params)
        if expects_resp2_shape(client) or expects_unified_shape(client):
            assert res.total == 3
            assert "doc0" == res.docs[0].id
        elif expects_resp3_shape(client):
            assert res["total_results"] == 3
            assert "doc0" == res["results"][0]["id"]


class TestHybridSearch(SearchTestsBase):
    _HYBRID_TIMEOUT_DIM = 8192
    # The 1ms timeout only fires once the query runs long enough to hit a
    # server timeout checkpoint; smaller data sets slip through and return no
    # warnings. 6000 docs keeps the query comfortably above the 1ms limit so
    # the timeout reliably triggers across hardware.
    _HYBRID_TIMEOUT_DOCS = 6000

    @pytest.mark.fixed_client
    def test_hybrid_search_forwards_zero_timeout(self):
        # TIMEOUT 0 means "no timeout" and is a value the server accepts for
        # FT.HYBRID, so it must reach the wire. Guarding the argument with a
        # truthiness test made timeout=0 indistinguishable from timeout=None,
        # which silently downgrades the query to the server's search-timeout
        # default instead of running it unlimited. Query.get_args() already
        # emits TIMEOUT 0 (see test_query_timeout); this keeps FT.HYBRID
        # consistent with it. Runs without a server.
        hybrid_query = HybridQuery(
            HybridSearchQuery("foo"),
            HybridVsimQuery(vector_field_name="@embedding", vector_data="$vec"),
        )
        ft = redis.Redis().ft("idx")

        def wire_args(timeout):
            with mock.patch.object(ft, "execute_command", return_value={}) as m:
                ft.hybrid_search(query=hybrid_query, timeout=timeout)
                return list(m.call_args[0])

        assert wire_args(5000)[-2:] == ["TIMEOUT", 5000]
        assert wire_args(0)[-2:] == ["TIMEOUT", 0]
        # None stays the "argument not set" sentinel.
        assert "TIMEOUT" not in wire_args(None)

    def _create_hybrid_search_index(self, client, dim=4):
        client.ft().create_index(
            (
                TextField("description"),
                NumericField("price"),
                TagField("color"),
                TagField("item_type"),
                NumericField("size"),
                VectorField(
                    "embedding",
                    "FLAT",
                    {
                        "TYPE": "FLOAT32",
                        "DIM": dim,
                        "DISTANCE_METRIC": "L2",
                    },
                ),
                VectorField(
                    "embedding-hnsw",
                    "HNSW",
                    {
                        "TYPE": "FLOAT32",
                        "DIM": dim,
                        "DISTANCE_METRIC": "L2",
                    },
                ),
            ),
            definition=IndexDefinition(prefix=["item:"]),
        )
        SearchTestsBase.waitForIndex(client, "idx")

    def _create_hybrid_search_timeout_index(self, client):
        client.ft().create_index(
            (
                TextField("description"),
                VectorField(
                    "embedding",
                    "FLAT",
                    {
                        "TYPE": "FLOAT32",
                        "DIM": self._HYBRID_TIMEOUT_DIM,
                        "DISTANCE_METRIC": "L2",
                    },
                ),
            ),
            definition=IndexDefinition(prefix=["timeout-item:"]),
        )
        SearchTestsBase.waitForIndex(client, "idx")

    def _add_data_for_hybrid_search_timeout(self, client):
        vectors = [
            np.full(self._HYBRID_TIMEOUT_DIM, value, dtype=np.float32).tobytes()
            for value in (0.1, 0.2, 0.3, 0.4, 0.5)
        ]
        pipeline = client.pipeline(transaction=False)
        # the documents are independent, so no MULTI/EXEC. Each carries a 32 KB
        # vector and is indexed as it is read; keep batches small so one async
        # pipeline write (bounded by socket_timeout) stays short on slow runners
        batch_size = 250
        for i in range(self._HYBRID_TIMEOUT_DOCS):
            pipeline.hset(
                f"timeout-item:{i}",
                mapping={
                    "description": "red shoes",
                    "embedding": vectors[i % len(vectors)],
                },
            )
            if (i + 1) % batch_size == 0:
                pipeline.execute()
                pipeline = client.pipeline(transaction=False)
        pipeline.execute()

    @staticmethod
    def _generate_random_vector(dim):
        return [random.random() for _ in range(dim)]

    @staticmethod
    def _generate_random_str_data(dim):
        chars = "abcdefgh12345678"
        return "".join(random.choice(chars) for _ in range(dim))

    @staticmethod
    def _add_data_for_hybrid_search(
        client,
        items_sets=1,
        randomize_data=False,
        dim_for_random_data=4,
        use_random_str_data=False,
    ):
        if randomize_data or use_random_str_data:
            generate_data_func = (
                TestHybridSearch._generate_random_str_data
                if use_random_str_data
                else TestHybridSearch._generate_random_vector
            )

            dim_for_random_data = (
                dim_for_random_data * 4 if use_random_str_data else dim_for_random_data
            )

            items = [
                (generate_data_func(dim_for_random_data), "red shoes"),
                (generate_data_func(dim_for_random_data), "green shoes with red laces"),
                (generate_data_func(dim_for_random_data), "red dress"),
                (generate_data_func(dim_for_random_data), "orange dress"),
                (generate_data_func(dim_for_random_data), "black shoes"),
            ]
        else:
            items = [
                ([1.0, 2.0, 7.0, 8.0], "red shoes"),
                ([1.0, 4.0, 7.0, 8.0], "green shoes with red laces"),
                ([1.0, 2.0, 6.0, 5.0], "red dress"),
                ([2.0, 3.0, 6.0, 5.0], "orange dress"),
                ([5.0, 6.0, 7.0, 8.0], "black shoes"),
            ]
        items = items * items_sets

        batch_size = 1000
        pipeline = client.pipeline(transaction=False)
        for i, vec in enumerate(items):
            vec, description = vec
            mapping = {
                "description": description,
                "embedding": np.array(vec, dtype=np.float32).tobytes()
                if not use_random_str_data
                else vec,
                "embedding-hnsw": np.array(vec, dtype=np.float32).tobytes()
                if not use_random_str_data
                else vec,
                "price": 15 + i % 4,
                "color": description.split(" ")[0],
                "item_type": description.split(" ")[1],
                "size": 10 + i % 3,
            }

            pipeline.hset(
                f"item:{i}",
                mapping=mapping,
            )
            if (i + 1) % batch_size == 0:
                pipeline.execute()
                pipeline = client.pipeline(transaction=False)
        pipeline.execute()  # Execute remaining

    @staticmethod
    def _convert_dict_values_to_str(list_of_dicts):
        res = []
        for d in list_of_dicts:
            res_dict = {}
            for k, v in d.items():
                if isinstance(v, list):
                    res_dict[k] = [safe_str(x) for x in v]
                else:
                    res_dict[k] = safe_str(v)
            res.append(res_dict)
        return res

    @staticmethod
    def compare_list_of_dicts(actual, expected):
        assert len(actual) == len(expected), (
            f"List of dicts length mismatch: {len(actual)} != {len(expected)}. "
            f"Full dicts: actual:{actual}; expected:{expected}"
        )
        for expected_dict_item in expected:
            found = False
            for actual_dict_item in actual:
                if actual_dict_item == expected_dict_item:
                    found = True
                    break
            if not found:
                assert False, (
                    f"Dict {expected_dict_item} not found in actual list of dicts: {actual}. "
                    f"All expected:{expected}"
                )

    @pytest.mark.redismod
    @skip_if_server_version_lt("8.4.0")
    def test_basic_hybrid_search(self, client):
        # Create index and add data
        self._create_hybrid_search_index(client)
        self._add_data_for_hybrid_search(client, items_sets=5)

        # set search query
        search_query = HybridSearchQuery("@color:{red} @color:{green}")

        vsim_query = HybridVsimQuery(
            vector_field_name="@embedding",
            vector_data="$vec",
        )

        hybrid_query = HybridQuery(search_query, vsim_query)

        res = client.ft().hybrid_search(
            query=hybrid_query,
            params_substitution={
                "vec": np.array([-100, -200, -200, -300], dtype=np.float32).tobytes()
            },
        )

        # the default results count limit is 10
        if expects_resp2_shape(client) or expects_unified_shape(client):
            assert res.total_results == 10
            assert len(res.results) == 10
            assert res.warnings == []
            assert res.execution_time > 0
            assert all(isinstance(res.results[i]["__score"], bytes) for i in range(10))
            assert all(isinstance(res.results[i]["__key"], bytes) for i in range(10))
        elif expects_resp3_shape(client):
            assert res["total_results"] == 10
            assert len(res["results"]) == 10
            assert res["warnings"] == []
            assert res["execution_time"] > 0
            assert all(
                isinstance(res["results"][i]["__score"], bytes) for i in range(10)
            )
            assert all(isinstance(res["results"][i]["__key"], bytes) for i in range(10))

    @pytest.mark.redismod
    @skip_if_server_version_lt("8.3.224")
    def test_hybrid_search_query_with_scorer(self, client):
        # Create index and add data
        self._create_hybrid_search_index(client)
        self._add_data_for_hybrid_search(client, items_sets=10)

        # set search query
        search_query = HybridSearchQuery("shoes")
        search_query.scorer("TFIDF")

        vsim_query = HybridVsimQuery(
            vector_field_name="@embedding",
            vector_data="$vec",
        )

        hybrid_query = HybridQuery(search_query, vsim_query)

        combine_config = CombineResultsMethod(
            CombinationMethods.LINEAR, ALPHA=1, BETA=0
        )

        postprocessing_config = HybridPostProcessingConfig()
        postprocessing_config.load(
            "@description", "@color", "@price", "@size", "@__score", "@__item"
        )
        postprocessing_config.limit(0, 2)

        res = client.ft().hybrid_search(
            query=hybrid_query,
            combine_method=combine_config,
            post_processing=postprocessing_config,
            params_substitution={
                "vec": np.array([1, 2, 2, 3], dtype=np.float32).tobytes()
            },
            timeout=10,
        )

        expected_results_tfidf = [
            {
                "description": b"red shoes",
                "color": b"red",
                "price": b"15",
                "size": b"10",
                "__score": b"2",
            },
            {
                "description": b"green shoes with red laces",
                "color": b"green",
                "price": b"16",
                "size": b"11",
                "__score": b"2",
            },
        ]

        if expects_resp2_shape(client) or expects_unified_shape(client):
            assert res.total_results >= 2
            assert len(res.results) == 2
            assert res.results == expected_results_tfidf
            assert res.warnings == []
        elif expects_resp3_shape(client):
            assert res["total_results"] >= 2
            assert len(res["results"]) == 2
            assert res["results"] == expected_results_tfidf
            assert res["warnings"] == []

        search_query.scorer("BM25")
        res = client.ft().hybrid_search(
            query=hybrid_query,
            combine_method=combine_config,
            post_processing=postprocessing_config,
            params_substitution={
                "vec": np.array([1, 2, 2, 3], dtype=np.float32).tobytes()
            },
            timeout=10,
        )
        expected_results_bm25 = [
            {
                "description": b"red shoes",
                "color": b"red",
                "price": b"15",
                "size": b"10",
                "__score": b"0.657894719299",
            },
            {
                "description": b"green shoes with red laces",
                "color": b"green",
                "price": b"16",
                "size": b"11",
                "__score": b"0.657894719299",
            },
        ]
        if expects_resp2_shape(client) or expects_unified_shape(client):
            assert res.total_results >= 2
            assert len(res.results) == 2
            assert res.results == expected_results_bm25
            assert res.warnings == []
        elif expects_resp3_shape(client):
            assert res["total_results"] >= 2
            assert len(res["results"]) == 2
            assert res["results"] == expected_results_bm25
            assert res["warnings"] == []

    @pytest.mark.redismod
    @skip_if_server_version_lt("8.3.224")
    def test_hybrid_search_query_with_supported_scorer(self, client):
        # Create index and add data
        self._create_hybrid_search_index(client)
        self._add_data_for_hybrid_search(client, items_sets=10)

        # set search query
        search_query = HybridSearchQuery("shoes")

        vsim_query = HybridVsimQuery(
            vector_field_name="@embedding",
            vector_data="$vec",
        )

        hybrid_query = HybridQuery(search_query, vsim_query)

        supported_scorers = [
            "TFIDF",
            "TFIDF.DOCNORM",
            "BM25",
            "BM25STD",
            "BM25STD.TANH",
            "DISMAX",
            "DOCSCORE",
            "HAMMING",
        ]
        for scorer in supported_scorers:
            search_query.scorer(scorer)

            res = client.ft().hybrid_search(
                query=hybrid_query,
                params_substitution={
                    "vec": np.array([1, 2, 2, 3], dtype=np.float32).tobytes()
                },
                timeout=10,
            )
            assert res is not None

    @pytest.mark.redismod
    @skip_if_server_version_lt("8.3.224")
    def test_hybrid_search_query_with_vsim_method_defined_query_init(self, client):
        # Create index and add data
        self._create_hybrid_search_index(client)
        self._add_data_for_hybrid_search(client, items_sets=5, use_random_str_data=True)
        # set search query
        search_query = HybridSearchQuery("shoes")

        vsim_query = HybridVsimQuery(
            vector_field_name="@embedding-hnsw",
            vector_data="$vec",
            vsim_search_method=VectorSearchMethods.KNN,
            vsim_search_method_params={"K": 3, "EF_RUNTIME": 1},
        )

        hybrid_query = HybridQuery(search_query, vsim_query)

        res = client.ft().hybrid_search(
            query=hybrid_query,
            params_substitution={"vec": "abcd1234efgh5678"},
            timeout=10,
        )
        if expects_resp2_shape(client) or expects_unified_shape(client):
            assert len(res.results) > 0
            assert res.warnings == []
        elif expects_resp3_shape(client):
            assert len(res["results"]) > 0
            assert res["warnings"] == []

    @pytest.mark.redismod
    @skip_if_server_version_lt("8.3.224")
    def test_hybrid_search_query_with_vsim_filter(self, client):
        # Create index and add data
        self._create_hybrid_search_index(client)
        self._add_data_for_hybrid_search(client, items_sets=5, use_random_str_data=True)

        search_query = HybridSearchQuery("@color:{missing}")

        vsim_query = HybridVsimQuery(
            vector_field_name="@embedding",
            vector_data="$vec",
        )
        vsim_query.filter(HybridFilter("@price:[15 16] @size:[10 11]"))

        hybrid_query = HybridQuery(search_query, vsim_query)

        postprocessing_config = HybridPostProcessingConfig()
        postprocessing_config.load("@price", "@size")

        res = client.ft().hybrid_search(
            query=hybrid_query,
            post_processing=postprocessing_config,
            params_substitution={
                "vec": np.array([1, 2, 2, 3], dtype=np.float32).tobytes()
            },
            timeout=10,
        )
        if expects_resp2_shape(client) or expects_unified_shape(client):
            assert len(res.results) > 0
            assert res.warnings == []
            for item in res.results:
                assert item["price"] in [b"15", b"16"]
                assert item["size"] in [b"10", b"11"]
        elif expects_resp3_shape(client):
            assert len(res["results"]) > 0
            assert res["warnings"] == []
            for item in res["results"]:
                assert item["price"] in [b"15", b"16"]
                assert item["size"] in [b"10", b"11"]

    @pytest.mark.redismod
    @skip_if_server_version_lt("8.3.224")
    def test_hybrid_search_query_with_search_score_aliases(self, client):
        # Create index and add data
        self._create_hybrid_search_index(client)
        self._add_data_for_hybrid_search(client, items_sets=1, use_random_str_data=True)

        search_query = HybridSearchQuery("shoes")
        search_query.yield_score_as("search_score")

        vsim_query = HybridVsimQuery(
            vector_field_name="@embedding",
            vector_data="$vec",
        )

        hybrid_query = HybridQuery(search_query, vsim_query)

        res = client.ft().hybrid_search(
            query=hybrid_query,
            params_substitution={"vec": "abcd1234efgh5678"},
            timeout=10,
        )

        if expects_resp2_shape(client) or expects_unified_shape(client):
            assert len(res.results) > 0
            assert res.warnings == []
            for item in res.results:
                if item["__key"] in [b"item:0", b"item:1", b"item:4"]:
                    assert item["search_score"] is not None
                    assert item["__score"] is not None
                else:
                    assert "search_score" not in item
                    assert item["__score"] is not None

        elif expects_resp3_shape(client):
            assert len(res["results"]) > 0
            assert res["warnings"] == []
            for item in res["results"]:
                if item["__key"] in [b"item:0", b"item:1", b"item:4"]:
                    assert item["search_score"] is not None
                    assert item["__score"] is not None
                else:
                    assert "search_score" not in item
                    assert item["__score"] is not None

    @pytest.mark.redismod
    @skip_if_server_version_lt("8.3.224")
    def test_hybrid_search_query_with_vsim_score_aliases(self, client):
        # Create index and add data
        self._create_hybrid_search_index(client)
        # The assertions name the exact KNN winners, so keep vectors deterministic.
        self._add_data_for_hybrid_search(client, items_sets=1)

        search_query = HybridSearchQuery("shoes")

        vsim_query = HybridVsimQuery(
            vector_field_name="@embedding-hnsw",
            vector_data="$vec",
            vsim_search_method=VectorSearchMethods.KNN,
            vsim_search_method_params={"K": 3, "EF_RUNTIME": 1},
            yield_score_as="vsim_score",
        )

        hybrid_query = HybridQuery(search_query, vsim_query)

        res = client.ft().hybrid_search(
            query=hybrid_query,
            params_substitution={
                "vec": np.array([1, 2, 7, 8], dtype=np.float32).tobytes()
            },
            timeout=10,
        )

        if expects_resp2_shape(client) or expects_unified_shape(client):
            assert len(res.results) > 0
            assert res.warnings == []
            for item in res.results:
                if item["__key"] in [b"item:0", b"item:1", b"item:2"]:
                    assert item["vsim_score"] is not None
                    assert item["__score"] is not None
                else:
                    assert "vsim_score" not in item
                    assert item["__score"] is not None

        elif expects_resp3_shape(client):
            assert len(res["results"]) > 0
            assert res["warnings"] == []
            for item in res["results"]:
                if item["__key"] in [b"item:0", b"item:1", b"item:2"]:
                    assert item["vsim_score"] is not None
                    assert item["__score"] is not None
                else:
                    assert "vsim_score" not in item
                    assert item["__score"] is not None

    @pytest.mark.redismod
    @skip_if_server_version_lt("8.3.224")
    def test_hybrid_search_query_with_combine_score_aliases(self, client):
        # Create index and add data
        self._create_hybrid_search_index(client)
        self._add_data_for_hybrid_search(client, items_sets=1, use_random_str_data=True)

        search_query = HybridSearchQuery("shoes")

        vsim_query = HybridVsimQuery(
            vector_field_name="@embedding-hnsw", vector_data="$vec"
        )

        hybrid_query = HybridQuery(search_query, vsim_query)
        combine_method = CombineResultsMethod(
            CombinationMethods.LINEAR,
            ALPHA=0.5,
            BETA=0.5,
            YIELD_SCORE_AS="combined_score",
        )

        res = client.ft().hybrid_search(
            query=hybrid_query,
            combine_method=combine_method,
            params_substitution={"vec": "abcd1234efgh5678"},
            timeout=10,
        )

        if expects_resp2_shape(client) or expects_unified_shape(client):
            assert len(res.results) > 0
            assert res.warnings == []
            for item in res.results:
                assert item["combined_score"] is not None
                assert "__score" not in item

        elif expects_resp3_shape(client):
            assert len(res["results"]) > 0
            assert res["warnings"] == []
            for item in res["results"]:
                assert item["combined_score"] is not None
                assert "__score" not in item

    @pytest.mark.redismod
    @skip_if_server_version_lt("8.3.224")
    def test_hybrid_search_query_with_combine_all_score_aliases(self, client):
        # Create index and add data
        self._create_hybrid_search_index(client)
        # The assertions name the exact KNN winners, so keep vectors deterministic.
        self._add_data_for_hybrid_search(client, items_sets=1)

        search_query = HybridSearchQuery("shoes")
        search_query.yield_score_as("search_score")

        vsim_query = HybridVsimQuery(
            vector_field_name="@embedding-hnsw",
            vector_data="$vec",
            vsim_search_method=VectorSearchMethods.KNN,
            vsim_search_method_params={"K": 3, "EF_RUNTIME": 1},
            yield_score_as="vsim_score",
        )

        hybrid_query = HybridQuery(search_query, vsim_query)

        combine_method = CombineResultsMethod(
            CombinationMethods.LINEAR,
            ALPHA=0.5,
            BETA=0.5,
            YIELD_SCORE_AS="combined_score",
        )

        res = client.ft().hybrid_search(
            query=hybrid_query,
            combine_method=combine_method,
            params_substitution={
                "vec": np.array([1, 2, 7, 8], dtype=np.float32).tobytes()
            },
            timeout=10,
        )

        if expects_resp2_shape(client) or expects_unified_shape(client):
            assert len(res.results) > 0
            assert res.warnings == []
            for item in res.results:
                assert item["combined_score"] is not None
                assert "__score" not in item
                if item["__key"] in [b"item:0", b"item:1", b"item:4"]:
                    assert item["search_score"] is not None
                else:
                    assert "search_score" not in item
                if item["__key"] in [b"item:0", b"item:1", b"item:2"]:
                    assert item["vsim_score"] is not None
                else:
                    assert "vsim_score" not in item

        elif expects_resp3_shape(client):
            assert len(res["results"]) > 0
            assert res["warnings"] == []
            for item in res["results"]:
                assert item["combined_score"] is not None
                assert "__score" not in item
                if item["__key"] in [b"item:0", b"item:1", b"item:4"]:
                    assert item["search_score"] is not None
                else:
                    assert "search_score" not in item
                if item["__key"] in [b"item:0", b"item:1", b"item:2"]:
                    assert item["vsim_score"] is not None
                else:
                    assert "vsim_score" not in item

    @pytest.mark.redismod
    @skip_if_server_version_lt("8.3.224")
    def test_hybrid_search_query_with_vsim_knn(self, client):
        # Create index and add data
        self._create_hybrid_search_index(client)
        self._add_data_for_hybrid_search(client, items_sets=10)

        # set search query
        # this query won't have results, so we will be able to validate vsim results
        search_query = HybridSearchQuery("@color:{none}")

        vsim_query = HybridVsimQuery(
            vector_field_name="@embedding",
            vector_data="$vec",
        )

        vsim_query.vsim_method_params(VectorSearchMethods.KNN, K=3)

        hybrid_query = HybridQuery(search_query, vsim_query)

        postprocessing_config = HybridPostProcessingConfig()

        res = client.ft().hybrid_search(
            query=hybrid_query,
            post_processing=postprocessing_config,
            params_substitution={
                "vec": np.array([1, 2, 2, 3], dtype=np.float32).tobytes()
            },
            timeout=10,
        )
        expected_results = [
            {"__key": b"item:2", "__score": b"0.016393442623"},
            {"__key": b"item:7", "__score": b"0.0161290322581"},
            {"__key": b"item:12", "__score": b"0.015873015873"},
        ]
        if expects_resp2_shape(client) or expects_unified_shape(client):
            assert res.total_results == 3  # KNN top-k value
            assert len(res.results) == 3
            assert res.results == expected_results
            assert res.warnings == []
            assert res.execution_time > 0
        elif expects_resp3_shape(client):
            assert res["total_results"] == 3  # KNN top-k value
            assert len(res["results"]) == 3
            assert res["results"] == expected_results
            assert res["warnings"] == []
            assert res["execution_time"] > 0

        vsim_query_with_hnsw = HybridVsimQuery(
            vector_field_name="@embedding-hnsw",
            vector_data="$vec",
        )
        vsim_query_with_hnsw.vsim_method_params(
            VectorSearchMethods.KNN, K=3, EF_RUNTIME=1
        )
        hybrid_query_with_hnsw = HybridQuery(search_query, vsim_query_with_hnsw)

        res2 = client.ft().hybrid_search(
            query=hybrid_query_with_hnsw,
            params_substitution={
                "vec": np.array([1, 2, 2, 3], dtype=np.float32).tobytes()
            },
            timeout=10,
        )

        # HNSW is an approximate, randomized index (random graph levels,
        # async insertion order), and EF_RUNTIME=1 makes the search maximally
        # sensitive to graph topology, so the top-K docs and their order can
        # differ between runs even with identical input data. With rank-based
        # RRF scoring, that reshuffle produces different __score values too.
        # Validate only the shape of each result dict.
        expected_keys = {"__key", "__score"}

        if expects_resp2_shape(client) or expects_unified_shape(client):
            assert res2.total_results == 3  # KNN top-k value
            assert len(res2.results) == 3
            for result in res2.results:
                assert set(result.keys()) == expected_keys
            assert res2.warnings == []
            assert res2.execution_time > 0
        elif expects_resp3_shape(client):
            assert res2["total_results"] == 3  # KNN top-k value
            assert len(res2["results"]) == 3
            for result in res2["results"]:
                assert set(result.keys()) == expected_keys
            assert res2["warnings"] == []
            assert res2["execution_time"] > 0

    @pytest.mark.redismod
    @skip_if_server_version_lt("8.3.224")
    def test_hybrid_search_query_with_vsim_range(self, client):
        # Create index and add data
        self._create_hybrid_search_index(client)
        self._add_data_for_hybrid_search(client, items_sets=10)

        # set search query
        # this query won't have results, so we will be able to validate vsim results
        search_query = HybridSearchQuery("@color:{none}")

        vsim_query = HybridVsimQuery(
            vector_field_name="@embedding",
            vector_data="$vec",
        )

        vsim_query.vsim_method_params(VectorSearchMethods.RANGE, RADIUS=2)

        hybrid_query = HybridQuery(search_query, vsim_query)

        postprocessing_config = HybridPostProcessingConfig()
        postprocessing_config.limit(0, 3)

        res = client.ft().hybrid_search(
            query=hybrid_query,
            post_processing=postprocessing_config,
            params_substitution={
                "vec": np.array([1, 2, 7, 6], dtype=np.float32).tobytes()
            },
            timeout=10,
        )

        # vsim RANGE does not guarantee a stable order between runs for
        # equal-distance candidates, and RRF scoring (1/(60+rank)) is
        # rank-based, so exact keys and scores cannot be pinned reliably
        # even with identical input data. Validate only the shape of each
        # result dict.
        expected_keys = {"__key", "__score"}

        if expects_resp2_shape(client) or expects_unified_shape(client):
            assert res.total_results >= 3  # at least 3 results
            assert len(res.results) == 3
            for result in res.results:
                assert set(result.keys()) == expected_keys
            assert res.warnings == []
            assert res.execution_time > 0
        elif expects_resp3_shape(client):
            assert res["total_results"] >= 3
            assert len(res["results"]) == 3
            for result in res["results"]:
                assert set(result.keys()) == expected_keys
            assert res["warnings"] == []
            assert res["execution_time"] > 0

        vsim_query_with_hnsw = HybridVsimQuery(
            vector_field_name="@embedding-hnsw",
            vector_data="$vec",
        )

        vsim_query_with_hnsw.vsim_method_params(
            VectorSearchMethods.RANGE, RADIUS=2, EPSILON=0.5
        )

        hybrid_query_with_hnsw = HybridQuery(search_query, vsim_query_with_hnsw)

        res = client.ft().hybrid_search(
            query=hybrid_query_with_hnsw,
            post_processing=postprocessing_config,
            params_substitution={
                "vec": np.array([1, 2, 7, 6], dtype=np.float32).tobytes()
            },
            timeout=10,
        )

        # HNSW is an approximate, randomized index (random graph levels,
        # async insertion order), so the set of docs returned by vsim RANGE
        # and their order can differ between runs even with identical input
        # data. With rank-based RRF scoring, that reshuffle produces
        # different __score values too. Validate only the shape of each
        # result dict.
        expected_keys = {"__key", "__score"}

        if expects_resp2_shape(client) or expects_unified_shape(client):
            assert res.total_results >= 3
            assert len(res.results) == 3
            for result in res.results:
                assert set(result.keys()) == expected_keys
            assert res.warnings == []
            assert res.execution_time > 0
        elif expects_resp3_shape(client):
            assert res["total_results"] >= 3
            assert len(res["results"]) == 3
            for result in res["results"]:
                assert set(result.keys()) == expected_keys
            assert res["warnings"] == []
            assert res["execution_time"] > 0

    @pytest.mark.redismod
    @skip_if_server_version_lt("8.3.224")
    def test_hybrid_search_query_with_combine(self, client):
        # Create index and add data
        self._create_hybrid_search_index(client)
        self._add_data_for_hybrid_search(client, items_sets=10)

        # set search query
        search_query = HybridSearchQuery("@color:{red}")

        vsim_query = HybridVsimQuery(
            vector_field_name="@embedding",
            vector_data="$vec",
        )

        hybrid_query = HybridQuery(search_query, vsim_query)

        combine_method_linear = CombineResultsMethod(
            CombinationMethods.LINEAR, ALPHA=0.5, BETA=0.5
        )

        postprocessing_config = HybridPostProcessingConfig()
        postprocessing_config.limit(0, 3)

        res = client.ft().hybrid_search(
            query=hybrid_query,
            combine_method=combine_method_linear,
            post_processing=postprocessing_config,
            params_substitution={
                "vec": np.array([1, 2, 7, 6], dtype=np.float32).tobytes()
            },
            timeout=10,
        )

        expected_results = [
            {"__key": b"item:2", "__score": b"0.166666666667"},
            {"__key": b"item:7", "__score": b"0.166666666667"},
            {"__key": b"item:12", "__score": b"0.166666666667"},
        ]
        if expects_resp2_shape(client) or expects_unified_shape(client):
            assert res.total_results >= 3
            assert len(res.results) == 3
            assert res.results == expected_results
            assert res.warnings == []
            assert res.execution_time > 0
        elif expects_resp3_shape(client):
            assert res["total_results"] >= 3
            assert len(res["results"]) == 3
            assert res["results"] == expected_results
            assert res["warnings"] == []
            assert res["execution_time"] > 0

        # combine with RRF and WINDOW + CONSTANT
        combine_method_rrf = CombineResultsMethod(
            CombinationMethods.RRF, WINDOW=3, CONSTANT=0.5
        )
        res = client.ft().hybrid_search(
            query=hybrid_query,
            combine_method=combine_method_rrf,
            post_processing=postprocessing_config,
            params_substitution={
                "vec": np.array([1, 2, 7, 6], dtype=np.float32).tobytes()
            },
            timeout=10,
        )

        expected_results = [
            {"__key": b"item:2", "__score": b"1.06666666667"},
            {"__key": b"item:0", "__score": b"0.666666666667"},
            {"__key": b"item:7", "__score": b"0.4"},
        ]
        if expects_resp2_shape(client) or expects_unified_shape(client):
            assert res.total_results >= 3
            assert len(res.results) == 3
            assert res.results == expected_results
            assert res.warnings == []
            assert res.execution_time > 0
        elif expects_resp3_shape(client):
            assert res["total_results"] >= 3
            assert len(res["results"]) == 3
            assert res["results"] == expected_results
            assert res["warnings"] == []
            assert res["execution_time"] > 0

        # combine with RRF, not all possible params provided
        combine_method_rrf_2 = CombineResultsMethod(CombinationMethods.RRF, WINDOW=3)
        res = client.ft().hybrid_search(
            query=hybrid_query,
            combine_method=combine_method_rrf_2,
            post_processing=postprocessing_config,
            params_substitution={
                "vec": np.array([1, 2, 7, 6], dtype=np.float32).tobytes()
            },
            timeout=10,
        )

        expected_results = [
            {"__key": b"item:2", "__score": b"0.032522474881"},
            {"__key": b"item:0", "__score": b"0.016393442623"},
            {"__key": b"item:7", "__score": b"0.0161290322581"},
        ]
        if expects_resp2_shape(client) or expects_unified_shape(client):
            assert res.total_results >= 3
            assert len(res.results) == 3
            assert res.results == expected_results
            assert res.warnings == []
            assert res.execution_time > 0
        elif expects_resp3_shape(client):
            assert res["total_results"] >= 3
            assert len(res["results"]) == 3
            assert res["results"] == expected_results
            assert res["warnings"] == []
            assert res["execution_time"] > 0

    @pytest.mark.redismod
    @skip_if_server_version_lt("8.3.224")
    def test_hybrid_search_query_with_load(self, client):
        # Create index and add data
        self._create_hybrid_search_index(client)
        self._add_data_for_hybrid_search(client, items_sets=10)

        # set search query
        search_query = HybridSearchQuery("@color:{red|green|black}")

        vsim_query = HybridVsimQuery(
            vector_field_name="@embedding",
            vector_data="$vec",
        )

        hybrid_query = HybridQuery(search_query, vsim_query)

        combine_method = CombineResultsMethod(
            CombinationMethods.LINEAR, ALPHA=0.5, BETA=0.5
        )

        postprocessing_config = HybridPostProcessingConfig()
        postprocessing_config.load(
            "@description", "@color", "@price", "@size", "@__key AS item_key"
        )
        postprocessing_config.limit(0, 1)

        res = client.ft().hybrid_search(
            query=hybrid_query,
            combine_method=combine_method,
            post_processing=postprocessing_config,
            params_substitution={
                "vec": np.array([1, 2, 7, 6], dtype=np.float32).tobytes()
            },
            timeout=10,
        )

        expected_results = [
            {
                "description": b"red dress",
                "color": b"red",
                "price": b"17",
                "size": b"12",
                "item_key": b"item:2",
            }
        ]
        if expects_resp2_shape(client) or expects_unified_shape(client):
            assert res.total_results >= 1
            assert len(res.results) == 1
            self.compare_list_of_dicts(res.results, expected_results)
            assert res.warnings == []
            assert res.execution_time > 0
        elif expects_resp3_shape(client):
            assert res["total_results"] >= 1
            assert len(res["results"]) == 1
            self.compare_list_of_dicts(res["results"], expected_results)
            assert res["warnings"] == []
            assert res["execution_time"] > 0

    @pytest.mark.redismod
    @skip_if_server_version_lt("8.3.224")
    def test_hybrid_search_query_with_binary_load(self, client):
        self._create_hybrid_search_index(client)
        self._add_data_for_hybrid_search(client, items_sets=1)

        search_query = HybridSearchQuery("@color:{red}")
        vsim_query = HybridVsimQuery(
            vector_field_name="@embedding",
            vector_data="$vec",
        )
        hybrid_query = HybridQuery(search_query, vsim_query)

        postprocessing_config = HybridPostProcessingConfig()
        postprocessing_config.load("@embedding", decode_field=False)
        postprocessing_config.limit(0, 1)

        res = client.ft().hybrid_search(
            query=hybrid_query,
            post_processing=postprocessing_config,
            params_substitution={
                "vec": np.array([1, 2, 7, 8], dtype=np.float32).tobytes()
            },
            timeout=10,
        )

        if expects_resp2_shape(client) or expects_unified_shape(client):
            loaded_embedding = res.results[0]["embedding"]
            assert res.warnings == []
        elif expects_resp3_shape(client):
            loaded_embedding = res["results"][0]["embedding"]
            assert res["warnings"] == []

        assert isinstance(loaded_embedding, bytes)
        assert np.frombuffer(loaded_embedding, dtype=np.float32).shape == (4,)

    @pytest.mark.redismod
    @skip_if_server_version_lt("8.3.224")
    # @pytest.mark.repeat(6)
    def test_hybrid_search_query_with_load_and_apply(self, client):
        # Create index and add data
        self._create_hybrid_search_index(client)
        self._add_data_for_hybrid_search(client, items_sets=10)

        # set search query
        search_query = HybridSearchQuery("@color:{red}")

        vsim_query = HybridVsimQuery(
            vector_field_name="@embedding",
            vector_data="$vec",
        )

        hybrid_query = HybridQuery(search_query, vsim_query)

        postprocessing_config = HybridPostProcessingConfig()
        postprocessing_config.load("@color", "@price", "@size")
        postprocessing_config.apply(
            price_discount="@price - (@price * 0.1)",
            tax_discount="@price_discount * 0.2",
        )
        postprocessing_config.limit(0, 3)

        res = client.ft().hybrid_search(
            query=hybrid_query,
            post_processing=postprocessing_config,
            params_substitution={
                "vec": np.array([1, 2, 7, 6], dtype=np.float32).tobytes()
            },
            timeout=10,
        )

        expected_results = [
            {
                "color": b"red",
                "price": b"15",
                "size": b"10",
                "price_discount": b"13.5",
                "tax_discount": b"2.7",
            },
            {
                "color": b"red",
                "price": b"17",
                "size": b"12",
                "price_discount": b"15.3",
                "tax_discount": b"3.06",
            },
            {
                "color": b"red",
                "price": b"18",
                "size": b"11",
                "price_discount": b"16.2",
                "tax_discount": b"3.24",
            },
        ]
        if expects_resp2_shape(client) or expects_unified_shape(client):
            assert len(res.results) == 3
            self.compare_list_of_dicts(res.results, expected_results)
            assert res.warnings == []
            assert res.execution_time > 0
        elif expects_resp3_shape(client):
            assert len(res["results"]) == 3
            self.compare_list_of_dicts(res["results"], expected_results)
            assert res["warnings"] == []
            assert res["execution_time"] > 0

    @pytest.mark.redismod
    @skip_if_server_version_lt("8.3.224")
    def test_hybrid_search_query_with_load_and_filter(self, client):
        # Create index and add data
        self._create_hybrid_search_index(client)
        self._add_data_for_hybrid_search(client, items_sets=10)

        # set search query
        search_query = HybridSearchQuery("@color:{red|green|black}")

        vsim_query = HybridVsimQuery(
            vector_field_name="@embedding",
            vector_data="$vec",
        )

        hybrid_query = HybridQuery(search_query, vsim_query)

        postprocessing_config = HybridPostProcessingConfig()
        postprocessing_config.load("@description", "@color", "@price", "@size")
        # for the postprocessing filter we need to filter on the loaded fields
        # expecting all of them to be interpreted as strings - the initial filed types
        # are not preserved
        postprocessing_config.filter(HybridFilter('@price=="15"'))
        postprocessing_config.limit(0, 3)

        res = client.ft().hybrid_search(
            query=hybrid_query,
            post_processing=postprocessing_config,
            params_substitution={
                "vec": np.array([1, 2, 7, 6], dtype=np.float32).tobytes()
            },
            timeout=10,
        )

        if expects_resp2_shape(client) or expects_unified_shape(client):
            assert len(res.results) == 3
            for item in res.results:
                assert item["price"] == b"15"
            assert res.warnings == []
            assert res.execution_time > 0
        elif expects_resp3_shape(client):
            assert len(res["results"]) == 3
            for item in res["results"]:
                assert item["price"] == b"15"
            assert res["warnings"] == []
            assert res["execution_time"] > 0

    @pytest.mark.redismod
    @skip_if_server_version_lt("8.3.224")
    def test_hybrid_search_query_with_load_apply_and_params(self, client):
        # Create index and add data
        self._create_hybrid_search_index(client)
        self._add_data_for_hybrid_search(client, items_sets=5, use_random_str_data=True)

        # set search query
        search_query = HybridSearchQuery("@color:{$color_criteria}")

        vsim_query = HybridVsimQuery(
            vector_field_name="@embedding",
            vector_data="$vector",
        )

        hybrid_query = HybridQuery(search_query, vsim_query)

        postprocessing_config = HybridPostProcessingConfig()
        postprocessing_config.load("@description", "@color", "@price")
        postprocessing_config.apply(price_discount="@price - (@price * 0.1)")
        postprocessing_config.limit(0, 3)

        params_substitution = {
            "vector": "abcd1234abcd5678",
            "color_criteria": "red",
        }

        res = client.ft().hybrid_search(
            query=hybrid_query,
            post_processing=postprocessing_config,
            params_substitution=params_substitution,
            timeout=10,
        )

        # The results fuse the tag match with a KNN ranking over random
        # vectors, so their order is random; check each returned item instead.
        expected_discounts = {
            b"15": b"13.5",
            b"16": b"14.4",
            b"17": b"15.3",
            b"18": b"16.2",
        }
        if expects_resp2_shape(client) or expects_unified_shape(client):
            assert len(res.results) == 3
            assert res.warnings == []
            assert res.execution_time > 0
            assert any(item["color"] == b"red" for item in res.results)
            for item in res.results:
                assert item.keys() == {
                    "description",
                    "color",
                    "price",
                    "price_discount",
                }
                assert item["color"] == item["description"].split()[0]
                assert item["price_discount"] == expected_discounts[item["price"]]
        elif expects_resp3_shape(client):
            assert len(res["results"]) == 3
            assert res["warnings"] == []
            assert res["execution_time"] > 0
            assert any(item["color"] == b"red" for item in res["results"])
            for item in res["results"]:
                assert item.keys() == {
                    "description",
                    "color",
                    "price",
                    "price_discount",
                }
                assert item["color"] == item["description"].split()[0]
                assert item["price_discount"] == expected_discounts[item["price"]]

    @pytest.mark.redismod
    @skip_if_server_version_lt("8.3.224")
    def test_hybrid_search_query_with_limit(self, client):
        # Create index and add data
        self._create_hybrid_search_index(client)
        self._add_data_for_hybrid_search(client, items_sets=10)

        # set search query
        search_query = HybridSearchQuery("@color:{red}")

        vsim_query = HybridVsimQuery(
            vector_field_name="@embedding",
            vector_data="$vec",
        )

        hybrid_query = HybridQuery(search_query, vsim_query)

        postprocessing_config = HybridPostProcessingConfig()
        postprocessing_config.limit(0, 3)

        res = client.ft().hybrid_search(
            query=hybrid_query,
            post_processing=postprocessing_config,
            params_substitution={
                "vec": np.array([1, 2, 7, 6], dtype=np.float32).tobytes()
            },
            timeout=10,
        )

        if expects_resp2_shape(client) or expects_unified_shape(client):
            assert len(res.results) == 3
            assert res.warnings == []
        elif expects_resp3_shape(client):
            assert len(res["results"]) == 3
            assert res["warnings"] == []

    @pytest.mark.redismod
    @skip_if_server_version_lt("8.3.224")
    def test_hybrid_search_query_with_load_apply_and_sortby(self, client):
        # Create index and add data
        self._create_hybrid_search_index(client)
        self._add_data_for_hybrid_search(client, items_sets=1)

        # set search query
        search_query = HybridSearchQuery("@color:{red|green}")

        vsim_query = HybridVsimQuery(
            vector_field_name="@embedding",
            vector_data="$vec",
        )

        hybrid_query = HybridQuery(search_query, vsim_query)

        postprocessing_config = HybridPostProcessingConfig()
        postprocessing_config.load("@color", "@price")
        postprocessing_config.apply(price_discount="@price - (@price * 0.1)")
        postprocessing_config.sort_by(
            SortbyField("@price_discount", asc=False), SortbyField("@color", asc=True)
        )
        postprocessing_config.limit(0, 5)

        res = client.ft().hybrid_search(
            query=hybrid_query,
            post_processing=postprocessing_config,
            params_substitution={
                "vec": np.array([1, 2, 7, 6], dtype=np.float32).tobytes()
            },
            timeout=10,
        )

        expected_results = [
            {"color": b"orange", "price": b"18", "price_discount": b"16.2"},
            {"color": b"red", "price": b"17", "price_discount": b"15.3"},
            {"color": b"green", "price": b"16", "price_discount": b"14.4"},
            {"color": b"black", "price": b"15", "price_discount": b"13.5"},
            {"color": b"red", "price": b"15", "price_discount": b"13.5"},
        ]
        if expects_resp2_shape(client) or expects_unified_shape(client):
            assert res.total_results >= 5
            assert len(res.results) == 5
            # the order here should match because of the sort
            assert res.results == expected_results
            assert res.warnings == []
            assert res.execution_time > 0
        elif expects_resp3_shape(client):
            assert res["total_results"] >= 5
            assert len(res["results"]) == 5
            # the order here should match because of the sort
            assert res["results"] == expected_results
            assert res["warnings"] == []
            assert res["execution_time"] > 0

    @pytest.mark.redismod
    @skip_if_server_version_lt("8.3.224")
    @pytest.mark.timeout(60)
    def test_hybrid_search_query_with_timeout(self, client):
        self._create_hybrid_search_timeout_index(client)
        self._add_data_for_hybrid_search_timeout(client)

        search_query = HybridSearchQuery("@description:(shoes)")
        vsim_query = HybridVsimQuery(
            vector_field_name="@embedding",
            vector_data="$vec",
        )
        vsim_query.vsim_method_params(
            VectorSearchMethods.KNN, K=self._HYBRID_TIMEOUT_DOCS
        )
        hybrid_query = HybridQuery(search_query, vsim_query)
        query_vector = np.full(
            self._HYBRID_TIMEOUT_DIM, 0.25, dtype=np.float32
        ).tobytes()

        res = client.ft().hybrid_search(
            query=hybrid_query,
            params_substitution={"vec": query_vector},
            timeout=1,
        )

        if expects_resp2_shape(client) or expects_unified_shape(client):
            warnings = res.warnings
            assert res.execution_time > 0
        elif expects_resp3_shape(client):
            warnings = res["warnings"]
            assert res["execution_time"] > 0

        assert warnings, f"Expected timeout warnings but none were returned: {warnings}"

        all_match = all(
            safe_str(warning)
            in {
                "Timeout limit was reached (VSIM)",
                "Timeout limit was reached (SEARCH)",
            }
            for warning in warnings
        )
        assert all_match, f"Not all warning are matching the pattern: {warnings}"

    @pytest.mark.redismod
    @skip_if_server_version_lt("8.3.224")
    def test_hybrid_search_query_with_load_and_groupby(self, client):
        # Create index and add data
        self._create_hybrid_search_index(client)
        self._add_data_for_hybrid_search(client, items_sets=10)

        # set search query
        search_query = HybridSearchQuery("@color:{red|green}")

        vsim_query = HybridVsimQuery(
            vector_field_name="@embedding",
            vector_data="$vec",
        )

        hybrid_query = HybridQuery(search_query, vsim_query)

        postprocessing_config = HybridPostProcessingConfig()
        postprocessing_config.load("@color", "@price", "@size", "@item_type")
        postprocessing_config.limit(0, 4)

        postprocessing_config.group_by(
            ["@item_type", "@price"],
            reducers.count_distinct("@color").alias("colors_count"),
            reducers.min("@size"),
        )

        postprocessing_config.sort_by(SortbyField("@price", asc=True))

        res = client.ft().hybrid_search(
            query=hybrid_query,
            post_processing=postprocessing_config,
            params_substitution={
                "vec": np.array([1, 2, 7, 6], dtype=np.float32).tobytes()
            },
            timeout=10,
        )

        expected_results = [
            {
                "item_type": b"dress",
                "price": b"15",
                "colors_count": b"1",
                "__generated_aliasminsize": b"10",
            },
            {
                "item_type": b"shoes",
                "price": b"15",
                "colors_count": b"2",
                "__generated_aliasminsize": b"10",
            },
            {
                "item_type": b"shoes",
                "price": b"16",
                "colors_count": b"2",
                "__generated_aliasminsize": b"10",
            },
            {
                "item_type": b"dress",
                "price": b"16",
                "colors_count": b"1",
                "__generated_aliasminsize": b"11",
            },
        ]

        # The query only sorts by @price, so the order of rows sharing the same
        # price (e.g. the two price=15 and the two price=16 groups) is not
        # deterministic. Validate the price sort separately, then compare the
        # groups order-independently by their unique (item_type, price) key.
        def _row_key(row):
            return (row["price"], row["item_type"])

        def _assert_hybrid_results(results, warnings):
            assert len(results) == 4
            prices = [row["price"] for row in results]
            assert prices == sorted(prices)
            assert sorted(results, key=_row_key) == sorted(
                expected_results, key=_row_key
            )
            assert warnings == []

        if expects_resp2_shape(client) or expects_unified_shape(client):
            _assert_hybrid_results(res.results, res.warnings)
        elif expects_resp3_shape(client):
            _assert_hybrid_results(res["results"], res["warnings"])

        postprocessing_config = HybridPostProcessingConfig()
        postprocessing_config.load("@color", "@price", "@size", "@item_type")
        postprocessing_config.limit(0, 6)
        postprocessing_config.sort_by(
            SortbyField("@price", asc=True),
            SortbyField("@item_type", asc=True),
        )

        postprocessing_config.group_by(
            ["@price", "@item_type"],
            reducers.count_distinct("@color").alias("unique_colors_count"),
        )

        res = client.ft().hybrid_search(
            query=hybrid_query,
            post_processing=postprocessing_config,
            params_substitution={
                "vec": np.array([1, 2, 7, 6], dtype=np.float32).tobytes()
            },
            timeout=1000,
        )

        expected_results = [
            {"price": b"15", "item_type": b"dress", "unique_colors_count": b"1"},
            {"price": b"15", "item_type": b"shoes", "unique_colors_count": b"2"},
            {"price": b"16", "item_type": b"dress", "unique_colors_count": b"1"},
            {"price": b"16", "item_type": b"shoes", "unique_colors_count": b"2"},
            {"price": b"17", "item_type": b"dress", "unique_colors_count": b"1"},
            {"price": b"17", "item_type": b"shoes", "unique_colors_count": b"2"},
        ]
        if expects_resp2_shape(client) or expects_unified_shape(client):
            assert len(res.results) == 6
            assert res.results == expected_results
            assert res.warnings == []
        elif expects_resp3_shape(client):
            assert len(res["results"]) == 6
            assert res["results"] == expected_results
            assert res["warnings"] == []

    @pytest.mark.redismod
    @skip_if_server_version_lt("8.4")
    @skip_if_server_version_gte("8.11")
    def test_hybrid_search_query_with_cursor(self, client):
        # Create index and add data
        self._create_hybrid_search_index(client)
        self._add_data_for_hybrid_search(client, items_sets=10)

        # set search query
        search_query = HybridSearchQuery("@color:{red|green}")

        vsim_query = HybridVsimQuery(
            vector_field_name="@embedding",
            vector_data="$vec",
        )

        hybrid_query = HybridQuery(search_query, vsim_query)

        # MAXIDLE is expressed in milliseconds, and the two cursors created here
        # are read over two further round trips. Keep the idle window wide enough
        # to survive that when the target deployment is remote.
        res = client.ft().hybrid_search(
            query=hybrid_query,
            cursor=HybridCursorQuery(count=5, max_idle=10000),
            params_substitution={
                "vec": np.array([1, 2, 7, 6], dtype=np.float32).tobytes()
            },
            timeout=10,
        )
        if expects_resp2_shape(client) or expects_unified_shape(client):
            assert isinstance(res, HybridCursorResult)
            assert res.search_cursor_id > 0
            assert res.vsim_cursor_id > 0
            search_cursor = aggregations.Cursor(res.search_cursor_id)
            vsim_cursor = aggregations.Cursor(res.vsim_cursor_id)
        elif expects_resp3_shape(client):
            assert res["SEARCH"] > 0
            assert res["VSIM"] > 0
            search_cursor = aggregations.Cursor(res["SEARCH"])
            vsim_cursor = aggregations.Cursor(res["VSIM"])

        search_res_from_cursor = client.ft().aggregate(query=search_cursor)
        if expects_resp2_shape(client) or expects_unified_shape(client):
            assert len(search_res_from_cursor.rows) == 5
        elif expects_resp3_shape(client):
            assert len(search_res_from_cursor[0]["results"]) == 5

        vsim_res_from_cursor = client.ft().aggregate(query=vsim_cursor)
        if expects_resp2_shape(client) or expects_unified_shape(client):
            assert len(vsim_res_from_cursor.rows) == 5
        elif expects_resp3_shape(client):
            assert len(vsim_res_from_cursor[0]["results"]) == 5

    @pytest.mark.redismod
    @skip_if_server_version_lt("8.3.224")
    def test_hybrid_search_query_with_multiple_loads_and_applies(self, client):
        # Create index and add data
        self._create_hybrid_search_index(client)
        self._add_data_for_hybrid_search(client, items_sets=1)

        # set search query
        search_query = HybridSearchQuery("@color:{red|green}")

        vsim_query = HybridVsimQuery(
            vector_field_name="@embedding",
            vector_data="$vec",
        )

        hybrid_query = HybridQuery(search_query, vsim_query)

        postprocessing_config = HybridPostProcessingConfig()
        postprocessing_config.load("@color", "@price")
        postprocessing_config.load("@description")
        postprocessing_config.apply(discount_10_percents="@price - (@price * 0.1)")
        postprocessing_config.apply(
            additional_discount="@discount_10_percents - (@discount_10_percents * 0.1)"
        )
        postprocessing_config.filter(HybridFilter('@price=="15"'))
        postprocessing_config.load("@description")
        postprocessing_config.sort_by(
            SortbyField("@discount_10_percents", asc=False),
            SortbyField("@color", asc=True),
        )
        postprocessing_config.limit(0, 5)

        res = client.ft().hybrid_search(
            query=hybrid_query,
            post_processing=postprocessing_config,
            params_substitution={
                "vec": np.array([1, 2, 7, 6], dtype=np.float32).tobytes()
            },
            timeout=10,
        )
        print(res)
        if expects_resp2_shape(client) or expects_unified_shape(client):
            assert len(res.results) == 2
            for item in res.results:
                assert item["color"] is not None
                assert item["price"] is not None
                assert item["description"] is not None
                assert item["discount_10_percents"] is not None
                assert item["additional_discount"] is not None
        elif expects_resp3_shape(client):
            assert len(res["results"]) == 2
            for item in res["results"]:
                assert item["color"] is not None
                assert item["price"] is not None
                assert item["description"] is not None
                assert item["discount_10_percents"] is not None
                assert item["additional_discount"] is not None


# Parametrise the bytes-key regression tests over RESP2 and the
# default protocol.  RESP2 uses ``_RedisCallbacksRESP2`` and anchors the
# expected legacy output shape with ``decode_responses=False``.  The
# default protocol (``SENTINEL`` -> not specified) leaves the wire on
# RESP3 with the ``_RedisCallbacksRESP3toRESP2Legacy`` adapter selected,
# which is where the bytes-key normalisation in ``_parse_search_resp3``,
# ``_parse_aggregate_resp3`` and ``_parse_spellcheck_resp3`` lives.  An
# explicit ``protocol=3`` would route through ``_RedisCallbacksRESP3``
# instead and bypass the methods we want to test.
_SEARCH_BYTES_PROTOCOLS = [
    pytest.param(2, id="resp2"),
    pytest.param(SENTINEL, id="default-resp3"),
]


def _make_bytes_search_client(request, stack_url, protocol):
    kwargs = {
        "decode_responses": False,
        "from_url": stack_url,
    }
    if protocol is not SENTINEL:
        kwargs["protocol"] = protocol
    client = _get_client(redis.Redis, request, **kwargs)
    client.flushdb()
    return client


class TestSearchResp3BytesKeys(SearchTestsBase):
    """Regression tests for #4107.

    With the default protocol (RESP3 on the wire) and
    ``decode_responses=False`` the server's structural map keys arrive
    as ``bytes`` (e.g. ``b"results"`` rather than ``"results"``).  The
    RESP3->legacy-RESP2 search callbacks used to look those keys up as
    plain strings, missed them, and silently produced empty results.
    Each test is parametrised over ``protocol=2`` (anchors the legacy
    output shape) and the default protocol (exercises the actual fixed
    parsers in ``_RedisCallbacksRESP3toRESP2Legacy``).
    """

    @pytest.mark.redismod
    @pytest.mark.fixed_client
    @pytest.mark.parametrize("protocol", _SEARCH_BYTES_PROTOCOLS)
    # Redis Enterprise's search module returns a different RESP3 result shape here.
    @skip_if_redis_enterprise()
    def test_search_resp3_bytes_keys(self, request, stack_url, protocol):
        client = _make_bytes_search_client(request, stack_url, protocol)
        client.ft().create_index((TextField("title"), TextField("body")))
        client.hset("doc1", mapping={"title": "hello", "body": "redis world"})
        client.hset("doc2", mapping={"title": "hello", "body": "search world"})
        self.waitForIndex(client, getattr(client.ft(), "index_name", "idx"))

        res = client.ft().search(Query("hello"))

        # Before the fix the default-RESP3 case returned
        # ``Result{0 total, docs: []}`` because ``Result.from_resp3``
        # looked up the bytes-keyed map by ``str`` keys.  ``Result``
        # always normalises the doc id with ``str_if_bytes`` so the id
        # assertion stays in ``str`` form even with
        # ``decode_responses=False``.
        assert res.total == 2
        assert {d.id for d in res.docs} == {"doc1", "doc2"}

    @pytest.mark.redismod
    @pytest.mark.fixed_client
    @pytest.mark.parametrize("protocol", _SEARCH_BYTES_PROTOCOLS)
    # Redis Enterprise's search module returns a different RESP3 result shape here.
    @skip_if_redis_enterprise()
    def test_aggregate_resp3_bytes_keys(self, request, stack_url, protocol):
        client = _make_bytes_search_client(request, stack_url, protocol)
        client.ft().create_index((TextField("title"), TextField("parent")))
        client.hset("doc1", mapping={"title": "alpha", "parent": "redis"})
        client.hset("doc2", mapping={"title": "beta", "parent": "redis"})
        client.hset("doc3", mapping={"title": "gamma", "parent": "redis"})
        self.waitForIndex(client, getattr(client.ft(), "index_name", "idx"))

        req = aggregations.AggregateRequest("redis").group_by(
            "@parent", reducers.count()
        )
        res = client.ft().aggregate(req)

        # Before the fix the default-RESP3 case missed
        # ``data.get("total_results")`` and ``data.get("results")``
        # because the keys were ``bytes``, yielding ``total=0`` and
        # ``rows=[]``.
        assert len(res.rows) == 1
        row = res.rows[0]
        # Row content stays as bytes (matches RESP2 with
        # decode_responses=False); only the structural map keys are
        # normalised.
        assert b"parent" in row
        assert b"redis" in row

    @pytest.mark.redismod
    @pytest.mark.fixed_client
    @pytest.mark.parametrize("protocol", _SEARCH_BYTES_PROTOCOLS)
    # Redis Enterprise's search module returns a different RESP3 result shape here.
    @skip_if_redis_enterprise()
    def test_spellcheck_resp3_bytes_keys(self, request, stack_url, protocol):
        client = _make_bytes_search_client(request, stack_url, protocol)
        client.ft().create_index((TextField("f1"),))
        client.hset("doc1", mapping={"f1": "some valid content"})
        client.hset("doc2", mapping={"f1": "very important"})
        self.waitForIndex(client, getattr(client.ft(), "index_name", "idx"))

        res = client.ft().spellcheck("impornant")

        # Before the fix the default-RESP3 case had
        # ``res.get("results", {})`` miss the ``b"results"`` key and
        # return an empty ``{}``.  Both protocols carry the term and
        # suggestion through as bytes, matching
        # ``_parse_spellcheck`` with ``decode_responses=False``.
        assert b"impornant" in res
        suggestions = res[b"impornant"]
        assert suggestions
        assert suggestions[0]["suggestion"] == b"important"


def _search_docs(res):
    """Search result documents, comparable across the response shapes."""
    if isinstance(res, Result):
        return [vars(doc) for doc in res.docs]
    if isinstance(res, dict):
        return res["results"]
    return res


def _contains_bytes(value):
    """Whether a parsed reply still holds undecoded bytes anywhere."""
    if isinstance(value, bytes):
        return True
    if isinstance(value, dict):
        return any(_contains_bytes(k) or _contains_bytes(v) for k, v in value.items())
    if isinstance(value, (list, tuple, set)):
        return any(_contains_bytes(v) for v in value)
    if hasattr(value, "__dict__"):
        return _contains_bytes(vars(value))
    return False


def _queue_parsed_commands(ft):
    """Queue the search commands whose replies are parsed by the client."""
    return [
        ft.info(),
        ft.aggregate(aggregations.AggregateRequest("*")),
        ft.profile(Query("foo")),
        ft.spellcheck("foo"),
        ft.config_get("TIMEOUT"),
        ft.config_set("TIMEOUT", "100"),
        ft.syndump(),
        ft.sugget("ac", "he"),
        ft.sugget("ac", "he", with_scores=True),
    ]
