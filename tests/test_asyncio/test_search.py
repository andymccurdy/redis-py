import bz2
import csv
import os
import asyncio
from io import TextIOWrapper
import random
from unittest import mock

import numpy as np
import pytest
import pytest_asyncio
from redis import ResponseError
import redis.asyncio as redis
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
from redis.commands.search import AsyncSearch
from redis.commands.search.field import (
    GeoField,
    NumericField,
    TagField,
    TextField,
    VectorField,
)
from redis.commands.search.index_definition import IndexDefinition, IndexType
from redis.commands.search.query import GeoFilter, NumericFilter, Query, SortbyField
from redis.commands.search.result import Result
from redis.commands.search.suggestion import Suggestion
from redis.utils import safe_str
from tests.conftest import (
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
    Make sure the result of a search is as expected, taking into account the
    RESP version being used.
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


class AsyncSearchTestsBase:
    @pytest_asyncio.fixture()
    async def decoded_r(self, create_redis, stack_url):
        return await create_redis(decode_responses=True, url=stack_url)

    @staticmethod
    async def waitForIndex(env, idx, timeout=None):
        delay = 0.02
        while True:
            try:
                res = await env.execute_command("FT.INFO", idx)
                if int(res[res.index("indexing") + 1]) == 0:
                    break
            except ValueError:
                break
            except AttributeError:
                try:
                    if int(res["indexing"]) == 0:
                        break
                except ValueError:
                    break
            except ResponseError:
                # index doesn't exist yet
                # continue to sleep and try again
                pass

            await asyncio.sleep(delay)
            if timeout is not None:
                timeout -= delay
                if timeout <= 0:
                    break

    @staticmethod
    def getClient(decoded_r: redis.Redis):
        """
        Gets a client client attached to an index name which is ready to be
        created
        """
        return decoded_r

    @staticmethod
    async def createIndex(decoded_r, num_docs=100, definition=None):
        try:
            await decoded_r.create_index(
                (
                    TextField("play", weight=5.0),
                    TextField("txt"),
                    NumericField("chapter"),
                ),
                definition=definition,
            )
        except redis.ResponseError:
            await decoded_r.dropindex(delete_documents=True)
            return await AsyncSearchTestsBase.createIndex(
                decoded_r, num_docs=num_docs, definition=definition
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

        indexer = decoded_r.batch_indexer(chunk_size=50)
        assert isinstance(indexer, AsyncSearch.BatchIndexer)
        assert 50 == indexer.chunk_size

        # one round trip for all documents, not one HSET each
        pipe = indexer.client.client.pipeline(transaction=False)
        for key, doc in chapters.items():
            pipe.hset(key, mapping=doc)
        await pipe.execute()
        await indexer.commit()


class TestBaseSearchFunctionality(AsyncSearchTestsBase):
    @pytest.mark.redismod
    async def test_client(self, decoded_r: redis.Redis):
        num_docs = 500
        await self.createIndex(decoded_r.ft(), num_docs=num_docs)
        await self.waitForIndex(decoded_r, "idx")
        # verify info
        info = await decoded_r.ft().info()
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

        assert decoded_r.ft().index_name == info["index_name"]
        assert num_docs == int(info["num_docs"])

        res = await decoded_r.ft().search("henry iv")
        if expects_resp2_shape(decoded_r) or expects_unified_shape(decoded_r):
            assert isinstance(res, Result)
            assert 225 == res.total
            assert 10 == len(res.docs)
            assert res.duration > 0

            for doc in res.docs:
                assert doc.id
                assert doc.play == "Henry IV"
                assert len(doc.txt) > 0

            # test no content
            res = await decoded_r.ft().search(Query("king").no_content())
            assert 194 == res.total
            assert 10 == len(res.docs)
            for doc in res.docs:
                assert "txt" not in doc.__dict__
                assert "play" not in doc.__dict__

            # test verbatim vs no verbatim
            total = (await decoded_r.ft().search(Query("kings").no_content())).total
            vtotal = (
                await decoded_r.ft().search(Query("kings").no_content().verbatim())
            ).total
            assert total > vtotal

            # test in fields
            txt_total = (
                await decoded_r.ft().search(
                    Query("henry").no_content().limit_fields("txt")
                )
            ).total
            play_total = (
                await decoded_r.ft().search(
                    Query("henry").no_content().limit_fields("play")
                )
            ).total
            both_total = (
                await decoded_r.ft().search(
                    Query("henry").no_content().limit_fields("play", "txt")
                )
            ).total
            assert 129 == txt_total
            assert 494 == play_total
            assert 494 == both_total

            # test load_document
            doc = await decoded_r.ft().load_document("henry vi part 3:62")
            assert doc is not None
            assert "henry vi part 3:62" == doc.id
            assert doc.play == "Henry VI Part 3"
            assert len(doc.txt) > 0

            # test in-keys
            ids = [x.id for x in (await decoded_r.ft().search(Query("henry"))).docs]
            assert 10 == len(ids)
            subset = ids[:5]
            docs = await decoded_r.ft().search(Query("henry").limit_ids(*subset))
            assert len(subset) == docs.total
            ids = [x.id for x in docs.docs]
            assert set(ids) == set(subset)

            # test slop and in order
            assert 193 == (await decoded_r.ft().search(Query("henry king"))).total
            assert (
                3
                == (
                    await decoded_r.ft().search(Query("henry king").slop(0).in_order())
                ).total
            )
            assert (
                52
                == (
                    await decoded_r.ft().search(Query("king henry").slop(0).in_order())
                ).total
            )
            assert (
                53 == (await decoded_r.ft().search(Query("henry king").slop(0))).total
            )
            assert (
                167
                == (await decoded_r.ft().search(Query("henry king").slop(100))).total
            )

            # test delete document
            await decoded_r.hset("doc-5ghs2", mapping={"play": "Death of a Salesman"})
            res = await decoded_r.ft().search(Query("death of a salesman"))
            assert 1 == res.total

            assert 1 == await decoded_r.ft().delete_document("doc-5ghs2")
            res = await decoded_r.ft().search(Query("death of a salesman"))
            assert 0 == res.total
            assert 0 == await decoded_r.ft().delete_document("doc-5ghs2")

            await decoded_r.hset("doc-5ghs2", mapping={"play": "Death of a Salesman"})
            res = await decoded_r.ft().search(Query("death of a salesman"))
            assert 1 == res.total
            await decoded_r.ft().delete_document("doc-5ghs2")
        elif expects_resp3_shape(decoded_r):
            assert isinstance(res, dict)
            assert 225 == res["total_results"]
            assert 10 == len(res["results"])

            for doc in res["results"]:
                assert doc["id"]
                assert doc["extra_attributes"]["play"] == "Henry IV"
                assert len(doc["extra_attributes"]["txt"]) > 0

            # test no content
            res = await decoded_r.ft().search(Query("king").no_content())
            assert 194 == res["total_results"]
            assert 10 == len(res["results"])
            for doc in res["results"]:
                assert "extra_attributes" not in doc.keys()

            # test verbatim vs no verbatim
            total = (await decoded_r.ft().search(Query("kings").no_content()))[
                "total_results"
            ]
            vtotal = (
                await decoded_r.ft().search(Query("kings").no_content().verbatim())
            )["total_results"]
            assert total > vtotal

            # test in fields
            txt_total = (
                await decoded_r.ft().search(
                    Query("henry").no_content().limit_fields("txt")
                )
            )["total_results"]
            play_total = (
                await decoded_r.ft().search(
                    Query("henry").no_content().limit_fields("play")
                )
            )["total_results"]
            both_total = (
                await decoded_r.ft().search(
                    Query("henry").no_content().limit_fields("play", "txt")
                )
            )["total_results"]
            assert 129 == txt_total
            assert 494 == play_total
            assert 494 == both_total

            # test load_document
            doc = await decoded_r.ft().load_document("henry vi part 3:62")
            assert doc is not None
            assert "henry vi part 3:62" == doc.id
            assert doc.play == "Henry VI Part 3"
            assert len(doc.txt) > 0

            # test in-keys
            ids = [
                x["id"]
                for x in (await decoded_r.ft().search(Query("henry")))["results"]
            ]
            assert 10 == len(ids)
            subset = ids[:5]
            docs = await decoded_r.ft().search(Query("henry").limit_ids(*subset))
            assert len(subset) == docs["total_results"]
            ids = [x["id"] for x in docs["results"]]
            assert set(ids) == set(subset)

            # test slop and in order
            assert (
                193
                == (await decoded_r.ft().search(Query("henry king")))["total_results"]
            )
            assert (
                3
                == (
                    await decoded_r.ft().search(Query("henry king").slop(0).in_order())
                )["total_results"]
            )
            assert (
                52
                == (
                    await decoded_r.ft().search(Query("king henry").slop(0).in_order())
                )["total_results"]
            )
            assert (
                53
                == (await decoded_r.ft().search(Query("henry king").slop(0)))[
                    "total_results"
                ]
            )
            assert (
                167
                == (await decoded_r.ft().search(Query("henry king").slop(100)))[
                    "total_results"
                ]
            )

            # test delete document
            await decoded_r.hset("doc-5ghs2", mapping={"play": "Death of a Salesman"})
            res = await decoded_r.ft().search(Query("death of a salesman"))
            assert 1 == res["total_results"]

            assert 1 == await decoded_r.ft().delete_document("doc-5ghs2")
            res = await decoded_r.ft().search(Query("death of a salesman"))
            assert 0 == res["total_results"]
            assert 0 == await decoded_r.ft().delete_document("doc-5ghs2")

            await decoded_r.hset("doc-5ghs2", mapping={"play": "Death of a Salesman"})
            res = await decoded_r.ft().search(Query("death of a salesman"))
            assert 1 == res["total_results"]
            await decoded_r.ft().delete_document("doc-5ghs2")

    @pytest.mark.redismod
    @pytest.mark.onlynoncluster
    @skip_if_server_version_gte("7.9.0")
    async def test_scores(self, decoded_r: redis.Redis):
        await decoded_r.ft().create_index((TextField("txt"),))

        await decoded_r.hset("doc1", mapping={"txt": "foo baz"})
        await decoded_r.hset("doc2", mapping={"txt": "foo bar"})

        q = Query("foo ~bar").with_scores()
        res = await decoded_r.ft().search(q)
        if expects_resp2_shape(decoded_r) or expects_unified_shape(decoded_r):
            assert 2 == res.total
            assert "doc2" == res.docs[0].id
            assert 3.0 == res.docs[0].score
            assert "doc1" == res.docs[1].id
        elif expects_resp3_shape(decoded_r):
            assert 2 == res["total_results"]
            assert "doc2" == res["results"][0]["id"]
            assert 3.0 == res["results"][0]["score"]
            assert "doc1" == res["results"][1]["id"]

    @pytest.mark.redismod
    @pytest.mark.onlynoncluster
    @skip_if_server_version_lt("7.9.0")
    async def test_scores_with_new_default_scorer(self, decoded_r: redis.Redis):
        await decoded_r.ft().create_index((TextField("txt"),))

        await decoded_r.hset("doc1", mapping={"txt": "foo baz"})
        await decoded_r.hset("doc2", mapping={"txt": "foo bar"})

        q = Query("foo ~bar").with_scores()
        res = await decoded_r.ft().search(q)
        if expects_resp2_shape(decoded_r) or expects_unified_shape(decoded_r):
            assert 2 == res.total
            assert "doc2" == res.docs[0].id
            assert 0.87 == pytest.approx(res.docs[0].score, 0.01)
            assert "doc1" == res.docs[1].id
        elif expects_resp3_shape(decoded_r):
            assert 2 == res["total_results"]
            assert "doc2" == res["results"][0]["id"]
            assert 0.87 == pytest.approx(res["results"][0]["score"], 0.01)
            assert "doc1" == res["results"][1]["id"]

    @pytest.mark.redismod
    async def test_stopwords(self, decoded_r: redis.Redis):
        stopwords = ["foo", "bar", "baz"]
        await decoded_r.ft().create_index((TextField("txt"),), stopwords=stopwords)
        await decoded_r.hset("doc1", mapping={"txt": "foo bar"})
        await decoded_r.hset("doc2", mapping={"txt": "hello world"})
        await self.waitForIndex(decoded_r, "idx")

        q1 = Query("foo bar").no_content()
        q2 = Query("foo bar hello world").no_content()
        res1, res2 = await decoded_r.ft().search(q1), await decoded_r.ft().search(q2)
        if expects_resp2_shape(decoded_r) or expects_unified_shape(decoded_r):
            assert 0 == res1.total
            assert 1 == res2.total
        elif expects_resp3_shape(decoded_r):
            assert 0 == res1["total_results"]
            assert 1 == res2["total_results"]

    @pytest.mark.redismod
    async def test_filters(self, decoded_r: redis.Redis):
        await decoded_r.ft().create_index(
            (TextField("txt"), NumericField("num"), GeoField("loc"))
        )
        await decoded_r.hset(
            "doc1", mapping={"txt": "foo bar", "num": 3.141, "loc": "-0.441,51.458"}
        )
        await decoded_r.hset(
            "doc2", mapping={"txt": "foo baz", "num": 2, "loc": "-0.1,51.2"}
        )

        await self.waitForIndex(decoded_r, "idx")
        # Test numerical filter
        q1 = Query("foo").add_filter(NumericFilter("num", 0, 2)).no_content()
        q2 = (
            Query("foo")
            .add_filter(NumericFilter("num", 2, NumericFilter.INF, minExclusive=True))
            .no_content()
        )
        res1, res2 = await decoded_r.ft().search(q1), await decoded_r.ft().search(q2)

        if expects_resp2_shape(decoded_r) or expects_unified_shape(decoded_r):
            assert 1 == res1.total
            assert 1 == res2.total
            assert "doc2" == res1.docs[0].id
            assert "doc1" == res2.docs[0].id
        elif expects_resp3_shape(decoded_r):
            assert 1 == res1["total_results"]
            assert 1 == res2["total_results"]
            assert "doc2" == res1["results"][0]["id"]
            assert "doc1" == res2["results"][0]["id"]

        # Test geo filter
        q1 = Query("foo").add_filter(GeoFilter("loc", -0.44, 51.45, 10)).no_content()
        q2 = Query("foo").add_filter(GeoFilter("loc", -0.44, 51.45, 100)).no_content()
        res1, res2 = await decoded_r.ft().search(q1), await decoded_r.ft().search(q2)

        if expects_resp2_shape(decoded_r) or expects_unified_shape(decoded_r):
            assert 1 == res1.total
            assert 2 == res2.total
            assert "doc1" == res1.docs[0].id

            # Sort results, after RDB reload order may change
            res = [res2.docs[0].id, res2.docs[1].id]
            res.sort()
            assert ["doc1", "doc2"] == res
        elif expects_resp3_shape(decoded_r):
            assert 1 == res1["total_results"]
            assert 2 == res2["total_results"]
            assert "doc1" == res1["results"][0]["id"]

            # Sort results, after RDB reload order may change
            res = [res2["results"][0]["id"], res2["results"][1]["id"]]
            res.sort()
            assert ["doc1", "doc2"] == res

    @pytest.mark.redismod
    async def test_sort_by(self, decoded_r: redis.Redis):
        await decoded_r.ft().create_index(
            (TextField("txt"), NumericField("num", sortable=True))
        )
        await decoded_r.hset("doc1", mapping={"txt": "foo bar", "num": 1})
        await decoded_r.hset("doc2", mapping={"txt": "foo baz", "num": 2})
        await decoded_r.hset("doc3", mapping={"txt": "foo qux", "num": 3})

        # Test sort
        q1 = Query("foo").sort_by("num", asc=True).no_content()
        q2 = Query("foo").sort_by("num", asc=False).no_content()
        res1, res2 = await decoded_r.ft().search(q1), await decoded_r.ft().search(q2)

        if expects_resp2_shape(decoded_r) or expects_unified_shape(decoded_r):
            assert 3 == res1.total
            assert "doc1" == res1.docs[0].id
            assert "doc2" == res1.docs[1].id
            assert "doc3" == res1.docs[2].id
            assert 3 == res2.total
            assert "doc1" == res2.docs[2].id
            assert "doc2" == res2.docs[1].id
            assert "doc3" == res2.docs[0].id
        elif expects_resp3_shape(decoded_r):
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
    async def test_drop_index(self, decoded_r: redis.Redis):
        """
        Ensure the index gets dropped by data remains by default
        """
        for keep_docs in [[True, {}], [False, {"name": "haveit"}]]:
            idx = "HaveIt"
            index = self.getClient(decoded_r)
            await index.hset("index:haveit", mapping={"name": "haveit"})
            idef = IndexDefinition(prefix=["index:"])
            await index.ft(idx).create_index((TextField("name"),), definition=idef)
            await self.waitForIndex(index, idx)
            await index.ft(idx).dropindex(delete_documents=keep_docs[0])
            i = await index.hgetall("index:haveit")
            assert i == keep_docs[1]

    @pytest.mark.redismod
    async def test_example(self, decoded_r: redis.Redis):
        # Creating the index definition and schema
        await decoded_r.ft().create_index(
            (TextField("title", weight=5.0), TextField("body"))
        )

        # Indexing a document
        await decoded_r.hset(
            "doc1",
            mapping={
                "title": "RediSearch",
                "body": "Redisearch impements a search engine on top of redis",
            },
        )

        # Searching with complex parameters:
        q = Query("search engine").verbatim().no_content().paging(0, 5)

        res = await decoded_r.ft().search(q)
        assert res is not None

    @pytest.mark.redismod
    async def test_auto_complete(self, decoded_r: redis.Redis):
        with open(TITLES_CSV) as f:
            suggestions = [
                Suggestion(row[0], score=float(row[1])) for row in csv.reader(f)
            ]

        # sugadd pipelines its suggestions and returns the last reply, which is
        # the dictionary size, so batching keeps the size check per batch
        ft = decoded_r.ft()
        n = 0
        for i in range(0, len(suggestions), 500):
            batch = suggestions[i : i + 500]
            n += len(batch)
            assert n == await ft.sugadd("ac", *batch)

        assert n == await decoded_r.ft().suglen("ac")
        ret = await decoded_r.ft().sugget("ac", "bad", with_scores=True)
        assert 2 == len(ret)
        assert "badger" == ret[0].string
        assert isinstance(ret[0].score, float)
        assert 1.0 != ret[0].score
        assert "badalte rishtey" == ret[1].string
        assert isinstance(ret[1].score, float)
        assert 1.0 != ret[1].score

        ret = await decoded_r.ft().sugget("ac", "bad", fuzzy=True, num=10)
        assert 10 == len(ret)
        assert 1.0 == ret[0].score
        strs = {x.string for x in ret}

        for sug in strs:
            assert 1 == await decoded_r.ft().sugdel("ac", sug)
        # make sure a second delete returns 0
        for sug in strs:
            assert 0 == await decoded_r.ft().sugdel("ac", sug)

        # make sure they were actually deleted
        ret2 = await decoded_r.ft().sugget("ac", "bad", fuzzy=True, num=10)
        for sug in ret2:
            assert sug.string not in strs

        # Test with payload
        await decoded_r.ft().sugadd("ac", Suggestion("pay1", payload="pl1"))
        await decoded_r.ft().sugadd("ac", Suggestion("pay2", payload="pl2"))
        await decoded_r.ft().sugadd("ac", Suggestion("pay3", payload="pl3"))

        sugs = await decoded_r.ft().sugget(
            "ac", "pay", with_payloads=True, with_scores=True
        )
        assert 3 == len(sugs)
        for sug in sugs:
            assert sug.payload
            assert sug.payload.startswith("pl")

    @pytest.mark.redismod
    async def test_no_index(self, decoded_r: redis.Redis):
        await decoded_r.ft().create_index(
            (
                TextField("field"),
                TextField("text", no_index=True, sortable=True),
                NumericField("numeric", no_index=True, sortable=True),
                GeoField("geo", no_index=True, sortable=True),
                TagField("tag", no_index=True, sortable=True),
            )
        )

        await decoded_r.hset(
            "doc1",
            mapping={
                "field": "aaa",
                "text": "1",
                "numeric": "1",
                "geo": "1,1",
                "tag": "1",
            },
        )
        await decoded_r.hset(
            "doc2",
            mapping={
                "field": "aab",
                "text": "2",
                "numeric": "2",
                "geo": "2,2",
                "tag": "2",
            },
        )
        await self.waitForIndex(decoded_r, "idx")

        if expects_resp2_shape(decoded_r) or expects_unified_shape(decoded_r):
            res = await decoded_r.ft().search(Query("@text:aa*"))
            assert 0 == res.total

            res = await decoded_r.ft().search(Query("@field:aa*"))
            assert 2 == res.total

            res = await decoded_r.ft().search(Query("*").sort_by("text", asc=False))
            assert 2 == res.total
            assert "doc2" == res.docs[0].id

            res = await decoded_r.ft().search(Query("*").sort_by("text", asc=True))
            assert "doc1" == res.docs[0].id

            res = await decoded_r.ft().search(Query("*").sort_by("numeric", asc=True))
            assert "doc1" == res.docs[0].id

            res = await decoded_r.ft().search(Query("*").sort_by("geo", asc=True))
            assert "doc1" == res.docs[0].id

            res = await decoded_r.ft().search(Query("*").sort_by("tag", asc=True))
            assert "doc1" == res.docs[0].id
        elif expects_resp3_shape(decoded_r):
            res = await decoded_r.ft().search(Query("@text:aa*"))
            assert 0 == res["total_results"]

            res = await decoded_r.ft().search(Query("@field:aa*"))
            assert 2 == res["total_results"]

            res = await decoded_r.ft().search(Query("*").sort_by("text", asc=False))
            assert 2 == res["total_results"]
            assert "doc2" == res["results"][0]["id"]

            res = await decoded_r.ft().search(Query("*").sort_by("text", asc=True))
            assert "doc1" == res["results"][0]["id"]

            res = await decoded_r.ft().search(Query("*").sort_by("numeric", asc=True))
            assert "doc1" == res["results"][0]["id"]

            res = await decoded_r.ft().search(Query("*").sort_by("geo", asc=True))
            assert "doc1" == res["results"][0]["id"]

            res = await decoded_r.ft().search(Query("*").sort_by("tag", asc=True))
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
    @skip_if_server_version_lt("7.4.0")
    @skip_ifmodversion_lt("2.10.0", "search")
    async def test_create_index_empty_or_missing_fields_with_sortable(
        self,
        decoded_r: redis.Redis,
    ):
        definition = IndexDefinition(prefix=["property:"], index_type=IndexType.HASH)

        fields = [
            TextField("title", sortable=True, index_empty=True),
            TagField("features", index_missing=True, sortable=True),
            TextField("description", no_index=True, sortable=True),
        ]

        await decoded_r.ft().create_index(fields, definition=definition)

    @pytest.mark.redismod
    async def test_explain(self, decoded_r: redis.Redis):
        await decoded_r.ft().create_index(
            (TextField("f1"), TextField("f2"), TextField("f3"))
        )
        res = await decoded_r.ft().explain("@f3:f3_val @f2:f2_val @f1:f1_val")
        assert res

    @pytest.mark.redismod
    async def test_explaincli(self, decoded_r: redis.Redis):
        with pytest.raises(NotImplementedError):
            await decoded_r.ft().explain_cli("foo")

    @pytest.mark.redismod
    async def test_summarize(self, decoded_r: redis.Redis):
        await self.createIndex(decoded_r.ft())
        await self.waitForIndex(decoded_r, "idx")

        q = Query('"king henry"').paging(0, 1)
        q.highlight(fields=("play", "txt"), tags=("<b>", "</b>"))
        q.summarize("txt")

        if expects_resp2_shape(decoded_r) or expects_unified_shape(decoded_r):
            doc = sorted((await decoded_r.ft().search(q)).docs)[0]
            assert "<b>Henry</b> IV" == doc.play
            assert (
                "ACT I SCENE I. London. The palace. Enter <b>KING</b> <b>HENRY</b>, LORD JOHN OF LANCASTER, the EARL of WESTMORELAND, SIR... "  # noqa
                == doc.txt
            )

            q = Query('"king henry"').paging(0, 1).summarize().highlight()

            doc = sorted((await decoded_r.ft().search(q)).docs)[0]
            assert "<b>Henry</b> ... " == doc.play
            assert (
                "ACT I SCENE I. London. The palace. Enter <b>KING</b> <b>HENRY</b>, LORD JOHN OF LANCASTER, the EARL of WESTMORELAND, SIR... "  # noqa
                == doc.txt
            )
        elif expects_resp3_shape(decoded_r):
            doc = sorted((await decoded_r.ft().search(q))["results"])[0]
            assert "<b>Henry</b> IV" == doc["extra_attributes"]["play"]
            assert (
                "ACT I SCENE I. London. The palace. Enter <b>KING</b> <b>HENRY</b>, LORD JOHN OF LANCASTER, the EARL of WESTMORELAND, SIR... "  # noqa
                == doc["extra_attributes"]["txt"]
            )

            q = Query('"king henry"').paging(0, 1).summarize().highlight()

            doc = sorted((await decoded_r.ft().search(q))["results"])[0]
            assert "<b>Henry</b> ... " == doc["extra_attributes"]["play"]
            assert (
                "ACT I SCENE I. London. The palace. Enter <b>KING</b> <b>HENRY</b>, LORD JOHN OF LANCASTER, the EARL of WESTMORELAND, SIR... "  # noqa
                == doc["extra_attributes"]["txt"]
            )

    @pytest.mark.redismod
    @skip_ifmodversion_lt("2.0.0", "search")
    async def test_alias(self, decoded_r: redis.Redis):
        index1 = self.getClient(decoded_r)
        index2 = self.getClient(decoded_r)

        def1 = IndexDefinition(prefix=["index1:"])
        def2 = IndexDefinition(prefix=["index2:"])

        ftindex1 = index1.ft("testAlias")
        ftindex2 = index2.ft("testAlias2")
        await ftindex1.create_index((TextField("name"),), definition=def1)
        await ftindex2.create_index((TextField("name"),), definition=def2)

        await index1.hset("index1:lonestar", mapping={"name": "lonestar"})
        await index2.hset("index2:yogurt", mapping={"name": "yogurt"})

        if expects_resp2_shape(decoded_r) or expects_unified_shape(decoded_r):
            res = (await ftindex1.search("*")).docs[0]
            assert "index1:lonestar" == res.id

            # create alias and check for results
            await ftindex1.aliasadd("spaceballs")
            alias_client = self.getClient(decoded_r).ft("spaceballs")
            res = (await alias_client.search("*")).docs[0]
            assert "index1:lonestar" == res.id

            # Throw an exception when trying to add an alias that already exists
            with pytest.raises(Exception):
                await ftindex2.aliasadd("spaceballs")

            # update alias and ensure new results
            await ftindex2.aliasupdate("spaceballs")
            alias_client2 = self.getClient(decoded_r).ft("spaceballs")

            res = (await alias_client2.search("*")).docs[0]
            assert "index2:yogurt" == res.id
        elif expects_resp3_shape(decoded_r):
            res = (await ftindex1.search("*"))["results"][0]
            assert "index1:lonestar" == res["id"]

            # create alias and check for results
            await ftindex1.aliasadd("spaceballs")
            alias_client = self.getClient(await decoded_r).ft("spaceballs")
            res = (await alias_client.search("*"))["results"][0]
            assert "index1:lonestar" == res["id"]

            # Throw an exception when trying to add an alias that already exists
            with pytest.raises(Exception):
                await ftindex2.aliasadd("spaceballs")

            # update alias and ensure new results
            await ftindex2.aliasupdate("spaceballs")
            alias_client2 = self.getClient(await decoded_r).ft("spaceballs")

            res = (await alias_client2.search("*"))["results"][0]
            assert "index2:yogurt" == res["id"]

        await ftindex2.aliasdel("spaceballs")
        with pytest.raises(Exception):
            (await alias_client2.search("*")).docs[0]

    @pytest.mark.redismod
    @pytest.mark.xfail(strict=False)
    async def test_alias_basic(self, decoded_r: redis.Redis):
        # Creating a client with one index
        client = self.getClient(decoded_r)
        await client.flushdb()
        index1 = self.getClient(decoded_r).ft("testAlias")

        await index1.create_index((TextField("txt"),))
        await index1.client.hset("doc1", mapping={"txt": "text goes here"})

        index2 = self.getClient(decoded_r).ft("testAlias2")
        await index2.create_index((TextField("txt"),))
        await index2.client.hset("doc2", mapping={"txt": "text goes here"})

        # add the actual alias and check
        await index1.aliasadd("myalias")
        alias_client = self.getClient(decoded_r).ft("myalias")
        if expects_resp2_shape(decoded_r) or expects_unified_shape(decoded_r):
            res = sorted((await alias_client.search("*")).docs, key=lambda x: x.id)
            assert "doc1" == res[0].id

            # Throw an exception when trying to add an alias that already exists
            with pytest.raises(Exception):
                await index2.aliasadd("myalias")

            # update the alias and ensure we get doc2
            await index2.aliasupdate("myalias")
            alias_client2 = self.getClient(decoded_r).ft("myalias")
            res = sorted((await alias_client2.search("*")).docs, key=lambda x: x.id)
            assert "doc1" == res[0].id
        elif expects_resp3_shape(decoded_r):
            res = sorted(
                (await alias_client.search("*"))["results"], key=lambda x: x["id"]
            )
            assert "doc1" == res[0]["id"]

            # Throw an exception when trying to add an alias that already exists
            with pytest.raises(Exception):
                await index2.aliasadd("myalias")

            # update the alias and ensure we get doc2
            await index2.aliasupdate("myalias")
            alias_client2 = self.getClient(client).ft("myalias")
            res = sorted(
                (await alias_client2.search("*"))["results"], key=lambda x: x["id"]
            )
            assert "doc1" == res[0]["id"]

        # delete the alias and expect an error if we try to query again
        await index2.aliasdel("myalias")
        with pytest.raises(Exception):
            _ = (await alias_client2.search("*")).docs[0]

    @pytest.mark.redismod
    @skip_if_server_version_lt("8.9.0")
    async def test_aliaslist(self, decoded_r: redis.Redis):
        index = decoded_r.ft("aliaslistidx")
        await index.create_index((TextField("txt"),))

        # An existing index with no aliases returns an empty set, not an error.
        assert await index.aliaslist() == set()

        # Aliases are returned as an unordered collection.
        await index.aliasadd("alias1")
        await index.aliasadd("alias2")
        aliases = await index.aliaslist()
        assert isinstance(aliases, set)
        assert aliases == {"alias1", "alias2"}

        # FT.ALIASUPDATE moving an alias to another index removes it from the
        # previous index's listing.
        index2 = decoded_r.ft("aliaslistidx2")
        await index2.create_index((TextField("txt"),))
        await index2.aliasupdate("alias1")
        assert await index.aliaslist() == {"alias2"}
        assert await index2.aliaslist() == {"alias1"}

        # FT.ALIASDEL removes the alias from the listing.
        await index.aliasdel("alias2")
        assert await index.aliaslist() == set()

        # A missing index and an alias name supplied as the index both fail
        # with the index-not-found error; the client must not resolve aliases.
        with pytest.raises(redis.ResponseError):
            await decoded_r.ft("nonexistent_aliaslist_index").aliaslist()
        with pytest.raises(redis.ResponseError):
            await decoded_r.ft("alias1").aliaslist()

    @pytest.mark.redismod
    @skip_if_server_version_lt("8.9.0")
    @skip_if_redis_enterprise()
    async def test_aliaslist_decode_responses_false(self, create_redis, stack_url):
        # ``decode_responses`` is not part of the CI protocol/legacy matrix,
        # so pin only that here and let the harness supply the protocol x
        # legacy combinations (as it does for ``test_aliaslist`` above).
        client = await create_redis(decode_responses=False, url=stack_url)
        index = client.ft("aliaslistbytesidx")
        await index.create_index((TextField("txt"),))

        # ``aliasadd`` accepts a ``KeyT`` (str | bytes | memoryview); mix a
        # ``str`` and a ``bytes`` alias to exercise the widened input type.
        await index.aliasadd("alias1")
        await index.aliasadd(b"alias2")

        # Alias names are user data, so with ``decode_responses=False`` they
        # are returned as ``bytes`` (honoring the flag, like FT.TAGVALS /
        # FT.DICTDUMP) rather than being force-decoded to ``str``.
        aliases = await index.aliaslist()
        assert isinstance(aliases, set)
        assert aliases == {b"alias1", b"alias2"}

    @pytest.mark.redismod
    async def test_tags(self, decoded_r: redis.Redis):
        await decoded_r.ft().create_index((TextField("txt"), TagField("tags")))
        tags = "foo,foo bar,hello;world"
        tags2 = "soba,ramen"

        await decoded_r.hset("doc1", mapping={"txt": "fooz barz", "tags": tags})
        await decoded_r.hset("doc2", mapping={"txt": "noodles", "tags": tags2})
        await self.waitForIndex(decoded_r, "idx")

        q = Query("@tags:{foo}")
        if expects_resp2_shape(decoded_r) or expects_unified_shape(decoded_r):
            res = await decoded_r.ft().search(q)
            assert 1 == res.total

            q = Query("@tags:{foo bar}")
            res = await decoded_r.ft().search(q)
            assert 1 == res.total

            q = Query("@tags:{foo\\ bar}")
            res = await decoded_r.ft().search(q)
            assert 1 == res.total

            q = Query("@tags:{hello\\;world}")
            res = await decoded_r.ft().search(q)
            assert 1 == res.total

            q2 = await decoded_r.ft().tagvals("tags")
            assert (tags.split(",") + tags2.split(",")).sort() == q2.sort()
        elif expects_resp3_shape(decoded_r):
            res = await decoded_r.ft().search(q)
            assert 1 == res["total_results"]

            q = Query("@tags:{foo bar}")
            res = await decoded_r.ft().search(q)
            assert 1 == res["total_results"]

            q = Query("@tags:{foo\\ bar}")
            res = await decoded_r.ft().search(q)
            assert 1 == res["total_results"]

            q = Query("@tags:{hello\\;world}")
            res = await decoded_r.ft().search(q)
            assert 1 == res["total_results"]

            q2 = await decoded_r.ft().tagvals("tags")
            assert set(tags.split(",") + tags2.split(",")) == set(q2)

    @pytest.mark.redismod
    async def test_textfield_sortable_nostem(self, decoded_r: redis.Redis):
        # Creating the index definition with sortable and no_stem
        await decoded_r.ft().create_index(
            (TextField("txt", sortable=True, no_stem=True),)
        )

        # Now get the index info to confirm its contents
        response = await decoded_r.ft().info()
        if expects_resp2_shape(decoded_r):
            assert "SORTABLE" in response["attributes"][0]
            assert "NOSTEM" in response["attributes"][0]
        elif expects_resp3_shape(decoded_r) or expects_unified_shape(decoded_r):
            assert "SORTABLE" in response["attributes"][0]["flags"]
            assert "NOSTEM" in response["attributes"][0]["flags"]

    @pytest.mark.redismod
    async def test_alter_schema_add(self, decoded_r: redis.Redis):
        # Creating the index definition and schema
        await decoded_r.ft().create_index(TextField("title"))

        # Using alter to add a field
        await decoded_r.ft().alter_schema_add(TextField("body"))

        # Indexing a document
        await decoded_r.hset(
            "doc1",
            mapping={"title": "MyTitle", "body": "Some content only in the body"},
        )

        # Searching with parameter only in the body (the added field)
        q = Query("only in the body")

        # Ensure we find the result searching on the added body field
        res = await decoded_r.ft().search(q)
        if expects_resp2_shape(decoded_r) or expects_unified_shape(decoded_r):
            assert 1 == res.total
        elif expects_resp3_shape(decoded_r):
            assert 1 == res["total_results"]

    @pytest.mark.redismod
    async def test_spell_check(self, decoded_r: redis.Redis):
        await decoded_r.ft().create_index((TextField("f1"), TextField("f2")))

        await decoded_r.hset(
            "doc1", mapping={"f1": "some valid content", "f2": "this is sample text"}
        )
        await decoded_r.hset(
            "doc2", mapping={"f1": "very important", "f2": "lorem ipsum"}
        )
        await self.waitForIndex(decoded_r, "idx")

        if expects_resp2_shape(decoded_r) or expects_unified_shape(decoded_r):
            # test spellcheck
            res = await decoded_r.ft().spellcheck("impornant")
            assert "important" == res["impornant"][0]["suggestion"]

            res = await decoded_r.ft().spellcheck("contnt")
            assert "content" == res["contnt"][0]["suggestion"]

            # test spellcheck with Levenshtein distance
            res = await decoded_r.ft().spellcheck("vlis")
            assert res == {}
            res = await decoded_r.ft().spellcheck("vlis", distance=2)
            assert "valid" == res["vlis"][0]["suggestion"]

            # test spellcheck include
            await decoded_r.ft().dict_add("dict", "lore", "lorem", "lorm")
            res = await decoded_r.ft().spellcheck("lorm", include="dict")
            assert len(res["lorm"]) == 3
            assert (
                res["lorm"][0]["suggestion"],
                res["lorm"][1]["suggestion"],
                res["lorm"][2]["suggestion"],
            ) == ("lorem", "lore", "lorm")
            assert (res["lorm"][0]["score"], res["lorm"][1]["score"]) == ("0.5", "0")

            # test spellcheck exclude
            res = await decoded_r.ft().spellcheck("lorm", exclude="dict")
            assert res == {}
        elif expects_resp3_shape(decoded_r):
            # test spellcheck
            res = await decoded_r.ft().spellcheck("impornant")
            assert "important" in res["results"]["impornant"][0].keys()

            res = await decoded_r.ft().spellcheck("contnt")
            assert "content" in res["results"]["contnt"][0].keys()

            # test spellcheck with Levenshtein distance
            res = await decoded_r.ft().spellcheck("vlis")
            assert res == {"results": {"vlis": []}}
            res = await decoded_r.ft().spellcheck("vlis", distance=2)
            assert "valid" in res["results"]["vlis"][0].keys()

            # test spellcheck include
            await decoded_r.ft().dict_add("dict", "lore", "lorem", "lorm")
            res = await decoded_r.ft().spellcheck("lorm", include="dict")
            assert len(res["results"]["lorm"]) == 3
            assert "lorem" in res["results"]["lorm"][0].keys()
            assert "lore" in res["results"]["lorm"][1].keys()
            assert "lorm" in res["results"]["lorm"][2].keys()
            assert (
                res["results"]["lorm"][0]["lorem"],
                res["results"]["lorm"][1]["lore"],
            ) == (0.5, 0)

            # test spellcheck exclude
            res = await decoded_r.ft().spellcheck("lorm", exclude="dict")
            assert res == {"results": {}}

    @pytest.mark.redismod
    async def test_dict_operations(self, decoded_r: redis.Redis):
        await decoded_r.ft().create_index((TextField("f1"), TextField("f2")))
        # Add three items
        res = await decoded_r.ft().dict_add("custom_dict", "item1", "item2", "item3")
        assert 3 == res

        # Remove one item
        res = await decoded_r.ft().dict_del("custom_dict", "item2")
        assert 1 == res

        # Dump dict and inspect content
        res = await decoded_r.ft().dict_dump("custom_dict")
        assert res == ["item1", "item3"]

        # Remove rest of the items before reload
        await decoded_r.ft().dict_del("custom_dict", *res)

    @pytest.mark.redismod
    async def test_phonetic_matcher(self, decoded_r: redis.Redis):
        await decoded_r.ft().create_index((TextField("name"),))
        await decoded_r.hset("doc1", mapping={"name": "Jon"})
        await decoded_r.hset("doc2", mapping={"name": "John"})

        res = await decoded_r.ft().search(Query("Jon"))
        if expects_resp2_shape(decoded_r) or expects_unified_shape(decoded_r):
            assert 1 == len(res.docs)
            assert "Jon" == res.docs[0].name
        elif expects_resp3_shape(decoded_r):
            assert 1 == res["total_results"]
            assert "Jon" == res["results"][0]["extra_attributes"]["name"]

        # Drop and create index with phonetic matcher
        await decoded_r.flushdb()

        await decoded_r.ft().create_index(
            (TextField("name", phonetic_matcher="dm:en"),)
        )
        await decoded_r.hset("doc1", mapping={"name": "Jon"})
        await decoded_r.hset("doc2", mapping={"name": "John"})

        res = await decoded_r.ft().search(Query("Jon"))
        if expects_resp2_shape(decoded_r) or expects_unified_shape(decoded_r):
            assert 2 == len(res.docs)
            assert ["John", "Jon"] == sorted(d.name for d in res.docs)
        elif expects_resp3_shape(decoded_r):
            assert 2 == res["total_results"]
            assert ["John", "Jon"] == sorted(
                d["extra_attributes"]["name"] for d in res["results"]
            )

    @pytest.mark.redismod
    async def test_get(self, decoded_r: redis.Redis):
        await decoded_r.ft().create_index((TextField("f1"), TextField("f2")))

        assert [None] == await decoded_r.ft().get("doc1")
        assert [None, None] == await decoded_r.ft().get("doc2", "doc1")

        await decoded_r.hset(
            "doc1",
            mapping={"f1": "some valid content dd1", "f2": "this is sample text f1"},
        )
        await decoded_r.hset(
            "doc2",
            mapping={"f1": "some valid content dd2", "f2": "this is sample text f2"},
        )

        assert [
            ["f1", "some valid content dd2", "f2", "this is sample text f2"]
        ] == await decoded_r.ft().get("doc2")
        assert [
            ["f1", "some valid content dd1", "f2", "this is sample text f1"],
            ["f1", "some valid content dd2", "f2", "this is sample text f2"],
        ] == await decoded_r.ft().get("doc1", "doc2")

    @pytest.mark.redismod
    async def test_query_timeout(self, decoded_r: redis.Redis):
        q1 = Query("foo").timeout(5000)
        assert q1.get_args() == ["foo", "TIMEOUT", 5000, "DIALECT", 2, "LIMIT", 0, 10]
        q2 = Query("foo").timeout("not_a_number")
        with pytest.raises(redis.ResponseError):
            await decoded_r.ft().search(q2)

    async def _create_search_timeout_index(self, decoded_r: redis.Redis):
        await decoded_r.ft().create_index(
            (TextField("description"),),
            definition=IndexDefinition(prefix=["search-timeout-item:"]),
        )
        await AsyncSearchTestsBase.waitForIndex(decoded_r, "idx")
        for i in range(5):
            await decoded_r.hset(f"search-timeout-item:{i}", "description", "red shoes")

    async def _search_with_forced_timeout(self, decoded_r: redis.Redis, query):
        # Whether a real query outlives its TIMEOUT depends on the dataset and
        # on how fast the server is, so force the timeout instead:
        # ``_FT.DEBUG FT.SEARCH ... TIMEOUT_AFTER_N 0`` runs the wrapped
        # FT.SEARCH and has it time out before the first result. search()
        # still builds the arguments and parses the reply.
        ft = decoded_r.ft()
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
        return await ft.search(query)

    @pytest.mark.redismod
    # The Redis Enterprise skip and the 8.0.0 gate come from the _FT.DEBUG
    # timeout hook, not from TIMEOUT itself: Redis Enterprise does not expose
    # the hook, and the 7.x stack images predate it.
    @skip_if_redis_enterprise()
    @skip_if_server_version_lt("8.0.0")
    async def test_search_query_with_timeout(self, decoded_r: redis.Redis):
        await self._create_search_timeout_index(decoded_r)

        query = Query("red").timeout(1)
        res = await self._search_with_forced_timeout(decoded_r, query)

        if expects_resp3_shape(decoded_r):
            warnings = res.get("warning", [])
            total = res["total_results"]
        else:
            warnings = res.warnings
            total = res.total

        # A timed-out search still returns a well-formed (partial) result.
        assert isinstance(total, int) and total >= 0

        if int(get_protocol_version(decoded_r)) == 3:
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
    async def test_search_query_with_timeout_fail_policy(self, decoded_r: redis.Redis):
        await self._create_search_timeout_index(decoded_r)

        query = Query("red").timeout(1)

        # ``search-on-timeout`` controls whether a timed-out query returns the
        # partial results collected so far (``return``, the default) or fails
        # the command (``fail``). The server refuses TIMEOUT_AFTER_N with
        # ``fail`` while search worker threads are enabled, so turn them off.
        # Capture the original values so they are always restored, even if
        # the assertion below raises.
        config = await decoded_r.config_get("search-on-timeout")
        original = config["search-on-timeout"]
        config = await decoded_r.config_get("search-workers")
        original_workers = config["search-workers"]
        try:
            assert await decoded_r.config_set("search-workers", 0)
            assert await decoded_r.config_set("search-on-timeout", "fail")
            # With the ``fail`` policy the server aborts the timed-out search
            # instead of returning partial results.
            with pytest.raises(redis.ResponseError, match="Timeout limit was reached"):
                await self._search_with_forced_timeout(decoded_r, query)
        finally:
            await decoded_r.config_set("search-on-timeout", original)
            await decoded_r.config_set("search-workers", original_workers)

    @pytest.mark.redismod
    async def test_binary_and_text_fields(self, decoded_r: redis.Redis):
        fake_vec = np.array([0.1, 0.2, 0.3, 0.4], dtype=np.float32)

        index_name = "mixed_index"
        mixed_data = {"first_name": "🐍python", "vector_emb": fake_vec.tobytes()}
        await decoded_r.hset(f"{index_name}:1", mapping=mixed_data)

        schema = [
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
        ]

        await decoded_r.ft(index_name).create_index(
            fields=schema,
            definition=IndexDefinition(
                prefix=[f"{index_name}:"], index_type=IndexType.HASH
            ),
        )
        await self.waitForIndex(decoded_r, index_name)

        query = (
            Query("*")
            .return_field("vector_emb", decode_field=False)
            .return_field("first_name")
        )
        result = await decoded_r.ft(index_name).search(query=query, query_params={})

        if expects_resp3_shape(decoded_r):
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


class TestSearchLanguages(AsyncSearchTestsBase):
    """Functional coverage for indexing and searching text in languages other
    than English: Snowball stemming for several languages, query-time language
    selection, the Chinese (friso) tokenizer and per-document ``LANGUAGE_FIELD``
    routing."""

    @pytest.mark.redismod
    @pytest.mark.parametrize(
        "language, text, query, doc_word",
        NON_ENGLISH_STEMMING_CASES,
    )
    async def test_search_non_english_stemming(
        self, decoded_r: redis.Redis, language, text, query, doc_word
    ):
        """A query for a different inflection of ``doc_word`` matches when the
        document is indexed with its own language stemmer, but not when the same
        text is indexed as English -- proving the language setting is what drives
        the stem match."""
        # Index the document under its own language.
        lang_def = IndexDefinition(prefix=["lang:"], language=language)
        await decoded_r.ft("idx_lang").create_index(
            (TextField("txt"),), definition=lang_def
        )
        await decoded_r.hset("lang:1", mapping={"txt": text})

        # Index the same text as English as a negative control.
        en_def = IndexDefinition(prefix=["en:"], language="english")
        await decoded_r.ft("idx_en").create_index(
            (TextField("txt"),), definition=en_def
        )
        await decoded_r.hset("en:1", mapping={"txt": text})

        await self.waitForIndex(decoded_r, "idx_lang")
        await self.waitForIndex(decoded_r, "idx_en")

        # Same query in both cases -- only the index language differs.
        q = Query(query).language(language).no_content()
        assert _search_total(decoded_r, await decoded_r.ft("idx_lang").search(q)) == 1
        assert _search_total(decoded_r, await decoded_r.ft("idx_en").search(q)) == 0

    @pytest.mark.redismod
    @skip_if_server_version_lt("8.9.0")
    async def test_search_stemming_indonesian(self, decoded_r: redis.Redis):
        """The Indonesian stemmer reduces "membaca" to its root "baca", so a
        query for the stem matches the indexed document.  The Indonesian
        stemmer is only available on newer server versions."""
        definition = IndexDefinition(prefix=["doc:"], language="indonesian")
        await decoded_r.ft("idx_id").create_index(
            (TextField("content"),), definition=definition
        )
        await decoded_r.hset(
            "doc:1", mapping={"content": "mereka membaca buku di perpustakaan"}
        )
        await self.waitForIndex(decoded_r, "idx_id")

        q = Query("baca").language("indonesian").no_content()
        assert _search_total(decoded_r, await decoded_r.ft("idx_id").search(q)) == 1

    @pytest.mark.redismod
    async def test_search_query_language(self, decoded_r: redis.Redis):
        """``Query.language()`` controls how the *query* is stemmed.  The German
        stemmer reduces "Kindern" to "kind" (matching the indexed document's
        stem), while English analysis leaves it as "kindern" (no match).  The
        query word is deliberately absent from the document text so the match
        can only come from stemming, not from the indexed raw term."""
        definition = IndexDefinition(prefix=["doc:"], language="german")
        await decoded_r.ft().create_index((TextField("txt"),), definition=definition)
        await decoded_r.hset("doc:1", mapping={"txt": "Die Kinder spielen im Garten"})
        await self.waitForIndex(decoded_r, "idx")

        matched = await decoded_r.ft().search(
            Query("Kindern").language("german").no_content()
        )
        assert _search_total(decoded_r, matched) == 1

        missed = await decoded_r.ft().search(
            Query("Kindern").language("english").no_content()
        )
        assert _search_total(decoded_r, missed) == 0

    @pytest.mark.redismod
    async def test_search_chinese_tokenization(self, decoded_r: redis.Redis):
        """Chinese text has no whitespace word boundaries; only the ``chinese``
        tokenizer (friso) segments it into terms so an inner term can be
        matched.  Indexed as English the text stays a single token."""
        text = "我喜欢编程"  # "I like programming"
        term = "编程"  # "programming"

        cn_def = IndexDefinition(prefix=["cn:"], language="chinese")
        await decoded_r.ft("idx_cn").create_index(
            (TextField("txt"),), definition=cn_def
        )
        await decoded_r.hset("cn:1", mapping={"txt": text})

        en_def = IndexDefinition(prefix=["en:"], language="english")
        await decoded_r.ft("idx_en").create_index(
            (TextField("txt"),), definition=en_def
        )
        await decoded_r.hset("en:1", mapping={"txt": text})

        await self.waitForIndex(decoded_r, "idx_cn")
        await self.waitForIndex(decoded_r, "idx_en")

        cn_res = await decoded_r.ft("idx_cn").search(
            Query(term).language("chinese").no_content()
        )
        assert _search_total(decoded_r, cn_res) == 1

        en_res = await decoded_r.ft("idx_en").search(Query(term).no_content())
        assert _search_total(decoded_r, en_res) == 0

    @pytest.mark.redismod
    async def test_search_language_field(self, decoded_r: redis.Redis):
        """``LANGUAGE_FIELD`` selects the stemmer per document from a document
        field, so the German document's "Kinder" is stemmed to "kind" while the
        English document is analysed with the English stemmer."""
        definition = IndexDefinition(prefix=["doc:"], language_field="__lang")
        await decoded_r.ft().create_index((TextField("txt"),), definition=definition)
        await decoded_r.hset(
            "doc:de", mapping={"txt": "Die Kinder spielen", "__lang": "german"}
        )
        await decoded_r.hset(
            "doc:en", mapping={"txt": "the children play", "__lang": "english"}
        )
        await self.waitForIndex(decoded_r, "idx")

        # "Kind" only reaches the German document, whose "Kinder" stems to "kind".
        res = await decoded_r.ft().search(Query("Kind").language("german").no_content())
        assert _search_total(decoded_r, res) == 1
        _assert_search_result(decoded_r, res, ["doc:de"])


class TestScorers(AsyncSearchTestsBase):
    @pytest.mark.redismod
    @pytest.mark.onlynoncluster
    # NOTE(imalinovskyi): This test contains hardcoded scores valid only for RediSearch 2.8+
    @skip_ifmodversion_lt("2.8.0", "search")
    @skip_if_server_version_gte("7.9.0")
    async def test_scorer(self, decoded_r: redis.Redis):
        await decoded_r.ft().create_index((TextField("description"),))

        await decoded_r.hset(
            "doc1",
            mapping={"description": "The quick brown fox jumps over the lazy dog"},
        )
        await decoded_r.hset(
            "doc2",
            mapping={
                "description": "Quick alice was beginning to get very tired of sitting by her quick sister on the bank, and of having nothing to do."  # noqa
            },
        )

        if expects_resp2_shape(decoded_r) or expects_unified_shape(decoded_r):
            # default scorer is TFIDF
            res = await decoded_r.ft().search(Query("quick").with_scores())
            assert 1.0 == res.docs[0].score
            res = await decoded_r.ft().search(
                Query("quick").scorer("TFIDF").with_scores()
            )
            assert 1.0 == res.docs[0].score
            res = await decoded_r.ft().search(
                Query("quick").scorer("TFIDF.DOCNORM").with_scores()
            )
            assert 0.14285714285714285 == res.docs[0].score
            res = await decoded_r.ft().search(
                Query("quick").scorer("BM25").with_scores()
            )
            assert 0.22471909420069797 == res.docs[0].score
            res = await decoded_r.ft().search(
                Query("quick").scorer("DISMAX").with_scores()
            )
            assert 2.0 == res.docs[0].score
            res = await decoded_r.ft().search(
                Query("quick").scorer("DOCSCORE").with_scores()
            )
            assert 1.0 == res.docs[0].score
            res = await decoded_r.ft().search(
                Query("quick").scorer("HAMMING").with_scores()
            )
            assert 0.0 == res.docs[0].score
        elif expects_resp3_shape(decoded_r):
            res = await decoded_r.ft().search(Query("quick").with_scores())
            assert 1.0 == res["results"][0]["score"]
            res = await decoded_r.ft().search(
                Query("quick").scorer("TFIDF").with_scores()
            )
            assert 1.0 == res["results"][0]["score"]
            res = await decoded_r.ft().search(
                Query("quick").scorer("TFIDF.DOCNORM").with_scores()
            )
            assert 0.14285714285714285 == res["results"][0]["score"]
            res = await decoded_r.ft().search(
                Query("quick").scorer("BM25").with_scores()
            )
            assert 0.22471909420069797 == res["results"][0]["score"]
            res = await decoded_r.ft().search(
                Query("quick").scorer("DISMAX").with_scores()
            )
            assert 2.0 == res["results"][0]["score"]
            res = await decoded_r.ft().search(
                Query("quick").scorer("DOCSCORE").with_scores()
            )
            assert 1.0 == res["results"][0]["score"]
            res = await decoded_r.ft().search(
                Query("quick").scorer("HAMMING").with_scores()
            )
            assert 0.0 == res["results"][0]["score"]

    @pytest.mark.redismod
    @pytest.mark.onlynoncluster
    # NOTE(imalinovskyi): This test contains hardcoded scores valid only for RediSearch 2.8+
    @skip_ifmodversion_lt("2.8.0", "search")
    @skip_if_server_version_lt("7.9.0")
    async def test_scorer_with_new_default_scorer(self, decoded_r: redis.Redis):
        await decoded_r.ft().create_index((TextField("description"),))

        await decoded_r.hset(
            "doc1",
            mapping={"description": "The quick brown fox jumps over the lazy dog"},
        )
        await decoded_r.hset(
            "doc2",
            mapping={
                "description": "Quick alice was beginning to get very tired of sitting by her quick sister on the bank, and of having nothing to do."  # noqa
            },
        )

        if expects_resp2_shape(decoded_r) or expects_unified_shape(decoded_r):
            # default scorer is BM25STD
            res = await decoded_r.ft().search(Query("quick").with_scores())
            assert 0.23 == pytest.approx(res.docs[0].score, 0.05)
            res = await decoded_r.ft().search(
                Query("quick").scorer("TFIDF").with_scores()
            )
            assert 1.0 == res.docs[0].score
            res = await decoded_r.ft().search(
                Query("quick").scorer("TFIDF.DOCNORM").with_scores()
            )
            assert 0.14285714285714285 == res.docs[0].score
            res = await decoded_r.ft().search(
                Query("quick").scorer("BM25").with_scores()
            )
            assert 0.22471909420069797 == res.docs[0].score
            res = await decoded_r.ft().search(
                Query("quick").scorer("DISMAX").with_scores()
            )
            assert 2.0 == res.docs[0].score
            res = await decoded_r.ft().search(
                Query("quick").scorer("DOCSCORE").with_scores()
            )
            assert 1.0 == res.docs[0].score
            res = await decoded_r.ft().search(
                Query("quick").scorer("HAMMING").with_scores()
            )
            assert 0.0 == res.docs[0].score
        elif expects_resp3_shape(decoded_r):
            res = await decoded_r.ft().search(Query("quick").with_scores())
            assert 0.23 == pytest.approx(res["results"][0]["score"], 0.05)
            res = await decoded_r.ft().search(
                Query("quick").scorer("TFIDF").with_scores()
            )
            assert 1.0 == res["results"][0]["score"]
            res = await decoded_r.ft().search(
                Query("quick").scorer("TFIDF.DOCNORM").with_scores()
            )
            assert 0.14285714285714285 == res["results"][0]["score"]
            res = await decoded_r.ft().search(
                Query("quick").scorer("BM25").with_scores()
            )
            assert 0.22471909420069797 == res["results"][0]["score"]
            res = await decoded_r.ft().search(
                Query("quick").scorer("DISMAX").with_scores()
            )
            assert 2.0 == res["results"][0]["score"]
            res = await decoded_r.ft().search(
                Query("quick").scorer("DOCSCORE").with_scores()
            )
            assert 1.0 == res["results"][0]["score"]
            res = await decoded_r.ft().search(
                Query("quick").scorer("HAMMING").with_scores()
            )
            assert 0.0 == res["results"][0]["score"]


class TestConfig(AsyncSearchTestsBase):
    @pytest.mark.redismod
    @pytest.mark.onlynoncluster
    @skip_ifmodversion_lt("2.2.0", "search")
    @skip_if_server_version_gte("7.9.0")
    async def test_config(self, decoded_r: redis.Redis):
        assert await decoded_r.ft().config_set("TIMEOUT", "100")
        with pytest.raises(redis.ResponseError):
            await decoded_r.ft().config_set("TIMEOUT", "null")
        res = await decoded_r.ft().config_get("*")
        assert "100" == res["TIMEOUT"]
        res = await decoded_r.ft().config_get("TIMEOUT")
        assert "100" == res["TIMEOUT"]

    @pytest.mark.redismod
    @pytest.mark.onlynoncluster
    @skip_if_server_version_lt("7.9.0")
    async def test_config_with_removed_ftconfig(self, decoded_r: redis.Redis):
        assert await decoded_r.config_set("timeout", "100")
        with pytest.raises(redis.ResponseError):
            await decoded_r.config_set("timeout", "null")
        res = await decoded_r.config_get("*")
        assert "100" == res["timeout"]
        res = await decoded_r.config_get("timeout")
        assert "100" == res["timeout"]


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


class TestAggregations(AsyncSearchTestsBase):
    @pytest.mark.redismod
    @pytest.mark.onlynoncluster
    async def test_aggregation_zero_limit(self, decoded_r: redis.Redis):
        await decoded_r.ft().create_index((TextField("title"),))
        for i in range(3):
            await decoded_r.hset(f"doc:{i}", mapping={"title": "redis"})
        await self.waitForIndex(decoded_r, "idx")

        res = await decoded_r.ft().aggregate(
            aggregations.AggregateRequest("redis").limit(0, 0)
        )

        if expects_resp3_shape(decoded_r):
            assert res["results"] == []
            assert res["total_results"] == 3
        else:
            assert res.rows == []
            expected_total = 3 if int(get_protocol_version(decoded_r)) == 3 else 0
            assert res.total == expected_total

    @pytest.mark.redismod
    @pytest.mark.onlynoncluster
    @skip_if_server_version_lt("8.4")
    async def test_aggregation_invalid_zero_limit(self, decoded_r: redis.Redis):
        await decoded_r.ft().create_index((TextField("title"),))
        await decoded_r.hset("doc:0", mapping={"title": "redis"})
        await self.waitForIndex(decoded_r, "idx")

        with pytest.raises(ResponseError):
            await decoded_r.ft().aggregate(
                aggregations.AggregateRequest("redis").limit(5, 0)
            )

    @pytest_asyncio.fixture
    async def collect_index(self, decoded_r):
        """Enable the preview feature and index the shared fruit documents.

        Restores ``search-enable-unstable-features`` to its original value on
        teardown so the preview flag does not leak into other tests.
        """
        config = await decoded_r.config_get("search-enable-unstable-features")
        original = config["search-enable-unstable-features"]
        await decoded_r.config_set("search-enable-unstable-features", "yes")
        await decoded_r.ft().create_index(
            (
                TextField("color"),
                TextField("name"),
                NumericField("sweetness"),
                TextField("origin"),
            )
        )
        for key, mapping in COLLECT_FRUITS.items():
            await decoded_r.hset(key, mapping=mapping)
        await self.waitForIndex(decoded_r, "idx")
        yield
        await decoded_r.config_set("search-enable-unstable-features", original)

    @pytest.mark.redismod
    @pytest.mark.onlynoncluster
    async def test_aggregations_groupby(self, decoded_r: redis.Redis):
        # Creating the index definition and schema
        await decoded_r.ft().create_index(
            (
                NumericField("random_num"),
                TextField("title"),
                TextField("body"),
                TextField("parent"),
            )
        )

        # Indexing a document
        await decoded_r.hset(
            "search",
            mapping={
                "title": "RediSearch",
                "body": "Redisearch impements a search engine on top of redis",
                "parent": "redis",
                "random_num": 10,
            },
        )
        await decoded_r.hset(
            "ai",
            mapping={
                "title": "RedisAI",
                "body": "RedisAI executes Deep Learning/Machine Learning models and managing their data.",  # noqa
                "parent": "redis",
                "random_num": 3,
            },
        )
        await decoded_r.hset(
            "json",
            mapping={
                "title": "RedisJson",
                "body": "RedisJSON implements ECMA-404 The JSON Data Interchange Standard as a native data type.",  # noqa
                "parent": "redis",
                "random_num": 8,
            },
        )

        for dialect in [1, 2]:
            if expects_resp2_shape(decoded_r) or expects_unified_shape(decoded_r):
                req = (
                    aggregations.AggregateRequest("redis")
                    .group_by("@parent", reducers.count())
                    .dialect(dialect)
                )

                res = (await decoded_r.ft().aggregate(req)).rows[0]
                assert res[1] == "redis"
                assert res[3] == "3"

                req = (
                    aggregations.AggregateRequest("redis")
                    .group_by("@parent", reducers.count_distinct("@title"))
                    .dialect(dialect)
                )

                res = (await decoded_r.ft().aggregate(req)).rows[0]
                assert res[1] == "redis"
                assert res[3] == "3"

                req = (
                    aggregations.AggregateRequest("redis")
                    .group_by("@parent", reducers.count_distinctish("@title"))
                    .dialect(dialect)
                )

                res = (await decoded_r.ft().aggregate(req)).rows[0]
                assert res[1] == "redis"
                assert res[3] == "3"

                req = (
                    aggregations.AggregateRequest("redis")
                    .group_by("@parent", reducers.sum("@random_num"))
                    .dialect(dialect)
                )

                res = (await decoded_r.ft().aggregate(req)).rows[0]
                assert res[1] == "redis"
                assert res[3] == "21"  # 10+8+3

                req = (
                    aggregations.AggregateRequest("redis")
                    .group_by("@parent", reducers.min("@random_num"))
                    .dialect(dialect)
                )

                res = (await decoded_r.ft().aggregate(req)).rows[0]
                assert res[1] == "redis"
                assert res[3] == "3"  # min(10,8,3)

                req = (
                    aggregations.AggregateRequest("redis")
                    .group_by("@parent", reducers.max("@random_num"))
                    .dialect(dialect)
                )

                res = (await decoded_r.ft().aggregate(req)).rows[0]
                assert res[1] == "redis"
                assert res[3] == "10"  # max(10,8,3)

                req = (
                    aggregations.AggregateRequest("redis")
                    .group_by("@parent", reducers.avg("@random_num"))
                    .dialect(dialect)
                )

                res = (await decoded_r.ft().aggregate(req)).rows[0]
                assert res[1] == "redis"
                assert res[3] == "7"  # (10+3+8)/3

                req = (
                    aggregations.AggregateRequest("redis")
                    .group_by("@parent", reducers.stddev("random_num"))
                    .dialect(dialect)
                )

                res = (await decoded_r.ft().aggregate(req)).rows[0]
                assert res[1] == "redis"
                assert res[3] == "3.60555127546"

                req = (
                    aggregations.AggregateRequest("redis")
                    .group_by("@parent", reducers.quantile("@random_num", 0.5))
                    .dialect(dialect)
                )

                res = (await decoded_r.ft().aggregate(req)).rows[0]
                assert res[1] == "redis"
                assert res[3] == "8"  # median of 3,8,10

                req = (
                    aggregations.AggregateRequest("redis")
                    .group_by("@parent", reducers.tolist("@title"))
                    .dialect(dialect)
                )

                res = (await decoded_r.ft().aggregate(req)).rows[0]
                assert res[1] == "redis"
                assert set(res[3]) == {"RediSearch", "RedisAI", "RedisJson"}

                req = (
                    aggregations.AggregateRequest("redis")
                    .group_by("@parent", reducers.first_value("@title").alias("first"))
                    .dialect(dialect)
                )

                res = (await decoded_r.ft().aggregate(req)).rows[0]
                assert res == ["parent", "redis", "first", "RediSearch"]

                req = (
                    aggregations.AggregateRequest("redis")
                    .group_by(
                        "@parent", reducers.random_sample("@title", 2).alias("random")
                    )
                    .dialect(dialect)
                )

                res = (await decoded_r.ft().aggregate(req)).rows[0]
                assert res[1] == "redis"
                assert res[2] == "random"
                assert len(res[3]) == 2
                assert res[3][0] in ["RediSearch", "RedisAI", "RedisJson"]
            elif expects_resp3_shape(decoded_r):
                req = (
                    aggregations.AggregateRequest("redis")
                    .group_by("@parent", reducers.count())
                    .dialect(dialect)
                )

                res = (await decoded_r.ft().aggregate(req))["results"][0]
                assert res["extra_attributes"]["parent"] == "redis"
                assert res["extra_attributes"]["__generated_aliascount"] == "3"

                req = (
                    aggregations.AggregateRequest("redis")
                    .group_by("@parent", reducers.count_distinct("@title"))
                    .dialect(dialect)
                )

                res = (await decoded_r.ft().aggregate(req))["results"][0]
                assert res["extra_attributes"]["parent"] == "redis"
                assert (
                    res["extra_attributes"]["__generated_aliascount_distincttitle"]
                    == "3"
                )

                req = (
                    aggregations.AggregateRequest("redis")
                    .group_by("@parent", reducers.count_distinctish("@title"))
                    .dialect(dialect)
                )

                res = (await decoded_r.ft().aggregate(req))["results"][0]
                assert res["extra_attributes"]["parent"] == "redis"
                assert (
                    res["extra_attributes"]["__generated_aliascount_distinctishtitle"]
                    == "3"
                )

                req = (
                    aggregations.AggregateRequest("redis")
                    .group_by("@parent", reducers.sum("@random_num"))
                    .dialect(dialect)
                )

                res = (await decoded_r.ft().aggregate(req))["results"][0]
                assert res["extra_attributes"]["parent"] == "redis"
                assert res["extra_attributes"]["__generated_aliassumrandom_num"] == "21"

                req = (
                    aggregations.AggregateRequest("redis")
                    .group_by("@parent", reducers.min("@random_num"))
                    .dialect(dialect)
                )

                res = (await decoded_r.ft().aggregate(req))["results"][0]
                assert res["extra_attributes"]["parent"] == "redis"
                assert res["extra_attributes"]["__generated_aliasminrandom_num"] == "3"

                req = (
                    aggregations.AggregateRequest("redis")
                    .group_by("@parent", reducers.max("@random_num"))
                    .dialect(dialect)
                )

                res = (await decoded_r.ft().aggregate(req))["results"][0]
                assert res["extra_attributes"]["parent"] == "redis"
                assert res["extra_attributes"]["__generated_aliasmaxrandom_num"] == "10"

                req = (
                    aggregations.AggregateRequest("redis")
                    .group_by("@parent", reducers.avg("@random_num"))
                    .dialect(dialect)
                )

                res = (await decoded_r.ft().aggregate(req))["results"][0]
                assert res["extra_attributes"]["parent"] == "redis"
                assert res["extra_attributes"]["__generated_aliasavgrandom_num"] == "7"

                req = (
                    aggregations.AggregateRequest("redis")
                    .group_by("@parent", reducers.stddev("random_num"))
                    .dialect(dialect)
                )

                res = (await decoded_r.ft().aggregate(req))["results"][0]
                assert res["extra_attributes"]["parent"] == "redis"
                assert (
                    res["extra_attributes"]["__generated_aliasstddevrandom_num"]
                    == "3.60555127546"
                )

                req = (
                    aggregations.AggregateRequest("redis")
                    .group_by("@parent", reducers.quantile("@random_num", 0.5))
                    .dialect(dialect)
                )

                res = (await decoded_r.ft().aggregate(req))["results"][0]
                assert res["extra_attributes"]["parent"] == "redis"
                assert (
                    res["extra_attributes"]["__generated_aliasquantilerandom_num,0.5"]
                    == "8"
                )

                req = (
                    aggregations.AggregateRequest("redis")
                    .group_by("@parent", reducers.tolist("@title"))
                    .dialect(dialect)
                )

                res = (await decoded_r.ft().aggregate(req))["results"][0]
                assert res["extra_attributes"]["parent"] == "redis"
                assert set(res["extra_attributes"]["__generated_aliastolisttitle"]) == {
                    "RediSearch",
                    "RedisAI",
                    "RedisJson",
                }

                req = (
                    aggregations.AggregateRequest("redis")
                    .group_by("@parent", reducers.first_value("@title").alias("first"))
                    .dialect(dialect)
                )

                res = (await decoded_r.ft().aggregate(req))["results"][0]
                assert res["extra_attributes"] == {
                    "parent": "redis",
                    "first": "RediSearch",
                }

                req = (
                    aggregations.AggregateRequest("redis")
                    .group_by(
                        "@parent", reducers.random_sample("@title", 2).alias("random")
                    )
                    .dialect(dialect)
                )

                res = (await decoded_r.ft().aggregate(req))["results"][0]
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
    async def test_aggregations_collect_sortby(
        self, decoded_r: redis.Redis, collect_index
    ):
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
        groups = collect_groups(
            decoded_r, await decoded_r.ft().aggregate(req), "fruits"
        )

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
    async def test_aggregations_collect_sortby_ascending(
        self, decoded_r: redis.Redis, collect_index
    ):
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
        groups = collect_groups(
            decoded_r, await decoded_r.ft().aggregate(req), "fruits"
        )

        assert groups["red"] == [
            {"name": "strawberry"},
            {"name": "apple"},
            {"name": "cherry"},
        ]
        assert groups["yellow"] == [{"name": "lemon"}, {"name": "banana"}]

    @pytest.mark.redismod
    @pytest.mark.onlynoncluster
    @skip_if_server_version_lt("8.9.0")
    async def test_aggregations_collect_limit_top_n(
        self, decoded_r: redis.Redis, collect_index
    ):
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
        groups = collect_groups(
            decoded_r, await decoded_r.ft().aggregate(req), "fruits"
        )

        # red desc: cherry, apple, strawberry -> skip 1, take 1 -> apple
        assert groups["red"] == [{"name": "apple"}]
        # yellow desc: banana, lemon -> skip 1, take 1 -> lemon
        assert groups["yellow"] == [{"name": "lemon"}]

    @pytest.mark.redismod
    @pytest.mark.onlynoncluster
    @skip_if_server_version_lt("8.9.0")
    async def test_aggregations_collect_multiple_sort_keys(
        self, decoded_r: redis.Redis, collect_index
    ):
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
        groups = collect_groups(
            decoded_r, await decoded_r.ft().aggregate(req), "fruits"
        )

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
    async def test_aggregations_collect_sparse_projection(
        self, decoded_r: redis.Redis, collect_index
    ):
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
        groups = collect_groups(
            decoded_r, await decoded_r.ft().aggregate(req), "fruits"
        )

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
    async def test_aggregations_collect_key_field(
        self, decoded_r: redis.Redis, collect_index
    ):
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
        groups = collect_groups(
            decoded_r, await decoded_r.ft().aggregate(req), "fruits"
        )

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
    async def test_aggregations_collect_fields_all(
        self, decoded_r: redis.Redis, collect_index
    ):
        # FIELDS * is stage-local: after GROUPBY only the group key is present.
        req = (
            aggregations.AggregateRequest("*")
            .load("*")
            .group_by("@color", reducers.collect("*").alias("fruits"))
        )
        groups = collect_groups(
            decoded_r, await decoded_r.ft().aggregate(req), "fruits"
        )

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
    async def test_aggregations_collect_single_str_field(
        self, decoded_r: redis.Redis, collect_index
    ):
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
        groups = collect_groups(
            decoded_r, await decoded_r.ft().aggregate(req), "fruits"
        )

        assert groups["red"] == [
            {"name": "strawberry"},
            {"name": "apple"},
            {"name": "cherry"},
        ]
        assert groups["yellow"] == [{"name": "lemon"}, {"name": "banana"}]

    @pytest.mark.redismod
    @pytest.mark.onlynoncluster
    @skip_if_server_version_lt("8.9.0")
    async def test_aggregations_collect_field_names_without_prefix(
        self, decoded_r: redis.Redis, collect_index
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
        groups = collect_groups(
            decoded_r, await decoded_r.ft().aggregate(req), "fruits"
        )

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
    async def test_aggregations_sort_by_and_limit(self, decoded_r: redis.Redis):
        await decoded_r.ft().create_index((TextField("t1"), TextField("t2")))

        await decoded_r.ft().client.hset("doc1", mapping={"t1": "a", "t2": "b"})
        await decoded_r.ft().client.hset("doc2", mapping={"t1": "b", "t2": "a"})

        if expects_resp2_shape(decoded_r) or expects_unified_shape(decoded_r):
            # test sort_by using SortDirection
            req = aggregations.AggregateRequest("*").sort_by(
                aggregations.Asc("@t2"), aggregations.Desc("@t1")
            )
            res = await decoded_r.ft().aggregate(req)
            assert res.rows[0] == ["t2", "a", "t1", "b"]
            assert res.rows[1] == ["t2", "b", "t1", "a"]

            # test sort_by without SortDirection
            req = aggregations.AggregateRequest("*").sort_by("@t1")
            res = await decoded_r.ft().aggregate(req)
            assert res.rows[0] == ["t1", "a"]
            assert res.rows[1] == ["t1", "b"]

            # test sort_by with max
            req = aggregations.AggregateRequest("*").sort_by("@t1", max=1)
            res = await decoded_r.ft().aggregate(req)
            assert len(res.rows) == 1

            # test limit
            req = aggregations.AggregateRequest("*").sort_by("@t1").limit(1, 1)
            res = await decoded_r.ft().aggregate(req)
            assert len(res.rows) == 1
            assert res.rows[0] == ["t1", "b"]
        elif expects_resp3_shape(decoded_r):
            # test sort_by using SortDirection
            req = aggregations.AggregateRequest("*").sort_by(
                aggregations.Asc("@t2"), aggregations.Desc("@t1")
            )
            res = (await decoded_r.ft().aggregate(req))["results"]
            assert res[0]["extra_attributes"] == {"t2": "a", "t1": "b"}
            assert res[1]["extra_attributes"] == {"t2": "b", "t1": "a"}

            # test sort_by without SortDirection
            req = aggregations.AggregateRequest("*").sort_by("@t1")
            res = (await decoded_r.ft().aggregate(req))["results"]
            assert res[0]["extra_attributes"] == {"t1": "a"}
            assert res[1]["extra_attributes"] == {"t1": "b"}

            # test sort_by with max
            req = aggregations.AggregateRequest("*").sort_by("@t1", max=1)
            res = await decoded_r.ft().aggregate(req)
            assert len(res["results"]) == 1

            # test limit
            req = aggregations.AggregateRequest("*").sort_by("@t1").limit(1, 1)
            res = await decoded_r.ft().aggregate(req)
            assert len(res["results"]) == 1
            assert res["results"][0]["extra_attributes"] == {"t1": "b"}

    @pytest.mark.redismod
    @pytest.mark.experimental
    async def test_withsuffixtrie(self, decoded_r: redis.Redis):
        # create index
        assert await decoded_r.ft().create_index((TextField("txt"),))
        await self.waitForIndex(decoded_r, getattr(decoded_r.ft(), "index_name", "idx"))
        if expects_resp2_shape(decoded_r):
            info = await decoded_r.ft().info()
            assert "WITHSUFFIXTRIE" not in info["attributes"][0]
            assert await decoded_r.ft().dropindex()

            # create withsuffixtrie index (text field)
            assert await decoded_r.ft().create_index(
                TextField("t", withsuffixtrie=True)
            )
            await self.waitForIndex(
                decoded_r, getattr(decoded_r.ft(), "index_name", "idx")
            )
            info = await decoded_r.ft().info()
            assert "WITHSUFFIXTRIE" in info["attributes"][0]
            assert await decoded_r.ft().dropindex()

            # create withsuffixtrie index (tag field)
            assert await decoded_r.ft().create_index(TagField("t", withsuffixtrie=True))
            await self.waitForIndex(
                decoded_r, getattr(decoded_r.ft(), "index_name", "idx")
            )
            info = await decoded_r.ft().info()
            assert "WITHSUFFIXTRIE" in info["attributes"][0]
        elif expects_resp3_shape(decoded_r) or expects_unified_shape(decoded_r):
            info = await decoded_r.ft().info()
            assert "WITHSUFFIXTRIE" not in info["attributes"][0]["flags"]
            assert await decoded_r.ft().dropindex()

            # create withsuffixtrie index (text fields)
            assert await decoded_r.ft().create_index(
                TextField("t", withsuffixtrie=True)
            )
            await self.waitForIndex(
                decoded_r, getattr(decoded_r.ft(), "index_name", "idx")
            )
            info = await decoded_r.ft().info()
            assert "WITHSUFFIXTRIE" in info["attributes"][0]["flags"]
            assert await decoded_r.ft().dropindex()

            # create withsuffixtrie index (tag field)
            assert await decoded_r.ft().create_index(TagField("t", withsuffixtrie=True))
            await self.waitForIndex(
                decoded_r, getattr(decoded_r.ft(), "index_name", "idx")
            )
            info = await decoded_r.ft().info()
            assert "WITHSUFFIXTRIE" in info["attributes"][0]["flags"]

    @pytest.mark.redismod
    @skip_ifmodversion_lt("2.10.05", "search")
    async def test_aggregations_add_scores(self, decoded_r: redis.Redis):
        assert await decoded_r.ft().create_index(
            (
                TextField("name", sortable=True, weight=5.0),
                NumericField("age", sortable=True),
            )
        )

        assert await decoded_r.hset("doc1", mapping={"name": "bar", "age": "25"})
        assert await decoded_r.hset("doc2", mapping={"name": "foo", "age": "19"})

        req = aggregations.AggregateRequest("*").add_scores()
        res = await decoded_r.ft().aggregate(req)

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
    async def test_aggregations_hybrid_scoring(self, decoded_r: redis.Redis):
        assert await decoded_r.ft().create_index(
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

        assert await decoded_r.hset(
            "doc1",
            mapping={
                "name": "cat book",
                "description": "an animal book about cats",
                "vector": np.array([0.1, 0.2]).astype(np.float32).tobytes(),
            },
        )
        assert await decoded_r.hset(
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

        res = await decoded_r.ft().aggregate(
            req,
            query_params={
                "vec_param": np.array([0.11, 0.22]).astype(np.float32).tobytes()
            },
        )

        if isinstance(res, dict):
            assert len(res["results"]) == 2
        else:
            assert len(res.rows) == 2
            for row in res.rows:
                len(row) == 6


class TestPipeline(AsyncSearchTestsBase):
    @pytest.mark.redismod
    @skip_if_redis_enterprise()
    async def test_search_commands_in_pipeline(self, decoded_r: redis.Redis):
        p = await decoded_r.ft().pipeline()
        p.create_index((TextField("txt"),))
        p.hset("doc1", mapping={"txt": "foo bar"})
        p.hset("doc2", mapping={"txt": "foo bar"})
        q = Query("foo bar").with_payloads()
        await p.search(q)
        res = await p.execute()
        if expects_resp2_shape(decoded_r):
            assert res[:3] == ["OK", True, True]
            assert 2 == res[3][0]
            assert "doc1" == res[3][1]
            assert "doc2" == res[3][4]
            assert res[3][5] is None
            assert res[3][3] == res[3][6] == ["txt", "foo bar"]
        elif expects_unified_shape(decoded_r):
            assert res[:3] == ["OK", True, True]
            assert isinstance(res[3], Result)
            assert 2 == res[3].total
            assert "doc1" == res[3].docs[0].id
            assert "doc2" == res[3].docs[1].id
            assert res[3].docs[0].payload is None
            assert res[3].docs[0].txt == res[3].docs[1].txt == "foo bar"
        elif expects_resp3_shape(decoded_r):
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
    async def test_parsed_commands_return_the_pipeline(self, decoded_r: redis.Redis):
        p = decoded_r.ft().pipeline()
        assert all(res is p for res in await _queue_parsed_commands(p))
        p = decoded_r.pipeline()
        assert all(res is p for res in await _queue_parsed_commands(p.ft()))

    @pytest.mark.redismod
    @pytest.mark.onlynoncluster
    @skip_if_redis_enterprise()
    async def test_parsed_commands_execute_in_pipeline(self, decoded_r: redis.Redis):
        await decoded_r.ft().create_index((TextField("txt"),))
        await decoded_r.hset("doc1", mapping={"txt": "foo bar"})
        await decoded_r.ft().synupdate("id1", False, "foo", "bar")
        await decoded_r.ft().sugadd("ac", Suggestion("hello"))
        p = decoded_r.ft().pipeline()
        await p.info()
        await p.aggregate(aggregations.AggregateRequest("*"))
        p.profile(Query("foo"))
        await p.spellcheck("fo")
        p.syndump()
        await p.sugget("ac", "he")
        assert len(await p.execute()) == 6

    @pytest.mark.redismod
    @skip_if_redis_enterprise()
    async def test_search_through_pipeline_ft_is_decoded(self, decoded_r: redis.Redis):
        await decoded_r.ft().create_index((TextField("txt"),))
        await decoded_r.hset("doc1", mapping={"txt": "foo bar"})
        p = decoded_r.pipeline()
        await p.ft().search(Query("foo bar"))
        res = await p.execute()
        assert not _contains_bytes(res[0])

    @pytest.mark.redismod
    @skip_if_redis_enterprise()
    @skip_if_server_version_lt("8.9.0")
    async def test_aliaslist_through_pipeline_ft(self, decoded_r: redis.Redis):
        index = decoded_r.ft("aliaslistpipeidx")
        await index.create_index((TextField("txt"),))
        await index.aliasadd("alias1")
        p = decoded_r.pipeline()
        await p.ft("aliaslistpipeidx").aliaslist()
        res = await p.execute()
        assert set(res[0]) == {"alias1"}

    @pytest.mark.redismod
    @skip_if_redis_enterprise()
    @skip_if_server_version_lt("8.9.0")
    async def test_aliaslist_in_pipeline(self, decoded_r: redis.Redis):
        index = decoded_r.ft("aliaslistpipeidx")
        await index.create_index((TextField("txt"),))

        # An empty listing normalizes to ``set()`` through the pipeline, not
        # the raw ``[]`` wire response.
        p = index.pipeline()
        await p.aliaslist()
        res = await p.execute()
        assert isinstance(res[0], set)
        assert res[0] == set()

        # Aliases are returned as an unordered set, matching the direct call.
        await index.aliasadd("alias1")
        await index.aliasadd("alias2")
        p = index.pipeline()
        await p.aliaslist()
        res = await p.execute()
        assert isinstance(res[0], set)
        assert res[0] == {"alias1", "alias2"}

    @pytest.mark.redismod
    @skip_if_server_version_lt("8.3.224")
    async def test_hybrid_search_query_with_pipeline(self, decoded_r: redis.Redis):
        p = decoded_r.ft().pipeline()
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

        hybrid_query = HybridQuery(search_query, vsim_query)

        await p.hybrid_search(
            query=hybrid_query,
            params_substitution={
                "vec": np.array([2, 2, 3, 3], dtype=np.float32).tobytes()
            },
        )
        res = await p.execute()

        # the default results count limit is 10
        assert res[:3] == ["OK", 2, 2]
        hybrid_search_res = res[3]
        if expects_resp2_shape(decoded_r):
            # it doesn't get parsed to object in pipeline
            assert hybrid_search_res[0] == "total_results"
            assert hybrid_search_res[1] == 2
            assert hybrid_search_res[2] == "results"
            assert len(hybrid_search_res[3]) == 2
            assert hybrid_search_res[4] == "warnings"
            assert hybrid_search_res[5] == []
            assert hybrid_search_res[6] == "execution_time"
            assert float(hybrid_search_res[7]) > 0
        elif expects_unified_shape(decoded_r):
            assert hybrid_search_res.total_results == 2
            assert len(hybrid_search_res.results) == 2
            assert hybrid_search_res.warnings == []
            assert hybrid_search_res.execution_time > 0
        elif expects_resp3_shape(decoded_r):
            assert hybrid_search_res["total_results"] == 2
            assert len(hybrid_search_res["results"]) == 2
            assert hybrid_search_res["warnings"] == []
            assert hybrid_search_res["execution_time"] > 0


class TestSearchWithVamana(AsyncSearchTestsBase):
    # SVS-VAMANA Async Tests
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
        # Field construction has no I/O, so this mirrors the sync serialization
        # test and is not an async test.
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
    async def test_async_hnsw_sq8_compression(self, decoded_r: redis.Redis):
        await decoded_r.ft().create_index(
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

        pipe = decoded_r.pipeline(transaction=False)
        for i in range(20):
            vec = np.array([float(i + j) for j in range(8)], dtype=np.float32)
            pipe.hset(
                f"doc{i}",
                mapping={
                    "sq8": vec.tobytes(),
                    "sq8_default": vec.astype(np.float16).tobytes(),
                },
            )
        await pipe.execute()

        query = Query("*=>[KNN 5 @sq8 $vec as score]").no_content()
        query_params = {"vec": np.arange(8, dtype=np.float32).tobytes()}
        res = await decoded_r.ft().search(query, query_params=query_params)
        if expects_resp2_shape(decoded_r) or expects_unified_shape(decoded_r):
            assert res.total == 5
            assert "doc0" == res.docs[0].id
        elif expects_resp3_shape(decoded_r):
            assert res["total_results"] == 5
            assert "doc0" == res["results"][0]["id"]

        attrs = (await decoded_r.ft().info())["attributes"]
        if expects_resp2_shape(decoded_r):
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
    async def test_async_hnsw_sq8_compression_errors(self, decoded_r: redis.Redis):
        with pytest.raises(ResponseError, match="FLOAT32 and FLOAT16"):
            await decoded_r.ft("idx_int8").create_index(
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
            await decoded_r.ft("idx_no_compression").create_index(
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

    @pytest.mark.redismod
    @skip_if_server_version_lt("8.1.224")
    async def test_async_svs_vamana_basic_functionality(self, decoded_r: redis.Redis):
        await decoded_r.ft().create_index(
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

        pipe = decoded_r.pipeline(transaction=False)
        for i, vec in enumerate(vectors):
            pipe.hset(f"doc{i}", "v", np.array(vec, dtype=np.float32).tobytes())
        await pipe.execute()

        query = "*=>[KNN 3 @v $vec]"
        q = Query(query).return_field("__v_score").sort_by("__v_score", True)
        res = await decoded_r.ft().search(
            q, query_params={"vec": np.array(vectors[0], dtype=np.float32).tobytes()}
        )

        if expects_resp2_shape(decoded_r) or expects_unified_shape(decoded_r):
            assert res.total == 3
            assert "doc0" == res.docs[0].id
        elif expects_resp3_shape(decoded_r):
            assert res["total_results"] == 3
            assert "doc0" == res["results"][0]["id"]

    @pytest.mark.redismod
    @skip_if_server_version_lt("8.1.224")
    async def test_async_svs_vamana_distance_metrics(self, decoded_r: redis.Redis):
        # Test COSINE distance
        await decoded_r.ft().create_index(
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

        pipe = decoded_r.pipeline(transaction=False)
        for i, vec in enumerate(vectors):
            pipe.hset(f"doc{i}", "v", np.array(vec, dtype=np.float32).tobytes())
        await pipe.execute()

        query = Query("*=>[KNN 2 @v $vec as score]").sort_by("score").no_content()
        query_params = {"vec": np.array(vectors[0], dtype=np.float32).tobytes()}

        res = await decoded_r.ft().search(query, query_params=query_params)
        if expects_resp2_shape(decoded_r) or expects_unified_shape(decoded_r):
            assert res.total == 2
            assert "doc0" == res.docs[0].id
        elif expects_resp3_shape(decoded_r):
            assert res["total_results"] == 2
            assert "doc0" == res["results"][0]["id"]

    @pytest.mark.redismod
    @skip_if_server_version_lt("8.1.224")
    async def test_async_svs_vamana_vector_types(self, decoded_r: redis.Redis):
        # Test FLOAT16
        await decoded_r.ft("idx16").create_index(
            (
                VectorField(
                    "v16",
                    "SVS-VAMANA",
                    {"TYPE": "FLOAT16", "DIM": 4, "DISTANCE_METRIC": "L2"},
                ),
            )
        )

        vectors = [[1.5, 2.5, 3.5, 4.5], [2.5, 3.5, 4.5, 5.5], [3.5, 4.5, 5.5, 6.5]]

        pipe = decoded_r.pipeline(transaction=False)
        for i, vec in enumerate(vectors):
            pipe.hset(f"doc16_{i}", "v16", np.array(vec, dtype=np.float16).tobytes())
        await pipe.execute()

        query = Query("*=>[KNN 2 @v16 $vec as score]").no_content()
        query_params = {"vec": np.array(vectors[0], dtype=np.float16).tobytes()}

        res = await decoded_r.ft("idx16").search(query, query_params=query_params)
        if expects_resp2_shape(decoded_r) or expects_unified_shape(decoded_r):
            assert res.total == 2
            assert "doc16_0" == res.docs[0].id
        elif expects_resp3_shape(decoded_r):
            assert res["total_results"] == 2
            assert "doc16_0" == res["results"][0]["id"]

    @pytest.mark.redismod
    @skip_if_server_version_lt("8.1.224")
    async def test_async_svs_vamana_compression(self, decoded_r: redis.Redis):
        await decoded_r.ft().create_index(
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
        pipe = decoded_r.pipeline(transaction=False)
        for i in range(20):
            vec = [float(i + j) for j in range(8)]
            vectors.append(vec)
            pipe.hset(f"doc{i}", "v", np.array(vec, dtype=np.float32).tobytes())
        await pipe.execute()

        query = Query("*=>[KNN 5 @v $vec as score]").no_content()
        query_params = {"vec": np.array(vectors[0], dtype=np.float32).tobytes()}

        res = await decoded_r.ft().search(query, query_params=query_params)
        if expects_resp2_shape(decoded_r) or expects_unified_shape(decoded_r):
            assert res.total == 5
            assert "doc0" == res.docs[0].id
        elif expects_resp3_shape(decoded_r):
            assert res["total_results"] == 5
            assert "doc0" == res["results"][0]["id"]

    @pytest.mark.redismod
    @skip_if_server_version_lt("8.1.224")
    async def test_async_svs_vamana_build_parameters(self, decoded_r: redis.Redis):
        await decoded_r.ft().create_index(
            (
                VectorField(
                    "v",
                    "SVS-VAMANA",
                    {
                        "TYPE": "FLOAT32",
                        "DIM": 6,
                        "DISTANCE_METRIC": "COSINE",
                        "CONSTRUCTION_WINDOW_SIZE": 300,
                        "GRAPH_MAX_DEGREE": 64,
                        "SEARCH_WINDOW_SIZE": 20,
                        "EPSILON": 0.05,
                    },
                ),
            )
        )

        vectors = []
        pipe = decoded_r.pipeline(transaction=False)
        for i in range(15):
            vec = [float(i + j) for j in range(6)]
            vectors.append(vec)
            pipe.hset(f"doc{i}", "v", np.array(vec, dtype=np.float32).tobytes())
        await pipe.execute()

        query = Query("*=>[KNN 3 @v $vec as score]").no_content()
        query_params = {"vec": np.array(vectors[0], dtype=np.float32).tobytes()}

        res = await decoded_r.ft().search(query, query_params=query_params)
        if expects_resp2_shape(decoded_r) or expects_unified_shape(decoded_r):
            assert res.total == 3
            assert "doc0" == res.docs[0].id
        elif expects_resp3_shape(decoded_r):
            assert res["total_results"] == 3
            assert "doc0" == res["results"][0]["id"]


class TestHybridSearch(AsyncSearchTestsBase):
    _HYBRID_TIMEOUT_DIM = 8192
    # The 1ms timeout only fires once the query runs long enough to hit a
    # server timeout checkpoint; smaller data sets slip through and return no
    # warnings. 6000 docs keeps the query comfortably above the 1ms limit so
    # the timeout reliably triggers across hardware.
    _HYBRID_TIMEOUT_DOCS = 6000

    @pytest.mark.fixed_client
    async def test_hybrid_search_forwards_zero_timeout(self):
        # Async mirror of the sync test with the same name. TIMEOUT 0 means
        # "no timeout" and must reach the wire instead of being dropped like
        # an unset argument.
        hybrid_query = HybridQuery(
            HybridSearchQuery("foo"),
            HybridVsimQuery(vector_field_name="@embedding", vector_data="$vec"),
        )
        ft = redis.Redis().ft("idx")

        async def wire_args(timeout):
            with mock.patch.object(
                ft, "execute_command", mock.AsyncMock(return_value={})
            ) as m:
                await ft.hybrid_search(query=hybrid_query, timeout=timeout)
                return list(m.call_args[0])

        assert (await wire_args(5000))[-2:] == ["TIMEOUT", 5000]
        assert (await wire_args(0))[-2:] == ["TIMEOUT", 0]
        # None stays the "argument not set" sentinel.
        assert "TIMEOUT" not in await wire_args(None)

    async def _create_hybrid_search_index(self, decoded_r: redis.Redis, dim=4):
        await decoded_r.ft().create_index(
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
        await AsyncSearchTestsBase.waitForIndex(decoded_r, "idx")

    async def _create_hybrid_search_timeout_index(self, decoded_r: redis.Redis):
        await decoded_r.ft().create_index(
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
        await AsyncSearchTestsBase.waitForIndex(decoded_r, "idx")

    async def _add_data_for_hybrid_search_timeout(self, decoded_r: redis.Redis):
        vectors = [
            np.full(self._HYBRID_TIMEOUT_DIM, value, dtype=np.float32).tobytes()
            for value in (0.1, 0.2, 0.3, 0.4, 0.5)
        ]
        pipeline = decoded_r.pipeline(transaction=False)
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
                await pipeline.execute()
                pipeline = decoded_r.pipeline(transaction=False)
        await pipeline.execute()

    @staticmethod
    def _generate_random_vector(dim):
        return [random.random() for _ in range(dim)]

    @staticmethod
    def _generate_random_str_data(dim):
        chars = "abcdefgh12345678"
        return "".join(random.choice(chars) for _ in range(dim))

    @staticmethod
    async def _add_data_for_hybrid_search(
        client: redis.Redis,
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
            pipeline.hset(f"item:{i}", mapping=mapping)
            if (i + 1) % batch_size == 0:
                await pipeline.execute()
                pipeline = client.pipeline(transaction=False)
        await pipeline.execute()  # Execute remaining

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
    @skip_if_server_version_lt("8.3.224")
    async def test_basic_hybrid_search(self, decoded_r):
        # Create index and add data
        await self._create_hybrid_search_index(decoded_r)
        await self._add_data_for_hybrid_search(decoded_r, items_sets=5)

        # set search query
        search_query = HybridSearchQuery("@color:{red} @color:{green}")

        vsim_query = HybridVsimQuery(
            vector_field_name="@embedding",
            vector_data="$vec",
        )

        hybrid_query = HybridQuery(search_query, vsim_query)

        res = await decoded_r.ft().hybrid_search(
            query=hybrid_query,
            params_substitution={
                "vec": np.array([-100, -200, -200, -300], dtype=np.float32).tobytes()
            },
        )

        # the default results count limit is 10
        if expects_resp2_shape(decoded_r) or expects_unified_shape(decoded_r):
            assert res.total_results == 10
            assert len(res.results) == 10
            assert res.warnings == []
            assert res.execution_time > 0
            assert all(isinstance(res.results[i]["__score"], bytes) for i in range(10))
            assert all(isinstance(res.results[i]["__key"], bytes) for i in range(10))
        elif expects_resp3_shape(decoded_r):
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
    async def test_hybrid_search_query_with_scorer(self, decoded_r):
        # Create index and add data
        await self._create_hybrid_search_index(decoded_r)
        await self._add_data_for_hybrid_search(decoded_r, items_sets=10)

        # set search query
        search_query = HybridSearchQuery("shoes")
        search_query.scorer("TFIDF")

        vsim_query = HybridVsimQuery(
            vector_field_name="@embedding",
            vector_data="$vec",
        )

        hybrid_query = HybridQuery(search_query, vsim_query)

        combine_method = CombineResultsMethod(
            CombinationMethods.LINEAR, ALPHA=1, BETA=0
        )

        postprocessing_config = HybridPostProcessingConfig()
        postprocessing_config.load(
            "@description", "@color", "@price", "@size", "@__score", "@__item"
        )
        postprocessing_config.limit(0, 2)

        res = await decoded_r.ft().hybrid_search(
            query=hybrid_query,
            combine_method=combine_method,
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

        if expects_resp2_shape(decoded_r) or expects_unified_shape(decoded_r):
            assert res.total_results >= 2
            assert len(res.results) == 2
            assert res.results == expected_results_tfidf
            assert res.warnings == []
        elif expects_resp3_shape(decoded_r):
            assert res["total_results"] >= 2
            assert len(res["results"]) == 2
            assert res["results"] == expected_results_tfidf
            assert res["warnings"] == []

        search_query.scorer("BM25")
        res = await decoded_r.ft().hybrid_search(
            query=hybrid_query,
            combine_method=combine_method,
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
        if expects_resp2_shape(decoded_r) or expects_unified_shape(decoded_r):
            assert res.total_results >= 2
            assert len(res.results) == 2
            assert res.results == expected_results_bm25
            assert res.warnings == []
        elif expects_resp3_shape(decoded_r):
            assert res["total_results"] >= 2
            assert len(res["results"]) == 2
            assert res["results"] == expected_results_bm25
            assert res["warnings"] == []

    @pytest.mark.redismod
    @skip_if_server_version_lt("8.3.224")
    async def test_hybrid_search_query_with_vsim_filter(self, decoded_r):
        # Create index and add data
        await self._create_hybrid_search_index(decoded_r)
        await self._add_data_for_hybrid_search(
            decoded_r, items_sets=5, use_random_str_data=True
        )

        search_query = HybridSearchQuery("@color:{missing}")

        vsim_query = HybridVsimQuery(
            vector_field_name="@embedding",
            vector_data="$vec",
        )
        vsim_query.filter(HybridFilter("@price:[15 16] @size:[10 11]"))

        hybrid_query = HybridQuery(search_query, vsim_query)

        postprocessing_config = HybridPostProcessingConfig()
        postprocessing_config.load("@price", "@size")

        res = await decoded_r.ft().hybrid_search(
            query=hybrid_query,
            post_processing=postprocessing_config,
            params_substitution={"vec": "abcd1234efgh5678"},
            timeout=10,
        )
        if expects_resp2_shape(decoded_r) or expects_unified_shape(decoded_r):
            assert len(res.results) > 0
            assert res.warnings == []
            for item in res.results:
                assert item["price"] in [b"15", b"16"]
                assert item["size"] in [b"10", b"11"]
        elif expects_resp3_shape(decoded_r):
            assert len(res["results"]) > 0
            assert res["warnings"] == []
            for item in res["results"]:
                assert item["price"] in [b"15", b"16"]
                assert item["size"] in [b"10", b"11"]

    @pytest.mark.redismod
    @skip_if_server_version_lt("8.3.224")
    async def test_hybrid_search_query_with_vsim_knn(self, decoded_r):
        # Create index and add data
        await self._create_hybrid_search_index(decoded_r)
        await self._add_data_for_hybrid_search(decoded_r, items_sets=10)

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

        res = await decoded_r.ft().hybrid_search(
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
        if expects_resp2_shape(decoded_r) or expects_unified_shape(decoded_r):
            assert res.total_results == 3  # KNN top-k value
            assert len(res.results) == 3
            assert res.results == expected_results
            assert res.warnings == []
            assert res.execution_time > 0
        elif expects_resp3_shape(decoded_r):
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

        res2 = await decoded_r.ft().hybrid_search(
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

        if expects_resp2_shape(decoded_r) or expects_unified_shape(decoded_r):
            assert res2.total_results == 3  # KNN top-k value
            assert len(res2.results) == 3

            for result in res2.results:
                assert set(result.keys()) == expected_keys
            assert res2.warnings == []
            assert res2.execution_time > 0
        elif expects_resp3_shape(decoded_r):
            assert res2["total_results"] == 3  # KNN top-k value
            assert len(res2["results"]) == 3

            for result in res2["results"]:
                assert set(result.keys()) == expected_keys
            assert res2["warnings"] == []
            assert res2["execution_time"] > 0

    @pytest.mark.redismod
    @skip_if_server_version_lt("8.3.224")
    async def test_hybrid_search_query_with_vsim_range(self, decoded_r):
        # Create index and add data
        await self._create_hybrid_search_index(decoded_r)
        await self._add_data_for_hybrid_search(decoded_r, items_sets=10)

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

        res = await decoded_r.ft().hybrid_search(
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

        if expects_resp2_shape(decoded_r) or expects_unified_shape(decoded_r):
            assert res.total_results >= 3  # at least 3 results
            assert len(res.results) == 3
            for result in res.results:
                assert set(result.keys()) == expected_keys
            assert res.warnings == []
            assert res.execution_time > 0
        elif expects_resp3_shape(decoded_r):
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

        res = await decoded_r.ft().hybrid_search(
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
        if expects_resp2_shape(decoded_r) or expects_unified_shape(decoded_r):
            assert res.total_results >= 3
            assert len(res.results) == 3
            for result in res.results:
                assert set(result.keys()) == expected_keys
            assert res.warnings == []
            assert res.execution_time > 0
        elif expects_resp3_shape(decoded_r):
            assert res["total_results"] >= 3
            assert len(res["results"]) == 3
            for result in res["results"]:
                assert set(result.keys()) == expected_keys
            assert res["warnings"] == []
            assert res["execution_time"] > 0

    @pytest.mark.redismod
    @skip_if_server_version_lt("8.3.224")
    async def test_hybrid_search_query_with_combine_all_score_aliases(self, decoded_r):
        # Create index and add data
        await self._create_hybrid_search_index(decoded_r)
        # The assertions name the exact KNN winners, so keep vectors deterministic.
        await self._add_data_for_hybrid_search(decoded_r, items_sets=1)

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

        res = await decoded_r.ft().hybrid_search(
            query=hybrid_query,
            combine_method=combine_method,
            params_substitution={
                "vec": np.array([1, 2, 7, 8], dtype=np.float32).tobytes()
            },
            timeout=10,
        )

        if expects_resp2_shape(decoded_r) or expects_unified_shape(decoded_r):
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

        elif expects_resp3_shape(decoded_r):
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
    async def test_hybrid_search_query_with_combine(self, decoded_r):
        # Create index and add data
        await self._create_hybrid_search_index(decoded_r)
        await self._add_data_for_hybrid_search(decoded_r, items_sets=10)

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

        res = await decoded_r.ft().hybrid_search(
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
        if expects_resp2_shape(decoded_r) or expects_unified_shape(decoded_r):
            assert res.total_results >= 3
            assert len(res.results) == 3
            assert res.results == expected_results
            assert res.warnings == []
            assert res.execution_time > 0
        elif expects_resp3_shape(decoded_r):
            assert res["total_results"] >= 3
            assert len(res["results"]) == 3
            assert res["results"] == expected_results
            assert res["warnings"] == []
            assert res["execution_time"] > 0

        # combine with RRF and WINDOW + CONSTANT
        combine_method_rrf = CombineResultsMethod(
            CombinationMethods.RRF, WINDOW=3, CONSTANT=0.5
        )
        res = await decoded_r.ft().hybrid_search(
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
        if expects_resp2_shape(decoded_r) or expects_unified_shape(decoded_r):
            assert res.total_results >= 3
            assert len(res.results) == 3
            assert res.results == expected_results
            assert res.warnings == []
            assert res.execution_time > 0
        elif expects_resp3_shape(decoded_r):
            assert res["total_results"] >= 3
            assert len(res["results"]) == 3
            assert res["results"] == expected_results
            assert res["warnings"] == []
            assert res["execution_time"] > 0

        # combine with RRF, not all possible params provided
        combine_method_rrf_2 = CombineResultsMethod(CombinationMethods.RRF, WINDOW=3)
        res = await decoded_r.ft().hybrid_search(
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
        if expects_resp2_shape(decoded_r) or expects_unified_shape(decoded_r):
            assert res.total_results >= 3
            assert len(res.results) == 3
            assert res.results == expected_results
            assert res.warnings == []
            assert res.execution_time > 0
        elif expects_resp3_shape(decoded_r):
            assert res["total_results"] >= 3
            assert len(res["results"]) == 3
            assert res["results"] == expected_results
            assert res["warnings"] == []
            assert res["execution_time"] > 0

    @pytest.mark.redismod
    @skip_if_server_version_lt("8.3.224")
    async def test_hybrid_search_query_with_load(self, decoded_r):
        # Create index and add data
        await self._create_hybrid_search_index(decoded_r)
        await self._add_data_for_hybrid_search(decoded_r, items_sets=10)

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

        res = await decoded_r.ft().hybrid_search(
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
        if expects_resp2_shape(decoded_r) or expects_unified_shape(decoded_r):
            assert res.total_results >= 1
            assert len(res.results) == 1
            self.compare_list_of_dicts(res.results, expected_results)
            assert res.warnings == []
            assert res.execution_time > 0
        elif expects_resp3_shape(decoded_r):
            assert res["total_results"] >= 1
            assert len(res["results"]) == 1
            self.compare_list_of_dicts(res["results"], expected_results)
            assert res["warnings"] == []
            assert res["execution_time"] > 0

    @pytest.mark.redismod
    @skip_if_server_version_lt("8.3.224")
    async def test_hybrid_search_query_with_binary_load(self, decoded_r):
        await self._create_hybrid_search_index(decoded_r)
        await self._add_data_for_hybrid_search(decoded_r, items_sets=1)

        search_query = HybridSearchQuery("@color:{red}")
        vsim_query = HybridVsimQuery(
            vector_field_name="@embedding",
            vector_data="$vec",
        )
        hybrid_query = HybridQuery(search_query, vsim_query)

        postprocessing_config = HybridPostProcessingConfig()
        postprocessing_config.load("@embedding", decode_field=False)
        postprocessing_config.limit(0, 1)

        res = await decoded_r.ft().hybrid_search(
            query=hybrid_query,
            post_processing=postprocessing_config,
            params_substitution={
                "vec": np.array([1, 2, 7, 8], dtype=np.float32).tobytes()
            },
            timeout=10,
        )

        if expects_resp2_shape(decoded_r) or expects_unified_shape(decoded_r):
            loaded_embedding = res.results[0]["embedding"]
            assert res.warnings == []
        elif expects_resp3_shape(decoded_r):
            loaded_embedding = res["results"][0]["embedding"]
            assert res["warnings"] == []

        assert isinstance(loaded_embedding, bytes)
        assert np.frombuffer(loaded_embedding, dtype=np.float32).shape == (4,)

    @pytest.mark.redismod
    @skip_if_server_version_lt("8.3.224")
    async def test_hybrid_search_query_with_load_and_apply(self, decoded_r):
        # Create index and add data
        await self._create_hybrid_search_index(decoded_r)
        await self._add_data_for_hybrid_search(decoded_r, items_sets=10)

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

        res = await decoded_r.ft().hybrid_search(
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
        if expects_resp2_shape(decoded_r) or expects_unified_shape(decoded_r):
            assert len(res.results) == 3
            self.compare_list_of_dicts(res.results, expected_results)
            assert res.warnings == []
            assert res.execution_time > 0
        elif expects_resp3_shape(decoded_r):
            assert len(res["results"]) == 3
            self.compare_list_of_dicts(res["results"], expected_results)
            assert res["warnings"] == []
            assert res["execution_time"] > 0

    @pytest.mark.redismod
    @skip_if_server_version_lt("8.3.224")
    async def test_hybrid_search_query_with_load_and_filter(self, decoded_r):
        # Create index and add data
        await self._create_hybrid_search_index(decoded_r)
        await self._add_data_for_hybrid_search(decoded_r, items_sets=10)

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

        res = await decoded_r.ft().hybrid_search(
            query=hybrid_query,
            post_processing=postprocessing_config,
            params_substitution={
                "vec": np.array([1, 2, 7, 6], dtype=np.float32).tobytes()
            },
            timeout=10,
        )

        if expects_resp2_shape(decoded_r) or expects_unified_shape(decoded_r):
            assert len(res.results) == 3
            for item in res.results:
                assert item["price"] == b"15"
            assert res.warnings == []
            assert res.execution_time > 0
        elif expects_resp3_shape(decoded_r):
            assert len(res["results"]) == 3
            for item in res["results"]:
                assert item["price"] == b"15"
            assert res["warnings"] == []
            assert res["execution_time"] > 0

    @pytest.mark.redismod
    @skip_if_server_version_lt("8.3.224")
    async def test_hybrid_search_query_with_load_apply_and_params(self, decoded_r):
        # Create index and add data
        await self._create_hybrid_search_index(decoded_r)
        await self._add_data_for_hybrid_search(
            decoded_r, items_sets=5, use_random_str_data=True
        )

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

        res = await decoded_r.ft().hybrid_search(
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
        if expects_resp2_shape(decoded_r) or expects_unified_shape(decoded_r):
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
        elif expects_resp3_shape(decoded_r):
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
    async def test_hybrid_search_query_with_zero_limit(self, decoded_r):
        await self._create_hybrid_search_index(decoded_r)
        await self._add_data_for_hybrid_search(decoded_r)

        hybrid_query = HybridQuery(
            HybridSearchQuery("@color:{red|green|orange|black}"),
            HybridVsimQuery(
                vector_field_name="@embedding",
                vector_data="$vec",
                vsim_search_method=VectorSearchMethods.KNN,
                vsim_search_method_params={"K": 5},
            ),
        )
        params = {"vec": np.array([1, 2, 7, 6], dtype=np.float32).tobytes()}

        res = await decoded_r.ft().hybrid_search(
            query=hybrid_query,
            post_processing=HybridPostProcessingConfig().limit(0, 0),
            params_substitution=params,
        )

        if expects_resp3_shape(decoded_r):
            assert res["results"] == []
            assert res["total_results"] == 5
        else:
            assert res.results == []
            assert res.total_results == 5

        with pytest.raises(ResponseError):
            await decoded_r.ft().hybrid_search(
                query=hybrid_query,
                post_processing=HybridPostProcessingConfig().limit(5, 0),
                params_substitution=params,
            )

    @pytest.mark.redismod
    @skip_if_server_version_lt("8.3.224")
    async def test_hybrid_search_query_with_limit(self, decoded_r):
        # Create index and add data
        await self._create_hybrid_search_index(decoded_r)
        await self._add_data_for_hybrid_search(decoded_r, items_sets=10)

        # set search query
        search_query = HybridSearchQuery("@color:{red}")

        vsim_query = HybridVsimQuery(
            vector_field_name="@embedding",
            vector_data="$vec",
        )

        hybrid_query = HybridQuery(search_query, vsim_query)

        postprocessing_config = HybridPostProcessingConfig()
        postprocessing_config.limit(0, 3)

        res = await decoded_r.ft().hybrid_search(
            query=hybrid_query,
            post_processing=postprocessing_config,
            params_substitution={
                "vec": np.array([1, 2, 7, 6], dtype=np.float32).tobytes()
            },
            timeout=10,
        )

        if expects_resp2_shape(decoded_r) or expects_unified_shape(decoded_r):
            assert len(res.results) == 3
            assert res.warnings == []
        elif expects_resp3_shape(decoded_r):
            assert len(res["results"]) == 3
            assert res["warnings"] == []

    @pytest.mark.redismod
    @skip_if_server_version_lt("8.3.224")
    async def test_hybrid_search_query_with_load_apply_and_sortby(self, decoded_r):
        # Create index and add data
        await self._create_hybrid_search_index(decoded_r)
        await self._add_data_for_hybrid_search(decoded_r, items_sets=1)

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

        res = await decoded_r.ft().hybrid_search(
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
        if expects_resp2_shape(decoded_r) or expects_unified_shape(decoded_r):
            assert res.total_results >= 5
            assert len(res.results) == 5
            # the order here should match because of the sort
            assert res.results == expected_results
            assert res.warnings == []
            assert res.execution_time > 0
        elif expects_resp3_shape(decoded_r):
            assert res["total_results"] >= 5
            assert len(res["results"]) == 5
            # the order here should match because of the sort
            assert res["results"] == expected_results
            assert res["warnings"] == []
            assert res["execution_time"] > 0

    @pytest.mark.redismod
    @skip_if_server_version_lt("8.3.224")
    @pytest.mark.timeout(60)
    async def test_hybrid_search_query_with_timeout(self, decoded_r):
        await self._create_hybrid_search_timeout_index(decoded_r)
        await self._add_data_for_hybrid_search_timeout(decoded_r)

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

        res = await decoded_r.ft().hybrid_search(
            query=hybrid_query,
            params_substitution={"vec": query_vector},
            timeout=1,
        )

        if expects_resp2_shape(decoded_r) or expects_unified_shape(decoded_r):
            warnings = res.warnings
            assert res.execution_time > 0
        elif expects_resp3_shape(decoded_r):
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

        assert all_match, f"Not all warnings are matching expected values: {warnings}"

    @pytest.mark.redismod
    @skip_if_server_version_lt("8.3.224")
    async def test_hybrid_search_query_with_load_and_groupby(self, decoded_r):
        # Create index and add data
        await self._create_hybrid_search_index(decoded_r)
        await self._add_data_for_hybrid_search(decoded_r, items_sets=10)

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
            ["@price"],
            reducers.count_distinct("@color").alias("colors_count"),
        )

        postprocessing_config.sort_by(SortbyField("@price", asc=True))

        res = await decoded_r.ft().hybrid_search(
            query=hybrid_query,
            post_processing=postprocessing_config,
            params_substitution={
                "vec": np.array([1, 2, 7, 6], dtype=np.float32).tobytes()
            },
            timeout=10,
        )

        expected_results = [
            {"price": b"15", "colors_count": b"2"},
            {"price": b"16", "colors_count": b"2"},
            {"price": b"17", "colors_count": b"2"},
            {"price": b"18", "colors_count": b"2"},
        ]

        if expects_resp2_shape(decoded_r) or expects_unified_shape(decoded_r):
            assert len(res.results) == 4
            assert res.results == expected_results
            assert res.warnings == []
        elif expects_resp3_shape(decoded_r):
            assert len(res["results"]) == 4
            assert res["results"] == expected_results
            assert res["warnings"] == []

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

        res = await decoded_r.ft().hybrid_search(
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
        if expects_resp2_shape(decoded_r) or expects_unified_shape(decoded_r):
            assert len(res.results) == 6
            assert res.results == expected_results
            assert res.warnings == []
        elif expects_resp3_shape(decoded_r):
            assert len(res["results"]) == 6
            assert res["results"] == expected_results
            assert res["warnings"] == []

    @pytest.mark.redismod
    @skip_if_server_version_lt("8.4")
    @skip_if_server_version_gte("8.11")
    async def test_hybrid_search_query_with_cursor(self, decoded_r):
        # Create index and add data
        await self._create_hybrid_search_index(decoded_r)
        await self._add_data_for_hybrid_search(decoded_r, items_sets=10)

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
        res = await decoded_r.ft().hybrid_search(
            query=hybrid_query,
            cursor=HybridCursorQuery(count=5, max_idle=10000),
            params_substitution={
                "vec": np.array([1, 2, 7, 6], dtype=np.float32).tobytes()
            },
            timeout=10,
        )
        if expects_resp2_shape(decoded_r) or expects_unified_shape(decoded_r):
            assert isinstance(res, HybridCursorResult)
            assert res.search_cursor_id > 0
            assert res.vsim_cursor_id > 0
            search_cursor = aggregations.Cursor(res.search_cursor_id)
            vsim_cursor = aggregations.Cursor(res.vsim_cursor_id)
        elif expects_resp3_shape(decoded_r):
            assert res["SEARCH"] > 0
            assert res["VSIM"] > 0
            search_cursor = aggregations.Cursor(res["SEARCH"])
            vsim_cursor = aggregations.Cursor(res["VSIM"])

        search_res_from_cursor = await decoded_r.ft().aggregate(query=search_cursor)
        if expects_resp2_shape(decoded_r) or expects_unified_shape(decoded_r):
            assert len(search_res_from_cursor.rows) == 5
        elif expects_resp3_shape(decoded_r):
            assert len(search_res_from_cursor[0]["results"]) == 5

        vsim_res_from_cursor = await decoded_r.ft().aggregate(query=vsim_cursor)
        if expects_resp2_shape(decoded_r) or expects_unified_shape(decoded_r):
            assert len(vsim_res_from_cursor.rows) == 5
        elif expects_resp3_shape(decoded_r):
            assert len(vsim_res_from_cursor[0]["results"]) == 5


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


async def _queue_parsed_commands(ft):
    """Queue the search commands whose replies are parsed by the client."""
    queued = [
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
    # ``profile`` and ``syndump`` are inherited from the sync commands.
    return [await res if asyncio.iscoroutine(res) else res for res in queued]
