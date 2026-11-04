"""Unit tests for search aggregation request building.

These tests exercise the pure-Python argument building logic and do not
require a running Redis server, so they live in the ``fixed_client`` test
group.
"""

import pytest

from redis.commands.search import reducers
from redis.commands.search.aggregation import FIELDNAME, AggregateRequest
from redis.commands.search.hybrid_query import HybridPostProcessingConfig


@pytest.mark.fixed_client
class TestRequestLimit:
    @pytest.mark.parametrize(
        "request_type", [AggregateRequest, HybridPostProcessingConfig]
    )
    @pytest.mark.parametrize("offset, count", [(0, 0), (0, 1), (0, 10), (3, 10)])
    def test_explicit_limit(self, request_type, offset, count):
        request = request_type().limit(offset, count)

        assert request.build_args()[-3:] == ["LIMIT", str(offset), str(count)]

    @pytest.mark.parametrize(
        "request_type", [AggregateRequest, HybridPostProcessingConfig]
    )
    def test_no_limit(self, request_type):
        assert "LIMIT" not in request_type().build_args()

    @pytest.mark.parametrize("limit_first", [False, True])
    def test_zero_limit_preserves_aggregation_stage_order(self, limit_first):
        request = AggregateRequest("*")
        if limit_first:
            request.limit(0, 0)
        request.group_by("@id", reducers.count())
        if not limit_first:
            request.limit(0, 0)

        group_args = ["GROUPBY", "1", "@id", "REDUCE", "COUNT", "0"]
        limit_args = ["LIMIT", "0", "0"]
        expected = limit_args + group_args if limit_first else group_args + limit_args
        assert request.build_args()[-len(expected) :] == expected


@pytest.mark.fixed_client
class TestReducerAlias:
    def test_fieldname_alias_with_at_prefix(self):
        reducer = reducers.sum("@paid").alias(FIELDNAME)
        assert reducer._alias == "paid"

    def test_fieldname_alias_without_at_prefix(self):
        # The '@' prefix is optional, so the name must be used as-is rather
        # than having its first character removed.
        reducer = reducers.sum("paid").alias(FIELDNAME)
        assert reducer._alias == "paid"

    def test_fieldname_alias_without_at_prefix_in_args(self):
        request = AggregateRequest("*").group_by(
            "@id", reducers.sum("paid").alias(FIELDNAME)
        )
        assert request.build_args()[-3:] == ["paid", "AS", "paid"]

    def test_fieldname_alias_without_field(self):
        with pytest.raises(ValueError, match="Cannot use FIELDNAME alias"):
            reducers.count().alias(FIELDNAME)
