"""Unit tests for the search query string builders.

These tests exercise the pure-Python query building logic and do not require a
running Redis server, so they live in the ``fixed_client`` test group.
"""

import pytest

from redis.commands.search.querystring import (
    GeoValue,
    OptionalNode,
    disjunct,
    disjunct_union,
    geo,
    intersect,
    tags,
    union,
)


@pytest.mark.fixed_client
class TestGeoValue:
    """``geo()`` must render longitude before latitude.

    RediSearch expects geo filters as ``[lon lat radius unit]``, which is also
    the order used by ``GeoValue`` and ``query.GeoFilter``.
    """

    def test_geo_renders_longitude_before_latitude(self):
        assert geo(lat=51.45, lon=-0.44, radius=10).to_string() == "[-0.44 51.45 10 km]"

    def test_geo_matches_geovalue_built_directly(self):
        assert (
            geo(lat=51.45, lon=-0.44, radius=10).to_string()
            == GeoValue(-0.44, 51.45, 10).to_string()
        )

    def test_geo_keeps_the_requested_unit(self):
        assert (
            geo(lat=51.45, lon=-0.44, radius=10, unit="mi").to_string()
            == "[-0.44 51.45 10 mi]"
        )

    def test_geo_in_a_query(self):
        query = intersect(location=geo(lat=51.45, lon=-0.44, radius=10))
        assert query.to_string() == "@location:[-0.44 51.45 10 km]"


@pytest.mark.fixed_client
class TestNegatedAndOptionalNodes:
    """``-`` and ``~`` must apply to the whole group of children.

    RediSearch binds the prefix operator to the expression right after it, so
    ``(-foo|bar)`` means "not foo, or bar" instead of "neither foo nor bar".
    """

    def test_disjunct_union_negates_all_children(self):
        assert disjunct_union("foo", "bar").to_string() == "-(foo|bar)"

    def test_disjunct_union_matches_disjunct_of_union(self):
        assert (
            disjunct_union("foo", "bar").to_string()
            == disjunct(union("foo", "bar")).to_string()
        )

    def test_disjunct_negates_the_whole_intersection(self):
        assert disjunct("foo", "bar").to_string() == "-(foo bar)"

    def test_disjunct_multiple_fields(self):
        assert (
            disjunct_union(country="il", city="tlv").to_string()
            == "-(@country:il|@city:tlv)"
        )

    def test_disjunct_single_child(self):
        assert disjunct("foo").to_string() == "-foo"
        assert disjunct_union(country=("il", "jp")).to_string() == "-@country:(il|jp)"

    def test_disjunct_nested_in_intersect(self):
        query = intersect("baz", disjunct_union("foo", "bar"))
        assert query.to_string() == "(baz -(foo|bar))"

    def test_optional_applies_to_all_children(self):
        assert OptionalNode("foo", "bar").to_string() == "~(foo bar)"
        assert OptionalNode("foo").to_string() == "~foo"

    def test_optional_nested_in_intersect(self):
        query = intersect("baz", OptionalNode("foo", "bar"))
        assert query.to_string() == "(baz ~(foo bar))"


@pytest.mark.fixed_client
class TestTagValue:
    """``tags()`` values must be matched literally.

    RediSearch reads ``|``, ``}``, ``*`` and other punctuation inside a
    ``{...}`` tag set as query syntax, so an unescaped value could add tags or
    clauses to the query it is part of.
    """

    def test_tags_are_joined_as_a_union(self):
        assert tags("il", "jp").to_string() == "{il | jp}"

    def test_plain_tags_are_unchanged(self):
        assert tags("hello_world1").to_string() == "{hello_world1}"
        assert tags(12).to_string() == "{12}"
        assert tags("café").to_string() == "{café}"

    def test_tag_separator_in_a_value_is_escaped(self):
        assert tags("alice | bob").to_string() == r"{alice\ \|\ bob}"

    def test_tag_set_breakout_is_escaped(self):
        query = intersect(owner=tags("alice} | @owner:{bob"))
        assert query.to_string() == r"@owner:{alice\}\ \|\ \@owner\:\{bob}"

    def test_wildcard_in_a_value_is_escaped(self):
        assert tags("al*").to_string() == r"{al\*}"

    def test_backslash_in_a_value_is_escaped(self):
        assert tags("a\\b").to_string() == r"{a\\b}"

    def test_punctuation_in_a_value_is_escaped(self):
        assert (
            tags("hello world", "foo-bar.baz").to_string()
            == r"{hello\ world | foo\-bar\.baz}"
        )
