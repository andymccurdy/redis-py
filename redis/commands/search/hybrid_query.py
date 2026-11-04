from enum import Enum
from typing import Any, Dict, List, Optional, Self, Union

from redis.commands.search.aggregation import Limit, Reducer
from redis.commands.search.query import Filter, SortbyField
from redis.utils import experimental


@experimental
class HybridSearchQuery:
    def __init__(
        self,
        query_string: str,
        scorer: Optional[str] = None,
        yield_score_as: Optional[str] = None,
    ) -> None:
        """
        Create a new hybrid search query object.

        Args:
            query_string: The query string.
            scorer: Scoring algorithm for text search query.
                Allowed values are "TFIDF", "TFIDF.DOCNORM", "DISMAX", "DOCSCORE",
                "BM25", "BM25STD", "BM25STD.TANH", "HAMMING", etc.
                For more information about supported scoring algorithms, see
                https://redis.io/docs/latest/develop/ai/search-and-query/advanced-concepts/scoring/
            yield_score_as: The name of the field to yield the score as.
        """
        self._query_string = query_string
        self._scorer = scorer
        self._yield_score_as = yield_score_as

    def query_string(self) -> str:
        """Return the query string of this query object."""
        return self._query_string

    def scorer(self, scorer: str) -> "HybridSearchQuery":
        """
        Scoring algorithm for text search query.
        Allowed values are "TFIDF", "TFIDF.DOCNORM", "DISMAX", "DOCSCORE", "BM25",
        "BM25STD", "BM25STD.TANH", "HAMMING", etc.

        For more information about supported scoring algorithms,
        see https://redis.io/docs/latest/develop/ai/search-and-query/advanced-concepts/scoring/
        """
        self._scorer = scorer
        return self

    def yield_score_as(self, alias: str) -> "HybridSearchQuery":
        """
        Yield the score as a field.
        """
        self._yield_score_as = alias
        return self

    def get_args(self) -> List[str]:
        args = ["SEARCH", self._query_string]
        if self._scorer:
            args.extend(("SCORER", self._scorer))
        if self._yield_score_as:
            args.extend(("YIELD_SCORE_AS", self._yield_score_as))
        return args


class VectorSearchMethods(Enum):
    KNN = "KNN"
    RANGE = "RANGE"


@experimental
class HybridVsimQuery:
    def __init__(
        self,
        vector_field_name: str,
        vector_data: Union[bytes, str],
        vsim_search_method: Optional[VectorSearchMethods] = None,
        vsim_search_method_params: Optional[Dict[str, Any]] = None,
        filter: Optional["Filter"] = None,
        yield_score_as: Optional[str] = None,
    ) -> None:
        """
        Create a new hybrid vsim query object.

        Args:
            vector_field_name: Vector field name.

            vector_data: Vector data for the search.

            vsim_search_method: Search method that will be used for the vsim search.

            vsim_search_method_params: Search method parameters. Use the param names
                for keys and the values for the values.
                Example for KNN: {"K": 10, "EF_RUNTIME": 100}
                                    where K is mandatory and defines the number of results
                                    and EF_RUNTIME is optional and definesthe exploration factor.
                Example for RANGE: {"RADIUS": 10, "EPSILON": 0.1}
                                    where RADIUS is mandatory and defines the radius of the search
                                    and EPSILON is optional and defines the accuracy of the search.
            yield_score_as: The name of the field to yield the score as.

            filter: If defined, a filter will be applied on the vsim query results.
        """
        self._vector_field = vector_field_name
        self._vector_data = vector_data
        if vsim_search_method and vsim_search_method_params:
            self.vsim_method_params(vsim_search_method, **vsim_search_method_params)
        else:
            self._vsim_method_params = None
        self._filter = filter
        self._yield_score_as = yield_score_as

    def vector_field(self) -> str:
        """Return the vector field name of this query object."""
        return self._vector_field

    def vector_data(self) -> Union[bytes, str]:
        """Return the vector data of this query object."""
        return self._vector_data

    def vsim_method_params(
        self,
        method: VectorSearchMethods,
        **kwargs,
    ) -> "HybridVsimQuery":
        """
        Add search method parameters to the query.

        Args:
            method: Vector search method name. Supported values are "KNN" or "RANGE".
            kwargs: Search method parameters. Use the param names for keys and the
                values for the values. Example: {"K": 10, "EF_RUNTIME": 100}.
        """
        vsim_method_params: List[Union[str, int]] = [method.value]
        if kwargs:
            vsim_method_params.append(len(kwargs.items()) * 2)
            for key, value in kwargs.items():
                vsim_method_params.extend((key, value))
        self._vsim_method_params = vsim_method_params

        return self

    def filter(self, flt: "HybridFilter") -> "HybridVsimQuery":
        """
        Add a filter to the query.

        Args:
            flt: A HybridFilter object, used on a corresponding field.
        """
        self._filter = flt
        return self

    def yield_score_as(self, alias: str) -> "HybridVsimQuery":
        """
        Return the score as a field with name `alias`.
        """
        self._yield_score_as = alias
        return self

    def get_args(self) -> List[str]:
        args = ["VSIM", self._vector_field, self._vector_data]
        if self._vsim_method_params:
            args.extend(self._vsim_method_params)
        if self._filter:
            args.extend(self._filter.args)
        if self._yield_score_as:
            args.extend(("YIELD_SCORE_AS", self._yield_score_as))

        return args


class HybridQuery:
    def __init__(
        self,
        search_query: HybridSearchQuery,
        vector_similarity_query: HybridVsimQuery,
    ) -> None:
        """
        Create a new hybrid query object.

        Args:
            search_query: HybridSearchQuery object containing the text query.
            vector_similarity_query: HybridVsimQuery object containing the vector similarity query.
        """
        self._search_query = search_query
        self._vector_similarity_query = vector_similarity_query

    def get_args(self) -> List[str]:
        args = []
        args.extend(self._search_query.get_args())
        args.extend(self._vector_similarity_query.get_args())
        return args


class CombinationMethods(Enum):
    RRF = "RRF"
    LINEAR = "LINEAR"


@experimental
class CombineResultsMethod:
    def __init__(self, method: CombinationMethods, **kwargs) -> None:
        """
        Create a new combine results method object.

        Args:
            method: The combine method to use - RRF or LINEAR.
            kwargs: Additional combine parameters.
                    For RRF, the following parameters are supported(at least one should be provided):
                                WINDOW: Limits fusion scopeLimits fusion scope.
                                CONSTANT: Controls decay of rank influence.
                                YIELD_SCORE_AS: The name of the field to yield the calculated score as.
                    For LINEAR, supported parameters (at least one should be provided):
                                ALPHA: The weight of the first query.
                                BETA: The weight of the second query.
                                YIELD_SCORE_AS: The name of the field to yield the calculated score as.

                    The additional parameters are not validated and are passed as is to the server.
                    The supported format is to provide the parameter names and values like the following:
                        CombineResultsMethod(CombinationMethods.RRF, WINDOW=3, CONSTANT=0.5)
                        CombineResultsMethod(CombinationMethods.LINEAR, ALPHA=0.5, BETA=0.5)
        """
        self._method = method
        self._kwargs = kwargs

    def get_args(self) -> List[Union[str, int]]:
        args: List[Union[str, int]] = ["COMBINE", self._method.value]
        if self._kwargs:
            args.append(len(self._kwargs.items()) * 2)
            for key, value in self._kwargs.items():
                args.extend((key, value))
        return args


@experimental
class HybridPostProcessingConfig:
    def __init__(self) -> None:
        """
        Create a new hybrid post processing configuration object.
        """
        self._load_statements = []
        self._return_fields_decode_as: Dict[str, Optional[str]] = {}
        self._apply_statements = []
        self._groupby_statements = []
        self._sortby_fields = []
        self._filter = None
        self._limit = None

    def load(
        self,
        *fields: str,
        decode_field: Optional[bool] = False,
        encoding: Optional[str] = "utf8",
    ) -> Self:
        """
        Add load statement parameters to the query.

        Args:
            fields: Fields to load.
            decode_field: Whether to decode loaded field values from bytes to strings.
                Defaults to False to preserve the legacy RESP2 HYBRID behavior and
                keep binary fields intact.
            encoding: The encoding to use when decoding loaded field values.
        """
        if fields:
            fields_str = " ".join(fields)
            fields_list = fields_str.split(" ")
            self._load_statements.extend(("LOAD", len(fields_list), *fields_list))
            self._set_load_field_encodings(
                fields_list, encoding if decode_field else None
            )
        return self

    def _set_load_field_encodings(
        self, fields: List[str], encoding: Optional[str]
    ) -> None:
        i = 0
        while i < len(fields):
            field = fields[i]
            if field.upper() == "AS":
                i += 2
                continue
            if i + 2 < len(fields) and fields[i + 1].upper() == "AS":
                self._return_fields_decode_as[fields[i + 2]] = encoding
                i += 3
                continue
            self._return_fields_decode_as[field.removeprefix("@")] = encoding
            i += 1

    def group_by(self, fields: List[str], *reducers: Reducer) -> Self:
        """
        Specify by which fields to group the aggregation.

        Args:
            fields: Fields to group by. This can either be a single string or a list
                of strings. In both cases, the field should be specified as `@field`.
            reducers: One or more reducers. Reducers may be found in the
                `aggregation` module.
        """

        fields = [fields] if isinstance(fields, str) else fields

        ret = ["GROUPBY", str(len(fields)), *fields]
        for reducer in reducers:
            ret.extend(("REDUCE", reducer.NAME, str(len(reducer.args))))
            ret.extend(reducer.args)
            if reducer._alias is not None:
                ret.extend(("AS", reducer._alias))

        self._groupby_statements.extend(ret)
        return self

    def apply(self, **kwexpr) -> Self:
        """
        Specify one or more projection expressions to add to each result.

        Args:
            kwexpr: One or more key-value pairs for a projection. The key is
                the alias for the projection, and the value is the projection
                expression itself, for example `apply(square_root="sqrt(@foo)")`.
        """
        apply_args = []
        for alias, expr in kwexpr.items():
            ret = ["APPLY", expr]
            if alias is not None:
                ret.extend(("AS", alias))
            apply_args.extend(ret)

        self._apply_statements.extend(apply_args)

        return self

    def sort_by(self, *sortby: "SortbyField") -> Self:
        """
        Add sortby parameters to the query.
        """
        self._sortby_fields = [*sortby]
        return self

    def filter(self, filter: "HybridFilter") -> Self:
        """
        Add a numeric or string filter to the query.

        Currently, only one of each filter is supported by the engine.

        Args:
            filter: A NumericFilter or GeoFilter object, used on a corresponding field.
        """
        self._filter = filter
        return self

    def limit(self, offset: int, num: int) -> Self:
        """
        Add limit parameters to the query.

        If `num` is 0, `offset` must also be 0; `LIMIT 0 0` omits result
        rows. Do not use it as a counting API. To count search matches, use
        `Query(...).paging(0, 0)` with `FT.SEARCH`. To count inside an
        aggregation, use `group_by([], reducers.count())`.
        """
        self._limit = Limit(offset, num)
        return self

    def build_args(self) -> List[str]:
        args = []
        if self._load_statements:
            args.extend(self._load_statements)
        if self._groupby_statements:
            args.extend(self._groupby_statements)
        if self._apply_statements:
            args.extend(self._apply_statements)
        if self._sortby_fields:
            sortby_args = []
            for f in self._sortby_fields:
                sortby_args.extend(f.args)
            args.extend(("SORTBY", len(sortby_args), *sortby_args))
        if self._filter:
            args.extend(self._filter.args)
        if self._limit:
            args.extend(self._limit.build_args())

        return args


@experimental
class HybridFilter(Filter):
    def __init__(
        self,
        conditions: str,
    ) -> None:
        """
        Create a new hybrid filter object.

        Args:
            conditions: Filter conditions.
        """
        args = [conditions]
        Filter.__init__(self, "FILTER", *args)


@experimental
class HybridCursorQuery:
    def __init__(self, count: int = 0, max_idle: int = 0) -> None:
        """
        Create a new hybrid cursor query object.

        ``WITHCURSOR`` for ``FT.HYBRID`` is exposed only by Redis versions
        8.4 to 8.10. Newer servers reject it as an unknown argument.

        Args:
            count: Number of results to return per cursor iteration.
            max_idle: Maximum idle time for the cursor.
        """
        self.count = count
        self.max_idle = max_idle

    def build_args(self):
        args = ["WITHCURSOR"]
        if self.count:
            args += ["COUNT", str(self.count)]
        if self.max_idle:
            args += ["MAXIDLE", str(self.max_idle)]
        return args
