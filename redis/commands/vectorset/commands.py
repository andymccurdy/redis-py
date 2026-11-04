from __future__ import annotations

import json
from enum import Enum
from typing import Any, Awaitable, overload

from redis.client import NEVER_DECODE
from redis.commands.helpers import get_protocol_version
from redis.exceptions import DataError
from redis.typing import (
    AsyncClientProtocol,
    CommandsProtocol,
    EncodableT,
    KeyT,
    Number,
    SyncClientProtocol,
)
from redis.utils import check_protocol_version

VADD_CMD = "VADD"
VSIM_CMD = "VSIM"
VREM_CMD = "VREM"
VDIM_CMD = "VDIM"
VCARD_CMD = "VCARD"
VEMB_CMD = "VEMB"
VLINKS_CMD = "VLINKS"
VINFO_CMD = "VINFO"
VSETATTR_CMD = "VSETATTR"
VGETATTR_CMD = "VGETATTR"
VRANDMEMBER_CMD = "VRANDMEMBER"
VRANGE_CMD = "VRANGE"

# Return type for vsim command
VSimResult = (
    list[list[EncodableT] | dict[EncodableT, Number] | dict[EncodableT, dict[str, Any]]]
    | None
)

# Return type for vemb command
VEmbResult = list[EncodableT] | dict[str, EncodableT] | None

# Return type for vlinks command
VLinksResult = list[list[str | bytes] | dict[str | bytes, Number]] | None

# Return type for vrandmember command
VRandMemberResult = list[str] | str | None

# Return type for vgetattr command
VGetAttrResult = dict | None


class QuantizationOptions(Enum):
    """Quantization options for the VADD command."""

    NOQUANT = "NOQUANT"
    BIN = "BIN"
    Q8 = "Q8"


class CallbacksOptions(Enum):
    """Options that can be set for the commands callbacks"""

    RAW = "RAW"
    WITHSCORES = "WITHSCORES"
    WITHATTRIBS = "WITHATTRIBS"
    ALLOW_DECODING = "ALLOW_DECODING"
    RESP3 = "RESP3"


class VectorSetCommands(CommandsProtocol):
    """Redis VectorSet commands"""

    @overload
    def vadd(
        self: SyncClientProtocol,
        key: KeyT,
        vector: list[float] | bytes,
        element: str,
        reduce_dim: int | None = None,
        cas: bool | None = False,
        quantization: QuantizationOptions | None = None,
        ef: Number | None = None,
        attributes: dict | str | None = None,
        numlinks: int | None = None,
    ) -> int: ...

    @overload
    def vadd(
        self: AsyncClientProtocol,
        key: KeyT,
        vector: list[float] | bytes,
        element: str,
        reduce_dim: int | None = None,
        cas: bool | None = False,
        quantization: QuantizationOptions | None = None,
        ef: Number | None = None,
        attributes: dict | str | None = None,
        numlinks: int | None = None,
    ) -> Awaitable[int]: ...

    def vadd(
        self,
        key: KeyT,
        vector: list[float] | bytes,
        element: str,
        reduce_dim: int | None = None,
        cas: bool | None = False,
        quantization: QuantizationOptions | None = None,
        ef: Number | None = None,
        attributes: dict | str | None = None,
        numlinks: int | None = None,
    ) -> Awaitable[int] | int:
        """
        Add vector ``vector`` for element ``element`` to a vector set ``key``.

        ``reduce_dim`` sets the dimensions to reduce the vector to.
                If not provided, the vector is not reduced.

        ``cas`` is a boolean flag that indicates whether to use CAS (check-and-set style)
                when adding the vector. If not provided, CAS is not used.

        ``quantization`` sets the quantization type to use.
                If not provided, int8 quantization is used.
                The options are:
                - NOQUANT: No quantization
                - BIN: Binary quantization
                - Q8: Signed 8-bit quantization

        ``ef`` sets the exploration factor to use.
                If not provided, the default exploration factor is used.

        ``attributes`` is a dictionary or json string that contains the attributes to set for the vector.
                If not provided, no attributes are set.
                An empty dictionary replaces existing attributes with an empty object;
                an empty string removes them. None leaves existing attributes unchanged.

        ``numlinks`` sets the number of links to create for the vector.
                If not provided, the default number of links is used.

        For more information, see https://redis.io/commands/vadd.
        """
        if not vector or not element:
            raise DataError("Both vector and element must be provided")

        pieces = []
        if reduce_dim:
            pieces.extend(["REDUCE", reduce_dim])

        values_pieces = []
        if isinstance(vector, bytes):
            values_pieces.extend(["FP32", vector])
        else:
            values_pieces.extend(["VALUES", len(vector)])
            values_pieces.extend(vector)
        pieces.extend(values_pieces)

        pieces.append(element)

        if cas:
            pieces.append("CAS")

        if quantization:
            pieces.append(quantization.value)

        if ef:
            pieces.extend(["EF", ef])

        if attributes is not None:
            if isinstance(attributes, dict):
                # transform attributes to json string
                attributes_json = json.dumps(attributes)
            else:
                attributes_json = attributes
            pieces.extend(["SETATTR", attributes_json])

        if numlinks:
            pieces.extend(["M", numlinks])

        return self.execute_command(VADD_CMD, key, *pieces)

    @overload
    def vsim(
        self: SyncClientProtocol,
        key: KeyT,
        input: list[float] | bytes | str,
        with_scores: bool | None = False,
        with_attribs: bool | None = False,
        count: int | None = None,
        ef: Number | None = None,
        filter: str | None = None,
        filter_ef: str | None = None,
        truth: bool | None = False,
        no_thread: bool | None = False,
        epsilon: Number | None = None,
    ) -> VSimResult: ...

    @overload
    def vsim(
        self: AsyncClientProtocol,
        key: KeyT,
        input: list[float] | bytes | str,
        with_scores: bool | None = False,
        with_attribs: bool | None = False,
        count: int | None = None,
        ef: Number | None = None,
        filter: str | None = None,
        filter_ef: str | None = None,
        truth: bool | None = False,
        no_thread: bool | None = False,
        epsilon: Number | None = None,
    ) -> Awaitable[VSimResult]: ...

    def vsim(
        self,
        key: KeyT,
        input: list[float] | bytes | str,
        with_scores: bool | None = False,
        with_attribs: bool | None = False,
        count: int | None = None,
        ef: Number | None = None,
        filter: str | None = None,
        filter_ef: str | None = None,
        truth: bool | None = False,
        no_thread: bool | None = False,
        epsilon: Number | None = None,
    ) -> Awaitable[VSimResult] | VSimResult:
        """
        Compare a vector or element ``input``  with the other vectors in a vector set ``key``.

        ``with_scores`` sets if similarity scores should be returned for each element in the result.

        ``with_attribs`` ``with_attribs`` sets if the results should be returned with the
                attributes of the elements in the result, or None when no attributes are present.

        ``count`` sets the number of results to return.

        ``ef`` sets the exploration factor.

        ``filter`` sets the filter that should be applied for the search.

        ``filter_ef`` sets the max filtering effort.

        ``truth`` when enabled, forces the command to perform a linear scan.

        ``no_thread`` when enabled forces the command to execute the search
                on the data structure in the main thread.

        ``epsilon`` floating point between 0 and 1, if specified will return
                only elements with distance no further than the specified one.

        For more information, see https://redis.io/commands/vsim.
        """

        if not input:
            raise DataError("'input' should be provided")

        pieces = []
        options = {}

        if isinstance(input, bytes):
            pieces.extend(["FP32", input])
        elif isinstance(input, list):
            pieces.extend(["VALUES", len(input)])
            pieces.extend(input)
        else:
            pieces.extend(["ELE", input])

        if with_scores or with_attribs:
            if check_protocol_version(get_protocol_version(self.client), 3):
                options[CallbacksOptions.RESP3.value] = True

            if with_scores:
                pieces.append("WITHSCORES")
                options[CallbacksOptions.WITHSCORES.value] = True

            if with_attribs:
                pieces.append("WITHATTRIBS")
                options[CallbacksOptions.WITHATTRIBS.value] = True

        if count:
            pieces.extend(["COUNT", count])

        if epsilon:
            pieces.extend(["EPSILON", epsilon])

        if ef:
            pieces.extend(["EF", ef])

        if filter:
            pieces.extend(["FILTER", filter])

        if filter_ef:
            pieces.extend(["FILTER-EF", filter_ef])

        if truth:
            pieces.append("TRUTH")

        if no_thread:
            pieces.append("NOTHREAD")

        return self.execute_command(VSIM_CMD, key, *pieces, **options)

    @overload
    def vdim(self: SyncClientProtocol, key: KeyT) -> int: ...

    @overload
    def vdim(self: AsyncClientProtocol, key: KeyT) -> Awaitable[int]: ...

    def vdim(self, key: KeyT) -> Awaitable[int] | int:
        """
        Get the dimension of a vector set.

        In the case of vectors that were populated using the `REDUCE`
        option, for random projection, the vector set will report the size of
        the projected (reduced) dimension.

        Raises `redis.exceptions.ResponseError` if the vector set doesn't exist.

        For more information, see https://redis.io/commands/vdim.
        """
        return self.execute_command(VDIM_CMD, key)

    @overload
    def vcard(self: SyncClientProtocol, key: KeyT) -> int: ...

    @overload
    def vcard(self: AsyncClientProtocol, key: KeyT) -> Awaitable[int]: ...

    def vcard(self, key: KeyT) -> Awaitable[int] | int:
        """
        Get the cardinality(the number of elements) of a vector set with key ``key``.

        Raises `redis.exceptions.ResponseError` if the vector set doesn't exist.

        For more information, see https://redis.io/commands/vcard.
        """
        return self.execute_command(VCARD_CMD, key)

    @overload
    def vrem(self: SyncClientProtocol, key: KeyT, element: str) -> int: ...

    @overload
    def vrem(self: AsyncClientProtocol, key: KeyT, element: str) -> Awaitable[int]: ...

    def vrem(self, key: KeyT, element: str) -> Awaitable[int] | int:
        """
        Remove an element from a vector set.

        For more information, see https://redis.io/commands/vrem.
        """
        return self.execute_command(VREM_CMD, key, element)

    @overload
    def vemb(
        self: SyncClientProtocol,
        key: KeyT,
        element: str,
        raw: bool | None = False,
    ) -> VEmbResult: ...

    @overload
    def vemb(
        self: AsyncClientProtocol,
        key: KeyT,
        element: str,
        raw: bool | None = False,
    ) -> Awaitable[VEmbResult]: ...

    def vemb(
        self, key: KeyT, element: str, raw: bool | None = False
    ) -> Awaitable[VEmbResult] | VEmbResult:
        """
        Get the approximated vector of an element ``element`` from vector set ``key``.

        ``raw`` is a boolean flag that indicates whether to return the
                internal representation used by the vector.


        For more information, see https://redis.io/commands/vemb.
        """
        options = {}
        pieces = []
        pieces.extend([key, element])

        if check_protocol_version(get_protocol_version(self.client), 3):
            options[CallbacksOptions.RESP3.value] = True

        if raw:
            pieces.append("RAW")

            options[NEVER_DECODE] = True
            if (
                hasattr(self.client, "connection_pool")
                and self.client.connection_pool.connection_kwargs.get(
                    "decode_responses", False
                )
            ) or (
                hasattr(self.client, "nodes_manager")
                and self.client.nodes_manager.connection_kwargs.get(
                    "decode_responses", False
                )
            ):
                # allow decoding in the postprocessing callback
                # if the user set decode_responses=True
                # in the connection pool
                options[CallbacksOptions.ALLOW_DECODING.value] = True

            options[CallbacksOptions.RAW.value] = True

        return self.execute_command(VEMB_CMD, *pieces, **options)

    @overload
    def vlinks(
        self: SyncClientProtocol,
        key: KeyT,
        element: str,
        with_scores: bool | None = False,
    ) -> VLinksResult: ...

    @overload
    def vlinks(
        self: AsyncClientProtocol,
        key: KeyT,
        element: str,
        with_scores: bool | None = False,
    ) -> Awaitable[VLinksResult]: ...

    def vlinks(
        self, key: KeyT, element: str, with_scores: bool | None = False
    ) -> Awaitable[VLinksResult] | VLinksResult:
        """
        Returns the neighbors for each level the element ``element`` exists in the vector set ``key``.

        The result is a list of lists, where each list contains the neighbors for one level.
        If the element does not exist, or if the vector set does not exist, None is returned.

        If the ``WITHSCORES`` option is provided, the result is a list of dicts,
        where each dict contains the neighbors for one level, with the scores as values.

        For more information, see https://redis.io/commands/vlinks
        """
        options = {}
        pieces = []
        pieces.extend([key, element])

        if with_scores:
            pieces.append("WITHSCORES")
            options[CallbacksOptions.WITHSCORES.value] = True

        return self.execute_command(VLINKS_CMD, *pieces, **options)

    @overload
    def vinfo(self: SyncClientProtocol, key: KeyT) -> dict | None: ...

    @overload
    def vinfo(self: AsyncClientProtocol, key: KeyT) -> Awaitable[dict | None]: ...

    def vinfo(self, key: KeyT) -> (dict | None) | Awaitable[dict | None]:
        """
        Get information about a vector set.

        For more information, see https://redis.io/commands/vinfo.
        """
        return self.execute_command(VINFO_CMD, key)

    @overload
    def vsetattr(
        self: SyncClientProtocol,
        key: KeyT,
        element: str,
        attributes: dict | str | None = None,
    ) -> int: ...

    @overload
    def vsetattr(
        self: AsyncClientProtocol,
        key: KeyT,
        element: str,
        attributes: dict | str | None = None,
    ) -> Awaitable[int]: ...

    def vsetattr(
        self, key: KeyT, element: str, attributes: dict | str | None = None
    ) -> Awaitable[int] | int:
        """
        Associate or remove JSON attributes ``attributes`` of element ``element``
        for vector set ``key``.

        For more information, see https://redis.io/commands/vsetattr
        """
        if attributes is None:
            attributes_json = "{}"
        elif isinstance(attributes, dict):
            # transform attributes to json string
            attributes_json = json.dumps(attributes)
        else:
            attributes_json = attributes

        return self.execute_command(VSETATTR_CMD, key, element, attributes_json)

    @overload
    def vgetattr(
        self: SyncClientProtocol, key: KeyT, element: str
    ) -> VGetAttrResult: ...

    @overload
    def vgetattr(
        self: AsyncClientProtocol, key: KeyT, element: str
    ) -> Awaitable[VGetAttrResult]: ...

    def vgetattr(
        self, key: KeyT, element: str
    ) -> Awaitable[VGetAttrResult] | VGetAttrResult:
        """
        Retrieve the JSON attributes of an element ``element `` for vector set ``key``.

        If the element does not exist, or if the vector set does not exist, None is
        returned.

        For more information, see https://redis.io/commands/vgetattr.
        """
        return self.execute_command(VGETATTR_CMD, key, element)

    @overload
    def vrandmember(
        self: SyncClientProtocol, key: KeyT, count: int | None = None
    ) -> VRandMemberResult: ...

    @overload
    def vrandmember(
        self: AsyncClientProtocol, key: KeyT, count: int | None = None
    ) -> Awaitable[VRandMemberResult]: ...

    def vrandmember(
        self, key: KeyT, count: int | None = None
    ) -> Awaitable[VRandMemberResult] | VRandMemberResult:
        """
        Returns random elements from a vector set ``key``.

        ``count`` is the number of elements to return.
                If ``count`` is not provided, a single element is returned as a single string.
                If ``count`` is positive(smaller than the number of elements
                            in the vector set), the command returns a list with up to ``count``
                            distinct elements from the vector set
                If ``count`` is negative, the command returns a list with ``count`` random elements,
                            potentially with duplicates.
                If ``count`` is greater than the number of elements in the vector set,
                            only the entire set is returned as a list.

        If the vector set does not exist, ``None`` is returned.

        For more information, see https://redis.io/commands/vrandmember.
        """
        pieces = []
        pieces.append(key)
        if count is not None:
            pieces.append(count)
        return self.execute_command(VRANDMEMBER_CMD, *pieces)

    @overload
    def vrange(
        self: SyncClientProtocol,
        key: KeyT,
        start: str,
        end: str,
        count: int | None = None,
    ) -> list[str]: ...

    @overload
    def vrange(
        self: AsyncClientProtocol,
        key: KeyT,
        start: str,
        end: str,
        count: int | None = None,
    ) -> Awaitable[list[str]]: ...

    def vrange(
        self, key: KeyT, start: str, end: str, count: int | None = None
    ) -> Awaitable[list[str]] | list[str]:
        """
        Return elements in a lexicographical range from a vector set ``key``.

        ``start`` is the starting point of the lexicographical range. Can be:
                - A string prefixed with '[' for inclusive range (e.g., '[Redis')
                - A string prefixed with '(' for exclusive range (e.g., '(a7')
                - The special symbol '-' to indicate the minimum element

        ``end`` is the ending point of the lexicographical range. Can be:
                - A string prefixed with '[' for inclusive range
                - A string prefixed with '(' for exclusive range
                - The special symbol '+' to indicate the maximum element

        ``count`` is the maximum number of elements to return.
                If ``count`` is not provided or negative, all elements in the range are returned.
                If ``count`` is positive, at most ``count`` elements are returned.

        Returns an array of elements in lexicographical order within the specified range.
        Returns an empty array if the key doesn't exist.

        For more information, see https://redis.io/commands/vrange.
        """
        pieces = [key, start, end]
        if count is not None:
            pieces.append(count)
        return self.execute_command(VRANGE_CMD, *pieces)
