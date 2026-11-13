from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import (
    TYPE_CHECKING,
    Any,
    Awaitable,
    Callable,
    Iterable,
    Literal,
    Mapping,
    Protocol,
    Type,
    TypeVar,
    Union,
)

if TYPE_CHECKING:
    from redis._parsers import Encoder
    from redis.event import EventDispatcherInterface


class AsyncClientProtocol(Protocol):
    """Protocol for asynchronous Redis clients (redis.asyncio.client.Redis).

    This protocol uses a Literal marker to identify async clients.
    Used in @overload to provide correct return types for async clients.
    """

    _is_async_client: Literal[True]


class SyncClientProtocol(Protocol):
    """Protocol for synchronous Redis clients (redis.client.Redis).

    This protocol uses a Literal marker to identify sync clients.
    Used in @overload to provide correct return types for sync clients.
    """

    _is_async_client: Literal[False]


Number = Union[int, float]

EncodedT = Union[bytes, bytearray, memoryview]
DecodedT = Union[str, int, float]
EncodableT = Union[EncodedT, DecodedT]
AbsExpiryT = Union[int, datetime]
ExpiryT = Union[int, timedelta]
ZScoreBoundT = Union[float, str]  # str allows for the [ or ( prefix
BitfieldOffsetT = Union[int, str]  # str allows for #x syntax
_StringLikeT = Union[bytes, str, memoryview]
KeyT = _StringLikeT  # Main redis key space
PatternT = _StringLikeT  # Patterns matched against keys, fields etc
FieldT = EncodableT  # Fields within hash tables, streams and geo commands
KeysT = Union[KeyT, Iterable[KeyT]]
ResponseT = Union[Awaitable[Any], Any]
GroupT = _StringLikeT  # Consumer group
ConsumerT = _StringLikeT  # Consumer name
StreamIdT = Union[int, _StringLikeT]
ScriptTextT = _StringLikeT
TimeoutSecT = Union[int, float, _StringLikeT]
ACLGetUserData = (
    dict[str, bool | list[str] | list[list[str]] | list[dict[str, str]]] | None
)
ACLLogEntry = dict[str, str | float | dict[str, str | int]]
ACLLogData = list[ACLLogEntry]
CommandGetKeysAndFlagsEntry = list[bytes | str | list[bytes | str]]
CommandGetKeysAndFlagsResponse = list[CommandGetKeysAndFlagsEntry]
ClientTrackingInfoResponse = list[bytes | str] | dict[str, Any]
BlockingListPopResponse = tuple[bytes | str, bytes | str] | list[bytes | str] | None
HRandFieldResponse = bytes | str | list[bytes | str] | list[list[bytes | str]] | None
HScanPayload = dict[bytes | str, bytes | str] | list[bytes | str]
HScanResponse = tuple[int, HScanPayload]
ListMultiPopResponse = list[bytes | str | list[bytes | str]] | None
ScanResponse = tuple[int, list[bytes | str]]
SortResponse = list[bytes | str] | list[tuple[bytes | str, ...]] | int
GeoCoordinate = tuple[float, float] | list[float]
GeoSearchItem = bytes | str | list[bytes | str | float | int | GeoCoordinate]
GeoSearchResponse = list[GeoSearchItem]
GeoRadiusResponse = GeoSearchResponse | int
StreamEntry = tuple[bytes | str | None, dict[bytes | str, bytes | str] | None]
StreamRangeResponse = list[StreamEntry]
XClaimResponse = StreamRangeResponse | list[bytes | str]
XPendingRangeEntry = dict[str, bytes | str | int]
XPendingRangeResponse = list[XPendingRangeEntry]
XReadGroupClaimEntry = tuple[
    bytes | str,
    dict[bytes | str, bytes | str],
    bytes | str | int,
    bytes | str | int,
]
XReadGroupStreamResponse = StreamRangeResponse | list[XReadGroupClaimEntry]
XReadResponse = (
    list[list[Any]]
    | dict[bytes | str, list[StreamRangeResponse]]
    | dict[bytes | str, StreamRangeResponse]
)
XReadGroupResponse = (
    list[list[Any]]
    | dict[bytes | str, list[XReadGroupStreamResponse]]
    | dict[bytes | str, XReadGroupStreamResponse]
)
ClusterNodeDetail = dict[str, str | bool | list[list[str]] | list[dict[str, str]]]
ClusterLink = dict[str, Any] | list[Any]
ClusterLinksResponse = list[ClusterLink]
ClusterShard = dict[str, Any]
ClusterShardsResponse = list[ClusterShard]
SentinelMasterAddress = tuple[bytes | str, int] | None
SentinelMastersResponse = dict[str, dict[str, Any]] | list[dict[str, Any]]
TimeSeriesSample = tuple[int, float] | list[int | float]
TimeSeriesRangeResponse = list[TimeSeriesSample]
TimeSeriesMRangeSeries = list[Any]
TimeSeriesMRangeResponse = list[Any] | dict[bytes | str, TimeSeriesMRangeSeries]
# Timestamp-major row ``[timestamp, [value_for_key_0, value_for_key_1, ...]]``
# returned by TS.NRANGE / TS.NREVRANGE.
TimeSeriesNRangeResponse = list[list[int | list[float]]]
BloomScanDumpResponse = tuple[int, bytes | None]
ModuleListResponse = list[bytes | int | float | str | None]
BlockingZSetPopResponse = (
    tuple[bytes | str, bytes | str, float] | list[bytes | str | float] | None
)
ZMPopResponse = list[bytes | str | list[list[Any]]] | None
# ZMOVE/BZMOVE/ZMOVEMEMBER: ``[[member, score], ...]`` or ``None`` if nothing moved.
ZMoveResponse = list[list[Any]] | None
ZRandMemberResponse = (
    bytes | str | None | list[bytes | str] | list[bytes | str | float] | list[list[Any]]
)
ZSetScoredMembers = list[tuple[bytes | str, Any]] | list[list[Any]]
ZSetRangeResponse = list[bytes | str] | ZSetScoredMembers
ZScanPair = tuple[bytes | str, float] | list[bytes | str | float]
ZScanResponse = tuple[int, list[ZScanPair]]
LCSRange = tuple[int, int] | list[int]
LCSMatch = list[int | LCSRange]
LCSResult = dict[str, int | list[LCSMatch]]
LCSIndexResponse = list[Any] | dict[bytes | str, Any]
LCSCommandResponse = bytes | str | int | LCSIndexResponse
StralgoResponse = str | int | LCSResult

# Mapping is not covariant in the key type, which prevents
# Mapping[_StringLikeT, X] from accepting arguments of type Dict[str, X]. Using
# a TypeVar instead of a Union allows mappings with any of the permitted types
# to be passed. Care is needed if there is more than one such mapping in a
# type signature because they will all be required to be the same key type.
AnyKeyT = TypeVar("AnyKeyT", bytes, str, memoryview)
AnyFieldT = TypeVar("AnyFieldT", bytes, str, memoryview)

ExceptionMappingT = Mapping[str, Union[Type[Exception], Mapping[str, Type[Exception]]]]

ChannelT = _StringLikeT
AnyChannelT = TypeVar("AnyChannelT", bytes, str, memoryview)
PubSubHandler = Callable[[dict[str, Any]], Any]


@dataclass(frozen=True)
class Subscription:
    """PubSub channel or pattern subscription with an optional handler."""

    name: ChannelT
    handler: PubSubHandler | None = None


class CommandsProtocol(Protocol):
    _event_dispatcher: "EventDispatcherInterface"

    def execute_command(self, *args, **options) -> ResponseT: ...


class ClusterCommandsProtocol(CommandsProtocol):
    encoder: "Encoder"
