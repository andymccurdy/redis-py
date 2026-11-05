from typing import Any, Awaitable, overload

from redis.client import NEVER_DECODE
from redis.typing import (
    AsyncClientProtocol,
    BloomScanDumpResponse,
    ModuleListResponse,
    SyncClientProtocol,
)
from redis.utils import deprecated_function

from .info import BFInfo, CFInfo, CMSInfo, TDigestInfo, TopKInfo

BF_RESERVE = "BF.RESERVE"
BF_ADD = "BF.ADD"
BF_MADD = "BF.MADD"
BF_INSERT = "BF.INSERT"
BF_EXISTS = "BF.EXISTS"
BF_MEXISTS = "BF.MEXISTS"
BF_SCANDUMP = "BF.SCANDUMP"
BF_LOADCHUNK = "BF.LOADCHUNK"
BF_INFO = "BF.INFO"
BF_CARD = "BF.CARD"

CF_RESERVE = "CF.RESERVE"
CF_ADD = "CF.ADD"
CF_ADDNX = "CF.ADDNX"
CF_INSERT = "CF.INSERT"
CF_INSERTNX = "CF.INSERTNX"
CF_EXISTS = "CF.EXISTS"
CF_MEXISTS = "CF.MEXISTS"
CF_DEL = "CF.DEL"
CF_COUNT = "CF.COUNT"
CF_SCANDUMP = "CF.SCANDUMP"
CF_LOADCHUNK = "CF.LOADCHUNK"
CF_INFO = "CF.INFO"

CMS_INITBYDIM = "CMS.INITBYDIM"
CMS_INITBYPROB = "CMS.INITBYPROB"
CMS_INCRBY = "CMS.INCRBY"
CMS_QUERY = "CMS.QUERY"
CMS_MERGE = "CMS.MERGE"
CMS_INFO = "CMS.INFO"

TOPK_RESERVE = "TOPK.RESERVE"
TOPK_ADD = "TOPK.ADD"
TOPK_INCRBY = "TOPK.INCRBY"
TOPK_QUERY = "TOPK.QUERY"
TOPK_COUNT = "TOPK.COUNT"
TOPK_LIST = "TOPK.LIST"
TOPK_INFO = "TOPK.INFO"

TDIGEST_CREATE = "TDIGEST.CREATE"
TDIGEST_RESET = "TDIGEST.RESET"
TDIGEST_ADD = "TDIGEST.ADD"
TDIGEST_MERGE = "TDIGEST.MERGE"
TDIGEST_CDF = "TDIGEST.CDF"
TDIGEST_QUANTILE = "TDIGEST.QUANTILE"
TDIGEST_MIN = "TDIGEST.MIN"
TDIGEST_MAX = "TDIGEST.MAX"
TDIGEST_INFO = "TDIGEST.INFO"
TDIGEST_TRIMMED_MEAN = "TDIGEST.TRIMMED_MEAN"
TDIGEST_RANK = "TDIGEST.RANK"
TDIGEST_REVRANK = "TDIGEST.REVRANK"
TDIGEST_BYRANK = "TDIGEST.BYRANK"
TDIGEST_BYREVRANK = "TDIGEST.BYREVRANK"


class BFCommands:
    """Bloom Filter commands."""

    @overload
    def create(
        self: SyncClientProtocol,
        key,
        errorRate,
        capacity,
        expansion=None,
        noScale=None,
    ) -> bool: ...

    @overload
    def create(
        self: AsyncClientProtocol,
        key,
        errorRate,
        capacity,
        expansion=None,
        noScale=None,
    ) -> Awaitable[bool]: ...

    def create(
        self, key, errorRate, capacity, expansion=None, noScale=None
    ) -> bool | Awaitable[bool]:
        """
        Create a new Bloom Filter `key` with desired probability of false positives
        `errorRate` expected entries to be inserted as `capacity`.
        Default expansion value is 2. By default, filter is auto-scaling.
        For more information see `BF.RESERVE <https://redis.io/commands/bf.reserve>`_.
        """  # noqa
        params = [key, errorRate, capacity]
        self.append_expansion(params, expansion)
        self.append_no_scale(params, noScale)
        return self.execute_command(BF_RESERVE, *params)

    reserve = create

    @overload
    def add(self: SyncClientProtocol, key, item) -> int: ...

    @overload
    def add(self: AsyncClientProtocol, key, item) -> Awaitable[int]: ...

    def add(self, key, item) -> int | Awaitable[int]:
        """
        Add to a Bloom Filter `key` an `item`.
        For more information see `BF.ADD <https://redis.io/commands/bf.add>`_.
        """  # noqa
        return self.execute_command(BF_ADD, key, item)

    @overload
    def madd(self: SyncClientProtocol, key, *items) -> list[int]: ...

    @overload
    def madd(self: AsyncClientProtocol, key, *items) -> Awaitable[list[int]]: ...

    def madd(self, key, *items) -> list[int] | Awaitable[list[int]]:
        """
        Add to a Bloom Filter `key` multiple `items`.
        For more information see `BF.MADD <https://redis.io/commands/bf.madd>`_.
        """  # noqa
        return self.execute_command(BF_MADD, key, *items)

    @overload
    def insert(
        self: SyncClientProtocol,
        key,
        items,
        capacity=None,
        error=None,
        noCreate=None,
        expansion=None,
        noScale=None,
    ) -> list[int]: ...

    @overload
    def insert(
        self: AsyncClientProtocol,
        key,
        items,
        capacity=None,
        error=None,
        noCreate=None,
        expansion=None,
        noScale=None,
    ) -> Awaitable[list[int]]: ...

    def insert(
        self,
        key,
        items,
        capacity=None,
        error=None,
        noCreate=None,
        expansion=None,
        noScale=None,
    ) -> list[int] | Awaitable[list[int]]:
        """
        Add to a Bloom Filter `key` multiple `items`.

        If `nocreate` remain `None` and `key` does not exist, a new Bloom Filter
        `key` will be created with desired probability of false positives `errorRate`
        and expected entries to be inserted as `size`.
        For more information see `BF.INSERT <https://redis.io/commands/bf.insert>`_.
        """  # noqa
        params = [key]
        self.append_capacity(params, capacity)
        self.append_error(params, error)
        self.append_expansion(params, expansion)
        self.append_no_create(params, noCreate)
        self.append_no_scale(params, noScale)
        self.append_items(params, items)

        return self.execute_command(BF_INSERT, *params)

    @overload
    def exists(self: SyncClientProtocol, key, item) -> int: ...

    @overload
    def exists(self: AsyncClientProtocol, key, item) -> Awaitable[int]: ...

    def exists(self, key, item) -> int | Awaitable[int]:
        """
        Check whether an `item` exists in Bloom Filter `key`.
        For more information see `BF.EXISTS <https://redis.io/commands/bf.exists>`_.
        """  # noqa
        return self.execute_command(BF_EXISTS, key, item)

    @overload
    def mexists(self: SyncClientProtocol, key, *items) -> list[int]: ...

    @overload
    def mexists(self: AsyncClientProtocol, key, *items) -> Awaitable[list[int]]: ...

    def mexists(self, key, *items) -> list[int] | Awaitable[list[int]]:
        """
        Check whether `items` exist in Bloom Filter `key`.
        For more information see `BF.MEXISTS <https://redis.io/commands/bf.mexists>`_.
        """  # noqa
        return self.execute_command(BF_MEXISTS, key, *items)

    @overload
    def scandump(self: SyncClientProtocol, key, iter) -> BloomScanDumpResponse: ...

    @overload
    def scandump(
        self: AsyncClientProtocol, key, iter
    ) -> Awaitable[BloomScanDumpResponse]: ...

    def scandump(
        self, key, iter
    ) -> BloomScanDumpResponse | Awaitable[BloomScanDumpResponse]:
        """
        Begin an incremental save of the bloom filter `key`.

        This is useful for large bloom filters which cannot fit into the normal SAVE and RESTORE model.
        The first time this command is called, the value of `iter` should be 0.
        This command will return successive (iter, data) pairs until (0, NULL) to indicate completion.
        For more information see `BF.SCANDUMP <https://redis.io/commands/bf.scandump>`_.
        """  # noqa
        params = [key, iter]
        options = {}
        options[NEVER_DECODE] = []
        return self.execute_command(BF_SCANDUMP, *params, **options)

    @overload
    def loadchunk(self: SyncClientProtocol, key, iter, data) -> bytes | str: ...

    @overload
    def loadchunk(
        self: AsyncClientProtocol, key, iter, data
    ) -> Awaitable[bytes | str]: ...

    def loadchunk(self, key, iter, data) -> (bytes | str) | Awaitable[bytes | str]:
        """
        Restore a filter previously saved using SCANDUMP.

        See the SCANDUMP command for example usage.
        This command will overwrite any bloom filter stored under key.
        Ensure that the bloom filter will not be modified between invocations.
        For more information see `BF.LOADCHUNK <https://redis.io/commands/bf.loadchunk>`_.
        """  # noqa
        return self.execute_command(BF_LOADCHUNK, key, iter, data)

    @overload
    def info(self: SyncClientProtocol, key) -> BFInfo | dict[str, Any]: ...

    @overload
    def info(self: AsyncClientProtocol, key) -> Awaitable[BFInfo | dict[str, Any]]: ...

    def info(self, key) -> (BFInfo | dict[str, Any]) | Awaitable[
        BFInfo | dict[str, Any]
    ]:
        """
        Return capacity, size, number of filters, number of items inserted, and expansion rate.
        For more information see `BF.INFO <https://redis.io/commands/bf.info>`_.
        """  # noqa
        return self.execute_command(BF_INFO, key)

    @overload
    def card(self: SyncClientProtocol, key) -> int: ...

    @overload
    def card(self: AsyncClientProtocol, key) -> Awaitable[int]: ...

    def card(self, key) -> int | Awaitable[int]:
        """
        Returns the cardinality of a Bloom filter - number of items that were added to a Bloom filter and detected as unique
        (items that caused at least one bit to be set in at least one sub-filter).
        For more information see `BF.CARD <https://redis.io/commands/bf.card>`_.
        """  # noqa
        return self.execute_command(BF_CARD, key)


class CFCommands:
    """Cuckoo Filter commands."""

    @overload
    def create(
        self: SyncClientProtocol,
        key,
        capacity,
        expansion=None,
        bucket_size=None,
        max_iterations=None,
    ) -> bool: ...

    @overload
    def create(
        self: AsyncClientProtocol,
        key,
        capacity,
        expansion=None,
        bucket_size=None,
        max_iterations=None,
    ) -> Awaitable[bool]: ...

    def create(
        self, key, capacity, expansion=None, bucket_size=None, max_iterations=None
    ) -> bool | Awaitable[bool]:
        """
        Create a new Cuckoo Filter `key` an initial `capacity` items.
        For more information see `CF.RESERVE <https://redis.io/commands/cf.reserve>`_.
        """  # noqa
        params = [key, capacity]
        self.append_expansion(params, expansion)
        self.append_bucket_size(params, bucket_size)
        self.append_max_iterations(params, max_iterations)
        return self.execute_command(CF_RESERVE, *params)

    reserve = create

    @overload
    def add(self: SyncClientProtocol, key, item) -> int: ...

    @overload
    def add(self: AsyncClientProtocol, key, item) -> Awaitable[int]: ...

    def add(self, key, item) -> int | Awaitable[int]:
        """
        Add an `item` to a Cuckoo Filter `key`.
        For more information see `CF.ADD <https://redis.io/commands/cf.add>`_.
        """  # noqa
        return self.execute_command(CF_ADD, key, item)

    @overload
    def addnx(self: SyncClientProtocol, key, item) -> int: ...

    @overload
    def addnx(self: AsyncClientProtocol, key, item) -> Awaitable[int]: ...

    def addnx(self, key, item) -> int | Awaitable[int]:
        """
        Add an `item` to a Cuckoo Filter `key` only if item does not yet exist.
        Command might be slower that `add`.
        For more information see `CF.ADDNX <https://redis.io/commands/cf.addnx>`_.
        """  # noqa
        return self.execute_command(CF_ADDNX, key, item)

    @overload
    def insert(
        self: SyncClientProtocol, key, items, capacity=None, nocreate=None
    ) -> list[int]: ...

    @overload
    def insert(
        self: AsyncClientProtocol, key, items, capacity=None, nocreate=None
    ) -> Awaitable[list[int]]: ...

    def insert(
        self, key, items, capacity=None, nocreate=None
    ) -> list[int] | Awaitable[list[int]]:
        """
        Add multiple `items` to a Cuckoo Filter `key`, allowing the filter
        to be created with a custom `capacity` if it does not yet exist.
        `items` must be provided as a list.
        For more information see `CF.INSERT <https://redis.io/commands/cf.insert>`_.
        """  # noqa
        params = [key]
        self.append_capacity(params, capacity)
        self.append_no_create(params, nocreate)
        self.append_items(params, items)
        return self.execute_command(CF_INSERT, *params)

    @overload
    def insertnx(
        self: SyncClientProtocol, key, items, capacity=None, nocreate=None
    ) -> list[int]: ...

    @overload
    def insertnx(
        self: AsyncClientProtocol, key, items, capacity=None, nocreate=None
    ) -> Awaitable[list[int]]: ...

    def insertnx(
        self, key, items, capacity=None, nocreate=None
    ) -> list[int] | Awaitable[list[int]]:
        """
        Add multiple `items` to a Cuckoo Filter `key` only if they do not exist yet,
        allowing the filter to be created with a custom `capacity` if it does not yet exist.
        `items` must be provided as a list.
        For more information see `CF.INSERTNX <https://redis.io/commands/cf.insertnx>`_.
        """  # noqa
        params = [key]
        self.append_capacity(params, capacity)
        self.append_no_create(params, nocreate)
        self.append_items(params, items)
        return self.execute_command(CF_INSERTNX, *params)

    @overload
    def exists(self: SyncClientProtocol, key, item) -> int: ...

    @overload
    def exists(self: AsyncClientProtocol, key, item) -> Awaitable[int]: ...

    def exists(self, key, item) -> int | Awaitable[int]:
        """
        Check whether an `item` exists in Cuckoo Filter `key`.
        For more information see `CF.EXISTS <https://redis.io/commands/cf.exists>`_.
        """  # noqa
        return self.execute_command(CF_EXISTS, key, item)

    @overload
    def mexists(self: SyncClientProtocol, key, *items) -> list[int]: ...

    @overload
    def mexists(self: AsyncClientProtocol, key, *items) -> Awaitable[list[int]]: ...

    def mexists(self, key, *items) -> list[int] | Awaitable[list[int]]:
        """
        Check whether an `items` exist in Cuckoo Filter `key`.
        For more information see `CF.MEXISTS <https://redis.io/commands/cf.mexists>`_.
        """  # noqa
        return self.execute_command(CF_MEXISTS, key, *items)

    @overload
    def delete(self: SyncClientProtocol, key, item) -> int: ...

    @overload
    def delete(self: AsyncClientProtocol, key, item) -> Awaitable[int]: ...

    def delete(self, key, item) -> int | Awaitable[int]:
        """
        Delete `item` from `key`.
        For more information see `CF.DEL <https://redis.io/commands/cf.del>`_.
        """  # noqa
        return self.execute_command(CF_DEL, key, item)

    @overload
    def count(self: SyncClientProtocol, key, item) -> int: ...

    @overload
    def count(self: AsyncClientProtocol, key, item) -> Awaitable[int]: ...

    def count(self, key, item) -> int | Awaitable[int]:
        """
        Return the number of times an `item` may be in the `key`.
        For more information see `CF.COUNT <https://redis.io/commands/cf.count>`_.
        """  # noqa
        return self.execute_command(CF_COUNT, key, item)

    @overload
    def scandump(self: SyncClientProtocol, key, iter) -> BloomScanDumpResponse: ...

    @overload
    def scandump(
        self: AsyncClientProtocol, key, iter
    ) -> Awaitable[BloomScanDumpResponse]: ...

    def scandump(
        self, key, iter
    ) -> BloomScanDumpResponse | Awaitable[BloomScanDumpResponse]:
        """
        Begin an incremental save of the Cuckoo filter `key`.

        This is useful for large Cuckoo filters which cannot fit into the normal
        SAVE and RESTORE model.
        The first time this command is called, the value of `iter` should be 0.
        This command will return successive (iter, data) pairs until
        (0, NULL) to indicate completion.
        For more information see `CF.SCANDUMP <https://redis.io/commands/cf.scandump>`_.
        """  # noqa
        params = [key, iter]
        options = {}
        options[NEVER_DECODE] = []
        return self.execute_command(CF_SCANDUMP, *params, **options)

    @overload
    def loadchunk(self: SyncClientProtocol, key, iter, data) -> bytes | str: ...

    @overload
    def loadchunk(
        self: AsyncClientProtocol, key, iter, data
    ) -> Awaitable[bytes | str]: ...

    def loadchunk(self, key, iter, data) -> (bytes | str) | Awaitable[bytes | str]:
        """
        Restore a filter previously saved using SCANDUMP. See the SCANDUMP command for example usage.

        This command will overwrite any Cuckoo filter stored under key.
        Ensure that the Cuckoo filter will not be modified between invocations.
        For more information see `CF.LOADCHUNK <https://redis.io/commands/cf.loadchunk>`_.
        """  # noqa
        return self.execute_command(CF_LOADCHUNK, key, iter, data)

    @overload
    def info(self: SyncClientProtocol, key) -> CFInfo | dict[str, Any]: ...

    @overload
    def info(self: AsyncClientProtocol, key) -> Awaitable[CFInfo | dict[str, Any]]: ...

    def info(self, key) -> (CFInfo | dict[str, Any]) | Awaitable[
        CFInfo | dict[str, Any]
    ]:
        """
        Return size, number of buckets, number of filter, number of items inserted,
        number of items deleted, bucket size, expansion rate, and max iteration.
        For more information see `CF.INFO <https://redis.io/commands/cf.info>`_.
        """  # noqa
        return self.execute_command(CF_INFO, key)


class TOPKCommands:
    """TOP-k Filter commands."""

    @overload
    def reserve(self: SyncClientProtocol, key, k, width, depth, decay) -> bool: ...

    @overload
    def reserve(
        self: AsyncClientProtocol, key, k, width, depth, decay
    ) -> Awaitable[bool]: ...

    def reserve(self, key, k, width, depth, decay) -> bool | Awaitable[bool]:
        """
        Create a new Top-K Filter `key` with desired probability of false
        positives `errorRate` expected entries to be inserted as `size`.
        For more information see `TOPK.RESERVE <https://redis.io/commands/topk.reserve>`_.
        """  # noqa
        return self.execute_command(TOPK_RESERVE, key, k, width, depth, decay)

    @overload
    def add(self: SyncClientProtocol, key, *items) -> ModuleListResponse: ...

    @overload
    def add(
        self: AsyncClientProtocol, key, *items
    ) -> Awaitable[ModuleListResponse]: ...

    def add(self, key, *items) -> ModuleListResponse | Awaitable[ModuleListResponse]:
        """
        Add one `item` or more to a Top-K Filter `key`.
        For more information see `TOPK.ADD <https://redis.io/commands/topk.add>`_.
        """  # noqa
        return self.execute_command(TOPK_ADD, key, *items)

    @overload
    def incrby(
        self: SyncClientProtocol, key, items, increments
    ) -> ModuleListResponse: ...

    @overload
    def incrby(
        self: AsyncClientProtocol, key, items, increments
    ) -> Awaitable[ModuleListResponse]: ...

    def incrby(
        self, key, items, increments
    ) -> ModuleListResponse | Awaitable[ModuleListResponse]:
        """
        Add/increase `items` to a Top-K Sketch `key` by ''increments''.
        Both `items` and `increments` are lists.
        For more information see `TOPK.INCRBY <https://redis.io/commands/topk.incrby>`_.

        Example:

        >>> client.topk().incrby('A', ['foo'], [1])
        """  # noqa
        params = [key]
        self.append_items_and_increments(params, items, increments)
        return self.execute_command(TOPK_INCRBY, *params)

    @overload
    def query(self: SyncClientProtocol, key, *items) -> list[int]: ...

    @overload
    def query(self: AsyncClientProtocol, key, *items) -> Awaitable[list[int]]: ...

    def query(self, key, *items) -> list[int] | Awaitable[list[int]]:
        """
        Check whether one `item` or more is a Top-K item at `key`.
        For more information see `TOPK.QUERY <https://redis.io/commands/topk.query>`_.
        """  # noqa
        return self.execute_command(TOPK_QUERY, key, *items)

    @overload
    def count(self: SyncClientProtocol, key, *items) -> list[int]: ...

    @overload
    def count(self: AsyncClientProtocol, key, *items) -> Awaitable[list[int]]: ...

    @deprecated_function(version="4.4.0", reason="deprecated since redisbloom 2.4.0")
    def count(self, key, *items) -> list[int] | Awaitable[list[int]]:
        """
        Return count for one `item` or more from `key`.
        For more information see `TOPK.COUNT <https://redis.io/commands/topk.count>`_.
        """  # noqa
        return self.execute_command(TOPK_COUNT, key, *items)

    @overload
    def list(
        self: SyncClientProtocol, key, withcount: bool = False
    ) -> ModuleListResponse: ...

    @overload
    def list(
        self: AsyncClientProtocol, key, withcount: bool = False
    ) -> Awaitable[ModuleListResponse]: ...

    def list(
        self, key, withcount: bool = False
    ) -> ModuleListResponse | Awaitable[ModuleListResponse]:
        """
        Return full list of items in Top-K list of `key`.
        If `withcount` set to True, return full list of items
        with probabilistic count in Top-K list of `key`.
        For more information see `TOPK.LIST <https://redis.io/commands/topk.list>`_.
        """  # noqa
        params = [key]
        if withcount:
            params.append("WITHCOUNT")
        return self.execute_command(TOPK_LIST, *params)

    @overload
    def info(self: SyncClientProtocol, key) -> TopKInfo | dict[str, Any]: ...

    @overload
    def info(
        self: AsyncClientProtocol, key
    ) -> Awaitable[TopKInfo | dict[str, Any]]: ...

    def info(self, key) -> (TopKInfo | dict[str, Any]) | Awaitable[
        TopKInfo | dict[str, Any]
    ]:
        """
        Return k, width, depth and decay values of `key`.
        For more information see `TOPK.INFO <https://redis.io/commands/topk.info>`_.
        """  # noqa
        return self.execute_command(TOPK_INFO, key)


class TDigestCommands:
    @overload
    def create(self: SyncClientProtocol, key, compression=100) -> bool: ...

    @overload
    def create(self: AsyncClientProtocol, key, compression=100) -> Awaitable[bool]: ...

    def create(self, key, compression=100) -> bool | Awaitable[bool]:
        """
        Allocate the memory and initialize the t-digest.
        For more information see `TDIGEST.CREATE <https://redis.io/commands/tdigest.create>`_.
        """  # noqa
        return self.execute_command(TDIGEST_CREATE, key, "COMPRESSION", compression)

    @overload
    def reset(self: SyncClientProtocol, key) -> bytes | str: ...

    @overload
    def reset(self: AsyncClientProtocol, key) -> Awaitable[bytes | str]: ...

    def reset(self, key) -> (bytes | str) | Awaitable[bytes | str]:
        """
        Reset the sketch `key` to zero - empty out the sketch and re-initialize it.
        For more information see `TDIGEST.RESET <https://redis.io/commands/tdigest.reset>`_.
        """  # noqa
        return self.execute_command(TDIGEST_RESET, key)

    @overload
    def add(self: SyncClientProtocol, key, values) -> bytes | str: ...

    @overload
    def add(self: AsyncClientProtocol, key, values) -> Awaitable[bytes | str]: ...

    def add(self, key, values) -> (bytes | str) | Awaitable[bytes | str]:
        """
        Adds one or more observations to a t-digest sketch `key`.

        For more information see `TDIGEST.ADD <https://redis.io/commands/tdigest.add>`_.
        """  # noqa
        return self.execute_command(TDIGEST_ADD, key, *values)

    @overload
    def merge(
        self: SyncClientProtocol,
        destination_key,
        num_keys,
        *keys,
        compression=None,
        override=False,
    ) -> bytes | str: ...

    @overload
    def merge(
        self: AsyncClientProtocol,
        destination_key,
        num_keys,
        *keys,
        compression=None,
        override=False,
    ) -> Awaitable[bytes | str]: ...

    def merge(
        self, destination_key, num_keys, *keys, compression=None, override=False
    ) -> (bytes | str) | Awaitable[bytes | str]:
        """
        Merges all of the values from `keys` to 'destination-key' sketch.
        It is mandatory to provide the `num_keys` before passing the input keys and
        the other (optional) arguments.
        If `destination_key` already exists its values are merged with the input keys.
        If you wish to override the destination key contents use the `OVERRIDE` parameter.

        For more information see `TDIGEST.MERGE <https://redis.io/commands/tdigest.merge>`_.
        """  # noqa
        params = [destination_key, num_keys, *keys]
        if compression is not None:
            params.extend(["COMPRESSION", compression])
        if override:
            params.append("OVERRIDE")
        return self.execute_command(TDIGEST_MERGE, *params)

    @overload
    def min(self: SyncClientProtocol, key) -> float: ...

    @overload
    def min(self: AsyncClientProtocol, key) -> Awaitable[float]: ...

    def min(self, key) -> float | Awaitable[float]:
        """
        Return minimum value from the sketch `key`. Will return DBL_MAX if the sketch is empty.
        For more information see `TDIGEST.MIN <https://redis.io/commands/tdigest.min>`_.
        """  # noqa
        return self.execute_command(TDIGEST_MIN, key)

    @overload
    def max(self: SyncClientProtocol, key) -> float: ...

    @overload
    def max(self: AsyncClientProtocol, key) -> Awaitable[float]: ...

    def max(self, key) -> float | Awaitable[float]:
        """
        Return maximum value from the sketch `key`. Will return DBL_MIN if the sketch is empty.
        For more information see `TDIGEST.MAX <https://redis.io/commands/tdigest.max>`_.
        """  # noqa
        return self.execute_command(TDIGEST_MAX, key)

    @overload
    def quantile(
        self: SyncClientProtocol, key, quantile, *quantiles
    ) -> ModuleListResponse: ...

    @overload
    def quantile(
        self: AsyncClientProtocol, key, quantile, *quantiles
    ) -> Awaitable[ModuleListResponse]: ...

    def quantile(
        self, key, quantile, *quantiles
    ) -> ModuleListResponse | Awaitable[ModuleListResponse]:
        """
        Returns estimates of one or more cutoffs such that a specified fraction of the
        observations added to this t-digest would be less than or equal to each of the
        specified cutoffs. (Multiple quantiles can be returned with one call)
        For more information see `TDIGEST.QUANTILE <https://redis.io/commands/tdigest.quantile>`_.
        """  # noqa
        return self.execute_command(TDIGEST_QUANTILE, key, quantile, *quantiles)

    @overload
    def cdf(self: SyncClientProtocol, key, value, *values) -> ModuleListResponse: ...

    @overload
    def cdf(
        self: AsyncClientProtocol, key, value, *values
    ) -> Awaitable[ModuleListResponse]: ...

    def cdf(
        self, key, value, *values
    ) -> ModuleListResponse | Awaitable[ModuleListResponse]:
        """
        Return double fraction of all points added which are <= value.
        For more information see `TDIGEST.CDF <https://redis.io/commands/tdigest.cdf>`_.
        """  # noqa
        return self.execute_command(TDIGEST_CDF, key, value, *values)

    @overload
    def info(self: SyncClientProtocol, key) -> TDigestInfo | dict[str, Any]: ...

    @overload
    def info(
        self: AsyncClientProtocol, key
    ) -> Awaitable[TDigestInfo | dict[str, Any]]: ...

    def info(self, key) -> (TDigestInfo | dict[str, Any]) | Awaitable[
        TDigestInfo | dict[str, Any]
    ]:
        """
        Return Compression, Capacity, Merged Nodes, Unmerged Nodes, Merged Weight, Unmerged Weight
        and Total Compressions.
        For more information see `TDIGEST.INFO <https://redis.io/commands/tdigest.info>`_.
        """  # noqa
        return self.execute_command(TDIGEST_INFO, key)

    @overload
    def trimmed_mean(
        self: SyncClientProtocol, key, low_cut_quantile, high_cut_quantile
    ) -> float: ...

    @overload
    def trimmed_mean(
        self: AsyncClientProtocol, key, low_cut_quantile, high_cut_quantile
    ) -> Awaitable[float]: ...

    def trimmed_mean(
        self, key, low_cut_quantile, high_cut_quantile
    ) -> float | Awaitable[float]:
        """
        Return mean value from the sketch, excluding observation values outside
        the low and high cutoff quantiles.
        For more information see `TDIGEST.TRIMMED_MEAN <https://redis.io/commands/tdigest.trimmed_mean>`_.
        """  # noqa
        return self.execute_command(
            TDIGEST_TRIMMED_MEAN, key, low_cut_quantile, high_cut_quantile
        )

    @overload
    def rank(self: SyncClientProtocol, key, value, *values) -> list[int]: ...

    @overload
    def rank(
        self: AsyncClientProtocol, key, value, *values
    ) -> Awaitable[list[int]]: ...

    def rank(self, key, value, *values) -> list[int] | Awaitable[list[int]]:
        """
        Retrieve the estimated rank of value (the number of observations in the sketch
        that are smaller than value + half the number of observations that are equal to value).

        For more information see `TDIGEST.RANK <https://redis.io/commands/tdigest.rank>`_.
        """  # noqa
        return self.execute_command(TDIGEST_RANK, key, value, *values)

    @overload
    def revrank(self: SyncClientProtocol, key, value, *values) -> list[int]: ...

    @overload
    def revrank(
        self: AsyncClientProtocol, key, value, *values
    ) -> Awaitable[list[int]]: ...

    def revrank(self, key, value, *values) -> list[int] | Awaitable[list[int]]:
        """
        Retrieve the estimated rank of value (the number of observations in the sketch
        that are larger than value + half the number of observations that are equal to value).

        For more information see `TDIGEST.REVRANK <https://redis.io/commands/tdigest.revrank>`_.
        """  # noqa
        return self.execute_command(TDIGEST_REVRANK, key, value, *values)

    @overload
    def byrank(self: SyncClientProtocol, key, rank, *ranks) -> ModuleListResponse: ...

    @overload
    def byrank(
        self: AsyncClientProtocol, key, rank, *ranks
    ) -> Awaitable[ModuleListResponse]: ...

    def byrank(
        self, key, rank, *ranks
    ) -> ModuleListResponse | Awaitable[ModuleListResponse]:
        """
        Retrieve an estimation of the value with the given rank.

        For more information see `TDIGEST.BY_RANK <https://redis.io/commands/tdigest.by_rank>`_.
        """  # noqa
        return self.execute_command(TDIGEST_BYRANK, key, rank, *ranks)

    @overload
    def byrevrank(
        self: SyncClientProtocol, key, rank, *ranks
    ) -> ModuleListResponse: ...

    @overload
    def byrevrank(
        self: AsyncClientProtocol, key, rank, *ranks
    ) -> Awaitable[ModuleListResponse]: ...

    def byrevrank(
        self, key, rank, *ranks
    ) -> ModuleListResponse | Awaitable[ModuleListResponse]:
        """
        Retrieve an estimation of the value with the given reverse rank.

        For more information see `TDIGEST.BY_REVRANK <https://redis.io/commands/tdigest.by_revrank>`_.
        """  # noqa
        return self.execute_command(TDIGEST_BYREVRANK, key, rank, *ranks)


class CMSCommands:
    """Count-Min Sketch Commands"""

    @overload
    def initbydim(
        self: SyncClientProtocol, key, width, depth, cell_size: int | None = None
    ) -> bool: ...

    @overload
    def initbydim(
        self: AsyncClientProtocol, key, width, depth, cell_size: int | None = None
    ) -> Awaitable[bool]: ...

    def initbydim(
        self, key, width, depth, cell_size: int | None = None
    ) -> bool | Awaitable[bool]:
        """
        Initialize a Count-Min Sketch `key` to dimensions (`width`, `depth`) specified by user.
        `cell_size` sets the size of each counter in bytes: 1, 2, 4 or 8 (server default is 4).
        Increments that would overflow a counter are rejected by the server.
        Requires a RedisBloom version newer than the one shipped with Redis 8.10.
        For more information see `CMS.INITBYDIM <https://redis.io/commands/cms.initbydim>`_.
        """  # noqa
        params = [key, width, depth]
        self.append_cell_size(params, cell_size)
        return self.execute_command(CMS_INITBYDIM, *params)

    @overload
    def initbyprob(
        self: SyncClientProtocol, key, error, probability, cell_size: int | None = None
    ) -> bool: ...

    @overload
    def initbyprob(
        self: AsyncClientProtocol,
        key,
        error,
        probability,
        cell_size: int | None = None,
    ) -> Awaitable[bool]: ...

    def initbyprob(
        self, key, error, probability, cell_size: int | None = None
    ) -> bool | Awaitable[bool]:
        """
        Initialize a Count-Min Sketch `key` to characteristics (`error`, `probability`) specified by user.
        `cell_size` sets the size of each counter in bytes: 1, 2, 4 or 8 (server default is 4).
        Increments that would overflow a counter are rejected by the server.
        Requires a RedisBloom version newer than the one shipped with Redis 8.10.
        For more information see `CMS.INITBYPROB <https://redis.io/commands/cms.initbyprob>`_.
        """  # noqa
        params = [key, error, probability]
        self.append_cell_size(params, cell_size)
        return self.execute_command(CMS_INITBYPROB, *params)

    @overload
    def incrby(self: SyncClientProtocol, key, items, increments) -> list[int]: ...

    @overload
    def incrby(
        self: AsyncClientProtocol, key, items, increments
    ) -> Awaitable[list[int]]: ...

    def incrby(self, key, items, increments) -> list[int] | Awaitable[list[int]]:
        """
        Add/increase `items` to a Count-Min Sketch `key` by ''increments''.
        Both `items` and `increments` are lists.
        For more information see `CMS.INCRBY <https://redis.io/commands/cms.incrby>`_.

        Example:

        >>> client.cms().incrby('A', ['foo'], [1])
        """  # noqa
        params = [key]
        self.append_items_and_increments(params, items, increments)
        return self.execute_command(CMS_INCRBY, *params)

    @overload
    def query(self: SyncClientProtocol, key, *items) -> list[int]: ...

    @overload
    def query(self: AsyncClientProtocol, key, *items) -> Awaitable[list[int]]: ...

    def query(self, key, *items) -> list[int] | Awaitable[list[int]]:
        """
        Return count for an `item` from `key`. Multiple items can be queried with one call.
        For more information see `CMS.QUERY <https://redis.io/commands/cms.query>`_.
        """  # noqa
        return self.execute_command(CMS_QUERY, key, *items)

    @overload
    def merge(
        self: SyncClientProtocol, destKey, numKeys, srcKeys, weights=[]
    ) -> bool: ...

    @overload
    def merge(
        self: AsyncClientProtocol, destKey, numKeys, srcKeys, weights=[]
    ) -> Awaitable[bool]: ...

    def merge(self, destKey, numKeys, srcKeys, weights=[]) -> bool | Awaitable[bool]:
        """
        Merge `numKeys` of sketches into `destKey`. Sketches specified in `srcKeys`.
        All sketches must have identical width and depth.
        `Weights` can be used to multiply certain sketches. Default weight is 1.
        Both `srcKeys` and `weights` are lists.
        For more information see `CMS.MERGE <https://redis.io/commands/cms.merge>`_.
        """  # noqa
        params = [destKey, numKeys]
        params += srcKeys
        self.append_weights(params, weights)
        return self.execute_command(CMS_MERGE, *params)

    @overload
    def info(self: SyncClientProtocol, key) -> CMSInfo | dict[str, Any]: ...

    @overload
    def info(self: AsyncClientProtocol, key) -> Awaitable[CMSInfo | dict[str, Any]]: ...

    def info(self, key) -> (CMSInfo | dict[str, Any]) | Awaitable[
        CMSInfo | dict[str, Any]
    ]:
        """
        Return width, depth, total count and, on servers that support it, the
        counter `cell_size` of the sketch.
        For more information see `CMS.INFO <https://redis.io/commands/cms.info>`_.
        """  # noqa
        return self.execute_command(CMS_INFO, key)
