from typing import Any, Awaitable, Dict, Iterable, List, Tuple, overload

from redis.exceptions import DataError
from redis.typing import (
    AsyncClientProtocol,
    EncodableT,
    KeyT,
    Number,
    SyncClientProtocol,
    TimeSeriesMRangeResponse,
    TimeSeriesNRangeResponse,
    TimeSeriesRangeResponse,
    TimeSeriesSample,
)

from .info import TSInfo

ADD_CMD = "TS.ADD"
ALTER_CMD = "TS.ALTER"
CREATERULE_CMD = "TS.CREATERULE"
CREATE_CMD = "TS.CREATE"
DECRBY_CMD = "TS.DECRBY"
DELETERULE_CMD = "TS.DELETERULE"
DEL_CMD = "TS.DEL"
GET_CMD = "TS.GET"
INCRBY_CMD = "TS.INCRBY"
INFO_CMD = "TS.INFO"
MADD_CMD = "TS.MADD"
MGET_CMD = "TS.MGET"
MRANGE_CMD = "TS.MRANGE"
MREVRANGE_CMD = "TS.MREVRANGE"
NRANGE_CMD = "TS.NRANGE"
NREVRANGE_CMD = "TS.NREVRANGE"
QUERYINDEX_CMD = "TS.QUERYINDEX"
QUERYLABELS_CMD = "TS.QUERYLABELS"
RANGE_CMD = "TS.RANGE"
READ_CMD = "TS.READ"
REVRANGE_CMD = "TS.REVRANGE"


class TimeSeriesCommands:
    """RedisTimeSeries Commands."""

    @overload
    def create(
        self: SyncClientProtocol,
        key: KeyT,
        retention_msecs: int | None = None,
        uncompressed: bool | None = False,
        labels: Dict[str, str] | None = None,
        chunk_size: int | None = None,
        duplicate_policy: str | None = None,
        ignore_max_time_diff: int | None = None,
        ignore_max_val_diff: Number | None = None,
    ) -> bool: ...

    @overload
    def create(
        self: AsyncClientProtocol,
        key: KeyT,
        retention_msecs: int | None = None,
        uncompressed: bool | None = False,
        labels: Dict[str, str] | None = None,
        chunk_size: int | None = None,
        duplicate_policy: str | None = None,
        ignore_max_time_diff: int | None = None,
        ignore_max_val_diff: Number | None = None,
    ) -> Awaitable[bool]: ...

    def create(
        self,
        key: KeyT,
        retention_msecs: int | None = None,
        uncompressed: bool | None = False,
        labels: Dict[str, str] | None = None,
        chunk_size: int | None = None,
        duplicate_policy: str | None = None,
        ignore_max_time_diff: int | None = None,
        ignore_max_val_diff: Number | None = None,
    ) -> bool | Awaitable[bool]:
        """
        Create a new time-series.

        For more information see https://redis.io/commands/ts.create/

        Args:
            key:
                The time-series key.
            retention_msecs:
                Maximum age for samples, compared to the highest reported timestamp in
                milliseconds. If `None` or `0` is passed, the series is not trimmed at
                all.
            uncompressed:
                Changes data storage from compressed (default) to uncompressed.
            labels:
                A dictionary of label-value pairs that represent metadata labels of the
                key.
            chunk_size:
                Memory size, in bytes, allocated for each data chunk. Must be a multiple
                of 8 in the range `[48..1048576]`. In earlier versions of the module the
                minimum value was different.
            duplicate_policy:
                Policy for handling multiple samples with identical timestamps. Can be
                one of:

                - 'block': An error will occur and the new value will be ignored.
                - 'first': Ignore the new value.
                - 'last': Override with the latest value.
                - 'min': Only override if the value is lower than the existing value.
                - 'max': Only override if the value is higher than the existing value.
                - 'sum': If a previous sample exists, add the new sample to it so
                  that the updated value is equal to (previous + new). If no
                  previous sample exists, set the updated value equal to the new
                  value.

            ignore_max_time_diff:
                A non-negative integer value, in milliseconds, that sets an ignore
                threshold for added timestamps. If the difference between the last
                timestamp and the new timestamp is lower than this threshold, the new
                entry is ignored. Only applicable if `duplicate_policy` is set to
                `last`, and if `ignore_max_val_diff` is also set. Available since
                RedisTimeSeries version 1.12.0.
            ignore_max_val_diff:
                A non-negative floating point value, that sets an ignore threshold for
                added values. If the difference between the last value and the new value
                is lower than this threshold, the new entry is ignored. Only applicable
                if `duplicate_policy` is set to `last`, and if `ignore_max_time_diff` is
                also set. Available since RedisTimeSeries version 1.12.0.
        """
        params: list[EncodableT] = [key]
        self._append_retention(params, retention_msecs)
        self._append_uncompressed(params, uncompressed)
        self._append_chunk_size(params, chunk_size)
        self._append_duplicate_policy(params, duplicate_policy)
        self._append_insertion_filters(
            params, ignore_max_time_diff, ignore_max_val_diff
        )
        self._append_labels(params, labels)

        return self.execute_command(CREATE_CMD, *params)

    @overload
    def alter(
        self: SyncClientProtocol,
        key: KeyT,
        retention_msecs: int | None = None,
        labels: Dict[str, str] | None = None,
        chunk_size: int | None = None,
        duplicate_policy: str | None = None,
        ignore_max_time_diff: int | None = None,
        ignore_max_val_diff: Number | None = None,
    ) -> bool: ...

    @overload
    def alter(
        self: AsyncClientProtocol,
        key: KeyT,
        retention_msecs: int | None = None,
        labels: Dict[str, str] | None = None,
        chunk_size: int | None = None,
        duplicate_policy: str | None = None,
        ignore_max_time_diff: int | None = None,
        ignore_max_val_diff: Number | None = None,
    ) -> Awaitable[bool]: ...

    def alter(
        self,
        key: KeyT,
        retention_msecs: int | None = None,
        labels: Dict[str, str] | None = None,
        chunk_size: int | None = None,
        duplicate_policy: str | None = None,
        ignore_max_time_diff: int | None = None,
        ignore_max_val_diff: Number | None = None,
    ) -> bool | Awaitable[bool]:
        """
        Update an existing time series.

        For more information see https://redis.io/commands/ts.alter/

        Args:
            key:
                The time-series key.
            retention_msecs:
                Maximum age for samples, compared to the highest reported timestamp in
                milliseconds. If `None` or `0` is passed, the series is not trimmed at
                all.
            labels:
                A dictionary of label-value pairs that represent metadata labels of the
                key. Labels not present in the dictionary are removed; an empty
                dictionary removes all labels.
            chunk_size:
                Memory size, in bytes, allocated for each data chunk. Must be a multiple
                of 8 in the range `[48..1048576]`. In earlier versions of the module the
                minimum value was different. Changing this value does not affect
                existing chunks.
            duplicate_policy:
                Policy for handling multiple samples with identical timestamps. Can be
                one of:

                - 'block': An error will occur and the new value will be ignored.
                - 'first': Ignore the new value.
                - 'last': Override with the latest value.
                - 'min': Only override if the value is lower than the existing value.
                - 'max': Only override if the value is higher than the existing value.
                - 'sum': If a previous sample exists, add the new sample to it so
                  that the updated value is equal to (previous + new). If no
                  previous sample exists, set the updated value equal to the new
                  value.

            ignore_max_time_diff:
                A non-negative integer value, in milliseconds, that sets an ignore
                threshold for added timestamps. If the difference between the last
                timestamp and the new timestamp is lower than this threshold, the new
                entry is ignored. Only applicable if `duplicate_policy` is set to
                `last`, and if `ignore_max_val_diff` is also set. Available since
                RedisTimeSeries version 1.12.0.
            ignore_max_val_diff:
                A non-negative floating point value, that sets an ignore threshold for
                added values. If the difference between the last value and the new value
                is lower than this threshold, the new entry is ignored. Only applicable
                if `duplicate_policy` is set to `last`, and if `ignore_max_time_diff` is
                also set. Available since RedisTimeSeries version 1.12.0.
        """
        params: list[EncodableT] = [key]
        self._append_retention(params, retention_msecs)
        self._append_chunk_size(params, chunk_size)
        self._append_duplicate_policy(params, duplicate_policy)
        self._append_insertion_filters(
            params, ignore_max_time_diff, ignore_max_val_diff
        )
        self._append_labels(params, labels)

        return self.execute_command(ALTER_CMD, *params)

    @overload
    def add(
        self: SyncClientProtocol,
        key: KeyT,
        timestamp: int | str,
        value: Number | str,
        retention_msecs: int | None = None,
        uncompressed: bool | None = False,
        labels: Dict[str, str] | None = None,
        chunk_size: int | None = None,
        duplicate_policy: str | None = None,
        ignore_max_time_diff: int | None = None,
        ignore_max_val_diff: Number | None = None,
        on_duplicate: str | None = None,
    ) -> int: ...

    @overload
    def add(
        self: AsyncClientProtocol,
        key: KeyT,
        timestamp: int | str,
        value: Number | str,
        retention_msecs: int | None = None,
        uncompressed: bool | None = False,
        labels: Dict[str, str] | None = None,
        chunk_size: int | None = None,
        duplicate_policy: str | None = None,
        ignore_max_time_diff: int | None = None,
        ignore_max_val_diff: Number | None = None,
        on_duplicate: str | None = None,
    ) -> Awaitable[int]: ...

    def add(
        self,
        key: KeyT,
        timestamp: int | str,
        value: Number | str,
        retention_msecs: int | None = None,
        uncompressed: bool | None = False,
        labels: Dict[str, str] | None = None,
        chunk_size: int | None = None,
        duplicate_policy: str | None = None,
        ignore_max_time_diff: int | None = None,
        ignore_max_val_diff: Number | None = None,
        on_duplicate: str | None = None,
    ) -> int | Awaitable[int]:
        """
        Append a sample to a time series. When the specified key does not exist, a new
        time series is created.

        For more information see https://redis.io/commands/ts.add/

        Args:
            key:
                The time-series key.
            timestamp:
                Timestamp of the sample. `*` can be used for automatic timestamp (using
                the system clock).
            value:
                Numeric data value of the sample.
            retention_msecs:
                Maximum age for samples, compared to the highest reported timestamp in
                milliseconds. If `None` or `0` is passed, the series is not trimmed at
                all.
            uncompressed:
                Changes data storage from compressed (default) to uncompressed.
            labels:
                A dictionary of label-value pairs that represent metadata labels of the
                key.
            chunk_size:
                Memory size, in bytes, allocated for each data chunk. Must be a multiple
                of 8 in the range `[48..1048576]`. In earlier versions of the module the
                minimum value was different.
            duplicate_policy:
                Policy for handling multiple samples with identical timestamps. Can be
                one of:

                - 'block': An error will occur and the new value will be ignored.
                - 'first': Ignore the new value.
                - 'last': Override with the latest value.
                - 'min': Only override if the value is lower than the existing value.
                - 'max': Only override if the value is higher than the existing value.
                - 'sum': If a previous sample exists, add the new sample to it so
                  that the updated value is equal to (previous + new). If no
                  previous sample exists, set the updated value equal to the new
                  value.

            ignore_max_time_diff:
                A non-negative integer value, in milliseconds, that sets an ignore
                threshold for added timestamps. If the difference between the last
                timestamp and the new timestamp is lower than this threshold, the new
                entry is ignored. Only applicable if `duplicate_policy` is set to
                `last`, and if `ignore_max_val_diff` is also set. Available since
                RedisTimeSeries version 1.12.0.
            ignore_max_val_diff:
                A non-negative floating point value, that sets an ignore threshold for
                added values. If the difference between the last value and the new value
                is lower than this threshold, the new entry is ignored. Only applicable
                if `duplicate_policy` is set to `last`, and if `ignore_max_time_diff` is
                also set. Available since RedisTimeSeries version 1.12.0.
            on_duplicate:
                Use a specific duplicate policy for the specified timestamp. Overrides
                the duplicate policy set by `duplicate_policy`.
        """
        params: list[EncodableT] = [key, timestamp, value]
        self._append_retention(params, retention_msecs)
        self._append_uncompressed(params, uncompressed)
        self._append_chunk_size(params, chunk_size)
        self._append_duplicate_policy(params, duplicate_policy)
        self._append_insertion_filters(
            params, ignore_max_time_diff, ignore_max_val_diff
        )
        self._append_on_duplicate(params, on_duplicate)
        self._append_labels(params, labels)

        return self.execute_command(ADD_CMD, *params)

    @overload
    def madd(
        self: SyncClientProtocol,
        ktv_tuples: List[Tuple[KeyT, int | str, Number | str]],
    ) -> list[int]: ...

    @overload
    def madd(
        self: AsyncClientProtocol,
        ktv_tuples: List[Tuple[KeyT, int | str, Number | str]],
    ) -> Awaitable[list[int]]: ...

    def madd(
        self, ktv_tuples: List[Tuple[KeyT, int | str, Number | str]]
    ) -> list[int] | Awaitable[list[int]]:
        """
        Append new samples to one or more time series.

        Each time series must already exist.

        The method expects a list of tuples. Each tuple should contain three elements:
        (`key`, `timestamp`, `value`). The `value` will be appended to the time series
        identified by 'key', at the given 'timestamp'.

        For more information see https://redis.io/commands/ts.madd/

        Args:
            ktv_tuples:
                A list of tuples, where each tuple contains:
                    - `key`: The key of the time series.
                    - `timestamp`: The timestamp at which the value should be appended.
                    - `value`: The value to append to the time series.

        Returns:
            A list that contains, for each sample, either the timestamp that was used,
            or an error, if the sample could not be added.
        """
        params: list[EncodableT] = []
        for ktv in ktv_tuples:
            params.extend(ktv)

        return self.execute_command(MADD_CMD, *params)

    @overload
    def incrby(
        self: SyncClientProtocol,
        key: KeyT,
        value: Number,
        timestamp: int | str | None = None,
        retention_msecs: int | None = None,
        uncompressed: bool | None = False,
        labels: Dict[str, str] | None = None,
        chunk_size: int | None = None,
        duplicate_policy: str | None = None,
        ignore_max_time_diff: int | None = None,
        ignore_max_val_diff: Number | None = None,
    ) -> int: ...

    @overload
    def incrby(
        self: AsyncClientProtocol,
        key: KeyT,
        value: Number,
        timestamp: int | str | None = None,
        retention_msecs: int | None = None,
        uncompressed: bool | None = False,
        labels: Dict[str, str] | None = None,
        chunk_size: int | None = None,
        duplicate_policy: str | None = None,
        ignore_max_time_diff: int | None = None,
        ignore_max_val_diff: Number | None = None,
    ) -> Awaitable[int]: ...

    def incrby(
        self,
        key: KeyT,
        value: Number,
        timestamp: int | str | None = None,
        retention_msecs: int | None = None,
        uncompressed: bool | None = False,
        labels: Dict[str, str] | None = None,
        chunk_size: int | None = None,
        duplicate_policy: str | None = None,
        ignore_max_time_diff: int | None = None,
        ignore_max_val_diff: Number | None = None,
    ) -> int | Awaitable[int]:
        """
        Increment the latest sample's of a series. When the specified key does not
        exist, a new time series is created.

        This command can be used as a counter or gauge that automatically gets history
        as a time series.

        For more information see https://redis.io/commands/ts.incrby/

        Args:
            key:
                The time-series key.
            value:
                Numeric value to be added (addend).
            timestamp:
                Timestamp of the sample. `*` can be used for automatic timestamp (using
                the system clock). `timestamp` must be equal to or higher than the
                maximum existing timestamp in the series. When equal, the value of the
                sample with the maximum existing timestamp is increased. If it is
                higher, a new sample with a timestamp set to `timestamp` is created, and
                its value is set to the value of the sample with the maximum existing
                timestamp plus the addend.
            retention_msecs:
                Maximum age for samples, compared to the highest reported timestamp in
                milliseconds. If `None` or `0` is passed, the series is not trimmed at
                all.
            uncompressed:
                Changes data storage from compressed (default) to uncompressed.
            labels:
                A dictionary of label-value pairs that represent metadata labels of the
                key.
            chunk_size:
                Memory size, in bytes, allocated for each data chunk. Must be a multiple
                of 8 in the range `[48..1048576]`. In earlier versions of the module the
                minimum value was different.
            duplicate_policy:
                Policy for handling multiple samples with identical timestamps. Can be
                one of:

                - 'block': An error will occur and the new value will be ignored.
                - 'first': Ignore the new value.
                - 'last': Override with the latest value.
                - 'min': Only override if the value is lower than the existing value.
                - 'max': Only override if the value is higher than the existing value.
                - 'sum': If a previous sample exists, add the new sample to it so
                  that the updated value is equal to (previous + new). If no
                  previous sample exists, set the updated value equal to the new
                  value.

            ignore_max_time_diff:
                A non-negative integer value, in milliseconds, that sets an ignore
                threshold for added timestamps. If the difference between the last
                timestamp and the new timestamp is lower than this threshold, the new
                entry is ignored. Only applicable if `duplicate_policy` is set to
                `last`, and if `ignore_max_val_diff` is also set. Available since
                RedisTimeSeries version 1.12.0.
            ignore_max_val_diff:
                A non-negative floating point value, that sets an ignore threshold for
                added values. If the difference between the last value and the new value
                is lower than this threshold, the new entry is ignored. Only applicable
                if `duplicate_policy` is set to `last`, and if `ignore_max_time_diff` is
                also set. Available since RedisTimeSeries version 1.12.0.

        Returns:
            The timestamp of the sample that was modified or added.
        """
        params: list[EncodableT] = [key, value]
        self._append_timestamp(params, timestamp)
        self._append_retention(params, retention_msecs)
        self._append_uncompressed(params, uncompressed)
        self._append_chunk_size(params, chunk_size)
        self._append_duplicate_policy(params, duplicate_policy)
        self._append_insertion_filters(
            params, ignore_max_time_diff, ignore_max_val_diff
        )
        self._append_labels(params, labels)

        return self.execute_command(INCRBY_CMD, *params)

    @overload
    def decrby(
        self: SyncClientProtocol,
        key: KeyT,
        value: Number,
        timestamp: int | str | None = None,
        retention_msecs: int | None = None,
        uncompressed: bool | None = False,
        labels: Dict[str, str] | None = None,
        chunk_size: int | None = None,
        duplicate_policy: str | None = None,
        ignore_max_time_diff: int | None = None,
        ignore_max_val_diff: Number | None = None,
    ) -> int: ...

    @overload
    def decrby(
        self: AsyncClientProtocol,
        key: KeyT,
        value: Number,
        timestamp: int | str | None = None,
        retention_msecs: int | None = None,
        uncompressed: bool | None = False,
        labels: Dict[str, str] | None = None,
        chunk_size: int | None = None,
        duplicate_policy: str | None = None,
        ignore_max_time_diff: int | None = None,
        ignore_max_val_diff: Number | None = None,
    ) -> Awaitable[int]: ...

    def decrby(
        self,
        key: KeyT,
        value: Number,
        timestamp: int | str | None = None,
        retention_msecs: int | None = None,
        uncompressed: bool | None = False,
        labels: Dict[str, str] | None = None,
        chunk_size: int | None = None,
        duplicate_policy: str | None = None,
        ignore_max_time_diff: int | None = None,
        ignore_max_val_diff: Number | None = None,
    ) -> int | Awaitable[int]:
        """
        Decrement the latest sample's of a series. When the specified key does not
        exist, a new time series is created.

        This command can be used as a counter or gauge that automatically gets history
        as a time series.

        For more information see https://redis.io/commands/ts.decrby/

        Args:
            key:
                The time-series key.
            value:
                Numeric value to subtract (subtrahend).
            timestamp:
                Timestamp of the sample. `*` can be used for automatic timestamp (using
                the system clock). `timestamp` must be equal to or higher than the
                maximum existing timestamp in the series. When equal, the value of the
                sample with the maximum existing timestamp is decreased. If it is
                higher, a new sample with a timestamp set to `timestamp` is created, and
                its value is set to the value of the sample with the maximum existing
                timestamp minus subtrahend.
            retention_msecs:
                Maximum age for samples, compared to the highest reported timestamp in
                milliseconds. If `None` or `0` is passed, the series is not trimmed at
                all.
            uncompressed:
                Changes data storage from compressed (default) to uncompressed.
            labels:
                A dictionary of label-value pairs that represent metadata labels of the
                key.
            chunk_size:
                Memory size, in bytes, allocated for each data chunk. Must be a multiple
                of 8 in the range `[48..1048576]`. In earlier versions of the module the
                minimum value was different.
            duplicate_policy:
                Policy for handling multiple samples with identical timestamps. Can be
                one of:

                - 'block': An error will occur and the new value will be ignored.
                - 'first': Ignore the new value.
                - 'last': Override with the latest value.
                - 'min': Only override if the value is lower than the existing value.
                - 'max': Only override if the value is higher than the existing value.
                - 'sum': If a previous sample exists, add the new sample to it so
                  that the updated value is equal to (previous + new). If no
                  previous sample exists, set the updated value equal to the new
                  value.

            ignore_max_time_diff:
                A non-negative integer value, in milliseconds, that sets an ignore
                threshold for added timestamps. If the difference between the last
                timestamp and the new timestamp is lower than this threshold, the new
                entry is ignored. Only applicable if `duplicate_policy` is set to
                `last`, and if `ignore_max_val_diff` is also set. Available since
                RedisTimeSeries version 1.12.0.
            ignore_max_val_diff:
                A non-negative floating point value, that sets an ignore threshold for
                added values. If the difference between the last value and the new value
                is lower than this threshold, the new entry is ignored. Only applicable
                if `duplicate_policy` is set to `last`, and if `ignore_max_time_diff` is
                also set. Available since RedisTimeSeries version 1.12.0.

        Returns:
            The timestamp of the sample that was modified or added.
        """
        params: list[EncodableT] = [key, value]
        self._append_timestamp(params, timestamp)
        self._append_retention(params, retention_msecs)
        self._append_uncompressed(params, uncompressed)
        self._append_chunk_size(params, chunk_size)
        self._append_duplicate_policy(params, duplicate_policy)
        self._append_insertion_filters(
            params, ignore_max_time_diff, ignore_max_val_diff
        )
        self._append_labels(params, labels)

        return self.execute_command(DECRBY_CMD, *params)

    @overload
    def delete(
        self: SyncClientProtocol, key: KeyT, from_time: int, to_time: int
    ) -> int: ...

    @overload
    def delete(
        self: AsyncClientProtocol, key: KeyT, from_time: int, to_time: int
    ) -> Awaitable[int]: ...

    def delete(self, key: KeyT, from_time: int, to_time: int) -> int | Awaitable[int]:
        """
        Delete all samples between two timestamps for a given time series.

        The given timestamp interval is closed (inclusive), meaning that samples whose
        timestamp equals `from_time` or `to_time` are also deleted.

        For more information see https://redis.io/commands/ts.del/

        Args:
            key:
                The time-series key.
            from_time:
                Start timestamp for the range deletion.
            to_time:
                End timestamp for the range deletion.

        Returns:
            The number of samples deleted.
        """
        return self.execute_command(DEL_CMD, key, from_time, to_time)

    @overload
    def createrule(
        self: SyncClientProtocol,
        source_key: KeyT,
        dest_key: KeyT,
        aggregation_type: str,
        bucket_size_msec: int,
        align_timestamp: int | None = None,
    ) -> bool: ...

    @overload
    def createrule(
        self: AsyncClientProtocol,
        source_key: KeyT,
        dest_key: KeyT,
        aggregation_type: str,
        bucket_size_msec: int,
        align_timestamp: int | None = None,
    ) -> Awaitable[bool]: ...

    def createrule(
        self,
        source_key: KeyT,
        dest_key: KeyT,
        aggregation_type: str,
        bucket_size_msec: int,
        align_timestamp: int | None = None,
    ) -> bool | Awaitable[bool]:
        """
        Create a compaction rule from values added to `source_key` into `dest_key`.

        For more information see https://redis.io/commands/ts.createrule/

        Args:
            source_key:
                Key name for source time series.
            dest_key:
                Key name for destination (compacted) time series.
            aggregation_type:
                Aggregation type: One of the following:
                [`avg`, `sum`, `min`, `max`, `range`, `count`, `first`, `last`, `std.p`,
                `std.s`, `var.p`, `var.s`, `twa`, 'countNaN', 'countAll']
            bucket_size_msec:
                Duration of each bucket, in milliseconds.
            align_timestamp:
                Assure that there is a bucket that starts at exactly align_timestamp and
                align all other buckets accordingly.
        """
        params: list[EncodableT] = [source_key, dest_key]
        self._append_aggregation(params, aggregation_type, bucket_size_msec)
        if align_timestamp is not None:
            params.append(align_timestamp)

        return self.execute_command(CREATERULE_CMD, *params)

    @overload
    def deleterule(
        self: SyncClientProtocol, source_key: KeyT, dest_key: KeyT
    ) -> bool: ...

    @overload
    def deleterule(
        self: AsyncClientProtocol, source_key: KeyT, dest_key: KeyT
    ) -> Awaitable[bool]: ...

    def deleterule(self, source_key: KeyT, dest_key: KeyT) -> bool | Awaitable[bool]:
        """
        Delete a compaction rule from `source_key` to `dest_key`.

        For more information see https://redis.io/commands/ts.deleterule/
        """
        return self.execute_command(DELETERULE_CMD, source_key, dest_key)

    def __range_params(
        self,
        key: KeyT,
        from_time: int | str,
        to_time: int | str,
        count: int | None,
        aggregation_type: str | list[str] | None,
        bucket_size_msec: int | None,
        filter_by_ts: List[int] | None,
        filter_by_min_value: int | None,
        filter_by_max_value: int | None,
        align: int | str | None,
        latest: bool | None,
        bucket_timestamp: str | None,
        empty: bool | None,
    ):
        """Create TS.RANGE and TS.REVRANGE arguments."""
        params: list[EncodableT] = [key, from_time, to_time]
        self._append_latest(params, latest)
        self._append_filer_by_ts(params, filter_by_ts)
        self._append_filer_by_value(params, filter_by_min_value, filter_by_max_value)
        self._append_count(params, count)
        self._append_align(params, align)
        self._append_aggregation(params, aggregation_type, bucket_size_msec)
        self._append_bucket_timestamp(params, bucket_timestamp)
        self._append_empty(params, empty)

        return params

    @overload
    def range(
        self: SyncClientProtocol,
        key: KeyT,
        from_time: int | str,
        to_time: int | str,
        count: int | None = None,
        aggregation_type: str | list[str] | None = None,
        bucket_size_msec: int | None = 0,
        filter_by_ts: List[int] | None = None,
        filter_by_min_value: int | None = None,
        filter_by_max_value: int | None = None,
        align: int | str | None = None,
        latest: bool | None = False,
        bucket_timestamp: str | None = None,
        empty: bool | None = False,
    ) -> TimeSeriesRangeResponse: ...

    @overload
    def range(
        self: AsyncClientProtocol,
        key: KeyT,
        from_time: int | str,
        to_time: int | str,
        count: int | None = None,
        aggregation_type: str | list[str] | None = None,
        bucket_size_msec: int | None = 0,
        filter_by_ts: List[int] | None = None,
        filter_by_min_value: int | None = None,
        filter_by_max_value: int | None = None,
        align: int | str | None = None,
        latest: bool | None = False,
        bucket_timestamp: str | None = None,
        empty: bool | None = False,
    ) -> Awaitable[TimeSeriesRangeResponse]: ...

    def range(
        self,
        key: KeyT,
        from_time: int | str,
        to_time: int | str,
        count: int | None = None,
        aggregation_type: str | list[str] | None = None,
        bucket_size_msec: int | None = 0,
        filter_by_ts: List[int] | None = None,
        filter_by_min_value: int | None = None,
        filter_by_max_value: int | None = None,
        align: int | str | None = None,
        latest: bool | None = False,
        bucket_timestamp: str | None = None,
        empty: bool | None = False,
    ) -> TimeSeriesRangeResponse | Awaitable[TimeSeriesRangeResponse]:
        """
        Query a range in forward direction for a specific time-series.

        For more information see https://redis.io/commands/ts.range/

        Args:
            key:
                Key name for timeseries.
            from_time:
                Start timestamp for the range query. `-` can be used to express the
                minimum possible timestamp (0).
            to_time:
                End timestamp for range query, `+` can be used to express the maximum
                possible timestamp.
            count:
                Limits the number of returned samples.
            aggregation_type:
                Optional aggregation type. Can be a single string or a list of strings
                for multiple aggregators (requires Redis 8.8+). Valid values:
                [`avg`, `sum`, `min`, `max`, `range`, `count`, `first`, `last`,
                `std.p`, `std.s`, `var.p`, `var.s`, `twa`, `countNaN`, `countAll`].
                When a list is passed, each sample in the response contains values
                in the same order as the specified aggregators.
            bucket_size_msec:
                Time bucket for aggregation in milliseconds.
            filter_by_ts:
                List of timestamps to filter the result by specific timestamps.
            filter_by_min_value:
                Filter result by minimum value (must mention also
                `filter by_max_value`).
            filter_by_max_value:
                Filter result by maximum value (must mention also
                `filter by_min_value`).
            align:
                Timestamp for alignment control for aggregation.
            latest:
                Used when a time series is a compaction, reports the compacted value of
                the latest possibly partial bucket.
            bucket_timestamp:
                Controls how bucket timestamps are reported. Can be one of [`-`, `low`,
                `+`, `high`, `~`, `mid`].
            empty:
                Reports aggregations for empty buckets.
        """
        params = self.__range_params(
            key,
            from_time,
            to_time,
            count,
            aggregation_type,
            bucket_size_msec,
            filter_by_ts,
            filter_by_min_value,
            filter_by_max_value,
            align,
            latest,
            bucket_timestamp,
            empty,
        )
        return self.execute_command(RANGE_CMD, *params, keys=[key])

    @overload
    def revrange(
        self: SyncClientProtocol,
        key: KeyT,
        from_time: int | str,
        to_time: int | str,
        count: int | None = None,
        aggregation_type: str | list[str] | None = None,
        bucket_size_msec: int | None = 0,
        filter_by_ts: List[int] | None = None,
        filter_by_min_value: int | None = None,
        filter_by_max_value: int | None = None,
        align: int | str | None = None,
        latest: bool | None = False,
        bucket_timestamp: str | None = None,
        empty: bool | None = False,
    ) -> TimeSeriesRangeResponse: ...

    @overload
    def revrange(
        self: AsyncClientProtocol,
        key: KeyT,
        from_time: int | str,
        to_time: int | str,
        count: int | None = None,
        aggregation_type: str | list[str] | None = None,
        bucket_size_msec: int | None = 0,
        filter_by_ts: List[int] | None = None,
        filter_by_min_value: int | None = None,
        filter_by_max_value: int | None = None,
        align: int | str | None = None,
        latest: bool | None = False,
        bucket_timestamp: str | None = None,
        empty: bool | None = False,
    ) -> Awaitable[TimeSeriesRangeResponse]: ...

    def revrange(
        self,
        key: KeyT,
        from_time: int | str,
        to_time: int | str,
        count: int | None = None,
        aggregation_type: str | list[str] | None = None,
        bucket_size_msec: int | None = 0,
        filter_by_ts: List[int] | None = None,
        filter_by_min_value: int | None = None,
        filter_by_max_value: int | None = None,
        align: int | str | None = None,
        latest: bool | None = False,
        bucket_timestamp: str | None = None,
        empty: bool | None = False,
    ) -> TimeSeriesRangeResponse | Awaitable[TimeSeriesRangeResponse]:
        """
        Query a range in reverse direction for a specific time-series.

        **Note**: This command is only available since RedisTimeSeries >= v1.4

        For more information see https://redis.io/commands/ts.revrange/

        Args:
            key:
                Key name for timeseries.
            from_time:
                Start timestamp for the range query. `-` can be used to express the
                minimum possible timestamp (0).
            to_time:
                End timestamp for range query, `+` can be used to express the maximum
                possible timestamp.
            count:
                Limits the number of returned samples.
            aggregation_type:
                Optional aggregation type. Can be a single string or a list of strings
                for multiple aggregators (requires Redis 8.8+). Valid values:
                [`avg`, `sum`, `min`, `max`, `range`, `count`, `first`, `last`,
                `std.p`, `std.s`, `var.p`, `var.s`, `twa`, `countNaN`, `countAll`].
                When a list is passed, each sample in the response contains values
                in the same order as the specified aggregators.
            bucket_size_msec:
                Time bucket for aggregation in milliseconds.
            filter_by_ts:
                List of timestamps to filter the result by specific timestamps.
            filter_by_min_value:
                Filter result by minimum value (must mention also
                `filter_by_max_value`).
            filter_by_max_value:
                Filter result by maximum value (must mention also
                `filter_by_min_value`).
            align:
                Timestamp for alignment control for aggregation.
            latest:
                Used when a time series is a compaction, reports the compacted value of
                the latest possibly partial bucket.
            bucket_timestamp:
                Controls how bucket timestamps are reported. Can be one of [`-`, `low`,
                `+`, `high`, `~`, `mid`].
            empty:
                Reports aggregations for empty buckets.
        """
        params = self.__range_params(
            key,
            from_time,
            to_time,
            count,
            aggregation_type,
            bucket_size_msec,
            filter_by_ts,
            filter_by_min_value,
            filter_by_max_value,
            align,
            latest,
            bucket_timestamp,
            empty,
        )
        return self.execute_command(REVRANGE_CMD, *params, keys=[key])

    @overload
    def read(
        self: SyncClientProtocol,
        key: KeyT,
        timestamp: int | str,
        block_milliseconds: int | None = None,
        block_min_count: int | None = None,
        max_count: int | None = None,
    ) -> TimeSeriesRangeResponse: ...

    @overload
    def read(
        self: AsyncClientProtocol,
        key: KeyT,
        timestamp: int | str,
        block_milliseconds: int | None = None,
        block_min_count: int | None = None,
        max_count: int | None = None,
    ) -> Awaitable[TimeSeriesRangeResponse]: ...

    def read(
        self,
        key: KeyT,
        timestamp: int | str,
        block_milliseconds: int | None = None,
        block_min_count: int | None = None,
        max_count: int | None = None,
    ) -> TimeSeriesRangeResponse | Awaitable[TimeSeriesRangeResponse]:
        """
        Read a batch of samples with timestamps at or after `timestamp`, in
        ascending timestamp order.

        Without blocking, returns immediately with whatever qualifies (possibly an
        empty list). With the `block_milliseconds` group, waits until at least
        `block_min_count` qualifying samples exist or until the timeout elapses.
        This allows consuming historical and newly-appended samples continuously,
        in batches, without polling `TS.RANGE`.

        For more information see https://redis.io/commands/ts.read/

        Args:
            key:
                Key name for the time series (a regular series or a compaction
                destination).
            timestamp:
                Inclusive cursor. Samples with `timestamp >= timestamp` qualify.
                A non-negative integer (Unix milliseconds, `0` reads from the
                beginning) or one of the sentinels `-` (earliest), `+` (latest
                existing sample, inclusive) or `$` (only samples added after the
                command is received). Sentinels are sent to the server as-is and
                resolved server-side; `$` is only meaningful together with
                `block_milliseconds`, since nothing can qualify at execution time
                without blocking.
            block_milliseconds:
                Opt into blocking. Maximum time to wait, in whole milliseconds;
                a non-negative integer where `0` means wait indefinitely. When
                `None` (default) the command does not block.
            block_min_count:
                The unblock threshold: the call returns once this many samples
                qualify. A positive integer, defaulting to `1` when blocking is
                requested. Only used when `block_milliseconds` is set; the value is
                always emitted on the wire inside the BLOCK group.
            max_count:
                Reply cap, a positive integer. When more samples qualify than
                `max_count`, the oldest `max_count` are returned so callers can page
                forward. `None` (default) means unlimited.

        Returns:
            A list of `[timestamp, value]` samples in ascending timestamp order.
            An empty list is a successful reply (returned when nothing qualifies,
            or when a blocking call times out with nothing available).

        .. warning::
            A blocking call keeps the connection parked for up to
            `block_milliseconds`. The client's `socket_timeout` still applies: with
            the default (5 seconds) a longer block raises `TimeoutError` before the
            server replies. When using `block_milliseconds`, configure the client
            with a `socket_timeout` larger than the block window (or `None`), as with
            other blocking commands. This command must not be retried automatically
            after an empty or partial reply.
        """
        params: list[EncodableT] = [key, timestamp]
        self._append_block(params, block_milliseconds, block_min_count)
        self._append_max_count(params, max_count)

        return self.execute_command(READ_CMD, *params, keys=[key])

    def __n_range_params(
        self,
        keys: List[KeyT],
        from_time: int | str,
        to_time: int | str,
        count: int | None,
        aggregators: str | list[str] | None,
        bucket_size_msec: int | None,
        filter_by_ts: List[int] | None,
        filter_by_min_value: int | None,
        filter_by_max_value: int | None,
        align: int | str | None,
        latest: bool | None,
        bucket_timestamp: str | None,
        empty: bool | None,
    ):
        """Create TS.NRANGE and TS.NREVRANGE arguments."""
        if not keys:
            raise DataError("At least one key must be provided.")
        # numkeys is always derived from the key list and precedes the keys;
        # key order and duplicates are preserved and map to output columns.
        params: list[EncodableT] = [len(keys), *keys, from_time, to_time]
        self._append_latest(params, latest)
        self._append_filer_by_ts(params, filter_by_ts)
        self._append_filer_by_value(params, filter_by_min_value, filter_by_max_value)
        self._append_count(params, count)
        self._append_align(params, align)
        self._append_n_aggregation(params, aggregators, bucket_size_msec, len(keys))
        self._append_bucket_timestamp(params, bucket_timestamp)
        self._append_empty(params, empty)

        return params

    @overload
    def nrange(
        self: SyncClientProtocol,
        keys: List[KeyT],
        from_time: int | str,
        to_time: int | str,
        count: int | None = None,
        aggregators: str | list[str] | None = None,
        bucket_size_msec: int | None = 0,
        filter_by_ts: List[int] | None = None,
        filter_by_min_value: int | None = None,
        filter_by_max_value: int | None = None,
        align: int | str | None = None,
        latest: bool | None = False,
        bucket_timestamp: str | None = None,
        empty: bool | None = False,
    ) -> TimeSeriesNRangeResponse: ...

    @overload
    def nrange(
        self: AsyncClientProtocol,
        keys: List[KeyT],
        from_time: int | str,
        to_time: int | str,
        count: int | None = None,
        aggregators: str | list[str] | None = None,
        bucket_size_msec: int | None = 0,
        filter_by_ts: List[int] | None = None,
        filter_by_min_value: int | None = None,
        filter_by_max_value: int | None = None,
        align: int | str | None = None,
        latest: bool | None = False,
        bucket_timestamp: str | None = None,
        empty: bool | None = False,
    ) -> Awaitable[TimeSeriesNRangeResponse]: ...

    def nrange(
        self,
        keys: List[KeyT],
        from_time: int | str,
        to_time: int | str,
        count: int | None = None,
        aggregators: str | list[str] | None = None,
        bucket_size_msec: int | None = 0,
        filter_by_ts: List[int] | None = None,
        filter_by_min_value: int | None = None,
        filter_by_max_value: int | None = None,
        align: int | str | None = None,
        latest: bool | None = False,
        bucket_timestamp: str | None = None,
        empty: bool | None = False,
    ) -> TimeSeriesNRangeResponse | Awaitable[TimeSeriesNRangeResponse]:
        """
        Query an explicit list of time-series over a range in forward direction
        and return timestamp-major rows ordered by increasing timestamp.

        Each returned row is ``[timestamp, [value, ...]]``. With no aggregation
        -- or a single-aggregator spec per key -- the value array holds exactly
        one value per queried key, in the input ``keys`` order. When a per-key
        AGGREGATION spec lists several aggregators (e.g. ``"avg,max"``), that key
        contributes one value per aggregator in the order the aggregators were
        listed, and the row's value array is those per-key blocks concatenated in
        ``keys`` order. For example ``keys=[a, b]`` with
        ``aggregators=["avg,max", "sum"]`` yields ``[a_avg, a_max, b_sum]`` for
        every row. A key (or aggregator column) with no sample or aggregation
        bucket at a row's timestamp is reported as ``NaN``, which is
        indistinguishable from a stored or aggregated ``NaN``.

        All keys must hash to the same slot when used against a cluster; this
        command is routed as a single-shard, key command and is never split
        across shards.

        For more information see https://redis.io/commands/ts.nrange/

        Args:
            keys:
                Explicit list of time-series keys. Order and duplicate keys are
                significant: the output column order maps to the input key
                order, and duplicate keys produce repeated value columns.
            from_time:
                Start timestamp for the range query. `-` can be used to express
                the minimum possible timestamp (0).
            to_time:
                End timestamp for range query, `+` can be used to express the
                maximum possible timestamp.
            count:
                Limits the number of returned rows, applied after the
                merge in increasing-timestamp order.
            aggregators:
                Optional aggregation, with exactly one spec token per key (a
                single aggregator is never broadcast across keys). Each per-key
                spec is a string and may list several comma-separated
                aggregators, e.g. ``["avg,max", "sum"]`` builds
                ``AGGREGATION avg,max sum`` for two keys. A bare string is the
                spec for a single-key query; a number of specs that differs from
                ``keys`` raises ``DataError``. Valid aggregators: [`avg`, `sum`,
                `min`, `max`, `range`, `count`, `first`, `last`, `std.p`,
                `std.s`, `var.p`, `var.s`, `twa`, `countNaN`, `countAll`].
            bucket_size_msec:
                Time bucket for aggregation in milliseconds. Shared by all keys.
            filter_by_ts:
                List of timestamps to keep before merging.
            filter_by_min_value:
                Keep only values greater than or equal to this value before
                merging (must also set `filter_by_max_value`).
            filter_by_max_value:
                Keep only values less than or equal to this value before
                merging (must also set `filter_by_min_value`).
            align:
                Timestamp for alignment control for aggregation.
            latest:
                Used when a time series is a compaction, reports the compacted
                value of the latest possibly partial bucket.
            bucket_timestamp:
                Controls how bucket timestamps are reported. Can be one of
                [`-`, `low`, `+`, `high`, `~`, `mid`].
            empty:
                Reports aggregations for empty buckets.
        """
        params = self.__n_range_params(
            keys,
            from_time,
            to_time,
            count,
            aggregators,
            bucket_size_msec,
            filter_by_ts,
            filter_by_min_value,
            filter_by_max_value,
            align,
            latest,
            bucket_timestamp,
            empty,
        )
        return self.execute_command(NRANGE_CMD, *params, keys=list(keys))

    @overload
    def nrevrange(
        self: SyncClientProtocol,
        keys: List[KeyT],
        from_time: int | str,
        to_time: int | str,
        count: int | None = None,
        aggregators: str | list[str] | None = None,
        bucket_size_msec: int | None = 0,
        filter_by_ts: List[int] | None = None,
        filter_by_min_value: int | None = None,
        filter_by_max_value: int | None = None,
        align: int | str | None = None,
        latest: bool | None = False,
        bucket_timestamp: str | None = None,
        empty: bool | None = False,
    ) -> TimeSeriesNRangeResponse: ...

    @overload
    def nrevrange(
        self: AsyncClientProtocol,
        keys: List[KeyT],
        from_time: int | str,
        to_time: int | str,
        count: int | None = None,
        aggregators: str | list[str] | None = None,
        bucket_size_msec: int | None = 0,
        filter_by_ts: List[int] | None = None,
        filter_by_min_value: int | None = None,
        filter_by_max_value: int | None = None,
        align: int | str | None = None,
        latest: bool | None = False,
        bucket_timestamp: str | None = None,
        empty: bool | None = False,
    ) -> Awaitable[TimeSeriesNRangeResponse]: ...

    def nrevrange(
        self,
        keys: List[KeyT],
        from_time: int | str,
        to_time: int | str,
        count: int | None = None,
        aggregators: str | list[str] | None = None,
        bucket_size_msec: int | None = 0,
        filter_by_ts: List[int] | None = None,
        filter_by_min_value: int | None = None,
        filter_by_max_value: int | None = None,
        align: int | str | None = None,
        latest: bool | None = False,
        bucket_timestamp: str | None = None,
        empty: bool | None = False,
    ) -> TimeSeriesNRangeResponse | Awaitable[TimeSeriesNRangeResponse]:
        """
        Query an explicit list of time-series over a range in reverse direction
        and return timestamp-major rows ordered by decreasing timestamp.

        Each returned row is ``[timestamp, [value, ...]]``. With no aggregation
        -- or a single-aggregator spec per key -- the value array holds exactly
        one value per queried key, in the input ``keys`` order. When a per-key
        AGGREGATION spec lists several aggregators (e.g. ``"avg,max"``), that key
        contributes one value per aggregator in the order the aggregators were
        listed, and the row's value array is those per-key blocks concatenated in
        ``keys`` order. For example ``keys=[a, b]`` with
        ``aggregators=["avg,max", "sum"]`` yields ``[a_avg, a_max, b_sum]`` for
        every row. A key (or aggregator column) with no sample or aggregation
        bucket at a row's timestamp is reported as ``NaN``, which is
        indistinguishable from a stored or aggregated ``NaN``.

        All keys must hash to the same slot when used against a cluster; this
        command is routed as a single-shard, key command and is never split
        across shards.

        For more information see https://redis.io/commands/ts.nrevrange/

        Args:
            keys:
                Explicit list of time-series keys. Order and duplicate keys are
                significant: the output column order maps to the input key
                order, and duplicate keys produce repeated value columns.
            from_time:
                Start timestamp for the range query. `-` can be used to express
                the minimum possible timestamp (0).
            to_time:
                End timestamp for range query, `+` can be used to express the
                maximum possible timestamp.
            count:
                Limits the number of returned rows, applied after the
                merge in decreasing-timestamp order.
            aggregators:
                Optional aggregation, with exactly one spec token per key (a
                single aggregator is never broadcast across keys). Each per-key
                spec is a string and may list several comma-separated
                aggregators, e.g. ``["avg,max", "sum"]`` builds
                ``AGGREGATION avg,max sum`` for two keys. A bare string is the
                spec for a single-key query; a number of specs that differs from
                ``keys`` raises ``DataError``. Valid aggregators: [`avg`, `sum`,
                `min`, `max`, `range`, `count`, `first`, `last`, `std.p`,
                `std.s`, `var.p`, `var.s`, `twa`, `countNaN`, `countAll`].
            bucket_size_msec:
                Time bucket for aggregation in milliseconds. Shared by all keys.
            filter_by_ts:
                List of timestamps to keep before merging.
            filter_by_min_value:
                Keep only values greater than or equal to this value before
                merging (must also set `filter_by_max_value`).
            filter_by_max_value:
                Keep only values less than or equal to this value before
                merging (must also set `filter_by_min_value`).
            align:
                Timestamp for alignment control for aggregation.
            latest:
                Used when a time series is a compaction, reports the compacted
                value of the latest possibly partial bucket.
            bucket_timestamp:
                Controls how bucket timestamps are reported. Can be one of
                [`-`, `low`, `+`, `high`, `~`, `mid`].
            empty:
                Reports aggregations for empty buckets.
        """
        params = self.__n_range_params(
            keys,
            from_time,
            to_time,
            count,
            aggregators,
            bucket_size_msec,
            filter_by_ts,
            filter_by_min_value,
            filter_by_max_value,
            align,
            latest,
            bucket_timestamp,
            empty,
        )
        return self.execute_command(NREVRANGE_CMD, *params, keys=list(keys))

    def __mrange_params(
        self,
        aggregation_type: str | list[str] | None,
        bucket_size_msec: int | None,
        count: int | None,
        filters: List[str],
        from_time: int | str,
        to_time: int | str,
        with_labels: bool | None,
        filter_by_ts: List[int] | None,
        filter_by_min_value: int | None,
        filter_by_max_value: int | None,
        groupby: str | None,
        reduce: str | None,
        select_labels: List[str] | None,
        align: int | str | None,
        latest: bool | None,
        bucket_timestamp: str | None,
        empty: bool | None,
        exclude_empty: bool | None,
    ):
        """Create TS.MRANGE and TS.MREVRANGE arguments."""
        if (
            groupby is not None
            and isinstance(aggregation_type, list)
            and len(aggregation_type) > 1
        ):
            raise DataError(
                "GROUPBY is not allowed when multiple aggregators are specified"
            )
        if exclude_empty and groupby is not None:
            raise DataError("EXCLUDEEMPTY is not allowed with GROUPBY")
        params: list[EncodableT] = [from_time, to_time]
        self._append_latest(params, latest)
        self._append_filer_by_ts(params, filter_by_ts)
        self._append_filer_by_value(params, filter_by_min_value, filter_by_max_value)
        self._append_with_labels(params, with_labels, select_labels)
        self._append_count(params, count)
        self._append_align(params, align)
        self._append_aggregation(params, aggregation_type, bucket_size_msec)
        self._append_bucket_timestamp(params, bucket_timestamp)
        self._append_empty(params, empty)
        self._append_exclude_empty(params, exclude_empty)
        params.extend(["FILTER"])
        params += filters
        self._append_groupby_reduce(params, groupby, reduce)
        return params

    @overload
    def mrange(
        self: SyncClientProtocol,
        from_time: int | str,
        to_time: int | str,
        filters: List[str],
        count: int | None = None,
        aggregation_type: str | list[str] | None = None,
        bucket_size_msec: int | None = 0,
        with_labels: bool | None = False,
        filter_by_ts: List[int] | None = None,
        filter_by_min_value: int | None = None,
        filter_by_max_value: int | None = None,
        groupby: str | None = None,
        reduce: str | None = None,
        select_labels: List[str] | None = None,
        align: int | str | None = None,
        latest: bool | None = False,
        bucket_timestamp: str | None = None,
        empty: bool | None = False,
        exclude_empty: bool | None = False,
    ) -> TimeSeriesMRangeResponse: ...

    @overload
    def mrange(
        self: AsyncClientProtocol,
        from_time: int | str,
        to_time: int | str,
        filters: List[str],
        count: int | None = None,
        aggregation_type: str | list[str] | None = None,
        bucket_size_msec: int | None = 0,
        with_labels: bool | None = False,
        filter_by_ts: List[int] | None = None,
        filter_by_min_value: int | None = None,
        filter_by_max_value: int | None = None,
        groupby: str | None = None,
        reduce: str | None = None,
        select_labels: List[str] | None = None,
        align: int | str | None = None,
        latest: bool | None = False,
        bucket_timestamp: str | None = None,
        empty: bool | None = False,
        exclude_empty: bool | None = False,
    ) -> Awaitable[TimeSeriesMRangeResponse]: ...

    def mrange(
        self,
        from_time: int | str,
        to_time: int | str,
        filters: List[str],
        count: int | None = None,
        aggregation_type: str | list[str] | None = None,
        bucket_size_msec: int | None = 0,
        with_labels: bool | None = False,
        filter_by_ts: List[int] | None = None,
        filter_by_min_value: int | None = None,
        filter_by_max_value: int | None = None,
        groupby: str | None = None,
        reduce: str | None = None,
        select_labels: List[str] | None = None,
        align: int | str | None = None,
        latest: bool | None = False,
        bucket_timestamp: str | None = None,
        empty: bool | None = False,
        exclude_empty: bool | None = False,
    ) -> TimeSeriesMRangeResponse | Awaitable[TimeSeriesMRangeResponse]:
        """
        Query a range across multiple time-series by filters in forward direction.

        For more information see https://redis.io/commands/ts.mrange/

        Args:
            from_time:
                Start timestamp for the range query. `-` can be used to express the
                minimum possible timestamp (0).
            to_time:
                End timestamp for range query, `+` can be used to express the maximum
                possible timestamp.
            filters:
                Filter to match the time-series labels.
            count:
                Limits the number of returned samples.
            aggregation_type:
                Optional aggregation type. Can be a single string or a list of strings
                for multiple aggregators (requires Redis 8.8+). Valid values:
                [`avg`, `sum`, `min`, `max`, `range`, `count`, `first`, `last`,
                `std.p`, `std.s`, `var.p`, `var.s`, `twa`, `countNaN`, `countAll`].
                When a list is passed, each sample in the response contains values
                in the same order as the specified aggregators.
                Note: GROUPBY is not allowed when multiple aggregators are specified.
            bucket_size_msec:
                Time bucket for aggregation in milliseconds.
            with_labels:
                Include in the reply all label-value pairs representing metadata labels
                of the time series.
            filter_by_ts:
                List of timestamps to filter the result by specific timestamps.
            filter_by_min_value:
                Filter result by minimum value (must mention also
                `filter_by_max_value`).
            filter_by_max_value:
                Filter result by maximum value (must mention also
                `filter_by_min_value`).
            groupby:
                Grouping by fields the results (must mention also `reduce`).
            reduce:
                Applying reducer functions on each group. Can be one of [`avg` `sum`,
                `min`, `max`, `range`, `count`, `std.p`, `std.s`, `var.p`, `var.s`].
            select_labels:
                Include in the reply only a subset of the key-value pair labels of a
                series.
            align:
                Timestamp for alignment control for aggregation.
            latest:
                Used when a time series is a compaction, reports the compacted value of
                the latest possibly partial bucket.
            bucket_timestamp:
                Controls how bucket timestamps are reported. Can be one of [`-`, `low`,
                `+`, `high`, `~`, `mid`].
            empty:
                Reports aggregations for empty buckets.
            exclude_empty:
                Omit matching series whose reported samples array is empty for the
                queried range and options. Must not be combined with `groupby`.
        """
        params = self.__mrange_params(
            aggregation_type,
            bucket_size_msec,
            count,
            filters,
            from_time,
            to_time,
            with_labels,
            filter_by_ts,
            filter_by_min_value,
            filter_by_max_value,
            groupby,
            reduce,
            select_labels,
            align,
            latest,
            bucket_timestamp,
            empty,
            exclude_empty,
        )

        return self.execute_command(
            MRANGE_CMD, *params, aggregation_type=aggregation_type
        )

    @overload
    def mrevrange(
        self: SyncClientProtocol,
        from_time: int | str,
        to_time: int | str,
        filters: List[str],
        count: int | None = None,
        aggregation_type: str | list[str] | None = None,
        bucket_size_msec: int | None = 0,
        with_labels: bool | None = False,
        filter_by_ts: List[int] | None = None,
        filter_by_min_value: int | None = None,
        filter_by_max_value: int | None = None,
        groupby: str | None = None,
        reduce: str | None = None,
        select_labels: List[str] | None = None,
        align: int | str | None = None,
        latest: bool | None = False,
        bucket_timestamp: str | None = None,
        empty: bool | None = False,
        exclude_empty: bool | None = False,
    ) -> TimeSeriesMRangeResponse: ...

    @overload
    def mrevrange(
        self: AsyncClientProtocol,
        from_time: int | str,
        to_time: int | str,
        filters: List[str],
        count: int | None = None,
        aggregation_type: str | list[str] | None = None,
        bucket_size_msec: int | None = 0,
        with_labels: bool | None = False,
        filter_by_ts: List[int] | None = None,
        filter_by_min_value: int | None = None,
        filter_by_max_value: int | None = None,
        groupby: str | None = None,
        reduce: str | None = None,
        select_labels: List[str] | None = None,
        align: int | str | None = None,
        latest: bool | None = False,
        bucket_timestamp: str | None = None,
        empty: bool | None = False,
        exclude_empty: bool | None = False,
    ) -> Awaitable[TimeSeriesMRangeResponse]: ...

    def mrevrange(
        self,
        from_time: int | str,
        to_time: int | str,
        filters: List[str],
        count: int | None = None,
        aggregation_type: str | list[str] | None = None,
        bucket_size_msec: int | None = 0,
        with_labels: bool | None = False,
        filter_by_ts: List[int] | None = None,
        filter_by_min_value: int | None = None,
        filter_by_max_value: int | None = None,
        groupby: str | None = None,
        reduce: str | None = None,
        select_labels: List[str] | None = None,
        align: int | str | None = None,
        latest: bool | None = False,
        bucket_timestamp: str | None = None,
        empty: bool | None = False,
        exclude_empty: bool | None = False,
    ) -> TimeSeriesMRangeResponse | Awaitable[TimeSeriesMRangeResponse]:
        """
        Query a range across multiple time-series by filters in reverse direction.

        For more information see https://redis.io/commands/ts.mrevrange/

        Args:
            from_time:
                Start timestamp for the range query. '-' can be used to express the
                minimum possible timestamp (0).
            to_time:
                End timestamp for range query, '+' can be used to express the maximum
                possible timestamp.
            filters:
                Filter to match the time-series labels.
            count:
                Limits the number of returned samples.
            aggregation_type:
                Optional aggregation type. Can be a single string or a list of strings
                for multiple aggregators (requires Redis 8.8+). Valid values:
                [`avg`, `sum`, `min`, `max`, `range`, `count`, `first`, `last`,
                `std.p`, `std.s`, `var.p`, `var.s`, `twa`, `countNaN`, `countAll`].
                When a list is passed, each sample in the response contains values
                in the same order as the specified aggregators.
                Note: GROUPBY is not allowed when multiple aggregators are specified.
            bucket_size_msec:
                Time bucket for aggregation in milliseconds.
            with_labels:
                Include in the reply all label-value pairs representing metadata labels
                of the time series.
            filter_by_ts:
                List of timestamps to filter the result by specific timestamps.
            filter_by_min_value:
                Filter result by minimum value (must mention also
                `filter_by_max_value`).
            filter_by_max_value:
                Filter result by maximum value (must mention also
                `filter_by_min_value`).
            groupby:
                Grouping by fields the results (must mention also `reduce`).
            reduce:
                Applying reducer functions on each group. Can be one of [`avg` `sum`,
                `min`, `max`, `range`, `count`, `std.p`, `std.s`, `var.p`, `var.s`].
            select_labels:
                Include in the reply only a subset of the key-value pair labels of a
                series.
            align:
                Timestamp for alignment control for aggregation.
            latest:
                Used when a time series is a compaction, reports the compacted value of
                the latest possibly partial bucket.
            bucket_timestamp:
                Controls how bucket timestamps are reported. Can be one of [`-`, `low`,
                `+`, `high`, `~`, `mid`].
            empty:
                Reports aggregations for empty buckets.
            exclude_empty:
                Omit matching series whose reported samples array is empty for the
                queried range and options. Must not be combined with `groupby`.
        """
        params = self.__mrange_params(
            aggregation_type,
            bucket_size_msec,
            count,
            filters,
            from_time,
            to_time,
            with_labels,
            filter_by_ts,
            filter_by_min_value,
            filter_by_max_value,
            groupby,
            reduce,
            select_labels,
            align,
            latest,
            bucket_timestamp,
            empty,
            exclude_empty,
        )

        return self.execute_command(
            MREVRANGE_CMD, *params, aggregation_type=aggregation_type
        )

    @overload
    def get(
        self: SyncClientProtocol, key: KeyT, latest: bool | None = False
    ) -> TimeSeriesSample | None: ...

    @overload
    def get(
        self: AsyncClientProtocol, key: KeyT, latest: bool | None = False
    ) -> Awaitable[TimeSeriesSample | None]: ...

    def get(self, key: KeyT, latest: bool | None = False) -> (
        TimeSeriesSample | None
    ) | Awaitable[TimeSeriesSample | None]:
        """
        Get the last sample of `key`.

        For more information see https://redis.io/commands/ts.get/

        Args:
            latest:
                Used when a time series is a compaction, reports the compacted value of
                the latest (possibly partial) bucket.
        """
        params: list[EncodableT] = [key]
        self._append_latest(params, latest)
        return self.execute_command(GET_CMD, *params, keys=[key])

    @overload
    def mget(
        self: SyncClientProtocol,
        filters: List[str],
        with_labels: bool | None = False,
        select_labels: List[str] | None = None,
        latest: bool | None = False,
    ) -> list[Any] | dict[str, list[Any]]: ...

    @overload
    def mget(
        self: AsyncClientProtocol,
        filters: List[str],
        with_labels: bool | None = False,
        select_labels: List[str] | None = None,
        latest: bool | None = False,
    ) -> Awaitable[list[Any] | dict[str, list[Any]]]: ...

    def mget(
        self,
        filters: List[str],
        with_labels: bool | None = False,
        select_labels: List[str] | None = None,
        latest: bool | None = False,
    ) -> (list[Any] | dict[str, list[Any]]) | Awaitable[
        list[Any] | dict[str, list[Any]]
    ]:
        """
        Get the last samples matching the specific `filter`.

        For more information see https://redis.io/commands/ts.mget/

        Args:
            filters:
                Filter to match the time-series labels.
            with_labels:
                Include in the reply all label-value pairs representing metadata labels
                of the time series.
            select_labels:
                Include in the reply only a subset of the key-value pair labels o the
                time series.
            latest:
                Used when a time series is a compaction, reports the compacted value of
                the latest possibly partial bucket.
        """
        params: list[EncodableT] = []
        self._append_latest(params, latest)
        self._append_with_labels(params, with_labels, select_labels)
        params.extend(["FILTER"])
        params += filters
        return self.execute_command(MGET_CMD, *params)

    @overload
    def info(self: SyncClientProtocol, key: KeyT) -> TSInfo | dict[str, Any]: ...

    @overload
    def info(
        self: AsyncClientProtocol, key: KeyT
    ) -> Awaitable[TSInfo | dict[str, Any]]: ...

    def info(self, key: KeyT) -> (TSInfo | dict[str, Any]) | Awaitable[
        TSInfo | dict[str, Any]
    ]:
        """
        Get information of `key`.

        For more information see https://redis.io/commands/ts.info/
        """
        return self.execute_command(INFO_CMD, key, keys=[key])

    @overload
    def queryindex(
        self: SyncClientProtocol, filters: List[str]
    ) -> list[bytes | str]: ...

    @overload
    def queryindex(
        self: AsyncClientProtocol, filters: List[str]
    ) -> Awaitable[list[bytes | str]]: ...

    def queryindex(
        self, filters: List[str]
    ) -> list[bytes | str] | Awaitable[list[bytes | str]]:
        """
        Get all time series keys matching the `filter` list.

        For more information see https://redis.io/commands/ts.queryindex/
        """
        return self.execute_command(QUERYINDEX_CMD, *filters)

    @overload
    def querylabels(
        self: SyncClientProtocol,
        label: str | None = None,
        filters: Iterable[str] | None = None,
    ) -> set[bytes | str]: ...

    @overload
    def querylabels(
        self: AsyncClientProtocol,
        label: str | None = None,
        filters: Iterable[str] | None = None,
    ) -> Awaitable[set[bytes | str]]: ...

    def querylabels(
        self, label: str | None = None, filters: Iterable[str] | None = None
    ) -> set[bytes | str] | Awaitable[set[bytes | str]]:
        """
        Get label metadata for the time series matching `filters`.

        The single `TS.QUERYLABELS` request is built from the supplied
        arguments:

        - When `label` is omitted (``None``), the ``LABELS`` form is issued and
          the reply is the set of all label names present on the matching (and
          readable) series, including the label names used in the filter
          itself.
        - When `label` is given, the ``VALUES`` form is issued and the reply is
          the set of all values assigned to `label` across the matching series.
          `label` is matched byte-exactly and is passed to the server without
          any normalization; a series that does not carry `label`, and a
          `label` that matches no series, contribute nothing (an empty reply,
          not an error).

        The reply is unordered and already deduplicated by the server, so it is
        returned as a Python ``set``. When `filters` is omitted (``None``), all
        indexed series are queried; filter expressions use the same language as
        `TS.QUERYINDEX` and are passed verbatim, while an explicitly empty
        collection raises a `DataError`.

        For more information see https://redis.io/commands/ts.querylabels/
        """
        if label is None:
            params: list[EncodableT] = ["LABELS"]
        else:
            params = ["VALUES", label]
        self._append_filter_expressions(params, filters)
        return self.execute_command(QUERYLABELS_CMD, *params)

    @staticmethod
    def _append_filter_expressions(
        params: list[EncodableT], filters: Iterable[str] | None
    ):
        """Append the optional FILTER clause for TS.QUERYLABELS.

        ``None`` omits ``FILTER`` entirely (the documented all-series query);
        an explicitly empty collection is a local usage error, since silently
        widening to all series would be surprising. Any iterable is accepted and
        materialized once so single-pass iterators are handled correctly.
        Expressions are passed through verbatim, without parsing, reordering, or
        normalizing.
        """
        if filters is None:
            return
        filters = list(filters)
        if not filters:
            raise DataError(
                "filters cannot be an empty collection; pass None to query "
                "all indexed series."
            )
        params.append("FILTER")
        params.extend(filters)

    @staticmethod
    def _append_uncompressed(params: list[EncodableT], uncompressed: bool | None):
        """Append UNCOMPRESSED tag to params."""
        if uncompressed:
            params.extend(["ENCODING", "UNCOMPRESSED"])

    @staticmethod
    def _append_with_labels(
        params: list[EncodableT],
        with_labels: bool | None,
        select_labels: list[str] | None,
    ):
        """Append labels behavior to params."""
        if with_labels and select_labels:
            raise DataError(
                "with_labels and select_labels cannot be provided together."
            )

        if with_labels:
            params.extend(["WITHLABELS"])
        if select_labels:
            params.extend(["SELECTED_LABELS", *select_labels])

    @staticmethod
    def _append_groupby_reduce(
        params: list[EncodableT], groupby: str | None, reduce: str | None
    ):
        """Append GROUPBY REDUCE property to params."""
        if groupby is not None and reduce is not None:
            params.extend(["GROUPBY", groupby, "REDUCE", reduce.upper()])

    @staticmethod
    def _append_retention(params: list[EncodableT], retention: int | None):
        """Append RETENTION property to params."""
        if retention is not None:
            params.extend(["RETENTION", retention])

    @staticmethod
    def _append_labels(params: list[EncodableT], labels: dict[str, str] | None):
        """Append LABELS property to params."""
        if labels is not None:
            params.append("LABELS")
            for k, v in labels.items():
                params.extend([k, v])

    @staticmethod
    def _append_count(params: list[EncodableT], count: int | None):
        """Append COUNT property to params."""
        if count is not None:
            params.extend(["COUNT", count])

    @staticmethod
    def _append_block(
        params: list[EncodableT],
        block_milliseconds: int | None,
        block_min_count: int | None,
    ):
        """Append the BLOCK group to params.

        The BLOCK group is all-or-nothing: when blocking is requested
        (`block_milliseconds` is set), both `milliseconds` and `min_count` are
        always emitted, with `min_count` defaulting to 1. There is no standalone
        MIN_COUNT keyword in this command.
        """
        if block_milliseconds is None:
            if block_min_count is not None:
                raise DataError(
                    "block_min_count requires block_milliseconds to be set; the "
                    "BLOCK group is all-or-nothing."
                )
            return
        min_count = 1 if block_min_count is None else block_min_count
        params.extend(["BLOCK", block_milliseconds, min_count])

    @staticmethod
    def _append_max_count(params: list[EncodableT], max_count: int | None):
        """Append MAX_COUNT property to params."""
        if max_count is not None:
            params.extend(["MAX_COUNT", max_count])

    @staticmethod
    def _append_timestamp(params: list[EncodableT], timestamp: int | None):
        """Append TIMESTAMP property to params."""
        if timestamp is not None:
            params.extend(["TIMESTAMP", timestamp])

    @staticmethod
    def _append_align(params: list[EncodableT], align: int | str | None):
        """Append ALIGN property to params."""
        if align is not None:
            params.extend(["ALIGN", align])

    @staticmethod
    def _append_aggregation(
        params: list[EncodableT],
        aggregation_type: str | list[str] | None,
        bucket_size_msec: int | None,
    ):
        """Append AGGREGATION property to params."""
        if aggregation_type is not None:
            if isinstance(aggregation_type, list):
                params.extend(
                    ["AGGREGATION", ",".join(aggregation_type), bucket_size_msec]
                )
            else:
                params.extend(["AGGREGATION", aggregation_type, bucket_size_msec])

    @staticmethod
    def _append_n_aggregation(
        params: list[EncodableT],
        aggregators: str | list[str] | None,
        bucket_size_msec: int | None,
        numkeys: int,
    ):
        """Append the AGGREGATION clause for TS.NRANGE / TS.NREVRANGE.

        These commands take exactly one aggregation spec token per queried key;
        a single aggregator is never broadcast across keys. A spec token may hold
        several comma-separated aggregators, so the wire form is e.g.
        ``AGGREGATION avg,max sum 1000`` for two keys -- key 0 aggregated by both
        ``avg`` and ``max``, key 1 by ``sum``. Pass one spec string per key, each
        optionally comma-joined (``["avg,max", "sum"]``); a bare string is the
        spec for a single-key query. (Matches RedisTimeSeries PR #2079, which
        replaced the earlier single comma-joined / broadcast token.)
        """
        if aggregators is None:
            return
        specs = [aggregators] if isinstance(aggregators, str) else list(aggregators)
        if len(specs) != numkeys:
            raise DataError(
                "AGGREGATION requires exactly one aggregation spec per key "
                f"(expected {numkeys}, got {len(specs)}); a spec may list "
                "multiple comma-separated aggregators."
            )
        params.extend(["AGGREGATION", *specs, bucket_size_msec])

    @staticmethod
    def _append_chunk_size(params: list[EncodableT], chunk_size: int | None):
        """Append CHUNK_SIZE property to params."""
        if chunk_size is not None:
            params.extend(["CHUNK_SIZE", chunk_size])

    @staticmethod
    def _append_duplicate_policy(
        params: list[EncodableT], duplicate_policy: str | None
    ):
        """Append DUPLICATE_POLICY property to params."""
        if duplicate_policy is not None:
            params.extend(["DUPLICATE_POLICY", duplicate_policy])

    @staticmethod
    def _append_on_duplicate(params: list[EncodableT], on_duplicate: str | None):
        """Append ON_DUPLICATE property to params."""
        if on_duplicate is not None:
            params.extend(["ON_DUPLICATE", on_duplicate])

    @staticmethod
    def _append_filer_by_ts(params: list[EncodableT], ts_list: list[int] | None):
        """Append FILTER_BY_TS property to params."""
        if ts_list is not None:
            params.extend(["FILTER_BY_TS", *ts_list])

    @staticmethod
    def _append_filer_by_value(
        params: list[EncodableT], min_value: int | None, max_value: int | None
    ):
        """Append FILTER_BY_VALUE property to params."""
        # FILTER_BY_VALUE is all-or-nothing: one bound alone cannot be sent.
        if (min_value is None) != (max_value is None):
            raise DataError(
                "filter_by_min_value and filter_by_max_value must be set together; "
                "the FILTER_BY_VALUE group is all-or-nothing."
            )

        if min_value is not None and max_value is not None:
            params.extend(["FILTER_BY_VALUE", min_value, max_value])

    @staticmethod
    def _append_latest(params: list[EncodableT], latest: bool | None):
        """Append LATEST property to params."""
        if latest:
            params.append("LATEST")

    @staticmethod
    def _append_bucket_timestamp(
        params: list[EncodableT], bucket_timestamp: str | None
    ):
        """Append BUCKET_TIMESTAMP property to params."""
        if bucket_timestamp is not None:
            params.extend(["BUCKETTIMESTAMP", bucket_timestamp])

    @staticmethod
    def _append_empty(params: list[EncodableT], empty: bool | None):
        """Append EMPTY property to params."""
        if empty:
            params.append("EMPTY")

    @staticmethod
    def _append_exclude_empty(params: list[EncodableT], exclude_empty: bool | None):
        """Append EXCLUDEEMPTY property to params."""
        if exclude_empty:
            params.append("EXCLUDEEMPTY")

    @staticmethod
    def _append_insertion_filters(
        params: list[EncodableT],
        ignore_max_time_diff: int | None = None,
        ignore_max_val_diff: Number | None = None,
    ):
        """Append insertion filters to params."""
        if (ignore_max_time_diff is None) != (ignore_max_val_diff is None):
            raise ValueError(
                "Both ignore_max_time_diff and ignore_max_val_diff must be set."
            )

        if ignore_max_time_diff is not None and ignore_max_val_diff is not None:
            params.extend(
                ["IGNORE", str(ignore_max_time_diff), str(ignore_max_val_diff)]
            )
