from redis.utils import str_if_bytes

from ..helpers import nativestr
from .utils import list_to_dict

# Mapping from RESP3 camelCase field names to the legacy snake_case
# attribute names. Used so callers can fetch the same value with either
# spelling regardless of which wire format produced the ``TSInfo``.
_FIELD_ALIASES = {
    "sourceKey": "source_key",
    "chunkCount": "chunk_count",
    "memoryUsage": "memory_usage",
    "totalSamples": "total_samples",
    "retentionTime": "retention_msecs",
    "lastTimestamp": "last_timestamp",
    "firstTimestamp": "first_timestamp",
    "maxSamplesPerChunk": "max_samples_per_chunk",
    "chunkSize": "chunk_size",
    "duplicatePolicy": "duplicate_policy",
}


class TSInfo:
    """
    Hold information and statistics on the time-series.
    Can be created using ``tsinfo`` command
    https://redis.io/docs/latest/commands/ts.info/

    Handles both RESP2 (flat list) and RESP3 (dict) responses.
    """

    rules = []
    labels = []
    sourceKey = None
    chunk_count = None
    memory_usage = None
    total_samples = None
    retention_msecs = None
    last_time_stamp = None
    first_time_stamp = None

    max_samples_per_chunk = None
    chunk_size = None
    duplicate_policy = None

    def __init__(self, args):
        """
        Hold information and statistics on the time-series.

        The supported params that can be passed as args:

        rules:
            A list of compaction rules of the time series.
        sourceKey:
            Key name for source time series in case the current series
            is a target of a rule.
        chunkCount:
            Number of Memory Chunks used for the time series.
        memoryUsage:
            Total number of bytes allocated for the time series.
        totalSamples:
            Total number of samples in the time series.
        labels:
            A list of label-value pairs that represent the metadata
            labels of the time series.
        retentionTime:
            Retention time, in milliseconds, for the time series.
        lastTimestamp:
            Last timestamp present in the time series.
        firstTimestamp:
            First timestamp present in the time series.
        maxSamplesPerChunk:
            Deprecated.
        chunkSize:
            Amount of memory, in bytes, allocated for data.
        duplicatePolicy:
            Policy that will define handling of duplicate samples.

        Can read more about on
        https://redis.io/docs/latest/develop/data-types/timeseries/configuration/#duplicate_policy
        """
        if isinstance(args, dict):
            # RESP3 wire: response is a native map (bytes keys when not decoded).
            response = {nativestr(k): v for k, v in args.items()}
            self.rules = response.get("rules") or {}
            labels = response.get("labels") or {}
            self.labels = {str_if_bytes(k): str_if_bytes(v) for k, v in labels.items()}
        else:
            # RESP2 wire: flat list of alternating key-value pairs.
            response = dict(zip(map(nativestr, args[::2]), args[1::2]))
            self.rules = response.get("rules")
            self.labels = list_to_dict(response.get("labels"))
        self.source_key = response.get("sourceKey")
        self.chunk_count = response.get("chunkCount")
        self.memory_usage = response.get("memoryUsage")
        self.total_samples = response.get("totalSamples")
        self.retention_msecs = response.get("retentionTime")
        self.last_timestamp = response.get("lastTimestamp")
        self.first_timestamp = response.get("firstTimestamp")
        if "maxSamplesPerChunk" in response:
            self.max_samples_per_chunk = response["maxSamplesPerChunk"]
            self.chunk_size = (
                self.max_samples_per_chunk * 16
            )  # backward compatible changes
        if "chunkSize" in response:
            self.chunk_size = response["chunkSize"]
        if "duplicatePolicy" in response:
            self.duplicate_policy = response["duplicatePolicy"]
            if isinstance(self.duplicate_policy, bytes):
                self.duplicate_policy = self.duplicate_policy.decode()

    def get(self, item):
        try:
            return self.__getitem__(item)
        except AttributeError:
            return None

    def __getitem__(self, item):
        return getattr(self, _FIELD_ALIASES.get(item, item))
