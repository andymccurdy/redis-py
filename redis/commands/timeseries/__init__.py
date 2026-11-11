from typing import Literal

import redis
from redis._parsers.helpers import bool_ok

from ..helpers import (
    apply_module_callbacks,
    get_legacy_responses,
    get_protocol_version,
    parse_to_list,
)
from .commands import (
    ALTER_CMD,
    CREATE_CMD,
    CREATERULE_CMD,
    DEL_CMD,
    DELETERULE_CMD,
    GET_CMD,
    INFO_CMD,
    MGET_CMD,
    MRANGE_CMD,
    MREVRANGE_CMD,
    NRANGE_CMD,
    NREVRANGE_CMD,
    QUERYINDEX_CMD,
    QUERYLABELS_CMD,
    RANGE_CMD,
    READ_CMD,
    REVRANGE_CMD,
    TimeSeriesCommands,
)
from .info import TSInfo, parse_info_unified
from .utils import (
    parse_get,
    parse_get_unified,
    parse_m_get,
    parse_m_get_resp3_to_resp2_legacy,
    parse_m_get_unified,
    parse_m_range,
    parse_m_range_resp3_to_resp2_legacy,
    parse_m_range_unified,
    parse_n_range,
    parse_query_labels,
    parse_range,
    parse_range_unified,
)


class _TimeSeriesBase(TimeSeriesCommands):
    """
    This class subclasses redis-py's `Redis` and implements RedisTimeSeries's
    commands (prefixed with "ts").
    The client allows to interact with RedisTimeSeries and use all of it's
    functionality.
    """

    def __init__(self, client=None, **kwargs):
        """Create a new RedisTimeSeries client."""
        # Set the module commands' callbacks
        _MODULE_CALLBACKS = {
            ALTER_CMD: bool_ok,
            CREATE_CMD: bool_ok,
            CREATERULE_CMD: bool_ok,
            DELETERULE_CMD: bool_ok,
            READ_CMD: parse_range_unified,
            NRANGE_CMD: parse_n_range,
            NREVRANGE_CMD: parse_n_range,
            QUERYLABELS_CMD: parse_query_labels,
        }

        _RESP2_MODULE_CALLBACKS = {
            DEL_CMD: int,
            GET_CMD: parse_get,
            INFO_CMD: TSInfo,
            MGET_CMD: parse_m_get,
            MRANGE_CMD: parse_m_range,
            MREVRANGE_CMD: parse_m_range,
            QUERYINDEX_CMD: parse_to_list,
            RANGE_CMD: parse_range,
            REVRANGE_CMD: parse_range,
        }
        _RESP3_MODULE_CALLBACKS = {}
        _RESP2_UNIFIED_MODULE_CALLBACKS = {
            DEL_CMD: int,
            GET_CMD: parse_get_unified,
            INFO_CMD: parse_info_unified,
            MGET_CMD: parse_m_get_unified,
            MRANGE_CMD: parse_m_range_unified,
            MREVRANGE_CMD: parse_m_range_unified,
            RANGE_CMD: parse_range_unified,
            READ_CMD: parse_range_unified,
            REVRANGE_CMD: parse_range_unified,
        }
        _RESP3_UNIFIED_MODULE_CALLBACKS = {
            DEL_CMD: int,
            GET_CMD: parse_get_unified,
            INFO_CMD: parse_info_unified,
            MGET_CMD: parse_m_get_unified,
            MRANGE_CMD: parse_m_range_unified,
            MREVRANGE_CMD: parse_m_range_unified,
            RANGE_CMD: parse_range_unified,
            REVRANGE_CMD: parse_range_unified,
        }
        _RESP3_TO_RESP2_LEGACY_MODULE_CALLBACKS = {
            DEL_CMD: int,
            GET_CMD: parse_get,
            INFO_CMD: TSInfo,
            MGET_CMD: parse_m_get_resp3_to_resp2_legacy,
            MRANGE_CMD: parse_m_range_resp3_to_resp2_legacy,
            MREVRANGE_CMD: parse_m_range_resp3_to_resp2_legacy,
            QUERYINDEX_CMD: parse_to_list,
            RANGE_CMD: parse_range,
            REVRANGE_CMD: parse_range,
        }

        self.client = client
        self.execute_command = client.execute_command

        self._MODULE_CALLBACKS = apply_module_callbacks(
            get_protocol_version(self.client),
            get_legacy_responses(self.client),
            common=_MODULE_CALLBACKS,
            resp2=_RESP2_MODULE_CALLBACKS,
            resp3=_RESP3_MODULE_CALLBACKS,
            resp2_unified=_RESP2_UNIFIED_MODULE_CALLBACKS,
            resp3_unified=_RESP3_UNIFIED_MODULE_CALLBACKS,
            resp3_to_resp2_legacy=_RESP3_TO_RESP2_LEGACY_MODULE_CALLBACKS,
        )

        for k, v in self._MODULE_CALLBACKS.items():
            self.client.set_response_callback(k, v)

    def pipeline(self, transaction=True, shard_hint=None):
        """Creates a pipeline for the TimeSeries module, that can be used
        for executing only TimeSeries commands and core commands.

        Usage example:

        r = redis.Redis()
        pipe = r.ts().pipeline()
        for i in range(100):
            pipeline.add("with_pipeline", i, 1.1 * i)
        pipeline.execute()

        """
        if isinstance(self.client, redis.RedisCluster):
            p = ClusterPipeline(
                nodes_manager=self.client.nodes_manager,
                commands_parser=self.client.commands_parser,
                startup_nodes=self.client.nodes_manager.startup_nodes,
                result_callbacks=self.client.result_callbacks,
                cluster_response_callbacks=self.client.cluster_response_callbacks,
                cluster_error_retry_attempts=self.client.retry.get_retries(),
                read_from_replicas=self.client.read_from_replicas,
                reinitialize_steps=self.client.reinitialize_steps,
                lock=self.client._lock,
            )

        else:
            p = Pipeline(
                connection_pool=self.client.connection_pool,
                response_callbacks=self._MODULE_CALLBACKS,
                transaction=transaction,
                shard_hint=shard_hint,
            )
        return p


class ClusterPipeline(TimeSeriesCommands, redis.cluster.ClusterPipeline):
    """Cluster pipeline for the module."""


class Pipeline(TimeSeriesCommands, redis.client.Pipeline):
    """Pipeline for the module."""


class TimeSeries(_TimeSeriesBase):
    _is_async_client: Literal[False] = False


class AsyncTimeSeries(_TimeSeriesBase):
    _is_async_client: Literal[True] = True
