import json
from typing import Literal

from redis._parsers.helpers import pairs_to_dict
from redis.commands.vectorset.utils import (
    parse_vemb_result,
    parse_vemb_result_unified,
    parse_vlinks_result,
    parse_vsim_result,
)

from ..helpers import (
    apply_module_callbacks,
    get_legacy_responses,
    get_protocol_version,
)
from .commands import (
    VEMB_CMD,
    VGETATTR_CMD,
    VINFO_CMD,
    VLINKS_CMD,
    VSIM_CMD,
    VectorSetCommands,
)


class _VectorSetBase(VectorSetCommands):
    """Base class with shared initialization logic for VectorSet clients."""

    def __init__(self, client, **kwargs):
        """Initialize VectorSet client with callbacks."""
        # Set the module commands' callbacks
        _MODULE_CALLBACKS = {  # noqa: N806
            VEMB_CMD: parse_vemb_result,
            VSIM_CMD: parse_vsim_result,
            VGETATTR_CMD: lambda r: r and json.loads(r) or None,
        }

        self._RESP2_MODULE_CALLBACKS = {
            VINFO_CMD: lambda r: r and pairs_to_dict(r) or None,
            VLINKS_CMD: parse_vlinks_result,
        }
        self._RESP3_MODULE_CALLBACKS = {}
        self._RESP2_UNIFIED_MODULE_CALLBACKS = {
            **self._RESP2_MODULE_CALLBACKS,
            VEMB_CMD: parse_vemb_result_unified,
        }
        self._RESP3_UNIFIED_MODULE_CALLBACKS = dict(self._RESP3_MODULE_CALLBACKS)
        self._RESP3_TO_RESP2_LEGACY_MODULE_CALLBACKS = {}

        self.client = client
        self.execute_command = client.execute_command

        self._MODULE_CALLBACKS = apply_module_callbacks(
            get_protocol_version(self.client),
            get_legacy_responses(self.client),
            common=_MODULE_CALLBACKS,
            resp2=self._RESP2_MODULE_CALLBACKS,
            resp3=self._RESP3_MODULE_CALLBACKS,
            resp2_unified=self._RESP2_UNIFIED_MODULE_CALLBACKS,
            resp3_unified=self._RESP3_UNIFIED_MODULE_CALLBACKS,
            resp3_to_resp2_legacy=self._RESP3_TO_RESP2_LEGACY_MODULE_CALLBACKS,
        )

        for k, v in self._MODULE_CALLBACKS.items():
            self.client.set_response_callback(k, v)


class VectorSet(_VectorSetBase):
    """Sync VectorSet client."""

    _is_async_client: Literal[False] = False


class AsyncVectorSet(_VectorSetBase):
    """Async VectorSet client.

    Note: Inherits from _VectorSetBase (not VectorSet) to maintain proper
    type discrimination. If AsyncVectorSet inherited from VectorSet, the
    type system would see it as a subtype of SyncClientProtocol, causing
    @overload resolution to incorrectly infer sync return types.
    """

    _is_async_client: Literal[True] = True
