import asyncio
import os
from json import JSONDecodeError, JSONDecoder, JSONEncoder, loads
from typing import Literal

import redis

from ..helpers import (
    apply_module_callbacks,
    get_legacy_responses,
    get_protocol_version,
    nativestr,
)
from .commands import FPHAType, JSONCommands
from .decoders import bulk_of_jsons, decode_list


class _JSONBase(JSONCommands):
    """
    Create a client for talking to json.

    :param decoder:
    :type json.JSONDecoder: An instance of json.JSONDecoder

    :param encoder:
    :type json.JSONEncoder: An instance of json.JSONEncoder
    """

    def __init__(
        self, client, version=None, decoder=JSONDecoder(), encoder=JSONEncoder()
    ):
        """
        Create a client for talking to json.

        :param decoder:
        :type json.JSONDecoder: An instance of json.JSONDecoder

        :param encoder:
        :type json.JSONEncoder: An instance of json.JSONEncoder
        """
        # Set the module commands' callbacks
        _MODULE_CALLBACKS = {
            "JSON.ARRPOP": self._decode,
            "JSON.DEBUG": self._decode,
            "JSON.GET": self._decode,
            "JSON.MERGE": lambda r: r and nativestr(r) == "OK",
            "JSON.MGET": bulk_of_jsons(self._decode),
            "JSON.MSET": lambda r: r and nativestr(r) == "OK",
            "JSON.RESP": self._decode,
            "JSON.SET": lambda r: r and nativestr(r) == "OK",
            "JSON.TOGGLE": self._decode,
        }

        _RESP2_MODULE_CALLBACKS = {
            "JSON.ARRAPPEND": self._decode,
            "JSON.ARRINDEX": self._decode,
            "JSON.ARRINSERT": self._decode,
            "JSON.ARRLEN": self._decode,
            "JSON.ARRTRIM": self._decode,
            "JSON.CLEAR": int,
            "JSON.DEL": int,
            "JSON.FORGET": int,
            "JSON.GET": self._decode,
            "JSON.NUMINCRBY": lambda r, **kwargs: self._decode(r),
            "JSON.NUMMULTBY": lambda r, **kwargs: self._decode(r),
            "JSON.OBJKEYS": self._decode,
            "JSON.STRAPPEND": self._decode,
            "JSON.OBJLEN": self._decode,
            "JSON.STRLEN": self._decode,
            "JSON.TOGGLE": self._decode,
        }

        _RESP3_MODULE_CALLBACKS = {}
        # RESP2 wire normalised to the unified shape from the reverted
        # unification PR: NUMINCRBY/NUMMULTBY are always arrays,
        # JSON.RESP uses native floats, and missing JSON.TYPE keys stay
        # ``None`` instead of becoming ``[None]``.
        _RESP2_UNIFIED_MODULE_CALLBACKS = {
            "JSON.CLEAR": int,
            "JSON.DEL": int,
            "JSON.FORGET": int,
            "JSON.NUMINCRBY": self._decode_json_numop,
            "JSON.NUMMULTBY": self._decode_json_numop,
            "JSON.RESP": self._decode_resp_command_unified,
            "JSON.TYPE": lambda r: [r] if r is not None else r,
        }
        _RESP3_UNIFIED_MODULE_CALLBACKS = {
            "JSON.TYPE": lambda r: None if r == [None] else r,
        }
        # RESP3 wire normalised back to today's RESP2 Python shapes:
        # keep ``nativestr`` for OBJKEYS, unwrap JSON.TYPE one level so
        # legacy paths return scalars and missing keys return ``None``,
        # and re-encode native floats inside JSON.RESP back to string
        # form. NUMINCRBY/NUMMULTBY use the command path captured at the
        # call site to unwrap legacy paths while preserving JSONPath arrays.
        _RESP3_TO_RESP2_LEGACY_MODULE_CALLBACKS = {
            "JSON.NUMINCRBY": self._decode_resp3_legacy_numop,
            "JSON.NUMMULTBY": self._decode_resp3_legacy_numop,
            "JSON.OBJKEYS": self._decode,
            "JSON.RESP": self._resp_floats_to_str_command,
            "JSON.TYPE": lambda r: r[0] if isinstance(r, list) and len(r) == 1 else r,
        }

        self.client = client
        self.execute_command = client.execute_command
        self.MODULE_VERSION = version

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

        for key, value in self._MODULE_CALLBACKS.items():
            self.client.set_response_callback(key, value)

        self.__encoder__ = encoder
        self.__decoder__ = decoder

    def _decode(self, obj):
        """Get the decoder."""
        if obj is None:
            return obj

        try:
            x = self.__decoder__.decode(obj)
            if x is None:
                raise TypeError
            return x
        except TypeError:
            try:
                return self.__decoder__.decode(obj.decode())
            except AttributeError:
                return decode_list(obj)
        except (AttributeError, JSONDecodeError):
            return decode_list(obj)

    def _decode_json_numop(self, obj, **kwargs):
        """Decode a JSON.NUMINCRBY / JSON.NUMMULTBY result and normalise
        it to the unified array form.

        RESP2 wire returns a JSON bulk string — a scalar for legacy
        paths and a JSON-encoded list for dollar paths. RESP3 wire
        returns a native list. The unified shape is always a list.
        """
        if obj is None:
            return obj
        try:
            result = self.__decoder__.decode(
                obj if isinstance(obj, str) else obj.decode()
            )
        except (AttributeError, JSONDecodeError):
            return obj
        if not isinstance(result, list):
            result = [result]
        return result

    def _decode_resp3_legacy_numop(self, obj, **kwargs):
        """Decode RESP3 JSON numeric operations back to legacy RESP2 shape.

        RedisJSON returns a RESP3 array for both legacy paths (``.foo``)
        and JSONPath paths (``$.foo``). The command builder records the
        path so default RESP3-wire legacy clients can keep v8 scalar
        results for legacy paths while leaving JSONPath results as lists.
        """
        result = self._decode(obj)
        path = kwargs.get("_json_path")
        if (
            isinstance(path, str)
            and not path.startswith("$")
            and isinstance(result, list)
            and len(result) == 1
        ):
            return result[0]
        return result

    def _decode_resp_command_unified(self, obj):
        """Decode JSON.RESP and lift string-encoded floats inside the
        nested response to native ``float`` values so the unified shape
        matches the RESP3 wire.
        """
        return self._convert_resp_floats(self._decode(obj))

    def _resp_floats_to_str_command(self, obj):
        """Decode JSON.RESP and re-encode native ``float`` values back
        to their string form so the legacy RESP2 shape is preserved
        when the wire is RESP3.
        """
        return self._resp_floats_to_str(self._decode(obj))

    @staticmethod
    def _convert_resp_floats(obj):
        """Recursively convert string-encoded JSON floats.

        RESP2 has no native double type, so JSON.RESP returns JSON floats as
        bulk strings. Any string/bytes leaf that parses as ``float`` is the
        unified representation of such a value; non-numeric strings are left
        untouched.
        """
        if isinstance(obj, list):
            return [_JSONBase._convert_resp_floats(item) for item in obj]
        if isinstance(obj, (str, bytes)):
            value = obj.decode() if isinstance(obj, bytes) else obj
            try:
                return float(value)
            except (ValueError, OverflowError):
                return obj
        return obj

    @staticmethod
    def _resp_floats_to_str(obj):
        """Recursively walk ``obj`` and convert native ``float`` values
        back to their string-encoded form. Lists are walked
        element-wise; non-float leaves are returned unchanged.
        """
        if isinstance(obj, list):
            return [_JSONBase._resp_floats_to_str(item) for item in obj]
        if isinstance(obj, float):
            return str(obj)
        return obj

    def _encode(self, obj):
        """Get the encoder."""
        return self.__encoder__.encode(obj)

    def pipeline(self, transaction=True, shard_hint=None):
        """Creates a pipeline for the JSON module, that can be used for executing
        JSON commands, as well as classic core commands.

        Usage example:

        r = redis.Redis()
        pipe = r.json().pipeline()
        pipe.jsonset('foo', '.', {'hello!': 'world'})
        pipe.jsonget('foo')
        pipe.jsonget('notakey')
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
                response_callbacks=dict(self.client.response_callbacks),
                transaction=transaction,
                shard_hint=shard_hint,
            )

        p._encode = self._encode
        p._decode = self._decode
        return p


class ClusterPipeline(JSONCommands, redis.cluster.ClusterPipeline):
    """Cluster pipeline for the module."""


class Pipeline(JSONCommands, redis.client.Pipeline):
    """Pipeline for the module."""


class JSON(_JSONBase):
    _is_async_client: Literal[False] = False


class AsyncJSON(_JSONBase):
    _is_async_client: Literal[True] = True

    async def set_file(
        self,
        name: str,
        path: str,
        file_name: str,
        nx: bool | None = False,
        xx: bool | None = False,
        decode_keys: bool | None = False,
        fpha: FPHAType | str | None = None,
    ) -> bool | None:
        """
        Set the JSON value at key ``name`` under the ``path`` to the content
        of the json file ``file_name``.

        This runs the blocking file read in a thread pool to avoid blocking
        the event loop.
        """

        def _read_file(fp: str) -> dict:
            with open(fp, "rb") as f:
                return loads(f.read())

        file_content = await asyncio.to_thread(_read_file, file_name)
        return await self.set(
            name,
            path,
            file_content,
            nx=nx,
            xx=xx,
            decode_keys=decode_keys,
            fpha=fpha,
        )

    async def set_path(
        self,
        json_path: str,
        root_folder: str,
        nx: bool | None = False,
        xx: bool | None = False,
        decode_keys: bool | None = False,
        fpha: FPHAType | str | None = None,
    ) -> dict[str, bool]:
        """
        Iterate over ``root_folder`` and set each JSON file to a value
        under ``json_path`` with the file name as the key.

        This method runs blocking filesystem operations (os.walk and file reads)
        in a thread pool to avoid blocking the event loop.
        """

        def _walk_directory(folder: str) -> list[str]:
            """Walk directory and return list of file paths (runs in thread pool)."""
            file_paths = []
            for root, dirs, files in os.walk(folder):
                for file in files:
                    file_paths.append(os.path.join(root, file))
            return file_paths

        set_files_result = {}

        # Run blocking os.walk in thread pool
        file_paths = await asyncio.to_thread(_walk_directory, root_folder)

        for file_path in file_paths:
            try:
                # TODO: rsplit(".") splits on all dots, mishandling paths
                # with dots in directories (e.g. /data/v1.2/file.json).
                # Should be rsplit(".", 1) — fix in a separate PR.
                file_name = file_path.rsplit(".")[0]
                await self.set_file(
                    file_name,
                    json_path,
                    file_path,
                    nx=nx,
                    xx=xx,
                    decode_keys=decode_keys,
                    fpha=fpha,
                )
                set_files_result[file_path] = True
            except (JSONDecodeError, UnicodeDecodeError):
                set_files_result[file_path] = False

        return set_files_result
