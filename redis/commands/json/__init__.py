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

        # Don't register JSON callbacks on the parent client — that would
        # mutate the shared response_callbacks dict and break bare
        # execute_command("JSON.GET") calls on the parent (issue #3937).
        # Each JSON sub-client decodes responses itself in execute_command().

        # When the client is a Pipeline (r.pipeline().json()), we do need
        # callbacks merged in so Pipeline.execute() can decode at batch time.
        # Always copy before merging to avoid polluting the parent's dict.
        if isinstance(client, (redis.client.Pipeline, redis.asyncio.client.Pipeline)):
            client.response_callbacks = dict(client.response_callbacks)
            client.response_callbacks.update(self._MODULE_CALLBACKS)
        elif isinstance(client, redis.cluster.ClusterPipeline):
            # Copy cluster_response_callbacks so we don't touch the parent.
            client.cluster_response_callbacks = dict(
                client.cluster_response_callbacks
            )
            client.cluster_response_callbacks.update(self._MODULE_CALLBACKS)
        elif isinstance(client, redis.asyncio.cluster.ClusterPipeline):
            # Async ClusterPipeline: never touch cluster_client.response_callbacks
            # — it is shared across all ClusterNode instances and mutating it
            # re-introduces #3937 for the async cluster path.
            #
            # Instead, wrap the strategy's execution method to post-process JSON
            # results after dispatch, using a local closure over _MODULE_CALLBACKS.
            # Only _json_path is forwarded to callbacks (mirrors the execute_command
            # path); redis-internal kwargs like keys= are stripped.
            #
            # _json_callbacks_installed guards against double-wrapping when
            # p.json() is called more than once on the same pipeline instance.
            _strategy = client._execution_strategy
            if not getattr(_strategy, "_json_callbacks_installed", False):
                _strategy._json_callbacks_installed = True
                _cbs = self._MODULE_CALLBACKS

                def _decode_results(results, stack):
                    out = []
                    for r, cmd in zip(results, stack):
                        if cmd.args and not isinstance(r, Exception):
                            cmd_name = cmd.args[0]
                            # Match the case-insensitive-lookup guard used in
                            # JSON.execute_command / AsyncJSON.execute_command:
                            # command names are always str in practice, but
                            # guard against unexpected bytes/other types
                            # rather than raising AttributeError on .upper().
                            key = (
                                cmd_name.upper()
                                if isinstance(cmd_name, str)
                                else cmd_name
                            )
                            if key in _cbs:
                                cb_kwargs = {}
                                if "_json_path" in cmd.kwargs:
                                    cb_kwargs["_json_path"] = cmd.kwargs["_json_path"]
                                r = _cbs[key](r, **cb_kwargs)
                        out.append(r)
                    return out

                if not client._transaction:
                    # Non-transactional (PipelineStrategy): results come from
                    # ClusterNode.execute_pipeline() with no JSON decoding.
                    _orig_execute = _strategy._execute

                    async def _json_decode_execute(
                        cluster_client_arg, stack,
                        raise_on_error=True, allow_redirections=True,
                    ):
                        results = await _orig_execute(
                            cluster_client_arg, stack,
                            raise_on_error, allow_redirections,
                        )
                        return _decode_results(results, stack)

                    _strategy._execute = _json_decode_execute
                else:
                    # Transactional (TransactionStrategy): results come from
                    # EXEC; non-JSON commands are already decoded but JSON
                    # commands are not (no JSON callbacks on cluster_client).
                    # Snapshot _command_queue at execute-time to match results.
                    _orig_txn_execute = _strategy.execute

                    async def _json_decode_txn_execute(
                        raise_on_error=True, allow_redirections=True,
                    ):
                        cmd_queue = list(_strategy._command_queue)
                        results = await _orig_txn_execute(
                            raise_on_error, allow_redirections,
                        )
                        return _decode_results(results, cmd_queue)

                    _strategy.execute = _json_decode_txn_execute

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
            # Merge JSON callbacks into a copy of the cluster callbacks so
            # ClusterPipeline decodes JSON.* responses correctly without
            # mutating the parent client's shared callback dict.
            cluster_callbacks = dict(self.client.cluster_response_callbacks)
            cluster_callbacks.update(self._MODULE_CALLBACKS)
            p = ClusterPipeline(
                nodes_manager=self.client.nodes_manager,
                commands_parser=self.client.commands_parser,
                startup_nodes=self.client.nodes_manager.startup_nodes,
                result_callbacks=self.client.result_callbacks,
                cluster_response_callbacks=cluster_callbacks,
                cluster_error_retry_attempts=self.client.retry.get_retries(),
                read_from_replicas=self.client.read_from_replicas,
                reinitialize_steps=self.client.reinitialize_steps,
                lock=self.client._lock,
            )

        else:
            # Merge parent callbacks with JSON module callbacks into the
            # pipeline's private copy.  This keeps the parent client clean
            # while still letting the pipeline decode JSON.* responses.
            pipeline_callbacks = dict(self.client.response_callbacks)
            pipeline_callbacks.update(self._MODULE_CALLBACKS)
            p = Pipeline(
                connection_pool=self.client.connection_pool,
                response_callbacks=pipeline_callbacks,
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

    def execute_command(self, *args, **kwargs):
        """Execute a command and apply any JSON module response callback.

        Callbacks are applied locally on this sub-client instance rather than
        being registered on the parent Redis client (see issue #3937).

        Only JSON-module kwargs (``_json_path``) are forwarded to the
        callback; redis-internal kwargs (``keys``, ``options``, etc.) are
        intentionally stripped to avoid TypeError when callbacks don't
        accept arbitrary keyword arguments.
        """
        cmd = args[0] if args else ""
        response = self.client.execute_command(*args, **kwargs)

        # FIX #1: Pipeline guard — when self.client is a Pipeline,
        # execute_command() returns the pipeline object for chaining,
        # not an actual Redis response.  Callbacks will be applied
        # later by Pipeline.execute() from its response_callbacks.
        if isinstance(
            response,
            (
                redis.client.Pipeline,
                redis.asyncio.client.Pipeline,
                redis.cluster.ClusterPipeline,
                redis.asyncio.cluster.ClusterPipeline,
            ),
        ):
            return response

        # FIX #2: Case-insensitive lookup — the original
        # response_callbacks was a CaseInsensitiveDict, so
        # j.execute_command("json.get", "k") worked.  We must
        # upper-case the command name before lookup.
        cmd_upper = cmd.upper() if isinstance(cmd, str) else cmd
        if cmd_upper in self._MODULE_CALLBACKS:
            cb_kwargs = {}
            if "_json_path" in kwargs:
                cb_kwargs["_json_path"] = kwargs["_json_path"]
            return self._MODULE_CALLBACKS[cmd_upper](response, **cb_kwargs)
        return response


class AsyncJSON(_JSONBase):
    _is_async_client: Literal[True] = True

    async def execute_command(self, *args, **kwargs):
        """Execute a command and apply any JSON module response callback.

        Async version — same logic as JSON.execute_command.
        """
        cmd = args[0] if args else ""
        response = await self.client.execute_command(*args, **kwargs)

        # Pipeline guard (same as sync path)
        if isinstance(
            response,
            (
                redis.client.Pipeline,
                redis.asyncio.client.Pipeline,
                redis.cluster.ClusterPipeline,
                redis.asyncio.cluster.ClusterPipeline,
            ),
        ):
            return response

        # Case-insensitive callback lookup (same as sync path)
        cmd_upper = cmd.upper() if isinstance(cmd, str) else cmd
        if cmd_upper in self._MODULE_CALLBACKS:
            cb_kwargs = {}
            if "_json_path" in kwargs:
                cb_kwargs["_json_path"] = kwargs["_json_path"]
            return self._MODULE_CALLBACKS[cmd_upper](response, **cb_kwargs)
        return response

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
            with open(fp) as f:
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
            except JSONDecodeError:
                set_files_result[file_path] = False

        return set_files_result
