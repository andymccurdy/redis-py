import os
from enum import Enum
from json import JSONDecodeError, loads
from typing import Any, Awaitable, overload

from redis.exceptions import DataError
from redis.typing import AsyncClientProtocol, SyncClientProtocol
from redis.utils import deprecated_function

from ._util import JsonType
from .decoders import decode_dict_keys
from .path import Path


def _path_arg(path: str | None) -> str:
    """Return ``path`` as a command argument, using the root path for ``None``."""
    return Path.root_path() if path is None else str(path)


class FPHAType(str, Enum):
    """Floating-point type options for homogeneous array storage in JSON.SET.

    Used with the ``fpha`` parameter to force Redis to store all FP arrays
    using the specified floating-point type.
    """

    BF16 = "BF16"
    FP16 = "FP16"
    FP32 = "FP32"
    FP64 = "FP64"

    @classmethod
    def from_value(cls, value: "FPHAType | str") -> "FPHAType":
        """Convert a string or FPHAType instance to a validated FPHAType.

        Args:
            value: An ``FPHAType`` member or a case-insensitive string
                (e.g. ``"bf16"``, ``"FP32"``).

        Returns:
            The corresponding ``FPHAType`` enum member.

        Raises:
            DataError: If the string does not match any valid FPHA type.
        """
        if isinstance(value, cls):
            return value
        try:
            return cls(value.upper())
        except ValueError:
            raise DataError(
                f"Invalid FPHA type: {value}. "
                f"Must be one of {', '.join(t.value for t in cls)}"
            )


class JSONCommands:
    """json commands."""

    @overload
    def arrappend(
        self: SyncClientProtocol,
        name: str,
        path: str | None = Path.root_path(),
        *args: JsonType,
    ) -> int | list[int | None] | None: ...

    @overload
    def arrappend(
        self: AsyncClientProtocol,
        name: str,
        path: str | None = Path.root_path(),
        *args: JsonType,
    ) -> Awaitable[int | list[int | None] | None]: ...

    def arrappend(
        self, name: str, path: str | None = Path.root_path(), *args: JsonType
    ) -> (int | list[int | None] | None) | Awaitable[int | list[int | None] | None]:
        """Append the objects ``args`` to the array under the
        ``path` in key ``name``.

        For more information see `JSON.ARRAPPEND <https://redis.io/commands/json.arrappend>`_..
        """  # noqa
        pieces = [name, _path_arg(path)]
        for o in args:
            pieces.append(self._encode(o))
        return self.execute_command("JSON.ARRAPPEND", *pieces)

    @overload
    def arrindex(
        self: SyncClientProtocol,
        name: str,
        path: str,
        scalar: int,
        start: int | None = None,
        stop: int | None = None,
    ) -> int | list[int | None] | None: ...

    @overload
    def arrindex(
        self: AsyncClientProtocol,
        name: str,
        path: str,
        scalar: int,
        start: int | None = None,
        stop: int | None = None,
    ) -> Awaitable[int | list[int | None] | None]: ...

    def arrindex(
        self,
        name: str,
        path: str,
        scalar: int,
        start: int | None = None,
        stop: int | None = None,
    ) -> (int | list[int | None] | None) | Awaitable[int | list[int | None] | None]:
        """
        Return the index of ``scalar`` in the JSON array under ``path`` at key
        ``name``.

        The search can be limited using the optional inclusive ``start``
        and exclusive ``stop`` indices.

        For more information see `JSON.ARRINDEX <https://redis.io/commands/json.arrindex>`_.
        """  # noqa
        pieces = [name, str(path), self._encode(scalar)]
        if start is not None or stop is not None:
            # ``stop`` is positional, so a ``stop`` without ``start`` needs the
            # server's default start (0) in front of it.
            pieces.append(0 if start is None else start)
            if stop is not None:
                pieces.append(stop)

        return self.execute_command("JSON.ARRINDEX", *pieces, keys=[name])

    @overload
    def arrinsert(
        self: SyncClientProtocol, name: str, path: str, index: int, *args: JsonType
    ) -> int | list[int | None] | None: ...

    @overload
    def arrinsert(
        self: AsyncClientProtocol,
        name: str,
        path: str,
        index: int,
        *args: JsonType,
    ) -> Awaitable[int | list[int | None] | None]: ...

    def arrinsert(self, name: str, path: str, index: int, *args: JsonType) -> (
        int | list[int | None] | None
    ) | Awaitable[int | list[int | None] | None]:
        """Insert the objects ``args`` to the array at index ``index``
        under the ``path` in key ``name``.

        For more information see `JSON.ARRINSERT <https://redis.io/commands/json.arrinsert>`_.
        """  # noqa
        pieces = [name, str(path), index]
        for o in args:
            pieces.append(self._encode(o))
        return self.execute_command("JSON.ARRINSERT", *pieces)

    @overload
    def arrlen(
        self: SyncClientProtocol, name: str, path: str | None = Path.root_path()
    ) -> int | list[int | None] | None: ...

    @overload
    def arrlen(
        self: AsyncClientProtocol, name: str, path: str | None = Path.root_path()
    ) -> Awaitable[int | list[int | None] | None]: ...

    def arrlen(self, name: str, path: str | None = Path.root_path()) -> (
        int | list[int | None] | None
    ) | Awaitable[int | list[int | None] | None]:
        """Return the length of the array JSON value under ``path``
        at key``name``.

        For more information see `JSON.ARRLEN <https://redis.io/commands/json.arrlen>`_.
        """  # noqa
        return self.execute_command("JSON.ARRLEN", name, _path_arg(path), keys=[name])

    @overload
    def arrpop(
        self: SyncClientProtocol,
        name: str,
        path: str | None = Path.root_path(),
        index: int | None = -1,
    ) -> JsonType | str | list[Any] | None: ...

    @overload
    def arrpop(
        self: AsyncClientProtocol,
        name: str,
        path: str | None = Path.root_path(),
        index: int | None = -1,
    ) -> Awaitable[JsonType | str | list[Any] | None]: ...

    def arrpop(
        self,
        name: str,
        path: str | None = Path.root_path(),
        index: int | None = -1,
    ) -> (JsonType | str | list[Any] | None) | Awaitable[
        JsonType | str | list[Any] | None
    ]:
        """Pop the element at ``index`` in the array JSON value under
        ``path`` at key ``name``.

        ``index`` defaults to -1 (the last element). ``None`` omits it, which
        the server also treats as the last element.

        For more information see `JSON.ARRPOP <https://redis.io/commands/json.arrpop>`_.
        """  # noqa
        pieces = [name, _path_arg(path)]
        if index is not None:
            pieces.append(index)
        return self.execute_command("JSON.ARRPOP", *pieces)

    @overload
    def arrtrim(
        self: SyncClientProtocol, name: str, path: str, start: int, stop: int
    ) -> int | list[int | None] | None: ...

    @overload
    def arrtrim(
        self: AsyncClientProtocol, name: str, path: str, start: int, stop: int
    ) -> Awaitable[int | list[int | None] | None]: ...

    def arrtrim(self, name: str, path: str, start: int, stop: int) -> (
        int | list[int | None] | None
    ) | Awaitable[int | list[int | None] | None]:
        """Trim the array JSON value under ``path`` at key ``name`` to the
        inclusive range given by ``start`` and ``stop``.

        For more information see `JSON.ARRTRIM <https://redis.io/commands/json.arrtrim>`_.
        """  # noqa
        return self.execute_command("JSON.ARRTRIM", name, str(path), start, stop)

    @overload
    def type(
        self: SyncClientProtocol, name: str, path: str | None = Path.root_path()
    ) -> str | None | list[str | None] | list[list[str]]: ...

    @overload
    def type(
        self: AsyncClientProtocol, name: str, path: str | None = Path.root_path()
    ) -> Awaitable[str | None | list[str | None] | list[list[str]]]: ...

    def type(self, name: str, path: str | None = Path.root_path()) -> (
        str | None | list[str | None] | list[list[str]]
    ) | Awaitable[str | None | list[str | None] | list[list[str]]]:
        """Get the type of the JSON value under ``path`` from key ``name``.

        For more information see `JSON.TYPE <https://redis.io/commands/json.type>`_.
        """  # noqa
        return self.execute_command("JSON.TYPE", name, _path_arg(path), keys=[name])

    @overload
    def resp(
        self: SyncClientProtocol, name: str, path: str | None = Path.root_path()
    ) -> Any: ...

    @overload
    def resp(
        self: AsyncClientProtocol, name: str, path: str | None = Path.root_path()
    ) -> Awaitable[Any]: ...

    def resp(
        self, name: str, path: str | None = Path.root_path()
    ) -> Any | Awaitable[Any]:
        """Return the JSON value under ``path`` at key ``name``.

        For more information see `JSON.RESP <https://redis.io/commands/json.resp>`_.
        """  # noqa
        return self.execute_command("JSON.RESP", name, _path_arg(path), keys=[name])

    @overload
    def objkeys(
        self: SyncClientProtocol, name: str, path: str | None = Path.root_path()
    ) -> list[str] | list[list[str] | None] | None: ...

    @overload
    def objkeys(
        self: AsyncClientProtocol, name: str, path: str | None = Path.root_path()
    ) -> Awaitable[list[str] | list[list[str] | None] | None]: ...

    def objkeys(self, name: str, path: str | None = Path.root_path()) -> (
        list[str] | list[list[str] | None] | None
    ) | Awaitable[list[str] | list[list[str] | None] | None]:
        """Return the key names in the dictionary JSON value under ``path`` at
        key ``name``.

        For more information see `JSON.OBJKEYS <https://redis.io/commands/json.objkeys>`_.
        """  # noqa
        return self.execute_command("JSON.OBJKEYS", name, _path_arg(path), keys=[name])

    @overload
    def objlen(
        self: SyncClientProtocol, name: str, path: str | None = Path.root_path()
    ) -> int | list[int | None] | None: ...

    @overload
    def objlen(
        self: AsyncClientProtocol, name: str, path: str | None = Path.root_path()
    ) -> Awaitable[int | list[int | None] | None]: ...

    def objlen(self, name: str, path: str | None = Path.root_path()) -> (
        int | list[int | None] | None
    ) | Awaitable[int | list[int | None] | None]:
        """Return the length of the dictionary JSON value under ``path`` at key
        ``name``.

        For more information see `JSON.OBJLEN <https://redis.io/commands/json.objlen>`_.
        """  # noqa
        return self.execute_command("JSON.OBJLEN", name, _path_arg(path), keys=[name])

    @overload
    def numincrby(
        self: SyncClientProtocol, name: str, path: str, number: int
    ) -> int | float | list[int | float | None]: ...

    @overload
    def numincrby(
        self: AsyncClientProtocol, name: str, path: str, number: int
    ) -> Awaitable[int | float | list[int | float | None]]: ...

    def numincrby(self, name: str, path: str, number: int) -> (
        int | float | list[int | float | None]
    ) | Awaitable[int | float | list[int | float | None]]:
        """Increment the numeric (integer or floating point) JSON value under
        ``path`` at key ``name`` by the provided ``number``.

        For more information see `JSON.NUMINCRBY <https://redis.io/commands/json.numincrby>`_.
        """  # noqa
        path = str(path)
        return self.execute_command(
            "JSON.NUMINCRBY", name, path, self._encode(number), _json_path=path
        )

    @overload
    def nummultby(
        self: SyncClientProtocol, name: str, path: str, number: int
    ) -> int | float | list[int | float | None]: ...

    @overload
    def nummultby(
        self: AsyncClientProtocol, name: str, path: str, number: int
    ) -> Awaitable[int | float | list[int | float | None]]: ...

    @deprecated_function(version="4.0.0", reason="deprecated since redisjson 1.0.0")
    def nummultby(self, name: str, path: str, number: int) -> (
        int | float | list[int | float | None]
    ) | Awaitable[int | float | list[int | float | None]]:
        """Multiply the numeric (integer or floating point) JSON value under
        ``path`` at key ``name`` with the provided ``number``.

        For more information see `JSON.NUMMULTBY <https://redis.io/commands/json.nummultby>`_.
        """  # noqa
        path = str(path)
        return self.execute_command(
            "JSON.NUMMULTBY", name, path, self._encode(number), _json_path=path
        )

    @overload
    def clear(
        self: SyncClientProtocol, name: str, path: str | None = Path.root_path()
    ) -> int: ...

    @overload
    def clear(
        self: AsyncClientProtocol, name: str, path: str | None = Path.root_path()
    ) -> Awaitable[int]: ...

    def clear(
        self, name: str, path: str | None = Path.root_path()
    ) -> int | Awaitable[int]:
        """Empty arrays and objects (to have zero slots/keys without deleting the
        array/object).

        Return the count of cleared paths (ignoring non-array and non-objects
        paths).

        For more information see `JSON.CLEAR <https://redis.io/commands/json.clear>`_.
        """  # noqa
        return self.execute_command("JSON.CLEAR", name, _path_arg(path))

    @overload
    def delete(
        self: SyncClientProtocol, key: str, path: str | None = Path.root_path()
    ) -> int: ...

    @overload
    def delete(
        self: AsyncClientProtocol, key: str, path: str | None = Path.root_path()
    ) -> Awaitable[int]: ...

    def delete(
        self, key: str, path: str | None = Path.root_path()
    ) -> int | Awaitable[int]:
        """Delete the JSON value stored at key ``key`` under ``path``.

        For more information see `JSON.DEL <https://redis.io/commands/json.del>`_.
        """
        return self.execute_command("JSON.DEL", key, _path_arg(path))

    # forget is an alias for delete
    forget = delete

    @overload
    def get(
        self: SyncClientProtocol, name: str, *args, no_escape: bool | None = False
    ) -> JsonType | None: ...

    @overload
    def get(
        self: AsyncClientProtocol, name: str, *args, no_escape: bool | None = False
    ) -> Awaitable[JsonType | None]: ...

    def get(self, name: str, *args, no_escape: bool | None = False) -> (
        JsonType | None
    ) | Awaitable[JsonType | None]:
        """
        Get the object stored as a JSON value at key ``name``.

        ``args`` is zero or more paths, and defaults to root path
        ```no_escape`` is a boolean flag to add no_escape option to get
        non-ascii characters

        For more information see `JSON.GET <https://redis.io/commands/json.get>`_.
        """  # noqa
        pieces = [name]
        if no_escape:
            pieces.append("noescape")

        if len(args) == 0:
            pieces.append(Path.root_path())

        else:
            for p in args:
                pieces.append(str(p))

        # Handle case where key doesn't exist. The JSONDecoder would raise a
        # TypeError exception since it can't decode None
        try:
            return self.execute_command("JSON.GET", *pieces, keys=[name])
        except TypeError:
            return None

    @overload
    def mget(
        self: SyncClientProtocol, keys: list[str], path: str
    ) -> list[JsonType | None]: ...

    @overload
    def mget(
        self: AsyncClientProtocol, keys: list[str], path: str
    ) -> Awaitable[list[JsonType | None]]: ...

    def mget(
        self, keys: list[str], path: str
    ) -> list[JsonType | None] | Awaitable[list[JsonType | None]]:
        """
        Get the objects stored as a JSON values under ``path``. ``keys``
        is a list of one or more keys.

        For more information see `JSON.MGET <https://redis.io/commands/json.mget>`_.
        """  # noqa
        pieces = []
        pieces += keys
        pieces.append(str(path))
        return self.execute_command("JSON.MGET", *pieces, keys=keys)

    @overload
    def set(
        self: SyncClientProtocol,
        name: str,
        path: str,
        obj: JsonType,
        nx: bool | None = False,
        xx: bool | None = False,
        decode_keys: bool | None = False,
        fpha: FPHAType | str | None = None,
    ) -> bool | None: ...

    @overload
    def set(
        self: AsyncClientProtocol,
        name: str,
        path: str,
        obj: JsonType,
        nx: bool | None = False,
        xx: bool | None = False,
        decode_keys: bool | None = False,
        fpha: FPHAType | str | None = None,
    ) -> Awaitable[bool | None]: ...

    def set(
        self,
        name: str,
        path: str,
        obj: JsonType,
        nx: bool | None = False,
        xx: bool | None = False,
        decode_keys: bool | None = False,
        fpha: FPHAType | str | None = None,
    ) -> (bool | None) | Awaitable[bool | None]:
        """
        Set the JSON value at key ``name`` under the ``path`` to ``obj``.

        ``nx`` if set to True, set ``value`` only if it does not exist.
        ``xx`` if set to True, set ``value`` only if it exists.
        ``decode_keys`` If set to True, the keys of ``obj`` will be decoded
        with utf-8.
        ``fpha`` if set, forces Redis to use the specified floating-point type
        for storing all FP homogeneous arrays in ``obj``.
        Accepts a :class:`FPHAType` enum value or a string
        (``"BF16"``, ``"FP16"``, ``"FP32"``, ``"FP64"``).

        For the purpose of using this within a pipeline, this command is also
        aliased to JSON.SET.

        For more information see `JSON.SET <https://redis.io/commands/json.set>`_.
        """
        if decode_keys:
            obj = decode_dict_keys(obj)

        pieces = [name, str(path), self._encode(obj)]

        # Handle existential modifiers
        if nx and xx:
            raise Exception(
                "nx and xx are mutually exclusive: use one, the "
                "other or neither - but not both"
            )
        elif nx:
            pieces.append("NX")
        elif xx:
            pieces.append("XX")

        if fpha is not None:
            pieces.extend(["FPHA", FPHAType.from_value(fpha).value])

        return self.execute_command("JSON.SET", *pieces)

    @overload
    def mset(
        self: SyncClientProtocol, triplets: list[tuple[str, str, JsonType]]
    ) -> bool: ...

    @overload
    def mset(
        self: AsyncClientProtocol, triplets: list[tuple[str, str, JsonType]]
    ) -> Awaitable[bool]: ...

    def mset(self, triplets: list[tuple[str, str, JsonType]]) -> bool | Awaitable[bool]:
        """
        Set the JSON value at key ``name`` under the ``path`` to ``obj``
        for one or more keys.

        ``triplets`` is a list of one or more triplets of key, path, value.

        For the purpose of using this within a pipeline, this command is also
        aliased to JSON.MSET.

        For more information see `JSON.MSET <https://redis.io/commands/json.mset>`_.
        """
        pieces = []
        for triplet in triplets:
            pieces.extend([triplet[0], str(triplet[1]), self._encode(triplet[2])])
        return self.execute_command("JSON.MSET", *pieces)

    @overload
    def merge(
        self: SyncClientProtocol,
        name: str,
        path: str,
        obj: JsonType,
        decode_keys: bool | None = False,
    ) -> bool: ...

    @overload
    def merge(
        self: AsyncClientProtocol,
        name: str,
        path: str,
        obj: JsonType,
        decode_keys: bool | None = False,
    ) -> Awaitable[bool]: ...

    def merge(
        self,
        name: str,
        path: str,
        obj: JsonType,
        decode_keys: bool | None = False,
    ) -> bool | Awaitable[bool]:
        """
        Merges a given JSON value into matching paths. Consequently, JSON values
        at matching paths are updated, deleted, or expanded with new children

        ``decode_keys`` If set to True, the keys of ``obj`` will be decoded
        with utf-8.

        For more information see `JSON.MERGE <https://redis.io/commands/json.merge>`_.
        """
        if decode_keys:
            obj = decode_dict_keys(obj)

        pieces = [name, str(path), self._encode(obj)]

        return self.execute_command("JSON.MERGE", *pieces)

    @overload
    def set_file(
        self: SyncClientProtocol,
        name: str,
        path: str,
        file_name: str,
        nx: bool | None = False,
        xx: bool | None = False,
        decode_keys: bool | None = False,
        fpha: FPHAType | str | None = None,
    ) -> bool | None: ...

    @overload
    def set_file(
        self: AsyncClientProtocol,
        name: str,
        path: str,
        file_name: str,
        nx: bool | None = False,
        xx: bool | None = False,
        decode_keys: bool | None = False,
        fpha: FPHAType | str | None = None,
    ) -> Awaitable[bool | None]: ...

    def set_file(
        self,
        name: str,
        path: str,
        file_name: str,
        nx: bool | None = False,
        xx: bool | None = False,
        decode_keys: bool | None = False,
        fpha: FPHAType | str | None = None,
    ) -> (bool | None) | Awaitable[bool | None]:
        """
        Set the JSON value at key ``name`` under the ``path`` to the content
        of the json file ``file_name``.

        ``nx`` if set to True, set ``value`` only if it does not exist.
        ``xx`` if set to True, set ``value`` only if it exists.
        ``decode_keys`` If set to True, the keys of ``obj`` will be decoded
        with utf-8.
        ``fpha`` if set, forces Redis to use the specified floating-point type
        for storing all FP homogeneous arrays in the file content.
        Accepts a :class:`FPHAType` enum value or a string
        (``"BF16"``, ``"FP16"``, ``"FP32"``, ``"FP64"``).

        """

        with open(file_name) as fp:
            file_content = loads(fp.read())

        return self.set(
            name, path, file_content, nx=nx, xx=xx, decode_keys=decode_keys, fpha=fpha
        )

    @overload
    def set_path(
        self: SyncClientProtocol,
        json_path: str,
        root_folder: str,
        nx: bool | None = False,
        xx: bool | None = False,
        decode_keys: bool | None = False,
        fpha: FPHAType | str | None = None,
    ) -> dict[str, bool]: ...

    @overload
    def set_path(
        self: AsyncClientProtocol,
        json_path: str,
        root_folder: str,
        nx: bool | None = False,
        xx: bool | None = False,
        decode_keys: bool | None = False,
        fpha: FPHAType | str | None = None,
    ) -> Awaitable[dict[str, bool]]: ...

    def set_path(
        self,
        json_path: str,
        root_folder: str,
        nx: bool | None = False,
        xx: bool | None = False,
        decode_keys: bool | None = False,
        fpha: FPHAType | str | None = None,
    ) -> dict[str, bool] | Awaitable[dict[str, bool]]:
        """
        Iterate over ``root_folder`` and set each JSON file to a value
        under ``json_path`` with the file name as the key.

        ``nx`` if set to True, set ``value`` only if it does not exist.
        ``xx`` if set to True, set ``value`` only if it exists.
        ``decode_keys`` If set to True, the keys of ``obj`` will be decoded
        with utf-8.
        ``fpha`` if set, forces Redis to use the specified floating-point type
        for storing all FP homogeneous arrays in the file content.
        Accepts a :class:`FPHAType` enum value or a string
        (``"BF16"``, ``"FP16"``, ``"FP32"``, ``"FP64"``).

        """
        set_files_result = {}
        for root, dirs, files in os.walk(root_folder):
            for file in files:
                file_path = os.path.join(root, file)
                try:
                    # Strip only the extension of the final path component.
                    file_name = os.path.splitext(file_path)[0]
                    self.set_file(
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

    @overload
    def strlen(
        self: SyncClientProtocol, name: str, path: str | None = None
    ) -> int | list[int | None] | None: ...

    @overload
    def strlen(
        self: AsyncClientProtocol, name: str, path: str | None = None
    ) -> Awaitable[int | list[int | None] | None]: ...

    def strlen(self, name: str, path: str | None = None) -> (
        int | list[int | None] | None
    ) | Awaitable[int | list[int | None] | None]:
        """Return the length of the string JSON value under ``path`` at key
        ``name``.

        For more information see `JSON.STRLEN <https://redis.io/commands/json.strlen>`_.
        """  # noqa
        pieces = [name]
        if path is not None:
            pieces.append(str(path))
        return self.execute_command("JSON.STRLEN", *pieces, keys=[name])

    @overload
    def toggle(
        self: SyncClientProtocol, name: str, path: str | None = Path.root_path()
    ) -> bool | list[int | None] | None: ...

    @overload
    def toggle(
        self: AsyncClientProtocol, name: str, path: str | None = Path.root_path()
    ) -> Awaitable[bool | list[int | None] | None]: ...

    def toggle(self, name: str, path: str | None = Path.root_path()) -> (
        bool | list[int | None] | None
    ) | Awaitable[bool | list[int | None] | None]:
        """Toggle boolean value under ``path`` at key ``name``.
        returning the new value.

        For more information see `JSON.TOGGLE <https://redis.io/commands/json.toggle>`_.
        """  # noqa
        return self.execute_command("JSON.TOGGLE", name, _path_arg(path))

    @overload
    def strappend(
        self: SyncClientProtocol,
        name: str,
        value: str,
        path: str | None = Path.root_path(),
    ) -> int | list[int | None] | None: ...

    @overload
    def strappend(
        self: AsyncClientProtocol,
        name: str,
        value: str,
        path: str | None = Path.root_path(),
    ) -> Awaitable[int | list[int | None] | None]: ...

    def strappend(self, name: str, value: str, path: str | None = Path.root_path()) -> (
        int | list[int | None] | None
    ) | Awaitable[int | list[int | None] | None]:
        """Append to the string JSON value. If two options are specified after
        the key name, the path is determined to be the first. If a single
        option is passed, then the root_path (i.e Path.root_path()) is used.

        For more information see `JSON.STRAPPEND <https://redis.io/commands/json.strappend>`_.
        """  # noqa
        pieces = [name, _path_arg(path), self._encode(value)]
        return self.execute_command("JSON.STRAPPEND", *pieces)

    @overload
    def debug(
        self: SyncClientProtocol,
        subcommand: str,
        key: str | None = None,
        path: str | None = Path.root_path(),
    ) -> int | list[str]: ...

    @overload
    def debug(
        self: AsyncClientProtocol,
        subcommand: str,
        key: str | None = None,
        path: str | None = Path.root_path(),
    ) -> Awaitable[int | list[str]]: ...

    def debug(
        self,
        subcommand: str,
        key: str | None = None,
        path: str | None = Path.root_path(),
    ) -> (int | list[str]) | Awaitable[int | list[str]]:
        """Return the memory usage in bytes of a value under ``path`` from
        key ``name``.

        For more information see `JSON.DEBUG <https://redis.io/commands/json.debug>`_.
        """  # noqa
        valid_subcommands = ["MEMORY", "HELP"]
        if subcommand not in valid_subcommands:
            raise DataError("The only valid subcommands are ", str(valid_subcommands))
        pieces = [subcommand]
        if subcommand == "MEMORY":
            if key is None:
                raise DataError("No key specified")
            pieces.append(key)
            pieces.append(_path_arg(path))
        return self.execute_command("JSON.DEBUG", *pieces)

    @overload
    def jsonget(self: SyncClientProtocol, *args, **kwargs) -> JsonType | None: ...

    @overload
    def jsonget(
        self: AsyncClientProtocol, *args, **kwargs
    ) -> Awaitable[JsonType | None]: ...

    @deprecated_function(
        version="4.0.0", reason="redisjson-py supported this, call get directly."
    )
    def jsonget(self, *args, **kwargs) -> (JsonType | None) | Awaitable[
        JsonType | None
    ]:
        return self.get(*args, **kwargs)

    @overload
    def jsonmget(
        self: SyncClientProtocol, *args, **kwargs
    ) -> list[JsonType | None]: ...

    @overload
    def jsonmget(
        self: AsyncClientProtocol, *args, **kwargs
    ) -> Awaitable[list[JsonType | None]]: ...

    @deprecated_function(
        version="4.0.0", reason="redisjson-py supported this, call get directly."
    )
    def jsonmget(
        self, *args, **kwargs
    ) -> list[JsonType | None] | Awaitable[list[JsonType | None]]:
        return self.mget(*args, **kwargs)

    @overload
    def jsonset(self: SyncClientProtocol, *args, **kwargs) -> bool | None: ...

    @overload
    def jsonset(
        self: AsyncClientProtocol, *args, **kwargs
    ) -> Awaitable[bool | None]: ...

    @deprecated_function(
        version="4.0.0", reason="redisjson-py supported this, call get directly."
    )
    def jsonset(self, *args, **kwargs) -> (bool | None) | Awaitable[bool | None]:
        return self.set(*args, **kwargs)
