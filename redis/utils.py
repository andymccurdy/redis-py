import datetime
import inspect
import logging
import sys
import textwrap
import warnings
from collections.abc import Callable
from contextlib import contextmanager
from functools import cache, wraps
from typing import TYPE_CHECKING, Any, Dict, List, Mapping, Optional, TypeVar, Union

from redis.exceptions import DataError
from redis.typing import AbsExpiryT, EncodableT, ExpiryT

if TYPE_CHECKING:
    from redis.client import Redis

try:
    import hiredis  # noqa

    # Only support Hiredis >= 3.0:
    hiredis_version = hiredis.__version__.split(".")
    HIREDIS_AVAILABLE = int(hiredis_version[0]) > 3 or (
        int(hiredis_version[0]) == 3 and int(hiredis_version[1]) >= 2
    )
    if not HIREDIS_AVAILABLE:
        raise ImportError("hiredis package should be >= 3.2.0")
except ImportError:
    HIREDIS_AVAILABLE = False

try:
    import ssl  # noqa

    SSL_AVAILABLE = True
except ImportError:
    SSL_AVAILABLE = False

try:
    import cryptography  # noqa

    CRYPTOGRAPHY_AVAILABLE = True
except ImportError:
    CRYPTOGRAPHY_AVAILABLE = False

from importlib import metadata

# Shared marker for omitted arguments, especially where None is a valid
# explicit value. Import this object from redis.utils instead of creating local
# sentinels, and compare it by identity only (`is` / `is not`).
SENTINEL = object()


def from_url(url: str, **kwargs: Any) -> "Redis":
    """
    Returns an active Redis client generated from the given database URL.

    Will attempt to extract the database id from the path url fragment, if
    none is provided.
    """
    from redis.client import Redis

    return Redis.from_url(url, **kwargs)


@contextmanager
def pipeline(redis_obj):
    p = redis_obj.pipeline()
    try:
        yield p
        p.execute()
    finally:
        p.reset()


def is_os_level_timeout(exc: BaseException) -> bool:
    """Tell an OS-level timeout apart from a socket/asyncio timeout.

    Since Python 3.10 ``socket.timeout`` and since 3.11 ``asyncio.TimeoutError``
    are the builtin ``TimeoutError``, an ``OSError`` subclass. A timeout the OS
    reports on a broken connection (``ETIMEDOUT``, e.g. after a network change)
    is therefore caught by the same ``except`` clauses as our own read/write
    timeouts, although it means the connection is dead. Only the OS-level error
    carries an errno.
    """
    return isinstance(exc, OSError) and exc.errno is not None


def str_if_bytes(value: Union[str, bytes]) -> str:
    return (
        value.decode("utf-8", errors="replace") if isinstance(value, bytes) else value
    )


def safe_str(value):
    return str(str_if_bytes(value))


def decode_field_value(value, key=None, field_encodings=None):
    """Decode a field value respecting optional per-field encoding overrides.

    - If *field_encodings* is provided and *key* is in it, the corresponding
      encoding is used (``None`` means keep raw bytes).
    - Otherwise falls back to :func:`str_if_bytes`.
    """
    if not isinstance(value, bytes):
        return value
    if field_encodings and key is not None and key in field_encodings:
        encoding = field_encodings[key]
        if encoding is None:
            return value
        return value.decode(encoding, "replace")
    return str_if_bytes(value)


def dict_merge(*dicts: Mapping[str, Any]) -> Dict[str, Any]:
    """
    Merge all provided dicts into 1 dict.
    *dicts : `dict`
        dictionaries to merge
    """
    merged = {}

    for d in dicts:
        merged.update(d)

    return merged


def list_keys_to_dict(key_list, callback):
    return dict.fromkeys(key_list, callback)


def merge_result(command, res):
    """
    Merge all items in `res` into a list.

    This command is used when sending a command to multiple nodes
    and the result from each node should be merged into a single list.

    res : 'dict'
    """
    result = set()

    for v in res.values():
        for value in v:
            result.add(value)

    return list(result)


def warn_deprecated(name, reason="", version="", stacklevel=2):
    import warnings

    msg = f"Call to deprecated {name}."
    if reason:
        msg += f" ({reason})"
    if version:
        msg += f" -- Deprecated since version {version}."
    warnings.warn(msg, category=DeprecationWarning, stacklevel=stacklevel)


def deprecated_function(reason="", version="", name=None):
    """
    Decorator to mark a function as deprecated.
    """

    def decorator(func):
        if inspect.iscoroutinefunction(func):
            # Create async wrapper for async functions
            @wraps(func)
            async def async_wrapper(*args, **kwargs):
                warn_deprecated(name or func.__name__, reason, version, stacklevel=3)
                return await func(*args, **kwargs)

            return async_wrapper
        else:
            # Create regular wrapper for sync functions
            @wraps(func)
            def wrapper(*args, **kwargs):
                warn_deprecated(name or func.__name__, reason, version, stacklevel=3)
                return func(*args, **kwargs)

            return wrapper

    return decorator


def warn_deprecated_arg_usage(
    arg_name: Union[list, str],
    function_name: str,
    reason: str = "",
    version: str = "",
    stacklevel: int = 2,
):
    import warnings

    msg = (
        f"Call to '{function_name}' function with deprecated"
        f" usage of input argument/s '{arg_name}'."
    )
    if reason:
        msg += f" ({reason})"
    if version:
        msg += f" -- Deprecated since version {version}."
    warnings.warn(msg, category=DeprecationWarning, stacklevel=stacklevel)


C = TypeVar("C", bound=Callable)


def _get_filterable_args(
    func: Callable, args: tuple, kwargs: dict, allowed_args: tuple = ()
) -> dict:
    """
    Extract arguments from function call that should be checked for deprecation/experimental warnings.
    Excludes 'self' and any explicitly allowed args.
    """
    arg_names = func.__code__.co_varnames[: func.__code__.co_argcount]
    filterable_args = dict(zip(arg_names, args))
    filterable_args.update(kwargs)
    filterable_args.pop("self", None)
    if allowed_args:
        for allowed_arg in allowed_args:
            filterable_args.pop(allowed_arg, None)
    return filterable_args


_DictKeys = type({}.keys())


def _get_warn_check(
    func: Callable, args_to_warn: tuple, allowed_args: tuple = ()
) -> tuple[int, Callable[[Any, frozenset], bool], frozenset]:
    """
    Build the call-time pre-check for the deprecated/experimental args decorators.
    Returns the lowest positional index whose presence may need a warning
    (sys.maxsize if none), and a dict_keys operation with the names it is
    applied to: keys_ok(kwargs.keys(), names) is True when the passed kwargs
    need no warning.
    """
    pos_names = func.__code__.co_varnames[: func.__code__.co_argcount]
    if "*" in args_to_warn:
        names = frozenset(allowed_args) | {"self"}
        min_idx = next(
            (i for i, name in enumerate(pos_names) if name not in names),
            sys.maxsize,
        )
        return min_idx, _DictKeys.__le__, names
    names = frozenset(args_to_warn) - frozenset(allowed_args) - {"self"}
    min_idx = next(
        (i for i, name in enumerate(pos_names) if name in names), sys.maxsize
    )
    return min_idx, _DictKeys.isdisjoint, names


def deprecated_args(
    args_to_warn: Optional[List[str]] = None,
    allowed_args: Optional[List[str]] = None,
    reason: str = "",
    version: str = "",
) -> Callable[[C], C]:
    """
    Decorator to mark specified args of a function as deprecated.
    If '*' is in args_to_warn, all arguments will be marked as deprecated.
    """
    warned = tuple(args_to_warn) if args_to_warn is not None else ("*",)
    allowed = tuple(allowed_args) if allowed_args is not None else ()

    def _check_deprecated_args(func, filterable_args):
        """Check and warn about deprecated arguments."""
        for arg in warned:
            if arg == "*" and len(filterable_args) > 0:
                warn_deprecated_arg_usage(
                    list(filterable_args.keys()),
                    func.__name__,
                    reason,
                    version,
                    stacklevel=5,
                )
            elif arg in filterable_args:
                warn_deprecated_arg_usage(
                    arg, func.__name__, reason, version, stacklevel=5
                )

    def decorator(func: C) -> C:
        min_idx, keys_ok, names = _get_warn_check(func, warned, allowed)

        if inspect.iscoroutinefunction(func):

            @wraps(func)
            async def async_wrapper(*args, **kwargs):
                if len(args) > min_idx or (
                    kwargs and not keys_ok(kwargs.keys(), names)
                ):
                    filterable_args = _get_filterable_args(func, args, kwargs, allowed)
                    _check_deprecated_args(func, filterable_args)
                return await func(*args, **kwargs)

            return async_wrapper
        else:

            @wraps(func)
            def wrapper(*args, **kwargs):
                if len(args) > min_idx or (
                    kwargs and not keys_ok(kwargs.keys(), names)
                ):
                    filterable_args = _get_filterable_args(func, args, kwargs, allowed)
                    _check_deprecated_args(func, filterable_args)
                return func(*args, **kwargs)

            return wrapper

    return decorator


def _set_info_logger():
    """
    Set up a logger that log info logs to stdout.
    (This is used by the default push response handler)
    """
    if "push_response" not in logging.root.manager.loggerDict.keys():
        logger = logging.getLogger("push_response")
        logger.setLevel(logging.INFO)
        handler = logging.StreamHandler()
        handler.setLevel(logging.INFO)
        logger.addHandler(handler)


#: Default RESP protocol version used on the wire when the user does not
#: supply an explicit ``protocol`` to the client / connection / pool. Lives
#: in ``redis.utils`` so both ``redis.connection`` (for the HELLO handshake)
#: and ``check_protocol_version`` (for protocol-gated features) can read it
#: without a circular import.
DEFAULT_RESP_VERSION = 3


def check_protocol_version(
    protocol: str | int | object | None, expected_version: int = 3
) -> bool:
    if protocol is None or protocol is SENTINEL:
        protocol = DEFAULT_RESP_VERSION
    if isinstance(protocol, str):
        try:
            protocol = int(protocol)
        except ValueError:
            return False
    return protocol == expected_version


@cache
def get_lib_version():
    try:
        libver = metadata.version("redis")
    except metadata.PackageNotFoundError:
        libver = "99.99.99"
    return libver


def format_error_message(host_error: str, exception: BaseException) -> str:
    if not exception.args:
        return f"Error connecting to {host_error}."
    elif len(exception.args) == 1:
        return f"Error {exception.args[0]} connecting to {host_error}."
    else:
        return (
            f"Error {exception.args[0]} connecting to {host_error}. "
            f"{exception.args[1]}."
        )


def compare_versions(version1: str, version2: str) -> int:
    """
    Compare two versions.

    :return: -1 if version1 > version2
             0 if both versions are equal
             1 if version1 < version2
    """

    num_versions1 = list(map(int, version1.split(".")))
    num_versions2 = list(map(int, version2.split(".")))

    if len(num_versions1) > len(num_versions2):
        diff = len(num_versions1) - len(num_versions2)
        for _ in range(diff):
            num_versions2.append(0)
    elif len(num_versions1) < len(num_versions2):
        diff = len(num_versions2) - len(num_versions1)
        for _ in range(diff):
            num_versions1.append(0)

    for i, ver in enumerate(num_versions1):
        if num_versions1[i] > num_versions2[i]:
            return -1
        elif num_versions1[i] < num_versions2[i]:
            return 1

    return 0


def ensure_string(key):
    if isinstance(key, bytes):
        return key.decode("utf-8")
    elif isinstance(key, str):
        return key
    else:
        raise TypeError("Key must be either a string or bytes")


def extract_expire_flags(
    ex: ExpiryT | str | None = None,
    px: ExpiryT | str | None = None,
    exat: Optional[AbsExpiryT] = None,
    pxat: Optional[AbsExpiryT] = None,
) -> List[EncodableT]:
    exp_options: list[EncodableT] = []
    if ex is not None:
        exp_options.append("EX")
        if isinstance(ex, datetime.timedelta):
            exp_options.append(int(ex.total_seconds()))
        elif isinstance(ex, int):
            exp_options.append(ex)
        elif isinstance(ex, str) and ex.isdecimal():
            exp_options.append(int(ex))
        else:
            raise DataError("ex must be datetime.timedelta, int, or a digit string")
    elif px is not None:
        exp_options.append("PX")
        if isinstance(px, datetime.timedelta):
            exp_options.append(int(px.total_seconds() * 1000))
        elif isinstance(px, int):
            exp_options.append(px)
        elif isinstance(px, str) and px.isdecimal():
            exp_options.append(int(px))
        else:
            raise DataError("px must be datetime.timedelta, int, or a digit string")
    elif exat is not None:
        if isinstance(exat, datetime.datetime):
            exat = int(exat.timestamp())
        exp_options.extend(["EXAT", exat])
    elif pxat is not None:
        if isinstance(pxat, datetime.datetime):
            pxat = int(pxat.timestamp() * 1000)
        exp_options.extend(["PXAT", pxat])

    return exp_options


def truncate_text(txt, max_length=100):
    return textwrap.shorten(
        text=txt, width=max_length, placeholder="...", break_long_words=True
    )


# Commands whose arguments can carry credentials.
_CREDENTIAL_COMMANDS = frozenset(
    {"AUTH", "HELLO", "MIGRATE", "ACL SETUSER", "CONFIG SET", "SENTINEL SET"}
)


def command_text(command):
    """
    Render a command for an error message, replacing the arguments of commands
    that can carry credentials (AUTH, HELLO, MIGRATE, ACL SETUSER, CONFIG SET,
    SENTINEL SET) with a placeholder, so passwords do not end up in exceptions
    and logs.
    """
    parts = [safe_str(arg) for arg in command]
    name_len = 0
    if parts:
        name = parts[0].upper()
        if name in _CREDENTIAL_COMMANDS:
            name_len = 1
        elif len(parts) > 1 and f"{name} {parts[1].upper()}" in _CREDENTIAL_COMMANDS:
            name_len = 2
    if name_len and len(parts) > name_len:
        parts[name_len:] = ["<REDACTED>"]
    return " ".join(parts)


def dummy_fail():
    """
    Fake function for a Retry object if you don't need to handle each failure.
    """
    pass


async def dummy_fail_async():
    """
    Async fake function for a Retry object if you don't need to handle each failure.
    """
    pass


def experimental(cls):
    """
    Decorator to mark a class as experimental.
    """
    original_init = cls.__init__

    @wraps(original_init)
    def new_init(self, *args, **kwargs):
        warnings.warn(
            f"{cls.__name__} is an experimental and may change or be removed in future versions.",
            category=UserWarning,
            stacklevel=2,
        )
        original_init(self, *args, **kwargs)

    cls.__init__ = new_init
    return cls


def warn_experimental(name, stacklevel=2):
    import warnings

    msg = (
        f"Call to experimental method {name}. "
        "Be aware that the function arguments can "
        "change or be removed in future versions."
    )
    warnings.warn(msg, category=UserWarning, stacklevel=stacklevel)


def experimental_method() -> Callable[[C], C]:
    """
    Decorator to mark a function as experimental.
    """

    def decorator(func: C) -> C:
        if inspect.iscoroutinefunction(func):
            # Create async wrapper for async functions
            @wraps(func)
            async def async_wrapper(*args, **kwargs):
                warn_experimental(func.__name__, stacklevel=2)
                return await func(*args, **kwargs)

            return async_wrapper
        else:
            # Create regular wrapper for sync functions
            @wraps(func)
            def wrapper(*args, **kwargs):
                warn_experimental(func.__name__, stacklevel=2)
                return func(*args, **kwargs)

            return wrapper

    return decorator


def warn_experimental_arg_usage(
    arg_name: Union[list, str],
    function_name: str,
    stacklevel: int = 2,
):
    import warnings

    msg = (
        f"Call to '{function_name}' method with experimental"
        f" usage of input argument/s '{arg_name}'."
    )
    warnings.warn(msg, category=UserWarning, stacklevel=stacklevel)


def experimental_args(
    args_to_warn: Optional[List[str]] = None,
) -> Callable[[C], C]:
    """
    Decorator to mark specified args of a function as experimental.
    If '*' is in args_to_warn, all arguments will be marked as experimental.
    """
    warned = tuple(args_to_warn) if args_to_warn is not None else ("*",)

    def _check_experimental_args(func, filterable_args):
        """Check and warn about experimental arguments."""
        for arg in warned:
            if arg == "*" and len(filterable_args) > 0:
                warn_experimental_arg_usage(
                    list(filterable_args.keys()), func.__name__, stacklevel=4
                )
            elif arg in filterable_args:
                warn_experimental_arg_usage(arg, func.__name__, stacklevel=4)

    def decorator(func: C) -> C:
        min_idx, keys_ok, names = _get_warn_check(func, warned)

        if inspect.iscoroutinefunction(func):

            @wraps(func)
            async def async_wrapper(*args, **kwargs):
                if len(args) > min_idx or (
                    kwargs and not keys_ok(kwargs.keys(), names)
                ):
                    filterable_args = _get_filterable_args(func, args, kwargs)
                    if len(filterable_args) > 0:
                        _check_experimental_args(func, filterable_args)
                return await func(*args, **kwargs)

            return async_wrapper
        else:

            @wraps(func)
            def wrapper(*args, **kwargs):
                if len(args) > min_idx or (
                    kwargs and not keys_ok(kwargs.keys(), names)
                ):
                    filterable_args = _get_filterable_args(func, args, kwargs)
                    if len(filterable_args) > 0:
                        _check_experimental_args(func, filterable_args)
                return func(*args, **kwargs)

            return wrapper

    return decorator
