"""
Redis Keyspace Notifications support for redis-py.

This module provides utilities for subscribing to and parsing Redis keyspace
notifications. Keyspace notifications allow clients to receive events when
keys are modified in Redis.

Note: Keyspace notifications must be enabled on the Redis server via the
``notify-keyspace-events`` configuration option. This is a server-side
configuration that should be done by your infrastructure/operations team.
See the Redis documentation for details:
https://redis.io/docs/latest/develop/pubsub/keyspace-notifications/

Standalone Redis Example:
    >>> from redis import Redis
    >>> from redis.keyspace_notifications import (
    ...     KeyspaceNotifications,
    ...     KeyspaceChannel,
    ...     EventType,
    ... )
    >>>
    >>> r = Redis()
    >>> # Server must have notify-keyspace-events configured (e.g., "KEA")
    >>> ksn = KeyspaceNotifications(r)
    >>>
    >>> # Subscribe using Channel class (patterns auto-detected)
    >>> channel = KeyspaceChannel("user:*")
    >>> ksn.subscribe(channel)
    >>>
    >>> # Or use convenience methods for specific event types
    >>> ksn.subscribe_keyevent(EventType.SET)
    >>>
    >>> for notification in ksn.listen():
    ...     print(f"Key: {notification.key}, Event: {notification.event_type}")

Redis Cluster Example:
    >>> from redis.cluster import RedisCluster
    >>> from redis.keyspace_notifications import (
    ...     ClusterKeyspaceNotifications,
    ...     KeyspaceChannel,
    ...     EventType,
    ... )
    >>>
    >>> rc = RedisCluster(host="localhost", port=7000)
    >>> # Server must have notify-keyspace-events configured (e.g., "KEA")
    >>> ksn = ClusterKeyspaceNotifications(rc)
    >>>
    >>> # Subscribe using Channel class (patterns auto-detected)
    >>> channel = KeyspaceChannel("user:*")
    >>> ksn.subscribe(channel)
    >>>
    >>> # Or use convenience methods for specific event types
    >>> ksn.subscribe_keyevent(EventType.SET)
    >>>
    >>> for notification in ksn.listen():
    ...     print(f"Key: {notification.key}, Event: {notification.event_type}")
"""

from __future__ import annotations

import logging
import re
import threading
import time
from abc import ABC, abstractmethod
from collections.abc import Callable
from dataclasses import dataclass
from enum import Enum
from typing import TYPE_CHECKING, Any, ClassVar, Union

from redis.client import Redis
from redis.cluster import RedisCluster
from redis.exceptions import (
    ConnectionError,
    RedisError,
    TimeoutError,
)
from redis.utils import safe_str

logger = logging.getLogger(__name__)

if TYPE_CHECKING:
    from typing import TypeAlias

# Type alias for channel arguments - can be a string, bytes, or Channel object
# This is defined here and the actual types are added after class definitions
ChannelT: TypeAlias = Union[
    str,
    bytes,
    "KeyspaceChannel",
    "KeyeventChannel",
    "SubkeyspaceChannel",
    "SubkeyeventChannel",
    "SubkeyspaceitemChannel",
    "SubkeyspaceeventChannel",
]


# Type alias for sync handlers
SyncHandlerT = Callable[["KeyNotification"], None]


# =============================================================================
# Event Type Constants
# =============================================================================
# These are common Redis keyspace notification event types provided for
# convenience. You can use any string as an event type - these constants
# are not exhaustive and Redis may add new events in future versions.


class EventType:
    """
    Common Redis keyspace notification event type constants.

    These are provided for convenience and IDE autocomplete. You can use
    any string as an event type - new Redis events will work without
    needing library updates.
    """

    # String commands
    SET = "set"
    SETEX = "setex"
    SETNX = "setnx"
    SETRANGE = "setrange"
    INCR = "incr"
    INCRBY = "incrby"
    INCRBYFLOAT = "incrbyfloat"
    DECR = "decr"
    DECRBY = "decrby"
    APPEND = "append"

    # Generic commands
    DEL = "del"
    UNLINK = "unlink"
    RENAME = "rename"
    RENAME_FROM = "rename_from"
    RENAME_TO = "rename_to"
    COPY_TO = "copy_to"
    MOVE = "move"
    RESTORE = "restore"

    # Expiration events
    EXPIRE = "expire"
    EXPIREAT = "expireat"
    PEXPIRE = "pexpire"
    PEXPIREAT = "pexpireat"
    EXPIRED = "expired"
    PERSIST = "persist"

    # Eviction events
    EVICTED = "evicted"

    # List commands
    LPUSH = "lpush"
    RPUSH = "rpush"
    LPOP = "lpop"
    RPOP = "rpop"
    LINSERT = "linsert"
    LSET = "lset"
    LTRIM = "ltrim"
    LMOVE = "lmove"
    BLPOP = "blpop"
    BRPOP = "brpop"
    BLMOVE = "blmove"

    # Set commands
    SADD = "sadd"
    SREM = "srem"
    SPOP = "spop"
    SMOVE = "smove"
    SINTERSTORE = "sinterstore"
    SUNIONSTORE = "sunionstore"
    SDIFFSTORE = "sdiffstore"

    # Sorted set commands
    ZADD = "zadd"
    ZINCRBY = "zincrby"
    ZREM = "zrem"
    ZREMRANGEBYRANK = "zremrangebyrank"
    ZREMRANGEBYSCORE = "zremrangebyscore"
    ZREMRANGEBYLEX = "zremrangebylex"
    ZPOPMIN = "zpopmin"
    ZPOPMAX = "zpopmax"
    BZPOPMIN = "bzpopmin"
    BZPOPMAX = "bzpopmax"
    ZINTERSTORE = "zinterstore"
    ZUNIONSTORE = "zunionstore"
    ZDIFFSTORE = "zdiffstore"
    ZRANGESTORE = "zrangestore"

    # Hash commands
    HSET = "hset"
    HSETNX = "hsetnx"
    HDEL = "hdel"
    HINCRBY = "hincrby"
    HINCRBYFLOAT = "hincrbyfloat"

    # Stream commands
    XADD = "xadd"
    XTRIM = "xtrim"
    XDEL = "xdel"
    XGROUP_CREATE = "xgroup-create"
    XGROUP_CREATECONSUMER = "xgroup-createconsumer"
    XGROUP_DELCONSUMER = "xgroup-delconsumer"
    XGROUP_DESTROY = "xgroup-destroy"
    XGROUP_SETID = "xgroup-setid"
    XSETID = "xsetid"
    XCLAIM = "xclaim"
    XAUTOCLAIM = "xautoclaim"
    XREADGROUP = "xreadgroup"

    # Other
    NEW = "new"  # Key created (when tracking new keys)
    SORTSTORE = "sortstore"
    GETEX = "getex"
    GETDEL = "getdel"
    SETIFGT = "setifgt"
    SETIFLT = "setiflt"
    SETIFEQ = "setifeq"
    SETIFNE = "setifne"


def _parse_length_prefixed_subkeys(s: bytes | str) -> list[str]:
    """Parse a length-prefixed subkey list.

    The wire format is ``<len>:<subkey>[,<len>:<subkey>...]``.

    The server writes ``<len>`` as the subkey size in **bytes** (it uses
    ``sdslen`` when formatting the notification), so parsing must happen in
    the byte domain; splitting the decoded string by character count
    corrupts every multi-byte subkey and desynchronizes the whole list.

    Returns:
        A list of subkey strings.
    """
    raw = s.encode("utf-8") if isinstance(s, str) else bytes(s)
    subkeys: list[str] = []
    pos = 0
    while pos < len(raw):
        colon = raw.find(b":", pos)
        if colon < 0 or not raw[pos:colon].isdigit():
            raise ValueError("Invalid subkey length prefix")
        length = int(raw[pos:colon])
        start = colon + 1
        end = start + length
        if end > len(raw):
            raise ValueError("Truncated subkey")
        subkeys.append(raw[start:end].decode("utf-8", "replace"))
        pos = end
        if pos < len(raw):
            if raw[pos] != ord(",") or pos + 1 == len(raw):
                raise ValueError("Invalid subkey separator")
            pos += 1
    return subkeys


@dataclass
class KeyNotification:
    """
    Represents a parsed Redis keyspace, keyevent, or subkey notification.

    This class provides convenient access to the notification details
    like key, event type, database number, and affected subkeys.

    Attributes:
        key: The Redis key that was affected (for keyspace notifications)
             or the key name from the message data (for keyevent notifications)
        event_type: The type of operation that occurred (e.g., "set", "del").
                   This is a plain string, so new Redis events work automatically.
                   Compare against EventType constants or any string.
        database: The database number where the event occurred
        channel: The original channel name
        is_keyspace: True if this is a keyspace notification, False for keyevent
        data: The raw data payload from the notification message.
        subkeys: List of affected subkeys (fields) for subkey notifications.
                Empty list for regular keyspace/keyevent notifications.
    """

    # Regex patterns for parsing keyspace/keyevent channels
    # Pattern: __keyspace@<db>__:<key> or __keyevent@<db>__:<event>
    # A key may contain any byte, newlines included, so the patterns that capture
    # one need re.DOTALL: without it "." stops at a newline and "$" matches before
    # a trailing one, dropping or truncating the key.
    _KEYSPACE_PATTERN: ClassVar[re.Pattern] = re.compile(
        r"^__keyspace@(\d+|\*)__:(.*)$", re.DOTALL
    )
    _KEYEVENT_PATTERN: ClassVar[re.Pattern] = re.compile(
        r"^__keyevent@(\d+|\*)__:(.+)$"
    )
    _SUBKEYSPACE_PATTERN: ClassVar[re.Pattern] = re.compile(
        r"^__subkeyspace@(\d+|\*)__:(.*)$", re.DOTALL
    )
    _SUBKEYEVENT_PATTERN: ClassVar[re.Pattern] = re.compile(
        r"^__subkeyevent@(\d+|\*)__:(.+)$"
    )
    _SUBKEYSPACEITEM_PATTERN: ClassVar[re.Pattern] = re.compile(
        r"^__subkeyspaceitem@(\d+|\*)__:(.+)$", re.DOTALL
    )
    _SUBKEYSPACEEVENT_PATTERN: ClassVar[re.Pattern] = re.compile(
        r"^__subkeyspaceevent@(\d+|\*)__:(.+)$", re.DOTALL
    )

    key: str
    event_type: str
    database: int
    channel: str
    is_keyspace: bool
    data: str
    subkeys: list[str] = None  # type: ignore[assignment]

    def __post_init__(self):
        if self.subkeys is None:
            self.subkeys = []

    @classmethod
    def from_message(
        cls,
        message: dict[str, Any] | None,
        key_prefix: str | bytes | None = None,
    ) -> KeyNotification | None:
        """
        Parse a pub/sub message into a KeyNotification.

        Args:
            message: A pub/sub message dict with 'channel', 'data', and 'type' keys
            key_prefix: Optional prefix to filter and strip from keys.
                       If provided, only notifications for keys starting with
                       this prefix will be returned, and the prefix will be
                       stripped from the key.

        Returns:
            A KeyNotification if the message is a valid keyspace/keyevent
            notification, None otherwise.

        Example:
            >>> message = {
            ...     'type': 'pmessage',
            ...     'pattern': '__keyspace@0__:user:*',
            ...     'channel': '__keyspace@0__:user:123',
            ...     'data': 'set'
            ... }
            >>> notification = KeyNotification.from_message(message)
            >>> notification.key
            'user:123'
            >>> notification.event_type
            'set'
        """
        if message is None:
            return None

        msg_type = message.get("type")
        if msg_type not in ("message", "pmessage"):
            return None

        channel = message.get("channel")
        data = message.get("data")

        if channel is None or data is None:
            return None

        return cls.try_parse(channel, data, key_prefix)

    @classmethod
    def try_parse(
        cls,
        channel: str | bytes,
        data: str | bytes,
        key_prefix: str | bytes | None = None,
    ) -> KeyNotification | None:
        """
        Try to parse a channel and data into a KeyNotification.

        This is a lower-level method that takes the channel and data directly,
        useful when working with callback-based subscription handlers.

        Args:
            channel: The channel name (e.g., "__keyspace@0__:mykey")
            data: The message data (event type for keyspace, key for keyevent)
            key_prefix: Optional prefix to filter and strip from keys

        Returns:
            A KeyNotification if valid, None otherwise.
        """
        raw_data = bytes(data) if isinstance(data, (bytes, bytearray)) else None
        channel = safe_str(channel)
        data = safe_str(data)

        try:
            return cls._parse(channel, data, key_prefix, raw_data=raw_data)
        except ValueError:
            return None

    @classmethod
    def _parse(
        cls,
        channel: str,
        data: str,
        key_prefix: str | bytes | None = None,
        raw_data: bytes | None = None,
    ) -> KeyNotification | None:
        """Internal parsing logic."""
        # Normalize key_prefix
        key_prefix = safe_str(key_prefix) if key_prefix else None
        # Byte-domain view of the payload: the server prefixes every element
        # with its size in bytes (sdslen), so length-prefixed fields must be
        # split before decoding. When the caller only has the decoded string
        # (decode_responses=True) the exact bytes are recovered by re-encoding.
        raw = raw_data if raw_data is not None else data.encode("utf-8")

        # Try keyspace pattern first: __keyspace@<db>__:<key>
        match = cls._KEYSPACE_PATTERN.match(channel)
        if match:
            db_str, key = match.groups()
            database = int(db_str) if db_str != "*" else -1
            event_type = data  # For keyspace, the data is the event type

            # Apply key prefix filter
            if key_prefix:
                if not key.startswith(key_prefix):
                    return None
                key = key[len(key_prefix) :]

            return cls(
                key=key,
                event_type=event_type,
                database=database,
                channel=channel,
                is_keyspace=True,
                data=data,
            )

        # Try keyevent pattern: __keyevent@<db>__:<event>
        match = cls._KEYEVENT_PATTERN.match(channel)
        if match:
            db_str, event_type = match.groups()
            database = int(db_str) if db_str != "*" else -1
            key = data  # For keyevent, the data is the key

            # Apply key prefix filter
            if key_prefix:
                if not key.startswith(key_prefix):
                    return None
                key = key[len(key_prefix) :]

            return cls(
                key=key,
                event_type=event_type,
                database=database,
                channel=channel,
                is_keyspace=False,
                data=data,
            )

        # Try subkeyspace: channel=__subkeyspace@<db>__:<key>
        # data=<event>|<subkey_len>:<subkey>[,<subkey_len>:<subkey>...]
        match = cls._SUBKEYSPACE_PATTERN.match(channel)
        if match:
            db_str, key = match.groups()
            database = int(db_str) if db_str != "*" else -1
            pipe_idx = raw.index(b"|")
            event_type = raw[:pipe_idx].decode("utf-8", "replace")
            subkeys = _parse_length_prefixed_subkeys(raw[pipe_idx + 1 :])

            if key_prefix:
                if not key.startswith(key_prefix):
                    return None
                key = key[len(key_prefix) :]

            return cls(
                key=key,
                event_type=event_type,
                database=database,
                channel=channel,
                is_keyspace=True,
                data=data,
                subkeys=subkeys,
            )

        # Try subkeyevent: channel=__subkeyevent@<db>__:<event>
        # data=<key_len>:<key>|<subkey_len>:<subkey>[,...]
        match = cls._SUBKEYEVENT_PATTERN.match(channel)
        if match:
            db_str, event_type = match.groups()
            database = int(db_str) if db_str != "*" else -1
            # Parse key by length prefix
            colon_idx = raw.index(b":")
            key_len = int(raw[:colon_idx])
            key_start = colon_idx + 1
            separator = key_start + key_len
            if key_len < 0 or separator >= len(raw) or raw[separator] != ord("|"):
                raise ValueError("Invalid subkeyevent key separator")
            key = raw[key_start : key_start + key_len].decode("utf-8", "replace")
            # After key, expect '|' then subkeys
            subkeys_start = separator + 1
            subkeys = _parse_length_prefixed_subkeys(raw[subkeys_start:])

            if key_prefix:
                if not key.startswith(key_prefix):
                    return None
                key = key[len(key_prefix) :]

            return cls(
                key=key,
                event_type=event_type,
                database=database,
                channel=channel,
                is_keyspace=False,
                data=data,
                subkeys=subkeys,
            )

        # Try subkeyspaceitem: channel=__subkeyspaceitem@<db>__:<key>\n<subkey>
        # data=<event>
        match = cls._SUBKEYSPACEITEM_PATTERN.match(channel)
        if match:
            db_str, key_and_subkey = match.groups()
            database = int(db_str) if db_str != "*" else -1
            newline_idx = key_and_subkey.index("\n")
            key = key_and_subkey[:newline_idx]
            subkey = key_and_subkey[newline_idx + 1 :]
            event_type = data

            if key_prefix:
                if not key.startswith(key_prefix):
                    return None
                key = key[len(key_prefix) :]

            return cls(
                key=key,
                event_type=event_type,
                database=database,
                channel=channel,
                is_keyspace=True,
                data=data,
                subkeys=[subkey],
            )

        # Try subkeyspaceevent: channel=__subkeyspaceevent@<db>__:<event>|<key>
        # data=<subkey_len>:<subkey>[,...]
        match = cls._SUBKEYSPACEEVENT_PATTERN.match(channel)
        if match:
            db_str, event_and_key = match.groups()
            database = int(db_str) if db_str != "*" else -1
            pipe_idx = event_and_key.index("|")
            event_type = event_and_key[:pipe_idx]
            key = event_and_key[pipe_idx + 1 :]
            subkeys = _parse_length_prefixed_subkeys(raw)

            if key_prefix:
                if not key.startswith(key_prefix):
                    return None
                key = key[len(key_prefix) :]

            return cls(
                key=key,
                event_type=event_type,
                database=database,
                channel=channel,
                is_keyspace=False,
                data=data,
                subkeys=subkeys,
            )

        return None

    def key_starts_with(self, prefix: str | bytes) -> bool:
        """Check if the key starts with the given prefix."""
        prefix = safe_str(prefix)
        return self.key.startswith(prefix)


# =============================================================================
# Channel Classes
# =============================================================================


class KeyspaceChannel:
    """
    Represents a keyspace notification channel for subscribing to events on keys.

    Keyspace notifications publish the event type (e.g., "set", "del") as the message
    when a key matching the pattern is modified.

    This class can be used directly with subscribe()/psubscribe() as it implements
    __str__ to return the channel string.

    Attributes:
        key_or_pattern: The key or pattern to monitor (use '*' for wildcards)
        db: The database number (defaults to 0, the only database in Redis Cluster)
        is_pattern: Whether this channel contains wildcards

    Examples:
        >>> channel = KeyspaceChannel("user:123", db=0)
        >>> str(channel)
        '__keyspace@0__:user:123'

        >>> # Pattern subscription (wildcards are auto-detected)
        >>> channel = KeyspaceChannel("user:*", db=0)
        >>> str(channel)
        '__keyspace@0__:user:*'

        >>> # Use with KeyspaceNotifications
        >>> notifications = KeyspaceNotifications(redis_client)
        >>> notifications.subscribe(channel)
    """

    PREFIX: ClassVar[str] = "__keyspace@"

    def __init__(self, key_or_pattern: str, db: int = 0):
        """
        Create a keyspace notification channel.

        Args:
            key_or_pattern: The key or pattern to monitor. Use '*' for wildcards.
            db: The database number. Defaults to 0 (the only database in Redis Cluster).
        """
        self.key_or_pattern = key_or_pattern
        self.db = db
        self._channel_str = self._build_channel_string()

    def _build_channel_string(self) -> str:
        return f"{self.PREFIX}{self.db}__:{self.key_or_pattern}"

    @property
    def is_pattern(self) -> bool:
        """Check if this channel contains wildcards and should use psubscribe."""
        return _is_pattern(self.key_or_pattern)

    def __str__(self) -> str:
        return self._channel_str

    def __repr__(self) -> str:
        return f"KeyspaceChannel({self.key_or_pattern!r}, db={self.db})"

    def __eq__(self, other: object) -> bool:
        if isinstance(other, KeyspaceChannel):
            return self._channel_str == other._channel_str
        if isinstance(other, str):
            return self._channel_str == other
        return NotImplemented

    def __hash__(self) -> int:
        return hash(self._channel_str)


class KeyeventChannel:
    """
    Represents a keyevent notification channel for subscribing to event types.

    Keyevent notifications publish the key name as the message when the specified
    event type occurs on any key.

    This class can be used directly with subscribe()/psubscribe() as it implements
    __str__ to return the channel string.

    Attributes:
        event: The event type to monitor
        db: The database number (defaults to 0, the only database in Redis Cluster)
        is_pattern: Whether this channel contains wildcards

    Examples:
        >>> channel = KeyeventChannel(EventType.SET, db=0)
        >>> str(channel)
        '__keyevent@0__:set'

        >>> channel = KeyeventChannel.all_events(db=0)
        >>> str(channel)
        '__keyevent@0__:*'

        >>> # Use with KeyspaceNotifications
        >>> notifications = KeyspaceNotifications(redis_client)
        >>> notifications.subscribe(channel)
    """

    PREFIX: ClassVar[str] = "__keyevent@"

    def __init__(self, event: str, db: int = 0):
        """
        Create a keyevent notification channel.

        Args:
            event: The event type to monitor (e.g., EventType.SET or "set")
            db: The database number. Defaults to 0 (the only database in Redis Cluster).
        """
        self.event = event
        self.db = db
        self._channel_str = self._build_channel_string()

    def _build_channel_string(self) -> str:
        return f"{self.PREFIX}{self.db}__:{self.event}"

    @property
    def is_pattern(self) -> bool:
        """Check if this channel contains wildcards and should use psubscribe."""
        return _is_pattern(self.event)

    @classmethod
    def all_events(cls, db: int = 0) -> "KeyeventChannel":
        """
        Create a keyevent pattern for subscribing to all event types.

        This is equivalent to KeyeventChannel("*").

        Args:
            db: The database number. Defaults to 0 (the only database in Redis Cluster).

        Returns:
            A KeyeventChannel configured to receive all events.

        Examples:
            >>> channel = KeyeventChannel.all_events()
            >>> str(channel)
            '__keyevent@0__:*'
        """
        return cls("*", db=db)

    def __str__(self) -> str:
        return self._channel_str

    def __repr__(self) -> str:
        return f"KeyeventChannel({self.event!r}, db={self.db})"

    def __eq__(self, other: object) -> bool:
        if isinstance(other, KeyeventChannel):
            return self._channel_str == other._channel_str
        if isinstance(other, str):
            return self._channel_str == other
        return NotImplemented

    def __hash__(self) -> int:
        return hash(self._channel_str)


class SubkeyspaceChannel:
    """
    Represents a subkeyspace notification channel for subscribing to
    subkey-level events on keys (e.g., hash field changes).

    The channel format is ``__subkeyspace@<db>__:<key>``.
    The message payload is ``<event>|<subkey_len>:<subkey>[,...]``.

    Examples:
        >>> channel = SubkeyspaceChannel("myhash", db=0)
        >>> str(channel)
        '__subkeyspace@0__:myhash'
    """

    PREFIX: ClassVar[str] = "__subkeyspace@"

    def __init__(self, key_or_pattern: str, db: int = 0):
        self.key_or_pattern = key_or_pattern
        self.db = db
        self._channel_str = self._build_channel_string()

    def _build_channel_string(self) -> str:
        return f"{self.PREFIX}{self.db}__:{self.key_or_pattern}"

    @property
    def is_pattern(self) -> bool:
        return _is_pattern(self.key_or_pattern)

    def __str__(self) -> str:
        return self._channel_str

    def __repr__(self) -> str:
        return f"SubkeyspaceChannel({self.key_or_pattern!r}, db={self.db})"

    def __eq__(self, other: object) -> bool:
        if isinstance(other, SubkeyspaceChannel):
            return self._channel_str == other._channel_str
        if isinstance(other, str):
            return self._channel_str == other
        return NotImplemented

    def __hash__(self) -> int:
        return hash(self._channel_str)


class SubkeyeventChannel:
    """
    Represents a subkeyevent notification channel for subscribing to
    specific event types with subkey-level detail.

    The channel format is ``__subkeyevent@<db>__:<event>``.
    The message payload is ``<key_len>:<key>|<subkey_len>:<subkey>[,...]``.

    Examples:
        >>> channel = SubkeyeventChannel("hdel", db=0)
        >>> str(channel)
        '__subkeyevent@0__:hdel'
    """

    PREFIX: ClassVar[str] = "__subkeyevent@"

    def __init__(self, event: str, db: int = 0):
        self.event = event
        self.db = db
        self._channel_str = self._build_channel_string()

    def _build_channel_string(self) -> str:
        return f"{self.PREFIX}{self.db}__:{self.event}"

    @property
    def is_pattern(self) -> bool:
        return _is_pattern(self.event)

    @classmethod
    def all_events(cls, db: int = 0) -> SubkeyeventChannel:
        """Create a channel for all subkeyevent types."""
        return cls("*", db=db)

    def __str__(self) -> str:
        return self._channel_str

    def __repr__(self) -> str:
        return f"SubkeyeventChannel({self.event!r}, db={self.db})"

    def __eq__(self, other: object) -> bool:
        if isinstance(other, SubkeyeventChannel):
            return self._channel_str == other._channel_str
        if isinstance(other, str):
            return self._channel_str == other
        return NotImplemented

    def __hash__(self) -> int:
        return hash(self._channel_str)


class SubkeyspaceitemChannel:
    """
    Represents a subkeyspaceitem notification channel for subscribing to
    events on a specific subkey (field) of a specific key.

    The channel format is ``__subkeyspaceitem@<db>__:<key>\\n<subkey>``.
    The message payload is the event type (e.g., ``"hset"``).

    Note:
        The server only emits this notification when the key does not
        contain a newline character.

    Examples:
        >>> channel = SubkeyspaceitemChannel("myhash", "myfield", db=0)
        >>> str(channel)
        '__subkeyspaceitem@0__:myhash\\nmyfield'
    """

    PREFIX: ClassVar[str] = "__subkeyspaceitem@"

    def __init__(self, key_or_pattern: str, subkey_or_pattern: str, db: int = 0):
        self.key_or_pattern = key_or_pattern
        self.subkey_or_pattern = subkey_or_pattern
        self.db = db
        self._channel_str = self._build_channel_string()

    def _build_channel_string(self) -> str:
        return (
            f"{self.PREFIX}{self.db}__:{self.key_or_pattern}\n{self.subkey_or_pattern}"
        )

    @property
    def is_pattern(self) -> bool:
        return _is_pattern(self.key_or_pattern) or _is_pattern(self.subkey_or_pattern)

    def __str__(self) -> str:
        return self._channel_str

    def __repr__(self) -> str:
        return (
            f"SubkeyspaceitemChannel({self.key_or_pattern!r}, "
            f"{self.subkey_or_pattern!r}, db={self.db})"
        )

    def __eq__(self, other: object) -> bool:
        if isinstance(other, SubkeyspaceitemChannel):
            return self._channel_str == other._channel_str
        if isinstance(other, str):
            return self._channel_str == other
        return NotImplemented

    def __hash__(self) -> int:
        return hash(self._channel_str)


class SubkeyspaceeventChannel:
    """
    Represents a subkeyspaceevent notification channel for subscribing to
    a specific event on a specific key, receiving affected subkeys.

    The channel format is ``__subkeyspaceevent@<db>__:<event>|<key>``.
    The message payload is a length-prefixed subkey list.

    Examples:
        >>> channel = SubkeyspaceeventChannel("hset", "myhash", db=0)
        >>> str(channel)
        '__subkeyspaceevent@0__:hset|myhash'
    """

    PREFIX: ClassVar[str] = "__subkeyspaceevent@"

    def __init__(self, event: str, key_or_pattern: str, db: int = 0):
        self.event = event
        self.key_or_pattern = key_or_pattern
        self.db = db
        self._channel_str = self._build_channel_string()

    def _build_channel_string(self) -> str:
        return f"{self.PREFIX}{self.db}__:{self.event}|{self.key_or_pattern}"

    @property
    def is_pattern(self) -> bool:
        return _is_pattern(self.event) or _is_pattern(self.key_or_pattern)

    def __str__(self) -> str:
        return self._channel_str

    def __repr__(self) -> str:
        return (
            f"SubkeyspaceeventChannel({self.event!r}, "
            f"{self.key_or_pattern!r}, db={self.db})"
        )

    def __eq__(self, other: object) -> bool:
        if isinstance(other, SubkeyspaceeventChannel):
            return self._channel_str == other._channel_str
        if isinstance(other, str):
            return self._channel_str == other
        return NotImplemented

    def __hash__(self) -> int:
        return hash(self._channel_str)


class ChannelType(Enum):
    """
    Enum representing the type of a Redis keyspace notification channel.

    Redis provides two types of keyspace notifications and four subkey
    notification types:

    - KEYSPACE: ``__keyspace@{db}__:{key}`` — data is the event type.
    - KEYEVENT: ``__keyevent@{db}__:{event}`` — data is the key name.
    - SUBKEYSPACE: ``__subkeyspace@{db}__:{key}`` — data is event + subkeys.
    - SUBKEYEVENT: ``__subkeyevent@{db}__:{event}`` — data is key + subkeys.
    - SUBKEYSPACEITEM: ``__subkeyspaceitem@{db}__:{key}\\n{subkey}`` — data
      is the event type.
    - SUBKEYSPACEEVENT: ``__subkeyspaceevent@{db}__:{event}|{key}`` — data
      is a subkey list.

    Examples:
        >>> get_channel_type("__keyspace@0__:mykey")
        ChannelType.KEYSPACE
        >>> get_channel_type("__subkeyspace@0__:myhash")
        ChannelType.SUBKEYSPACE
    """

    KEYSPACE = "keyspace"
    KEYEVENT = "keyevent"
    SUBKEYSPACE = "subkeyspace"
    SUBKEYEVENT = "subkeyevent"
    SUBKEYSPACEITEM = "subkeyspaceitem"
    SUBKEYSPACEEVENT = "subkeyspaceevent"


def get_channel_type(channel: str | bytes) -> ChannelType | None:
    """
    Determine the type of a Redis keyspace notification channel.

    Args:
        channel: The channel name to check (string or bytes).

    Returns:
        ChannelType.KEYSPACE if it's a keyspace notification channel,
        ChannelType.KEYEVENT if it's a keyevent notification channel,
        None if it's not a keyspace notification channel.

    Examples:
        >>> get_channel_type("__keyspace@0__:mykey")
        ChannelType.KEYSPACE
        >>> get_channel_type("__keyevent@0__:set")
        ChannelType.KEYEVENT
        >>> get_channel_type("regular_channel") is None
        True
        >>> get_channel_type(b"__keyspace@0__:mykey")
        ChannelType.KEYSPACE
    """
    channel_str = safe_str(channel)
    # Check subkey prefixes first (they are longer and more specific)
    if channel_str.startswith(SubkeyspaceitemChannel.PREFIX):
        return ChannelType.SUBKEYSPACEITEM
    if channel_str.startswith(SubkeyspaceeventChannel.PREFIX):
        return ChannelType.SUBKEYSPACEEVENT
    if channel_str.startswith(SubkeyspaceChannel.PREFIX):
        return ChannelType.SUBKEYSPACE
    if channel_str.startswith(SubkeyeventChannel.PREFIX):
        return ChannelType.SUBKEYEVENT
    if channel_str.startswith(KeyspaceChannel.PREFIX):
        return ChannelType.KEYSPACE
    if channel_str.startswith(KeyeventChannel.PREFIX):
        return ChannelType.KEYEVENT
    return None


def _is_pattern(
    channel: str | bytes | KeyspaceChannel | KeyeventChannel,
) -> bool:
    """
    Check if a channel string contains glob-style pattern characters.

    Redis uses glob-style patterns for psubscribe:
    - * matches any sequence of characters
    - ? matches any single character
    - [...] matches any character in the brackets

    Args:
        channel: The channel string to check. Can be a string, bytes,
                 or a KeyspaceChannel/KeyeventChannel object.

    Returns:
        True if the channel contains pattern characters, False otherwise.
    """
    # Handle Channel objects that have _channel_str attribute
    # (KeyspaceChannel, KeyeventChannel)
    if hasattr(channel, "_channel_str"):
        channel = channel._channel_str
    channel = safe_str(channel)
    # Check for unescaped glob pattern characters.
    # * and ? are always pattern characters.
    # [ is only a pattern character when followed by a matching unescaped ],
    # forming a bracket expression like [abc] or [a-z].  A lone [ (e.g. in
    # a key named "my[key") is treated as a literal by Redis.
    i = 0
    while i < len(channel):
        char = channel[i]
        if char == "\\":
            # Skip escaped character
            i += 2
            continue
        if char in ("*", "?"):
            return True
        if char == "[":
            # Look for a matching unescaped ]
            j = i + 1
            while j < len(channel):
                if channel[j] == "\\":
                    j += 2
                    continue
                if channel[j] == "]":
                    return True
                j += 1
            # No matching ] found — literal [
        i += 1
    return False


# =============================================================================
# Abstract Base Class for Keyspace Notifications
# =============================================================================


class KeyspaceNotificationsInterface(ABC):
    """
    Interface for keyspace notification managers.

    This interface provides a consistent API for both standalone (KeyspaceNotifications)
    and cluster (ClusterKeyspaceNotifications) implementations, allowing the same
    code patterns to work with both standalone and cluster Redis deployments.
    """

    @abstractmethod
    def subscribe(
        self,
        *channels: ChannelT,
        handler: SyncHandlerT | None = None,
    ):
        """Subscribe to keyspace notification channels."""
        pass

    @abstractmethod
    def unsubscribe(self, *channels: ChannelT):
        """Unsubscribe from keyspace notification channels."""
        pass

    @abstractmethod
    def subscribe_keyspace(
        self,
        key_or_pattern: str,
        db: int = 0,
        handler: SyncHandlerT | None = None,
    ):
        """Subscribe to keyspace notifications for specific keys."""
        pass

    @abstractmethod
    def subscribe_keyevent(
        self,
        event: str,
        db: int = 0,
        handler: SyncHandlerT | None = None,
    ):
        """Subscribe to keyevent notifications for specific event types."""
        pass

    @abstractmethod
    def subscribe_subkeyspace(
        self,
        key_or_pattern: str,
        db: int = 0,
        handler: SyncHandlerT | None = None,
    ):
        """Subscribe to subkeyspace notifications for specific keys."""
        pass

    @abstractmethod
    def subscribe_subkeyevent(
        self,
        event: str,
        db: int = 0,
        handler: SyncHandlerT | None = None,
    ):
        """Subscribe to subkeyevent notifications for specific event types."""
        pass

    @abstractmethod
    def subscribe_subkeyspaceitem(
        self,
        key_or_pattern: str,
        subkey_or_pattern: str,
        db: int = 0,
        handler: SyncHandlerT | None = None,
    ):
        """Subscribe to subkeyspaceitem notifications for a specific subkey."""
        pass

    @abstractmethod
    def subscribe_subkeyspaceevent(
        self,
        event: str,
        key_or_pattern: str,
        db: int = 0,
        handler: SyncHandlerT | None = None,
    ):
        """Subscribe to subkeyspaceevent notifications for an event on a key."""
        pass

    @abstractmethod
    def get_message(
        self,
        ignore_subscribe_messages: bool | None = None,
        timeout: float = 0.0,
    ) -> KeyNotification | None:
        """Get the next keyspace notification if one is available."""
        pass

    @abstractmethod
    def listen(self):
        """Listen for keyspace notifications."""
        pass

    @abstractmethod
    def close(self):
        """Close the notification manager and clean up resources."""
        pass

    @abstractmethod
    def __enter__(self):
        pass

    @abstractmethod
    def __exit__(self, _exc_type, _exc_val, _exc_tb):
        pass

    @property
    @abstractmethod
    def subscribed(self) -> bool:
        """Check if there are any active subscriptions and not closed."""
        pass

    @abstractmethod
    def run_in_thread(
        self,
        poll_timeout: float = 0.0,
        daemon: bool = False,
        exception_handler: Callable[
            [
                BaseException,
                KeyspaceNotificationsInterface,
                KeyspaceWorkerThread,
            ],
            None,
        ]
        | None = None,
    ) -> KeyspaceWorkerThread:
        """Start a background thread that polls for notifications."""
        pass


class AbstractKeyspaceNotifications(KeyspaceNotificationsInterface):
    """
    Abstract base class for keyspace notification managers.

    Provides shared implementation for subscribe/unsubscribe logic.
    Subclasses must implement:
    - _execute_subscribe: Execute the subscribe operation
    - _execute_unsubscribe: Execute the unsubscribe operation
    - get_message: Get the next notification
    - listen: Generator for notifications
    - close: Clean up resources
    """

    def __init__(
        self,
        key_prefix: str | bytes | None = None,
        ignore_subscribe_messages: bool = True,
    ):
        """
        Initialize the base keyspace notification manager.

        Args:
            key_prefix: Optional prefix to filter and strip from keys in notifications
            ignore_subscribe_messages: If True, subscribe/unsubscribe confirmations
                                      are not returned by get_message/listen
        """
        self.key_prefix = key_prefix
        self.ignore_subscribe_messages = ignore_subscribe_messages
        self._closed = False

    def subscribe(
        self,
        *channels: ChannelT,
        handler: SyncHandlerT | None = None,
    ):
        """
        Subscribe to keyspace notification channels.

        Automatically detects whether each channel is a pattern (contains
        wildcards like *, ?, [) or an exact channel name and uses the
        appropriate Redis subscribe command internally.

        Args:
            *channels: Channels to subscribe to. Can be strings, KeyspaceChannel,
                      or KeyeventChannel objects. Patterns are auto-detected.
            handler: Optional callback function that receives KeyNotification
                    objects. If provided, notifications are passed to the handler
                    instead of being returned by get_message()/listen().
        """
        # Wrap the handler to convert raw messages to KeyNotification objects
        wrapped_handler: Callable | None = None
        if handler is not None:
            # Capture key_prefix in closure for consistent filtering/stripping
            key_prefix = self.key_prefix

            def _wrap_handler(message):
                notification = KeyNotification.from_message(
                    message, key_prefix=key_prefix
                )
                if notification is not None:
                    handler(notification)

            wrapped_handler = _wrap_handler

        patterns = {}
        exact_channels = {}

        for channel in channels:
            if hasattr(channel, "_channel_str"):
                channel_str = str(channel)
            else:
                channel_str = safe_str(channel)
            if _is_pattern(channel):
                patterns[channel_str] = wrapped_handler
            else:
                exact_channels[channel_str] = wrapped_handler

        # Delegate to subclass implementation first.  For standalone Redis
        # this raises on failure, keeping tracking state clean.  For cluster
        # implementations the operation is best-effort (partial failures are
        # logged, not raised) so tracking state is always updated afterwards.
        self._execute_subscribe(patterns, exact_channels)
        self._track_subscribe(patterns, exact_channels)

    @abstractmethod
    def _execute_subscribe(
        self, patterns: dict[str, Any], exact_channels: dict[str, Any]
    ) -> None:
        """
        Execute the subscribe operation.

        Args:
            patterns: Dict mapping pattern strings to handlers (for psubscribe)
            exact_channels: Dict mapping channel strings to handlers (for subscribe)
        """
        pass

    def unsubscribe(self, *channels: ChannelT):
        """
        Unsubscribe from keyspace notification channels.

        Automatically detects whether each channel is a pattern or exact
        channel and uses the appropriate Redis unsubscribe command.

        Args:
            *channels: Channels to unsubscribe from.
        """
        patterns = []
        exact_channels = []

        for channel in channels:
            if hasattr(channel, "_channel_str"):
                channel_str = str(channel)
            else:
                channel_str = safe_str(channel)
            if _is_pattern(channel):
                patterns.append(channel_str)
            else:
                exact_channels.append(channel_str)

        # Delegate to subclass implementation first.  For standalone Redis
        # this raises on failure, keeping tracking state intact.  For cluster
        # implementations the operation is best-effort (partial failures are
        # logged, not raised) so tracking state is always removed afterwards
        # — this is intentional: the user asked to unsubscribe, so
        # refresh_subscriptions should not re-subscribe these channels.
        self._execute_unsubscribe(patterns, exact_channels)
        self._untrack_subscribe(patterns, exact_channels)

    @abstractmethod
    def _execute_unsubscribe(
        self, patterns: list[str], exact_channels: list[str]
    ) -> None:
        """
        Execute the unsubscribe operation.

        Args:
            patterns: List of pattern strings to punsubscribe from
            exact_channels: List of channel strings to unsubscribe from
        """
        pass

    def _track_subscribe(
        self, patterns: dict[str, Any], exact_channels: dict[str, Any]
    ) -> None:
        """Track newly subscribed patterns/channels.

        Override in subclasses that need to maintain their own subscription
        registry (e.g. cluster implementations that must re-subscribe
        new/failed-over nodes).  The default is a no-op because standalone
        implementations delegate tracking to the underlying PubSub object.
        """

    def _untrack_subscribe(
        self, patterns: list[str], exact_channels: list[str]
    ) -> None:
        """Remove patterns/channels from the subscription registry.

        Override in subclasses that maintain their own subscription registry.
        The default is a no-op.
        """

    def subscribe_keyspace(
        self,
        key_or_pattern: str,
        db: int = 0,
        handler: SyncHandlerT | None = None,
    ):
        """
        Subscribe to keyspace notifications for specific keys.

        Args:
            key_or_pattern: The key or pattern to monitor. Use '*' for wildcards.
            db: The database number (default 0).
            handler: Optional callback for notifications.

        Example:
            >>> ksn.subscribe_keyspace("user:123", db=0)
            >>> ksn.subscribe_keyspace("user:*", db=0)
        """
        channel = KeyspaceChannel(key_or_pattern, db=db)
        self.subscribe(channel, handler=handler)

    def subscribe_keyevent(
        self,
        event: str,
        db: int = 0,
        handler: SyncHandlerT | None = None,
    ):
        """
        Subscribe to keyevent notifications for specific event types.

        Args:
            event: The event type to monitor (e.g., EventType.SET or "set")
            db: The database number (default 0).
            handler: Optional callback for notifications.

        Example:
            >>> ksn.subscribe_keyevent(EventType.SET)
            >>> ksn.subscribe_keyevent(EventType.EXPIRED, handler=my_handler)
        """
        channel = KeyeventChannel(event, db=db)
        self.subscribe(channel, handler=handler)

    def subscribe_subkeyspace(
        self,
        key_or_pattern: str,
        db: int = 0,
        handler: SyncHandlerT | None = None,
    ):
        """
        Subscribe to subkeyspace notifications for specific keys.

        Receives events with affected subkeys (fields) for the given key.

        Args:
            key_or_pattern: The key or pattern to monitor.
            db: The database number (default 0).
            handler: Optional callback for notifications.
        """
        channel = SubkeyspaceChannel(key_or_pattern, db=db)
        self.subscribe(channel, handler=handler)

    def subscribe_subkeyevent(
        self,
        event: str,
        db: int = 0,
        handler: SyncHandlerT | None = None,
    ):
        """
        Subscribe to subkeyevent notifications for specific event types.

        Receives the affected key and subkeys when the given event occurs.

        Args:
            event: The event type to monitor (e.g., "hset", "hdel").
            db: The database number (default 0).
            handler: Optional callback for notifications.
        """
        channel = SubkeyeventChannel(event, db=db)
        self.subscribe(channel, handler=handler)

    def subscribe_subkeyspaceitem(
        self,
        key_or_pattern: str,
        subkey_or_pattern: str,
        db: int = 0,
        handler: SyncHandlerT | None = None,
    ):
        """
        Subscribe to subkeyspaceitem notifications for a specific subkey.

        Receives the event type when the given subkey of the given key is
        modified.

        Args:
            key_or_pattern: The key or pattern to monitor.
            subkey_or_pattern: The subkey (field) or pattern to monitor.
            db: The database number (default 0).
            handler: Optional callback for notifications.
        """
        channel = SubkeyspaceitemChannel(key_or_pattern, subkey_or_pattern, db=db)
        self.subscribe(channel, handler=handler)

    def subscribe_subkeyspaceevent(
        self,
        event: str,
        key_or_pattern: str,
        db: int = 0,
        handler: SyncHandlerT | None = None,
    ):
        """
        Subscribe to subkeyspaceevent notifications for an event on a key.

        Receives the affected subkeys when the given event occurs on the
        given key.

        Args:
            event: The event type to monitor.
            key_or_pattern: The key or pattern to monitor.
            db: The database number (default 0).
            handler: Optional callback for notifications.
        """
        channel = SubkeyspaceeventChannel(event, key_or_pattern, db=db)
        self.subscribe(channel, handler=handler)

    def __enter__(self):
        return self

    def __exit__(self, _exc_type, _exc_val, _exc_tb):
        self.close()
        return False

    def run_in_thread(
        self,
        poll_timeout: float = 0.0,
        daemon: bool = False,
        exception_handler: Callable[
            [
                BaseException,
                KeyspaceNotificationsInterface,
                KeyspaceWorkerThread,
            ],
            None,
        ]
        | None = None,
    ) -> KeyspaceWorkerThread:
        """
        Start a background thread that polls for notifications and triggers handlers.

        This method spawns a thread that continuously calls get_message() to
        process incoming notifications. When a notification arrives, any
        registered handler for that channel/pattern is invoked automatically.

        All subscriptions must have handlers registered before calling this method.

        Args:
            poll_timeout: Timeout in seconds for get_message() calls. When no message
                         is available, the thread waits up to this long before checking
                         again. Default 0.0 (non-blocking). WARNING: the default
                         causes a CPU spin-loop. It is preferred to pass a positive
                         value (e.g. 0.1 or 1.0).
            daemon: If True, the thread will be a daemon thread and will be
                   terminated when the main program exits. Default False.
            exception_handler: Optional callback invoked when an exception occurs
                              in the worker thread. Receives (exception, notifications,
                              thread) as arguments. If None, exceptions are raised.

        Returns:
            KeyspaceWorkerThread: The started worker thread. Call stop() on it
                                 to stop the thread and close the notifications.

        Raises:
            RedisError: If any subscription doesn't have a handler registered.

        Example:
            >>> def my_handler(notification):
            ...     print(f"Got: {notification.key} - {notification.event_type}")
            >>>
            >>> notifications.subscribe(KeyspaceChannel("user:*"), handler=my_handler)
            >>> thread = notifications.run_in_thread(poll_timeout=0.1, daemon=True)
            >>> # ... handlers are called automatically ...
            >>> thread.stop()
        """
        self._validate_all_handlers()

        thread = KeyspaceWorkerThread(
            self,
            poll_timeout,
            daemon=daemon,
            exception_handler=exception_handler,
        )
        thread.start()
        return thread

    @abstractmethod
    def _validate_all_handlers(self) -> None:
        """Raise :class:`~redis.RedisError` if any subscription lacks a handler.

        Subclasses inspect their own subscription state to perform
        the validation.
        """
        pass


class KeyspaceWorkerThread(threading.Thread):
    """
    Background thread for processing keyspace notifications.

    This thread continuously polls for notifications and invokes registered
    handlers. It works with both KeyspaceNotifications (standalone) and
    ClusterKeyspaceNotifications.

    Example:
        >>> thread = notifications.run_in_thread(poll_timeout=0.1)
        >>> # ... handlers are called automatically ...
        >>> thread.stop()
    """

    def __init__(
        self,
        notifications: KeyspaceNotificationsInterface,
        poll_timeout: float,
        daemon: bool = False,
        exception_handler: Callable[
            [
                BaseException,
                KeyspaceNotificationsInterface,
                KeyspaceWorkerThread,
            ],
            None,
        ]
        | None = None,
    ):
        super().__init__()
        self.daemon = daemon
        self.notifications = notifications
        self.poll_timeout = poll_timeout
        self.exception_handler = exception_handler
        self._running = threading.Event()

    def run(self) -> None:
        """Main loop that polls for notifications and triggers handlers."""
        if self._running.is_set():
            return
        self._running.set()
        notifications = self.notifications
        poll_timeout = self.poll_timeout
        while self._running.is_set():
            try:
                notifications.get_message(
                    ignore_subscribe_messages=True, timeout=poll_timeout
                )
            except BaseException as e:
                if self.exception_handler is None:
                    raise
                self.exception_handler(e, notifications, self)
        notifications.close()

    def stop(self) -> None:
        """
        Stop the worker thread.

        This signals the thread to exit its run loop. The thread will close
        the notifications object before terminating.
        """
        self._running.clear()


# =============================================================================
# Standalone Keyspace Notification Manager
# =============================================================================


class KeyspaceNotifications(AbstractKeyspaceNotifications):
    """
    Manages keyspace notification subscriptions for standalone Redis.

    For standalone Redis, keyspace notifications work with a single PubSub
    connection. This class wraps that connection and provides:
    - Automatic pattern vs exact channel detection
    - KeyNotification parsing with optional key_prefix filtering
    - Convenience methods for keyspace and keyevent subscriptions
    - Context manager and run_in_thread support
    """

    def __init__(
        self,
        redis_client: Redis,
        key_prefix: str | bytes | None = None,
        ignore_subscribe_messages: bool = True,
    ):
        """
        Initialize the standalone keyspace notification manager.

        Note: Keyspace notifications must be enabled on the Redis server via
        the ``notify-keyspace-events`` configuration option. This is a server-side
        configuration that should be done by your infrastructure/operations team.

        Args:
            redis_client: A Redis client instance
            key_prefix: Optional prefix to filter and strip from keys in notifications
            ignore_subscribe_messages: If True, subscribe/unsubscribe confirmations
                                      are not returned by get_message/listen
        """
        super().__init__(key_prefix, ignore_subscribe_messages)
        self.redis = redis_client

        # Create the PubSub instance with ignore_subscribe_messages=False
        # so that the per-call argument in get_message() can control behavior
        self._pubsub = redis_client.pubsub(ignore_subscribe_messages=False)

    def _execute_subscribe(
        self, patterns: dict[str, Any], exact_channels: dict[str, Any]
    ) -> None:
        """Execute subscribe on the single pubsub connection."""
        if patterns:
            self._pubsub.psubscribe(**patterns)
        if exact_channels:
            self._pubsub.subscribe(**exact_channels)

    def _execute_unsubscribe(
        self, patterns: list[str], exact_channels: list[str]
    ) -> None:
        """Execute unsubscribe on the single pubsub connection."""
        if patterns:
            self._pubsub.punsubscribe(*patterns)
        if exact_channels:
            self._pubsub.unsubscribe(*exact_channels)

    def get_message(
        self,
        ignore_subscribe_messages: bool | None = None,
        timeout: float = 0.0,
    ) -> KeyNotification | None:
        """
        Get the next keyspace notification if one is available.

        Note: If a handler was registered for the channel, pubsub will call
        the handler directly and this method returns None for that message.

        Args:
            ignore_subscribe_messages: If True, skip subscribe/unsubscribe messages.
                                      Defaults to the value set in __init__ (True).
            timeout: Time to wait for a message.

        Returns:
            A KeyNotification if a notification is available and no handler
            was registered for the channel, None otherwise.
        """
        if ignore_subscribe_messages is None:
            ignore_subscribe_messages = self.ignore_subscribe_messages

        if self._closed:
            return None

        # Pubsub's get_message will call wrapped handlers directly for channels
        # with registered handlers and return None. For channels without handlers,
        # it returns the raw message which we parse to KeyNotification.
        message = self._pubsub.get_message(
            ignore_subscribe_messages=ignore_subscribe_messages,
            timeout=timeout,
        )

        if message is not None:
            return KeyNotification.from_message(message, key_prefix=self.key_prefix)

        return None

    def listen(self):
        """
        Listen for keyspace notifications.

        This is a generator that yields KeyNotification objects as they arrive.
        It blocks until a notification is received.

        Yields:
            KeyNotification objects for each keyspace/keyevent notification.

        Example:
            >>> for notification in ksn.listen():
            ...     print(f"{notification.key}: {notification.event_type}")
        """
        while self.subscribed:
            notification = self.get_message(timeout=1.0)
            if notification is not None:
                yield notification

    @property
    def subscribed(self) -> bool:
        """Check if there are any active subscriptions and not closed."""
        return not self._closed and self._pubsub.subscribed

    def _validate_all_handlers(self) -> None:
        """Raise if any subscription in the underlying PubSub lacks a handler."""
        for channel, handler in self._pubsub.channels.items():
            if handler is None:
                raise RedisError(f"Channel '{channel}' has no handler registered")
        for pattern, handler in self._pubsub.patterns.items():
            if handler is None:
                raise RedisError(f"Pattern '{pattern}' has no handler registered")

    def close(self):
        """Close the pubsub connection and clean up resources."""
        self._closed = True
        try:
            self._pubsub.close()
        except Exception:
            pass


# =============================================================================
# Cluster-Aware Keyspace Notification Manager
# =============================================================================


class ClusterKeyspaceNotifications(AbstractKeyspaceNotifications):
    """
    Manages keyspace notification subscriptions across all nodes in a Redis Cluster.

    In Redis Cluster, keyspace notifications are NOT broadcast between nodes.
    Each node only emits notifications for keys it owns. This class automatically
    subscribes to all primary nodes in the cluster and handles topology changes.
    """

    def __init__(
        self,
        redis_cluster: RedisCluster,
        key_prefix: str | bytes | None = None,
        ignore_subscribe_messages: bool = True,
    ):
        """
        Initialize the cluster keyspace notification manager.

        Note: Keyspace notifications must be enabled on all Redis cluster nodes via
        the ``notify-keyspace-events`` configuration option. This is a server-side
        configuration that should be done by your infrastructure/operations team.

        Args:
            redis_cluster: A RedisCluster instance
            key_prefix: Optional prefix to filter and strip from keys in notifications
            ignore_subscribe_messages: If True, subscribe/unsubscribe confirmations
                                      are not returned by get_message/listen
        """
        super().__init__(key_prefix, ignore_subscribe_messages)
        self.cluster = redis_cluster

        # Canonical subscription registry: pattern/channel -> wrapped handler.
        # In cluster mode there are multiple PubSub objects (one per node), so
        # this is the single source of truth used to (re-)subscribe new or
        # failed-over nodes.
        self._subscribed_patterns: dict[str, Any] = {}
        self._subscribed_channels: dict[str, Any] = {}

        # Track subscriptions per node
        self._node_pubsubs: dict[str, Any] = {}

        # Lock for topology refresh operations
        self._refresh_lock = threading.Lock()

        # Current pubsub index for round-robin polling
        self._poll_index = 0

    @property
    def subscribed(self) -> bool:
        """Check if there are any active subscriptions and not closed."""
        return not self._closed and bool(
            self._subscribed_patterns or self._subscribed_channels
        )

    def _track_subscribe(
        self, patterns: dict[str, Any], exact_channels: dict[str, Any]
    ) -> None:
        """Track newly subscribed patterns/channels in the cluster registry."""
        if patterns:
            self._subscribed_patterns.update(patterns)
        if exact_channels:
            self._subscribed_channels.update(exact_channels)

    def _untrack_subscribe(
        self, patterns: list[str], exact_channels: list[str]
    ) -> None:
        """Remove patterns/channels from the cluster registry."""
        for p in patterns:
            self._subscribed_patterns.pop(p, None)
        for c in exact_channels:
            self._subscribed_channels.pop(c, None)

    def _validate_all_handlers(self) -> None:
        """Raise if any subscription in the cluster registry lacks a handler."""
        for channel, handler in self._subscribed_channels.items():
            if handler is None:
                raise RedisError(f"Channel '{channel}' has no handler registered")
        for pattern, handler in self._subscribed_patterns.items():
            if handler is None:
                raise RedisError(f"Pattern '{pattern}' has no handler registered")

    def _get_all_primary_nodes(self):
        """Get all primary nodes in the cluster."""
        return self.cluster.get_primaries()

    def _cleanup_node(self, node_name: str) -> None:
        """Remove and close a node's PubSub.

        Closing the ``PubSub`` disconnects its connection so it is not
        left in a subscribed state inside the connection pool.
        """
        pubsub = self._node_pubsubs.pop(node_name, None)
        if pubsub:
            try:
                pubsub.close()
            except Exception:
                pass

    def _ensure_node_pubsub(self, node) -> Any:
        """Get or create a PubSub instance for a node."""
        if node.name not in self._node_pubsubs:
            redis_conn = self.cluster.get_redis_connection(node)
            # Always create PubSub with ignore_subscribe_messages=False
            # so that the per-call argument in get_message() can control
            # the behavior reliably
            pubsub = redis_conn.pubsub(ignore_subscribe_messages=False)
            self._node_pubsubs[node.name] = pubsub
        return self._node_pubsubs[node.name]

    def _execute_subscribe(
        self, patterns: dict[str, Any], exact_channels: dict[str, Any]
    ) -> None:
        """Execute subscribe on all cluster nodes.

        Patterns and exact channels are subscribed in a single pass over
        nodes so that a mid-batch node failure cannot create a
        partially-caught-up replacement.  If a node fails during this
        call it is removed from ``_node_pubsubs`` and will be fully
        re-subscribed on the next ``refresh_subscriptions`` cycle.

        If a newly discovered node is encountered (not yet in
        ``_node_pubsubs``), it is also subscribed to all *previously*
        tracked patterns/channels so it doesn't miss notifications for
        subscriptions that were established before this node joined.
        """
        if not patterns and not exact_channels:
            return

        failed_nodes: list[str] = []
        for node in self._get_all_primary_nodes():
            is_new_node = node.name not in self._node_pubsubs
            pubsub = self._ensure_node_pubsub(node)
            try:
                # If this is a brand-new node, catch it up on existing
                # subscriptions before adding the new channels.
                if is_new_node:
                    if self._subscribed_patterns:
                        pubsub.psubscribe(**self._subscribed_patterns)
                    if self._subscribed_channels:
                        pubsub.subscribe(**self._subscribed_channels)

                if patterns:
                    pubsub.psubscribe(**patterns)
                if exact_channels:
                    pubsub.subscribe(**exact_channels)
            except Exception:
                # Remove the broken pubsub so refresh_subscriptions can
                # re-create it later.
                self._cleanup_node(node.name)
                failed_nodes.append(node.name)

        if failed_nodes:
            logger.warning(
                "Failed to subscribe on cluster nodes: %s. "
                "These nodes will be retried on the next refresh cycle.",
                ", ".join(failed_nodes),
            )

    def _execute_unsubscribe(
        self, patterns: list[str], exact_channels: list[str]
    ) -> None:
        """Execute unsubscribe on all cluster nodes."""
        if patterns:
            self._unsubscribe_from_all_nodes(patterns, use_punsubscribe=True)
        if exact_channels:
            self._unsubscribe_from_all_nodes(exact_channels, use_punsubscribe=False)

    def _unsubscribe_from_all_nodes(self, channels: list[str], use_punsubscribe: bool):
        """Unsubscribe from patterns/channels on all nodes.

        Best-effort: tries every node so that a single broken connection
        does not prevent the remaining nodes from being unsubscribed.
        Broken pubsubs are cleaned up; the tracking state is still removed
        by the caller, so ``refresh_subscriptions`` will *not* re-subscribe
        these channels on replacement nodes.
        """
        failed_nodes: list[str] = []
        for node_name, pubsub in list(self._node_pubsubs.items()):
            try:
                if use_punsubscribe:
                    pubsub.punsubscribe(*channels)
                else:
                    pubsub.unsubscribe(*channels)
            except Exception:
                self._cleanup_node(node_name)
                failed_nodes.append(node_name)

        if failed_nodes:
            logger.warning(
                "Failed to unsubscribe on cluster nodes: %s. "
                "These nodes will be re-created on the next refresh cycle.",
                ", ".join(failed_nodes),
            )

    def get_message(
        self,
        ignore_subscribe_messages: bool | None = None,
        timeout: float = 0.0,
    ) -> KeyNotification | None:
        """
        Get the next keyspace notification if one is available.

        This method polls all node pubsubs in round-robin fashion until
        a message is received or the timeout expires.
        If a connection error occurs, subscriptions are automatically refreshed.

        Args:
            ignore_subscribe_messages: If True, skip subscribe/unsubscribe messages.
                                      Defaults to the value set in __init__ (True).
            timeout: Total time to wait for a message (distributed across all nodes)

        Returns:
            A KeyNotification if a notification is available, None otherwise.
        """
        if self._closed:
            return None

        total_nodes = len(self._node_pubsubs)
        if total_nodes == 0:
            # Sleep for the requested timeout so callers that loop
            # (run_in_thread, listen) don't spin the CPU when all node
            # connections have been cleaned up.
            if timeout > 0:
                time.sleep(timeout)
            return None

        # Use instance default if not specified
        if ignore_subscribe_messages is None:
            ignore_subscribe_messages = self.ignore_subscribe_messages

        # Handle timeout=0 as a single non-blocking poll over all pubsubs
        # This matches the expected semantics of PubSub.get_message(timeout=0)
        if timeout == 0.0:
            return self._poll_all_nodes_once(ignore_subscribe_messages)

        # Calculate per-node timeout for each poll
        # Use a small timeout per node to allow round-robin polling
        per_node_timeout = min(0.1, timeout / max(total_nodes, 1))

        start_time = time.monotonic()
        end_time = start_time + timeout

        while True:
            # Check if we've exceeded the total timeout
            if time.monotonic() >= end_time:
                return None

            pubsubs = list(self._node_pubsubs.values())
            if not pubsubs:
                return None

            # Round-robin polling
            self._poll_index = self._poll_index % len(pubsubs)
            pubsub = pubsubs[self._poll_index]
            self._poll_index += 1

            try:
                message = pubsub.get_message(
                    ignore_subscribe_messages=ignore_subscribe_messages,
                    timeout=per_node_timeout,
                )
            except (ConnectionError, TimeoutError, RedisError):
                # Connection error - refresh subscriptions and continue
                self._refresh_subscriptions_on_error()
                continue

            if message is not None:
                # Note: If a handler was registered, PubSub already invoked it
                # and returned None, so we only reach here for handler-less subscriptions
                notification = KeyNotification.from_message(
                    message, key_prefix=self.key_prefix
                )
                if notification is not None:
                    return notification
                # If not a keyspace notification, continue checking other nodes

    def _poll_all_nodes_once(
        self, ignore_subscribe_messages: bool
    ) -> KeyNotification | None:
        """
        Perform a single non-blocking poll over all node pubsubs.

        This is used when timeout=0 to match the expected semantics of
        PubSub.get_message(timeout=0) - a non-blocking check for messages.

        Returns:
            A KeyNotification if one is available, None otherwise.
        """
        had_error = False
        for pubsub in list(self._node_pubsubs.values()):
            try:
                message = pubsub.get_message(
                    ignore_subscribe_messages=ignore_subscribe_messages,
                    timeout=0.0,
                )
            except (ConnectionError, TimeoutError, RedisError):
                # Record the error but continue polling remaining healthy
                # nodes so that already-buffered notifications are not lost.
                had_error = True
                continue

            if message is not None:
                # Note: If a handler was registered, PubSub already invoked it
                # and returned None, so we only reach here for handler-less subscriptions
                notification = KeyNotification.from_message(
                    message, key_prefix=self.key_prefix
                )
                if notification is not None:
                    # Refresh before returning if any node had an error,
                    # so the next poll cycle has fresh state.
                    if had_error:
                        self._refresh_subscriptions_on_error()
                    return notification

        # Refresh after polling all nodes if any had errors
        if had_error:
            self._refresh_subscriptions_on_error()
        return None

    def listen(self):
        """
        Listen for keyspace notifications from all cluster nodes.

        This is a generator that yields KeyNotification objects as they arrive.
        It blocks until a notification is received.

        Yields:
            KeyNotification objects for each keyspace/keyevent notification.

        Example:
            >>> for notification in ksn.listen():
            ...     print(f"{notification.key}: {notification.event_type}")
        """
        while self.subscribed:
            notification = self.get_message(timeout=1.0)
            if notification is not None:
                yield notification

    def _refresh_subscriptions_on_error(self):
        """
        Refresh subscriptions after a connection error.

        This is called automatically when a connection error occurs during
        get_message(). It checks if nodes changed before refreshing.
        """
        self._poll_index = 0  # Reset round-robin index

        try:
            self.refresh_subscriptions()
        except Exception:
            logger.warning(
                "Failed to refresh cluster subscriptions, will retry on next error",
                exc_info=True,
            )

    def _is_pubsub_connected(self, pubsub) -> bool:
        """Check if a pubsub connection is still alive."""
        try:
            conn = pubsub.connection
            if conn is None:
                return False
            return conn.is_connected
        except Exception:
            return False

    def refresh_subscriptions(self):
        """
        Refresh subscriptions after a topology change.

        This method is called automatically when topology changes are detected
        or when connection errors occur. You can also call it manually if needed.

        This method:
        1. Discovers any new primary nodes and subscribes them
        2. Removes pubsubs for nodes that are no longer primaries
        3. Re-creates broken pubsub connections for existing nodes
        """
        with self._refresh_lock:
            current_primaries = {
                node.name: node for node in self._get_all_primary_nodes()
            }

            # Remove pubsubs for nodes that are no longer primaries
            removed_nodes = set(self._node_pubsubs.keys()) - set(
                current_primaries.keys()
            )
            for node_name in removed_nodes:
                self._cleanup_node(node_name)

            # Detect broken connections for existing nodes and remove them
            # so they get re-created below
            existing_nodes = set(self._node_pubsubs.keys()) & set(
                current_primaries.keys()
            )
            for node_name in existing_nodes:
                pubsub = self._node_pubsubs.get(node_name)
                if pubsub and not self._is_pubsub_connected(pubsub):
                    # Connection is broken, remove it so it gets re-created
                    self._cleanup_node(node_name)

            # Subscribe new nodes (and nodes with broken connections) to existing
            # patterns/channels
            new_nodes = set(current_primaries.keys()) - set(self._node_pubsubs.keys())
            failed_nodes: list[str] = []
            for node_name in new_nodes:
                node = current_primaries[node_name]
                pubsub = self._ensure_node_pubsub(node)

                try:
                    if self._subscribed_patterns:
                        pubsub.psubscribe(**self._subscribed_patterns)
                    if self._subscribed_channels:
                        pubsub.subscribe(**self._subscribed_channels)
                except Exception:
                    # Subscription failed - remove from dict so retry is possible
                    self._cleanup_node(node_name)
                    failed_nodes.append(node_name)

            # Raise after attempting all nodes so we don't skip any
            if failed_nodes:
                raise ConnectionError(
                    f"Failed to subscribe to cluster nodes: {', '.join(failed_nodes)}"
                )

    def close(self):
        """Close all pubsub connections and clean up resources."""
        self._closed = True
        for node_name in list(self._node_pubsubs.keys()):
            self._cleanup_node(node_name)
        self._subscribed_patterns.clear()
        self._subscribed_channels.clear()
