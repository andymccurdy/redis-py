from __future__ import annotations

import datetime
import hashlib
import inspect

# Try to import the xxhash library as an optional dependency
try:
    import xxhash

    HAS_XXHASH = True
except ImportError:
    HAS_XXHASH = False

import warnings
from enum import Enum
from typing import (
    TYPE_CHECKING,
    Any,
    AsyncIterator,
    Awaitable,
    Callable,
    Dict,
    Iterable,
    Iterator,
    List,
    Literal,
    Mapping,
    Optional,
    Sequence,
    Tuple,
    Union,
    overload,
)

from redis.asyncio.observability.recorder import (
    record_streaming_lag_from_response as async_record_streaming_lag,
)
from redis.exceptions import ConnectionError, DataError, NoScriptError, RedisError
from redis.himport import (
    HIMPORT_DISCARD,
    HIMPORT_DISCARDALL,
    HIMPORT_PREPARE,
    HIMPORT_SET,
)
from redis.typing import (
    AbsExpiryT,
    ACLGetUserData,
    ACLLogData,
    AnyKeyT,
    AsyncClientProtocol,
    BitfieldOffsetT,
    BlockingListPopResponse,
    BlockingZSetPopResponse,
    ChannelT,
    ClientTrackingInfoResponse,
    CommandGetKeysAndFlagsResponse,
    CommandsProtocol,
    ConsumerT,
    EncodableT,
    ExpiryT,
    FieldT,
    GeoCoordinate,
    GeoRadiusResponse,
    GeoSearchResponse,
    GroupT,
    HRandFieldResponse,
    HScanResponse,
    KeysT,
    KeyT,
    LCSCommandResponse,
    ListMultiPopResponse,
    Number,
    PatternT,
    ResponseT,
    ScanResponse,
    ScriptTextT,
    SortResponse,
    StralgoResponse,
    StreamIdT,
    StreamRangeResponse,
    SyncClientProtocol,
    TimeoutSecT,
    XClaimResponse,
    XPendingRangeResponse,
    XReadGroupResponse,
    XReadResponse,
    ZMPopResponse,
    ZRandMemberResponse,
    ZScanResponse,
    ZScoreBoundT,
    ZSetRangeResponse,
)
from redis.utils import (
    experimental_args,
    experimental_method,
    extract_expire_flags,
    str_if_bytes,
)

from ..observability.attributes import PubSubDirection
from ..observability.recorder import (
    record_pubsub_message,
    record_streaming_lag_from_response,
)
from .helpers import at_most_one_value_set, list_or_args

if TYPE_CHECKING:
    import redis.asyncio.client
    import redis.asyncio.cluster
    import redis.client
    import redis.cluster


class ACLCommands(CommandsProtocol):
    """
    Redis Access Control List (ACL) commands.
    see: https://redis.io/topics/acl
    """

    @overload
    def acl_cat(
        self: SyncClientProtocol, category: str | None = None, **kwargs
    ) -> list[bytes | str]: ...

    @overload
    def acl_cat(
        self: AsyncClientProtocol, category: str | None = None, **kwargs
    ) -> Awaitable[list[bytes | str]]: ...

    def acl_cat(
        self, category: str | None = None, **kwargs
    ) -> list[bytes | str] | Awaitable[list[bytes | str]]:
        """
        Returns a list of categories or commands within a category.

        If ``category`` is not supplied, returns a list of all categories.
        If ``category`` is supplied, returns a list of all commands within
        that category.

        For more information, see https://redis.io/commands/acl-cat
        """
        pieces: list[EncodableT] = [category] if category else []
        return self.execute_command("ACL CAT", *pieces, **kwargs)

    @overload
    def acl_dryrun(
        self: SyncClientProtocol, username: str, *args: EncodableT, **kwargs
    ) -> bytes | str: ...

    @overload
    def acl_dryrun(
        self: AsyncClientProtocol, username: str, *args: EncodableT, **kwargs
    ) -> Awaitable[bytes | str]: ...

    def acl_dryrun(self, username: str, *args: EncodableT, **kwargs) -> (
        bytes | str
    ) | Awaitable[bytes | str]:
        """
        Simulate the execution of a given command by a given ``username``.

        For more information, see https://redis.io/commands/acl-dryrun
        """
        return self.execute_command("ACL DRYRUN", username, *args, **kwargs)

    @overload
    def acl_deluser(self: SyncClientProtocol, *username: str, **kwargs) -> int: ...

    @overload
    def acl_deluser(
        self: AsyncClientProtocol, *username: str, **kwargs
    ) -> Awaitable[int]: ...

    def acl_deluser(self, *username: str, **kwargs) -> int | Awaitable[int]:
        """
        Delete the ACL for the specified ``username``\\s

        For more information, see https://redis.io/commands/acl-deluser
        """
        return self.execute_command("ACL DELUSER", *username, **kwargs)

    @overload
    def acl_genpass(
        self: SyncClientProtocol, bits: int | None = None, **kwargs
    ) -> bytes | str: ...

    @overload
    def acl_genpass(
        self: AsyncClientProtocol, bits: int | None = None, **kwargs
    ) -> Awaitable[bytes | str]: ...

    def acl_genpass(self, bits: int | None = None, **kwargs) -> (
        bytes | str
    ) | Awaitable[bytes | str]:
        """Generate a random password value.
        If ``bits`` is supplied then use this number of bits, rounded to
        the next multiple of 4.
        See: https://redis.io/commands/acl-genpass
        """
        pieces = []
        if bits is not None:
            try:
                b = int(bits)
                if b < 0 or b > 4096:
                    raise ValueError
                pieces.append(b)
            except ValueError:
                raise DataError(
                    "genpass optionally accepts a bits argument, between 0 and 4096."
                )
        return self.execute_command("ACL GENPASS", *pieces, **kwargs)

    @overload
    def acl_getuser(
        self: SyncClientProtocol, username: str, **kwargs
    ) -> ACLGetUserData: ...

    @overload
    def acl_getuser(
        self: AsyncClientProtocol, username: str, **kwargs
    ) -> Awaitable[ACLGetUserData]: ...

    def acl_getuser(
        self, username: str, **kwargs
    ) -> ACLGetUserData | Awaitable[ACLGetUserData]:
        """
        Get the ACL details for the specified ``username``.

        If ``username`` does not exist, return None

        For more information, see https://redis.io/commands/acl-getuser
        """
        return self.execute_command("ACL GETUSER", username, **kwargs)

    @overload
    def acl_help(self: SyncClientProtocol, **kwargs) -> list[bytes | str]: ...

    @overload
    def acl_help(
        self: AsyncClientProtocol, **kwargs
    ) -> Awaitable[list[bytes | str]]: ...

    def acl_help(self, **kwargs) -> list[bytes | str] | Awaitable[list[bytes | str]]:
        """The ACL HELP command returns helpful text describing
        the different subcommands.

        For more information, see https://redis.io/commands/acl-help
        """
        return self.execute_command("ACL HELP", **kwargs)

    @overload
    def acl_list(self: SyncClientProtocol, **kwargs) -> list[bytes | str]: ...

    @overload
    def acl_list(
        self: AsyncClientProtocol, **kwargs
    ) -> Awaitable[list[bytes | str]]: ...

    def acl_list(self, **kwargs) -> list[bytes | str] | Awaitable[list[bytes | str]]:
        """
        Return a list of all ACLs on the server

        For more information, see https://redis.io/commands/acl-list
        """
        return self.execute_command("ACL LIST", **kwargs)

    @overload
    def acl_log(
        self: SyncClientProtocol, count: int | None = None, **kwargs
    ) -> ACLLogData: ...

    @overload
    def acl_log(
        self: AsyncClientProtocol, count: int | None = None, **kwargs
    ) -> Awaitable[ACLLogData]: ...

    def acl_log(
        self, count: int | None = None, **kwargs
    ) -> ACLLogData | Awaitable[ACLLogData]:
        """
        Get ACL logs as a list.
        :param int count: Get logs[0:count].
        :rtype: List.

        For more information, see https://redis.io/commands/acl-log
        """
        args = []
        if count is not None:
            if not isinstance(count, int):
                raise DataError("ACL LOG count must be an integer")
            args.append(count)

        return self.execute_command("ACL LOG", *args, **kwargs)

    @overload
    def acl_log_reset(self: SyncClientProtocol, **kwargs) -> bool: ...

    @overload
    def acl_log_reset(self: AsyncClientProtocol, **kwargs) -> Awaitable[bool]: ...

    def acl_log_reset(self, **kwargs) -> bool | Awaitable[bool]:
        """
        Reset ACL logs.
        :rtype: Boolean.

        For more information, see https://redis.io/commands/acl-log
        """
        args = [b"RESET"]
        return self.execute_command("ACL LOG", *args, **kwargs)

    @overload
    def acl_load(self: SyncClientProtocol, **kwargs) -> bool: ...

    @overload
    def acl_load(self: AsyncClientProtocol, **kwargs) -> Awaitable[bool]: ...

    def acl_load(self, **kwargs) -> bool | Awaitable[bool]:
        """
        Load ACL rules from the configured ``aclfile``.

        Note that the server must be configured with the ``aclfile``
        directive to be able to load ACL rules from an aclfile.

        For more information, see https://redis.io/commands/acl-load
        """
        return self.execute_command("ACL LOAD", **kwargs)

    @overload
    def acl_save(self: SyncClientProtocol, **kwargs) -> bool: ...

    @overload
    def acl_save(self: AsyncClientProtocol, **kwargs) -> Awaitable[bool]: ...

    def acl_save(self, **kwargs) -> bool | Awaitable[bool]:
        """
        Save ACL rules to the configured ``aclfile``.

        Note that the server must be configured with the ``aclfile``
        directive to be able to save ACL rules to an aclfile.

        For more information, see https://redis.io/commands/acl-save
        """
        return self.execute_command("ACL SAVE", **kwargs)

    @overload
    def acl_setuser(
        self: SyncClientProtocol,
        username: str,
        enabled: bool = False,
        nopass: bool = False,
        passwords: str | Iterable[str] | None = None,
        hashed_passwords: str | Iterable[str] | None = None,
        categories: Iterable[str] | None = None,
        commands: Iterable[str] | None = None,
        keys: Iterable[KeyT] | None = None,
        channels: Iterable[ChannelT] | None = None,
        selectors: Iterable[Tuple[str, KeyT]] | None = None,
        reset: bool = False,
        reset_keys: bool = False,
        reset_channels: bool = False,
        reset_passwords: bool = False,
        **kwargs,
    ) -> bool: ...

    @overload
    def acl_setuser(
        self: AsyncClientProtocol,
        username: str,
        enabled: bool = False,
        nopass: bool = False,
        passwords: str | Iterable[str] | None = None,
        hashed_passwords: str | Iterable[str] | None = None,
        categories: Iterable[str] | None = None,
        commands: Iterable[str] | None = None,
        keys: Iterable[KeyT] | None = None,
        channels: Iterable[ChannelT] | None = None,
        selectors: Iterable[Tuple[str, KeyT]] | None = None,
        reset: bool = False,
        reset_keys: bool = False,
        reset_channels: bool = False,
        reset_passwords: bool = False,
        **kwargs,
    ) -> Awaitable[bool]: ...

    def acl_setuser(
        self,
        username: str,
        enabled: bool = False,
        nopass: bool = False,
        passwords: str | Iterable[str] | None = None,
        hashed_passwords: str | Iterable[str] | None = None,
        categories: Iterable[str] | None = None,
        commands: Iterable[str] | None = None,
        keys: Iterable[KeyT] | None = None,
        channels: Iterable[ChannelT] | None = None,
        selectors: Iterable[Tuple[str, KeyT]] | None = None,
        reset: bool = False,
        reset_keys: bool = False,
        reset_channels: bool = False,
        reset_passwords: bool = False,
        **kwargs,
    ) -> bool | Awaitable[bool]:
        """
        Create or update an ACL user.

        Create or update the ACL for `username`. If the user already exists,
        the existing ACL is completely overwritten and replaced with the
        specified values.

        For more information, see https://redis.io/commands/acl-setuser

        Args:
            username: The name of the user whose ACL is to be created or updated.
            enabled: Indicates whether the user should be allowed to authenticate.
                     Defaults to `False`.
            nopass: Indicates whether the user can authenticate without a password.
                    This cannot be `True` if `passwords` are also specified.
            passwords: A list of plain text passwords to add to or remove from the user.
                       Each password must be prefixed with a '+' to add or a '-' to
                       remove. For convenience, a single prefixed string can be used
                       when adding or removing a single password.
            hashed_passwords: A list of SHA-256 hashed passwords to add to or remove
                              from the user. Each hashed password must be prefixed with
                              a '+' to add or a '-' to remove. For convenience, a single
                              prefixed string can be used when adding or removing a
                              single password.
            categories: A list of strings representing category permissions. Each string
                        must be prefixed with either a '+' to add the category
                        permission or a '-' to remove the category permission.
            commands: A list of strings representing command permissions. Each string
                      must be prefixed with either a '+' to add the command permission
                      or a '-' to remove the command permission.
            keys: A list of key patterns to grant the user access to. Key patterns allow
                  ``'*'`` to support wildcard matching. For example, ``'*'`` grants
                  access to all keys while ``'cache:*'`` grants access to all keys that
                  are prefixed with ``cache:``.
                  `keys` should not be prefixed with a ``'~'``.
            selectors: A list of ``(command, key)`` tuples, each added as one
                       selector. `command` must be prefixed with a '+' to add or
                       a '-' to remove the command permission, on keys matching
                       `key`. A ``'~'`` is prepended to `key` unless it
                       already starts with ``'~'`` or ``'%'``.
                       Both values are joined into a single selector string,
                       ``(command key)``, which the server splits on whitespace
                       and applies token by token. Several commands or key
                       patterns can therefore be packed into one tuple, e.g.
                       ``("+get +set", "~app:*")``, but any whitespace in either
                       value starts a new rule. Do not build these fields from
                       unvalidated input such as a tenant or user name.
            reset: Indicates whether the user should be fully reset prior to applying
                   the new ACL. Setting this to `True` will remove all existing
                   passwords, flags, and privileges from the user and then apply the
                   specified rules. If `False`, the user's existing passwords, flags,
                   and privileges will be kept and any new specified rules will be
                   applied on top.
            reset_keys: Indicates whether the user's key permissions should be reset
                        prior to applying any new key permissions specified in `keys`.
                        If `False`, the user's existing key permissions will be kept and
                        any new specified key permissions will be applied on top.
            reset_channels: Indicates whether the user's channel permissions should be
                            reset prior to applying any new channel permissions
                            specified in `channels`. If `False`, the user's existing
                            channel permissions will be kept and any new specified
                            channel permissions will be applied on top.
            reset_passwords: Indicates whether to remove all existing passwords and the
                             `nopass` flag from the user prior to applying any new
                             passwords specified in `passwords` or `hashed_passwords`.
                             If `False`, the user's existing passwords and `nopass`
                             status will be kept and any new specified passwords or
                             hashed passwords will be applied on top.
        """
        encoder = self.get_encoder()
        pieces: List[EncodableT] = [username]

        if reset:
            pieces.append(b"reset")

        if reset_keys:
            pieces.append(b"resetkeys")

        if reset_channels:
            pieces.append(b"resetchannels")

        if reset_passwords:
            pieces.append(b"resetpass")

        if enabled:
            pieces.append(b"on")
        else:
            pieces.append(b"off")

        if (passwords or hashed_passwords) and nopass:
            raise DataError(
                "Cannot set 'nopass' and supply 'passwords' or 'hashed_passwords'"
            )

        if passwords:
            # as most users will have only one password, allow remove_passwords
            # to be specified as a simple string or a list
            passwords = list_or_args(passwords, [])
            for i, password in enumerate(passwords):
                password = encoder.encode(password)
                if password.startswith(b"+"):
                    pieces.append(b">%s" % password[1:])
                elif password.startswith(b"-"):
                    pieces.append(b"<%s" % password[1:])
                else:
                    raise DataError(
                        f"Password {i} must be prefixed with a "
                        f'"+" to add or a "-" to remove'
                    )

        if hashed_passwords:
            # as most users will have only one password, allow remove_passwords
            # to be specified as a simple string or a list
            hashed_passwords = list_or_args(hashed_passwords, [])
            for i, hashed_password in enumerate(hashed_passwords):
                hashed_password = encoder.encode(hashed_password)
                if hashed_password.startswith(b"+"):
                    pieces.append(b"#%s" % hashed_password[1:])
                elif hashed_password.startswith(b"-"):
                    pieces.append(b"!%s" % hashed_password[1:])
                else:
                    raise DataError(
                        f"Hashed password {i} must be prefixed with a "
                        f'"+" to add or a "-" to remove'
                    )

        if nopass:
            pieces.append(b"nopass")

        if categories:
            for category in categories:
                category = encoder.encode(category)
                # categories can be prefixed with one of (+@, +, -@, -)
                if category.startswith(b"+@"):
                    pieces.append(category)
                elif category.startswith(b"+"):
                    pieces.append(b"+@%s" % category[1:])
                elif category.startswith(b"-@"):
                    pieces.append(category)
                elif category.startswith(b"-"):
                    pieces.append(b"-@%s" % category[1:])
                else:
                    raise DataError(
                        f'Category "{encoder.decode(category, force=True)}" '
                        'must be prefixed with "+" or "-"'
                    )
        if commands:
            for cmd in commands:
                cmd = encoder.encode(cmd)
                if not cmd.startswith(b"+") and not cmd.startswith(b"-"):
                    raise DataError(
                        f'Command "{encoder.decode(cmd, force=True)}" '
                        'must be prefixed with "+" or "-"'
                    )
                pieces.append(cmd)

        if keys:
            for key in keys:
                key = encoder.encode(key)
                if not key.startswith(b"%") and not key.startswith(b"~"):
                    key = b"~%s" % key
                pieces.append(key)

        if channels:
            for channel in channels:
                channel = encoder.encode(channel)
                pieces.append(b"&%s" % channel)

        if selectors:
            for cmd, key in selectors:
                cmd = encoder.encode(cmd)
                if not cmd.startswith(b"+") and not cmd.startswith(b"-"):
                    raise DataError(
                        f'Command "{encoder.decode(cmd, force=True)}" '
                        'must be prefixed with "+" or "-"'
                    )

                key = encoder.encode(key)
                if not key.startswith(b"%") and not key.startswith(b"~"):
                    key = b"~%s" % key

                pieces.append(b"(%s %s)" % (cmd, key))

        return self.execute_command("ACL SETUSER", *pieces, **kwargs)

    @overload
    def acl_users(self: SyncClientProtocol, **kwargs) -> list[bytes | str]: ...

    @overload
    def acl_users(
        self: AsyncClientProtocol, **kwargs
    ) -> Awaitable[list[bytes | str]]: ...

    def acl_users(self, **kwargs) -> list[bytes | str] | Awaitable[list[bytes | str]]:
        """Returns a list of all registered users on the server.

        For more information, see https://redis.io/commands/acl-users
        """
        return self.execute_command("ACL USERS", **kwargs)

    @overload
    def acl_whoami(self: SyncClientProtocol, **kwargs) -> bytes | str: ...

    @overload
    def acl_whoami(self: AsyncClientProtocol, **kwargs) -> Awaitable[bytes | str]: ...

    def acl_whoami(self, **kwargs) -> (bytes | str) | Awaitable[bytes | str]:
        """Get the username for the current connection

        For more information, see https://redis.io/commands/acl-whoami
        """
        return self.execute_command("ACL WHOAMI", **kwargs)


AsyncACLCommands = ACLCommands


class HotkeysMetricsTypes(Enum):
    CPU = "CPU"
    NET = "NET"


class ManagementCommands(CommandsProtocol):
    """
    Redis management commands
    """

    @overload
    def auth(
        self: SyncClientProtocol,
        password: str,
        username: str | None = None,
        **kwargs,
    ) -> bool: ...

    @overload
    def auth(
        self: AsyncClientProtocol,
        password: str,
        username: str | None = None,
        **kwargs,
    ) -> Awaitable[bool]: ...

    def auth(
        self, password: str, username: str | None = None, **kwargs
    ) -> bool | Awaitable[bool]:
        """
        Authenticates the user. If you do not pass username, Redis will try to
        authenticate for the "default" user. If you do pass username, it will
        authenticate for the given user.
        For more information, see https://redis.io/commands/auth
        """
        pieces = []
        if username is not None:
            pieces.append(username)
        pieces.append(password)
        return self.execute_command("AUTH", *pieces, **kwargs)

    @overload
    def bgrewriteaof(self: SyncClientProtocol, **kwargs) -> bool | bytes | str: ...

    @overload
    def bgrewriteaof(
        self: AsyncClientProtocol, **kwargs
    ) -> Awaitable[bool | bytes | str]: ...

    def bgrewriteaof(self, **kwargs) -> (bool | bytes | str) | Awaitable[
        bool | bytes | str
    ]:
        """Tell the Redis server to rewrite the AOF file from data in memory.

        For more information, see https://redis.io/commands/bgrewriteaof
        """
        return self.execute_command("BGREWRITEAOF", **kwargs)

    @overload
    def bgsave(
        self: SyncClientProtocol, schedule: bool = True, **kwargs
    ) -> bool | bytes | str: ...

    @overload
    def bgsave(
        self: AsyncClientProtocol, schedule: bool = True, **kwargs
    ) -> Awaitable[bool | bytes | str]: ...

    def bgsave(self, schedule: bool = True, **kwargs) -> (
        bool | bytes | str
    ) | Awaitable[bool | bytes | str]:
        """
        Tell the Redis server to save its data to disk.  Unlike save(),
        this method is asynchronous and returns immediately.

        For more information, see https://redis.io/commands/bgsave
        """
        pieces = []
        if schedule:
            pieces.append("SCHEDULE")
        return self.execute_command("BGSAVE", *pieces, **kwargs)

    @overload
    def role(self: SyncClientProtocol) -> list[Any]: ...

    @overload
    def role(self: AsyncClientProtocol) -> Awaitable[list[Any]]: ...

    def role(self) -> list[Any] | Awaitable[list[Any]]:
        """
        Provide information on the role of a Redis instance in
        the context of replication, by returning if the instance
        is currently a master, slave, or sentinel.

        For more information, see https://redis.io/commands/role
        """
        return self.execute_command("ROLE")

    @overload
    def client_kill(self: SyncClientProtocol, address: str, **kwargs) -> bool | int: ...

    @overload
    def client_kill(
        self: AsyncClientProtocol, address: str, **kwargs
    ) -> Awaitable[bool | int]: ...

    def client_kill(self, address: str, **kwargs) -> (bool | int) | Awaitable[
        bool | int
    ]:
        """Disconnects the client at ``address`` (ip:port)

        For more information, see https://redis.io/commands/client-kill
        """
        return self.execute_command("CLIENT KILL", address, **kwargs)

    @overload
    def client_kill_filter(
        self: SyncClientProtocol,
        _id: str | None = None,
        _type: str | None = None,
        addr: str | None = None,
        skipme: bool | None = None,
        laddr: bool | None = None,
        user: str | None = None,
        maxage: int | None = None,
        **kwargs,
    ) -> int: ...

    @overload
    def client_kill_filter(
        self: AsyncClientProtocol,
        _id: str | None = None,
        _type: str | None = None,
        addr: str | None = None,
        skipme: bool | None = None,
        laddr: bool | None = None,
        user: str | None = None,
        maxage: int | None = None,
        **kwargs,
    ) -> Awaitable[int]: ...

    def client_kill_filter(
        self,
        _id: str | None = None,
        _type: str | None = None,
        addr: str | None = None,
        skipme: bool | None = None,
        laddr: bool | None = None,
        user: str | None = None,
        maxage: int | None = None,
        **kwargs,
    ) -> int | Awaitable[int]:
        """
        Disconnects client(s) using a variety of filter options
        :param _id: Kills a client by its unique ID field
        :param _type: Kills a client by type where type is one of 'normal',
        'master', 'slave', 'replica' or 'pubsub'
        :param addr: Kills a client by its 'address:port'
        :param skipme: If True, then the client calling the command
        will not get killed even if it is identified by one of the filter
        options. If skipme is not provided, the server defaults to skipme=True
        :param laddr: Kills a client by its 'local (bind) address:port'
        :param user: Kills a client for a specific user name
        :param maxage: Kills clients that are older than the specified age in seconds
        """
        args = []
        if _type is not None:
            client_types = ("normal", "master", "slave", "replica", "pubsub")
            if str(_type).lower() not in client_types:
                raise DataError(f"CLIENT KILL type must be one of {client_types!r}")
            args.extend((b"TYPE", _type))
        if skipme is not None:
            if not isinstance(skipme, bool):
                raise DataError("CLIENT KILL skipme must be a bool")
            if skipme:
                args.extend((b"SKIPME", b"YES"))
            else:
                args.extend((b"SKIPME", b"NO"))
        if _id is not None:
            args.extend((b"ID", _id))
        if addr is not None:
            args.extend((b"ADDR", addr))
        if laddr is not None:
            args.extend((b"LADDR", laddr))
        if user is not None:
            args.extend((b"USER", user))
        if maxage is not None:
            args.extend((b"MAXAGE", maxage))
        if not args:
            raise DataError(
                "CLIENT KILL <filter> <value> ... ... <filter> "
                "<value> must specify at least one filter"
            )
        return self.execute_command("CLIENT KILL", *args, **kwargs)

    @overload
    def client_info(self: SyncClientProtocol, **kwargs) -> dict[str, str | int]: ...

    @overload
    def client_info(
        self: AsyncClientProtocol, **kwargs
    ) -> Awaitable[dict[str, str | int]]: ...

    def client_info(
        self, **kwargs
    ) -> dict[str, str | int] | Awaitable[dict[str, str | int]]:
        """
        Returns information and statistics about the current
        client connection.

        For more information, see https://redis.io/commands/client-info
        """
        return self.execute_command("CLIENT INFO", **kwargs)

    @overload
    def client_list(
        self: SyncClientProtocol,
        _type: str | None = None,
        client_id: List[EncodableT] = [],
        **kwargs,
    ) -> list[dict[str, str]]: ...

    @overload
    def client_list(
        self: AsyncClientProtocol,
        _type: str | None = None,
        client_id: List[EncodableT] = [],
        **kwargs,
    ) -> Awaitable[list[dict[str, str]]]: ...

    def client_list(
        self, _type: str | None = None, client_id: List[EncodableT] = [], **kwargs
    ) -> list[dict[str, str]] | Awaitable[list[dict[str, str]]]:
        """
        Returns a list of currently connected clients.
        If type of client specified, only that type will be returned.

        :param _type: optional. one of the client types (normal, master,
         replica, pubsub)
        :param client_id: optional. a list of client ids

        For more information, see https://redis.io/commands/client-list
        """
        args = []
        if _type is not None:
            client_types = ("normal", "master", "replica", "pubsub")
            if str(_type).lower() not in client_types:
                raise DataError(f"CLIENT LIST _type must be one of {client_types!r}")
            args.append(b"TYPE")
            args.append(_type)
        if not isinstance(client_id, list):
            raise DataError("client_id must be a list")
        if client_id:
            args.append(b"ID")
            args += client_id
        return self.execute_command("CLIENT LIST", *args, **kwargs)

    @overload
    def client_list_iter(
        self: SyncClientProtocol,
        _type: str | None = None,
        client_id: List[EncodableT] = [],
        **kwargs,
    ) -> Iterator[dict[str, str]]: ...

    @overload
    def client_list_iter(
        self: AsyncClientProtocol,
        _type: str | None = None,
        client_id: List[EncodableT] = [],
        **kwargs,
    ) -> Awaitable[Iterator[dict[str, str]]]: ...

    def client_list_iter(
        self, _type: str | None = None, client_id: List[EncodableT] = [], **kwargs
    ) -> Iterator[dict[str, str]] | Awaitable[Iterator[dict[str, str]]]:
        """
        Like ``client_list()``, but returns an iterator that parses and
        yields one client record at a time instead of building the full
        list upfront. This bounds the memory held for parsed records to
        one record at a time rather than all of them at once, which
        matters when there are many thousands of connected clients.

        The full CLIENT LIST reply is still read off the socket in one
        piece before this can start yielding - that part is unavoidable
        given how RESP bulk-string framing works - so this only reduces
        the memory used to hold the parsed records, not the raw reply.

        :param _type: optional. one of the client types (normal, master,
         replica, pubsub)
        :param client_id: optional. a list of client ids

        For more information, see https://redis.io/commands/client-list
        """
        args = []
        if _type is not None:
            client_types = ("normal", "master", "replica", "pubsub")
            if str(_type).lower() not in client_types:
                raise DataError(f"CLIENT LIST _type must be one of {client_types!r}")
            args.append(b"TYPE")
            args.append(_type)
        if not isinstance(client_id, list):
            raise DataError("client_id must be a list")
        if client_id:
            args.append(b"ID")
            args += client_id
        return self.execute_command("CLIENT LIST", *args, as_iter=True, **kwargs)

    @overload
    def client_getname(self: SyncClientProtocol, **kwargs) -> bytes | str | None: ...

    @overload
    def client_getname(
        self: AsyncClientProtocol, **kwargs
    ) -> Awaitable[bytes | str | None]: ...

    def client_getname(self, **kwargs) -> (bytes | str | None) | Awaitable[
        bytes | str | None
    ]:
        """
        Returns the current connection name

        For more information, see https://redis.io/commands/client-getname
        """
        return self.execute_command("CLIENT GETNAME", **kwargs)

    @overload
    def client_getredir(self: SyncClientProtocol, **kwargs) -> int: ...

    @overload
    def client_getredir(self: AsyncClientProtocol, **kwargs) -> Awaitable[int]: ...

    def client_getredir(self, **kwargs) -> int | Awaitable[int]:
        """
        Returns the ID (an integer) of the client to whom we are
        redirecting tracking notifications.

        see: https://redis.io/commands/client-getredir
        """
        return self.execute_command("CLIENT GETREDIR", **kwargs)

    @overload
    def client_reply(
        self: SyncClientProtocol,
        reply: Literal["ON", "OFF", "SKIP"],
        **kwargs,
    ) -> bytes | str: ...

    @overload
    def client_reply(
        self: AsyncClientProtocol,
        reply: Literal["ON", "OFF", "SKIP"],
        **kwargs,
    ) -> Awaitable[bytes | str]: ...

    def client_reply(self, reply: Literal["ON", "OFF", "SKIP"], **kwargs) -> (
        bytes | str
    ) | Awaitable[bytes | str]:
        """
        Enable and disable redis server replies.

        ``reply`` Must be ON OFF or SKIP,
        ON - The default most with server replies to commands
        OFF - Disable server responses to commands
        SKIP - Skip the response of the immediately following command.

        Note: When setting OFF or SKIP replies, you will need a client object
        with a timeout specified in seconds, and will need to catch the
        TimeoutError.
        The test_client_reply unit test illustrates this, and
        conftest.py has a client with a timeout.

        See https://redis.io/commands/client-reply
        """
        replies = ["ON", "OFF", "SKIP"]
        if reply not in replies:
            raise DataError(f"CLIENT REPLY must be one of {replies!r}")
        return self.execute_command("CLIENT REPLY", reply, **kwargs)

    @overload
    def client_id(self: SyncClientProtocol, **kwargs) -> int: ...

    @overload
    def client_id(self: AsyncClientProtocol, **kwargs) -> Awaitable[int]: ...

    def client_id(self, **kwargs) -> int | Awaitable[int]:
        """
        Returns the current connection id

        For more information, see https://redis.io/commands/client-id
        """
        return self.execute_command("CLIENT ID", **kwargs)

    @overload
    def client_tracking_on(
        self: SyncClientProtocol,
        clientid: int | None = None,
        prefix: Sequence[KeyT] = [],
        bcast: bool = False,
        optin: bool = False,
        optout: bool = False,
        noloop: bool = False,
    ) -> bytes | str: ...

    @overload
    def client_tracking_on(
        self: AsyncClientProtocol,
        clientid: int | None = None,
        prefix: Sequence[KeyT] = [],
        bcast: bool = False,
        optin: bool = False,
        optout: bool = False,
        noloop: bool = False,
    ) -> Awaitable[bytes | str]: ...

    def client_tracking_on(
        self,
        clientid: int | None = None,
        prefix: Sequence[KeyT] = [],
        bcast: bool = False,
        optin: bool = False,
        optout: bool = False,
        noloop: bool = False,
    ) -> (bytes | str) | Awaitable[bytes | str]:
        """
        Turn on the tracking mode.
        For more information, about the options look at client_tracking func.

        On a client with client-side caching enabled this raises ``RedisError``:
        the cache enables tracking itself.

        See https://redis.io/commands/client-tracking
        """
        return self.client_tracking(
            True, clientid, prefix, bcast, optin, optout, noloop
        )

    @overload
    def client_tracking_off(
        self: SyncClientProtocol,
        clientid: int | None = None,
        prefix: Sequence[KeyT] = [],
        bcast: bool = False,
        optin: bool = False,
        optout: bool = False,
        noloop: bool = False,
    ) -> bytes | str: ...

    @overload
    def client_tracking_off(
        self: AsyncClientProtocol,
        clientid: int | None = None,
        prefix: Sequence[KeyT] = [],
        bcast: bool = False,
        optin: bool = False,
        optout: bool = False,
        noloop: bool = False,
    ) -> Awaitable[bytes | str]: ...

    def client_tracking_off(
        self,
        clientid: int | None = None,
        prefix: Sequence[KeyT] = [],
        bcast: bool = False,
        optin: bool = False,
        optout: bool = False,
        noloop: bool = False,
    ) -> (bytes | str) | Awaitable[bytes | str]:
        """
        Turn off the tracking mode.
        For more information, about the options look at client_tracking func.

        On a client with client-side caching enabled this raises ``RedisError``:
        turning tracking off would stop invalidations for cached replies.

        See https://redis.io/commands/client-tracking
        """
        return self.client_tracking(
            False, clientid, prefix, bcast, optin, optout, noloop
        )

    @overload
    def client_tracking(
        self: SyncClientProtocol,
        on: bool = True,
        clientid: int | None = None,
        prefix: Sequence[KeyT] = [],
        bcast: bool = False,
        optin: bool = False,
        optout: bool = False,
        noloop: bool = False,
        **kwargs,
    ) -> bytes | str: ...

    @overload
    def client_tracking(
        self: AsyncClientProtocol,
        on: bool = True,
        clientid: int | None = None,
        prefix: Sequence[KeyT] = [],
        bcast: bool = False,
        optin: bool = False,
        optout: bool = False,
        noloop: bool = False,
        **kwargs,
    ) -> Awaitable[bytes | str]: ...

    def client_tracking(
        self,
        on: bool = True,
        clientid: int | None = None,
        prefix: Sequence[KeyT] = [],
        bcast: bool = False,
        optin: bool = False,
        optout: bool = False,
        noloop: bool = False,
        **kwargs,
    ) -> (bytes | str) | Awaitable[bytes | str]:
        """
        Enables the tracking feature of the Redis server, that is used
        for server assisted client side caching.

        ``on`` indicate for tracking on or tracking off. The default is on.

        ``clientid`` send invalidation messages to the connection with
        the specified ID.

        ``bcast`` enable tracking in broadcasting mode. In this mode
        invalidation messages are reported for all the prefixes
        specified, regardless of the keys requested by the connection.

        ``optin``  when broadcasting is NOT active, normally don't track
        keys in read only commands, unless they are called immediately
        after a CLIENT CACHING yes command.

        ``optout`` when broadcasting is NOT active, normally track keys in
        read only commands, unless they are called immediately after a
        CLIENT CACHING no command.

        ``noloop`` don't send notifications about keys modified by this
        connection itself.

        ``prefix``  for broadcasting, register a given key prefix, so that
        notifications will be provided only for keys starting with this string.

        On a client with client-side caching enabled this raises ``RedisError``:
        the cache enables tracking itself, and changing it would stop
        invalidations for cached replies.

        See https://redis.io/commands/client-tracking
        """

        if len(prefix) != 0 and bcast is False:
            raise DataError("Prefix can only be used with bcast")

        pieces = ["ON"] if on else ["OFF"]
        if clientid is not None:
            pieces.extend(["REDIRECT", clientid])
        for p in prefix:
            pieces.extend(["PREFIX", p])
        if bcast:
            pieces.append("BCAST")
        if optin:
            pieces.append("OPTIN")
        if optout:
            pieces.append("OPTOUT")
        if noloop:
            pieces.append("NOLOOP")

        return self.execute_command("CLIENT TRACKING", *pieces, **kwargs)

    @overload
    def client_trackinginfo(
        self: SyncClientProtocol, **kwargs
    ) -> ClientTrackingInfoResponse: ...

    @overload
    def client_trackinginfo(
        self: AsyncClientProtocol, **kwargs
    ) -> Awaitable[ClientTrackingInfoResponse]: ...

    def client_trackinginfo(
        self, **kwargs
    ) -> ClientTrackingInfoResponse | Awaitable[ClientTrackingInfoResponse]:
        """
        Returns the information about the current client connection's
        use of the server assisted client side cache.

        See https://redis.io/commands/client-trackinginfo
        """
        return self.execute_command("CLIENT TRACKINGINFO", **kwargs)

    @overload
    def client_setname(self: SyncClientProtocol, name: str, **kwargs) -> bool: ...

    @overload
    def client_setname(
        self: AsyncClientProtocol, name: str, **kwargs
    ) -> Awaitable[bool]: ...

    def client_setname(self, name: str, **kwargs) -> bool | Awaitable[bool]:
        """
        Sets the current connection name

        For more information, see https://redis.io/commands/client-setname

        .. note::
           This method sets client name only for **current** connection.

           If you want to set a common name for all connections managed
           by this client, use ``client_name`` constructor argument.
        """
        return self.execute_command("CLIENT SETNAME", name, **kwargs)

    @overload
    def client_setinfo(
        self: SyncClientProtocol, attr: str, value: str, **kwargs
    ) -> bool: ...

    @overload
    def client_setinfo(
        self: AsyncClientProtocol, attr: str, value: str, **kwargs
    ) -> Awaitable[bool]: ...

    def client_setinfo(self, attr: str, value: str, **kwargs) -> bool | Awaitable[bool]:
        """
        Sets the current connection library name or version
        For more information see https://redis.io/commands/client-setinfo
        """
        return self.execute_command("CLIENT SETINFO", attr, value, **kwargs)

    @overload
    def client_unblock(
        self: SyncClientProtocol, client_id: int, error: bool = False, **kwargs
    ) -> bool: ...

    @overload
    def client_unblock(
        self: AsyncClientProtocol, client_id: int, error: bool = False, **kwargs
    ) -> Awaitable[bool]: ...

    def client_unblock(
        self, client_id: int, error: bool = False, **kwargs
    ) -> bool | Awaitable[bool]:
        """
        Unblocks a connection by its client id.
        If ``error`` is True, unblocks the client with a special error message.
        If ``error`` is False (default), the client is unblocked using the
        regular timeout mechanism.

        For more information, see https://redis.io/commands/client-unblock
        """
        args = ["CLIENT UNBLOCK", int(client_id)]
        if error:
            args.append(b"ERROR")
        return self.execute_command(*args, **kwargs)

    @overload
    def client_pause(
        self: SyncClientProtocol, timeout: int, all: bool = True, **kwargs
    ) -> bool: ...

    @overload
    def client_pause(
        self: AsyncClientProtocol, timeout: int, all: bool = True, **kwargs
    ) -> Awaitable[bool]: ...

    def client_pause(
        self, timeout: int, all: bool = True, **kwargs
    ) -> bool | Awaitable[bool]:
        """
        Suspend all the Redis clients for the specified amount of time.


        For more information, see https://redis.io/commands/client-pause

        Args:
            timeout: milliseconds to pause clients
            all: If true (default) all client commands are blocked.
                 otherwise, clients are only blocked if they attempt to execute
                 a write command.

        For the WRITE mode, some commands have special behavior:

        * EVAL/EVALSHA: Will block client for all scripts.
        * PUBLISH: Will block client.
        * PFCOUNT: Will block client.
        * WAIT: Acknowledgments will be delayed, so this command will
            appear blocked.
        """
        args = ["CLIENT PAUSE", str(timeout)]
        if not isinstance(timeout, int):
            raise DataError("CLIENT PAUSE timeout must be an integer")
        if not all:
            args.append("WRITE")
        return self.execute_command(*args, **kwargs)

    @overload
    def client_unpause(self: SyncClientProtocol, **kwargs) -> bytes | str: ...

    @overload
    def client_unpause(
        self: AsyncClientProtocol, **kwargs
    ) -> Awaitable[bytes | str]: ...

    def client_unpause(self, **kwargs) -> (bytes | str) | Awaitable[bytes | str]:
        """
        Unpause all redis clients

        For more information, see https://redis.io/commands/client-unpause
        """
        return self.execute_command("CLIENT UNPAUSE", **kwargs)

    @overload
    def client_no_evict(self: SyncClientProtocol, mode: str) -> bytes | str: ...

    @overload
    def client_no_evict(
        self: AsyncClientProtocol, mode: str
    ) -> Awaitable[bytes | str]: ...

    def client_no_evict(self, mode: str) -> (bytes | str) | Awaitable[bytes | str]:
        """
        Sets the client eviction mode for the current connection.

        For more information, see https://redis.io/commands/client-no-evict
        """
        return self.execute_command("CLIENT NO-EVICT", mode)

    @overload
    def client_no_touch(self: SyncClientProtocol, mode: str) -> bytes | str: ...

    @overload
    def client_no_touch(
        self: AsyncClientProtocol, mode: str
    ) -> Awaitable[bytes | str]: ...

    def client_no_touch(self, mode: str) -> (bytes | str) | Awaitable[bytes | str]:
        """
        # The command controls whether commands sent by the client will alter
        # the LRU/LFU of the keys they access.
        # When turned on, the current client will not change LFU/LRU stats,
        # unless it sends the TOUCH command.

        For more information, see https://redis.io/commands/client-no-touch
        """
        return self.execute_command("CLIENT NO-TOUCH", mode)

    @overload
    def command(self: SyncClientProtocol, **kwargs) -> dict[str, dict[str, Any]]: ...

    @overload
    def command(
        self: AsyncClientProtocol, **kwargs
    ) -> Awaitable[dict[str, dict[str, Any]]]: ...

    def command(
        self, **kwargs
    ) -> dict[str, dict[str, Any]] | Awaitable[dict[str, dict[str, Any]]]:
        """
        Returns dict reply of details about all Redis commands.

        For more information, see https://redis.io/commands/command
        """
        return self.execute_command("COMMAND", **kwargs)

    def command_info(self, **kwargs) -> None:
        raise NotImplementedError(
            "COMMAND INFO is intentionally not implemented in the client."
        )

    @overload
    def command_count(self: SyncClientProtocol, **kwargs) -> int: ...

    @overload
    def command_count(self: AsyncClientProtocol, **kwargs) -> Awaitable[int]: ...

    def command_count(self, **kwargs) -> int | Awaitable[int]:
        return self.execute_command("COMMAND COUNT", **kwargs)

    @overload
    def command_list(
        self: SyncClientProtocol,
        module: str | None = None,
        category: str | None = None,
        pattern: str | None = None,
    ) -> list[bytes | str]: ...

    @overload
    def command_list(
        self: AsyncClientProtocol,
        module: str | None = None,
        category: str | None = None,
        pattern: str | None = None,
    ) -> Awaitable[list[bytes | str]]: ...

    def command_list(
        self,
        module: str | None = None,
        category: str | None = None,
        pattern: str | None = None,
    ) -> list[bytes | str] | Awaitable[list[bytes | str]]:
        """
        Return an array of the server's command names.
        You can use one of the following filters:
        ``module``: get the commands that belong to the module
        ``category``: get the commands in the ACL category
        ``pattern``: get the commands that match the given pattern

        For more information, see https://redis.io/commands/command-list/
        """
        pieces = []
        if module is not None:
            pieces.extend(["MODULE", module])
        if category is not None:
            pieces.extend(["ACLCAT", category])
        if pattern is not None:
            pieces.extend(["PATTERN", pattern])

        if pieces:
            pieces.insert(0, "FILTERBY")

        return self.execute_command("COMMAND LIST", *pieces)

    @overload
    def command_getkeysandflags(
        self: SyncClientProtocol, *args: str
    ) -> CommandGetKeysAndFlagsResponse: ...

    @overload
    def command_getkeysandflags(
        self: AsyncClientProtocol, *args: str
    ) -> Awaitable[CommandGetKeysAndFlagsResponse]: ...

    def command_getkeysandflags(
        self, *args: str
    ) -> CommandGetKeysAndFlagsResponse | Awaitable[CommandGetKeysAndFlagsResponse]:
        """
        Returns array of keys from a full Redis command and their usage flags.

        For more information, see https://redis.io/commands/command-getkeysandflags
        """
        return self.execute_command("COMMAND GETKEYSANDFLAGS", *args)

    def command_docs(self, *args):
        """
        This function throws a NotImplementedError since it is intentionally
        not supported.
        """
        raise NotImplementedError(
            "COMMAND DOCS is intentionally not implemented in the client."
        )

    @overload
    def config_get(
        self: SyncClientProtocol, pattern: PatternT = "*", *args: PatternT, **kwargs
    ) -> dict[str | None, str | None]: ...

    @overload
    def config_get(
        self: AsyncClientProtocol, pattern: PatternT = "*", *args: PatternT, **kwargs
    ) -> Awaitable[dict[str | None, str | None]]: ...

    def config_get(
        self, pattern: PatternT = "*", *args: PatternT, **kwargs
    ) -> dict[str | None, str | None] | Awaitable[dict[str | None, str | None]]:
        """
        Return a dictionary of configuration based on the ``pattern``

        For more information, see https://redis.io/commands/config-get
        """
        return self.execute_command("CONFIG GET", pattern, *args, **kwargs)

    @overload
    def config_set(
        self: SyncClientProtocol,
        name: KeyT,
        value: EncodableT,
        *args: Union[KeyT, EncodableT],
        **kwargs,
    ) -> bool: ...

    @overload
    def config_set(
        self: AsyncClientProtocol,
        name: KeyT,
        value: EncodableT,
        *args: Union[KeyT, EncodableT],
        **kwargs,
    ) -> Awaitable[bool]: ...

    def config_set(
        self,
        name: KeyT,
        value: EncodableT,
        *args: Union[KeyT, EncodableT],
        **kwargs,
    ) -> bool | Awaitable[bool]:
        """Set config item ``name`` with ``value``

        For more information, see https://redis.io/commands/config-set
        """
        return self.execute_command("CONFIG SET", name, value, *args, **kwargs)

    @overload
    def config_resetstat(self: SyncClientProtocol, **kwargs) -> bool: ...

    @overload
    def config_resetstat(self: AsyncClientProtocol, **kwargs) -> Awaitable[bool]: ...

    def config_resetstat(self, **kwargs) -> bool | Awaitable[bool]:
        """
        Reset runtime statistics

        For more information, see https://redis.io/commands/config-resetstat
        """
        return self.execute_command("CONFIG RESETSTAT", **kwargs)

    @overload
    def config_rewrite(self: SyncClientProtocol, **kwargs) -> bytes | str: ...

    @overload
    def config_rewrite(
        self: AsyncClientProtocol, **kwargs
    ) -> Awaitable[bytes | str]: ...

    def config_rewrite(self, **kwargs) -> (bytes | str) | Awaitable[bytes | str]:
        """
        Rewrite config file with the minimal change to reflect running config.

        For more information, see https://redis.io/commands/config-rewrite
        """
        return self.execute_command("CONFIG REWRITE", **kwargs)

    @overload
    def dbsize(self: SyncClientProtocol, **kwargs) -> int: ...

    @overload
    def dbsize(self: AsyncClientProtocol, **kwargs) -> Awaitable[int]: ...

    def dbsize(self, **kwargs) -> int | Awaitable[int]:
        """
        Returns the number of keys in the current database

        For more information, see https://redis.io/commands/dbsize
        """
        return self.execute_command("DBSIZE", **kwargs)

    @overload
    def debug_object(
        self: SyncClientProtocol, key: KeyT, **kwargs
    ) -> dict[str, str | int] | bytes | str: ...

    @overload
    def debug_object(
        self: AsyncClientProtocol, key: KeyT, **kwargs
    ) -> Awaitable[dict[str, str | int] | bytes | str]: ...

    def debug_object(self, key: KeyT, **kwargs) -> (
        dict[str, str | int] | bytes | str
    ) | Awaitable[dict[str, str | int] | bytes | str]:
        """
        Returns version specific meta information about a given key

        For more information, see https://redis.io/commands/debug-object
        """
        return self.execute_command("DEBUG OBJECT", key, **kwargs)

    def debug_segfault(self, **kwargs) -> None:
        raise NotImplementedError(
            """
            DEBUG SEGFAULT is intentionally not implemented in the client.

            For more information, see https://redis.io/commands/debug-segfault
            """
        )

    @overload
    def echo(self: SyncClientProtocol, value: EncodableT, **kwargs) -> bytes | str: ...

    @overload
    def echo(
        self: AsyncClientProtocol, value: EncodableT, **kwargs
    ) -> Awaitable[bytes | str]: ...

    def echo(self, value: EncodableT, **kwargs) -> (bytes | str) | Awaitable[
        bytes | str
    ]:
        """
        Echo the string back from the server

        For more information, see https://redis.io/commands/echo
        """
        return self.execute_command("ECHO", value, **kwargs)

    @overload
    def flushall(
        self: SyncClientProtocol, asynchronous: bool = False, **kwargs
    ) -> bool: ...

    @overload
    def flushall(
        self: AsyncClientProtocol, asynchronous: bool = False, **kwargs
    ) -> Awaitable[bool]: ...

    def flushall(self, asynchronous: bool = False, **kwargs) -> bool | Awaitable[bool]:
        """
        Delete all keys in all databases on the current host.

        ``asynchronous`` indicates whether the operation is
        executed asynchronously by the server.

        For more information, see https://redis.io/commands/flushall
        """
        args = []
        if asynchronous:
            args.append(b"ASYNC")
        return self.execute_command("FLUSHALL", *args, **kwargs)

    @overload
    def flushdb(
        self: SyncClientProtocol, asynchronous: bool = False, **kwargs
    ) -> bool: ...

    @overload
    def flushdb(
        self: AsyncClientProtocol, asynchronous: bool = False, **kwargs
    ) -> Awaitable[bool]: ...

    def flushdb(self, asynchronous: bool = False, **kwargs) -> bool | Awaitable[bool]:
        """
        Delete all keys in the current database.

        ``asynchronous`` indicates whether the operation is
        executed asynchronously by the server.

        For more information, see https://redis.io/commands/flushdb
        """
        args = []
        if asynchronous:
            args.append(b"ASYNC")
        return self.execute_command("FLUSHDB", *args, **kwargs)

    @overload
    def sync(self: SyncClientProtocol) -> bytes: ...

    @overload
    def sync(self: AsyncClientProtocol) -> Awaitable[bytes]: ...

    def sync(self) -> bytes | Awaitable[bytes]:
        """
        Initiates a replication stream from the master.

        For more information, see https://redis.io/commands/sync
        """
        from redis.client import NEVER_DECODE

        options = {}
        options[NEVER_DECODE] = []
        return self.execute_command("SYNC", **options)

    @overload
    def psync(self: SyncClientProtocol, replicationid: str, offset: int) -> bytes: ...

    @overload
    def psync(
        self: AsyncClientProtocol, replicationid: str, offset: int
    ) -> Awaitable[bytes]: ...

    def psync(self, replicationid: str, offset: int) -> bytes | Awaitable[bytes]:
        """
        Initiates a replication stream from the master.
        Newer version for `sync`.

        For more information, see https://redis.io/commands/sync
        """
        from redis.client import NEVER_DECODE

        options = {}
        options[NEVER_DECODE] = []
        return self.execute_command("PSYNC", replicationid, offset, **options)

    @overload
    def swapdb(self: SyncClientProtocol, first: int, second: int, **kwargs) -> bool: ...

    @overload
    def swapdb(
        self: AsyncClientProtocol, first: int, second: int, **kwargs
    ) -> Awaitable[bool]: ...

    def swapdb(self, first: int, second: int, **kwargs) -> bool | Awaitable[bool]:
        """
        Swap two databases

        For more information, see https://redis.io/commands/swapdb
        """
        return self.execute_command("SWAPDB", first, second, **kwargs)

    @overload
    def select(self: SyncClientProtocol, index: int, **kwargs) -> bool: ...

    @overload
    def select(self: AsyncClientProtocol, index: int, **kwargs) -> Awaitable[bool]: ...

    def select(self, index: int, **kwargs) -> bool | Awaitable[bool]:
        """Select the Redis logical database at index.

        See: https://redis.io/commands/select
        """
        return self.execute_command("SELECT", index, **kwargs)

    @overload
    def info(
        self: SyncClientProtocol,
        section: Optional[str] = None,
        *args: str,
        **kwargs,
    ) -> dict[str, Any]: ...

    @overload
    def info(
        self: AsyncClientProtocol,
        section: Optional[str] = None,
        *args: str,
        **kwargs,
    ) -> Awaitable[dict[str, Any]]: ...

    def info(
        self, section: Optional[str] = None, *args: str, **kwargs
    ) -> dict[str, Any] | Awaitable[dict[str, Any]]:
        """
        Returns a dictionary containing information about the Redis server

        The ``section`` option can be used to select a specific section
        of information

        The section option is not supported by older versions of Redis Server,
        and will generate ResponseError

        For more information, see https://redis.io/commands/info
        """
        from redis.client import NEVER_DECODE

        kwargs[NEVER_DECODE] = []
        if section is None:
            return self.execute_command("INFO", **kwargs)
        else:
            return self.execute_command("INFO", section, *args, **kwargs)

    @overload
    def lastsave(self: SyncClientProtocol, **kwargs) -> datetime.datetime: ...

    @overload
    def lastsave(
        self: AsyncClientProtocol, **kwargs
    ) -> Awaitable[datetime.datetime]: ...

    def lastsave(self, **kwargs) -> datetime.datetime | Awaitable[datetime.datetime]:
        """
        Return a Python datetime object representing the last time the
        Redis database was saved to disk

        For more information, see https://redis.io/commands/lastsave
        """
        return self.execute_command("LASTSAVE", **kwargs)

    def latency_doctor(self):
        """Raise a NotImplementedError, as the client will not support LATENCY DOCTOR.
        This function is best used within the redis-cli.

        For more information, see https://redis.io/commands/latency-doctor
        """
        raise NotImplementedError(
            """
            LATENCY DOCTOR is intentionally not implemented in the client.

            For more information, see https://redis.io/commands/latency-doctor
            """
        )

    def latency_graph(self):
        """Raise a NotImplementedError, as the client will not support LATENCY GRAPH.
        This function is best used within the redis-cli.

        For more information, see https://redis.io/commands/latency-graph.
        """
        raise NotImplementedError(
            """
            LATENCY GRAPH is intentionally not implemented in the client.

            For more information, see https://redis.io/commands/latency-graph
            """
        )

    @overload
    def lolwut(
        self: SyncClientProtocol, *version_numbers: Union[str, float], **kwargs
    ) -> bytes | str: ...

    @overload
    def lolwut(
        self: AsyncClientProtocol, *version_numbers: Union[str, float], **kwargs
    ) -> Awaitable[bytes | str]: ...

    def lolwut(self, *version_numbers: Union[str, float], **kwargs) -> (
        bytes | str
    ) | Awaitable[bytes | str]:
        """
        Get the Redis version and a piece of generative computer art

        See: https://redis.io/commands/lolwut
        """
        if version_numbers:
            return self.execute_command("LOLWUT VERSION", *version_numbers, **kwargs)
        else:
            return self.execute_command("LOLWUT", **kwargs)

    @overload
    def reset(self: SyncClientProtocol) -> bytes | str: ...

    @overload
    def reset(self: AsyncClientProtocol) -> Awaitable[bytes | str]: ...

    def reset(self) -> (bytes | str) | Awaitable[bytes | str]:
        """Perform a full reset on the connection's server-side context.

        On a client with client-side caching enabled this raises ``RedisError``:
        the reset turns off the tracking the cache relies on for invalidations.

        See: https://redis.io/commands/reset
        """
        return self.execute_command("RESET")

    @overload
    def migrate(
        self: SyncClientProtocol,
        host: str,
        port: int,
        keys: KeysT,
        destination_db: int,
        timeout: int,
        copy: bool = False,
        replace: bool = False,
        auth: str | None = None,
        **kwargs,
    ) -> bytes | str: ...

    @overload
    def migrate(
        self: AsyncClientProtocol,
        host: str,
        port: int,
        keys: KeysT,
        destination_db: int,
        timeout: int,
        copy: bool = False,
        replace: bool = False,
        auth: str | None = None,
        **kwargs,
    ) -> Awaitable[bytes | str]: ...

    def migrate(
        self,
        host: str,
        port: int,
        keys: KeysT,
        destination_db: int,
        timeout: int,
        copy: bool = False,
        replace: bool = False,
        auth: str | None = None,
        **kwargs,
    ) -> (bytes | str) | Awaitable[bytes | str]:
        """
        Migrate 1 or more keys from the current Redis server to a different
        server specified by the ``host``, ``port`` and ``destination_db``.

        The ``timeout``, specified in milliseconds, indicates the maximum
        time the connection between the two servers can be idle before the
        command is interrupted.

        If ``copy`` is True, the specified ``keys`` are NOT deleted from
        the source server.

        If ``replace`` is True, this operation will overwrite the keys
        on the destination server if they exist.

        If ``auth`` is specified, authenticate to the destination server with
        the password provided.

        For more information, see https://redis.io/commands/migrate
        """
        keys = list_or_args(keys, [])
        if not keys:
            raise DataError("MIGRATE requires at least one key")
        pieces = []
        if copy:
            pieces.append(b"COPY")
        if replace:
            pieces.append(b"REPLACE")
        if auth:
            pieces.append(b"AUTH")
            pieces.append(auth)
        pieces.append(b"KEYS")
        pieces.extend(keys)
        return self.execute_command(
            "MIGRATE", host, port, "", destination_db, timeout, *pieces, **kwargs
        )

    @overload
    def object(self: SyncClientProtocol, infotype: str, key: KeyT, **kwargs) -> Any: ...

    @overload
    def object(
        self: AsyncClientProtocol, infotype: str, key: KeyT, **kwargs
    ) -> Awaitable[Any]: ...

    def object(self, infotype: str, key: KeyT, **kwargs) -> Any | Awaitable[Any]:
        """
        Return the encoding, idletime, or refcount about the key
        """
        return self.execute_command(
            "OBJECT", infotype, key, infotype=infotype, **kwargs
        )

    def memory_doctor(self, **kwargs) -> None:
        raise NotImplementedError(
            """
            MEMORY DOCTOR is intentionally not implemented in the client.

            For more information, see https://redis.io/commands/memory-doctor
            """
        )

    def memory_help(self, **kwargs) -> None:
        raise NotImplementedError(
            """
            MEMORY HELP is intentionally not implemented in the client.

            For more information, see https://redis.io/commands/memory-help
            """
        )

    @overload
    def memory_stats(self: SyncClientProtocol, **kwargs) -> dict[str, Any]: ...

    @overload
    def memory_stats(
        self: AsyncClientProtocol, **kwargs
    ) -> Awaitable[dict[str, Any]]: ...

    def memory_stats(self, **kwargs) -> dict[str, Any] | Awaitable[dict[str, Any]]:
        """
        Return a dictionary of memory stats

        For more information, see https://redis.io/commands/memory-stats
        """
        return self.execute_command("MEMORY STATS", **kwargs)

    @overload
    def memory_malloc_stats(self: SyncClientProtocol, **kwargs) -> bytes | str: ...

    @overload
    def memory_malloc_stats(
        self: AsyncClientProtocol, **kwargs
    ) -> Awaitable[bytes | str]: ...

    def memory_malloc_stats(self, **kwargs) -> (bytes | str) | Awaitable[bytes | str]:
        """
        Return an internal statistics report from the memory allocator.

        See: https://redis.io/commands/memory-malloc-stats
        """
        return self.execute_command("MEMORY MALLOC-STATS", **kwargs)

    @overload
    def memory_usage(
        self: SyncClientProtocol, key: KeyT, samples: int | None = None, **kwargs
    ) -> int | None: ...

    @overload
    def memory_usage(
        self: AsyncClientProtocol, key: KeyT, samples: int | None = None, **kwargs
    ) -> Awaitable[int | None]: ...

    def memory_usage(self, key: KeyT, samples: int | None = None, **kwargs) -> (
        int | None
    ) | Awaitable[int | None]:
        """
        Return the total memory usage for key, its value and associated
        administrative overheads.

        For nested data structures, ``samples`` is the number of elements to
        sample. If left unspecified, the server's default is 5. Use 0 to sample
        all elements.

        For more information, see https://redis.io/commands/memory-usage
        """
        args = []
        if isinstance(samples, int):
            args.extend([b"SAMPLES", samples])
        return self.execute_command("MEMORY USAGE", key, *args, **kwargs)

    @overload
    def memory_purge(self: SyncClientProtocol, **kwargs) -> bool: ...

    @overload
    def memory_purge(self: AsyncClientProtocol, **kwargs) -> Awaitable[bool]: ...

    def memory_purge(self, **kwargs) -> bool | Awaitable[bool]:
        """
        Attempts to purge dirty pages for reclamation by allocator

        For more information, see https://redis.io/commands/memory-purge
        """
        return self.execute_command("MEMORY PURGE", **kwargs)

    def latency_histogram(self, *args):
        """
        This function throws a NotImplementedError since it is intentionally
        not supported.
        """
        raise NotImplementedError(
            "LATENCY HISTOGRAM is intentionally not implemented in the client."
        )

    @overload
    def latency_history(self: SyncClientProtocol, event: str) -> list[list[int]]: ...

    @overload
    def latency_history(
        self: AsyncClientProtocol, event: str
    ) -> Awaitable[list[list[int]]]: ...

    def latency_history(
        self, event: str
    ) -> list[list[int]] | Awaitable[list[list[int]]]:
        """
        Returns the raw data of the ``event``'s latency spikes time series.

        For more information, see https://redis.io/commands/latency-history
        """
        return self.execute_command("LATENCY HISTORY", event)

    @overload
    def latency_latest(self: SyncClientProtocol) -> list[list[bytes | str | int]]: ...

    @overload
    def latency_latest(
        self: AsyncClientProtocol,
    ) -> Awaitable[list[list[bytes | str | int]]]: ...

    def latency_latest(
        self,
    ) -> list[list[bytes | str | int]] | Awaitable[list[list[bytes | str | int]]]:
        """
        Reports the latest latency events logged.

        For more information, see https://redis.io/commands/latency-latest
        """
        return self.execute_command("LATENCY LATEST")

    @overload
    def latency_reset(self: SyncClientProtocol, *events: str) -> int: ...

    @overload
    def latency_reset(self: AsyncClientProtocol, *events: str) -> Awaitable[int]: ...

    def latency_reset(self, *events: str) -> int | Awaitable[int]:
        """
        Resets the latency spikes time series of all, or only some, events.

        For more information, see https://redis.io/commands/latency-reset
        """
        return self.execute_command("LATENCY RESET", *events)

    @overload
    def ping(self: SyncClientProtocol, **kwargs) -> bool: ...

    @overload
    def ping(self: AsyncClientProtocol, **kwargs) -> Awaitable[bool]: ...

    def ping(self, **kwargs) -> Union[Awaitable[bool], bool]:
        """
        Ping the Redis server to test connectivity.

        Sends a PING command to the Redis server and returns True if the server
        responds with "PONG".

        This command is useful for:
        - Testing whether a connection is still alive
        - Verifying the server's ability to serve data

        For more information on the underlying ping command see https://redis.io/commands/ping
        """
        return self.execute_command("PING", **kwargs)

    @overload
    def quit(self: SyncClientProtocol, **kwargs) -> bool: ...

    @overload
    def quit(self: AsyncClientProtocol, **kwargs) -> Awaitable[bool]: ...

    def quit(self, **kwargs) -> bool | Awaitable[bool]:
        """
        Ask the server to close the connection.

        For more information, see https://redis.io/commands/quit
        """
        return self.execute_command("QUIT", **kwargs)

    @overload
    def replicaof(self: SyncClientProtocol, *args, **kwargs) -> bytes | str: ...

    @overload
    def replicaof(
        self: AsyncClientProtocol, *args, **kwargs
    ) -> Awaitable[bytes | str]: ...

    def replicaof(self, *args, **kwargs) -> (bytes | str) | Awaitable[bytes | str]:
        """
        Update the replication settings of a redis replica, on the fly.

        Examples of valid arguments include:

        NO ONE (set no replication)
        host port (set to the host and port of a redis server)

        For more information, see  https://redis.io/commands/replicaof
        """
        return self.execute_command("REPLICAOF", *args, **kwargs)

    @overload
    def save(self: SyncClientProtocol, **kwargs) -> bool: ...

    @overload
    def save(self: AsyncClientProtocol, **kwargs) -> Awaitable[bool]: ...

    def save(self, **kwargs) -> bool | Awaitable[bool]:
        """
        Tell the Redis server to save its data to disk,
        blocking until the save is complete

        For more information, see https://redis.io/commands/save
        """
        return self.execute_command("SAVE", **kwargs)

    def shutdown(
        self,
        save: bool = False,
        nosave: bool = False,
        now: bool = False,
        force: bool = False,
        abort: bool = False,
        **kwargs,
    ) -> None:
        """Shutdown the Redis server.  If Redis has persistence configured,
        data will be flushed before shutdown.
        It is possible to specify modifiers to alter the behavior of the command:
        ``save`` will force a DB saving operation even if no save points are configured.
        ``nosave`` will prevent a DB saving operation even if one or more save points
        are configured.
        ``now`` skips waiting for lagging replicas, i.e. it bypasses the first step in
        the shutdown sequence.
        ``force`` ignores any errors that would normally prevent the server from exiting
        ``abort`` cancels an ongoing shutdown and cannot be combined with other flags.

        For more information, see https://redis.io/commands/shutdown
        """
        if save and nosave:
            raise DataError("SHUTDOWN save and nosave cannot both be set")
        args = ["SHUTDOWN"]
        if save:
            args.append("SAVE")
        if nosave:
            args.append("NOSAVE")
        if now:
            args.append("NOW")
        if force:
            args.append("FORCE")
        if abort:
            args.append("ABORT")
        try:
            self.execute_command(*args, **kwargs)
        except ConnectionError:
            # a ConnectionError here is expected
            return
        raise RedisError("SHUTDOWN seems to have failed.")

    @overload
    def slaveof(
        self: SyncClientProtocol,
        host: str | None = None,
        port: int | None = None,
        **kwargs,
    ) -> bool: ...

    @overload
    def slaveof(
        self: AsyncClientProtocol,
        host: str | None = None,
        port: int | None = None,
        **kwargs,
    ) -> Awaitable[bool]: ...

    def slaveof(
        self, host: str | None = None, port: int | None = None, **kwargs
    ) -> bool | Awaitable[bool]:
        """
        Set the server to be a replicated slave of the instance identified
        by the ``host`` and ``port``. If called without arguments, the
        instance is promoted to a master instead.

        For more information, see https://redis.io/commands/slaveof
        """
        if host is None and port is None:
            return self.execute_command("SLAVEOF", b"NO", b"ONE", **kwargs)
        return self.execute_command("SLAVEOF", host, port, **kwargs)

    @overload
    def slowlog_get(
        self: SyncClientProtocol, num: int | None = None, **kwargs
    ) -> list[dict[str, Any]]: ...

    @overload
    def slowlog_get(
        self: AsyncClientProtocol, num: int | None = None, **kwargs
    ) -> Awaitable[list[dict[str, Any]]]: ...

    def slowlog_get(
        self, num: int | None = None, **kwargs
    ) -> list[dict[str, Any]] | Awaitable[list[dict[str, Any]]]:
        """
        Get the entries from the slowlog. If ``num`` is specified, get the
        most recent ``num`` items.

        For more information, see https://redis.io/commands/slowlog-get
        """
        from redis.client import NEVER_DECODE

        args = ["SLOWLOG GET"]
        if num is not None:
            args.append(num)
        decode_responses = self.get_connection_kwargs().get("decode_responses", False)
        if decode_responses is True:
            kwargs[NEVER_DECODE] = []
        return self.execute_command(*args, **kwargs)

    @overload
    def slowlog_len(self: SyncClientProtocol, **kwargs) -> int: ...

    @overload
    def slowlog_len(self: AsyncClientProtocol, **kwargs) -> Awaitable[int]: ...

    def slowlog_len(self, **kwargs) -> int | Awaitable[int]:
        """
        Get the number of items in the slowlog

        For more information, see https://redis.io/commands/slowlog-len
        """
        return self.execute_command("SLOWLOG LEN", **kwargs)

    @overload
    def slowlog_reset(self: SyncClientProtocol, **kwargs) -> bool: ...

    @overload
    def slowlog_reset(self: AsyncClientProtocol, **kwargs) -> Awaitable[bool]: ...

    def slowlog_reset(self, **kwargs) -> bool | Awaitable[bool]:
        """
        Remove all items in the slowlog

        For more information, see https://redis.io/commands/slowlog-reset
        """
        return self.execute_command("SLOWLOG RESET", **kwargs)

    @overload
    def time(self: SyncClientProtocol, **kwargs) -> tuple[int, int]: ...

    @overload
    def time(self: AsyncClientProtocol, **kwargs) -> Awaitable[tuple[int, int]]: ...

    def time(self, **kwargs) -> tuple[int, int] | Awaitable[tuple[int, int]]:
        """
        Returns the server time as a 2-item tuple of ints:
        (seconds since epoch, microseconds into this second).

        For more information, see https://redis.io/commands/time
        """
        return self.execute_command("TIME", **kwargs)

    @overload
    def wait(
        self: SyncClientProtocol, num_replicas: int, timeout: int, **kwargs
    ) -> int: ...

    @overload
    def wait(
        self: AsyncClientProtocol, num_replicas: int, timeout: int, **kwargs
    ) -> Awaitable[int]: ...

    def wait(self, num_replicas: int, timeout: int, **kwargs) -> int | Awaitable[int]:
        """
        Redis synchronous replication
        That returns the number of replicas that processed the query when
        we finally have at least ``num_replicas``, or when the ``timeout`` was
        reached.

        For more information, see https://redis.io/commands/wait
        """
        return self.execute_command("WAIT", num_replicas, timeout, **kwargs)

    @overload
    def waitaof(
        self: SyncClientProtocol,
        num_local: int,
        num_replicas: int,
        timeout: int,
        **kwargs,
    ) -> list[int]: ...

    @overload
    def waitaof(
        self: AsyncClientProtocol,
        num_local: int,
        num_replicas: int,
        timeout: int,
        **kwargs,
    ) -> Awaitable[list[int]]: ...

    def waitaof(
        self, num_local: int, num_replicas: int, timeout: int, **kwargs
    ) -> list[int] | Awaitable[list[int]]:
        """
        This command blocks the current client until all previous write
        commands by that client are acknowledged as having been fsynced
        to the AOF of the local Redis and/or at least the specified number
        of replicas.

        For more information, see https://redis.io/commands/waitaof
        """
        return self.execute_command(
            "WAITAOF", num_local, num_replicas, timeout, **kwargs
        )

    def hello(self):
        """
        This function throws a NotImplementedError since it is intentionally
        not supported.
        """
        raise NotImplementedError(
            "HELLO is intentionally not implemented in the client."
        )

    def failover(self):
        """
        This function throws a NotImplementedError since it is intentionally
        not supported.
        """
        raise NotImplementedError(
            "FAILOVER is intentionally not implemented in the client."
        )

    @overload
    def hotkeys_start(
        self: SyncClientProtocol,
        metrics: List[HotkeysMetricsTypes],
        count: int | None = None,
        duration: int | None = None,
        sample_ratio: int | None = None,
        slots: List[int] | None = None,
        **kwargs,
    ) -> bytes | str: ...

    @overload
    def hotkeys_start(
        self: AsyncClientProtocol,
        metrics: List[HotkeysMetricsTypes],
        count: int | None = None,
        duration: int | None = None,
        sample_ratio: int | None = None,
        slots: List[int] | None = None,
        **kwargs,
    ) -> Awaitable[bytes | str]: ...

    def hotkeys_start(
        self,
        metrics: List[HotkeysMetricsTypes],
        count: int | None = None,
        duration: int | None = None,
        sample_ratio: int | None = None,
        slots: List[int] | None = None,
        **kwargs,
    ) -> (bytes | str) | Awaitable[bytes | str]:
        """
        Start collecting hotkeys data.
        Returns an error if there is an ongoing collection session.

        Args:
            count: The number of keys to collect in each criteria (CPU and network consumption)
            metrics: List of metrics to track. Supported values: [HotkeysMetricsTypes.CPU, HotkeysMetricsTypes.NET]
            duration: Automatically stop the collection after `duration` seconds
            sample_ratio: Commands are sampled with probability 1/ratio (1 means no sampling)
            slots: Only track keys on the specified hash slots; an empty list is
                treated as not given

        For more information, see https://redis.io/commands/hotkeys-start
        """
        args: List[Union[str, int]] = ["HOTKEYS", "START"]

        # Add METRICS
        args.extend(["METRICS", len(metrics)])
        args.extend([str(m.value) for m in metrics])

        # Add COUNT
        if count is not None:
            args.extend(["COUNT", count])

        # Add optional DURATION
        if duration is not None:
            args.extend(["DURATION", duration])

        # Add optional SAMPLE ratio
        if sample_ratio is not None:
            args.extend(["SAMPLE", sample_ratio])

        # Add optional SLOTS
        if slots:
            args.append("SLOTS")
            args.append(len(slots))
            args.extend(slots)

        return self.execute_command(*args, **kwargs)

    @overload
    def hotkeys_stop(self: SyncClientProtocol, **kwargs) -> bytes | str: ...

    @overload
    def hotkeys_stop(self: AsyncClientProtocol, **kwargs) -> Awaitable[bytes | str]: ...

    def hotkeys_stop(self, **kwargs) -> (bytes | str) | Awaitable[bytes | str]:
        """
        Stop the ongoing hotkeys collection session (if any).
        The results of the last collection session are kept for consumption with HOTKEYS GET.

        For more information, see https://redis.io/commands/hotkeys-stop
        """
        return self.execute_command("HOTKEYS STOP", **kwargs)

    @overload
    def hotkeys_reset(self: SyncClientProtocol, **kwargs) -> bytes | str: ...

    @overload
    def hotkeys_reset(
        self: AsyncClientProtocol, **kwargs
    ) -> Awaitable[bytes | str]: ...

    def hotkeys_reset(self, **kwargs) -> (bytes | str) | Awaitable[bytes | str]:
        """
        Discard the last hotkeys collection session results (in order to save memory).
        Error if there is an ongoing collection session.

        For more information, see https://redis.io/commands/hotkeys-reset
        """
        return self.execute_command("HOTKEYS RESET", **kwargs)

    @overload
    def hotkeys_get(
        self: SyncClientProtocol, **kwargs
    ) -> list[dict[str | bytes, Any]] | None: ...

    @overload
    def hotkeys_get(
        self: AsyncClientProtocol, **kwargs
    ) -> Awaitable[list[dict[str | bytes, Any]] | None]: ...

    def hotkeys_get(
        self, **kwargs
    ) -> Union[
        Awaitable[list[dict[str | bytes, Any]] | None],
        list[dict[str | bytes, Any]] | None,
    ]:
        """
        Retrieve the result of the ongoing collection session (if any),
        or the last collection session (if any).
        Returns None if there is no collection session.

        HOTKEYS GET response is wrapped in an array for aggregation support.
        Each node returns a single-element array, allowing multiple node
        responses to be concatenated by DMC or other aggregators.

        For more information, see https://redis.io/commands/hotkeys-get
        """
        return self.execute_command("HOTKEYS GET", **kwargs)


class AsyncManagementCommands(ManagementCommands):
    async def command_info(self, **kwargs) -> None:
        return super().command_info(**kwargs)

    async def debug_segfault(self, **kwargs) -> None:
        return super().debug_segfault(**kwargs)

    async def memory_doctor(self, **kwargs) -> None:
        return super().memory_doctor(**kwargs)

    async def memory_help(self, **kwargs) -> None:
        return super().memory_help(**kwargs)

    async def shutdown(
        self,
        save: bool = False,
        nosave: bool = False,
        now: bool = False,
        force: bool = False,
        abort: bool = False,
        **kwargs,
    ) -> None:
        """Shutdown the Redis server.  If Redis has persistence configured,
        data will be flushed before shutdown.  If the "save" option is set,
        a data flush will be attempted even if there is no persistence
        configured.  If the "nosave" option is set, no data flush will be
        attempted.  The "save" and "nosave" options cannot both be set.

        For more information, see https://redis.io/commands/shutdown
        """
        if save and nosave:
            raise DataError("SHUTDOWN save and nosave cannot both be set")
        args = ["SHUTDOWN"]
        if save:
            args.append("SAVE")
        if nosave:
            args.append("NOSAVE")
        if now:
            args.append("NOW")
        if force:
            args.append("FORCE")
        if abort:
            args.append("ABORT")
        try:
            await self.execute_command(*args, **kwargs)
        except ConnectionError:
            # a ConnectionError here is expected
            return
        raise RedisError("SHUTDOWN seems to have failed.")


class BitFieldOperation:
    """
    Command builder for BITFIELD commands.
    """

    def __init__(
        self,
        client: Union["redis.client.Redis", "redis.asyncio.client.Redis"],
        key: str,
        default_overflow: Optional[str] = None,
    ):
        self.client = client
        self.key = key
        self._default_overflow = default_overflow
        # for typing purposes, run the following in constructor and in reset()
        self.operations: list[tuple[EncodableT, ...]] = []
        self._last_overflow = "WRAP"
        self.reset()

    def reset(self):
        """
        Reset the state of the instance to when it was constructed
        """
        self.operations = []
        self._last_overflow = "WRAP"
        self.overflow(self._default_overflow or self._last_overflow)

    def overflow(self, overflow: str):
        """
        Update the overflow algorithm of successive INCRBY operations
        :param overflow: Overflow algorithm, one of WRAP, SAT, FAIL. See the
            Redis docs for descriptions of these algorithms.
        :returns: a :py:class:`BitFieldOperation` instance.
        """
        overflow = overflow.upper()
        if overflow != self._last_overflow:
            self._last_overflow = overflow
            self.operations.append(("OVERFLOW", overflow))
        return self

    def incrby(
        self,
        fmt: str,
        offset: BitfieldOffsetT,
        increment: int,
        overflow: Optional[str] = None,
    ):
        """
        Increment a bitfield by a given amount.
        :param fmt: format-string for the bitfield being updated, e.g. 'u8'
            for an unsigned 8-bit integer.
        :param offset: offset (in number of bits). If prefixed with a
            '#', this is an offset multiplier, e.g. given the arguments
            fmt='u8', offset='#2', the offset will be 16.
        :param int increment: value to increment the bitfield by.
        :param str overflow: overflow algorithm. Defaults to WRAP, but other
            acceptable values are SAT and FAIL. See the Redis docs for
            descriptions of these algorithms.
        :returns: a :py:class:`BitFieldOperation` instance.
        """
        if overflow is not None:
            self.overflow(overflow)

        self.operations.append(("INCRBY", fmt, offset, increment))
        return self

    def get(self, fmt: str, offset: BitfieldOffsetT):
        """
        Get the value of a given bitfield.
        :param fmt: format-string for the bitfield being read, e.g. 'u8' for
            an unsigned 8-bit integer.
        :param offset: offset (in number of bits). If prefixed with a
            '#', this is an offset multiplier, e.g. given the arguments
            fmt='u8', offset='#2', the offset will be 16.
        :returns: a :py:class:`BitFieldOperation` instance.
        """
        self.operations.append(("GET", fmt, offset))
        return self

    def set(self, fmt: str, offset: BitfieldOffsetT, value: int):
        """
        Set the value of a given bitfield.
        :param fmt: format-string for the bitfield being read, e.g. 'u8' for
            an unsigned 8-bit integer.
        :param offset: offset (in number of bits). If prefixed with a
            '#', this is an offset multiplier, e.g. given the arguments
            fmt='u8', offset='#2', the offset will be 16.
        :param int value: value to set at the given position.
        :returns: a :py:class:`BitFieldOperation` instance.
        """
        self.operations.append(("SET", fmt, offset, value))
        return self

    @property
    def command(self):
        cmd = ["BITFIELD", self.key]
        for ops in self.operations:
            cmd.extend(ops)
        return cmd

    def execute(self) -> ResponseT:
        """
        Execute the operation(s) in a single BITFIELD command. The return value
        is a list of values corresponding to each operation. If the client
        used to create this instance was a pipeline, the list of values
        will be present within the pipeline's execute.
        """
        command = self.command
        self.reset()
        return self.client.execute_command(*command)


class DataPersistOptions(Enum):
    # set the value for each provided key to each
    # provided value only if all do not already exist.
    NX = "NX"

    # set the value for each provided key to each
    # provided value only if all already exist.
    XX = "XX"


class BasicKeyCommands(CommandsProtocol):
    """
    Redis basic key-based commands
    """

    @overload
    def append(self: SyncClientProtocol, key: KeyT, value: EncodableT) -> int: ...

    @overload
    def append(
        self: AsyncClientProtocol, key: KeyT, value: EncodableT
    ) -> Awaitable[int]: ...

    def append(self, key: KeyT, value: EncodableT) -> int | Awaitable[int]:
        """
        Appends the string ``value`` to the value at ``key``. If ``key``
        doesn't already exist, create it with a value of ``value``.
        Returns the new length of the value at ``key``.

        For more information, see https://redis.io/commands/append
        """
        return self.execute_command("APPEND", key, value)

    @overload
    def bitcount(
        self: SyncClientProtocol,
        key: KeyT,
        start: int | None = None,
        end: int | None = None,
        mode: str | None = None,
    ) -> int: ...

    @overload
    def bitcount(
        self: AsyncClientProtocol,
        key: KeyT,
        start: int | None = None,
        end: int | None = None,
        mode: str | None = None,
    ) -> Awaitable[int]: ...

    def bitcount(
        self,
        key: KeyT,
        start: int | None = None,
        end: int | None = None,
        mode: str | None = None,
    ) -> int | Awaitable[int]:
        """
        Returns the count of set bits in the value of ``key``.  Optional
        ``start`` and ``end`` parameters indicate which bytes to consider

        For more information, see https://redis.io/commands/bitcount
        """
        params = [key]
        if start is not None and end is not None:
            params.append(start)
            params.append(end)
        elif (start is not None and end is None) or (end is not None and start is None):
            raise DataError("Both start and end must be specified")
        if mode is not None:
            params.append(mode)
        return self.execute_command("BITCOUNT", *params, keys=[key])

    def bitfield(
        self: Union["redis.client.Redis", "redis.asyncio.client.Redis"],
        key: KeyT,
        default_overflow: str | None = None,
    ) -> BitFieldOperation:
        """
        Return a BitFieldOperation instance to conveniently construct one or
        more bitfield operations on ``key``.

        For more information, see https://redis.io/commands/bitfield
        """
        return BitFieldOperation(self, key, default_overflow=default_overflow)

    @overload
    def bitfield_ro(
        self: SyncClientProtocol,
        key: KeyT,
        encoding: str,
        offset: BitfieldOffsetT,
        items: list[tuple[str, BitfieldOffsetT]] | None = None,
    ) -> list[int]: ...

    @overload
    def bitfield_ro(
        self: AsyncClientProtocol,
        key: KeyT,
        encoding: str,
        offset: BitfieldOffsetT,
        items: list[tuple[str, BitfieldOffsetT]] | None = None,
    ) -> Awaitable[list[int]]: ...

    def bitfield_ro(
        self: Union["redis.client.Redis", "redis.asyncio.client.Redis"],
        key: KeyT,
        encoding: str,
        offset: BitfieldOffsetT,
        items: list[tuple[str, BitfieldOffsetT]] | None = None,
    ) -> list[int] | Awaitable[list[int]]:
        """
        Return an array of the specified bitfield values
        where the first value is found using ``encoding`` and ``offset``
        parameters and remaining values are result of corresponding
        encoding/offset pairs in optional list ``items``
        Read-only variant of the BITFIELD command.

        For more information, see https://redis.io/commands/bitfield_ro
        """
        params = [key, "GET", encoding, offset]

        items = items or []
        for encoding, offset in items:
            params.extend(["GET", encoding, offset])
        return self.execute_command("BITFIELD_RO", *params, keys=[key])

    @overload
    def bitop(
        self: SyncClientProtocol, operation: str, dest: KeyT, *keys: KeyT
    ) -> int: ...

    @overload
    def bitop(
        self: AsyncClientProtocol, operation: str, dest: KeyT, *keys: KeyT
    ) -> Awaitable[int]: ...

    def bitop(self, operation: str, dest: KeyT, *keys: KeyT) -> int | Awaitable[int]:
        """
        Perform a bitwise operation using ``operation`` between ``keys`` and
        store the result in ``dest``.

        For more information, see https://redis.io/commands/bitop
        """
        return self.execute_command("BITOP", operation, dest, *keys)

    @overload
    def bitpos(
        self: SyncClientProtocol,
        key: KeyT,
        bit: int,
        start: int | None = None,
        end: int | None = None,
        mode: str | None = None,
    ) -> int: ...

    @overload
    def bitpos(
        self: AsyncClientProtocol,
        key: KeyT,
        bit: int,
        start: int | None = None,
        end: int | None = None,
        mode: str | None = None,
    ) -> Awaitable[int]: ...

    def bitpos(
        self,
        key: KeyT,
        bit: int,
        start: int | None = None,
        end: int | None = None,
        mode: str | None = None,
    ) -> int | Awaitable[int]:
        """
        Return the position of the first bit set to 1 or 0 in a string.
        ``start`` and ``end`` defines search range. The range is interpreted
        as a range of bytes and not a range of bits, so start=0 and end=2
        means to look at the first three bytes.

        For more information, see https://redis.io/commands/bitpos
        """
        if bit not in (0, 1):
            raise DataError("bit must be 0 or 1")
        params = [key, bit]

        start is not None and params.append(start)

        if start is not None and end is not None:
            params.append(end)
        elif start is None and end is not None:
            raise DataError("start argument is not set, when end is specified")

        if mode is not None:
            params.append(mode)
        return self.execute_command("BITPOS", *params, keys=[key])

    @overload
    def copy(
        self: SyncClientProtocol,
        source: str,
        destination: str,
        destination_db: str | None = None,
        replace: bool = False,
    ) -> bool: ...

    @overload
    def copy(
        self: AsyncClientProtocol,
        source: str,
        destination: str,
        destination_db: str | None = None,
        replace: bool = False,
    ) -> Awaitable[bool]: ...

    def copy(
        self,
        source: str,
        destination: str,
        destination_db: str | None = None,
        replace: bool = False,
    ) -> bool | Awaitable[bool]:
        """
        Copy the value stored in the ``source`` key to the ``destination`` key.

        ``destination_db`` an alternative destination database. By default,
        the ``destination`` key is created in the source Redis database.

        ``replace`` whether the ``destination`` key should be removed before
        copying the value to it. By default, the value is not copied if
        the ``destination`` key already exists.

        For more information, see https://redis.io/commands/copy
        """
        params = [source, destination]
        if destination_db is not None:
            params.extend(["DB", destination_db])
        if replace:
            params.append("REPLACE")
        return self.execute_command("COPY", *params)

    @overload
    def decrby(self: SyncClientProtocol, name: KeyT, amount: int = 1) -> int: ...

    @overload
    def decrby(
        self: AsyncClientProtocol, name: KeyT, amount: int = 1
    ) -> Awaitable[int]: ...

    def decrby(self, name: KeyT, amount: int = 1) -> int | Awaitable[int]:
        """
        Decrements the value of ``key`` by ``amount``.  If no key exists,
        the value will be initialized as 0 - ``amount``

        For more information, see https://redis.io/commands/decrby
        """
        return self.execute_command("DECRBY", name, amount)

    decr = decrby

    @overload
    def delete(self: SyncClientProtocol, *names: KeyT) -> int: ...

    @overload
    def delete(self: AsyncClientProtocol, *names: KeyT) -> Awaitable[int]: ...

    def delete(self, *names: KeyT) -> int | Awaitable[int]:
        """
        Delete one or more keys specified by ``names``
        """
        return self.execute_command("DEL", *names)

    def __delitem__(self, name: KeyT):
        self.delete(name)

    @overload
    def delex(
        self: SyncClientProtocol,
        name: KeyT,
        ifeq: bytes | str | None = None,
        ifne: bytes | str | None = None,
        ifdeq: str | None = None,
        ifdne: str | None = None,
    ) -> int: ...

    @overload
    def delex(
        self: AsyncClientProtocol,
        name: KeyT,
        ifeq: bytes | str | None = None,
        ifne: bytes | str | None = None,
        ifdeq: str | None = None,
        ifdne: str | None = None,
    ) -> Awaitable[int]: ...

    @experimental_method()
    def delex(
        self,
        name: KeyT,
        ifeq: bytes | str | None = None,
        ifne: bytes | str | None = None,
        ifdeq: str | None = None,  # hex digest
        ifdne: str | None = None,  # hex digest
    ) -> int | Awaitable[int]:
        """
        Conditionally removes the specified key.

        Warning:
        **Experimental** since 7.1.
        This API may change or be removed without notice.
        The API may change based on feedback.

        Arguments:
            name: KeyT - the key to delete
            ifeq match-valu: Optional[Union[bytes, str]] - Delete the key only if its value is equal to match-value
            ifne match-value: Optional[Union[bytes, str]] - Delete the key only if its value is not equal to match-value
            ifdeq match-digest: Optional[str] - Delete the key only if the digest of its value is equal to match-digest
            ifdne match-digest: Optional[str] - Delete the key only if the digest of its value is not equal to match-digest

        Returns:
            int: 1 if the key was deleted, 0 otherwise.
        Raises:
            redis.exceptions.ResponseError: if key exists but is not a string
                                            and a condition is specified.
            ValueError: if more than one condition is provided.


        Requires Redis 8.4 or greater.
        For more information, see https://redis.io/commands/delex
        """
        conds = [x is not None for x in (ifeq, ifne, ifdeq, ifdne)]
        if sum(conds) > 1:
            raise ValueError("Only one of IFEQ/IFNE/IFDEQ/IFDNE may be specified")

        pieces = ["DELEX", name]
        if ifeq is not None:
            pieces += ["IFEQ", ifeq]
        elif ifne is not None:
            pieces += ["IFNE", ifne]
        elif ifdeq is not None:
            pieces += ["IFDEQ", ifdeq]
        elif ifdne is not None:
            pieces += ["IFDNE", ifdne]

        return self.execute_command(*pieces)

    @overload
    def dump(self: SyncClientProtocol, name: KeyT) -> bytes | None: ...

    @overload
    def dump(self: AsyncClientProtocol, name: KeyT) -> Awaitable[bytes | None]: ...

    def dump(self, name: KeyT) -> (bytes | None) | Awaitable[bytes | None]:
        """
        Return a serialized version of the value stored at the specified key.
        If key does not exist a nil bulk reply is returned.

        For more information, see https://redis.io/commands/dump
        """
        from redis.client import NEVER_DECODE

        options = {}
        options[NEVER_DECODE] = []
        return self.execute_command("DUMP", name, **options)

    @overload
    def exists(self: SyncClientProtocol, *names: KeyT) -> int: ...

    @overload
    def exists(self: AsyncClientProtocol, *names: KeyT) -> Awaitable[int]: ...

    def exists(self, *names: KeyT) -> int | Awaitable[int]:
        """
        Returns the number of ``names`` that exist

        For more information, see https://redis.io/commands/exists
        """
        return self.execute_command("EXISTS", *names, keys=names)

    __contains__ = exists

    @overload
    def expire(
        self: SyncClientProtocol,
        name: KeyT,
        time: ExpiryT,
        nx: bool = False,
        xx: bool = False,
        gt: bool = False,
        lt: bool = False,
    ) -> bool: ...

    @overload
    def expire(
        self: AsyncClientProtocol,
        name: KeyT,
        time: ExpiryT,
        nx: bool = False,
        xx: bool = False,
        gt: bool = False,
        lt: bool = False,
    ) -> Awaitable[bool]: ...

    def expire(
        self,
        name: KeyT,
        time: ExpiryT,
        nx: bool = False,
        xx: bool = False,
        gt: bool = False,
        lt: bool = False,
    ) -> bool | Awaitable[bool]:
        """
        Set an expire flag on key ``name`` for ``time`` seconds with given
        ``option``. ``time`` can be represented by an integer or a Python timedelta
        object.

        Valid options are:
            NX -> Set expiry only when the key has no expiry
            XX -> Set expiry only when the key has an existing expiry
            GT -> Set expiry only when the new expiry is greater than current one
            LT -> Set expiry only when the new expiry is less than current one

        For more information, see https://redis.io/commands/expire
        """
        if isinstance(time, datetime.timedelta):
            time = int(time.total_seconds())

        exp_option = list()
        if nx:
            exp_option.append("NX")
        if xx:
            exp_option.append("XX")
        if gt:
            exp_option.append("GT")
        if lt:
            exp_option.append("LT")

        return self.execute_command("EXPIRE", name, time, *exp_option)

    @overload
    def expireat(
        self: SyncClientProtocol,
        name: KeyT,
        when: AbsExpiryT,
        nx: bool = False,
        xx: bool = False,
        gt: bool = False,
        lt: bool = False,
    ) -> bool: ...

    @overload
    def expireat(
        self: AsyncClientProtocol,
        name: KeyT,
        when: AbsExpiryT,
        nx: bool = False,
        xx: bool = False,
        gt: bool = False,
        lt: bool = False,
    ) -> Awaitable[bool]: ...

    def expireat(
        self,
        name: KeyT,
        when: AbsExpiryT,
        nx: bool = False,
        xx: bool = False,
        gt: bool = False,
        lt: bool = False,
    ) -> bool | Awaitable[bool]:
        """
        Set an expire flag on key ``name`` with given ``option``. ``when``
        can be represented as an integer indicating unix time or a Python
        datetime object.

        Valid options are:
            -> NX -- Set expiry only when the key has no expiry
            -> XX -- Set expiry only when the key has an existing expiry
            -> GT -- Set expiry only when the new expiry is greater than current one
            -> LT -- Set expiry only when the new expiry is less than current one

        For more information, see https://redis.io/commands/expireat
        """
        if isinstance(when, datetime.datetime):
            when = int(when.timestamp())

        exp_option = list()
        if nx:
            exp_option.append("NX")
        if xx:
            exp_option.append("XX")
        if gt:
            exp_option.append("GT")
        if lt:
            exp_option.append("LT")

        return self.execute_command("EXPIREAT", name, when, *exp_option)

    @overload
    def expiretime(self: SyncClientProtocol, key: str) -> int: ...

    @overload
    def expiretime(self: AsyncClientProtocol, key: str) -> Awaitable[int]: ...

    def expiretime(self, key: str) -> int | Awaitable[int]:
        """
        Returns the absolute Unix timestamp (since January 1, 1970) in seconds
        at which the given key will expire.

        For more information, see https://redis.io/commands/expiretime
        """
        return self.execute_command("EXPIRETIME", key, keys=[key])

    @experimental_method()
    def digest_local(self, value: bytes | str) -> bytes | str:
        """
        Compute the hexadecimal digest of the value locally, without sending it to the server.

        This is useful for conditional operations like IFDEQ/IFDNE where you need to
        compute the digest client-side before sending a command.

        Warning:
        **Experimental** - This API may change or be removed without notice.

        Arguments:
          - value: Union[bytes, str] - the value to compute the digest of.
            If a string is provided, it will be encoded using the client's configured
            encoding (default UTF-8) before hashing, which matches Redis's encoding behavior.

        Returns:
          - (str | bytes) the XXH3 digest of the value as a hex string (16 hex characters).
            Returns bytes if decode_responses is False, otherwise returns str.

        For more information, see https://redis.io/commands/digest
        """
        if not HAS_XXHASH:
            raise NotImplementedError(
                "XXHASH support requires the optional 'xxhash' library. "
                "Install it with 'pip install xxhash' or use this package's extra with "
                "'pip install redis[xxhash]' to enable this feature."
            )

        encoder = self.get_encoder()
        encoded_value = encoder.encode(value)

        # xxhash.xxh3_64 produces a 64-bit (16-hex character) hash
        local_digest = xxhash.xxh3_64(encoded_value).hexdigest()

        # To align with digest, we want to return bytes if decode_responses is False.
        # The following works because of Python's mixin-based client class hierarchy.
        if not encoder.decode_responses:
            local_digest = local_digest.encode("ascii")

        return local_digest

    @overload
    def digest(self: SyncClientProtocol, name: KeyT) -> str | bytes | None: ...

    @overload
    def digest(
        self: AsyncClientProtocol, name: KeyT
    ) -> Awaitable[str | bytes | None]: ...

    @experimental_method()
    def digest(self, name: KeyT) -> (str | bytes | None) | Awaitable[
        str | bytes | None
    ]:
        """
        Return the digest of the value stored at the specified key.

        Warning:
        **Experimental** since 7.1.
        This API may change or be removed without notice.
        The API may change based on feedback.

        Arguments:
          - name: KeyT - the key to get the digest of

        Returns:
          - None if the key does not exist
          - (bulk string) the XXH3 digest of the value as a hex string
        Raises:
          - ResponseError if key exists but is not a string


        Requires Redis 8.4 or greater.
        For more information, see https://redis.io/commands/digest
        """
        # Bulk string response is already handled (bytes/str based on decode_responses)
        return self.execute_command("DIGEST", name, keys=[name])

    @overload
    def get(self: SyncClientProtocol, name: KeyT) -> bytes | str | None: ...

    @overload
    def get(self: AsyncClientProtocol, name: KeyT) -> Awaitable[bytes | str | None]: ...

    def get(self, name: KeyT) -> (bytes | str | None) | Awaitable[bytes | str | None]:
        """
        Return the value at key ``name``, or None if the key doesn't exist

        For more information, see https://redis.io/commands/get
        """
        return self.execute_command("GET", name, keys=[name])

    @overload
    def getdel(self: SyncClientProtocol, name: KeyT) -> bytes | str | None: ...

    @overload
    def getdel(
        self: AsyncClientProtocol, name: KeyT
    ) -> Awaitable[bytes | str | None]: ...

    def getdel(self, name: KeyT) -> (bytes | str | None) | Awaitable[
        bytes | str | None
    ]:
        """
        Get the value at key ``name`` and delete the key. This command
        is similar to GET, except for the fact that it also deletes
        the key on success (if and only if the key's value type
        is a string).

        For more information, see https://redis.io/commands/getdel
        """
        return self.execute_command("GETDEL", name)

    @overload
    def getex(
        self: SyncClientProtocol,
        name: KeyT,
        ex: ExpiryT | str | None = None,
        px: ExpiryT | str | None = None,
        exat: AbsExpiryT | None = None,
        pxat: AbsExpiryT | None = None,
        persist: bool = False,
    ) -> bytes | str | None: ...

    @overload
    def getex(
        self: AsyncClientProtocol,
        name: KeyT,
        ex: ExpiryT | str | None = None,
        px: ExpiryT | str | None = None,
        exat: AbsExpiryT | None = None,
        pxat: AbsExpiryT | None = None,
        persist: bool = False,
    ) -> Awaitable[bytes | str | None]: ...

    def getex(
        self,
        name: KeyT,
        ex: ExpiryT | str | None = None,
        px: ExpiryT | str | None = None,
        exat: AbsExpiryT | None = None,
        pxat: AbsExpiryT | None = None,
        persist: bool = False,
    ) -> (bytes | str | None) | Awaitable[bytes | str | None]:
        """
        Get the value of key and optionally set its expiration.
        GETEX is similar to GET, but is a write command with
        additional options. All time parameters can be given as
        datetime.timedelta or integers.

        ``ex`` sets an expire flag on key ``name`` for ``ex`` seconds.

        ``px`` sets an expire flag on key ``name`` for ``px`` milliseconds.

        ``ex`` and ``px`` also accept digit-only strings, such as ``"60"``
        and ``"500"``, respectively.

        ``exat`` sets an expire flag on key ``name`` for ``ex`` seconds,
        specified in unix time.

        ``pxat`` sets an expire flag on key ``name`` for ``ex`` milliseconds,
        specified in unix time.

        ``persist`` remove the time to live associated with ``name``.

        For more information, see https://redis.io/commands/getex
        """
        if not at_most_one_value_set(
            (
                ex is not None,
                px is not None,
                exat is not None,
                pxat is not None,
                persist,
            )
        ):
            raise DataError(
                "``ex``, ``px``, ``exat``, ``pxat``, "
                "and ``persist`` are mutually exclusive."
            )

        exp_options: list[EncodableT] = extract_expire_flags(ex, px, exat, pxat)

        if persist:
            exp_options.append("PERSIST")

        return self.execute_command("GETEX", name, *exp_options)

    def __getitem__(self, name: KeyT):
        """
        Return the value at key ``name``, raises a KeyError if the key
        doesn't exist.
        """
        value = self.get(name)
        if value is not None:
            return value
        raise KeyError(name)

    @overload
    def getbit(self: SyncClientProtocol, name: KeyT, offset: int) -> int: ...

    @overload
    def getbit(
        self: AsyncClientProtocol, name: KeyT, offset: int
    ) -> Awaitable[int]: ...

    def getbit(self, name: KeyT, offset: int) -> int | Awaitable[int]:
        """
        Returns an integer indicating the value of ``offset`` in ``name``

        For more information, see https://redis.io/commands/getbit
        """
        return self.execute_command("GETBIT", name, offset, keys=[name])

    @overload
    def getrange(
        self: SyncClientProtocol, key: KeyT, start: int, end: int
    ) -> bytes | str: ...

    @overload
    def getrange(
        self: AsyncClientProtocol, key: KeyT, start: int, end: int
    ) -> Awaitable[bytes | str]: ...

    def getrange(self, key: KeyT, start: int, end: int) -> (bytes | str) | Awaitable[
        bytes | str
    ]:
        """
        Returns the substring of the string value stored at ``key``,
        determined by the offsets ``start`` and ``end`` (both are inclusive)

        For more information, see https://redis.io/commands/getrange
        """
        return self.execute_command("GETRANGE", key, start, end, keys=[key])

    @overload
    def getset(
        self: SyncClientProtocol, name: KeyT, value: EncodableT
    ) -> bytes | str | None: ...

    @overload
    def getset(
        self: AsyncClientProtocol, name: KeyT, value: EncodableT
    ) -> Awaitable[bytes | str | None]: ...

    def getset(self, name: KeyT, value: EncodableT) -> (bytes | str | None) | Awaitable[
        bytes | str | None
    ]:
        """
        Sets the value at key ``name`` to ``value``
        and returns the old value at key ``name`` atomically.

        As per Redis 6.2, GETSET is considered deprecated.
        Please use SET with GET parameter in new code.

        For more information, see https://redis.io/commands/getset
        """
        return self.execute_command("GETSET", name, value)

    @overload
    def incrby(self: SyncClientProtocol, name: KeyT, amount: int = 1) -> int: ...

    @overload
    def incrby(
        self: AsyncClientProtocol, name: KeyT, amount: int = 1
    ) -> Awaitable[int]: ...

    def incrby(self, name: KeyT, amount: int = 1) -> int | Awaitable[int]:
        """
        Increments the value of ``key`` by ``amount``.  If no key exists,
        the value will be initialized as ``amount``

        For more information, see https://redis.io/commands/incrby
        """
        return self.execute_command("INCRBY", name, amount)

    incr = incrby

    @overload
    def incrbyfloat(
        self: SyncClientProtocol, name: KeyT, amount: float = 1.0
    ) -> float: ...

    @overload
    def incrbyfloat(
        self: AsyncClientProtocol, name: KeyT, amount: float = 1.0
    ) -> Awaitable[float]: ...

    def incrbyfloat(self, name: KeyT, amount: float = 1.0) -> float | Awaitable[float]:
        """
        Increments the value at key ``name`` by floating ``amount``.
        If no key exists, the value will be initialized as ``amount``

        For more information, see https://redis.io/commands/incrbyfloat
        """
        return self.execute_command("INCRBYFLOAT", name, amount)

    @overload
    def increx(
        self: SyncClientProtocol,
        name: KeyT,
        *,
        byfloat: EncodableT | None = None,
        byint: EncodableT | None = None,
        lbound: EncodableT | None = None,
        ubound: EncodableT | None = None,
        saturate: bool = False,
        ex: ExpiryT | str | None = None,
        px: ExpiryT | str | None = None,
        exat: AbsExpiryT | None = None,
        pxat: AbsExpiryT | None = None,
        persist: bool = False,
        enx: bool = False,
    ) -> list[Number | bytes | str]: ...

    @overload
    def increx(
        self: AsyncClientProtocol,
        name: KeyT,
        *,
        byfloat: EncodableT | None = None,
        byint: EncodableT | None = None,
        lbound: EncodableT | None = None,
        ubound: EncodableT | None = None,
        saturate: bool = False,
        ex: ExpiryT | str | None = None,
        px: ExpiryT | str | None = None,
        exat: AbsExpiryT | None = None,
        pxat: AbsExpiryT | None = None,
        persist: bool = False,
        enx: bool = False,
    ) -> Awaitable[list[Number | bytes | str]]: ...

    def increx(
        self,
        name: KeyT,
        *,
        byfloat: EncodableT | None = None,
        byint: EncodableT | None = None,
        lbound: EncodableT | None = None,
        ubound: EncodableT | None = None,
        saturate: bool = False,
        ex: ExpiryT | str | None = None,
        px: ExpiryT | str | None = None,
        exat: AbsExpiryT | None = None,
        pxat: AbsExpiryT | None = None,
        persist: bool = False,
        enx: bool = False,
    ) -> list[Number | bytes | str] | Awaitable[list[Number | bytes | str]]:
        """
        Increment the numeric value at key ``name`` and return the new value
        and the actual increment.

        ``byfloat`` increments a floating point value by the provided amount.

        ``byint`` increments an integer value by the provided amount.

        If neither ``byfloat`` nor ``byint`` is specified, the value is
        incremented by one.

        ``lbound`` and ``ubound`` constrain the valid range of the result.

        If ``saturate`` is True, out-of-bounds results are saturated to the
        specified bound, or to the type limit when no bound is specified.
        Otherwise, out-of-bounds results are rejected, leaving the value and
        TTL unchanged and returning the current value and zero as the actual
        increment.

        ``ex`` and ``px`` accept digit-only strings for seconds and milliseconds,
        respectively, in addition to integers and timedeltas.

        ``enx`` applies the expiration only when the key does not already
        have an expiration, and requires ``ex``, ``px``, ``exat``, or ``pxat``.
        """
        if not at_most_one_value_set(
            (
                byfloat is not None,
                byint is not None,
            )
        ):
            raise DataError("``byfloat`` and ``byint`` are mutually exclusive.")

        if not at_most_one_value_set(
            (
                ex is not None,
                px is not None,
                exat is not None,
                pxat is not None,
                persist,
            )
        ):
            raise DataError(
                "``ex``, ``px``, ``exat``, ``pxat``, "
                "and ``persist`` are mutually exclusive."
            )

        if enx and ex is None and px is None and exat is None and pxat is None:
            raise DataError(
                "``enx`` requires one of ``ex``, ``px``, ``exat``, or ``pxat``."
            )

        pieces: list[EncodableT] = [name]

        if byfloat is not None:
            pieces.extend(("BYFLOAT", byfloat))
        elif byint is not None:
            pieces.extend(("BYINT", byint))

        if lbound is not None:
            pieces.extend(("LBOUND", lbound))
        if ubound is not None:
            pieces.extend(("UBOUND", ubound))
        if saturate:
            pieces.append("SATURATE")

        pieces.extend(extract_expire_flags(ex, px, exat, pxat))
        if persist:
            pieces.append("PERSIST")
        if enx:
            pieces.append("ENX")

        return self.execute_command("INCREX", *pieces)

    @overload
    def keys(
        self: SyncClientProtocol, pattern: PatternT = "*", **kwargs
    ) -> list[bytes | str]: ...

    @overload
    def keys(
        self: AsyncClientProtocol, pattern: PatternT = "*", **kwargs
    ) -> Awaitable[list[bytes | str]]: ...

    def keys(
        self, pattern: PatternT = "*", **kwargs
    ) -> list[bytes | str] | Awaitable[list[bytes | str]]:
        """
        Returns a list of keys matching ``pattern``

        For more information, see https://redis.io/commands/keys
        """
        return self.execute_command("KEYS", pattern, **kwargs)

    @overload
    def lmove(
        self: SyncClientProtocol,
        first_list: str,
        second_list: str,
        src: str = "LEFT",
        dest: str = "RIGHT",
    ) -> bytes | str | None: ...

    @overload
    def lmove(
        self: AsyncClientProtocol,
        first_list: str,
        second_list: str,
        src: str = "LEFT",
        dest: str = "RIGHT",
    ) -> Awaitable[bytes | str | None]: ...

    def lmove(
        self, first_list: str, second_list: str, src: str = "LEFT", dest: str = "RIGHT"
    ) -> (bytes | str | None) | Awaitable[bytes | str | None]:
        """
        Atomically returns and removes the first/last element of a list,
        pushing it as the first/last element on the destination list.
        Returns the element being popped and pushed.

        For more information, see https://redis.io/commands/lmove
        """
        params = [first_list, second_list, src, dest]
        return self.execute_command("LMOVE", *params)

    @overload
    def blmove(
        self: SyncClientProtocol,
        first_list: str,
        second_list: str,
        timeout: int,
        src: str = "LEFT",
        dest: str = "RIGHT",
    ) -> bytes | str | None: ...

    @overload
    def blmove(
        self: AsyncClientProtocol,
        first_list: str,
        second_list: str,
        timeout: int,
        src: str = "LEFT",
        dest: str = "RIGHT",
    ) -> Awaitable[bytes | str | None]: ...

    def blmove(
        self,
        first_list: str,
        second_list: str,
        timeout: int,
        src: str = "LEFT",
        dest: str = "RIGHT",
    ) -> (bytes | str | None) | Awaitable[bytes | str | None]:
        """
        Blocking version of lmove.

        For more information, see https://redis.io/commands/blmove
        """
        params = [first_list, second_list, src, dest, timeout]
        return self.execute_command("BLMOVE", *params)

    @overload
    def lmovem(
        self: SyncClientProtocol,
        first_list: str,
        second_list: str,
        src: str = "LEFT",
        dest: str = "RIGHT",
        count: int | None = None,
        mode: Literal["COUNT", "EXACTLY"] | None = None,
        ordering: Literal["OBO", "BULK"] | None = None,
    ) -> list[bytes | str] | None: ...

    @overload
    def lmovem(
        self: AsyncClientProtocol,
        first_list: str,
        second_list: str,
        src: str = "LEFT",
        dest: str = "RIGHT",
        count: int | None = None,
        mode: Literal["COUNT", "EXACTLY"] | None = None,
        ordering: Literal["OBO", "BULK"] | None = None,
    ) -> Awaitable[list[bytes | str] | None]: ...

    def lmovem(
        self,
        first_list: str,
        second_list: str,
        src: str = "LEFT",
        dest: str = "RIGHT",
        count: int | None = None,
        mode: Literal["COUNT", "EXACTLY"] | None = None,
        ordering: Literal["OBO", "BULK"] | None = None,
    ) -> (list[bytes | str] | None) | Awaitable[list[bytes | str] | None]:
        """
        Atomically moves multiple elements from one end of the ``first_list``
        to one end of the ``second_list``, returning the moved elements in
        destination order.

        ``src`` and ``dest`` are the ends to pop from / push to, either
        ``LEFT`` (head) or ``RIGHT`` (tail).

        When ``count`` is given, ``mode`` selects how many elements to move:
        ``COUNT`` (the default) moves up to ``count`` elements, while
        ``EXACTLY`` moves exactly ``count`` elements or nothing at all. When
        ``count`` is not given a single element is moved.

        ``ordering`` controls the order at the destination: ``OBO`` pushes each
        element as it is popped (reversing block order, stack semantics) while
        ``BULK`` preserves the original relative order (queue semantics). It is
        required whenever ``count`` is given.

        Returns the array of moved elements, or ``None`` if nothing was moved.

        For more information, see https://redis.io/commands/lmovem
        """
        if count is None:
            if mode is not None or ordering is not None:
                raise DataError(
                    "``count`` is required when ``mode`` or ``ordering`` is set"
                )
        elif ordering is None:
            raise DataError(
                "``ordering`` (OBO or BULK) is required when ``count`` is set"
            )
        pieces: list[EncodableT] = [first_list, second_list, src, dest]
        if count is not None:
            pieces.extend([mode or "COUNT", count, ordering])
        return self.execute_command("LMOVEM", *pieces)

    @overload
    def blmovem(
        self: SyncClientProtocol,
        first_list: str,
        second_list: str,
        timeout: float,
        src: str = "LEFT",
        dest: str = "RIGHT",
        count: int | None = None,
        mode: Literal["COUNT", "EXACTLY"] | None = None,
        ordering: Literal["OBO", "BULK"] | None = None,
    ) -> list[bytes | str] | None: ...

    @overload
    def blmovem(
        self: AsyncClientProtocol,
        first_list: str,
        second_list: str,
        timeout: float,
        src: str = "LEFT",
        dest: str = "RIGHT",
        count: int | None = None,
        mode: Literal["COUNT", "EXACTLY"] | None = None,
        ordering: Literal["OBO", "BULK"] | None = None,
    ) -> Awaitable[list[bytes | str] | None]: ...

    def blmovem(
        self,
        first_list: str,
        second_list: str,
        timeout: float,
        src: str = "LEFT",
        dest: str = "RIGHT",
        count: int | None = None,
        mode: Literal["COUNT", "EXACTLY"] | None = None,
        ordering: Literal["OBO", "BULK"] | None = None,
    ) -> (list[bytes | str] | None) | Awaitable[list[bytes | str] | None]:
        """
        Blocking version of lmovem.

        Blocks until elements are available to move or ``timeout`` (in seconds)
        is reached; a ``timeout`` of ``0`` blocks indefinitely. With ``COUNT``
        the command unblocks once at least one element is available and moves
        ``min(count, available)``; with ``EXACTLY`` it blocks until the source
        holds at least ``count`` elements, then moves exactly ``count``
        atomically. On timeout ``None`` is returned and nothing is moved.

        As with ``lmovem``, ``ordering`` (OBO or BULK) is required whenever
        ``count`` is given.

        For more information, see https://redis.io/commands/blmovem
        """
        if count is None:
            if mode is not None or ordering is not None:
                raise DataError(
                    "``count`` is required when ``mode`` or ``ordering`` is set"
                )
        elif ordering is None:
            raise DataError(
                "``ordering`` (OBO or BULK) is required when ``count`` is set"
            )
        pieces: list[EncodableT] = [first_list, second_list, src, dest, timeout]
        if count is not None:
            pieces.extend([mode or "COUNT", count, ordering])
        return self.execute_command("BLMOVEM", *pieces)

    @overload
    def mget(
        self: SyncClientProtocol, keys: KeysT, *args: EncodableT
    ) -> list[bytes | str | None]: ...

    @overload
    def mget(
        self: AsyncClientProtocol, keys: KeysT, *args: EncodableT
    ) -> Awaitable[list[bytes | str | None]]: ...

    def mget(
        self, keys: KeysT, *args: EncodableT
    ) -> list[bytes | str | None] | Awaitable[list[bytes | str | None]]:
        """
        Returns a list of values ordered identically to ``keys``

        ** Important ** When this method is used with Cluster clients, all keys
                must be in the same hash slot, otherwise a RedisClusterException
                will be raised.

        For more information, see https://redis.io/commands/mget
        """
        from redis.client import EMPTY_RESPONSE

        args = list_or_args(keys, args)
        options = {}
        if not args:
            options[EMPTY_RESPONSE] = []
        options["keys"] = args
        return self.execute_command("MGET", *args, **options)

    @overload
    def mset(
        self: SyncClientProtocol, mapping: Mapping[AnyKeyT, EncodableT]
    ) -> bool: ...

    @overload
    def mset(
        self: AsyncClientProtocol, mapping: Mapping[AnyKeyT, EncodableT]
    ) -> Awaitable[bool]: ...

    def mset(self, mapping: Mapping[AnyKeyT, EncodableT]) -> bool | Awaitable[bool]:
        """
        Sets key/values based on a mapping. Mapping is a dictionary of
        key/value pairs. Both keys and values should be strings or types that
        can be cast to a string via str().

        ** Important ** When this method is used with Cluster clients, all keys
                must be in the same hash slot, otherwise a RedisClusterException
                will be raised.

        For more information, see https://redis.io/commands/mset
        """
        items = []
        for pair in mapping.items():
            items.extend(pair)
        return self.execute_command("MSET", *items)

    @overload
    def msetex(
        self: SyncClientProtocol,
        mapping: Mapping[AnyKeyT, EncodableT],
        data_persist_option: DataPersistOptions | None = None,
        ex: ExpiryT | str | None = None,
        px: ExpiryT | str | None = None,
        exat: AbsExpiryT | None = None,
        pxat: AbsExpiryT | None = None,
        keepttl: bool = False,
    ) -> int: ...

    @overload
    def msetex(
        self: AsyncClientProtocol,
        mapping: Mapping[AnyKeyT, EncodableT],
        data_persist_option: DataPersistOptions | None = None,
        ex: ExpiryT | str | None = None,
        px: ExpiryT | str | None = None,
        exat: AbsExpiryT | None = None,
        pxat: AbsExpiryT | None = None,
        keepttl: bool = False,
    ) -> Awaitable[int]: ...

    def msetex(
        self,
        mapping: Mapping[AnyKeyT, EncodableT],
        data_persist_option: DataPersistOptions | None = None,
        ex: ExpiryT | str | None = None,
        px: ExpiryT | str | None = None,
        exat: AbsExpiryT | None = None,
        pxat: AbsExpiryT | None = None,
        keepttl: bool = False,
    ) -> int | Awaitable[int]:
        """
        Sets key/values based on the provided ``mapping`` items.

        ** Important ** When this method is used with Cluster clients, all keys
                        must be in the same hash slot, otherwise a RedisClusterException
                        will be raised.

        ``mapping`` accepts a dict of key/value pairs that will be added to the database.

        ``data_persist_option`` can be set to ``NX`` or ``XX`` to control the
            behavior of the command.
            ``NX`` will set the value for each provided key to each
                provided value only if all do not already exist.
            ``XX`` will set the value for each provided key to each
                provided value only if all already exist.

        ``ex`` sets an expire flag on the keys in ``mapping`` for ``ex`` seconds.

        ``px`` sets an expire flag on the keys in ``mapping`` for ``px`` milliseconds.

        ``ex`` and ``px`` also accept digit-only strings, such as ``"60"``
        and ``"500"``, respectively.

        ``exat`` sets an expire flag on the keys in ``mapping`` for ``exat`` seconds,
            specified in unix time.

        ``pxat`` sets an expire flag on the keys in ``mapping`` for ``pxat`` milliseconds,
            specified in unix time.

        ``keepttl`` if True, retain the time to live associated with the keys.

        Returns the number of fields that were added.

        Available since Redis 8.4
        For more information, see https://redis.io/commands/msetex
        """
        if not at_most_one_value_set(
            (
                ex is not None,
                px is not None,
                exat is not None,
                pxat is not None,
                keepttl,
            )
        ):
            raise DataError(
                "``ex``, ``px``, ``exat``, ``pxat``, "
                "and ``keepttl`` are mutually exclusive."
            )

        exp_options: list[EncodableT] = []
        if data_persist_option:
            exp_options.append(data_persist_option.value)

        exp_options.extend(extract_expire_flags(ex, px, exat, pxat))

        if keepttl:
            exp_options.append("KEEPTTL")

        pieces = ["MSETEX", len(mapping)]

        for pair in mapping.items():
            pieces.extend(pair)

        return self.execute_command(*pieces, *exp_options)

    @overload
    def msetnx(
        self: SyncClientProtocol, mapping: Mapping[AnyKeyT, EncodableT]
    ) -> bool: ...

    @overload
    def msetnx(
        self: AsyncClientProtocol, mapping: Mapping[AnyKeyT, EncodableT]
    ) -> Awaitable[bool]: ...

    def msetnx(self, mapping: Mapping[AnyKeyT, EncodableT]) -> bool | Awaitable[bool]:
        """
        Sets key/values based on a mapping if none of the keys are already set.
        Mapping is a dictionary of key/value pairs. Both keys and values
        should be strings or types that can be cast to a string via str().
        Returns a boolean indicating if the operation was successful.

        ** Important ** When this method is used with Cluster clients, all keys
                        must be in the same hash slot, otherwise a RedisClusterException
                        will be raised.

        For more information, see https://redis.io/commands/msetnx
        """
        items = []
        for pair in mapping.items():
            items.extend(pair)
        return self.execute_command("MSETNX", *items)

    @overload
    def move(self: SyncClientProtocol, name: KeyT, db: int) -> bool: ...

    @overload
    def move(self: AsyncClientProtocol, name: KeyT, db: int) -> Awaitable[bool]: ...

    def move(self, name: KeyT, db: int) -> bool | Awaitable[bool]:
        """
        Moves the key ``name`` to a different Redis database ``db``

        For more information, see https://redis.io/commands/move
        """
        return self.execute_command("MOVE", name, db)

    @overload
    def persist(self: SyncClientProtocol, name: KeyT) -> bool: ...

    @overload
    def persist(self: AsyncClientProtocol, name: KeyT) -> Awaitable[bool]: ...

    def persist(self, name: KeyT) -> bool | Awaitable[bool]:
        """
        Removes an expiration on ``name``

        For more information, see https://redis.io/commands/persist
        """
        return self.execute_command("PERSIST", name)

    @overload
    def pexpire(
        self: SyncClientProtocol,
        name: KeyT,
        time: ExpiryT,
        nx: bool = False,
        xx: bool = False,
        gt: bool = False,
        lt: bool = False,
    ) -> bool: ...

    @overload
    def pexpire(
        self: AsyncClientProtocol,
        name: KeyT,
        time: ExpiryT,
        nx: bool = False,
        xx: bool = False,
        gt: bool = False,
        lt: bool = False,
    ) -> Awaitable[bool]: ...

    def pexpire(
        self,
        name: KeyT,
        time: ExpiryT,
        nx: bool = False,
        xx: bool = False,
        gt: bool = False,
        lt: bool = False,
    ) -> bool | Awaitable[bool]:
        """
        Set an expire flag on key ``name`` for ``time`` milliseconds
        with given ``option``. ``time`` can be represented by an
        integer or a Python timedelta object.

        Valid options are:
            NX -> Set expiry only when the key has no expiry
            XX -> Set expiry only when the key has an existing expiry
            GT -> Set expiry only when the new expiry is greater than current one
            LT -> Set expiry only when the new expiry is less than current one

        For more information, see https://redis.io/commands/pexpire
        """
        if isinstance(time, datetime.timedelta):
            time = int(time.total_seconds() * 1000)

        exp_option = list()
        if nx:
            exp_option.append("NX")
        if xx:
            exp_option.append("XX")
        if gt:
            exp_option.append("GT")
        if lt:
            exp_option.append("LT")
        return self.execute_command("PEXPIRE", name, time, *exp_option)

    @overload
    def pexpireat(
        self: SyncClientProtocol,
        name: KeyT,
        when: AbsExpiryT,
        nx: bool = False,
        xx: bool = False,
        gt: bool = False,
        lt: bool = False,
    ) -> bool: ...

    @overload
    def pexpireat(
        self: AsyncClientProtocol,
        name: KeyT,
        when: AbsExpiryT,
        nx: bool = False,
        xx: bool = False,
        gt: bool = False,
        lt: bool = False,
    ) -> Awaitable[bool]: ...

    def pexpireat(
        self,
        name: KeyT,
        when: AbsExpiryT,
        nx: bool = False,
        xx: bool = False,
        gt: bool = False,
        lt: bool = False,
    ) -> bool | Awaitable[bool]:
        """
        Set an expire flag on key ``name`` with given ``option``. ``when``
        can be represented as an integer representing unix time in
        milliseconds (unix time * 1000) or a Python datetime object.

        Valid options are:
            NX -> Set expiry only when the key has no expiry
            XX -> Set expiry only when the key has an existing expiry
            GT -> Set expiry only when the new expiry is greater than current one
            LT -> Set expiry only when the new expiry is less than current one

        For more information, see https://redis.io/commands/pexpireat
        """
        if isinstance(when, datetime.datetime):
            when = int(when.timestamp() * 1000)
        exp_option = list()
        if nx:
            exp_option.append("NX")
        if xx:
            exp_option.append("XX")
        if gt:
            exp_option.append("GT")
        if lt:
            exp_option.append("LT")
        return self.execute_command("PEXPIREAT", name, when, *exp_option)

    @overload
    def pexpiretime(self: SyncClientProtocol, key: str) -> int: ...

    @overload
    def pexpiretime(self: AsyncClientProtocol, key: str) -> Awaitable[int]: ...

    def pexpiretime(self, key: str) -> int | Awaitable[int]:
        """
        Returns the absolute Unix timestamp (since January 1, 1970) in milliseconds
        at which the given key will expire.

        For more information, see https://redis.io/commands/pexpiretime
        """
        return self.execute_command("PEXPIRETIME", key, keys=[key])

    @overload
    def psetex(
        self: SyncClientProtocol, name: KeyT, time_ms: ExpiryT, value: EncodableT
    ) -> bool: ...

    @overload
    def psetex(
        self: AsyncClientProtocol, name: KeyT, time_ms: ExpiryT, value: EncodableT
    ) -> Awaitable[bool]: ...

    def psetex(
        self, name: KeyT, time_ms: ExpiryT, value: EncodableT
    ) -> bool | Awaitable[bool]:
        """
        Set the value of key ``name`` to ``value`` that expires in ``time_ms``
        milliseconds. ``time_ms`` can be represented by an integer or a Python
        timedelta object

        For more information, see https://redis.io/commands/psetex
        """
        if isinstance(time_ms, datetime.timedelta):
            time_ms = int(time_ms.total_seconds() * 1000)
        return self.execute_command("PSETEX", name, time_ms, value)

    @overload
    def pttl(self: SyncClientProtocol, name: KeyT) -> int: ...

    @overload
    def pttl(self: AsyncClientProtocol, name: KeyT) -> Awaitable[int]: ...

    def pttl(self, name: KeyT) -> int | Awaitable[int]:
        """
        Returns the number of milliseconds until the key ``name`` will expire

        For more information, see https://redis.io/commands/pttl
        """
        return self.execute_command("PTTL", name)

    @overload
    def hrandfield(
        self: SyncClientProtocol,
        key: KeyT,
        count: int | None = None,
        withvalues: bool = False,
    ) -> HRandFieldResponse: ...

    @overload
    def hrandfield(
        self: AsyncClientProtocol,
        key: KeyT,
        count: int | None = None,
        withvalues: bool = False,
    ) -> Awaitable[HRandFieldResponse]: ...

    def hrandfield(
        self, key: KeyT, count: int | None = None, withvalues: bool = False
    ) -> HRandFieldResponse | Awaitable[HRandFieldResponse]:
        """
        Return a random field from the hash value stored at key.

        count: if the argument is positive, return an array of distinct fields.
        If called with a negative count, the behavior changes and the command
        is allowed to return the same field multiple times. In this case,
        the number of returned fields is the absolute value of the
        specified count.
        withvalues: The optional WITHVALUES modifier changes the reply so it
        includes the respective values of the randomly selected hash fields.

        For more information, see https://redis.io/commands/hrandfield
        """
        params = []
        if count is not None:
            params.append(count)
        if withvalues:
            params.append("WITHVALUES")

        return self.execute_command("HRANDFIELD", key, *params, withvalues=withvalues)

    @overload
    def randomkey(self: SyncClientProtocol, **kwargs) -> bytes | str | None: ...

    @overload
    def randomkey(
        self: AsyncClientProtocol, **kwargs
    ) -> Awaitable[bytes | str | None]: ...

    def randomkey(self, **kwargs) -> (bytes | str | None) | Awaitable[
        bytes | str | None
    ]:
        """
        Returns the name of a random key

        For more information, see https://redis.io/commands/randomkey
        """
        return self.execute_command("RANDOMKEY", **kwargs)

    @overload
    def rename(self: SyncClientProtocol, src: KeyT, dst: KeyT) -> bool: ...

    @overload
    def rename(self: AsyncClientProtocol, src: KeyT, dst: KeyT) -> Awaitable[bool]: ...

    def rename(self, src: KeyT, dst: KeyT) -> bool | Awaitable[bool]:
        """
        Rename key ``src`` to ``dst``

        For more information, see https://redis.io/commands/rename
        """
        return self.execute_command("RENAME", src, dst)

    @overload
    def renamenx(self: SyncClientProtocol, src: KeyT, dst: KeyT) -> bool: ...

    @overload
    def renamenx(
        self: AsyncClientProtocol, src: KeyT, dst: KeyT
    ) -> Awaitable[bool]: ...

    def renamenx(self, src: KeyT, dst: KeyT) -> bool | Awaitable[bool]:
        """
        Rename key ``src`` to ``dst`` if ``dst`` doesn't already exist

        For more information, see https://redis.io/commands/renamenx
        """
        return self.execute_command("RENAMENX", src, dst)

    @overload
    def restore(
        self: SyncClientProtocol,
        name: KeyT,
        ttl: float,
        value: EncodableT,
        replace: bool = False,
        absttl: bool = False,
        idletime: int | None = None,
        frequency: int | None = None,
    ) -> bytes | str: ...

    @overload
    def restore(
        self: AsyncClientProtocol,
        name: KeyT,
        ttl: float,
        value: EncodableT,
        replace: bool = False,
        absttl: bool = False,
        idletime: int | None = None,
        frequency: int | None = None,
    ) -> Awaitable[bytes | str]: ...

    def restore(
        self,
        name: KeyT,
        ttl: float,
        value: EncodableT,
        replace: bool = False,
        absttl: bool = False,
        idletime: int | None = None,
        frequency: int | None = None,
    ) -> (bytes | str) | Awaitable[bytes | str]:
        """
        Create a key using the provided serialized value, previously obtained
        using DUMP.

        ``replace`` allows an existing key on ``name`` to be overridden. If
        it's not specified an error is raised on collision.

        ``absttl`` if True, specified ``ttl`` should represent an absolute Unix
        timestamp in milliseconds in which the key will expire. (Redis 5.0 or
        greater).

        ``idletime`` Used for eviction, this is the number of seconds the
        key must be idle, prior to execution.

        ``frequency`` Used for eviction, this is the frequency counter of
        the object stored at the key, prior to execution.

        For more information, see https://redis.io/commands/restore
        """
        params = [name, ttl, value]
        if replace:
            params.append("REPLACE")
        if absttl:
            params.append("ABSTTL")
        if idletime is not None:
            params.append("IDLETIME")
            try:
                params.append(int(idletime))
            except ValueError:
                raise DataError("idletimemust be an integer")

        if frequency is not None:
            params.append("FREQ")
            try:
                params.append(int(frequency))
            except ValueError:
                raise DataError("frequency must be an integer")

        return self.execute_command("RESTORE", *params)

    @overload
    def set(
        self: SyncClientProtocol,
        name: KeyT,
        value: EncodableT,
        ex: ExpiryT | str | None = ...,
        px: ExpiryT | str | None = ...,
        nx: bool = ...,
        xx: bool = ...,
        keepttl: bool = ...,
        get: bool = ...,
        exat: AbsExpiryT | None = ...,
        pxat: AbsExpiryT | None = ...,
        ifeq: bytes | str | None = ...,
        ifne: bytes | str | None = ...,
        ifdeq: str | None = ...,
        ifdne: str | None = ...,
    ) -> bool | str | bytes | None: ...

    @overload
    def set(
        self: AsyncClientProtocol,
        name: KeyT,
        value: EncodableT,
        ex: ExpiryT | str | None = ...,
        px: ExpiryT | str | None = ...,
        nx: bool = ...,
        xx: bool = ...,
        keepttl: bool = ...,
        get: bool = ...,
        exat: AbsExpiryT | None = ...,
        pxat: AbsExpiryT | None = ...,
        ifeq: bytes | str | None = ...,
        ifne: bytes | str | None = ...,
        ifdeq: str | None = ...,
        ifdne: str | None = ...,
    ) -> Awaitable[bool | str | bytes | None]: ...

    @experimental_args(["ifeq", "ifne", "ifdeq", "ifdne"])
    def set(
        self,
        name: KeyT,
        value: EncodableT,
        ex: ExpiryT | str | None = None,
        px: ExpiryT | str | None = None,
        nx: bool = False,
        xx: bool = False,
        keepttl: bool = False,
        get: bool = False,
        exat: AbsExpiryT | None = None,
        pxat: AbsExpiryT | None = None,
        ifeq: bytes | str | None = None,
        ifne: bytes | str | None = None,
        ifdeq: str | None = None,  # hex digest of current value
        ifdne: str | None = None,  # hex digest of current value
    ) -> (bool | str | bytes | None) | Awaitable[bool | str | bytes | None]:
        """
        Set the value at key ``name`` to ``value``

        Warning:
        **Experimental** since 7.1.
        The usage of the arguments ``ifeq``, ``ifne``, ``ifdeq``, and ``ifdne``
        is experimental. The API or returned results when those parameters are used
        may change based on feedback.

        ``ex`` sets an expire flag on key ``name`` for ``ex`` seconds.

        ``px`` sets an expire flag on key ``name`` for ``px`` milliseconds.

        ``ex`` and ``px`` also accept digit-only strings, such as ``"60"``
        and ``"500"``, respectively.

        ``nx`` if set to True, set the value at key ``name`` to ``value`` only
            if it does not exist.

        ``xx`` if set to True, set the value at key ``name`` to ``value`` only
            if it already exists.

        ``keepttl`` if True, retain the time to live associated with the key.
            (Available since Redis 6.0)

        ``get`` if True, set the value at key ``name`` to ``value`` and return
            the old value stored at key, or None if the key did not exist.
            (Available since Redis 6.2)

        ``exat`` sets an expire flag on key ``name`` for ``ex`` seconds,
            specified in unix time.

        ``pxat`` sets an expire flag on key ``name`` for ``ex`` milliseconds,
            specified in unix time.

        ``ifeq`` set the value at key ``name`` to ``value`` only if the current
            value exactly matches the argument.
            If key doesn’t exist - it won’t be created.
            (Requires Redis 8.4 or greater)

        ``ifne`` set the value at key ``name`` to ``value`` only if the current
            value does not exactly match the argument.
            If key doesn’t exist - it will be created.
            (Requires Redis 8.4 or greater)

        ``ifdeq`` set the value at key ``name`` to ``value`` only if the current
            value XXH3 hex digest exactly matches the argument.
            If key doesn’t exist - it won’t be created.
            (Requires Redis 8.4 or greater)

        ``ifdne`` set the value at key ``name`` to ``value`` only if the current
            value XXH3 hex digest does not exactly match the argument.
            If key doesn’t exist - it will be created.
            (Requires Redis 8.4 or greater)

        For more information, see https://redis.io/commands/set
        """

        if not at_most_one_value_set(
            (
                ex is not None,
                px is not None,
                exat is not None,
                pxat is not None,
                keepttl,
            )
        ):
            raise DataError(
                "``ex``, ``px``, ``exat``, ``pxat``, "
                "and ``keepttl`` are mutually exclusive."
            )

        # Enforce mutual exclusivity among all conditional switches.
        if not at_most_one_value_set(
            (
                nx,
                xx,
                ifeq is not None,
                ifne is not None,
                ifdeq is not None,
                ifdne is not None,
            )
        ):
            raise DataError(
                "``nx``, ``xx``, ``ifeq``, ``ifne``, ``ifdeq``, ``ifdne`` are mutually exclusive."
            )

        pieces: list[EncodableT] = [name, value]
        options = {}

        # Conditional modifier (exactly one at most)
        if nx:
            pieces.append("NX")
        elif xx:
            pieces.append("XX")
        elif ifeq is not None:
            pieces.extend(("IFEQ", ifeq))
        elif ifne is not None:
            pieces.extend(("IFNE", ifne))
        elif ifdeq is not None:
            pieces.extend(("IFDEQ", ifdeq))
        elif ifdne is not None:
            pieces.extend(("IFDNE", ifdne))

        if get:
            pieces.append("GET")
            options["get"] = True

        pieces.extend(extract_expire_flags(ex, px, exat, pxat))

        if keepttl:
            pieces.append("KEEPTTL")

        return self.execute_command("SET", *pieces, **options)

    def __setitem__(self, name: KeyT, value: EncodableT):
        self.set(name, value)

    @overload
    def setbit(
        self: SyncClientProtocol, name: KeyT, offset: int, value: int
    ) -> int: ...

    @overload
    def setbit(
        self: AsyncClientProtocol, name: KeyT, offset: int, value: int
    ) -> Awaitable[int]: ...

    def setbit(self, name: KeyT, offset: int, value: int) -> int | Awaitable[int]:
        """
        Flag the ``offset`` in ``name`` as ``value``. Returns an integer
        indicating the previous value of ``offset``.

        For more information, see https://redis.io/commands/setbit
        """
        value = value and 1 or 0
        return self.execute_command("SETBIT", name, offset, value)

    @overload
    def setex(
        self: SyncClientProtocol, name: KeyT, time: ExpiryT, value: EncodableT
    ) -> bool: ...

    @overload
    def setex(
        self: AsyncClientProtocol, name: KeyT, time: ExpiryT, value: EncodableT
    ) -> Awaitable[bool]: ...

    def setex(
        self, name: KeyT, time: ExpiryT, value: EncodableT
    ) -> bool | Awaitable[bool]:
        """
        Set the value of key ``name`` to ``value`` that expires in ``time``
        seconds. ``time`` can be represented by an integer or a Python
        timedelta object.

        Redis has deprecated SETEX since version 2.6.12. The command remains
        available for compatibility. Please use SET with the EX parameter in
        new code.

        For more information, see https://redis.io/commands/setex
        """
        if isinstance(time, datetime.timedelta):
            time = int(time.total_seconds())
        return self.execute_command("SETEX", name, time, value)

    @overload
    def setnx(self: SyncClientProtocol, name: KeyT, value: EncodableT) -> bool: ...

    @overload
    def setnx(
        self: AsyncClientProtocol, name: KeyT, value: EncodableT
    ) -> Awaitable[bool]: ...

    def setnx(self, name: KeyT, value: EncodableT) -> bool | Awaitable[bool]:
        """
        Set the value of key ``name`` to ``value`` if key doesn't exist

        For more information, see https://redis.io/commands/setnx
        """
        return self.execute_command("SETNX", name, value)

    @overload
    def setrange(
        self: SyncClientProtocol, name: KeyT, offset: int, value: EncodableT
    ) -> int: ...

    @overload
    def setrange(
        self: AsyncClientProtocol, name: KeyT, offset: int, value: EncodableT
    ) -> Awaitable[int]: ...

    def setrange(
        self, name: KeyT, offset: int, value: EncodableT
    ) -> int | Awaitable[int]:
        """
        Overwrite bytes in the value of ``name`` starting at ``offset`` with
        ``value``. If ``offset`` plus the length of ``value`` exceeds the
        length of the original value, the new value will be larger than before.
        If ``offset`` exceeds the length of the original value, null bytes
        will be used to pad between the end of the previous value and the start
        of what's being injected.

        Returns the length of the new string.

        For more information, see https://redis.io/commands/setrange
        """
        return self.execute_command("SETRANGE", name, offset, value)

    @overload
    def stralgo(
        self: SyncClientProtocol,
        algo: Literal["LCS"],
        value1: KeyT,
        value2: KeyT,
        specific_argument: Literal["strings"] | Literal["keys"] = "strings",
        len: bool = False,
        idx: bool = False,
        minmatchlen: int | None = None,
        withmatchlen: bool = False,
        **kwargs,
    ) -> StralgoResponse: ...

    @overload
    def stralgo(
        self: AsyncClientProtocol,
        algo: Literal["LCS"],
        value1: KeyT,
        value2: KeyT,
        specific_argument: Literal["strings"] | Literal["keys"] = "strings",
        len: bool = False,
        idx: bool = False,
        minmatchlen: int | None = None,
        withmatchlen: bool = False,
        **kwargs,
    ) -> Awaitable[StralgoResponse]: ...

    def stralgo(
        self,
        algo: Literal["LCS"],
        value1: KeyT,
        value2: KeyT,
        specific_argument: Literal["strings"] | Literal["keys"] = "strings",
        len: bool = False,
        idx: bool = False,
        minmatchlen: int | None = None,
        withmatchlen: bool = False,
        **kwargs,
    ) -> StralgoResponse | Awaitable[StralgoResponse]:
        """
        Implements complex algorithms that operate on strings.
        Right now the only algorithm implemented is the LCS algorithm
        (longest common substring). However new algorithms could be
        implemented in the future.

        ``algo`` Right now must be LCS
        ``value1`` and ``value2`` Can be two strings or two keys
        ``specific_argument`` Specifying if the arguments to the algorithm
        will be keys or strings. strings is the default.
        ``len`` Returns just the len of the match.
        ``idx`` Returns the match positions in each string.
        ``minmatchlen`` Restrict the list of matches to the ones of a given
        minimal length. Can be provided only when ``idx`` set to True.
        ``withmatchlen`` Returns the matches with the len of the match.
        Can be provided only when ``idx`` set to True.

        For more information, see https://redis.io/commands/stralgo
        """
        # check validity
        supported_algo = ["LCS"]
        if algo not in supported_algo:
            supported_algos_str = ", ".join(supported_algo)
            raise DataError(f"The supported algorithms are: {supported_algos_str}")
        if specific_argument not in ["keys", "strings"]:
            raise DataError("specific_argument can be only keys or strings")
        if len and idx:
            raise DataError("len and idx cannot be provided together.")

        pieces: list[EncodableT] = [algo, specific_argument.upper(), value1, value2]
        if len:
            pieces.append(b"LEN")
        if idx:
            pieces.append(b"IDX")
        try:
            int(minmatchlen)
            pieces.extend([b"MINMATCHLEN", minmatchlen])
        except TypeError:
            pass
        if withmatchlen:
            pieces.append(b"WITHMATCHLEN")

        return self.execute_command(
            "STRALGO",
            *pieces,
            len=len,
            idx=idx,
            minmatchlen=minmatchlen,
            withmatchlen=withmatchlen,
            **kwargs,
        )

    @overload
    def strlen(self: SyncClientProtocol, name: KeyT) -> int: ...

    @overload
    def strlen(self: AsyncClientProtocol, name: KeyT) -> Awaitable[int]: ...

    def strlen(self, name: KeyT) -> int | Awaitable[int]:
        """
        Return the number of bytes stored in the value of ``name``

        For more information, see https://redis.io/commands/strlen
        """
        return self.execute_command("STRLEN", name, keys=[name])

    @overload
    def substr(
        self: SyncClientProtocol, name: KeyT, start: int, end: int = -1
    ) -> bytes | str: ...

    @overload
    def substr(
        self: AsyncClientProtocol, name: KeyT, start: int, end: int = -1
    ) -> Awaitable[bytes | str]: ...

    def substr(self, name: KeyT, start: int, end: int = -1) -> (
        bytes | str
    ) | Awaitable[bytes | str]:
        """
        Return a substring of the string at key ``name``. ``start`` and ``end``
        are 0-based integers specifying the portion of the string to return.
        """
        return self.execute_command("SUBSTR", name, start, end, keys=[name])

    @overload
    def touch(self: SyncClientProtocol, *args: KeyT) -> int: ...

    @overload
    def touch(self: AsyncClientProtocol, *args: KeyT) -> Awaitable[int]: ...

    def touch(self, *args: KeyT) -> int | Awaitable[int]:
        """
        Alters the last access time of a key(s) ``*args``. A key is ignored
        if it does not exist.

        For more information, see https://redis.io/commands/touch
        """
        return self.execute_command("TOUCH", *args)

    @overload
    def ttl(self: SyncClientProtocol, name: KeyT) -> int: ...

    @overload
    def ttl(self: AsyncClientProtocol, name: KeyT) -> Awaitable[int]: ...

    def ttl(self, name: KeyT) -> int | Awaitable[int]:
        """
        Returns the number of seconds until the key ``name`` will expire

        For more information, see https://redis.io/commands/ttl
        """
        return self.execute_command("TTL", name)

    @overload
    def type(self: SyncClientProtocol, name: KeyT) -> bytes | str: ...

    @overload
    def type(self: AsyncClientProtocol, name: KeyT) -> Awaitable[bytes | str]: ...

    def type(self, name: KeyT) -> (bytes | str) | Awaitable[bytes | str]:
        """
        Returns the type of key ``name``

        For more information, see https://redis.io/commands/type
        """
        return self.execute_command("TYPE", name, keys=[name])

    def watch(self, *names: KeyT) -> None:
        """
        Watches the values at keys ``names``, or None if the key doesn't exist

        For more information, see https://redis.io/commands/watch
        """
        warnings.warn(DeprecationWarning("Call WATCH from a Pipeline object"))

    def unwatch(self) -> None:
        """
        Unwatches all previously watched keys for a transaction

        For more information, see https://redis.io/commands/unwatch
        """
        warnings.warn(DeprecationWarning("Call UNWATCH from a Pipeline object"))

    @overload
    def unlink(self: SyncClientProtocol, *names: KeyT) -> int: ...

    @overload
    def unlink(self: AsyncClientProtocol, *names: KeyT) -> Awaitable[int]: ...

    def unlink(self, *names: KeyT) -> int | Awaitable[int]:
        """
        Unlink one or more keys specified by ``names``

        For more information, see https://redis.io/commands/unlink
        """
        return self.execute_command("UNLINK", *names)

    @overload
    def lcs(
        self: SyncClientProtocol,
        key1: str,
        key2: str,
        len: bool | None = False,
        idx: bool | None = False,
        minmatchlen: int | None = 0,
        withmatchlen: bool | None = False,
    ) -> LCSCommandResponse: ...

    @overload
    def lcs(
        self: AsyncClientProtocol,
        key1: str,
        key2: str,
        len: bool | None = False,
        idx: bool | None = False,
        minmatchlen: int | None = 0,
        withmatchlen: bool | None = False,
    ) -> Awaitable[LCSCommandResponse]: ...

    def lcs(
        self,
        key1: str,
        key2: str,
        len: bool | None = False,
        idx: bool | None = False,
        minmatchlen: int | None = 0,
        withmatchlen: bool | None = False,
    ) -> LCSCommandResponse | Awaitable[LCSCommandResponse]:
        """
        Find the longest common subsequence between ``key1`` and ``key2``.
        If ``len`` is true the length of the match will be returned.
        If ``idx`` is true the match position in each strings will be returned.
        ``minmatchlen`` restrict the list of matches to the ones of
        the given ``minmatchlen``.
        If ``withmatchlen`` the length of the match also will be returned.
        For more information, see https://redis.io/commands/lcs
        """
        pieces: list[str | int] = [key1, key2]
        if len:
            pieces.append("LEN")
        if idx:
            pieces.append("IDX")
        if minmatchlen is not None and minmatchlen != 0:
            pieces.extend(["MINMATCHLEN", minmatchlen])
        if withmatchlen:
            pieces.append("WITHMATCHLEN")
        return self.execute_command("LCS", *pieces, keys=[key1, key2])


class AsyncBasicKeyCommands(BasicKeyCommands):
    def __delitem__(self, name: KeyT):
        raise TypeError("Async Redis client does not support class deletion")

    def __contains__(self, name: KeyT):
        raise TypeError("Async Redis client does not support class inclusion")

    def __getitem__(self, name: KeyT):
        raise TypeError("Async Redis client does not support class retrieval")

    def __setitem__(self, name: KeyT, value: EncodableT):
        raise TypeError("Async Redis client does not support class assignment")

    async def watch(self, *names: KeyT) -> None:
        return super().watch(*names)

    async def unwatch(self) -> None:
        return super().unwatch()


class ListCommands(CommandsProtocol):
    """
    Redis commands for List data type.
    see: https://redis.io/topics/data-types#lists
    """

    @overload
    def blpop(
        self: SyncClientProtocol, keys: KeysT, timeout: Number | None = 0
    ) -> BlockingListPopResponse: ...

    @overload
    def blpop(
        self: AsyncClientProtocol, keys: KeysT, timeout: Number | None = 0
    ) -> Awaitable[BlockingListPopResponse]: ...

    def blpop(
        self, keys: KeysT, timeout: Number | None = 0
    ) -> BlockingListPopResponse | Awaitable[BlockingListPopResponse]:
        """
        LPOP a value off of the first non-empty list
        named in ``keys``.

        If none of the lists in ``keys`` has a value to LPOP, then block
        for ``timeout`` seconds, or until a value gets pushed on to one
        of the lists.

        If timeout is 0, then block indefinitely.

        For more information, see https://redis.io/commands/blpop
        """
        if timeout is None:
            timeout = 0
        keys = list_or_args(keys, None)
        keys.append(timeout)
        return self.execute_command("BLPOP", *keys)

    @overload
    def brpop(
        self: SyncClientProtocol, keys: KeysT, timeout: Number | None = 0
    ) -> BlockingListPopResponse: ...

    @overload
    def brpop(
        self: AsyncClientProtocol, keys: KeysT, timeout: Number | None = 0
    ) -> Awaitable[BlockingListPopResponse]: ...

    def brpop(
        self, keys: KeysT, timeout: Number | None = 0
    ) -> BlockingListPopResponse | Awaitable[BlockingListPopResponse]:
        """
        RPOP a value off of the first non-empty list
        named in ``keys``.

        If none of the lists in ``keys`` has a value to RPOP, then block
        for ``timeout`` seconds, or until a value gets pushed on to one
        of the lists.

        If timeout is 0, then block indefinitely.

        For more information, see https://redis.io/commands/brpop
        """
        if timeout is None:
            timeout = 0
        keys = list_or_args(keys, None)
        keys.append(timeout)
        return self.execute_command("BRPOP", *keys)

    @overload
    def brpoplpush(
        self: SyncClientProtocol, src: KeyT, dst: KeyT, timeout: Number | None = 0
    ) -> bytes | str | None: ...

    @overload
    def brpoplpush(
        self: AsyncClientProtocol, src: KeyT, dst: KeyT, timeout: Number | None = 0
    ) -> Awaitable[bytes | str | None]: ...

    def brpoplpush(self, src: KeyT, dst: KeyT, timeout: Number | None = 0) -> (
        bytes | str | None
    ) | Awaitable[bytes | str | None]:
        """
        Pop a value off the tail of ``src``, push it on the head of ``dst``
        and then return it.

        This command blocks until a value is in ``src`` or until ``timeout``
        seconds elapse, whichever is first. A ``timeout`` value of 0 blocks
        forever.

        For more information, see https://redis.io/commands/brpoplpush
        """
        if timeout is None:
            timeout = 0
        return self.execute_command("BRPOPLPUSH", src, dst, timeout)

    @overload
    def blmpop(
        self: SyncClientProtocol,
        timeout: float,
        numkeys: int,
        *args: str,
        direction: str,
        count: int | None = 1,
    ) -> ListMultiPopResponse: ...

    @overload
    def blmpop(
        self: AsyncClientProtocol,
        timeout: float,
        numkeys: int,
        *args: str,
        direction: str,
        count: int | None = 1,
    ) -> Awaitable[ListMultiPopResponse]: ...

    def blmpop(
        self,
        timeout: float,
        numkeys: int,
        *args: str,
        direction: str,
        count: int | None = 1,
    ) -> ListMultiPopResponse | Awaitable[ListMultiPopResponse]:
        """
        Pop ``count`` values (default 1) from first non-empty in the list
        of provided key names.

        When all lists are empty this command blocks the connection until another
        client pushes to it or until the timeout, timeout of 0 blocks indefinitely

        For more information, see https://redis.io/commands/blmpop
        """
        cmd_args = [timeout, numkeys, *args, direction]
        if count is not None:
            cmd_args.extend(["COUNT", count])

        return self.execute_command("BLMPOP", *cmd_args)

    @overload
    def lmpop(
        self: SyncClientProtocol,
        num_keys: int,
        *args: str,
        direction: str,
        count: int | None = 1,
    ) -> ListMultiPopResponse: ...

    @overload
    def lmpop(
        self: AsyncClientProtocol,
        num_keys: int,
        *args: str,
        direction: str,
        count: int | None = 1,
    ) -> Awaitable[ListMultiPopResponse]: ...

    def lmpop(
        self,
        num_keys: int,
        *args: str,
        direction: str,
        count: int | None = 1,
    ) -> ListMultiPopResponse | Awaitable[ListMultiPopResponse]:
        """
        Pop ``count`` values (default 1) first non-empty list key from the list
        of args provided key names.

        For more information, see https://redis.io/commands/lmpop
        """
        cmd_args = [num_keys] + list(args) + [direction]
        if count is not None and count != 1:
            cmd_args.extend(["COUNT", count])

        return self.execute_command("LMPOP", *cmd_args)

    @overload
    def lindex(
        self: SyncClientProtocol, name: KeyT, index: int
    ) -> bytes | str | None: ...

    @overload
    def lindex(
        self: AsyncClientProtocol, name: KeyT, index: int
    ) -> Awaitable[bytes | str | None]: ...

    def lindex(self, name: KeyT, index: int) -> (bytes | str | None) | Awaitable[
        bytes | str | None
    ]:
        """
        Return the item from list ``name`` at position ``index``

        Negative indexes are supported and will return an item at the
        end of the list

        For more information, see https://redis.io/commands/lindex
        """
        return self.execute_command("LINDEX", name, index, keys=[name])

    @overload
    def linsert(
        self: SyncClientProtocol, name: KeyT, where: str, refvalue: str, value: str
    ) -> int: ...

    @overload
    def linsert(
        self: AsyncClientProtocol, name: KeyT, where: str, refvalue: str, value: str
    ) -> Awaitable[int]: ...

    def linsert(
        self, name: KeyT, where: str, refvalue: str, value: str
    ) -> int | Awaitable[int]:
        """
        Insert ``value`` in list ``name`` either immediately before or after
        [``where``] ``refvalue``

        Returns the new length of the list on success or -1 if ``refvalue``
        is not in the list.

        For more information, see https://redis.io/commands/linsert
        """
        return self.execute_command("LINSERT", name, where, refvalue, value)

    @overload
    def llen(self: SyncClientProtocol, name: KeyT) -> int: ...

    @overload
    def llen(self: AsyncClientProtocol, name: KeyT) -> Awaitable[int]: ...

    def llen(self, name: KeyT) -> int | Awaitable[int]:
        """
        Return the length of the list ``name``

        For more information, see https://redis.io/commands/llen
        """
        return self.execute_command("LLEN", name, keys=[name])

    @overload
    def lpop(
        self: SyncClientProtocol,
        name: KeyT,
        count: int | None = None,
    ) -> bytes | str | list[bytes | str] | None: ...

    @overload
    def lpop(
        self: AsyncClientProtocol,
        name: KeyT,
        count: int | None = None,
    ) -> Awaitable[bytes | str | list[bytes | str] | None]: ...

    def lpop(
        self,
        name: KeyT,
        count: int | None = None,
    ) -> (bytes | str | list[bytes | str] | None) | Awaitable[
        bytes | str | list[bytes | str] | None
    ]:
        """
        Removes and returns the first elements of the list ``name``.

        By default, the command pops a single element from the beginning of
        the list. When provided with the optional ``count`` argument, the reply
        will consist of up to count elements, depending on the list's length.

        For more information, see https://redis.io/commands/lpop
        """
        if count is not None:
            return self.execute_command("LPOP", name, count)
        else:
            return self.execute_command("LPOP", name)

    @overload
    def lpush(self: SyncClientProtocol, name: KeyT, *values: FieldT) -> int: ...

    @overload
    def lpush(
        self: AsyncClientProtocol, name: KeyT, *values: FieldT
    ) -> Awaitable[int]: ...

    def lpush(self, name: KeyT, *values: FieldT) -> int | Awaitable[int]:
        """
        Push ``values`` onto the head of the list ``name``

        For more information, see https://redis.io/commands/lpush
        """
        return self.execute_command("LPUSH", name, *values)

    @overload
    def lpushx(self: SyncClientProtocol, name: KeyT, *values: FieldT) -> int: ...

    @overload
    def lpushx(
        self: AsyncClientProtocol, name: KeyT, *values: FieldT
    ) -> Awaitable[int]: ...

    def lpushx(self, name: KeyT, *values: FieldT) -> int | Awaitable[int]:
        """
        Push ``value`` onto the head of the list ``name`` if ``name`` exists

        For more information, see https://redis.io/commands/lpushx
        """
        return self.execute_command("LPUSHX", name, *values)

    @overload
    def lrange(
        self: SyncClientProtocol, name: KeyT, start: int, end: int
    ) -> list[bytes | str]: ...

    @overload
    def lrange(
        self: AsyncClientProtocol, name: KeyT, start: int, end: int
    ) -> Awaitable[list[bytes | str]]: ...

    def lrange(
        self, name: KeyT, start: int, end: int
    ) -> list[bytes | str] | Awaitable[list[bytes | str]]:
        """
        Return a slice of the list ``name`` between
        position ``start`` and ``end``

        ``start`` and ``end`` can be negative numbers just like
        Python slicing notation

        For more information, see https://redis.io/commands/lrange
        """
        return self.execute_command("LRANGE", name, start, end, keys=[name])

    @overload
    def lrem(self: SyncClientProtocol, name: KeyT, count: int, value: str) -> int: ...

    @overload
    def lrem(
        self: AsyncClientProtocol, name: KeyT, count: int, value: str
    ) -> Awaitable[int]: ...

    def lrem(self, name: KeyT, count: int, value: str) -> int | Awaitable[int]:
        """
        Remove the first ``count`` occurrences of elements equal to ``value``
        from the list stored at ``name``.

        The count argument influences the operation in the following ways:
            count > 0: Remove elements equal to value moving from head to tail.
            count < 0: Remove elements equal to value moving from tail to head.
            count = 0: Remove all elements equal to value.

            For more information, see https://redis.io/commands/lrem
        """
        return self.execute_command("LREM", name, count, value)

    @overload
    def lset(self: SyncClientProtocol, name: KeyT, index: int, value: str) -> bool: ...

    @overload
    def lset(
        self: AsyncClientProtocol, name: KeyT, index: int, value: str
    ) -> Awaitable[bool]: ...

    def lset(self, name: KeyT, index: int, value: str) -> bool | Awaitable[bool]:
        """
        Set element at ``index`` of list ``name`` to ``value``

        For more information, see https://redis.io/commands/lset
        """
        return self.execute_command("LSET", name, index, value)

    @overload
    def ltrim(self: SyncClientProtocol, name: KeyT, start: int, end: int) -> bool: ...

    @overload
    def ltrim(
        self: AsyncClientProtocol, name: KeyT, start: int, end: int
    ) -> Awaitable[bool]: ...

    def ltrim(self, name: KeyT, start: int, end: int) -> bool | Awaitable[bool]:
        """
        Trim the list ``name``, removing all values not within the slice
        between ``start`` and ``end``

        ``start`` and ``end`` can be negative numbers just like
        Python slicing notation

        For more information, see https://redis.io/commands/ltrim
        """
        return self.execute_command("LTRIM", name, start, end)

    @overload
    def rpop(
        self: SyncClientProtocol,
        name: KeyT,
        count: int | None = None,
    ) -> bytes | str | list[bytes | str] | None: ...

    @overload
    def rpop(
        self: AsyncClientProtocol,
        name: KeyT,
        count: int | None = None,
    ) -> Awaitable[bytes | str | list[bytes | str] | None]: ...

    def rpop(
        self,
        name: KeyT,
        count: int | None = None,
    ) -> (bytes | str | list[bytes | str] | None) | Awaitable[
        bytes | str | list[bytes | str] | None
    ]:
        """
        Removes and returns the last elements of the list ``name``.

        By default, the command pops a single element from the end of the list.
        When provided with the optional ``count`` argument, the reply will
        consist of up to count elements, depending on the list's length.

        For more information, see https://redis.io/commands/rpop
        """
        if count is not None:
            return self.execute_command("RPOP", name, count)
        else:
            return self.execute_command("RPOP", name)

    @overload
    def rpoplpush(
        self: SyncClientProtocol, src: KeyT, dst: KeyT
    ) -> bytes | str | None: ...

    @overload
    def rpoplpush(
        self: AsyncClientProtocol, src: KeyT, dst: KeyT
    ) -> Awaitable[bytes | str | None]: ...

    def rpoplpush(self, src: KeyT, dst: KeyT) -> (bytes | str | None) | Awaitable[
        bytes | str | None
    ]:
        """
        RPOP a value off of the ``src`` list and atomically LPUSH it
        on to the ``dst`` list.  Returns the value.

        For more information, see https://redis.io/commands/rpoplpush
        """
        return self.execute_command("RPOPLPUSH", src, dst)

    @overload
    def rpush(self: SyncClientProtocol, name: KeyT, *values: FieldT) -> int: ...

    @overload
    def rpush(
        self: AsyncClientProtocol, name: KeyT, *values: FieldT
    ) -> Awaitable[int]: ...

    def rpush(self, name: KeyT, *values: FieldT) -> int | Awaitable[int]:
        """
        Push ``values`` onto the tail of the list ``name``

        For more information, see https://redis.io/commands/rpush
        """
        return self.execute_command("RPUSH", name, *values)

    @overload
    def rpushx(self: SyncClientProtocol, name: KeyT, *values: str) -> int: ...

    @overload
    def rpushx(
        self: AsyncClientProtocol, name: KeyT, *values: str
    ) -> Awaitable[int]: ...

    def rpushx(self, name: KeyT, *values: str) -> int | Awaitable[int]:
        """
        Push ``value`` onto the tail of the list ``name`` if ``name`` exists

        For more information, see https://redis.io/commands/rpushx
        """
        return self.execute_command("RPUSHX", name, *values)

    @overload
    def lpos(
        self: SyncClientProtocol,
        name: KeyT,
        value: str,
        rank: int | None = None,
        count: int | None = None,
        maxlen: int | None = None,
    ) -> int | list[int] | None: ...

    @overload
    def lpos(
        self: AsyncClientProtocol,
        name: KeyT,
        value: str,
        rank: int | None = None,
        count: int | None = None,
        maxlen: int | None = None,
    ) -> Awaitable[int | list[int] | None]: ...

    def lpos(
        self,
        name: KeyT,
        value: str,
        rank: int | None = None,
        count: int | None = None,
        maxlen: int | None = None,
    ) -> (int | list[int] | None) | Awaitable[int | list[int] | None]:
        """
        Get position of ``value`` within the list ``name``

         If specified, ``rank`` indicates the "rank" of the first element to
         return in case there are multiple copies of ``value`` in the list.
         By default, LPOS returns the position of the first occurrence of
         ``value`` in the list. When ``rank`` 2, LPOS returns the position of
         the second ``value`` in the list. If ``rank`` is negative, LPOS
         searches the list in reverse. For example, -1 would return the
         position of the last occurrence of ``value`` and -2 would return the
         position of the next to last occurrence of ``value``.

         If specified, ``count`` indicates that LPOS should return a list of
         up to ``count`` positions. A ``count`` of 2 would return a list of
         up to 2 positions. A ``count`` of 0 returns a list of all positions
         matching ``value``. When ``count`` is specified and but ``value``
         does not exist in the list, an empty list is returned.

         If specified, ``maxlen`` indicates the maximum number of list
         elements to scan. A ``maxlen`` of 1000 will only return the
         position(s) of items within the first 1000 entries in the list.
         A ``maxlen`` of 0 (the default) will scan the entire list.

         For more information, see https://redis.io/commands/lpos
        """
        pieces: list[EncodableT] = [name, value]
        if rank is not None:
            pieces.extend(["RANK", rank])

        if count is not None:
            pieces.extend(["COUNT", count])

        if maxlen is not None:
            pieces.extend(["MAXLEN", maxlen])

        return self.execute_command("LPOS", *pieces, keys=[name])

    @overload
    def sort(
        self: SyncClientProtocol,
        name: KeyT,
        start: int | None = None,
        num: int | None = None,
        by: str | None = None,
        get: list[str] | None = None,
        desc: bool = False,
        alpha: bool = False,
        store: str | None = None,
        groups: bool | None = False,
    ) -> SortResponse: ...

    @overload
    def sort(
        self: AsyncClientProtocol,
        name: KeyT,
        start: int | None = None,
        num: int | None = None,
        by: str | None = None,
        get: list[str] | None = None,
        desc: bool = False,
        alpha: bool = False,
        store: str | None = None,
        groups: bool | None = False,
    ) -> Awaitable[SortResponse]: ...

    def sort(
        self,
        name: KeyT,
        start: int | None = None,
        num: int | None = None,
        by: str | None = None,
        get: list[str] | None = None,
        desc: bool = False,
        alpha: bool = False,
        store: str | None = None,
        groups: bool | None = False,
    ) -> SortResponse | Awaitable[SortResponse]:
        """
        Sort and return the list, set or sorted set at ``name``.

        ``start`` and ``num`` allow for paging through the sorted data

        ``by`` allows using an external key to weight and sort the items.
            Use an "*" to indicate where in the key the item value is located

        ``get`` allows for returning items from external keys rather than the
            sorted data itself.  Use an "*" to indicate where in the key
            the item value is located

        ``desc`` allows for reversing the sort

        ``alpha`` allows for sorting lexicographically rather than numerically

        ``store`` allows for storing the result of the sort into
            the key ``store``

        ``groups`` if set to True and if ``get`` contains at least two
            elements, sort will return a list of tuples, each containing the
            values fetched from the arguments to ``get``.

        For more information, see https://redis.io/commands/sort
        """
        if (start is not None and num is None) or (num is not None and start is None):
            raise DataError("``start`` and ``num`` must both be specified")

        pieces: list[EncodableT] = [name]
        if by is not None:
            pieces.extend([b"BY", by])
        if start is not None and num is not None:
            pieces.extend([b"LIMIT", start, num])
        if get is not None:
            # If get is a string assume we want to get a single value.
            # Otherwise assume it's an iterable and we want to get multiple
            # values. We can't just iterate blindly because strings are
            # iterable.
            if isinstance(get, (bytes, str)):
                pieces.extend([b"GET", get])
            else:
                for g in get:
                    pieces.extend([b"GET", g])
        if desc:
            pieces.append(b"DESC")
        if alpha:
            pieces.append(b"ALPHA")
        if store is not None:
            pieces.extend([b"STORE", store])
        if groups:
            if not get or isinstance(get, (bytes, str)) or len(get) < 2:
                raise DataError(
                    'when using "groups" the "get" argument '
                    "must be specified and contain at least "
                    "two keys"
                )

        options = {"groups": len(get) if groups else None}
        options["keys"] = [name]
        return self.execute_command("SORT", *pieces, **options)

    @overload
    def sort_ro(
        self: SyncClientProtocol,
        key: str,
        start: int | None = None,
        num: int | None = None,
        by: str | None = None,
        get: list[str] | None = None,
        desc: bool = False,
        alpha: bool = False,
    ) -> list[bytes | str]: ...

    @overload
    def sort_ro(
        self: AsyncClientProtocol,
        key: str,
        start: int | None = None,
        num: int | None = None,
        by: str | None = None,
        get: list[str] | None = None,
        desc: bool = False,
        alpha: bool = False,
    ) -> Awaitable[list[bytes | str]]: ...

    def sort_ro(
        self,
        key: str,
        start: int | None = None,
        num: int | None = None,
        by: str | None = None,
        get: list[str] | None = None,
        desc: bool = False,
        alpha: bool = False,
    ) -> list[bytes | str] | Awaitable[list[bytes | str]]:
        """
        Returns the elements contained in the list, set or sorted set at key.
        (read-only variant of the SORT command)

        ``start`` and ``num`` allow for paging through the sorted data

        ``by`` allows using an external key to weight and sort the items.
            Use an "*" to indicate where in the key the item value is located

        ``get`` allows for returning items from external keys rather than the
            sorted data itself.  Use an "*" to indicate where in the key
            the item value is located

        ``desc`` allows for reversing the sort

        ``alpha`` allows for sorting lexicographically rather than numerically

        For more information, see https://redis.io/commands/sort_ro
        """
        return self.sort(
            key, start=start, num=num, by=by, get=get, desc=desc, alpha=alpha
        )


AsyncListCommands = ListCommands


class ArrayAggregateOperations(Enum):
    SUM = "SUM"
    MIN = "MIN"
    MAX = "MAX"
    AND = "AND"
    OR = "OR"
    XOR = "XOR"
    MATCH = "MATCH"
    USED = "USED"


class ArrayPredicateType(Enum):
    EXACT = "EXACT"
    MATCH = "MATCH"
    GLOB = "GLOB"
    RE = "RE"


class ArrayPredicateCombinator(Enum):
    AND = "AND"
    OR = "OR"


class ArrayCommands(CommandsProtocol):
    """
    Redis commands for Array data type.
    """

    @overload
    def arset(
        self: SyncClientProtocol, name: KeyT, index: int, *values: FieldT
    ) -> int: ...

    @overload
    def arset(
        self: AsyncClientProtocol, name: KeyT, index: int, *values: FieldT
    ) -> Awaitable[int]: ...

    def arset(self, name: KeyT, index: int, *values: FieldT) -> int | Awaitable[int]:
        """
        Set one or more contiguous ``values`` in the array stored at ``name``
        starting at ``index``. When multiple values are provided, they are
        stored at consecutive indices beginning at ``index``.

        Returns the number of new slots that were set (previously empty).

        For more information, see https://redis.io/commands/arset
        """
        return self.execute_command("ARSET", name, index, *values)

    @overload
    def armset(
        self: SyncClientProtocol, name: KeyT, mapping: Mapping[int, FieldT]
    ) -> int: ...

    @overload
    def armset(
        self: AsyncClientProtocol, name: KeyT, mapping: Mapping[int, FieldT]
    ) -> Awaitable[int]: ...

    def armset(self, name: KeyT, mapping: Mapping[int, FieldT]) -> int | Awaitable[int]:
        """
        Set multiple index/value pairs in the array stored at ``name``.
        ``mapping`` is a dictionary of zero-based integer indices to values.
        Pairs may be non-contiguous and in any order.

        Returns the number of new slots that were set (previously empty).

        For more information, see https://redis.io/commands/armset
        """
        items: list = []
        for pair in mapping.items():
            items.extend(pair)
        return self.execute_command("ARMSET", name, *items)

    @overload
    def arget(
        self: SyncClientProtocol, name: KeyT, index: int
    ) -> bytes | str | None: ...

    @overload
    def arget(
        self: AsyncClientProtocol, name: KeyT, index: int
    ) -> Awaitable[bytes | str | None]: ...

    def arget(self, name: KeyT, index: int) -> (bytes | str | None) | Awaitable[
        bytes | str | None
    ]:
        """
        Return the value at ``index`` in the array stored at ``name``.

        Returns ``None`` if ``name`` does not exist or no value is set at
        ``index``.

        For more information, see https://redis.io/commands/arget
        """
        return self.execute_command("ARGET", name, index)

    @overload
    def armget(
        self: SyncClientProtocol, name: KeyT, *indices: int
    ) -> list[bytes | str | None]: ...

    @overload
    def armget(
        self: AsyncClientProtocol, name: KeyT, *indices: int
    ) -> Awaitable[list[bytes | str | None]]: ...

    def armget(
        self, name: KeyT, *indices: int
    ) -> list[bytes | str | None] | Awaitable[list[bytes | str | None]]:
        """
        Return the values at the specified ``indices`` in the array stored at
        ``name``. The reply preserves the order of the requested indices and
        returns ``None`` for any index that is not set or when ``name`` does
        not exist.

        For more information, see https://redis.io/commands/armget
        """
        return self.execute_command("ARMGET", name, *indices)

    @overload
    def argetrange(
        self: SyncClientProtocol, name: KeyT, start: int, end: int
    ) -> list[bytes | str | None]: ...

    @overload
    def argetrange(
        self: AsyncClientProtocol, name: KeyT, start: int, end: int
    ) -> Awaitable[list[bytes | str | None]]: ...

    def argetrange(
        self, name: KeyT, start: int, end: int
    ) -> list[bytes | str | None] | Awaitable[list[bytes | str | None]]:
        """
        Return the values in the inclusive index range [``start``, ``end``]
        in the array stored at ``name``. If ``start`` is greater than ``end``,
        elements are returned in reverse index order.

        For more information, see https://redis.io/commands/argetrange
        """
        return self.execute_command("ARGETRANGE", name, start, end)

    @overload
    def arscan(
        self: SyncClientProtocol,
        name: KeyT,
        start: int,
        end: int,
        limit: int | None = None,
    ) -> list[list[int | bytes | str]]: ...

    @overload
    def arscan(
        self: AsyncClientProtocol,
        name: KeyT,
        start: int,
        end: int,
        limit: int | None = None,
    ) -> Awaitable[list[list[int | bytes | str]]]: ...

    def arscan(
        self,
        name: KeyT,
        start: int,
        end: int,
        limit: int | None = None,
    ) -> list[list[int | bytes | str]] | Awaitable[list[list[int | bytes | str]]]:
        """
        Iterate populated elements of the array stored at ``name`` in the
        inclusive range [``start``, ``end``] and return a list of
        ``[index, value]`` pairs in traversal order:
        ``[[idx1, val1], [idx2, val2], ...]``. Empty slots are skipped. If
        ``start`` is greater than ``end``, the iteration is reversed.

        ``limit`` caps the number of populated elements returned. When
        omitted, all populated elements in range are returned.

        For more information, see https://redis.io/commands/arscan
        """
        pieces: list = [name, start, end]
        if limit is not None:
            pieces.extend([b"LIMIT", limit])
        return self.execute_command("ARSCAN", *pieces)

    @overload
    def argrep(
        self: SyncClientProtocol,
        name: KeyT,
        start: int,
        end: int,
        predicates: list[tuple[ArrayPredicateType, EncodableT]],
        combinator: ArrayPredicateCombinator | None = None,
        limit: int | None = None,
        withvalues: bool = False,
        nocase: bool = False,
    ) -> list[int] | list[list[int | bytes | str]]: ...

    @overload
    def argrep(
        self: AsyncClientProtocol,
        name: KeyT,
        start: int,
        end: int,
        predicates: list[tuple[ArrayPredicateType, EncodableT]],
        combinator: ArrayPredicateCombinator | None = None,
        limit: int | None = None,
        withvalues: bool = False,
        nocase: bool = False,
    ) -> Awaitable[list[int] | list[list[int | bytes | str]]]: ...

    def argrep(
        self,
        name: KeyT,
        start: int,
        end: int,
        predicates: list[tuple[ArrayPredicateType, EncodableT]],
        combinator: ArrayPredicateCombinator | None = None,
        limit: int | None = None,
        withvalues: bool = False,
        nocase: bool = False,
    ) -> (
        list[int]
        | list[list[int | bytes | str]]
        | Awaitable[list[int] | list[list[int | bytes | str]]]
    ):
        """
        Search elements of the array stored at ``name`` within the inclusive
        index range [``start``, ``end``] using one or more textual
        ``predicates``. Each predicate is a ``(type, value)`` tuple where
        ``type`` is a member of ``ArrayPredicateType``
        (``EXACT``, ``MATCH``, ``GLOB``, ``RE``).

        Multiple predicates are combined via ``combinator``
        (``ArrayPredicateCombinator.AND`` or ``OR``); the server defaults to
        OR when omitted. ``limit`` caps the number of matches.
        ``withvalues=True`` returns a list of ``[index, value]`` pairs
        (``[[idx, val], ...]``) instead of a flat list of indices.
        ``nocase=True`` makes all comparisons case-insensitive.

        Without ``withvalues``, returns a flat list of matching indices in
        traversal order. With ``withvalues``, returns a list of
        ``[index, value]`` pairs in traversal order. Empty slots are
        skipped.

        For more information, see https://redis.io/commands/argrep
        """
        pieces: list = [name, start, end]
        for predicate_type, value in predicates:
            pieces.extend([predicate_type.value, value])
        if combinator is not None:
            pieces.append(combinator.value)
        if limit is not None:
            pieces.extend([b"LIMIT", limit])
        if withvalues:
            pieces.append(b"WITHVALUES")
        if nocase:
            pieces.append(b"NOCASE")
        return self.execute_command("ARGREP", *pieces)

    @overload
    def ardel(self: SyncClientProtocol, name: KeyT, *indices: int) -> int: ...

    @overload
    def ardel(
        self: AsyncClientProtocol, name: KeyT, *indices: int
    ) -> Awaitable[int]: ...

    def ardel(self, name: KeyT, *indices: int) -> int | Awaitable[int]:
        """
        Delete elements at the specified ``indices`` in the array stored at
        ``name``. Deleting an index that does not exist counts as zero
        elements deleted and does not modify the array.

        Returns the number of elements deleted.

        For more information, see https://redis.io/commands/ardel
        """
        return self.execute_command("ARDEL", name, *indices)

    @overload
    def ardelrange(
        self: SyncClientProtocol, name: KeyT, *ranges: tuple[int, int]
    ) -> int: ...

    @overload
    def ardelrange(
        self: AsyncClientProtocol, name: KeyT, *ranges: tuple[int, int]
    ) -> Awaitable[int]: ...

    def ardelrange(self, name: KeyT, *ranges: tuple[int, int]) -> int | Awaitable[int]:
        """
        Delete elements within one or more inclusive index ranges in the
        array stored at ``name``. Each range is a ``(start, end)`` tuple.
        If ``start`` is greater than ``end`` for a given pair, the range is
        processed in ascending order regardless. Multiple pairs may overlap;
        each element is counted at most once.

        Returns the number of elements deleted.

        For more information, see https://redis.io/commands/ardelrange
        """
        pieces: list = [name]
        for start, end in ranges:
            pieces.extend([start, end])
        return self.execute_command("ARDELRANGE", *pieces)

    @overload
    def arcount(self: SyncClientProtocol, name: KeyT) -> int: ...

    @overload
    def arcount(self: AsyncClientProtocol, name: KeyT) -> Awaitable[int]: ...

    def arcount(self, name: KeyT) -> int | Awaitable[int]:
        """
        Return the number of non-empty elements in the array stored at
        ``name``. Returns 0 if ``name`` does not exist.

        For more information, see https://redis.io/commands/arcount
        """
        return self.execute_command("ARCOUNT", name)

    @overload
    def arlen(self: SyncClientProtocol, name: KeyT) -> int: ...

    @overload
    def arlen(self: AsyncClientProtocol, name: KeyT) -> Awaitable[int]: ...

    def arlen(self, name: KeyT) -> int | Awaitable[int]:
        """
        Return the length of the array stored at ``name``, defined as the
        maximum set index plus one. Returns 0 if ``name`` does not exist.

        For more information, see https://redis.io/commands/arlen
        """
        return self.execute_command("ARLEN", name)

    @overload
    def arinfo(
        self: SyncClientProtocol, name: KeyT, full: bool = False
    ) -> dict[str, int]: ...

    @overload
    def arinfo(
        self: AsyncClientProtocol, name: KeyT, full: bool = False
    ) -> Awaitable[dict[str, int]]: ...

    def arinfo(
        self, name: KeyT, full: bool = False
    ) -> dict[str, int] | Awaitable[dict[str, int]]:
        """
        Return metadata about the array stored at ``name`` as a dictionary.
        When ``full`` is ``True``, additionally include per-slice statistics
        (dense/sparse slice counts, average sizes, fill rates), which makes
        the call O(N) instead of O(1).

        Raises ``ResponseError`` when the key does not exist.

        For more information, see https://redis.io/commands/arinfo
        """
        pieces: list = [name]
        if full:
            pieces.append(b"FULL")
        return self.execute_command("ARINFO", *pieces)

    @overload
    def arnext(self: SyncClientProtocol, name: KeyT) -> int | None: ...

    @overload
    def arnext(self: AsyncClientProtocol, name: KeyT) -> Awaitable[int | None]: ...

    def arnext(self, name: KeyT) -> (int | None) | Awaitable[int | None]:
        """
        Return the next index ``ARINSERT`` would use for the array stored at
        ``name``. Returns 0 if ``name`` does not exist or no insert happened
        yet, and ``None`` when the insertion cursor is exhausted (next insert
        would overflow).

        For more information, see https://redis.io/commands/arnext
        """
        return self.execute_command("ARNEXT", name)

    @overload
    def arseek(self: SyncClientProtocol, name: KeyT, index: int) -> int: ...

    @overload
    def arseek(self: AsyncClientProtocol, name: KeyT, index: int) -> Awaitable[int]: ...

    def arseek(self, name: KeyT, index: int) -> int | Awaitable[int]:
        """
        Set the insert cursor of the array stored at ``name`` to ``index``.
        Subsequent ``ARINSERT`` and ``ARRING`` operations begin inserting at
        this position.

        Returns 1 if the cursor was set, 0 if ``name`` does not exist.

        For more information, see https://redis.io/commands/arseek
        """
        return self.execute_command("ARSEEK", name, index)

    @overload
    def arinsert(self: SyncClientProtocol, name: KeyT, *values: FieldT) -> int: ...

    @overload
    def arinsert(
        self: AsyncClientProtocol, name: KeyT, *values: FieldT
    ) -> Awaitable[int]: ...

    def arinsert(self, name: KeyT, *values: FieldT) -> int | Awaitable[int]:
        """
        Insert one or more ``values`` at consecutive indices in the array
        stored at ``name``, beginning at the current insert cursor position.
        The cursor advances by one for each value inserted. Use ``ARNEXT``
        to inspect the current cursor position and ``ARSEEK`` to reposition
        it.

        Returns the last index where a value was inserted.

        For more information, see https://redis.io/commands/arinsert
        """
        return self.execute_command("ARINSERT", name, *values)

    @overload
    def arring(
        self: SyncClientProtocol, name: KeyT, size: int, *values: FieldT
    ) -> int: ...

    @overload
    def arring(
        self: AsyncClientProtocol, name: KeyT, size: int, *values: FieldT
    ) -> Awaitable[int]: ...

    def arring(self, name: KeyT, size: int, *values: FieldT) -> int | Awaitable[int]:
        """
        Insert one or more ``values`` into the array stored at ``name`` as a
        fixed-size ring buffer of ``size`` slots. Each value is placed at
        ``insert_idx % size``, wrapping back to index 0 once the end is
        reached and overwriting older values when full. If ``size`` is
        smaller than the current window, the array is truncated to fit.

        Returns the last index where a value was inserted.

        For more information, see https://redis.io/commands/arring
        """
        return self.execute_command("ARRING", name, size, *values)

    @overload
    def arlastitems(
        self: SyncClientProtocol, name: KeyT, count: int, rev: bool = False
    ) -> list[bytes | str | None]: ...

    @overload
    def arlastitems(
        self: AsyncClientProtocol, name: KeyT, count: int, rev: bool = False
    ) -> Awaitable[list[bytes | str | None]]: ...

    def arlastitems(
        self, name: KeyT, count: int, rev: bool = False
    ) -> list[bytes | str | None] | Awaitable[list[bytes | str | None]]:
        """
        Return up to ``count`` most recently inserted elements from the array
        stored at ``name``. If the array contains fewer elements than
        ``count``, all elements are returned.

        When ``rev`` is ``True``, elements are returned in reverse
        chronological order (most recent first) instead of the default
        oldest-first order.

        For more information, see https://redis.io/commands/arlastitems
        """
        pieces: list = [name, count]
        if rev:
            pieces.append(b"REV")
        return self.execute_command("ARLASTITEMS", *pieces)

    @overload
    def arop(
        self: SyncClientProtocol,
        name: KeyT,
        start: int,
        end: int,
        operation: ArrayAggregateOperations,
        value: EncodableT | None = None,
    ) -> bytes | str | int | None: ...

    @overload
    def arop(
        self: AsyncClientProtocol,
        name: KeyT,
        start: int,
        end: int,
        operation: ArrayAggregateOperations,
        value: EncodableT | None = None,
    ) -> Awaitable[bytes | str | int | None]: ...

    def arop(
        self,
        name: KeyT,
        start: int,
        end: int,
        operation: ArrayAggregateOperations,
        value: EncodableT | None = None,
    ) -> (bytes | str | int | None) | Awaitable[bytes | str | int | None]:
        """
        Perform an aggregate ``operation`` on elements of the array stored at
        ``name`` in the inclusive index range [``start``, ``end``]. The range
        is always scanned from the lower to the higher index regardless of
        argument order.

        ``operation`` is one of the members of ``ArrayAggregateOperations``.
        ``value`` is required when ``operation`` is
        ``ArrayAggregateOperations.MATCH`` and specifies the value to compare
        against.

        Returns a bulk string for ``SUM``, ``MIN`` and ``MAX``, an integer
        for ``AND``, ``OR``, ``XOR``, ``MATCH`` and ``USED``, or ``None``
        when no elements match the operation.

        For more information, see https://redis.io/commands/arop
        """
        pieces: list = [name, start, end, operation.value]
        if value is not None:
            pieces.append(value)
        return self.execute_command("AROP", *pieces)


AsyncArrayCommands = ArrayCommands


class ScanCommands(CommandsProtocol):
    """
    Redis SCAN commands.
    see: https://redis.io/commands/scan
    """

    @overload
    def scan(
        self: SyncClientProtocol,
        cursor: int = 0,
        match: PatternT | None = None,
        count: int | None = None,
        _type: str | None = None,
        **kwargs,
    ) -> ScanResponse: ...

    @overload
    def scan(
        self: AsyncClientProtocol,
        cursor: int = 0,
        match: PatternT | None = None,
        count: int | None = None,
        _type: str | None = None,
        **kwargs,
    ) -> Awaitable[ScanResponse]: ...

    def scan(
        self,
        cursor: int = 0,
        match: PatternT | None = None,
        count: int | None = None,
        _type: str | None = None,
        **kwargs,
    ) -> ScanResponse | Awaitable[ScanResponse]:
        """
        Incrementally return lists of key names. Also return a cursor
        indicating the scan position.

        ``match`` allows for filtering the keys by pattern

        ``count`` provides a hint to Redis about the number of keys to
            return per batch.

        ``_type`` filters the returned values by a particular Redis type.
            Stock Redis instances allow for the following types:
            HASH, LIST, SET, STREAM, STRING, ZSET
            Additionally, Redis modules can expose other types as well.

        For more information, see https://redis.io/commands/scan
        """
        pieces: list[EncodableT] = [cursor]
        if match is not None:
            pieces.extend([b"MATCH", match])
        if count is not None:
            pieces.extend([b"COUNT", count])
        if _type is not None:
            pieces.extend([b"TYPE", _type])
        return self.execute_command("SCAN", *pieces, **kwargs)

    def scan_iter(
        self,
        match: Union[PatternT, None] = None,
        count: Optional[int] = None,
        _type: Optional[str] = None,
        **kwargs,
    ) -> Iterator:
        """
        Make an iterator using the SCAN command so that the client doesn't
        need to remember the cursor position.

        ``match`` allows for filtering the keys by pattern

        ``count`` provides a hint to Redis about the number of keys to
            return per batch.

        ``_type`` filters the returned values by a particular Redis type.
            Stock Redis instances allow for the following types:
            HASH, LIST, SET, STREAM, STRING, ZSET
            Additionally, Redis modules can expose other types as well.
        """
        cursor = "0"
        while cursor != 0:
            cursor, data = self.scan(
                cursor=cursor, match=match, count=count, _type=_type, **kwargs
            )
            yield from data

    @overload
    def sscan(
        self: SyncClientProtocol,
        name: KeyT,
        cursor: int = 0,
        match: PatternT | None = None,
        count: int | None = None,
    ) -> ScanResponse: ...

    @overload
    def sscan(
        self: AsyncClientProtocol,
        name: KeyT,
        cursor: int = 0,
        match: PatternT | None = None,
        count: int | None = None,
    ) -> Awaitable[ScanResponse]: ...

    def sscan(
        self,
        name: KeyT,
        cursor: int = 0,
        match: PatternT | None = None,
        count: int | None = None,
    ) -> ScanResponse | Awaitable[ScanResponse]:
        """
        Incrementally return lists of elements in a set. Also return a cursor
        indicating the scan position.

        ``match`` allows for filtering the keys by pattern

        ``count`` allows for hint the minimum number of returns

        For more information, see https://redis.io/commands/sscan
        """
        pieces: list[EncodableT] = [name, cursor]
        if match is not None:
            pieces.extend([b"MATCH", match])
        if count is not None:
            pieces.extend([b"COUNT", count])
        return self.execute_command("SSCAN", *pieces)

    def sscan_iter(
        self,
        name: KeyT,
        match: Union[PatternT, None] = None,
        count: Optional[int] = None,
    ) -> Iterator:
        """
        Make an iterator using the SSCAN command so that the client doesn't
        need to remember the cursor position.

        ``match`` allows for filtering the keys by pattern

        ``count`` allows for hint the minimum number of returns
        """
        cursor = "0"
        while cursor != 0:
            cursor, data = self.sscan(name, cursor=cursor, match=match, count=count)
            yield from data

    @overload
    def hscan(
        self: SyncClientProtocol,
        name: KeyT,
        cursor: int = 0,
        match: PatternT | None = None,
        count: int | None = None,
        no_values: bool | None = None,
    ) -> HScanResponse: ...

    @overload
    def hscan(
        self: AsyncClientProtocol,
        name: KeyT,
        cursor: int = 0,
        match: PatternT | None = None,
        count: int | None = None,
        no_values: bool | None = None,
    ) -> Awaitable[HScanResponse]: ...

    def hscan(
        self,
        name: KeyT,
        cursor: int = 0,
        match: PatternT | None = None,
        count: int | None = None,
        no_values: bool | None = None,
    ) -> HScanResponse | Awaitable[HScanResponse]:
        """
        Incrementally return key/value slices in a hash. Also return a cursor
        indicating the scan position.

        ``match`` allows for filtering the keys by pattern

        ``count`` allows for hint the minimum number of returns

        ``no_values`` indicates to return only the keys, without values.

        For more information, see https://redis.io/commands/hscan
        """
        pieces: list[EncodableT] = [name, cursor]
        if match is not None:
            pieces.extend([b"MATCH", match])
        if count is not None:
            pieces.extend([b"COUNT", count])
        if no_values:
            pieces.extend([b"NOVALUES"])
        return self.execute_command("HSCAN", *pieces, no_values=no_values)

    def hscan_iter(
        self,
        name: KeyT,
        match: Union[PatternT, None] = None,
        count: Optional[int] = None,
        no_values: Union[bool, None] = None,
    ) -> Iterator:
        """
        Make an iterator using the HSCAN command so that the client doesn't
        need to remember the cursor position.

        ``match`` allows for filtering the keys by pattern

        ``count`` allows for hint the minimum number of returns

        ``no_values`` indicates to return only the keys, without values
        """
        cursor = "0"
        while cursor != 0:
            cursor, data = self.hscan(
                name, cursor=cursor, match=match, count=count, no_values=no_values
            )
            if no_values:
                yield from data
            else:
                yield from data.items()

    @overload
    def zscan(
        self: SyncClientProtocol,
        name: KeyT,
        cursor: int = 0,
        match: PatternT | None = None,
        count: int | None = None,
        score_cast_func: type | Callable = float,
    ) -> ZScanResponse: ...

    @overload
    def zscan(
        self: AsyncClientProtocol,
        name: KeyT,
        cursor: int = 0,
        match: PatternT | None = None,
        count: int | None = None,
        score_cast_func: type | Callable = float,
    ) -> Awaitable[ZScanResponse]: ...

    def zscan(
        self,
        name: KeyT,
        cursor: int = 0,
        match: PatternT | None = None,
        count: int | None = None,
        score_cast_func: type | Callable = float,
    ) -> ZScanResponse | Awaitable[ZScanResponse]:
        """
        Incrementally return lists of elements in a sorted set. Also return a
        cursor indicating the scan position.

        ``match`` allows for filtering the keys by pattern

        ``count`` allows for hint the minimum number of returns

        ``score_cast_func`` a callable used to cast the score return value

        For more information, see https://redis.io/commands/zscan
        """
        pieces = [name, cursor]
        if match is not None:
            pieces.extend([b"MATCH", match])
        if count is not None:
            pieces.extend([b"COUNT", count])
        options = {"score_cast_func": score_cast_func}
        return self.execute_command("ZSCAN", *pieces, **options)

    def zscan_iter(
        self,
        name: KeyT,
        match: Union[PatternT, None] = None,
        count: Optional[int] = None,
        score_cast_func: Union[type, Callable] = float,
    ) -> Iterator:
        """
        Make an iterator using the ZSCAN command so that the client doesn't
        need to remember the cursor position.

        ``match`` allows for filtering the keys by pattern

        ``count`` allows for hint the minimum number of returns

        ``score_cast_func`` a callable used to cast the score return value
        """
        cursor = "0"
        while cursor != 0:
            cursor, data = self.zscan(
                name,
                cursor=cursor,
                match=match,
                count=count,
                score_cast_func=score_cast_func,
            )
            yield from data


class AsyncScanCommands(ScanCommands):
    async def scan_iter(
        self,
        match: Union[PatternT, None] = None,
        count: Optional[int] = None,
        _type: Optional[str] = None,
        **kwargs,
    ) -> AsyncIterator:
        """
        Make an iterator using the SCAN command so that the client doesn't
        need to remember the cursor position.

        ``match`` allows for filtering the keys by pattern

        ``count`` provides a hint to Redis about the number of keys to
            return per batch.

        ``_type`` filters the returned values by a particular Redis type.
            Stock Redis instances allow for the following types:
            HASH, LIST, SET, STREAM, STRING, ZSET
            Additionally, Redis modules can expose other types as well.
        """
        cursor = "0"
        while cursor != 0:
            cursor, data = await self.scan(
                cursor=cursor, match=match, count=count, _type=_type, **kwargs
            )
            for d in data:
                yield d

    async def sscan_iter(
        self,
        name: KeyT,
        match: Union[PatternT, None] = None,
        count: Optional[int] = None,
    ) -> AsyncIterator:
        """
        Make an iterator using the SSCAN command so that the client doesn't
        need to remember the cursor position.

        ``match`` allows for filtering the keys by pattern

        ``count`` allows for hint the minimum number of returns
        """
        cursor = "0"
        while cursor != 0:
            cursor, data = await self.sscan(
                name, cursor=cursor, match=match, count=count
            )
            for d in data:
                yield d

    async def hscan_iter(
        self,
        name: KeyT,
        match: Union[PatternT, None] = None,
        count: Optional[int] = None,
        no_values: Union[bool, None] = None,
    ) -> AsyncIterator:
        """
        Make an iterator using the HSCAN command so that the client doesn't
        need to remember the cursor position.

        ``match`` allows for filtering the keys by pattern

        ``count`` allows for hint the minimum number of returns

        ``no_values`` indicates to return only the keys, without values
        """
        cursor = "0"
        while cursor != 0:
            cursor, data = await self.hscan(
                name, cursor=cursor, match=match, count=count, no_values=no_values
            )
            if no_values:
                for it in data:
                    yield it
            else:
                for it in data.items():
                    yield it

    async def zscan_iter(
        self,
        name: KeyT,
        match: Union[PatternT, None] = None,
        count: Optional[int] = None,
        score_cast_func: Union[type, Callable] = float,
    ) -> AsyncIterator:
        """
        Make an iterator using the ZSCAN command so that the client doesn't
        need to remember the cursor position.

        ``match`` allows for filtering the keys by pattern

        ``count`` allows for hint the minimum number of returns

        ``score_cast_func`` a callable used to cast the score return value
        """
        cursor = "0"
        while cursor != 0:
            cursor, data = await self.zscan(
                name,
                cursor=cursor,
                match=match,
                count=count,
                score_cast_func=score_cast_func,
            )
            for d in data:
                yield d


class SetDataPersistOptions(Enum):
    # add each provided member only if none of them
    # already exists.
    MNX = "MNX"

    # add each provided member only if all of them
    # already exist.
    MXX = "MXX"


class SetCommands(CommandsProtocol):
    """
    Redis commands for Set data type.
    see: https://redis.io/topics/data-types#sets
    """

    @overload
    def sadd(self: SyncClientProtocol, name: KeyT, *values: FieldT) -> int: ...

    @overload
    def sadd(
        self: AsyncClientProtocol, name: KeyT, *values: FieldT
    ) -> Awaitable[int]: ...

    def sadd(self, name: KeyT, *values: FieldT) -> int | Awaitable[int]:
        """
        Add ``value(s)`` to set ``name``

        For more information, see https://redis.io/commands/sadd
        """
        return self.execute_command("SADD", name, *values)

    @overload
    def scard(self: SyncClientProtocol, name: KeyT) -> int: ...

    @overload
    def scard(self: AsyncClientProtocol, name: KeyT) -> Awaitable[int]: ...

    def scard(self, name: KeyT) -> int | Awaitable[int]:
        """
        Return the number of elements in set ``name``

        For more information, see https://redis.io/commands/scard
        """
        return self.execute_command("SCARD", name, keys=[name])

    @overload
    def sdiff(
        self: SyncClientProtocol, keys: List, *args: List
    ) -> set[bytes | str]: ...

    @overload
    def sdiff(
        self: AsyncClientProtocol, keys: List, *args: List
    ) -> Awaitable[set[bytes | str]]: ...

    def sdiff(
        self, keys: List, *args: List
    ) -> set[bytes | str] | Awaitable[set[bytes | str]]:
        """
        Return the difference of sets specified by ``keys``

        For more information, see https://redis.io/commands/sdiff
        """
        args = list_or_args(keys, args)
        return self.execute_command("SDIFF", *args, keys=args)

    @overload
    def sdiffcard(
        self: SyncClientProtocol, numkeys: int, keys: List[KeyT], limit: int = 0
    ) -> int: ...

    @overload
    def sdiffcard(
        self: AsyncClientProtocol, numkeys: int, keys: List[KeyT], limit: int = 0
    ) -> Awaitable[int]: ...

    def sdiffcard(
        self, numkeys: int, keys: List[KeyT], limit: int = 0
    ) -> int | Awaitable[int]:
        """
        Return the cardinality of the difference between the first set and all
        the successive sets specified by ``keys``, without returning the
        difference itself.

        When LIMIT is provided (defaults to 0 and means unlimited), the returned
        cardinality is capped at ``limit`` and the server may stop the
        computation once the capped result is known.

        For more information, see https://redis.io/commands/sdiffcard
        """
        args = [numkeys, *keys, "LIMIT", limit]
        return self.execute_command("SDIFFCARD", *args, keys=keys)

    @overload
    def sdiffstore(
        self: SyncClientProtocol, dest: str, keys: List, *args: List
    ) -> int: ...

    @overload
    def sdiffstore(
        self: AsyncClientProtocol, dest: str, keys: List, *args: List
    ) -> Awaitable[int]: ...

    def sdiffstore(self, dest: str, keys: List, *args: List) -> int | Awaitable[int]:
        """
        Store the difference of sets specified by ``keys`` into a new
        set named ``dest``.  Returns the number of keys in the new set.

        For more information, see https://redis.io/commands/sdiffstore
        """
        args = list_or_args(keys, args)
        return self.execute_command("SDIFFSTORE", dest, *args)

    @overload
    def sinter(
        self: SyncClientProtocol, keys: List, *args: List
    ) -> set[bytes | str]: ...

    @overload
    def sinter(
        self: AsyncClientProtocol, keys: List, *args: List
    ) -> Awaitable[set[bytes | str]]: ...

    def sinter(
        self, keys: List, *args: List
    ) -> set[bytes | str] | Awaitable[set[bytes | str]]:
        """
        Return the intersection of sets specified by ``keys``

        For more information, see https://redis.io/commands/sinter
        """
        args = list_or_args(keys, args)
        return self.execute_command("SINTER", *args, keys=args)

    @overload
    def sintercard(
        self: SyncClientProtocol, numkeys: int, keys: List[KeyT], limit: int = 0
    ) -> int: ...

    @overload
    def sintercard(
        self: AsyncClientProtocol, numkeys: int, keys: List[KeyT], limit: int = 0
    ) -> Awaitable[int]: ...

    def sintercard(
        self, numkeys: int, keys: List[KeyT], limit: int = 0
    ) -> int | Awaitable[int]:
        """
        Return the cardinality of the intersect of multiple sets specified by ``keys``.

        When LIMIT provided (defaults to 0 and means unlimited), if the intersection
        cardinality reaches limit partway through the computation, the algorithm will
        exit and yield limit as the cardinality

        For more information, see https://redis.io/commands/sintercard
        """
        args = [numkeys, *keys, "LIMIT", limit]
        return self.execute_command("SINTERCARD", *args, keys=keys)

    @overload
    def sinterstore(
        self: SyncClientProtocol, dest: KeyT, keys: List, *args: List
    ) -> int: ...

    @overload
    def sinterstore(
        self: AsyncClientProtocol, dest: KeyT, keys: List, *args: List
    ) -> Awaitable[int]: ...

    def sinterstore(self, dest: KeyT, keys: List, *args: List) -> int | Awaitable[int]:
        """
        Store the intersection of sets specified by ``keys`` into a new
        set named ``dest``.  Returns the number of keys in the new set.

        For more information, see https://redis.io/commands/sinterstore
        """
        args = list_or_args(keys, args)
        return self.execute_command("SINTERSTORE", dest, *args)

    @overload
    def sismember(
        self: SyncClientProtocol, name: KeyT, value: str
    ) -> Literal[0] | Literal[1]: ...

    @overload
    def sismember(
        self: AsyncClientProtocol, name: KeyT, value: str
    ) -> Awaitable[Literal[0] | Literal[1]]: ...

    def sismember(self, name: KeyT, value: str) -> (
        Literal[0] | Literal[1]
    ) | Awaitable[Literal[0] | Literal[1]]:
        """
        Return whether ``value`` is a member of set ``name``:
        - 1 if the value is a member of the set.
        - 0 if the value is not a member of the set or if key does not exist.

        For more information, see https://redis.io/commands/sismember
        """
        return self.execute_command("SISMEMBER", name, value, keys=[name])

    @overload
    def smembers(self: SyncClientProtocol, name: KeyT) -> set[bytes | str]: ...

    @overload
    def smembers(
        self: AsyncClientProtocol, name: KeyT
    ) -> Awaitable[set[bytes | str]]: ...

    def smembers(self, name: KeyT) -> set[bytes | str] | Awaitable[set[bytes | str]]:
        """
        Return all members of the set ``name``

        For more information, see https://redis.io/commands/smembers
        """
        return self.execute_command("SMEMBERS", name, keys=[name])

    @overload
    def smismember(
        self: SyncClientProtocol, name: KeyT, values: List, *args: List
    ) -> list[Literal[0] | Literal[1]]: ...

    @overload
    def smismember(
        self: AsyncClientProtocol, name: KeyT, values: List, *args: List
    ) -> Awaitable[list[Literal[0] | Literal[1]]]: ...

    def smismember(
        self, name: KeyT, values: List, *args: List
    ) -> list[Literal[0] | Literal[1]] | Awaitable[list[Literal[0] | Literal[1]]]:
        """
        Return whether each value in ``values`` is a member of the set ``name``
        as a list of ``int`` in the order of ``values``:
        - 1 if the value is a member of the set.
        - 0 if the value is not a member of the set or if key does not exist.

        For more information, see https://redis.io/commands/smismember
        """
        args = list_or_args(values, args)
        return self.execute_command("SMISMEMBER", name, *args, keys=[name])

    @overload
    def smove(self: SyncClientProtocol, src: KeyT, dst: KeyT, value: str) -> bool: ...

    @overload
    def smove(
        self: AsyncClientProtocol, src: KeyT, dst: KeyT, value: str
    ) -> Awaitable[bool]: ...

    def smove(self, src: KeyT, dst: KeyT, value: str) -> bool | Awaitable[bool]:
        """
        Move ``value`` from set ``src`` to set ``dst`` atomically

        For more information, see https://redis.io/commands/smove
        """
        return self.execute_command("SMOVE", src, dst, value)

    @overload
    def spop(
        self: SyncClientProtocol, name: KeyT, count: int | None = None
    ) -> bytes | str | set[bytes | str] | None: ...

    @overload
    def spop(
        self: AsyncClientProtocol, name: KeyT, count: int | None = None
    ) -> Awaitable[bytes | str | set[bytes | str] | None]: ...

    def spop(self, name: KeyT, count: int | None = None) -> (
        bytes | str | set[bytes | str] | None
    ) | Awaitable[bytes | str | set[bytes | str] | None]:
        """
        Remove and return a random member of set ``name``

        For more information, see https://redis.io/commands/spop
        """
        args = (count is not None) and [count] or []
        return self.execute_command("SPOP", name, *args)

    @overload
    def srandmember(
        self: SyncClientProtocol, name: KeyT, number: int | None = None
    ) -> bytes | str | list[bytes | str] | None: ...

    @overload
    def srandmember(
        self: AsyncClientProtocol, name: KeyT, number: int | None = None
    ) -> Awaitable[bytes | str | list[bytes | str] | None]: ...

    def srandmember(self, name: KeyT, number: int | None = None) -> (
        bytes | str | list[bytes | str] | None
    ) | Awaitable[bytes | str | list[bytes | str] | None]:
        """
        If ``number`` is None, returns a random member of set ``name``.

        If ``number`` is supplied, returns a list of ``number`` random
        members of set ``name``. Note this is only available when running
        Redis 2.6+.

        For more information, see https://redis.io/commands/srandmember
        """
        args = (number is not None) and [number] or []
        return self.execute_command("SRANDMEMBER", name, *args)

    @overload
    def srem(self: SyncClientProtocol, name: KeyT, *values: FieldT) -> int: ...

    @overload
    def srem(
        self: AsyncClientProtocol, name: KeyT, *values: FieldT
    ) -> Awaitable[int]: ...

    def srem(self, name: KeyT, *values: FieldT) -> int | Awaitable[int]:
        """
        Remove ``values`` from set ``name``

        For more information, see https://redis.io/commands/srem
        """
        return self.execute_command("SREM", name, *values)

    @overload
    def sunion(
        self: SyncClientProtocol, keys: List, *args: List
    ) -> set[bytes | str]: ...

    @overload
    def sunion(
        self: AsyncClientProtocol, keys: List, *args: List
    ) -> Awaitable[set[bytes | str]]: ...

    def sunion(
        self, keys: List, *args: List
    ) -> set[bytes | str] | Awaitable[set[bytes | str]]:
        """
        Return the union of sets specified by ``keys``

        For more information, see https://redis.io/commands/sunion
        """
        args = list_or_args(keys, args)
        return self.execute_command("SUNION", *args, keys=args)

    @overload
    def sunioncard(
        self: SyncClientProtocol,
        numkeys: int,
        keys: List[KeyT],
        limit: int = 0,
        approx: bool = False,
    ) -> int: ...

    @overload
    def sunioncard(
        self: AsyncClientProtocol,
        numkeys: int,
        keys: List[KeyT],
        limit: int = 0,
        approx: bool = False,
    ) -> Awaitable[int]: ...

    def sunioncard(
        self,
        numkeys: int,
        keys: List[KeyT],
        limit: int = 0,
        approx: bool = False,
    ) -> int | Awaitable[int]:
        """
        Return the cardinality of the union of multiple sets specified by
        ``keys``, without returning the union itself.

        When ``approx`` is True, the ``APPROX`` option is sent and the server
        returns an approximate cardinality computed using HyperLogLog.

        When LIMIT is provided (defaults to 0 and means unlimited), the returned
        cardinality is capped at ``limit``. In exact mode the result equals
        ``limit`` when the real cardinality exceeds ``limit``; in approximate
        mode the result will not exceed ``limit``. Options are emitted in the
        canonical order ``APPROX`` before ``LIMIT``.

        For more information, see https://redis.io/commands/sunioncard
        """
        pieces: list = [numkeys, *keys]
        if approx:
            pieces.append("APPROX")
        pieces.extend(["LIMIT", limit])
        return self.execute_command("SUNIONCARD", *pieces, keys=keys)

    @overload
    def sunionstore(
        self: SyncClientProtocol, dest: KeyT, keys: List, *args: List
    ) -> int: ...

    @overload
    def sunionstore(
        self: AsyncClientProtocol, dest: KeyT, keys: List, *args: List
    ) -> Awaitable[int]: ...

    def sunionstore(self, dest: KeyT, keys: List, *args: List) -> int | Awaitable[int]:
        """
        Store the union of sets specified by ``keys`` into a new
        set named ``dest``.  Returns the number of keys in the new set.

        For more information, see https://redis.io/commands/sunionstore
        """
        args = list_or_args(keys, args)
        return self.execute_command("SUNIONSTORE", dest, *args)

    @overload
    def saddex(
        self: SyncClientProtocol,
        name: KeyT,
        *members: FieldT,
        ex: ExpiryT | str | None = None,
        px: ExpiryT | str | None = None,
        exat: AbsExpiryT | None = None,
        pxat: AbsExpiryT | None = None,
        data_persist_option: SetDataPersistOptions | None = None,
        keepttl: bool = False,
    ) -> int: ...

    @overload
    def saddex(
        self: AsyncClientProtocol,
        name: KeyT,
        *members: FieldT,
        ex: ExpiryT | str | None = None,
        px: ExpiryT | str | None = None,
        exat: AbsExpiryT | None = None,
        pxat: AbsExpiryT | None = None,
        data_persist_option: SetDataPersistOptions | None = None,
        keepttl: bool = False,
    ) -> Awaitable[int]: ...

    def saddex(
        self,
        name: KeyT,
        *members: FieldT,
        ex: ExpiryT | str | None = None,
        px: ExpiryT | str | None = None,
        exat: AbsExpiryT | None = None,
        pxat: AbsExpiryT | None = None,
        data_persist_option: SetDataPersistOptions | None = None,
        keepttl: bool = False,
    ) -> int | Awaitable[int]:
        """
        Add ``members`` to set ``name``, optionally setting an expiration time
        on every member in the command.

        ``ex`` sets an expire flag on ``members`` for ``ex`` seconds.

        ``px`` sets an expire flag on ``members`` for ``px`` milliseconds.

        ``ex`` and ``px`` also accept digit-only strings, such as ``"60"``
        and ``"500"``, respectively.

        ``exat`` sets an expire flag on ``members`` at ``exat`` seconds,
            specified in unix time.

        ``pxat`` sets an expire flag on ``members`` at ``pxat`` milliseconds,
            specified in unix time.

        ``data_persist_option`` can be set to ``MNX`` or ``MXX`` to control the
            behavior of the command.
            ``MNX`` will add the members only if none of them already exists.
            ``MXX`` will add the members only if all of them already exist.
            A logically expired member does not exist.

        ``keepttl`` if True, retain the time to live associated with the
            members that already exist. Without any expiration option, the
            expiration of an existing member is discarded.

        Returns ``1`` if at least one member was added, updated or deleted, and
        ``0`` otherwise, for example when the ``MNX`` or ``MXX`` condition was
        not met. ``1`` does not mean that the members exist: an expiration time
        in the past removes the members that exist and adds none.

        Available since Redis 8.12
        For more information, see https://redis.io/commands/saddex
        """
        if not at_most_one_value_set(
            (
                ex is not None,
                px is not None,
                exat is not None,
                pxat is not None,
                keepttl,
            )
        ):
            raise DataError(
                "``ex``, ``px``, ``exat``, ``pxat``, "
                "and ``keepttl`` are mutually exclusive."
            )

        exp_options: list[EncodableT] = extract_expire_flags(ex, px, exat, pxat)
        if data_persist_option:
            exp_options.append(data_persist_option.value)

        if keepttl:
            exp_options.append("KEEPTTL")

        return self.execute_command(
            "SADDEX", name, *exp_options, "MEMBERS", len(members), *members
        )

    @overload
    def sexpire(
        self: SyncClientProtocol,
        name: KeyT,
        seconds: ExpiryT,
        *members: FieldT,
        nx: bool = False,
        xx: bool = False,
        gt: bool = False,
        lt: bool = False,
    ) -> list[int]: ...

    @overload
    def sexpire(
        self: AsyncClientProtocol,
        name: KeyT,
        seconds: ExpiryT,
        *members: FieldT,
        nx: bool = False,
        xx: bool = False,
        gt: bool = False,
        lt: bool = False,
    ) -> Awaitable[list[int]]: ...

    def sexpire(
        self,
        name: KeyT,
        seconds: ExpiryT,
        *members: FieldT,
        nx: bool = False,
        xx: bool = False,
        gt: bool = False,
        lt: bool = False,
    ) -> list[int] | Awaitable[list[int]]:
        """
        Sets or updates the expiration time for members within a set key, using
        relative time in seconds.

        If a member already has an expiration time, the behavior of the update can
        be controlled using the `nx`, `xx`, `gt`, and `lt` parameters. A member
        without an expiration time counts as one that never expires.

        The return value provides detailed information about the outcome for each
        member.

        Available since Redis 8.12
        For more information, see https://redis.io/commands/sexpire

        Args:
            name: The name of the set key.
            seconds: Expiration time in seconds, relative. Can be an integer, or a
                     Python `timedelta` object.
            members: List of members within the set to apply the expiration time to.
            nx: Set expiry only when the member has no expiry.
            xx: Set expiry only when the member has an existing expiry.
            gt: Set expiry only when the new expiry is greater than the current one.
            lt: Set expiry only when the new expiry is less than the current one.

        Returns:
            Returns a list which contains for each member in the request:
                - `-2` if the member does not exist, or if the key does not exist.
                - `0` if the specified NX | XX | GT | LT condition was not met.
                - `1` if the expiration time was set or updated.
                - `2` if the member was deleted because the specified expiration
                  time is in the past.
        """
        if nx + xx + gt + lt > 1:
            raise ValueError("Only one of 'nx', 'xx', 'gt', 'lt' can be specified.")

        if isinstance(seconds, datetime.timedelta):
            seconds = int(seconds.total_seconds())

        options = []
        if nx:
            options.append("NX")
        if xx:
            options.append("XX")
        if gt:
            options.append("GT")
        if lt:
            options.append("LT")

        return self.execute_command(
            "SEXPIRE", name, seconds, *options, "MEMBERS", len(members), *members
        )

    @overload
    def spexpire(
        self: SyncClientProtocol,
        name: KeyT,
        milliseconds: ExpiryT,
        *members: FieldT,
        nx: bool = False,
        xx: bool = False,
        gt: bool = False,
        lt: bool = False,
    ) -> list[int]: ...

    @overload
    def spexpire(
        self: AsyncClientProtocol,
        name: KeyT,
        milliseconds: ExpiryT,
        *members: FieldT,
        nx: bool = False,
        xx: bool = False,
        gt: bool = False,
        lt: bool = False,
    ) -> Awaitable[list[int]]: ...

    def spexpire(
        self,
        name: KeyT,
        milliseconds: ExpiryT,
        *members: FieldT,
        nx: bool = False,
        xx: bool = False,
        gt: bool = False,
        lt: bool = False,
    ) -> list[int] | Awaitable[list[int]]:
        """
        Sets or updates the expiration time for members within a set key, using
        relative time in milliseconds.

        If a member already has an expiration time, the behavior of the update can
        be controlled using the `nx`, `xx`, `gt`, and `lt` parameters. A member
        without an expiration time counts as one that never expires.

        The return value provides detailed information about the outcome for each
        member.

        Available since Redis 8.12
        For more information, see https://redis.io/commands/spexpire

        Args:
            name: The name of the set key.
            milliseconds: Expiration time in milliseconds, relative. Can be an
                          integer, or a Python `timedelta` object.
            members: List of members within the set to apply the expiration time to.
            nx: Set expiry only when the member has no expiry.
            xx: Set expiry only when the member has an existing expiry.
            gt: Set expiry only when the new expiry is greater than the current one.
            lt: Set expiry only when the new expiry is less than the current one.

        Returns:
            Returns a list which contains for each member in the request:
                - `-2` if the member does not exist, or if the key does not exist.
                - `0` if the specified NX | XX | GT | LT condition was not met.
                - `1` if the expiration time was set or updated.
                - `2` if the member was deleted because the specified expiration
                  time is in the past.
        """
        if nx + xx + gt + lt > 1:
            raise ValueError("Only one of 'nx', 'xx', 'gt', 'lt' can be specified.")

        if isinstance(milliseconds, datetime.timedelta):
            milliseconds = int(milliseconds.total_seconds() * 1000)

        options = []
        if nx:
            options.append("NX")
        if xx:
            options.append("XX")
        if gt:
            options.append("GT")
        if lt:
            options.append("LT")

        return self.execute_command(
            "SPEXPIRE", name, milliseconds, *options, "MEMBERS", len(members), *members
        )

    @overload
    def sexpireat(
        self: SyncClientProtocol,
        name: KeyT,
        unix_time_seconds: AbsExpiryT,
        *members: FieldT,
        nx: bool = False,
        xx: bool = False,
        gt: bool = False,
        lt: bool = False,
    ) -> list[int]: ...

    @overload
    def sexpireat(
        self: AsyncClientProtocol,
        name: KeyT,
        unix_time_seconds: AbsExpiryT,
        *members: FieldT,
        nx: bool = False,
        xx: bool = False,
        gt: bool = False,
        lt: bool = False,
    ) -> Awaitable[list[int]]: ...

    def sexpireat(
        self,
        name: KeyT,
        unix_time_seconds: AbsExpiryT,
        *members: FieldT,
        nx: bool = False,
        xx: bool = False,
        gt: bool = False,
        lt: bool = False,
    ) -> list[int] | Awaitable[list[int]]:
        """
        Sets or updates the expiration time for members within a set key, using an
        absolute Unix timestamp in seconds.

        If a member already has an expiration time, the behavior of the update can
        be controlled using the `nx`, `xx`, `gt`, and `lt` parameters. A member
        without an expiration time counts as one that never expires.

        The return value provides detailed information about the outcome for each
        member.

        Available since Redis 8.12
        For more information, see https://redis.io/commands/sexpireat

        Args:
            name: The name of the set key.
            unix_time_seconds: Expiration time as Unix timestamp in seconds. Can be
                               an integer or a Python `datetime` object.
            members: List of members within the set to apply the expiration time to.
            nx: Set expiry only when the member has no expiry.
            xx: Set expiry only when the member has an existing expiry.
            gt: Set expiry only when the new expiry is greater than the current one.
            lt: Set expiry only when the new expiry is less than the current one.

        Returns:
            Returns a list which contains for each member in the request:
                - `-2` if the member does not exist, or if the key does not exist.
                - `0` if the specified NX | XX | GT | LT condition was not met.
                - `1` if the expiration time was set or updated.
                - `2` if the member was deleted because the specified expiration
                  time is in the past.
        """
        if nx + xx + gt + lt > 1:
            raise ValueError("Only one of 'nx', 'xx', 'gt', 'lt' can be specified.")

        if isinstance(unix_time_seconds, datetime.datetime):
            unix_time_seconds = int(unix_time_seconds.timestamp())

        options = []
        if nx:
            options.append("NX")
        if xx:
            options.append("XX")
        if gt:
            options.append("GT")
        if lt:
            options.append("LT")

        return self.execute_command(
            "SEXPIREAT",
            name,
            unix_time_seconds,
            *options,
            "MEMBERS",
            len(members),
            *members,
        )

    @overload
    def spexpireat(
        self: SyncClientProtocol,
        name: KeyT,
        unix_time_milliseconds: AbsExpiryT,
        *members: FieldT,
        nx: bool = False,
        xx: bool = False,
        gt: bool = False,
        lt: bool = False,
    ) -> list[int]: ...

    @overload
    def spexpireat(
        self: AsyncClientProtocol,
        name: KeyT,
        unix_time_milliseconds: AbsExpiryT,
        *members: FieldT,
        nx: bool = False,
        xx: bool = False,
        gt: bool = False,
        lt: bool = False,
    ) -> Awaitable[list[int]]: ...

    def spexpireat(
        self,
        name: KeyT,
        unix_time_milliseconds: AbsExpiryT,
        *members: FieldT,
        nx: bool = False,
        xx: bool = False,
        gt: bool = False,
        lt: bool = False,
    ) -> list[int] | Awaitable[list[int]]:
        """
        Sets or updates the expiration time for members within a set key, using an
        absolute Unix timestamp in milliseconds.

        If a member already has an expiration time, the behavior of the update can
        be controlled using the `nx`, `xx`, `gt`, and `lt` parameters. A member
        without an expiration time counts as one that never expires.

        The return value provides detailed information about the outcome for each
        member.

        Available since Redis 8.12
        For more information, see https://redis.io/commands/spexpireat

        Args:
            name: The name of the set key.
            unix_time_milliseconds: Expiration time as Unix timestamp in
                                    milliseconds. Can be an integer or a Python
                                    `datetime` object.
            members: List of members within the set to apply the expiration time to.
            nx: Set expiry only when the member has no expiry.
            xx: Set expiry only when the member has an existing expiry.
            gt: Set expiry only when the new expiry is greater than the current one.
            lt: Set expiry only when the new expiry is less than the current one.

        Returns:
            Returns a list which contains for each member in the request:
                - `-2` if the member does not exist, or if the key does not exist.
                - `0` if the specified NX | XX | GT | LT condition was not met.
                - `1` if the expiration time was set or updated.
                - `2` if the member was deleted because the specified expiration
                  time is in the past.
        """
        if nx + xx + gt + lt > 1:
            raise ValueError("Only one of 'nx', 'xx', 'gt', 'lt' can be specified.")

        if isinstance(unix_time_milliseconds, datetime.datetime):
            unix_time_milliseconds = int(unix_time_milliseconds.timestamp() * 1000)

        options = []
        if nx:
            options.append("NX")
        if xx:
            options.append("XX")
        if gt:
            options.append("GT")
        if lt:
            options.append("LT")

        return self.execute_command(
            "SPEXPIREAT",
            name,
            unix_time_milliseconds,
            *options,
            "MEMBERS",
            len(members),
            *members,
        )

    @overload
    def spersist(
        self: SyncClientProtocol, name: KeyT, *members: FieldT
    ) -> list[int]: ...

    @overload
    def spersist(
        self: AsyncClientProtocol, name: KeyT, *members: FieldT
    ) -> Awaitable[list[int]]: ...

    def spersist(
        self, name: KeyT, *members: FieldT
    ) -> list[int] | Awaitable[list[int]]:
        """
        Removes the expiration time for each specified member in a set.

        Available since Redis 8.12
        For more information, see https://redis.io/commands/spersist

        Args:
            name: The name of the set key.
            members: A list of members within the set from which to remove the
                     expiration time.

        Returns:
            Returns a list which contains for each member in the request:
                - `-2` if the member does not exist, or if the key does not exist.
                - `-1` if the member exists but has no associated expiration time.
                - `1` if the expiration time was successfully removed from the
                  member.
        """
        return self.execute_command("SPERSIST", name, "MEMBERS", len(members), *members)

    @overload
    def sexpiretime(
        self: SyncClientProtocol, name: KeyT, *members: FieldT
    ) -> list[int]: ...

    @overload
    def sexpiretime(
        self: AsyncClientProtocol, name: KeyT, *members: FieldT
    ) -> Awaitable[list[int]]: ...

    def sexpiretime(
        self, name: KeyT, *members: FieldT
    ) -> list[int] | Awaitable[list[int]]:
        """
        Returns the expiration times of set members as Unix timestamps in seconds,
        rounded up to the next second.

        Available since Redis 8.12
        For more information, see https://redis.io/commands/sexpiretime

        Args:
            name: The set key.
            members: A list of members within the set for which to get the
                     expiration time.

        Returns:
            Returns a list which contains for each member in the request:
                - `-2` if the member does not exist, or if the key does not exist.
                - `-1` if the member exists but has no associated expire time.
                - A positive integer representing the expiration Unix timestamp in
                  seconds, if the member has an associated expiration time.
        """
        return self.execute_command(
            "SEXPIRETIME", name, "MEMBERS", len(members), *members, keys=[name]
        )

    @overload
    def spexpiretime(
        self: SyncClientProtocol, name: KeyT, *members: FieldT
    ) -> list[int]: ...

    @overload
    def spexpiretime(
        self: AsyncClientProtocol, name: KeyT, *members: FieldT
    ) -> Awaitable[list[int]]: ...

    def spexpiretime(
        self, name: KeyT, *members: FieldT
    ) -> list[int] | Awaitable[list[int]]:
        """
        Returns the expiration times of set members as Unix timestamps in
        milliseconds.

        Available since Redis 8.12
        For more information, see https://redis.io/commands/spexpiretime

        Args:
            name: The set key.
            members: A list of members within the set for which to get the
                     expiration time.

        Returns:
            Returns a list which contains for each member in the request:
                - `-2` if the member does not exist, or if the key does not exist.
                - `-1` if the member exists but has no associated expire time.
                - A positive integer representing the expiration Unix timestamp in
                  milliseconds, if the member has an associated expiration time.
        """
        return self.execute_command(
            "SPEXPIRETIME", name, "MEMBERS", len(members), *members, keys=[name]
        )

    @overload
    def sttl(self: SyncClientProtocol, name: KeyT, *members: FieldT) -> list[int]: ...

    @overload
    def sttl(
        self: AsyncClientProtocol, name: KeyT, *members: FieldT
    ) -> Awaitable[list[int]]: ...

    def sttl(self, name: KeyT, *members: FieldT) -> list[int] | Awaitable[list[int]]:
        """
        Returns the TTL (Time To Live) in seconds for each specified member within a
        set key, rounded up to the next second.

        Available since Redis 8.12
        For more information, see https://redis.io/commands/sttl

        Args:
            name: The set key.
            members: A list of members within the set for which to get the TTL.

        Returns:
            Returns a list which contains for each member in the request:
                - `-2` if the member does not exist, or if the key does not exist.
                - `-1` if the member exists but has no associated expire time.
                - A non-negative integer representing the TTL in seconds if the
                  member has an associated expiration time.
        """
        return self.execute_command(
            "STTL", name, "MEMBERS", len(members), *members, keys=[name]
        )

    @overload
    def spttl(self: SyncClientProtocol, name: KeyT, *members: FieldT) -> list[int]: ...

    @overload
    def spttl(
        self: AsyncClientProtocol, name: KeyT, *members: FieldT
    ) -> Awaitable[list[int]]: ...

    def spttl(self, name: KeyT, *members: FieldT) -> list[int] | Awaitable[list[int]]:
        """
        Returns the TTL (Time To Live) in milliseconds for each specified member
        within a set key.

        Available since Redis 8.12
        For more information, see https://redis.io/commands/spttl

        Args:
            name: The set key.
            members: A list of members within the set for which to get the TTL.

        Returns:
            Returns a list which contains for each member in the request:
                - `-2` if the member does not exist, or if the key does not exist.
                - `-1` if the member exists but has no associated expire time.
                - A non-negative integer representing the TTL in milliseconds if
                  the member has an associated expiration time.
        """
        return self.execute_command(
            "SPTTL", name, "MEMBERS", len(members), *members, keys=[name]
        )


AsyncSetCommands = SetCommands


class StreamCommands(CommandsProtocol):
    """
    Redis commands for Stream data type.
    see: https://redis.io/topics/streams-intro
    """

    @overload
    def xack(
        self: SyncClientProtocol, name: KeyT, groupname: GroupT, *ids: StreamIdT
    ) -> int: ...

    @overload
    def xack(
        self: AsyncClientProtocol, name: KeyT, groupname: GroupT, *ids: StreamIdT
    ) -> Awaitable[int]: ...

    def xack(
        self, name: KeyT, groupname: GroupT, *ids: StreamIdT
    ) -> int | Awaitable[int]:
        """
        Acknowledges the successful processing of one or more messages.

        Args:
            name: name of the stream.
            groupname: name of the consumer group.
            *ids: message ids to acknowledge.

        For more information, see https://redis.io/commands/xack
        """
        return self.execute_command("XACK", name, groupname, *ids)

    @overload
    def xackdel(
        self: SyncClientProtocol,
        name: KeyT,
        groupname: GroupT,
        *ids: StreamIdT,
        ref_policy: Literal["KEEPREF", "DELREF", "ACKED"] = "KEEPREF",
    ) -> int: ...

    @overload
    def xackdel(
        self: AsyncClientProtocol,
        name: KeyT,
        groupname: GroupT,
        *ids: StreamIdT,
        ref_policy: Literal["KEEPREF", "DELREF", "ACKED"] = "KEEPREF",
    ) -> Awaitable[int]: ...

    def xackdel(
        self,
        name: KeyT,
        groupname: GroupT,
        *ids: StreamIdT,
        ref_policy: Literal["KEEPREF", "DELREF", "ACKED"] = "KEEPREF",
    ) -> int | Awaitable[int]:
        """
        Combines the functionality of XACK and XDEL. Acknowledges the specified
        message IDs in the given consumer group and simultaneously attempts to
        delete the corresponding entries from the stream.
        """
        if not ids:
            raise DataError("XACKDEL requires at least one message ID")

        if ref_policy not in {"KEEPREF", "DELREF", "ACKED"}:
            raise DataError("XACKDEL ref_policy must be one of: KEEPREF, DELREF, ACKED")

        pieces = [name, groupname, ref_policy, "IDS", len(ids)]
        pieces.extend(ids)
        return self.execute_command("XACKDEL", *pieces)

    @overload
    def xadd(
        self: SyncClientProtocol,
        name: KeyT,
        fields: Dict[FieldT, EncodableT],
        id: StreamIdT = "*",
        maxlen: int | None = None,
        approximate: bool = True,
        nomkstream: bool = False,
        minid: StreamIdT | None = None,
        limit: int | None = None,
        ref_policy: Literal["KEEPREF", "DELREF", "ACKED"] | None = None,
        idmpauto: str | None = None,
        idmp: tuple[str, bytes] | None = None,
    ) -> bytes | str: ...

    @overload
    def xadd(
        self: AsyncClientProtocol,
        name: KeyT,
        fields: Dict[FieldT, EncodableT],
        id: StreamIdT = "*",
        maxlen: int | None = None,
        approximate: bool = True,
        nomkstream: bool = False,
        minid: StreamIdT | None = None,
        limit: int | None = None,
        ref_policy: Literal["KEEPREF", "DELREF", "ACKED"] | None = None,
        idmpauto: str | None = None,
        idmp: tuple[str, bytes] | None = None,
    ) -> Awaitable[bytes | str]: ...

    def xadd(
        self,
        name: KeyT,
        fields: Dict[FieldT, EncodableT],
        id: StreamIdT = "*",
        maxlen: Optional[int] = None,
        approximate: bool = True,
        nomkstream: bool = False,
        minid: Union[StreamIdT, None] = None,
        limit: int | None = None,
        ref_policy: Literal["KEEPREF", "DELREF", "ACKED"] | None = None,
        idmpauto: str | None = None,
        idmp: tuple[str, bytes] | None = None,
    ) -> (bytes | str) | Awaitable[bytes | str]:
        """
        Add to a stream.
        name: name of the stream
        fields: dict of field/value pairs to insert into the stream
        id: Location to insert this record. By default it is appended.
        maxlen: truncate old stream members beyond this size.
        Can't be specified with minid.
        approximate: actual stream length may be slightly more than maxlen
        nomkstream: When set to true, do not make a stream
        minid: the minimum id in the stream to query.
        Can't be specified with maxlen.
        limit: specifies the maximum number of entries to retrieve
        ref_policy: optional reference policy for consumer groups when trimming:
            - KEEPREF (default): When trimming, preserves references in consumer groups' PEL
            - DELREF: When trimming, removes all references from consumer groups' PEL
            - ACKED: When trimming, only removes entries acknowledged by all consumer groups
        idmpauto: Producer ID for automatic idempotent ID calculation.
            Automatically calculates an idempotent ID based on entry content to prevent
            duplicate entries. Can only be used with id='*'. Creates an IDMP map if it
            doesn't exist yet. The producer ID must be unique per producer and consistent
            across restarts.
        idmp: Tuple of (producer_id, idempotent_id) for explicit idempotent ID.
            Uses a specific idempotent ID to prevent duplicate entries. Can only be used
            with id='*'. The producer ID must be unique per producer and consistent across
            restarts. The idempotent ID must be unique per message and per producer.
            Shorter idempotent IDs require less memory and allow faster processing.
            Creates an IDMP map if it doesn't exist yet.

        For more information, see https://redis.io/commands/xadd
        """
        pieces: list[EncodableT] = []
        if maxlen is not None and minid is not None:
            raise DataError("Only one of ```maxlen``` or ```minid``` may be specified")

        if idmpauto is not None and idmp is not None:
            raise DataError("Only one of ```idmpauto``` or ```idmp``` may be specified")

        if (idmpauto is not None or idmp is not None) and id != "*":
            raise DataError("IDMPAUTO and IDMP can only be used with id='*'")

        if ref_policy is not None and ref_policy not in {"KEEPREF", "DELREF", "ACKED"}:
            raise DataError("XADD ref_policy must be one of: KEEPREF, DELREF, ACKED")

        if nomkstream:
            pieces.append(b"NOMKSTREAM")
        if ref_policy is not None:
            pieces.append(ref_policy)
        if idmpauto is not None:
            pieces.extend([b"IDMPAUTO", idmpauto])
        if idmp is not None:
            if not isinstance(idmp, tuple) or len(idmp) != 2:
                raise DataError(
                    "XADD idmp must be a tuple of (producer_id, idempotent_id)"
                )
            pieces.extend([b"IDMP", idmp[0], idmp[1]])
        if maxlen is not None:
            if not isinstance(maxlen, int) or maxlen < 0:
                raise DataError("XADD maxlen must be non-negative integer")
            pieces.append(b"MAXLEN")
            if approximate:
                pieces.append(b"~")
            pieces.append(str(maxlen))
        if minid is not None:
            pieces.append(b"MINID")
            if approximate:
                pieces.append(b"~")
            pieces.append(minid)
        if limit is not None:
            pieces.extend([b"LIMIT", limit])
        pieces.append(id)
        if not isinstance(fields, dict) or len(fields) == 0:
            raise DataError("XADD fields must be a non-empty dict")
        for pair in fields.items():
            pieces.extend(pair)
        return self.execute_command("XADD", name, *pieces)

    @overload
    def xcfgset(
        self: SyncClientProtocol,
        name: KeyT,
        idmp_duration: int | None = None,
        idmp_maxsize: int | None = None,
    ) -> bytes | str: ...

    @overload
    def xcfgset(
        self: AsyncClientProtocol,
        name: KeyT,
        idmp_duration: int | None = None,
        idmp_maxsize: int | None = None,
    ) -> Awaitable[bytes | str]: ...

    def xcfgset(
        self,
        name: KeyT,
        idmp_duration: int | None = None,
        idmp_maxsize: int | None = None,
    ) -> (bytes | str) | Awaitable[bytes | str]:
        """
        Configure the idempotency parameters for a stream's IDMP map.

        Sets how long Redis remembers each idempotent ID (iid) and the maximum
        number of iids to track. This command clears the existing IDMP map
        (Redis forgets all previously stored iids), but only if the configuration
        value actually changes.

        Args:
            name: The name of the stream.
            idmp_duration: How long Redis remembers each iid in seconds.
                Default: 100 seconds (or value set by stream-idmp-duration config).
                Minimum: 1 second, Maximum: 300 seconds.
                Redis won't forget an iid for this duration (unless maxsize is reached).
                Should accommodate application crash recovery time.
            idmp_maxsize: Maximum number of iids Redis remembers per producer ID (pid).
                Default: 100 iids (or value set by stream-idmp-maxsize config).
                Minimum: 1 iid, Maximum: 1,000,000 (1M) iids.
                Should be set to: mark-delay [in msec] × (messages/msec) + margin.
                Example: 10K msgs/sec (10 msgs/msec), 80 msec mark-delay
                → maxsize = 10 × 80 + margin = 1000 iids.

        Returns:
            OK on success.

        For more information, see https://redis.io/commands/xcfgset
        """
        if idmp_duration is None and idmp_maxsize is None:
            raise DataError(
                "XCFGSET requires at least one of idmp_duration or idmp_maxsize"
            )

        pieces: list[EncodableT] = []

        if idmp_duration is not None:
            if (
                not isinstance(idmp_duration, int)
                or idmp_duration < 1
                or idmp_duration > 300
            ):
                raise DataError(
                    "XCFGSET idmp_duration must be an integer between 1 and 300"
                )
            pieces.extend([b"IDMP-DURATION", idmp_duration])

        if idmp_maxsize is not None:
            if (
                not isinstance(idmp_maxsize, int)
                or idmp_maxsize < 1
                or idmp_maxsize > 1000000
            ):
                raise DataError(
                    "XCFGSET idmp_maxsize must be an integer between 1 and 1,000,000"
                )
            pieces.extend([b"IDMP-MAXSIZE", idmp_maxsize])

        return self.execute_command("XCFGSET", name, *pieces)

    @overload
    def xautoclaim(
        self: SyncClientProtocol,
        name: KeyT,
        groupname: GroupT,
        consumername: ConsumerT,
        min_idle_time: int,
        start_id: StreamIdT = "0-0",
        count: int | None = None,
        justid: bool = False,
    ) -> list[Any]: ...

    @overload
    def xautoclaim(
        self: AsyncClientProtocol,
        name: KeyT,
        groupname: GroupT,
        consumername: ConsumerT,
        min_idle_time: int,
        start_id: StreamIdT = "0-0",
        count: int | None = None,
        justid: bool = False,
    ) -> Awaitable[list[Any]]: ...

    def xautoclaim(
        self,
        name: KeyT,
        groupname: GroupT,
        consumername: ConsumerT,
        min_idle_time: int,
        start_id: StreamIdT = "0-0",
        count: int | None = None,
        justid: bool = False,
    ) -> list[Any] | Awaitable[list[Any]]:
        """
        Transfers ownership of pending stream entries that match the specified
        criteria. Conceptually, equivalent to calling XPENDING and then XCLAIM,
        but provides a more straightforward way to deal with message delivery
        failures via SCAN-like semantics.
        name: name of the stream.
        groupname: name of the consumer group.
        consumername: name of a consumer that claims the message.
        min_idle_time: filter messages that were idle less than this amount of
        milliseconds.
        start_id: filter messages with equal or greater ID.
        count: optional integer, upper limit of the number of entries that the
        command attempts to claim. Set to 100 by default.
        justid: optional boolean, false by default. Return just an array of IDs
        of messages successfully claimed, without returning the actual message

        For more information, see https://redis.io/commands/xautoclaim
        """
        try:
            if int(min_idle_time) < 0:
                raise DataError(
                    "XAUTOCLAIM min_idle_time must be a nonnegative integer"
                )
        except TypeError:
            pass

        kwargs = {}
        pieces = [name, groupname, consumername, min_idle_time, start_id]

        try:
            if int(count) < 0:
                raise DataError("XPENDING count must be a integer >= 0")
            pieces.extend([b"COUNT", count])
        except TypeError:
            pass
        if justid:
            pieces.append(b"JUSTID")
            kwargs["parse_justid"] = True

        return self.execute_command("XAUTOCLAIM", *pieces, **kwargs)

    @overload
    def xclaim(
        self: SyncClientProtocol,
        name: KeyT,
        groupname: GroupT,
        consumername: ConsumerT,
        min_idle_time: int,
        message_ids: Union[List[StreamIdT], Tuple[StreamIdT]],
        idle: int | None = None,
        time: int | None = None,
        retrycount: int | None = None,
        force: bool = False,
        justid: bool = False,
    ) -> XClaimResponse: ...

    @overload
    def xclaim(
        self: AsyncClientProtocol,
        name: KeyT,
        groupname: GroupT,
        consumername: ConsumerT,
        min_idle_time: int,
        message_ids: Union[List[StreamIdT], Tuple[StreamIdT]],
        idle: int | None = None,
        time: int | None = None,
        retrycount: int | None = None,
        force: bool = False,
        justid: bool = False,
    ) -> Awaitable[XClaimResponse]: ...

    def xclaim(
        self,
        name: KeyT,
        groupname: GroupT,
        consumername: ConsumerT,
        min_idle_time: int,
        message_ids: Union[List[StreamIdT], Tuple[StreamIdT]],
        idle: int | None = None,
        time: int | None = None,
        retrycount: int | None = None,
        force: bool = False,
        justid: bool = False,
    ) -> XClaimResponse | Awaitable[XClaimResponse]:
        """
        Changes the ownership of a pending message.

        name: name of the stream.

        groupname: name of the consumer group.

        consumername: name of a consumer that claims the message.

        min_idle_time: filter messages that were idle less than this amount of
        milliseconds

        message_ids: non-empty list or tuple of message IDs to claim

        idle: optional. Set the idle time (last time it was delivered) of the
        message in ms

        time: optional integer. This is the same as idle but instead of a
        relative amount of milliseconds, it sets the idle time to a specific
        Unix time (in milliseconds).

        retrycount: optional integer. set the retry counter to the specified
        value. This counter is incremented every time a message is delivered
        again.

        force: optional boolean, false by default. Creates the pending message
        entry in the PEL even if certain specified IDs are not already in the
        PEL assigned to a different client.

        justid: optional boolean, false by default. Return just an array of IDs
        of messages successfully claimed, without returning the actual message

        For more information, see https://redis.io/commands/xclaim
        """
        if not isinstance(min_idle_time, int) or min_idle_time < 0:
            raise DataError("XCLAIM min_idle_time must be a non negative integer")
        if not isinstance(message_ids, (list, tuple)) or not message_ids:
            raise DataError(
                "XCLAIM message_ids must be a non empty list or "
                "tuple of message IDs to claim"
            )

        kwargs = {}
        pieces: list[EncodableT] = [name, groupname, consumername, str(min_idle_time)]
        pieces.extend(list(message_ids))

        if idle is not None:
            if not isinstance(idle, int):
                raise DataError("XCLAIM idle must be an integer")
            pieces.extend((b"IDLE", str(idle)))
        if time is not None:
            if not isinstance(time, int):
                raise DataError("XCLAIM time must be an integer")
            pieces.extend((b"TIME", str(time)))
        if retrycount is not None:
            if not isinstance(retrycount, int):
                raise DataError("XCLAIM retrycount must be an integer")
            pieces.extend((b"RETRYCOUNT", str(retrycount)))

        if force:
            if not isinstance(force, bool):
                raise DataError("XCLAIM force must be a boolean")
            pieces.append(b"FORCE")
        if justid:
            if not isinstance(justid, bool):
                raise DataError("XCLAIM justid must be a boolean")
            pieces.append(b"JUSTID")
            kwargs["parse_justid"] = True
        return self.execute_command("XCLAIM", *pieces, **kwargs)

    @overload
    def xdel(self: SyncClientProtocol, name: KeyT, *ids: StreamIdT) -> int: ...

    @overload
    def xdel(
        self: AsyncClientProtocol, name: KeyT, *ids: StreamIdT
    ) -> Awaitable[int]: ...

    def xdel(self, name: KeyT, *ids: StreamIdT) -> int | Awaitable[int]:
        """
        Deletes one or more messages from a stream.

        Args:
            name: name of the stream.
            *ids: message ids to delete.

        For more information, see https://redis.io/commands/xdel
        """
        return self.execute_command("XDEL", name, *ids)

    @overload
    def xdelex(
        self: SyncClientProtocol,
        name: KeyT,
        *ids: StreamIdT,
        ref_policy: Literal["KEEPREF", "DELREF", "ACKED"] = "KEEPREF",
    ) -> int: ...

    @overload
    def xdelex(
        self: AsyncClientProtocol,
        name: KeyT,
        *ids: StreamIdT,
        ref_policy: Literal["KEEPREF", "DELREF", "ACKED"] = "KEEPREF",
    ) -> Awaitable[int]: ...

    def xdelex(
        self,
        name: KeyT,
        *ids: StreamIdT,
        ref_policy: Literal["KEEPREF", "DELREF", "ACKED"] = "KEEPREF",
    ) -> int | Awaitable[int]:
        """
        Extended version of XDEL that provides more control over how message entries
        are deleted concerning consumer groups.
        """
        if not ids:
            raise DataError("XDELEX requires at least one message ID")

        if ref_policy not in {"KEEPREF", "DELREF", "ACKED"}:
            raise DataError("XDELEX ref_policy must be one of: KEEPREF, DELREF, ACKED")

        pieces = [name, ref_policy, "IDS", len(ids)]
        pieces.extend(ids)
        return self.execute_command("XDELEX", *pieces)

    @overload
    def xgroup_create(
        self: SyncClientProtocol,
        name: KeyT,
        groupname: GroupT,
        id: StreamIdT = "$",
        mkstream: bool = False,
        entries_read: int | None = None,
    ) -> bool: ...

    @overload
    def xgroup_create(
        self: AsyncClientProtocol,
        name: KeyT,
        groupname: GroupT,
        id: StreamIdT = "$",
        mkstream: bool = False,
        entries_read: int | None = None,
    ) -> Awaitable[bool]: ...

    def xgroup_create(
        self,
        name: KeyT,
        groupname: GroupT,
        id: StreamIdT = "$",
        mkstream: bool = False,
        entries_read: int | None = None,
    ) -> bool | Awaitable[bool]:
        """
        Create a new consumer group associated with a stream.
        name: name of the stream.
        groupname: name of the consumer group.
        id: ID of the last item in the stream to consider already delivered.

        For more information, see https://redis.io/commands/xgroup-create
        """
        pieces: list[EncodableT] = ["XGROUP CREATE", name, groupname, id]
        if mkstream:
            pieces.append(b"MKSTREAM")
        if entries_read is not None:
            pieces.extend(["ENTRIESREAD", entries_read])

        return self.execute_command(*pieces)

    @overload
    def xgroup_delconsumer(
        self: SyncClientProtocol, name: KeyT, groupname: GroupT, consumername: ConsumerT
    ) -> int: ...

    @overload
    def xgroup_delconsumer(
        self: AsyncClientProtocol,
        name: KeyT,
        groupname: GroupT,
        consumername: ConsumerT,
    ) -> Awaitable[int]: ...

    def xgroup_delconsumer(
        self, name: KeyT, groupname: GroupT, consumername: ConsumerT
    ) -> int | Awaitable[int]:
        """
        Remove a specific consumer from a consumer group.
        Returns the number of pending messages that the consumer had before it
        was deleted.
        name: name of the stream.
        groupname: name of the consumer group.
        consumername: name of consumer to delete

        For more information, see https://redis.io/commands/xgroup-delconsumer
        """
        return self.execute_command("XGROUP DELCONSUMER", name, groupname, consumername)

    @overload
    def xgroup_destroy(
        self: SyncClientProtocol, name: KeyT, groupname: GroupT
    ) -> bool: ...

    @overload
    def xgroup_destroy(
        self: AsyncClientProtocol, name: KeyT, groupname: GroupT
    ) -> Awaitable[bool]: ...

    def xgroup_destroy(self, name: KeyT, groupname: GroupT) -> bool | Awaitable[bool]:
        """
        Destroy a consumer group.
        name: name of the stream.
        groupname: name of the consumer group.

        For more information, see https://redis.io/commands/xgroup-destroy
        """
        return self.execute_command("XGROUP DESTROY", name, groupname)

    @overload
    def xgroup_createconsumer(
        self: SyncClientProtocol, name: KeyT, groupname: GroupT, consumername: ConsumerT
    ) -> int: ...

    @overload
    def xgroup_createconsumer(
        self: AsyncClientProtocol,
        name: KeyT,
        groupname: GroupT,
        consumername: ConsumerT,
    ) -> Awaitable[int]: ...

    def xgroup_createconsumer(
        self, name: KeyT, groupname: GroupT, consumername: ConsumerT
    ) -> int | Awaitable[int]:
        """
        Consumers in a consumer group are auto-created every time a new
        consumer name is mentioned by some command.
        They can be explicitly created by using this command.
        name: name of the stream.
        groupname: name of the consumer group.
        consumername: name of consumer to create.

        See: https://redis.io/commands/xgroup-createconsumer
        """
        return self.execute_command(
            "XGROUP CREATECONSUMER", name, groupname, consumername
        )

    @overload
    def xgroup_setid(
        self: SyncClientProtocol,
        name: KeyT,
        groupname: GroupT,
        id: StreamIdT,
        entries_read: int | None = None,
    ) -> bool: ...

    @overload
    def xgroup_setid(
        self: AsyncClientProtocol,
        name: KeyT,
        groupname: GroupT,
        id: StreamIdT,
        entries_read: int | None = None,
    ) -> Awaitable[bool]: ...

    def xgroup_setid(
        self,
        name: KeyT,
        groupname: GroupT,
        id: StreamIdT,
        entries_read: int | None = None,
    ) -> bool | Awaitable[bool]:
        """
        Set the consumer group last delivered ID to something else.
        name: name of the stream.
        groupname: name of the consumer group.
        id: ID of the last item in the stream to consider already delivered.

        For more information, see https://redis.io/commands/xgroup-setid
        """
        pieces = [name, groupname, id]
        if entries_read is not None:
            pieces.extend(["ENTRIESREAD", entries_read])
        return self.execute_command("XGROUP SETID", *pieces)

    @overload
    def xinfo_consumers(
        self: SyncClientProtocol, name: KeyT, groupname: GroupT
    ) -> list[dict[str, Any]]: ...

    @overload
    def xinfo_consumers(
        self: AsyncClientProtocol, name: KeyT, groupname: GroupT
    ) -> Awaitable[list[dict[str, Any]]]: ...

    def xinfo_consumers(
        self, name: KeyT, groupname: GroupT
    ) -> list[dict[str, Any]] | Awaitable[list[dict[str, Any]]]:
        """
        Returns general information about the consumers in the group.
        name: name of the stream.
        groupname: name of the consumer group.

        For more information, see https://redis.io/commands/xinfo-consumers
        """
        return self.execute_command("XINFO CONSUMERS", name, groupname)

    @overload
    def xinfo_groups(self: SyncClientProtocol, name: KeyT) -> list[dict[str, Any]]: ...

    @overload
    def xinfo_groups(
        self: AsyncClientProtocol, name: KeyT
    ) -> Awaitable[list[dict[str, Any]]]: ...

    def xinfo_groups(
        self, name: KeyT
    ) -> list[dict[str, Any]] | Awaitable[list[dict[str, Any]]]:
        """
        Returns general information about the consumer groups of the stream.
        name: name of the stream.

        For more information, see https://redis.io/commands/xinfo-groups
        """
        return self.execute_command("XINFO GROUPS", name)

    @overload
    def xinfo_stream(
        self: SyncClientProtocol, name: KeyT, full: bool = False
    ) -> dict[str, Any]: ...

    @overload
    def xinfo_stream(
        self: AsyncClientProtocol, name: KeyT, full: bool = False
    ) -> Awaitable[dict[str, Any]]: ...

    def xinfo_stream(
        self, name: KeyT, full: bool = False
    ) -> dict[str, Any] | Awaitable[dict[str, Any]]:
        """
        Returns general information about the stream.
        name: name of the stream.
        full: optional boolean, false by default. Return full summary

        For more information, see https://redis.io/commands/xinfo-stream
        """
        pieces = [name]
        options = {}
        if full:
            pieces.append(b"FULL")
            options = {"full": full}
        return self.execute_command("XINFO STREAM", *pieces, **options)

    @overload
    def xlen(self: SyncClientProtocol, name: KeyT) -> int: ...

    @overload
    def xlen(self: AsyncClientProtocol, name: KeyT) -> Awaitable[int]: ...

    def xlen(self, name: KeyT) -> int | Awaitable[int]:
        """
        Returns the number of elements in a given stream.

        For more information, see https://redis.io/commands/xlen
        """
        return self.execute_command("XLEN", name, keys=[name])

    @overload
    def xnack(
        self: SyncClientProtocol,
        name: KeyT,
        groupname: GroupT,
        mode: Literal["SILENT", "FAIL", "FATAL"],
        *ids: StreamIdT,
        retrycount: int | None = None,
        force: bool = False,
    ) -> int: ...

    @overload
    def xnack(
        self: AsyncClientProtocol,
        name: KeyT,
        groupname: GroupT,
        mode: Literal["SILENT", "FAIL", "FATAL"],
        *ids: StreamIdT,
        retrycount: int | None = None,
        force: bool = False,
    ) -> Awaitable[int]: ...

    def xnack(
        self,
        name: KeyT,
        groupname: GroupT,
        mode: Literal["SILENT", "FAIL", "FATAL"],
        *ids: StreamIdT,
        retrycount: int | None = None,
        force: bool = False,
    ) -> int | Awaitable[int]:
        """
        Negatively acknowledges one or more messages in a consumer group's
        Pending Entries List (PEL).

        Args:
            name: name of the stream.
            groupname: name of the consumer group.
            mode: the nacking mode. One of SILENT, FAIL, or FATAL.
                SILENT: consumer shutting down; decrements delivery counter.
                FAIL: consumer unable to process; delivery counter unchanged.
                FATAL: invalid/malicious message; delivery counter set to max.
            *ids: one or more message IDs to NACK.
            retrycount: optional integer >= 0. Overrides the mode's implicit
                delivery counter adjustment with an exact value.
            force: if True, creates a new unowned PEL entry for any ID not
                already in the group's PEL.

        Returns:
            The number of messages successfully NACKed.

        For more information, see https://redis.io/commands/xnack
        """
        if not ids:
            raise DataError("XNACK requires at least one message ID")

        if mode not in {"SILENT", "FAIL", "FATAL"}:
            raise DataError("XNACK mode must be one of: SILENT, FAIL, FATAL")

        pieces: list = [name, groupname, mode, "IDS", len(ids)]
        pieces.extend(ids)

        if retrycount is not None:
            if retrycount < 0:
                raise DataError("XNACK retrycount must be >= 0")
            pieces.extend([b"RETRYCOUNT", retrycount])

        if force:
            pieces.append(b"FORCE")

        return self.execute_command("XNACK", *pieces)

    @overload
    def xpending(
        self: SyncClientProtocol, name: KeyT, groupname: GroupT
    ) -> dict[str, Any]: ...

    @overload
    def xpending(
        self: AsyncClientProtocol, name: KeyT, groupname: GroupT
    ) -> Awaitable[dict[str, Any]]: ...

    def xpending(
        self, name: KeyT, groupname: GroupT
    ) -> dict[str, Any] | Awaitable[dict[str, Any]]:
        """
        Returns information about pending messages of a group.
        name: name of the stream.
        groupname: name of the consumer group.

        For more information, see https://redis.io/commands/xpending
        """
        return self.execute_command("XPENDING", name, groupname, keys=[name])

    @overload
    def xpending_range(
        self: SyncClientProtocol,
        name: KeyT,
        groupname: GroupT,
        min: StreamIdT,
        max: StreamIdT,
        count: int,
        consumername: ConsumerT | None = None,
        idle: int | None = None,
    ) -> XPendingRangeResponse: ...

    @overload
    def xpending_range(
        self: AsyncClientProtocol,
        name: KeyT,
        groupname: GroupT,
        min: StreamIdT,
        max: StreamIdT,
        count: int,
        consumername: ConsumerT | None = None,
        idle: int | None = None,
    ) -> Awaitable[XPendingRangeResponse]: ...

    def xpending_range(
        self,
        name: KeyT,
        groupname: GroupT,
        min: StreamIdT,
        max: StreamIdT,
        count: int,
        consumername: ConsumerT | None = None,
        idle: int | None = None,
    ) -> XPendingRangeResponse | Awaitable[XPendingRangeResponse]:
        """
        Returns information about pending messages, in a range.

        name: name of the stream.
        groupname: name of the consumer group.
        idle: available from  version 6.2. filter entries by their
        idle-time, given in milliseconds (optional).
        min: minimum stream ID.
        max: maximum stream ID.
        count: number of messages to return
        consumername: name of a consumer to filter by (optional). Empty names
        are supported; only None omits the consumer filter.
        """
        if {min, max, count} == {None}:
            if idle is not None or consumername is not None:
                raise DataError(
                    "if XPENDING is provided with idle time"
                    " or consumername, it must be provided"
                    " with min, max and count parameters"
                )
            return self.xpending(name, groupname)

        pieces = [name, groupname]
        if min is None or max is None or count is None:
            raise DataError(
                "XPENDING must be provided with min, max "
                "and count parameters, or none of them."
            )
        # idle
        try:
            if int(idle) < 0:
                raise DataError("XPENDING idle must be a integer >= 0")
            pieces.extend(["IDLE", idle])
        except TypeError:
            pass
        # count
        try:
            if int(count) < 0:
                raise DataError("XPENDING count must be a integer >= 0")
            pieces.extend([min, max, count])
        except TypeError:
            pass
        # consumername
        if consumername is not None:
            pieces.append(consumername)

        return self.execute_command("XPENDING", *pieces, parse_detail=True)

    @overload
    def xrange(
        self: SyncClientProtocol,
        name: KeyT,
        min: StreamIdT = "-",
        max: StreamIdT = "+",
        count: int | None = None,
    ) -> StreamRangeResponse | None: ...

    @overload
    def xrange(
        self: AsyncClientProtocol,
        name: KeyT,
        min: StreamIdT = "-",
        max: StreamIdT = "+",
        count: int | None = None,
    ) -> Awaitable[StreamRangeResponse | None]: ...

    def xrange(
        self,
        name: KeyT,
        min: StreamIdT = "-",
        max: StreamIdT = "+",
        count: int | None = None,
    ) -> (StreamRangeResponse | None) | Awaitable[StreamRangeResponse | None]:
        """
        Read stream values within an interval.

        name: name of the stream.

        start: first stream ID. defaults to '-',
               meaning the earliest available.

        finish: last stream ID. defaults to '+',
                meaning the latest available.

        count: if set, only return this many items, beginning with the
               earliest available.

        For more information, see https://redis.io/commands/xrange
        """
        pieces = [min, max]
        if count is not None:
            if not isinstance(count, int) or count < 1:
                raise DataError("XRANGE count must be a positive integer")
            pieces.append(b"COUNT")
            pieces.append(str(count))

        return self.execute_command("XRANGE", name, *pieces, keys=[name])

    @overload
    def xread(
        self: SyncClientProtocol,
        streams: Dict[KeyT, StreamIdT],
        count: int | None = None,
        block: int | None = None,
        max_count: int | None = None,
        max_size: int | None = None,
    ) -> XReadResponse: ...

    @overload
    def xread(
        self: AsyncClientProtocol,
        streams: Dict[KeyT, StreamIdT],
        count: int | None = None,
        block: int | None = None,
        max_count: int | None = None,
        max_size: int | None = None,
    ) -> Awaitable[XReadResponse]: ...

    def xread(
        self,
        streams: Dict[KeyT, StreamIdT],
        count: int | None = None,
        block: int | None = None,
        max_count: int | None = None,
        max_size: int | None = None,
    ) -> XReadResponse | Awaitable[XReadResponse]:
        """
        Block and monitor multiple streams for new data.

        streams: a dict of stream names to stream IDs, where
                   IDs indicate the last ID already seen.

        count: if set, only return this many items per stream, beginning with
               the earliest available.

        block: number of milliseconds to wait, if nothing already present.

        max_count: if set, cap the total number of entries returned across all
                   streams combined. Unlike ``count`` (a per-stream limit),
                   this is a cumulative cap over the whole reply. Must be a
                   positive integer and, when ``count`` is also set, must be
                   greater than or equal to ``count``. Requires Redis >= 8.10.0.

        max_size: if set, a soft cap on the total server reply size in bytes
                  across all streams combined. Measured server-side including
                  protocol overhead, so it is not an exact application-payload
                  size guarantee; a single available entry larger than the cap
                  may still be returned. Must be a positive integer. Requires
                  Redis >= 8.10.0.

        For more information, see https://redis.io/commands/xread
        """
        pieces = []
        if block is not None:
            if not isinstance(block, int) or block < 0:
                raise DataError("XREAD block must be a non-negative integer")
            pieces.append(b"BLOCK")
            pieces.append(str(block))
        if count is not None:
            if not isinstance(count, int) or count < 1:
                raise DataError("XREAD count must be a positive integer")
            pieces.append(b"COUNT")
            pieces.append(str(count))
        if max_count is not None:
            if not isinstance(max_count, int) or max_count < 1:
                raise DataError("XREAD max_count must be a positive integer")
            if count is not None and max_count < count:
                raise DataError(
                    "XREAD max_count must be greater than or equal to count"
                )
            pieces.append(b"MAXCOUNT")
            pieces.append(str(max_count))
        if max_size is not None:
            if not isinstance(max_size, int) or max_size < 1:
                raise DataError("XREAD max_size must be a positive integer")
            pieces.append(b"MAXSIZE")
            pieces.append(str(max_size))
        if not isinstance(streams, dict) or len(streams) == 0:
            raise DataError("XREAD streams must be a non empty dict")
        pieces.append(b"STREAMS")
        keys, values = zip(*streams.items())
        pieces.extend(keys)
        pieces.extend(values)
        response = self.execute_command("XREAD", *pieces, keys=keys)

        if inspect.iscoroutine(response):
            # Async client - wrap in coroutine that awaits and records
            async def _record_and_return():
                actual_response = await response

                await async_record_streaming_lag(response=actual_response)
                return actual_response

            return _record_and_return()
        else:
            # Sync client
            record_streaming_lag_from_response(response=response)
            return response

    @overload
    def xreadgroup(
        self: SyncClientProtocol,
        groupname: str,
        consumername: str,
        streams: Dict[KeyT, StreamIdT],
        count: int | None = None,
        block: int | None = None,
        noack: bool = False,
        claim_min_idle_time: int | None = None,
        max_count: int | None = None,
        max_size: int | None = None,
    ) -> XReadGroupResponse: ...

    @overload
    def xreadgroup(
        self: AsyncClientProtocol,
        groupname: str,
        consumername: str,
        streams: Dict[KeyT, StreamIdT],
        count: int | None = None,
        block: int | None = None,
        noack: bool = False,
        claim_min_idle_time: int | None = None,
        max_count: int | None = None,
        max_size: int | None = None,
    ) -> Awaitable[XReadGroupResponse]: ...

    def xreadgroup(
        self,
        groupname: str,
        consumername: str,
        streams: Dict[KeyT, StreamIdT],
        count: int | None = None,
        block: int | None = None,
        noack: bool = False,
        claim_min_idle_time: int | None = None,
        max_count: int | None = None,
        max_size: int | None = None,
    ) -> XReadGroupResponse | Awaitable[XReadGroupResponse]:
        """
        Read from a stream via a consumer group.

        groupname: name of the consumer group.

        consumername: name of the requesting consumer.

        streams: a dict of stream names to stream IDs, where
               IDs indicate the last ID already seen.

        count: if set, only return this many items per stream, beginning with
               the earliest available.

        block: number of milliseconds to wait, if nothing already present.
        noack: do not add messages to the PEL

        claim_min_idle_time: accepts an integer type and represents a
                             time interval in milliseconds

        max_count: if set, cap the total number of entries returned across all
                   streams combined. Unlike ``count`` (a per-stream limit),
                   this is a cumulative cap over the whole reply. Must be a
                   positive integer and, when ``count`` is also set, must be
                   greater than or equal to ``count``. Requires Redis 8.10.0.

        max_size: if set, a soft cap on the total server reply size in bytes
                  across all streams combined. Measured server-side including
                  protocol overhead, so it is not an exact application-payload
                  size guarantee; a single available entry larger than the cap
                  may still be returned. Must be a positive integer. Requires
                  Redis 8.10.0.

        For more information, see https://redis.io/commands/xreadgroup
        """
        options = {}
        pieces: list[EncodableT] = [b"GROUP", groupname, consumername]
        if count is not None:
            if not isinstance(count, int) or count < 1:
                raise DataError("XREADGROUP count must be a positive integer")
            pieces.append(b"COUNT")
            pieces.append(str(count))
        if max_count is not None:
            if not isinstance(max_count, int) or max_count < 1:
                raise DataError("XREADGROUP max_count must be a positive integer")
            if count is not None and max_count < count:
                raise DataError(
                    "XREADGROUP max_count must be greater than or equal to count"
                )
            pieces.append(b"MAXCOUNT")
            pieces.append(str(max_count))
        if max_size is not None:
            if not isinstance(max_size, int) or max_size < 1:
                raise DataError("XREADGROUP max_size must be a positive integer")
            pieces.append(b"MAXSIZE")
            pieces.append(str(max_size))
        if block is not None:
            if not isinstance(block, int) or block < 0:
                raise DataError("XREADGROUP block must be a non-negative integer")
            pieces.append(b"BLOCK")
            pieces.append(str(block))
        if noack:
            pieces.append(b"NOACK")
        if claim_min_idle_time is not None:
            if not isinstance(claim_min_idle_time, int) or claim_min_idle_time < 0:
                raise DataError(
                    "XREADGROUP claim_min_idle_time must be a non-negative integer"
                )
            pieces.append(b"CLAIM")
            pieces.append(claim_min_idle_time)
            options["claim_min_idle_time"] = claim_min_idle_time
        if not isinstance(streams, dict) or len(streams) == 0:
            raise DataError("XREADGROUP streams must be a non empty dict")
        pieces.append(b"STREAMS")
        pieces.extend(streams.keys())
        pieces.extend(streams.values())
        response = self.execute_command("XREADGROUP", *pieces, **options)

        if inspect.iscoroutine(response):
            # Async client - wrap in coroutine that awaits and records
            async def _record_and_return():
                actual_response = await response

                await async_record_streaming_lag(
                    response=actual_response,
                    consumer_group=groupname,
                )
                return actual_response

            return _record_and_return()
        else:
            # Sync client
            record_streaming_lag_from_response(
                response=response,
                consumer_group=groupname,
            )
            return response

    @overload
    def xrevrange(
        self: SyncClientProtocol,
        name: KeyT,
        max: StreamIdT = "+",
        min: StreamIdT = "-",
        count: int | None = None,
    ) -> StreamRangeResponse | None: ...

    @overload
    def xrevrange(
        self: AsyncClientProtocol,
        name: KeyT,
        max: StreamIdT = "+",
        min: StreamIdT = "-",
        count: int | None = None,
    ) -> Awaitable[StreamRangeResponse | None]: ...

    def xrevrange(
        self,
        name: KeyT,
        max: StreamIdT = "+",
        min: StreamIdT = "-",
        count: int | None = None,
    ) -> (StreamRangeResponse | None) | Awaitable[StreamRangeResponse | None]:
        """
        Read stream values within an interval, in reverse order.

        name: name of the stream

        start: first stream ID. defaults to '+',
               meaning the latest available.

        finish: last stream ID. defaults to '-',
                meaning the earliest available.

        count: if set, only return this many items, beginning with the
               latest available.

        For more information, see https://redis.io/commands/xrevrange
        """
        pieces: list[EncodableT] = [max, min]
        if count is not None:
            if not isinstance(count, int) or count < 1:
                raise DataError("XREVRANGE count must be a positive integer")
            pieces.append(b"COUNT")
            pieces.append(str(count))

        return self.execute_command("XREVRANGE", name, *pieces, keys=[name])

    @overload
    def xtrim(
        self: SyncClientProtocol,
        name: KeyT,
        maxlen: int | None = None,
        approximate: bool = True,
        minid: StreamIdT | None = None,
        limit: int | None = None,
        ref_policy: Literal["KEEPREF", "DELREF", "ACKED"] | None = None,
    ) -> int: ...

    @overload
    def xtrim(
        self: AsyncClientProtocol,
        name: KeyT,
        maxlen: int | None = None,
        approximate: bool = True,
        minid: StreamIdT | None = None,
        limit: int | None = None,
        ref_policy: Literal["KEEPREF", "DELREF", "ACKED"] | None = None,
    ) -> Awaitable[int]: ...

    def xtrim(
        self,
        name: KeyT,
        maxlen: int | None = None,
        approximate: bool = True,
        minid: StreamIdT | None = None,
        limit: int | None = None,
        ref_policy: Literal["KEEPREF", "DELREF", "ACKED"] | None = None,
    ) -> int | Awaitable[int]:
        """
        Trims old messages from a stream.
        name: name of the stream.
        maxlen: truncate old stream messages beyond this size
        Can't be specified with minid.
        approximate: actual stream length may be slightly more than maxlen
        minid: the minimum id in the stream to query
        Can't be specified with maxlen.
        limit: specifies the maximum number of entries to retrieve
        ref_policy: optional reference policy for consumer groups:
            - KEEPREF (default): Trims entries but preserves references in consumer groups' PEL
            - DELREF: Trims entries and removes all references from consumer groups' PEL
            - ACKED: Only trims entries that were read and acknowledged by all consumer groups

        For more information, see https://redis.io/commands/xtrim
        """
        pieces: list[EncodableT] = []
        if maxlen is not None and minid is not None:
            raise DataError("Only one of ``maxlen`` or ``minid`` may be specified")

        if maxlen is None and minid is None:
            raise DataError("One of ``maxlen`` or ``minid`` must be specified")

        if ref_policy is not None and ref_policy not in {"KEEPREF", "DELREF", "ACKED"}:
            raise DataError("XTRIM ref_policy must be one of: KEEPREF, DELREF, ACKED")

        if maxlen is not None:
            pieces.append(b"MAXLEN")
        if minid is not None:
            pieces.append(b"MINID")
        if approximate:
            pieces.append(b"~")
        if maxlen is not None:
            pieces.append(maxlen)
        if minid is not None:
            pieces.append(minid)
        if limit is not None:
            pieces.append(b"LIMIT")
            pieces.append(limit)
        if ref_policy is not None:
            pieces.append(ref_policy)

        return self.execute_command("XTRIM", name, *pieces)


AsyncStreamCommands = StreamCommands


class SortedSetCommands(CommandsProtocol):
    """
    Redis commands for Sorted Sets data type.
    see: https://redis.io/topics/data-types-intro#redis-sorted-sets
    """

    def _zset_decode_responses(self) -> bool:
        # Hosts that don't define get_connection_kwargs() keep the undecoded
        # (bytes) legacy score text.
        get_connection_kwargs = getattr(self, "get_connection_kwargs", None)
        if get_connection_kwargs is None:
            return False
        return bool(get_connection_kwargs().get("decode_responses", False))

    @overload
    def zadd(
        self: SyncClientProtocol,
        name: KeyT,
        mapping: Mapping[AnyKeyT, EncodableT],
        nx: bool = False,
        xx: bool = False,
        ch: bool = False,
        incr: bool = False,
        gt: bool = False,
        lt: bool = False,
    ) -> int | float | None: ...

    @overload
    def zadd(
        self: AsyncClientProtocol,
        name: KeyT,
        mapping: Mapping[AnyKeyT, EncodableT],
        nx: bool = False,
        xx: bool = False,
        ch: bool = False,
        incr: bool = False,
        gt: bool = False,
        lt: bool = False,
    ) -> Awaitable[int | float | None]: ...

    def zadd(
        self,
        name: KeyT,
        mapping: Mapping[AnyKeyT, EncodableT],
        nx: bool = False,
        xx: bool = False,
        ch: bool = False,
        incr: bool = False,
        gt: bool = False,
        lt: bool = False,
    ) -> (int | float | None) | Awaitable[int | float | None]:
        """
        Set any number of element-name, score pairs to the key ``name``. Pairs
        are specified as a dict of element-names keys to score values.

        ``nx`` forces ZADD to only create new elements and not to update
        scores for elements that already exist.

        ``xx`` forces ZADD to only update scores of elements that already
        exist. New elements will not be added.

        ``ch`` modifies the return value to be the numbers of elements changed.
        Changed elements include new elements that were added and elements
        whose scores changed.

        ``incr`` modifies ZADD to behave like ZINCRBY. In this mode only a
        single element/score pair can be specified and the score is the amount
        the existing score will be incremented by. When using this mode the
        return value of ZADD will be the new score of the element.

        ``lt`` only updates existing elements if the new score is less than
        the current score. This flag doesn't prevent adding new elements.

        ``gt`` only updates existing elements if the new score is greater than
        the current score. This flag doesn't prevent adding new elements.

        The return value of ZADD varies based on the mode specified. With no
        options, ZADD returns the number of new elements added to the sorted
        set.

        ``nx``, ``lt``, and ``gt`` are mutually exclusive options.

        See: https://redis.io/commands/ZADD
        """
        if not mapping:
            raise DataError("ZADD requires at least one element/score pair")
        if nx and xx:
            raise DataError("ZADD allows either 'nx' or 'xx', not both")
        if gt and lt:
            raise DataError("ZADD allows either 'gt' or 'lt', not both")
        if incr and len(mapping) != 1:
            raise DataError(
                "ZADD option 'incr' only works when passing a single element/score pair"
            )
        if nx and (gt or lt):
            raise DataError("Only one of 'nx', 'gt', or 'lt' may be defined.")

        pieces: list[EncodableT] = []
        options = {}
        if nx:
            pieces.append(b"NX")
        if xx:
            pieces.append(b"XX")
        if ch:
            pieces.append(b"CH")
        if incr:
            pieces.append(b"INCR")
            options["as_score"] = True
        if gt:
            pieces.append(b"GT")
        if lt:
            pieces.append(b"LT")
        for pair in mapping.items():
            pieces.append(pair[1])
            pieces.append(pair[0])
        return self.execute_command("ZADD", name, *pieces, **options)

    @overload
    def zcard(self: SyncClientProtocol, name: KeyT) -> int: ...

    @overload
    def zcard(self: AsyncClientProtocol, name: KeyT) -> Awaitable[int]: ...

    def zcard(self, name: KeyT) -> int | Awaitable[int]:
        """
        Return the number of elements in the sorted set ``name``

        For more information, see https://redis.io/commands/zcard
        """
        return self.execute_command("ZCARD", name, keys=[name])

    @overload
    def zcount(
        self: SyncClientProtocol, name: KeyT, min: ZScoreBoundT, max: ZScoreBoundT
    ) -> int: ...

    @overload
    def zcount(
        self: AsyncClientProtocol, name: KeyT, min: ZScoreBoundT, max: ZScoreBoundT
    ) -> Awaitable[int]: ...

    def zcount(
        self, name: KeyT, min: ZScoreBoundT, max: ZScoreBoundT
    ) -> int | Awaitable[int]:
        """
        Returns the number of elements in the sorted set at key ``name`` with
        a score between ``min`` and ``max``.

        For more information, see https://redis.io/commands/zcount
        """
        return self.execute_command("ZCOUNT", name, min, max, keys=[name])

    @overload
    def zdiff(
        self: SyncClientProtocol, keys: KeysT, withscores: bool = False
    ) -> ZSetRangeResponse: ...

    @overload
    def zdiff(
        self: AsyncClientProtocol, keys: KeysT, withscores: bool = False
    ) -> Awaitable[ZSetRangeResponse]: ...

    def zdiff(
        self, keys: KeysT, withscores: bool = False
    ) -> ZSetRangeResponse | Awaitable[ZSetRangeResponse]:
        """
        Returns the difference between the first and all successive input
        sorted sets provided in ``keys``.

        For more information, see https://redis.io/commands/zdiff
        """
        pieces = [len(keys), *keys]
        if withscores:
            pieces.append("WITHSCORES")
        return self.execute_command("ZDIFF", *pieces, keys=keys, withscores=withscores)

    @overload
    def zdiffstore(self: SyncClientProtocol, dest: KeyT, keys: KeysT) -> int: ...

    @overload
    def zdiffstore(
        self: AsyncClientProtocol, dest: KeyT, keys: KeysT
    ) -> Awaitable[int]: ...

    def zdiffstore(self, dest: KeyT, keys: KeysT) -> int | Awaitable[int]:
        """
        Computes the difference between the first and all successive input
        sorted sets provided in ``keys`` and stores the result in ``dest``.

        For more information, see https://redis.io/commands/zdiffstore
        """
        pieces = [len(keys), *keys]
        return self.execute_command("ZDIFFSTORE", dest, *pieces)

    @overload
    def zincrby(
        self: SyncClientProtocol, name: KeyT, amount: float, value: EncodableT
    ) -> float | None: ...

    @overload
    def zincrby(
        self: AsyncClientProtocol, name: KeyT, amount: float, value: EncodableT
    ) -> Awaitable[float | None]: ...

    def zincrby(self, name: KeyT, amount: float, value: EncodableT) -> (
        float | None
    ) | Awaitable[float | None]:
        """
        Increment the score of ``value`` in sorted set ``name`` by ``amount``

        For more information, see https://redis.io/commands/zincrby
        """
        return self.execute_command("ZINCRBY", name, amount, value)

    @overload
    def zinter(
        self: SyncClientProtocol,
        keys: KeysT,
        aggregate: str | None = None,
        withscores: bool = False,
    ) -> ZSetRangeResponse: ...

    @overload
    def zinter(
        self: AsyncClientProtocol,
        keys: KeysT,
        aggregate: str | None = None,
        withscores: bool = False,
    ) -> Awaitable[ZSetRangeResponse]: ...

    def zinter(
        self, keys: KeysT, aggregate: str | None = None, withscores: bool = False
    ) -> ZSetRangeResponse | Awaitable[ZSetRangeResponse]:
        """
        Return the intersect of multiple sorted sets specified by ``keys``.

        With the ``aggregate`` option, it is possible to specify how the
        results of the intersection are aggregated. Available aggregation
        modes:

        - ``SUM`` (default): the score of an element is summed across the
          inputs where it exists.
          Score = SUM(score₁×weight₁, score₂×weight₂, ...)
        - ``MIN``: the resulting set will contain the minimum score of an
          element across the inputs where it exists.
          Score = MIN(score₁×weight₁, score₂×weight₂, ...)
        - ``MAX``: the resulting set will contain the maximum score of an
          element across the inputs where it exists.
          Score = MAX(score₁×weight₁, score₂×weight₂, ...)
        - ``COUNT``: ignores the original scores and counts weighted set
          membership. Each element's score is the sum of the weights of
          the input sets that contain it.
          Score = SUM(weight₁, weight₂, ...) for sets containing the element.
          When all weights are 1 (default), the score equals the number
          of input sets containing the element.

        For more information, see https://redis.io/commands/zinter
        """
        return self._zaggregate("ZINTER", None, keys, aggregate, withscores=withscores)

    @overload
    def zinterstore(
        self,
        dest: KeyT,
        keys: Sequence[KeyT] | Mapping[AnyKeyT, float],
        aggregate: str | None = None,
    ) -> int: ...

    @overload
    def zinterstore(
        self: AsyncClientProtocol,
        dest: KeyT,
        keys: Sequence[KeyT] | Mapping[AnyKeyT, float],
        aggregate: str | None = None,
    ) -> Awaitable[int]: ...

    def zinterstore(
        self,
        dest: KeyT,
        keys: Sequence[KeyT] | Mapping[AnyKeyT, float],
        aggregate: str | None = None,
    ) -> int | Awaitable[int]:
        """
        Intersect multiple sorted sets specified by ``keys`` into a new
        sorted set, ``dest``. Scores in the destination will be aggregated
        based on the ``aggregate``.

        Available aggregation modes:

        - ``SUM`` (default): the score of an element is summed across the
          inputs where it exists.
          Score = SUM(score₁×weight₁, score₂×weight₂, ...)
        - ``MIN``: the resulting set will contain the minimum score of an
          element across the inputs where it exists.
          Score = MIN(score₁×weight₁, score₂×weight₂, ...)
        - ``MAX``: the resulting set will contain the maximum score of an
          element across the inputs where it exists.
          Score = MAX(score₁×weight₁, score₂×weight₂, ...)
        - ``COUNT``: ignores the original scores and counts weighted set
          membership. Each element's score is the sum of the weights of
          the input sets that contain it.
          Score = SUM(weight₁, weight₂, ...) for sets containing the element.
          When all weights are 1 (default), the score equals the number
          of input sets containing the element.

        For more information, see https://redis.io/commands/zinterstore
        """
        return self._zaggregate("ZINTERSTORE", dest, keys, aggregate)

    @overload
    def zintercard(
        self: SyncClientProtocol, numkeys: int, keys: List[str], limit: int = 0
    ) -> int: ...

    @overload
    def zintercard(
        self: AsyncClientProtocol, numkeys: int, keys: List[str], limit: int = 0
    ) -> Awaitable[int]: ...

    def zintercard(
        self, numkeys: int, keys: List[str], limit: int = 0
    ) -> int | Awaitable[int]:
        """
        Return the cardinality of the intersect of multiple sorted sets
        specified by ``keys``.
        When LIMIT provided (defaults to 0 and means unlimited), if the intersection
        cardinality reaches limit partway through the computation, the algorithm will
        exit and yield limit as the cardinality

        For more information, see https://redis.io/commands/zintercard
        """
        args = [numkeys, *keys, "LIMIT", limit]
        return self.execute_command("ZINTERCARD", *args, keys=keys)

    @overload
    def zlexcount(
        self: SyncClientProtocol, name: KeyT, min: EncodableT, max: EncodableT
    ) -> int: ...

    @overload
    def zlexcount(
        self: AsyncClientProtocol, name: KeyT, min: EncodableT, max: EncodableT
    ) -> Awaitable[int]: ...

    def zlexcount(
        self, name: KeyT, min: EncodableT, max: EncodableT
    ) -> int | Awaitable[int]:
        """
        Return the number of items in the sorted set ``name`` between the
        lexicographical range ``min`` and ``max``.

        For more information, see https://redis.io/commands/zlexcount
        """
        return self.execute_command("ZLEXCOUNT", name, min, max, keys=[name])

    @overload
    def zpopmax(
        self: SyncClientProtocol, name: KeyT, count: int | None = None
    ) -> ZSetRangeResponse: ...

    @overload
    def zpopmax(
        self: AsyncClientProtocol, name: KeyT, count: int | None = None
    ) -> Awaitable[ZSetRangeResponse]: ...

    def zpopmax(
        self, name: KeyT, count: int | None = None
    ) -> ZSetRangeResponse | Awaitable[ZSetRangeResponse]:
        """
        Remove and return up to ``count`` members with the highest scores
        from the sorted set ``name``.

        For more information, see https://redis.io/commands/zpopmax
        """
        args = (count is not None) and [count] or []
        options = {"withscores": True}
        return self.execute_command("ZPOPMAX", name, *args, **options)

    @overload
    def zpopmin(
        self: SyncClientProtocol, name: KeyT, count: int | None = None
    ) -> ZSetRangeResponse: ...

    @overload
    def zpopmin(
        self: AsyncClientProtocol, name: KeyT, count: int | None = None
    ) -> Awaitable[ZSetRangeResponse]: ...

    def zpopmin(
        self, name: KeyT, count: int | None = None
    ) -> ZSetRangeResponse | Awaitable[ZSetRangeResponse]:
        """
        Remove and return up to ``count`` members with the lowest scores
        from the sorted set ``name``.

        For more information, see https://redis.io/commands/zpopmin
        """
        args = (count is not None) and [count] or []
        options = {"withscores": True}
        return self.execute_command("ZPOPMIN", name, *args, **options)

    @overload
    def zrandmember(
        self: SyncClientProtocol,
        key: KeyT,
        count: int | None = None,
        withscores: bool = False,
    ) -> ZRandMemberResponse: ...

    @overload
    def zrandmember(
        self: AsyncClientProtocol,
        key: KeyT,
        count: int | None = None,
        withscores: bool = False,
    ) -> Awaitable[ZRandMemberResponse]: ...

    def zrandmember(
        self, key: KeyT, count: int | None = None, withscores: bool = False
    ) -> ZRandMemberResponse | Awaitable[ZRandMemberResponse]:
        """
        Return a random element from the sorted set value stored at key.

        ``count`` if the argument is positive, return an array of distinct
        fields. If called with a negative count, the behavior changes and
        the command is allowed to return the same field multiple times.
        In this case, the number of returned fields is the absolute value
        of the specified count.

        ``withscores`` The optional WITHSCORES modifier changes the reply so it
        includes the respective scores of the randomly selected elements from
        the sorted set.

        For more information, see https://redis.io/commands/zrandmember
        """
        params = []
        if count is not None:
            params.append(count)
        if withscores:
            params.append("WITHSCORES")

        return self.execute_command("ZRANDMEMBER", key, *params, withscores=withscores)

    @overload
    def bzpopmax(
        self: SyncClientProtocol, keys: KeysT, timeout: TimeoutSecT = 0
    ) -> BlockingZSetPopResponse: ...

    @overload
    def bzpopmax(
        self: AsyncClientProtocol, keys: KeysT, timeout: TimeoutSecT = 0
    ) -> Awaitable[BlockingZSetPopResponse]: ...

    def bzpopmax(
        self, keys: KeysT, timeout: TimeoutSecT = 0
    ) -> BlockingZSetPopResponse | Awaitable[BlockingZSetPopResponse]:
        """
        ZPOPMAX a value off of the first non-empty sorted set
        named in the ``keys`` list.

        If none of the sorted sets in ``keys`` has a value to ZPOPMAX,
        then block for ``timeout`` seconds, or until a member gets added
        to one of the sorted sets.

        If timeout is 0, then block indefinitely.

        For more information, see https://redis.io/commands/bzpopmax
        """
        if timeout is None:
            timeout = 0
        keys = list_or_args(keys, None)
        keys.append(timeout)
        return self.execute_command("BZPOPMAX", *keys)

    @overload
    def bzpopmin(
        self: SyncClientProtocol, keys: KeysT, timeout: TimeoutSecT = 0
    ) -> BlockingZSetPopResponse: ...

    @overload
    def bzpopmin(
        self: AsyncClientProtocol, keys: KeysT, timeout: TimeoutSecT = 0
    ) -> Awaitable[BlockingZSetPopResponse]: ...

    def bzpopmin(
        self, keys: KeysT, timeout: TimeoutSecT = 0
    ) -> BlockingZSetPopResponse | Awaitable[BlockingZSetPopResponse]:
        """
        ZPOPMIN a value off of the first non-empty sorted set
        named in the ``keys`` list.

        If none of the sorted sets in ``keys`` has a value to ZPOPMIN,
        then block for ``timeout`` seconds, or until a member gets added
        to one of the sorted sets.

        If timeout is 0, then block indefinitely.

        For more information, see https://redis.io/commands/bzpopmin
        """
        if timeout is None:
            timeout = 0
        keys: list[EncodableT] = list_or_args(keys, None)
        keys.append(timeout)
        return self.execute_command("BZPOPMIN", *keys)

    @overload
    def zmpop(
        self: SyncClientProtocol,
        num_keys: int,
        keys: List[str],
        min: bool | None = False,
        max: bool | None = False,
        count: int | None = 1,
    ) -> ZMPopResponse: ...

    @overload
    def zmpop(
        self: AsyncClientProtocol,
        num_keys: int,
        keys: List[str],
        min: bool | None = False,
        max: bool | None = False,
        count: int | None = 1,
    ) -> Awaitable[ZMPopResponse]: ...

    def zmpop(
        self,
        num_keys: int,
        keys: List[str],
        min: bool | None = False,
        max: bool | None = False,
        count: int | None = 1,
    ) -> ZMPopResponse | Awaitable[ZMPopResponse]:
        """
        Pop ``count`` values (default 1) off of the first non-empty sorted set
        named in the ``keys`` list.
        For more information, see https://redis.io/commands/zmpop
        """
        args = [num_keys] + keys
        if (min and max) or (not min and not max):
            raise DataError
        elif min:
            args.append("MIN")
        else:
            args.append("MAX")
        if count is not None and count != 1:
            args.extend(["COUNT", count])

        return self.execute_command("ZMPOP", *args)

    @overload
    def bzmpop(
        self: SyncClientProtocol,
        timeout: float,
        numkeys: int,
        keys: List[str],
        min: bool | None = False,
        max: bool | None = False,
        count: int | None = 1,
    ) -> ZMPopResponse: ...

    @overload
    def bzmpop(
        self: AsyncClientProtocol,
        timeout: float,
        numkeys: int,
        keys: List[str],
        min: bool | None = False,
        max: bool | None = False,
        count: int | None = 1,
    ) -> Awaitable[ZMPopResponse]: ...

    def bzmpop(
        self,
        timeout: float,
        numkeys: int,
        keys: List[str],
        min: bool | None = False,
        max: bool | None = False,
        count: int | None = 1,
    ) -> ZMPopResponse | Awaitable[ZMPopResponse]:
        """
        Pop ``count`` values (default 1) off of the first non-empty sorted set
        named in the ``keys`` list.

        If none of the sorted sets in ``keys`` has a value to pop,
        then block for ``timeout`` seconds, or until a member gets added
        to one of the sorted sets.

        If timeout is 0, then block indefinitely.

        For more information, see https://redis.io/commands/bzmpop
        """
        args = [timeout, numkeys, *keys]
        if (min and max) or (not min and not max):
            raise DataError("Either min or max, but not both must be set")
        elif min:
            args.append("MIN")
        else:
            args.append("MAX")
        if count is not None:
            args.extend(["COUNT", count])

        return self.execute_command("BZMPOP", *args)

    def _zrange(
        self,
        command,
        dest: Union[KeyT, None],
        name: KeyT,
        start: EncodableT,
        end: EncodableT,
        desc: bool = False,
        byscore: bool = False,
        bylex: bool = False,
        withscores: bool = False,
        score_cast_func: Union[type, Callable, None] = float,
        offset: Optional[int] = None,
        num: Optional[int] = None,
    ) -> ResponseT:
        if byscore and bylex:
            raise DataError("``byscore`` and ``bylex`` can not be specified together.")
        if (offset is not None and num is None) or (num is not None and offset is None):
            raise DataError("``offset`` and ``num`` must both be specified.")
        if bylex and withscores:
            raise DataError(
                "``withscores`` not supported in combination with ``bylex``."
            )
        pieces = [command]
        if dest is not None:
            pieces.append(dest)
        pieces.extend([name, start, end])
        if byscore:
            pieces.append("BYSCORE")
        if bylex:
            pieces.append("BYLEX")
        if desc:
            pieces.append("REV")
        if offset is not None and num is not None:
            pieces.extend(["LIMIT", offset, num])
        if withscores:
            pieces.append("WITHSCORES")
        options = {"withscores": withscores, "score_cast_func": score_cast_func}
        options["keys"] = [name]
        return self.execute_command(*pieces, **options)

    @overload
    def zrange(
        self: SyncClientProtocol,
        name: KeyT,
        start: EncodableT,
        end: EncodableT,
        desc: bool = False,
        withscores: bool = False,
        score_cast_func: type | Callable = float,
        byscore: bool = False,
        bylex: bool = False,
        offset: int | None = None,
        num: int | None = None,
        use_rev: bool = False,
    ) -> ZSetRangeResponse: ...

    @overload
    def zrange(
        self: AsyncClientProtocol,
        name: KeyT,
        start: EncodableT,
        end: EncodableT,
        desc: bool = False,
        withscores: bool = False,
        score_cast_func: type | Callable = float,
        byscore: bool = False,
        bylex: bool = False,
        offset: int | None = None,
        num: int | None = None,
        use_rev: bool = False,
    ) -> Awaitable[ZSetRangeResponse]: ...

    def zrange(
        self,
        name: KeyT,
        start: EncodableT,
        end: EncodableT,
        desc: bool = False,
        withscores: bool = False,
        score_cast_func: type | Callable = float,
        byscore: bool = False,
        bylex: bool = False,
        offset: int | None = None,
        num: int | None = None,
        use_rev: bool = False,
    ) -> ZSetRangeResponse | Awaitable[ZSetRangeResponse]:
        """
        Return a range of values from sorted set ``name`` between
        ``start`` and ``end`` sorted in ascending order.

        ``start`` and ``end`` can be negative, indicating the end of the range.

        ``desc`` a boolean indicating whether to sort the results in reversed
        order.

        ``use_rev`` only affects the rank-based reverse path: when True
        together with ``desc``, it sends ``ZRANGE ... REV`` instead of the
        deprecated ``ZREVRANGE``. It is ignored for the byscore, bylex,
        and offset/num paths, which already send ``ZRANGE ... REV``.
        It defaults to False so callers on servers older than Redis 6.2
        keep the ``ZREVRANGE`` fallback.

        ``withscores`` indicates to return the scores along with the values.
        The return type is a list of (value, score) pairs.

        ``score_cast_func`` a callable used to cast the score return value.

        ``byscore`` when set to True, returns the range of elements from the
        sorted set having scores equal or between ``start`` and ``end``.

        ``bylex`` when set to True, returns the range of elements from the
        sorted set between the ``start`` and ``end`` lexicographical closed
        range intervals.
        Valid ``start`` and ``end`` must start with ( or [, in order to specify
        whether the range interval is exclusive or inclusive, respectively.

        ``offset`` and ``num`` are specified, then return a slice of the range.
        Can't be provided when using ``bylex``.

        For more information, see https://redis.io/commands/zrange
        """
        # Need to support ``desc`` also when using old redis version
        # because it was supported in 3.5.3 (of redis-py)
        if (
            not byscore
            and not bylex
            and (offset is None and num is None)
            and desc
            and not use_rev
        ):
            return self.zrevrange(name, start, end, withscores, score_cast_func)

        return self._zrange(
            "ZRANGE",
            None,
            name,
            start,
            end,
            desc,
            byscore,
            bylex,
            withscores,
            score_cast_func,
            offset,
            num,
        )

    @overload
    def zrevrange(
        self: SyncClientProtocol,
        name: KeyT,
        start: int,
        end: int,
        withscores: bool = False,
        score_cast_func: type | Callable = float,
    ) -> ZSetRangeResponse: ...

    @overload
    def zrevrange(
        self: AsyncClientProtocol,
        name: KeyT,
        start: int,
        end: int,
        withscores: bool = False,
        score_cast_func: type | Callable = float,
    ) -> Awaitable[ZSetRangeResponse]: ...

    def zrevrange(
        self,
        name: KeyT,
        start: int,
        end: int,
        withscores: bool = False,
        score_cast_func: type | Callable = float,
    ) -> ZSetRangeResponse | Awaitable[ZSetRangeResponse]:
        """
        Return a range of values from sorted set ``name`` between
        ``start`` and ``end`` sorted in descending order.

        ``start`` and ``end`` can be negative, indicating the end of the range.

        ``withscores`` indicates to return the scores along with the values
        The return type is a list of (value, score) pairs

        ``score_cast_func`` a callable used to cast the score return value

        For more information, see https://redis.io/commands/zrevrange
        """
        pieces = ["ZREVRANGE", name, start, end]
        if withscores:
            pieces.append(b"WITHSCORES")
        options = {"withscores": withscores, "score_cast_func": score_cast_func}
        options["keys"] = [name]
        return self.execute_command(*pieces, **options)

    @overload
    def zrangestore(
        self: SyncClientProtocol,
        dest: KeyT,
        name: KeyT,
        start: EncodableT,
        end: EncodableT,
        byscore: bool = False,
        bylex: bool = False,
        desc: bool = False,
        offset: int | None = None,
        num: int | None = None,
    ) -> int: ...

    @overload
    def zrangestore(
        self: AsyncClientProtocol,
        dest: KeyT,
        name: KeyT,
        start: EncodableT,
        end: EncodableT,
        byscore: bool = False,
        bylex: bool = False,
        desc: bool = False,
        offset: int | None = None,
        num: int | None = None,
    ) -> Awaitable[int]: ...

    def zrangestore(
        self,
        dest: KeyT,
        name: KeyT,
        start: EncodableT,
        end: EncodableT,
        byscore: bool = False,
        bylex: bool = False,
        desc: bool = False,
        offset: int | None = None,
        num: int | None = None,
    ) -> int | Awaitable[int]:
        """
        Stores in ``dest`` the result of a range of values from sorted set
        ``name`` between ``start`` and ``end`` sorted in ascending order.

        ``start`` and ``end`` can be negative, indicating the end of the range.

        ``byscore`` when set to True, returns the range of elements from the
        sorted set having scores equal or between ``start`` and ``end``.

        ``bylex`` when set to True, returns the range of elements from the
        sorted set between the ``start`` and ``end`` lexicographical closed
        range intervals.
        Valid ``start`` and ``end`` must start with ( or [, in order to specify
        whether the range interval is exclusive or inclusive, respectively.

        ``desc`` a boolean indicating whether to sort the results in reversed
        order.

        ``offset`` and ``num`` are specified, then return a slice of the range.
        Can't be provided when using ``bylex``.

        For more information, see https://redis.io/commands/zrangestore
        """
        return self._zrange(
            "ZRANGESTORE",
            dest,
            name,
            start,
            end,
            desc,
            byscore,
            bylex,
            False,
            None,
            offset,
            num,
        )

    @overload
    def zrangebylex(
        self: SyncClientProtocol,
        name: KeyT,
        min: EncodableT,
        max: EncodableT,
        start: int | None = None,
        num: int | None = None,
    ) -> list[bytes | str]: ...

    @overload
    def zrangebylex(
        self: AsyncClientProtocol,
        name: KeyT,
        min: EncodableT,
        max: EncodableT,
        start: int | None = None,
        num: int | None = None,
    ) -> Awaitable[list[bytes | str]]: ...

    def zrangebylex(
        self,
        name: KeyT,
        min: EncodableT,
        max: EncodableT,
        start: int | None = None,
        num: int | None = None,
    ) -> list[bytes | str] | Awaitable[list[bytes | str]]:
        """
        Return the lexicographical range of values from sorted set ``name``
        between ``min`` and ``max``.

        If ``start`` and ``num`` are specified, then return a slice of the
        range.

        For more information, see https://redis.io/commands/zrangebylex
        """
        if (start is not None and num is None) or (num is not None and start is None):
            raise DataError("``start`` and ``num`` must both be specified")
        pieces = ["ZRANGEBYLEX", name, min, max]
        if start is not None and num is not None:
            pieces.extend([b"LIMIT", start, num])
        return self.execute_command(*pieces, keys=[name])

    @overload
    def zrevrangebylex(
        self: SyncClientProtocol,
        name: KeyT,
        max: EncodableT,
        min: EncodableT,
        start: int | None = None,
        num: int | None = None,
    ) -> list[bytes | str]: ...

    @overload
    def zrevrangebylex(
        self: AsyncClientProtocol,
        name: KeyT,
        max: EncodableT,
        min: EncodableT,
        start: int | None = None,
        num: int | None = None,
    ) -> Awaitable[list[bytes | str]]: ...

    def zrevrangebylex(
        self,
        name: KeyT,
        max: EncodableT,
        min: EncodableT,
        start: int | None = None,
        num: int | None = None,
    ) -> list[bytes | str] | Awaitable[list[bytes | str]]:
        """
        Return the reversed lexicographical range of values from sorted set
        ``name`` between ``max`` and ``min``.

        If ``start`` and ``num`` are specified, then return a slice of the
        range.

        For more information, see https://redis.io/commands/zrevrangebylex
        """
        if (start is not None and num is None) or (num is not None and start is None):
            raise DataError("``start`` and ``num`` must both be specified")
        pieces = ["ZREVRANGEBYLEX", name, max, min]
        if start is not None and num is not None:
            pieces.extend(["LIMIT", start, num])
        return self.execute_command(*pieces, keys=[name])

    @overload
    def zrangebyscore(
        self: SyncClientProtocol,
        name: KeyT,
        min: ZScoreBoundT,
        max: ZScoreBoundT,
        start: int | None = None,
        num: int | None = None,
        withscores: bool = False,
        score_cast_func: type | Callable = float,
    ) -> ZSetRangeResponse: ...

    @overload
    def zrangebyscore(
        self: AsyncClientProtocol,
        name: KeyT,
        min: ZScoreBoundT,
        max: ZScoreBoundT,
        start: int | None = None,
        num: int | None = None,
        withscores: bool = False,
        score_cast_func: type | Callable = float,
    ) -> Awaitable[ZSetRangeResponse]: ...

    def zrangebyscore(
        self,
        name: KeyT,
        min: ZScoreBoundT,
        max: ZScoreBoundT,
        start: int | None = None,
        num: int | None = None,
        withscores: bool = False,
        score_cast_func: type | Callable = float,
    ) -> ZSetRangeResponse | Awaitable[ZSetRangeResponse]:
        """
        Return a range of values from the sorted set ``name`` with scores
        between ``min`` and ``max``.

        If ``start`` and ``num`` are specified, then return a slice
        of the range.

        ``withscores`` indicates to return the scores along with the values.
        The return type is a list of (value, score) pairs

        `score_cast_func`` a callable used to cast the score return value

        For more information, see https://redis.io/commands/zrangebyscore
        """
        if (start is not None and num is None) or (num is not None and start is None):
            raise DataError("``start`` and ``num`` must both be specified")
        pieces = ["ZRANGEBYSCORE", name, min, max]
        if start is not None and num is not None:
            pieces.extend(["LIMIT", start, num])
        if withscores:
            pieces.append("WITHSCORES")
        options = {"withscores": withscores, "score_cast_func": score_cast_func}
        options["keys"] = [name]
        return self.execute_command(*pieces, **options)

    @overload
    def zrevrangebyscore(
        self: SyncClientProtocol,
        name: KeyT,
        max: ZScoreBoundT,
        min: ZScoreBoundT,
        start: int | None = None,
        num: int | None = None,
        withscores: bool = False,
        score_cast_func: type | Callable = float,
    ) -> ZSetRangeResponse: ...

    @overload
    def zrevrangebyscore(
        self: AsyncClientProtocol,
        name: KeyT,
        max: ZScoreBoundT,
        min: ZScoreBoundT,
        start: int | None = None,
        num: int | None = None,
        withscores: bool = False,
        score_cast_func: type | Callable = float,
    ) -> Awaitable[ZSetRangeResponse]: ...

    def zrevrangebyscore(
        self,
        name: KeyT,
        max: ZScoreBoundT,
        min: ZScoreBoundT,
        start: int | None = None,
        num: int | None = None,
        withscores: bool = False,
        score_cast_func: type | Callable = float,
    ) -> ZSetRangeResponse | Awaitable[ZSetRangeResponse]:
        """
        Return a range of values from the sorted set ``name`` with scores
        between ``min`` and ``max`` in descending order.

        If ``start`` and ``num`` are specified, then return a slice
        of the range.

        ``withscores`` indicates to return the scores along with the values.
        The return type is a list of (value, score) pairs

        ``score_cast_func`` a callable used to cast the score return value

        For more information, see https://redis.io/commands/zrevrangebyscore
        """
        if (start is not None and num is None) or (num is not None and start is None):
            raise DataError("``start`` and ``num`` must both be specified")
        pieces = ["ZREVRANGEBYSCORE", name, max, min]
        if start is not None and num is not None:
            pieces.extend(["LIMIT", start, num])
        if withscores:
            pieces.append("WITHSCORES")
        options = {"withscores": withscores, "score_cast_func": score_cast_func}
        options["keys"] = [name]
        return self.execute_command(*pieces, **options)

    @overload
    def zrank(
        self: SyncClientProtocol,
        name: KeyT,
        value: EncodableT,
        withscore: bool = False,
        score_cast_func: type | Callable = float,
    ) -> int | list[Any] | None: ...

    @overload
    def zrank(
        self: AsyncClientProtocol,
        name: KeyT,
        value: EncodableT,
        withscore: bool = False,
        score_cast_func: type | Callable = float,
    ) -> Awaitable[int | list[Any] | None]: ...

    def zrank(
        self,
        name: KeyT,
        value: EncodableT,
        withscore: bool = False,
        score_cast_func: type | Callable = float,
    ) -> (int | list[Any] | None) | Awaitable[int | list[Any] | None]:
        """
        Returns a 0-based value indicating the rank of ``value`` in sorted set
        ``name``.
        The optional WITHSCORE argument supplements the command's
        reply with the score of the element returned.

        ``score_cast_func`` a callable used to cast the score return value

        For more information, see https://redis.io/commands/zrank
        """
        pieces = ["ZRANK", name, value]
        if withscore:
            pieces.append("WITHSCORE")

        options = {"withscore": withscore, "score_cast_func": score_cast_func}
        if withscore and score_cast_func is not float:
            # Lets the RESP3 -> legacy callback match RESP2's str scores.
            options["decode_responses"] = self._zset_decode_responses()

        return self.execute_command(*pieces, **options)

    @overload
    def zrem(self: SyncClientProtocol, name: KeyT, *values: FieldT) -> int: ...

    @overload
    def zrem(
        self: AsyncClientProtocol, name: KeyT, *values: FieldT
    ) -> Awaitable[int]: ...

    def zrem(self, name: KeyT, *values: FieldT) -> int | Awaitable[int]:
        """
        Remove member ``values`` from sorted set ``name``

        For more information, see https://redis.io/commands/zrem
        """
        return self.execute_command("ZREM", name, *values)

    @overload
    def zremrangebylex(
        self: SyncClientProtocol, name: KeyT, min: EncodableT, max: EncodableT
    ) -> int: ...

    @overload
    def zremrangebylex(
        self: AsyncClientProtocol, name: KeyT, min: EncodableT, max: EncodableT
    ) -> Awaitable[int]: ...

    def zremrangebylex(
        self, name: KeyT, min: EncodableT, max: EncodableT
    ) -> int | Awaitable[int]:
        """
        Remove all elements in the sorted set ``name`` between the
        lexicographical range specified by ``min`` and ``max``.

        Returns the number of elements removed.

        For more information, see https://redis.io/commands/zremrangebylex
        """
        return self.execute_command("ZREMRANGEBYLEX", name, min, max)

    @overload
    def zremrangebyrank(
        self: SyncClientProtocol, name: KeyT, min: int, max: int
    ) -> int: ...

    @overload
    def zremrangebyrank(
        self: AsyncClientProtocol, name: KeyT, min: int, max: int
    ) -> Awaitable[int]: ...

    def zremrangebyrank(self, name: KeyT, min: int, max: int) -> int | Awaitable[int]:
        """
        Remove all elements in the sorted set ``name`` with ranks between
        ``min`` and ``max``. Values are 0-based, ordered from smallest score
        to largest. Values can be negative indicating the highest scores.
        Returns the number of elements removed

        For more information, see https://redis.io/commands/zremrangebyrank
        """
        return self.execute_command("ZREMRANGEBYRANK", name, min, max)

    @overload
    def zremrangebyscore(
        self: SyncClientProtocol, name: KeyT, min: ZScoreBoundT, max: ZScoreBoundT
    ) -> int: ...

    @overload
    def zremrangebyscore(
        self: AsyncClientProtocol, name: KeyT, min: ZScoreBoundT, max: ZScoreBoundT
    ) -> Awaitable[int]: ...

    def zremrangebyscore(
        self, name: KeyT, min: ZScoreBoundT, max: ZScoreBoundT
    ) -> int | Awaitable[int]:
        """
        Remove all elements in the sorted set ``name`` with scores
        between ``min`` and ``max``. Returns the number of elements removed.

        For more information, see https://redis.io/commands/zremrangebyscore
        """
        return self.execute_command("ZREMRANGEBYSCORE", name, min, max)

    @overload
    def zrevrank(
        self: SyncClientProtocol,
        name: KeyT,
        value: EncodableT,
        withscore: bool = False,
        score_cast_func: type | Callable = float,
    ) -> int | list[Any] | None: ...

    @overload
    def zrevrank(
        self: AsyncClientProtocol,
        name: KeyT,
        value: EncodableT,
        withscore: bool = False,
        score_cast_func: type | Callable = float,
    ) -> Awaitable[int | list[Any] | None]: ...

    def zrevrank(
        self,
        name: KeyT,
        value: EncodableT,
        withscore: bool = False,
        score_cast_func: type | Callable = float,
    ) -> (int | list[Any] | None) | Awaitable[int | list[Any] | None]:
        """
        Returns a 0-based value indicating the descending rank of
        ``value`` in sorted set ``name``.
        The optional ``withscore`` argument supplements the command's
        reply with the score of the element returned.

        ``score_cast_func`` a callable used to cast the score return value

        For more information, see https://redis.io/commands/zrevrank
        """
        pieces = ["ZREVRANK", name, value]
        if withscore:
            pieces.append("WITHSCORE")

        options = {"withscore": withscore, "score_cast_func": score_cast_func}
        if withscore and score_cast_func is not float:
            # Lets the RESP3 -> legacy callback match RESP2's str scores.
            options["decode_responses"] = self._zset_decode_responses()

        return self.execute_command(*pieces, **options)

    @overload
    def zscore(
        self: SyncClientProtocol, name: KeyT, value: EncodableT
    ) -> float | None: ...

    @overload
    def zscore(
        self: AsyncClientProtocol, name: KeyT, value: EncodableT
    ) -> Awaitable[float | None]: ...

    def zscore(self, name: KeyT, value: EncodableT) -> (float | None) | Awaitable[
        float | None
    ]:
        """
        Return the score of element ``value`` in sorted set ``name``

        For more information, see https://redis.io/commands/zscore
        """
        return self.execute_command("ZSCORE", name, value, keys=[name])

    @overload
    def zunion(
        self: SyncClientProtocol,
        keys: Sequence[KeyT] | Mapping[AnyKeyT, float],
        aggregate: str | None = None,
        withscores: bool = False,
        score_cast_func: type | Callable = float,
    ) -> ZSetRangeResponse: ...

    @overload
    def zunion(
        self: AsyncClientProtocol,
        keys: Sequence[KeyT] | Mapping[AnyKeyT, float],
        aggregate: str | None = None,
        withscores: bool = False,
        score_cast_func: type | Callable = float,
    ) -> Awaitable[ZSetRangeResponse]: ...

    def zunion(
        self,
        keys: Sequence[KeyT] | Mapping[AnyKeyT, float],
        aggregate: str | None = None,
        withscores: bool = False,
        score_cast_func: type | Callable = float,
    ) -> ZSetRangeResponse | Awaitable[ZSetRangeResponse]:
        """
        Return the union of multiple sorted sets specified by ``keys``.
        ``keys`` can be provided as dictionary of keys and their weights.
        Scores will be aggregated based on the ``aggregate``, or SUM if
        none is provided.

        Available aggregation modes:

        - ``SUM`` (default): the score of an element is summed across the
          inputs where it exists.
          Score = SUM(score₁×weight₁, score₂×weight₂, ...)
        - ``MIN``: the resulting set will contain the minimum score of an
          element across the inputs where it exists.
          Score = MIN(score₁×weight₁, score₂×weight₂, ...)
        - ``MAX``: the resulting set will contain the maximum score of an
          element across the inputs where it exists.
          Score = MAX(score₁×weight₁, score₂×weight₂, ...)
        - ``COUNT``: ignores the original scores and counts weighted set
          membership. Each element's score is the sum of the weights of
          the input sets that contain it.
          Score = SUM(weight₁, weight₂, ...) for sets containing the element.
          When all weights are 1 (default), the score equals the number
          of input sets containing the element.

        ``score_cast_func`` a callable used to cast the score return value

        For more information, see https://redis.io/commands/zunion
        """
        return self._zaggregate(
            "ZUNION",
            None,
            keys,
            aggregate,
            withscores=withscores,
            score_cast_func=score_cast_func,
        )

    @overload
    def zunionstore(
        self: SyncClientProtocol,
        dest: KeyT,
        keys: Sequence[KeyT] | Mapping[AnyKeyT, float],
        aggregate: str | None = None,
    ) -> int: ...

    @overload
    def zunionstore(
        self: AsyncClientProtocol,
        dest: KeyT,
        keys: Sequence[KeyT] | Mapping[AnyKeyT, float],
        aggregate: str | None = None,
    ) -> Awaitable[int]: ...

    def zunionstore(
        self,
        dest: KeyT,
        keys: Sequence[KeyT] | Mapping[AnyKeyT, float],
        aggregate: str | None = None,
    ) -> int | Awaitable[int]:
        """
        Union multiple sorted sets specified by ``keys`` into
        a new sorted set, ``dest``. Scores in the destination will be
        aggregated based on the ``aggregate``, or SUM if none is provided.

        Available aggregation modes:

        - ``SUM`` (default): the score of an element is summed across the
          inputs where it exists.
          Score = SUM(score₁×weight₁, score₂×weight₂, ...)
        - ``MIN``: the resulting set will contain the minimum score of an
          element across the inputs where it exists.
          Score = MIN(score₁×weight₁, score₂×weight₂, ...)
        - ``MAX``: the resulting set will contain the maximum score of an
          element across the inputs where it exists.
          Score = MAX(score₁×weight₁, score₂×weight₂, ...)
        - ``COUNT``: ignores the original scores and counts weighted set
          membership. Each element's score is the sum of the weights of
          the input sets that contain it.
          Score = SUM(weight₁, weight₂, ...) for sets containing the element.
          When all weights are 1 (default), the score equals the number
          of input sets containing the element.

        For more information, see https://redis.io/commands/zunionstore
        """
        return self._zaggregate("ZUNIONSTORE", dest, keys, aggregate)

    @overload
    def zmscore(
        self: SyncClientProtocol, key: KeyT, members: List[str]
    ) -> list[float | None]: ...

    @overload
    def zmscore(
        self: AsyncClientProtocol, key: KeyT, members: List[str]
    ) -> Awaitable[list[float | None]]: ...

    def zmscore(
        self, key: KeyT, members: List[str]
    ) -> list[float | None] | Awaitable[list[float | None]]:
        """
        Returns the scores associated with the specified members
        in the sorted set stored at key.
        ``members`` should be a list of the member name.
        Return type is a list of score.
        If the member does not exist, a None will be returned
        in corresponding position.

        For more information, see https://redis.io/commands/zmscore
        """
        if not members:
            raise DataError("ZMSCORE members must be a non-empty list")
        pieces = [key] + members
        return self.execute_command("ZMSCORE", *pieces, keys=[key])

    def _zaggregate(
        self,
        command: str,
        dest: Union[KeyT, None],
        keys: Union[Sequence[KeyT], Mapping[AnyKeyT, float]],
        aggregate: Optional[str] = None,
        **options,
    ) -> ResponseT:
        pieces: list[EncodableT] = [command]
        if dest is not None:
            pieces.append(dest)
        pieces.append(len(keys))
        if isinstance(keys, Mapping):
            keys, weights = keys.keys(), keys.values()
        else:
            weights = None
        pieces.extend(keys)
        if weights:
            pieces.append(b"WEIGHTS")
            pieces.extend(weights)
        if aggregate:
            if aggregate.upper() in ["SUM", "MIN", "MAX", "COUNT"]:
                pieces.append(b"AGGREGATE")
                pieces.append(aggregate)
            else:
                raise DataError("aggregate can be sum, min, max or count.")
        if options.get("withscores", False):
            pieces.append(b"WITHSCORES")
        options["keys"] = keys
        return self.execute_command(*pieces, **options)


AsyncSortedSetCommands = SortedSetCommands


class HyperlogCommands(CommandsProtocol):
    """
    Redis commands of HyperLogLogs data type.
    see: https://redis.io/topics/data-types-intro#hyperloglogs
    """

    @overload
    def pfadd(self: SyncClientProtocol, name: KeyT, *values: FieldT) -> int: ...

    @overload
    def pfadd(
        self: AsyncClientProtocol, name: KeyT, *values: FieldT
    ) -> Awaitable[int]: ...

    def pfadd(self, name: KeyT, *values: FieldT) -> int | Awaitable[int]:
        """
        Adds the specified elements to the specified HyperLogLog.

        For more information, see https://redis.io/commands/pfadd
        """
        return self.execute_command("PFADD", name, *values)

    @overload
    def pfcount(self: SyncClientProtocol, *sources: KeyT) -> int: ...

    @overload
    def pfcount(self: AsyncClientProtocol, *sources: KeyT) -> Awaitable[int]: ...

    def pfcount(self, *sources: KeyT) -> int | Awaitable[int]:
        """
        Return the approximated cardinality of
        the set observed by the HyperLogLog at key(s).

        For more information, see https://redis.io/commands/pfcount
        """
        return self.execute_command("PFCOUNT", *sources)

    @overload
    def pfmerge(self: SyncClientProtocol, dest: KeyT, *sources: KeyT) -> bool: ...

    @overload
    def pfmerge(
        self: AsyncClientProtocol, dest: KeyT, *sources: KeyT
    ) -> Awaitable[bool]: ...

    def pfmerge(self, dest: KeyT, *sources: KeyT) -> bool | Awaitable[bool]:
        """
        Merge N different HyperLogLogs into a single one.

        For more information, see https://redis.io/commands/pfmerge
        """
        return self.execute_command("PFMERGE", dest, *sources)


AsyncHyperlogCommands = HyperlogCommands


class HashDataPersistOptions(Enum):
    # set the value for each provided key to each
    # provided value only if all do not already exist.
    FNX = "FNX"

    # set the value for each provided key to each
    # provided value only if all already exist.
    FXX = "FXX"


class HashCommands(CommandsProtocol):
    """
    Redis commands for Hash data type.
    see: https://redis.io/topics/data-types-intro#redis-hashes
    """

    @overload
    def hdel(self: SyncClientProtocol, name: KeyT, *keys: FieldT) -> int: ...

    @overload
    def hdel(
        self: AsyncClientProtocol, name: KeyT, *keys: FieldT
    ) -> Awaitable[int]: ...

    def hdel(self, name: KeyT, *keys: FieldT) -> int | Awaitable[int]:
        """
        Delete ``keys`` from hash ``name``

        For more information, see https://redis.io/commands/hdel
        """
        return self.execute_command("HDEL", name, *keys)

    @overload
    def hexists(self: SyncClientProtocol, name: KeyT, key: FieldT) -> bool: ...

    @overload
    def hexists(
        self: AsyncClientProtocol, name: KeyT, key: FieldT
    ) -> Awaitable[bool]: ...

    def hexists(self, name: KeyT, key: FieldT) -> bool | Awaitable[bool]:
        """
        Returns a boolean indicating if ``key`` exists within hash ``name``

        For more information, see https://redis.io/commands/hexists
        """
        return self.execute_command("HEXISTS", name, key, keys=[name])

    @overload
    def hget(
        self: SyncClientProtocol, name: KeyT, key: FieldT
    ) -> bytes | str | None: ...

    @overload
    def hget(
        self: AsyncClientProtocol, name: KeyT, key: FieldT
    ) -> Awaitable[bytes | str | None]: ...

    def hget(self, name: KeyT, key: FieldT) -> (bytes | str | None) | Awaitable[
        bytes | str | None
    ]:
        """
        Return the value of ``key`` within the hash ``name``

        For more information, see https://redis.io/commands/hget
        """
        return self.execute_command("HGET", name, key, keys=[name])

    @overload
    def hgetall(
        self: SyncClientProtocol, name: KeyT
    ) -> dict[bytes | str, bytes | str]: ...

    @overload
    def hgetall(
        self: AsyncClientProtocol, name: KeyT
    ) -> Awaitable[dict[bytes | str, bytes | str]]: ...

    def hgetall(
        self, name: KeyT
    ) -> dict[bytes | str, bytes | str] | Awaitable[dict[bytes | str, bytes | str]]:
        """
        Return a Python dict of the hash's name/value pairs

        For more information, see https://redis.io/commands/hgetall
        """
        return self.execute_command("HGETALL", name, keys=[name])

    @overload
    def hgetdel(
        self: SyncClientProtocol, name: KeyT, *keys: FieldT
    ) -> list[bytes | str | None]: ...

    @overload
    def hgetdel(
        self: AsyncClientProtocol, name: KeyT, *keys: FieldT
    ) -> Awaitable[list[bytes | str | None]]: ...

    def hgetdel(
        self, name: KeyT, *keys: FieldT
    ) -> list[bytes | str | None] | Awaitable[list[bytes | str | None]]:
        """
        Return the value of ``key`` within the hash ``name`` and
        delete the field in the hash.
        This command is similar to HGET, except for the fact that it also deletes
        the key on success from the hash with the provided ```name```.

        Available since Redis 8.0
        For more information, see https://redis.io/commands/hgetdel
        """
        if len(keys) == 0:
            raise DataError("'hgetdel' should have at least one key provided")

        return self.execute_command("HGETDEL", name, "FIELDS", len(keys), *keys)

    @overload
    def hgetex(
        self: SyncClientProtocol,
        name: KeyT,
        *keys: FieldT,
        ex: ExpiryT | str | None = None,
        px: ExpiryT | str | None = None,
        exat: AbsExpiryT | None = None,
        pxat: AbsExpiryT | None = None,
        persist: bool = False,
    ) -> list[bytes | str | None]: ...

    @overload
    def hgetex(
        self: AsyncClientProtocol,
        name: KeyT,
        *keys: FieldT,
        ex: ExpiryT | str | None = None,
        px: ExpiryT | str | None = None,
        exat: AbsExpiryT | None = None,
        pxat: AbsExpiryT | None = None,
        persist: bool = False,
    ) -> Awaitable[list[bytes | str | None]]: ...

    def hgetex(
        self,
        name: KeyT,
        *keys: FieldT,
        ex: ExpiryT | str | None = None,
        px: ExpiryT | str | None = None,
        exat: AbsExpiryT | None = None,
        pxat: AbsExpiryT | None = None,
        persist: bool = False,
    ) -> list[bytes | str | None] | Awaitable[list[bytes | str | None]]:
        """
        Return the values of ``key`` and ``keys`` within the hash ``name``
        and optionally set their expiration.

        ``ex`` sets an expire flag on ``kyes`` for ``ex`` seconds.

        ``px`` sets an expire flag on ``keys`` for ``px`` milliseconds.

        ``ex`` and ``px`` also accept digit-only strings, such as ``"60"``
        and ``"500"``, respectively.

        ``exat`` sets an expire flag on ``keys`` for ``ex`` seconds,
        specified in unix time.

        ``pxat`` sets an expire flag on ``keys`` for ``ex`` milliseconds,
        specified in unix time.

        ``persist`` remove the time to live associated with the ``keys``.

        Available since Redis 8.0
        For more information, see https://redis.io/commands/hgetex
        """
        if not keys:
            raise DataError("'hgetex' should have at least one key provided")

        if not at_most_one_value_set(
            (
                ex is not None,
                px is not None,
                exat is not None,
                pxat is not None,
                persist,
            )
        ):
            raise DataError(
                "``ex``, ``px``, ``exat``, ``pxat``, "
                "and ``persist`` are mutually exclusive."
            )

        exp_options: list[EncodableT] = extract_expire_flags(ex, px, exat, pxat)

        if persist:
            exp_options.append("PERSIST")

        return self.execute_command(
            "HGETEX",
            name,
            *exp_options,
            "FIELDS",
            len(keys),
            *keys,
        )

    @overload
    def hincrby(
        self: SyncClientProtocol, name: KeyT, key: FieldT, amount: int = 1
    ) -> int: ...

    @overload
    def hincrby(
        self: AsyncClientProtocol, name: KeyT, key: FieldT, amount: int = 1
    ) -> Awaitable[int]: ...

    def hincrby(self, name: KeyT, key: FieldT, amount: int = 1) -> int | Awaitable[int]:
        """
        Increment the value of ``key`` in hash ``name`` by ``amount``

        For more information, see https://redis.io/commands/hincrby
        """
        return self.execute_command("HINCRBY", name, key, amount)

    @overload
    def hincrbyfloat(
        self: SyncClientProtocol, name: KeyT, key: FieldT, amount: float = 1.0
    ) -> float: ...

    @overload
    def hincrbyfloat(
        self: AsyncClientProtocol, name: KeyT, key: FieldT, amount: float = 1.0
    ) -> Awaitable[float]: ...

    def hincrbyfloat(
        self, name: KeyT, key: FieldT, amount: float = 1.0
    ) -> float | Awaitable[float]:
        """
        Increment the value of ``key`` in hash ``name`` by floating ``amount``

        For more information, see https://redis.io/commands/hincrbyfloat
        """
        return self.execute_command("HINCRBYFLOAT", name, key, amount)

    @overload
    def hkeys(self: SyncClientProtocol, name: KeyT) -> list[bytes | str]: ...

    @overload
    def hkeys(
        self: AsyncClientProtocol, name: KeyT
    ) -> Awaitable[list[bytes | str]]: ...

    def hkeys(self, name: KeyT) -> list[bytes | str] | Awaitable[list[bytes | str]]:
        """
        Return the list of keys within hash ``name``

        For more information, see https://redis.io/commands/hkeys
        """
        return self.execute_command("HKEYS", name, keys=[name])

    @overload
    def hlen(self: SyncClientProtocol, name: KeyT) -> int: ...

    @overload
    def hlen(self: AsyncClientProtocol, name: KeyT) -> Awaitable[int]: ...

    def hlen(self, name: KeyT) -> int | Awaitable[int]:
        """
        Return the number of elements in hash ``name``

        For more information, see https://redis.io/commands/hlen
        """
        return self.execute_command("HLEN", name, keys=[name])

    @overload
    def hset(
        self: SyncClientProtocol,
        name: KeyT,
        key: FieldT | None = None,
        value: EncodableT | None = None,
        mapping: Mapping[FieldT, EncodableT] | None = None,
        items: Sequence[EncodableT] | None = None,
    ) -> int: ...

    @overload
    def hset(
        self: AsyncClientProtocol,
        name: KeyT,
        key: FieldT | None = None,
        value: EncodableT | None = None,
        mapping: Mapping[FieldT, EncodableT] | None = None,
        items: Sequence[EncodableT] | None = None,
    ) -> Awaitable[int]: ...

    def hset(
        self,
        name: KeyT,
        key: FieldT | None = None,
        value: EncodableT | None = None,
        mapping: Mapping[FieldT, EncodableT] | None = None,
        items: Sequence[EncodableT] | None = None,
    ) -> int | Awaitable[int]:
        """
        Set ``key`` to ``value`` within hash ``name``,
        ``mapping`` accepts a dict of key/value pairs that will be
        added to hash ``name``.
        ``items`` accepts a list of key/value pairs that will be
        added to hash ``name``.
        Returns the number of fields that were added.

        For more information, see https://redis.io/commands/hset
        """

        if key is None and not mapping and not items:
            raise DataError("'hset' with no key value pairs")

        pieces = []
        if items:
            pieces.extend(items)
        if key is not None:
            pieces.extend((key, value))
        if mapping:
            for pair in mapping.items():
                pieces.extend(pair)

        return self.execute_command("HSET", name, *pieces)

    @overload
    def hsetex(
        self: SyncClientProtocol,
        name: KeyT,
        key: FieldT | None = None,
        value: EncodableT | None = None,
        mapping: Mapping[FieldT, EncodableT] | None = None,
        items: Sequence[EncodableT] | None = None,
        ex: ExpiryT | str | None = None,
        px: ExpiryT | str | None = None,
        exat: AbsExpiryT | None = None,
        pxat: AbsExpiryT | None = None,
        data_persist_option: HashDataPersistOptions | None = None,
        keepttl: bool = False,
    ) -> int: ...

    @overload
    def hsetex(
        self: AsyncClientProtocol,
        name: KeyT,
        key: FieldT | None = None,
        value: EncodableT | None = None,
        mapping: Mapping[FieldT, EncodableT] | None = None,
        items: Sequence[EncodableT] | None = None,
        ex: ExpiryT | str | None = None,
        px: ExpiryT | str | None = None,
        exat: AbsExpiryT | None = None,
        pxat: AbsExpiryT | None = None,
        data_persist_option: HashDataPersistOptions | None = None,
        keepttl: bool = False,
    ) -> Awaitable[int]: ...

    def hsetex(
        self,
        name: KeyT,
        key: FieldT | None = None,
        value: EncodableT | None = None,
        mapping: Mapping[FieldT, EncodableT] | None = None,
        items: Sequence[EncodableT] | None = None,
        ex: ExpiryT | str | None = None,
        px: ExpiryT | str | None = None,
        exat: AbsExpiryT | None = None,
        pxat: AbsExpiryT | None = None,
        data_persist_option: HashDataPersistOptions | None = None,
        keepttl: bool = False,
    ) -> int | Awaitable[int]:
        """
        Set ``key`` to ``value`` within hash ``name``

        ``mapping`` accepts a dict of key/value pairs that will be
        added to hash ``name``.

        ``items`` accepts a list of key/value pairs that will be
        added to hash ``name``.

        ``ex`` sets an expire flag on ``keys`` for ``ex`` seconds.

        ``px`` sets an expire flag on ``keys`` for ``px`` milliseconds.

        ``ex`` and ``px`` also accept digit-only strings, such as ``"60"``
        and ``"500"``, respectively.

        ``exat`` sets an expire flag on ``keys`` for ``ex`` seconds,
            specified in unix time.

        ``pxat`` sets an expire flag on ``keys`` for ``ex`` milliseconds,
            specified in unix time.

        ``data_persist_option`` can be set to ``FNX`` or ``FXX`` to control the
            behavior of the command.
            ``FNX`` will set the value for each provided key to each
                provided value only if all do not already exist.
            ``FXX`` will set the value for each provided key to each
                provided value only if all already exist.

        ``keepttl`` if True, retain the time to live associated with the keys.

        Returns the number of fields that were added.

        Available since Redis 8.0
        For more information, see https://redis.io/commands/hsetex
        """
        if key is None and not mapping and not items:
            raise DataError("'hsetex' with no key value pairs")

        if items and len(items) % 2 != 0:
            raise DataError(
                "'hsetex' with odd number of items. "
                "'items' must contain a list of key/value pairs."
            )

        if not at_most_one_value_set(
            (
                ex is not None,
                px is not None,
                exat is not None,
                pxat is not None,
                keepttl,
            )
        ):
            raise DataError(
                "``ex``, ``px``, ``exat``, ``pxat``, "
                "and ``keepttl`` are mutually exclusive."
            )

        exp_options: list[EncodableT] = extract_expire_flags(ex, px, exat, pxat)
        if data_persist_option:
            exp_options.append(data_persist_option.value)

        if keepttl:
            exp_options.append("KEEPTTL")

        pieces = []
        if items:
            pieces.extend(items)
        if key is not None:
            pieces.extend((key, value))
        if mapping:
            for pair in mapping.items():
                pieces.extend(pair)

        return self.execute_command(
            "HSETEX", name, *exp_options, "FIELDS", int(len(pieces) / 2), *pieces
        )

    @overload
    def hsetnx(
        self: SyncClientProtocol, name: KeyT, key: FieldT, value: EncodableT
    ) -> int: ...

    @overload
    def hsetnx(
        self: AsyncClientProtocol, name: KeyT, key: FieldT, value: EncodableT
    ) -> Awaitable[int]: ...

    def hsetnx(
        self, name: KeyT, key: FieldT, value: EncodableT
    ) -> int | Awaitable[int]:
        """
        Set ``key`` to ``value`` within hash ``name`` if ``key`` does not
        exist.  Returns 1 if HSETNX created a field, otherwise 0.

        For more information, see https://redis.io/commands/hsetnx
        """
        return self.execute_command("HSETNX", name, key, value)

    @overload
    def hmset(
        self: SyncClientProtocol, name: KeyT, mapping: Mapping[FieldT, EncodableT]
    ) -> bool: ...

    @overload
    def hmset(
        self: AsyncClientProtocol,
        name: KeyT,
        mapping: Mapping[FieldT, EncodableT],
    ) -> Awaitable[bool]: ...

    def hmset(
        self, name: KeyT, mapping: Mapping[FieldT, EncodableT]
    ) -> bool | Awaitable[bool]:
        """
        Set key to value within hash ``name`` for each corresponding
        key and value from the ``mapping`` dict.

        Redis has deprecated HMSET since version 4.0.0. The command remains
        available for compatibility. Please use HSET in new code.

        For more information, see https://redis.io/commands/hmset
        """
        if not mapping:
            raise DataError("'hmset' with 'mapping' of length 0")
        items = []
        for pair in mapping.items():
            items.extend(pair)
        return self.execute_command("HMSET", name, *items)

    @overload
    def hmget(
        self: SyncClientProtocol,
        name: KeyT,
        keys: FieldT | Iterable[FieldT],
        *args: FieldT,
    ) -> list[bytes | str | None]: ...

    @overload
    def hmget(
        self: AsyncClientProtocol,
        name: KeyT,
        keys: FieldT | Iterable[FieldT],
        *args: FieldT,
    ) -> Awaitable[list[bytes | str | None]]: ...

    def hmget(
        self, name: KeyT, keys: FieldT | Iterable[FieldT], *args: FieldT
    ) -> list[bytes | str | None] | Awaitable[list[bytes | str | None]]:
        """
        Returns a list of values ordered identically to ``keys``

        For more information, see https://redis.io/commands/hmget
        """
        args = list_or_args(keys, args)
        return self.execute_command("HMGET", name, *args, keys=[name])

    @overload
    def hvals(self: SyncClientProtocol, name: KeyT) -> list[bytes | str]: ...

    @overload
    def hvals(
        self: AsyncClientProtocol, name: KeyT
    ) -> Awaitable[list[bytes | str]]: ...

    def hvals(self, name: KeyT) -> list[bytes | str] | Awaitable[list[bytes | str]]:
        """
        Return the list of values within hash ``name``

        For more information, see https://redis.io/commands/hvals
        """
        return self.execute_command("HVALS", name, keys=[name])

    @overload
    def hstrlen(self: SyncClientProtocol, name: KeyT, key: FieldT) -> int: ...

    @overload
    def hstrlen(
        self: AsyncClientProtocol, name: KeyT, key: FieldT
    ) -> Awaitable[int]: ...

    def hstrlen(self, name: KeyT, key: FieldT) -> int | Awaitable[int]:
        """
        Return the number of bytes stored in the value of ``key``
        within hash ``name``

        For more information, see https://redis.io/commands/hstrlen
        """
        return self.execute_command("HSTRLEN", name, key, keys=[name])

    @overload
    def hexpire(
        self: SyncClientProtocol,
        name: KeyT,
        seconds: ExpiryT,
        *fields: FieldT,
        nx: bool = False,
        xx: bool = False,
        gt: bool = False,
        lt: bool = False,
    ) -> list[int]: ...

    @overload
    def hexpire(
        self: AsyncClientProtocol,
        name: KeyT,
        seconds: ExpiryT,
        *fields: FieldT,
        nx: bool = False,
        xx: bool = False,
        gt: bool = False,
        lt: bool = False,
    ) -> Awaitable[list[int]]: ...

    def hexpire(
        self,
        name: KeyT,
        seconds: ExpiryT,
        *fields: FieldT,
        nx: bool = False,
        xx: bool = False,
        gt: bool = False,
        lt: bool = False,
    ) -> list[int] | Awaitable[list[int]]:
        """
        Sets or updates the expiration time for fields within a hash key, using relative
        time in seconds.

        If a field already has an expiration time, the behavior of the update can be
        controlled using the `nx`, `xx`, `gt`, and `lt` parameters.

        The return value provides detailed information about the outcome for each field.

        For more information, see https://redis.io/commands/hexpire

        Args:
            name: The name of the hash key.
            seconds: Expiration time in seconds, relative. Can be an integer, or a
                     Python `timedelta` object.
            fields: List of fields within the hash to apply the expiration time to.
            nx: Set expiry only when the field has no expiry.
            xx: Set expiry only when the field has an existing expiry.
            gt: Set expiry only when the new expiry is greater than the current one.
            lt: Set expiry only when the new expiry is less than the current one.

        Returns:
            Returns a list which contains for each field in the request:
                - `-2` if the field does not exist, or if the key does not exist.
                - `0` if the specified NX | XX | GT | LT condition was not met.
                - `1` if the expiration time was set or updated.
                - `2` if the field was deleted because the specified expiration time is
                  in the past.
        """
        conditions = [nx, xx, gt, lt]
        if sum(conditions) > 1:
            raise ValueError("Only one of 'nx', 'xx', 'gt', 'lt' can be specified.")

        if isinstance(seconds, datetime.timedelta):
            seconds = int(seconds.total_seconds())

        options = []
        if nx:
            options.append("NX")
        if xx:
            options.append("XX")
        if gt:
            options.append("GT")
        if lt:
            options.append("LT")

        return self.execute_command(
            "HEXPIRE", name, seconds, *options, "FIELDS", len(fields), *fields
        )

    @overload
    def hpexpire(
        self: SyncClientProtocol,
        name: KeyT,
        milliseconds: ExpiryT,
        *fields: FieldT,
        nx: bool = False,
        xx: bool = False,
        gt: bool = False,
        lt: bool = False,
    ) -> list[int]: ...

    @overload
    def hpexpire(
        self: AsyncClientProtocol,
        name: KeyT,
        milliseconds: ExpiryT,
        *fields: FieldT,
        nx: bool = False,
        xx: bool = False,
        gt: bool = False,
        lt: bool = False,
    ) -> Awaitable[list[int]]: ...

    def hpexpire(
        self,
        name: KeyT,
        milliseconds: ExpiryT,
        *fields: FieldT,
        nx: bool = False,
        xx: bool = False,
        gt: bool = False,
        lt: bool = False,
    ) -> list[int] | Awaitable[list[int]]:
        """
        Sets or updates the expiration time for fields within a hash key, using relative
        time in milliseconds.

        If a field already has an expiration time, the behavior of the update can be
        controlled using the `nx`, `xx`, `gt`, and `lt` parameters.

        The return value provides detailed information about the outcome for each field.

        For more information, see https://redis.io/commands/hpexpire

        Args:
            name: The name of the hash key.
            milliseconds: Expiration time in milliseconds, relative. Can be an integer,
                          or a Python `timedelta` object.
            fields: List of fields within the hash to apply the expiration time to.
            nx: Set expiry only when the field has no expiry.
            xx: Set expiry only when the field has an existing expiry.
            gt: Set expiry only when the new expiry is greater than the current one.
            lt: Set expiry only when the new expiry is less than the current one.

        Returns:
            Returns a list which contains for each field in the request:
                - `-2` if the field does not exist, or if the key does not exist.
                - `0` if the specified NX | XX | GT | LT condition was not met.
                - `1` if the expiration time was set or updated.
                - `2` if the field was deleted because the specified expiration time is
                  in the past.
        """
        conditions = [nx, xx, gt, lt]
        if sum(conditions) > 1:
            raise ValueError("Only one of 'nx', 'xx', 'gt', 'lt' can be specified.")

        if isinstance(milliseconds, datetime.timedelta):
            milliseconds = int(milliseconds.total_seconds() * 1000)

        options = []
        if nx:
            options.append("NX")
        if xx:
            options.append("XX")
        if gt:
            options.append("GT")
        if lt:
            options.append("LT")

        return self.execute_command(
            "HPEXPIRE", name, milliseconds, *options, "FIELDS", len(fields), *fields
        )

    @overload
    def hexpireat(
        self: SyncClientProtocol,
        name: KeyT,
        unix_time_seconds: AbsExpiryT,
        *fields: FieldT,
        nx: bool = False,
        xx: bool = False,
        gt: bool = False,
        lt: bool = False,
    ) -> list[int]: ...

    @overload
    def hexpireat(
        self: AsyncClientProtocol,
        name: KeyT,
        unix_time_seconds: AbsExpiryT,
        *fields: FieldT,
        nx: bool = False,
        xx: bool = False,
        gt: bool = False,
        lt: bool = False,
    ) -> Awaitable[list[int]]: ...

    def hexpireat(
        self,
        name: KeyT,
        unix_time_seconds: AbsExpiryT,
        *fields: FieldT,
        nx: bool = False,
        xx: bool = False,
        gt: bool = False,
        lt: bool = False,
    ) -> list[int] | Awaitable[list[int]]:
        """
        Sets or updates the expiration time for fields within a hash key, using an
        absolute Unix timestamp in seconds.

        If a field already has an expiration time, the behavior of the update can be
        controlled using the `nx`, `xx`, `gt`, and `lt` parameters.

        The return value provides detailed information about the outcome for each field.

        For more information, see https://redis.io/commands/hexpireat

        Args:
            name: The name of the hash key.
            unix_time_seconds: Expiration time as Unix timestamp in seconds. Can be an
                               integer or a Python `datetime` object.
            fields: List of fields within the hash to apply the expiration time to.
            nx: Set expiry only when the field has no expiry.
            xx: Set expiry only when the field has an existing expiration time.
            gt: Set expiry only when the new expiry is greater than the current one.
            lt: Set expiry only when the new expiry is less than the current one.

        Returns:
            Returns a list which contains for each field in the request:
                - `-2` if the field does not exist, or if the key does not exist.
                - `0` if the specified NX | XX | GT | LT condition was not met.
                - `1` if the expiration time was set or updated.
                - `2` if the field was deleted because the specified expiration time is
                  in the past.
        """
        conditions = [nx, xx, gt, lt]
        if sum(conditions) > 1:
            raise ValueError("Only one of 'nx', 'xx', 'gt', 'lt' can be specified.")

        if isinstance(unix_time_seconds, datetime.datetime):
            unix_time_seconds = int(unix_time_seconds.timestamp())

        options = []
        if nx:
            options.append("NX")
        if xx:
            options.append("XX")
        if gt:
            options.append("GT")
        if lt:
            options.append("LT")

        return self.execute_command(
            "HEXPIREAT",
            name,
            unix_time_seconds,
            *options,
            "FIELDS",
            len(fields),
            *fields,
        )

    @overload
    def hpexpireat(
        self: SyncClientProtocol,
        name: KeyT,
        unix_time_milliseconds: AbsExpiryT,
        *fields: FieldT,
        nx: bool = False,
        xx: bool = False,
        gt: bool = False,
        lt: bool = False,
    ) -> list[int]: ...

    @overload
    def hpexpireat(
        self: AsyncClientProtocol,
        name: KeyT,
        unix_time_milliseconds: AbsExpiryT,
        *fields: FieldT,
        nx: bool = False,
        xx: bool = False,
        gt: bool = False,
        lt: bool = False,
    ) -> Awaitable[list[int]]: ...

    def hpexpireat(
        self,
        name: KeyT,
        unix_time_milliseconds: AbsExpiryT,
        *fields: FieldT,
        nx: bool = False,
        xx: bool = False,
        gt: bool = False,
        lt: bool = False,
    ) -> list[int] | Awaitable[list[int]]:
        """
        Sets or updates the expiration time for fields within a hash key, using an
        absolute Unix timestamp in milliseconds.

        If a field already has an expiration time, the behavior of the update can be
        controlled using the `nx`, `xx`, `gt`, and `lt` parameters.

        The return value provides detailed information about the outcome for each field.

        For more information, see https://redis.io/commands/hpexpireat

        Args:
            name: The name of the hash key.
            unix_time_milliseconds: Expiration time as Unix timestamp in milliseconds.
                                    Can be an integer or a Python `datetime` object.
            fields: List of fields within the hash to apply the expiry.
            nx: Set expiry only when the field has no expiry.
            xx: Set expiry only when the field has an existing expiry.
            gt: Set expiry only when the new expiry is greater than the current one.
            lt: Set expiry only when the new expiry is less than the current one.

        Returns:
            Returns a list which contains for each field in the request:
                - `-2` if the field does not exist, or if the key does not exist.
                - `0` if the specified NX | XX | GT | LT condition was not met.
                - `1` if the expiration time was set or updated.
                - `2` if the field was deleted because the specified expiration time is
                  in the past.
        """
        conditions = [nx, xx, gt, lt]
        if sum(conditions) > 1:
            raise ValueError("Only one of 'nx', 'xx', 'gt', 'lt' can be specified.")

        if isinstance(unix_time_milliseconds, datetime.datetime):
            unix_time_milliseconds = int(unix_time_milliseconds.timestamp() * 1000)

        options = []
        if nx:
            options.append("NX")
        if xx:
            options.append("XX")
        if gt:
            options.append("GT")
        if lt:
            options.append("LT")

        return self.execute_command(
            "HPEXPIREAT",
            name,
            unix_time_milliseconds,
            *options,
            "FIELDS",
            len(fields),
            *fields,
        )

    @overload
    def hpersist(
        self: SyncClientProtocol, name: KeyT, *fields: FieldT
    ) -> list[int]: ...

    @overload
    def hpersist(
        self: AsyncClientProtocol, name: KeyT, *fields: FieldT
    ) -> Awaitable[list[int]]: ...

    def hpersist(self, name: KeyT, *fields: FieldT) -> list[int] | Awaitable[list[int]]:
        """
        Removes the expiration time for each specified field in a hash.

        For more information, see https://redis.io/commands/hpersist

        Args:
            name: The name of the hash key.
            fields: A list of fields within the hash from which to remove the
                    expiration time.

        Returns:
            Returns a list which contains for each field in the request:
                - `-2` if the field does not exist, or if the key does not exist.
                - `-1` if the field exists but has no associated expiration time.
                - `1` if the expiration time was successfully removed from the field.
        """
        return self.execute_command("HPERSIST", name, "FIELDS", len(fields), *fields)

    @overload
    def hexpiretime(
        self: SyncClientProtocol, key: KeyT, *fields: FieldT
    ) -> list[int]: ...

    @overload
    def hexpiretime(
        self: AsyncClientProtocol, key: KeyT, *fields: FieldT
    ) -> Awaitable[list[int]]: ...

    def hexpiretime(
        self, key: KeyT, *fields: FieldT
    ) -> list[int] | Awaitable[list[int]]:
        """
        Returns the expiration times of hash fields as Unix timestamps in seconds.

        For more information, see https://redis.io/commands/hexpiretime

        Args:
            key: The hash key.
            fields: A list of fields within the hash for which to get the expiration
                    time.

        Returns:
            Returns a list which contains for each field in the request:
                - `-2` if the field does not exist, or if the key does not exist.
                - `-1` if the field exists but has no associated expire time.
                - A positive integer representing the expiration Unix timestamp in
                  seconds, if the field has an associated expiration time.
        """
        return self.execute_command(
            "HEXPIRETIME", key, "FIELDS", len(fields), *fields, keys=[key]
        )

    @overload
    def hpexpiretime(
        self: SyncClientProtocol, key: KeyT, *fields: FieldT
    ) -> list[int]: ...

    @overload
    def hpexpiretime(
        self: AsyncClientProtocol, key: KeyT, *fields: FieldT
    ) -> Awaitable[list[int]]: ...

    def hpexpiretime(
        self, key: KeyT, *fields: FieldT
    ) -> list[int] | Awaitable[list[int]]:
        """
        Returns the expiration times of hash fields as Unix timestamps in milliseconds.

        For more information, see https://redis.io/commands/hpexpiretime

        Args:
            key: The hash key.
            fields: A list of fields within the hash for which to get the expiration
                    time.

        Returns:
            Returns a list which contains for each field in the request:
                - `-2` if the field does not exist, or if the key does not exist.
                - `-1` if the field exists but has no associated expire time.
                - A positive integer representing the expiration Unix timestamp in
                  milliseconds, if the field has an associated expiration time.
        """
        return self.execute_command(
            "HPEXPIRETIME", key, "FIELDS", len(fields), *fields, keys=[key]
        )

    @overload
    def httl(self: SyncClientProtocol, key: KeyT, *fields: FieldT) -> list[int]: ...

    @overload
    def httl(
        self: AsyncClientProtocol, key: KeyT, *fields: FieldT
    ) -> Awaitable[list[int]]: ...

    def httl(self, key: KeyT, *fields: FieldT) -> list[int] | Awaitable[list[int]]:
        """
        Returns the TTL (Time To Live) in seconds for each specified field within a hash
        key.

        For more information, see https://redis.io/commands/httl

        Args:
            key: The hash key.
            fields: A list of fields within the hash for which to get the TTL.

        Returns:
            Returns a list which contains for each field in the request:
                - `-2` if the field does not exist, or if the key does not exist.
                - `-1` if the field exists but has no associated expire time.
                - A positive integer representing the TTL in seconds if the field has
                  an associated expiration time.
        """
        return self.execute_command(
            "HTTL", key, "FIELDS", len(fields), *fields, keys=[key]
        )

    @overload
    def hpttl(self: SyncClientProtocol, key: KeyT, *fields: FieldT) -> list[int]: ...

    @overload
    def hpttl(
        self: AsyncClientProtocol, key: KeyT, *fields: FieldT
    ) -> Awaitable[list[int]]: ...

    def hpttl(self, key: KeyT, *fields: FieldT) -> list[int] | Awaitable[list[int]]:
        """
        Returns the TTL (Time To Live) in milliseconds for each specified field within a
        hash key.

        For more information, see https://redis.io/commands/hpttl

        Args:
            key: The hash key.
            fields: A list of fields within the hash for which to get the TTL.

        Returns:
            Returns a list which contains for each field in the request:
                - `-2` if the field does not exist, or if the key does not exist.
                - `-1` if the field exists but has no associated expire time.
                - A positive integer representing the TTL in milliseconds if the field
                  has an associated expiration time.
        """
        return self.execute_command(
            "HPTTL", key, "FIELDS", len(fields), *fields, keys=[key]
        )

    # -- HIMPORT (Hinted Hash Templates) --------------------------------------
    # himport_set is the full public command: the executor lazily PREPAREs the
    # fieldset on the serving connection and reconciles deferred DISCARDs, so it
    # needs no client-level wrapper. The *_internal methods are the pure PREPARE /
    # DISCARD / DISCARDALL wire mappings; the client-side orchestration around them
    # (the shared registry, and running them immediately on a single-connection
    # client) lives on the client classes as himport_prepare / himport_discard /
    # himport_discard_all.

    @overload
    def himport_prepare_internal(
        self: SyncClientProtocol, fieldset_name: str, fields: Iterable[FieldT]
    ) -> bool: ...

    @overload
    def himport_prepare_internal(
        self: AsyncClientProtocol, fieldset_name: str, fields: Iterable[FieldT]
    ) -> Awaitable[bool]: ...

    def himport_prepare_internal(
        self, fieldset_name: str, fields: Iterable[FieldT]
    ) -> bool | Awaitable[bool]:
        """Send ``HIMPORT PREPARE fieldset_name field [field ...]``.

        Registers the ordered field list under ``fieldset_name`` in the executing
        connection's session. Fields are sent in the caller's order, unmodified.

        .. warning::
            Internal wire mapping — not for direct use on a pooled client. It
            PREPAREs on a single arbitrary borrowed connection and bypasses the
            client's shared fieldset registry, so other connections stay unaware and
            reintroduce the nondeterministic "no such fieldset" the HIMPORT design
            prevents. Use the client's ``himport_prepare`` instead; call this only
            on a pinned single-connection client where you manage session state
            yourself.
        """
        return self.execute_command(HIMPORT_PREPARE, fieldset_name, *fields)

    @overload
    def himport_set(
        self: SyncClientProtocol,
        key: KeyT,
        fieldset_name: str,
        values: Iterable[EncodableT],
    ) -> bool: ...

    @overload
    def himport_set(
        self: AsyncClientProtocol,
        key: KeyT,
        fieldset_name: str,
        values: Iterable[EncodableT],
    ) -> Awaitable[bool]: ...

    @experimental_method()
    def himport_set(
        self, key: KeyT, fieldset_name: str, values: Iterable[EncodableT]
    ) -> bool | Awaitable[bool]:
        """Create/replace ``key`` as a hash using ``fieldset_name``'s field list.

        Values are sent in the caller's order and map positionally to the prepared
        fields. The fieldset is prepared on the serving connection on first use
        (PREPARE bundled with SET in one write) and reused on subsequent calls;
        runtime discards are reconciled on the next use of the connection. All of
        this happens inside the normal command path, so retry, disconnect-on-error
        and (for the cluster) routing / MOVED-ASK apply exactly as for any command.

        .. note::
            Recovery from a mid-connection fieldset loss (the server dropping the
            prepared fieldset without dropping the socket, e.g. ``RESET`` or
            ``maxmemory-clients`` eviction) applies only to a standalone
            ``himport_set`` call: it catches the resulting ``no such fieldset`` and
            re-prepares on the same socket, retrying the SET once. A ``himport_set``
            buffered in a **pipeline or transaction** has no such per-command
            recovery — its fieldset is PREPAREd once as a pre-flight before the
            batch, and if the server loses it between that pre-flight and the batch
            the SET fails and surfaces as an error in the batch results. Callers who
            need the automatic recovery should issue those sets outside a pipeline.
        """
        # A bare str/bytes-like value is iterable element-by-element; splatting it
        # would send single chars/bytes as separate positional values instead of one
        # value. Reject it as a caller mistake, mirroring HImportRegistry's field-list
        # guard.
        if isinstance(values, (str, bytes, bytearray, memoryview)):
            raise DataError(
                "HIMPORT values must be a collection of values, not a string"
            )
        return self.execute_command(
            HIMPORT_SET, key, fieldset_name, *values, keys=[key]
        )

    @overload
    def himport_discard_internal(
        self: SyncClientProtocol, fieldset_name: str
    ) -> int: ...

    @overload
    def himport_discard_internal(
        self: AsyncClientProtocol, fieldset_name: str
    ) -> Awaitable[int]: ...

    def himport_discard_internal(self, fieldset_name: str) -> int | Awaitable[int]:
        """Send ``HIMPORT DISCARD fieldset_name`` (``1`` removed, ``0`` not found).

        .. warning::
            Internal wire mapping — not for direct use on a pooled client. It
            DISCARDs on a single arbitrary borrowed connection and bypasses the
            client's shared fieldset registry, leaving other connections and the
            registry out of sync. Use the client's ``himport_discard`` instead; call
            this only on a pinned single-connection client where you manage session
            state yourself.
        """
        return self.execute_command(HIMPORT_DISCARD, fieldset_name)

    @overload
    def himport_discard_all_internal(self: SyncClientProtocol) -> int: ...

    @overload
    def himport_discard_all_internal(
        self: AsyncClientProtocol,
    ) -> Awaitable[int]: ...

    def himport_discard_all_internal(self) -> int | Awaitable[int]:
        """Send ``HIMPORT DISCARDALL`` (returns the number of fieldsets removed).

        .. warning::
            Internal wire mapping — not for direct use on a pooled client. It
            DISCARDs on a single arbitrary borrowed connection and bypasses the
            client's shared fieldset registry, leaving other connections and the
            registry out of sync. Use the client's ``himport_discard_all`` instead;
            call this only on a pinned single-connection client where you manage
            session state yourself.
        """
        return self.execute_command(HIMPORT_DISCARDALL)


AsyncHashCommands = HashCommands


class Script:
    """
    An executable Lua script object returned by ``register_script``
    """

    def __init__(
        self,
        registered_client: Union["redis.client.Redis", "redis.cluster.RedisCluster"],
        script: ScriptTextT,
    ):
        self.registered_client = registered_client
        self.script = script
        # Precalculate and store the SHA1 hex digest of the script.

        if isinstance(script, str):
            # We need the encoding from the client in order to generate an
            # accurate byte representation of the script
            encoder = self.get_encoder()
            script = encoder.encode(script)
        self.sha = hashlib.sha1(script).hexdigest()

    def __call__(
        self,
        keys: Union[Sequence[KeyT], None] = None,
        args: Union[Iterable[EncodableT], None] = None,
        client: Union["redis.client.Redis", "redis.cluster.RedisCluster", None] = None,
    ):
        """Execute the script, passing any required ``args``"""
        keys = keys or []
        args = args or []
        if client is None:
            client = self.registered_client
        args = tuple(keys) + tuple(args)
        # make sure the Redis server knows about the script
        from redis.client import Pipeline
        from redis.cluster import ClusterPipeline

        if isinstance(client, Pipeline):
            # Make sure the pipeline can register the script before executing.
            client.scripts.add(self)
        if isinstance(client, ClusterPipeline):
            # ClusterPipeline does not support script_load. Queue EVALSHA and
            # leave NOSCRIPT recovery to the caller (same as AsyncScript).
            return client.evalsha(self.sha, len(keys), *args)
        try:
            return client.evalsha(self.sha, len(keys), *args)
        except NoScriptError:
            # Maybe the client is pointed to a different server than the client
            # that created this instance?
            # Overwrite the sha just in case there was a discrepancy.
            self.sha = client.script_load(self.script)
            return client.evalsha(self.sha, len(keys), *args)

    def get_encoder(self):
        """Get the encoder to encode string scripts into bytes."""
        try:
            return self.registered_client.get_encoder()
        except AttributeError:
            # DEPRECATED
            # In version <=4.1.2, this was the code we used to get the encoder.
            # However, after 4.1.2 we added support for scripting in clustered
            # redis. ClusteredRedis doesn't have a `.connection_pool` attribute
            # so we changed the Script class to use
            # `self.registered_client.get_encoder` (see above).
            # However, that is technically a breaking change, as consumers who
            # use Scripts directly might inject a `registered_client` that
            # doesn't have a `.get_encoder` field. This try/except prevents us
            # from breaking backward-compatibility. Ideally, it would be
            # removed in the next major release.
            return self.registered_client.connection_pool.get_encoder()


class AsyncScript:
    """
    An executable Lua script object returned by ``register_script``
    """

    def __init__(
        self,
        registered_client: Union[
            "redis.asyncio.client.Redis", "redis.asyncio.cluster.RedisCluster"
        ],
        script: ScriptTextT,
    ):
        self.registered_client = registered_client
        self.script = script
        # Precalculate and store the SHA1 hex digest of the script.

        if isinstance(script, str):
            # We need the encoding from the client in order to generate an
            # accurate byte representation of the script
            try:
                encoder = registered_client.connection_pool.get_encoder()
            except AttributeError:
                # Cluster
                encoder = registered_client.get_encoder()
            script = encoder.encode(script)
        self.sha = hashlib.sha1(script).hexdigest()

    async def __call__(
        self,
        keys: Union[Sequence[KeyT], None] = None,
        args: Union[Iterable[EncodableT], None] = None,
        client: Union[
            "redis.asyncio.client.Redis", "redis.asyncio.cluster.RedisCluster", None
        ] = None,
    ):
        """Execute the script, passing any required ``args``"""
        keys = keys or []
        args = args or []
        if client is None:
            client = self.registered_client
        args = tuple(keys) + tuple(args)
        # make sure the Redis server knows about the script
        from redis.asyncio.client import Pipeline
        from redis.asyncio.cluster import ClusterPipeline

        if isinstance(client, Pipeline):
            # Make sure the pipeline can register the script before executing.
            client.scripts.add(self)
        if isinstance(client, ClusterPipeline):
            # ClusterPipeline.evalsha queues synchronously. Awaiting the
            # pipeline would call initialize() and drop the queued command.
            return client.evalsha(self.sha, len(keys), *args)
        try:
            return await client.evalsha(self.sha, len(keys), *args)
        except NoScriptError:
            # Maybe the client is pointed to a different server than the client
            # that created this instance?
            # Overwrite the sha just in case there was a discrepancy.
            self.sha = await client.script_load(self.script)
            return await client.evalsha(self.sha, len(keys), *args)


class PubSubCommands(CommandsProtocol):
    """
    Redis PubSub commands.
    see https://redis.io/topics/pubsub
    """

    @overload
    def publish(
        self: SyncClientProtocol, channel: ChannelT, message: EncodableT, **kwargs
    ) -> int: ...

    @overload
    def publish(
        self: AsyncClientProtocol, channel: ChannelT, message: EncodableT, **kwargs
    ) -> Awaitable[int]: ...

    def publish(
        self, channel: ChannelT, message: EncodableT, **kwargs
    ) -> int | Awaitable[int]:
        """
        Publish ``message`` on ``channel``.
        Returns the number of subscribers the message was delivered to.

        For more information, see https://redis.io/commands/publish
        """
        response = self.execute_command("PUBLISH", channel, message, **kwargs)
        record_pubsub_message(
            direction=PubSubDirection.PUBLISH,
            channel=str_if_bytes(channel),
        )
        return response

    @overload
    def spublish(
        self: SyncClientProtocol, shard_channel: ChannelT, message: EncodableT
    ) -> int: ...

    @overload
    def spublish(
        self: AsyncClientProtocol, shard_channel: ChannelT, message: EncodableT
    ) -> Awaitable[int]: ...

    def spublish(
        self, shard_channel: ChannelT, message: EncodableT
    ) -> int | Awaitable[int]:
        """
        Posts a message to the given shard channel.
        Returns the number of clients that received the message

        For more information, see https://redis.io/commands/spublish
        """
        response = self.execute_command("SPUBLISH", shard_channel, message)
        record_pubsub_message(
            direction=PubSubDirection.PUBLISH,
            channel=str_if_bytes(shard_channel),
            sharded=True,
        )
        return response

    @overload
    def pubsub_channels(
        self: SyncClientProtocol, pattern: PatternT = "*", **kwargs
    ) -> list[bytes | str]: ...

    @overload
    def pubsub_channels(
        self: AsyncClientProtocol, pattern: PatternT = "*", **kwargs
    ) -> Awaitable[list[bytes | str]]: ...

    def pubsub_channels(
        self, pattern: PatternT = "*", **kwargs
    ) -> list[bytes | str] | Awaitable[list[bytes | str]]:
        """
        Return a list of channels that have at least one subscriber

        For more information, see https://redis.io/commands/pubsub-channels
        """
        return self.execute_command("PUBSUB CHANNELS", pattern, **kwargs)

    @overload
    def pubsub_shardchannels(
        self: SyncClientProtocol, pattern: PatternT = "*", **kwargs
    ) -> list[bytes | str]: ...

    @overload
    def pubsub_shardchannels(
        self: AsyncClientProtocol, pattern: PatternT = "*", **kwargs
    ) -> Awaitable[list[bytes | str]]: ...

    def pubsub_shardchannels(
        self, pattern: PatternT = "*", **kwargs
    ) -> list[bytes | str] | Awaitable[list[bytes | str]]:
        """
        Return a list of shard_channels that have at least one subscriber

        For more information, see https://redis.io/commands/pubsub-shardchannels
        """
        return self.execute_command("PUBSUB SHARDCHANNELS", pattern, **kwargs)

    @overload
    def pubsub_numpat(self: SyncClientProtocol, **kwargs) -> int: ...

    @overload
    def pubsub_numpat(self: AsyncClientProtocol, **kwargs) -> Awaitable[int]: ...

    def pubsub_numpat(self, **kwargs) -> int | Awaitable[int]:
        """
        Returns the number of subscriptions to patterns

        For more information, see https://redis.io/commands/pubsub-numpat
        """
        return self.execute_command("PUBSUB NUMPAT", **kwargs)

    @overload
    def pubsub_numsub(
        self: SyncClientProtocol, *args: ChannelT, **kwargs
    ) -> list[tuple[bytes | str, int]]: ...

    @overload
    def pubsub_numsub(
        self: AsyncClientProtocol, *args: ChannelT, **kwargs
    ) -> Awaitable[list[tuple[bytes | str, int]]]: ...

    def pubsub_numsub(
        self, *args: ChannelT, **kwargs
    ) -> list[tuple[bytes | str, int]] | Awaitable[list[tuple[bytes | str, int]]]:
        """
        Return a list of (channel, number of subscribers) tuples
        for each channel given in ``*args``

        For more information, see https://redis.io/commands/pubsub-numsub
        """
        return self.execute_command("PUBSUB NUMSUB", *args, **kwargs)

    @overload
    def pubsub_shardnumsub(
        self: SyncClientProtocol, *args: ChannelT, **kwargs
    ) -> list[tuple[bytes | str, int]]: ...

    @overload
    def pubsub_shardnumsub(
        self: AsyncClientProtocol, *args: ChannelT, **kwargs
    ) -> Awaitable[list[tuple[bytes | str, int]]]: ...

    def pubsub_shardnumsub(
        self, *args: ChannelT, **kwargs
    ) -> list[tuple[bytes | str, int]] | Awaitable[list[tuple[bytes | str, int]]]:
        """
        Return a list of (shard_channel, number of subscribers) tuples
        for each channel given in ``*args``

        For more information, see https://redis.io/commands/pubsub-shardnumsub
        """
        return self.execute_command("PUBSUB SHARDNUMSUB", *args, **kwargs)


AsyncPubSubCommands = PubSubCommands


class ScriptCommands(CommandsProtocol):
    """
    Redis Lua script commands. see:
    https://redis.io/ebook/part-3-next-steps/chapter-11-scripting-redis-with-lua/
    """

    def _eval(
        self,
        command: str,
        script: str,
        numkeys: int,
        *keys_and_args: Union[KeyT, EncodableT],
    ) -> Any:
        return self.execute_command(command, script, numkeys, *keys_and_args)

    @overload
    def eval(
        self: SyncClientProtocol,
        script: str,
        numkeys: int,
        *keys_and_args: KeyT | EncodableT,
    ) -> Any: ...

    @overload
    def eval(
        self: AsyncClientProtocol,
        script: str,
        numkeys: int,
        *keys_and_args: KeyT | EncodableT,
    ) -> Awaitable[Any]: ...

    def eval(
        self, script: str, numkeys: int, *keys_and_args: KeyT | EncodableT
    ) -> Any | Awaitable[Any]:
        """
        Execute the Lua ``script``, specifying the ``numkeys`` the script
        will touch and the key names and argument values in ``keys_and_args``.
        Returns the result of the script.

        In practice, use the object returned by ``register_script``. This
        function exists purely for Redis API completion.

        For more information, see  https://redis.io/commands/eval
        """
        return self._eval("EVAL", script, numkeys, *keys_and_args)

    @overload
    def eval_ro(
        self: SyncClientProtocol,
        script: str,
        numkeys: int,
        *keys_and_args: KeyT | EncodableT,
    ) -> Any: ...

    @overload
    def eval_ro(
        self: AsyncClientProtocol,
        script: str,
        numkeys: int,
        *keys_and_args: KeyT | EncodableT,
    ) -> Awaitable[Any]: ...

    def eval_ro(
        self, script: str, numkeys: int, *keys_and_args: KeyT | EncodableT
    ) -> Any | Awaitable[Any]:
        """
        The read-only variant of the EVAL command

        Execute the read-only Lua ``script`` specifying the ``numkeys`` the script
        will touch and the key names and argument values in ``keys_and_args``.
        Returns the result of the script.

        For more information, see  https://redis.io/commands/eval_ro
        """
        return self._eval("EVAL_RO", script, numkeys, *keys_and_args)

    def _evalsha(
        self,
        command: str,
        sha: str,
        numkeys: int,
        *keys_and_args: Union[KeyT, EncodableT],
    ) -> Any:
        return self.execute_command(command, sha, numkeys, *keys_and_args)

    @overload
    def evalsha(
        self: SyncClientProtocol,
        sha: str,
        numkeys: int,
        *keys_and_args: KeyT | EncodableT,
    ) -> Any: ...

    @overload
    def evalsha(
        self: AsyncClientProtocol,
        sha: str,
        numkeys: int,
        *keys_and_args: KeyT | EncodableT,
    ) -> Awaitable[Any]: ...

    def evalsha(
        self, sha: str, numkeys: int, *keys_and_args: KeyT | EncodableT
    ) -> Any | Awaitable[Any]:
        """
        Use the ``sha`` to execute a Lua script already registered via EVAL
        or SCRIPT LOAD. Specify the ``numkeys`` the script will touch and the
        key names and argument values in ``keys_and_args``. Returns the result
        of the script.

        In practice, use the object returned by ``register_script``. This
        function exists purely for Redis API completion.

        For more information, see  https://redis.io/commands/evalsha
        """
        return self._evalsha("EVALSHA", sha, numkeys, *keys_and_args)

    @overload
    def evalsha_ro(
        self: SyncClientProtocol,
        sha: str,
        numkeys: int,
        *keys_and_args: KeyT | EncodableT,
    ) -> Any: ...

    @overload
    def evalsha_ro(
        self: AsyncClientProtocol,
        sha: str,
        numkeys: int,
        *keys_and_args: KeyT | EncodableT,
    ) -> Awaitable[Any]: ...

    def evalsha_ro(
        self, sha: str, numkeys: int, *keys_and_args: KeyT | EncodableT
    ) -> Any | Awaitable[Any]:
        """
        The read-only variant of the EVALSHA command

        Use the ``sha`` to execute a read-only Lua script already registered via EVAL
        or SCRIPT LOAD. Specify the ``numkeys`` the script will touch and the
        key names and argument values in ``keys_and_args``. Returns the result
        of the script.

        For more information, see  https://redis.io/commands/evalsha_ro
        """
        return self._evalsha("EVALSHA_RO", sha, numkeys, *keys_and_args)

    @overload
    def script_exists(self: SyncClientProtocol, *args: str) -> list[bool]: ...

    @overload
    def script_exists(
        self: AsyncClientProtocol, *args: str
    ) -> Awaitable[list[bool]]: ...

    def script_exists(self, *args: str) -> list[bool] | Awaitable[list[bool]]:
        """
        Check if a script exists in the script cache by specifying the SHAs of
        each script as ``args``. Returns a list of boolean values indicating if
        if each already script exists in the cache_data.

        For more information, see  https://redis.io/commands/script-exists
        """
        return self.execute_command("SCRIPT EXISTS", *args)

    def script_debug(self, *args) -> None:
        raise NotImplementedError(
            "SCRIPT DEBUG is intentionally not implemented in the client."
        )

    @overload
    def script_flush(
        self: SyncClientProtocol,
        sync_type: Literal["SYNC"] | Literal["ASYNC"] | None = None,
    ) -> bool: ...

    @overload
    def script_flush(
        self: AsyncClientProtocol,
        sync_type: Literal["SYNC"] | Literal["ASYNC"] | None = None,
    ) -> Awaitable[bool]: ...

    def script_flush(
        self, sync_type: Literal["SYNC"] | Literal["ASYNC"] | None = None
    ) -> bool | Awaitable[bool]:
        """Flush all scripts from the script cache_data.

        ``sync_type`` is by default SYNC (synchronous) but it can also be
                      ASYNC.

        For more information, see  https://redis.io/commands/script-flush
        """

        # Redis pre 6 had no sync_type.
        if sync_type not in ["SYNC", "ASYNC", None]:
            raise DataError(
                "SCRIPT FLUSH defaults to SYNC in redis > 6.2, or "
                "accepts SYNC/ASYNC. For older versions, "
                "of redis leave as None."
            )
        if sync_type is None:
            pieces = []
        else:
            pieces = [sync_type]
        return self.execute_command("SCRIPT FLUSH", *pieces)

    @overload
    def script_kill(self: SyncClientProtocol) -> bool: ...

    @overload
    def script_kill(self: AsyncClientProtocol) -> Awaitable[bool]: ...

    def script_kill(self) -> bool | Awaitable[bool]:
        """
        Kill the currently executing Lua script

        For more information, see https://redis.io/commands/script-kill
        """
        return self.execute_command("SCRIPT KILL")

    @overload
    def script_load(self: SyncClientProtocol, script: ScriptTextT) -> str: ...

    @overload
    def script_load(
        self: AsyncClientProtocol, script: ScriptTextT
    ) -> Awaitable[str]: ...

    def script_load(self, script: ScriptTextT) -> str | Awaitable[str]:
        """
        Load a Lua ``script`` into the script cache_data. Returns the SHA.

        For more information, see https://redis.io/commands/script-load
        """
        return self.execute_command("SCRIPT LOAD", script)

    def register_script(
        self: Union["redis.client.Redis", "redis.cluster.RedisCluster"],
        script: ScriptTextT,
    ) -> Script:
        """
        Register a Lua ``script`` specifying the ``keys`` it will touch.
        Returns a Script object that is callable and hides the complexity of
        deal with scripts, keys, and shas. This is the preferred way to work
        with Lua scripts.
        """
        return Script(self, script)


class AsyncScriptCommands(ScriptCommands):
    async def script_debug(self, *args) -> None:
        return super().script_debug()

    def register_script(
        self: Union["redis.asyncio.client.Redis", "redis.asyncio.cluster.RedisCluster"],
        script: ScriptTextT,
    ) -> AsyncScript:
        """
        Register a Lua ``script`` specifying the ``keys`` it will touch.
        Returns a Script object that is callable and hides the complexity of
        deal with scripts, keys, and shas. This is the preferred way to work
        with Lua scripts.
        """
        return AsyncScript(self, script)


class GeoCommands(CommandsProtocol):
    """
    Redis Geospatial commands.
    see: https://redis.com/redis-best-practices/indexing-patterns/geospatial/
    """

    @overload
    def geoadd(
        self: SyncClientProtocol,
        name: KeyT,
        values: Sequence[EncodableT],
        nx: bool = False,
        xx: bool = False,
        ch: bool = False,
    ) -> int: ...

    @overload
    def geoadd(
        self: AsyncClientProtocol,
        name: KeyT,
        values: Sequence[EncodableT],
        nx: bool = False,
        xx: bool = False,
        ch: bool = False,
    ) -> Awaitable[int]: ...

    def geoadd(
        self,
        name: KeyT,
        values: Sequence[EncodableT],
        nx: bool = False,
        xx: bool = False,
        ch: bool = False,
    ) -> int | Awaitable[int]:
        """
        Add the specified geospatial items to the specified key identified
        by the ``name`` argument. The Geospatial items are given as ordered
        members of the ``values`` argument, each item or place is formed by
        the triad longitude, latitude and name.

        Note: You can use ZREM to remove elements.

        ``nx`` forces ZADD to only create new elements and not to update
        scores for elements that already exist.

        ``xx`` forces ZADD to only update scores of elements that already
        exist. New elements will not be added.

        ``ch`` modifies the return value to be the numbers of elements changed.
        Changed elements include new elements that were added and elements
        whose scores changed.

        For more information, see https://redis.io/commands/geoadd
        """
        if nx and xx:
            raise DataError("GEOADD allows either 'nx' or 'xx', not both")
        if len(values) % 3 != 0:
            raise DataError("GEOADD requires places with lon, lat and name values")
        pieces = [name]
        if nx:
            pieces.append("NX")
        if xx:
            pieces.append("XX")
        if ch:
            pieces.append("CH")
        pieces.extend(values)
        return self.execute_command("GEOADD", *pieces)

    @overload
    def geodist(
        self: SyncClientProtocol,
        name: KeyT,
        place1: FieldT,
        place2: FieldT,
        unit: str | None = None,
    ) -> float | None: ...

    @overload
    def geodist(
        self: AsyncClientProtocol,
        name: KeyT,
        place1: FieldT,
        place2: FieldT,
        unit: str | None = None,
    ) -> Awaitable[float | None]: ...

    def geodist(
        self, name: KeyT, place1: FieldT, place2: FieldT, unit: str | None = None
    ) -> (float | None) | Awaitable[float | None]:
        """
        Return the distance between ``place1`` and ``place2`` members of the
        ``name`` key.
        The units must be one of the following : m, km mi, ft. By default
        meters are used.

        For more information, see https://redis.io/commands/geodist
        """
        pieces: list[EncodableT] = [name, place1, place2]
        if unit and unit not in ("m", "km", "mi", "ft"):
            raise DataError("GEODIST invalid unit")
        elif unit:
            pieces.append(unit)
        return self.execute_command("GEODIST", *pieces, keys=[name])

    @overload
    def geohash(
        self: SyncClientProtocol, name: KeyT, *values: FieldT
    ) -> list[bytes | str | None]: ...

    @overload
    def geohash(
        self: AsyncClientProtocol, name: KeyT, *values: FieldT
    ) -> Awaitable[list[bytes | str | None]]: ...

    def geohash(
        self, name: KeyT, *values: FieldT
    ) -> list[bytes | str | None] | Awaitable[list[bytes | str | None]]:
        """
        Return the geo hash string for each item of ``values`` members of
        the specified key identified by the ``name`` argument.

        For more information, see https://redis.io/commands/geohash
        """
        return self.execute_command("GEOHASH", name, *values, keys=[name])

    @overload
    def geopos(
        self: SyncClientProtocol, name: KeyT, *values: FieldT
    ) -> list[GeoCoordinate | None]: ...

    @overload
    def geopos(
        self: AsyncClientProtocol, name: KeyT, *values: FieldT
    ) -> Awaitable[list[GeoCoordinate | None]]: ...

    def geopos(
        self, name: KeyT, *values: FieldT
    ) -> list[GeoCoordinate | None] | Awaitable[list[GeoCoordinate | None]]:
        """
        Return the positions of each item of ``values`` as members of
        the specified key identified by the ``name`` argument. Each position
        is represented by the pairs lon and lat.

        For more information, see https://redis.io/commands/geopos
        """
        return self.execute_command("GEOPOS", name, *values, keys=[name])

    @overload
    def georadius(
        self: SyncClientProtocol,
        name: KeyT,
        longitude: float,
        latitude: float,
        radius: float,
        unit: str | None = None,
        withdist: bool = False,
        withcoord: bool = False,
        withhash: bool = False,
        count: int | None = None,
        sort: str | None = None,
        store: KeyT | None = None,
        store_dist: KeyT | None = None,
        any: bool = False,
    ) -> GeoRadiusResponse: ...

    @overload
    def georadius(
        self: AsyncClientProtocol,
        name: KeyT,
        longitude: float,
        latitude: float,
        radius: float,
        unit: str | None = None,
        withdist: bool = False,
        withcoord: bool = False,
        withhash: bool = False,
        count: int | None = None,
        sort: str | None = None,
        store: KeyT | None = None,
        store_dist: KeyT | None = None,
        any: bool = False,
    ) -> Awaitable[GeoRadiusResponse]: ...

    def georadius(
        self,
        name: KeyT,
        longitude: float,
        latitude: float,
        radius: float,
        unit: str | None = None,
        withdist: bool = False,
        withcoord: bool = False,
        withhash: bool = False,
        count: int | None = None,
        sort: str | None = None,
        store: KeyT | None = None,
        store_dist: KeyT | None = None,
        any: bool = False,
    ) -> GeoRadiusResponse | Awaitable[GeoRadiusResponse]:
        """
        Return the members of the specified key identified by the
        ``name`` argument which are within the borders of the area specified
        with the ``latitude`` and ``longitude`` location and the maximum
        distance from the center specified by the ``radius`` value.

        The units must be one of the following : m, km mi, ft. By default

        ``withdist`` indicates to return the distances of each place.

        ``withcoord`` indicates to return the latitude and longitude of
        each place.

        ``withhash`` indicates to return the geohash string of each place.

        ``count`` indicates to return the number of elements up to N.

        ``sort`` indicates to return the places in a sorted way, ASC for
        nearest to fairest and DESC for fairest to nearest.

        ``store`` indicates to save the places names in a sorted set named
        with a specific key, each element of the destination sorted set is
        populated with the score got from the original geo sorted set.

        ``store_dist`` indicates to save the places names in a sorted set
        named with a specific key, instead of ``store`` the sorted set
        destination score is set with the distance.

        For more information, see https://redis.io/commands/georadius
        """
        return self._georadiusgeneric(
            "GEORADIUS",
            name,
            longitude,
            latitude,
            radius,
            unit=unit,
            withdist=withdist,
            withcoord=withcoord,
            withhash=withhash,
            count=count,
            sort=sort,
            store=store,
            store_dist=store_dist,
            any=any,
        )

    @overload
    def georadiusbymember(
        self: SyncClientProtocol,
        name: KeyT,
        member: FieldT,
        radius: float,
        unit: str | None = None,
        withdist: bool = False,
        withcoord: bool = False,
        withhash: bool = False,
        count: int | None = None,
        sort: str | None = None,
        store: KeyT | None = None,
        store_dist: KeyT | None = None,
        any: bool = False,
    ) -> GeoRadiusResponse: ...

    @overload
    def georadiusbymember(
        self: AsyncClientProtocol,
        name: KeyT,
        member: FieldT,
        radius: float,
        unit: str | None = None,
        withdist: bool = False,
        withcoord: bool = False,
        withhash: bool = False,
        count: int | None = None,
        sort: str | None = None,
        store: KeyT | None = None,
        store_dist: KeyT | None = None,
        any: bool = False,
    ) -> Awaitable[GeoRadiusResponse]: ...

    def georadiusbymember(
        self,
        name: KeyT,
        member: FieldT,
        radius: float,
        unit: str | None = None,
        withdist: bool = False,
        withcoord: bool = False,
        withhash: bool = False,
        count: int | None = None,
        sort: str | None = None,
        store: Union[KeyT, None] = None,
        store_dist: Union[KeyT, None] = None,
        any: bool = False,
    ) -> GeoRadiusResponse | Awaitable[GeoRadiusResponse]:
        """
        This command is exactly like ``georadius`` with the sole difference
        that instead of taking, as the center of the area to query, a longitude
        and latitude value, it takes the name of a member already existing
        inside the geospatial index represented by the sorted set.

        For more information, see https://redis.io/commands/georadiusbymember
        """
        return self._georadiusgeneric(
            "GEORADIUSBYMEMBER",
            name,
            member,
            radius,
            unit=unit,
            withdist=withdist,
            withcoord=withcoord,
            withhash=withhash,
            count=count,
            sort=sort,
            store=store,
            store_dist=store_dist,
            any=any,
        )

    def _georadiusgeneric(
        self, command: str, *args: EncodableT, **kwargs: Union[EncodableT, None]
    ) -> ResponseT:
        pieces = list(args)
        if kwargs["unit"] and kwargs["unit"] not in ("m", "km", "mi", "ft"):
            raise DataError("GEORADIUS invalid unit")
        elif kwargs["unit"]:
            pieces.append(kwargs["unit"])
        else:
            pieces.append("m")

        if kwargs["any"] and kwargs["count"] is None:
            raise DataError("``any`` can't be provided without ``count``")

        for arg_name, byte_repr in (
            ("withdist", "WITHDIST"),
            ("withcoord", "WITHCOORD"),
            ("withhash", "WITHHASH"),
        ):
            if kwargs[arg_name]:
                pieces.append(byte_repr)

        if kwargs["count"] is not None:
            pieces.extend(["COUNT", kwargs["count"]])
            if kwargs["any"]:
                pieces.append("ANY")

        if kwargs["sort"]:
            if kwargs["sort"] == "ASC":
                pieces.append("ASC")
            elif kwargs["sort"] == "DESC":
                pieces.append("DESC")
            else:
                raise DataError("GEORADIUS invalid sort")

        if kwargs["store"] and kwargs["store_dist"]:
            raise DataError("GEORADIUS store and store_dist can't be set together")

        if kwargs["store"]:
            pieces.extend([b"STORE", kwargs["store"]])

        if kwargs["store_dist"]:
            pieces.extend([b"STOREDIST", kwargs["store_dist"]])

        return self.execute_command(command, *pieces, **kwargs)

    @overload
    def geosearch(
        self: SyncClientProtocol,
        name: KeyT,
        member: FieldT | None = None,
        longitude: float | None = None,
        latitude: float | None = None,
        unit: str = "m",
        radius: float | None = None,
        width: float | None = None,
        height: float | None = None,
        sort: str | None = None,
        count: int | None = None,
        any: bool = False,
        withcoord: bool = False,
        withdist: bool = False,
        withhash: bool = False,
    ) -> GeoSearchResponse: ...

    @overload
    def geosearch(
        self: AsyncClientProtocol,
        name: KeyT,
        member: FieldT | None = None,
        longitude: float | None = None,
        latitude: float | None = None,
        unit: str = "m",
        radius: float | None = None,
        width: float | None = None,
        height: float | None = None,
        sort: str | None = None,
        count: int | None = None,
        any: bool = False,
        withcoord: bool = False,
        withdist: bool = False,
        withhash: bool = False,
    ) -> Awaitable[GeoSearchResponse]: ...

    def geosearch(
        self,
        name: KeyT,
        member: FieldT | None = None,
        longitude: float | None = None,
        latitude: float | None = None,
        unit: str = "m",
        radius: float | None = None,
        width: float | None = None,
        height: float | None = None,
        sort: str | None = None,
        count: int | None = None,
        any: bool = False,
        withcoord: bool = False,
        withdist: bool = False,
        withhash: bool = False,
    ) -> GeoSearchResponse | Awaitable[GeoSearchResponse]:
        """
        Return the members of specified key identified by the
        ``name`` argument, which are within the borders of the
        area specified by a given shape. This command extends the
        GEORADIUS command, so in addition to searching within circular
        areas, it supports searching within rectangular areas.

        This command should be used in place of the deprecated
        GEORADIUS and GEORADIUSBYMEMBER commands.

        ``member`` Use the position of the given existing
         member in the sorted set. Can't be given with ``longitude``
         and ``latitude``.

        ``longitude`` and ``latitude`` Use the position given by
        this coordinates. Can't be given with ``member``
        ``radius`` Similar to GEORADIUS, search inside circular
        area according the given radius. Can't be given with
        ``height`` and ``width``.
        ``height`` and ``width`` Search inside an axis-aligned
        rectangle, determined by the given height and width.
        Can't be given with ``radius``

        ``unit`` must be one of the following : m, km, mi, ft.
        `m` for meters (the default value), `km` for kilometers,
        `mi` for miles and `ft` for feet.

        ``sort`` indicates to return the places in a sorted way,
        ASC for nearest to furthest and DESC for furthest to nearest.

        ``count`` limit the results to the first count matching items.

        ``any`` is set to True, the command will return as soon as
        enough matches are found. Can't be provided without ``count``

        ``withdist`` indicates to return the distances of each place.
        ``withcoord`` indicates to return the latitude and longitude of
        each place.

        ``withhash`` indicates to return the geohash string of each place.

        For more information, see https://redis.io/commands/geosearch
        """

        return self._geosearchgeneric(
            "GEOSEARCH",
            name,
            member=member,
            longitude=longitude,
            latitude=latitude,
            unit=unit,
            radius=radius,
            width=width,
            height=height,
            sort=sort,
            count=count,
            any=any,
            withcoord=withcoord,
            withdist=withdist,
            withhash=withhash,
            store=None,
            store_dist=None,
        )

    @overload
    def geosearchstore(
        self: SyncClientProtocol,
        dest: KeyT,
        name: KeyT,
        member: FieldT | None = None,
        longitude: float | None = None,
        latitude: float | None = None,
        unit: str = "m",
        radius: float | None = None,
        width: float | None = None,
        height: float | None = None,
        sort: str | None = None,
        count: int | None = None,
        any: bool = False,
        storedist: bool = False,
    ) -> int: ...

    @overload
    def geosearchstore(
        self: AsyncClientProtocol,
        dest: KeyT,
        name: KeyT,
        member: FieldT | None = None,
        longitude: float | None = None,
        latitude: float | None = None,
        unit: str = "m",
        radius: float | None = None,
        width: float | None = None,
        height: float | None = None,
        sort: str | None = None,
        count: int | None = None,
        any: bool = False,
        storedist: bool = False,
    ) -> Awaitable[int]: ...

    def geosearchstore(
        self,
        dest: KeyT,
        name: KeyT,
        member: FieldT | None = None,
        longitude: float | None = None,
        latitude: float | None = None,
        unit: str = "m",
        radius: float | None = None,
        width: float | None = None,
        height: float | None = None,
        sort: str | None = None,
        count: int | None = None,
        any: bool = False,
        storedist: bool = False,
    ) -> int | Awaitable[int]:
        """
        This command is like GEOSEARCH, but stores the result in
        ``dest``. By default, it stores the results in the destination
        sorted set with their geospatial information.
        if ``store_dist`` set to True, the command will stores the
        items in a sorted set populated with their distance from the
        center of the circle or box, as a floating-point number.

        For more information, see https://redis.io/commands/geosearchstore
        """
        return self._geosearchgeneric(
            "GEOSEARCHSTORE",
            dest,
            name,
            member=member,
            longitude=longitude,
            latitude=latitude,
            unit=unit,
            radius=radius,
            width=width,
            height=height,
            sort=sort,
            count=count,
            any=any,
            withcoord=None,
            withdist=None,
            withhash=None,
            store=None,
            store_dist=storedist,
        )

    def _geosearchgeneric(
        self, command: str, *args: EncodableT, **kwargs: Union[EncodableT, None]
    ) -> ResponseT:
        pieces = list(args)

        # FROMMEMBER or FROMLONLAT
        if kwargs["member"] is None:
            if kwargs["longitude"] is None or kwargs["latitude"] is None:
                raise DataError("GEOSEARCH must have member or longitude and latitude")
        if kwargs["member"] is not None:
            if kwargs["longitude"] is not None or kwargs["latitude"] is not None:
                raise DataError(
                    "GEOSEARCH member and longitude or latitude can't be set together"
                )
            pieces.extend([b"FROMMEMBER", kwargs["member"]])
        if kwargs["longitude"] is not None and kwargs["latitude"] is not None:
            pieces.extend([b"FROMLONLAT", kwargs["longitude"], kwargs["latitude"]])

        # BYRADIUS or BYBOX
        if kwargs["radius"] is None:
            if kwargs["width"] is None or kwargs["height"] is None:
                raise DataError("GEOSEARCH must have radius or width and height")
        if kwargs["unit"] is None:
            raise DataError("GEOSEARCH must have unit")
        if kwargs["unit"].lower() not in ("m", "km", "mi", "ft"):
            raise DataError("GEOSEARCH invalid unit")
        if kwargs["radius"] is not None:
            if kwargs["width"] is not None or kwargs["height"] is not None:
                raise DataError(
                    "GEOSEARCH radius and width or height can't be set together"
                )
            pieces.extend([b"BYRADIUS", kwargs["radius"], kwargs["unit"]])
        if kwargs["width"] is not None and kwargs["height"] is not None:
            pieces.extend([b"BYBOX", kwargs["width"], kwargs["height"], kwargs["unit"]])

        # sort
        if kwargs["sort"]:
            if kwargs["sort"].upper() == "ASC":
                pieces.append(b"ASC")
            elif kwargs["sort"].upper() == "DESC":
                pieces.append(b"DESC")
            else:
                raise DataError("GEOSEARCH invalid sort")

        # count any
        if kwargs["count"] is not None:
            pieces.extend([b"COUNT", kwargs["count"]])
            if kwargs["any"]:
                pieces.append(b"ANY")
        elif kwargs["any"]:
            raise DataError("GEOSEARCH ``any`` can't be provided without count")

        # other properties
        for arg_name, byte_repr in (
            ("withdist", b"WITHDIST"),
            ("withcoord", b"WITHCOORD"),
            ("withhash", b"WITHHASH"),
            ("store_dist", b"STOREDIST"),
        ):
            if kwargs[arg_name]:
                pieces.append(byte_repr)

        kwargs["keys"] = [args[0] if command == "GEOSEARCH" else args[1]]

        return self.execute_command(command, *pieces, **kwargs)


AsyncGeoCommands = GeoCommands


class ModuleCommands(CommandsProtocol):
    """
    Redis Module commands.
    see: https://redis.io/topics/modules-intro
    """

    @overload
    def module_load(self: SyncClientProtocol, path, *args) -> bool: ...

    @overload
    def module_load(self: AsyncClientProtocol, path, *args) -> Awaitable[bool]: ...

    def module_load(self, path, *args) -> bool | Awaitable[bool]:
        """
        Loads the module from ``path``.
        Passes all ``*args`` to the module, during loading.
        Raises ``ModuleError`` if a module is not found at ``path``.

        For more information, see https://redis.io/commands/module-load
        """
        return self.execute_command("MODULE LOAD", path, *args)

    @overload
    def module_loadex(
        self: SyncClientProtocol,
        path: str,
        options: List[str] | None = None,
        args: List[str] | None = None,
    ) -> bytes | str: ...

    @overload
    def module_loadex(
        self: AsyncClientProtocol,
        path: str,
        options: List[str] | None = None,
        args: List[str] | None = None,
    ) -> Awaitable[bytes | str]: ...

    def module_loadex(
        self,
        path: str,
        options: List[str] | None = None,
        args: List[str] | None = None,
    ) -> (bytes | str) | Awaitable[bytes | str]:
        """
        Loads a module from a dynamic library at runtime with configuration directives.

        For more information, see https://redis.io/commands/module-loadex
        """
        pieces = []
        if options is not None:
            pieces.append("CONFIG")
            pieces.extend(options)
        if args is not None:
            pieces.append("ARGS")
            pieces.extend(args)

        return self.execute_command("MODULE LOADEX", path, *pieces)

    @overload
    def module_unload(self: SyncClientProtocol, name) -> bool: ...

    @overload
    def module_unload(self: AsyncClientProtocol, name) -> Awaitable[bool]: ...

    def module_unload(self, name) -> bool | Awaitable[bool]:
        """
        Unloads the module ``name``.
        Raises ``ModuleError`` if ``name`` is not in loaded modules.

        For more information, see https://redis.io/commands/module-unload
        """
        return self.execute_command("MODULE UNLOAD", name)

    @overload
    def module_list(self: SyncClientProtocol) -> list[dict[Any, Any]]: ...

    @overload
    def module_list(self: AsyncClientProtocol) -> Awaitable[list[dict[Any, Any]]]: ...

    def module_list(self) -> list[dict[Any, Any]] | Awaitable[list[dict[Any, Any]]]:
        """
        Returns a list of dictionaries containing the name and version of
        all loaded modules.

        For more information, see https://redis.io/commands/module-list
        """
        return self.execute_command("MODULE LIST")

    def command_info(self) -> None:
        raise NotImplementedError(
            "COMMAND INFO is intentionally not implemented in the client."
        )

    @overload
    def command_count(self: SyncClientProtocol) -> int: ...

    @overload
    def command_count(self: AsyncClientProtocol) -> Awaitable[int]: ...

    def command_count(self) -> int | Awaitable[int]:
        return self.execute_command("COMMAND COUNT")

    @overload
    def command_getkeys(self: SyncClientProtocol, *args) -> list[bytes | str]: ...

    @overload
    def command_getkeys(
        self: AsyncClientProtocol, *args
    ) -> Awaitable[list[bytes | str]]: ...

    def command_getkeys(
        self, *args
    ) -> list[bytes | str] | Awaitable[list[bytes | str]]:
        return self.execute_command("COMMAND GETKEYS", *args)

    @overload
    def command(self: SyncClientProtocol) -> dict[str, dict[str, Any]]: ...

    @overload
    def command(
        self: AsyncClientProtocol,
    ) -> Awaitable[dict[str, dict[str, Any]]]: ...

    def command(
        self,
    ) -> dict[str, dict[str, Any]] | Awaitable[dict[str, dict[str, Any]]]:
        return self.execute_command("COMMAND")


class AsyncModuleCommands(ModuleCommands):
    async def command_info(self) -> None:
        return super().command_info()


class ClusterCommands(CommandsProtocol):
    """
    Class for Redis Cluster commands
    """

    @overload
    def cluster(self: SyncClientProtocol, cluster_arg, *args, **kwargs) -> Any: ...

    @overload
    def cluster(
        self: AsyncClientProtocol, cluster_arg, *args, **kwargs
    ) -> Awaitable[Any]: ...

    def cluster(self, cluster_arg, *args, **kwargs) -> Any | Awaitable[Any]:
        return self.execute_command(f"CLUSTER {cluster_arg.upper()}", *args, **kwargs)

    @overload
    def readwrite(self: SyncClientProtocol, **kwargs) -> bool: ...

    @overload
    def readwrite(self: AsyncClientProtocol, **kwargs) -> Awaitable[bool]: ...

    def readwrite(self, **kwargs) -> bool | Awaitable[bool]:
        """
        Disables read queries for a connection to a Redis Cluster slave node.

        For more information, see https://redis.io/commands/readwrite
        """
        return self.execute_command("READWRITE", **kwargs)

    @overload
    def readonly(self: SyncClientProtocol, **kwargs) -> bool: ...

    @overload
    def readonly(self: AsyncClientProtocol, **kwargs) -> Awaitable[bool]: ...

    def readonly(self, **kwargs) -> bool | Awaitable[bool]:
        """
        Enables read queries for a connection to a Redis Cluster replica node.

        For more information, see https://redis.io/commands/readonly
        """
        return self.execute_command("READONLY", **kwargs)


AsyncClusterCommands = ClusterCommands


class FunctionCommands:
    """
    Redis Function commands
    """

    @overload
    def function_load(
        self: SyncClientProtocol, code: str, replace: bool | None = False
    ) -> bytes | str: ...

    @overload
    def function_load(
        self: AsyncClientProtocol, code: str, replace: bool | None = False
    ) -> Awaitable[bytes | str]: ...

    def function_load(self, code: str, replace: bool | None = False) -> (
        bytes | str
    ) | Awaitable[bytes | str]:
        """
        Load a library to Redis.
        :param code: the source code (must start with
        Shebang statement that provides a metadata about the library)
        :param replace: changes the behavior to overwrite the existing library
        with the new contents.
        Return the library name that was loaded.

        For more information, see https://redis.io/commands/function-load
        """
        pieces = ["REPLACE"] if replace else []
        pieces.append(code)
        return self.execute_command("FUNCTION LOAD", *pieces)

    @overload
    def function_delete(self: SyncClientProtocol, library: str) -> bool: ...

    @overload
    def function_delete(self: AsyncClientProtocol, library: str) -> Awaitable[bool]: ...

    def function_delete(self, library: str) -> bool | Awaitable[bool]:
        """
        Delete the library called ``library`` and all its functions.

        For more information, see https://redis.io/commands/function-delete
        """
        return self.execute_command("FUNCTION DELETE", library)

    @overload
    def function_flush(self: SyncClientProtocol, mode: str = "SYNC") -> bool: ...

    @overload
    def function_flush(
        self: AsyncClientProtocol, mode: str = "SYNC"
    ) -> Awaitable[bool]: ...

    def function_flush(self, mode: str = "SYNC") -> bool | Awaitable[bool]:
        """
        Deletes all the libraries.

        For more information, see https://redis.io/commands/function-flush
        """
        return self.execute_command("FUNCTION FLUSH", mode)

    @overload
    def function_list(
        self: SyncClientProtocol,
        library: str | None = "*",
        withcode: bool | None = False,
    ) -> list[Any]: ...

    @overload
    def function_list(
        self: AsyncClientProtocol,
        library: str | None = "*",
        withcode: bool | None = False,
    ) -> Awaitable[list[Any]]: ...

    def function_list(
        self, library: str | None = "*", withcode: bool | None = False
    ) -> list[Any] | Awaitable[list[Any]]:
        """
        Return information about the functions and libraries.

        Args:

            library: specify a pattern for matching library names
            withcode: cause the server to include the libraries source implementation
                in the reply
        """
        args = ["LIBRARYNAME", library]
        if withcode:
            args.append("WITHCODE")
        return self.execute_command("FUNCTION LIST", *args)

    def _fcall(self, command: str, function, numkeys: int, *keys_and_args: Any) -> Any:
        return self.execute_command(command, function, numkeys, *keys_and_args)

    @overload
    def fcall(
        self: SyncClientProtocol, function, numkeys: int, *keys_and_args: Any
    ) -> Any: ...

    @overload
    def fcall(
        self: AsyncClientProtocol, function, numkeys: int, *keys_and_args: Any
    ) -> Awaitable[Any]: ...

    def fcall(
        self, function, numkeys: int, *keys_and_args: Any
    ) -> Any | Awaitable[Any]:
        """
        Invoke a function.

        For more information, see https://redis.io/commands/fcall
        """
        return self._fcall("FCALL", function, numkeys, *keys_and_args)

    @overload
    def fcall_ro(
        self: SyncClientProtocol, function, numkeys: int, *keys_and_args: Any
    ) -> Any: ...

    @overload
    def fcall_ro(
        self: AsyncClientProtocol, function, numkeys: int, *keys_and_args: Any
    ) -> Awaitable[Any]: ...

    def fcall_ro(
        self, function, numkeys: int, *keys_and_args: Any
    ) -> Any | Awaitable[Any]:
        """
        This is a read-only variant of the FCALL command that cannot
        execute commands that modify data.

        For more information, see https://redis.io/commands/fcall_ro
        """
        return self._fcall("FCALL_RO", function, numkeys, *keys_and_args)

    @overload
    def function_dump(self: SyncClientProtocol) -> bytes: ...

    @overload
    def function_dump(self: AsyncClientProtocol) -> Awaitable[bytes]: ...

    def function_dump(self) -> bytes | Awaitable[bytes]:
        """
        Return the serialized payload of loaded libraries.

        For more information, see https://redis.io/commands/function-dump
        """
        from redis.client import NEVER_DECODE

        options = {}
        options[NEVER_DECODE] = []

        return self.execute_command("FUNCTION DUMP", **options)

    @overload
    def function_restore(
        self: SyncClientProtocol, payload: str, policy: str | None = "APPEND"
    ) -> bool: ...

    @overload
    def function_restore(
        self: AsyncClientProtocol, payload: str, policy: str | None = "APPEND"
    ) -> Awaitable[bool]: ...

    def function_restore(
        self, payload: str, policy: str | None = "APPEND"
    ) -> bool | Awaitable[bool]:
        """
        Restore libraries from the serialized ``payload``.
        You can use the optional policy argument to provide a policy
        for handling existing libraries.

        For more information, see https://redis.io/commands/function-restore
        """
        return self.execute_command("FUNCTION RESTORE", payload, policy)

    @overload
    def function_kill(self: SyncClientProtocol) -> bytes | str: ...

    @overload
    def function_kill(self: AsyncClientProtocol) -> Awaitable[bytes | str]: ...

    def function_kill(self) -> (bytes | str) | Awaitable[bytes | str]:
        """
        Kill a function that is currently executing.

        For more information, see https://redis.io/commands/function-kill
        """
        return self.execute_command("FUNCTION KILL")

    @overload
    def function_stats(self: SyncClientProtocol) -> Any: ...

    @overload
    def function_stats(self: AsyncClientProtocol) -> Awaitable[Any]: ...

    def function_stats(self) -> Any | Awaitable[Any]:
        """
        Return information about the function that's currently running
        and information about the available execution engines.

        For more information, see https://redis.io/commands/function-stats
        """
        return self.execute_command("FUNCTION STATS")


AsyncFunctionCommands = FunctionCommands


BlessFlag = Literal["NO-EVICT"]


class BlessCommands(CommandsProtocol):
    """
    Redis BLESS commands: per-key flags that alter how the server treats a key.
    """

    @overload
    def bless_get(self: SyncClientProtocol, name: KeyT) -> list[bytes | str]: ...

    @overload
    def bless_get(
        self: AsyncClientProtocol, name: KeyT
    ) -> Awaitable[list[bytes | str]]: ...

    def bless_get(self, name: KeyT) -> list[bytes | str] | Awaitable[list[bytes | str]]:
        """
        Return the list of bless flags currently set on the key ``name``.

        Raises a ``ResponseError`` if the key does not exist.

        For more information, see https://redis.io/commands/bless-get
        """
        return self.execute_command("BLESS GET", name)

    @overload
    def bless_set(self: SyncClientProtocol, name: KeyT, flag: BlessFlag) -> int: ...

    @overload
    def bless_set(
        self: AsyncClientProtocol, name: KeyT, flag: BlessFlag
    ) -> Awaitable[int]: ...

    def bless_set(self, name: KeyT, flag: BlessFlag) -> int | Awaitable[int]:
        """
        Set the bless ``flag`` on the key ``name``.

        Returns 1 if the flag was set, 0 if it was already set.
        Raises a ``ResponseError`` if the key does not exist.

        For more information, see https://redis.io/commands/bless-set
        """
        return self.execute_command("BLESS SET", name, flag)

    @overload
    def bless_clear(self: SyncClientProtocol, name: KeyT, flag: BlessFlag) -> int: ...

    @overload
    def bless_clear(
        self: AsyncClientProtocol, name: KeyT, flag: BlessFlag
    ) -> Awaitable[int]: ...

    def bless_clear(self, name: KeyT, flag: BlessFlag) -> int | Awaitable[int]:
        """
        Clear the bless ``flag`` from the key ``name``.

        Returns 1 if the flag was cleared, 0 if it was not set.
        Raises a ``ResponseError`` if the key does not exist.

        For more information, see https://redis.io/commands/bless-clear
        """
        return self.execute_command("BLESS CLEAR", name, flag)

    @overload
    def bless_scan(
        self: SyncClientProtocol,
        cursor: int,
        flag: BlessFlag,
        count: int | None = None,
        **kwargs,
    ) -> ScanResponse: ...

    @overload
    def bless_scan(
        self: AsyncClientProtocol,
        cursor: int,
        flag: BlessFlag,
        count: int | None = None,
        **kwargs,
    ) -> Awaitable[ScanResponse]: ...

    def bless_scan(
        self,
        cursor: int,
        flag: BlessFlag,
        count: int | None = None,
        **kwargs,
    ) -> ScanResponse | Awaitable[ScanResponse]:
        """
        Incrementally return lists of key names that carry the bless ``flag``.
        Also return a cursor indicating the scan position.

        ``cursor`` is the scan position to continue from; pass 0 to start
            a new scan.

        ``flag`` is the bless flag the returned keys must carry.

        ``count`` provides a hint to Redis about the number of keys to
            return per batch.

        For more information, see https://redis.io/commands/bless-scan
        """
        pieces: list[EncodableT] = [cursor, flag]
        if count is not None:
            pieces.extend([b"COUNT", count])
        return self.execute_command("BLESS SCAN", *pieces, **kwargs)

    def bless_scan_iter(
        self,
        flag: BlessFlag,
        count: int | None = None,
        **kwargs,
    ) -> Iterator[bytes | str]:
        """
        Make an iterator using the BLESS SCAN command so that the client doesn't
        need to remember the cursor position.

        ``count`` provides a hint to Redis about the number of keys to
            return per batch.
        """
        cursor = "0"
        while cursor != 0:
            cursor, data = self.bless_scan(
                cursor=cursor, flag=flag, count=count, **kwargs
            )
            yield from data


class AsyncBlessCommands(BlessCommands):
    async def bless_scan_iter(
        self,
        flag: BlessFlag,
        count: int | None = None,
        **kwargs,
    ) -> AsyncIterator[bytes | str]:
        """
        Make an iterator using the BLESS SCAN command so that the client doesn't
        need to remember the cursor position.

        ``count`` provides a hint to Redis about the number of keys to
            return per batch.
        """
        cursor = "0"
        while cursor != 0:
            cursor, data = await self.bless_scan(
                cursor=cursor, flag=flag, count=count, **kwargs
            )
            for d in data:
                yield d


class DataAccessCommands(
    BasicKeyCommands,
    HyperlogCommands,
    HashCommands,
    GeoCommands,
    ListCommands,
    ArrayCommands,
    ScanCommands,
    SetCommands,
    StreamCommands,
    SortedSetCommands,
):
    """
    A class containing all of the implemented data access redis commands.
    This class is to be used as a mixin for synchronous Redis clients.
    """


class AsyncDataAccessCommands(
    AsyncBasicKeyCommands,
    AsyncHyperlogCommands,
    AsyncHashCommands,
    AsyncGeoCommands,
    AsyncListCommands,
    AsyncArrayCommands,
    AsyncScanCommands,
    AsyncSetCommands,
    AsyncStreamCommands,
    AsyncSortedSetCommands,
):
    """
    A class containing all of the implemented data access redis commands.
    This class is to be used as a mixin for asynchronous Redis clients.
    """


class CoreCommands(
    ACLCommands,
    ClusterCommands,
    DataAccessCommands,
    ManagementCommands,
    ModuleCommands,
    PubSubCommands,
    ScriptCommands,
    FunctionCommands,
    BlessCommands,
):
    """
    A class containing all of the implemented redis commands. This class is
    to be used as a mixin for synchronous Redis clients.
    """


class AsyncCoreCommands(
    AsyncACLCommands,
    AsyncClusterCommands,
    AsyncDataAccessCommands,
    AsyncManagementCommands,
    AsyncModuleCommands,
    AsyncPubSubCommands,
    AsyncScriptCommands,
    AsyncFunctionCommands,
    AsyncBlessCommands,
):
    """
    A class containing all of the implemented redis commands. This class is
    to be used as a mixin for asynchronous Redis clients.
    """
