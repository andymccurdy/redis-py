import warnings
from typing import Any, Awaitable, overload

from redis.typing import (
    AsyncClientProtocol,
    SentinelMasterAddress,
    SentinelMastersResponse,
    SyncClientProtocol,
)


class SentinelCommands:
    """
    A class containing the commands specific to redis sentinel. This class is
    to be used as a mixin.
    """

    def sentinel(self, *args):
        """Redis Sentinel's SENTINEL command."""
        warnings.warn(DeprecationWarning("Use the individual sentinel_* methods"))

    @overload
    def sentinel_get_master_addr_by_name(
        self: SyncClientProtocol,
        service_name,
        return_responses: bool = False,
    ) -> SentinelMasterAddress | bool: ...

    @overload
    def sentinel_get_master_addr_by_name(
        self: AsyncClientProtocol,
        service_name,
        return_responses: bool = False,
    ) -> Awaitable[SentinelMasterAddress | bool]: ...

    def sentinel_get_master_addr_by_name(
        self, service_name, return_responses: bool = False
    ) -> (SentinelMasterAddress | bool) | Awaitable[SentinelMasterAddress | bool]:
        """
        Returns a (host, port) pair for the given ``service_name`` when return_responses is True,
        otherwise returns a boolean value that indicates if the command was successful.
        """
        return self.execute_command(
            "SENTINEL GET-MASTER-ADDR-BY-NAME",
            service_name,
            once=True,
            return_responses=return_responses,
        )

    @overload
    def sentinel_master(
        self: SyncClientProtocol,
        service_name,
        return_responses: bool = False,
    ) -> dict[str, Any] | bool: ...

    @overload
    def sentinel_master(
        self: AsyncClientProtocol,
        service_name,
        return_responses: bool = False,
    ) -> Awaitable[dict[str, Any] | bool]: ...

    def sentinel_master(self, service_name, return_responses: bool = False) -> (
        dict[str, Any] | bool
    ) | Awaitable[dict[str, Any] | bool]:
        """
        Returns a dictionary containing the specified masters state, when return_responses is True,
        otherwise returns a boolean value that indicates if the command was successful.
        """
        return self.execute_command(
            "SENTINEL MASTER", service_name, return_responses=return_responses
        )

    @overload
    def sentinel_masters(self: SyncClientProtocol) -> SentinelMastersResponse: ...

    @overload
    def sentinel_masters(
        self: AsyncClientProtocol,
    ) -> Awaitable[SentinelMastersResponse]: ...

    def sentinel_masters(
        self,
    ) -> SentinelMastersResponse | Awaitable[SentinelMastersResponse]:
        """
        Returns a list of dictionaries containing each master's state.

        Important: This function is called by the Sentinel implementation and is
        called directly on the Redis standalone client for sentinels,
        so it doesn't support the "once" and "return_responses" options.
        """
        return self.execute_command("SENTINEL MASTERS")

    @overload
    def sentinel_monitor(self: SyncClientProtocol, name, ip, port, quorum) -> bool: ...

    @overload
    def sentinel_monitor(
        self: AsyncClientProtocol, name, ip, port, quorum
    ) -> Awaitable[bool]: ...

    def sentinel_monitor(self, name, ip, port, quorum) -> bool | Awaitable[bool]:
        """Add a new master to Sentinel to be monitored"""
        return self.execute_command("SENTINEL MONITOR", name, ip, port, quorum)

    @overload
    def sentinel_remove(self: SyncClientProtocol, name) -> bool: ...

    @overload
    def sentinel_remove(self: AsyncClientProtocol, name) -> Awaitable[bool]: ...

    def sentinel_remove(self, name) -> bool | Awaitable[bool]:
        """Remove a master from Sentinel's monitoring"""
        return self.execute_command("SENTINEL REMOVE", name)

    @overload
    def sentinel_sentinels(
        self: SyncClientProtocol,
        service_name,
        return_responses: bool = False,
    ) -> list[dict[str, Any]] | bool: ...

    @overload
    def sentinel_sentinels(
        self: AsyncClientProtocol,
        service_name,
        return_responses: bool = False,
    ) -> Awaitable[list[dict[str, Any]] | bool]: ...

    def sentinel_sentinels(self, service_name, return_responses: bool = False) -> (
        list[dict[str, Any]] | bool
    ) | Awaitable[list[dict[str, Any]] | bool]:
        """
        Returns a list of sentinels for ``service_name``, when return_responses is True,
        otherwise returns a boolean value that indicates if the command was successful.
        """
        return self.execute_command(
            "SENTINEL SENTINELS", service_name, return_responses=return_responses
        )

    @overload
    def sentinel_set(self: SyncClientProtocol, name, option, value) -> bool: ...

    @overload
    def sentinel_set(
        self: AsyncClientProtocol, name, option, value
    ) -> Awaitable[bool]: ...

    def sentinel_set(self, name, option, value) -> bool | Awaitable[bool]:
        """Set Sentinel monitoring parameters for a given master"""
        return self.execute_command("SENTINEL SET", name, option, value)

    @overload
    def sentinel_slaves(
        self: SyncClientProtocol, service_name
    ) -> list[dict[str, Any]]: ...

    @overload
    def sentinel_slaves(
        self: AsyncClientProtocol, service_name
    ) -> Awaitable[list[dict[str, Any]]]: ...

    def sentinel_slaves(
        self,
        service_name,
    ) -> list[dict[str, Any]] | Awaitable[list[dict[str, Any]]]:
        """
        Returns a list of slaves for ``service_name``

        Important: This function is called by the Sentinel implementation and is
        called directly on the Redis standalone client for sentinels,
        so it doesn't support the "once" and "return_responses" options.
        """
        return self.execute_command("SENTINEL SLAVES", service_name)

    @overload
    def sentinel_reset(self: SyncClientProtocol, pattern) -> bool: ...

    @overload
    def sentinel_reset(self: AsyncClientProtocol, pattern) -> Awaitable[bool]: ...

    def sentinel_reset(self, pattern) -> bool | Awaitable[bool]:
        """
        This command will reset all the masters with matching name.
        The pattern argument is a glob-style pattern.

        The reset process clears any previous state in a master (including a
        failover in progress), and removes every slave and sentinel already
        discovered and associated with the master.
        """
        return self.execute_command("SENTINEL RESET", pattern, once=True)

    @overload
    def sentinel_failover(self: SyncClientProtocol, new_master_name) -> bool: ...

    @overload
    def sentinel_failover(
        self: AsyncClientProtocol, new_master_name
    ) -> Awaitable[bool]: ...

    def sentinel_failover(self, new_master_name) -> bool | Awaitable[bool]:
        """
        Force a failover as if the master was not reachable, and without
        asking for agreement to other Sentinels (however a new version of the
        configuration will be published so that the other Sentinels will
        update their configurations).
        """
        return self.execute_command("SENTINEL FAILOVER", new_master_name)

    @overload
    def sentinel_ckquorum(self: SyncClientProtocol, new_master_name) -> bool: ...

    @overload
    def sentinel_ckquorum(
        self: AsyncClientProtocol, new_master_name
    ) -> Awaitable[bool]: ...

    def sentinel_ckquorum(self, new_master_name) -> bool | Awaitable[bool]:
        """
        Check if the current Sentinel configuration is able to reach the
        quorum needed to failover a master, and the majority needed to
        authorize the failover.

        This command should be used in monitoring systems to check if a
        Sentinel deployment is ok.
        """
        return self.execute_command("SENTINEL CKQUORUM", new_master_name, once=True)

    @overload
    def sentinel_flushconfig(self: SyncClientProtocol) -> bool: ...

    @overload
    def sentinel_flushconfig(self: AsyncClientProtocol) -> Awaitable[bool]: ...

    def sentinel_flushconfig(self) -> bool | Awaitable[bool]:
        """
        Force Sentinel to rewrite its configuration on disk, including the
        current Sentinel state.

        Normally Sentinel rewrites the configuration every time something
        changes in its state (in the context of the subset of the state which
        is persisted on disk across restart).
        However sometimes it is possible that the configuration file is lost
        because of operation errors, disk failures, package upgrade scripts or
        configuration managers. In those cases a way to force Sentinel to
        rewrite the configuration file is handy.

        This command works even if the previous configuration file is
        completely missing.
        """
        return self.execute_command("SENTINEL FLUSHCONFIG")


class AsyncSentinelCommands(SentinelCommands):
    async def sentinel(self, *args) -> None:
        """Redis Sentinel's SENTINEL command."""
        super().sentinel(*args)
