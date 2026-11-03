import asyncio
import random
import weakref
from typing import AsyncIterator, Iterable, Mapping, Optional, Sequence, Tuple, Type

from redis.asyncio.client import Redis
from redis.asyncio.connection import (
    Connection,
    ConnectionPool,
    EncodableT,
    SSLConnection,
)
from redis.commands import AsyncSentinelCommands
from redis.exceptions import (
    ConnectionError,
    ReadOnlyError,
    ResponseError,
    TimeoutError,
)
from redis.sentinel import DEMOTED_MASTER_UNBLOCKED_ERROR


class MasterNotFoundError(ConnectionError):
    pass


class SlaveNotFoundError(ConnectionError):
    pass


ReplicaNotFoundError = SlaveNotFoundError


class SentinelManagedConnection(Connection):
    def __init__(self, **kwargs):
        self.connection_pool = kwargs.pop("connection_pool")
        super().__init__(**kwargs)

    def __repr__(self):
        s = f"<{self.__class__.__module__}.{self.__class__.__name__}"
        if self.host:
            host_info = f",host={self.host},port={self.port}"
            s += host_info
        return s + ")>"

    async def connect_to(self, address):
        self.host, self.port = address
        await self.connect_check_health(
            check_health=self.connection_pool.check_connection,
            retry_socket_connect=False,
        )

    async def _connect_retry(self):
        if self._reader:
            return  # already connected
        if self.connection_pool.is_master:
            await self.connect_to(await self.connection_pool.get_master_address())
        else:
            async for slave in self.connection_pool.rotate_slaves():
                try:
                    return await self.connect_to(slave)
                except ConnectionError:
                    continue
            raise SlaveNotFoundError  # Never be here

    async def connect(self):
        return await self.retry.call_with_retry(
            self._connect_retry,
            lambda error: asyncio.sleep(0),
        )

    async def read_response(
        self,
        disable_decoding: bool = False,
        timeout: Optional[float] = None,
        *,
        disconnect_on_error: Optional[float] = True,
        push_request: Optional[bool] = False,
    ):
        try:
            return await super().read_response(
                disable_decoding=disable_decoding,
                timeout=timeout,
                disconnect_on_error=disconnect_on_error,
                push_request=push_request,
            )
        except ReadOnlyError:
            if self.connection_pool.is_master:
                # When talking to a master, a ReadOnlyError when likely
                # indicates that the previous master that we're still connected
                # to has been demoted to a slave and there's a new master.
                # calling disconnect will force the connection to re-query
                # sentinel during the next connect() attempt.
                await self.disconnect()
                raise ConnectionError("The previous master is now a slave")
            raise
        except ResponseError as e:
            if self.connection_pool.is_master and str(e).startswith(
                DEMOTED_MASTER_UNBLOCKED_ERROR
            ):
                # A master that turns into a replica unblocks every client
                # waiting on a blocking command (BLPOP, BRPOP, XREAD...) with
                # this error and closes the connection, as when Sentinel runs
                # a coordinated failover. Same meaning as the ReadOnlyError
                # above: re-query Sentinel on the next connect() attempt.
                await self.disconnect()
                raise ConnectionError("The previous master is now a slave")
            raise


class SentinelManagedSSLConnection(SentinelManagedConnection, SSLConnection):
    pass


class SentinelConnectionPool(ConnectionPool):
    """
    Sentinel backed connection pool.

    If ``check_connection`` flag is set to True, SentinelManagedConnection
    sends a PING command right after establishing the connection.
    """

    def __init__(self, service_name, sentinel_manager, **kwargs):
        kwargs["connection_class"] = kwargs.get(
            "connection_class",
            (
                SentinelManagedSSLConnection
                if kwargs.pop("ssl", False)
                else SentinelManagedConnection
            ),
        )
        self.is_master = kwargs.pop("is_master", True)
        self.check_connection = kwargs.pop("check_connection", False)
        super().__init__(**kwargs)
        self.connection_kwargs["connection_pool"] = weakref.proxy(self)
        self.service_name = service_name
        self.sentinel_manager = sentinel_manager
        self.master_address = None
        self.slave_rr_counter = None

    def __repr__(self):
        return (
            f"<{self.__class__.__module__}.{self.__class__.__name__}"
            f"(service={self.service_name}({self.is_master and 'master' or 'slave'}))>"
        )

    def reset(self):
        super().reset()
        self.master_address = None
        self.slave_rr_counter = None

    def owns_connection(self, connection: Connection):
        check = not self.is_master or (
            self.is_master and self.master_address == (connection.host, connection.port)
        )
        return check and super().owns_connection(connection)

    async def get_master_address(self):
        master_address = await self.sentinel_manager.discover_master(self.service_name)
        if self.is_master:
            if self.master_address != master_address:
                self.master_address = master_address
                # disconnect any idle connections so that they reconnect
                # to the new master the next time that they are used.
                await self.disconnect(inuse_connections=False)
        return master_address

    async def rotate_slaves(self) -> AsyncIterator:
        """Round-robin slave balancer"""
        slaves = await self.sentinel_manager.discover_slaves(self.service_name)
        if slaves:
            if self.slave_rr_counter is None:
                self.slave_rr_counter = random.randint(0, len(slaves) - 1)
            for _ in range(len(slaves)):
                self.slave_rr_counter = (self.slave_rr_counter + 1) % len(slaves)
                slave = slaves[self.slave_rr_counter]
                yield slave
        # Fallback to the master connection
        try:
            yield await self.get_master_address()
        except MasterNotFoundError:
            pass
        raise SlaveNotFoundError(f"No slave found for {self.service_name!r}")

    def rotate_replicas(self) -> AsyncIterator:
        """Round-robin replica balancer.

        This is an alias for :py:meth:`rotate_slaves`,
        using the preferred Redis 5.0+ terminology.
        """
        return self.rotate_slaves()


class Sentinel(AsyncSentinelCommands):
    # How many subsequent discovery calls keep a sentinel that just failed
    # at the end of the try order before it gets another chance.
    FAILED_SENTINEL_QUARANTINE = 8

    """
    Redis Sentinel cluster client

    >>> from redis.sentinel import Sentinel
    >>> sentinel = Sentinel([('localhost', 26379)], socket_timeout=0.1)
    >>> master = sentinel.master_for('mymaster', socket_timeout=0.1)
    >>> await master.set('foo', 'bar')
    >>> slave = sentinel.slave_for('mymaster', socket_timeout=0.1)
    >>> await slave.get('foo')
    b'bar'

    ``sentinels`` is a list of sentinel nodes. Each node is represented by
    a pair (hostname, port).

    ``min_other_sentinels`` defined a minimum number of peers for a sentinel.
    When querying a sentinel, if it doesn't meet this threshold, responses
    from that sentinel won't be considered valid.

    ``sentinel_kwargs`` is a dictionary of connection arguments used when
    connecting to sentinel instances. Any argument that can be passed to
    a normal Redis connection can be specified here. If ``sentinel_kwargs`` is
    not specified, any socket_timeout and socket_keepalive options specified
    in ``connection_kwargs`` will be used.

    ``connection_kwargs`` are keyword arguments that will be used when
    establishing a connection to a Redis server.
    """

    def __init__(
        self,
        sentinels,
        min_other_sentinels=0,
        sentinel_kwargs=None,
        force_master_ip=None,
        **connection_kwargs,
    ):
        # if sentinel_kwargs isn't defined, use the socket_* options from
        # connection_kwargs
        if sentinel_kwargs is None:
            sentinel_kwargs = {
                k: v for k, v in connection_kwargs.items() if k.startswith("socket_")
            }
        self.sentinel_kwargs = sentinel_kwargs

        self.sentinels = [
            Redis(host=hostname, port=port, **self.sentinel_kwargs)
            for hostname, port in sentinels
        ]
        self.min_other_sentinels = min_other_sentinels
        self.connection_kwargs = connection_kwargs
        self._force_master_ip = force_master_ip
        # Sentinel clients that failed during a discovery call, mapped to the
        # number of later discovery calls that keep trying them last. A dead
        # sentinel therefore does not cost a connect timeout on every
        # discovery call, but it is retried periodically and rejoins the
        # rotation once it answers again.
        self._failed_sentinels = {}

    def _discovery_order(self):
        # Iterate over a snapshot: a concurrent discovery call may rebind
        # self.sentinels (see _promote_answering_sentinel) while this loop
        # is still running, and iterating a list another call is mutating
        # could skip or repeat a sentinel.
        sentinels = list(self.sentinels)
        if not self._failed_sentinels:
            return sentinels
        healthy = []
        quarantined = []
        for sentinel in sentinels:
            remaining = self._failed_sentinels.get(sentinel)
            if remaining is None:
                healthy.append(sentinel)
            elif remaining > 1:
                self._failed_sentinels[sentinel] = remaining - 1
                quarantined.append(sentinel)
            else:
                del self._failed_sentinels[sentinel]
                healthy.append(sentinel)
        return healthy + quarantined

    def _promote_answering_sentinel(self, sentinel):
        self._failed_sentinels.pop(sentinel, None)
        # Build the next order as a new list and rebind self.sentinels in a
        # single step: rebinding (instead of in-place pop/append/insert) is
        # what keeps a concurrent discovery call - which iterates a snapshot
        # of the old list - from seeing duplicates or losing a sentinel.
        # - The head answered: rotate it to the end of the healthy subset, so
        #   repeated discovery calls spread the load across the answering
        #   nodes even while other sentinels sit in quarantine.
        # - A non-head sentinel answered: promote it to the head, preserving
        #   the pre-existing failover fast path of trying it first on the
        #   next call.
        next_order = [s for s in self.sentinels if s is not sentinel]
        if sentinel is self.sentinels[0]:
            healthy = [s for s in next_order if s not in self._failed_sentinels]
            quarantined = [s for s in next_order if s in self._failed_sentinels]
            next_order = healthy + [sentinel] + quarantined
        else:
            next_order.insert(0, sentinel)
        self.sentinels = next_order

    async def execute_command(self, *args, **kwargs):
        """
        Execute Sentinel command in sentinel nodes.
        once - If set to True, then execute the resulting command on a single
               node at random, rather than across the entire sentinel cluster.
        """
        once = bool(kwargs.pop("once", False))

        # Check if command is supposed to return the original
        # responses instead of boolean value.
        return_responses = bool(kwargs.pop("return_responses", False))

        if once:
            response = await random.choice(self.sentinels).execute_command(
                *args, **kwargs
            )
            if return_responses:
                return [response]
            else:
                return True if response else False

        tasks = [
            asyncio.Task(sentinel.execute_command(*args, **kwargs))
            for sentinel in self.sentinels
        ]
        responses = await asyncio.gather(*tasks)

        if return_responses:
            return responses

        return all(responses)

    def __repr__(self):
        sentinel_addresses = []
        for sentinel in self.sentinels:
            sentinel_addresses.append(
                f"{sentinel.connection_pool.connection_kwargs['host']}:"
                f"{sentinel.connection_pool.connection_kwargs['port']}"
            )
        return (
            f"<{self.__class__}.{self.__class__.__name__}"
            f"(sentinels=[{','.join(sentinel_addresses)}])>"
        )

    async def aclose(self) -> None:
        """
        Close all sentinel clients created by this Sentinel and their
        connection pools.

        Each client is closed independently: if one raises, the remaining
        clients are still closed and the first error is re-raised afterwards.

        Clients returned by ``master_for``/``slave_for`` are owned by the
        caller and are not closed here.
        """
        resp = await asyncio.gather(
            *(sentinel.aclose() for sentinel in self.sentinels),
            return_exceptions=True,
        )
        exc = next((r for r in resp if isinstance(r, BaseException)), None)
        if exc:
            raise exc

    async def __aenter__(self) -> "Sentinel":
        return self

    async def __aexit__(self, exc_type, exc_value, traceback) -> None:
        await self.aclose()

    def check_master_state(self, state: dict, service_name: str) -> bool:
        if not state["is_master"] or state["is_sdown"] or state["is_odown"]:
            return False
        # Check if our sentinel doesn't see other nodes
        if state["num-other-sentinels"] < self.min_other_sentinels:
            return False
        return True

    async def discover_master(self, service_name: str):
        """
        Asks sentinel servers for the Redis master's address corresponding
        to the service labeled ``service_name``.

        Returns a pair (address, port) or raises MasterNotFoundError if no
        master is found.
        """
        collected_errors = list()
        for sentinel in self._discovery_order():
            try:
                masters = await sentinel.sentinel_masters()
            except (ConnectionError, TimeoutError) as e:
                collected_errors.append(f"{sentinel} - {e!r}")
                self._failed_sentinels[sentinel] = self.FAILED_SENTINEL_QUARANTINE
                continue
            state = masters.get(service_name)
            if state and self.check_master_state(state, service_name):
                self._promote_answering_sentinel(sentinel)

                ip = (
                    self._force_master_ip
                    if self._force_master_ip is not None
                    else state["ip"]
                )
                return ip, state["port"]

        error_info = ""
        if len(collected_errors) > 0:
            error_info = f" : {', '.join(collected_errors)}"
        raise MasterNotFoundError(f"No master found for {service_name!r}{error_info}")

    def filter_slaves(
        self, slaves: Iterable[Mapping]
    ) -> Sequence[Tuple[EncodableT, EncodableT]]:
        """Remove slaves that are in an ODOWN or SDOWN state"""
        slaves_alive = []
        for slave in slaves:
            if slave["is_odown"] or slave["is_sdown"]:
                continue
            slaves_alive.append((slave["ip"], slave["port"]))
        return slaves_alive

    def filter_replicas(
        self, replicas: Iterable[Mapping]
    ) -> Sequence[Tuple[EncodableT, EncodableT]]:
        """Remove replicas that are in an ODOWN or SDOWN state.

        This is an alias for :py:meth:`filter_slaves`,
        using the preferred Redis 5.0+ terminology.
        """
        return self.filter_slaves(replicas)

    async def discover_slaves(
        self, service_name: str
    ) -> Sequence[Tuple[EncodableT, EncodableT]]:
        """Returns a list of alive slaves for service ``service_name``"""
        for sentinel in self._discovery_order():
            try:
                slaves = await sentinel.sentinel_slaves(service_name)
            except (ConnectionError, ResponseError, TimeoutError):
                self._failed_sentinels[sentinel] = self.FAILED_SENTINEL_QUARANTINE
                continue
            slaves = self.filter_slaves(slaves)
            if slaves:
                self._promote_answering_sentinel(sentinel)
                return slaves
        return []

    async def discover_replicas(
        self, service_name: str
    ) -> Sequence[Tuple[EncodableT, EncodableT]]:
        """Returns a list of alive replicas for service ``service_name``.

        This is an alias for :py:meth:`discover_slaves`,
        using the preferred Redis 5.0+ terminology.
        """
        return await self.discover_slaves(service_name)

    def master_for(
        self,
        service_name: str,
        redis_class: Type[Redis] = Redis,
        connection_pool_class: Type[SentinelConnectionPool] = SentinelConnectionPool,
        **kwargs,
    ):
        """
        Returns a redis client instance for the ``service_name`` master.
        Sentinel client will detect failover and reconnect Redis clients
        automatically.

        A :py:class:`~redis.sentinel.SentinelConnectionPool` class is
        used to retrieve the master's address before establishing a new
        connection.

        NOTE: If the master's address has changed, any cached connections to
        the old master are closed.

        By default clients will be a :py:class:`~redis.Redis` instance.
        Specify a different class to the ``redis_class`` argument if you
        desire something different.

        The ``connection_pool_class`` specifies the connection pool to
        use.  The :py:class:`~redis.sentinel.SentinelConnectionPool`
        will be used by default.

        All other keyword arguments are merged with any connection_kwargs
        passed to this class and passed to the connection pool as keyword
        arguments to be used to initialize Redis connections.

        HIMPORT note: the returned client exposes the HIMPORT command family
        (``himport_prepare``, ``himport_set``, ``himport_discard``,
        ``himport_discard_all``). Fieldsets are declared at runtime with
        ``himport_prepare`` on the client and live on that client's shared registry,
        so they survive Sentinel failover automatically: the pool re-points to the
        new master and the fieldset is re-prepared lazily on the next
        ``himport_set``. Each call to the current command returns a *new* client with its *own* empty
        registry, so call ``himport_prepare`` on the long-lived client you reuse
        rather than creating a fresh one per operation; otherwise ``himport_set``
        fails with ``no such fieldset``. (``himport_set`` is a write and is served by
        the master.)
        """
        kwargs["is_master"] = True
        connection_kwargs = dict(self.connection_kwargs)
        connection_kwargs.update(kwargs)

        connection_pool = connection_pool_class(service_name, self, **connection_kwargs)
        # The Redis object "owns" the pool
        return redis_class.from_pool(connection_pool)

    def slave_for(
        self,
        service_name: str,
        redis_class: Type[Redis] = Redis,
        connection_pool_class: Type[SentinelConnectionPool] = SentinelConnectionPool,
        **kwargs,
    ):
        """
        Returns redis client instance for the ``service_name`` slave(s).

        A SentinelConnectionPool class is used to retrieve the slave's
        address before establishing a new connection.

        By default clients will be a :py:class:`~redis.Redis` instance.
        Specify a different class to the ``redis_class`` argument if you
        desire something different.

        The ``connection_pool_class`` specifies the connection pool to use.
        The SentinelConnectionPool will be used by default.

        All other keyword arguments are merged with any connection_kwargs
        passed to this class and passed to the connection pool as keyword
        arguments to be used to initialize Redis connections.

        HIMPORT note: the returned client exposes the HIMPORT command family
        (``himport_prepare``, ``himport_set``, ``himport_discard``,
        ``himport_discard_all``). Fieldsets are declared at runtime with
        ``himport_prepare`` on the client and live on that client's shared registry,
        so they survive Sentinel failover automatically: the pool re-points to the
        new master and the fieldset is re-prepared lazily on the next
        ``himport_set``. Each call to the current command returns a *new* client with its *own* empty
        registry, so call ``himport_prepare`` on the long-lived client you reuse
        rather than creating a fresh one per operation; otherwise ``himport_set``
        fails with ``no such fieldset``. (``himport_set`` is a write and is served by
        the master.)
        """
        kwargs["is_master"] = False
        connection_kwargs = dict(self.connection_kwargs)
        connection_kwargs.update(kwargs)

        connection_pool = connection_pool_class(service_name, self, **connection_kwargs)
        # The Redis object "owns" the pool
        return redis_class.from_pool(connection_pool)

    def replica_for(
        self,
        service_name: str,
        redis_class: Type[Redis] = Redis,
        connection_pool_class: Type[SentinelConnectionPool] = SentinelConnectionPool,
        **kwargs,
    ):
        """
        Returns redis client instance for the ``service_name`` replica(s).

        This is an alias for :py:meth:`slave_for`,
        using the preferred Redis 5.0+ terminology.
        """
        return self.slave_for(
            service_name,
            redis_class=redis_class,
            connection_pool_class=connection_pool_class,
            **kwargs,
        )
