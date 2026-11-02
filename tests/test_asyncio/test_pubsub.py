import asyncio
import functools
import logging
import socket
import sys
import time
import weakref
from collections import defaultdict
from typing import Optional
from unittest.mock import patch, AsyncMock, MagicMock

# the functionality is available in 3.11.x but has a major issue before
# 3.11.3. See https://github.com/redis/redis-py/issues/2633
if sys.version_info >= (3, 11, 3):
    from asyncio import timeout as async_timeout
else:
    from async_timeout import timeout as async_timeout

from unittest import mock

import pytest
import pytest_asyncio
import redis.asyncio as redis
from redis.asyncio import Subscription
from redis._parsers import Encoder
from redis.asyncio.client import PubSub
from redis.asyncio.cluster import ClusterPubSub
from redis.asyncio.retry import Retry
from redis.backoff import NoBackoff
from redis.crc import key_slot

from redis.exceptions import (
    ConnectionError,
    RedisError,
    ResponseError,
    SlotNotCoveredError,
    TimeoutError,
)
from redis.typing import EncodableT
from redis.utils import str_if_bytes
from tests.conftest import REDIS_INFO, get_protocol_version, skip_if_server_version_lt

from .compat import aclosing, create_task

# Slack for the lower bound of a measured ``asyncio.sleep``. uvloop drives its
# timers off libuv's cached millisecond-resolution clock rather than
# ``time.monotonic()``, so a sleep can return just under a millisecond before the
# requested delay has elapsed on the clock these tests measure with - which makes
# an exact lower bound fail intermittently on the ``--uvloop`` matrix leg. The
# sync counterparts of these tests need no such slack: ``time.sleep`` does not
# return early. Kept well above the observed sub-millisecond undershoot, and far
# below any delay a test asserts on, so it cannot mask a wait that never happened.
EVENT_LOOP_TIMER_SLACK = 0.005


def with_timeout(t):
    def wrapper(corofunc):
        @functools.wraps(corofunc)
        async def run(*args, **kwargs):
            async with async_timeout(t):
                return await corofunc(*args, **kwargs)

        return run

    return wrapper


async def wait_for_message(pubsub, timeout=0.5, ignore_subscribe_messages=False):
    now = asyncio.get_running_loop().time()
    timeout = now + timeout
    while now < timeout:
        message = await pubsub.get_message(
            ignore_subscribe_messages=ignore_subscribe_messages
        )
        if message is not None:
            return message
        await asyncio.sleep(0.01)
        now = asyncio.get_running_loop().time()
    return None


async def wait_for_handled_message(test, get_message, expected, timeout=5.0):
    """Handle messages until the test's handler has seen ``expected``.

    In a cluster, a PUBLISH reaches subscribers on other nodes over the cluster
    bus, so a message an earlier test published on the same channel can arrive
    late and be handled first. Handling until the expected message shows up
    skips such strays. A handled message is never returned, so every call must
    still return None.
    """
    deadline = time.monotonic() + timeout
    while getattr(test, "message", None) != expected:
        assert time.monotonic() < deadline, (
            f"handler saw {getattr(test, 'message', None)!r}, expected {expected!r}"
        )
        assert await get_message(timeout=0.1) is None


def make_message(
    type, channel: Optional[str], data: EncodableT, pattern: Optional[str] = None
):
    return {
        "type": type,
        "pattern": pattern and pattern.encode("utf-8") or None,
        "channel": channel and channel.encode("utf-8") or None,
        "data": data.encode("utf-8") if isinstance(data, str) else data,
    }


def make_subscribe_test_data(pubsub, type):
    if type == "channel":
        return {
            "p": pubsub,
            "sub_type": "subscribe",
            "unsub_type": "unsubscribe",
            "sub_func": pubsub.subscribe,
            "unsub_func": pubsub.unsubscribe,
            "keys": ["foo", "bar", "uni" + chr(4456) + "code"],
        }
    elif type == "shard_channel":
        return {
            "p": pubsub,
            "sub_type": "ssubscribe",
            "unsub_type": "sunsubscribe",
            "sub_func": pubsub.ssubscribe,
            "unsub_func": pubsub.sunsubscribe,
            "keys": ["foo", "bar", "uni" + chr(4456) + "code"],
        }
    elif type == "pattern":
        return {
            "p": pubsub,
            "sub_type": "psubscribe",
            "unsub_type": "punsubscribe",
            "sub_func": pubsub.psubscribe,
            "unsub_func": pubsub.punsubscribe,
            "keys": ["f*", "b*", "uni" + chr(4456) + "*"],
        }
    assert False, f"invalid subscribe type: {type}"


@pytest_asyncio.fixture()
async def pubsub(r: redis.Redis):
    async with r.pubsub() as p:
        yield p


@pytest.mark.fixed_client
class TestPubSubSubscriberModeProtocol:
    DEFAULT_STANDALONE_URL = "redis://localhost:6379/0"
    DEFAULT_CLUSTER_URL = "redis://localhost:16379/0"

    async def _get_resp2_client(self, request, cluster=False):
        client = None
        redis_url = self._get_url(request, cluster)
        try:
            if cluster:
                client = redis.RedisCluster.from_url(
                    redis_url,
                    protocol=2,
                    socket_connect_timeout=1,
                    socket_timeout=1,
                )
                await client.initialize()
            else:
                client = redis.Redis.from_url(
                    redis_url,
                    protocol=2,
                    socket_connect_timeout=1,
                    socket_timeout=1,
                )
            await client.ping()
        except RedisError as exc:
            await self._close_client(client)
            pytest.skip(
                f"Redis {'cluster' if cluster else 'standalone'} unavailable: {exc}"
            )

        if get_protocol_version(client) not in [2, "2"]:
            await self._close_client(client)
            pytest.skip("subscriber-mode command rejection is RESP2-specific")

        return client

    def _get_url(self, request, cluster):
        if bool(REDIS_INFO.get("cluster_enabled", False)) == cluster:
            return request.config.getoption("--redis-url")
        if cluster:
            return self.DEFAULT_CLUSTER_URL
        return self.DEFAULT_STANDALONE_URL

    async def _close_client(self, client):
        if client is not None:
            await client.aclose()

    async def test_subscriber_mode_rejects_regular_command_after_subscribe(
        self, request
    ):
        r = await self._get_resp2_client(request)
        p = r.pubsub()
        try:
            await p.subscribe("subscriber-mode")
            assert (await wait_for_message(p))["type"] == "subscribe"

            await p.execute_command("SET", "should-not-run", "1")
            with pytest.raises(ResponseError, match="only .* allowed|Can't execute"):
                await p.parse_response(block=False, timeout=1)
        finally:
            await p.aclose()
            await self._close_client(r)

    async def test_subscriber_mode_rejects_regular_command_after_psubscribe(
        self, request
    ):
        r = await self._get_resp2_client(request)
        p = r.pubsub()
        try:
            await p.psubscribe("subscriber-*")
            assert (await wait_for_message(p))["type"] == "psubscribe"

            await p.execute_command("SET", "should-not-run", "1")
            with pytest.raises(ResponseError, match="only .* allowed|Can't execute"):
                await p.parse_response(block=False, timeout=1)
        finally:
            await p.aclose()
            await self._close_client(r)

    @skip_if_server_version_lt("7.0.0")
    async def test_subscriber_mode_rejects_regular_command_after_ssubscribe(
        self, request
    ):
        r = await self._get_resp2_client(request, cluster=True)
        channel = "subscriber-mode-{shard}"
        node = r.get_node_from_key(channel)
        p = r.pubsub()
        try:
            await p.ssubscribe(channel)
            message = await p.get_sharded_message(target_node=node, timeout=1)
            assert message["type"] == "ssubscribe"

            per_node_pubsub = p.node_pubsub_mapping[node.name]
            await per_node_pubsub.execute_command("SET", "should-not-run-{shard}", "1")
            with pytest.raises(ResponseError, match="only .* allowed|Can't execute"):
                await per_node_pubsub.parse_response(block=False, timeout=1)
        finally:
            await p.aclose()
            await self._close_client(r)


class TestPubSubCloseServerCleanup:
    async def _wait_for_result_to_match_expected(
        self, get_result, expected, timeout=2.0
    ):
        deadline = asyncio.get_running_loop().time() + timeout
        result = None
        while asyncio.get_running_loop().time() < deadline:
            result = await get_result()
            if result == expected:
                return
            await asyncio.sleep(0.01)
        assert result == expected

    @pytest.mark.onlynoncluster
    async def test_close_while_subscribed_removes_numsub_entry(self, r: redis.Redis):
        channel = "close-cleanup"
        p = r.pubsub()
        try:
            await p.subscribe(channel)
            assert (await wait_for_message(p))["type"] == "subscribe"
            await self._wait_for_result_to_match_expected(
                lambda: r.pubsub_numsub(channel),
                [(channel.encode(), 1)],
            )
        finally:
            await p.aclose()

        assert p.connection is None
        assert p.channels == {}
        await self._wait_for_result_to_match_expected(
            lambda: r.pubsub_numsub(channel),
            [(channel.encode(), 0)],
        )

    @pytest.mark.onlycluster
    @skip_if_server_version_lt("7.0.0")
    async def test_close_while_ssubscribed_removes_shardnumsub_entry(
        self, r: redis.Redis
    ):
        channel = "close-cleanup-{shard}"
        node = r.get_node_from_key(channel)
        p = r.pubsub()
        try:
            await p.ssubscribe(channel)
            message = await p.get_sharded_message(target_node=node, timeout=1)
            assert message["type"] == "ssubscribe"
            await self._wait_for_result_to_match_expected(
                lambda: r.pubsub_shardnumsub(channel, target_nodes=node),
                [(channel.encode(), 1)],
            )
        finally:
            await p.aclose()

        assert p.node_pubsub_mapping == {}
        assert p.shard_channels == {}
        await self._wait_for_result_to_match_expected(
            lambda: r.pubsub_shardnumsub(channel, target_nodes=node),
            [(channel.encode(), 0)],
        )


@pytest.mark.onlycluster
@skip_if_server_version_lt("7.0.0")
class TestClusterShardedPubSubDelivery:
    async def _wait_for_sharded_acks(
        self, pubsub, count, node=None, message_type="ssubscribe"
    ):
        for _ in range(count):
            if node is not None:
                message = await pubsub.get_sharded_message(
                    target_node=node, timeout=2.0
                )
            else:
                message = await pubsub.get_sharded_message(timeout=2.0)
            assert message is not None
            assert message["type"] == message_type

    async def _read_sharded_messages(self, pubsub, expected_count, timeout=5.0):
        messages = []
        deadline = asyncio.get_running_loop().time() + timeout
        while (
            asyncio.get_running_loop().time() < deadline
            and len(messages) < expected_count
        ):
            message = await pubsub.get_sharded_message(timeout=0.05)
            if message is not None and message["type"] == "smessage":
                messages.append(message)
        return messages

    async def test_multiple_sharded_subscribers_receive_all_messages(
        self, r: redis.Redis
    ):
        channels = ["multi-sub-{shared}:0", "multi-sub-{shared}:1"]
        subscriber_count = 2
        message_count = 20
        expected = set(range(message_count))
        received = [defaultdict(set) for _ in range(subscriber_count)]
        subscribers = [r.pubsub() for _ in range(subscriber_count)]

        try:
            for pubsub in subscribers:
                await pubsub.ssubscribe(*channels)
                await self._wait_for_sharded_acks(pubsub, len(channels))

            for seq in range(message_count):
                for channel in channels:
                    assert await r.spublish(channel, str(seq)) == subscriber_count

            deadline = asyncio.get_running_loop().time() + 5.0
            while asyncio.get_running_loop().time() < deadline:
                for index, pubsub in enumerate(subscribers):
                    message = await pubsub.get_sharded_message(timeout=0.01)
                    if message is not None and message["type"] == "smessage":
                        channel = str_if_bytes(message["channel"])
                        received[index][channel].add(int(message["data"]))

                if all(
                    all(subscriber[channel] == expected for channel in channels)
                    for subscriber in received
                ):
                    return

            assert [
                {channel: sorted(seqs) for channel, seqs in subscriber.items()}
                for subscriber in received
            ] == [
                {channel: list(range(message_count)) for channel in channels}
                for _ in range(subscriber_count)
            ]
        finally:
            for pubsub in subscribers:
                await pubsub.aclose()

    async def test_duplicate_ssubscribe_delivers_each_message_once(
        self, r: redis.Redis
    ):
        channel = "duplicate-ssubscribe-{shared}"
        p = r.pubsub()
        try:
            await p.ssubscribe(channel)
            await self._wait_for_sharded_acks(p, 1)

            await p.ssubscribe(channel)
            maybe_ack = await p.get_sharded_message(timeout=0.5)
            assert maybe_ack is None or maybe_ack["type"] == "ssubscribe"

            for seq in range(10):
                assert await r.spublish(channel, str(seq)) == 1

            messages = await self._read_sharded_messages(p, expected_count=10)
            received = [int(message["data"]) for message in messages]

            assert sorted(received) == list(range(10))
            assert await p.get_sharded_message(timeout=0.5) is None
        finally:
            await p.aclose()

    async def test_no_messages_delivered_after_sunsubscribe(self, r: redis.Redis):
        kept = "still-subscribed-{shared}"
        removed = "removed-{shared}"
        p = r.pubsub()
        try:
            await p.ssubscribe(kept, removed)
            await self._wait_for_sharded_acks(p, 2)

            await p.sunsubscribe(removed)
            await self._wait_for_sharded_acks(p, 1, message_type="sunsubscribe")

            for seq in range(10):
                assert await r.spublish(kept, str(seq)) == 1
                assert await r.spublish(removed, str(seq)) == 0

            messages = await self._read_sharded_messages(p, expected_count=10)
            delivered_channels = [
                str_if_bytes(message["channel"]) for message in messages
            ]

            assert delivered_channels == [kept] * 10
            assert await p.get_sharded_message(timeout=0.5) is None
        finally:
            await p.aclose()


@pytest.mark.onlycluster
@skip_if_server_version_lt("7.0.0")
class TestClusterShardedPubSubConnectionLifecycle:
    async def _wait_for_ssubscribe_ack(self, pubsub):
        message = await pubsub.get_sharded_message(timeout=2.0)
        assert message is not None
        assert message["type"] == "ssubscribe"

    async def test_cluster_sharded_pubsub_creates_connections_lazily(
        self, r: redis.Redis
    ):
        channel = "lazy-connection-{shared}"
        node = r.get_node_from_key(channel)
        p = r.pubsub()
        try:
            assert p.node_pubsub_mapping == {}
            assert p.connection is None

            await p.ssubscribe(channel)
            await self._wait_for_ssubscribe_ack(p)

            assert list(p.node_pubsub_mapping) == [node.name]
            assert p.node_pubsub_mapping[node.name].connection is not None
        finally:
            await p.aclose()

    async def test_cluster_sharded_pubsub_reuses_connection_for_same_shard(
        self, r: redis.Redis
    ):
        first = "reuse-1-{same-shard}"
        second = "reuse-2-{same-shard}"
        node = r.get_node_from_key(first)
        assert node.name == r.get_node_from_key(second).name
        p = r.pubsub()
        try:
            await p.ssubscribe(first)
            await self._wait_for_ssubscribe_ack(p)
            first_mapping = dict(p.node_pubsub_mapping)
            first_pubsub = p.node_pubsub_mapping[node.name]

            await p.ssubscribe(second)
            await self._wait_for_ssubscribe_ack(p)

            assert p.node_pubsub_mapping == first_mapping
            assert p.node_pubsub_mapping[node.name] is first_pubsub
            assert len(p.node_pubsub_mapping) == 1
        finally:
            await p.aclose()


@pytest.mark.onlynoncluster
class TestPubSubSubscribeUnsubscribe:
    async def _test_subscribe_unsubscribe(
        self, p, sub_type, unsub_type, sub_func, unsub_func, keys
    ):
        for key in keys:
            assert await sub_func(key) is None

        # should be a message for each channel/pattern we just subscribed to
        for i, key in enumerate(keys):
            assert await wait_for_message(p) == make_message(sub_type, key, i + 1)

        for key in keys:
            assert await unsub_func(key) is None

        # should be a message for each channel/pattern we just unsubscribed
        # from
        for i, key in enumerate(keys):
            i = len(keys) - 1 - i
            assert await wait_for_message(p) == make_message(unsub_type, key, i)

    async def test_channel_subscribe_unsubscribe(self, pubsub):
        kwargs = make_subscribe_test_data(pubsub, "channel")
        await self._test_subscribe_unsubscribe(**kwargs)

    async def test_pattern_subscribe_unsubscribe(self, pubsub):
        kwargs = make_subscribe_test_data(pubsub, "pattern")
        await self._test_subscribe_unsubscribe(**kwargs)

    async def test_pattern_unsubscribe_with_a_list(self, pubsub):
        # Like unsubscribe() and the sync PubSub.punsubscribe(), the first
        # argument can be a list of patterns
        p = pubsub
        await p.psubscribe("f*", "b*")
        for _ in range(2):
            assert (await wait_for_message(p))["type"] == "psubscribe"

        assert await p.punsubscribe(["f*", "b*"]) is None
        for _ in range(2):
            assert (await wait_for_message(p))["type"] == "punsubscribe"
        assert p.subscribed is False

    @pytest.mark.onlynoncluster
    @skip_if_server_version_lt("7.0.0")
    async def test_shard_channel_subscribe_unsubscribe(self, pubsub):
        kwargs = make_subscribe_test_data(pubsub, "shard_channel")
        await self._test_subscribe_unsubscribe(**kwargs)

    @pytest.mark.onlynoncluster
    @skip_if_server_version_lt("7.0.0")
    async def test_get_sharded_message_reads_shard_channel_messages(self, r, pubsub):
        """``get_sharded_message`` mirrors the sync alias of ``get_message``."""
        assert await pubsub.ssubscribe("foo") is None
        assert (await pubsub.get_sharded_message(timeout=1))["type"] == "ssubscribe"

        assert await r.spublish("foo", "hello") == 1
        message = await pubsub.get_sharded_message(timeout=1)
        assert message["type"] == "smessage"
        assert message["channel"] == b"foo"
        assert message["data"] == b"hello"

    @pytest.mark.onlynoncluster
    async def _test_resubscribe_on_reconnection(
        self, p, sub_type, unsub_type, sub_func, unsub_func, keys
    ):
        for key in keys:
            assert await sub_func(key) is None

        # should be a message for each channel/pattern we just subscribed to
        for i, key in enumerate(keys):
            assert await wait_for_message(p) == make_message(sub_type, key, i + 1)

        # manually disconnect
        await p.connection.disconnect()

        # calling get_message again reconnects and resubscribes
        # note, we may not re-subscribe to channels in exactly the same order
        # so we have to do some extra checks to make sure we got them all
        messages = []
        for i in range(len(keys)):
            messages.append(await wait_for_message(p))

        unique_channels = set()
        assert len(messages) == len(keys)
        for i, message in enumerate(messages):
            assert message["type"] == sub_type
            assert message["data"] == i + 1
            assert isinstance(message["channel"], bytes)
            channel = message["channel"].decode("utf-8")
            unique_channels.add(channel)

        assert len(unique_channels) == len(keys)
        for channel in unique_channels:
            assert channel in keys

    async def test_resubscribe_to_channels_on_reconnection(self, pubsub):
        kwargs = make_subscribe_test_data(pubsub, "channel")
        await self._test_resubscribe_on_reconnection(**kwargs)

    async def test_resubscribe_to_patterns_on_reconnection(self, pubsub):
        kwargs = make_subscribe_test_data(pubsub, "pattern")
        await self._test_resubscribe_on_reconnection(**kwargs)

    async def test_resubscribe_binary_channel_on_reconnection(self, pubsub):
        """Binary channel names that are not valid UTF-8 must survive
        reconnection without raising ``UnicodeDecodeError``.
        See https://github.com/redis/redis-py/issues/3912
        """
        # b'\x80\x81\x82' is deliberately invalid UTF-8
        binary_channel = b"\x80\x81\x82"
        p = pubsub
        await p.subscribe(binary_channel)
        assert await wait_for_message(p) is not None  # consume subscribe ack

        # force reconnect
        await p.connection.disconnect()

        # get_message triggers on_connect → re-subscribe; must not raise
        messages = []
        for _ in range(1):
            message = await wait_for_message(p)
            assert message is not None
            messages.append(message)

        assert len(messages) == 1
        assert messages[0]["type"] == "subscribe"
        assert messages[0]["channel"] == binary_channel

    async def test_resubscribe_binary_pattern_on_reconnection(self, pubsub):
        """Binary pattern names that are not valid UTF-8 must survive
        reconnection without raising ``UnicodeDecodeError``.
        See https://github.com/redis/redis-py/issues/3912
        """
        binary_pattern = b"\x80\x81*"
        p = pubsub
        await p.psubscribe(binary_pattern)
        assert await wait_for_message(p) is not None  # consume psubscribe ack

        # force reconnect
        await p.connection.disconnect()

        messages = []
        for _ in range(1):
            message = await wait_for_message(p)
            assert message is not None
            messages.append(message)

        assert len(messages) == 1
        assert messages[0]["type"] == "psubscribe"
        assert messages[0]["channel"] == binary_pattern

    async def test_resubscribe_binary_channel_with_handler_on_reconnection(
        self, pubsub, r
    ):
        binary_channel = b"\x80\x81\x82"
        handled = []

        def handler(message):
            handled.append(message)

        p = pubsub
        await p.subscribe(Subscription(binary_channel, handler))
        assert await wait_for_message(p) == {
            "type": "subscribe",
            "pattern": None,
            "channel": binary_channel,
            "data": 1,
        }

        await p.connection.disconnect()

        assert await wait_for_message(p) == {
            "type": "subscribe",
            "pattern": None,
            "channel": binary_channel,
            "data": 1,
        }
        assert await r.publish(binary_channel, b"test message") == 1
        assert await p.get_message(timeout=1) is None
        assert handled == [
            {
                "type": "message",
                "pattern": None,
                "channel": binary_channel,
                "data": b"test message",
            }
        ]

    @skip_if_server_version_lt("7.0.0")
    async def test_standalone_pubsub_on_connect_batches_shard_channels(self, pubsub):
        """The standalone PubSub has no notion of slots and must keep
        batching every tracked shard channel into a single SSUBSCRIBE call
        on reconnect, as it did before cluster-aware resubscription existed.
        """
        p = pubsub
        ch_a = b"{slot-a}-one"
        ch_b = b"{slot-b}-one"
        assert key_slot(ch_a) != key_slot(ch_b)

        p.shard_channels = {ch_a: None, ch_b: None}

        calls = []

        async def fake_ssubscribe(*args, **kwargs):
            calls.append((tuple(args), dict(kwargs)))

        with mock.patch.object(p, "ssubscribe", side_effect=fake_ssubscribe):
            await p.on_connect(connection=None)

        assert len(calls) == 1
        args, kwargs = calls[0]
        assert set(args) == {ch_a, ch_b}
        assert kwargs == {}

    async def _test_subscribed_property(
        self, p, sub_type, unsub_type, sub_func, unsub_func, keys
    ):
        assert p.subscribed is False
        await sub_func(keys[0])
        # we're now subscribed even though we haven't processed the
        # reply from the server just yet
        assert p.subscribed is True
        assert await wait_for_message(p) == make_message(sub_type, keys[0], 1)
        # we're still subscribed
        assert p.subscribed is True

        # unsubscribe from all channels
        await unsub_func()
        # we're still technically subscribed until we process the
        # response messages from the server
        assert p.subscribed is True
        assert await wait_for_message(p) == make_message(unsub_type, keys[0], 0)
        # now we're no longer subscribed as no more messages can be delivered
        # to any channels we were listening to
        assert p.subscribed is False

        # subscribing again flips the flag back
        await sub_func(keys[0])
        assert p.subscribed is True
        assert await wait_for_message(p) == make_message(sub_type, keys[0], 1)

        # unsubscribe again
        await unsub_func()
        assert p.subscribed is True
        # subscribe to another channel before reading the unsubscribe response
        await sub_func(keys[1])
        assert p.subscribed is True
        # read the unsubscribe for key1
        assert await wait_for_message(p) == make_message(unsub_type, keys[0], 0)
        # we're still subscribed to key2, so subscribed should still be True
        assert p.subscribed is True
        # read the key2 subscribe message
        assert await wait_for_message(p) == make_message(sub_type, keys[1], 1)
        await unsub_func()
        # haven't read the message yet, so we're still subscribed
        assert p.subscribed is True
        assert await wait_for_message(p) == make_message(unsub_type, keys[1], 0)
        # now we're finally unsubscribed
        assert p.subscribed is False

    async def test_subscribe_property_with_channels(self, pubsub):
        kwargs = make_subscribe_test_data(pubsub, "channel")
        await self._test_subscribed_property(**kwargs)

    @pytest.mark.onlynoncluster
    async def test_subscribe_property_with_patterns(self, pubsub):
        kwargs = make_subscribe_test_data(pubsub, "pattern")
        await self._test_subscribed_property(**kwargs)

    @pytest.mark.onlynoncluster
    @skip_if_server_version_lt("7.0.0")
    async def test_subscribe_property_with_shard_channels(self, pubsub):
        kwargs = make_subscribe_test_data(pubsub, "shard_channel")
        await self._test_subscribed_property(**kwargs)

    async def test_aclosing(self, r: redis.Redis):
        p = r.pubsub()
        async with aclosing(p):
            assert p.subscribed is False
            await p.subscribe("foo")
            assert p.subscribed is True
        assert p.subscribed is False

    async def test_context_manager(self, r: redis.Redis):
        p = r.pubsub()
        async with p:
            assert p.subscribed is False
            await p.subscribe("foo")
            assert p.subscribed is True
        assert p.subscribed is False

    async def test_close_is_aclose(self, r: redis.Redis):
        """
        Test backwards compatible close method
        """
        p = r.pubsub()
        assert p.subscribed is False
        await p.subscribe("foo")
        assert p.subscribed is True
        with pytest.deprecated_call():
            await p.close()
        assert p.subscribed is False

    async def test_reset_is_aclose(self, r: redis.Redis):
        """
        Test backwards compatible reset method
        """
        p = r.pubsub()
        assert p.subscribed is False
        await p.subscribe("foo")
        assert p.subscribed is True
        with pytest.deprecated_call():
            await p.reset()
        assert p.subscribed is False

    async def test_ignore_all_subscribe_messages(self, r: redis.Redis):
        p = r.pubsub(ignore_subscribe_messages=True)

        checks = (
            (p.subscribe, "foo"),
            (p.unsubscribe, "foo"),
            (p.psubscribe, "f*"),
            (p.punsubscribe, "f*"),
        )

        assert p.subscribed is False
        for func, channel in checks:
            assert await func(channel) is None
            assert p.subscribed is True
            assert await p.get_message(timeout=1) is None
        assert p.subscribed is False
        await p.aclose()

    async def test_ignore_individual_subscribe_messages(self, pubsub):
        p = pubsub

        checks = (
            (p.subscribe, "foo"),
            (p.unsubscribe, "foo"),
            (p.psubscribe, "f*"),
            (p.punsubscribe, "f*"),
        )

        assert p.subscribed is False
        for func, channel in checks:
            assert await func(channel) is None
            assert p.subscribed is True
            message = await p.get_message(ignore_subscribe_messages=True, timeout=1)
            assert message is None
        assert p.subscribed is False

    async def test_sub_unsub_resub_channels(self, pubsub):
        kwargs = make_subscribe_test_data(pubsub, "channel")
        await self._test_sub_unsub_resub(**kwargs)

    @pytest.mark.onlynoncluster
    async def test_sub_unsub_resub_patterns(self, pubsub):
        kwargs = make_subscribe_test_data(pubsub, "pattern")
        await self._test_sub_unsub_resub(**kwargs)

    async def _test_sub_unsub_resub(
        self, p, sub_type, unsub_type, sub_func, unsub_func, keys
    ):
        # https://github.com/andymccurdy/redis-py/issues/764
        key = keys[0]
        await sub_func(key)
        await unsub_func(key)
        await sub_func(key)
        assert p.subscribed is True
        assert await wait_for_message(p) == make_message(sub_type, key, 1)
        assert await wait_for_message(p) == make_message(unsub_type, key, 0)
        assert await wait_for_message(p) == make_message(sub_type, key, 1)
        assert p.subscribed is True

    async def test_sub_unsub_all_resub_channels(self, pubsub):
        kwargs = make_subscribe_test_data(pubsub, "channel")
        await self._test_sub_unsub_all_resub(**kwargs)

    async def test_sub_unsub_all_resub_patterns(self, pubsub):
        kwargs = make_subscribe_test_data(pubsub, "pattern")
        await self._test_sub_unsub_all_resub(**kwargs)

    async def _test_sub_unsub_all_resub(
        self, p, sub_type, unsub_type, sub_func, unsub_func, keys
    ):
        # https://github.com/andymccurdy/redis-py/issues/764
        key = keys[0]
        await sub_func(key)
        await unsub_func()
        await sub_func(key)
        assert p.subscribed is True
        assert await wait_for_message(p) == make_message(sub_type, key, 1)
        assert await wait_for_message(p) == make_message(unsub_type, key, 0)
        assert await wait_for_message(p) == make_message(sub_type, key, 1)
        assert p.subscribed is True


@pytest.mark.fixed_client
class TestPubSubResubscribeMocked:
    """Resubscribe handling checked against mocked connections; no server needed."""

    async def test_cluster_pubsub_resubscribe_binary_shard_channel_with_handler(self):
        pool = MagicMock()
        encoder = Encoder(
            encoding="utf-8", encoding_errors="strict", decode_responses=False
        )
        p = PubSub(connection_pool=pool, encoder=encoder)
        binary_channel = b"\x80\x81\x82"
        handler = MagicMock()
        p.shard_channels = {binary_channel: handler}

        calls = []

        async def fake_ssubscribe(*args, **kwargs):
            calls.append((tuple(args), dict(kwargs)))

        with mock.patch.object(p, "ssubscribe", side_effect=fake_ssubscribe):
            await ClusterPubSub._resubscribe_shard_channels(p)

        assert calls == [((Subscription(binary_channel, handler),), {})]

    async def test_cluster_pubsub_resubscribe_shard_channels_groups_by_slot(self):
        """The cluster-aware ``_resubscribe_shard_channels`` must group shard
        channels by hash slot on reconnect. A cluster node can own multiple
        slot ranges, so a single batched SSUBSCRIBE across different slots
        would be rejected by Redis with CROSSSLOT and resubscription would
        fail silently. The same method is bound to the per-node PubSub
        instances managed by ``ClusterPubSub``.
        """
        # Pure client-side logic: mock the pool, use a real encoder, no
        # server needed. ``key_slot`` is client-side CRC16, so slot
        # assignments are deterministic without a cluster.
        pool = MagicMock()
        encoder = Encoder(
            encoding="utf-8", encoding_errors="strict", decode_responses=False
        )
        p = PubSub(connection_pool=pool, encoder=encoder)

        # Two channels share a hash tag (same slot); a third hashes to a
        # different slot.
        ch_a1 = b"{slot-a}-one"
        ch_a2 = b"{slot-a}-two"
        ch_b = b"{slot-b}-one"
        assert key_slot(ch_a1) == key_slot(ch_a2)
        assert key_slot(ch_a1) != key_slot(ch_b)

        # Populate state as if previously subscribed, without going to the wire.
        p.shard_channels = {ch_a1: None, ch_a2: None, ch_b: None}

        calls = []

        async def fake_ssubscribe(*args, **kwargs):
            calls.append((tuple(args), dict(kwargs)))

        with mock.patch.object(p, "ssubscribe", side_effect=fake_ssubscribe):
            await ClusterPubSub._resubscribe_shard_channels(p)

        # Every ssubscribe call must target exactly one slot.
        slots_per_call = []
        all_channels = set()
        for args, kwargs in calls:
            channels = set(args) | set(
                k.encode() if isinstance(k, str) else k for k in kwargs
            )
            assert channels, "ssubscribe called with no channels"
            call_slots = {key_slot(c) for c in channels}
            assert len(call_slots) == 1, (
                f"ssubscribe grouped channels from multiple slots: {call_slots}"
            )
            slots_per_call.append(next(iter(call_slots)))
            all_channels.update(channels)

        # All original channels must be resubscribed, across exactly two
        # distinct slot groups.
        assert all_channels == {ch_a1, ch_a2, ch_b}
        assert set(slots_per_call) == {key_slot(ch_a1), key_slot(ch_b)}
        # Channels sharing a slot are batched: two unique slots => two calls.
        assert len(calls) == 2

    async def test_resubscribe_binary_handler_uses_subscription(self):
        pool = MagicMock()
        encoder = Encoder(
            encoding="utf-8", encoding_errors="strict", decode_responses=False
        )
        p = PubSub(connection_pool=pool, encoder=encoder)
        binary_channel = b"\x80\x81\x82"
        handler = MagicMock()
        p.channels = {binary_channel: handler}

        calls = []

        async def fake_subscribe(*args, **kwargs):
            calls.append((tuple(args), dict(kwargs)))

        with mock.patch.object(p, "subscribe", side_effect=fake_subscribe):
            await p._resubscribe(p.channels, p.subscribe)

        assert calls == [((Subscription(binary_channel, handler),), {})]


@pytest.mark.onlynoncluster
class TestPubSubMessages:
    def setup_method(self, method):
        self.message = None

    def message_handler(self, message):
        self.message = message

    async def async_message_handler(self, message):
        self.async_message = message

    async def test_published_message_to_channel(self, r: redis.Redis, pubsub):
        p = pubsub
        await p.subscribe("foo")
        assert await wait_for_message(p) == make_message("subscribe", "foo", 1)
        assert await r.publish("foo", "test message") == 1

        message = await wait_for_message(p)
        assert isinstance(message, dict)
        assert message == make_message("message", "foo", "test message")

    async def test_published_message_to_pattern(self, r: redis.Redis, pubsub):
        p = pubsub
        await p.subscribe("foo")
        await p.psubscribe("f*")
        assert await wait_for_message(p) == make_message("subscribe", "foo", 1)
        assert await wait_for_message(p) == make_message("psubscribe", "f*", 2)
        # 1 to pattern, 1 to channel
        assert await r.publish("foo", "test message") == 2

        message1 = await wait_for_message(p)
        message2 = await wait_for_message(p)
        assert isinstance(message1, dict)
        assert isinstance(message2, dict)

        expected = [
            make_message("message", "foo", "test message"),
            make_message("pmessage", "foo", "test message", pattern="f*"),
        ]

        assert message1 in expected
        assert message2 in expected
        assert message1 != message2

    async def test_channel_message_handler(self, r: redis.Redis):
        p = r.pubsub(ignore_subscribe_messages=True)
        await p.subscribe(foo=self.message_handler)
        assert await p.get_message(timeout=1) is None
        assert await r.publish("foo", "test message") == 1
        await wait_for_handled_message(
            self, p.get_message, make_message("message", "foo", "test message")
        )
        await p.aclose()

    async def test_binary_channel_message_handler_with_subscription(self, r):
        p = r.pubsub(ignore_subscribe_messages=True)
        channel = b"\x80\x81\x82"
        await p.subscribe(Subscription(channel, self.message_handler))
        assert await p.get_message(timeout=1) is None
        assert await r.publish(channel, b"test message") == 1
        await wait_for_handled_message(
            self,
            p.get_message,
            {
                "type": "message",
                "pattern": None,
                "channel": channel,
                "data": b"test message",
            },
        )
        await p.aclose()

    async def test_channel_async_message_handler(self, r):
        p = r.pubsub(ignore_subscribe_messages=True)
        await p.subscribe(foo=self.async_message_handler)
        assert await p.get_message(timeout=1) is None
        assert await r.publish("foo", "test message") == 1
        assert await p.get_message(timeout=1) is None
        assert self.async_message == make_message("message", "foo", "test message")
        await p.aclose()

    async def test_channel_sync_async_message_handler(self, r):
        p = r.pubsub(ignore_subscribe_messages=True)
        await p.subscribe(foo=self.message_handler)
        await p.subscribe(bar=self.async_message_handler)
        # get_message handles one message per call: two acks, then two messages
        assert await p.get_message(timeout=1) is None
        assert await p.get_message(timeout=1) is None
        assert await r.publish("foo", "test message") == 1
        assert await r.publish("bar", "test message 2") == 1
        assert await p.get_message(timeout=1) is None
        assert await p.get_message(timeout=1) is None
        assert self.message == make_message("message", "foo", "test message")
        assert self.async_message == make_message("message", "bar", "test message 2")
        await p.aclose()

    @skip_if_server_version_lt("7.0.0")
    async def test_binary_shard_channel_message_handler_with_subscription(self, r):
        p = r.pubsub(ignore_subscribe_messages=True)
        channel = b"\x80\x81\x82"
        await p.ssubscribe(Subscription(channel, self.message_handler))
        assert await p.get_message(timeout=1) is None
        assert await r.spublish(channel, b"test message") == 1
        await wait_for_handled_message(
            self,
            p.get_message,
            {
                "type": "smessage",
                "pattern": None,
                "channel": channel,
                "data": b"test message",
            },
        )
        await p.aclose()

    @pytest.mark.onlynoncluster
    async def test_pattern_message_handler(self, r: redis.Redis):
        p = r.pubsub(ignore_subscribe_messages=True)
        await p.psubscribe(**{"f*": self.message_handler})
        assert await p.get_message(timeout=1) is None
        assert await r.publish("foo", "test message") == 1
        await wait_for_handled_message(
            self,
            p.get_message,
            make_message("pmessage", "foo", "test message", pattern="f*"),
        )
        await p.aclose()

    @pytest.mark.onlynoncluster
    async def test_binary_pattern_message_handler_with_subscription(self, r):
        p = r.pubsub(ignore_subscribe_messages=True)
        pattern = b"\x80*"
        channel = b"\x80channel"
        await p.psubscribe(Subscription(pattern, self.message_handler))
        assert await p.get_message(timeout=1) is None
        assert await r.publish(channel, b"test message") == 1
        await wait_for_handled_message(
            self,
            p.get_message,
            {
                "type": "pmessage",
                "pattern": pattern,
                "channel": channel,
                "data": b"test message",
            },
        )
        await p.aclose()

    async def test_unicode_channel_message_handler(self, r: redis.Redis):
        p = r.pubsub(ignore_subscribe_messages=True)
        channel = "uni" + chr(4456) + "code"
        channels = {channel: self.message_handler}
        await p.subscribe(**channels)
        assert await p.get_message(timeout=1) is None
        assert await r.publish(channel, "test message") == 1
        await wait_for_handled_message(
            self, p.get_message, make_message("message", channel, "test message")
        )
        await p.aclose()

    @pytest.mark.onlynoncluster
    # see: https://redis.readthedocs.io/en/stable/clustering.html#known-pubsub-limitations
    async def test_unicode_pattern_message_handler(self, r: redis.Redis):
        p = r.pubsub(ignore_subscribe_messages=True)
        pattern = "uni" + chr(4456) + "*"
        channel = "uni" + chr(4456) + "code"
        await p.psubscribe(**{pattern: self.message_handler})
        assert await p.get_message(timeout=1) is None
        assert await r.publish(channel, "test message") == 1
        await wait_for_handled_message(
            self,
            p.get_message,
            make_message("pmessage", channel, "test message", pattern=pattern),
        )
        await p.aclose()

    async def test_get_message_without_subscribe(self, r: redis.Redis, pubsub):
        p = pubsub
        with pytest.raises(RuntimeError) as info:
            await p.get_message()
        expect = (
            "connection not set: did you forget to call subscribe() or psubscribe()?"
        )
        assert expect in info.exconly()


@pytest.mark.onlynoncluster
class TestPubSubRESP3Handler:
    async def my_handler(self, message):
        self.message = ["my handler", message]

    async def test_push_handler(self, r):
        if get_protocol_version(r) in [2, "2", None]:
            return
        p = r.pubsub(push_handler_func=self.my_handler)
        await p.subscribe("foo")
        await wait_for_handled_message(
            self, p.get_message, ["my handler", [b"subscribe", b"foo", 1]]
        )
        assert await r.publish("foo", "test message") == 1
        await wait_for_handled_message(
            self, p.get_message, ["my handler", [b"message", b"foo", b"test message"]]
        )


@pytest.mark.onlynoncluster
class TestPubSubAutoDecoding:
    """These tests only validate that we get unicode values back"""

    channel = "uni" + chr(4456) + "code"
    pattern = "uni" + chr(4456) + "*"
    data = "abc" + chr(4458) + "123"

    def make_message(self, type, channel, data, pattern=None):
        return {"type": type, "channel": channel, "pattern": pattern, "data": data}

    def setup_method(self, method):
        self.message = None

    def message_handler(self, message):
        self.message = message

    @pytest_asyncio.fixture()
    async def r(self, create_redis):
        return await create_redis(decode_responses=True)

    async def test_channel_subscribe_unsubscribe(self, pubsub):
        p = pubsub
        await p.subscribe(self.channel)
        assert await wait_for_message(p) == self.make_message(
            "subscribe", self.channel, 1
        )

        await p.unsubscribe(self.channel)
        assert await wait_for_message(p) == self.make_message(
            "unsubscribe", self.channel, 0
        )

    async def test_pattern_subscribe_unsubscribe(self, pubsub):
        p = pubsub
        await p.psubscribe(self.pattern)
        assert await wait_for_message(p) == self.make_message(
            "psubscribe", self.pattern, 1
        )

        await p.punsubscribe(self.pattern)
        assert await wait_for_message(p) == self.make_message(
            "punsubscribe", self.pattern, 0
        )

    async def test_channel_publish(self, r: redis.Redis, pubsub):
        p = pubsub
        await p.subscribe(self.channel)
        assert await wait_for_message(p) == self.make_message(
            "subscribe", self.channel, 1
        )
        await r.publish(self.channel, self.data)
        assert await wait_for_message(p) == self.make_message(
            "message", self.channel, self.data
        )

    @pytest.mark.onlynoncluster
    async def test_pattern_publish(self, r: redis.Redis, pubsub):
        p = pubsub
        await p.psubscribe(self.pattern)
        assert await wait_for_message(p) == self.make_message(
            "psubscribe", self.pattern, 1
        )
        await r.publish(self.channel, self.data)
        assert await wait_for_message(p) == self.make_message(
            "pmessage", self.channel, self.data, pattern=self.pattern
        )

    async def test_channel_message_handler(self, r: redis.Redis):
        p = r.pubsub(ignore_subscribe_messages=True)
        await p.subscribe(**{self.channel: self.message_handler})
        assert await p.get_message(timeout=1) is None
        await r.publish(self.channel, self.data)
        await wait_for_handled_message(
            self, p.get_message, self.make_message("message", self.channel, self.data)
        )

        # test that we reconnected to the correct channel
        self.message = None
        await p.connection.disconnect()
        assert await p.get_message(timeout=1) is None  # should reconnect
        new_data = self.data + "new data"
        await r.publish(self.channel, new_data)
        await wait_for_handled_message(
            self, p.get_message, self.make_message("message", self.channel, new_data)
        )
        await p.aclose()

    async def test_pattern_message_handler(self, r: redis.Redis):
        p = r.pubsub(ignore_subscribe_messages=True)
        await p.psubscribe(**{self.pattern: self.message_handler})
        assert await p.get_message(timeout=1) is None
        await r.publish(self.channel, self.data)
        await wait_for_handled_message(
            self,
            p.get_message,
            self.make_message(
                "pmessage", self.channel, self.data, pattern=self.pattern
            ),
        )

        # test that we reconnected to the correct pattern
        self.message = None
        await p.connection.disconnect()
        assert await p.get_message(timeout=1) is None  # should reconnect
        new_data = self.data + "new data"
        await r.publish(self.channel, new_data)
        await wait_for_handled_message(
            self,
            p.get_message,
            self.make_message("pmessage", self.channel, new_data, pattern=self.pattern),
        )
        await p.aclose()

    async def test_context_manager(self, r: redis.Redis):
        async with r.pubsub() as pubsub:
            await pubsub.subscribe("foo")
            assert pubsub.connection is not None

        assert pubsub.connection is None
        assert pubsub.channels == {}
        assert pubsub.patterns == {}
        await pubsub.aclose()


@pytest.mark.onlynoncluster
class TestPubSubRedisDown:
    async def test_channel_subscribe(self):
        # nothing listens on this port, so skip the default backoff retries
        r = redis.Redis(host="localhost", port=6390, retry=Retry(NoBackoff(), 0))
        p = r.pubsub()
        with pytest.raises(ConnectionError):
            await p.subscribe("foo")


@pytest.mark.onlynoncluster
class TestPubSubSubcommands:
    @pytest.mark.onlynoncluster
    @skip_if_server_version_lt("2.8.0")
    async def test_pubsub_channels(self, r: redis.Redis, pubsub):
        p = pubsub
        await p.subscribe("foo", "bar", "baz", "quux")
        for i in range(4):
            assert (await wait_for_message(p))["type"] == "subscribe"
        expected = [b"bar", b"baz", b"foo", b"quux"]
        assert all([channel in await r.pubsub_channels() for channel in expected])

    @pytest.mark.onlynoncluster
    @skip_if_server_version_lt("2.8.0")
    async def test_pubsub_numsub(self, r: redis.Redis):
        p1 = r.pubsub()
        await p1.subscribe("foo", "bar", "baz")
        for i in range(3):
            assert (await wait_for_message(p1))["type"] == "subscribe"
        p2 = r.pubsub()
        await p2.subscribe("bar", "baz")
        for i in range(2):
            assert (await wait_for_message(p2))["type"] == "subscribe"
        p3 = r.pubsub()
        await p3.subscribe("baz")
        assert (await wait_for_message(p3))["type"] == "subscribe"

        channels = [(b"foo", 1), (b"bar", 2), (b"baz", 3)]
        assert await r.pubsub_numsub("foo", "bar", "baz") == channels
        await p1.aclose()
        await p2.aclose()
        await p3.aclose()

    @skip_if_server_version_lt("2.8.0")
    async def test_pubsub_numpat(self, r: redis.Redis):
        p = r.pubsub()
        await p.psubscribe("*oo", "*ar", "b*z")
        for i in range(3):
            assert (await wait_for_message(p))["type"] == "psubscribe"
        assert await r.pubsub_numpat() == 3
        await p.aclose()


@pytest.mark.onlynoncluster
class TestPubSubPings:
    @pytest.mark.fixed_client
    @skip_if_server_version_lt("6.0.0")
    @pytest.mark.parametrize("protocol", [2, 3])
    @pytest.mark.parametrize("decode_responses", [False, True])
    @pytest.mark.parametrize("message", [None, "x", "hello\u2603"])
    @pytest.mark.parametrize("read_method", ["get_message", "listen"])
    async def test_ping_response_shapes(
        self, create_redis, protocol, decode_responses, message, read_method
    ):
        client = await create_redis(
            protocol=protocol, decode_responses=decode_responses
        )
        async with client.pubsub() as pubsub:
            await pubsub.subscribe("foo")
            assert (await pubsub.get_message(timeout=1))["type"] == "subscribe"
            await pubsub.ping(message)

            if read_method == "get_message":
                response = await pubsub.get_message(timeout=1)
            else:
                messages = pubsub.listen()
                try:
                    response = await asyncio.wait_for(messages.__anext__(), timeout=1)
                finally:
                    await messages.aclose()

            data = message if message is not None else ""
            assert response == {
                "type": "pong",
                "pattern": None,
                "channel": None,
                "data": data if decode_responses else data.encode(),
            }

    @skip_if_server_version_lt("3.0.0")
    async def test_send_pubsub_ping(self, r: redis.Redis):
        p = r.pubsub(ignore_subscribe_messages=True)
        await p.subscribe("foo")
        await p.ping()
        assert await wait_for_message(p) == make_message(
            type="pong", channel=None, data="", pattern=None
        )
        await p.aclose()

    @skip_if_server_version_lt("3.0.0")
    async def test_send_pubsub_ping_message(self, r: redis.Redis):
        p = r.pubsub(ignore_subscribe_messages=True)
        await p.subscribe("foo")
        await p.ping(message="hello world")
        assert await wait_for_message(p) == make_message(
            type="pong", channel=None, data="hello world", pattern=None
        )
        await p.aclose()


@pytest.mark.onlynoncluster
class TestPubSubHealthCheckResponse:
    """Tests for health check response validation with different decode_responses settings"""

    async def test_health_check_response_decode_false_list_format(self, r: redis.Redis):
        """Test health_check_response includes list format with decode_responses=False"""
        p = r.pubsub()
        # List format: [b"pong", b"redis-py-health-check"]
        assert [b"pong", b"redis-py-health-check"] in p.health_check_response
        await p.aclose()

    async def test_health_check_response_decode_false_bytes_format(
        self, r: redis.Redis
    ):
        """Test health_check_response includes bytes format with decode_responses=False"""
        p = r.pubsub()
        # Bytes format: b"redis-py-health-check"
        assert b"redis-py-health-check" in p.health_check_response
        await p.aclose()

    async def test_health_check_response_decode_true_list_format(self, create_redis):
        """Test health_check_response includes list format with decode_responses=True"""
        r = await create_redis(decode_responses=True)
        p = r.pubsub()
        # List format: ["pong", "redis-py-health-check"]
        assert ["pong", "redis-py-health-check"] in p.health_check_response
        await p.aclose()
        await r.aclose()

    async def test_health_check_response_decode_true_string_format(self, create_redis):
        """Test health_check_response includes string format with decode_responses=True"""
        r = await create_redis(decode_responses=True)
        p = r.pubsub()
        # String format: "redis-py-health-check" (THE FIX!)
        assert "redis-py-health-check" in p.health_check_response
        await p.aclose()
        await r.aclose()

    async def test_health_check_response_decode_false_excludes_string(
        self, r: redis.Redis
    ):
        """Test health_check_response excludes string format with decode_responses=False"""
        p = r.pubsub()
        # String format should NOT be in the list when decode_responses=False
        assert "redis-py-health-check" not in p.health_check_response
        await p.aclose()

    async def test_health_check_response_decode_true_excludes_bytes(self, create_redis):
        """Test health_check_response excludes bytes format with decode_responses=True"""
        r = await create_redis(decode_responses=True)
        p = r.pubsub()
        # Bytes format should NOT be in the list when decode_responses=True
        assert b"redis-py-health-check" not in p.health_check_response
        await p.aclose()
        await r.aclose()


@pytest.mark.onlynoncluster
class TestPubSubConnectionKilled:
    @skip_if_server_version_lt("3.0.0")
    async def test_connection_error_raised_when_connection_dies(
        self, r: redis.Redis, pubsub
    ):
        p = pubsub
        await p.subscribe("foo")
        assert await wait_for_message(p) == make_message("subscribe", "foo", 1)
        for client in await r.client_list():
            if client["cmd"] == "subscribe":
                await r.client_kill_filter(_id=client["id"])
        with pytest.raises(ConnectionError):
            await wait_for_message(p)


@pytest.mark.onlynoncluster
class TestPubSubTimeouts:
    async def test_get_message_with_timeout_returns_none(self, pubsub):
        p = pubsub
        await p.subscribe("foo")
        assert await wait_for_message(p) == make_message("subscribe", "foo", 1)
        assert await p.get_message(timeout=0.01) is None


@pytest.mark.onlynoncluster
class TestPubSubReconnect:
    @with_timeout(2)
    async def test_reconnect_listen(self, r: redis.Redis, pubsub):
        """
        Test that a loop processing PubSub messages can survive
        a disconnect, by issuing a connect() call.
        """
        messages = asyncio.Queue()
        interrupt = False

        async def loop():
            # must make sure the task exits
            async with async_timeout(2):
                nonlocal interrupt
                await pubsub.subscribe("foo")
                while True:
                    try:
                        try:
                            await pubsub.connect()
                            await loop_step()
                        except redis.ConnectionError:
                            await asyncio.sleep(0.1)
                    except asyncio.CancelledError:
                        # we use a cancel to interrupt the "listen"
                        # when we perform a disconnect
                        if interrupt:
                            interrupt = False
                        else:
                            raise

        async def loop_step():
            # get a single message via listen()
            async for message in pubsub.listen():
                await messages.put(message)
                break

        task = asyncio.get_running_loop().create_task(loop())
        # get the initial connect message
        async with async_timeout(1):
            message = await messages.get()
        assert message == {
            "channel": b"foo",
            "data": 1,
            "pattern": None,
            "type": "subscribe",
        }
        # now, disconnect the connection.
        await pubsub.connection.disconnect()
        interrupt = True
        task.cancel()  # interrupt the listen call
        # await another auto-connect message
        message = await messages.get()
        assert message == {
            "channel": b"foo",
            "data": 1,
            "pattern": None,
            "type": "subscribe",
        }
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task


@pytest.mark.fixed_client
async def test_run_requires_a_handler_for_every_shard_channel():
    p = PubSub(MagicMock(), ignore_subscribe_messages=True)
    p.shard_channels = {b"foo": None}

    with pytest.raises(
        redis.PubSubError, match="Shard Channel: 'b'foo'' has no handler registered"
    ):
        await p.run()


@pytest.mark.onlynoncluster
class TestPubSubRun:
    async def _subscribe(self, p, *args, **kwargs):
        await p.subscribe(*args, **kwargs)
        # Wait for the server to act on the subscription, to be sure that
        # a subsequent publish on another connection will reach the pubsub.
        while True:
            message = await p.get_message(timeout=1)
            if (
                message is not None
                and message["type"] == "subscribe"
                and message["channel"] == b"foo"
            ):
                return

    async def test_callbacks(self, r: redis.Redis, pubsub):
        def callback(message):
            messages.put_nowait(message)

        messages = asyncio.Queue()
        p = pubsub
        await self._subscribe(p, foo=callback)
        task = asyncio.get_running_loop().create_task(p.run())
        await r.publish("foo", "bar")
        message = await messages.get()
        task.cancel()
        try:
            await task
        except asyncio.CancelledError:
            pass
        assert message == {
            "channel": b"foo",
            "data": b"bar",
            "pattern": None,
            "type": "message",
        }

    async def test_exception_handler(self, r: redis.Redis, pubsub):
        def exception_handler_callback(e, pubsub) -> None:
            assert pubsub == p
            exceptions.put_nowait(e)

        exceptions = asyncio.Queue()
        p = pubsub
        await self._subscribe(p, foo=lambda x: None)
        with mock.patch.object(p, "get_message", side_effect=Exception("error")):
            task = asyncio.get_running_loop().create_task(
                p.run(exception_handler=exception_handler_callback)
            )
            e = await exceptions.get()
            task.cancel()
            try:
                await task
            except asyncio.CancelledError:
                pass
        assert str(e) == "error"

    async def test_late_subscribe(self, r: redis.Redis, pubsub):
        def callback(message):
            messages.put_nowait(message)

        messages = asyncio.Queue()
        p = pubsub
        # Establish the connection before run() so CI does not race startup.
        await p.connect()
        task = asyncio.get_running_loop().create_task(p.run())
        await p.subscribe(foo=callback)
        # wait tof the subscribe to finish.  Cannot use _subscribe() because
        # p.run() is already accepting messages
        while True:
            n = await r.publish("foo", "bar")
            if n == 1:
                break
            await asyncio.sleep(0.1)
        message_task = asyncio.create_task(messages.get())
        done, _ = await asyncio.wait(
            {message_task, task},
            timeout=1,
            return_when=asyncio.FIRST_COMPLETED,
        )
        if task in done:
            message_task.cancel()
            await asyncio.gather(message_task, return_exceptions=True)
            await task
            pytest.fail("PubSub run task exited before receiving late subscription")
        if message_task not in done:
            message_task.cancel()
            await asyncio.gather(message_task, return_exceptions=True)
            pytest.fail("Timed out waiting for late subscription message")

        message = message_task.result()
        task.cancel()
        # we expect a cancelled error, not the Runtime error
        # ("did you forget to call subscribe()"")
        with pytest.raises(asyncio.CancelledError):
            await task
        assert message == {
            "channel": b"foo",
            "data": b"bar",
            "pattern": None,
            "type": "message",
        }


# @pytest.mark.xfail
@pytest.mark.parametrize("method", ["get_message", "listen"])
@pytest.mark.onlynoncluster
class TestPubSubAutoReconnect:
    timeout = 4

    async def mysetup(self, r, method):
        self.messages = asyncio.Queue()
        self.pubsub = r.pubsub()
        # State: 0 = initial state , 1 = after disconnect, 2 = ConnectionError is seen,
        # 3=successfully reconnected 4 = exit
        self.state = 0
        self.cond = asyncio.Condition()
        if method == "get_message":
            self.get_message = self.loop_step_get_message
        else:
            self.get_message = self.loop_step_listen

        self.task = create_task(self.loop())
        # get the initial connect message
        message = await self.messages.get()
        assert message == {
            "channel": b"foo",
            "data": 1,
            "pattern": None,
            "type": "subscribe",
        }

    async def myfinish(self):
        message = await self.messages.get()
        assert message == {
            "channel": b"foo",
            "data": 1,
            "pattern": None,
            "type": "subscribe",
        }

    async def mykill(self):
        # kill thread
        async with self.cond:
            self.state = 4  # quit
        await self.task

    async def test_reconnect_socket_error(self, r: redis.Redis, method):
        """
        Test that a socket error will cause reconnect
        """
        try:
            async with async_timeout(self.timeout):
                await self.mysetup(r, method)
                # now, disconnect the connection, and wait for it to be re-established
                async with self.cond:
                    assert self.state == 0
                    self.state = 1
                    with mock.patch.object(self.pubsub.connection, "_parser") as m:
                        m.read_response.side_effect = socket.error
                        m.can_read_destructive.side_effect = socket.error
                        # wait until task noticies the disconnect until we
                        # undo the patch
                        await self.cond.wait_for(lambda: self.state >= 2)
                        assert not self.pubsub.connection.is_connected
                        # it is in a disconnecte state
                    # wait for reconnect
                    await self.cond.wait_for(
                        lambda: self.pubsub.connection.is_connected
                    )
                    assert self.state == 3

                await self.myfinish()
        finally:
            await self.mykill()

    async def test_reconnect_disconnect(self, r: redis.Redis, method):
        """
        Test that a manual disconnect() will cause reconnect
        """
        try:
            async with async_timeout(self.timeout):
                await self.mysetup(r, method)
                # now, disconnect the connection, and wait for it to be re-established
                async with self.cond:
                    self.state = 1
                    await self.pubsub.connection.disconnect()
                    assert not self.pubsub.connection.is_connected
                    # wait for reconnect
                    await self.cond.wait_for(
                        lambda: self.pubsub.connection.is_connected
                    )
                    assert self.state == 3

                await self.myfinish()
        finally:
            await self.mykill()

    async def loop(self):
        # reader loop, performing state transitions as it
        # discovers disconnects and reconnects
        await self.pubsub.subscribe("foo")
        while True:
            await asyncio.sleep(0.01)  # give main thread chance to get lock
            async with self.cond:
                old_state = self.state
                try:
                    if self.state == 4:
                        break
                    got_msg = await self.get_message()
                    assert got_msg
                    if self.state in (1, 2):
                        self.state = 3  # successful reconnect
                except redis.ConnectionError:
                    assert self.state in (1, 2)
                    self.state = 2  # signal that we noticed the disconnect
                finally:
                    self.cond.notify()
                # make sure that we did notice the connection error
                # or reconnected without any error
                if old_state == 1:
                    assert self.state in (2, 3)

    async def loop_step_get_message(self):
        # get a single message via get_message
        message = await self.pubsub.get_message(timeout=0.1)
        if message is not None:
            await self.messages.put(message)
            return True
        return False

    async def loop_step_listen(self):
        # get a single message via listen()
        try:
            async with async_timeout(0.5):
                async for message in self.pubsub.listen():
                    await self.messages.put(message)
                    return True
        except asyncio.TimeoutError:
            return False


@pytest.mark.onlynoncluster
class TestBaseException:
    async def test_outer_timeout(self, r: redis.Redis):
        """
        Using asyncio_timeout manually outside the inner method timeouts works.
        This works on Python versions 3.8 and greater, at which time asyncio.
        CancelledError became a BaseException instead of an Exception before.
        """
        pubsub = r.pubsub()
        await pubsub.subscribe("foo")
        assert pubsub.connection.is_connected

        async def get_msg_or_timeout(timeout=0.1):
            async with async_timeout(timeout):
                # blocking method to return messages
                while True:
                    response = await pubsub.parse_response(block=True)
                    message = await pubsub.handle_message(
                        response, ignore_subscribe_messages=False
                    )
                    if message is not None:
                        return message

        # get subscribe message
        msg = await get_msg_or_timeout(10)
        assert msg is not None
        # timeout waiting for another message which never arrives
        assert pubsub.connection.is_connected
        with pytest.raises(asyncio.TimeoutError):
            await get_msg_or_timeout()
        # the timeout on the read should not cause disconnect
        assert pubsub.connection.is_connected

    async def test_base_exception(self, r: redis.Redis):
        """
        Manually trigger a BaseException inside the parser's .read_response method
        and verify that it isn't caught
        """
        pubsub = r.pubsub()
        await pubsub.subscribe("foo")
        assert pubsub.connection.is_connected

        async def get_msg():
            # blocking method to return messages
            while True:
                response = await pubsub.parse_response(block=True)
                message = await pubsub.handle_message(
                    response, ignore_subscribe_messages=False
                )
                if message is not None:
                    return message

        # get subscribe message
        msg = await get_msg()
        assert msg is not None
        # timeout waiting for another message which never arrives
        assert pubsub.connection.is_connected
        with (
            patch("redis._parsers._AsyncRESP2Parser.read_response") as mock1,
            patch("redis._parsers._AsyncHiredisParser.read_response") as mock2,
            patch("redis._parsers._AsyncRESP3Parser.read_response") as mock3,
        ):
            mock1.side_effect = BaseException("boom")
            mock2.side_effect = BaseException("boom")
            mock3.side_effect = BaseException("boom")

            with pytest.raises(BaseException):
                await get_msg()

        # the timeout on the read should not cause disconnect
        assert pubsub.connection.is_connected


@pytest.mark.onlynoncluster
class TestAsyncPubSubTimeoutPropagation:
    """
    Tests for timeout propagation through the entire async pubsub read chain.
    Ensures that timeouts are properly passed from get_message() through
    parse_response() to the parser and socket buffer layers.
    """

    @pytest.mark.asyncio
    async def test_get_message_timeout_is_respected(self, r):
        """
        Test that get_message() with timeout parameter respects the timeout
        and returns None when no message arrives within the timeout period.
        """
        p = r.pubsub()
        await p.subscribe("foo")
        # Read subscription message
        msg = await wait_for_message(p, timeout=1.0)
        assert msg is not None
        assert msg["type"] == "subscribe"

        # Call get_message with a short timeout - should return None
        start = asyncio.get_running_loop().time()
        msg = await p.get_message(timeout=0.1)
        elapsed = asyncio.get_running_loop().time() - start
        assert msg is None
        # Verify timeout was actually respected (within reasonable bounds)
        assert elapsed < 1.0  # generous: a stalled runner must not fail it
        await p.aclose()

    @pytest.mark.asyncio
    async def test_get_message_timeout_with_published_message(self, r):
        """
        Test that get_message() with timeout returns a message if one
        arrives before the timeout expires.
        """
        p = r.pubsub()
        await p.subscribe("foo")
        # Read subscription message
        msg = await wait_for_message(p, timeout=1.0)
        assert msg is not None

        # Publish a message
        await r.publish("foo", "hello")

        # get_message with timeout should return the message
        msg = await p.get_message(timeout=1.0)
        assert msg is not None
        assert msg["type"] == "message"
        assert msg["data"] == b"hello"
        await p.aclose()

    @pytest.mark.asyncio
    async def test_parse_response_timeout_propagation(self, r):
        """
        Test that parse_response() properly propagates timeout to read_response().
        """
        p = r.pubsub()
        await p.subscribe("foo")
        # Read subscription message
        msg = await wait_for_message(p, timeout=1.0)
        assert msg is not None

        # Call parse_response with timeout - should respect it
        start = asyncio.get_running_loop().time()
        response = await p.parse_response(block=False, timeout=0.1)
        elapsed = asyncio.get_running_loop().time() - start
        assert response is None
        assert elapsed < 1.0  # generous: a stalled runner must not fail it
        await p.aclose()

    @pytest.mark.asyncio
    async def test_get_message_timeout_zero_returns_immediately(self, r):
        """
        Test that get_message(timeout=0) returns immediately without blocking.
        """
        p = r.pubsub()
        await p.subscribe("foo")
        # Read subscription message
        msg = await wait_for_message(p, timeout=1.0)
        assert msg is not None

        # get_message with timeout=0 should return immediately
        start = asyncio.get_running_loop().time()
        msg = await p.get_message(timeout=0)
        elapsed = asyncio.get_running_loop().time() - start
        assert msg is None
        assert elapsed < 0.5  # well below any blocking wait, above a runner stall
        await p.aclose()

    @pytest.mark.asyncio
    async def test_get_message_timeout_none_blocks(self, r):
        """
        Test that get_message(timeout=None) blocks indefinitely.
        We test this by using a task to publish a message after a delay.
        """
        p = r.pubsub()
        await p.subscribe("foo")
        # Read subscription message
        msg = await wait_for_message(p, timeout=1.0)
        assert msg is not None

        # Publish a message after a short delay in a task
        start_publishing = asyncio.Event()

        async def publish_after_delay():
            await start_publishing.wait()
            await asyncio.sleep(0.2)
            await r.publish("foo", "delayed_message")

        task = asyncio.create_task(publish_after_delay())

        # get_message with timeout=None should block until message arrives
        start = asyncio.get_running_loop().time()
        start_publishing.set()
        msg = await p.get_message(timeout=None)
        elapsed = asyncio.get_running_loop().time() - start
        assert msg is not None
        assert msg["type"] == "message"
        assert msg["data"] == b"delayed_message"
        # Should have waited at least 0.15 seconds
        assert elapsed >= 0.15
        await task
        await p.aclose()

    @pytest.mark.asyncio
    async def test_multiple_messages_with_timeout(self, r):
        """
        Test that timeout is properly handled when reading multiple messages.
        """
        p = r.pubsub()
        await p.subscribe("foo")
        # Read subscription message
        msg = await wait_for_message(p, timeout=1.0)
        assert msg is not None

        # Publish multiple messages
        await r.publish("foo", "msg1")
        await r.publish("foo", "msg2")
        await r.publish("foo", "msg3")

        # Read all messages with timeout
        messages = []
        for _ in range(3):
            msg = await wait_for_message(p, timeout=1.0)
            if msg:
                messages.append(msg)

        assert len(messages) == 3
        assert messages[0]["data"] == b"msg1"
        assert messages[1]["data"] == b"msg2"
        assert messages[2]["data"] == b"msg3"
        await p.aclose()

    @pytest.mark.asyncio
    async def test_timeout_with_pattern_subscribe(self, r):
        """
        Test that timeout works correctly with pattern subscriptions.
        """
        p = r.pubsub()
        # A prefix no other test publishes to: on a cluster, a PUBLISH to "foo"
        # from an earlier test can still be crossing the cluster bus.
        await p.psubscribe("timeout-pattern*")
        # Read subscription message
        msg = await wait_for_message(p, timeout=1.0)
        assert msg is not None
        assert msg["type"] == "psubscribe"

        # Publish a message matching the pattern
        await r.publish("timeout-pattern-bar", "hello")

        # get_message with timeout should return the message
        msg = await p.get_message(timeout=1.0)
        assert msg is not None
        assert msg["type"] == "pmessage"
        assert msg["data"] == b"hello"
        await p.aclose()

    @pytest.mark.asyncio
    async def test_timeout_with_no_subscription(self, r):
        """
        Test that get_message with timeout returns None when subscribed but no messages.
        """
        p = r.pubsub()
        await p.subscribe("foo")
        # Read subscription message
        msg = await wait_for_message(p, timeout=1.0)
        assert msg is not None

        # get_message with timeout should return None when no messages
        msg = await p.get_message(timeout=0.1)
        assert msg is None
        await p.aclose()


@pytest.mark.asyncio
@pytest.mark.onlynoncluster
class TestAsyncPubSubBlockingListen:
    """
    Tests that PubSub.listen() and parse_response(block=True) block
    indefinitely waiting for a message, ignoring the connection's
    configured socket_timeout. Regression tests for the issue where
    listen() would raise TimeoutError once socket_timeout elapsed.

    Retries are disabled on the subscriber so a TimeoutError from the
    blocking read surfaces immediately instead of being silently retried
    by the client's default retry policy (which would mask the bug).
    """

    SOCKET_TIMEOUT = 0.3
    PUBLISH_DELAY = SOCKET_TIMEOUT * 2

    async def _make_subscriber(self, create_redis):
        from redis.asyncio.retry import Retry
        from redis.backoff import NoBackoff

        return await create_redis(
            socket_timeout=self.SOCKET_TIMEOUT,
            retry=Retry(NoBackoff(), 0),
            retry_on_timeout=False,
        )

    async def test_listen_blocks_longer_than_socket_timeout(self, create_redis):
        """
        listen() must keep blocking past socket_timeout. Configure a very
        short socket_timeout, then publish after a delay that exceeds it,
        and assert listen() yields the message instead of raising
        TimeoutError.
        """
        r = await self._make_subscriber(create_redis)
        publisher = await create_redis()
        p = r.pubsub()
        await p.subscribe("foo")
        msg = await wait_for_message(p, timeout=1.0)
        assert msg is not None and msg["type"] == "subscribe"

        async def publish_after_delay():
            await asyncio.sleep(self.PUBLISH_DELAY)
            await publisher.publish("foo", "hello")

        task = asyncio.create_task(publish_after_delay())

        start = asyncio.get_running_loop().time()
        msg = await p.listen().__anext__()
        elapsed = asyncio.get_running_loop().time() - start
        assert msg["type"] == "message"
        assert msg["data"] == b"hello"
        # Should have blocked past socket_timeout to receive the message.
        assert elapsed > self.SOCKET_TIMEOUT
        await task
        await p.aclose()

    async def test_get_message_block_indefinitely_past_socket_timeout(
        self, create_redis
    ):
        """
        get_message(timeout=None) must block past socket_timeout.
        """
        r = await self._make_subscriber(create_redis)
        publisher = await create_redis()
        p = r.pubsub()
        await p.subscribe("foo")
        msg = await wait_for_message(p, timeout=1.0)
        assert msg is not None

        async def publish_after_delay():
            await asyncio.sleep(self.PUBLISH_DELAY)
            await publisher.publish("foo", "delayed")

        task = asyncio.create_task(publish_after_delay())

        start = asyncio.get_running_loop().time()
        msg = await p.get_message(timeout=None)
        elapsed = asyncio.get_running_loop().time() - start
        assert msg is not None
        assert msg["type"] == "message"
        assert msg["data"] == b"delayed"
        assert elapsed > self.SOCKET_TIMEOUT
        await task
        await p.aclose()

    async def test_parse_response_block_true_blocks_past_socket_timeout(
        self, create_redis
    ):
        """
        parse_response(block=True) must block past socket_timeout.
        """
        r = await self._make_subscriber(create_redis)
        publisher = await create_redis()
        p = r.pubsub(ignore_subscribe_messages=True)
        await p.subscribe("foo")
        # Drain the subscribe ack before calling parse_response directly.
        # The ack is ignored, so get_message returns None as soon as it is read.
        await p.get_message(timeout=1.0)

        async def publish_after_delay():
            await asyncio.sleep(self.PUBLISH_DELAY)
            await publisher.publish("foo", "raw")

        task = asyncio.create_task(publish_after_delay())

        start = asyncio.get_running_loop().time()
        response = await p.parse_response(block=True)
        elapsed = asyncio.get_running_loop().time() - start
        # Response is the raw pubsub frame [b"message", b"foo", b"raw"].
        assert response is not None
        assert response[0] == b"message"
        assert response[-1] == b"raw"
        assert elapsed > self.SOCKET_TIMEOUT
        await task
        await p.aclose()

    async def test_blocking_listen_does_not_mutate_socket_timeout(self, create_redis):
        """
        A blocking read must not mutate the connection's configured
        socket_timeout. Otherwise reconnect/AUTH/HELLO/resubscribe paths
        triggered by retries inside _execute would inherit no timeout and
        could hang indefinitely. The fix uses a per-read sentinel rather
        than clearing socket_timeout for the duration of the call.
        """
        r = await self._make_subscriber(create_redis)
        publisher = await create_redis()
        p = r.pubsub()
        await p.subscribe("foo")
        await wait_for_message(p, timeout=1.0)

        orig_timeout = p.connection.socket_timeout
        assert orig_timeout == self.SOCKET_TIMEOUT

        # Sample socket_timeout while the blocking read is in progress, to
        # catch any mutation that happens only for the duration of the call.
        async def sample_timeout_during_read():
            # Yield a few times so the get_message read is in flight.
            for _ in range(10):
                await asyncio.sleep(0)
            assert p.connection.socket_timeout == orig_timeout

        async def publish_after_delay():
            await asyncio.sleep(self.PUBLISH_DELAY)
            await publisher.publish("foo", "msg")

        sample_task = asyncio.create_task(sample_timeout_during_read())
        publish_task = asyncio.create_task(publish_after_delay())

        msg = await p.get_message(timeout=None)
        await sample_task
        assert msg is not None and msg["data"] == b"msg"
        assert p.connection.socket_timeout == orig_timeout
        await publish_task
        await p.aclose()

    async def test_block_indefinitely_does_not_alter_subsequent_reads(
        self, create_redis
    ):
        """
        After a blocking read returns, a follow-up non-blocking read must
        still honor the configured ``socket_timeout`` (i.e. the per-read
        ``block_indefinitely`` does not bleed into later operations and
        does not require mutating any connection-wide state).
        """
        r = await self._make_subscriber(create_redis)
        publisher = await create_redis()
        p = r.pubsub()
        await p.subscribe("foo")
        await wait_for_message(p, timeout=1.0)

        # First a blocking read.
        await publisher.publish("foo", "first")
        msg = await p.get_message(timeout=None)
        assert msg is not None and msg["data"] == b"first"

        # Then a non-blocking read with a short timeout and no message.
        # If block_indefinitely leaked, this would block past the timeout.
        start = asyncio.get_running_loop().time()
        msg = await p.get_message(timeout=0.1)
        elapsed = asyncio.get_running_loop().time() - start
        assert msg is None
        assert elapsed < 1.0  # generous: a stalled runner must not fail it
        await p.aclose()


@pytest.mark.fixed_client
@pytest.mark.asyncio
class TestPubSubHandleMessageMetrics:
    """Tests for handle_message recording pubsub metrics."""

    @pytest.fixture
    def mock_pubsub(self):
        """Create a mock PubSub instance for testing handle_message."""
        pubsub = MagicMock()
        pubsub.UNSUBSCRIBE_MESSAGE_TYPES = ("unsubscribe", "punsubscribe")
        pubsub.PUBLISH_MESSAGE_TYPES = ("message", "pmessage")
        pubsub.pending_unsubscribe_patterns = set()
        pubsub.pending_unsubscribe_channels = set()
        pubsub.patterns = {}
        pubsub.channels = {}
        pubsub.ignore_subscribe_messages = False
        return pubsub

    async def test_handle_message_records_metric_for_message_type(self, mock_pubsub):
        """Test that handle_message calls record_pubsub_message for 'message' type."""

        response = [b"message", b"test-channel", b"test-data"]

        with patch(
            "redis.asyncio.client.record_pubsub_message", new_callable=AsyncMock
        ) as mock_record:
            # Call the actual handle_message method
            await PubSub.handle_message(
                mock_pubsub, response, ignore_subscribe_messages=False
            )

            # Verify record_pubsub_message was called
            mock_record.assert_awaited_once()
            call_kwargs = mock_record.call_args[1]
            from redis.observability.attributes import PubSubDirection

            assert call_kwargs["direction"] == PubSubDirection.RECEIVE
            assert call_kwargs["channel"] == "test-channel"

    async def test_handle_message_records_metric_for_pmessage_type(self, mock_pubsub):
        """Test that handle_message calls record_pubsub_message for 'pmessage' type."""

        response = [b"pmessage", b"test-pattern*", b"test-channel", b"test-data"]

        with patch(
            "redis.asyncio.client.record_pubsub_message", new_callable=AsyncMock
        ) as mock_record:
            await PubSub.handle_message(
                mock_pubsub, response, ignore_subscribe_messages=False
            )

            mock_record.assert_awaited_once()
            call_kwargs = mock_record.call_args[1]
            from redis.observability.attributes import PubSubDirection

            assert call_kwargs["direction"] == PubSubDirection.RECEIVE
            assert call_kwargs["channel"] == "test-channel"

    async def test_handle_message_does_not_record_metric_for_subscribe_type(
        self, mock_pubsub
    ):
        """Test that handle_message does NOT call record_pubsub_message for 'subscribe' type."""

        response = [b"subscribe", b"test-channel", 1]

        with patch(
            "redis.asyncio.client.record_pubsub_message", new_callable=AsyncMock
        ) as mock_record:
            await PubSub.handle_message(
                mock_pubsub, response, ignore_subscribe_messages=False
            )

            mock_record.assert_not_called()

    async def test_handle_message_does_not_record_metric_for_pong_type(
        self, mock_pubsub
    ):
        """Test that handle_message does NOT call record_pubsub_message for 'pong' type."""

        response = b"PONG"

        with patch(
            "redis.asyncio.client.record_pubsub_message", new_callable=AsyncMock
        ) as mock_record:
            await PubSub.handle_message(
                mock_pubsub, response, ignore_subscribe_messages=False
            )

            mock_record.assert_not_called()


@pytest.mark.asyncio
@pytest.mark.fixed_client
class TestPubSubAcloseWithMocks:
    """
    Regression tests for https://github.com/redis/redis-py/issues/3941 —
    PubSub.aclose() must not hang inside StreamWriter.wait_closed() when a
    concurrent reader task still owns the pubsub connection's transport.
    """

    def _make_pubsub(self):
        pool = MagicMock()
        pool.get_encoder = MagicMock(
            return_value=MagicMock(
                decode_responses=False,
                encode=lambda v: v.encode() if isinstance(v, str) else v,
            )
        )
        pool.release = AsyncMock()
        pubsub = PubSub(connection_pool=pool)
        connection = MagicMock()
        connection.disconnect = AsyncMock()
        connection.deregister_connect_callback = MagicMock()
        pubsub.connection = connection
        return pubsub, pool, connection

    async def test_aclose_disconnects_with_nowait(self):
        """aclose() must call connection.disconnect(nowait=True) to avoid
        awaiting StreamWriter.wait_closed(), which can deadlock when another
        task is blocked in parse_response() on the same socket."""
        pubsub, pool, connection = self._make_pubsub()

        await pubsub.aclose()

        connection.disconnect.assert_awaited_once_with(nowait=True)
        connection.deregister_connect_callback.assert_called_once_with(
            pubsub.on_connect
        )
        pool.release.assert_awaited_once_with(connection)
        assert pubsub.connection is None

    async def test_aclose_does_not_hang_when_wait_closed_would_block(self):
        """
        End-to-end regression: even if the underlying StreamWriter.wait_closed()
        would hang forever (as happens when a concurrent reader still holds the
        transport), aclose() returns promptly because it passes nowait=True to
        disconnect().
        """
        pubsub, _pool, connection = self._make_pubsub()

        async def fake_disconnect(nowait: bool = False, **_):
            # Simulates AbstractConnection.disconnect(): if nowait=False we
            # would hang awaiting wait_closed(); with nowait=True we return
            # immediately.
            if not nowait:
                await asyncio.Event().wait()

        connection.disconnect = AsyncMock(side_effect=fake_disconnect)

        async with async_timeout(2):
            await pubsub.aclose()

        connection.disconnect.assert_awaited_once_with(nowait=True)


class TestClusterPubSubResubscribe:
    """
    Cluster-only tests verifying that sharded pubsub resubscription after a
    reconnect groups channels by slot and does not trigger CROSSSLOT errors
    on nodes that own multiple slots.
    """

    async def _read_ssubscribe_acks(self, pubsub, expected_channels, timeout=3.0):
        acks = {}
        loop = asyncio.get_event_loop()
        deadline = loop.time() + timeout
        while loop.time() < deadline and len(acks) < len(expected_channels):
            msg = await pubsub.get_sharded_message(timeout=0.5)
            if msg is None:
                continue
            if msg["type"] == "ssubscribe":
                acks[msg["channel"]] = msg
        return acks

    @pytest.mark.onlycluster
    @skip_if_server_version_lt("7.0.0")
    async def test_resubscribe_shard_channels_different_slots_same_node(self, r):
        """After a reconnect, shard channels on the same node but different
        slots must be resubscribed without raising CROSSSLOT.
        """
        # Hardcoded for the project's default 3-master test cluster; both
        # channels hash to slots owned by the first master.
        ch_a, ch_b = "resub-a-0", "resub-b-0"
        assert r.keyslot(ch_a) != r.keyslot(ch_b)
        assert r.get_node_from_key(ch_a).name == r.get_node_from_key(ch_b).name

        p = r.pubsub()
        try:
            await p.ssubscribe(ch_a)
            assert await self._read_ssubscribe_acks(p, [ch_a], timeout=2.0), (
                "missing initial ssubscribe ack for ch_a"
            )
            await p.ssubscribe(ch_b)
            assert await self._read_ssubscribe_acks(p, [ch_b], timeout=2.0), (
                "missing initial ssubscribe ack for ch_b"
            )

            # Force reconnect of the per-node subscriber connection.
            node = r.get_node_from_key(ch_a)
            per_node_pubsub = p.node_pubsub_mapping[node.name]
            assert per_node_pubsub.connection is not None
            await per_node_pubsub.connection.disconnect()

            # Trigger reconnect + resubscription. With batched resubscription
            # this would raise a CROSSSLOT ResponseError; with per-slot
            # grouping it must succeed and deliver fresh ssubscribe acks.
            acks = await self._read_ssubscribe_acks(p, [ch_a, ch_b], timeout=3.0)
            assert ch_a.encode() in acks
            assert ch_b.encode() in acks

            # Both channels must still be tracked in the aggregate state.
            assert ch_a.encode() in p.shard_channels
            assert ch_b.encode() in p.shard_channels
        finally:
            await p.aclose()


@pytest.mark.fixed_client
class TestClusterPubSubSlotMigration:
    """
    Deterministic unit tests for async ClusterPubSub shard-channel slot
    migration handling. These tests bypass all I/O by mocking the per-node
    PubSub instances and the cluster's node-resolution layer, verifying the
    reconciler logic and the reverse-index routing in isolation (no live
    cluster required).
    """

    def _make_cluster_pubsub(self):
        from redis._parsers.encoders import Encoder
        from redis.asyncio.cluster import ClusterPubSub

        pubsub = ClusterPubSub.__new__(ClusterPubSub)
        # Base PubSub state normally wired up by PubSub.__init__.
        pubsub.encoder = Encoder("utf-8", "strict", False)
        pubsub.connection = None
        pubsub._lock = asyncio.Lock()
        pubsub.health_check_response_counter = 0
        pubsub.channels = {}
        pubsub.shard_channels = {}
        pubsub.pending_unsubscribe_shard_channels = set()
        pubsub.patterns = {}
        pubsub.ignore_subscribe_messages = False
        # ClusterPubSub-specific state.
        pubsub.cluster = MagicMock()
        pubsub.cluster._initialize = False
        pubsub.node_pubsub_mapping = {}
        pubsub._shard_channel_to_node = {}
        pubsub._poll_cool_off = weakref.WeakKeyDictionary()
        pubsub._unreachable_nodes = set()
        pubsub._next_topology_repair = 0.0
        pubsub._shard_state_lock = asyncio.Lock()
        pubsub._reconcile_tasks = set()
        pubsub._topology_repair_tasks = set()
        pubsub.push_handler_func = None
        return pubsub

    def _make_node(self, name):
        node = MagicMock()
        node.name = name
        return node

    def _make_node_pubsub(self, shard_channels=None):
        p = MagicMock()
        p.shard_channels = dict(shard_channels or {})
        p.pending_unsubscribe_shard_channels = set()
        p.subscribed = True
        p.ssubscribe = AsyncMock()
        p.sunsubscribe = AsyncMock()
        p.get_message = AsyncMock()
        p.aclose = AsyncMock()
        # ClusterPubSub polls a per-node pubsub as parse_response (under the
        # per-node I/O lock) followed by handle_message (outside it, so a user
        # handler may re-subscribe). Tests keep configuring the single
        # ``get_message`` seam; these two forward to it - parse_response yields
        # the raw reply shape _is_publish_response inspects, handle_message
        # returns the already-parsed message the assertions compare against.
        p._last_message = None

        async def _parse_response(block=True, timeout=0.0):
            message = await p.get_message(
                ignore_subscribe_messages=False, timeout=timeout
            )
            p._last_message = message
            if message is None:
                return None
            return [message["type"], message.get("channel"), message.get("data")]

        async def _handle_message(response, ignore_subscribe_messages=False):
            return p._last_message

        p.parse_response = AsyncMock(side_effect=_parse_response)
        p.handle_message = AsyncMock(side_effect=_handle_message)
        return p

    async def test_reinitialize_moves_channel_to_new_owner(self):
        pubsub = self._make_cluster_pubsub()
        old_node = self._make_node("127.0.0.1:7000")
        new_node = self._make_node("127.0.0.1:7001")
        channel = b"foo"
        old_ps = self._make_node_pubsub({channel: None})
        new_ps = self._make_node_pubsub()
        pubsub.node_pubsub_mapping[old_node.name] = old_ps
        pubsub.node_pubsub_mapping[new_node.name] = new_ps
        pubsub.shard_channels = {channel: None}
        pubsub._shard_channel_to_node = {channel: old_node.name}
        pubsub.cluster.get_node_from_key.return_value = new_node

        await pubsub.reinitialize_shard_subscriptions()

        old_ps.sunsubscribe.assert_awaited_once_with(channel)
        new_ps.ssubscribe.assert_awaited_once_with(channel)
        assert pubsub._shard_channel_to_node[channel] == new_node.name

    async def test_reinitialize_noop_when_owner_unchanged(self):
        pubsub = self._make_cluster_pubsub()
        owner = self._make_node("127.0.0.1:7000")
        channel = b"foo"
        owner_ps = self._make_node_pubsub({channel: None})
        pubsub.node_pubsub_mapping[owner.name] = owner_ps
        pubsub.shard_channels = {channel: None}
        pubsub._shard_channel_to_node = {channel: owner.name}
        pubsub.cluster.get_node_from_key.return_value = owner

        await pubsub.reinitialize_shard_subscriptions()

        owner_ps.sunsubscribe.assert_not_awaited()
        owner_ps.ssubscribe.assert_not_awaited()
        assert pubsub._shard_channel_to_node[channel] == owner.name

    async def test_reinitialize_skips_pending_unsubscribe(self):
        """A topology refresh must not restore a channel being removed."""
        pubsub = self._make_cluster_pubsub()
        old_node = self._make_node("127.0.0.1:7000")
        new_node = self._make_node("127.0.0.1:7001")
        channel = b"foo"
        old_ps = self._make_node_pubsub({channel: None})
        new_ps = self._make_node_pubsub()
        pubsub.node_pubsub_mapping[old_node.name] = old_ps
        pubsub.node_pubsub_mapping[new_node.name] = new_ps
        old_ps.pending_unsubscribe_shard_channels = {channel}
        pubsub.shard_channels = {channel: None}
        pubsub.pending_unsubscribe_shard_channels = {channel}
        pubsub._shard_channel_to_node = {channel: old_node.name}
        pubsub.cluster.get_node_from_key.return_value = new_node

        await pubsub.reinitialize_shard_subscriptions()

        old_ps.sunsubscribe.assert_not_awaited()
        new_ps.ssubscribe.assert_not_awaited()
        assert pubsub._shard_channel_to_node[channel] == old_node.name

    async def test_initialize_refresh_does_not_restore_sunsubscribed_channel(self):
        pubsub = self._make_cluster_pubsub()
        old_node = self._make_node("127.0.0.1:7000")
        new_node = self._make_node("127.0.0.1:7001")
        channel = b"foo"
        old_ps = self._make_node_pubsub({channel: None})
        new_ps = self._make_node_pubsub()
        pubsub.node_pubsub_mapping = {old_node.name: old_ps, new_node.name: new_ps}
        pubsub.shard_channels = {channel: None}
        pubsub._shard_channel_to_node = {channel: old_node.name}
        pubsub.cluster._initialize = True
        pubsub.cluster.get_node_from_key.return_value = new_node
        old_ps.sunsubscribe.side_effect = lambda *_: (
            old_ps.pending_unsubscribe_shard_channels.add(channel)
        )
        reconcile_task = None

        async def initialize():
            nonlocal reconcile_task
            pubsub.cluster._initialize = False
            reconcile_task = asyncio.create_task(
                pubsub.reinitialize_shard_subscriptions()
            )

        pubsub.cluster.initialize = AsyncMock(side_effect=initialize)

        await pubsub.sunsubscribe(channel)
        await reconcile_task

        new_ps.ssubscribe.assert_not_awaited()
        assert channel in pubsub.pending_unsubscribe_shard_channels

    async def test_reinitialize_moves_channel_after_reconnect_drops_pending_unsubscribe(
        self,
    ):
        """
        A reconnect between ``sunsubscribe`` and its confirmation clears the
        per-node pending set and replays ``SSUBSCRIBE``, but the cluster-level
        set kept the channel. Reconciliation skipped it, so it stayed on the
        old owner after a slot migration.
        """
        pubsub = self._make_cluster_pubsub()
        old_node = self._make_node("127.0.0.1:7000")
        new_node = self._make_node("127.0.0.1:7001")
        channel = b"foo"
        # on_connect cleared the per-node pending set and re-subscribed.
        old_ps = self._make_node_pubsub({channel: None})
        new_ps = self._make_node_pubsub()
        pubsub.node_pubsub_mapping[old_node.name] = old_ps
        pubsub.node_pubsub_mapping[new_node.name] = new_ps
        pubsub.shard_channels = {channel: None}
        pubsub.pending_unsubscribe_shard_channels = {channel}
        pubsub._shard_channel_to_node = {channel: old_node.name}
        pubsub.cluster.get_node_from_key.return_value = new_node

        await pubsub.reinitialize_shard_subscriptions()

        old_ps.sunsubscribe.assert_awaited_once_with(channel)
        new_ps.ssubscribe.assert_awaited_once_with(channel)
        assert pubsub._shard_channel_to_node[channel] == new_node.name
        assert channel not in pubsub.pending_unsubscribe_shard_channels

    async def test_reinitialize_retries_revived_channel_after_failed_attach(self):
        """
        Moving a channel a reconnect subscribed again sends ``SUNSUBSCRIBE`` to
        the old node before it attaches to the new owner. If the attach fails,
        the stale cluster-level pending entry must not make the next pass skip
        the channel as a user unsubscribe.
        """
        pubsub = self._make_cluster_pubsub()
        old_node = self._make_node("127.0.0.1:7000")
        new_node = self._make_node("127.0.0.1:7001")
        channel = b"foo"
        old_ps = self._make_node_pubsub({channel: None})
        old_ps.sunsubscribe.side_effect = lambda *_: (
            old_ps.pending_unsubscribe_shard_channels.add(channel)
        )
        new_ps = self._make_node_pubsub()
        new_ps.ssubscribe.side_effect = [ConnectionError("new owner is down"), None]
        pubsub.node_pubsub_mapping[old_node.name] = old_ps
        pubsub.node_pubsub_mapping[new_node.name] = new_ps
        pubsub.shard_channels = {channel: None}
        pubsub.pending_unsubscribe_shard_channels = {channel}
        pubsub._shard_channel_to_node = {channel: old_node.name}
        pubsub.cluster.get_node_from_key.return_value = new_node

        with pytest.raises(ConnectionError):
            await pubsub.reinitialize_shard_subscriptions()
        await pubsub.reinitialize_shard_subscriptions()

        assert new_ps.ssubscribe.await_count == 2
        assert pubsub._shard_channel_to_node[channel] == new_node.name
        assert channel not in pubsub.pending_unsubscribe_shard_channels

    async def test_reinitialize_skips_channel_whose_unsubscribe_the_node_confirmed(
        self,
    ):
        """
        The per-node pubsub drops the channel when it reads the ``SUNSUBSCRIBE``
        confirmation, before ``get_sharded_message`` updates the cluster-level
        state. A pass running in between must not subscribe it again.
        """
        pubsub = self._make_cluster_pubsub()
        old_node = self._make_node("127.0.0.1:7000")
        new_node = self._make_node("127.0.0.1:7001")
        channel = b"foo"
        old_ps = self._make_node_pubsub()
        new_ps = self._make_node_pubsub()
        pubsub.node_pubsub_mapping[old_node.name] = old_ps
        pubsub.node_pubsub_mapping[new_node.name] = new_ps
        pubsub.shard_channels = {channel: None}
        pubsub.pending_unsubscribe_shard_channels = {channel}
        pubsub._shard_channel_to_node = {channel: old_node.name}
        pubsub.cluster.get_node_from_key.return_value = new_node

        await pubsub.reinitialize_shard_subscriptions()

        new_ps.ssubscribe.assert_not_awaited()
        assert channel in pubsub.pending_unsubscribe_shard_channels

    async def test_sunsubscribe_keeps_tracking_for_channel_resubscribed_on_reconnect(
        self,
    ):
        """
        A ``sunsubscribe`` for a channel the per-node pubsub still holds is not
        the confirmation of a user unsubscribe, even when a reconnect left the
        channel in the cluster-level pending set, so the channel stays tracked.
        """
        pubsub = self._make_cluster_pubsub()
        old_node = self._make_node("127.0.0.1:7000")
        channel = b"foo"
        old_ps = self._make_node_pubsub({channel: None})
        old_ps.get_message.return_value = {"type": "sunsubscribe", "channel": channel}
        pubsub.node_pubsub_mapping[old_node.name] = old_ps
        pubsub.shard_channels = {channel: None}
        pubsub.pending_unsubscribe_shard_channels = {channel}
        pubsub._shard_channel_to_node = {channel: old_node.name}

        message = await pubsub.get_sharded_message(target_node=old_node)

        assert message is not None
        assert channel in pubsub.shard_channels
        assert pubsub._shard_channel_to_node[channel] == old_node.name

    async def test_reinitialize_reattaches_when_owner_lost_the_subscription(self):
        """
        A pass that detached from the old owner and then failed to attach leaves
        the channel subscribed nowhere while the reverse index still names a
        node. If ownership then moves back to that node, the unchanged-owner
        short-circuit would skip the channel for the lifetime of the pubsub, so
        the missing subscription has to be re-attached instead.
        """
        pubsub = self._make_cluster_pubsub()
        owner = self._make_node("127.0.0.1:7000")
        channel = b"foo"
        # Index says owner holds the channel; its pubsub does not.
        owner_ps = self._make_node_pubsub()
        pubsub.node_pubsub_mapping[owner.name] = owner_ps
        pubsub.shard_channels = {channel: None}
        pubsub._shard_channel_to_node = {channel: owner.name}
        pubsub.cluster.get_node_from_key.return_value = owner

        with patch.object(pubsub, "_get_node_pubsub", return_value=owner_ps):
            await pubsub.reinitialize_shard_subscriptions()

        # Nothing to sunsubscribe from - the channel was already detached.
        owner_ps.sunsubscribe.assert_not_awaited()
        owner_ps.ssubscribe.assert_awaited_once_with(channel)
        assert pubsub._shard_channel_to_node[channel] == owner.name

    async def test_reinitialize_tolerates_old_node_disconnect(self):
        """
        When the old node is still part of the cluster topology but just
        transiently unreachable / slow, the failed sunsubscribe must not
        abort migration, and the old per-node pubsub must be left in
        place so ``PubSub._execute`` can auto-reconnect and
        ``on_connect`` can re-subscribe the remaining channels on the
        next read. The migrated channel is not one of them: it is
        detached locally so the replay cannot put it back on the node it
        was migrated away from.
        """
        pubsub = self._make_cluster_pubsub()
        old_node = self._make_node("127.0.0.1:7000")
        new_node = self._make_node("127.0.0.1:7001")
        channel = b"foo"
        old_ps = self._make_node_pubsub({channel: None})
        old_ps.sunsubscribe.side_effect = TimeoutError("old node is slow")
        new_ps = self._make_node_pubsub()
        pubsub.node_pubsub_mapping[old_node.name] = old_ps
        pubsub.node_pubsub_mapping[new_node.name] = new_ps
        pubsub.shard_channels = {channel: None}
        pubsub._shard_channel_to_node = {channel: old_node.name}
        pubsub.cluster.get_node_from_key.return_value = new_node
        # Old node is still known to the cluster (transient error).
        pubsub.cluster.get_node.return_value = old_node

        await pubsub.reinitialize_shard_subscriptions()

        old_ps.sunsubscribe.assert_awaited_once_with(channel)
        new_ps.ssubscribe.assert_awaited_once_with(channel)
        assert pubsub._shard_channel_to_node[channel] == new_node.name
        # Node still in topology → keep the pubsub so auto-reconnect can
        # recover it; the dedicated socket was already torn down by
        # Connection.disconnect() before the exception surfaced.
        old_ps.aclose.assert_not_awaited()
        assert old_node.name in pubsub.node_pubsub_mapping
        assert channel not in old_ps.shard_channels
        assert channel not in old_ps.pending_unsubscribe_shard_channels

    async def test_reinitialize_drops_old_pubsub_when_node_removed_from_topology(self):
        """
        When the failed sunsubscribe coincides with the old node being
        removed from the cluster topology (failover / topology refresh
        dropped it), the pubsub has nowhere to reconnect to and would
        stay in ``node_pubsub_mapping`` with ``subscribed=True`` forever
        (the ACK can never arrive), poisoning the round-robin generator.
        Drop it eagerly in that case.
        """
        pubsub = self._make_cluster_pubsub()
        old_node = self._make_node("127.0.0.1:7000")
        new_node = self._make_node("127.0.0.1:7001")
        channel = b"foo"
        old_ps = self._make_node_pubsub({channel: None})
        old_ps.sunsubscribe.side_effect = ConnectionError("old node is gone")
        new_ps = self._make_node_pubsub()
        pubsub.node_pubsub_mapping[old_node.name] = old_ps
        pubsub.node_pubsub_mapping[new_node.name] = new_ps
        pubsub.shard_channels = {channel: None}
        pubsub._shard_channel_to_node = {channel: old_node.name}
        pubsub.cluster.get_node_from_key.return_value = new_node
        # Old node was removed from topology.
        pubsub.cluster.get_node.return_value = None

        await pubsub.reinitialize_shard_subscriptions()

        assert pubsub._shard_channel_to_node[channel] == new_node.name
        old_ps.aclose.assert_awaited_once()
        assert old_node.name not in pubsub.node_pubsub_mapping

    async def test_sunsubscribe_routes_via_reverse_index_after_migration(self):
        """
        After a slot migration, sunsubscribe must hit the node that currently
        holds the subscription (tracked in ``_shard_channel_to_node``) rather
        than re-resolving via ``cluster.get_node_from_key``, which by then
        points to the new owner and would miss the actual subscription.
        """
        pubsub = self._make_cluster_pubsub()
        holding_node_name = "127.0.0.1:7000"
        new_owner = self._make_node("127.0.0.1:7001")
        channel = b"foo"
        holding_ps = self._make_node_pubsub({channel: None})
        new_ps = self._make_node_pubsub()
        pubsub.node_pubsub_mapping[holding_node_name] = holding_ps
        pubsub.node_pubsub_mapping[new_owner.name] = new_ps
        pubsub.shard_channels = {channel: None}
        pubsub._shard_channel_to_node = {channel: holding_node_name}
        pubsub.cluster.get_node_from_key.return_value = new_owner

        await pubsub.sunsubscribe(channel)

        holding_ps.sunsubscribe.assert_awaited_once_with(channel)
        new_ps.sunsubscribe.assert_not_awaited()

    async def test_ssubscribe_migration_applies_newly_supplied_handler(self):
        """
        Regression: the lazy-reroute branch in ssubscribe must honour the
        caller's newly supplied handler, matching PubSub.ssubscribe()'s
        dict.update() semantics. Previously it fell back to the stale handler
        tracked in ``shard_channels`` and silently dropped the new one.
        """
        pubsub = self._make_cluster_pubsub()
        old_node = self._make_node("127.0.0.1:7000")
        new_node = self._make_node("127.0.0.1:7001")
        channel = b"foo"
        old_ps = self._make_node_pubsub({channel: None})
        new_ps = self._make_node_pubsub()
        pubsub.node_pubsub_mapping[old_node.name] = old_ps
        pubsub.node_pubsub_mapping[new_node.name] = new_ps
        # Channel was previously subscribed with no handler.
        pubsub.shard_channels = {channel: None}
        pubsub._shard_channel_to_node = {channel: old_node.name}
        pubsub.cluster.get_node_from_key.return_value = new_node

        new_handler = MagicMock()
        await pubsub.ssubscribe(foo=new_handler)

        old_ps.sunsubscribe.assert_awaited_once_with(channel)
        new_ps.ssubscribe.assert_awaited_once_with(Subscription(channel, new_handler))

    async def test_ssubscribe_migration_accepts_binary_subscription_with_handler(self):
        pubsub = self._make_cluster_pubsub()
        old_node = self._make_node("127.0.0.1:7000")
        new_node = self._make_node("127.0.0.1:7001")
        channel = b"\x80\x81\x82"
        old_ps = self._make_node_pubsub({channel: None})
        new_ps = self._make_node_pubsub()
        pubsub.node_pubsub_mapping[old_node.name] = old_ps
        pubsub.node_pubsub_mapping[new_node.name] = new_ps
        pubsub.shard_channels = {channel: None}
        pubsub._shard_channel_to_node = {channel: old_node.name}
        pubsub.cluster.get_node_from_key.return_value = new_node

        new_handler = MagicMock()
        await pubsub.ssubscribe(Subscription(channel, new_handler))

        old_ps.sunsubscribe.assert_awaited_once_with(channel)
        new_ps.ssubscribe.assert_awaited_once_with(Subscription(channel, new_handler))

    async def test_ssubscribe_skips_channel_when_node_resolution_returns_none(self):
        """
        Regression: cluster.get_node_from_key may return None for channels
        whose slot is transiently uncovered. ssubscribe must skip such
        channels rather than dereference None (which would crash on
        ``node.name`` either in the reverse-index comparison or inside
        ``_get_node_pubsub``). Mirrors the sync counterpart's guard.
        """
        pubsub = self._make_cluster_pubsub()
        pubsub.cluster.get_node_from_key.return_value = None
        # Stale reverse-index entry also present to exercise both crash paths:
        # the old_name != node.name comparison and the downstream fallthrough.
        pubsub._shard_channel_to_node = {b"foo": "127.0.0.1:7000"}

        await pubsub.ssubscribe(b"foo")

        # No migration, no per-node pubsub creation, no mapping mutation.
        assert pubsub.node_pubsub_mapping == {}
        assert pubsub._shard_channel_to_node == {b"foo": "127.0.0.1:7000"}

    async def test_ssubscribe_initializes_cluster_before_resolving_slot(self):
        """
        Async RedisCluster lazily initializes on first awaited command.
        ClusterPubSub.ssubscribe must honor that contract before it resolves
        the shard channel slot through the cluster's slots cache.
        """
        pubsub = self._make_cluster_pubsub()
        node = self._make_node("127.0.0.1:7000")
        node_ps = self._make_node_pubsub()
        pubsub.node_pubsub_mapping[node.name] = node_ps
        pubsub.cluster._initialize = True
        pubsub.cluster.initialize = AsyncMock()
        pubsub.cluster.get_node_from_key.return_value = node

        await pubsub.ssubscribe(b"foo")

        pubsub.cluster.initialize.assert_awaited_once()
        pubsub.cluster.get_node_from_key.assert_called_once_with(b"foo")
        node_ps.ssubscribe.assert_awaited_once_with(b"foo")

    async def test_aclose_clears_state_and_cancels_reconcile_tasks(self):
        """
        Regression: aclose() must clear the ``_shard_channel_to_node`` reverse
        index so a reused instance does not route against stale mappings, and
        must cancel any in-flight reconciliation tasks so they do not race
        with the teardown of per-node pubsubs.
        """
        pubsub = self._make_cluster_pubsub()
        # Populate the reverse index and per-node pubsub mapping.
        node = self._make_node("127.0.0.1:7000")
        channel = b"foo"
        pubsub.shard_channels = {channel: None}
        pubsub._shard_channel_to_node = {channel: node.name}
        pubsub.node_pubsub_mapping[node.name] = self._make_node_pubsub({channel: None})

        # Schedule a reconcile task that never completes on its own so we can
        # assert it gets cancelled by aclose().
        async def _never():
            await asyncio.sleep(3600)

        reconcile_task = asyncio.create_task(_never())
        pubsub._reconcile_tasks.add(reconcile_task)

        await pubsub.aclose()

        assert pubsub._shard_channel_to_node == {}
        assert pubsub._reconcile_tasks == set()
        assert reconcile_task.cancelled()

    async def test_aclose_does_not_cancel_a_topology_refresh(self):
        """
        Regression: the topology repair used to be tracked in the same set
        aclose() cancels. ``NodesManager.initialize`` refreshes the slots cache
        of the whole cluster client, which every other command on it reads, so
        closing one pubsub used to abort a refresh in flight and leave that
        shared cache stale until some later MOVED or failed poll asked again.
        The sync counterpart has the same property: ``reset()`` retires the
        reconciliation executor with ``cancel_futures=True``, which drops queued
        work but lets a running ``initialize`` finish.
        """
        pubsub = self._make_cluster_pubsub()
        started = asyncio.Event()
        finished = asyncio.Event()

        async def _refresh():
            started.set()
            await asyncio.sleep(0)
            finished.set()

        pubsub.shard_channels = {b"foo": None}
        # The repair runs the refresh through the cluster client
        pubsub.cluster.initialize = _refresh
        pubsub._schedule_topology_repair()
        (task,) = list(pubsub._topology_repair_tasks)
        await started.wait()

        await pubsub.aclose()

        assert not task.cancelled()
        await task
        assert finished.is_set()

    async def test_aclose_clears_the_topology_repair_throttle(self):
        """
        A reused pubsub must not inherit the previous incarnation's throttle
        window: it would skip the first repair after resubscribing, delaying the
        move of its shard channels off a node that is already gone.
        """
        pubsub = self._make_cluster_pubsub()
        pubsub._next_topology_repair = time.monotonic() + 300

        await pubsub.aclose()

        assert pubsub._next_topology_repair == 0.0

    async def test_migration_driven_sunsubscribe_drops_empty_pubsub(self):
        """
        Regression: when a migration-driven sunsubscribe confirmation arrives
        (the channel is not in ``pending_unsubscribe_shard_channels`` because
        migration must not touch cluster-level tracking), the per-node pubsub
        must still be dropped from ``node_pubsub_mapping`` once it no longer
        holds any subscriptions. Otherwise long-running clients with slot
        churn accumulate dead per-node pubsubs and their connections.
        """
        pubsub = self._make_cluster_pubsub()
        old_node = self._make_node("127.0.0.1:7000")
        channel = b"foo"
        # Old per-node pubsub has already processed the sunsubscribe internally
        # (shard_channels empty, subscribed=False); the message is about to be
        # surfaced to the cluster-level pubsub.
        old_ps = self._make_node_pubsub()
        old_ps.subscribed = False
        old_ps.get_message.return_value = {"type": "sunsubscribe", "channel": channel}
        pubsub.node_pubsub_mapping[old_node.name] = old_ps
        # Migration-driven: channel is already tracked on the new owner, so
        # cluster-level state does not reference old_node and the channel is
        # NOT in pending_unsubscribe_shard_channels.
        pubsub.shard_channels = {}
        pubsub._shard_channel_to_node = {}
        pubsub.pending_unsubscribe_shard_channels = set()

        message = await pubsub.get_sharded_message(target_node=old_node)

        assert message is not None
        assert old_node.name not in pubsub.node_pubsub_mapping
        # The per-node pubsub's dedicated connection must be released back
        # to its pool before the mapping drop; otherwise the instance is
        # GC-eligible with no path that closes it (PubSub.__del__ does not
        # release connections), leaking a pool slot per migration.
        old_ps.aclose.assert_awaited_once()

    async def test_reinitialize_partial_progress_when_slot_uncovered(self):
        """
        A SlotNotCoveredError on one channel (slot transiently uncovered
        mid-migration) must not abort the reconcile pass for coverable
        siblings, but it MUST be surfaced at the end so the caller knows
        reconciliation was incomplete and a retry will happen on the next
        slots-cache change notification.
        """
        pubsub = self._make_cluster_pubsub()
        old_node = self._make_node("127.0.0.1:7000")
        new_node = self._make_node("127.0.0.1:7001")
        covered_channel = b"covered"
        uncovered_channel = b"uncovered"
        old_ps = self._make_node_pubsub(
            {covered_channel: None, uncovered_channel: None}
        )
        new_ps = self._make_node_pubsub()
        pubsub.node_pubsub_mapping[old_node.name] = old_ps
        pubsub.node_pubsub_mapping[new_node.name] = new_ps
        pubsub.shard_channels = {covered_channel: None, uncovered_channel: None}
        pubsub._shard_channel_to_node = {
            covered_channel: old_node.name,
            uncovered_channel: old_node.name,
        }

        def _resolve(channel, *args, **kwargs):
            if channel == uncovered_channel:
                raise SlotNotCoveredError('Slot "0" is not covered by the cluster.')
            return new_node

        pubsub.cluster.get_node_from_key.side_effect = _resolve

        with pytest.raises(SlotNotCoveredError):
            await pubsub.reinitialize_shard_subscriptions()

        # covered_channel was migrated before the error surfaced;
        # uncovered_channel is left in place for the next reconcile pass.
        new_ps.ssubscribe.assert_awaited_once_with(covered_channel)
        old_ps.sunsubscribe.assert_awaited_once_with(covered_channel)
        assert pubsub._shard_channel_to_node[covered_channel] == new_node.name
        assert pubsub._shard_channel_to_node[uncovered_channel] == old_node.name

    async def test_on_slots_changed_consumes_and_logs_task_exception(self, caplog):
        """
        Regression: on_slots_changed schedules reinitialize_shard_subscriptions
        as a fire-and-forget task. When that coroutine raises (e.g. the Option D
        SlotNotCoveredError signalling an incomplete reconcile pass), the
        exception must be consumed and logged so Python does not emit a noisy
        "Task exception was never retrieved" warning at GC time, and so the
        incomplete-reconcile signal flows through the library logger — matching
        the sync path, where ClusterPubSubSlotsCacheListener already logs via
        logger.exception.
        """
        pubsub = self._make_cluster_pubsub()
        # Has at least one shard subscription so on_slots_changed actually
        # schedules a task (the method short-circuits when shard_channels is
        # empty).
        pubsub.shard_channels = {b"foo": None}
        boom = SlotNotCoveredError("simulated transient uncovered slot")

        async def _raise():
            raise boom

        with mock.patch.object(
            pubsub, "reinitialize_shard_subscriptions", side_effect=_raise
        ):
            with caplog.at_level(logging.ERROR, logger="redis.asyncio.cluster"):
                await pubsub.on_slots_changed()
                # Await the scheduled task directly so both its done-callbacks
                # run (exception consumption + set discard). Suppress here so
                # the test itself does not re-raise what the callback already
                # logged.
                assert len(pubsub._reconcile_tasks) == 1
                (task,) = list(pubsub._reconcile_tasks)
                try:
                    await task
                except SlotNotCoveredError:
                    pass
                # Yield once more so the done-callbacks (scheduled via
                # call_soon) actually execute.
                await asyncio.sleep(0)

        # Task is gone from the tracking set (discard callback ran).
        assert pubsub._reconcile_tasks == set()
        # Exception was surfaced via the library logger.
        assert any(
            "shard subscription reconciliation failed" in rec.message
            and rec.levelno == logging.ERROR
            for rec in caplog.records
        )

    async def test_drop_node_pubsub_recreates_pubsubs_generator(self):
        """
        Regression: only ``aclose()`` rebound ``_pubsubs_generator``, but a
        single-node drop has the same exposure - the generator captures
        ``node_pubsub_mapping.values()`` into a local list inside ``yield
        from``, which ``_drop_node_pubsub``'s pop does not reach. So the round
        robin kept handing the retired per-node pubsub to the next poll.

        It cannot stall the reader here the way it can in the sync stack, where
        the prelude's subscription wait parks on it: ``_poll_node_pubsub``'s
        ``connection is None`` guard skips it. But the pass only runs
        ``range(len(node_pubsub_mapping))`` iterations, so spending one of them
        on a dead entry costs a healthy sibling its turn.
        """
        pubsub = self._make_cluster_pubsub()
        pubsub._pubsubs_generator = ClusterPubSub._pubsubs_generator(pubsub)
        ps_a = self._make_stateful_node_pubsub({b"foo": None})
        ps_b = self._make_stateful_node_pubsub({b"bar": None})
        pubsub.node_pubsub_mapping = {
            "127.0.0.1:7000": ps_a,
            "127.0.0.1:7001": ps_b,
        }
        # Advance into yield-from so the generator's frame holds a captured
        # list referencing both per-node pubsubs.
        assert next(pubsub._pubsubs_generator) is ps_a
        old_generator = pubsub._pubsubs_generator

        async with pubsub._shard_state_lock:
            await pubsub._drop_node_pubsub("127.0.0.1:7000", ps_a)

        assert pubsub._pubsubs_generator is not old_generator
        # The fresh generator must not drain the old captured list first.
        assert next(pubsub._pubsubs_generator) is ps_b
        assert next(pubsub._pubsubs_generator) is ps_b

    def _make_stateful_node_pubsub(self, shard_channels=None):
        """A per-node pubsub whose ``subscribed`` follows its channels.

        ``_make_node_pubsub`` pins ``subscribed`` to True, which is what the
        tests above need. The detach-driven garbage collection only becomes
        observable when the flag tracks ``shard_channels`` the way a real
        ``PubSub`` does, so those tests use this instead.
        """
        return _StatefulNodePubSub(shard_channels)

    async def test_reinitialize_detaches_migrated_channel_from_old_pubsub(self):
        """
        Regression: a ``SUNSUBSCRIBE`` that never reaches the server used to
        leave the channel in the old pubsub's ``shard_channels``.
        ``PubSub.on_connect`` clears ``pending_unsubscribe_shard_channels`` and
        replays ``SSUBSCRIBE`` from ``shard_channels``, so the channel was
        re-subscribed on the node it had just been migrated away from on every
        reconnect - and because the reverse index had already advanced,
        reconciliation never revisited it. Delivery on that channel was lost
        for the lifetime of the pubsub.
        """
        pubsub = self._make_cluster_pubsub()
        old_node = self._make_node("127.0.0.1:7000")
        new_node = self._make_node("127.0.0.1:7001")
        migrated = b"foo"
        staying = b"bar"
        old_ps = self._make_stateful_node_pubsub({migrated: None, staying: None})
        old_ps.sunsubscribe_error = ConnectionError("connection closed by server")
        new_ps = self._make_stateful_node_pubsub()
        pubsub.node_pubsub_mapping[old_node.name] = old_ps
        pubsub.node_pubsub_mapping[new_node.name] = new_ps
        pubsub.shard_channels = {migrated: None}
        pubsub._shard_channel_to_node = {migrated: old_node.name}
        pubsub.cluster.get_node_from_key.return_value = new_node
        pubsub.cluster.get_node.return_value = old_node

        await pubsub.reinitialize_shard_subscriptions()

        # The channel is gone from the old node's replay set, and present on
        # the new owner.
        assert migrated not in old_ps.shard_channels
        assert migrated not in old_ps.pending_unsubscribe_shard_channels
        assert migrated in new_ps.shard_channels
        assert pubsub._shard_channel_to_node[migrated] == new_node.name
        # A sibling subscription on the same node is untouched, so the pubsub
        # survives to recover it through on_connect.
        assert staying in old_ps.shard_channels
        assert old_node.name in pubsub.node_pubsub_mapping

    async def test_reinitialize_collects_old_pubsub_once_detach_empties_it(self):
        """
        The end-of-pass garbage collection drops per-node pubsubs that hold no
        subscription. Detaching the last channel of a node whose SUNSUBSCRIBE
        failed must therefore release its dedicated connection instead of
        leaving a pubsub the round-robin generator keeps polling for nothing.
        """
        pubsub = self._make_cluster_pubsub()
        old_node = self._make_node("127.0.0.1:7000")
        new_node = self._make_node("127.0.0.1:7001")
        channel = b"foo"
        old_ps = self._make_stateful_node_pubsub({channel: None})
        old_ps.sunsubscribe_error = TimeoutError("timeout reading from socket")
        new_ps = self._make_stateful_node_pubsub()
        pubsub.node_pubsub_mapping[old_node.name] = old_ps
        pubsub.node_pubsub_mapping[new_node.name] = new_ps
        pubsub.shard_channels = {channel: None}
        pubsub._shard_channel_to_node = {channel: old_node.name}
        pubsub.cluster.get_node_from_key.return_value = new_node
        pubsub.cluster.get_node.return_value = old_node

        await pubsub.reinitialize_shard_subscriptions()

        assert old_ps.aclose_calls == 1
        assert old_node.name not in pubsub.node_pubsub_mapping
        assert channel in new_ps.shard_channels

    async def test_ssubscribe_reroute_collects_old_pubsub_once_detach_empties_it(self):
        """
        Regression: the detach-driven collection cannot be left to the
        end-of-pass garbage collection, because ``ssubscribe``'s lazy re-route
        reaches ``_forget_shard_channel_on_old_node`` without ever running
        ``reinitialize_shard_subscriptions``. The other two collectors cannot
        reach it either: no ``SUNSUBSCRIBE`` confirmation can arrive for a
        channel forgotten locally, so ``get_sharded_message``'s unsubscribe
        branch never fires, and the old node is still in the topology, so the
        departed-node fast path does not apply.

        An empty pubsub left in ``node_pubsub_mapping`` keeps a live connection
        with nothing subscribed on it, so ``_poll_node_pubsub`` passes its
        ``connection is None`` guard and blocks reading a socket no message can
        arrive on - forever for a ``timeout=None`` caller, and for the whole
        timeout of every pass otherwise, before a single healthy node is read.
        """
        pubsub = self._make_cluster_pubsub()
        old_node = self._make_node("127.0.0.1:7000")
        new_node = self._make_node("127.0.0.1:7001")
        channel = b"foo"
        old_ps = self._make_stateful_node_pubsub({channel: None})
        new_ps = self._make_stateful_node_pubsub()
        pubsub.node_pubsub_mapping[old_node.name] = old_ps
        pubsub.node_pubsub_mapping[new_node.name] = new_ps
        pubsub.shard_channels = {channel: None}
        pubsub._shard_channel_to_node = {channel: old_node.name}
        # The reader has just failed to reach the old node, so the migration
        # forgets the channel locally rather than paying a reconnect for a
        # SUNSUBSCRIBE that cannot arrive.
        pubsub._unreachable_nodes = {old_node.name}
        pubsub.cluster.get_node_from_key.return_value = new_node
        # Still in the topology - the departed-node fast path must not be what
        # collects it, or this test would pass with the bug present.
        pubsub.cluster.get_node.return_value = old_node

        await pubsub.ssubscribe(channel)

        assert channel in new_ps.shard_channels
        assert pubsub._shard_channel_to_node[channel] == new_node.name
        assert not old_ps.subscribed
        assert old_ps.aclose_calls == 1
        assert old_node.name not in pubsub.node_pubsub_mapping

    async def test_ssubscribe_reroute_keeps_old_pubsub_with_sibling_channels(self):
        """
        The collection above is conditional on the detach having emptied the
        pubsub. A node that still holds a sibling subscription has to stay in
        ``node_pubsub_mapping``, so the reader keeps delivering that sibling.
        """
        pubsub = self._make_cluster_pubsub()
        old_node = self._make_node("127.0.0.1:7000")
        new_node = self._make_node("127.0.0.1:7001")
        migrated = b"foo"
        staying = b"bar"
        old_ps = self._make_stateful_node_pubsub({migrated: None, staying: None})
        new_ps = self._make_stateful_node_pubsub()
        pubsub.node_pubsub_mapping[old_node.name] = old_ps
        pubsub.node_pubsub_mapping[new_node.name] = new_ps
        pubsub.shard_channels = {migrated: None, staying: None}
        pubsub._shard_channel_to_node = {
            migrated: old_node.name,
            staying: old_node.name,
        }
        pubsub._unreachable_nodes = {old_node.name}
        pubsub.cluster.get_node_from_key.return_value = new_node
        pubsub.cluster.get_node.return_value = old_node

        await pubsub.ssubscribe(migrated)

        assert migrated not in old_ps.shard_channels
        assert staying in old_ps.shard_channels
        assert old_ps.aclose_calls == 0
        assert old_node.name in pubsub.node_pubsub_mapping

    def _prime_generator(self, pubsub):
        from redis.asyncio.cluster import ClusterPubSub

        pubsub._pubsubs_generator = ClusterPubSub._pubsubs_generator(pubsub)

    async def test_sharded_message_generator_skips_a_failing_pubsub(self):
        """
        Regression: a single reader serves every per-node pubsub, so letting an
        exception from one abort the pass stopped delivery cluster-wide while
        only one node was actually affected.
        """
        pubsub = self._make_cluster_pubsub()
        bad = self._make_node_pubsub({b"foo": None})
        bad.get_message.side_effect = ConnectionError("node is gone")
        good = self._make_node_pubsub({b"bar": None})
        message = {"type": "smessage", "channel": b"bar", "data": b"hi"}
        good.get_message.return_value = message
        pubsub.node_pubsub_mapping = {"127.0.0.1:7000": bad, "127.0.0.1:7001": good}
        self._prime_generator(pubsub)

        source, delivered = await pubsub._sharded_message_generator(timeout=0.01)

        assert source is good
        assert delivered is message

    async def test_sharded_message_generator_raises_when_every_pubsub_fails(self):
        """
        Isolation must not swallow a total outage: with nothing left to poll
        successfully, the first error is surfaced to the caller as before.
        """
        pubsub = self._make_cluster_pubsub()
        first = self._make_node_pubsub({b"foo": None})
        first.get_message.side_effect = ConnectionError("first is gone")
        second = self._make_node_pubsub({b"bar": None})
        second.get_message.side_effect = TimeoutError("second is slow")
        pubsub.node_pubsub_mapping = {
            "127.0.0.1:7000": first,
            "127.0.0.1:7001": second,
        }
        self._prime_generator(pubsub)

        with pytest.raises((ConnectionError, TimeoutError)):
            await pubsub._sharded_message_generator(timeout=0.01)

    async def test_sharded_message_generator_repairs_moved(self):
        """
        ``on_connect`` replays ``SSUBSCRIBE`` to the node its connection is
        bound to, so a channel left on a former owner is answered with
        ``MOVED``. ``MovedError`` is not retried by ``Connection.retry`` and
        nothing else on the read path refreshes the slots cache, so without
        this repair the subscription could never recover.
        """
        from redis.crc import key_slot
        from redis.exceptions import MovedError

        pubsub = self._make_cluster_pubsub()
        channel = b"foo"
        slot = key_slot(channel)
        stale = self._make_stateful_node_pubsub({channel: None})
        pubsub.node_pubsub_mapping = {"127.0.0.1:7000": stale}
        pubsub.shard_channels = {channel: None}
        pubsub._shard_channel_to_node = {channel: "127.0.0.1:7000"}
        self._prime_generator(pubsub)
        stale.get_message_error = MovedError(f"{slot} 127.0.0.1:7001")
        pubsub.cluster.nodes_manager.move_slot = AsyncMock()
        pubsub.on_slots_changed = AsyncMock()

        source, delivered = await pubsub._sharded_message_generator(timeout=0.01)

        assert (source, delivered) == (None, None)
        # The stale subscription is dropped so on_connect stops replaying it,
        # and its recorded owner is forgotten so reconciliation does not
        # short-circuit on an already-advanced reverse index.
        assert channel not in stale.shard_channels
        assert channel not in pubsub._shard_channel_to_node
        # The channel stays in the cluster-level intent, which is what
        # reconciliation walks.
        assert channel in pubsub.shard_channels
        pubsub.cluster.nodes_manager.move_slot.assert_awaited_once()
        pubsub.on_slots_changed.assert_awaited_once()
        # Cooled off, so an uncorrectable slots cache cannot make the repair
        # re-run on every poll.
        assert stale in pubsub._poll_cool_off

    async def test_moved_repair_collects_the_pubsub_it_empties(self):
        """
        A node that lost its only slot answers ``MOVED`` for every channel it
        held, so the repair's detach can leave its pubsub with nothing
        subscribed - and an empty one left in ``node_pubsub_mapping`` keeps a
        live connection with nothing subscribed on it, which
        ``_poll_node_pubsub`` blocks on reading. The other collectors cannot
        reach it: no ``SUNSUBSCRIBE`` confirmation arrives for a channel
        forgotten locally, and the reconciliation pass scheduled by the repair
        only garbage-collects it once its task runs - a whole poll cool-off
        later, at best.
        """
        from redis.crc import key_slot
        from redis.exceptions import MovedError

        pubsub = self._make_cluster_pubsub()
        channel = b"foo"
        stale = self._make_stateful_node_pubsub({channel: None})
        pubsub.node_pubsub_mapping = {"127.0.0.1:7000": stale}
        pubsub.shard_channels = {channel: None}
        pubsub._shard_channel_to_node = {channel: "127.0.0.1:7000"}
        self._prime_generator(pubsub)
        stale.get_message_error = MovedError(f"{key_slot(channel)} 127.0.0.1:7001")
        pubsub.cluster.nodes_manager.move_slot = AsyncMock()
        pubsub.on_slots_changed = AsyncMock()

        assert await pubsub._sharded_message_generator(timeout=0.01) == (None, None)

        assert stale.aclose_calls == 1
        assert "127.0.0.1:7000" not in pubsub.node_pubsub_mapping
        # The cluster-level intent survives, so the reconciliation the repair
        # scheduled still re-attaches the channel on its new owner.
        assert channel in pubsub.shard_channels

    async def test_moved_repair_keeps_a_pubsub_with_other_slots(self):
        """
        Only the channels of the moved slot are re-routed, so a pubsub that
        still holds a subscription on another slot must stay in the mapping and
        keep serving it.
        """
        from redis.crc import key_slot
        from redis.exceptions import MovedError

        pubsub = self._make_cluster_pubsub()
        moved = b"foo"
        staying = b"bar"
        stale = self._make_stateful_node_pubsub({moved: None, staying: None})
        pubsub.node_pubsub_mapping = {"127.0.0.1:7000": stale}
        pubsub.shard_channels = {moved: None, staying: None}
        pubsub._shard_channel_to_node = {
            moved: "127.0.0.1:7000",
            staying: "127.0.0.1:7000",
        }
        self._prime_generator(pubsub)
        stale.get_message_error = MovedError(f"{key_slot(moved)} 127.0.0.1:7001")
        pubsub.cluster.nodes_manager.move_slot = AsyncMock()
        pubsub.on_slots_changed = AsyncMock()

        assert await pubsub._sharded_message_generator(timeout=0.01) == (None, None)

        assert stale.aclose_calls == 0
        assert pubsub.node_pubsub_mapping == {"127.0.0.1:7000": stale}
        assert moved not in stale.shard_channels
        assert staying in stale.shard_channels
        assert pubsub._shard_channel_to_node == {staying: "127.0.0.1:7000"}

    async def test_sunsubscribe_bookkeeping_survives_a_racing_detach(self):
        """
        Mirrors the sync regression: ``handle_message`` did its unsubscribe
        bookkeeping as a check-then-``remove`` on
        ``pending_unsubscribe_shard_channels``, which a concurrent
        ``_detach_shard_channel`` of the same channel could turn into a
        ``KeyError`` raised out of a pubsub read that no caller catches. The
        async detach cannot actually interleave (it runs to completion without
        an ``await``), but the bookkeeping is a mirror and must not drift.
        """
        # A real PubSub, not a mock: handle_message's bookkeeping is the code
        # under test here.
        node_pubsub = PubSub.__new__(PubSub)
        # __del__ reads it, so a bare __new__ instance would raise on collection.
        node_pubsub.connection = None
        node_pubsub.shard_channels = {b"foo": None}
        node_pubsub.channels = {}
        node_pubsub.patterns = {}
        node_pubsub.ignore_subscribe_messages = False

        class _RacingSet(set):
            """Detaches the channel exactly in the check-to-removal window."""

            def __contains__(self, item):
                present = super().__contains__(item)
                if present:
                    ClusterPubSub._detach_shard_channel(node_pubsub, item)
                return present

        node_pubsub.pending_unsubscribe_shard_channels = _RacingSet({b"foo"})

        await node_pubsub.handle_message(
            ["sunsubscribe", b"foo", 0], ignore_subscribe_messages=False
        )

        assert b"foo" not in node_pubsub.shard_channels
        assert b"foo" not in node_pubsub.pending_unsubscribe_shard_channels

    async def test_target_node_poll_repairs_moved(self):
        """
        Regression: a poll that names one node used to hand the caller a raw
        ``MovedError`` - an error it cannot act on - and left the channel pinned
        to the former owner, while the round-robin path re-routed it. The two
        poll paths must repair the same way.
        """
        from redis.crc import key_slot
        from redis.exceptions import MovedError

        pubsub = self._make_cluster_pubsub()
        channel = b"foo"
        node = self._make_node("127.0.0.1:7000")
        stale = self._make_stateful_node_pubsub({channel: None})
        pubsub.node_pubsub_mapping = {node.name: stale}
        pubsub.shard_channels = {channel: None}
        pubsub._shard_channel_to_node = {channel: node.name}
        stale.get_message_error = MovedError(f"{key_slot(channel)} 127.0.0.1:7001")
        pubsub.cluster.nodes_manager.move_slot = AsyncMock()
        pubsub.on_slots_changed = AsyncMock()

        message = await pubsub.get_sharded_message(timeout=0.01, target_node=node)

        assert message is None

        # Same repair as the round-robin path: the stale subscription is dropped
        # so on_connect stops replaying it, its recorded owner is forgotten, the
        # cluster-level intent is kept for reconciliation to walk, and the node
        # is cooled off so an uncorrectable slots cache cannot re-run the repair
        # on every poll.
        assert channel not in stale.shard_channels
        assert channel not in pubsub._shard_channel_to_node
        assert channel in pubsub.shard_channels
        pubsub.on_slots_changed.assert_awaited_once()
        assert stale in pubsub._poll_cool_off

    async def test_target_node_poll_still_raises_connection_errors(self):
        """
        Isolation from a sick node exists to protect its healthy siblings, and a
        caller that named a single node has no sibling to protect - so a
        connectivity failure stays visible there.
        """
        pubsub = self._make_cluster_pubsub()
        node = self._make_node("127.0.0.1:7000")
        bad = self._make_stateful_node_pubsub({b"foo": None})
        bad.get_message_error = ConnectionError("node is gone")
        pubsub.node_pubsub_mapping = {node.name: bad}

        with pytest.raises(ConnectionError):
            await pubsub.get_sharded_message(timeout=0.01, target_node=node)

    async def test_sharded_message_generator_survives_a_failing_redirect(self):
        """
        ``move_slot`` indexes ``slots_cache`` by the redirected slot, so an
        as-yet-uncovered slot raises. That must not turn a pubsub read into an
        exception: reconciliation is scheduled either way.
        """
        from redis.crc import key_slot
        from redis.exceptions import MovedError

        pubsub = self._make_cluster_pubsub()
        channel = b"foo"
        stale = self._make_stateful_node_pubsub({channel: None})
        pubsub.node_pubsub_mapping = {"127.0.0.1:7000": stale}
        pubsub.shard_channels = {channel: None}
        pubsub._shard_channel_to_node = {channel: "127.0.0.1:7000"}
        self._prime_generator(pubsub)
        stale.get_message_error = MovedError(f"{key_slot(channel)} 127.0.0.1:7001")
        pubsub.cluster.nodes_manager.move_slot = AsyncMock(
            side_effect=KeyError(key_slot(channel))
        )
        pubsub.on_slots_changed = AsyncMock()

        assert await pubsub._sharded_message_generator(timeout=0.01) == (None, None)

        pubsub.on_slots_changed.assert_awaited_once()

    async def test_pubsubs_generator_survives_a_momentarily_empty_mapping(self):
        """
        Regression: the generator used to ``return`` on an empty
        ``node_pubsub_mapping``, which exhausts it for good - and only
        ``reset`` recreates it. Reconciliation drops a per-node pubsub before
        creating its replacement, so one such window silently ended sharded
        delivery for the rest of the pubsub's life.
        """
        pubsub = self._make_cluster_pubsub()
        self._prime_generator(pubsub)

        assert next(pubsub._pubsubs_generator) is None

        ps = self._make_node_pubsub({b"foo": None})
        pubsub.node_pubsub_mapping["127.0.0.1:7000"] = ps

        assert next(pubsub._pubsubs_generator) is ps

    async def test_sharded_message_generator_cools_off_a_failing_pubsub(self):
        """
        ``PubSub._execute`` reconnects and then retries through the
        connection's own ``Retry``, so one "bounded" poll on an unreachable node
        can cost its whole retry budget instead of the timeout the caller asked
        for. Going straight back to that node on the next pass is what turns one
        sick node into a cluster-wide delivery stall, so it is skipped for a
        cool-off window while its healthy siblings keep being polled.
        """
        from redis.asyncio.cluster import SHARD_POLL_COOL_OFF_SECONDS

        pubsub = self._make_cluster_pubsub()
        bad = self._make_node_pubsub({b"foo": None})
        bad.get_message.side_effect = ConnectionError("node is gone")
        good = self._make_node_pubsub({b"bar": None})
        good.get_message.return_value = None
        pubsub.node_pubsub_mapping = {"127.0.0.1:7000": bad, "127.0.0.1:7001": good}
        self._prime_generator(pubsub)

        assert await pubsub._sharded_message_generator(timeout=0.01) == (None, None)
        assert bad.get_message.await_count == 1
        polls_of_good = good.get_message.await_count

        # Second pass: the bad one is skipped, the good one is polled again.
        assert await pubsub._sharded_message_generator(timeout=0.01) == (None, None)
        assert bad.get_message.await_count == 1
        assert good.get_message.await_count > polls_of_good

        # Once the window expires it is tried again.
        pubsub._poll_cool_off[bad] = time.monotonic() - SHARD_POLL_COOL_OFF_SECONDS
        assert await pubsub._sharded_message_generator(timeout=0.01) == (None, None)
        assert bad.get_message.await_count == 2

    async def test_sharded_message_generator_blocks_when_all_nodes_cool_off(self):
        """
        A pass in which every node is skipped for cool-off performs no wire
        read, so returning straight away ignores the timeout the caller asked
        to block for - and a reader loop on ``get_sharded_message`` polls right
        back, spinning until the cool-off expires instead of blocking.
        """
        pubsub = self._make_cluster_pubsub()
        bad = self._make_node_pubsub({b"foo": None})
        bad.get_message.side_effect = ConnectionError("node is gone")
        pubsub.node_pubsub_mapping = {"127.0.0.1:7000": bad}
        self._prime_generator(pubsub)
        pubsub._poll_cool_off[bad] = time.monotonic() + 30

        start = time.monotonic()
        assert await pubsub._sharded_message_generator(timeout=0.05) == (None, None)
        elapsed = time.monotonic() - start

        assert bad.get_message.await_count == 0
        assert elapsed >= 0.05 - EVENT_LOOP_TIMER_SLACK

    async def test_sharded_message_generator_cool_off_wait_ends_with_the_window(self):
        """The wait is bounded by the cool-off, not by a long caller timeout."""
        pubsub = self._make_cluster_pubsub()
        bad = self._make_node_pubsub({b"foo": None})
        bad.get_message.side_effect = ConnectionError("node is gone")
        pubsub.node_pubsub_mapping = {"127.0.0.1:7000": bad}
        self._prime_generator(pubsub)

        # Anchor the deadline to ``start``: _wait_out_cool_off sleeps up to the
        # absolute deadline, so a deadline taken before ``start`` shortens the
        # measured wait by the setup's own cost and makes the lower bound below
        # unreachable.
        start = time.monotonic()
        # A stall longer than the window between here and the generator's
        # deadline check lets it poll the failing node; keep the window wide.
        pubsub._poll_cool_off[bad] = start + 0.5
        assert await pubsub._sharded_message_generator(timeout=30) == (None, None)
        elapsed = time.monotonic() - start

        assert 0.5 - EVENT_LOOP_TIMER_SLACK <= elapsed < 5

    async def test_sharded_message_generator_cool_off_does_not_block_zero_timeout(
        self,
    ):
        """A non-blocking poll must stay non-blocking while nodes cool off."""
        pubsub = self._make_cluster_pubsub()
        bad = self._make_node_pubsub({b"foo": None})
        bad.get_message.side_effect = ConnectionError("node is gone")
        pubsub.node_pubsub_mapping = {"127.0.0.1:7000": bad}
        self._prime_generator(pubsub)
        pubsub._poll_cool_off[bad] = time.monotonic() + 30

        start = time.monotonic()
        assert await pubsub._sharded_message_generator(timeout=0.0) == (None, None)

        assert time.monotonic() - start < 1

    async def test_sharded_message_generator_clears_cool_off_on_success(self):
        """A pubsub that polls cleanly must not stay in cool-off."""
        pubsub = self._make_cluster_pubsub()
        ps = self._make_node_pubsub({b"foo": None})
        ps.get_message.return_value = None
        pubsub.node_pubsub_mapping = {"127.0.0.1:7000": ps}
        self._prime_generator(pubsub)
        pubsub._poll_cool_off[ps] = time.monotonic() - 1

        await pubsub._sharded_message_generator(timeout=0.01)

        assert ps not in pubsub._poll_cool_off

    async def test_failed_poll_schedules_one_throttled_topology_refresh(self):
        """
        A node that has left the deployment answers ECONNREFUSED, never MOVED, so
        the failed poll is the only signal that its shard channels need a new
        owner. Reconciliation is otherwise purely event-driven, so the read path
        has to ask for a slots-cache refresh - throttled, because a node that is
        down fails every poll.
        """
        from redis.asyncio.cluster import SHARD_TOPOLOGY_REPAIR_INTERVAL_SECONDS

        pubsub = self._make_cluster_pubsub()
        pubsub.shard_channels = {b"foo": None}
        bad = self._make_node_pubsub({b"foo": None})
        bad.get_message.side_effect = ConnectionError("node is gone")
        pubsub.node_pubsub_mapping = {"127.0.0.1:7000": bad}
        self._prime_generator(pubsub)
        # The repair runs the refresh through the cluster client
        pubsub.cluster.initialize = AsyncMock()

        with pytest.raises(ConnectionError):
            await pubsub._sharded_message_generator(timeout=0.01)
        await asyncio.gather(*pubsub._topology_repair_tasks, return_exceptions=True)
        assert pubsub.cluster.initialize.await_count == 1
        # Asked for on the client, so a failed refresh stays asked for
        pubsub.cluster._request_refresh.assert_called_once_with()
        assert pubsub._unreachable_nodes == {"127.0.0.1:7000"}

        # Second failure inside the throttle window must not refresh again.
        pubsub._poll_cool_off.pop(bad, None)
        with pytest.raises(ConnectionError):
            await pubsub._sharded_message_generator(timeout=0.01)
        await asyncio.gather(*pubsub._topology_repair_tasks, return_exceptions=True)
        assert pubsub.cluster.initialize.await_count == 1

        # Once the window expires the next failure asks again.
        pubsub._next_topology_repair = (
            time.monotonic() - SHARD_TOPOLOGY_REPAIR_INTERVAL_SECONDS
        )
        pubsub._poll_cool_off.pop(bad, None)
        with pytest.raises(ConnectionError):
            await pubsub._sharded_message_generator(timeout=0.01)
        await asyncio.gather(*pubsub._topology_repair_tasks, return_exceptions=True)
        assert pubsub.cluster.initialize.await_count == 2

    async def test_failed_poll_does_not_refresh_without_shard_channels(self):
        """Nothing to reconcile means nothing to refresh the topology for."""
        pubsub = self._make_cluster_pubsub()
        bad = self._make_node_pubsub()
        bad.get_message.side_effect = ConnectionError("node is gone")
        pubsub.node_pubsub_mapping = {"127.0.0.1:7000": bad}
        self._prime_generator(pubsub)
        pubsub.cluster.initialize = AsyncMock()

        with pytest.raises(ConnectionError):
            await pubsub._sharded_message_generator(timeout=0.01)

        pubsub.cluster.initialize.assert_not_awaited()
        assert pubsub.cluster._initialize is False

    async def test_successful_poll_clears_the_unreachable_mark(self):
        """A node that answers again must not keep the migration fast path."""
        pubsub = self._make_cluster_pubsub()
        ps = self._make_node_pubsub({b"foo": None})
        ps.get_message.return_value = None
        pubsub.node_pubsub_mapping = {"127.0.0.1:7000": ps}
        self._prime_generator(pubsub)
        pubsub._unreachable_nodes = {"127.0.0.1:7000"}

        await pubsub._sharded_message_generator(timeout=0.01)

        assert pubsub._unreachable_nodes == set()

    async def test_migration_skips_sunsubscribe_on_an_unreachable_old_node(self):
        """
        A SUNSUBSCRIBE to a node the reader just failed to reach cannot arrive,
        and attempting it costs a full reconnect plus the client's retry budget -
        queued behind the reader on the same per-node io lock, while this pass
        holds ``_shard_state_lock``. Detach locally instead and re-SSUBSCRIBE on
        the new owner straight away.
        """
        pubsub = self._make_cluster_pubsub()
        old_pubsub = self._make_node_pubsub({b"foo": None})
        new_pubsub = self._make_node_pubsub()
        new_pubsub.shard_channels = {b"foo": None}
        new_node = self._make_node("127.0.0.1:7001")
        pubsub.node_pubsub_mapping = {
            "127.0.0.1:7000": old_pubsub,
            "127.0.0.1:7001": new_pubsub,
        }
        pubsub.shard_channels = {b"foo": None}
        pubsub._shard_channel_to_node = {b"foo": "127.0.0.1:7000"}
        pubsub._unreachable_nodes = {"127.0.0.1:7000"}
        pubsub.cluster.get_node.return_value = self._make_node("127.0.0.1:7000")

        with patch.object(
            pubsub, "_get_node_pubsub", return_value=new_pubsub
        ) as get_node_pubsub:
            await pubsub._migrate_shard_channel(
                b"foo", None, "127.0.0.1:7000", new_node
            )

        old_pubsub.sunsubscribe.assert_not_awaited()
        assert b"foo" not in old_pubsub.shard_channels
        get_node_pubsub.assert_called_once_with(new_node)
        new_pubsub.ssubscribe.assert_awaited_once_with(b"foo")
        assert pubsub._shard_channel_to_node[b"foo"] == "127.0.0.1:7001"

    async def test_migration_drops_an_unreachable_node_gone_from_the_topology(self):
        """
        A departed node has no reconnect target, so its per-node pubsub is
        dropped rather than left for the round robin to keep yielding - and the
        unreachable mark goes with it.
        """
        pubsub = self._make_cluster_pubsub()
        old_pubsub = self._make_node_pubsub({b"foo": None})
        new_pubsub = self._make_node_pubsub()
        new_node = self._make_node("127.0.0.1:7001")
        pubsub.node_pubsub_mapping = {"127.0.0.1:7000": old_pubsub}
        pubsub.shard_channels = {b"foo": None}
        pubsub._shard_channel_to_node = {b"foo": "127.0.0.1:7000"}
        pubsub._unreachable_nodes = {"127.0.0.1:7000"}
        pubsub.cluster.get_node.return_value = None

        with patch.object(pubsub, "_get_node_pubsub", return_value=new_pubsub):
            await pubsub._migrate_shard_channel(
                b"foo", None, "127.0.0.1:7000", new_node
            )

        old_pubsub.sunsubscribe.assert_not_awaited()
        old_pubsub.aclose.assert_awaited_once()
        assert "127.0.0.1:7000" not in pubsub.node_pubsub_mapping
        assert pubsub._unreachable_nodes == set()

    # --- per-node I/O lock scope ------------------------------------------

    def _make_real_node_pubsub(self, raw_response, handler=None, channel=b"foo"):
        """A real per-node ``PubSub`` with only the wire read stubbed.

        ``handle_message`` stays real so the assertions exercise the actual
        dispatch, and a real ``_shard_io_lock`` gets attached by
        ``_pubsub_io_lock`` - a ``MagicMock`` stand-in would hand back a mock
        that supports ``__aenter__``, making every lock-scope assertion
        vacuously true.
        """
        p = PubSub(MagicMock(), encoder=Encoder("utf-8", "strict", False))
        p.shard_channels = {channel: handler}
        # A subscribed per-node pubsub is connected; the poll path skips one
        # whose connection is gone. Only the wire read is stubbed, so this is
        # just the non-None marker that state needs.
        p.connection = MagicMock()
        p.parse_response = AsyncMock(return_value=raw_response)
        return p

    @pytest.mark.parametrize("via_target_node", [True, False])
    async def test_handler_runs_without_the_node_io_lock(self, via_target_node):
        """
        The per-node I/O lock must not be held while handle_message awaits a
        user handler. A handler may await ssubscribe / sunsubscribe on this
        ClusterPubSub, which takes the same I/O lock, and asyncio.Lock is not
        reentrant - so the reader task used to hang permanently and silently.
        """
        pubsub = self._make_cluster_pubsub()
        node = self._make_node("127.0.0.1:7000")
        observed = []

        async def handler(message):
            observed.append(io_lock.locked())

        node_ps = self._make_real_node_pubsub([b"smessage", b"foo", b"hi"], handler)
        io_lock = ClusterPubSub._pubsub_io_lock(node_ps)
        pubsub.node_pubsub_mapping[node.name] = node_ps
        pubsub._pubsubs_generator = ClusterPubSub._pubsubs_generator(pubsub)

        if via_target_node:
            message = await pubsub.get_sharded_message(timeout=0.01, target_node=node)
        else:
            message = await pubsub.get_sharded_message(timeout=0.01)

        assert observed == [False]
        # A dispatched handler consumes the message.
        assert message is None

    @pytest.mark.parametrize(
        "raw_response,expect_lock_held",
        [
            ([b"ssubscribe", b"foo", 1], True),
            ([b"sunsubscribe", b"foo", 0], True),
            ([b"smessage", b"foo", b"hi"], False),
        ],
    )
    async def test_bookkeeping_stays_under_the_io_lock(
        self, raw_response, expect_lock_held
    ):
        """
        Only handler dispatch leaves the I/O lock. handle_message's
        subscription bookkeeping mutates the same shard_channels /
        pending_unsubscribe_shard_channels that ssubscribe / sunsubscribe
        mutate under this lock, so it must stay inside it.
        """
        pubsub = self._make_cluster_pubsub()
        node = self._make_node("127.0.0.1:7000")

        async def handler(message):
            return None

        node_ps = self._make_real_node_pubsub(raw_response, handler)
        io_lock = ClusterPubSub._pubsub_io_lock(node_ps)
        pubsub.node_pubsub_mapping[node.name] = node_ps

        observed = []
        real_handle_message = node_ps.handle_message

        async def _spy(response, ignore_subscribe_messages=False):
            observed.append(io_lock.locked())
            return await real_handle_message(response, ignore_subscribe_messages)

        node_ps.handle_message = _spy

        await pubsub.get_sharded_message(timeout=0.01, target_node=node)

        assert observed == [expect_lock_held]

    async def test_handler_may_resubscribe_from_inside_the_handler(self):
        """
        Regression: asyncio.Lock is not reentrant, so a handler awaiting
        ssubscribe / sunsubscribe on this ClusterPubSub - which re-takes the
        same per-node I/O lock - hung the reader task permanently and
        silently. wait_for so a regression fails fast instead of hanging CI.
        """
        pubsub = self._make_cluster_pubsub()
        node = self._make_node("127.0.0.1:7000")
        pubsub.cluster.get_node_from_key.return_value = node

        async def handler(message):
            await pubsub.ssubscribe(b"bar")

        node_ps = self._make_real_node_pubsub([b"smessage", b"foo", b"hi"], handler)
        node_ps.ssubscribe = AsyncMock()
        pubsub.node_pubsub_mapping[node.name] = node_ps

        with mock.patch.object(pubsub, "_get_node_pubsub", return_value=node_ps):
            message = await asyncio.wait_for(
                pubsub.get_sharded_message(timeout=0.01, target_node=node),
                timeout=2.0,
            )

        assert message is None
        node_ps.ssubscribe.assert_awaited_once_with(b"bar")
        assert not ClusterPubSub._pubsub_io_lock(node_ps).locked()

    async def test_detach_shard_channel_on_a_real_node_pubsub(self):
        """
        _detach_shard_channel is ported from sync, where PubSub has a
        subscribed_event to clear. The async PubSub has none, so touching it
        raised AttributeError out of _forget_shard_channel_on_old_node -
        which reinitialize_shard_subscriptions does not catch.
        """
        node_ps = PubSub(MagicMock(), encoder=Encoder("utf-8", "strict", False))
        node_ps.shard_channels = {b"foo": None}

        ClusterPubSub._detach_shard_channel(node_ps, b"foo")

        assert node_ps.shard_channels == {}
        assert not node_ps.subscribed

    def _make_gone_node_pubsub(self):
        """A per-node ``PubSub`` in the state the reconciliation GC leaves.

        ``parse_response`` stays real - it is what raises ``RuntimeError`` on a
        ``None`` connection, so stubbing it would make the regression
        untestable.
        """
        p = PubSub(MagicMock(), encoder=Encoder("utf-8", "strict", False))
        p.shard_channels = {b"foo": None}
        return p

    async def test_poll_skips_a_pubsub_the_gc_closed_mid_pass(self):
        """
        _pubsubs_generator yields from a snapshot of node_pubsub_mapping, so a
        per-node pubsub the reconciliation GC aclose()d and popped can still
        reach _poll_node_pubsub. PubSub.parse_response raises RuntimeError on a
        None connection, and _sharded_message_generator catches only
        Moved/Connection/Timeout/OSError - so the whole pass aborted, taking
        the healthy siblings' messages with it.
        """
        pubsub = self._make_cluster_pubsub()
        closed_ps = self._make_gone_node_pubsub()
        await closed_ps.aclose()
        live_ps = self._make_real_node_pubsub([b"smessage", b"foo", b"hi"])
        pubsub.node_pubsub_mapping = {
            "127.0.0.1:7000": closed_ps,
            "127.0.0.1:7001": live_ps,
        }
        pubsub._pubsubs_generator = ClusterPubSub._pubsubs_generator(pubsub)

        message = await pubsub.get_sharded_message(timeout=0.01)

        assert closed_ps.connection is None
        assert message is not None
        assert message["channel"] == b"foo"

    @pytest.mark.parametrize("via_target_node", [True, False])
    async def test_poll_skips_a_never_subscribed_pubsub(self, via_target_node):
        """
        _get_node_pubsub registers a per-node pubsub before its caller's first
        ssubscribe, and awaiting the I/O lock in between lets the reader task
        in - so a poll can land on a pubsub whose connection is still None.
        """
        pubsub = self._make_cluster_pubsub()
        node = self._make_node("127.0.0.1:7000")
        node_ps = PubSub(MagicMock(), encoder=Encoder("utf-8", "strict", False))
        pubsub.node_pubsub_mapping[node.name] = node_ps
        pubsub._pubsubs_generator = ClusterPubSub._pubsubs_generator(pubsub)

        if via_target_node:
            message = await pubsub.get_sharded_message(timeout=0.01, target_node=node)
        else:
            message = await pubsub.get_sharded_message(timeout=0.01)

        assert message is None

    # --- shard commands routed through execute_command --------------------

    @pytest.mark.parametrize(
        "command,override",
        [("SSUBSCRIBE", "ssubscribe"), ("SUNSUBSCRIBE", "sunsubscribe")],
    )
    async def test_execute_command_delegates_shard_subscriptions(
        self, command, override
    ):
        """
        execute_command's shard branch used to dispatch SSUBSCRIBE /
        SUNSUBSCRIBE straight at the per-node pubsub, which both wrote the
        socket without the per-node I/O lock and recorded nothing - so the
        channel stayed invisible to the reader loop and to on_connect's replay.
        It must go through the overrides, which own the lock and the
        bookkeeping.
        """
        pubsub = self._make_cluster_pubsub()
        node = self._make_node("127.0.0.1:7000")
        node_ps = self._make_node_pubsub()
        pubsub.node_pubsub_mapping[node.name] = node_ps
        pubsub.cluster.get_node_from_key.return_value = node
        delegate = AsyncMock()
        setattr(pubsub, override, delegate)

        await pubsub.execute_command(command, b"foo")

        delegate.assert_awaited_once_with(b"foo")
        node_ps.execute_command.assert_not_called()

    async def test_execute_command_ssubscribe_records_the_channel(self):
        """
        End-to-end counterpart to the delegation test: the bookkeeping the raw
        dispatch skipped actually lands.
        """
        pubsub = self._make_cluster_pubsub()
        node = self._make_node("127.0.0.1:7000")
        node_ps = self._make_node_pubsub()
        pubsub.node_pubsub_mapping[node.name] = node_ps
        pubsub.cluster.get_node_from_key.return_value = node

        await pubsub.execute_command("SSUBSCRIBE", b"foo")

        node_ps.ssubscribe.assert_awaited_once_with(b"foo")
        assert pubsub._shard_channel_to_node[b"foo"] == node.name

    async def test_execute_command_spublish_holds_the_node_io_lock(self):
        """
        SPUBLISH stays a raw send - there is no override to delegate to - so
        the branch itself has to take the per-node I/O lock, or the send can
        reconnect the socket under a concurrent bounded poll.
        """
        pubsub = self._make_cluster_pubsub()
        node = self._make_node("127.0.0.1:7000")
        node_ps = self._make_real_node_pubsub(None)
        io_lock = ClusterPubSub._pubsub_io_lock(node_ps)
        pubsub.node_pubsub_mapping[node.name] = node_ps
        pubsub.cluster.get_node_from_key.return_value = node

        observed = []
        node_ps.execute_command = AsyncMock(
            side_effect=lambda *a, **kw: observed.append(io_lock.locked())
        )

        await pubsub.execute_command("SPUBLISH", b"foo", b"hi")

        assert observed == [True]
        assert not io_lock.locked()

    async def test_aclose_holds_the_node_io_lock(self):
        """
        The other four per-node aclose() sites take the I/O lock; teardown must
        too, so it does not disconnect the socket beneath a bounded poll parked
        in parse_response.
        """
        pubsub = self._make_cluster_pubsub()
        node = self._make_node("127.0.0.1:7000")
        node_ps = self._make_real_node_pubsub(None)
        io_lock = ClusterPubSub._pubsub_io_lock(node_ps)
        pubsub.node_pubsub_mapping[node.name] = node_ps

        observed = []
        node_ps.aclose = AsyncMock(
            side_effect=lambda: observed.append(io_lock.locked())
        )

        await pubsub.aclose()

        assert observed == [True]
        assert not io_lock.locked()
        assert pubsub.node_pubsub_mapping == {}


class _StatefulNodePubSub:
    """Per-node ``PubSub`` stand-in with real subscription state.

    Only the surface ``ClusterPubSub`` touches on a per-node pubsub is
    implemented: the subscription dicts, the ``subscribed`` flag derived from
    them, and the three wire calls. ``sunsubscribe_error`` /
    ``get_message_error`` inject the transient failures the tests are about.
    """

    def __init__(self, shard_channels=None):
        self.shard_channels = dict(shard_channels or {})
        self.pending_unsubscribe_shard_channels = set()
        self.channels = {}
        self.patterns = {}
        self.subscribed_event = asyncio.Event()
        if self.shard_channels:
            self.subscribed_event.set()
        # Connected, like any per-node pubsub the reader can poll: the poll
        # path skips one whose connection a teardown has already dropped.
        self.connection = MagicMock()
        self.sunsubscribe_error = None
        self.get_message_error = None
        self.aclose_calls = 0

    @property
    def subscribed(self):
        # Mirrors the real async PubSub.subscribed property, which is derived
        # from the subscription dicts. The async PubSub has no
        # subscribed_event; this stand-in keeps one only so the sync and async
        # test doubles stay diffable.
        return bool(self.channels or self.patterns or self.shard_channels)

    async def ssubscribe(self, *args):
        for arg in args:
            name = getattr(arg, "name", arg)
            self.shard_channels[name] = getattr(arg, "handler", None)
        self.subscribed_event.set()

    async def sunsubscribe(self, channel):
        if self.sunsubscribe_error is not None:
            raise self.sunsubscribe_error
        self.shard_channels.pop(channel, None)
        if not self.shard_channels:
            self.subscribed_event.clear()

    async def parse_response(self, block=True, timeout=0.0):
        # ClusterPubSub polls a per-node pubsub as parse_response (under the
        # per-node I/O lock) followed by handle_message (outside it), so the
        # injected failure has to surface from the read half.
        if self.get_message_error is not None:
            raise self.get_message_error
        return None

    async def handle_message(self, response, ignore_subscribe_messages=False):
        return response

    async def get_message(self, ignore_subscribe_messages=False, timeout=0.0):
        return await self.handle_message(
            await self.parse_response(block=(timeout is None), timeout=timeout),
            ignore_subscribe_messages,
        )

    async def aclose(self):
        self.aclose_calls += 1
        self.shard_channels.clear()
        self.pending_unsubscribe_shard_channels.clear()
        self.subscribed_event.clear()
