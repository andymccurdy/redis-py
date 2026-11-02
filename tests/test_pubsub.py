import logging
import platform
import queue
import socket
import sys
import threading
import time
import weakref
from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor
from contextlib import contextmanager
from unittest import mock
from unittest.mock import patch

import pytest
import redis
from redis import Subscription
from redis._parsers import Encoder
from redis.backoff import NoBackoff
from redis.client import PubSub
from redis.cluster import ClusterPubSub
from redis.crc import key_slot
from redis.event import EventDispatcher
from redis.exceptions import (
    ConnectionError,
    RedisError,
    ResponseError,
    SlotNotCoveredError,
    TimeoutError,
)
from redis.observability import recorder
from redis.observability.config import OTelConfig, MetricGroup
from redis.observability.metrics import RedisMetricsCollector
from redis.retry import Retry
from redis.utils import str_if_bytes

from .conftest import (
    REDIS_INFO,
    _get_client,
    is_resp2_connection,
    skip_if_redis_enterprise,
    skip_if_server_version_lt,
)


def wait_for_message(
    pubsub, timeout=0.5, ignore_subscribe_messages=False, node=None, func=None
):
    now = time.monotonic()
    timeout = now + timeout
    while now < timeout:
        if node:
            message = pubsub.get_sharded_message(
                ignore_subscribe_messages=ignore_subscribe_messages, target_node=node
            )
        elif func:
            message = func(ignore_subscribe_messages=ignore_subscribe_messages)
        else:
            message = pubsub.get_message(
                ignore_subscribe_messages=ignore_subscribe_messages
            )
        if message is not None:
            return message
        time.sleep(0.01)
        now = time.monotonic()
    return None


def wait_for_handled_message(test, get_message, expected, timeout=5.0):
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
        assert get_message(timeout=0.1) is None


def make_message(type, channel, data, pattern=None):
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


@pytest.mark.fixed_client
class TestPubSubSubscriberModeProtocol:
    DEFAULT_STANDALONE_URL = "redis://localhost:6379/0"
    DEFAULT_CLUSTER_URL = "redis://localhost:16379/0"

    def _get_resp2_client(self, request, cluster=False):
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
            else:
                client = redis.Redis.from_url(
                    redis_url,
                    protocol=2,
                    socket_connect_timeout=1,
                    socket_timeout=1,
                )
            client.ping()
        except RedisError as exc:
            self._close_client(client)
            pytest.skip(
                f"Redis {'cluster' if cluster else 'standalone'} unavailable: {exc}"
            )

        if not is_resp2_connection(client):
            self._close_client(client)
            pytest.skip("subscriber-mode command rejection is RESP2-specific")

        request.addfinalizer(lambda: self._close_client(client))
        return client

    def _get_url(self, request, cluster):
        if bool(REDIS_INFO.get("cluster_enabled", False)) == cluster:
            return request.config.getoption("--redis-url")
        if cluster:
            return self.DEFAULT_CLUSTER_URL
        return self.DEFAULT_STANDALONE_URL

    def _close_client(self, client):
        if client is None:
            return
        client.close()
        if hasattr(client, "disconnect_connection_pools"):
            client.disconnect_connection_pools()

    def test_subscriber_mode_rejects_regular_command_after_subscribe(self, request):
        r = self._get_resp2_client(request)
        p = r.pubsub()
        try:
            p.subscribe("subscriber-mode")
            assert wait_for_message(p)["type"] == "subscribe"

            p.execute_command("SET", "should-not-run", "1")
            with pytest.raises(ResponseError, match="only .* allowed|Can't execute"):
                p.parse_response(block=False, timeout=1)
        finally:
            p.close()

    def test_subscriber_mode_rejects_regular_command_after_psubscribe(self, request):
        r = self._get_resp2_client(request)
        p = r.pubsub()
        try:
            p.psubscribe("subscriber-*")
            assert wait_for_message(p)["type"] == "psubscribe"

            p.execute_command("SET", "should-not-run", "1")
            with pytest.raises(ResponseError, match="only .* allowed|Can't execute"):
                p.parse_response(block=False, timeout=1)
        finally:
            p.close()

    @skip_if_server_version_lt("7.0.0")
    def test_subscriber_mode_rejects_regular_command_after_ssubscribe(self, request):
        r = self._get_resp2_client(request, cluster=True)
        channel = "subscriber-mode-{shard}"
        node = r.get_node_from_key(channel)
        p = r.pubsub()
        try:
            p.ssubscribe(channel)
            assert wait_for_message(p, node=node)["type"] == "ssubscribe"

            per_node_pubsub = p.node_pubsub_mapping[node.name]
            per_node_pubsub.execute_command("SET", "should-not-run-{shard}", "1")
            with pytest.raises(ResponseError, match="only .* allowed|Can't execute"):
                per_node_pubsub.parse_response(block=False, timeout=1)
        finally:
            p.close()


class TestPubSubCloseServerCleanup:
    def _wait_for_result_to_match_expected(self, get_result, expected, timeout=2.0):
        deadline = time.monotonic() + timeout
        result = None
        while time.monotonic() < deadline:
            result = get_result()
            if result == expected:
                return
            time.sleep(0.01)
        assert result == expected

    @pytest.mark.onlynoncluster
    def test_close_while_subscribed_removes_numsub_entry(self, r):
        channel = "close-cleanup"
        p = r.pubsub()
        try:
            p.subscribe(channel)
            assert wait_for_message(p)["type"] == "subscribe"
            self._wait_for_result_to_match_expected(
                lambda: r.pubsub_numsub(channel),
                [(channel.encode(), 1)],
            )
        finally:
            p.close()

        assert p.connection is None
        assert p.channels == {}
        self._wait_for_result_to_match_expected(
            lambda: r.pubsub_numsub(channel),
            [(channel.encode(), 0)],
        )

    @pytest.mark.onlycluster
    @skip_if_server_version_lt("7.0.0")
    def test_close_while_ssubscribed_removes_shardnumsub_entry(self, r):
        channel = "close-cleanup-{shard}"
        node = r.get_node_from_key(channel)
        p = r.pubsub()
        try:
            p.ssubscribe(channel)
            assert wait_for_message(p, node=node)["type"] == "ssubscribe"
            self._wait_for_result_to_match_expected(
                lambda: r.pubsub_shardnumsub(channel, target_nodes=node),
                [(channel.encode(), 1)],
            )
        finally:
            p.close()

        assert p.node_pubsub_mapping == {}
        assert p.shard_channels == {}
        self._wait_for_result_to_match_expected(
            lambda: r.pubsub_shardnumsub(channel, target_nodes=node),
            [(channel.encode(), 0)],
        )


@pytest.mark.onlycluster
@skip_if_server_version_lt("7.0.0")
class TestClusterShardedPubSubDelivery:
    def _wait_for_sharded_acks(
        self, pubsub, count, node=None, message_type="ssubscribe"
    ):
        for _ in range(count):
            message = wait_for_message(
                pubsub,
                timeout=2.0,
                node=node,
                func=None if node else pubsub.get_sharded_message,
            )
            assert message is not None
            assert message["type"] == message_type

    def _read_sharded_messages(self, pubsub, expected_count, timeout=5.0):
        messages = []
        deadline = time.monotonic() + timeout
        while time.monotonic() < deadline and len(messages) < expected_count:
            message = pubsub.get_sharded_message(timeout=0.05)
            if message is not None and message["type"] == "smessage":
                messages.append(message)
        return messages

    def test_multiple_sharded_subscribers_receive_all_messages(self, r):
        channels = ["multi-sub-{shared}:0", "multi-sub-{shared}:1"]
        subscriber_count = 2
        message_count = 20
        expected = set(range(message_count))
        received = [defaultdict(set) for _ in range(subscriber_count)]
        subscribers = [r.pubsub() for _ in range(subscriber_count)]

        try:
            for pubsub in subscribers:
                pubsub.ssubscribe(*channels)
                self._wait_for_sharded_acks(pubsub, len(channels))

            for seq in range(message_count):
                for channel in channels:
                    assert r.spublish(channel, str(seq)) == subscriber_count

            deadline = time.monotonic() + 5.0
            while time.monotonic() < deadline:
                for index, pubsub in enumerate(subscribers):
                    message = pubsub.get_sharded_message(timeout=0.01)
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
                pubsub.close()

    def test_duplicate_ssubscribe_delivers_each_message_once(self, r):
        channel = "duplicate-ssubscribe-{shared}"
        p = r.pubsub()
        try:
            p.ssubscribe(channel)
            self._wait_for_sharded_acks(p, 1)

            p.ssubscribe(channel)
            maybe_ack = p.get_sharded_message(timeout=0.5)
            assert maybe_ack is None or maybe_ack["type"] == "ssubscribe"

            for seq in range(10):
                assert r.spublish(channel, str(seq)) == 1

            messages = self._read_sharded_messages(p, expected_count=10)
            received = [int(message["data"]) for message in messages]

            assert sorted(received) == list(range(10))
            assert p.get_sharded_message(timeout=0.5) is None
        finally:
            p.close()

    def test_handler_may_ssubscribe_from_inside_the_handler(self, r):
        """
        A message handler runs inside get_sharded_message, so an ssubscribe
        from inside it re-enters the per-node I/O lock and then takes
        _shard_state_lock. Holding the I/O lock across the dispatch deadlocked
        against the reconciliation worker.
        """
        first = "handler-resub-a-{shared}"
        second = "handler-resub-b-{shared}"
        p = r.pubsub()
        resubscribed = threading.Event()

        def handler(message):
            if not resubscribed.is_set():
                p.ssubscribe(second)
                resubscribed.set()

        try:
            p.ssubscribe(**{first: handler})
            self._wait_for_sharded_acks(p, 1)

            assert r.spublish(first, "go") == 1
            deadline = time.monotonic() + 5.0
            while time.monotonic() < deadline and not resubscribed.is_set():
                p.get_sharded_message(timeout=0.05)
            assert resubscribed.is_set(), (
                "handler never completed: the dispatch deadlocked on the "
                "per-node I/O lock"
            )

            self._wait_for_sharded_acks(p, 1)
            assert r.spublish(second, "hi") == 1
            messages = self._read_sharded_messages(p, expected_count=1)
            assert [str_if_bytes(m["channel"]) for m in messages] == [second]
        finally:
            p.close()

    def test_no_messages_delivered_after_sunsubscribe(self, r):
        kept = "still-subscribed-{shared}"
        removed = "removed-{shared}"
        p = r.pubsub()
        try:
            p.ssubscribe(kept, removed)
            self._wait_for_sharded_acks(p, 2)

            p.sunsubscribe(removed)
            self._wait_for_sharded_acks(p, 1, message_type="sunsubscribe")

            for seq in range(10):
                assert r.spublish(kept, str(seq)) == 1
                assert r.spublish(removed, str(seq)) == 0

            messages = self._read_sharded_messages(p, expected_count=10)
            delivered_channels = [
                str_if_bytes(message["channel"]) for message in messages
            ]

            assert delivered_channels == [kept] * 10
            assert p.get_sharded_message(timeout=0.5) is None
        finally:
            p.close()


@pytest.mark.onlycluster
@skip_if_server_version_lt("7.0.0")
class TestClusterShardedPubSubConnectionLifecycle:
    def _wait_for_ssubscribe_ack(self, pubsub):
        message = wait_for_message(
            pubsub,
            timeout=2.0,
            func=pubsub.get_sharded_message,
        )
        assert message is not None
        assert message["type"] == "ssubscribe"

    def test_cluster_sharded_pubsub_creates_connections_lazily(self, r):
        channel = "lazy-connection-{shared}"
        node = r.get_node_from_key(channel)
        p = r.pubsub()
        try:
            assert p.node_pubsub_mapping == {}
            assert p.connection is None

            p.ssubscribe(channel)
            self._wait_for_ssubscribe_ack(p)

            assert list(p.node_pubsub_mapping) == [node.name]
            assert p.node_pubsub_mapping[node.name].connection is not None
        finally:
            p.close()

    def test_cluster_sharded_pubsub_reuses_connection_for_same_shard(self, r):
        first = "reuse-1-{same-shard}"
        second = "reuse-2-{same-shard}"
        node = r.get_node_from_key(first)
        assert node.name == r.get_node_from_key(second).name
        p = r.pubsub()
        try:
            p.ssubscribe(first)
            self._wait_for_ssubscribe_ack(p)
            first_mapping = dict(p.node_pubsub_mapping)
            first_pubsub = p.node_pubsub_mapping[node.name]

            p.ssubscribe(second)
            self._wait_for_ssubscribe_ack(p)

            assert p.node_pubsub_mapping == first_mapping
            assert p.node_pubsub_mapping[node.name] is first_pubsub
            assert len(p.node_pubsub_mapping) == 1
        finally:
            p.close()


class TestPubSubSubscribeUnsubscribe:
    def _test_subscribe_unsubscribe(
        self, p, sub_type, unsub_type, sub_func, unsub_func, keys
    ):
        for key in keys:
            assert sub_func(key) is None

        # should be a message for each channel/pattern we just subscribed to
        for i, key in enumerate(keys):
            assert wait_for_message(p) == make_message(sub_type, key, i + 1)

        for key in keys:
            assert unsub_func(key) is None

        # should be a message for each channel/pattern we just unsubscribed
        # from
        for i, key in enumerate(keys):
            i = len(keys) - 1 - i
            assert wait_for_message(p) == make_message(unsub_type, key, i)

    def test_channel_subscribe_unsubscribe(self, r):
        kwargs = make_subscribe_test_data(r.pubsub(), "channel")
        self._test_subscribe_unsubscribe(**kwargs)

    def test_pattern_subscribe_unsubscribe(self, r):
        kwargs = make_subscribe_test_data(r.pubsub(), "pattern")
        self._test_subscribe_unsubscribe(**kwargs)

    @pytest.mark.onlynoncluster
    @skip_if_server_version_lt("7.0.0")
    def test_shard_channel_subscribe_unsubscribe(self, r):
        kwargs = make_subscribe_test_data(r.pubsub(), "shard_channel")
        self._test_subscribe_unsubscribe(**kwargs)

    @pytest.mark.onlycluster
    @skip_if_server_version_lt("7.0.0")
    def test_shard_channel_subscribe_unsubscribe_cluster(self, r):
        node_channels = defaultdict(int)
        p = r.pubsub()
        keys = {
            "foo": r.get_node_from_key("foo"),
            "bar": r.get_node_from_key("bar"),
            "uni" + chr(4456) + "code": r.get_node_from_key("uni" + chr(4456) + "code"),
        }

        for key, node in keys.items():
            assert p.ssubscribe(key) is None

        # should be a message for each shard_channel we just subscribed to
        for key, node in keys.items():
            node_channels[node.name] += 1
            assert wait_for_message(p, node=node) == make_message(
                "ssubscribe", key, node_channels[node.name]
            )

        for key in keys.keys():
            assert p.sunsubscribe(key) is None

        # should be a message for each shard_channel we just unsubscribed
        # from
        for key, node in keys.items():
            node_channels[node.name] -= 1
            assert wait_for_message(p, node=node) == make_message(
                "sunsubscribe", key, node_channels[node.name]
            )

    def _test_resubscribe_on_reconnection(
        self, p, sub_type, unsub_type, sub_func, unsub_func, keys
    ):
        for key in keys:
            assert sub_func(key) is None

        # should be a message for each channel/pattern we just subscribed to
        for i, key in enumerate(keys):
            assert wait_for_message(p) == make_message(sub_type, key, i + 1)

        # manually disconnect
        p.connection.disconnect()

        # calling get_message again reconnects and resubscribes
        # note, we may not re-subscribe to channels in exactly the same order
        # so we have to do some extra checks to make sure we got them all
        messages = []
        for i in range(len(keys)):
            messages.append(wait_for_message(p))

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

    def test_resubscribe_to_channels_on_reconnection(self, r):
        kwargs = make_subscribe_test_data(r.pubsub(), "channel")
        self._test_resubscribe_on_reconnection(**kwargs)

    @pytest.mark.onlynoncluster
    def test_resubscribe_to_patterns_on_reconnection(self, r):
        kwargs = make_subscribe_test_data(r.pubsub(), "pattern")
        self._test_resubscribe_on_reconnection(**kwargs)

    @pytest.mark.onlynoncluster
    @skip_if_server_version_lt("7.0.0")
    def test_resubscribe_to_shard_channels_on_reconnection(self, r):
        kwargs = make_subscribe_test_data(r.pubsub(), "shard_channel")
        self._test_resubscribe_on_reconnection(**kwargs)

    @pytest.mark.onlynoncluster
    def test_resubscribe_binary_channel_on_reconnection(self, r):
        """Binary channel names that are not valid UTF-8 must survive
        reconnection without raising ``UnicodeDecodeError``.
        See https://github.com/redis/redis-py/issues/3912
        """
        # b'\x80\x81\x82' is deliberately invalid UTF-8
        binary_channel = b"\x80\x81\x82"
        p = r.pubsub()
        p.subscribe(binary_channel)
        assert wait_for_message(p) is not None  # consume subscribe ack

        # force reconnect
        p.connection.disconnect()

        # get_message triggers on_connect → re-subscribe; must not raise
        messages = []
        for _ in range(1):
            message = wait_for_message(p)
            assert message is not None
            messages.append(message)

        assert len(messages) == 1
        assert messages[0]["type"] == "subscribe"
        assert messages[0]["channel"] == binary_channel

    @pytest.mark.onlynoncluster
    def test_resubscribe_binary_pattern_on_reconnection(self, r):
        """Binary pattern names that are not valid UTF-8 must survive
        reconnection without raising ``UnicodeDecodeError``.
        See https://github.com/redis/redis-py/issues/3912
        """
        binary_pattern = b"\x80\x81*"
        p = r.pubsub()
        p.psubscribe(binary_pattern)
        assert wait_for_message(p) is not None  # consume psubscribe ack

        # force reconnect
        p.connection.disconnect()

        messages = []
        for _ in range(1):
            message = wait_for_message(p)
            assert message is not None
            messages.append(message)

        assert len(messages) == 1
        assert messages[0]["type"] == "psubscribe"
        assert messages[0]["channel"] == binary_pattern

    @pytest.mark.onlynoncluster
    def test_resubscribe_binary_channel_with_handler_on_reconnection(self, r):
        binary_channel = b"\x80\x81\x82"
        handled = []

        def handler(message):
            handled.append(message)

        p = r.pubsub()
        p.subscribe(Subscription(binary_channel, handler))
        assert wait_for_message(p) == {
            "type": "subscribe",
            "pattern": None,
            "channel": binary_channel,
            "data": 1,
        }

        p.connection.disconnect()

        assert wait_for_message(p) == {
            "type": "subscribe",
            "pattern": None,
            "channel": binary_channel,
            "data": 1,
        }
        assert r.publish(binary_channel, b"test message") == 1
        assert p.get_message(timeout=1) is None
        assert handled == [
            {
                "type": "message",
                "pattern": None,
                "channel": binary_channel,
                "data": b"test message",
            }
        ]

    @pytest.mark.fixed_client
    def test_resubscribe_binary_handler_uses_subscription(self):
        pool = mock.MagicMock()
        encoder = Encoder(
            encoding="utf-8", encoding_errors="strict", decode_responses=False
        )
        p = PubSub(connection_pool=pool, encoder=encoder)
        binary_channel = b"\x80\x81\x82"
        handler = mock.Mock()
        p.channels = {binary_channel: handler}

        calls = []
        with mock.patch.object(
            p,
            "subscribe",
            side_effect=lambda *a, **kw: calls.append((tuple(a), dict(kw))),
        ):
            p._resubscribe(p.channels, p.subscribe)

        assert calls == [((Subscription(binary_channel, handler),), {})]

    @pytest.mark.fixed_client
    def test_cluster_pubsub_resubscribe_shard_channels_groups_by_slot(self):
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
        pool = mock.MagicMock()
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
        with mock.patch.object(
            p,
            "ssubscribe",
            side_effect=lambda *a, **kw: calls.append((tuple(a), dict(kw))),
        ):
            ClusterPubSub._resubscribe_shard_channels(p)

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

    @pytest.mark.fixed_client
    def test_cluster_pubsub_resubscribe_binary_shard_channel_with_handler(self):
        pool = mock.MagicMock()
        encoder = Encoder(
            encoding="utf-8", encoding_errors="strict", decode_responses=False
        )
        p = PubSub(connection_pool=pool, encoder=encoder)
        binary_channel = b"\x80\x81\x82"
        handler = mock.Mock()
        p.shard_channels = {binary_channel: handler}

        calls = []
        with mock.patch.object(
            p,
            "ssubscribe",
            side_effect=lambda *a, **kw: calls.append((tuple(a), dict(kw))),
        ):
            ClusterPubSub._resubscribe_shard_channels(p)

        assert calls == [((Subscription(binary_channel, handler),), {})]

    @pytest.mark.onlynoncluster
    @skip_if_server_version_lt("7.0.0")
    def test_standalone_pubsub_on_connect_batches_shard_channels(self, r):
        """The standalone PubSub has no notion of slots and must keep
        batching every tracked shard channel into a single SSUBSCRIBE call
        on reconnect, as it did before cluster-aware resubscription existed.
        """
        p = r.pubsub()
        ch_a = b"{slot-a}-one"
        ch_b = b"{slot-b}-one"
        assert key_slot(ch_a) != key_slot(ch_b)

        p.shard_channels = {ch_a: None, ch_b: None}

        calls = []
        with mock.patch.object(
            p,
            "ssubscribe",
            side_effect=lambda *a, **kw: calls.append((tuple(a), dict(kw))),
        ):
            p.on_connect(connection=None)

        assert len(calls) == 1
        args, kwargs = calls[0]
        assert set(args) == {ch_a, ch_b}
        assert kwargs == {}
        p.close()

    def _test_subscribed_property(
        self, p, sub_type, unsub_type, sub_func, unsub_func, keys
    ):
        assert p.subscribed is False
        sub_func(keys[0])
        # we're now subscribed even though we haven't processed the
        # reply from the server just yet
        assert p.subscribed is True
        assert wait_for_message(p) == make_message(sub_type, keys[0], 1)
        # we're still subscribed
        assert p.subscribed is True

        # unsubscribe from all channels
        unsub_func()
        # we're still technically subscribed until we process the
        # response messages from the server
        assert p.subscribed is True
        assert wait_for_message(p) == make_message(unsub_type, keys[0], 0)
        # now we're no longer subscribed as no more messages can be delivered
        # to any channels we were listening to
        assert p.subscribed is False

        # subscribing again flips the flag back
        sub_func(keys[0])
        assert p.subscribed is True
        assert wait_for_message(p) == make_message(sub_type, keys[0], 1)

        # unsubscribe again
        unsub_func()
        assert p.subscribed is True
        # subscribe to another channel before reading the unsubscribe response
        sub_func(keys[1])
        assert p.subscribed is True
        # read the unsubscribe for key1
        assert wait_for_message(p) == make_message(unsub_type, keys[0], 0)
        # we're still subscribed to key2, so subscribed should still be True
        assert p.subscribed is True
        # read the key2 subscribe message
        assert wait_for_message(p) == make_message(sub_type, keys[1], 1)
        unsub_func()
        # haven't read the message yet, so we're still subscribed
        assert p.subscribed is True
        assert wait_for_message(p) == make_message(unsub_type, keys[1], 0)
        # now we're finally unsubscribed
        assert p.subscribed is False

    def test_subscribe_property_with_channels(self, r):
        kwargs = make_subscribe_test_data(r.pubsub(), "channel")
        self._test_subscribed_property(**kwargs)

    @pytest.mark.onlynoncluster
    def test_subscribe_property_with_patterns(self, r):
        kwargs = make_subscribe_test_data(r.pubsub(), "pattern")
        self._test_subscribed_property(**kwargs)

    @pytest.mark.onlynoncluster
    @skip_if_server_version_lt("7.0.0")
    def test_subscribe_property_with_shard_channels(self, r):
        kwargs = make_subscribe_test_data(r.pubsub(), "shard_channel")
        self._test_subscribed_property(**kwargs)

    @pytest.mark.onlycluster
    @skip_if_server_version_lt("7.0.0")
    def test_subscribe_property_with_shard_channels_cluster(self, r):
        p = r.pubsub()
        keys = ["foo", "bar", "uni" + chr(4456) + "code"]
        nodes = [r.get_node_from_key(key) for key in keys]
        assert p.subscribed is False
        p.ssubscribe(keys[0])
        # we're now subscribed even though we haven't processed the
        # reply from the server just yet
        assert p.subscribed is True
        assert wait_for_message(p, node=nodes[0]) == make_message(
            "ssubscribe", keys[0], 1
        )
        # we're still subscribed
        assert p.subscribed is True

        # unsubscribe from all shard_channels
        p.sunsubscribe()
        # we're still technically subscribed until we process the
        # response messages from the server
        assert p.subscribed is True
        assert wait_for_message(p, node=nodes[0]) == make_message(
            "sunsubscribe", keys[0], 0
        )
        # now we're no longer subscribed as no more messages can be delivered
        # to any channels we were listening to
        assert p.subscribed is False

        # subscribing again flips the flag back
        p.ssubscribe(keys[0])
        assert p.subscribed is True
        assert wait_for_message(p, node=nodes[0]) == make_message(
            "ssubscribe", keys[0], 1
        )

        # unsubscribe again
        p.sunsubscribe()
        assert p.subscribed is True
        # subscribe to another shard_channel before reading the unsubscribe response
        p.ssubscribe(keys[1])
        assert p.subscribed is True
        # read the unsubscribe for key1
        assert wait_for_message(p, node=nodes[0]) == make_message(
            "sunsubscribe", keys[0], 0
        )
        # we're still subscribed to key2, so subscribed should still be True
        assert p.subscribed is True
        # read the key2 subscribe message
        assert wait_for_message(p, node=nodes[1]) == make_message(
            "ssubscribe", keys[1], 1
        )
        p.sunsubscribe()
        # haven't read the message yet, so we're still subscribed
        assert p.subscribed is True
        assert wait_for_message(p, node=nodes[1]) == make_message(
            "sunsubscribe", keys[1], 0
        )
        # now we're finally unsubscribed
        assert p.subscribed is False

    @skip_if_server_version_lt("7.0.0")
    def test_ignore_all_subscribe_messages(self, r):
        p = r.pubsub(ignore_subscribe_messages=True)

        checks = (
            (p.subscribe, "foo", p.get_message),
            (p.unsubscribe, "foo", p.get_message),
            (p.psubscribe, "f*", p.get_message),
            (p.punsubscribe, "f*", p.get_message),
            (p.ssubscribe, "foo", p.get_sharded_message),
            (p.sunsubscribe, "foo", p.get_sharded_message),
        )

        assert p.subscribed is False
        for func, channel, get_func in checks:
            assert func(channel) is None
            assert p.subscribed is True
            assert get_func(timeout=1) is None
        assert p.subscribed is False

    @skip_if_server_version_lt("7.0.0")
    def test_ignore_individual_subscribe_messages(self, r):
        p = r.pubsub()

        checks = (
            (p.subscribe, "foo", p.get_message),
            (p.unsubscribe, "foo", p.get_message),
            (p.psubscribe, "f*", p.get_message),
            (p.punsubscribe, "f*", p.get_message),
            (p.ssubscribe, "foo", p.get_sharded_message),
            (p.sunsubscribe, "foo", p.get_sharded_message),
        )

        assert p.subscribed is False
        for func, channel, get_func in checks:
            assert func(channel) is None
            assert p.subscribed is True
            message = get_func(ignore_subscribe_messages=True, timeout=1)
            assert message is None
        assert p.subscribed is False

    def test_sub_unsub_resub_channels(self, r):
        kwargs = make_subscribe_test_data(r.pubsub(), "channel")
        self._test_sub_unsub_resub(**kwargs)

    @pytest.mark.onlynoncluster
    def test_sub_unsub_resub_patterns(self, r):
        kwargs = make_subscribe_test_data(r.pubsub(), "pattern")
        self._test_sub_unsub_resub(**kwargs)

    @pytest.mark.onlynoncluster
    @skip_if_server_version_lt("7.0.0")
    def test_sub_unsub_resub_shard_channels(self, r):
        kwargs = make_subscribe_test_data(r.pubsub(), "shard_channel")
        self._test_sub_unsub_resub(**kwargs)

    def _test_sub_unsub_resub(
        self, p, sub_type, unsub_type, sub_func, unsub_func, keys
    ):
        # https://github.com/andymccurdy/redis-py/issues/764
        key = keys[0]
        sub_func(key)
        unsub_func(key)
        sub_func(key)
        assert p.subscribed is True
        assert wait_for_message(p) == make_message(sub_type, key, 1)
        assert wait_for_message(p) == make_message(unsub_type, key, 0)
        assert wait_for_message(p) == make_message(sub_type, key, 1)
        assert p.subscribed is True

    @pytest.mark.onlycluster
    @skip_if_server_version_lt("7.0.0")
    def test_sub_unsub_resub_shard_channels_cluster(self, r):
        p = r.pubsub()
        key = "foo"
        p.ssubscribe(key)
        p.sunsubscribe(key)
        p.ssubscribe(key)
        assert p.subscribed is True
        assert wait_for_message(p, func=p.get_sharded_message) == make_message(
            "ssubscribe", key, 1
        )
        assert wait_for_message(p, func=p.get_sharded_message) == make_message(
            "sunsubscribe", key, 0
        )
        assert wait_for_message(p, func=p.get_sharded_message) == make_message(
            "ssubscribe", key, 1
        )
        assert p.subscribed is True

    def test_sub_unsub_all_resub_channels(self, r):
        kwargs = make_subscribe_test_data(r.pubsub(), "channel")
        self._test_sub_unsub_all_resub(**kwargs)

    def test_sub_unsub_all_resub_patterns(self, r):
        kwargs = make_subscribe_test_data(r.pubsub(), "pattern")
        self._test_sub_unsub_all_resub(**kwargs)

    @pytest.mark.onlynoncluster
    @skip_if_server_version_lt("7.0.0")
    def test_sub_unsub_all_resub_shard_channels(self, r):
        kwargs = make_subscribe_test_data(r.pubsub(), "shard_channel")
        self._test_sub_unsub_all_resub(**kwargs)

    def _test_sub_unsub_all_resub(
        self, p, sub_type, unsub_type, sub_func, unsub_func, keys
    ):
        # https://github.com/andymccurdy/redis-py/issues/764
        key = keys[0]
        sub_func(key)
        unsub_func()
        sub_func(key)
        assert p.subscribed is True
        assert wait_for_message(p) == make_message(sub_type, key, 1)
        assert wait_for_message(p) == make_message(unsub_type, key, 0)
        assert wait_for_message(p) == make_message(sub_type, key, 1)
        assert p.subscribed is True

    @pytest.mark.onlycluster
    @skip_if_server_version_lt("7.0.0")
    def test_sub_unsub_all_resub_shard_channels_cluster(self, r):
        p = r.pubsub()
        key = "foo"
        p.ssubscribe(key)
        p.sunsubscribe()
        p.ssubscribe(key)
        assert p.subscribed is True
        assert wait_for_message(p, func=p.get_sharded_message) == make_message(
            "ssubscribe", key, 1
        )
        assert wait_for_message(p, func=p.get_sharded_message) == make_message(
            "sunsubscribe", key, 0
        )
        assert wait_for_message(p, func=p.get_sharded_message) == make_message(
            "ssubscribe", key, 1
        )
        assert p.subscribed is True


class TestPubSubMessages:
    def setup_method(self, method):
        self.message = None

    def message_handler(self, message):
        self.message = message

    def test_published_message_to_channel(self, r):
        p = r.pubsub()
        p.subscribe("foo")
        assert wait_for_message(p) == make_message("subscribe", "foo", 1)
        assert r.publish("foo", "test message") == 1

        message = wait_for_message(p)
        assert isinstance(message, dict)
        assert message == make_message("message", "foo", "test message")

    @pytest.mark.onlynoncluster
    @skip_if_server_version_lt("7.0.0")
    def test_published_message_to_shard_channel(self, r):
        p = r.pubsub()
        p.ssubscribe("foo")
        assert wait_for_message(p) == make_message("ssubscribe", "foo", 1)
        assert r.spublish("foo", "test message") == 1

        message = wait_for_message(p)
        assert isinstance(message, dict)
        assert message == make_message("smessage", "foo", "test message")

    @pytest.mark.onlycluster
    @skip_if_server_version_lt("7.0.0")
    def test_published_message_to_shard_channel_cluster(self, r):
        p = r.pubsub()
        p.ssubscribe("foo")
        assert wait_for_message(p, func=p.get_sharded_message) == make_message(
            "ssubscribe", "foo", 1
        )
        assert r.spublish("foo", "test message") == 1

        message = wait_for_message(p, func=p.get_sharded_message)
        assert isinstance(message, dict)
        assert message == make_message("smessage", "foo", "test message")

    def test_published_message_to_pattern(self, r):
        p = r.pubsub()
        p.subscribe("foo")
        p.psubscribe("f*")
        assert wait_for_message(p) == make_message("subscribe", "foo", 1)
        assert wait_for_message(p) == make_message("psubscribe", "f*", 2)
        # 1 to pattern, 1 to channel
        assert r.publish("foo", "test message") == 2

        message1 = wait_for_message(p)
        message2 = wait_for_message(p)
        assert isinstance(message1, dict)
        assert isinstance(message2, dict)

        expected = [
            make_message("message", "foo", "test message"),
            make_message("pmessage", "foo", "test message", pattern="f*"),
        ]

        assert message1 in expected
        assert message2 in expected
        assert message1 != message2

    def test_channel_message_handler(self, r):
        p = r.pubsub(ignore_subscribe_messages=True)
        p.subscribe(foo=self.message_handler)
        assert p.get_message(timeout=1) is None
        assert r.publish("foo", "test message") == 1
        wait_for_handled_message(
            self, p.get_message, make_message("message", "foo", "test message")
        )

    @pytest.mark.onlynoncluster
    def test_binary_channel_message_handler_with_subscription(self, r):
        p = r.pubsub(ignore_subscribe_messages=True)
        channel = b"\x80\x81\x82"
        p.subscribe(Subscription(channel, self.message_handler))
        assert p.get_message(timeout=1) is None
        assert r.publish(channel, b"test message") == 1
        wait_for_handled_message(
            self,
            p.get_message,
            {
                "type": "message",
                "pattern": None,
                "channel": channel,
                "data": b"test message",
            },
        )

    @skip_if_server_version_lt("7.0.0")
    def test_shard_channel_message_handler(self, r):
        p = r.pubsub(ignore_subscribe_messages=True)
        p.ssubscribe(foo=self.message_handler)
        assert p.get_sharded_message(timeout=1) is None
        assert r.spublish("foo", "test message") == 1
        wait_for_handled_message(
            self, p.get_sharded_message, make_message("smessage", "foo", "test message")
        )

    @skip_if_server_version_lt("7.0.0")
    def test_binary_shard_channel_message_handler_with_subscription(self, r):
        p = r.pubsub(ignore_subscribe_messages=True)
        channel = b"\x80\x81\x82"
        p.ssubscribe(Subscription(channel, self.message_handler))
        assert p.get_sharded_message(timeout=1) is None
        assert r.spublish(channel, b"test message") == 1
        wait_for_handled_message(
            self,
            p.get_sharded_message,
            {
                "type": "smessage",
                "pattern": None,
                "channel": channel,
                "data": b"test message",
            },
        )

    @pytest.mark.onlynoncluster
    def test_pattern_message_handler(self, r):
        p = r.pubsub(ignore_subscribe_messages=True)
        p.psubscribe(**{"f*": self.message_handler})
        assert p.get_message(timeout=1) is None
        assert r.publish("foo", "test message") == 1
        wait_for_handled_message(
            self,
            p.get_message,
            make_message("pmessage", "foo", "test message", pattern="f*"),
        )

    @pytest.mark.onlynoncluster
    def test_binary_pattern_message_handler_with_subscription(self, r):
        p = r.pubsub(ignore_subscribe_messages=True)
        pattern = b"\x80*"
        channel = b"\x80channel"
        p.psubscribe(Subscription(pattern, self.message_handler))
        assert p.get_message(timeout=1) is None
        assert r.publish(channel, b"test message") == 1
        wait_for_handled_message(
            self,
            p.get_message,
            {
                "type": "pmessage",
                "pattern": pattern,
                "channel": channel,
                "data": b"test message",
            },
        )

    def test_unicode_channel_message_handler(self, r):
        p = r.pubsub(ignore_subscribe_messages=True)
        channel = "uni" + chr(4456) + "code"
        channels = {channel: self.message_handler}
        p.subscribe(**channels)
        assert p.get_message(timeout=1) is None
        assert r.publish(channel, "test message") == 1
        wait_for_handled_message(
            self, p.get_message, make_message("message", channel, "test message")
        )

    @skip_if_server_version_lt("7.0.0")
    def test_unicode_shard_channel_message_handler(self, r):
        p = r.pubsub(ignore_subscribe_messages=True)
        channel = "uni" + chr(4456) + "code"
        channels = {channel: self.message_handler}
        p.ssubscribe(**channels)
        assert p.get_sharded_message(timeout=1) is None
        assert r.spublish(channel, "test message") == 1
        wait_for_handled_message(
            self,
            p.get_sharded_message,
            make_message("smessage", channel, "test message"),
        )

    @pytest.mark.onlynoncluster
    # see: https://redis.readthedocs.io/en/stable/clustering.html#known-pubsub-limitations
    def test_unicode_pattern_message_handler(self, r):
        p = r.pubsub(ignore_subscribe_messages=True)
        pattern = "uni" + chr(4456) + "*"
        channel = "uni" + chr(4456) + "code"
        p.psubscribe(**{pattern: self.message_handler})
        assert p.get_message(timeout=1) is None
        assert r.publish(channel, "test message") == 1
        wait_for_handled_message(
            self,
            p.get_message,
            make_message("pmessage", channel, "test message", pattern=pattern),
        )


class TestPubSubRESP3Handler:
    def my_handler(self, message):
        self.message = ["my handler", message]

    def test_push_handler(self, r):
        if is_resp2_connection(r):
            return
        p = r.pubsub(push_handler_func=self.my_handler)
        p.subscribe("foo")
        wait_for_handled_message(
            self, p.get_message, ["my handler", [b"subscribe", b"foo", 1]]
        )
        assert r.publish("foo", "test message") == 1
        wait_for_handled_message(
            self, p.get_message, ["my handler", [b"message", b"foo", b"test message"]]
        )

    @skip_if_server_version_lt("7.0.0")
    def test_push_handler_sharded_pubsub(self, r):
        if is_resp2_connection(r):
            return
        p = r.pubsub(push_handler_func=self.my_handler)
        p.ssubscribe("foo")
        wait_for_handled_message(
            self, p.get_sharded_message, ["my handler", [b"ssubscribe", b"foo", 1]]
        )
        assert r.spublish("foo", "test message") == 1
        wait_for_handled_message(
            self,
            p.get_sharded_message,
            ["my handler", [b"smessage", b"foo", b"test message"]],
        )


class TestPubSubAutoDecoding:
    "These tests only validate that we get unicode values back"

    channel = "uni" + chr(4456) + "code"
    pattern = "uni" + chr(4456) + "*"
    data = "abc" + chr(4458) + "123"

    def make_message(self, type, channel, data, pattern=None):
        return {"type": type, "channel": channel, "pattern": pattern, "data": data}

    def setup_method(self, method):
        self.message = None

    def message_handler(self, message):
        self.message = message

    @pytest.fixture()
    def r(self, request):
        return _get_client(redis.Redis, request=request, decode_responses=True)

    def test_channel_subscribe_unsubscribe(self, r):
        p = r.pubsub()
        p.subscribe(self.channel)
        assert wait_for_message(p) == self.make_message("subscribe", self.channel, 1)

        p.unsubscribe(self.channel)
        assert wait_for_message(p) == self.make_message("unsubscribe", self.channel, 0)

    def test_pattern_subscribe_unsubscribe(self, r):
        p = r.pubsub()
        p.psubscribe(self.pattern)
        assert wait_for_message(p) == self.make_message("psubscribe", self.pattern, 1)

        p.punsubscribe(self.pattern)
        assert wait_for_message(p) == self.make_message("punsubscribe", self.pattern, 0)

    @skip_if_server_version_lt("7.0.0")
    def test_shard_channel_subscribe_unsubscribe(self, r):
        p = r.pubsub()
        p.ssubscribe(self.channel)
        assert wait_for_message(p, func=p.get_sharded_message) == self.make_message(
            "ssubscribe", self.channel, 1
        )

        p.sunsubscribe(self.channel)
        assert wait_for_message(p, func=p.get_sharded_message) == self.make_message(
            "sunsubscribe", self.channel, 0
        )

    def test_channel_publish(self, r):
        p = r.pubsub()
        p.subscribe(self.channel)
        assert wait_for_message(p) == self.make_message("subscribe", self.channel, 1)
        r.publish(self.channel, self.data)
        assert wait_for_message(p) == self.make_message(
            "message", self.channel, self.data
        )

    @pytest.mark.onlynoncluster
    def test_pattern_publish(self, r):
        p = r.pubsub()
        p.psubscribe(self.pattern)
        assert wait_for_message(p) == self.make_message("psubscribe", self.pattern, 1)
        r.publish(self.channel, self.data)
        assert wait_for_message(p) == self.make_message(
            "pmessage", self.channel, self.data, pattern=self.pattern
        )

    @skip_if_server_version_lt("7.0.0")
    def test_shard_channel_publish(self, r):
        p = r.pubsub()
        p.ssubscribe(self.channel)
        assert wait_for_message(p, func=p.get_sharded_message) == self.make_message(
            "ssubscribe", self.channel, 1
        )
        r.spublish(self.channel, self.data)
        assert wait_for_message(p, func=p.get_sharded_message) == self.make_message(
            "smessage", self.channel, self.data
        )

    def test_channel_message_handler(self, r):
        p = r.pubsub(ignore_subscribe_messages=True)
        p.subscribe(**{self.channel: self.message_handler})
        assert p.get_message(timeout=1) is None
        r.publish(self.channel, self.data)
        wait_for_handled_message(
            self, p.get_message, self.make_message("message", self.channel, self.data)
        )

        # test that we reconnected to the correct channel
        self.message = None
        p.connection.disconnect()
        assert p.get_message(timeout=1) is None  # should reconnect
        new_data = self.data + "new data"
        r.publish(self.channel, new_data)
        wait_for_handled_message(
            self, p.get_message, self.make_message("message", self.channel, new_data)
        )

    def test_pattern_message_handler(self, r):
        p = r.pubsub(ignore_subscribe_messages=True)
        p.psubscribe(**{self.pattern: self.message_handler})
        assert p.get_message(timeout=1) is None
        r.publish(self.channel, self.data)
        wait_for_handled_message(
            self,
            p.get_message,
            self.make_message(
                "pmessage", self.channel, self.data, pattern=self.pattern
            ),
        )

        # test that we reconnected to the correct pattern
        self.message = None
        p.connection.disconnect()
        assert p.get_message(timeout=1) is None  # should reconnect
        new_data = self.data + "new data"
        r.publish(self.channel, new_data)
        wait_for_handled_message(
            self,
            p.get_message,
            self.make_message("pmessage", self.channel, new_data, pattern=self.pattern),
        )

    @skip_if_server_version_lt("7.0.0")
    def test_shard_channel_message_handler(self, r):
        p = r.pubsub(ignore_subscribe_messages=True)
        p.ssubscribe(**{self.channel: self.message_handler})
        assert p.get_sharded_message(timeout=1) is None
        r.spublish(self.channel, self.data)
        wait_for_handled_message(
            self,
            p.get_sharded_message,
            self.make_message("smessage", self.channel, self.data),
        )

        # test that we reconnected to the correct channel
        self.message = None
        try:
            # cluster mode
            p.disconnect()
        except AttributeError:
            # standalone mode
            p.connection.disconnect()
        # should reconnect
        assert p.get_sharded_message(timeout=1) is None
        new_data = self.data + "new data"
        r.spublish(self.channel, new_data)
        wait_for_handled_message(
            self,
            p.get_sharded_message,
            self.make_message("smessage", self.channel, new_data),
        )

    def test_context_manager(self, r):
        with r.pubsub() as pubsub:
            pubsub.subscribe("foo")
            assert pubsub.connection is not None

        assert pubsub.connection is None
        assert pubsub.channels == {}
        assert pubsub.patterns == {}


class TestPubSubRedisDown:
    def test_channel_subscribe(self):
        # nothing listens on this port, so skip the default backoff retries
        r = redis.Redis(host="localhost", port=6390, retry=Retry(NoBackoff(), 0))
        p = r.pubsub()
        with pytest.raises(ConnectionError):
            p.subscribe("foo")


class TestPubSubSubcommands:
    @pytest.mark.onlynoncluster
    @skip_if_server_version_lt("2.8.0")
    def test_pubsub_channels(self, r):
        p = r.pubsub()
        p.subscribe("foo", "bar", "baz", "quux")
        for i in range(4):
            assert wait_for_message(p)["type"] == "subscribe"
        expected = [b"bar", b"baz", b"foo", b"quux"]
        assert all([channel in r.pubsub_channels() for channel in expected])

    @pytest.mark.onlynoncluster
    @skip_if_server_version_lt("7.0.0")
    def test_pubsub_shardchannels(self, r):
        p = r.pubsub()
        p.ssubscribe("foo", "bar", "baz", "quux")
        for i in range(4):
            assert wait_for_message(p)["type"] == "ssubscribe"
        expected = [b"bar", b"baz", b"foo", b"quux"]
        assert all([channel in r.pubsub_shardchannels() for channel in expected])

    @pytest.mark.onlycluster
    @skip_if_server_version_lt("7.0.0")
    def test_pubsub_shardchannels_cluster(self, r):
        channels = {
            b"foo": r.get_node_from_key("foo"),
            b"bar": r.get_node_from_key("bar"),
            b"baz": r.get_node_from_key("baz"),
            b"quux": r.get_node_from_key("quux"),
        }
        p = r.pubsub()
        p.ssubscribe("foo", "bar", "baz", "quux")
        for node in channels.values():
            assert wait_for_message(p, node=node)["type"] == "ssubscribe"
        for channel, node in channels.items():
            assert channel in r.pubsub_shardchannels(target_nodes=node)
        assert all(
            [
                channel in r.pubsub_shardchannels(target_nodes="all")
                for channel in channels.keys()
            ]
        )

    @pytest.mark.onlynoncluster
    @skip_if_server_version_lt("2.8.0")
    def test_pubsub_numsub(self, r):
        p1 = r.pubsub()
        p1.subscribe("foo", "bar", "baz")
        for i in range(3):
            assert wait_for_message(p1)["type"] == "subscribe"
        p2 = r.pubsub()
        p2.subscribe("bar", "baz")
        for i in range(2):
            assert wait_for_message(p2)["type"] == "subscribe"
        p3 = r.pubsub()
        p3.subscribe("baz")
        assert wait_for_message(p3)["type"] == "subscribe"

        channels = [(b"foo", 1), (b"bar", 2), (b"baz", 3)]
        assert r.pubsub_numsub("foo", "bar", "baz") == channels

    @skip_if_server_version_lt("2.8.0")
    def test_pubsub_numpat(self, r):
        p = r.pubsub()
        p.psubscribe("*oo", "*ar", "b*z")
        for i in range(3):
            assert wait_for_message(p)["type"] == "psubscribe"
        assert r.pubsub_numpat() == 3

    @pytest.mark.onlycluster
    @skip_if_server_version_lt("7.0.0")
    def test_pubsub_shardnumsub(self, r):
        channels = {
            b"foo": r.get_node_from_key("foo"),
            b"bar": r.get_node_from_key("bar"),
            b"baz": r.get_node_from_key("baz"),
        }
        p1 = r.pubsub()
        p1.ssubscribe(*channels.keys())
        for node in channels.values():
            assert wait_for_message(p1, node=node)["type"] == "ssubscribe"
        p2 = r.pubsub()
        p2.ssubscribe("bar", "baz")
        for i in range(2):
            assert (
                wait_for_message(p2, func=p2.get_sharded_message)["type"]
                == "ssubscribe"
            )
        p3 = r.pubsub()
        p3.ssubscribe("baz")
        assert wait_for_message(p3, node=channels[b"baz"])["type"] == "ssubscribe"

        channels = [(b"foo", 1), (b"bar", 2), (b"baz", 3)]
        assert r.pubsub_shardnumsub("foo", "bar", "baz", target_nodes="all") == channels

    @pytest.mark.onlycluster
    @skip_if_server_version_lt("7.0.0")
    def test_ssubscribe_multiple_channels_different_nodes(self, r):
        """
        Test subscribing to multiple sharded channels on different nodes.
        Validates that the generator properly handles multiple node_pubsub_mapping entries.
        """
        pubsub = r.pubsub()
        channel1 = "test-channel:{0}"
        channel2 = "test-channel:{6}"

        # Subscribe to first channel
        pubsub.ssubscribe(channel1)
        msg = wait_for_message(pubsub, timeout=1.0, func=pubsub.get_sharded_message)
        assert msg is not None
        assert msg["type"] == "ssubscribe"

        # Subscribe to second channel (likely different node)
        pubsub.ssubscribe(channel2)
        msg = wait_for_message(pubsub, timeout=1.0, func=pubsub.get_sharded_message)
        assert msg is not None
        assert msg["type"] == "ssubscribe"

        # Verify both channels are in shard_channels
        assert channel1.encode() in pubsub.shard_channels
        assert channel2.encode() in pubsub.shard_channels

        pubsub.close()

    @pytest.mark.onlycluster
    @skip_if_server_version_lt("7.0.0")
    def test_ssubscribe_multiple_channels_publish_and_read(self, r):
        """
        Test publishing to multiple sharded channels and reading messages.
        Validates that _sharded_message_generator properly cycles through
        multiple node_pubsub_mapping entries.
        """
        pubsub = r.pubsub()
        channel1 = "test-channel:{0}"
        channel2 = "test-channel:{6}"
        msg1_data = "message-1"
        msg2_data = "message-2"

        # Subscribe to both channels
        pubsub.ssubscribe(channel1, channel2)

        # Read subscription confirmations
        for _ in range(2):
            msg = wait_for_message(pubsub, timeout=1.0, func=pubsub.get_sharded_message)
            assert msg is not None
            assert msg["type"] == "ssubscribe"

        # Publish messages to both channels
        r.spublish(channel1, msg1_data)
        r.spublish(channel2, msg2_data)

        # Read messages - should get both messages
        messages = []
        for _ in range(2):
            msg = wait_for_message(pubsub, timeout=1.0, func=pubsub.get_sharded_message)
            assert msg is not None
            assert msg["type"] == "smessage"
            messages.append(msg)

        # Verify we got messages from both channels
        channels_received = {msg["channel"] for msg in messages}
        assert channel1.encode() in channels_received
        assert channel2.encode() in channels_received

        pubsub.close()

    @pytest.mark.onlycluster
    @skip_if_server_version_lt("7.0.0")
    def test_generator_handles_concurrent_mapping_changes(self, r):
        """
        Test that the generator properly handles mapping changes during iteration.
        This validates the fix for the RuntimeError: dictionary changed size during iteration.
        """
        pubsub = r.pubsub()
        channel1 = "test-channel:{0}"
        channel2 = "test-channel:{6}"

        # Subscribe to first channel
        pubsub.ssubscribe(channel1)
        msg = wait_for_message(pubsub, timeout=1.0, func=pubsub.get_sharded_message)
        assert msg is not None
        assert msg["type"] == "ssubscribe"

        # Get initial mapping size (cluster pubsub only)
        assert hasattr(pubsub, "node_pubsub_mapping"), "Test requires ClusterPubSub"
        initial_size = len(pubsub.node_pubsub_mapping)

        # Subscribe to second channel (modifies mapping during potential iteration)
        pubsub.ssubscribe(channel2)
        msg = wait_for_message(pubsub, timeout=1.0, func=pubsub.get_sharded_message)
        assert msg is not None
        assert msg["type"] == "ssubscribe"

        # Verify mapping was updated
        assert len(pubsub.node_pubsub_mapping) >= initial_size

        # Publish and read messages - should not raise RuntimeError
        r.spublish(channel1, "msg1")
        r.spublish(channel2, "msg2")

        messages_received = 0
        for _ in range(2):
            msg = wait_for_message(pubsub, timeout=1.0, func=pubsub.get_sharded_message)
            if msg and msg["type"] == "smessage":
                messages_received += 1

        assert messages_received == 2
        pubsub.close()


class TestPubSubPings:
    @pytest.mark.fixed_client
    @pytest.mark.onlynoncluster
    @skip_if_server_version_lt("6.0.0")
    @pytest.mark.parametrize("protocol", [2, 3])
    @pytest.mark.parametrize("decode_responses", [False, True])
    @pytest.mark.parametrize("message", [None, "x", "hello\u2603"])
    @pytest.mark.parametrize("read_method", ["get_message", "listen"])
    def test_ping_response_shapes(
        self, request, protocol, decode_responses, message, read_method
    ):
        client = _get_client(
            redis.Redis, request, protocol=protocol, decode_responses=decode_responses
        )
        with client.pubsub() as pubsub:
            pubsub.subscribe("foo")
            assert pubsub.get_message(timeout=1)["type"] == "subscribe"
            pubsub.ping(message)

            if read_method == "get_message":
                response = pubsub.get_message(timeout=1)
            else:
                messages = pubsub.listen()
                try:
                    response = next(messages)
                finally:
                    messages.close()

            data = message if message is not None else ""
            assert response == {
                "type": "pong",
                "pattern": None,
                "channel": None,
                "data": data if decode_responses else data.encode(),
            }

    @skip_if_server_version_lt("3.0.0")
    def test_send_pubsub_ping(self, r):
        p = r.pubsub(ignore_subscribe_messages=True)
        p.subscribe("foo")
        p.ping()
        assert wait_for_message(p) == make_message(
            type="pong", channel=None, data="", pattern=None
        )

    @skip_if_server_version_lt("3.0.0")
    def test_send_pubsub_ping_message(self, r):
        p = r.pubsub(ignore_subscribe_messages=True)
        p.subscribe("foo")
        p.ping(message="hello world")
        assert wait_for_message(p) == make_message(
            type="pong", channel=None, data="hello world", pattern=None
        )


@pytest.mark.onlynoncluster
class TestPubSubHealthCheckResponse:
    """Tests for health check response validation with different decode_responses settings"""

    def test_is_health_check_response_decode_false_list_format(self, r):
        """Test is_health_check_response recognizes list format with decode_responses=False"""
        p = r.pubsub()
        # List format: [b"pong", b"redis-py-health-check"]
        assert p.is_health_check_response([b"pong", b"redis-py-health-check"])

    def test_is_health_check_response_decode_false_bytes_format(self, r):
        """Test is_health_check_response recognizes bytes format with decode_responses=False"""
        p = r.pubsub()
        # Bytes format: b"redis-py-health-check"
        assert p.is_health_check_response(b"redis-py-health-check")

    def test_is_health_check_response_decode_false_rejects_string(self, r):
        """Test is_health_check_response rejects string format with decode_responses=False"""
        p = r.pubsub()
        # String format should NOT be recognized when decode_responses=False
        assert not p.is_health_check_response("redis-py-health-check")

    def test_is_health_check_response_decode_true_list_format(self, request):
        """Test is_health_check_response recognizes list format with decode_responses=True"""
        r = _get_client(redis.Redis, request, decode_responses=True)
        p = r.pubsub()
        # List format: ["pong", "redis-py-health-check"]
        assert p.is_health_check_response(["pong", "redis-py-health-check"])

    def test_is_health_check_response_decode_true_string_format(self, request):
        """Test is_health_check_response recognizes string format with decode_responses=True"""
        r = _get_client(redis.Redis, request, decode_responses=True)
        p = r.pubsub()
        # String format: "redis-py-health-check" (THE FIX!)
        assert p.is_health_check_response("redis-py-health-check")

    def test_is_health_check_response_decode_true_rejects_bytes(self, request):
        """Test is_health_check_response rejects bytes format with decode_responses=True"""
        r = _get_client(redis.Redis, request, decode_responses=True)
        p = r.pubsub()
        # Bytes format should NOT be recognized when decode_responses=True
        assert not p.is_health_check_response(b"redis-py-health-check")

    def test_is_health_check_response_decode_true_rejects_invalid(self, request):
        """Test is_health_check_response rejects invalid responses with decode_responses=True"""
        r = _get_client(redis.Redis, request, decode_responses=True)
        p = r.pubsub()
        # Invalid responses should be rejected
        assert not p.is_health_check_response("invalid-response")
        assert not p.is_health_check_response(["pong", "invalid-response"])
        assert not p.is_health_check_response(None)

    def test_is_health_check_response_decode_false_rejects_invalid(self, r):
        """Test is_health_check_response rejects invalid responses with decode_responses=False"""
        p = r.pubsub()
        # Invalid responses should be rejected
        assert not p.is_health_check_response(b"invalid-response")
        assert not p.is_health_check_response([b"pong", b"invalid-response"])
        assert not p.is_health_check_response(None)


@pytest.mark.onlynoncluster
class TestPubSubConnectionKilled:
    @skip_if_server_version_lt("3.0.0")
    @skip_if_redis_enterprise()
    def test_connection_error_raised_when_connection_dies(self, r):
        p = r.pubsub()
        p.subscribe("foo")
        assert wait_for_message(p) == make_message("subscribe", "foo", 1)
        for client in r.client_list():
            if client["cmd"] == "subscribe":
                r.client_kill_filter(_id=client["id"])
        with pytest.raises(ConnectionError):
            wait_for_message(p)


class TestPubSubTimeouts:
    def test_get_message_with_timeout_returns_none(self, r):
        p = r.pubsub()
        p.subscribe("foo")
        assert wait_for_message(p) == make_message("subscribe", "foo", 1)
        assert p.get_message(timeout=0.01) is None

    def test_get_message_not_subscribed_return_none(self, r):
        p = r.pubsub()
        assert p.subscribed is False
        assert p.get_message() is None
        assert p.get_message(timeout=0.1) is None
        with patch.object(threading.Event, "wait") as mock:
            mock.return_value = False
            assert p.get_message(timeout=0.01) is None
            assert mock.called

    def test_get_message_subscribe_during_waiting(self, r):
        p = r.pubsub()
        checked_unsubscribed = threading.Event()
        waiting_for_subscription = threading.Event()
        errors = []

        # signal from inside get_message's wait, so subscribe() lands in it
        wait_for_subscription = p.subscribed_event.wait

        def signalling_wait(timeout=None):
            if timeout != 0:  # the first, zero-timeout poll does not wait
                waiting_for_subscription.set()
            return wait_for_subscription(timeout)

        p.subscribed_event.wait = signalling_wait

        def poll(ps, expected_res):
            try:
                assert ps.get_message() is None
                checked_unsubscribed.set()
                message = ps.get_message(timeout=5)
                assert message == expected_res
            except BaseException as e:
                errors.append(e)
                checked_unsubscribed.set()
                waiting_for_subscription.set()

        subscribe_response = make_message("subscribe", "foo", 1)
        poller = threading.Thread(target=poll, args=(p, subscribe_response))
        poller.start()
        # Subscribe only once the poller is waiting for a subscription.
        assert checked_unsubscribed.wait(timeout=5)
        assert waiting_for_subscription.wait(timeout=5)
        p.subscribe("foo")
        poller.join(timeout=10)
        assert not poller.is_alive()
        # an assertion failure in the thread would otherwise only be a warning
        if errors:
            raise errors[0]

    def test_get_message_timeout_none_survives_a_late_subscription(self):
        """
        Regression: the subscription-wait prelude charged the time spent
        waiting against ``timeout``, which raises TypeError when
        ``timeout=None`` means "block indefinitely".
        """
        p = PubSub(mock.MagicMock(), encoder=Encoder("utf-8", "strict", False))
        p.parse_response = mock.MagicMock(return_value=None)
        assert p.subscribed is False

        def _late_subscribe():
            time.sleep(0.05)
            p.subscribed_event.set()

        subscriber = threading.Thread(target=_late_subscribe, daemon=True)
        subscriber.start()
        try:
            assert p.get_message(timeout=None) is None
        finally:
            subscriber.join(timeout=5.0)
        p.parse_response.assert_called_once_with(block=True, timeout=None)

    def test_get_message_wait_for_subscription_not_being_called(self, r):
        p = r.pubsub()
        p.subscribe("foo")
        assert p.subscribed is True

        # Ensure p has the event attribute your wait_for_message would call:
        ev = getattr(p, "subscribed_event", None)

        assert ev is not None, (
            "PubSub event attribute not found (check redis-py version)"
        )

        with patch.object(ev, "wait") as mock:
            assert wait_for_message(p) == make_message("subscribe", "foo", 1)
            assert mock.called is False


class TestPubSubWorkerThread:
    @pytest.mark.skipif(
        platform.python_implementation() == "PyPy", reason="Pypy threading issue"
    )
    def test_pubsub_worker_thread_exception_handler(self, r):
        event = threading.Event()

        def exception_handler(ex, pubsub, thread):
            thread.stop()
            event.set()

        p = r.pubsub()
        p.subscribe(**{"foo": lambda m: m})
        pubsub_thread = None
        try:
            with mock.patch.object(p, "get_message", side_effect=Exception("error")):
                pubsub_thread = p.run_in_thread(
                    daemon=True, exception_handler=exception_handler
                )

                assert event.wait(timeout=1.0)
        finally:
            if pubsub_thread is not None:
                pubsub_thread.stop()
                pubsub_thread.join(timeout=1.0)
        assert not pubsub_thread.is_alive()

    @pytest.mark.parametrize("sharded_pubsub", [False, True])
    @pytest.mark.parametrize("timeout", [None, 0.01])
    def test_stop_before_run_does_not_restart_worker(self, sharded_pubsub, timeout):
        entered = threading.Event()
        release = threading.Event()
        stopping = threading.Event()
        finished = threading.Event()
        pubsub = mock.Mock()
        thread = redis.client.PubSubWorkerThread(
            pubsub, sleep_time=0.01, sharded_pubsub=sharded_pubsub
        )
        original_run = thread.run
        original_join = thread.join

        def delayed_run():
            entered.set()
            release.wait(timeout=2.0)
            original_run()

        def observed_join(timeout=None):
            stopping.set()
            original_join(timeout)

        def call_stop():
            thread.stop(timeout=timeout)
            finished.set()

        stopper = threading.Thread(target=call_stop)
        with (
            mock.patch.object(thread, "run", side_effect=delayed_run),
            mock.patch.object(thread, "join", side_effect=observed_join),
        ):
            thread.start()
            try:
                assert entered.wait(timeout=1.0)
                stopper.start()
                # stop() has cleared the flag and started waiting before run().
                assert stopping.wait(timeout=1.0)
                if timeout is None:
                    assert not finished.wait(timeout=0.05)
                else:
                    assert finished.wait(timeout=1.0)
                release.set()
                assert finished.wait(timeout=1.0)
                original_join(timeout=1.0)
                assert not thread.is_alive()
                pubsub.get_message.assert_not_called()
                pubsub.get_sharded_message.assert_not_called()
                pubsub.close.assert_called_once_with()
            finally:
                # Also shut down cleanly when the old code loses the stop request.
                def stop_polling(**kwargs):
                    thread._running.clear()

                pubsub.get_message.side_effect = stop_polling
                pubsub.get_sharded_message.side_effect = stop_polling
                release.set()
                thread._running.clear()
                original_join(timeout=2.0)
                if stopper.ident is not None:
                    stopper.join(timeout=2.0)

    def test_stop_waits_for_worker_before_returning(self):
        entered = threading.Event()
        release = threading.Event()
        closed = []

        class FakePubSub:
            def get_message(self, ignore_subscribe_messages=True, timeout=0.0):
                entered.set()
                release.wait(timeout=2)
                return None

            def close(self):
                closed.append(threading.current_thread())

        thread = redis.client.PubSubWorkerThread(FakePubSub(), sleep_time=0.01)
        thread.start()
        assert entered.wait(timeout=1.0)
        finished = threading.Event()

        def call_stop():
            thread.stop()
            finished.set()

        stopper = threading.Thread(target=call_stop)
        stopper.start()
        assert finished.wait(timeout=0.2) is False
        release.set()
        assert finished.wait(timeout=2.0)
        stopper.join(timeout=1.0)
        assert not thread.is_alive()
        assert closed == [thread]

    def test_stop_from_worker_does_not_deadlock(self):
        entered = threading.Event()
        closed = []

        class FakePubSub:
            def get_message(self, ignore_subscribe_messages=True, timeout=0.0):
                entered.set()
                threading.current_thread().stop()
                return None

            def close(self):
                closed.append(threading.current_thread())

        thread = redis.client.PubSubWorkerThread(FakePubSub(), sleep_time=0.01)
        thread.start()
        assert entered.wait(timeout=1.0)
        thread.join(timeout=2.0)
        assert not thread.is_alive()
        assert closed == [thread]

    def test_stop_timeout_returns_while_worker_is_blocked(self):
        entered = threading.Event()
        release = threading.Event()

        class FakePubSub:
            def get_message(self, ignore_subscribe_messages=True, timeout=0.0):
                entered.set()
                release.wait(timeout=2)
                return None

            def close(self):
                return None

        thread = redis.client.PubSubWorkerThread(FakePubSub(), sleep_time=0.01)
        thread.start()
        assert entered.wait(timeout=1.0)
        started = time.monotonic()
        thread.stop(timeout=0.2)
        elapsed = time.monotonic() - started
        assert thread.is_alive()
        assert elapsed < 1.0
        release.set()
        thread.join(timeout=2.0)
        assert not thread.is_alive()


class TestPubSubDeadlock:
    @pytest.mark.timeout(30, method="thread")
    def test_pubsub_deadlock(self, master_host):
        pool = redis.ConnectionPool(host=master_host[0], port=master_host[1])
        r = redis.Redis(connection_pool=pool)

        for i in range(60):
            p = r.pubsub()
            p.subscribe("my-channel-1", "my-channel-2")
            pool.reset()


@pytest.mark.timeout(5, method="thread")
@pytest.mark.parametrize("method", ["get_message", "listen"])
@pytest.mark.onlynoncluster
class TestPubSubAutoReconnect:
    def mysetup(self, r, method):
        self.messages = queue.Queue()
        self.pubsub = r.pubsub()
        self.state = 0
        self.cond = threading.Condition()
        if method == "get_message":
            self.get_message = self.loop_step_get_message
        else:
            self.get_message = self.loop_step_listen

        self.thread = threading.Thread(target=self.loop)
        self.thread.daemon = True
        self.thread.start()
        # get the initial connect message
        message = self.messages.get(timeout=1)
        assert message == {
            "channel": b"foo",
            "data": 1,
            "pattern": None,
            "type": "subscribe",
        }

    def wait_for_reconnect(self):
        self.cond.wait_for(lambda: self.pubsub.connection._sock is not None, timeout=2)
        assert self.pubsub.connection._sock is not None  # we didn't time out
        assert self.state == 3

        message = self.messages.get(timeout=1)
        assert message == {
            "channel": b"foo",
            "data": 1,
            "pattern": None,
            "type": "subscribe",
        }

    def mycleanup(self):
        # kill thread
        with self.cond:
            self.state = 4  # quit
            self.cond.notify()
        self.thread.join()

    def test_reconnect_socket_error(self, r: redis.Redis, method):
        """
        Test that a socket error will cause reconnect
        """
        self.mysetup(r, method)
        try:
            # now, disconnect the connection, and wait for it to be re-established
            with self.cond:
                self.state = 1
                with mock.patch.object(self.pubsub.connection, "_parser") as mockobj:
                    mockobj.read_response.side_effect = socket.error
                    mockobj.can_read.side_effect = socket.error
                    # wait until thread notices the disconnect until we undo the patch
                    self.cond.wait_for(lambda: self.state >= 2)
                    assert (
                        self.pubsub.connection._sock is None
                    )  # it is in a disconnected state
                self.wait_for_reconnect()

        finally:
            self.mycleanup()

    def test_reconnect_disconnect(self, r: redis.Redis, method):
        """
        Test that a manual disconnect() will cause reconnect
        """
        self.mysetup(r, method)
        try:
            # now, disconnect the connection, and wait for it to be re-established
            with self.cond:
                self.state = 1
                self.pubsub.connection.disconnect()
                assert self.pubsub.connection._sock is None
                # wait for reconnect
                self.wait_for_reconnect()
        finally:
            self.mycleanup()

    def loop(self):
        # reader loop, performing state transitions as it
        # discovers disconnects and reconnects
        self.pubsub.subscribe("foo")
        while True:
            time.sleep(0.01)  # give main thread chance to get lock
            with self.cond:
                old_state = self.state
                try:
                    if self.state == 4:
                        break
                    # print ('state, %s, sock %s' % (state, pubsub.connection._sock))
                    got_msg = self.get_message()
                    assert got_msg
                    if self.state in (1, 2):
                        self.state = 3  # successful reconnect
                except redis.ConnectionError:
                    assert self.state in (1, 2)
                    self.state = 2
                finally:
                    self.cond.notify()
                # assert that we noticed a connect error, or automatically
                # reconnected without error
                if old_state == 1:
                    assert self.state in (2, 3)

    def loop_step_get_message(self):
        # get a single message via listen()
        message = self.pubsub.get_message(timeout=0.1)
        if message is not None:
            self.messages.put(message)
            return True
        return False

    def loop_step_listen(self):
        # get a single message via listen()
        for message in self.pubsub.listen():
            self.messages.put(message)
            return True


@pytest.mark.onlynoncluster
class TestBaseException:
    def test_base_exception(self, r: redis.Redis):
        """
        Manually trigger a BaseException inside the parser's .read_response method
        and verify that it isn't caught
        """
        pubsub = r.pubsub()
        pubsub.subscribe("foo")

        def is_connected():
            return pubsub.connection._sock is not None

        assert is_connected()

        def get_msg():
            # blocking method to return messages
            while True:
                response = pubsub.parse_response(block=True)
                message = pubsub.handle_message(
                    response, ignore_subscribe_messages=False
                )
                if message is not None:
                    return message

        # get subscribe message
        msg = get_msg()
        assert msg is not None
        # timeout waiting for another message which never arrives
        assert is_connected()
        with (
            patch("redis._parsers._RESP2Parser.read_response") as mock1,
            patch("redis._parsers._HiredisParser.read_response") as mock2,
            patch("redis._parsers._RESP3Parser.read_response") as mock3,
        ):
            mock1.side_effect = BaseException("boom")
            mock2.side_effect = BaseException("boom")
            mock3.side_effect = BaseException("boom")

            with pytest.raises(BaseException):
                get_msg()

        # the timeout on the read should not cause disconnect
        assert is_connected()


@pytest.mark.fixed_client
class TestPubSubMetricsRecording:
    """
    Unit tests that verify metrics are properly recorded from PubSub operations
    through the direct record_* function calls.

    These tests use fully mocked connection and connection pool - no real Redis
    or OTel integration is used.
    """

    @pytest.fixture
    def mock_connection(self):
        """Create a mock connection with required attributes."""
        conn = mock.MagicMock()
        conn.host = "localhost"
        conn.port = 6379
        conn.db = 0
        conn.should_reconnect.return_value = False

        # Mock retry to just execute the function directly
        def mock_call_with_retry(do, fail, is_retryable=None, with_failure_count=False):
            return do()

        conn.retry.call_with_retry = mock_call_with_retry
        conn.retry.get_retries.return_value = 0

        return conn

    @pytest.fixture
    def mock_connection_pool(self, mock_connection):
        """Create a mock connection pool."""
        pool = mock.MagicMock()
        pool.get_connection.return_value = mock_connection
        pool.get_encoder.return_value = mock.MagicMock()
        return pool

    @pytest.fixture
    def mock_meter(self):
        """Create a mock Meter that tracks all instrument calls."""
        meter = mock.MagicMock()

        # Create mock histogram for operation duration
        self.operation_duration = mock.MagicMock()
        # Create mock counter for client errors
        self.client_errors = mock.MagicMock()

        def create_histogram_side_effect(name, **kwargs):
            if name == "db.client.operation.duration":
                return self.operation_duration
            return mock.MagicMock()

        def create_counter_side_effect(name, **kwargs):
            if name == "redis.client.errors":
                return self.client_errors
            return mock.MagicMock()

        meter.create_counter.side_effect = create_counter_side_effect
        meter.create_up_down_counter.return_value = mock.MagicMock()
        meter.create_histogram.side_effect = create_histogram_side_effect

        return meter

    @pytest.fixture
    def setup_pubsub_with_otel(self, mock_connection_pool, mock_connection, mock_meter):
        """
        Setup a PubSub with mocked connection and OTel collector.
        Returns tuple of (pubsub, operation_duration_mock).
        """
        from redis.client import PubSub
        from redis.event import EventDispatcher
        from redis.observability import recorder
        from redis.observability.config import OTelConfig, MetricGroup
        from redis.observability.metrics import RedisMetricsCollector

        # Reset any existing collector state
        recorder.reset_collector()

        # Create config with COMMAND group enabled
        config = OTelConfig(metric_groups=[MetricGroup.COMMAND])

        # Create collector with mocked meter
        with mock.patch("redis.observability.metrics.OTEL_AVAILABLE", True):
            collector = RedisMetricsCollector(mock_meter, config)

        # Patch the recorder to use our collector
        with mock.patch.object(recorder, "_metrics_collector", collector):
            # Create event dispatcher (real one, to test the full chain)
            event_dispatcher = EventDispatcher()

            # Create PubSub with mocked connection pool
            pubsub = PubSub(
                connection_pool=mock_connection_pool,
                event_dispatcher=event_dispatcher,
            )

            # Set the connection directly to avoid subscribe flow
            pubsub.connection = mock_connection

            yield pubsub, self.operation_duration

        # Cleanup
        recorder.reset_collector()

    def test_pubsub_execute_records_metric(self, setup_pubsub_with_otel):
        """
        Test that executing a PubSub command records operation duration metric
        which is delivered to the Meter's histogram.record() method.
        """

        pubsub, operation_duration_mock = setup_pubsub_with_otel

        # Mock the command to return successfully
        mock_command = mock.MagicMock(return_value=True)

        # Execute a command through _execute
        pubsub._execute(pubsub.connection, mock_command, "SUBSCRIBE", "foo")

        # Verify the Meter's histogram.record() was called
        operation_duration_mock.record.assert_called_once()

        # Get the call arguments
        call_args = operation_duration_mock.record.call_args

        # Verify duration was recorded (first positional arg)
        duration = call_args[0][0]
        assert isinstance(duration, float)
        assert duration >= 0

        # Verify attributes
        attrs = call_args[1]["attributes"]
        assert attrs["db.operation.name"] == "SUBSCRIBE"
        assert attrs["server.address"] == "localhost"
        assert attrs["server.port"] == 6379
        assert attrs["db.namespace"] == "0"

    def test_pubsub_error_records_error_count(
        self, mock_connection_pool, mock_connection, mock_meter
    ):
        """
        Test that when a PubSub command raises an exception,
        error count is recorded via record_error_count.

        Note: record_operation_duration is NOT called for final errors -
        only record_error_count is called. record_operation_duration is
        only called during retries (in _close_connection) and on success.
        """

        recorder.reset_collector()
        # Enable RESILIENCY metric group for error counting
        config = OTelConfig(metric_groups=[MetricGroup.COMMAND, MetricGroup.RESILIENCY])

        with mock.patch("redis.observability.metrics.OTEL_AVAILABLE", True):
            collector = RedisMetricsCollector(mock_meter, config)

        with mock.patch.object(recorder, "_metrics_collector", collector):
            event_dispatcher = EventDispatcher()

            pubsub = PubSub(
                connection_pool=mock_connection_pool,
                event_dispatcher=event_dispatcher,
            )
            pubsub.connection = mock_connection

            # Make command raise an exception
            test_error = redis.ConnectionError("Connection failed")
            mock_command = mock.MagicMock(side_effect=test_error)

            # Execute should raise the error
            with pytest.raises(redis.ConnectionError):
                pubsub._execute(pubsub.connection, mock_command, "SUBSCRIBE", "foo")

            # Verify record_error_count was called (via client_errors counter)
            self.client_errors.add.assert_called_once()

            # Verify error type is recorded in attributes
            call_args = self.client_errors.add.call_args
            attrs = call_args[1]["attributes"]
            assert "error.type" in attrs

            # Verify operation_duration was NOT called (no retries, direct failure)
            self.operation_duration.record.assert_not_called()

        recorder.reset_collector()

    def test_pubsub_server_attributes_recorded(self, setup_pubsub_with_otel):
        """
        Test that server address, port, and db namespace are correctly recorded.
        """
        pubsub, operation_duration_mock = setup_pubsub_with_otel

        mock_command = mock.MagicMock(return_value=True)
        pubsub._execute(pubsub.connection, mock_command, "PING")

        call_args = operation_duration_mock.record.call_args
        attrs = call_args[1]["attributes"]

        # Verify server attributes match mock connection
        assert attrs["server.address"] == "localhost"
        assert attrs["server.port"] == 6379
        assert attrs["db.namespace"] == "0"

    def test_pubsub_retry_records_metric_on_each_attempt(
        self, mock_connection_pool, mock_meter
    ):
        """
        Test that when a PubSub command is retried, operation duration metric
        is recorded for each retry attempt with retry_attempts attribute.
        """

        # Create connection with retry behavior
        mock_connection = mock.MagicMock()
        mock_connection.host = "localhost"
        mock_connection.port = 6379
        mock_connection.db = 0
        mock_connection.should_reconnect.return_value = False

        max_retries = 2

        def call_with_retry_impl(
            func, error_handler, is_retryable=None, with_failure_count=False
        ):
            """Simulate retry behavior - fail twice, then succeed."""
            for attempt in range(max_retries + 1):
                try:
                    return func()
                except redis.ConnectionError as e:
                    if attempt < max_retries:
                        if with_failure_count:
                            error_handler(e, attempt + 1)
                        else:
                            error_handler(e)
                    else:
                        raise

        mock_connection.retry.call_with_retry = call_with_retry_impl
        mock_connection.retry.get_retries.return_value = max_retries

        mock_connection_pool.get_connection.return_value = mock_connection

        recorder.reset_collector()
        config = OTelConfig(metric_groups=[MetricGroup.COMMAND])

        with mock.patch("redis.observability.metrics.OTEL_AVAILABLE", True):
            collector = RedisMetricsCollector(mock_meter, config)

        with mock.patch.object(recorder, "_metrics_collector", collector):
            event_dispatcher = EventDispatcher()

            pubsub = PubSub(
                connection_pool=mock_connection_pool,
                event_dispatcher=event_dispatcher,
            )
            pubsub.connection = mock_connection

            # Make command fail twice then succeed
            call_count = [0]

            def command_impl(*args, **kwargs):
                call_count[0] += 1
                if call_count[0] <= 2:
                    raise redis.ConnectionError("Connection failed")
                return True

            mock_command = mock.MagicMock(side_effect=command_impl)

            # Execute command - should retry twice then succeed
            pubsub._execute(pubsub.connection, mock_command, "SUBSCRIBE", "foo")

            # Verify histogram.record() was called 3 times:
            # 2 retry attempts + 1 final success
            assert self.operation_duration.record.call_count == 3

            calls = self.operation_duration.record.call_args_list

            # First two calls should have error.type (retry attempts)
            assert "error.type" in calls[0][1]["attributes"]
            assert "error.type" in calls[1][1]["attributes"]

            # Last call should be success (no error.type)
            assert "error.type" not in calls[2][1]["attributes"]

        recorder.reset_collector()

    def test_pubsub_retry_exhausted_records_final_error_metric(
        self, mock_connection_pool, mock_meter
    ):
        """
        Test that when all retries are exhausted, operation duration metrics
        are recorded for each retry attempt, and error count is recorded for
        the final error.

        Note: record_operation_duration is called during retries (in _close_connection),
        but record_error_count is called for the final error (not record_operation_duration).
        """

        mock_connection = mock.MagicMock()
        mock_connection.host = "localhost"
        mock_connection.port = 6379
        mock_connection.db = 0
        mock_connection.should_reconnect.return_value = False

        max_retries = 2

        def call_with_retry_impl(
            func, error_handler, is_retryable=None, with_failure_count=False
        ):
            """Simulate retry behavior - always fail."""
            for attempt in range(max_retries + 1):
                try:
                    return func()
                except redis.ConnectionError as e:
                    if attempt < max_retries:
                        if with_failure_count:
                            error_handler(e, attempt + 1)
                        else:
                            error_handler(e)
                    else:
                        raise

        mock_connection.retry.call_with_retry = call_with_retry_impl
        mock_connection.retry.get_retries.return_value = max_retries

        mock_connection_pool.get_connection.return_value = mock_connection

        recorder.reset_collector()
        # Enable both COMMAND and RESILIENCY metric groups
        config = OTelConfig(metric_groups=[MetricGroup.COMMAND, MetricGroup.RESILIENCY])

        with mock.patch("redis.observability.metrics.OTEL_AVAILABLE", True):
            collector = RedisMetricsCollector(mock_meter, config)

        with mock.patch.object(recorder, "_metrics_collector", collector):
            event_dispatcher = EventDispatcher()

            pubsub = PubSub(
                connection_pool=mock_connection_pool,
                event_dispatcher=event_dispatcher,
            )
            pubsub.connection = mock_connection

            # Make command always fail
            mock_command = mock.MagicMock(
                side_effect=redis.ConnectionError("Connection failed")
            )

            # Execute command - should fail after all retries
            with pytest.raises(redis.ConnectionError):
                pubsub._execute(pubsub.connection, mock_command, "SUBSCRIBE", "foo")

            # Verify histogram.record() was called 2 times (for retry attempts only)
            # The final error uses record_error_count, not record_operation_duration
            assert self.operation_duration.record.call_count == 2

            calls = self.operation_duration.record.call_args_list

            # All retry calls should have error.type
            for call in calls:
                assert "error.type" in call[1]["attributes"]
                assert call[1]["attributes"]["db.operation.name"] == "SUBSCRIBE"

            # Verify record_error_count was called once for the final error
            self.client_errors.add.assert_called_once()

            # Verify error type is recorded in the final error
            error_call_args = self.client_errors.add.call_args
            error_attrs = error_call_args[1]["attributes"]
            assert "error.type" in error_attrs

        recorder.reset_collector()

    def test_pubsub_no_metric_when_no_command_name(self, setup_pubsub_with_otel):
        """
        Test that no metric is recorded when command_name is None.
        """
        pubsub, operation_duration_mock = setup_pubsub_with_otel

        mock_command = mock.MagicMock(return_value=True)

        # Execute without command name (no args)
        pubsub._execute(pubsub.connection, mock_command)

        # Verify no event was emitted
        operation_duration_mock.record.assert_not_called()

    def test_pubsub_different_commands_record_correct_names(
        self, mock_connection_pool, mock_connection, mock_meter
    ):
        """
        Test that different PubSub commands record metrics with correct command names.
        """

        recorder.reset_collector()
        config = OTelConfig(metric_groups=[MetricGroup.COMMAND])

        with mock.patch("redis.observability.metrics.OTEL_AVAILABLE", True):
            collector = RedisMetricsCollector(mock_meter, config)

        with mock.patch.object(recorder, "_metrics_collector", collector):
            event_dispatcher = EventDispatcher()

            pubsub = PubSub(
                connection_pool=mock_connection_pool,
                event_dispatcher=event_dispatcher,
            )
            pubsub.connection = mock_connection

            mock_command = mock.MagicMock(return_value=True)

            commands = [
                "SUBSCRIBE",
                "UNSUBSCRIBE",
                "PSUBSCRIBE",
                "PUNSUBSCRIBE",
                "PING",
            ]

            for cmd in commands:
                pubsub._execute(pubsub.connection, mock_command, cmd, "channel")

            # Verify all commands were recorded
            assert self.operation_duration.record.call_count == len(commands)

            calls = self.operation_duration.record.call_args_list
            recorded_commands = [
                call[1]["attributes"]["db.operation.name"] for call in calls
            ]

            assert recorded_commands == commands

        recorder.reset_collector()


class TestPubSubTimeoutPropagation:
    """
    Tests for timeout propagation through the entire pubsub read chain.
    Ensures that timeouts are properly passed from get_message() through
    parse_response() to the parser and socket buffer layers.
    """

    def test_get_message_timeout_is_respected(self, r):
        """
        Test that get_message() with timeout parameter respects the timeout
        and returns None when no message arrives within the timeout period.
        """
        p = r.pubsub()
        p.subscribe("foo")
        # Read subscription message
        msg = wait_for_message(p, timeout=1.0)
        assert msg is not None
        assert msg["type"] == "subscribe"

        # Call get_message with a short timeout - should return None
        # since no message is published
        start = time.monotonic()
        msg = p.get_message(timeout=0.1)
        elapsed = time.monotonic() - start
        assert msg is None
        # Verify timeout was actually respected (within reasonable bounds)
        assert elapsed < 1.0  # generous: a stalled runner must not fail it

    def test_get_message_timeout_with_published_message(self, r):
        """
        Test that get_message() with timeout returns a message if one
        arrives before the timeout expires.
        """
        p = r.pubsub()
        p.subscribe("foo")
        # Read subscription message
        msg = wait_for_message(p, timeout=1.0)
        assert msg is not None

        # Publish a message
        r.publish("foo", "hello")

        # get_message with timeout should return the message
        msg = p.get_message(timeout=1.0)
        assert msg is not None
        assert msg["type"] == "message"
        assert msg["data"] == b"hello"

    def test_parse_response_timeout_propagation(self, r):
        """
        Test that parse_response() properly propagates timeout to read_response().
        """
        p = r.pubsub()
        p.subscribe("foo")
        # Read subscription message
        msg = wait_for_message(p, timeout=1.0)
        assert msg is not None

        # Call parse_response with timeout - should respect it
        start = time.monotonic()
        response = p.parse_response(block=False, timeout=0.1)
        elapsed = time.monotonic() - start
        assert response is None
        assert elapsed < 1.0  # generous: a stalled runner must not fail it

    def test_get_message_timeout_zero_returns_immediately(self, r):
        """
        Test that get_message(timeout=0) returns immediately without blocking.
        """
        p = r.pubsub()
        p.subscribe("foo")
        # Read subscription message
        msg = wait_for_message(p, timeout=1.0)
        assert msg is not None

        # get_message with timeout=0 should return immediately
        start = time.monotonic()
        msg = p.get_message(timeout=0)
        elapsed = time.monotonic() - start
        assert msg is None
        assert elapsed < 0.5  # well below any blocking wait, above a runner stall

    def test_get_message_timeout_none_blocks(self, r):
        """
        Test that get_message(timeout=None) blocks indefinitely.
        We test this by using a thread to publish a message after a delay.
        """
        p = r.pubsub()
        p.subscribe("foo")
        # Read subscription message
        msg = wait_for_message(p, timeout=1.0)
        assert msg is not None

        # Publish a message after a short delay in a thread
        start_publishing = threading.Event()

        def publish_after_delay():
            start_publishing.wait()
            time.sleep(0.2)
            r.publish("foo", "delayed_message")

        thread = threading.Thread(target=publish_after_delay, daemon=True)
        thread.start()

        # get_message with timeout=None should block until message arrives
        start = time.monotonic()
        start_publishing.set()
        msg = p.get_message(timeout=None)
        elapsed = time.monotonic() - start
        assert msg is not None
        assert msg["type"] == "message"
        assert msg["data"] == b"delayed_message"
        # Should have waited at least 0.2 seconds
        assert elapsed >= 0.15
        thread.join(timeout=1.0)

    def test_multiple_messages_with_timeout(self, r):
        """
        Test that timeout is properly handled when reading multiple messages.
        """
        p = r.pubsub()
        p.subscribe("foo")
        # Read subscription message
        msg = wait_for_message(p, timeout=1.0)
        assert msg is not None

        # Publish multiple messages
        r.publish("foo", "msg1")
        r.publish("foo", "msg2")
        r.publish("foo", "msg3")

        # Read all messages with timeout
        messages = []
        for _ in range(3):
            msg = wait_for_message(p, timeout=1.0, func=p.get_message)
            if msg:
                messages.append(msg)

        assert len(messages) == 3
        assert messages[0]["data"] == b"msg1"
        assert messages[1]["data"] == b"msg2"
        assert messages[2]["data"] == b"msg3"

    def test_timeout_with_pattern_subscribe(self, r):
        """
        Test that timeout works correctly with pattern subscriptions.
        """
        p = r.pubsub()
        # A prefix no other test publishes to: on a cluster, a PUBLISH to "foo"
        # from an earlier test can still be crossing the cluster bus.
        p.psubscribe("timeout-pattern*")
        # Read subscription message
        msg = wait_for_message(p, timeout=1.0)
        assert msg is not None
        assert msg["type"] == "psubscribe"

        # Publish a message matching the pattern
        r.publish("timeout-pattern-bar", "hello")

        # get_message with timeout should return the message
        msg = p.get_message(timeout=1.0)
        assert msg is not None
        assert msg["type"] == "pmessage"
        assert msg["data"] == b"hello"

    def test_timeout_with_no_subscription(self, r):
        """
        Test that get_message with timeout returns None when subscribed but no messages.
        """
        p = r.pubsub()
        p.subscribe("foo")
        # Read subscription message
        msg = wait_for_message(p, timeout=1.0)
        assert msg is not None

        # get_message with timeout should return None when no messages
        msg = p.get_message(timeout=0.1)
        assert msg is None


@pytest.mark.onlynoncluster
class TestPubSubBlockingListen:
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

    def _make_client(self, request, no_retry=False, **kwargs):
        if no_retry:
            from redis.backoff import NoBackoff
            from redis.retry import Retry

            kwargs.setdefault("retry", Retry(NoBackoff(), 0))
            kwargs.setdefault("retry_on_timeout", False)
        return _get_client(
            redis.Redis, request, single_connection_client=False, **kwargs
        )

    def test_listen_blocks_longer_than_socket_timeout(self, request):
        """
        listen() must keep blocking past socket_timeout. Configure a very
        short socket_timeout, then publish after a delay that exceeds it,
        and assert listen() yields the message instead of raising
        TimeoutError.
        """
        r = self._make_client(
            request, socket_timeout=self.SOCKET_TIMEOUT, no_retry=True
        )
        publisher = self._make_client(request)
        p = r.pubsub()
        p.subscribe("foo")
        msg = wait_for_message(p, timeout=1.0)
        assert msg is not None and msg["type"] == "subscribe"

        def publish_after_delay():
            time.sleep(self.PUBLISH_DELAY)
            publisher.publish("foo", "hello")

        thread = threading.Thread(target=publish_after_delay, daemon=True)
        thread.start()

        start = time.monotonic()
        msg = next(p.listen())
        elapsed = time.monotonic() - start
        assert msg["type"] == "message"
        assert msg["data"] == b"hello"
        # Should have blocked past socket_timeout to receive the message.
        assert elapsed > self.SOCKET_TIMEOUT
        thread.join(timeout=1.0)
        p.close()

    def test_get_message_block_indefinitely_past_socket_timeout(self, request):
        """
        get_message(timeout=None) must block past socket_timeout.
        """
        r = self._make_client(
            request, socket_timeout=self.SOCKET_TIMEOUT, no_retry=True
        )
        publisher = self._make_client(request)
        p = r.pubsub()
        p.subscribe("foo")
        msg = wait_for_message(p, timeout=1.0)
        assert msg is not None

        def publish_after_delay():
            time.sleep(self.PUBLISH_DELAY)
            publisher.publish("foo", "delayed")

        thread = threading.Thread(target=publish_after_delay, daemon=True)
        thread.start()

        start = time.monotonic()
        msg = p.get_message(timeout=None)
        elapsed = time.monotonic() - start
        assert msg is not None
        assert msg["type"] == "message"
        assert msg["data"] == b"delayed"
        assert elapsed > self.SOCKET_TIMEOUT
        thread.join(timeout=1.0)
        p.close()

    def test_parse_response_block_true_blocks_past_socket_timeout(self, request):
        """
        parse_response(block=True) must block past socket_timeout.
        """
        r = self._make_client(
            request, socket_timeout=self.SOCKET_TIMEOUT, no_retry=True
        )
        publisher = self._make_client(request)
        p = r.pubsub(ignore_subscribe_messages=True)
        p.subscribe("foo")
        # Drain the subscribe ack before calling parse_response directly.
        # The ack is ignored, so get_message returns None as soon as it is read.
        p.get_message(timeout=1.0)

        def publish_after_delay():
            time.sleep(self.PUBLISH_DELAY)
            publisher.publish("foo", "raw")

        thread = threading.Thread(target=publish_after_delay, daemon=True)
        thread.start()

        start = time.monotonic()
        response = p.parse_response(block=True)
        elapsed = time.monotonic() - start
        # Response is the raw pubsub frame [b"message", b"foo", b"raw"].
        assert response is not None
        assert response[0] == b"message"
        assert response[-1] == b"raw"
        assert elapsed > self.SOCKET_TIMEOUT
        thread.join(timeout=1.0)
        p.close()


class TestClusterPubSubTimeoutPropagation:
    """
    Tests for timeout propagation in ClusterPubSub for sharded pubsub.
    """

    @pytest.mark.onlycluster
    @skip_if_server_version_lt("7.0.0")
    def test_get_sharded_message_timeout_is_respected(self, r):
        """
        Test that get_sharded_message() with timeout parameter respects the timeout
        and returns None when no message arrives within the timeout period.
        """
        pubsub = r.pubsub()
        channel = "test-channel:{0}"
        pubsub.ssubscribe(channel)
        # Read subscription message
        msg = wait_for_message(pubsub, timeout=1.0, func=pubsub.get_sharded_message)
        assert msg is not None
        assert msg["type"] == "ssubscribe"

        # Call get_sharded_message with a short timeout - should return None
        start = time.monotonic()
        msg = pubsub.get_sharded_message(timeout=0.1)
        elapsed = time.monotonic() - start
        assert msg is None
        # Verify timeout was actually respected
        assert elapsed < 1.0  # generous: a stalled runner must not fail it
        pubsub.close()

    @pytest.mark.onlycluster
    @skip_if_server_version_lt("7.0.0")
    def test_get_sharded_message_timeout_with_published_message(self, r):
        """
        Test that get_sharded_message() with timeout returns a message if one
        arrives before the timeout expires.
        """
        pubsub = r.pubsub()
        channel = "test-channel:{0}"
        pubsub.ssubscribe(channel)
        # Read subscription message
        msg = wait_for_message(pubsub, timeout=1.0, func=pubsub.get_sharded_message)
        assert msg is not None

        # Publish a message
        r.spublish(channel, "hello")

        # get_sharded_message with timeout should return the message
        msg = pubsub.get_sharded_message(timeout=1.0)
        assert msg is not None
        assert msg["type"] == "smessage"
        assert msg["data"] == b"hello"
        pubsub.close()

    @pytest.mark.onlycluster
    @skip_if_server_version_lt("7.0.0")
    def test_get_sharded_message_timeout_zero_returns_immediately(self, r):
        """
        Test that get_sharded_message(timeout=0) returns immediately without blocking.
        """
        pubsub = r.pubsub()
        channel = "test-channel:{0}"
        pubsub.ssubscribe(channel)
        # Read subscription message
        msg = wait_for_message(pubsub, timeout=1.0, func=pubsub.get_sharded_message)
        assert msg is not None

        # get_sharded_message with timeout=0 should return immediately
        start = time.monotonic()
        msg = pubsub.get_sharded_message(timeout=0)
        elapsed = time.monotonic() - start
        assert msg is None
        assert elapsed < 0.5  # well below any blocking wait, above a runner stall
        pubsub.close()

    @pytest.mark.onlycluster
    @skip_if_server_version_lt("7.0.0")
    def test_get_sharded_message_multiple_channels_with_timeout(self, r):
        """
        Test that timeout is properly handled when reading from multiple sharded channels.
        """
        pubsub = r.pubsub()
        channel1 = "test-channel:{0}"
        channel2 = "test-channel:{6}"
        pubsub.ssubscribe(channel1, channel2)
        # Read subscription messages
        for _ in range(2):
            msg = wait_for_message(pubsub, timeout=1.0, func=pubsub.get_sharded_message)
            assert msg is not None
            assert msg["type"] == "ssubscribe"

        # Publish messages to both channels
        r.spublish(channel1, "msg1")
        r.spublish(channel2, "msg2")

        # Read messages with timeout
        messages = []
        for _ in range(2):
            msg = wait_for_message(pubsub, timeout=1.0, func=pubsub.get_sharded_message)
            if msg and msg["type"] == "smessage":
                messages.append(msg)

        assert len(messages) == 2
        pubsub.close()


class TestClusterPubSubResubscribe:
    """
    Cluster-only tests verifying that sharded pubsub resubscription after a
    reconnect groups channels by slot and does not trigger CROSSSLOT errors
    on nodes that own multiple slots.
    """

    @pytest.mark.onlycluster
    @skip_if_server_version_lt("7.0.0")
    def test_resubscribe_shard_channels_different_slots_same_node(self, r):
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
            p.ssubscribe(ch_a)
            assert (
                wait_for_message(p, timeout=2.0, func=p.get_sharded_message) is not None
            )
            p.ssubscribe(ch_b)
            assert (
                wait_for_message(p, timeout=2.0, func=p.get_sharded_message) is not None
            )

            # Force reconnect of the per-node subscriber connection.
            node = r.get_node_from_key(ch_a)
            per_node_pubsub = p.node_pubsub_mapping[node.name]
            assert per_node_pubsub.connection is not None
            per_node_pubsub.connection.disconnect()

            # Trigger reconnect + resubscription. With batched resubscription
            # this would raise a CROSSSLOT ResponseError; with per-slot
            # grouping it must succeed and deliver fresh ssubscribe acks.
            acks = {}
            deadline = time.monotonic() + 3.0
            while time.monotonic() < deadline and len(acks) < 2:
                msg = p.get_sharded_message(timeout=0.5)
                if msg is None:
                    continue
                if msg["type"] == "ssubscribe":
                    acks[msg["channel"]] = msg

            assert ch_a.encode() in acks
            assert ch_b.encode() in acks

            # Both channels must still be tracked in the aggregate state.
            assert ch_a.encode() in p.shard_channels
            assert ch_b.encode() in p.shard_channels
        finally:
            p.close()


@pytest.mark.fixed_client
class TestClusterPubSubSlotMigration:
    """
    Deterministic unit tests for ClusterPubSub shard-channel slot migration
    handling. These tests bypass all I/O by mocking the per-node PubSub
    instances and the cluster's node-resolution layer, verifying the
    reconciler logic and the reverse-index routing in isolation (no live
    cluster required).
    """

    def _make_cluster_pubsub(self):
        from redis._parsers.encoders import Encoder
        from redis.cluster import ClusterPubSub

        pubsub = ClusterPubSub.__new__(ClusterPubSub)
        # Base PubSub state normally wired up by PubSub.__init__.
        pubsub.encoder = Encoder("utf-8", "strict", False)
        pubsub._lock = threading.RLock()
        pubsub.subscribed_event = threading.Event()
        pubsub.health_check_response_counter = 0
        pubsub.connection = None
        pubsub.channels = {}
        pubsub.pending_unsubscribe_channels = set()
        pubsub.shard_channels = {}
        pubsub.pending_unsubscribe_shard_channels = set()
        pubsub.patterns = {}
        pubsub.pending_unsubscribe_patterns = set()
        pubsub.ignore_subscribe_messages = False
        # ClusterPubSub-specific state.
        pubsub.cluster = mock.MagicMock()
        pubsub.node_pubsub_mapping = {}
        pubsub._shard_channel_to_node = {}
        pubsub._poll_cool_off = weakref.WeakKeyDictionary()
        pubsub._unreachable_nodes = set()
        pubsub._next_topology_repair = 0.0
        pubsub._shard_state_lock = threading.RLock()
        pubsub.push_handler_func = None
        # Reconciliation worker. The real __init__ gets one from reset(); a
        # ThreadPoolExecutor starts no thread until the first submit, so this
        # costs nothing for the tests that never schedule a pass.
        pubsub._reconcile_executor = ThreadPoolExecutor(
            max_workers=1, thread_name_prefix="redis-cluster-pubsub-reconcile"
        )
        return pubsub

    def _make_node(self, name):
        node = mock.MagicMock()
        node.name = name
        return node

    def _make_node_pubsub(self, shard_channels=None):
        p = mock.MagicMock()
        p.shard_channels = dict(shard_channels or {})
        p.pending_unsubscribe_shard_channels = set()
        p.subscribed = True
        # A real, already-set Event: _poll_node_pubsub mirrors get_message's
        # prelude, which waits on it whenever ``subscribed`` is False.
        p.subscribed_event = threading.Event()
        p.subscribed_event.set()
        # ClusterPubSub polls a per-node pubsub as parse_response (under the
        # per-node I/O lock) followed by handle_message (outside it, so a user
        # handler may re-subscribe). Tests keep configuring the single
        # ``get_message`` seam; these two forward to it - parse_response yields
        # the raw reply shape _is_publish_response inspects, handle_message
        # returns the already-parsed message the assertions compare against.
        p.get_message = mock.MagicMock(return_value=None)
        p._last_message = None

        def _parse_response(block=True, timeout=0.0):
            message = p.get_message(ignore_subscribe_messages=False, timeout=timeout)
            p._last_message = message
            if message is None:
                return None
            return [message["type"], message.get("channel"), message.get("data")]

        def _handle_message(response, ignore_subscribe_messages=False):
            return p._last_message

        p.parse_response = mock.MagicMock(side_effect=_parse_response)
        p.handle_message = mock.MagicMock(side_effect=_handle_message)
        return p

    def test_reinitialize_moves_channel_to_new_owner(self):
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

        pubsub.reinitialize_shard_subscriptions()

        old_ps.sunsubscribe.assert_called_once_with(channel)
        new_ps.ssubscribe.assert_called_once_with(channel)
        assert pubsub._shard_channel_to_node[channel] == new_node.name

    def test_reinitialize_noop_when_owner_unchanged(self):
        pubsub = self._make_cluster_pubsub()
        owner = self._make_node("127.0.0.1:7000")
        channel = b"foo"
        owner_ps = self._make_node_pubsub({channel: None})
        pubsub.node_pubsub_mapping[owner.name] = owner_ps
        pubsub.shard_channels = {channel: None}
        pubsub._shard_channel_to_node = {channel: owner.name}
        pubsub.cluster.get_node_from_key.return_value = owner

        pubsub.reinitialize_shard_subscriptions()

        owner_ps.sunsubscribe.assert_not_called()
        owner_ps.ssubscribe.assert_not_called()
        assert pubsub._shard_channel_to_node[channel] == owner.name

    def test_concurrent_refresh_does_not_restore_sunsubscribed_channel(self):
        pubsub = self._make_cluster_pubsub()
        old_node = self._make_node("127.0.0.1:7000")
        new_node = self._make_node("127.0.0.1:7001")
        channel = b"foo"
        old_ps = self._make_node_pubsub({channel: None})
        new_ps = self._make_node_pubsub()
        pubsub.node_pubsub_mapping = {old_node.name: old_ps, new_node.name: new_ps}
        pubsub.shard_channels = {channel: None}
        pubsub._shard_channel_to_node = {channel: old_node.name}
        pubsub.cluster.get_node_from_key.return_value = new_node
        worker = None

        def sunsubscribe(*_):
            nonlocal worker
            worker = threading.Thread(target=pubsub.reinitialize_shard_subscriptions)
            worker.start()
            old_ps.pending_unsubscribe_shard_channels.add(channel)

        old_ps.sunsubscribe.side_effect = sunsubscribe

        pubsub.sunsubscribe(channel)
        worker.join()

        new_ps.ssubscribe.assert_not_called()
        assert channel in pubsub.pending_unsubscribe_shard_channels

    def test_reinitialize_moves_channel_after_reconnect_drops_pending_unsubscribe(
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

        pubsub.reinitialize_shard_subscriptions()

        old_ps.sunsubscribe.assert_called_once_with(channel)
        new_ps.ssubscribe.assert_called_once_with(channel)
        assert pubsub._shard_channel_to_node[channel] == new_node.name
        assert channel not in pubsub.pending_unsubscribe_shard_channels

    def test_reinitialize_retries_revived_channel_after_failed_attach(self):
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
            pubsub.reinitialize_shard_subscriptions()
        pubsub.reinitialize_shard_subscriptions()

        assert new_ps.ssubscribe.call_count == 2
        assert pubsub._shard_channel_to_node[channel] == new_node.name
        assert channel not in pubsub.pending_unsubscribe_shard_channels

    def test_reinitialize_skips_channel_whose_unsubscribe_the_node_confirmed(self):
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

        pubsub.reinitialize_shard_subscriptions()

        new_ps.ssubscribe.assert_not_called()
        assert channel in pubsub.pending_unsubscribe_shard_channels

    def test_reinitialize_skips_channel_confirmed_during_the_check(self):
        """
        The reader thread can apply a ``SUNSUBSCRIBE`` confirmation while a
        pass checks whether the node subscribed the channel again. Seeing the
        channel still held but no longer pending must not make the pass
        subscribe it again.
        """
        pubsub = self._make_cluster_pubsub()
        old_node = self._make_node("127.0.0.1:7000")
        new_node = self._make_node("127.0.0.1:7001")
        channel = b"foo"
        # A real PubSub, not a mock: handle_message's bookkeeping order is
        # part of what is under test.
        old_ps = PubSub.__new__(PubSub)
        old_ps.channels = {}
        old_ps.patterns = {}
        old_ps.ignore_subscribe_messages = False
        old_ps.subscribed_event = threading.Event()
        old_ps.subscribed_event.set()
        old_ps.sunsubscribe = mock.Mock()
        old_ps.reset = mock.Mock()
        confirmed = False

        def confirm():
            nonlocal confirmed
            if not confirmed:
                confirmed = True
                old_ps.handle_message(
                    ["sunsubscribe", channel, 0], ignore_subscribe_messages=False
                )

        class _ConfirmingDict(dict):
            def __contains__(self, item):
                present = super().__contains__(item)
                confirm()
                return present

        class _ConfirmingSet(set):
            def __contains__(self, item):
                present = super().__contains__(item)
                confirm()
                return present

        old_ps.shard_channels = _ConfirmingDict({channel: None})
        old_ps.pending_unsubscribe_shard_channels = _ConfirmingSet({channel})
        new_ps = self._make_node_pubsub()
        pubsub.node_pubsub_mapping[old_node.name] = old_ps
        pubsub.node_pubsub_mapping[new_node.name] = new_ps
        pubsub.shard_channels = {channel: None}
        pubsub.pending_unsubscribe_shard_channels = {channel}
        pubsub._shard_channel_to_node = {channel: old_node.name}
        pubsub.cluster.get_node_from_key.return_value = new_node

        pubsub.reinitialize_shard_subscriptions()

        new_ps.ssubscribe.assert_not_called()
        assert channel in pubsub.pending_unsubscribe_shard_channels

    def test_sunsubscribe_keeps_tracking_for_channel_resubscribed_on_reconnect(self):
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

        message = pubsub.get_sharded_message(target_node=old_node)

        assert message is not None
        assert channel in pubsub.shard_channels
        assert pubsub._shard_channel_to_node[channel] == old_node.name

    def test_reinitialize_reattaches_when_owner_lost_the_subscription(self):
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

        with mock.patch.object(pubsub, "_get_node_pubsub", return_value=owner_ps):
            pubsub.reinitialize_shard_subscriptions()

        # Nothing to sunsubscribe from - the channel was already detached.
        owner_ps.sunsubscribe.assert_not_called()
        owner_ps.ssubscribe.assert_called_once_with(channel)
        assert pubsub._shard_channel_to_node[channel] == owner.name

    def test_reinitialize_tolerates_old_node_disconnect(self):
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

        pubsub.reinitialize_shard_subscriptions()

        old_ps.sunsubscribe.assert_called_once_with(channel)
        new_ps.ssubscribe.assert_called_once_with(channel)
        assert pubsub._shard_channel_to_node[channel] == new_node.name
        # Node still in topology → keep the pubsub so auto-reconnect can
        # recover it; the dedicated socket was already torn down by
        # Connection.disconnect() before the exception surfaced.
        old_ps.reset.assert_not_called()
        assert old_node.name in pubsub.node_pubsub_mapping
        assert channel not in old_ps.shard_channels
        assert channel not in old_ps.pending_unsubscribe_shard_channels

    def test_reinitialize_drops_old_pubsub_when_node_removed_from_topology(self):
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

        pubsub.reinitialize_shard_subscriptions()

        assert pubsub._shard_channel_to_node[channel] == new_node.name
        old_ps.reset.assert_called_once()
        assert old_node.name not in pubsub.node_pubsub_mapping

    def test_sunsubscribe_routes_via_reverse_index_after_migration(self):
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

        pubsub.sunsubscribe(channel)

        holding_ps.sunsubscribe.assert_called_once_with(channel)
        new_ps.sunsubscribe.assert_not_called()

    def test_ssubscribe_migration_applies_newly_supplied_handler(self):
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

        new_handler = mock.Mock()
        pubsub.ssubscribe(foo=new_handler)

        old_ps.sunsubscribe.assert_called_once_with(channel)
        new_ps.ssubscribe.assert_called_once_with(Subscription(channel, new_handler))

    def test_ssubscribe_migration_accepts_binary_subscription_with_handler(self):
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

        new_handler = mock.Mock()
        pubsub.ssubscribe(Subscription(channel, new_handler))

        old_ps.sunsubscribe.assert_called_once_with(channel)
        new_ps.ssubscribe.assert_called_once_with(Subscription(channel, new_handler))

    def test_ssubscribe_skips_channel_when_node_resolution_returns_none(self):
        """
        Regression: cluster.get_node_from_key may return None for channels
        whose slot is transiently uncovered. ssubscribe must skip such
        channels rather than dereference None (which would crash on
        ``node.name`` either in the reverse-index comparison or inside
        ``_get_node_pubsub``). Mirrors the async counterpart's guard.
        """
        pubsub = self._make_cluster_pubsub()
        pubsub.cluster.get_node_from_key.return_value = None
        # Stale reverse-index entry also present to exercise both crash paths:
        # the old_name != node.name comparison and the downstream fallthrough.
        pubsub._shard_channel_to_node = {b"foo": "127.0.0.1:7000"}

        pubsub.ssubscribe(b"foo")

        # No migration, no per-node pubsub creation, no mapping mutation.
        assert pubsub.node_pubsub_mapping == {}
        assert pubsub._shard_channel_to_node == {b"foo": "127.0.0.1:7000"}

    def test_migration_driven_sunsubscribe_drops_empty_pubsub(self):
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

        message = pubsub.get_sharded_message(target_node=old_node)

        assert message is not None
        assert old_node.name not in pubsub.node_pubsub_mapping
        # The per-node pubsub's dedicated connection must be released back
        # to its pool before the mapping drop; otherwise the instance is
        # GC-eligible with no path that closes it (PubSub.__del__ does not
        # release connections), leaking a pool slot per migration.
        old_ps.reset.assert_called_once()

    def test_reinitialize_partial_progress_when_slot_uncovered(self):
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
            pubsub.reinitialize_shard_subscriptions()

        # covered_channel was migrated before the error surfaced;
        # uncovered_channel is left in place for the next reconcile pass.
        new_ps.ssubscribe.assert_called_once_with(covered_channel)
        old_ps.sunsubscribe.assert_called_once_with(covered_channel)
        assert pubsub._shard_channel_to_node[covered_channel] == new_node.name
        assert pubsub._shard_channel_to_node[uncovered_channel] == old_node.name

    def test_on_slots_changed_schedules_reconcile_off_caller_thread(self):
        """
        on_slots_changed must offload reinitialize_shard_subscriptions to a
        worker thread so the dispatch call site (MovedError handling in
        _execute_command / topology refresh in initialize) is not blocked on
        per-channel network I/O. Mirrors the async path's create_task model.
        """
        pubsub = self._make_cluster_pubsub()
        pubsub.shard_channels = {b"foo": None}
        caller_thread = threading.get_ident()
        worker_thread = {}
        started = threading.Event()
        release = threading.Event()

        def _record_and_wait():
            worker_thread["id"] = threading.get_ident()
            started.set()
            # Block so the test can assert caller-thread non-blocking
            # behavior while reconciliation is still in flight.
            release.wait(timeout=5.0)

        with mock.patch.object(
            pubsub, "reinitialize_shard_subscriptions", side_effect=_record_and_wait
        ):
            try:
                pubsub.on_slots_changed()
                # on_slots_changed returned while the pass is still parked in
                # the side-effect, so the work cannot have run on our thread.
                assert started.wait(timeout=5.0)
                assert worker_thread["id"] != caller_thread
                release.set()
                self._drain_reconcile_worker(pubsub)
            finally:
                release.set()
                pubsub.reset()

    def test_on_slots_changed_consumes_and_logs_future_exception(self, caplog):
        """
        Regression: on_slots_changed schedules reinitialize_shard_subscriptions
        on a worker thread; when that raises (e.g. SlotNotCoveredError for a
        transient uncovered slot), the exception must be consumed and logged
        via the library logger so it is not silently lost.
        """
        pubsub = self._make_cluster_pubsub()
        pubsub.shard_channels = {b"foo": None}
        boom = SlotNotCoveredError("simulated transient uncovered slot")

        with mock.patch.object(
            pubsub, "reinitialize_shard_subscriptions", side_effect=boom
        ):
            with caplog.at_level(logging.ERROR, logger="redis.cluster"):
                try:
                    pubsub.on_slots_changed()
                    # Draining the single-threaded worker also waits out the
                    # done-callback that does the logging.
                    self._drain_reconcile_worker(pubsub)
                finally:
                    pubsub.reset()

        assert any(
            "shard subscription reconciliation failed" in rec.message
            and rec.levelno == logging.ERROR
            for rec in caplog.records
        )

    def test_on_slots_changed_does_not_block_on_shard_state_lock(self):
        """
        Regression: on_slots_changed must not wait for _shard_state_lock.

        It runs inline on whichever thread refreshed the topology, which can be
        a command or pubsub thread that handled an SMIGRATED push and still
        holds OSSMaintNotificationsHandler._lock. _shard_state_lock is held
        across per-node pubsub wire I/O by the reconciliation worker, which in
        turn waits for a per-node I/O lock held by a third thread blocked on
        that handler lock - a three-thread cycle. So this hook takes no lock at
        all; the worker it submits to is installed once by reset().
        """
        pubsub = self._make_cluster_pubsub()
        pubsub.shard_channels = {b"foo": None}

        holder_acquired = threading.Event()
        release_holder = threading.Event()
        returned = threading.Event()
        submitted = threading.Event()

        def _hold_shard_state_lock():
            with pubsub._shard_state_lock:
                holder_acquired.set()
                release_holder.wait(timeout=5.0)

        holder = threading.Thread(target=_hold_shard_state_lock)
        holder.start()
        try:
            assert holder_acquired.wait(timeout=2.0)

            def _schedule():
                pubsub.on_slots_changed()
                returned.set()

            with mock.patch.object(
                pubsub,
                "reinitialize_shard_subscriptions",
                side_effect=lambda: submitted.set(),
            ):
                caller = threading.Thread(target=_schedule)
                caller.start()
                try:
                    # The holder still owns _shard_state_lock, yet the call
                    # returned and the pass already ran on the worker.
                    assert returned.wait(timeout=2.0)
                    assert submitted.wait(timeout=2.0)
                finally:
                    caller.join(timeout=2.0)
        finally:
            release_holder.set()
            holder.join(timeout=2.0)
            pubsub.reset()

    def test_schedule_topology_repair_does_not_block_on_shard_state_lock(self):
        """
        Regression: the read path schedules a slots-cache refresh after a failed
        poll, and must neither block on _shard_state_lock nor silently skip the
        repair while another thread holds it - the cluster-wide delivery stall
        that lock can represent is the very thing the repair exists to end.
        """
        pubsub = self._make_cluster_pubsub()
        pubsub.shard_channels = {b"foo": None}
        nodes_manager = pubsub.cluster.nodes_manager

        holder_acquired = threading.Event()
        release_holder = threading.Event()
        scheduled = threading.Event()

        def _hold_shard_state_lock():
            with pubsub._shard_state_lock:
                holder_acquired.set()
                release_holder.wait(timeout=5.0)

        holder = threading.Thread(target=_hold_shard_state_lock)
        holder.start()
        try:
            assert holder_acquired.wait(timeout=2.0)

            def _schedule():
                pubsub._schedule_topology_repair()
                scheduled.set()

            caller = threading.Thread(target=_schedule)
            caller.start()
            try:
                assert scheduled.wait(timeout=2.0)
                # The refresh was actually submitted, not skipped, and the
                # throttle window was armed.
                self._drain_reconcile_worker(pubsub)
                nodes_manager._request_refresh.assert_called_once_with(
                    disconnect_startup_nodes_pools=False
                )
                nodes_manager._run_requested_refresh.assert_called_once_with(
                    pubsub.cluster.retry
                )
                assert pubsub._next_topology_repair > 0.0
            finally:
                caller.join(timeout=2.0)
        finally:
            release_holder.set()
            holder.join(timeout=2.0)
            pubsub.reset()

    def test_reset_clears_the_topology_repair_throttle(self):
        """
        A reused pubsub must not inherit the previous incarnation's throttle
        window: it would skip the first repair after resubscribing, delaying the
        move of its shard channels off a node that is already gone.
        """
        pubsub = self._make_cluster_pubsub()
        pubsub._next_topology_repair = time.monotonic() + 300

        try:
            pubsub.reset()
            assert pubsub._next_topology_repair == 0.0
        finally:
            pubsub._reconcile_executor.shutdown(wait=True)

    def test_reset_retires_the_reconcile_executor(self):
        """
        reset() must retire the reconciliation executor and install a live
        replacement, so the lock-free schedule path always has one to submit
        to. Work aimed at the retired executor is rejected rather than raising:
        reset() has already dropped the shard channels such a pass would have
        reconciled.
        """
        pubsub = self._make_cluster_pubsub()
        pubsub.shard_channels = {b"foo": None}
        retired = pubsub._reconcile_executor

        pubsub.reset()

        assert pubsub._reconcile_executor is not retired
        assert pubsub._reconcile_executor is not None
        # The replacement is usable...
        assert pubsub._submit_reconcile_work(lambda: None) is not None
        # ...and the retired one rejects new work instead of raising.
        pubsub._reconcile_executor = retired
        assert pubsub._submit_reconcile_work(lambda: None) is None

    # --- shard commands routed through execute_command --------------------

    @pytest.mark.parametrize(
        "command,override",
        [("SSUBSCRIBE", "ssubscribe"), ("SUNSUBSCRIBE", "sunsubscribe")],
    )
    def test_execute_command_delegates_shard_subscriptions(self, command, override):
        """
        execute_command's shard branch used to dispatch SSUBSCRIBE /
        SUNSUBSCRIBE straight at the pubsub's own connection, which both wrote
        the socket without the per-node I/O lock and recorded nothing - so the
        channel stayed invisible to the reader loop and to on_connect's replay.
        It must go through the overrides, which own the lock and the
        bookkeeping.
        """
        pubsub = self._make_cluster_pubsub()
        node = self._make_node("127.0.0.1:7000")
        node_ps = self._make_node_pubsub()
        pubsub.node_pubsub_mapping[node.name] = node_ps
        pubsub.cluster.get_node_from_key.return_value = node
        delegate = mock.MagicMock()
        setattr(pubsub, override, delegate)

        pubsub.execute_command(command, b"foo")

        delegate.assert_called_once_with(b"foo")
        node_ps.execute_command.assert_not_called()

    def test_execute_command_ssubscribe_records_the_channel(self):
        """
        End-to-end counterpart to the delegation test: the bookkeeping the raw
        dispatch skipped actually lands.
        """
        pubsub = self._make_cluster_pubsub()
        node = self._make_node("127.0.0.1:7000")
        node_ps = self._make_node_pubsub()
        pubsub.node_pubsub_mapping[node.name] = node_ps
        pubsub.cluster.get_node_from_key.return_value = node

        pubsub.execute_command("SSUBSCRIBE", b"foo")

        node_ps.ssubscribe.assert_called_once_with(b"foo")
        assert pubsub._shard_channel_to_node[b"foo"] == node.name

    def test_execute_command_spublish_holds_the_node_io_lock(self):
        """
        SPUBLISH stays a raw send - there is no override to delegate to - so
        the branch itself has to take the per-node I/O lock, or the send can
        reconnect the socket under a concurrent bounded poll.
        """
        pubsub = self._make_cluster_pubsub()
        node = self._make_node("127.0.0.1:7000")
        node_ps = self._make_node_pubsub()
        # Preset a real lock: getattr on a MagicMock auto-creates an attribute,
        # so _pubsub_io_lock would otherwise hand back a mock instead of one.
        node_ps._shard_io_lock = threading.RLock()
        io_lock = ClusterPubSub._pubsub_io_lock(node_ps)
        assert io_lock is node_ps._shard_io_lock
        pubsub.node_pubsub_mapping[node.name] = node_ps
        pubsub.cluster.get_node_from_key.return_value = node

        observed = []
        node_ps.execute_command = mock.MagicMock(
            side_effect=lambda *a, **kw: observed.append(io_lock._is_owned())
        )

        pubsub.execute_command("SPUBLISH", b"foo", b"hi")

        assert observed == [True]
        assert not io_lock._is_owned()

    def test_ssubscribe_serializes_against_reconcile_lock(self):
        """
        Regression: ssubscribe / sunsubscribe / get_sharded_message must
        acquire self._shard_state_lock so they are mutually exclusive with
        the worker thread running reinitialize_shard_subscriptions. Without
        this serialization, the reverse index, shard_channels, and
        node_pubsub_mapping can be mutated concurrently and the user's
        subscription intent can be overwritten by the reconciliation pass.
        The lock is dedicated to shard-state bookkeeping (separate from
        PubSub.self._lock) so reconciliation does not starve aclose /
        send_command on the cluster-level connection.
        """
        pubsub = self._make_cluster_pubsub()
        node = self._make_node("127.0.0.1:7000")
        node_ps = self._make_node_pubsub()
        pubsub.node_pubsub_mapping[node.name] = node_ps
        pubsub.cluster.get_node_from_key.return_value = node

        holder_acquired = threading.Event()
        release_holder = threading.Event()
        user_done = threading.Event()

        def _hold_lock():
            with pubsub._shard_state_lock:
                holder_acquired.set()
                release_holder.wait(timeout=5.0)

        holder = threading.Thread(target=_hold_lock)
        holder.start()
        try:
            assert holder_acquired.wait(timeout=2.0)

            def _user_ssubscribe():
                pubsub.ssubscribe(b"foo")
                user_done.set()

            user = threading.Thread(target=_user_ssubscribe)
            user.start()
            try:
                # Holder still owns the lock: ssubscribe must be blocked and
                # cannot have produced any side effect on the per-node pubsub.
                assert not user_done.wait(timeout=0.1)
                node_ps.ssubscribe.assert_not_called()

                release_holder.set()
                assert user_done.wait(timeout=2.0)
                node_ps.ssubscribe.assert_called_once_with(b"foo")
            finally:
                user.join(timeout=2.0)
        finally:
            release_holder.set()
            holder.join(timeout=2.0)

    def test_on_slots_changed_concurrent_calls_reuse_one_executor(self):
        """
        Regression: EventDispatcher.dispatch() releases its lock before
        invoking listeners, so concurrent MovedError-handling threads can all
        reach on_slots_changed at the same time. None of them may construct an
        executor - orphaned ThreadPoolExecutors leak their worker threads. The
        executor is installed once by reset(), which is what lets the schedule
        path stay lock-free: there is no creation race left to serialize.
        """
        from redis import cluster as cluster_module

        pubsub = self._make_cluster_pubsub()
        pubsub.shard_channels = {b"foo": None}
        installed = pubsub._reconcile_executor

        start_barrier = threading.Barrier(8)

        def _worker():
            start_barrier.wait(timeout=2.0)
            pubsub.on_slots_changed()

        # The patch is module-wide, so a ClusterPubSub left over from an earlier
        # test that is finalized meanwhile (__del__ -> reset()) constructs one
        # too, on any thread. Count only constructions made by the pubsub under
        # test: the first frame outside unittest.mock is the constructing code.
        own_constructions = []

        def _record_own_construction(*args, **kwargs):
            frame = sys._getframe(1)
            while frame is not None and frame.f_code.co_filename == mock.__file__:
                frame = frame.f_back
            if frame is not None and frame.f_locals.get("self") is pubsub:
                own_constructions.append(mock.call(*args, **kwargs))
            return mock.DEFAULT

        with mock.patch.object(
            cluster_module,
            "ThreadPoolExecutor",
            side_effect=_record_own_construction,
        ):
            with mock.patch.object(
                pubsub, "reinitialize_shard_subscriptions", return_value=None
            ):
                threads = [threading.Thread(target=_worker) for _ in range(8)]
                try:
                    for t in threads:
                        t.start()
                    for t in threads:
                        t.join(timeout=5.0)
                        assert not t.is_alive()
                    self._drain_reconcile_worker(pubsub)
                    # Every caller used the one installed executor.
                    assert own_constructions == []
                    assert pubsub._reconcile_executor is installed
                finally:
                    installed.shutdown(wait=False, cancel_futures=True)

    def test_reset_tears_down_per_node_pubsubs(self):
        """
        Regression: reset() must close every per-node pubsub so its dedicated
        connection is released and its (now stale) shard_channels cannot be
        replayed by PubSub.on_connect on a subsequent reconnect. Mirrors the
        async aclose() path which calls aclose() on each per-node pubsub
        before clearing cluster-level state.
        """
        pubsub = self._make_cluster_pubsub()
        ps_a = self._make_node_pubsub({b"foo": None})
        ps_b = self._make_node_pubsub({b"bar": None})
        pubsub.node_pubsub_mapping = {
            "127.0.0.1:7000": ps_a,
            "127.0.0.1:7001": ps_b,
        }
        pubsub.shard_channels = {b"foo": None, b"bar": None}
        pubsub._shard_channel_to_node = {
            b"foo": "127.0.0.1:7000",
            b"bar": "127.0.0.1:7001",
        }

        pubsub.reset()

        ps_a.reset.assert_called_once()
        ps_b.reset.assert_called_once()
        # Cluster-level shard state cleared by super().reset() and our hook.
        assert pubsub.shard_channels == {}
        assert pubsub._shard_channel_to_node == {}
        # Mapping itself must also be cleared so the round-robin in
        # _pubsubs_generator can't yield dead per-node pubsubs between
        # teardown and re-subscription.
        assert pubsub.node_pubsub_mapping == {}

    def test_reset_swallows_per_node_teardown_errors(self):
        """
        reset() is also a fallback path from __del__; one buggy per-node
        pubsub raising must not mask teardown of the others or of the
        cluster-level state.
        """
        pubsub = self._make_cluster_pubsub()
        ps_bad = self._make_node_pubsub()
        ps_bad.reset.side_effect = RuntimeError("boom")
        ps_good = self._make_node_pubsub()
        pubsub.node_pubsub_mapping = {
            "127.0.0.1:7000": ps_bad,
            "127.0.0.1:7001": ps_good,
        }

        pubsub.reset()

        ps_bad.reset.assert_called_once()
        ps_good.reset.assert_called_once()
        assert pubsub._shard_channel_to_node == {}
        assert pubsub.node_pubsub_mapping == {}

    def test_get_sharded_message_with_missing_target_node_returns_none(self):
        """
        Regression: get_sharded_message(target_node=...) must not raise
        KeyError when the per-node pubsub has been removed from
        node_pubsub_mapping. Migration-driven cleanup in the sunsubscribe
        branch and reset() both remove entries, so a caller polling with
        target_node can race the cleanup. Mirrors the async counterpart's
        .get()/None-fallthrough behavior.
        """
        pubsub = self._make_cluster_pubsub()
        # node_pubsub_mapping is empty - the entry was already cleaned up
        # (e.g. by a migration-driven sunsubscribe pop or by reset()).
        evicted_node = self._make_node("127.0.0.1:7000")

        message = pubsub.get_sharded_message(target_node=evicted_node)

        assert message is None

    def test_reset_recreates_pubsubs_generator(self):
        """
        Regression: _pubsubs_generator captures node_pubsub_mapping.values()
        into a local list inside ``yield from``; clearing the mapping does
        not reach references already held by that captured snapshot. A
        generator suspended mid-yield-from would surface stale (now reset())
        per-node pubsubs after re-subscription unless reset() also rebinds
        _pubsubs_generator to a fresh generator instance.
        """
        from redis.cluster import ClusterPubSub

        pubsub = self._make_cluster_pubsub()
        pubsub._pubsubs_generator = ClusterPubSub._pubsubs_generator(pubsub)
        ps_a = self._make_node_pubsub({b"foo": None})
        ps_b = self._make_node_pubsub({b"bar": None})
        pubsub.node_pubsub_mapping = {
            "127.0.0.1:7000": ps_a,
            "127.0.0.1:7001": ps_b,
        }
        # Advance into yield-from so the generator's frame holds a
        # captured list referencing both stale pubsubs.
        first = next(pubsub._pubsubs_generator)
        assert first in (ps_a, ps_b)
        old_generator = pubsub._pubsubs_generator

        pubsub.reset()

        assert pubsub._pubsubs_generator is not old_generator
        # After re-subscription the fresh generator must yield the new
        # entry rather than draining the old captured list first.
        fresh_ps = self._make_node_pubsub({b"baz": None})
        pubsub.node_pubsub_mapping["127.0.0.1:7002"] = fresh_ps
        assert next(pubsub._pubsubs_generator) is fresh_ps

    def test_drop_node_pubsub_recreates_pubsubs_generator(self):
        """
        Regression: only ``reset()`` rebound ``_pubsubs_generator``, but a
        single-node drop has the same exposure - the generator captures
        ``node_pubsub_mapping.values()`` into a local list inside ``yield
        from``, which ``_drop_node_pubsub``'s pop does not reach. So the round
        robin kept handing the retired per-node pubsub to the next poll.
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

        with pubsub._shard_state_lock:
            pubsub._drop_node_pubsub("127.0.0.1:7000", ps_a)

        assert pubsub._pubsubs_generator is not old_generator
        # The fresh generator must not drain the old captured list first.
        assert next(pubsub._pubsubs_generator) is ps_b
        assert next(pubsub._pubsubs_generator) is ps_b

    def test_unbounded_poll_bails_out_of_a_dropped_pubsub(self):
        """
        Regression: ``_poll_node_pubsub``'s prelude waited on
        ``subscribed_event`` uninterrupted. A per-node pubsub
        ``_drop_node_pubsub`` retired has that event cleared and is never
        resubscribed, so a reader still holding the object - handed it by a
        ``_pubsubs_generator`` snapshot taken before the drop, or by a
        ``target_node`` lookup that raced it - parked for good, and the healthy
        siblings in that pass were never read.
        """
        pubsub = self._make_cluster_pubsub()
        dropped = self._make_stateful_node_pubsub({b"foo": None})
        pubsub.node_pubsub_mapping["127.0.0.1:7000"] = dropped

        with pubsub._shard_state_lock:
            pubsub._drop_node_pubsub("127.0.0.1:7000", dropped)

        # The retirement stays honest: ``subscribed`` is ``subscribed_event``
        # in the sync stack, so the bail-out cannot be spelled by setting it.
        assert not dropped.subscribed

        with mock.patch("redis.cluster.SHARD_SUBSCRIBE_WAIT_TICK_SECONDS", 0.01):
            start = time.monotonic()
            assert pubsub._poll_node_pubsub(dropped, None) is None
        assert time.monotonic() - start < 1.0

    def test_unbounded_poll_skips_a_dropped_pubsub_and_reads_its_sibling(self):
        """
        The pass must survive being handed a retired pubsub, not just avoid
        parking on it: the round robin serves every per-node pubsub from one
        reader, so stalling on the corpse withholds the siblings' messages.

        Restoring the pre-drop generator models the window the rebind in
        ``_drop_node_pubsub`` cannot close - a reader that had already read
        ``self._pubsubs_generator`` when the worker thread rebound it.
        """
        pubsub = self._make_cluster_pubsub()
        silent = self._make_node_pubsub({b"quiet": None})
        dropped = self._make_stateful_node_pubsub({b"foo": None})
        live = self._make_node_pubsub({b"bar": None})
        live.get_message.return_value = {
            "type": "smessage",
            "channel": b"bar",
            "data": b"hi",
        }
        pubsub.node_pubsub_mapping = {
            "127.0.0.1:7000": silent,
            "127.0.0.1:7001": dropped,
            "127.0.0.1:7002": live,
        }
        pubsub._pubsubs_generator = ClusterPubSub._pubsubs_generator(pubsub)
        # Advance into yield-from, leaving ``dropped`` unconsumed in the
        # captured snapshot so the next poll is handed it after the drop.
        assert next(pubsub._pubsubs_generator) is silent
        stale_generator = pubsub._pubsubs_generator

        with pubsub._shard_state_lock:
            pubsub._drop_node_pubsub("127.0.0.1:7001", dropped)
        pubsub._pubsubs_generator = stale_generator

        with mock.patch("redis.cluster.SHARD_SUBSCRIBE_WAIT_TICK_SECONDS", 0.01):
            message = pubsub.get_sharded_message(timeout=None)

        assert message is not None
        assert message["channel"] == b"bar"

    def test_unbounded_poll_waits_for_a_pubsub_still_in_the_mapping(self):
        """
        The bail-out must key off retirement, not off ``subscribed``:
        ``_get_node_pubsub`` registers a per-node pubsub before its caller's
        first ``ssubscribe``, and an unbounded poll that landed in between has
        to keep waiting for that subscription rather than skip the node.
        """
        pubsub = self._make_cluster_pubsub()
        node_ps = self._make_stateful_node_pubsub()
        pubsub.node_pubsub_mapping["127.0.0.1:7000"] = node_ps
        assert not node_ps.subscribed
        smessage = {"type": "smessage", "channel": b"foo", "data": b"hi"}
        node_ps.parse_response = lambda block=True, timeout=0.0: [
            b"smessage",
            b"foo",
            b"hi",
        ]
        node_ps.handle_message = (
            lambda response, ignore_subscribe_messages=False: smessage
        )
        subscriber = threading.Thread(
            target=lambda: (time.sleep(0.05), node_ps.ssubscribe(b"foo")),
            daemon=True,
        )

        with mock.patch("redis.cluster.SHARD_SUBSCRIBE_WAIT_TICK_SECONDS", 0.01):
            subscriber.start()
            message = pubsub._poll_node_pubsub(node_ps, None)
        subscriber.join(timeout=5)

        # Waited out the ticks and then read, rather than skipping the node.
        assert node_ps.subscribed
        assert message == smessage

    def test_bounded_poll_waits_once_for_the_callers_timeout(self):
        """
        The interruptible wait must not change the bounded contract: one wait
        of the caller's timeout, then "no message available". A bounded poll
        recovers on the next pass by re-snapshotting, so it neither ticks nor
        pays the mapping lookup.
        """
        pubsub = self._make_cluster_pubsub()
        node_ps = self._make_stateful_node_pubsub()
        pubsub.node_pubsub_mapping["127.0.0.1:7000"] = node_ps

        with mock.patch.object(
            node_ps.subscribed_event, "wait", return_value=False
        ) as wait:
            assert pubsub._poll_node_pubsub(node_ps, 0.05) is None

        wait.assert_called_once_with(0.05)

    def _make_stateful_node_pubsub(self, shard_channels=None):
        """A per-node pubsub whose ``subscribed`` follows its channels.

        ``_make_node_pubsub`` pins ``subscribed`` to True, which is what the
        tests above need. The detach-driven garbage collection only becomes
        observable when the flag tracks ``shard_channels`` the way a real
        ``PubSub`` does, so those tests use this instead.
        """
        return _StatefulNodePubSub(shard_channels)

    def test_sunsubscribe_bookkeeping_survives_a_racing_detach(self):
        """
        Regression: ``handle_message`` did its unsubscribe bookkeeping as a
        check-then-``remove`` on ``pending_unsubscribe_shard_channels``. A
        reconciliation pass detaching the same channel between the two raised
        ``KeyError`` out of the poll - which no poll site catches, so one racing
        detach aborted sharded delivery. ``_detach_shard_channel`` runs without
        the per-node I/O lock this bookkeeping holds (waiting for it would stall
        reconciliation behind a poll's whole retry budget on the node being
        migrated away from), so the bookkeeping has to tolerate the race.
        """
        # A real PubSub, not a mock: handle_message's bookkeeping is the code
        # under test here.
        node_pubsub = PubSub.__new__(PubSub)
        node_pubsub.shard_channels = {b"foo": None}
        node_pubsub.channels = {}
        node_pubsub.patterns = {}
        node_pubsub.ignore_subscribe_messages = False
        node_pubsub.subscribed_event = threading.Event()
        node_pubsub.subscribed_event.set()

        class _RacingSet(set):
            """Detaches the channel exactly in the check-to-removal window."""

            def __contains__(self, item):
                present = super().__contains__(item)
                if present:
                    ClusterPubSub._detach_shard_channel(node_pubsub, item)
                return present

        node_pubsub.pending_unsubscribe_shard_channels = _RacingSet({b"foo"})

        node_pubsub.handle_message(
            ["sunsubscribe", b"foo", 0], ignore_subscribe_messages=False
        )

        assert b"foo" not in node_pubsub.shard_channels
        assert b"foo" not in node_pubsub.pending_unsubscribe_shard_channels

    def test_reinitialize_detaches_migrated_channel_from_old_pubsub(self):
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

        pubsub.reinitialize_shard_subscriptions()

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

    def test_reinitialize_collects_old_pubsub_once_detach_empties_it(self):
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

        pubsub.reinitialize_shard_subscriptions()

        assert old_ps.reset_calls == 1
        assert old_node.name not in pubsub.node_pubsub_mapping
        assert channel in new_ps.shard_channels

    def test_ssubscribe_reroute_collects_old_pubsub_once_detach_empties_it(self):
        """
        Regression: the detach-driven collection cannot be left to the
        end-of-pass garbage collection, because ``ssubscribe``'s lazy re-route
        reaches ``_forget_shard_channel_on_old_node`` without ever running
        ``reinitialize_shard_subscriptions``. The other two collectors cannot
        reach it either: no ``SUNSUBSCRIBE`` confirmation can arrive for a
        channel forgotten locally, so ``get_sharded_message``'s unsubscribe
        branch never fires, and the old node is still in the topology, so the
        departed-node fast path does not apply.

        An empty pubsub left in ``node_pubsub_mapping`` has had its
        ``subscribed_event`` cleared by the detach, so ``_poll_node_pubsub``
        waits on an event nothing will ever set - forever for a
        ``timeout=None`` caller, and for the whole timeout of every pass
        otherwise, before a single healthy node is read.
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

        pubsub.ssubscribe(channel)

        assert channel in new_ps.shard_channels
        assert pubsub._shard_channel_to_node[channel] == new_node.name
        assert not old_ps.subscribed
        assert old_ps.reset_calls == 1
        assert old_node.name not in pubsub.node_pubsub_mapping

    def test_ssubscribe_reroute_keeps_old_pubsub_with_sibling_channels(self):
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

        pubsub.ssubscribe(migrated)

        assert migrated not in old_ps.shard_channels
        assert staying in old_ps.shard_channels
        assert old_ps.reset_calls == 0
        assert old_node.name in pubsub.node_pubsub_mapping

    def _prime_generator(self, pubsub):
        from redis.cluster import ClusterPubSub

        pubsub._pubsubs_generator = ClusterPubSub._pubsubs_generator(pubsub)

    def test_sharded_message_generator_skips_a_failing_pubsub(self):
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

        source, delivered = pubsub._sharded_message_generator(timeout=0.01)

        assert source is good
        assert delivered is message

    def test_sharded_message_generator_raises_when_every_pubsub_fails(self):
        """
        Isolation must not swallow a total outage: with nothing left to poll
        successfully, the first error is surfaced to the caller (and so to
        ``PubSubWorkerThread``'s exception handler) as before.
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
            pubsub._sharded_message_generator(timeout=0.01)

    def test_sharded_message_generator_repairs_moved(self):
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
        pubsub.on_slots_changed = mock.MagicMock()

        source, delivered = pubsub._sharded_message_generator(timeout=0.01)

        assert (source, delivered) == (None, None)
        # The stale subscription is dropped so on_connect stops replaying it,
        # and its recorded owner is forgotten so reconciliation does not
        # short-circuit on an already-advanced reverse index.
        assert channel not in stale.shard_channels
        assert channel not in pubsub._shard_channel_to_node
        # The channel stays in the cluster-level intent, which is what
        # reconciliation walks.
        assert channel in pubsub.shard_channels
        pubsub.cluster.nodes_manager.move_slot.assert_called_once()
        pubsub.on_slots_changed.assert_called_once()
        # Cooled off, so an uncorrectable slots cache cannot make the repair
        # re-run on every poll.
        assert stale in pubsub._poll_cool_off

    def test_moved_repair_collects_the_pubsub_it_empties(self):
        """
        A node that lost its only slot answers ``MOVED`` for every channel it
        held, so the repair's detach can leave its pubsub with nothing
        subscribed - and an empty one left in ``node_pubsub_mapping`` has had
        its ``subscribed_event`` cleared, so ``_poll_node_pubsub`` waits on an
        event nothing will ever set: for the whole timeout of every pass, and
        forever for a ``timeout=None`` caller. The other collectors cannot
        reach it: no ``SUNSUBSCRIBE`` confirmation arrives for a channel
        forgotten locally, and the reconciliation pass scheduled by the repair
        only garbage-collects it once the worker runs - a whole poll cool-off
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
        pubsub.on_slots_changed = mock.MagicMock()

        assert pubsub._sharded_message_generator(timeout=0.01) == (None, None)

        assert stale.reset_calls == 1
        assert "127.0.0.1:7000" not in pubsub.node_pubsub_mapping
        # The cluster-level intent survives, so the reconciliation the repair
        # scheduled still re-attaches the channel on its new owner.
        assert channel in pubsub.shard_channels

    def test_moved_repair_keeps_a_pubsub_with_other_slots(self):
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
        pubsub.on_slots_changed = mock.MagicMock()

        assert pubsub._sharded_message_generator(timeout=0.01) == (None, None)

        assert stale.reset_calls == 0
        assert pubsub.node_pubsub_mapping == {"127.0.0.1:7000": stale}
        assert moved not in stale.shard_channels
        assert staying in stale.shard_channels
        assert pubsub._shard_channel_to_node == {staying: "127.0.0.1:7000"}

    def test_target_node_poll_repairs_moved(self):
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
        pubsub.on_slots_changed = mock.MagicMock()

        assert pubsub.get_sharded_message(timeout=0.01, target_node=node) is None

        # Same repair as the round-robin path: the stale subscription is dropped
        # so on_connect stops replaying it, its recorded owner is forgotten, the
        # cluster-level intent is kept for reconciliation to walk, and the node
        # is cooled off so an uncorrectable slots cache cannot re-run the repair
        # on every poll.
        assert channel not in stale.shard_channels
        assert channel not in pubsub._shard_channel_to_node
        assert channel in pubsub.shard_channels
        pubsub.on_slots_changed.assert_called_once()
        assert stale in pubsub._poll_cool_off

    def test_target_node_poll_still_raises_connection_errors(self):
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
            pubsub.get_sharded_message(timeout=0.01, target_node=node)

    def test_sharded_message_generator_survives_a_failing_redirect(self):
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
        pubsub.cluster.nodes_manager.move_slot.side_effect = KeyError(key_slot(channel))
        pubsub.on_slots_changed = mock.MagicMock()

        assert pubsub._sharded_message_generator(timeout=0.01) == (None, None)

        pubsub.on_slots_changed.assert_called_once()

    def test_pubsubs_generator_survives_a_momentarily_empty_mapping(self):
        """
        Regression: the generator used to ``return`` on an empty
        ``node_pubsub_mapping``, which exhausts it for good - and only
        ``reset()`` recreates it. Reconciliation drops a per-node pubsub before
        creating its replacement, so one such window silently ended sharded
        delivery for the rest of the pubsub's life.
        """
        pubsub = self._make_cluster_pubsub()
        self._prime_generator(pubsub)

        assert next(pubsub._pubsubs_generator) is None

        ps = self._make_node_pubsub({b"foo": None})
        pubsub.node_pubsub_mapping["127.0.0.1:7000"] = ps

        assert next(pubsub._pubsubs_generator) is ps

    def test_sharded_message_generator_cools_off_a_failing_pubsub(self):
        """
        ``PubSub._execute`` reconnects and then retries through the
        connection's own ``Retry``, so one "bounded" poll on an unreachable node
        can cost its whole retry budget instead of the timeout the caller asked
        for. Going straight back to that node on the next pass is what turns one
        sick node into a cluster-wide delivery stall, so it is skipped for a
        cool-off window while its healthy siblings keep being polled.
        """
        from redis.cluster import SHARD_POLL_COOL_OFF_SECONDS

        pubsub = self._make_cluster_pubsub()
        bad = self._make_node_pubsub({b"foo": None})
        bad.get_message.side_effect = ConnectionError("node is gone")
        good = self._make_node_pubsub({b"bar": None})
        good.get_message.return_value = None
        pubsub.node_pubsub_mapping = {"127.0.0.1:7000": bad, "127.0.0.1:7001": good}
        self._prime_generator(pubsub)

        assert pubsub._sharded_message_generator(timeout=0.01) == (None, None)
        assert bad.get_message.call_count == 1
        polls_of_good = good.get_message.call_count

        # Second pass: the bad one is skipped, the good one is polled again.
        assert pubsub._sharded_message_generator(timeout=0.01) == (None, None)
        assert bad.get_message.call_count == 1
        assert good.get_message.call_count > polls_of_good

        # Once the window expires it is tried again.
        pubsub._poll_cool_off[bad] = time.monotonic() - SHARD_POLL_COOL_OFF_SECONDS
        assert pubsub._sharded_message_generator(timeout=0.01) == (None, None)
        assert bad.get_message.call_count == 2

    def test_sharded_message_generator_blocks_when_all_nodes_cool_off(self):
        """
        A pass in which every node is skipped for cool-off performs no wire
        read, so returning straight away ignores the timeout the caller asked
        to block for - and ``PubSubWorkerThread`` polls right back, spinning
        until the cool-off expires instead of blocking.
        """
        pubsub = self._make_cluster_pubsub()
        bad = self._make_node_pubsub({b"foo": None})
        bad.get_message.side_effect = ConnectionError("node is gone")
        pubsub.node_pubsub_mapping = {"127.0.0.1:7000": bad}
        self._prime_generator(pubsub)
        pubsub._poll_cool_off[bad] = time.monotonic() + 30

        start = time.monotonic()
        assert pubsub._sharded_message_generator(timeout=0.05) == (None, None)
        elapsed = time.monotonic() - start

        assert bad.get_message.call_count == 0
        assert elapsed >= 0.05

    def test_sharded_message_generator_cool_off_wait_ends_with_the_window(self):
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
        assert pubsub._sharded_message_generator(timeout=30) == (None, None)
        elapsed = time.monotonic() - start

        assert 0.5 <= elapsed < 5

    def test_sharded_message_generator_cool_off_does_not_block_zero_timeout(self):
        """A non-blocking poll must stay non-blocking while nodes cool off."""
        pubsub = self._make_cluster_pubsub()
        bad = self._make_node_pubsub({b"foo": None})
        bad.get_message.side_effect = ConnectionError("node is gone")
        pubsub.node_pubsub_mapping = {"127.0.0.1:7000": bad}
        self._prime_generator(pubsub)
        pubsub._poll_cool_off[bad] = time.monotonic() + 30

        start = time.monotonic()
        assert pubsub._sharded_message_generator(timeout=0.0) == (None, None)

        assert time.monotonic() - start < 1

    def test_sharded_message_generator_clears_cool_off_on_success(self):
        """A pubsub that polls cleanly must not stay in cool-off."""
        pubsub = self._make_cluster_pubsub()
        ps = self._make_node_pubsub({b"foo": None})
        ps.get_message.return_value = None
        pubsub.node_pubsub_mapping = {"127.0.0.1:7000": ps}
        self._prime_generator(pubsub)
        pubsub._poll_cool_off[ps] = time.monotonic() - 1

        pubsub._sharded_message_generator(timeout=0.01)

        assert ps not in pubsub._poll_cool_off

    def _drain_reconcile_worker(self, pubsub):
        """Wait out the reconciliation worker so submitted work has finished.

        The worker is a single-threaded executor, so a barrier task can only
        run once everything submitted before it has completed - including the
        done-callbacks, which run on the worker before it takes the next task.
        """
        pubsub._reconcile_executor.submit(lambda: None).result(timeout=5)

    def test_failed_poll_schedules_one_throttled_topology_refresh(self):
        """
        A node that has left the deployment answers ECONNREFUSED, never MOVED, so
        the failed poll is the only signal that its shard channels need a new
        owner. Reconciliation is otherwise purely event-driven, so the read path
        has to ask for a slots-cache refresh - throttled, because a node that is
        down fails every poll.
        """
        from redis.cluster import SHARD_TOPOLOGY_REPAIR_INTERVAL_SECONDS

        pubsub = self._make_cluster_pubsub()
        pubsub.shard_channels = {b"foo": None}
        bad = self._make_node_pubsub({b"foo": None})
        bad.get_message.side_effect = ConnectionError("node is gone")
        pubsub.node_pubsub_mapping = {"127.0.0.1:7000": bad}
        self._prime_generator(pubsub)
        nodes_manager = pubsub.cluster.nodes_manager
        # The repair is an error-triggered refresh like any other: it asks for
        # one and runs it on the worker once the minimum interval allows
        request = nodes_manager._request_refresh
        run_requested = nodes_manager._run_requested_refresh

        try:
            with pytest.raises(ConnectionError):
                pubsub._sharded_message_generator(timeout=0.01)
            self._drain_reconcile_worker(pubsub)
            assert run_requested.call_count == 1
            run_requested.assert_called_with(pubsub.cluster.retry)
            # A repair asks for a fresh slot map only: it keeps the idle
            # connections of a startup node pool that serves no node of the
            # topology.
            request.assert_called_with(disconnect_startup_nodes_pools=False)
            assert pubsub._unreachable_nodes == {"127.0.0.1:7000"}

            # Second failure inside the throttle window must not refresh again.
            pubsub._poll_cool_off.pop(bad, None)
            with pytest.raises(ConnectionError):
                pubsub._sharded_message_generator(timeout=0.01)
            self._drain_reconcile_worker(pubsub)
            assert run_requested.call_count == 1

            # Once the window expires the next failure asks again.
            pubsub._next_topology_repair = (
                time.monotonic() - SHARD_TOPOLOGY_REPAIR_INTERVAL_SECONDS
            )
            pubsub._poll_cool_off.pop(bad, None)
            with pytest.raises(ConnectionError):
                pubsub._sharded_message_generator(timeout=0.01)
            self._drain_reconcile_worker(pubsub)
            assert run_requested.call_count == 2
        finally:
            pubsub._reconcile_executor.shutdown(wait=True)

    def test_failed_poll_does_not_refresh_without_shard_channels(self):
        """Nothing to reconcile means nothing to refresh the topology for."""
        pubsub = self._make_cluster_pubsub()
        bad = self._make_node_pubsub()
        bad.get_message.side_effect = ConnectionError("node is gone")
        pubsub.node_pubsub_mapping = {"127.0.0.1:7000": bad}
        self._prime_generator(pubsub)

        with pytest.raises(ConnectionError):
            pubsub._sharded_message_generator(timeout=0.01)

        pubsub.cluster.nodes_manager.initialize.assert_not_called()

    def test_successful_poll_clears_the_unreachable_mark(self):
        """A node that answers again must not keep the migration fast path."""
        pubsub = self._make_cluster_pubsub()
        ps = self._make_node_pubsub({b"foo": None})
        ps.get_message.return_value = None
        pubsub.node_pubsub_mapping = {"127.0.0.1:7000": ps}
        self._prime_generator(pubsub)
        pubsub._unreachable_nodes = {"127.0.0.1:7000"}

        pubsub._sharded_message_generator(timeout=0.01)

        assert pubsub._unreachable_nodes == set()

    def test_migration_skips_sunsubscribe_on_an_unreachable_old_node(self):
        """
        A SUNSUBSCRIBE to a node the reader just failed to reach cannot arrive,
        and attempting it costs a full reconnect plus the client's retry budget -
        queued behind the reader on the same per-node io lock, while this pass
        holds ``_shard_state_lock``. Detach locally instead and re-SSUBSCRIBE on
        the new owner straight away.
        """
        pubsub = self._make_cluster_pubsub()
        old_ps = self._make_node_pubsub({b"foo": None})
        new_ps = self._make_node_pubsub()
        new_ps.shard_channels = {b"foo": None}
        new_node = self._make_node("127.0.0.1:7001")
        pubsub.node_pubsub_mapping = {
            "127.0.0.1:7000": old_ps,
            "127.0.0.1:7001": new_ps,
        }
        pubsub.shard_channels = {b"foo": None}
        pubsub._shard_channel_to_node = {b"foo": "127.0.0.1:7000"}
        pubsub._unreachable_nodes = {"127.0.0.1:7000"}
        pubsub.cluster.get_node.return_value = self._make_node("127.0.0.1:7000")

        with mock.patch.object(
            pubsub, "_get_node_pubsub", return_value=new_ps
        ) as get_node_pubsub:
            pubsub._migrate_shard_channel(b"foo", None, "127.0.0.1:7000", new_node)

        old_ps.sunsubscribe.assert_not_called()
        assert b"foo" not in old_ps.shard_channels
        get_node_pubsub.assert_called_once_with(new_node)
        new_ps.ssubscribe.assert_called_once_with(b"foo")
        assert pubsub._shard_channel_to_node[b"foo"] == "127.0.0.1:7001"

    def test_migration_drops_an_unreachable_node_gone_from_the_topology(self):
        """
        A departed node has no reconnect target, so its per-node pubsub is
        dropped rather than left for the round robin to keep yielding - and the
        unreachable mark goes with it.
        """
        pubsub = self._make_cluster_pubsub()
        old_ps = self._make_node_pubsub({b"foo": None})
        new_ps = self._make_node_pubsub()
        new_node = self._make_node("127.0.0.1:7001")
        pubsub.node_pubsub_mapping = {"127.0.0.1:7000": old_ps}
        pubsub.shard_channels = {b"foo": None}
        pubsub._shard_channel_to_node = {b"foo": "127.0.0.1:7000"}
        pubsub._unreachable_nodes = {"127.0.0.1:7000"}
        pubsub.cluster.get_node.return_value = None

        with mock.patch.object(pubsub, "_get_node_pubsub", return_value=new_ps):
            pubsub._migrate_shard_channel(b"foo", None, "127.0.0.1:7000", new_node)

        old_ps.sunsubscribe.assert_not_called()
        old_ps.reset.assert_called_once()
        assert "127.0.0.1:7000" not in pubsub.node_pubsub_mapping
        assert pubsub._unreachable_nodes == set()

    # --- per-node I/O lock scope ------------------------------------------

    def _make_real_node_pubsub(self, raw_response, handler=None, channel=b"foo"):
        """A real per-node ``PubSub`` with only the wire read stubbed.

        ``handle_message`` stays real so the assertions exercise the actual
        dispatch, and a real ``_shard_io_lock`` gets attached by
        ``_pubsub_io_lock`` - a ``MagicMock`` stand-in would hand back a mock
        that supports ``__enter__``, making every lock-scope assertion
        vacuously true.
        """
        p = PubSub(mock.MagicMock(), encoder=Encoder("utf-8", "strict", False))
        p.shard_channels = {channel: handler}
        p.subscribed_event.set()
        # A subscribed per-node pubsub is connected; the poll path skips one
        # whose connection is gone. Only the wire read is stubbed, so this is
        # just the non-None marker that state needs.
        p.connection = mock.MagicMock()
        p.parse_response = mock.MagicMock(return_value=raw_response)
        return p

    @pytest.mark.parametrize("via_target_node", [True, False])
    def test_handler_runs_without_the_node_io_lock(self, via_target_node):
        """
        The per-node I/O lock must not be held while handle_message invokes a
        user handler. A handler may call ssubscribe / sunsubscribe on this
        ClusterPubSub, which takes _shard_state_lock and then the same I/O
        lock - and the reconciliation worker takes them in that order too, so
        holding the I/O lock across the handler is an ABBA deadlock (see
        test_handler_resubscribe_does_not_deadlock_against_reconciler).
        """
        pubsub = self._make_cluster_pubsub()
        node = self._make_node("127.0.0.1:7000")
        observed = []
        node_ps = self._make_real_node_pubsub(
            [b"smessage", b"foo", b"hi"],
            handler=lambda message: observed.append(
                _rlock_held_by_another_thread(io_lock)
            ),
        )
        io_lock = ClusterPubSub._pubsub_io_lock(node_ps)
        pubsub.node_pubsub_mapping[node.name] = node_ps
        pubsub._pubsubs_generator = ClusterPubSub._pubsubs_generator(pubsub)

        if via_target_node:
            message = pubsub.get_sharded_message(timeout=0.01, target_node=node)
        else:
            message = pubsub.get_sharded_message(timeout=0.01)

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
    def test_bookkeeping_stays_under_the_io_lock(self, raw_response, expect_lock_held):
        """
        Only handler dispatch leaves the I/O lock. handle_message's
        subscription bookkeeping mutates the same shard_channels /
        pending_unsubscribe_shard_channels that ssubscribe / sunsubscribe
        mutate under this lock, so it must stay inside it.
        """
        pubsub = self._make_cluster_pubsub()
        node = self._make_node("127.0.0.1:7000")
        node_ps = self._make_real_node_pubsub(raw_response, handler=lambda m: None)
        io_lock = ClusterPubSub._pubsub_io_lock(node_ps)
        pubsub.node_pubsub_mapping[node.name] = node_ps

        observed = []
        real_handle_message = node_ps.handle_message

        def _spy(response, ignore_subscribe_messages=False):
            observed.append(_rlock_held_by_another_thread(io_lock))
            return real_handle_message(response, ignore_subscribe_messages)

        node_ps.handle_message = _spy

        pubsub.get_sharded_message(timeout=0.01, target_node=node)

        assert observed == [expect_lock_held]

    def test_handler_may_resubscribe_from_inside_the_handler(self):
        """
        A handler may call ssubscribe from inside itself. Passes before the
        fix too - threading.RLock is reentrant, so the reader re-entering its
        own I/O lock succeeds. The teeth are in
        test_handler_resubscribe_does_not_deadlock_against_reconciler, where a
        second thread owns _shard_state_lock.
        """
        pubsub = self._make_cluster_pubsub()
        node = self._make_node("127.0.0.1:7000")
        pubsub.cluster.get_node_from_key.return_value = node
        node_ps = self._make_real_node_pubsub(
            [b"smessage", b"foo", b"hi"],
            handler=lambda message: pubsub.ssubscribe(b"bar"),
        )
        node_ps.ssubscribe = mock.MagicMock()
        pubsub.node_pubsub_mapping[node.name] = node_ps

        with mock.patch.object(pubsub, "_get_node_pubsub", return_value=node_ps):
            assert pubsub.get_sharded_message(timeout=0.01, target_node=node) is None

        node_ps.ssubscribe.assert_called_once_with(b"bar")

    def test_handler_resubscribe_does_not_deadlock_against_reconciler(self):
        """
        ABBA regression: the reconciliation worker holds _shard_state_lock and
        then waits for a node's I/O lock, while a handler that re-subscribes
        needs _shard_state_lock. So a handler must never run while the reader
        holds that I/O lock. threading.RLock's self-reentrancy does not help -
        the two locks are owned by different threads.
        """
        pubsub = self._make_cluster_pubsub()
        node = self._make_node("127.0.0.1:7000")
        pubsub.cluster.get_node_from_key.return_value = node

        reconciler_may_start = threading.Event()
        reconciler_holds_state_lock = threading.Event()
        reconciler_done = threading.Event()
        reader_done = threading.Event()

        def handler(message):
            # Needs _shard_state_lock, which the reconciler holds while it
            # waits for this node's I/O lock.
            pubsub.ssubscribe(b"bar")

        node_ps = self._make_real_node_pubsub([b"smessage", b"foo", b"hi"], handler)
        node_ps.ssubscribe = mock.MagicMock()
        io_lock = ClusterPubSub._pubsub_io_lock(node_ps)
        pubsub.node_pubsub_mapping[node.name] = node_ps

        def _parse_response(block=True, timeout=0.0):
            # Runs inside the I/O lock: park the reconciler on
            # _shard_state_lock first, so the cycle is fully formed before
            # this read returns.
            reconciler_may_start.set()
            assert reconciler_holds_state_lock.wait(timeout=5.0)
            return [b"smessage", b"foo", b"hi"]

        node_ps.parse_response = mock.MagicMock(side_effect=_parse_response)

        def _reconciler():
            assert reconciler_may_start.wait(timeout=5.0)
            with pubsub._shard_state_lock:
                reconciler_holds_state_lock.set()
                with io_lock:
                    pass
            reconciler_done.set()

        def _reader():
            with mock.patch.object(pubsub, "_get_node_pubsub", return_value=node_ps):
                pubsub.get_sharded_message(timeout=0.01, target_node=node)
            reader_done.set()

        reconciler = threading.Thread(target=_reconciler, daemon=True)
        reader = threading.Thread(target=_reader, daemon=True)
        reconciler.start()
        reader.start()

        assert reader_done.wait(timeout=5.0), (
            "reader deadlocked: the handler waited for _shard_state_lock while "
            "still holding the node I/O lock the reconciler needs"
        )
        assert reconciler_done.wait(timeout=5.0)
        node_ps.ssubscribe.assert_called_once_with(b"bar")

    def test_poll_rechecks_the_subscription_under_the_io_lock(self):
        """
        The prelude's subscription check is deliberately outside the I/O lock,
        so the GC can close this pubsub while the reader waits for that lock -
        every teardown site does its reset() holding it. That leaves
        parse_response with a None connection and a RuntimeError no poll site
        catches, so the check has to be repeated once the lock is held.
        """
        pubsub = self._make_cluster_pubsub()
        node_ps = self._make_real_node_pubsub(None)
        node_ps.parse_response = mock.MagicMock(
            side_effect=AssertionError("read a pubsub the GC had already closed")
        )
        real_poll_io_lock = pubsub._poll_io_lock

        @contextmanager
        def _closing_lock(target, timeout):
            with real_poll_io_lock(target, timeout):
                # What a teardown site does while holding this very lock.
                target.shard_channels.clear()
                target.subscribed_event.clear()
                target.connection = None
                yield

        with mock.patch.object(pubsub, "_poll_io_lock", _closing_lock):
            assert pubsub._poll_node_pubsub(node_ps, 0.01) is None

    def test_reset_holds_the_node_io_lock(self):
        """
        The other per-node teardown sites take the I/O lock; reset() must too,
        so it does not tear the socket down beneath a bounded poll parked in
        parse_response. Mirrors async ClusterPubSub.aclose.
        """
        pubsub = self._make_cluster_pubsub()
        node = self._make_node("127.0.0.1:7000")
        node_ps = self._make_real_node_pubsub(None)
        io_lock = ClusterPubSub._pubsub_io_lock(node_ps)
        pubsub.node_pubsub_mapping[node.name] = node_ps

        observed = []
        # reset() swallows exceptions from a per-node reset, so the observed
        # list - not an assert inside the stub - is what makes this fail.
        node_ps.reset = lambda: observed.append(_rlock_held_by_another_thread(io_lock))

        pubsub.reset()

        assert observed == [True]
        assert not _rlock_held_by_another_thread(io_lock)
        assert pubsub.node_pubsub_mapping == {}


def _rlock_held_by_another_thread(lock):
    """Whether ``lock`` is currently held, probed from a foreign thread.

    ``threading.RLock`` is reentrant, so ``acquire(blocking=False)`` on the
    owning thread always succeeds and would make the assertion vacuous.
    """
    result = []

    def _probe():
        acquired = lock.acquire(blocking=False)
        if acquired:
            lock.release()
        result.append(not acquired)

    probe = threading.Thread(target=_probe, daemon=True)
    probe.start()
    probe.join(timeout=5.0)
    assert result, "lock probe thread did not finish"
    return result[0]


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
        self.subscribed_event = threading.Event()
        if self.shard_channels:
            self.subscribed_event.set()
        # Connected, like any per-node pubsub the reader can poll: the poll
        # path skips one whose connection a teardown has already dropped.
        self.connection = mock.MagicMock()
        self.sunsubscribe_error = None
        self.get_message_error = None
        self.reset_calls = 0

    @property
    def subscribed(self):
        return self.subscribed_event.is_set()

    def ssubscribe(self, *args):
        for arg in args:
            name = getattr(arg, "name", arg)
            self.shard_channels[name] = getattr(arg, "handler", None)
        self.subscribed_event.set()

    def sunsubscribe(self, channel):
        if self.sunsubscribe_error is not None:
            raise self.sunsubscribe_error
        self.shard_channels.pop(channel, None)
        if not self.shard_channels:
            self.subscribed_event.clear()

    def parse_response(self, block=True, timeout=0.0):
        # ClusterPubSub polls a per-node pubsub as parse_response (under the
        # per-node I/O lock) followed by handle_message (outside it), so the
        # injected failure has to surface from the read half.
        if self.get_message_error is not None:
            raise self.get_message_error
        return None

    def handle_message(self, response, ignore_subscribe_messages=False):
        return response

    def get_message(self, ignore_subscribe_messages=False, timeout=0.0):
        return self.handle_message(
            self.parse_response(block=(timeout is None), timeout=timeout),
            ignore_subscribe_messages,
        )

    def reset(self):
        self.reset_calls += 1
        self.shard_channels.clear()
        self.pending_unsubscribe_shard_channels.clear()
        self.subscribed_event.clear()
