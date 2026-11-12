import asyncio
import itertools
import json
import logging
import random
import threading
import time
from collections import defaultdict
from typing import Any
from urllib.parse import urlparse

import pytest
import pytest_asyncio
from redis import RedisCluster
from redis.asyncio import RedisCluster as AsyncRedisCluster
from redis.asyncio.retry import Retry as AsyncRetry
from redis.backoff import ExponentialWithJitterBackoff
from redis.retry import Retry
from tests.helpers import wait_for_condition
from tests.test_asyncio.helpers import wait_for_condition as async_wait_for_condition
from tests.test_scenario.fault_injector_client import (
    ActionRequest,
    ActionType,
    FaultInjectorClient,
    ProxyServerFaultInjector,
    SlotMigrateEffects,
)
from tests.test_scenario.conftest import _FAULT_INJECTOR_CLIENT_OSS_API
from tests.test_scenario.maint_notifications_helpers import (
    ClusterOperations,
    KeyGenerationHelpers,
    generate_params,
)


POST_RECOVERY_DELIVERY_RATIO = 0.90
# Messages a subscriber has to receive on a channel, out of those published after the
# cluster operation, before the delivery-ratio window is allowed to open on it. The
# window must not open while a subscriber is still reconnecting: Pub/Sub never replays
# what was missed, so a window covering the reconnect gap makes the ratio below
# unsatisfiable from the moment it is measured, and the test spends its whole recovery
# budget on a foregone result instead of failing on what actually broke.
DELIVERY_RESUMED_MESSAGES = 3
# Messages per channel the window has to hold before the ratio is worth evaluating.
# POST_RECOVERY_DELIVERY_RATIO of this is what a subscriber is allowed to miss.
POST_RECOVERY_WINDOW_MESSAGES = 20
# Subscriber read errors logged in full before sampling starts. A reconnect shows up as
# a short burst, so sampling from the first error hides the very errors that explain it.
SUBSCRIBER_ERROR_LOG_BURST = 5
SUBSCRIBER_ERROR_LOG_INTERVAL = 10
# Pause after a failed read before polling again. Without it a node that fails to
# connect is retried as fast as the network stack allows, which buries the log in
# identical errors and starves the healthy nodes of the one reader's attention.
SUBSCRIBER_ERROR_BACKOFF = 0.05
BASELINE_TIMEOUT = 30
RECOVERY_TIMEOUT = 120
SHARD_FAILURE_RECOVERY_TIMEOUT = 180
# The poll budget for every failure scenario's fault-injector action. node_failure/reboot
# is the widest of them: it can wait up to 180s for shutdown and 180s for startup before
# the action completes. One budget covers them all because a poll is a ceiling on
# failure, not a target - get_operation_result returns as soon as the action reaches a
# completed status - and it has to outlast the injector's own server-side wait, which for
# the shard failure is the active_timeout of SHARD_FAILURE_RECOVERY_TIMEOUT it is sent.
INFRASTRUCTURE_ACTION_TIMEOUT = 360
# Worst case is the baseline wait, the fault injector's own wait, and the two
# post-recovery waits run back to back - the same shape as EFFECT_TRIGGER_TEST_TIMEOUT
# and MIGRATION_TEST_TIMEOUT below. Every failure scenario shares one injector budget, so
# the shard failure dominates: recovery_timeout_for_failure gives it the longer
# post-recovery wait. Derived from the parts rather than set to a round number so the cap
# cannot silently fall behind them; it only bites on failure.
INFRASTRUCTURE_RECOVERY_TEST_TIMEOUT = (
    BASELINE_TIMEOUT
    + INFRASTRUCTURE_ACTION_TIMEOUT
    + 2 * SHARD_FAILURE_RECOVERY_TIMEOUT
    + 120
)
PUBLISH_INTERVAL = 0.02
PUBSUB_PROGRESS_LOG_MESSAGE_INTERVAL = 300
PUBSUB_TEST_SHARDS_COUNT = 3
# Redis Enterprise shard placement. Sparse spreads the shards of a database over as
# many nodes as it can, dense packs them onto as few as it can. Sparse is what the
# scenarios want by default - a fault injected on any node then lands on this database.
# The migration scenarios need dense instead: the fault injector's migrate action picks
# the target itself, a node holding neither a master nor a replica of the database, and
# on the three-node test cluster sparse placement of three replicated shards leaves no
# such node, so the action fails before the client is exercised at all. Dense puts the
# masters on one node and the replicas on another, which frees the third and makes the
# migration move every master shard onto a node that was not in the slot map when the
# channels were subscribed.
SPARSE_SHARDS_PLACEMENT = "sparse"
DENSE_SHARDS_PLACEMENT = "dense"
# More than one channel per shard so a per-node PubSub holds several shard channels on
# several slots. That reaches two branches no live test reaches at one channel per
# shard: the slot-grouped SSUBSCRIBE replay on reconnect
# (ClusterPubSub._resubscribe_shard_channels, which exists to avoid CROSSSLOT) and
# "keep the per-node pubsub while it still has channels, drop it only once empty".
# Two is the minimum that reaches both;
DEFAULT_CHANNELS_PER_SHARD = 2
PUBSUB_DB_PORT_BASE = 14000
# Every database this suite creates takes its port - and, where the name is ours to
# choose, its name suffix - from this counter, so no port is used twice in a run.
# Redis Enterprise releases a deleted database's port asynchronously and rejects a
# create that lands on one still held, which is what a fixed port per config runs into
# when one case's database is created seconds after the previous case's was deleted.
# The base is random so a run cannot collide with a database another run left behind.
_DB_SUFFIXES = itertools.count(random.randint(0, 500))
SHARD_KEY_REGEX = [{"regex": ".*\\{(?<tag>.*)\\}.*"}, {"regex": "(?<tag>.*)"}]
PUBSUB_CLIENT_TIMEOUT = 5
# The fault injector blocks on a reshard for up to 600s (ASM scale), so the wait for the
# effect must outlast it or a slow-but-successful topology change reads as a failure.
EFFECT_TRIGGER_OP_TIMEOUT = 660
# Node add and remove reconciliation has more to do than a shuffle between existing
# nodes: a per-node PubSub is created for a node that did not exist at subscribe time
# and another is torn down, and a channel whose migration is deferred on a transient
# ConnectionError only retries on the next slots-cache change.
EFFECT_RECOVERY_TIMEOUT = 240
# Worst case is the baseline wait, the fault injector's own wait, and the two
# post-recovery waits run back to back. Derived from the parts rather than set to a
# round number so the cap cannot silently fall behind them; it only bites on failure.
EFFECT_TRIGGER_TEST_TIMEOUT = (
    BASELINE_TIMEOUT + EFFECT_TRIGGER_OP_TIMEOUT + 2 * EFFECT_RECOVERY_TIMEOUT + 180
)
# Same shape for the plain-action migration tests: their fault-injector wait is
# RECOVERY_TIMEOUT rather than the effect budget, so all three waits are equal.
MIGRATION_TEST_TIMEOUT = BASELINE_TIMEOUT + 3 * RECOVERY_TIMEOUT + 120


FAILURE_SCENARIOS = [
    pytest.param(
        "failover",
        lambda endpoint_config: ActionRequest(
            action_type=ActionType.FAILOVER,
            parameters={
                "bdb_id": endpoint_config["bdb_id"],
                "cluster_index": 0,
            },
        ),
        id="failover",
    ),
    pytest.param(
        "node_reboot",
        lambda endpoint_config: ActionRequest(
            action_type=ActionType.NODE_FAILURE,
            parameters={
                "cluster_index": 0,
                "node_id": 1,
                "method": "reboot",
            },
        ),
        id="node-reboot",
    ),
    pytest.param(
        "proxy_restart",
        lambda endpoint_config: ActionRequest(
            action_type=ActionType.PROXY_FAILURE,
            parameters={
                "bdb_id": endpoint_config["bdb_id"],
                "cluster_index": 0,
                "action": "restart",
            },
        ),
        id="proxy-restart",
    ),
    pytest.param(
        "shard_failure",
        lambda endpoint_config: ActionRequest(
            action_type=ActionType.SHARD_FAILURE,
            parameters={
                "bdb_id": endpoint_config["bdb_id"],
                "cluster_index": 0,
                # The fault injector kills the master shard and then waits for the
                # database to serve again, so the test does not poll cluster state.
                "wait_for_active": True,
                "active_timeout": SHARD_FAILURE_RECOVERY_TIMEOUT,
            },
        ),
        id="shard-failure",
    ),
]


def recovery_timeout_for_failure(failure_name):
    if failure_name == "shard_failure":
        return SHARD_FAILURE_RECOVERY_TIMEOUT
    return RECOVERY_TIMEOUT


def recovery_timeout_for_effect(effect_name):
    if effect_name == SlotMigrateEffects.SLOT_SHUFFLE:
        return RECOVERY_TIMEOUT
    return EFFECT_RECOVERY_TIMEOUT


def execute_failure_scenario(
    fault_injector_client: FaultInjectorClient,
    create_action,
    cluster_endpoint_config,
):
    result = fault_injector_client.trigger_action(
        create_action(cluster_endpoint_config)
    )
    fault_injector_client.get_operation_result(
        result["action_id"],
        timeout=INFRASTRUCTURE_ACTION_TIMEOUT,
    )


def execute_migration(
    fault_injector_client: FaultInjectorClient,
    cluster_endpoint_config,
):
    """Move all master shards of the test database to another node.

    The fault injector picks the target node itself - one holding neither a master
    nor a replica of this database - so the test never inspects cluster topology.
    """
    action_id = ClusterOperations.migrate(
        fault_injector_client, cluster_endpoint_config
    )
    fault_injector_client.get_operation_result(action_id, timeout=RECOVERY_TIMEOUT)


def execute_effect_trigger(
    fault_injector_client: FaultInjectorClient,
    cluster_endpoint_config,
    effect_name,
    trigger,
    timeout=EFFECT_TRIGGER_OP_TIMEOUT,
):
    """Run the fault injector effect/trigger that moves slots between nodes.

    The effect says what changes and the trigger says how it is caused; the fault
    injector picks the nodes involved, so the test never inspects cluster topology.
    """
    action_id = ClusterOperations.trigger_effect(
        fault_injector=fault_injector_client,
        endpoint_config=cluster_endpoint_config,
        effect_name=effect_name,
        trigger_name=trigger,
    )
    fault_injector_client.get_operation_result(action_id, timeout=timeout)


def delete_database_if_exists(
    fault_injector_client: FaultInjectorClient, database_name: str
):
    try:
        bdb_id = ClusterOperations.find_database_id_by_name(
            fault_injector_client, database_name
        )
    except Exception as exc:
        logging.info("Database %s not found during cleanup: %s", database_name, exc)
        return

    if bdb_id:
        fault_injector_client.delete_database(bdb_id)


def next_db_suffix() -> int:
    """Return the next name and port suffix for a database this suite creates."""
    return next(_DB_SUFFIXES)


def create_effect_database(
    fault_injector_client: FaultInjectorClient,
    db_config: dict[str, Any],
) -> dict[str, Any]:
    """Create the database a fault-injector effect/trigger requires.

    Returns the endpoint config with the shard count copied across: the injector's
    create_database output does not carry it, and the drivers need it to place channels
    on every shard. Unlike make_pubsub_db_config the config is not authored here, so it
    is used as the injector supplies it - every slot-migrate dbconfig it serves is
    sharded, oss_cluster, and carries the standard hashtag shard_key_regex, which is
    what the channel-to-slot mapping in KeyGenerationHelpers.redis_slot relies on.

    The port is the one value not taken as supplied. The injector serves a fixed port
    per effect config, several configs share one, and Redis Enterprise still holds the
    port of a database deleted seconds earlier, so a case whose predecessor used the
    same port fails to provision with port_unavailable. The copy also keeps the port
    off the parametrize dict, which the sync and async classes share.
    """
    delete_database_if_exists(fault_injector_client, db_config["name"])
    db_config = {**db_config, "port": PUBSUB_DB_PORT_BASE + next_db_suffix()}
    endpoint_config = fault_injector_client.create_database(db_config)
    endpoint_config["shards_count"] = db_config["shards_count"]
    return endpoint_config


def dedupe_effect_params(
    params: list[tuple[Any, ...]],
) -> list[tuple[Any, ...]]:
    """Keep one parameter set per effect and trigger.

    The injector offers every effect/trigger pair twice, once per
    oss_cluster_api_preferred_endpoint_type (ip and hostname). get_cluster_client
    resolves endpoints[0] the same way for both, so the pair is a near-duplicate for
    Pub/Sub, and generate_params already strips the random name suffix that told them
    apart - both arrive carrying the same database name. Keeping one halves an
    already slow matrix without dropping a distinct topology.
    """
    seen = set()
    deduped = []
    for param in params:
        effect_and_trigger = (param[0], param[1])
        if effect_and_trigger in seen:
            continue
        seen.add(effect_and_trigger)
        deduped.append(param)
    return deduped


def make_pubsub_db_config(shards_placement: str = SPARSE_SHARDS_PLACEMENT):
    """Build the database config the Pub/Sub scenarios need.

    Defined here rather than read from a bdb config file so the suite carries its own
    requirements: sharded, OSS-cluster API across every master shard, and a hashtag
    shard_key_regex so channels can be pinned to a slot. Name and port carry a suffix
    unique to the run, mirroring how the fault injector generates its own configs, so
    concurrent runs cannot collide with each other or with a fault-injector database.

    shards_placement is a parameter because the two groups of scenarios want opposite
    layouts; see the pubsub_shards_placement fixture and its override.
    """
    suffix = next_db_suffix()
    return {
        "name": f"pubsub-oss-api-{suffix}",
        "port": PUBSUB_DB_PORT_BASE + suffix,
        "memory_size": 1273741824,
        "eviction_policy": "noeviction",
        "sharding": True,
        "shards_count": PUBSUB_TEST_SHARDS_COUNT,
        "shards_placement": shards_placement,
        "replication": True,
        "oss_cluster": True,
        "oss_cluster_api_preferred_ip_type": "external",
        "oss_cluster_api_preferred_endpoint_type": "ip",
        "proxy_policy": "all-master-shards",
        "shard_key_regex": SHARD_KEY_REGEX,
    }


def get_cluster_client(
    endpoints_config: dict[str, Any],
    client_class: type[RedisCluster] | type[AsyncRedisCluster] = RedisCluster,
    protocol: int = 3,
    retry_class: type[Retry] | type[AsyncRetry] = Retry,
    socket_timeout: float = PUBSUB_CLIENT_TIMEOUT,
) -> RedisCluster | AsyncRedisCluster:
    endpoints = endpoints_config.get("endpoints", [])
    if not endpoints:
        raise ValueError("No endpoints found in configuration")

    parsed = urlparse(endpoints[0])
    if not parsed.hostname:
        raise ValueError(f"Could not parse host from endpoint URL: {endpoints[0]}")
    if parsed.scheme == "rediss":
        raise ValueError("Pub/Sub scenario tests do not support TLS endpoints")

    return client_class(
        host=parsed.hostname,
        port=parsed.port,
        socket_timeout=socket_timeout,
        username=endpoints_config.get("username"),
        password=endpoints_config.get("password"),
        protocol=protocol,
        retry=retry_class(
            backoff=ExponentialWithJitterBackoff(base=0.1, cap=10),
            retries=10,
        ),
    )


@pytest.fixture()
def pubsub_shards_placement():
    """Shard placement for the database cluster_endpoint_config creates.

    Sparse by default, so the shards spread over every node and a fault injected on any
    one of them lands on this database. The migration scenarios need the opposite and
    override this fixture per class.
    """
    return SPARSE_SHARDS_PLACEMENT


@pytest.fixture()
def cluster_endpoint_config(
    fault_injector_client_oss_api: FaultInjectorClient,
    pubsub_shards_placement,
):
    """Create a Pub/Sub database for one test and delete it afterwards."""
    if isinstance(fault_injector_client_oss_api, ProxyServerFaultInjector):
        pytest.skip("mock proxy does not currently support Pub/Sub flows")

    db_config = make_pubsub_db_config(shards_placement=pubsub_shards_placement)

    delete_database_if_exists(fault_injector_client_oss_api, db_config["name"])
    try:
        endpoint_config = fault_injector_client_oss_api.create_database(db_config)
        endpoint_config["shards_count"] = db_config["shards_count"]
        yield endpoint_config
    finally:
        delete_database_if_exists(fault_injector_client_oss_api, db_config["name"])


@pytest.fixture()
def cluster_client(
    cluster_endpoint_config,
):
    client = get_cluster_client(
        endpoints_config=cluster_endpoint_config,
    )
    try:
        yield client
    finally:
        client.close()


@pytest_asyncio.fixture()
async def async_cluster_client(
    cluster_endpoint_config,
):
    client = get_cluster_client(
        endpoints_config=cluster_endpoint_config,
        client_class=AsyncRedisCluster,
        retry_class=AsyncRetry,
    )
    try:
        yield client
    finally:
        await client.aclose()


def delivery_breakdown(received_by_subscriber, channels, subscribers_pubsubs):
    """Per-channel delivery counts and per-node pubsub fan-out.

    ``received`` on its own cannot say whether delivery stopped on the channels
    a fault actually touched or on all of them, and the two point at very
    different causes: the first is a routing or re-subscription failure on the
    migrated slots, the second is the one reader being starved by an unhealthy
    per-node pubsub. ``node_pubsubs`` is here for the same reason - an empty
    mapping means there is nothing left to poll at all.
    """
    parts = []
    # Driven by the pubsubs actually created, not by received_by_subscriber: the
    # latter is sized up front while the former grows during setup, and this runs
    # from a finally block that must not raise over a half-built subscriber list.
    for index, pubsub in enumerate(subscribers_pubsubs):
        received = received_by_subscriber[index]
        counts = " ".join(f"{channel}={len(received[channel])}" for channel in channels)
        try:
            node_pubsubs = sorted(getattr(pubsub, "node_pubsub_mapping", {}))
        except RuntimeError:
            # Reconciliation mutates node_pubsub_mapping from its own thread and
            # this is diagnostics: never let it raise over the failure it is
            # meant to explain.
            node_pubsubs = "<mutating>"
        parts.append(f"subscriber{index}: {counts} node_pubsubs={node_pubsubs}")
    return "; ".join(parts)


def run_sharded_pubsub_scenario(
    client,
    endpoint_config,
    channel_prefix,
    subscriber_count,
    cluster_op_action,
    recovery_timeout=RECOVERY_TIMEOUT,
    channels_per_shard=DEFAULT_CHANNELS_PER_SHARD,
):
    channels = KeyGenerationHelpers.generate_keys_for_all_shards(
        shards_count=endpoint_config["shards_count"],
        prefix=channel_prefix,
        keys_per_shard=channels_per_shard,
    )
    state_lock = threading.Lock()
    subscribers = []
    received_by_subscriber = [defaultdict(set) for _ in range(subscriber_count)]
    stop_event = threading.Event()
    sent_by_channel = defaultdict(set)
    publisher_thread = None
    sent_messages = 0
    received_messages = 0
    publish_errors = 0
    subscriber_errors = 0
    last_logged_error_type = None

    logging.info(
        "Pub/Sub scenario started: channels=%s channels_per_shard=%s subscribers=%s",
        len(channels),
        channels_per_shard,
        subscriber_count,
    )

    def progress_message():
        with state_lock:
            subscriber_threads_alive = sum(
                thread.is_alive() for _, thread in subscribers
            )
            breakdown = delivery_breakdown(
                received_by_subscriber, channels, [ps for ps, _ in subscribers]
            )
            return (
                f"sent={sent_messages}, received={received_messages}, "
                f"publish_errors={publish_errors}, "
                f"subscriber_errors={subscriber_errors}, "
                f"subscriber_threads_alive={subscriber_threads_alive}/{len(subscribers)}"
                f"; {breakdown}"
            )

    def handle_subscriber_error(error, pubsub, thread):
        nonlocal subscriber_errors, last_logged_error_type
        error_type = type(error).__name__
        with state_lock:
            subscriber_errors += 1
            errors = subscriber_errors
            new_error_type = error_type != last_logged_error_type
            if new_error_type:
                last_logged_error_type = error_type
        # Log the opening burst in full, then sample, and always log an error kind not
        # seen before - a lone distinct error inside a flood of ConnectionErrors is
        # what explains the flood, and sampling alone only shows it by luck.
        if (
            errors <= SUBSCRIBER_ERROR_LOG_BURST
            or errors % SUBSCRIBER_ERROR_LOG_INTERVAL == 0
            or new_error_type
        ):
            # Both the repr and the str: RedisError.__repr__ renders only
            # "network:TimeoutError", which drops the host and the reason - and
            # those are what tell a poll timeout apart from a reconnect
            # handshake timing out on one specific node.
            logging.info(
                "Pub/Sub subscriber read error: errors=%s error=%r (%s)",
                errors,
                error,
                error,
            )

        # PubSubWorkerThread re-enters get_sharded_message the moment the handler
        # returns, so without this a node that fails to connect is retried as fast
        # as the network stack allows. Matches the async reader's own backoff.
        time.sleep(SUBSCRIBER_ERROR_BACKOFF)

    def publish_messages():
        nonlocal publish_errors, sent_messages
        seq_by_channel = defaultdict(int)
        while not stop_event.is_set():
            for channel in channels:
                seq = seq_by_channel[channel]
                payload = json.dumps({"channel": channel, "seq": seq})
                try:
                    client.spublish(channel, payload)
                except Exception:
                    # Pub/Sub is best-effort during injected infrastructure faults.
                    # Only successfully published post-recovery messages enter the
                    # delivery-ratio denominator.
                    with state_lock:
                        publish_errors += 1
                else:
                    seq_by_channel[channel] += 1
                    with state_lock:
                        sent_by_channel[channel].add(seq)
                        sent_messages += 1
                        should_log_progress = (
                            sent_messages % PUBSUB_PROGRESS_LOG_MESSAGE_INTERVAL == 0
                        )
                        sent = sent_messages
                        received = received_messages
                        errors = publish_errors
                        sub_errors = subscriber_errors
                        subscriber_threads_alive = sum(
                            thread.is_alive() for _, thread in subscribers
                        )
                    if should_log_progress:
                        logging.info(
                            "Pub/Sub progress: sent=%s received=%s "
                            "publish_errors=%s subscriber_errors=%s "
                            "subscriber_threads_alive=%s/%s",
                            sent,
                            received,
                            errors,
                            sub_errors,
                            subscriber_threads_alive,
                            len(subscribers),
                        )
            time.sleep(PUBLISH_INTERVAL)

    try:
        for index in range(subscriber_count):
            pubsub = client.pubsub()

            def make_handler(subscriber_index):
                def handler(message):
                    nonlocal received_messages
                    payload = json.loads(message["data"])
                    with state_lock:
                        received_by_subscriber[subscriber_index][
                            payload["channel"]
                        ].add(payload["seq"])
                        received_messages += 1

                return handler

            pubsub.ssubscribe(**{channel: make_handler(index) for channel in channels})
            thread = pubsub.run_in_thread(
                sleep_time=0.01,
                daemon=True,
                exception_handler=handle_subscriber_error,
                sharded_pubsub=True,
            )
            subscribers.append((pubsub, thread))

        publisher_thread = threading.Thread(
            target=publish_messages,
            daemon=True,
        )
        publisher_thread.start()
        logging.info("Pub/Sub publisher thread started")

        def baseline_messages_received():
            with state_lock:
                return all(
                    all(len(subscriber[channel]) >= 3 for channel in channels)
                    for subscriber in received_by_subscriber
                )

        try:
            wait_for_condition(
                baseline_messages_received,
                timeout=BASELINE_TIMEOUT,
                check_interval=0.1,
                error_message=(
                    "Timed out waiting for each subscriber to receive messages"
                ),
            )
        except AssertionError as error:
            raise AssertionError(f"{error}; {progress_message()}") from error
        logging.info("Pub/Sub baseline reached: %s", progress_message())

        logging.info("Pub/Sub cluster action started: %s", cluster_op_action.__name__)
        cluster_op_action()
        logging.info("Pub/Sub cluster action completed: %s", progress_message())

        client.nodes_manager.initialize()
        time.sleep(5)

        with state_lock:
            recovery_baseline = {
                channel: set(seqs) for channel, seqs in sent_by_channel.items()
            }

        # Set by recovery_window_ready once delivery is observed live again, and the
        # baseline the delivery ratio is measured against from then on. None until then.
        measurement_baseline = None

        def recovery_window_ready():
            nonlocal measurement_baseline
            with state_lock:
                if measurement_baseline is not None:
                    return all(
                        len(sent_by_channel[channel] - measurement_baseline[channel])
                        >= POST_RECOVERY_WINDOW_MESSAGES
                        for channel in channels
                    )
                # Every subscriber has to be receiving again on every channel before
                # the window opens. Opening it on what the publisher sent instead - the
                # publisher recovers first, and needs about a second to produce
                # POST_RECOVERY_WINDOW_MESSAGES - puts the reconnect gap inside the
                # window, and those messages are gone for good.
                for channel in channels:
                    published = sent_by_channel[channel] - recovery_baseline.get(
                        channel, set()
                    )
                    for subscriber in received_by_subscriber:
                        if (
                            len(subscriber[channel] & published)
                            < DELIVERY_RESUMED_MESSAGES
                        ):
                            return False
                measurement_baseline = {
                    channel: set(sent_by_channel[channel]) for channel in channels
                }
            logging.info("Pub/Sub delivery resumed: %s", progress_message())
            return False

        try:
            wait_for_condition(
                recovery_window_ready,
                timeout=recovery_timeout,
                check_interval=0.1,
                error_message="Timed out waiting for the post-recovery window",
            )
        except AssertionError as error:
            stage = (
                "delivery never resumed"
                if measurement_baseline is None
                else "window never filled"
            )
            raise AssertionError(f"{error} ({stage}); {progress_message()}") from error
        logging.info("Pub/Sub post-recovery window ready: %s", progress_message())

        with state_lock:
            sent_after_recovery = {
                channel: sent_by_channel[channel] - measurement_baseline[channel]
                for channel in channels
            }

        def delivery_ratio_met():
            with state_lock:
                if any(not sent_after_recovery[channel] for channel in channels):
                    return False
                for subscriber in received_by_subscriber:
                    for channel in channels:
                        delivered = len(
                            subscriber[channel] & sent_after_recovery[channel]
                        )
                        ratio = delivered / len(sent_after_recovery[channel])
                        if ratio < POST_RECOVERY_DELIVERY_RATIO:
                            return False
                return True

        try:
            wait_for_condition(
                delivery_ratio_met,
                timeout=recovery_timeout,
                check_interval=0.1,
                error_message="Timed out waiting for post-recovery delivery ratio",
            )
        except AssertionError as error:
            raise AssertionError(f"{error}; {progress_message()}") from error
        logging.info("Pub/Sub delivery ratio reached: %s", progress_message())
    finally:
        stop_event.set()
        if publisher_thread is not None:
            publisher_thread.join(timeout=5)
        for pubsub, thread in subscribers:
            thread.stop()
            thread.join(timeout=5)
            pubsub.close()
        logging.info("Pub/Sub scenario stopped: %s", progress_message())


async def async_run_sharded_pubsub_recovery_scenario(
    client,
    endpoint_config,
    channel_prefix,
    subscriber_count,
    cluster_op_action,
    recovery_timeout=RECOVERY_TIMEOUT,
    channels_per_shard=DEFAULT_CHANNELS_PER_SHARD,
):
    channels = KeyGenerationHelpers.generate_keys_for_all_shards(
        shards_count=endpoint_config["shards_count"],
        prefix=channel_prefix,
        keys_per_shard=channels_per_shard,
    )
    subscribers = []
    reader_tasks = []
    received_by_subscriber = [defaultdict(set) for _ in range(subscriber_count)]
    stop_event = asyncio.Event()
    sent_by_channel = defaultdict(set)
    publisher_task = None
    sent_messages = 0
    received_messages = 0
    publish_errors = 0
    subscriber_errors = 0
    last_logged_error_type = None

    logging.info(
        "Async Pub/Sub scenario started: channels=%s channels_per_shard=%s "
        "subscribers=%s",
        len(channels),
        channels_per_shard,
        subscriber_count,
    )

    def progress_message():
        subscriber_tasks_alive = sum(not task.done() for task in reader_tasks)
        breakdown = delivery_breakdown(received_by_subscriber, channels, subscribers)
        return (
            f"sent={sent_messages}, received={received_messages}, "
            f"publish_errors={publish_errors}, "
            f"subscriber_errors={subscriber_errors}, "
            f"subscriber_tasks_alive={subscriber_tasks_alive}/{len(reader_tasks)}"
            f"; {breakdown}"
        )

    def handle_subscriber_error(error):
        # No lock, unlike the sync twin: every reader runs on the one event loop, so
        # the increment cannot interleave with another reader's.
        nonlocal subscriber_errors, last_logged_error_type
        subscriber_errors += 1
        error_type = type(error).__name__
        new_error_type = error_type != last_logged_error_type
        if new_error_type:
            last_logged_error_type = error_type
        # Log the opening burst in full, then sample, and always log an error kind not
        # seen before - a lone distinct error inside a flood of ConnectionErrors is
        # what explains the flood, and sampling alone only shows it by luck.
        if (
            subscriber_errors <= SUBSCRIBER_ERROR_LOG_BURST
            or subscriber_errors % SUBSCRIBER_ERROR_LOG_INTERVAL == 0
            or new_error_type
        ):
            # Both the repr and the str: RedisError.__repr__ renders only
            # "network:TimeoutError", which drops the host and the reason - and
            # those are what tell a poll timeout apart from a reconnect
            # handshake timing out on one specific node.
            logging.info(
                "Async Pub/Sub subscriber read error: errors=%s error=%r (%s)",
                subscriber_errors,
                error,
                error,
            )

    async def publish_messages():
        nonlocal publish_errors, sent_messages
        seq_by_channel = defaultdict(int)
        while not stop_event.is_set():
            for channel in channels:
                seq = seq_by_channel[channel]
                payload = json.dumps({"channel": channel, "seq": seq})
                try:
                    await client.spublish(channel, payload)
                except Exception:
                    # Pub/Sub is best-effort during injected infrastructure faults.
                    # Only successfully published post-recovery messages enter the
                    # delivery-ratio denominator.
                    publish_errors += 1
                else:
                    seq_by_channel[channel] += 1
                    sent_by_channel[channel].add(seq)
                    sent_messages += 1
                    if sent_messages % PUBSUB_PROGRESS_LOG_MESSAGE_INTERVAL == 0:
                        logging.info(
                            "Async Pub/Sub progress: sent=%s received=%s "
                            "publish_errors=%s subscriber_errors=%s "
                            "subscriber_tasks_alive=%s/%s",
                            sent_messages,
                            received_messages,
                            publish_errors,
                            subscriber_errors,
                            sum(not task.done() for task in reader_tasks),
                            len(reader_tasks),
                        )
            await asyncio.sleep(PUBLISH_INTERVAL)

    async def read_messages(pubsub):
        while not stop_event.is_set():
            try:
                await pubsub.get_sharded_message(
                    ignore_subscribe_messages=True,
                    timeout=0.01,
                )
            except Exception as error:
                if stop_event.is_set():
                    return
                handle_subscriber_error(error)
                await asyncio.sleep(SUBSCRIBER_ERROR_BACKOFF)

    try:
        for index in range(subscriber_count):
            pubsub = client.pubsub()

            def make_handler(subscriber_index):
                async def handler(message):
                    nonlocal received_messages
                    payload = json.loads(message["data"])
                    received_by_subscriber[subscriber_index][payload["channel"]].add(
                        payload["seq"]
                    )
                    received_messages += 1

                return handler

            await pubsub.ssubscribe(
                **{channel: make_handler(index) for channel in channels}
            )
            subscribers.append(pubsub)
            reader_tasks.append(asyncio.create_task(read_messages(pubsub)))

        publisher_task = asyncio.create_task(publish_messages())
        logging.info("Async Pub/Sub publisher task started")

        def baseline_messages_received():
            return all(
                all(len(subscriber[channel]) >= 3 for channel in channels)
                for subscriber in received_by_subscriber
            )

        try:
            await async_wait_for_condition(
                baseline_messages_received,
                timeout=BASELINE_TIMEOUT,
                check_interval=0.1,
                error_message=(
                    "Timed out waiting for each subscriber to receive messages"
                ),
            )
        except AssertionError as error:
            raise AssertionError(f"{error}; {progress_message()}") from error
        logging.info("Async Pub/Sub baseline reached: %s", progress_message())

        logging.info(
            "Async Pub/Sub cluster action started: %s", cluster_op_action.__name__
        )
        await asyncio.to_thread(cluster_op_action)
        logging.info("Async Pub/Sub cluster action completed: %s", progress_message())

        await client.nodes_manager.initialize()
        await asyncio.sleep(5)

        recovery_baseline = {
            channel: set(seqs) for channel, seqs in sent_by_channel.items()
        }

        # Set by recovery_window_ready once delivery is observed live again, and the
        # baseline the delivery ratio is measured against from then on. None until then.
        measurement_baseline = None

        def recovery_window_ready():
            nonlocal measurement_baseline
            if measurement_baseline is not None:
                return all(
                    len(sent_by_channel[channel] - measurement_baseline[channel])
                    >= POST_RECOVERY_WINDOW_MESSAGES
                    for channel in channels
                )
            # Every subscriber has to be receiving again on every channel before the
            # window opens. Opening it on what the publisher sent instead - the
            # publisher recovers first, and needs about a second to produce
            # POST_RECOVERY_WINDOW_MESSAGES - puts the reconnect gap inside the window,
            # and those messages are gone for good.
            for channel in channels:
                published = sent_by_channel[channel] - recovery_baseline.get(
                    channel, set()
                )
                for subscriber in received_by_subscriber:
                    if len(subscriber[channel] & published) < DELIVERY_RESUMED_MESSAGES:
                        return False
            measurement_baseline = {
                channel: set(sent_by_channel[channel]) for channel in channels
            }
            logging.info("Async Pub/Sub delivery resumed: %s", progress_message())
            return False

        try:
            await async_wait_for_condition(
                recovery_window_ready,
                timeout=recovery_timeout,
                check_interval=0.1,
                error_message="Timed out waiting for the post-recovery window",
            )
        except AssertionError as error:
            stage = (
                "delivery never resumed"
                if measurement_baseline is None
                else "window never filled"
            )
            raise AssertionError(f"{error} ({stage}); {progress_message()}") from error
        logging.info(
            "Async Pub/Sub post-recovery window ready: %s",
            progress_message(),
        )

        sent_after_recovery = {
            channel: sent_by_channel[channel] - measurement_baseline[channel]
            for channel in channels
        }

        def delivery_ratio_met():
            if any(not sent_after_recovery[channel] for channel in channels):
                return False
            for subscriber in received_by_subscriber:
                for channel in channels:
                    delivered = len(subscriber[channel] & sent_after_recovery[channel])
                    ratio = delivered / len(sent_after_recovery[channel])
                    if ratio < POST_RECOVERY_DELIVERY_RATIO:
                        return False
            return True

        try:
            await async_wait_for_condition(
                delivery_ratio_met,
                timeout=recovery_timeout,
                check_interval=0.1,
                error_message="Timed out waiting for post-recovery delivery ratio",
            )
        except AssertionError as error:
            raise AssertionError(f"{error}; {progress_message()}") from error
        logging.info("Async Pub/Sub delivery ratio reached: %s", progress_message())
    finally:
        stop_event.set()
        tasks = reader_tasks + ([publisher_task] if publisher_task is not None else [])
        for task in tasks:
            task.cancel()
        await asyncio.gather(*tasks, return_exceptions=True)
        for pubsub in subscribers:
            await pubsub.aclose()
        logging.info("Async Pub/Sub scenario stopped: %s", progress_message())


class TestShardedPubSubMigrationScenario:
    @pytest.fixture()
    def pubsub_shards_placement(self):
        """Pack the shards so the migrate action has a node to migrate to.

        See DENSE_SHARDS_PLACEMENT for why sparse leaves it without a target.
        """
        return DENSE_SHARDS_PLACEMENT

    @pytest.mark.timeout(MIGRATION_TEST_TIMEOUT)
    @pytest.mark.parametrize("subscriber_count", [1, 2])
    def test_sharded_pubsub_delivery_after_shard_migration(
        self,
        fault_injector_client_oss_api: FaultInjectorClient,
        subscriber_count,
        cluster_endpoint_config,
        cluster_client,
    ):
        def migrate():
            execute_migration(fault_injector_client_oss_api, cluster_endpoint_config)

        run_sharded_pubsub_scenario(
            cluster_client,
            cluster_endpoint_config,
            channel_prefix="pubsub-migration",
            subscriber_count=subscriber_count,
            cluster_op_action=migrate,
        )


class TestShardedPubSubInfrastructureRecovery:
    @pytest.mark.timeout(INFRASTRUCTURE_RECOVERY_TEST_TIMEOUT)
    @pytest.mark.parametrize("subscriber_count", [1, 2])
    @pytest.mark.parametrize("failure_name, create_action", FAILURE_SCENARIOS)
    def test_sharded_pubsub_recovers_after_infrastructure_failure(
        self,
        fault_injector_client_oss_api: FaultInjectorClient,
        subscriber_count,
        failure_name,
        create_action,
        cluster_endpoint_config,
        cluster_client,
    ):
        def inject_failure():
            execute_failure_scenario(
                fault_injector_client_oss_api,
                create_action,
                cluster_endpoint_config,
            )

        run_sharded_pubsub_scenario(
            cluster_client,
            cluster_endpoint_config,
            channel_prefix=f"pubsub-recovery-{failure_name}",
            subscriber_count=subscriber_count,
            cluster_op_action=inject_failure,
            recovery_timeout=recovery_timeout_for_failure(failure_name),
        )


class TestAsyncShardedPubSubFaultInjectorMigrationScenario:
    @pytest.fixture()
    def pubsub_shards_placement(self):
        """Pack the shards so the migrate action has a node to migrate to.

        See DENSE_SHARDS_PLACEMENT for why sparse leaves it without a target.
        """
        return DENSE_SHARDS_PLACEMENT

    @pytest.mark.asyncio
    @pytest.mark.timeout(MIGRATION_TEST_TIMEOUT)
    @pytest.mark.parametrize("subscriber_count", [1, 2])
    async def test_sharded_pubsub_delivery_after_migration(
        self,
        fault_injector_client_oss_api: FaultInjectorClient,
        subscriber_count,
        cluster_endpoint_config,
        async_cluster_client,
    ):
        def migrate():
            execute_migration(fault_injector_client_oss_api, cluster_endpoint_config)

        await async_run_sharded_pubsub_recovery_scenario(
            async_cluster_client,
            cluster_endpoint_config,
            channel_prefix="async-pubsub-migration",
            subscriber_count=subscriber_count,
            cluster_op_action=migrate,
        )


class TestAsyncShardedPubSubInfrastructureRecovery:
    @pytest.mark.asyncio
    @pytest.mark.timeout(INFRASTRUCTURE_RECOVERY_TEST_TIMEOUT)
    @pytest.mark.parametrize("subscriber_count", [1, 2])
    @pytest.mark.parametrize("failure_name, create_action", FAILURE_SCENARIOS)
    async def test_sharded_pubsub_recovers_after_infrastructure_failure(
        self,
        fault_injector_client_oss_api: FaultInjectorClient,
        subscriber_count,
        failure_name,
        create_action,
        cluster_endpoint_config,
        async_cluster_client,
    ):
        def inject_failure():
            execute_failure_scenario(
                fault_injector_client_oss_api,
                create_action,
                cluster_endpoint_config,
            )

        await async_run_sharded_pubsub_recovery_scenario(
            async_cluster_client,
            cluster_endpoint_config,
            channel_prefix=f"async-pubsub-recovery-{failure_name}",
            subscriber_count=subscriber_count,
            cluster_op_action=inject_failure,
            recovery_timeout=recovery_timeout_for_failure(failure_name),
        )


# Collected from the live fault injector at import time. TLS variants are dropped
# because the Pub/Sub client rejects rediss:// endpoints, and the injector's two
# endpoint-type variants of each pair are collapsed to one. All four slot-migrate
# effects are covered: slot-shuffle moves slots between existing nodes, add brings in a
# node that did not exist when the channels were subscribed, remove takes the owning
# node out of the topology, and remove-add does both. No skip_combinations are passed:
# the maint-notification suite skips pairs whose maintenance window closes too fast to
# observe a notification on a freshly opened connection, but these tests measure
# end-to-end delivery after the operation completes, so a short window is still a valid
# case. When the deployment offers nothing usable (fault injector unreachable, or every
# combination filtered out) the suite reports a skip rather than silently collecting
# zero tests.
SLOT_MIGRATE_EFFECT_PARAMS = dedupe_effect_params(
    generate_params(
        _FAULT_INJECTOR_CLIENT_OSS_API,
        [
            SlotMigrateEffects.SLOT_SHUFFLE,
            SlotMigrateEffects.ADD,
            SlotMigrateEffects.REMOVE,
            SlotMigrateEffects.REMOVE_ADD,
        ],
        include_tls=False,
    )
) or [
    pytest.param(
        None,
        None,
        None,
        None,
        marks=pytest.mark.skip(
            reason="fault injector returned no usable slot-migrate effect/trigger params"
        ),
    )
]
# The effect matrix already varies shards_count from 1 to 10, which exercises the
# reconciler harder than a second subscriber does, and the infrastructure-recovery
# classes above already cover subscriber_count 2 on both stacks. One subscriber here
# keeps a matrix that creates and deletes a database per case affordable.
EFFECT_TRIGGER_SUBSCRIBER_COUNT = 1


class TestShardedPubSubTopologyChangeWithEffectTrigger:
    """Sharded Pub/Sub delivery across fault-injector effects and triggers.

    The infrastructure-recovery class above drives unplanned faults as plain actions.
    These cases are the opposite kind of event: planned topology changes, which the
    fault injector models as an effect (what changes) plus a trigger (how it is
    caused), and for which it supplies the database config each combination needs.
    Parameters therefore come from the fault injector rather than from a local bdb
    config, and a combination the deployment does not support never appears.
    """

    @pytest.fixture(autouse=True)
    def setup_and_cleanup(self):
        self._bdb_name = None
        self._client = None

        yield

        if self._client is not None:
            self._client.close()
        if self._bdb_name:
            delete_database_if_exists(_FAULT_INJECTOR_CLIENT_OSS_API, self._bdb_name)

    def setup_env(
        self,
        fault_injector_client: FaultInjectorClient,
        db_config: dict[str, Any],
    ):
        """Create the database the effect/trigger requires and a client for it."""
        self._bdb_name = db_config["name"]
        endpoint_config = create_effect_database(fault_injector_client, db_config)
        self._client = get_cluster_client(endpoints_config=endpoint_config)
        return self._client, endpoint_config

    @pytest.mark.timeout(EFFECT_TRIGGER_TEST_TIMEOUT)
    @pytest.mark.parametrize(
        "effect_name, trigger, db_config, db_name", SLOT_MIGRATE_EFFECT_PARAMS
    )
    def test_sharded_pubsub_delivery_during_slot_migration(
        self,
        fault_injector_client_oss_api: FaultInjectorClient,
        effect_name,
        trigger,
        db_config,
        db_name,
    ):
        """Slots move between nodes; nodes may also be added or removed.

        Sharded Pub/Sub must keep delivering: the client has to notice the slot map
        change and re-subscribe the affected channels on their new owner, creating a
        per-node PubSub for a node that did not exist at subscribe time (add) and
        dropping one whose node left the topology (remove).
        """
        logging.info(
            "DB name: %s | effect=%s trigger=%s", db_name, effect_name, trigger
        )

        client, endpoint_config = self.setup_env(
            fault_injector_client_oss_api, db_config
        )

        def migrate_slots():
            execute_effect_trigger(
                fault_injector_client_oss_api, endpoint_config, effect_name, trigger
            )

        run_sharded_pubsub_scenario(
            client,
            endpoint_config,
            channel_prefix=f"pubsub-{effect_name.value}-{trigger}",
            subscriber_count=EFFECT_TRIGGER_SUBSCRIBER_COUNT,
            cluster_op_action=migrate_slots,
            recovery_timeout=recovery_timeout_for_effect(effect_name),
        )


class TestAsyncShardedPubSubTopologyChangeWithEffectTrigger:
    """Async mirror of TestShardedPubSubTopologyChangeWithEffectTrigger.

    Same fault-injector effects and triggers, driven through the async cluster client,
    so the async reconciliation path is exercised by a planned topology change and not
    only by the unplanned faults above. The fault injector client is the sync one:
    AsyncFaultInjectorClient has no migrate, generate_params is synchronous and runs at
    collection time, and the driver already pushes the blocking cluster action onto a
    worker thread, so an async fault-injector surface would buy nothing here.
    """

    @pytest_asyncio.fixture(autouse=True)
    async def setup_and_cleanup(self):
        self._bdb_name = None
        self._client = None

        yield

        if self._client is not None:
            await self._client.aclose()
        if self._bdb_name:
            delete_database_if_exists(_FAULT_INJECTOR_CLIENT_OSS_API, self._bdb_name)

    def setup_env(
        self,
        fault_injector_client: FaultInjectorClient,
        db_config: dict[str, Any],
    ):
        """Create the database the effect/trigger requires and a client for it.

        Stays synchronous even though it builds an async client: the fault-injector
        calls block, but they run before any publisher or reader task exists, so there
        is no event loop to starve. ClusterPubSub.ssubscribe awaits its own
        _ensure_cluster_initialized, so no explicit initialize() is needed either.
        """
        self._bdb_name = db_config["name"]
        endpoint_config = create_effect_database(fault_injector_client, db_config)
        self._client = get_cluster_client(
            endpoints_config=endpoint_config,
            client_class=AsyncRedisCluster,
            retry_class=AsyncRetry,
        )
        return self._client, endpoint_config

    @pytest.mark.asyncio
    @pytest.mark.timeout(EFFECT_TRIGGER_TEST_TIMEOUT)
    @pytest.mark.parametrize(
        "effect_name, trigger, db_config, db_name", SLOT_MIGRATE_EFFECT_PARAMS
    )
    async def test_sharded_pubsub_delivery_during_slot_migration(
        self,
        fault_injector_client_oss_api: FaultInjectorClient,
        effect_name,
        trigger,
        db_config,
        db_name,
    ):
        """Slots move between nodes; nodes may also be added or removed.

        Sharded Pub/Sub must keep delivering: the client has to notice the slot map
        change and re-subscribe the affected channels on their new owner, creating a
        per-node PubSub for a node that did not exist at subscribe time (add) and
        dropping one whose node left the topology (remove).
        """
        logging.info(
            "DB name: %s | effect=%s trigger=%s", db_name, effect_name, trigger
        )

        client, endpoint_config = self.setup_env(
            fault_injector_client_oss_api, db_config
        )

        def migrate_slots():
            execute_effect_trigger(
                fault_injector_client_oss_api, endpoint_config, effect_name, trigger
            )

        await async_run_sharded_pubsub_recovery_scenario(
            client,
            endpoint_config,
            channel_prefix=f"async-pubsub-{effect_name.value}-{trigger}",
            subscriber_count=EFFECT_TRIGGER_SUBSCRIBER_COUNT,
            cluster_op_action=migrate_slots,
            recovery_timeout=recovery_timeout_for_effect(effect_name),
        )
