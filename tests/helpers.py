import logging
import time
from time import sleep
from typing import Callable

from redis.commands.metadata import RequestPolicy, ResponsePolicy


def wait_for_condition(
    predicate: Callable[[], bool],
    timeout: float = 5.0,
    check_interval: float = 0.01,
    error_message: str = "Timeout waiting for condition",
) -> None:
    """
    Poll a condition until it becomes True or timeout is reached.

    Args:
        predicate: A callable that returns True when the condition is met
        timeout: Maximum time to wait in seconds (default: 5s). A passing
            condition returns as soon as it holds, so a long deadline only
            costs time on a stalled runner.
        check_interval: Time to sleep between checks in seconds (default: 0.01s)
        error_message: Error message to raise if timeout occurs

    Raises:
        AssertionError: If the condition is not met within the timeout period

    Example:
        # Wait for circuit breaker to open
        wait_for_condition(
            lambda: cb2.state == CBState.OPEN,
            timeout=5.0,
            error_message="Timeout waiting for cb2 to open"
        )

        # Wait for failover strategy to select a specific database
        wait_for_condition(
            lambda: client.command_executor.active_database is mock_db,
            timeout=5.0,
            error_message="Timeout waiting for active database to change"
        )
    """
    # Bounded by elapsed time, not by an iteration count: a slow predicate or a
    # late wakeup must not stretch or shorten the deadline.
    deadline = time.monotonic() + timeout
    attempt = 0
    while True:
        if predicate():
            logging.debug(f"Condition met after {attempt} attempts")
            return
        if time.monotonic() >= deadline:
            raise AssertionError(error_message)
        attempt += 1
        sleep(check_interval)


def get_expected_command_policies(changes_in_defaults={}):
    default_cmd_policies = {
        "core": {
            "keys": [
                "keys",
                RequestPolicy.ALL_SHARDS,
                ResponsePolicy.DEFAULT_KEYLESS,
            ],
            "acl setuser": [
                "acl setuser",
                RequestPolicy.ALL_NODES,
                ResponsePolicy.ALL_SUCCEEDED,
            ],
            "exists": ["exists", RequestPolicy.MULTI_SHARD, ResponsePolicy.AGG_SUM],
            "config resetstat": [
                "config resetstat",
                RequestPolicy.ALL_NODES,
                ResponsePolicy.ALL_SUCCEEDED,
            ],
            "slowlog len": [
                "slowlog len",
                RequestPolicy.ALL_NODES,
                ResponsePolicy.AGG_SUM,
            ],
            "scan": ["scan", RequestPolicy.SPECIAL, ResponsePolicy.SPECIAL],
            "latency history": [
                "latency history",
                RequestPolicy.ALL_NODES,
                ResponsePolicy.SPECIAL,
            ],
            "memory doctor": [
                "memory doctor",
                RequestPolicy.ALL_SHARDS,
                ResponsePolicy.SPECIAL,
            ],
            "randomkey": [
                "randomkey",
                RequestPolicy.ALL_SHARDS,
                ResponsePolicy.SPECIAL,
            ],
            "mget": [
                "mget",
                RequestPolicy.MULTI_SHARD,
                ResponsePolicy.DEFAULT_KEYED,
            ],
            "function restore": [
                "function restore",
                RequestPolicy.ALL_SHARDS,
                ResponsePolicy.ALL_SUCCEEDED,
            ],
        },
        "json": {
            "debug": [
                "debug",
                RequestPolicy.DEFAULT_KEYED,
                ResponsePolicy.DEFAULT_KEYED,
            ],
            "get": [
                "get",
                RequestPolicy.DEFAULT_KEYED,
                ResponsePolicy.DEFAULT_KEYED,
            ],
        },
        "ft": {
            "search": [
                "search",
                RequestPolicy.DEFAULT_KEYLESS,
                ResponsePolicy.DEFAULT_KEYLESS,
            ],
            "create": [
                "create",
                RequestPolicy.DEFAULT_KEYLESS,
                ResponsePolicy.DEFAULT_KEYLESS,
            ],
        },
        "bf": {
            "add": [
                "add",
                RequestPolicy.DEFAULT_KEYED,
                ResponsePolicy.DEFAULT_KEYED,
            ],
            "madd": [
                "madd",
                RequestPolicy.DEFAULT_KEYED,
                ResponsePolicy.DEFAULT_KEYED,
            ],
        },
        "cf": {
            "add": [
                "add",
                RequestPolicy.DEFAULT_KEYED,
                ResponsePolicy.DEFAULT_KEYED,
            ],
            "mexists": [
                "mexists",
                RequestPolicy.DEFAULT_KEYED,
                ResponsePolicy.DEFAULT_KEYED,
            ],
        },
        "tdigest": {
            "add": [
                "add",
                RequestPolicy.DEFAULT_KEYED,
                ResponsePolicy.DEFAULT_KEYED,
            ],
            "min": [
                "min",
                RequestPolicy.DEFAULT_KEYED,
                ResponsePolicy.DEFAULT_KEYED,
            ],
        },
        "ts": {
            "create": [
                "create",
                RequestPolicy.DEFAULT_KEYED,
                ResponsePolicy.DEFAULT_KEYED,
            ],
            "info": [
                "info",
                RequestPolicy.DEFAULT_KEYED,
                ResponsePolicy.DEFAULT_KEYED,
            ],
        },
        "topk": {
            "list": [
                "list",
                RequestPolicy.DEFAULT_KEYED,
                ResponsePolicy.DEFAULT_KEYED,
            ],
            "query": [
                "query",
                RequestPolicy.DEFAULT_KEYED,
                ResponsePolicy.DEFAULT_KEYED,
            ],
        },
    }
    default_cmd_policies.update(changes_in_defaults)
    return default_cmd_policies
