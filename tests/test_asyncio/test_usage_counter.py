import asyncio
from unittest.mock import AsyncMock

import pytest
from redis.asyncio import Redis, RedisCluster


@pytest.mark.asyncio
async def test_usage_counter(r):
    async def dummy_task():
        async with r:
            await asyncio.sleep(0.01)

    tasks = [dummy_task() for _ in range(20)]
    await asyncio.gather(*tasks)

    # After all tasks have completed, the usage counter should be back to zero.
    assert r._usage_counter == 0


@pytest.fixture(params=[Redis, RedisCluster])
def client(request, monkeypatch):
    client = request.param(host="localhost", port=6379)
    monkeypatch.setattr(client, "initialize", AsyncMock(return_value=client))
    return client


@pytest.mark.fixed_client
async def test_cancelled_context_entry_releases_usage(client, monkeypatch):
    initializing = asyncio.Event()

    async def initialize():
        initializing.set()
        await asyncio.Event().wait()

    monkeypatch.setattr(client, "initialize", initialize)
    close = AsyncMock()
    monkeypatch.setattr(client, "aclose", close)

    async def use_client():
        async with client:
            pytest.fail("Cancelled initialization must not enter the context")

    task = asyncio.create_task(use_client())
    await initializing.wait()
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task

    assert client._usage_counter == 0
    close.assert_not_awaited()

    monkeypatch.setattr(client, "initialize", AsyncMock(return_value=client))
    async with client as entered:
        assert entered is client
    assert client._usage_counter == 0
    close.assert_awaited_once()


@pytest.mark.fixed_client
@pytest.mark.parametrize("cancel_exit", [False, True])
async def test_context_entry_waits_for_previous_close(client, monkeypatch, cancel_exit):
    closing = asyncio.Event()
    finish_close = asyncio.Event()
    entering = asyncio.Event()
    entered = asyncio.Event()
    finish_context = asyncio.Event()

    async def close():
        closing.set()
        await finish_close.wait()

    monkeypatch.setattr(client, "aclose", close)

    async def first_context():
        async with client:
            pass

    async def second_context():
        entering.set()
        async with client:
            entered.set()
            await finish_context.wait()

    first = asyncio.create_task(first_context())
    await closing.wait()
    if cancel_exit:
        first.cancel()
        with pytest.raises(asyncio.CancelledError):
            await first

    second = asyncio.create_task(second_context())
    try:
        await entering.wait()
        assert not entered.is_set()
        finish_close.set()
        if not cancel_exit:
            await first
        await entered.wait()
        assert client._usage_counter == 1
    finally:
        finish_close.set()
        finish_context.set()
        await asyncio.gather(first, second, return_exceptions=True)

    assert client._usage_counter == 0


@pytest.mark.fixed_client
async def test_cancelled_context_exit_still_closes_client(client, monkeypatch):
    close = AsyncMock()
    monkeypatch.setattr(client, "aclose", close)
    released = asyncio.Event()
    release_usage = client._release_usage

    async def release():
        try:
            return await release_usage()
        finally:
            released.set()

    monkeypatch.setattr(client, "_release_usage", release)
    await client.__aenter__()

    async with client._usage_lock:
        exiting = asyncio.Event()

        async def exit_context():
            exiting.set()
            await client.__aexit__(None, None, None)

        task = asyncio.create_task(exit_context())
        await exiting.wait()
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task

    await released.wait()
    assert client._usage_counter == 0
    close.assert_awaited_once()


@pytest.mark.fixed_client
async def test_failed_context_entry_preserves_active_context(client, monkeypatch):
    close = AsyncMock()
    monkeypatch.setattr(client, "aclose", close)

    async with client:
        monkeypatch.setattr(
            client, "initialize", AsyncMock(side_effect=RuntimeError("initialization"))
        )
        with pytest.raises(RuntimeError, match="initialization"):
            async with client:
                pytest.fail("Failed initialization must not enter the context")
        assert client._usage_counter == 1
        close.assert_not_awaited()

    close.assert_awaited_once()


@pytest.mark.fixed_client
@pytest.mark.parametrize("error_type", [RuntimeError, asyncio.CancelledError])
async def test_failed_context_entry_does_not_close_client(
    client, monkeypatch, error_type
):
    error = error_type("initialization")
    monkeypatch.setattr(client, "initialize", AsyncMock(side_effect=error))
    close = AsyncMock()
    monkeypatch.setattr(client, "aclose", close)

    with pytest.raises(error_type) as exc_info:
        async with client:
            pytest.fail("Failed initialization must not enter the context")

    assert exc_info.value is error
    assert client._usage_counter == 0
    close.assert_not_awaited()


@pytest.mark.fixed_client
async def test_nested_context_exit_preserves_active_context(client, monkeypatch):
    close = AsyncMock()
    monkeypatch.setattr(client, "aclose", close)

    async with client:
        async with client:
            assert client._usage_counter == 2
        assert client._usage_counter == 1
        close.assert_not_awaited()

    assert client._usage_counter == 0
    close.assert_awaited_once()


@pytest.mark.fixed_client
async def test_context_can_be_reused_after_close_failure(client, monkeypatch):
    close = AsyncMock(side_effect=[RuntimeError("closing"), None])
    monkeypatch.setattr(client, "aclose", close)

    with pytest.raises(RuntimeError, match="closing"):
        async with client:
            pass

    async with client:
        assert client._usage_counter == 1

    assert client._usage_counter == 0
    assert close.await_count == 2


@pytest.mark.fixed_client
async def test_repeated_context_entry_cancellation_still_cleans_up(client, monkeypatch):
    initializing = asyncio.Event()
    cleaning_up = asyncio.Event()
    cleaned_up = asyncio.Event()
    close = AsyncMock()
    monkeypatch.setattr(client, "aclose", close)
    decrement_usage = client._decrement_usage

    async def initialize():
        initializing.set()
        await asyncio.Event().wait()

    async def decrement():
        cleaning_up.set()
        try:
            return await decrement_usage()
        finally:
            cleaned_up.set()

    monkeypatch.setattr(client, "initialize", initialize)
    monkeypatch.setattr(client, "_decrement_usage", decrement)
    task = asyncio.create_task(client.__aenter__())
    await initializing.wait()

    async with client._usage_lock:
        task.cancel()
        await cleaning_up.wait()
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task

    await cleaned_up.wait()
    assert client._usage_counter == 0
    close.assert_not_awaited()


@pytest.mark.fixed_client
async def test_cancelled_context_entry_waiting_for_lock_does_not_change_usage(
    client, monkeypatch
):
    close = AsyncMock()
    monkeypatch.setattr(client, "aclose", close)
    entering = asyncio.Event()

    async def enter_context():
        entering.set()
        await client.__aenter__()

    async with client._usage_lock:
        task = asyncio.create_task(enter_context())
        await entering.wait()
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task

    assert client._usage_counter == 0
    close.assert_not_awaited()
