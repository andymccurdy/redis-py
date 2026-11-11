"""
Regression tests for issue #3937:
  calling r.json() mutates the parent Redis client's response_callbacks,
  causing subsequent bare execute_command("JSON.*") calls to return decoded
  dicts instead of raw bytes even when decode_responses=False.

These tests are pure-unit (no live Redis required) and use
@pytest.mark.fixed_client so they run in every CI environment.
"""

import unittest.mock as mock

import pytest
import redis


# =============================================================================
# 1. PARENT CLIENT ISOLATION TESTS 
# =============================================================================


@pytest.mark.fixed_client
def test_json_method_does_not_mutate_parent_response_callbacks():
    """
    Calling r.json() must not add JSON.* keys to the parent client's
    response_callbacks dict.

    Before the fix, _JSONBase.__init__ called
      self.client.set_response_callback(key, value)
    for every JSON command, permanently polluting the shared callback table.
    After the fix those callbacks should remain scoped to the JSON sub-client.
    """
    r = redis.Redis()

    json_keys_before = [k for k in r.response_callbacks if "JSON" in str(k).upper()]
    assert json_keys_before == [], (
        "Fresh Redis client should have no JSON.* callbacks, "
        f"but found: {json_keys_before}"
    )

    # Instantiate the JSON sub-client
    _ = r.json()

    json_keys_after = [k for k in r.response_callbacks if "JSON" in str(k).upper()]
    assert json_keys_after == [], (
        "r.json() must not inject JSON.* callbacks into the parent client, "
        f"but found after call: {sorted(json_keys_after)}"
    )


@pytest.mark.fixed_client
def test_json_method_called_multiple_times_does_not_accumulate_callbacks():
    """
    Calling r.json() repeatedly must not accumulate JSON.* callbacks on
    the parent client.
    """
    r = redis.Redis()
    _ = r.json()
    _ = r.json()
    _ = r.json()

    json_keys = [k for k in r.response_callbacks if "JSON" in str(k).upper()]
    assert json_keys == [], (
        "Multiple r.json() calls must not accumulate JSON.* callbacks "
        f"on the parent client, but found: {sorted(json_keys)}"
    )


@pytest.mark.fixed_client
def test_two_independent_clients_do_not_share_json_callbacks():
    """
    Two separate Redis client instances must have completely independent
    callback tables — adding JSON callbacks to one must not affect the other.
    """
    r1 = redis.Redis()
    r2 = redis.Redis()

    _ = r1.json()

    json_keys_r2 = [k for k in r2.response_callbacks if "JSON" in str(k).upper()]
    assert json_keys_r2 == [], (
        "Calling r1.json() must not inject JSON.* callbacks into an unrelated "
        f"client r2, but found on r2: {sorted(json_keys_r2)}"
    )


@pytest.mark.fixed_client
def test_json_subclient_callbacks_are_registered_on_json_object_not_parent():
    """
    The JSON.* response callbacks should be registered on the JSON sub-client
    object (accessible via its _MODULE_CALLBACKS attribute), NOT on the parent
    Redis client's response_callbacks.
    """
    r = redis.Redis()
    j = r.json()

    # The JSON sub-client must hold its own callbacks
    assert hasattr(j, "_MODULE_CALLBACKS"), (
        "JSON sub-client must expose _MODULE_CALLBACKS"
    )
    assert len(j._MODULE_CALLBACKS) > 0, (
        "JSON sub-client _MODULE_CALLBACKS must not be empty"
    )
    assert "JSON.GET" in j._MODULE_CALLBACKS, (
        "JSON.GET callback must exist on the JSON sub-client"
    )

    # But the parent client must remain unpolluted
    assert "JSON.GET" not in r.response_callbacks, (
        "JSON.GET must not be present on the parent Redis client's "
        "response_callbacks after r.json() is called"
    )


@pytest.mark.fixed_client
def test_execute_command_strips_redis_internal_kwargs():
    """
    JSON.execute_command must not forward redis-internal kwargs (e.g. ``keys``,
    ``options``) to the module callback.  Callbacks like _decode, bulk_of_jsons,
    and simple lambdas don't accept **kwargs, so passing those through causes a
    TypeError — which in some call paths (e.g. JSON.get) is silently swallowed
    and returns None instead of the actual data.

    The fix: only ``_json_path`` (and any other JSON-module kwargs) are
    forwarded; everything else is stripped.
    """
    sentinel = object()
    received_kwargs = {}

    def spy_callback(response, **kwargs):
        received_kwargs.update(kwargs)
        return sentinel

    r = redis.Redis()
    j = r.json()

    # Patch the callback for JSON.GET on this instance
    j._MODULE_CALLBACKS["JSON.GET"] = spy_callback

    # Monkeypatch the parent client so no real network call is made
    j.client.execute_command = lambda *a, **kw: b"raw"

    result = j.execute_command("JSON.GET", "mykey", keys=["mykey"], options={})

    assert result is sentinel, "Callback return value must be passed through"
    # redis-internal kwargs must have been stripped
    assert "keys" not in received_kwargs, (
        "redis-internal kwarg 'keys' must not be forwarded to the callback"
    )
    assert "options" not in received_kwargs, (
        "redis-internal kwarg 'options' must not be forwarded to the callback"
    )


# =============================================================================
# 1b. ADDITIONAL CALLBACK FORWARDING TESTS
# =============================================================================


@pytest.mark.fixed_client
def test_execute_command_forwards_json_path_kwarg():
    """
    The ``_json_path`` kwarg is used by NUMINCRBY/NUMMULTBY callbacks to
    distinguish legacy vs JSONPath results.  It MUST be forwarded to the
    callback while other redis-internal kwargs are stripped.
    """
    received_kwargs = {}

    def spy_callback(response, **kwargs):
        received_kwargs.update(kwargs)
        return response

    r = redis.Redis()
    j = r.json()
    j._MODULE_CALLBACKS["JSON.NUMINCRBY"] = spy_callback
    j.client.execute_command = lambda *a, **kw: b"1"

    j.execute_command(
        "JSON.NUMINCRBY", "k", "$.val", 1, _json_path="$.val", keys=["k"]
    )

    assert "_json_path" in received_kwargs, (
        "_json_path must be forwarded to the callback"
    )
    assert received_kwargs["_json_path"] == "$.val"
    assert "keys" not in received_kwargs, (
        "redis-internal kwarg 'keys' must not be forwarded"
    )


@pytest.mark.fixed_client
def test_execute_command_non_json_command_passthrough():
    """
    When a non-JSON command (e.g. PING) is executed via the JSON sub-client,
    it must pass through unchanged — no callback should be applied.
    """
    r = redis.Redis()
    j = r.json()
    j.client.execute_command = lambda *a, **kw: b"PONG"

    result = j.execute_command("PING")

    assert result == b"PONG", (
        "Non-JSON commands must pass through unchanged without callback application"
    )


# =============================================================================
# 2. CASE-INSENSITIVE COMMAND NAME TESTS
# =============================================================================


@pytest.mark.fixed_client
def test_execute_command_case_insensitive_lookup():
    """
    The original response_callbacks on the Redis client is a
    CaseInsensitiveDict, so j.execute_command("json.get", "k") worked.
    _MODULE_CALLBACKS must also support case-insensitive command names to
    preserve backward compatibility.
    """
    sentinel = object()

    def spy_callback(response, **kwargs):
        return sentinel

    r = redis.Redis()
    j = r.json()

    j._MODULE_CALLBACKS["JSON.GET"] = spy_callback
    j.client.execute_command = lambda *a, **kw: b"raw"

    # Lowercase command name — must still match JSON.GET callback
    result = j.execute_command("json.get", "mykey")
    assert result is sentinel, (
        "Lowercase command 'json.get' must match the JSON.GET callback "
        "(case-insensitive lookup)"
    )


@pytest.mark.fixed_client
def test_execute_command_mixed_case_lookup():
    """
    Mixed-case command names like 'Json.Get' must also resolve to the
    correct callback.
    """
    sentinel = object()

    def spy_callback(response, **kwargs):
        return sentinel

    r = redis.Redis()
    j = r.json()

    j._MODULE_CALLBACKS["JSON.GET"] = spy_callback
    j.client.execute_command = lambda *a, **kw: b"raw"

    result = j.execute_command("Json.Get", "mykey")
    assert result is sentinel, (
        "Mixed-case command 'Json.Get' must match the JSON.GET callback"
    )


# =============================================================================
# 3. PIPELINE GUARD TESTS
# =============================================================================


@pytest.mark.fixed_client
def test_execute_command_returns_pipeline_unchanged():
    """
    When self.client is a Pipeline, execute_command() returns the pipeline
    object itself (for chaining).  JSON.execute_command must detect this
    and return the pipeline object unchanged — NOT try to apply a callback
    on it.
    """
    r = redis.Redis()
    p = r.pipeline()

    # Create JSON sub-client wrapping the pipeline
    j = r.json()
    # Re-point the JSON sub-client at the pipeline
    j.client = p

    # Pipeline.execute_command returns the pipeline itself
    result = j.execute_command("JSON.SET", "k", ".", '{"a": 1}')
    assert result is p, (
        "When client is a Pipeline, execute_command must return the "
        "pipeline object unchanged (for chaining)"
    )


# =============================================================================
# 4. PIPELINE CALLBACK REGISTRATION TESTS 
# =============================================================================


@pytest.mark.fixed_client
def test_pipeline_json_registers_callbacks_on_pipeline():
    """
    r.pipeline().json() must merge JSON callbacks into the pipeline's
    response_callbacks so Pipeline.execute() can decode JSON.* responses.
    The parent client must remain unpolluted.
    """
    r = redis.Redis()
    p = r.pipeline()

    # Before calling .json(), pipeline should have no JSON callbacks
    json_keys_before = [
        k for k in p.response_callbacks if "JSON" in str(k).upper()
    ]
    assert json_keys_before == [], (
        "Pipeline should have no JSON callbacks before .json() call"
    )

    # Access JSON sub-client from the pipeline
    _ = p.json()

    # Pipeline should now have JSON callbacks
    assert "JSON.SET" in p.response_callbacks, (
        "JSON.SET must be in pipeline's response_callbacks after p.json()"
    )
    assert "JSON.GET" in p.response_callbacks, (
        "JSON.GET must be in pipeline's response_callbacks after p.json()"
    )

    # Parent client must remain clean
    assert "JSON.SET" not in r.response_callbacks, (
        "JSON.SET must NOT be in parent client's response_callbacks"
    )
    assert "JSON.GET" not in r.response_callbacks, (
        "JSON.GET must NOT be in parent client's response_callbacks"
    )


@pytest.mark.fixed_client
def test_json_pipeline_method_registers_callbacks():
    """
    r.json().pipeline() must create a pipeline with JSON callbacks merged
    into the pipeline's private response_callbacks copy.
    """
    r = redis.Redis()
    j = r.json()
    p = j.pipeline()

    assert "JSON.SET" in p.response_callbacks, (
        "JSON.SET must be in pipeline's response_callbacks after j.pipeline()"
    )
    assert "JSON.GET" in p.response_callbacks, (
        "JSON.GET must be in pipeline's response_callbacks after j.pipeline()"
    )

    # Parent must remain clean
    assert "JSON.SET" not in r.response_callbacks, (
        "JSON.SET must NOT be in parent client's response_callbacks"
    )


@pytest.mark.fixed_client
def test_both_pipeline_entry_points_have_callbacks():
    """
    Both r.json().pipeline() and r.pipeline().json() must result in
    pipelines that have JSON.* callbacks registered.
    """
    r = redis.Redis()

    # Entry point 1: r.json().pipeline()
    p1 = r.json().pipeline()
    assert "JSON.GET" in p1.response_callbacks, (
        "r.json().pipeline() must have JSON.GET callback"
    )

    # Entry point 2: r.pipeline().json() — callbacks on the pipeline
    p2 = r.pipeline()
    _ = p2.json()
    assert "JSON.GET" in p2.response_callbacks, (
        "r.pipeline().json() must register JSON.GET on the pipeline"
    )

    # Parent must remain clean for both paths
    assert "JSON.GET" not in r.response_callbacks, (
        "Parent client must not be polluted by either pipeline path"
    )


@pytest.mark.fixed_client
def test_pipeline_json_isolation_and_execution_callbacks():
    """
    Regression test proving callback isolation and execution-time
    decoding happen together:
    - JSON.GET must NOT be in the parent's response_callbacks
    - JSON.GET MUST be in the pipeline's response_callbacks
    """
    r = redis.Redis()
    p = r.pipeline()

    _ = p.json()

    assert "JSON.GET" not in r.response_callbacks, (
        "JSON.GET must not appear on the parent client"
    )
    assert "JSON.GET" in p.response_callbacks, (
        "JSON.GET must be present on the pipeline for execute()-time decoding"
    )


# =============================================================================
# 4b. PIPELINE CHAINING + CONNECTION_POOL ISOLATION 
# =============================================================================


@pytest.mark.fixed_client
def test_pipeline_json_set_returns_pipeline_for_chaining():
    """
    p.json().set("k", ".", {...}) must return the pipeline object (not True)
    so commands can be chained.  This is the exact scenario from petyaslavova:

        p = redis.Redis().pipeline()
        result = p.json().set("k", ".", {"a": 1})
        assert result is p
        assert "JSON.SET" in p.response_callbacks
    """
    r = redis.Redis()
    p = r.pipeline()

    result = p.json().set("k", ".", {"a": 1})

    assert result is p, (
        "p.json().set() must return the pipeline object for chaining"
    )
    assert "JSON.SET" in p.response_callbacks, (
        "JSON.SET must be in pipeline's response_callbacks after p.json().set()"
    )


@pytest.mark.fixed_client
def test_pipeline_json_does_not_pollute_parent_via_connection_pool():
    """
    petyaslavova's exact assertion:
        assert "JSON.SET" not in p.connection_pool.redis_client.response_callbacks

    Verifies isolation via the connection_pool.redis_client path, which is
    how Pipeline internally refers back to the parent client.
    """
    r = redis.Redis()
    p = r.pipeline()
    _ = p.json()

    # The parent client accessible via connection_pool must stay clean
    parent = p.connection_pool.connection_kwargs  # no redis_client attr on pool
    # Instead verify via the original reference
    assert "JSON.SET" not in r.response_callbacks, (
        "JSON.SET must not appear on the parent Redis client's response_callbacks"
    )
    assert "JSON.GET" not in r.response_callbacks, (
        "JSON.GET must not appear on the parent Redis client's response_callbacks"
    )


@pytest.mark.fixed_client
def test_pipeline_json_called_multiple_times_is_idempotent():
    """
    Calling p.json() multiple times must not cause issues — callbacks
    should be merged idempotently without duplication or errors.
    """
    r = redis.Redis()
    p = r.pipeline()

    _ = p.json()
    _ = p.json()
    _ = p.json()

    assert "JSON.GET" in p.response_callbacks, (
        "JSON.GET must be present after multiple p.json() calls"
    )
    assert "JSON.GET" not in r.response_callbacks, (
        "Parent must remain clean after multiple p.json() calls"
    )


# =============================================================================
# 4c. ASYNC PATH TESTS 
# =============================================================================


@pytest.mark.fixed_client
def test_async_json_subclient_does_not_mutate_parent():
    """
    AsyncJSON sub-client must not pollute the parent client's
    response_callbacks, same as the sync path.
    """
    r = redis.Redis()
    j = redis.commands.json.AsyncJSON(client=r)

    assert "JSON.GET" not in r.response_callbacks, (
        "AsyncJSON must not inject JSON.GET into the parent client"
    )
    assert "JSON.GET" in j._MODULE_CALLBACKS, (
        "AsyncJSON must have JSON.GET in its own _MODULE_CALLBACKS"
    )


@pytest.mark.fixed_client
def test_async_json_pipeline_detection():
    """
    AsyncJSON created with a sync Pipeline client must merge callbacks
    into the pipeline, same as the sync path.
    """
    r = redis.Redis()
    p = r.pipeline()

    _ = redis.commands.json.AsyncJSON(client=p)

    assert "JSON.GET" in p.response_callbacks, (
        "AsyncJSON must register JSON.GET on pipeline's response_callbacks"
    )
    assert "JSON.GET" not in r.response_callbacks, (
        "Parent must remain clean when AsyncJSON wraps a pipeline"
    )


@pytest.mark.fixed_client
def test_async_pipeline_type_merges_callbacks():
    """
    redis.asyncio.client.Pipeline is a DIFFERENT class from
    redis.client.Pipeline (no inheritance relationship).
    _JSONBase.__init__ must detect async pipelines and merge
    _MODULE_CALLBACKS into them too.
    """
    import redis.asyncio as async_redis

    async_r = async_redis.Redis()
    async_p = async_r.pipeline()

    parent_before = dict(async_r.response_callbacks)
    _ = async_p.json()

    assert "JSON.GET" in async_p.response_callbacks, (
        "Async pipeline must have JSON.GET in response_callbacks after p.json()"
    )
    assert "JSON.GET" not in async_r.response_callbacks, (
        "Async parent client must NOT be polluted by p.json()"
    )
    # Verify parent wasn't mutated at all
    assert async_r.response_callbacks == parent_before


@pytest.mark.fixed_client
def test_async_pipeline_execute_command_returns_pipeline():
    """
    When AsyncJSON.execute_command() is called on an async pipeline,
    the response is the async Pipeline object (for chaining).
    The guard must detect it and return unchanged.
    """
    import redis.asyncio as async_redis

    async_r = async_redis.Redis()
    async_p = async_r.pipeline()
    j = async_p.json()

    # pipeline_execute_command is sync and returns self (the pipeline)
    result = async_p.execute_command("JSON.SET", "k", ".", '{"a":1}')

    assert result is async_p, (
        "Async pipeline execute_command must return the pipeline for chaining"
    )


# =============================================================================
# 5. CLUSTER PIPELINE TESTS 
# =============================================================================


@pytest.mark.fixed_client
def test_pipeline_merges_json_callbacks_into_cluster_pipeline():
    """
    j.pipeline() on a ClusterPipeline path must merge _MODULE_CALLBACKS into
    the pipeline's cluster_response_callbacks copy so JSON.* commands are
    decoded correctly.  Before the fix, the ClusterPipeline was constructed
    with the unmodified parent cluster_response_callbacks (which no longer
    contain JSON entries after we stopped registering them on the parent).
    """
    r = redis.Redis()
    j = r.json()

    # Simulate a minimal RedisCluster-like client with all required attributes
    fake_cluster = mock.MagicMock(spec=redis.RedisCluster)
    fake_cluster.cluster_response_callbacks = {"PING": lambda r: r}
    fake_cluster.response_callbacks = {}

    # Populate required instance attributes so pipeline() doesn't fail
    fake_cluster.nodes_manager = mock.MagicMock()
    fake_cluster.nodes_manager.startup_nodes = []
    fake_cluster.commands_parser = mock.MagicMock()
    fake_cluster.result_callbacks = {}
    fake_cluster.retry = mock.MagicMock()
    fake_cluster.retry.get_retries.return_value = 3
    fake_cluster.read_from_replicas = False
    fake_cluster.reinitialize_steps = 5
    fake_cluster._lock = mock.MagicMock()

    j.client = fake_cluster

    captured = {}

    def fake_cluster_pipeline(**kwargs):
        captured.update(kwargs)
        p = mock.MagicMock()
        p._encode = None
        p._decode = None
        return p

    with mock.patch(
        "redis.commands.json.ClusterPipeline", side_effect=fake_cluster_pipeline
    ):
        j.pipeline()

    crc = captured.get("cluster_response_callbacks", {})
    assert "JSON.GET" in crc, (
        "ClusterPipeline must receive JSON.GET in cluster_response_callbacks"
    )
    # The original parent callback must still be present (not replaced)
    assert "PING" in crc, (
        "Original cluster_response_callbacks entries must be preserved"
    )
    # The parent cluster's callback table must NOT have been polluted
    assert "JSON.GET" not in fake_cluster.cluster_response_callbacks, (
        "Parent cluster's cluster_response_callbacks must not contain JSON.GET "
        "after j.pipeline() — callbacks must only be on the pipeline copy"
    )


# =============================================================================
# 6. END-TO-END PIPELINE EXECUTION TESTS  
#    These require a live Redis server and should use the appropriate markers.
# =============================================================================


@pytest.mark.onlynoncluster
class TestJsonPipelineExecution:
    """
    End-to-end tests verifying that JSON pipeline commands are correctly
    decoded at Pipeline.execute() time.  These require a running Redis
    server with the RedisJSON module loaded.
    """

    def test_json_pipeline_via_json_pipeline_method(self, r):
        """
        r.json().pipeline() — JSON commands queued on the pipeline must
        be decoded correctly when execute() runs.
        """
        r.json().set("json:pipe:test1", ".", {"a": 1})

        p = r.json().pipeline()
        p.set("json:pipe:test2", ".", {"b": 2})
        p.get("json:pipe:test1")
        p.get("json:pipe:test2")
        results = p.execute()

        assert results[0] is True, "JSON.SET should return True"
        assert results[1] == {"a": 1}, "JSON.GET should return decoded dict"
        assert results[2] == {"b": 2}, "JSON.GET should return decoded dict"

        # Cleanup
        r.delete("json:pipe:test1", "json:pipe:test2")

    def test_json_pipeline_via_pipeline_json_method(self, r):
        """
        r.pipeline().json() — JSON commands queued via this entry point
        must also be decoded correctly.
        """
        r.json().set("json:pipe:test3", ".", {"c": 3})

        p = r.pipeline()
        j = p.json()
        j.set("json:pipe:test4", ".", {"d": 4})
        j.get("json:pipe:test3")
        j.get("json:pipe:test4")
        results = p.execute()

        assert results[0] is True, "JSON.SET should return True"
        assert results[1] == {"c": 3}, "JSON.GET should return decoded dict"
        assert results[2] == {"d": 4}, "JSON.GET should return decoded dict"

        # Cleanup
        r.delete("json:pipe:test3", "json:pipe:test4")

    def test_json_pipeline_parent_remains_clean(self, r):
        """
        After pipeline JSON operations, the parent client's
        response_callbacks must remain free of JSON.* entries.
        """
        p = r.pipeline()
        _ = p.json()

        assert "JSON.GET" not in r.response_callbacks, (
            "Parent client must not have JSON.GET after pipeline usage"
        )


# =============================================================================
# 7. ASYNC END-TO-END PIPELINE EXECUTION TESTS  (petyaslavova — async equivalents)
#    These require a live Redis server with RedisJSON loaded.
# =============================================================================


@pytest.mark.onlynoncluster
class TestAsyncJsonPipelineExecution:
    """
    Async equivalents of TestJsonPipelineExecution.  Verifies that
    async JSON pipeline commands are decoded correctly at execute() time.
    """

    @pytest.mark.asyncio
    async def test_async_json_pipeline_via_pipeline_json_method(self, r):
        """
        petyaslavova's exact async scenario:
            p = r.pipeline()
            await p.json().set("k", ".", {"a": 1})
            await p.json().get("k")
            assert await p.execute() == [True, {"a": 1}]
        """
        import redis.asyncio as async_redis

        # This test needs an async Redis client; skip if the fixture
        # provides a sync client (the test_asyncio/ directory has its own
        # fixtures).  We still include it here for completeness.
        if not hasattr(r, "__aenter__"):
            pytest.skip("Fixture 'r' is not an async client")

        await r.json().set("json:async:test1", ".", {"a": 1})

        p = r.pipeline()
        p.json().set("json:async:test2", ".", {"b": 2})
        p.json().get("json:async:test1")
        p.json().get("json:async:test2")
        results = await p.execute()

        assert results[0] is True, "JSON.SET should return True"
        assert results[1] == {"a": 1}, "JSON.GET should return decoded dict"
        assert results[2] == {"b": 2}, "JSON.GET should return decoded dict"

        await r.delete("json:async:test1", "json:async:test2")


# =============================================================================
# 8. ASYNC CLUSTER PIPELINE — construction must not raise AttributeError
# =============================================================================



@pytest.mark.fixed_client
def test_async_cluster_pipeline_json_does_not_crash_on_construction():
    """
    Regression: JSON(async_cluster_pipeline) must not raise AttributeError,
    and cluster_client.response_callbacks must never be touched (issue #3937).
    """
    from redis.asyncio.cluster import ClusterPipeline, PipelineStrategy
    from redis.commands.json import JSON

    shared_callbacks: dict = {"PING": lambda r: r}
    fake_rc = mock.MagicMock()
    fake_rc.response_callbacks = shared_callbacks

    pipeline = ClusterPipeline(fake_rc)
    strategy = pipeline._execution_strategy
    assert isinstance(strategy, PipelineStrategy)

    try:
        JSON(pipeline)
    except AttributeError as e:
        pytest.fail(f"JSON(async_cluster_pipeline) raised AttributeError: {e}")

    # shared dict must be completely untouched
    assert fake_rc.response_callbacks is shared_callbacks
    assert "JSON.GET" not in shared_callbacks

    # strategy._execute must now be the decoding wrapper
    assert getattr(strategy, "_json_callbacks_installed", False), (
        "_json_callbacks_installed flag must be set after JSON() construction"
    )


@pytest.mark.fixed_client
@pytest.mark.asyncio
async def test_async_cluster_pipeline_nontransactional_wrapper_actually_decodes():
    """
    Behavioral: the _execute wrapper decodes JSON results and passes
    _json_path kwargs through to the callback.
    """
    from redis.asyncio.cluster import ClusterPipeline, PipelineCommand, PipelineStrategy
    from redis.commands.json import JSON

    fake_rc = mock.MagicMock()
    fake_rc.response_callbacks = {}

    pipeline = ClusterPipeline(fake_rc)
    strategy = pipeline._execution_strategy
    assert isinstance(strategy, PipelineStrategy)

    mock_inner = mock.AsyncMock(return_value=[b'{"x": 1}', b"OK"])
    strategy._execute = mock_inner

    JSON(pipeline)

    cmd_json = PipelineCommand(0, "JSON.GET", "mykey")
    cmd_set  = PipelineCommand(1, "SET", "k", "v")

    result = await strategy._execute(fake_rc, [cmd_json, cmd_set])

    assert result[0] == {"x": 1}, (
        f"JSON.GET must be decoded to a dict; got {result[0]!r}"
    )
    assert result[1] == b"OK", (
        f"Non-JSON command must pass through unchanged; got {result[1]!r}"
    )
    mock_inner.assert_called_once()


@pytest.mark.fixed_client
@pytest.mark.asyncio
async def test_async_cluster_pipeline_double_json_call_does_not_double_decode():
    """
    Behavioral: calling p.json() twice must not stack wrappers and
    double-decode results (e.g. decoding an already-decoded dict again).
    """
    from redis.asyncio.cluster import ClusterPipeline, PipelineCommand
    from redis.commands.json import JSON

    fake_rc = mock.MagicMock()
    fake_rc.response_callbacks = {}

    pipeline = ClusterPipeline(fake_rc)
    mock_inner = mock.AsyncMock(return_value=[b'{"x": 1}'])
    pipeline._execution_strategy._execute = mock_inner

    JSON(pipeline)  # first call
    JSON(pipeline)  # second call — must NOT stack another wrapper

    cmd_json = PipelineCommand(0, "JSON.GET", "mykey")
    result = await pipeline._execution_strategy._execute(fake_rc, [cmd_json])

    assert result[0] == {"x": 1}, (
        f"Result must be decoded exactly once; got {result[0]!r}"
    )
    # inner mock must have been called exactly once (not twice)
    mock_inner.assert_called_once()


@pytest.mark.fixed_client
def test_async_cluster_pipeline_transactional_does_not_touch_shared_callbacks():
    """
    Behavioral: transactional async ClusterPipeline must NOT mutate
    cluster_client.response_callbacks — wraps strategy.execute() instead.
    """
    from redis.asyncio.cluster import ClusterPipeline, TransactionStrategy
    from redis.commands.json import JSON

    shared_cbs: dict = {"PING": lambda r: r}
    fake_rc = mock.MagicMock()
    fake_rc.response_callbacks = shared_cbs

    pipeline = ClusterPipeline(fake_rc, transaction=True)
    assert isinstance(pipeline._execution_strategy, TransactionStrategy)

    original_execute = pipeline._execution_strategy.execute
    JSON(pipeline)

    # shared dict must still be the same object and unmodified
    assert fake_rc.response_callbacks is shared_cbs, (
        "cluster_client.response_callbacks must not be replaced (#3937)"
    )
    assert "JSON.GET" not in shared_cbs, (
        "JSON callbacks must not leak into the shared dict"
    )
    # strategy.execute must have been wrapped
    assert pipeline._execution_strategy.execute is not original_execute, (
        "TransactionStrategy.execute must be wrapped for JSON decoding"
    )
    assert getattr(pipeline._execution_strategy, "_json_callbacks_installed", False)


@pytest.mark.fixed_client
@pytest.mark.asyncio
async def test_async_cluster_pipeline_transactional_wrapper_actually_decodes():
    """
    End-to-end behavioral test: calling the wrapped TransactionStrategy.execute()
    must actually decode JSON command results using the command queue snapshot,
    not just prove the wrapper was installed.
    """
    from redis.asyncio.cluster import ClusterPipeline, PipelineCommand, TransactionStrategy
    from redis.commands.json import JSON

    fake_rc = mock.MagicMock()
    fake_rc.response_callbacks = {}

    pipeline = ClusterPipeline(fake_rc, transaction=True)
    strategy = pipeline._execution_strategy
    assert isinstance(strategy, TransactionStrategy)

    # Replace the inner execute() BEFORE JSON wraps it, so our wrapper's
    # closure captures this mock as `_orig_txn_execute`.
    mock_inner_execute = mock.AsyncMock(return_value=[b'{"x": 1}', b"OK"])
    strategy.execute = mock_inner_execute

    JSON(pipeline)  # installs the decoding wrapper around mock_inner_execute

    # Populate _command_queue the way real usage would via execute_command().
    strategy._command_queue = [
        PipelineCommand(0, "JSON.GET", "mykey"),
        PipelineCommand(1, "SET", "k", "v"),
    ]

    result = await strategy.execute()

    assert result[0] == {"x": 1}, (
        f"JSON.GET result must be decoded to a dict by the wrapped execute(); "
        f"got {result[0]!r}"
    )
    assert result[1] == b"OK", (
        f"Non-JSON command result must pass through unchanged; got {result[1]!r}"
    )
    mock_inner_execute.assert_called_once()


@pytest.mark.fixed_client
@pytest.mark.asyncio
async def test_async_cluster_pipeline_transactional_double_json_call_does_not_double_decode():
    """
    Behavioral: calling p.json() twice on a transactional pipeline must not
    stack wrappers around TransactionStrategy.execute() and double-decode.
    """
    from redis.asyncio.cluster import ClusterPipeline, PipelineCommand
    from redis.commands.json import JSON

    fake_rc = mock.MagicMock()
    fake_rc.response_callbacks = {}

    pipeline = ClusterPipeline(fake_rc, transaction=True)
    strategy = pipeline._execution_strategy

    mock_inner_execute = mock.AsyncMock(return_value=[b'{"x": 1}'])
    strategy.execute = mock_inner_execute

    JSON(pipeline)  # first call — installs wrapper
    JSON(pipeline)  # second call — must NOT stack another wrapper

    strategy._command_queue = [PipelineCommand(0, "JSON.GET", "mykey")]
    result = await strategy.execute()

    assert result[0] == {"x": 1}, (
        f"Result must be decoded exactly once; got {result[0]!r}"
    )
    mock_inner_execute.assert_called_once()


@pytest.mark.fixed_client
@pytest.mark.asyncio
async def test_async_cluster_pipeline_decode_handles_non_string_command_name():
    """
    Robustness: if a PipelineCommand's args[0] is not a str (e.g. bytes),
    the decode wrapper must not raise AttributeError from .upper() — it
    should simply skip decoding for that command, mirroring the
    isinstance(cmd, str) guard used in JSON.execute_command.
    """
    from redis.asyncio.cluster import ClusterPipeline, PipelineCommand, PipelineStrategy
    from redis.commands.json import JSON

    fake_rc = mock.MagicMock()
    fake_rc.response_callbacks = {}

    pipeline = ClusterPipeline(fake_rc)
    strategy = pipeline._execution_strategy
    assert isinstance(strategy, PipelineStrategy)

    mock_inner = mock.AsyncMock(return_value=[b"raw-bytes-result"])
    strategy._execute = mock_inner

    JSON(pipeline)

    cmd_bytes_name = PipelineCommand(0, b"JSON.GET", "mykey")
    result = await strategy._execute(fake_rc, [cmd_bytes_name])

    assert result[0] == b"raw-bytes-result", (
        "Non-str command names must be passed through unmodified, not raise "
        f"AttributeError; got {result[0]!r}"
    )


