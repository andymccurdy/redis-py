from datetime import datetime
import inspect
import warnings
import pytest
import redis
from redis.asyncio.utils import pipeline
from redis.utils import (
    deprecated_function,
    deprecated_args,
    experimental_method,
    experimental_args,
)


async def redis_server_time(client: redis.Redis):
    seconds, milliseconds = await client.time()
    timestamp = float(f"{seconds}.{milliseconds}")
    return datetime.fromtimestamp(timestamp)


# Async tests for deprecated_function decorator
@pytest.mark.fixed_client
class TestDeprecatedFunctionAsync:
    @pytest.mark.asyncio
    async def test_async_function_warns(self):
        @deprecated_function(reason="use new_async_func", version="2.0.0")
        async def old_async_func():
            return "async_result"

        with warnings.catch_warnings(record=True) as w:
            warnings.simplefilter("always")
            result = await old_async_func()
            assert result == "async_result"
            assert len(w) == 1
            assert issubclass(w[0].category, DeprecationWarning)
            assert "old_async_func" in str(w[0].message)

    @pytest.mark.asyncio
    async def test_async_preserves_function_metadata(self):
        @deprecated_function()
        async def async_documented_func():
            """This is the async docstring."""
            pass

        assert async_documented_func.__name__ == "async_documented_func"
        assert async_documented_func.__doc__ == "This is the async docstring."


# Async tests for deprecated_args decorator
@pytest.mark.fixed_client
class TestDeprecatedArgsAsync:
    @pytest.mark.asyncio
    async def test_async_function_warns_on_deprecated_arg(self):
        @deprecated_args(args_to_warn=["old_param"], reason="use new_param")
        async def async_func_with_args(new_param=None, old_param=None):
            return new_param or old_param

        with warnings.catch_warnings(record=True) as w:
            warnings.simplefilter("always")
            result = await async_func_with_args(old_param="async_value")
            assert result == "async_value"
            assert len(w) == 1
            assert issubclass(w[0].category, DeprecationWarning)
            assert "old_param" in str(w[0].message)

    @pytest.mark.asyncio
    async def test_async_function_no_warning_on_allowed_arg(self):
        @deprecated_args(args_to_warn=["*"], allowed_args=["allowed_param"])
        async def async_func_with_allowed(allowed_param=None):
            return allowed_param

        with warnings.catch_warnings(record=True) as w:
            warnings.simplefilter("always")
            result = await async_func_with_allowed(allowed_param="async_value")
            assert result == "async_value"
            assert len(w) == 0

    @pytest.mark.asyncio
    async def test_async_wildcard_warns_all_args(self):
        @deprecated_args(args_to_warn=["*"])
        async def async_func_all_deprecated(param1=None, param2=None):
            return (param1, param2)

        with warnings.catch_warnings(record=True) as w:
            warnings.simplefilter("always")
            result = await async_func_all_deprecated(param1="a", param2="b")
            assert result == ("a", "b")
            assert len(w) == 1
            assert "param1" in str(w[0].message) or "param2" in str(w[0].message)

    @pytest.mark.asyncio
    async def test_async_stacked_layers_merge_into_one_wrapper(self):
        async def async_func(a=None, b=None, c=None):
            return (a, b, c)

        inner = deprecated_args(args_to_warn=["c"], reason="inner")(async_func)
        outer = deprecated_args(args_to_warn=["b"], reason="outer")(inner)
        assert outer.__wrapped__ is async_func

        with warnings.catch_warnings(record=True) as w:
            warnings.simplefilter("always")
            assert await outer(1) == (1, None, None)
            assert len(w) == 0

        with warnings.catch_warnings(record=True) as w:
            warnings.simplefilter("always")
            assert await outer(1, 2, 3) == (1, 2, 3)
            # Each layer keeps its own message, outermost first.
            assert [str(x.message) for x in w] == [
                "Call to 'async_func' function with deprecated usage of input "
                f"argument/s '{arg}'. ({reason})"
                for arg, reason in (("b", "outer"), ("c", "inner"))
            ]

    @pytest.mark.asyncio
    async def test_async_deprecated_args_warn_at_call_site(self):
        async def async_func(a=None, b=None):
            return (a, b)

        single = deprecated_args(args_to_warn=["b"])(async_func)
        merged = deprecated_args(args_to_warn=["a"])(
            deprecated_args(args_to_warn=["b"])(async_func)
        )

        with warnings.catch_warnings(record=True) as w:
            warnings.simplefilter("always")
            single_line = inspect.currentframe().f_lineno + 1
            await single(b=2)
            merged_line = inspect.currentframe().f_lineno + 1
            await merged(a=1, b=2)
        # Every layer points at the line that called the decorated function.
        assert [(x.filename, x.lineno) for x in w] == [
            (__file__, single_line),
            (__file__, merged_line),
            (__file__, merged_line),
        ]


# Async tests for experimental_method decorator
@pytest.mark.fixed_client
class TestExperimentalMethodAsync:
    @pytest.mark.asyncio
    async def test_async_function_warns(self):
        @experimental_method()
        async def async_experimental_func():
            return "async_experimental_result"

        with warnings.catch_warnings(record=True) as w:
            warnings.simplefilter("always")
            result = await async_experimental_func()
            assert result == "async_experimental_result"
            assert len(w) == 1
            assert issubclass(w[0].category, UserWarning)
            assert "async_experimental_func" in str(w[0].message)

    @pytest.mark.asyncio
    async def test_async_preserves_function_metadata(self):
        @experimental_method()
        async def async_experimental_documented():
            """Experimental async docstring."""
            pass

        assert async_experimental_documented.__name__ == "async_experimental_documented"
        assert async_experimental_documented.__doc__ == "Experimental async docstring."


# Async tests for experimental_args decorator
@pytest.mark.fixed_client
class TestExperimentalArgsAsync:
    @pytest.mark.asyncio
    async def test_async_function_warns_on_experimental_arg(self):
        @experimental_args(args_to_warn=["beta_param"])
        async def async_func_with_experimental(stable_param=None, beta_param=None):
            return stable_param or beta_param

        with warnings.catch_warnings(record=True) as w:
            warnings.simplefilter("always")
            result = await async_func_with_experimental(beta_param="async_beta")
            assert result == "async_beta"
            assert len(w) == 1
            assert issubclass(w[0].category, UserWarning)
            assert "beta_param" in str(w[0].message)

    @pytest.mark.asyncio
    async def test_async_no_warning_when_no_args_provided(self):
        @experimental_args(args_to_warn=["beta_param"])
        async def async_func_no_args():
            return "no_args"

        with warnings.catch_warnings(record=True) as w:
            warnings.simplefilter("always")
            result = await async_func_no_args()
            assert result == "no_args"
            assert len(w) == 0


@pytest.mark.fixed_client
class TestPipelineContextManagerAsync:
    class _FakePipeline:
        def __init__(self):
            self.executed = False
            self.reset_calls = 0

        async def execute(self):
            self.executed = True

        async def reset(self):
            self.reset_calls += 1

    class _FakeRedis:
        def __init__(self, fake_pipeline):
            self._fake_pipeline = fake_pipeline

        def pipeline(self):
            return self._fake_pipeline

    async def test_executes_and_resets_on_success(self):
        fake_pipeline = self._FakePipeline()
        async with pipeline(self._FakeRedis(fake_pipeline)) as p:
            assert p is fake_pipeline
        assert fake_pipeline.executed is True
        assert fake_pipeline.reset_calls == 1

    async def test_resets_but_does_not_execute_on_exception(self):
        # Same as the sync helper: the commands queued before the exception
        # must not be sent, and reset() must still release the connection.
        fake_pipeline = self._FakePipeline()
        with pytest.raises(ValueError):
            async with pipeline(self._FakeRedis(fake_pipeline)):
                raise ValueError("boom")
        assert fake_pipeline.executed is False
        assert fake_pipeline.reset_calls == 1


class OutOfStock(Exception):
    pass


async def _reserve(r, order):
    async with pipeline(r) as p:
        await p.watch(*order)
        stock = dict(zip(order, await p.mget(*order)))
        p.multi()
        for key, qty in order.items():
            if int(stock[key]) < qty:
                raise OutOfStock(key)
            p.decrby(key, qty)


@pytest.mark.onlynoncluster
async def test_pipeline_aborted_transaction_is_not_executed(r):
    await r.mset({"stock:a": 5, "stock:b": 0})
    in_use = len(list(r.connection_pool._get_in_use_connections()))
    with pytest.raises(OutOfStock):
        await _reserve(r, {"stock:a": 2, "stock:b": 1})
    # the rejected order must not reserve anything, and the pipeline
    # connection must be released
    assert await r.get("stock:a") == b"5"
    assert await r.get("stock:b") == b"0"
    assert len(list(r.connection_pool._get_in_use_connections())) == in_use


@pytest.mark.onlynoncluster
async def test_pipeline_transaction_is_executed_on_success(r):
    await r.mset({"stock:a": 5, "stock:b": 3})
    in_use = len(list(r.connection_pool._get_in_use_connections()))
    await _reserve(r, {"stock:a": 2, "stock:b": 1})
    assert await r.get("stock:a") == b"3"
    assert await r.get("stock:b") == b"2"
    assert len(list(r.connection_pool._get_in_use_connections())) == in_use
