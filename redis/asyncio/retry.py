import warnings
from asyncio import sleep
from inspect import iscoroutinefunction
from typing import (
    TYPE_CHECKING,
    Any,
    Awaitable,
    Callable,
    Tuple,
    Type,
    TypeVar,
    Union,
)

from redis.exceptions import ConnectionError, TimeoutError
from redis.retry import AbstractRetry

T = TypeVar("T")

if TYPE_CHECKING:
    from redis.backoff import AbstractBackoff


class Retry(AbstractRetry[Exception]):
    __hash__ = AbstractRetry.__hash__

    def __init__(
        self,
        backoff: "AbstractBackoff",
        retries: int,
        supported_errors: Tuple[Type[Exception], ...] = (
            ConnectionError,
            TimeoutError,
        ),
        is_retryable: Callable[[Exception], bool] | None = None,
    ):
        super().__init__(backoff, retries, supported_errors, is_retryable)

    def __eq__(self, other: Any) -> bool:
        if not isinstance(other, Retry):
            return NotImplemented

        return (
            self._backoff == other._backoff
            and self._retries == other._retries
            and set(self._supported_errors) == set(other._supported_errors)
            and self._is_retryable == other._is_retryable
        )

    async def call_with_retry(
        self,
        do: Callable[[], Awaitable[T]],
        fail: Union[
            Callable[[Exception], Any],
            Callable[[Exception, int], Any],
        ],
        is_retryable: Callable[[Exception], bool] | None = None,
        with_failure_count: bool = False,
    ) -> T:
        """
        Execute an operation that might fail and returns its result, or
        raise the exception that was thrown depending on the `Backoff` object.
        `do`: the operation to call. Expects no argument.
        `fail`: the failure handler, expects the last error that was thrown
        ``is_retryable``: optional function to determine if an error is retryable.
        It is composed with the checker set on this object (if any) using AND:
        an error is retried only if both checkers return True.
        ``with_failure_count``: if True, the failure count is passed to the failure handler
        """
        self._backoff.reset()
        failures = 0
        while True:
            try:
                return await do()
            except self._supported_errors as error:
                if self._is_retryable is not None and not self._is_retryable(error):
                    # Run the failure handler before raising so connection-scoped
                    # cleanup (for example a pipeline's WATCH state) still happens.
                    if with_failure_count:
                        await fail(error, failures + 1)
                    else:
                        await fail(error)
                    raise
                if is_retryable is not None and not is_retryable(error):
                    raise
                failures += 1

                if with_failure_count:
                    await fail(error, failures)
                else:
                    await fail(error)

                if self._retries >= 0 and failures > self._retries:
                    raise error
                backoff = self._backoff.compute(failures)
                if backoff > 0:
                    await sleep(backoff)


def _to_async_retry(retry: Any) -> Any:
    """Convert a synchronous retry policy while preserving async-shaped ones."""
    if iscoroutinefunction(getattr(type(retry), "call_with_retry", None)):
        return retry
    if not isinstance(retry, AbstractRetry):
        return retry

    warnings.warn(
        "A synchronous redis.retry.Retry was passed to an asyncio client and has "
        "been converted to redis.asyncio.retry.Retry. A custom call_with_retry "
        "implementation is not preserved - please use redis.asyncio.retry.Retry.",
        UserWarning,
        stacklevel=2,
    )

    return Retry(
        backoff=retry._backoff,
        retries=retry._retries,
        supported_errors=retry._supported_errors,
        is_retryable=getattr(retry, "_is_retryable", None),
    )
