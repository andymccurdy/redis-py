import abc
import errno
import socket
from time import sleep
from typing import (
    TYPE_CHECKING,
    Any,
    Callable,
    Generic,
    Iterable,
    Tuple,
    Type,
    TypeVar,
    Union,
)

from redis.exceptions import ConnectionError, TimeoutError

T = TypeVar("T")
E = TypeVar("E", bound=Exception, covariant=True)

if TYPE_CHECKING:
    from redis.backoff import AbstractBackoff


class AbstractRetry(Generic[E], abc.ABC):
    """Retry a specific number of times after a failure"""

    _supported_errors: Tuple[Type[E], ...]

    def __init__(
        self,
        backoff: "AbstractBackoff",
        retries: int,
        supported_errors: Tuple[Type[E], ...],
        is_retryable: Callable[[E], bool] | None = None,
    ):
        """
        Initialize a `Retry` object with a `Backoff` object
        that retries a maximum of `retries` times.
        `retries` can be negative to retry forever.
        You can specify the types of supported errors which trigger
        a retry with the `supported_errors` parameter.
        You can pass an optional ``is_retryable`` checker to refine the
        classification: an error is retried only if its type is one of the
        ``supported_errors`` AND the checker returns ``True`` for it. When no
        checker is set, the behavior is unchanged.
        """
        self._backoff = backoff
        self._retries = retries
        self._supported_errors = supported_errors
        self._is_retryable = is_retryable

    @property
    def is_retryable(self) -> Callable[[E], bool] | None:
        """Optional checker applied in addition to ``supported_errors``."""
        return self._is_retryable

    @is_retryable.setter
    def is_retryable(self, value: Callable[[E], bool] | None) -> None:
        self._is_retryable = value

    @abc.abstractmethod
    def __eq__(self, other: Any) -> bool:
        return NotImplemented

    def __hash__(self) -> int:
        # A callable checker is not necessarily hashable, and equality already
        # allows unequal policies to share a hash. Keep the policy fields that
        # defined the hash before the checker existed.
        return hash((self._backoff, self._retries, frozenset(self._supported_errors)))

    def update_supported_errors(self, specified_errors: Iterable[Type[E]]) -> None:
        """
        Updates the supported errors with the specified error types
        """
        self._supported_errors = tuple(
            set(self._supported_errors + tuple(specified_errors))
        )

    def get_retries(self) -> int:
        """
        Get the number of retries.
        """
        return self._retries

    def update_retries(self, value: int) -> None:
        """
        Set the number of retries.
        """
        self._retries = value


class Retry(AbstractRetry[Exception]):
    __hash__ = AbstractRetry.__hash__

    def __init__(
        self,
        backoff: "AbstractBackoff",
        retries: int,
        supported_errors: Tuple[Type[Exception], ...] = (
            ConnectionError,
            TimeoutError,
            socket.timeout,
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

    def call_with_retry(
        self,
        do: Callable[[], T],
        fail: Union[Callable[[Exception], Any], Callable[[Exception, int], Any]],
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
                return do()
            except self._supported_errors as error:
                if self._is_retryable is not None and not self._is_retryable(error):
                    # Run the failure handler before raising so connection-scoped
                    # cleanup (for example a pipeline's WATCH state) still happens.
                    if with_failure_count:
                        fail(error, failures + 1)
                    else:
                        fail(error)
                    raise
                if is_retryable is not None and not is_retryable(error):
                    raise
                failures += 1

                if with_failure_count:
                    fail(error, failures)
                else:
                    fail(error)

                if self._retries >= 0 and failures > self._retries:
                    raise error
                backoff = self._backoff.compute(failures)
                if backoff > 0:
                    sleep(backoff)


# OS-level errno values for which reconnecting can never succeed, so there is
# no point in spending the retry/backoff budget on them.
_UNRETRYABLE_CONNECT_ERRNOS = frozenset({errno.ENOENT})


def is_retryable_connect_error(error: Exception) -> bool:
    """``is_retryable`` checker that fails fast on unrecoverable connect errors.

    Returns ``False`` only when ``error`` was raised from an ``OSError`` whose
    ``errno`` is in a conservative set of definitively unrecoverable socket
    errors (currently only ``ENOENT``: the socket path does not exist). Every
    other error, including transient failures such as ``ECONNREFUSED`` while a
    server is still starting, is left to the normal retry policy.

    Use it as the optional checker on a :class:`Retry` object to stop a missing
    Unix socket path from consuming the full backoff budget::

        retry = Retry(
            NoBackoff(), 3, is_retryable=is_retryable_connect_error
        )
    """
    cause = error.__cause__
    return not (
        isinstance(cause, OSError) and cause.errno in _UNRETRYABLE_CONNECT_ERRNOS
    )
