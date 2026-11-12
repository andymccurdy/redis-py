from unittest.mock import Mock

import pytest

from redis.backoff import (
    EqualJitterBackoff,
    ExponentialBackoff,
    ExponentialWithJitterBackoff,
    FullJitterBackoff,
)


@pytest.mark.fixed_client
def test_exponential_with_jitter_backoff(monkeypatch: pytest.MonkeyPatch) -> None:
    mock_random = Mock(side_effect=[0.25, 0.5, 0.75, 1.0, 0.9])
    monkeypatch.setattr("random.random", mock_random)

    bo = ExponentialWithJitterBackoff(cap=5, base=1)

    assert bo.compute(0) == 0.25  # min(5, 0.25*2^0)
    assert bo.compute(1) == 1.0  # min(5, 0.5*2^1)
    assert bo.compute(2) == 3.0  # min(5, 0.75*2^2)
    assert bo.compute(3) == 5.0  # min(5, 1*2^3)
    assert bo.compute(4) == 5.0  # min(5, 0.9*2^4)


@pytest.mark.fixed_client
def test_exponential_with_jitter_backoff_keeps_zero_jitter_after_overflow(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr("random.random", lambda: 0.0)

    bo = ExponentialWithJitterBackoff(cap=3.0, base=0.1)

    assert bo.compute(5000) == 0.0


@pytest.mark.fixed_client
@pytest.mark.parametrize(
    "backoff_class",
    [
        ExponentialBackoff,
        FullJitterBackoff,
        EqualJitterBackoff,
        ExponentialWithJitterBackoff,
    ],
)
def test_backoff_survives_a_long_outage(backoff_class) -> None:
    """A `Retry` built with a negative retry count retries forever, so the
    failure count is unbounded. `2**failures` stops fitting in a float at 1024,
    which used to raise OverflowError instead of returning the capped delay.
    """
    bo = backoff_class(cap=3.0, base=0.1)

    for failures in (1023, 1024, 5000, 10**6):
        delay = bo.compute(failures)
        assert 0 <= delay <= 3.0


@pytest.mark.fixed_client
def test_exponential_backoff_is_unchanged_for_finite_delays() -> None:
    """Avoiding the overflow must not alter any delay that fits in a float."""
    bo = ExponentialBackoff(cap=1e9, base=0.008)

    for failures in range(0, 60):
        assert bo.compute(failures) == min(1e9, 0.008 * 2**failures)


@pytest.mark.fixed_client
def test_exponential_backoff_keeps_growing_past_1023_failures() -> None:
    """A tiny base still grows toward the cap once `2**failures` leaves the
    float range, rather than stopping at whatever 1023 doublings reached.
    """
    bo = ExponentialBackoff(cap=0.512, base=1e-310)

    assert bo.compute(1023) < bo.compute(1024) < bo.compute(1028) < 0.512
    assert bo.compute(1029) == 0.512
