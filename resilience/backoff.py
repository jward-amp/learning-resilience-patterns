"""Collection of backoff strategies that can be used for resilience functionality

These strategies can be used by resilience patterns, such as retry, circuit breakers, rate limiters, etc., to
control the amount of time between runs. Strategies should be injected into the resilience functionality so
that behavior can be changed easily.
"""
import random
from collections.abc import Callable
from typing import Iterable

__all__ = [
    "BackoffStrategy",
    "ExponentialBackoff",
    "ExponentialBackoffWithJitter",
]


class BackoffStrategy(Callable):
    """Functional (Callable) interface for backoff strategies.

    Expectations for each backoff strategy that implements this interface:
        * returns **AN INFINITE** iterator of wait times in seconds (either int or float) that can be passed to
          functions such as `time.sleep` or `asyncio.sleep`, and
        * each invocation of `__call__` should return a new iterator
    """

    def __call__(self) -> Iterable[int | float]:
        ...


class ExponentialBackoff(BackoffStrategy):
    """Implements exponential backoff

    Each number in the sequence is generated by the following formula:
        `wait_time = base * (2 ** n) where n is the attempt number minus one`

    Setting `base=1`, this generates the following sequence:
        `wait_times = [ 2**0, 2**1, 2**2, ...] = [1, 2, 4, ...]`

    This sequence is adjusted so that the `min_wait_seconds` and `max_wait_seconds` are observed.

    Usage:
        >>> import itertools
        >>>
        >>> strategy = ExponentialBackoff(min_wait_seconds=0, max_wait_seconds=10)
        >>> list(itertools.islice(strategy(), 6))
        [1, 2, 4, 8, 10, 10]

    Args:
        base: a number by which to scale each wait_time (default: 1)
        min_wait_seconds: the minimum wait time in seconds (default: 0)
        max_wait_seconds: the maximum wait time in seconds (default: 30)
    """

    def __init__(
            self,
            *,
            base: float | int = 1,
            min_wait_seconds: float | int = 0,
            max_wait_seconds: float | int = 30,
    ):
        self.base = base
        self.min_wait_seconds = min_wait_seconds
        self.max_wait_seconds = max_wait_seconds
        self.counter = 0

    def __call__(self) -> Iterable[int | float]:
        while True:
            # using bitwise operator as faster implementation of 2**n
            upper_limit = min(self.base << self.counter, self.max_wait_seconds)
            yield max(self.min_wait_seconds, upper_limit)
            self.counter = self.counter + 1


class ExponentialBackoffWithJitter(BackoffStrategy):
    """Implements exponential backoff with jitter

    This sequence generates **random** wait times that exist between 0 and the wait times generated by the
    `ExponentialBackoff` strategy. This sequence is adjusted so that the `min_wait_seconds` and `max_wait_seconds` are
    observed.

    The advantage of this implementation is that, in practice, the work is completed more quickly, and the system
    better avoids "retry storms" where multiple clients end up in the same retry pattern.


    Usage:
        >>> import itertools
        >>>
        >>> strategy = ExponentialBackoffWithJitter(min_wait_seconds=0, max_wait_seconds=10)
        >>> list(itertools.islice(strategy(), 5))
        [0.1761, 1.7152, 2.7559, 1.0013, 7.4858]

    References:
        https://aws.amazon.com/blogs/architecture/exponential-backoff-and-jitter/

    Args:
        base: a number by which to scale each wait_time (default: 1)
        min_wait_seconds: the minimum wait time in seconds (default: 0)
        max_wait_seconds: the maximum wait time in seconds (default: 30)
        random_number_gen: a function that takes two integers and returns a random number (default: random.uniform)
            In practice, this parameter should rarely be used, but it is provided for testing purposes and for the off
            chance that the caller wants to change the distribution used for random number generation.
    """

    def __init__(
            self,
            *,
            base: float | int = 1,
            min_wait_seconds: float | int = 0,
            max_wait_seconds: float | int = 30,
            random_number_gen: Callable[[int, int], int | float] = random.uniform,
    ):
        self.base = base
        self.exponential_backoff = ExponentialBackoff(
            base=base,
            min_wait_seconds=min_wait_seconds,
            max_wait_seconds=max_wait_seconds,
        )
        self.min_wait_seconds = min_wait_seconds
        self.random_number_gen = random_number_gen
        self.counter = 0

    def __call__(self):
        for wait_time in self.exponential_backoff():
            random_wait = self.random_number_gen(0, wait_time)
            yield max(self.min_wait_seconds, random_wait)
