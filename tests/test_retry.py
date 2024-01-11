import unittest
from typing import Callable, Iterable

from resilience.backoff import BackoffStrategy
from resilience.retry import RetryOnException


class _NoWait(BackoffStrategy):

    def __call__(self) -> Iterable[int | float]:
        while True:
            yield 0


class WorksOnNthAttempt:
    def __init__(
            self,
            n: int,
            error_gen: Callable[[int], Exception] = lambda i: ValueError(f"Failed {i} time[s]"),
    ):
        self.n = n
        self.attempts = 1
        self.error_gen = error_gen

    @RetryOnException(
        max_retries=5,
        retry_on_exceptions=[ValueError, ZeroDivisionError],
        backoff_strategy=_NoWait(),
    )
    def __call__(self, s):
        if self.attempts == self.n:
            return s, self.attempts
        self.attempts += 1
        raise self.error_gen(self.attempts - 1)


def odd_even_error(i: int) -> Exception:
    if i % 2 == 0:
        return ValueError(f"EVEN {i}")
    else:
        return ZeroDivisionError(f"ODD {i}")


class TestRetryOnException(unittest.TestCase):

    def test_returns_value_on_success(self):
        works_on_nth_attempt = WorksOnNthAttempt(1)
        got, _ = works_on_nth_attempt("foo")
        self.assertEqual(got, "foo")

    def test_succeeds_on_first_attempt(self):
        works_on_nth_attempt = WorksOnNthAttempt(1)
        self.assertEquals(works_on_nth_attempt("foo"), ("foo", 1))

    def test_succeeds_on_second_attempt(self):
        works_on_nth_attempt = WorksOnNthAttempt(2)
        self.assertEquals(works_on_nth_attempt("bar"), ("bar", 2))

    def test_succeeds_on_third_attempt(self):
        works_on_nth_attempt = WorksOnNthAttempt(3)
        self.assertEquals(works_on_nth_attempt("baz"), ("baz", 3))

    def test_fails_after_five_attempts(self):
        works_on_nth_attempt = WorksOnNthAttempt(10)
        with self.assertRaises(ValueError) as e:
            works_on_nth_attempt("fizzbuzz")
        self.assertEquals(str(e.exception), "Failed 5 time[s]")

    def test_fails_on_first_attempt_if_error_is_unexpected(self):
        works_on_nth_attempt = WorksOnNthAttempt(10, error_gen=lambda i: FileNotFoundError(f"Failed {i} time[s]"))
        with self.assertRaises(FileNotFoundError) as e:
            works_on_nth_attempt("fizzbuzz")
        self.assertEquals(str(e.exception), "Failed 1 time[s]")

    def test_succeeds_on_third_attempt_multiple_errors(self):
        works_on_nth_attempt = WorksOnNthAttempt(5, error_gen=odd_even_error)
        self.assertEquals(works_on_nth_attempt("amp"), ("amp", 5))
