import unittest
import itertools

from resilience.backoff import ExponentialBackoff, ExponentialBackoffWithJitter


class TestExponentialBackoff(unittest.TestCase):
    def test_strategy_generates_increasing_interval_maximums(self):
        strategy = ExponentialBackoff(min_wait_seconds=0, max_wait_seconds=100)
        actual = list(itertools.islice(strategy(), 5))
        self.assertEquals(actual, [1, 2, 4, 8, 16])

    def test_strategy_respects_minimum_wait_time(self):
        strategy = ExponentialBackoff(min_wait_seconds=3, max_wait_seconds=100)
        actual = list(itertools.islice(strategy(), 3))
        self.assertEquals(actual, [3, 3, 4])

    def test_strategy_respects_maximum_wait_time(self):
        strategy = ExponentialBackoff(min_wait_seconds=1, max_wait_seconds=5)
        actual = list(itertools.islice(strategy(), 5))
        self.assertEquals(actual, [1, 2, 4, 5, 5])

    def test_strategy_respects_minimum_and_maximum_wait_times(self):
        strategy = ExponentialBackoff(min_wait_seconds=3, max_wait_seconds=5)
        actual = list(itertools.islice(strategy(), 5))
        self.assertEquals(actual, [3, 3, 4, 5, 5])

    def test_strategy_scales_up_wait_times_by_base_factor(self):
        strategy = ExponentialBackoff(base=3, min_wait_seconds=0, max_wait_seconds=100)
        actual = list(itertools.islice(strategy(), 5))
        self.assertEquals(actual, [3, 6, 12, 24, 48])


class TestExponentialBackoffWithJitter(unittest.TestCase):

    def test_strategy_generates_increasing_interval_maximums(self):
        strategy = ExponentialBackoffWithJitter(
            min_wait_seconds=0,
            max_wait_seconds=100,
            random_number_gen=lambda _, mx: mx,
        )

        self.assertEquals(list(itertools.islice(strategy(), 5)), [1, 2, 4, 8, 16])

    def test_strategy_generates_wait_times_that_are_between_min_and_max_waits(self):
        strategy = ExponentialBackoffWithJitter(
            min_wait_seconds=0,
            max_wait_seconds=100,
        )

        n = 1000
        wait_times = list(itertools.islice(strategy(), n))

        minimums = [0] * n
        maximums = [1 << n for n in range(n)]

        self.assertTrue(all(mn <= x < mx for mn, x, mx in zip(minimums, wait_times, maximums)))

    def test_strategy_respects_minimum_wait_time(self):
        strategy = ExponentialBackoffWithJitter(
            min_wait_seconds=1,
            max_wait_seconds=5,
            random_number_gen=lambda mn, mx: -1,
        )
        self.assertEquals(list(itertools.islice(strategy(), 3)), [1, 1, 1])

    def test_strategy_respects_maximum_wait_time(self):
        strategy = ExponentialBackoffWithJitter(
            min_wait_seconds=1,
            max_wait_seconds=5,
            random_number_gen=lambda mn, mx: mx,
        )
        self.assertEquals(list(itertools.islice(strategy(), 5)), [1, 2, 4, 5, 5])

    def test_strategy_respects_minimum_and_maximum_wait_times(self):
        strategy = ExponentialBackoffWithJitter(
            min_wait_seconds=3,
            max_wait_seconds=5,
            random_number_gen=lambda mn, mx: mx,
        )
        self.assertEquals(list(itertools.islice(strategy(), 5)), [3, 3, 4, 5, 5])

    def test_strategy_scales_up_wait_times_by_base_factor(self):
        strategy = ExponentialBackoffWithJitter(
            base=3,
            min_wait_seconds=0,
            max_wait_seconds=100,
            random_number_gen=lambda _, mx: mx,
        )
        self.assertEquals(list(itertools.islice(strategy(), 5)), [3, 6, 12, 24, 48])
