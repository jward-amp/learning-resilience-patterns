"""Module for retry functionality.

Contains retry decorators that can wrap any type of non-deterministic function or method. These decorators are
particularly useful for network calls to other APIs that might fail for any number of reasons. The goals of this
module is to reduce transient errors and, therefore, increase the resiliency of calling code.
"""
import logging
import time
from functools import wraps
from typing import Type

from resilience.backoff import BackoffStrategy, ExponentialBackoffWithJitter

__all__ = ["RetryOnException"]

logger = logging.getLogger(__name__)


class RetryOnException:
    """Decorator for retrying a function or method when an expected exception is raised.

    If the decorated function is unsuccessful after `max_retries` attempts, then whatever exception caused the failure
    will be raised. That means calling code should also handle these exceptions.

        >>> import requests
        >>> from resilience.backoff import ExponentialBackoffWithJitter
        >>>
        >>> @RetryOnException(
        >>>     max_retries=10,
        >>>     retry_on_exceptions=[requests.exceptions.ReadTimeout],
        >>>     backoff_strategy=ExponentialBackoffWithJitter(
        >>>         min_wait_seconds=1,
        >>>         max_wait_seconds=30,
        >>>     ),
        >>> )
        >>> def flaky_function():
        >>>     try:
        >>>         rv = requests.get("http://unreliable.org/foo", timeout=10)
        >>>     except requests.exceptions.ReadTimeout:
        >>>         # handle exception
        >>>     else:
        >>>         return rv.json()

    To retry on certain HTTP response codes, then define an error class for the situation.

        >>> class TeapotResponse(Exception):
        >>>     pass
        >>>
        >>> @RetryOnException(retry_on_exceptions=[TeapotResponse])
        >>> def flaky_function():
        >>>     rv = requests.get("http://unreliable.org/foo", timeout=10)
        >>>     if rv.status_code == 418:
        >>>         raise TeapotResponse
        >>>     return rv.json()

    Attributes:
        max_retries: The maximum number of attempts to retry the wrapped function or method
        retry_on_exceptions: A list of the Exception types that should be handled and then retry the wrapped function
            or method
        backoff_strategy: Provides an infinite iterator of wait times between retries (e.g., exponential backoff)
    """

    def __init__(
            self,
            max_retries: int = 5,
            retry_on_exceptions: list[Type[Exception]] | None = None,
            backoff_strategy: BackoffStrategy | None = None,
    ):
        self.max_retries = max_retries
        self.retry_on_exceptions = tuple(retry_on_exceptions) if retry_on_exceptions else (Exception,)
        self.backoff_strategy = backoff_strategy if backoff_strategy else ExponentialBackoffWithJitter()

    def __call__(self, func):
        @wraps(func)
        def wrapped(*args, **kwargs):

            for i, wait in enumerate(self.backoff_strategy()):
                attempt_number = i + 1
                logger.debug(f"Attempt %s of %s", attempt_number, self.max_retries)
                try:
                    return func(*args, **kwargs)
                except self.retry_on_exceptions as e:
                    if attempt_number == self.max_retries:
                        raise e

                    logger.debug(f"an error occurred, retrying in {wait:.2f} seconds")
                    time.sleep(wait)

            # using raw Exception because this error should not be handled by the caller. This should not happen,
            # but, if it does, then it shouldn't fail silently
            raise Exception("Critical bug in RetryOnException. This line should never be executed.")

        return wrapped
