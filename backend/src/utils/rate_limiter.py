"""
Rate Limiter Utilities
=======================

TokenBucket implementation for rate limiting API requests.

CRITICAL FIX (from Zen expert review):
- Lock MUST be released during sleep to allow concurrent workers
- Original bug: sleep inside lock blocked all workers (defeated concurrency)
- Fixed: Calculate wait_time inside lock, sleep outside lock
"""

import time
import threading
from typing import Optional


class TokenBucket:
    """Rate limiter using token bucket algorithm.

    Thread-safe implementation for concurrent API requests with proper
    lock management to enable true concurrency.

    Usage:
        ```python
        rate_limiter = TokenBucket(rate=1.0)  # 1 request per second

        def make_request():
            rate_limiter.acquire()  # Blocks until token available
            response = requests.get(url)
            return response
        ```

    CRITICAL: Lock is released during sleep to allow other workers to proceed.
    """

    def __init__(self, rate: float = 1.0):
        """Initialize token bucket.

        Args:
            rate: Tokens per second (e.g., 1.0 = 1 request/second)
        """
        self.rate = rate
        self.tokens = rate
        self.last_update = time.time()
        self.lock = threading.Lock()

    def acquire(self):
        """Block until a token is available, then consume it.

        Lock is released during sleep to allow other workers to check
        for available tokens concurrently.

        CRITICAL: This implementation fixes the concurrency bug where
        sleeping inside the lock would block all workers.
        """
        while True:
            with self.lock:
                now = time.time()
                elapsed = now - self.last_update
                self.tokens = min(self.rate, self.tokens + elapsed * self.rate)
                self.last_update = now

                if self.tokens >= 1.0:
                    self.tokens -= 1.0
                    return  # Token acquired

                # Calculate wait time and release lock before sleeping
                wait_time = (1.0 - self.tokens) / self.rate

            # CRITICAL: Sleep OUTSIDE lock so other workers can proceed
            time.sleep(wait_time)

    def try_acquire(self) -> bool:
        """Try to acquire a token without blocking.

        Returns:
            True if token acquired, False if no tokens available
        """
        with self.lock:
            now = time.time()
            elapsed = now - self.last_update
            self.tokens = min(self.rate, self.tokens + elapsed * self.rate)
            self.last_update = now

            if self.tokens >= 1.0:
                self.tokens -= 1.0
                return True

            return False

    def reset(self):
        """Reset the token bucket to full capacity."""
        with self.lock:
            self.tokens = self.rate
            self.last_update = time.time()

    def get_available_tokens(self) -> float:
        """Get current number of available tokens (for testing/debugging)."""
        with self.lock:
            now = time.time()
            elapsed = now - self.last_update
            return min(self.rate, self.tokens + elapsed * self.rate)
