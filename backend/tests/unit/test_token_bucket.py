"""
Unit Tests: TokenBucket Rate Limiter
=====================================

Tests the TokenBucket rate limiting algorithm with proper concurrency.

Critical Bug Fix (from Zen review):
- Lock MUST be released during sleep to allow concurrent workers
- Tests verify concurrent acquisition works correctly

Test Strategy:
- Mock time.time() for deterministic tests
- Test single-threaded behavior (basic algorithm)
- Test multi-threaded behavior (concurrency)
- Verify lock is released during sleep
"""

import pytest
import time
import threading
from unittest.mock import patch, MagicMock
from utils.rate_limiter import TokenBucket


class TestTokenBucket:
    """Unit tests for TokenBucket rate limiter."""

    def test_initialization_default_rate(self):
        """TokenBucket should initialize with default rate=1.0."""
        bucket = TokenBucket()
        assert bucket.rate == 1.0
        assert bucket.tokens == 1.0

    def test_initialization_custom_rate(self):
        """TokenBucket should accept custom rate."""
        bucket = TokenBucket(rate=5.0)
        assert bucket.rate == 5.0
        assert bucket.tokens == 5.0

    def test_acquire_with_available_token(self):
        """acquire() should return immediately when token available."""
        bucket = TokenBucket(rate=1.0)

        start = time.time()
        bucket.acquire()
        elapsed = time.time() - start

        # Should return almost immediately (< 0.1 seconds)
        assert elapsed < 0.1, \
            f"acquire() took {elapsed}s, expected < 0.1s"

        # Token should be consumed
        assert bucket.tokens < 1.0

    def test_acquire_blocks_when_no_tokens(self):
        """acquire() should block when no tokens available."""
        bucket = TokenBucket(rate=1.0)

        # Consume the initial token
        bucket.acquire()

        # Second acquire should block
        start = time.time()
        bucket.acquire()
        elapsed = time.time() - start

        # Should wait ~1 second for token to refill
        assert 0.9 <= elapsed <= 1.2, \
            f"acquire() took {elapsed}s, expected ~1.0s"

    def test_try_acquire_returns_true_when_token_available(self):
        """try_acquire() should return True when token available."""
        bucket = TokenBucket(rate=1.0)
        assert bucket.try_acquire() is True

    def test_try_acquire_returns_false_when_no_tokens(self):
        """try_acquire() should return False when no tokens available."""
        bucket = TokenBucket(rate=1.0)

        # Consume the initial token
        bucket.try_acquire()

        # No tokens left (not enough time for refill)
        assert bucket.try_acquire() is False

    def test_try_acquire_does_not_block(self):
        """try_acquire() should never block."""
        bucket = TokenBucket(rate=1.0)

        # Consume the initial token
        bucket.try_acquire()

        # Second try_acquire should return immediately (not block)
        start = time.time()
        result = bucket.try_acquire()
        elapsed = time.time() - start

        assert result is False
        assert elapsed < 0.1, \
            f"try_acquire() took {elapsed}s, should not block"

    def test_reset_refills_bucket(self):
        """reset() should refill tokens to max capacity."""
        bucket = TokenBucket(rate=5.0)

        # Consume some tokens
        bucket.acquire()
        bucket.acquire()

        # Reset should refill
        bucket.reset()
        assert bucket.tokens == 5.0

    def test_get_available_tokens_accuracy(self):
        """get_available_tokens() should return current token count."""
        bucket = TokenBucket(rate=2.0)

        # Initially should have 2 tokens
        assert bucket.get_available_tokens() == 2.0

        # After acquiring one
        bucket.acquire()
        tokens = bucket.get_available_tokens()
        assert 0.9 <= tokens <= 1.1  # ~1 token left

    def test_tokens_refill_over_time(self):
        """Tokens should refill at specified rate."""
        bucket = TokenBucket(rate=2.0)  # 2 tokens per second

        # Consume both tokens
        bucket.acquire()
        bucket.acquire()

        # Wait 0.5 seconds (should refill 1 token)
        time.sleep(0.5)

        # Should have ~1 token available
        tokens = bucket.get_available_tokens()
        assert 0.8 <= tokens <= 1.2, \
            f"Expected ~1 token after 0.5s, got {tokens}"

    def test_tokens_capped_at_max_rate(self):
        """Tokens should not exceed max rate even after long wait."""
        bucket = TokenBucket(rate=2.0)

        # Wait long time (should not accumulate more than 2 tokens)
        time.sleep(2.0)

        tokens = bucket.get_available_tokens()
        assert tokens <= 2.0, \
            f"Tokens should be capped at {bucket.rate}, got {tokens}"

    def test_concurrent_acquisition_works(self):
        """Multiple threads should be able to acquire tokens concurrently.

        CRITICAL: This verifies the concurrency bug fix where lock must
        be released during sleep.
        """
        bucket = TokenBucket(rate=2.0)  # 2 tokens per second
        acquired_times = []
        lock = threading.Lock()

        def worker():
            bucket.acquire()
            with lock:
                acquired_times.append(time.time())

        # Start 4 workers concurrently
        threads = [threading.Thread(target=worker) for _ in range(4)]
        start = time.time()

        for t in threads:
            t.start()

        for t in threads:
            t.join()

        elapsed = time.time() - start

        # 4 tokens at 2/sec rate should take ~2 seconds
        # (2 immediate + 2 after 1 second)
        assert 1.0 <= elapsed <= 2.5, \
            f"Concurrent acquisition took {elapsed}s, expected ~1-2s"

        # All 4 workers should have acquired
        assert len(acquired_times) == 4

    def test_lock_released_during_sleep(self):
        """Verify that lock is released while waiting for token.

        This is the CRITICAL bug fix from Zen review.
        """
        bucket = TokenBucket(rate=1.0)

        # Consume the initial token
        bucket.acquire()

        # Track whether lock is released during acquire()
        lock_was_available = False

        def checker():
            nonlocal lock_was_available
            time.sleep(0.1)  # Let main thread start blocking
            # Try to acquire lock (should succeed if released during sleep)
            if bucket.lock.acquire(blocking=False):
                lock_was_available = True
                bucket.lock.release()

        checker_thread = threading.Thread(target=checker)
        checker_thread.start()

        # This acquire() should block and release lock during sleep
        bucket.acquire()

        checker_thread.join()

        assert lock_was_available, \
            "Lock was NOT released during sleep - concurrency bug!"

    def test_wait_time_calculation_accuracy(self):
        """Wait time should be calculated correctly."""
        bucket = TokenBucket(rate=2.0)  # 2 tokens per second

        # Consume both tokens
        bucket.acquire()
        bucket.acquire()

        # Next acquire should wait ~0.5 seconds (1 token / 2 per sec)
        start = time.time()
        bucket.acquire()
        elapsed = time.time() - start

        assert 0.4 <= elapsed <= 0.7, \
            f"Expected ~0.5s wait, got {elapsed}s"
