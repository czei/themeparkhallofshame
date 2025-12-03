"""
Query Cache Tests (TDD)
=======================

Tests for the in-memory query cache with TTL.

TDD Flow:
1. Tests FAIL initially (cache module doesn't exist)
2. Implement utils/cache.py
3. Tests PASS

The cache provides:
- Thread-safe in-memory caching
- Configurable TTL (default 5 minutes)
- Automatic expiration
- Key generation from query parameters
"""

import pytest
import time


class TestQueryCache:
    """Test the QueryCache class functionality."""

    def test_cache_module_exists(self):
        """The cache module should exist and be importable."""
        try:
            from utils.cache import QueryCache
            assert QueryCache is not None
        except ImportError:
            pytest.fail(
                "utils.cache module not found. "
                "Create backend/src/utils/cache.py with QueryCache class."
            )

    def test_cache_returns_same_result_within_ttl(self):
        """Same query within TTL should return cached result."""
        from utils.cache import QueryCache

        cache = QueryCache(ttl_seconds=300)  # 5 minutes
        call_count = 0

        def expensive_query():
            nonlocal call_count
            call_count += 1
            return {"data": f"result_{call_count}", "timestamp": time.time()}

        # First call - should execute query
        result1 = cache.get_or_compute(key="test_key", compute_fn=expensive_query)

        # Second call - should return cached result
        result2 = cache.get_or_compute(key="test_key", compute_fn=expensive_query)

        # Third call - should still return cached result
        result3 = cache.get_or_compute(key="test_key", compute_fn=expensive_query)

        assert result1 == result2 == result3, "Cache should return same result within TTL"
        assert call_count == 1, "Query should only be executed once"

    def test_cache_expires_after_ttl(self):
        """Cache should expire and recompute after TTL."""
        from utils.cache import QueryCache

        cache = QueryCache(ttl_seconds=1)  # 1 second for fast testing
        call_count = 0

        def query():
            nonlocal call_count
            call_count += 1
            return f"result_{call_count}"

        # First call
        result1 = cache.get_or_compute(key="test", compute_fn=query)
        assert result1 == "result_1"
        assert call_count == 1

        # Wait past TTL
        time.sleep(1.1)

        # Should recompute
        result2 = cache.get_or_compute(key="test", compute_fn=query)
        assert result2 == "result_2"
        assert call_count == 2

    def test_different_keys_have_different_values(self):
        """Different cache keys should store different values."""
        from utils.cache import QueryCache

        cache = QueryCache(ttl_seconds=300)

        cache.get_or_compute(key="key_a", compute_fn=lambda: "value_a")
        cache.get_or_compute(key="key_b", compute_fn=lambda: "value_b")

        result_a = cache.get_or_compute(key="key_a", compute_fn=lambda: "should_not_see")
        result_b = cache.get_or_compute(key="key_b", compute_fn=lambda: "should_not_see")

        assert result_a == "value_a"
        assert result_b == "value_b"

    def test_cache_can_be_invalidated_by_key(self):
        """Invalidating a specific key should clear only that entry."""
        from utils.cache import QueryCache

        cache = QueryCache(ttl_seconds=300)
        call_count_a = 0
        call_count_b = 0

        def query_a():
            nonlocal call_count_a
            call_count_a += 1
            return f"a_{call_count_a}"

        def query_b():
            nonlocal call_count_b
            call_count_b += 1
            return f"b_{call_count_b}"

        # Populate cache
        cache.get_or_compute(key="key_a", compute_fn=query_a)
        cache.get_or_compute(key="key_b", compute_fn=query_b)

        # Invalidate only key_a
        cache.invalidate(key="key_a")

        # key_a should recompute
        result_a = cache.get_or_compute(key="key_a", compute_fn=query_a)
        assert result_a == "a_2"
        assert call_count_a == 2

        # key_b should still be cached
        result_b = cache.get_or_compute(key="key_b", compute_fn=query_b)
        assert result_b == "b_1"
        assert call_count_b == 1

    def test_cache_can_be_fully_cleared(self):
        """Invalidating without a key should clear all entries."""
        from utils.cache import QueryCache

        cache = QueryCache(ttl_seconds=300)
        call_counts = {"a": 0, "b": 0}

        def query_a():
            call_counts["a"] += 1
            return f"a_{call_counts['a']}"

        def query_b():
            call_counts["b"] += 1
            return f"b_{call_counts['b']}"

        # Populate cache
        cache.get_or_compute(key="key_a", compute_fn=query_a)
        cache.get_or_compute(key="key_b", compute_fn=query_b)

        # Clear all
        cache.invalidate()

        # Both should recompute
        cache.get_or_compute(key="key_a", compute_fn=query_a)
        cache.get_or_compute(key="key_b", compute_fn=query_b)

        assert call_counts["a"] == 2
        assert call_counts["b"] == 2


class TestCacheKeyGeneration:
    """Test the cache key generation function."""

    def test_generate_cache_key_exists(self):
        """The generate_cache_key function should exist."""
        try:
            from utils.cache import generate_cache_key
            assert generate_cache_key is not None
        except ImportError:
            pytest.fail(
                "generate_cache_key function not found in utils.cache. "
                "Add: def generate_cache_key(endpoint: str, **params) -> str"
            )

    def test_same_params_produce_same_key(self):
        """Same endpoint and params should produce identical keys."""
        from utils.cache import generate_cache_key

        key1 = generate_cache_key("downtime", period="today", filter="disney")
        key2 = generate_cache_key("downtime", period="today", filter="disney")

        assert key1 == key2

    def test_different_params_produce_different_keys(self):
        """Different params should produce different keys."""
        from utils.cache import generate_cache_key

        key1 = generate_cache_key("downtime", period="today", filter="disney")
        key2 = generate_cache_key("downtime", period="7days", filter="disney")
        key3 = generate_cache_key("downtime", period="today", filter="all")

        assert key1 != key2
        assert key1 != key3
        assert key2 != key3

    def test_param_order_does_not_affect_key(self):
        """Parameter order should not affect the cache key."""
        from utils.cache import generate_cache_key

        key1 = generate_cache_key("test", a="1", b="2", c="3")
        key2 = generate_cache_key("test", c="3", a="1", b="2")
        key3 = generate_cache_key("test", b="2", c="3", a="1")

        assert key1 == key2 == key3


class TestGlobalCacheInstance:
    """Test the global cache instance getter."""

    def test_get_query_cache_returns_singleton(self):
        """get_query_cache should return the same instance."""
        from utils.cache import get_query_cache

        cache1 = get_query_cache()
        cache2 = get_query_cache()

        assert cache1 is cache2, "get_query_cache should return singleton"

    def test_global_cache_has_5_minute_ttl(self):
        """Global cache should have 5-minute (300 second) TTL."""
        from utils.cache import get_query_cache

        cache = get_query_cache()

        # Check TTL is 300 seconds (5 minutes)
        assert hasattr(cache, '_ttl'), "Cache should have _ttl attribute"
        assert cache._ttl == 300, f"Global cache TTL should be 300s, got {cache._ttl}s"


class TestCacheThreadSafety:
    """Test thread safety of the cache."""

    def test_concurrent_access_is_safe(self):
        """Cache should handle concurrent access without errors."""
        from utils.cache import QueryCache
        import threading

        cache = QueryCache(ttl_seconds=300)
        results = []
        errors = []

        def query():
            return "cached_value"

        def worker():
            try:
                result = cache.get_or_compute(key="shared_key", compute_fn=query)
                results.append(result)
            except Exception as e:
                errors.append(str(e))

        # Start multiple threads accessing the same key
        threads = [threading.Thread(target=worker) for _ in range(10)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()

        # All threads should complete without errors
        assert len(errors) == 0, f"Thread errors: {errors}"
        assert len(results) == 10, "All threads should complete"

        # All results should be the same value
        assert all(r == "cached_value" for r in results), "All results should match"

    def test_cache_reduces_compute_calls_under_load(self):
        """Cache should reduce compute calls when accessed repeatedly."""
        from utils.cache import QueryCache
        import threading

        cache = QueryCache(ttl_seconds=300)
        call_count = 0
        lock = threading.Lock()

        def expensive_query():
            nonlocal call_count
            with lock:
                call_count += 1
            time.sleep(0.001)
            return "result"

        # First, prime the cache
        cache.get_or_compute(key="test", compute_fn=expensive_query)
        initial_calls = call_count

        # Now access it from multiple threads
        def worker():
            cache.get_or_compute(key="test", compute_fn=expensive_query)

        threads = [threading.Thread(target=worker) for _ in range(10)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()

        # After priming, no additional calls should be needed
        assert call_count == initial_calls, (
            f"Cache should prevent recomputation, but got {call_count - initial_calls} extra calls"
        )
