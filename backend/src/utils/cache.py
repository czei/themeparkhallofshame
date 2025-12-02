"""
Query Result Cache with TTL
===========================

A simple, thread-safe in-memory cache for query results.

Features:
- Configurable TTL (default 5 minutes)
- Thread-safe operations
- Automatic expiration
- Key generation from query parameters

Usage:
    from utils.cache import get_query_cache, generate_cache_key

    cache = get_query_cache()
    cache_key = generate_cache_key("parks_downtime", period="today", filter="disney")

    result = cache.get_or_compute(
        key=cache_key,
        compute_fn=lambda: expensive_database_query()
    )

Performance Impact:
    - First request: Executes query, caches result
    - Subsequent requests (within TTL): Returns cached result instantly
    - After TTL: Recomputes and caches new result
"""

import time
import hashlib
from typing import Any, Callable, Optional, TypeVar
from threading import Lock

T = TypeVar('T')


class QueryCache:
    """
    Thread-safe in-memory cache with configurable TTL.

    Attributes:
        _cache: Dictionary storing (value, timestamp) tuples
        _lock: Threading lock for thread safety
        _ttl: Time-to-live in seconds
    """

    def __init__(self, ttl_seconds: int = 300):
        """
        Initialize cache with TTL.

        Args:
            ttl_seconds: Time-to-live for cached entries (default 5 minutes)
        """
        self._cache: dict[str, tuple[Any, float]] = {}
        self._lock = Lock()
        self._ttl = ttl_seconds

    def get(self, key: str) -> Optional[Any]:
        """
        Get cached value if valid.

        Args:
            key: Cache key

        Returns:
            Cached value if valid, None otherwise
        """
        with self._lock:
            if key in self._cache:
                value, timestamp = self._cache[key]
                if time.time() - timestamp < self._ttl:
                    return value
        return None

    def set(self, key: str, value: Any) -> None:
        """
        Store value in cache.

        Args:
            key: Cache key
            value: Value to cache
        """
        with self._lock:
            self._cache[key] = (value, time.time())

    def get_or_compute(self, key: str, compute_fn: Callable[[], T]) -> T:
        """
        Get cached value or compute and cache new value.

        Thread-safe implementation:
        1. Check cache under lock
        2. If valid cached value exists, return it
        3. Otherwise, release lock, compute value
        4. Re-acquire lock, cache and return value

        This approach prevents blocking during expensive computations.

        Args:
            key: Cache key
            compute_fn: Function to compute value if not cached

        Returns:
            Cached or computed value
        """
        # Check cache first
        cached = self.get(key)
        if cached is not None:
            return cached

        # Compute value outside lock to avoid blocking other threads
        result = compute_fn()

        # Cache the result
        self.set(key, result)

        return result

    def invalidate(self, key: Optional[str] = None) -> None:
        """
        Clear cache entry or all entries.

        Args:
            key: Specific key to invalidate, or None to clear all
        """
        with self._lock:
            if key is not None:
                self._cache.pop(key, None)
            else:
                self._cache.clear()

    def get_stats(self) -> dict[str, Any]:
        """
        Get cache statistics for monitoring.

        Returns:
            Dictionary with cache stats
        """
        with self._lock:
            now = time.time()
            valid_entries = sum(
                1 for _, (_, ts) in self._cache.items()
                if now - ts < self._ttl
            )
            return {
                "total_entries": len(self._cache),
                "valid_entries": valid_entries,
                "ttl_seconds": self._ttl
            }


def generate_cache_key(endpoint: str, **params) -> str:
    """
    Generate consistent cache key from endpoint and parameters.

    Keys are deterministic - same inputs always produce same key.
    Parameter order doesn't matter (sorted before hashing).

    Args:
        endpoint: API endpoint name (e.g., "parks_downtime")
        **params: Query parameters (e.g., period="today", filter="disney")

    Returns:
        Cache key string in format "endpoint:hash"

    Example:
        >>> generate_cache_key("downtime", period="today", filter="disney")
        'downtime:a1b2c3d4'
    """
    # Sort params for consistent ordering
    param_str = "&".join(f"{k}={v}" for k, v in sorted(params.items()))
    # Use MD5 hash for compact key (not for security, just uniqueness)
    hash_value = hashlib.md5(param_str.encode()).hexdigest()[:8]
    return f"{endpoint}:{hash_value}"


# Global cache instance (5 minutes = 300 seconds TTL)
_query_cache: Optional[QueryCache] = None
_cache_lock = Lock()


def get_query_cache() -> QueryCache:
    """
    Get the global query cache singleton.

    Thread-safe lazy initialization ensures only one cache instance exists.

    Returns:
        Global QueryCache instance
    """
    global _query_cache
    if _query_cache is None:
        with _cache_lock:
            # Double-check locking pattern
            if _query_cache is None:
                _query_cache = QueryCache(ttl_seconds=300)
    return _query_cache


def reset_query_cache() -> None:
    """
    Reset the global cache (useful for testing).

    Creates a new cache instance, discarding all cached entries.
    """
    global _query_cache
    with _cache_lock:
        _query_cache = QueryCache(ttl_seconds=300)
