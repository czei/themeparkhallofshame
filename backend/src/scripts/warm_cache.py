#!/usr/bin/env python3
"""
Cache Warming Script
====================

Warms the API cache by hitting all main endpoints after data collection.
This ensures users always get fast cached responses.

Run after collect_snapshots completes:
    python -m scripts.warm_cache

Should complete in ~20 seconds on cold cache, <1 second on warm cache.
"""

import sys
import time
import urllib.request
from concurrent.futures import ThreadPoolExecutor, as_completed

# API base URL (internal, bypasses HTTPS)
BASE_URL = "http://127.0.0.1:5001"

# Endpoints to warm - these are the main page load requests
ENDPOINTS = [
    # LIVE period (default view on page load)
    "/api/parks/downtime?period=live&filter=all-parks&limit=50",
    "/api/parks/downtime?period=live&filter=disney-universal&limit=50",
    "/api/rides/downtime?period=live&filter=all-parks&limit=50",
    "/api/rides/downtime?period=live&filter=disney-universal&limit=50",
    "/api/live/status-summary?filter=all-parks",

    # TODAY period
    "/api/parks/downtime?period=today&filter=all-parks&limit=50",
    "/api/parks/downtime?period=today&filter=disney-universal&limit=50",
    "/api/rides/downtime?period=today&filter=all-parks&limit=50",
    "/api/rides/downtime?period=today&filter=disney-universal&limit=50",

    # 7 DAYS period
    "/api/parks/downtime?period=7days&filter=all-parks&limit=50",
    "/api/rides/downtime?period=7days&filter=all-parks&limit=50",

    # 30 DAYS period
    "/api/parks/downtime?period=30days&filter=all-parks&limit=50",
    "/api/rides/downtime?period=30days&filter=all-parks&limit=50",

    # Wait times (also requested on page load)
    "/api/parks/waittimes?period=live&filter=all-parks&limit=50",
    "/api/rides/waittimes?period=live&filter=all-parks&limit=50",

    # Trends tab - charts
    "/api/trends/chart-data?type=waittimes&period=today&limit=4",
    "/api/trends/chart-data?type=waittimes&period=7days&limit=4",

    # Trends tab - Awards (CRITICAL: Must be warmed to match Wait Times table)
    "/api/trends/longest-wait-times?period=today&filter=all-parks&entity=rides&limit=10",
    "/api/trends/longest-wait-times?period=today&filter=all-parks&entity=parks&limit=10",
    "/api/trends/least-reliable?period=today&filter=all-parks&entity=rides&limit=10",
    "/api/trends/least-reliable?period=today&filter=all-parks&entity=parks&limit=10",
]


def warm_endpoint(endpoint: str) -> tuple[str, float, bool]:
    """
    Hit an endpoint to warm its cache.

    Returns:
        (endpoint, duration_seconds, success)
    """
    url = f"{BASE_URL}{endpoint}"
    start = time.time()
    success = False

    try:
        req = urllib.request.Request(url)
        req.add_header("User-Agent", "CacheWarmer/1.0")
        with urllib.request.urlopen(req, timeout=120) as resp:
            resp.read()  # Consume response
            success = resp.status == 200
    except Exception as e:
        print(f"  Error warming {endpoint}: {e}", file=sys.stderr)

    duration = time.time() - start
    return (endpoint, duration, success)


def main():
    """Warm all cached endpoints."""
    print(f"Warming {len(ENDPOINTS)} endpoints...")
    start_time = time.time()

    # Use thread pool for concurrent requests (4 threads = 4 workers)
    results = []
    with ThreadPoolExecutor(max_workers=4) as executor:
        futures = {executor.submit(warm_endpoint, ep): ep for ep in ENDPOINTS}
        for future in as_completed(futures):
            endpoint, duration, success = future.result()
            status = "OK" if success else "FAIL"
            print(f"  [{status}] {endpoint[:50]:50} {duration:.2f}s")
            results.append((endpoint, duration, success))

    total_time = time.time() - start_time
    success_count = sum(1 for _, _, s in results if s)

    print("\nCache warming complete:")
    print(f"  Endpoints: {success_count}/{len(ENDPOINTS)} succeeded")
    print(f"  Total time: {total_time:.2f}s")

    if success_count < len(ENDPOINTS):
        sys.exit(1)


if __name__ == "__main__":
    main()
