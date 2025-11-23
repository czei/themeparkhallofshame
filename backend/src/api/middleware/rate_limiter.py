"""
Theme Park Downtime Tracker - Rate Limiting Middleware
Implements per-API-key rate limiting (100 req/hour, 1000 req/day).
"""

from functools import wraps
from datetime import datetime, timedelta
from collections import defaultdict
from flask import request, jsonify

try:
    from ...utils.logger import logger
except ImportError:
    from utils.logger import logger


class RateLimiter:
    """
    Simple in-memory rate limiter.

    Limits:
    - 100 requests per hour per API key
    - 1000 requests per day per API key

    For production: Consider using Redis for distributed rate limiting.
    """

    def __init__(
        self,
        hourly_limit: int = 100,
        daily_limit: int = 1000
    ):
        """
        Initialize rate limiter.

        Args:
            hourly_limit: Maximum requests per hour
            daily_limit: Maximum requests per day
        """
        self.hourly_limit = hourly_limit
        self.daily_limit = daily_limit

        # Storage: {api_key: {hour: count, day: count}}
        self.hourly_counts = defaultdict(lambda: defaultdict(int))
        self.daily_counts = defaultdict(lambda: defaultdict(int))

        logger.info(f"RateLimiter initialized (hourly={hourly_limit}, daily={daily_limit})")

    def _get_hour_key(self) -> str:
        """Get current hour key (YYYY-MM-DD-HH)."""
        return datetime.now().strftime('%Y-%m-%d-%H')

    def _get_day_key(self) -> str:
        """Get current day key (YYYY-MM-DD)."""
        return datetime.now().strftime('%Y-%m-%d')

    def _cleanup_old_entries(self):
        """Remove entries older than 24 hours."""
        cutoff_hour = (datetime.now() - timedelta(hours=1)).strftime('%Y-%m-%d-%H')
        cutoff_day = (datetime.now() - timedelta(days=1)).strftime('%Y-%m-%d')

        # Cleanup hourly counts
        for api_key in list(self.hourly_counts.keys()):
            old_hours = [h for h in self.hourly_counts[api_key] if h < cutoff_hour]
            for hour in old_hours:
                del self.hourly_counts[api_key][hour]

        # Cleanup daily counts
        for api_key in list(self.daily_counts.keys()):
            old_days = [d for d in self.daily_counts[api_key] if d < cutoff_day]
            for day in old_days:
                del self.daily_counts[api_key][day]

    def check_rate_limit(self, api_key: str) -> tuple[bool, str]:
        """
        Check if request is within rate limits.

        Args:
            api_key: API key to check

        Returns:
            Tuple of (allowed: bool, reason: str)
        """
        self._cleanup_old_entries()

        hour_key = self._get_hour_key()
        day_key = self._get_day_key()

        # Check hourly limit
        hourly_count = self.hourly_counts[api_key][hour_key]
        if hourly_count >= self.hourly_limit:
            return False, f"Hourly rate limit exceeded ({self.hourly_limit} req/hour)"

        # Check daily limit
        daily_count = self.daily_counts[api_key][day_key]
        if daily_count >= self.daily_limit:
            return False, f"Daily rate limit exceeded ({self.daily_limit} req/day)"

        # Increment counts
        self.hourly_counts[api_key][hour_key] += 1
        self.daily_counts[api_key][day_key] += 1

        return True, ""

    def limit_rate(self, f):
        """
        Decorator to apply rate limiting.

        Usage:
            @app.route('/api/endpoint')
            @rate_limiter.limit_rate
            def endpoint():
                return jsonify({"data": "value"})
        """
        @wraps(f)
        def decorated_function(*args, **kwargs):
            # Get API key from header (or use IP as fallback)
            api_key = request.headers.get('X-API-Key', request.remote_addr)

            # Check rate limit
            allowed, reason = self.check_rate_limit(api_key)

            if not allowed:
                logger.warning(f"Rate limit exceeded: {reason}", extra={
                    "api_key_prefix": api_key[:8] if len(api_key) >= 8 else "***",
                    "path": request.path,
                    "remote_addr": request.remote_addr
                })

                return jsonify({
                    "error": "Too Many Requests",
                    "message": reason
                }), 429

            # Request allowed
            return f(*args, **kwargs)

        return decorated_function


# Global instance
try:
    from ...utils.config import API_RATE_LIMIT_PER_HOUR, API_RATE_LIMIT_PER_DAY
except ImportError:
    from utils.config import API_RATE_LIMIT_PER_HOUR, API_RATE_LIMIT_PER_DAY

rate_limiter = RateLimiter(
    hourly_limit=API_RATE_LIMIT_PER_HOUR,
    daily_limit=API_RATE_LIMIT_PER_DAY
)
