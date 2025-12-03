"""
Yesterday Query Classes
=======================

Query handlers for the YESTERDAY time period.
Returns data from the previous complete Pacific day.

Unlike TODAY (which is partial and changes), YESTERDAY is immutable
and highly cacheable.
"""

from .yesterday_park_rankings import YesterdayParkRankingsQuery
from .yesterday_ride_rankings import YesterdayRideRankingsQuery

__all__ = [
    'YesterdayParkRankingsQuery',
    'YesterdayRideRankingsQuery',
]
