"""
Core Entity Tables
==================

Domain entities: parks, rides, ride_classifications

These are the primary entities in the system. Parks contain rides,
and rides have tier classifications that affect shame score weighting.

Database: MySQL/MariaDB
Source: migrations/001_initial_schema.sql, 006_add_category_column.sql, 008_themeparks_wiki.sql
"""

from sqlalchemy import (
    Table,
    Column,
    Integer,
    String,
    Boolean,
    DateTime,
    Numeric,
    Text,
    Enum,
    ForeignKey,
    Index,
)
from sqlalchemy.sql import func
from .metadata import metadata


# =============================================================================
# PARKS TABLE
# =============================================================================
# Core entity representing a theme park.
#
# Key columns for queries:
#   - park_id: Primary key, used in all joins
#   - is_disney, is_universal: Used for Disney & Universal filter
#   - is_active: Filter to only tracked parks
#   - timezone: For date calculations (aggregation uses park's local time)
#
# How to Modify:
#   - To add a column: Add here AND in 001_initial_schema.sql migration
#   - To add an index: Use Index() below AND update 004_indexes.sql
# =============================================================================

parks = Table(
    "parks",
    metadata,
    Column("park_id", Integer, primary_key=True, autoincrement=True),
    Column("queue_times_id", Integer, unique=True, nullable=False),
    Column("themeparks_wiki_id", String(36), nullable=True),
    Column("name", String(255), nullable=False),
    Column("city", String(100), nullable=False),
    Column("state_province", String(100), nullable=True),
    Column("country", String(2), nullable=False),
    Column("latitude", Numeric(10, 8), nullable=True),
    Column("longitude", Numeric(11, 8), nullable=True),
    Column("timezone", String(50), nullable=False, server_default="America/New_York"),
    Column("operator", String(100), nullable=True),
    Column("is_disney", Boolean, nullable=False, server_default="0"),
    Column("is_universal", Boolean, nullable=False, server_default="0"),
    Column("is_active", Boolean, nullable=False, server_default="1"),
    Column("created_at", DateTime, server_default=func.now()),
    Column("updated_at", DateTime, server_default=func.now(), onupdate=func.now()),
    # Indexes
    Index("idx_operator", "operator"),
    Index("idx_disney_universal", "is_disney", "is_universal"),
    Index("idx_country", "country"),
    Index("idx_active", "is_active"),
    Index("idx_themeparks_wiki_id", "themeparks_wiki_id"),
)


# =============================================================================
# RIDES TABLE
# =============================================================================
# Core entity representing a ride/attraction within a park.
#
# Key columns for queries:
#   - ride_id: Primary key
#   - park_id: Foreign key to parks
#   - tier: 1 (flagship), 2 (standard), 3 (minor) - affects shame score weight
#   - category: ATTRACTION, MEET_AND_GREET, SHOW, EXPERIENCE
#   - is_active: Filter to only tracked rides
#
# Tier Weights (for Shame Score):
#   Tier 1 = 3x weight (flagship rides like Space Mountain)
#   Tier 2 = 2x weight (standard rides like Haunted Mansion)
#   Tier 3 = 1x weight (minor rides like carousel)
#
# How to Modify:
#   - To add a column: Add here AND create a migration
#   - Category enum: Extend in 006_add_category_column.sql first
# =============================================================================

rides = Table(
    "rides",
    metadata,
    Column("ride_id", Integer, primary_key=True, autoincrement=True),
    Column("queue_times_id", Integer, unique=True, nullable=False),
    Column("themeparks_wiki_id", String(36), nullable=True),
    Column("park_id", Integer, ForeignKey("parks.park_id", ondelete="CASCADE"), nullable=False),
    Column("name", String(255), nullable=False),
    Column("entity_type", Enum("ATTRACTION", "SHOW", "RESTAURANT", name="entity_type_enum"), server_default="ATTRACTION"),
    Column("land_area", String(100), nullable=True),
    Column("tier", Integer, nullable=True),  # 1, 2, or 3
    Column(
        "category",
        Enum("ATTRACTION", "MEET_AND_GREET", "SHOW", "EXPERIENCE", name="category_enum"),
        server_default="ATTRACTION",
    ),
    Column("is_active", Boolean, nullable=False, server_default="1"),
    Column("created_at", DateTime, server_default=func.now()),
    Column("updated_at", DateTime, server_default=func.now(), onupdate=func.now()),
    # Indexes
    Index("idx_park_id", "park_id"),
    Index("idx_ride_active", "is_active"),
    Index("idx_park_active", "park_id", "is_active"),
    Index("idx_tier", "tier"),
    Index("idx_rides_category", "category"),
    Index("idx_entity_type", "entity_type"),
    Index("idx_rides_themeparks_wiki_id", "themeparks_wiki_id"),
)


# =============================================================================
# RIDE_CLASSIFICATIONS TABLE
# =============================================================================
# Tier classification data for rides, determining shame score weighting.
#
# Classification Methods (priority order):
#   1. manual_override: Human-specified tier
#   2. cached_ai: Previously computed AI classification
#   3. pattern_match: Regex-based classification
#   4. ai_agent: Real-time AI classification
#
# Tier Weights:
#   tier=1 -> tier_weight=3 (flagship, 3x impact on shame score)
#   tier=2 -> tier_weight=2 (standard, 2x impact)
#   tier=3 -> tier_weight=1 (minor, 1x impact)
#
# How to Modify:
#   - To change weights: Update this table AND metrics.py TIER_WEIGHTS
#   - To add classification method: Extend enum in migration first
# =============================================================================

ride_classifications = Table(
    "ride_classifications",
    metadata,
    Column("classification_id", Integer, primary_key=True, autoincrement=True),
    Column("ride_id", Integer, ForeignKey("rides.ride_id", ondelete="CASCADE"), nullable=False, unique=True),
    Column("tier", Integer, nullable=False),  # 1, 2, or 3
    Column("tier_weight", Integer, nullable=False),  # 3, 2, or 1
    Column(
        "category",
        Enum("ATTRACTION", "MEET_AND_GREET", "SHOW", "EXPERIENCE", name="rc_category_enum"),
        server_default="ATTRACTION",
    ),
    Column(
        "classification_method",
        Enum("manual_override", "cached_ai", "pattern_match", "ai_agent", name="classification_method_enum"),
        nullable=False,
    ),
    Column("confidence_score", Numeric(3, 2), nullable=True),
    Column("reasoning_text", Text, nullable=True),
    Column("override_reason", String(500), nullable=True),
    Column("research_sources", Text, nullable=True),  # JSON array
    Column("cache_key", String(50), nullable=True),
    Column("schema_version", String(10), server_default="1.0"),
    Column("classified_at", DateTime, server_default=func.now()),
    Column("updated_at", DateTime, server_default=func.now(), onupdate=func.now()),
    # Indexes
    Index("idx_rc_tier", "tier"),
    Index("idx_rc_method", "classification_method"),
    Index("idx_rc_confidence", "confidence_score"),
    Index("idx_rc_cache_key", "cache_key"),
)


# =============================================================================
# PARK_SCHEDULES TABLE
# =============================================================================
# Official operating schedules from ThemeParks.wiki API.
#
# This replaces the hacky "park_appears_open" heuristic that inferred park
# status from ride counts. Now we use actual schedule data from the API.
#
# Key columns for queries:
#   - park_id: Foreign key to parks
#   - schedule_date: Date in park's local timezone
#   - opening_time, closing_time: UTC times for easy comparison
#   - schedule_type: OPERATING (normal hours), TICKETED_EVENT, etc.
#
# How to use:
#   Check if park is open: NOW() BETWEEN opening_time AND closing_time
#                          AND schedule_type = 'OPERATING'
#
# How to Modify:
#   - To add schedule types: Update enum here AND in migration
# =============================================================================

park_schedules = Table(
    "park_schedules",
    metadata,
    Column("schedule_id", Integer, primary_key=True, autoincrement=True),
    Column("park_id", Integer, ForeignKey("parks.park_id", ondelete="CASCADE"), nullable=False),
    Column("schedule_date", DateTime, nullable=False),  # Date only, stored as datetime
    Column("opening_time", DateTime, nullable=True),  # UTC
    Column("closing_time", DateTime, nullable=True),  # UTC
    Column(
        "schedule_type",
        Enum("OPERATING", "TICKETED_EVENT", "PRIVATE_EVENT", "EXTRA_HOURS", "INFO", name="schedule_type_enum"),
        server_default="OPERATING",
    ),
    Column("fetched_at", DateTime, server_default=func.now()),
    Column("created_at", DateTime, server_default=func.now()),
    Column("updated_at", DateTime, server_default=func.now(), onupdate=func.now()),
    # Indexes
    Index("idx_ps_park_date", "park_id", "schedule_date"),
    Index("idx_ps_date", "schedule_date"),
    Index("idx_ps_fetched", "fetched_at"),
    Index("idx_ps_type", "schedule_type"),
)
