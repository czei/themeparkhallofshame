"""
SQLAlchemy ORM Model: RideClassification
Represents ride tier classification data for shame score weighting.
"""

from sqlalchemy import String, Integer, ForeignKey, DateTime, Enum, Numeric, Text, func
from sqlalchemy.orm import Mapped, mapped_column, relationship
from src.models.base import Base
from datetime import datetime
from typing import Optional
from decimal import Decimal


class RideClassification(Base):
    """
    Tier classification data for rides, determining shame score weighting.

    Classification Methods (priority order):
      1. manual_override: Human-specified tier
      2. cached_ai: Previously computed AI classification
      3. pattern_match: Regex-based classification
      4. ai_agent: Real-time AI classification

    Tier Weights:
      tier=1 -> tier_weight=3 (flagship, 3x impact on shame score)
      tier=2 -> tier_weight=2 (standard, 2x impact)
      tier=3 -> tier_weight=1 (minor, 1x impact)
    """
    __tablename__ = "ride_classifications"
    __table_args__ = {'extend_existing': True}

    # Primary Key
    classification_id: Mapped[int] = mapped_column(primary_key=True)

    # Foreign Keys
    ride_id: Mapped[int] = mapped_column(
        ForeignKey("rides.ride_id", ondelete="CASCADE"),
        nullable=False,
        unique=True
    )

    # Classification Data
    tier: Mapped[int] = mapped_column(
        Integer,
        nullable=False,
        comment="1 (flagship), 2 (standard), or 3 (minor)"
    )
    tier_weight: Mapped[int] = mapped_column(
        Integer,
        nullable=False,
        comment="3, 2, or 1 - multiplier for shame score"
    )
    category: Mapped[Optional[str]] = mapped_column(
        Enum('ATTRACTION', 'MEET_AND_GREET', 'SHOW', 'EXPERIENCE', name='rc_category_enum'),
        server_default='ATTRACTION'
    )
    classification_method: Mapped[str] = mapped_column(
        Enum('manual_override', 'cached_ai', 'pattern_match', 'ai_agent', name='classification_method_enum'),
        nullable=False
    )
    confidence_score: Mapped[Optional[Decimal]] = mapped_column(
        Numeric(3, 2),
        comment="Confidence score for AI classifications"
    )
    reasoning_text: Mapped[Optional[str]] = mapped_column(
        Text,
        comment="AI reasoning for classification"
    )
    override_reason: Mapped[Optional[str]] = mapped_column(
        String(500),
        comment="Reason for manual override"
    )
    research_sources: Mapped[Optional[str]] = mapped_column(
        Text,
        comment="JSON array of research sources"
    )
    cache_key: Mapped[Optional[str]] = mapped_column(String(50))
    schema_version: Mapped[str] = mapped_column(
        String(10),
        server_default="1.0"
    )

    # Timestamps
    classified_at: Mapped[datetime] = mapped_column(
        DateTime,
        nullable=False,
        server_default=func.now()
    )
    updated_at: Mapped[datetime] = mapped_column(
        DateTime,
        nullable=False,
        server_default=func.now(),
        onupdate=func.now()
    )

    # Relationships
    ride: Mapped["Ride"] = relationship(
        "Ride",
        back_populates="classification"
    )

    def __repr__(self) -> str:
        return f"<RideClassification(ride_id={self.ride_id}, tier={self.tier}, weight={self.tier_weight})>"
