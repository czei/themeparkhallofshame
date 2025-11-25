"""
Theme Park Downtime Tracker - Classification Service Unit Tests

Tests ClassificationService:
- Classification result dataclass

Note: Classification service methods require file I/O, database operations,
and AI integration. These are tested in integration tests.

Priority: P2 - Important for ride classification orchestration
"""

from classifier.classification_service import ClassificationResult


class TestClassificationResultDataclass:
    """Test ClassificationResult dataclass."""

    def test_classification_result_all_fields(self):
        """ClassificationResult should have all required fields."""
        result = ClassificationResult(
            ride_id=1,
            ride_name="Space Mountain",
            park_id=101,
            park_name="Magic Kingdom",
            tier=1,
            tier_weight=3,
            classification_method="manual_override",
            confidence_score=1.00,
            reasoning_text="Manual override for testing",
            override_reason="Known E-ticket attraction",
            research_sources=["https://example.com"],
            cache_key="magic-kingdom_space-mountain",
            flagged_for_review=False
        )

        assert result.ride_id == 1
        assert result.ride_name == "Space Mountain"
        assert result.park_id == 101
        assert result.park_name == "Magic Kingdom"
        assert result.tier == 1
        assert result.tier_weight == 3
        assert result.classification_method == "manual_override"
        assert result.confidence_score == 1.00
        assert result.reasoning_text == "Manual override for testing"
        assert result.override_reason == "Known E-ticket attraction"
        assert result.research_sources == ["https://example.com"]
        assert result.cache_key == "magic-kingdom_space-mountain"
        assert result.flagged_for_review is False

    def test_classification_result_optional_fields_none(self):
        """ClassificationResult should allow None for optional fields."""
        result = ClassificationResult(
            ride_id=1,
            ride_name="Test Ride",
            park_id=101,
            park_name="Test Park",
            tier=2,
            tier_weight=2,
            classification_method="pattern_match",
            confidence_score=0.70,
            reasoning_text="Pattern matched",
            override_reason=None,
            research_sources=None,
            cache_key=None,
            flagged_for_review=True
        )

        assert result.override_reason is None
        assert result.research_sources is None
        assert result.cache_key is None
        assert result.flagged_for_review is True
