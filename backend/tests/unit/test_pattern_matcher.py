"""
Theme Park Downtime Tracker - Pattern Matcher Unit Tests

Tests PatternMatcher:
- Tier 1 classification (major attractions, coasters, towers)
- Tier 2 classification (standard dark rides, shows)
- Tier 3 classification (kiddie rides, carousels, theaters)
- Confidence scoring
- Batch classification
- Edge cases (no matches, case insensitivity)

Priority: P2 - Important for ride classification system
"""

import pytest
from classifier.pattern_matcher import PatternMatcher, PatternMatchResult


class TestPatternMatcherInit:
    """Test pattern matcher initialization."""

    def test_init_compiles_regex_patterns(self):
        """__init__() should compile all regex patterns."""
        matcher = PatternMatcher()

        # Should have compiled patterns for Tier 1 and Tier 3
        assert len(matcher.tier_1_compiled) == 12  # Number of Tier 1 patterns
        assert len(matcher.tier_3_compiled) == 12  # Number of Tier 3 patterns

        # Each compiled pattern should have pattern and description
        for pattern, description in matcher.tier_1_compiled:
            assert hasattr(pattern, 'search')  # Compiled regex
            assert isinstance(description, str)


class TestClassifyTier1:
    """Test Tier 1 (major attraction) classification."""

    def test_classify_coaster(self):
        """classify() should detect 'coaster' keyword as Tier 1."""
        matcher = PatternMatcher()

        result = matcher.classify("Space Mountain Coaster")

        assert result.tier == 1
        assert result.confidence == 0.75
        assert "Roller coaster" in result.reasoning
        assert result.matched_pattern is not None

    def test_classify_mountain(self):
        """classify() should detect 'mountain' keyword as Tier 1."""
        matcher = PatternMatcher()

        result = matcher.classify("Big Thunder Mountain Railroad")

        assert result.tier == 1
        assert result.confidence == 0.75
        assert "Mountain attraction" in result.reasoning

    def test_classify_tower(self):
        """classify() should detect 'tower' keyword as Tier 1."""
        matcher = PatternMatcher()

        result = matcher.classify("Tower of Terror")

        assert result.tier == 1
        assert result.confidence == 0.75
        assert "Tower ride" in result.reasoning

    def test_classify_drop(self):
        """classify() should detect 'drop' keyword as Tier 1."""
        matcher = PatternMatcher()

        result = matcher.classify("Guardians Galaxy Drop")

        assert result.tier == 1
        assert result.confidence == 0.75
        assert "Drop ride" in result.reasoning

    def test_classify_splash(self):
        """classify() should detect 'splash' keyword as Tier 1."""
        matcher = PatternMatcher()

        # Note: "Splash Mountain" matches "mountain" first (pattern order)
        result = matcher.classify("Splash Mountain")

        assert result.tier == 1
        assert result.confidence == 0.75
        # Matches "mountain" pattern, not "splash"
        assert "Mountain attraction" in result.reasoning

    def test_classify_splash_without_mountain(self):
        """classify() should detect 'splash' keyword when no other Tier 1 patterns match."""
        matcher = PatternMatcher()

        result = matcher.classify("Logger's Splash Run")

        assert result.tier == 1
        assert result.confidence == 0.75
        assert "Splash/water ride" in result.reasoning

    def test_classify_case_insensitive(self):
        """classify() should be case-insensitive."""
        matcher = PatternMatcher()

        result = matcher.classify("SPACE MOUNTAIN COASTER")

        assert result.tier == 1
        assert result.confidence == 0.75


class TestClassifyTier3:
    """Test Tier 3 (minor attraction) classification."""

    def test_classify_kiddie(self):
        """classify() should detect 'kiddie' keyword as Tier 3."""
        matcher = PatternMatcher()

        # Note: "Kiddie Coaster" matches "coaster" (Tier 1) first due to pattern priority
        # Use a different ride name that doesn't have Tier 1 keywords
        result = matcher.classify("Kiddie Train")

        assert result.tier == 3
        assert result.confidence == 0.70
        assert "Kiddie ride" in result.reasoning
        assert result.matched_pattern is not None

    def test_classify_carousel(self):
        """classify() should detect 'carousel' keyword as Tier 3."""
        matcher = PatternMatcher()

        result = matcher.classify("Prince Charming Carousel")

        assert result.tier == 3
        assert result.confidence == 0.70
        assert "Carousel" in result.reasoning

    def test_classify_theater(self):
        """classify() should detect 'theater' keyword as Tier 3."""
        matcher = PatternMatcher()

        result = matcher.classify("Main Street Theater")

        assert result.tier == 3
        assert result.confidence == 0.70
        assert "Theater show" in result.reasoning

    def test_classify_dumbo(self):
        """classify() should detect 'dumbo' keyword as Tier 3."""
        matcher = PatternMatcher()

        result = matcher.classify("Dumbo the Flying Elephant")

        assert result.tier == 3
        assert result.confidence == 0.70
        assert "Dumbo-style spinner" in result.reasoning

    def test_classify_teacups(self):
        """classify() should detect 'teacups' keyword as Tier 3."""
        matcher = PatternMatcher()

        result = matcher.classify("Mad Tea Party Teacups")

        assert result.tier == 3
        assert result.confidence == 0.70

    def test_classify_merry_go_round(self):
        """classify() should detect 'merry-go-round' variations as Tier 3."""
        matcher = PatternMatcher()

        result1 = matcher.classify("Classic Merry-Go-Round")
        result2 = matcher.classify("Classic Merrygoround")

        assert result1.tier == 3
        assert result2.tier == 3


class TestClassifyTier2:
    """Test Tier 2 (standard attraction) classification."""

    def test_classify_ride_keyword(self):
        """classify() should detect 'ride' keyword as Tier 2."""
        matcher = PatternMatcher()

        result = matcher.classify("Haunted Mansion Ride")

        assert result.tier == 2
        assert result.confidence == 0.60
        assert "Standard attraction with keyword 'ride'" in result.reasoning
        assert result.matched_pattern is not None

    def test_classify_adventure_keyword(self):
        """classify() should detect 'adventure' keyword as Tier 2."""
        matcher = PatternMatcher()

        result = matcher.classify("Indiana Jones Adventure")

        assert result.tier == 2
        assert result.confidence == 0.60
        assert "adventure" in result.reasoning

    def test_classify_safari_keyword(self):
        """classify() should detect 'safari' keyword as Tier 2."""
        matcher = PatternMatcher()

        result = matcher.classify("Kilimanjaro Safaris")

        assert result.tier == 2
        assert result.confidence == 0.60

    def test_classify_cruise_keyword(self):
        """classify() should detect 'cruise' keyword as Tier 2."""
        matcher = PatternMatcher()

        result = matcher.classify("Jungle Cruise")

        assert result.tier == 2
        assert result.confidence == 0.60

    def test_classify_pirates_keyword(self):
        """classify() should detect 'pirates' keyword as Tier 2."""
        matcher = PatternMatcher()

        result = matcher.classify("Pirates of the Caribbean")

        assert result.tier == 2
        assert result.confidence == 0.60


class TestClassifyNoMatch:
    """Test classification when no patterns match."""

    def test_classify_no_match(self):
        """classify() should return None tier when no patterns match."""
        matcher = PatternMatcher()

        result = matcher.classify("Random Attraction XYZ")

        assert result.tier is None
        assert result.confidence == 0.0
        assert "No keyword pattern matched" in result.reasoning
        assert result.matched_pattern is None

    def test_classify_empty_string(self):
        """classify() should handle empty string gracefully."""
        matcher = PatternMatcher()

        result = matcher.classify("")

        assert result.tier is None
        assert result.confidence == 0.0


class TestClassifyPriority:
    """Test pattern matching priority (Tier 1 > Tier 3 > Tier 2)."""

    def test_tier_1_takes_priority_over_tier_3(self):
        """classify() should prioritize Tier 1 over Tier 3 when both match."""
        matcher = PatternMatcher()

        # "Kiddie Coaster" has both 'kiddie' (Tier 3) and 'coaster' (Tier 1)
        result = matcher.classify("Kiddie Coaster")

        # Tier 1 should win (checked first)
        assert result.tier == 1
        assert result.confidence == 0.75
        assert "Roller coaster" in result.reasoning

    def test_tier_3_takes_priority_over_tier_2(self):
        """classify() should prioritize Tier 3 over Tier 2 when both match."""
        matcher = PatternMatcher()

        # "Carousel Ride" has both 'carousel' (Tier 3) and 'ride' (Tier 2)
        result = matcher.classify("Carousel Ride")

        # Tier 3 should win (checked before Tier 2)
        assert result.tier == 3
        assert result.confidence == 0.70


class TestBatchClassify:
    """Test batch classification of multiple rides."""

    def test_batch_classify_multiple_rides(self):
        """batch_classify() should classify multiple rides."""
        matcher = PatternMatcher()

        rides = [
            (1, "Space Mountain Coaster", "Magic Kingdom"),
            (2, "Dumbo the Flying Elephant", "Magic Kingdom"),
            (3, "Jungle Cruise", "Magic Kingdom"),
            (4, "Unknown Attraction", "Magic Kingdom")
        ]

        results = matcher.batch_classify(rides)

        assert len(results) == 4
        assert results[1].tier == 1  # Space Mountain Coaster
        assert results[2].tier == 3  # Dumbo
        assert results[3].tier == 2  # Jungle Cruise
        assert results[4].tier is None  # Unknown

    def test_batch_classify_empty_list(self):
        """batch_classify() should handle empty list."""
        matcher = PatternMatcher()

        results = matcher.batch_classify([])

        assert results == {}

    def test_batch_classify_with_park_name(self):
        """batch_classify() should accept park_name parameter (currently unused)."""
        matcher = PatternMatcher()

        rides = [
            (1, "Tower of Terror", "Hollywood Studios")
        ]

        results = matcher.batch_classify(rides)

        assert len(results) == 1
        assert results[1].tier == 1


class TestPatternMatchResult:
    """Test PatternMatchResult dataclass."""

    def test_pattern_match_result_fields(self):
        """PatternMatchResult should have tier, confidence, reasoning, matched_pattern."""
        result = PatternMatchResult(
            tier=1,
            confidence=0.75,
            reasoning="Test",
            matched_pattern="\\bcoaster\\b"
        )

        assert result.tier == 1
        assert result.confidence == 0.75
        assert result.reasoning == "Test"
        assert result.matched_pattern == "\\bcoaster\\b"

    def test_pattern_match_result_none_values(self):
        """PatternMatchResult should allow None values."""
        result = PatternMatchResult(
            tier=None,
            confidence=0.0,
            reasoning="No match",
            matched_pattern=None
        )

        assert result.tier is None
        assert result.matched_pattern is None
