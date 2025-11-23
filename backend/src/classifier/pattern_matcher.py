"""
Theme Park Downtime Tracker - Pattern-Based Ride Classifier
Implements keyword-based tier classification (Priority 3 in classification hierarchy).
"""

import re
from typing import Optional, Tuple
from dataclasses import dataclass

try:
    from ..utils.logger import logger
except ImportError:
    from utils.logger import logger


@dataclass
class PatternMatchResult:
    """Result of pattern-based classification."""
    tier: Optional[int]
    confidence: float
    reasoning: str
    matched_pattern: Optional[str]


class PatternMatcher:
    """
    Keyword-based ride tier classifier (Priority 3).

    Uses pattern matching on ride names to classify rides:
    - Tier 1 (major, 3x weight): Major attractions, coasters, mountains
    - Tier 2 (standard, 2x weight): Standard dark rides, shows
    - Tier 3 (minor, 1x weight): Kiddie rides, carousels, theaters

    Confidence scores: 0.6-0.75 (lower than AI or exact matches)
    """

    # Tier 1 patterns: Major attractions (3x weight)
    TIER_1_PATTERNS = [
        (r'\bcoaster\b', 'Roller coaster'),
        (r'\bmountain\b', 'Mountain attraction (typically coaster)'),
        (r'\bspace\b', 'Space-themed attraction (often major)'),
        (r'\btower\b', 'Tower ride (typically high-thrill)'),
        (r'\bdrop\b', 'Drop ride'),
        (r'\bexpedition\b', 'Expedition attraction (often major)'),
        (r'\bsplash\b', 'Splash/water ride (often major)'),
        (r'\bradio flyer\b', 'Radio Flyer attractions are coasters'),
        (r'\binverted\b', 'Inverted coaster'),
        (r'\bhyper\b', 'Hypercoaster'),
        (r'\bgiga\b', 'Giga coaster'),
        (r'\bwing\b.*\bcoaster\b', 'Wing coaster'),
    ]

    # Tier 3 patterns: Minor attractions (1x weight)
    TIER_3_PATTERNS = [
        (r'\bkiddie\b', 'Kiddie ride'),
        (r'\bcarousel\b', 'Carousel'),
        (r'\bteacups\b', 'Teacups ride'),
        (r'\bspinning\b.*\bteacups\b', 'Spinning teacups'),
        (r'\bdumbo\b', 'Dumbo-style spinner'),
        (r'\bspinner\b', 'Spinner ride'),
        (r'\bastrorbiter\b', 'Astro Orbiter-style spinner'),
        (r'\bcarpets\b', 'Flying carpets spinner'),
        (r'\btheater\b', 'Theater show (low capacity impact)'),
        (r'\bmerry.?go.?round\b', 'Merry-go-round'),
        (r'\bplayground\b', 'Playground area'),
        (r'\b(junior|jr\.?)\b', 'Junior/kid version'),
    ]

    # Tier 2 patterns: Standard attractions (2x weight)
    # Anything not matching Tier 1 or Tier 3 patterns
    TIER_2_KEYWORDS = [
        'ride', 'adventure', 'safari', 'cruise', 'journey',
        'voyage', 'flight', 'mansion', 'pirates', 'world'
    ]

    def __init__(self):
        """Initialize pattern matcher with compiled regex patterns."""
        self.tier_1_compiled = [(re.compile(pattern, re.IGNORECASE), desc)
                                for pattern, desc in self.TIER_1_PATTERNS]
        self.tier_3_compiled = [(re.compile(pattern, re.IGNORECASE), desc)
                                for pattern, desc in self.TIER_3_PATTERNS]

    def classify(self, ride_name: str, park_name: Optional[str] = None) -> PatternMatchResult:
        """
        Classify ride based on name patterns.

        Args:
            ride_name: Name of the ride
            park_name: Name of the park (optional, for context)

        Returns:
            PatternMatchResult with tier, confidence, and reasoning
        """
        ride_name_lower = ride_name.lower()

        # Check Tier 1 patterns (highest priority)
        for pattern, description in self.tier_1_compiled:
            if pattern.search(ride_name):
                logger.debug(f"Pattern match: {ride_name} -> Tier 1 ({description})")
                return PatternMatchResult(
                    tier=1,
                    confidence=0.75,
                    reasoning=f"Matched Tier 1 pattern: {description}",
                    matched_pattern=pattern.pattern
                )

        # Check Tier 3 patterns (kiddie rides, theaters)
        for pattern, description in self.tier_3_compiled:
            if pattern.search(ride_name):
                logger.debug(f"Pattern match: {ride_name} -> Tier 3 ({description})")
                return PatternMatchResult(
                    tier=3,
                    confidence=0.70,
                    reasoning=f"Matched Tier 3 pattern: {description}",
                    matched_pattern=pattern.pattern
                )

        # Check for Tier 2 keywords (default for generic rides)
        for keyword in self.TIER_2_KEYWORDS:
            if keyword in ride_name_lower:
                logger.debug(f"Pattern match: {ride_name} -> Tier 2 (generic attraction)")
                return PatternMatchResult(
                    tier=2,
                    confidence=0.60,
                    reasoning=f"Standard attraction with keyword '{keyword}'",
                    matched_pattern=f"\\b{keyword}\\b"
                )

        # No pattern match
        logger.debug(f"No pattern match for: {ride_name}")
        return PatternMatchResult(
            tier=None,
            confidence=0.0,
            reasoning="No keyword pattern matched",
            matched_pattern=None
        )

    def batch_classify(self, rides: list) -> dict:
        """
        Classify multiple rides at once.

        Args:
            rides: List of tuples (ride_id, ride_name, park_name)

        Returns:
            Dictionary mapping ride_id to PatternMatchResult
        """
        results = {}
        for ride_id, ride_name, park_name in rides:
            results[ride_id] = self.classify(ride_name, park_name)

        logger.info(f"Pattern-matched {len(results)} rides")
        return results
