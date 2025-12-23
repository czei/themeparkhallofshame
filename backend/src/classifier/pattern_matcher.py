"""
Theme Park Downtime Tracker - Pattern-Based Ride Classifier
Implements keyword-based tier classification (Priority 3 in classification hierarchy).
"""

import re
from typing import Optional, Tuple
from dataclasses import dataclass

from src.utils.logger import logger


@dataclass
class PatternMatchResult:
    """Result of pattern-based classification."""
    tier: Optional[int]
    category: str  # 'ATTRACTION', 'MEET_AND_GREET', 'SHOW', 'EXPERIENCE'
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

    # Category patterns (checked before tier patterns)
    # MEET_AND_GREET: Character encounters
    MEET_AND_GREET_PATTERNS = [
        (r'\bmeet\b', 'Meet and Greet'),
        (r'\bcharacter\b.*\b(spot|encounter|greeting|experience)\b', 'Character encounter'),
        (r'\bgreeting\b', 'Character greeting'),
        (r'\bphoto\b.*\b(opportunity|spot|op)\b', 'Photo opportunity'),
        (r'\bencounter\b.*\b(character|princess|villain)\b', 'Character encounter'),
    ]

    # SHOW: Theater shows, presentations
    SHOW_PATTERNS = [
        (r'\bshow\b', 'Show'),
        (r'\btheater\b', 'Theater'),
        (r'\btheatre\b', 'Theatre'),
        (r'\bpresents?\b', 'Presentation'),
        (r'\bspectacular\b', 'Spectacular show'),
        (r'\bmusical\b', 'Musical show'),
        (r'\b4-?d\b', '4D show'),
        (r'\bsing[- ]?along\b', 'Sing-along show'),
        (r'\bstunt\b', 'Stunt show'),
        (r'\bfireworks\b', 'Fireworks show'),
        (r'\bparade\b', 'Parade'),
    ]

    # EXPERIENCE: Walk-throughs, exhibits, trails
    EXPERIENCE_PATTERNS = [
        (r'\btrail\b', 'Trail'),
        (r'\bexhibit\b', 'Exhibit'),
        (r'\bdiscovery\b.*\b(center|zone|area)\b', 'Discovery center'),
        (r'\bwalk[- ]?through\b', 'Walk-through'),
        (r'\btour\b', 'Tour'),
        (r'\bplay\s*(ground|area|zone)\b', 'Playground'),
    ]

    def __init__(self):
        """Initialize pattern matcher with compiled regex patterns."""
        self.tier_1_compiled = [(re.compile(pattern, re.IGNORECASE), desc)
                                for pattern, desc in self.TIER_1_PATTERNS]
        self.tier_3_compiled = [(re.compile(pattern, re.IGNORECASE), desc)
                                for pattern, desc in self.TIER_3_PATTERNS]
        # Category patterns
        self.meet_greet_compiled = [(re.compile(pattern, re.IGNORECASE), desc)
                                    for pattern, desc in self.MEET_AND_GREET_PATTERNS]
        self.show_compiled = [(re.compile(pattern, re.IGNORECASE), desc)
                              for pattern, desc in self.SHOW_PATTERNS]
        self.experience_compiled = [(re.compile(pattern, re.IGNORECASE), desc)
                                    for pattern, desc in self.EXPERIENCE_PATTERNS]

    def _detect_category(self, ride_name: str) -> Tuple[str, Optional[str]]:
        """
        Detect ride category based on name patterns.

        Returns:
            Tuple of (category, matched_description or None)
        """
        # Check MEET_AND_GREET patterns
        for pattern, description in self.meet_greet_compiled:
            if pattern.search(ride_name):
                return ('MEET_AND_GREET', description)

        # Check SHOW patterns
        for pattern, description in self.show_compiled:
            if pattern.search(ride_name):
                return ('SHOW', description)

        # Check EXPERIENCE patterns
        for pattern, description in self.experience_compiled:
            if pattern.search(ride_name):
                return ('EXPERIENCE', description)

        # Default to ATTRACTION
        return ('ATTRACTION', None)

    def classify(self, ride_name: str, park_name: Optional[str] = None) -> PatternMatchResult:
        """
        Classify ride based on name patterns.

        Args:
            ride_name: Name of the ride
            park_name: Name of the park (optional, for context)

        Returns:
            PatternMatchResult with tier, category, confidence, and reasoning
        """
        ride_name_lower = ride_name.lower()

        # First, detect category
        category, category_match = self._detect_category(ride_name)

        # Check Tier 1 patterns (highest priority)
        for pattern, description in self.tier_1_compiled:
            if pattern.search(ride_name):
                reasoning = f"Matched Tier 1 pattern: {description}"
                if category_match:
                    reasoning += f"; Category: {category_match}"
                logger.debug(f"Pattern match: {ride_name} -> Tier 1, {category} ({description})")
                return PatternMatchResult(
                    tier=1,
                    category=category,
                    confidence=0.75,
                    reasoning=reasoning,
                    matched_pattern=pattern.pattern
                )

        # Check Tier 3 patterns (kiddie rides, theaters)
        for pattern, description in self.tier_3_compiled:
            if pattern.search(ride_name):
                reasoning = f"Matched Tier 3 pattern: {description}"
                if category_match:
                    reasoning += f"; Category: {category_match}"
                logger.debug(f"Pattern match: {ride_name} -> Tier 3, {category} ({description})")
                return PatternMatchResult(
                    tier=3,
                    category=category,
                    confidence=0.70,
                    reasoning=reasoning,
                    matched_pattern=pattern.pattern
                )

        # Check for Tier 2 keywords (default for generic rides)
        for keyword in self.TIER_2_KEYWORDS:
            if keyword in ride_name_lower:
                reasoning = f"Standard attraction with keyword '{keyword}'"
                if category_match:
                    reasoning += f"; Category: {category_match}"
                logger.debug(f"Pattern match: {ride_name} -> Tier 2, {category} (generic attraction)")
                return PatternMatchResult(
                    tier=2,
                    category=category,
                    confidence=0.60,
                    reasoning=reasoning,
                    matched_pattern=f"\\b{keyword}\\b"
                )

        # No tier pattern match, but category may still be detected
        if category != 'ATTRACTION':
            # Non-attraction categories without tier match default to Tier 3
            reasoning = f"Category detected: {category_match}"
            logger.debug(f"Pattern match: {ride_name} -> Tier 3 (default), {category}")
            return PatternMatchResult(
                tier=3,
                category=category,
                confidence=0.65,
                reasoning=reasoning,
                matched_pattern=None
            )

        # No pattern match at all
        logger.debug(f"No pattern match for: {ride_name}")
        return PatternMatchResult(
            tier=None,
            category='ATTRACTION',  # Default category
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
