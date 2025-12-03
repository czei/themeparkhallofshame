"""
Theme Park Downtime Tracker - Classification Service
Orchestrates 4-tier hierarchical ride classification with caching and parallel processing.
"""

import csv
import json
import os
from typing import List, Dict, Any, Optional, Tuple
from dataclasses import dataclass
from datetime import datetime

from classifier.ai_classifier import AIClassifier
from classifier.pattern_matcher import PatternMatcher
from utils.logger import logger
from database.connection import get_db_connection
from sqlalchemy import text


VALID_CATEGORIES = ['ATTRACTION', 'MEET_AND_GREET', 'SHOW', 'EXPERIENCE']


@dataclass
class ClassificationResult:
    """Final classification result with metadata."""
    ride_id: int
    ride_name: str
    park_id: int
    park_name: str
    tier: int
    category: str  # 'ATTRACTION', 'MEET_AND_GREET', 'SHOW', 'EXPERIENCE'
    tier_weight: int
    classification_method: str  # manual_override, cached_ai, ai_agent
    confidence_score: float
    reasoning_text: str
    override_reason: Optional[str]
    research_sources: Optional[List[str]]
    cache_key: Optional[str]
    flagged_for_review: bool


class ClassificationService:
    """
    Hierarchical ride classification service.

    Classification priority (highest to lowest):
    1. Manual overrides (data/manual_overrides.csv) - confidence 1.00
    2. Cached AI classifications (data/exact_matches.json) - confidence >= 0.85
    3. AI agent (OpenAI GPT-4 with research) - confidence 0.50-1.00

    Features:
    - Automatic caching of high-confidence AI results (>= 0.85)
    - Parallel AI processing with ThreadPoolExecutor
    - Confidence-based flagging for human review (< 0.50)
    - Cache invalidation via schema versioning
    """

    CACHE_THRESHOLD = 0.85  # Confidence threshold for caching
    REVIEW_THRESHOLD = 0.50  # Confidence threshold for flagging review
    SCHEMA_VERSION = "2.0"  # Bumped for category support

    def __init__(
        self,
        manual_overrides_path: str = "data/manual_overrides.csv",
        category_overrides_path: str = "data/manual_category_overrides.csv",
        exact_matches_path: str = "data/exact_matches.json",
        working_directory: Optional[str] = None
    ):
        """
        Initialize classification service.

        Args:
            manual_overrides_path: Path to manual tier overrides CSV
            category_overrides_path: Path to manual category overrides CSV
            exact_matches_path: Path to cached classifications JSON
            working_directory: Working directory for AI classifier
        """
        self.manual_overrides_path = manual_overrides_path
        self.category_overrides_path = category_overrides_path
        self.exact_matches_path = exact_matches_path
        self.working_directory = working_directory or os.getcwd()

        self.ai_classifier = AIClassifier(working_directory=self.working_directory)
        self.pattern_matcher = PatternMatcher()

        # Load caches
        self.manual_overrides = self._load_manual_overrides()
        self.category_overrides = self._load_category_overrides()
        self.exact_matches = self._load_exact_matches()

        logger.info("ClassificationService initialized", extra={
            "manual_overrides_count": len(self.manual_overrides),
            "category_overrides_count": len(self.category_overrides),
            "exact_matches_count": len(self.exact_matches)
        })

    def _load_manual_overrides(self) -> Dict[Tuple[int, int], Dict[str, Any]]:
        """
        Load manual overrides from CSV.

        Returns:
            Dictionary mapping (park_id, ride_id) to override data
        """
        overrides = {}

        if not os.path.exists(self.manual_overrides_path):
            logger.warning(f"Manual overrides file not found: {self.manual_overrides_path}")
            return overrides

        try:
            with open(self.manual_overrides_path, 'r') as f:
                reader = csv.DictReader(f)
                for row in reader:
                    # Skip comments and empty lines
                    if not row.get('park_id') or row['park_id'].startswith('#'):
                        continue

                    park_id = int(row['park_id'])
                    ride_id = int(row['ride_id'])
                    tier = int(row['override_tier'])
                    reason = row['reason']
                    date_added = row.get('date_added', '')

                    overrides[(park_id, ride_id)] = {
                        'tier': tier,
                        'reason': reason,
                        'date_added': date_added
                    }

            logger.info(f"Loaded {len(overrides)} manual overrides")

        except Exception as e:
            logger.error(f"Failed to load manual overrides: {e}")

        return overrides

    def _load_category_overrides(self) -> Dict[Tuple[int, int], Dict[str, Any]]:
        """
        Load manual category overrides from CSV.

        Returns:
            Dictionary mapping (park_id, ride_id) to category override data
        """
        overrides = {}

        if not os.path.exists(self.category_overrides_path):
            logger.warning(f"Category overrides file not found: {self.category_overrides_path}")
            return overrides

        try:
            with open(self.category_overrides_path, 'r') as f:
                reader = csv.DictReader(f)
                for row in reader:
                    # Skip comments and empty lines
                    if not row.get('park_id') or row['park_id'].startswith('#'):
                        continue

                    park_id = int(row['park_id'])
                    ride_id = int(row['ride_id'])
                    category = row['override_category']
                    reason = row['reason']
                    date_added = row.get('date_added', '')

                    # Validate category
                    if category not in VALID_CATEGORIES:
                        logger.warning(f"Invalid category '{category}' for ride {ride_id}, skipping")
                        continue

                    overrides[(park_id, ride_id)] = {
                        'category': category,
                        'reason': reason,
                        'date_added': date_added
                    }

            logger.info(f"Loaded {len(overrides)} category overrides")

        except Exception as e:
            logger.error(f"Failed to load category overrides: {e}")

        return overrides

    def _load_exact_matches(self) -> Dict[str, Dict[str, Any]]:
        """
        Load cached AI classifications from JSON.

        Returns:
            Dictionary mapping cache_key to classification data
        """
        matches = {}

        if not os.path.exists(self.exact_matches_path):
            logger.warning(f"Exact matches file not found: {self.exact_matches_path}")
            return matches

        try:
            with open(self.exact_matches_path, 'r') as f:
                data = json.load(f)

            # Validate schema version
            schema_version = data.get('_meta', {}).get('schema_version', '1.0')
            if schema_version != self.SCHEMA_VERSION:
                logger.warning(f"Schema version mismatch: {schema_version} != {self.SCHEMA_VERSION}, invalidating cache")
                return {}

            matches = data.get('classifications', {})
            logger.info(f"Loaded {len(matches)} cached classifications")

        except Exception as e:
            logger.error(f"Failed to load exact matches: {e}")

        return matches

    def _save_exact_matches(self):
        """Save cached classifications to JSON file."""
        try:
            data = {
                "_meta": {
                    "description": "Cached ride tier classifications from AI agent (Priority 2)",
                    "schema_version": self.SCHEMA_VERSION,
                    "cache_invalidation_rules": [
                        "Cached entries are used when confidence_score >= 0.85",
                        "Cache is invalidated when schema_version changes",
                        "Entries can be manually removed to force re-classification"
                    ],
                    "cache_key_format": "{park_id}:{ride_id}",
                    "tier_weights": {
                        "1": 3,
                        "2": 2,
                        "3": 1
                    },
                    "last_updated": datetime.now().isoformat()
                },
                "classifications": self.exact_matches
            }

            with open(self.exact_matches_path, 'w') as f:
                json.dump(data, f, indent=2)

            logger.info(f"Saved {len(self.exact_matches)} cached classifications")

        except Exception as e:
            logger.error(f"Failed to save exact matches: {e}")

    def _get_category(self, park_id: int, ride_id: int, ride_name: Optional[str] = None,
                       ai_category: Optional[str] = None,
                       cached_category: Optional[str] = None) -> str:
        """
        Get category for a ride, checking overrides first.

        Priority:
        1. Manual category override
        2. AI/cached category
        3. Pattern matching (from ride name)
        4. Default to ATTRACTION
        """
        # Check manual category override (highest priority)
        cat_override = self.category_overrides.get((park_id, ride_id))
        if cat_override:
            return cat_override['category']

        # Use AI or cached category
        if ai_category and ai_category in VALID_CATEGORIES:
            return ai_category
        if cached_category and cached_category in VALID_CATEGORIES:
            return cached_category

        # Try pattern matching on ride name
        if ride_name:
            pattern_result = self.pattern_matcher.classify(ride_name)
            if pattern_result.category in VALID_CATEGORIES:
                return pattern_result.category

        # Default to ATTRACTION
        return 'ATTRACTION'

    def classify_ride(
        self,
        ride_id: int,
        ride_name: str,
        park_id: int,
        park_name: str,
        park_location: Optional[str] = None
    ) -> ClassificationResult:
        """
        Classify a single ride using 4-tier hierarchy.

        Args:
            ride_id: Ride ID
            ride_name: Ride name
            park_id: Park ID
            park_name: Park name
            park_location: Park location (city, state)

        Returns:
            ClassificationResult with tier and category
        """
        cache_key = f"{park_id}:{ride_id}"

        # Priority 1: Manual overrides (for tier)
        override = self.manual_overrides.get((park_id, ride_id))
        if override:
            category = self._get_category(park_id, ride_id, ride_name=ride_name)
            logger.info(f"Manual override for {ride_name}: Tier {override['tier']}, Category {category}")
            return ClassificationResult(
                ride_id=ride_id,
                ride_name=ride_name,
                park_id=park_id,
                park_name=park_name,
                tier=override['tier'],
                category=category,
                tier_weight=self._get_tier_weight(override['tier']),
                classification_method='manual_override',
                confidence_score=1.00,
                reasoning_text=override['reason'],
                override_reason=override['reason'],
                research_sources=None,
                cache_key=cache_key,
                flagged_for_review=False
            )

        # Priority 2: Cached AI classifications
        cached = self.exact_matches.get(cache_key)
        if cached:
            category = self._get_category(park_id, ride_id, ride_name=ride_name, cached_category=cached.get('category'))
            logger.info(f"Cached classification for {ride_name}: Tier {cached['tier']}, Category {category}")
            return ClassificationResult(
                ride_id=ride_id,
                ride_name=ride_name,
                park_id=park_id,
                park_name=park_name,
                tier=cached['tier'],
                category=category,
                tier_weight=self._get_tier_weight(cached['tier']),
                classification_method='cached_ai',
                confidence_score=cached['confidence'],
                reasoning_text=cached['reasoning'],
                override_reason=None,
                research_sources=cached.get('research_sources'),
                cache_key=cache_key,
                flagged_for_review=False
            )

        # Priority 3: AI agent classification (no pattern matching - name doesn't determine importance)
        logger.info(f"AI classification needed for {ride_name}")
        try:
            ai_result = self.ai_classifier.classify(
                ride_name=ride_name,
                park_name=park_name,
                park_location=park_location
            )

            # Cache high-confidence AI results for future lookups
            if ai_result.confidence >= self.CACHE_THRESHOLD:
                self._cache_ai_result(cache_key, ai_result)

            category = self._get_category(park_id, ride_id, ride_name=ride_name, ai_category=ai_result.category)
            return ClassificationResult(
                ride_id=ride_id,
                ride_name=ride_name,
                park_id=park_id,
                park_name=park_name,
                tier=ai_result.tier,
                category=category,
                tier_weight=self._get_tier_weight(ai_result.tier),
                classification_method='ai_agent',
                confidence_score=ai_result.confidence,
                reasoning_text=ai_result.reasoning,
                override_reason=None,
                research_sources=ai_result.research_sources,
                cache_key=cache_key,
                flagged_for_review=ai_result.confidence < self.REVIEW_THRESHOLD
            )

        except Exception as e:
            logger.error(f"AI classification failed for {ride_name}: {e}")
            # Fall back to Tier 2 default with low confidence
            category = self._get_category(park_id, ride_id, ride_name=ride_name)
            return ClassificationResult(
                ride_id=ride_id,
                ride_name=ride_name,
                park_id=park_id,
                park_name=park_name,
                tier=2,  # Default to Tier 2 (standard)
                category=category,
                tier_weight=2,
                classification_method='ai_agent_failed',
                confidence_score=0.30,  # Very low confidence
                reasoning_text=f"AI classification failed: {str(e)}",
                override_reason=None,
                research_sources=None,
                cache_key=cache_key,
                flagged_for_review=True
            )

    def classify_batch(
        self,
        rides: List[Dict[str, Any]],
        max_concurrent_ai: int = 5
    ) -> List[ClassificationResult]:
        """
        Classify multiple rides with parallel AI processing.

        Args:
            rides: List of dicts with keys: ride_id, ride_name, park_id, park_name, park_location
            max_concurrent_ai: Maximum concurrent AI requests

        Returns:
            List of ClassificationResult objects
        """
        results = []
        ai_pending = []

        # First pass: Check manual overrides, cache, and patterns
        for ride in rides:
            result = self.classify_ride(
                ride_id=ride['ride_id'],
                ride_name=ride['ride_name'],
                park_id=ride['park_id'],
                park_name=ride['park_name'],
                park_location=ride.get('park_location')
            )

            if result.classification_method == 'ai_agent' and result.confidence_score < self.REVIEW_THRESHOLD:
                # Needs AI classification
                ai_pending.append(ride)
            else:
                results.append(result)

        logger.info(f"Classified {len(results)} rides without AI, {len(ai_pending)} need AI")

        # Second pass: Parallel AI classification (if needed)
        if ai_pending:
            ai_results = self._batch_ai_classify(ai_pending, max_concurrent_ai)
            results.extend(ai_results)

        return results

    def _batch_ai_classify(
        self,
        rides: List[Dict[str, Any]],
        max_concurrent: int
    ) -> List[ClassificationResult]:
        """
        Classify rides using AI with ThreadPoolExecutor.

        Args:
            rides: List of rides needing AI classification
            max_concurrent: Maximum concurrent AI requests

        Returns:
            List of ClassificationResult objects
        """
        # Placeholder for AI batch processing
        # In real implementation, this would use ThreadPoolExecutor
        # with mcp__zen__chat calls
        logger.info(f"AI batch classification for {len(rides)} rides (max_concurrent={max_concurrent})")

        results = []
        for ride in rides:
            # Return placeholder results
            cache_key = f"{ride['park_id']}:{ride['ride_id']}"
            category = self._get_category(ride['park_id'], ride['ride_id'], ride_name=ride['ride_name'])
            results.append(ClassificationResult(
                ride_id=ride['ride_id'],
                ride_name=ride['ride_name'],
                park_id=ride['park_id'],
                park_name=ride['park_name'],
                tier=2,
                category=category,
                tier_weight=2,
                classification_method='ai_agent',
                confidence_score=0.40,
                reasoning_text="AI classification pending - requires MCP zen__chat integration",
                override_reason=None,
                research_sources=None,
                cache_key=cache_key,
                flagged_for_review=True
            ))

        return results

    def save_classification(self, result: ClassificationResult, conn=None):
        """
        Save classification result to database.

        Args:
            result: ClassificationResult to save
            conn: Optional database connection (for testing with transactions)
        """
        if conn is not None:
            # Use provided connection (for testing)
            self._save_classification_with_connection(conn, result)
        else:
            # Create new connection (production use)
            with get_db_connection() as conn:
                self._save_classification_with_connection(conn, result)

    def _save_classification_with_connection(self, conn, result: ClassificationResult):
        """Internal method to save classification with a given connection."""
        # Update ride tier and category
        update_ride = text("""
            UPDATE rides
            SET tier = :tier, category = :category
            WHERE ride_id = :ride_id
        """)

        conn.execute(update_ride, {
            "tier": result.tier,
            "category": result.category,
            "ride_id": result.ride_id
        })

        # Insert/update classification record
        upsert_classification = text("""
            INSERT INTO ride_classifications (
                ride_id, tier, tier_weight, category, classification_method,
                confidence_score, reasoning_text, override_reason,
                research_sources, cache_key, schema_version
            )
            VALUES (
                :ride_id, :tier, :tier_weight, :category, :classification_method,
                :confidence_score, :reasoning_text, :override_reason,
                :research_sources, :cache_key, :schema_version
            )
            ON DUPLICATE KEY UPDATE
                tier = VALUES(tier),
                tier_weight = VALUES(tier_weight),
                category = VALUES(category),
                classification_method = VALUES(classification_method),
                confidence_score = VALUES(confidence_score),
                reasoning_text = VALUES(reasoning_text),
                override_reason = VALUES(override_reason),
                research_sources = VALUES(research_sources),
                cache_key = VALUES(cache_key),
                schema_version = VALUES(schema_version),
                updated_at = CURRENT_TIMESTAMP
        """)

        conn.execute(upsert_classification, {
            "ride_id": result.ride_id,
            "tier": result.tier,
            "tier_weight": result.tier_weight,
            "category": result.category,
            "classification_method": result.classification_method,
            "confidence_score": result.confidence_score,
            "reasoning_text": result.reasoning_text,
            "override_reason": result.override_reason,
            "research_sources": json.dumps(result.research_sources) if result.research_sources else None,
            "cache_key": result.cache_key,
            "schema_version": self.SCHEMA_VERSION
        })

        logger.info(f"Saved classification for ride {result.ride_id}: Tier {result.tier}, Category {result.category}")

        # Cache high-confidence AI results
        if (result.classification_method == 'ai_agent' and
            result.confidence_score >= self.CACHE_THRESHOLD and
            result.cache_key):

            self.exact_matches[result.cache_key] = {
                "tier": result.tier,
                "category": result.category,
                "confidence": result.confidence_score,
                "reasoning": result.reasoning_text,
                "research_sources": result.research_sources or [],
                "cached_at": datetime.now().isoformat()
            }
            self._save_exact_matches()

    def _cache_ai_result(self, cache_key: str, ai_result) -> None:
        """Cache high-confidence AI classification result."""
        self.exact_matches[cache_key] = {
            "tier": ai_result.tier,
            "category": ai_result.category,
            "confidence": ai_result.confidence,
            "reasoning": ai_result.reasoning,
            "research_sources": ai_result.research_sources or [],
            "cached_at": datetime.now().isoformat()
        }
        self._save_exact_matches()
        logger.info(f"Cached AI result for {cache_key}: Tier {ai_result.tier}, Category {ai_result.category}")

    def _get_tier_weight(self, tier: int) -> int:
        """Get weight multiplier for tier."""
        weights = {1: 3, 2: 2, 3: 1}
        return weights.get(tier, 2)
