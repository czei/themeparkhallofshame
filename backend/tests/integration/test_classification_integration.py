"""
Theme Park Downtime Tracker - Classification Integration Tests

Tests complete classification workflow including database persistence and retrieval:
- Saving classifications (tier AND category) to both rides and ride_classifications tables
- Data consistency between the two tables
- Integration with weighted downtime calculations
- UPSERT behavior (INSERT vs UPDATE)
- Category filtering (ATTRACTION, MEET_AND_GREET, SHOW, EXPERIENCE)

Priority: P1 - Critical for weighted downtime calculations
"""

import pytest
import sys
import json
from pathlib import Path

backend_src = Path(__file__).parent.parent.parent / 'src'
sys.path.insert(0, str(backend_src.absolute()))

from classifier.classification_service import ClassificationService, ClassificationResult
from sqlalchemy import text


@pytest.fixture(scope="module", autouse=True)
def cleanup_before_classification_tests(mysql_engine):
    """Clean up all test data once at start of this test module."""
    from sqlalchemy import text
    with mysql_engine.begin() as conn:
        conn.execute(text("DELETE FROM ride_status_snapshots"))
        conn.execute(text("DELETE FROM ride_status_changes"))
        conn.execute(text("DELETE FROM park_activity_snapshots"))
        conn.execute(text("DELETE FROM ride_daily_stats"))
        conn.execute(text("DELETE FROM ride_weekly_stats"))
        conn.execute(text("DELETE FROM ride_monthly_stats"))
        conn.execute(text("DELETE FROM park_daily_stats"))
        conn.execute(text("DELETE FROM park_weekly_stats"))
        conn.execute(text("DELETE FROM park_monthly_stats"))
        conn.execute(text("DELETE FROM ride_classifications"))
        conn.execute(text("DELETE FROM rides"))
        conn.execute(text("DELETE FROM parks"))
    yield


class TestClassificationDatabasePersistence:
    """Test classification data is correctly saved to database."""

    def test_save_classification_creates_database_records(
        self, mysql_session, sample_park_data, sample_ride_data
    ):
        """Saving a classification should create records in both rides and ride_classifications tables."""
        from tests.conftest import insert_sample_park, insert_sample_ride

        # Setup: Create park and ride
        park_id = insert_sample_park(mysql_session, sample_park_data)
        sample_ride_data['park_id'] = park_id
        ride_id = insert_sample_ride(mysql_session, sample_ride_data)

        # Create classification service
        service = ClassificationService(
            manual_overrides_path='data/manual_overrides.csv',
            exact_matches_path='data/exact_matches.json'
        )

        # Create classification result (Tier 1, ATTRACTION)
        result = ClassificationResult(
            ride_id=ride_id,
            ride_name="Test Coaster",
            park_id=park_id,
            park_name="Test Park",
            tier=1,
            category="ATTRACTION",
            tier_weight=3,
            classification_method='ai_agent',
            confidence_score=0.95,
            reasoning_text="Major roller coaster with high capacity",
            override_reason=None,
            research_sources=["https://rcdb.com/test"],
            cache_key=f"{park_id}:{ride_id}",
            flagged_for_review=False
        )

        # Act: Save classification (pass connection for transaction)
        service.save_classification(result, conn=mysql_session)

        # Assert 1: rides.tier and rides.category updated
        ride_query = text("SELECT tier, category FROM rides WHERE ride_id = :ride_id")
        ride_result = mysql_session.execute(ride_query, {"ride_id": ride_id}).fetchone()
        assert ride_result is not None
        assert ride_result[0] == 1, "rides.tier should be updated to 1"
        assert ride_result[1] == "ATTRACTION", "rides.category should be ATTRACTION"

        # Assert 2: ride_classifications record created
        classification_query = text("""
            SELECT tier, tier_weight, category, classification_method, confidence_score,
                   reasoning_text, research_sources, cache_key
            FROM ride_classifications
            WHERE ride_id = :ride_id
        """)
        classification_result = mysql_session.execute(
            classification_query, {"ride_id": ride_id}
        ).fetchone()

        assert classification_result is not None, "ride_classifications record should exist"
        assert classification_result[0] == 1, "tier should be 1"
        assert classification_result[1] == 3, "tier_weight should be 3"
        assert classification_result[2] == "ATTRACTION", "category should be ATTRACTION"
        assert classification_result[3] == 'ai_agent', "classification_method should be ai_agent"
        assert float(classification_result[4]) == 0.95, "confidence_score should be 0.95"
        assert "Major roller coaster" in classification_result[5], "reasoning_text should be saved"
        assert classification_result[6] is not None, "research_sources should be saved"
        assert classification_result[7] == f"{park_id}:{ride_id}", "cache_key should match"

    def test_save_classification_updates_existing_records(
        self, mysql_session, sample_park_data, sample_ride_data
    ):
        """Saving a classification twice should UPDATE existing records (UPSERT)."""
        from tests.conftest import insert_sample_park, insert_sample_ride

        # Setup: Create park and ride
        park_id = insert_sample_park(mysql_session, sample_park_data)
        sample_ride_data['park_id'] = park_id
        ride_id = insert_sample_ride(mysql_session, sample_ride_data)

        service = ClassificationService(
            manual_overrides_path='data/manual_overrides.csv',
            exact_matches_path='data/exact_matches.json'
        )

        # Act 1: Save initial classification (Tier 2, ATTRACTION)
        result_tier_2 = ClassificationResult(
            ride_id=ride_id,
            ride_name="Test Ride",
            park_id=park_id,
            park_name="Test Park",
            tier=2,
            category="ATTRACTION",
            tier_weight=2,
            classification_method='ai_agent',
            confidence_score=0.80,
            reasoning_text="Standard dark ride",
            override_reason=None,
            research_sources=[],
            cache_key=f"{park_id}:{ride_id}",
            flagged_for_review=False
        )
        service.save_classification(result_tier_2, conn=mysql_session)

        # Act 2: Update to Tier 1 (re-classification)
        result_tier_1 = ClassificationResult(
            ride_id=ride_id,
            ride_name="Test Ride",
            park_id=park_id,
            park_name="Test Park",
            tier=1,
            category="ATTRACTION",
            tier_weight=3,
            classification_method='manual_override',
            confidence_score=1.00,
            reasoning_text="Upgraded to signature attraction",
            override_reason="Manual correction",
            research_sources=["https://example.com"],
            cache_key=f"{park_id}:{ride_id}",
            flagged_for_review=False
        )
        service.save_classification(result_tier_1, conn=mysql_session)

        # Assert 1: rides.tier and category updated to new value
        ride_query = text("SELECT tier, category FROM rides WHERE ride_id = :ride_id")
        ride_result = mysql_session.execute(ride_query, {"ride_id": ride_id}).fetchone()
        assert ride_result[0] == 1, "rides.tier should be updated to 1"
        assert ride_result[1] == "ATTRACTION", "rides.category should remain ATTRACTION"

        # Assert 2: ride_classifications updated (not duplicated)
        count_query = text(
            "SELECT COUNT(*) FROM ride_classifications WHERE ride_id = :ride_id"
        )
        count = mysql_session.execute(count_query, {"ride_id": ride_id}).fetchone()[0]
        assert count == 1, "Should have exactly 1 record (UPSERT, not duplicate)"

        # Assert 3: Classification data updated
        classification_query = text("""
            SELECT tier, tier_weight, category, classification_method, confidence_score, reasoning_text
            FROM ride_classifications
            WHERE ride_id = :ride_id
        """)
        result = mysql_session.execute(classification_query, {"ride_id": ride_id}).fetchone()
        assert result[0] == 1, "tier should be updated to 1"
        assert result[1] == 3, "tier_weight should be updated to 3"
        assert result[2] == "ATTRACTION", "category should be ATTRACTION"
        assert result[3] == 'manual_override', "classification_method should be updated"
        assert float(result[4]) == 1.00, "confidence_score should be updated to 1.00"
        assert "Upgraded to signature" in result[5], "reasoning_text should be updated"

    def test_rides_tier_and_classifications_tier_match(
        self, mysql_session, sample_park_data, sample_ride_data
    ):
        """The tier and category values in rides table should always match ride_classifications table."""
        from tests.conftest import insert_sample_park, insert_sample_ride

        park_id = insert_sample_park(mysql_session, sample_park_data)
        sample_ride_data['park_id'] = park_id
        ride_id = insert_sample_ride(mysql_session, sample_ride_data)

        service = ClassificationService(
            manual_overrides_path='data/manual_overrides.csv',
            exact_matches_path='data/exact_matches.json'
        )

        # Test all three tier levels with ATTRACTION category
        for tier, expected_weight in [(1, 3), (2, 2), (3, 1)]:
            result = ClassificationResult(
                ride_id=ride_id,
                ride_name="Test Ride",
                park_id=park_id,
                park_name="Test Park",
                tier=tier,
                category="ATTRACTION",
                tier_weight=expected_weight,
                classification_method='ai_agent',
                confidence_score=0.90,
                reasoning_text=f"Tier {tier} classification",
                override_reason=None,
                research_sources=[],
                cache_key=f"{park_id}:{ride_id}",
                flagged_for_review=False
            )
            service.save_classification(result, conn=mysql_session)

            # Verify consistency
            consistency_query = text("""
                SELECT r.tier as rides_tier, rc.tier as classifications_tier, rc.tier_weight,
                       r.category as rides_category, rc.category as classifications_category
                FROM rides r
                JOIN ride_classifications rc ON r.ride_id = rc.ride_id
                WHERE r.ride_id = :ride_id
            """)
            row = mysql_session.execute(consistency_query, {"ride_id": ride_id}).fetchone()

            assert row[0] == row[1], f"rides.tier ({row[0]}) should match ride_classifications.tier ({row[1]})"
            assert row[0] == tier, f"Both tiers should be {tier}"
            assert row[2] == expected_weight, f"tier_weight should be {expected_weight} for tier {tier}"
            assert row[3] == row[4], f"rides.category ({row[3]}) should match ride_classifications.category ({row[4]})"
            assert row[3] == "ATTRACTION", "Both categories should be ATTRACTION"


class TestClassificationIntegrationWithCalculations:
    """Test that saved classifications work correctly with weighted downtime calculations."""

    def test_classification_available_for_weighted_calculations(
        self, mysql_session, sample_park_data
    ):
        """After saving a classification, ParkRepository should retrieve correct tier_weight for calculations."""
        from tests.conftest import insert_sample_park

        # Setup: Create park with 3 rides of different tiers
        park_id = insert_sample_park(mysql_session, sample_park_data)

        # Insert 3 rides
        rides_data = [
            (1, "Major Coaster", 1, 3),      # Tier 1, weight 3
            (2, "Standard Ride", 2, 2),      # Tier 2, weight 2
            (3, "Kiddie Ride", 3, 1),        # Tier 3, weight 1
        ]

        for ride_id, ride_name, tier, tier_weight in rides_data:
            # Insert ride
            insert_ride = text("""
                INSERT INTO rides (ride_id, park_id, queue_times_id, name, is_active)
                VALUES (:ride_id, :park_id, :queue_times_id, :name, 1)
            """)
            mysql_session.execute(insert_ride, {
                "ride_id": ride_id,
                "park_id": park_id,
                "queue_times_id": 1000 + ride_id,  # Unique queue_times_id
                "name": ride_name
            })

            # Save classification
            service = ClassificationService(
                manual_overrides_path='data/manual_overrides.csv',
                exact_matches_path='data/exact_matches.json'
            )
            result = ClassificationResult(
                ride_id=ride_id,
                ride_name=ride_name,
                park_id=park_id,
                park_name="Test Park",
                tier=tier,
                category="ATTRACTION",
                tier_weight=tier_weight,
                classification_method='ai_agent',
                confidence_score=0.90,
                reasoning_text=f"Classified as Tier {tier}",
                override_reason=None,
                research_sources=[],
                cache_key=f"{park_id}:{ride_id}",
                flagged_for_review=False
            )
            service.save_classification(result, conn=mysql_session)

        # Verify: Query tier_weight from ride_classifications (as weighted calculations do)
        weighted_query = text("""
            SELECT r.ride_id, r.name, IFNULL(rc.tier_weight, 2) as tier_weight
            FROM rides r
            LEFT JOIN ride_classifications rc ON r.ride_id = rc.ride_id
            WHERE r.park_id = :park_id
            ORDER BY r.ride_id
        """)
        results = mysql_session.execute(weighted_query, {"park_id": park_id}).fetchall()

        assert len(results) == 3, "Should have 3 rides"
        assert results[0][2] == 3, "Ride 1 should have tier_weight 3"
        assert results[1][2] == 2, "Ride 2 should have tier_weight 2"
        assert results[2][2] == 1, "Ride 3 should have tier_weight 1"

    def test_unclassified_ride_defaults_to_weight_2(self, mysql_session, sample_park_data):
        """Unclassified rides should default to tier_weight=2 in weighted calculations."""
        from tests.conftest import insert_sample_park

        park_id = insert_sample_park(mysql_session, sample_park_data)

        # Insert ride WITHOUT classification
        insert_ride = text("""
            INSERT INTO rides (ride_id, park_id, queue_times_id, name, is_active)
            VALUES (:ride_id, :park_id, :queue_times_id, :name, 1)
        """)
        mysql_session.execute(insert_ride, {
            "ride_id": 999,
            "park_id": park_id,
            "queue_times_id": 999999,  # Unique queue_times_id
            "name": "Unclassified Ride"
        })

        # Verify: LEFT JOIN with IFNULL defaults to 2
        weighted_query = text("""
            SELECT r.ride_id, r.name, IFNULL(rc.tier_weight, 2) as tier_weight
            FROM rides r
            LEFT JOIN ride_classifications rc ON r.ride_id = rc.ride_id
            WHERE r.ride_id = 999
        """)
        result = mysql_session.execute(weighted_query).fetchone()

        assert result is not None, "Unclassified ride should exist"
        assert result[2] == 2, "Unclassified ride should default to tier_weight=2"

    def test_classification_metadata_persists(self, mysql_session, sample_park_data, sample_ride_data):
        """All classification metadata fields should be correctly persisted."""
        from tests.conftest import insert_sample_park, insert_sample_ride

        park_id = insert_sample_park(mysql_session, sample_park_data)
        sample_ride_data['park_id'] = park_id
        ride_id = insert_sample_ride(mysql_session, sample_ride_data)

        service = ClassificationService(
            manual_overrides_path='data/manual_overrides.csv',
            exact_matches_path='data/exact_matches.json'
        )

        # Create result with all metadata
        result = ClassificationResult(
            ride_id=ride_id,
            ride_name="Space Mountain",
            park_id=park_id,
            park_name="Magic Kingdom",
            tier=1,
            category="ATTRACTION",
            tier_weight=3,
            classification_method='ai_agent',
            confidence_score=0.95,
            reasoning_text="Iconic indoor roller coaster with high capacity and long wait times",
            override_reason=None,
            research_sources=["https://rcdb.com/123", "https://wikipedia.org/Space_Mountain"],
            cache_key=f"{park_id}:{ride_id}",
            flagged_for_review=False
        )
        service.save_classification(result, conn=mysql_session)

        # Verify all fields
        query = text("""
            SELECT tier, tier_weight, category, classification_method, confidence_score,
                   reasoning_text, override_reason, research_sources, cache_key,
                   schema_version, classified_at, updated_at
            FROM ride_classifications
            WHERE ride_id = :ride_id
        """)
        row = mysql_session.execute(query, {"ride_id": ride_id}).fetchone()

        assert row[0] == 1, "tier should be 1"
        assert row[1] == 3, "tier_weight should be 3"
        assert row[2] == "ATTRACTION", "category should be ATTRACTION"
        assert row[3] == 'ai_agent', "classification_method should be ai_agent"
        assert float(row[4]) == 0.95, "confidence_score should be 0.95"
        assert "Iconic indoor roller coaster" in row[5], "reasoning_text should be saved"
        assert row[6] is None, "override_reason should be NULL for ai_agent"

        # Parse research_sources JSON
        research_sources = json.loads(row[7]) if row[7] else []
        assert len(research_sources) == 2, "Should have 2 research sources"
        assert "rcdb.com" in research_sources[0], "First source should be RCDB"

        assert row[8] == f"{park_id}:{ride_id}", "cache_key should match"
        assert row[9] == "2.0", "schema_version should be 2.0"
        assert row[10] is not None, "classified_at timestamp should exist"
        assert row[11] is not None, "updated_at timestamp should exist"
