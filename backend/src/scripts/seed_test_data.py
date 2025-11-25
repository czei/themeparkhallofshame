#!/usr/bin/env python3
"""
Theme Park Downtime Tracker - Test Data Seed Script
Populates the database with realistic test data for frontend development and testing.

This script creates:
- 50+ real theme parks (Disney, Universal, Six Flags, Cedar Fair, SeaWorld, etc.)
- 800-1000 rides with tier classifications
- Current ride status snapshots
- Daily, weekly, and monthly aggregate statistics
- Edge cases: perfect performers, problem parks, improving/declining trends

Usage:
    python -m src.scripts.seed_test_data
"""

import sys
import random
from pathlib import Path
from datetime import datetime, timedelta, date
from typing import List, Dict, Tuple
from decimal import Decimal

# Add src to path
backend_src = Path(__file__).parent.parent
sys.path.insert(0, str(backend_src.absolute()))

from sqlalchemy import text
from database.connection import get_db_connection

# ============================================================================
# PARK DATA - Real theme parks with locations
# ============================================================================

PARKS_DATA = [
    # Disney Parks (12)
    {"name": "Magic Kingdom", "city": "Orlando", "state": "FL", "country": "US", "tz": "America/New_York", "operator": "Disney", "is_disney": True, "is_universal": False},
    {"name": "EPCOT", "city": "Orlando", "state": "FL", "country": "US", "tz": "America/New_York", "operator": "Disney", "is_disney": True, "is_universal": False},
    {"name": "Hollywood Studios", "city": "Orlando", "state": "FL", "country": "US", "tz": "America/New_York", "operator": "Disney", "is_disney": True, "is_universal": False},
    {"name": "Animal Kingdom", "city": "Orlando", "state": "FL", "country": "US", "tz": "America/New_York", "operator": "Disney", "is_disney": True, "is_universal": False},
    {"name": "Disneyland", "city": "Anaheim", "state": "CA", "country": "US", "tz": "America/Los_Angeles", "operator": "Disney", "is_disney": True, "is_universal": False},
    {"name": "Disney California Adventure", "city": "Anaheim", "state": "CA", "country": "US", "tz": "America/Los_Angeles", "operator": "Disney", "is_disney": True, "is_universal": False},
    {"name": "Tokyo Disneyland", "city": "Urayasu", "state": "Chiba", "country": "JP", "tz": "Asia/Tokyo", "operator": "Disney", "is_disney": True, "is_universal": False},
    {"name": "Tokyo DisneySea", "city": "Urayasu", "state": "Chiba", "country": "JP", "tz": "Asia/Tokyo", "operator": "Disney", "is_disney": True, "is_universal": False},
    {"name": "Disneyland Paris", "city": "Marne-la-Vallée", "state": "", "country": "FR", "tz": "Europe/Paris", "operator": "Disney", "is_disney": True, "is_universal": False},
    {"name": "Walt Disney Studios Park", "city": "Marne-la-Vallée", "state": "", "country": "FR", "tz": "Europe/Paris", "operator": "Disney", "is_disney": True, "is_universal": False},
    {"name": "Hong Kong Disneyland", "city": "Lantau Island", "state": "", "country": "HK", "tz": "Asia/Hong_Kong", "operator": "Disney", "is_disney": True, "is_universal": False},
    {"name": "Shanghai Disneyland", "city": "Shanghai", "state": "", "country": "CN", "tz": "Asia/Shanghai", "operator": "Disney", "is_disney": True, "is_universal": False},

    # Universal Parks (6)
    {"name": "Universal Studios Florida", "city": "Orlando", "state": "FL", "country": "US", "tz": "America/New_York", "operator": "Universal", "is_disney": False, "is_universal": True},
    {"name": "Islands of Adventure", "city": "Orlando", "state": "FL", "country": "US", "tz": "America/New_York", "operator": "Universal", "is_disney": False, "is_universal": True},
    {"name": "Universal's Volcano Bay", "city": "Orlando", "state": "FL", "country": "US", "tz": "America/New_York", "operator": "Universal", "is_disney": False, "is_universal": True},
    {"name": "Universal Studios Hollywood", "city": "Los Angeles", "state": "CA", "country": "US", "tz": "America/Los_Angeles", "operator": "Universal", "is_disney": False, "is_universal": True},
    {"name": "Universal Studios Japan", "city": "Osaka", "state": "", "country": "JP", "tz": "Asia/Tokyo", "operator": "Universal", "is_disney": False, "is_universal": True},
    {"name": "Universal Studios Singapore", "city": "Sentosa", "state": "", "country": "SG", "tz": "Asia/Singapore", "operator": "Universal", "is_disney": False, "is_universal": True},

    # Six Flags (12)
    {"name": "Six Flags Magic Mountain", "city": "Valencia", "state": "CA", "country": "US", "tz": "America/Los_Angeles", "operator": "Six Flags", "is_disney": False, "is_universal": False},
    {"name": "Six Flags Great Adventure", "city": "Jackson", "state": "NJ", "country": "US", "tz": "America/New_York", "operator": "Six Flags", "is_disney": False, "is_universal": False},
    {"name": "Six Flags Great America", "city": "Gurnee", "state": "IL", "country": "US", "tz": "America/Chicago", "operator": "Six Flags", "is_disney": False, "is_universal": False},
    {"name": "Six Flags Over Texas", "city": "Arlington", "state": "TX", "country": "US", "tz": "America/Chicago", "operator": "Six Flags", "is_disney": False, "is_universal": False},
    {"name": "Six Flags Over Georgia", "city": "Austell", "state": "GA", "country": "US", "tz": "America/New_York", "operator": "Six Flags", "is_disney": False, "is_universal": False},
    {"name": "Six Flags Fiesta Texas", "city": "San Antonio", "state": "TX", "country": "US", "tz": "America/Chicago", "operator": "Six Flags", "is_disney": False, "is_universal": False},
    {"name": "Six Flags New England", "city": "Agawam", "state": "MA", "country": "US", "tz": "America/New_York", "operator": "Six Flags", "is_disney": False, "is_universal": False},
    {"name": "Six Flags St. Louis", "city": "Eureka", "state": "MO", "country": "US", "tz": "America/Chicago", "operator": "Six Flags", "is_disney": False, "is_universal": False},
    {"name": "Six Flags Discovery Kingdom", "city": "Vallejo", "state": "CA", "country": "US", "tz": "America/Los_Angeles", "operator": "Six Flags", "is_disney": False, "is_universal": False},
    {"name": "Six Flags America", "city": "Bowie", "state": "MD", "country": "US", "tz": "America/New_York", "operator": "Six Flags", "is_disney": False, "is_universal": False},
    {"name": "La Ronde", "city": "Montreal", "state": "QC", "country": "CA", "tz": "America/Montreal", "operator": "Six Flags", "is_disney": False, "is_universal": False},
    {"name": "Six Flags México", "city": "Mexico City", "state": "", "country": "MX", "tz": "America/Mexico_City", "operator": "Six Flags", "is_disney": False, "is_universal": False},

    # Cedar Fair (11)
    {"name": "Cedar Point", "city": "Sandusky", "state": "OH", "country": "US", "tz": "America/New_York", "operator": "Cedar Fair", "is_disney": False, "is_universal": False},
    {"name": "Kings Island", "city": "Mason", "state": "OH", "country": "US", "tz": "America/New_York", "operator": "Cedar Fair", "is_disney": False, "is_universal": False},
    {"name": "Knott's Berry Farm", "city": "Buena Park", "state": "CA", "country": "US", "tz": "America/Los_Angeles", "operator": "Cedar Fair", "is_disney": False, "is_universal": False},
    {"name": "Canada's Wonderland", "city": "Vaughan", "state": "ON", "country": "CA", "tz": "America/Toronto", "operator": "Cedar Fair", "is_disney": False, "is_universal": False},
    {"name": "Carowinds", "city": "Charlotte", "state": "NC", "country": "US", "tz": "America/New_York", "operator": "Cedar Fair", "is_disney": False, "is_universal": False},
    {"name": "Kings Dominion", "city": "Doswell", "state": "VA", "country": "US", "tz": "America/New_York", "operator": "Cedar Fair", "is_disney": False, "is_universal": False},
    {"name": "California's Great America", "city": "Santa Clara", "state": "CA", "country": "US", "tz": "America/Los_Angeles", "operator": "Cedar Fair", "is_disney": False, "is_universal": False},
    {"name": "Valleyfair", "city": "Shakopee", "state": "MN", "country": "US", "tz": "America/Chicago", "operator": "Cedar Fair", "is_disney": False, "is_universal": False},
    {"name": "Worlds of Fun", "city": "Kansas City", "state": "MO", "country": "US", "tz": "America/Chicago", "operator": "Cedar Fair", "is_disney": False, "is_universal": False},
    {"name": "Dorney Park", "city": "Allentown", "state": "PA", "country": "US", "tz": "America/New_York", "operator": "Cedar Fair", "is_disney": False, "is_universal": False},
    {"name": "Michigan's Adventure", "city": "Muskegon", "state": "MI", "country": "US", "tz": "America/Detroit", "operator": "Cedar Fair", "is_disney": False, "is_universal": False},

    # SeaWorld Parks (6)
    {"name": "SeaWorld Orlando", "city": "Orlando", "state": "FL", "country": "US", "tz": "America/New_York", "operator": "SeaWorld", "is_disney": False, "is_universal": False},
    {"name": "SeaWorld San Diego", "city": "San Diego", "state": "CA", "country": "US", "tz": "America/Los_Angeles", "operator": "SeaWorld", "is_disney": False, "is_universal": False},
    {"name": "SeaWorld San Antonio", "city": "San Antonio", "state": "TX", "country": "US", "tz": "America/Chicago", "operator": "SeaWorld", "is_disney": False, "is_universal": False},
    {"name": "Busch Gardens Tampa Bay", "city": "Tampa", "state": "FL", "country": "US", "tz": "America/New_York", "operator": "SeaWorld", "is_disney": False, "is_universal": False},
    {"name": "Busch Gardens Williamsburg", "city": "Williamsburg", "state": "VA", "country": "US", "tz": "America/New_York", "operator": "SeaWorld", "is_disney": False, "is_universal": False},
    {"name": "Sesame Place Philadelphia", "city": "Langhorne", "state": "PA", "country": "US", "tz": "America/New_York", "operator": "SeaWorld", "is_disney": False, "is_universal": False},

    # Other Major Parks (10)
    {"name": "Hersheypark", "city": "Hershey", "state": "PA", "country": "US", "tz": "America/New_York", "operator": "Hershey", "is_disney": False, "is_universal": False},
    {"name": "Dollywood", "city": "Pigeon Forge", "state": "TN", "country": "US", "tz": "America/New_York", "operator": "Dollywood", "is_disney": False, "is_universal": False},
    {"name": "Silver Dollar City", "city": "Branson", "state": "MO", "country": "US", "tz": "America/Chicago", "operator": "Herschend", "is_disney": False, "is_universal": False},
    {"name": "Legoland California", "city": "Carlsbad", "state": "CA", "country": "US", "tz": "America/Los_Angeles", "operator": "Merlin", "is_disney": False, "is_universal": False},
    {"name": "Legoland Florida", "city": "Winter Haven", "state": "FL", "country": "US", "tz": "America/New_York", "operator": "Merlin", "is_disney": False, "is_universal": False},
    {"name": "Holiday World", "city": "Santa Claus", "state": "IN", "country": "US", "tz": "America/Indianapolis", "operator": "Koch Family", "is_disney": False, "is_universal": False},
    {"name": "Adventureland", "city": "Altoona", "state": "IA", "country": "US", "tz": "America/Chicago", "operator": "Palace Entertainment", "is_disney": False, "is_universal": False},
    {"name": "Kennywood", "city": "West Mifflin", "state": "PA", "country": "US", "tz": "America/New_York", "operator": "Palace Entertainment", "is_disney": False, "is_universal": False},
    {"name": "Knoebels", "city": "Elysburg", "state": "PA", "country": "US", "tz": "America/New_York", "operator": "Independent", "is_disney": False, "is_universal": False},
    {"name": "Lagoon", "city": "Farmington", "state": "UT", "country": "US", "tz": "America/Denver", "operator": "Independent", "is_disney": False, "is_universal": False},
]

# ============================================================================
# RIDE DATA - Real rides per park (famous attractions)
# ============================================================================

RIDES_BY_PARK = {
    "Magic Kingdom": [
        ("Space Mountain", 1), ("Splash Mountain", 1), ("Big Thunder Mountain Railroad", 1),
        ("Pirates of the Caribbean", 1), ("Haunted Mansion", 1), ("Seven Dwarfs Mine Train", 1),
        ("TRON Lightcycle / Run", 1), ("Jungle Cruise", 2), ("It's a Small World", 2),
        ("Peter Pan's Flight", 2), ("The Many Adventures of Winnie the Pooh", 2),
        ("Buzz Lightyear's Space Ranger Spin", 2), ("Tomorrowland Speedway", 3),
        ("Mad Tea Party", 3), ("Dumbo the Flying Elephant", 3), ("Astro Orbiter", 3),
        ("Prince Charming Regal Carrousel", 3), ("The Barnstormer", 3),
    ],
    "EPCOT": [
        ("Guardians of the Galaxy: Cosmic Rewind", 1), ("Test Track", 1), ("Frozen Ever After", 1),
        ("Remy's Ratatouille Adventure", 1), ("Soarin' Around the World", 1),
        ("Spaceship Earth", 2), ("Living with the Land", 2), ("The Seas with Nemo & Friends", 2),
        ("Journey Into Imagination with Figment", 2), ("Gran Fiesta Tour", 3),
        ("Mission: SPACE", 2), ("Turtle Talk with Crush", 3),
    ],
    "Hollywood Studios": [
        ("Star Wars: Rise of the Resistance", 1), ("Tower of Terror", 1),
        ("Rock 'n' Roller Coaster", 1), ("Millennium Falcon: Smugglers Run", 1),
        ("Slinky Dog Dash", 1), ("Mickey & Minnie's Runaway Railway", 2),
        ("Toy Story Mania!", 2), ("Star Tours", 2), ("Alien Swirling Saucers", 3),
    ],
    "Animal Kingdom": [
        ("Avatar Flight of Passage", 1), ("Expedition Everest", 1), ("Kilimanjaro Safaris", 1),
        ("Na'vi River Journey", 2), ("Dinosaur", 2), ("Kali River Rapids", 2),
        ("TriceraTop Spin", 3), ("Primeval Whirl", 3),
    ],
    "Disneyland": [
        ("Space Mountain", 1), ("Matterhorn Bobsleds", 1), ("Indiana Jones Adventure", 1),
        ("Pirates of the Caribbean", 1), ("Haunted Mansion", 1), ("Big Thunder Mountain Railroad", 1),
        ("Splash Mountain", 1), ("Star Wars: Rise of the Resistance", 1),
        ("It's a Small World", 2), ("Jungle Cruise", 2), ("Mr. Toad's Wild Ride", 2),
        ("Alice in Wonderland", 2), ("Finding Nemo Submarine Voyage", 2),
        ("Dumbo the Flying Elephant", 3), ("Mad Tea Party", 3), ("Astro Orbiter", 3),
    ],
    "Disney California Adventure": [
        ("Radiator Springs Racers", 1), ("Guardians of the Galaxy - Mission: BREAKOUT!", 1),
        ("Incredicoaster", 1), ("Web Slingers: A Spider-Man Adventure", 1),
        ("Soarin' Around the World", 1), ("Grizzly River Run", 2),
        ("Toy Story Midway Mania!", 2), ("Monsters, Inc. Mike & Sulley to the Rescue!", 2),
        ("Luigi's Rollickin' Roadsters", 3), ("Mater's Junkyard Jamboree", 3),
    ],
    "Universal Studios Florida": [
        ("Hagrid's Magical Creatures Motorbike Adventure", 1), ("Revenge of the Mummy", 1),
        ("Harry Potter and the Escape from Gringotts", 1), ("Hollywood Rip Ride Rockit", 1),
        ("Transformers: The Ride-3D", 2), ("Men in Black: Alien Attack", 2),
        ("The Simpsons Ride", 2), ("E.T. Adventure", 2), ("Fast & Furious: Supercharged", 2),
        ("Kang & Kodos' Twirl 'n' Hurl", 3), ("Woody Woodpecker's Nuthouse Coaster", 3),
    ],
    "Islands of Adventure": [
        ("VelociCoaster", 1), ("Hagrid's Magical Creatures Motorbike Adventure", 1),
        ("The Incredible Hulk Coaster", 1), ("Harry Potter and the Forbidden Journey", 1),
        ("The Amazing Adventures of Spider-Man", 2), ("Jurassic World VelociCoaster", 1),
        ("Skull Island: Reign of Kong", 2), ("Doctor Doom's Fearfall", 2),
        ("Popeye & Bluto's Bilge-Rat Barges", 2), ("One Fish, Two Fish, Red Fish, Blue Fish", 3),
        ("Storm Force Accelatron", 3), ("Caro-Seuss-el", 3),
    ],
    "Cedar Point": [
        ("Steel Vengeance", 1), ("Millennium Force", 1), ("Top Thrill 2", 1),
        ("Maverick", 1), ("Valravn", 1), ("Gatekeeper", 1), ("Raptor", 1),
        ("Magnum XL-200", 2), ("Gemini", 2), ("Blue Streak", 2),
        ("Iron Dragon", 2), ("Rougarou", 2), ("Corkscrew", 3), ("Cedar Downs Racing Derby", 3),
        ("Pipe Scream", 3), ("Wilderness Run", 3), ("Kiddy Kingdom", 3),
    ],
    "Six Flags Magic Mountain": [
        ("X2", 1), ("Twisted Colossus", 1), ("Goliath", 1), ("Tatsu", 1),
        ("Full Throttle", 1), ("West Coast Racers", 1), ("Superman: Escape from Krypton", 1),
        ("Batman: The Ride", 2), ("Riddler's Revenge", 2), ("Scream!", 2),
        ("Ninja", 2), ("Viper", 2), ("Apocalypse", 2), ("Gold Rusher", 3),
        ("Canyon Blaster", 3), ("Road Runner Express", 3),
    ],
}

# Generic rides for parks without specific data
GENERIC_RIDES = {
    "tier1": ["Mega Coaster", "Hyper Coaster", "Dark Ride Adventure", "Launch Coaster", "Flying Coaster"],
    "tier2": ["Log Flume", "River Rapids", "Spinning Coaster", "Indoor Coaster", "3D Simulator", "Family Coaster"],
    "tier3": ["Tea Cups", "Carousel", "Ferris Wheel", "Kiddie Coaster", "Bumper Cars", "Swings", "Train Ride", "Boat Ride"],
}


class TestDataSeeder:
    """Seeds the database with comprehensive test data."""

    def __init__(self):
        self.stats = {
            'parks_inserted': 0,
            'rides_inserted': 0,
            'snapshots_inserted': 0,
            'daily_stats_inserted': 0,
            'weekly_stats_inserted': 0,
            'monthly_stats_inserted': 0,
        }
        self.park_ids = {}  # name -> park_id
        self.ride_ids = {}  # (park_id, ride_name) -> ride_id
        self.edge_case_parks = {}  # park_name -> edge_case_type
        self.edge_case_rides = {}  # ride_id -> edge_case_type

    def run(self):
        """Main execution method."""
        print("=" * 60)
        print("TEST DATA SEEDING - Starting")
        print("=" * 60)

        with get_db_connection() as conn:
            self._clear_existing_data(conn)
            self._seed_parks(conn)
            self._seed_rides(conn)
            self._assign_edge_cases()
            self._seed_ride_snapshots(conn)
            self._seed_daily_stats(conn)
            self._seed_weekly_stats(conn)
            self._seed_monthly_stats(conn)

        self._print_summary()
        print("=" * 60)
        print("TEST DATA SEEDING - Complete")
        print("=" * 60)

    def _clear_existing_data(self, conn):
        """Clear all existing data from relevant tables."""
        print("\nClearing existing data...")
        tables = [
            "ride_status_snapshots",
            "ride_status_changes",
            "park_activity_snapshots",
            "ride_daily_stats",
            "ride_weekly_stats",
            "ride_monthly_stats",
            "ride_yearly_stats",
            "park_daily_stats",
            "park_weekly_stats",
            "park_monthly_stats",
            "park_yearly_stats",
            "park_operating_sessions",
            "aggregation_log",
            "ride_classifications",
            "rides",
            "parks",
        ]
        for table in tables:
            try:
                conn.execute(text(f"DELETE FROM {table}"))
            except Exception as e:
                print(f"  Warning: Could not clear {table}: {e}")
        print("  Done clearing tables")

    def _seed_parks(self, conn):
        """Insert all parks."""
        print("\nSeeding parks...")
        for i, park in enumerate(PARKS_DATA):
            queue_times_id = 9000 + i  # Start high to avoid conflicts
            result = conn.execute(text("""
                INSERT INTO parks (queue_times_id, name, city, state_province, country, timezone, operator, is_disney, is_universal, is_active)
                VALUES (:qt_id, :name, :city, :state, :country, :tz, :operator, :is_disney, :is_universal, TRUE)
            """), {
                "qt_id": queue_times_id,
                "name": park["name"],
                "city": park["city"],
                "state": park["state"],
                "country": park["country"],
                "tz": park["tz"],
                "operator": park["operator"],
                "is_disney": park["is_disney"],
                "is_universal": park["is_universal"],
            })
            park_id = result.lastrowid
            self.park_ids[park["name"]] = park_id
            self.stats['parks_inserted'] += 1
        print(f"  Inserted {self.stats['parks_inserted']} parks")

    def _seed_rides(self, conn):
        """Insert rides for all parks."""
        print("\nSeeding rides...")
        ride_counter = 90000  # Start high for queue_times_id

        for park_name, park_id in self.park_ids.items():
            # Get specific rides if available, otherwise generate generic ones
            if park_name in RIDES_BY_PARK:
                rides = RIDES_BY_PARK[park_name]
            else:
                rides = self._generate_generic_rides(park_name)

            for ride_name, tier in rides:
                ride_counter += 1
                result = conn.execute(text("""
                    INSERT INTO rides (queue_times_id, park_id, name, tier, is_active)
                    VALUES (:qt_id, :park_id, :name, :tier, TRUE)
                """), {
                    "qt_id": ride_counter,
                    "park_id": park_id,
                    "name": ride_name,
                    "tier": tier,
                })
                ride_id = result.lastrowid
                self.ride_ids[(park_id, ride_name)] = ride_id
                self.stats['rides_inserted'] += 1

        print(f"  Inserted {self.stats['rides_inserted']} rides")

    def _generate_generic_rides(self, park_name: str) -> List[Tuple[str, int]]:
        """Generate generic rides for a park without specific data."""
        rides = []
        # 3-4 tier 1, 5-6 tier 2, 6-8 tier 3
        for i, name in enumerate(random.sample(GENERIC_RIDES["tier1"], min(4, len(GENERIC_RIDES["tier1"])))):
            rides.append((f"{name} {i+1}", 1))
        for i, name in enumerate(random.sample(GENERIC_RIDES["tier2"], min(6, len(GENERIC_RIDES["tier2"])))):
            rides.append((f"{name} {i+1}", 2))
        for i, name in enumerate(random.sample(GENERIC_RIDES["tier3"], min(7, len(GENERIC_RIDES["tier3"])))):
            rides.append((f"{name} {i+1}", 3))
        return rides

    def _assign_edge_cases(self):
        """Assign edge case types to specific parks and rides."""
        park_names = list(self.park_ids.keys())

        # Perfect performers (99%+ uptime)
        self.edge_case_parks["Tokyo Disneyland"] = "perfect"
        self.edge_case_parks["Tokyo DisneySea"] = "perfect"

        # Problem park (50-70% uptime)
        self.edge_case_parks["Six Flags St. Louis"] = "problem"

        # Improving parks (+5 to +15% uptime increase)
        self.edge_case_parks["Cedar Point"] = "improving"
        self.edge_case_parks["Magic Kingdom"] = "improving"
        self.edge_case_parks["Knott's Berry Farm"] = "improving"
        self.edge_case_parks["Disneyland"] = "improving"
        self.edge_case_parks["Universal Studios Florida"] = "improving"
        self.edge_case_parks["Islands of Adventure"] = "improving"
        self.edge_case_parks["SeaWorld Orlando"] = "improving"
        self.edge_case_parks["Busch Gardens Tampa Bay"] = "improving"

        # Declining parks (-5 to -15% uptime decrease)
        self.edge_case_parks["Six Flags America"] = "declining"
        self.edge_case_parks["Michigan's Adventure"] = "declining"
        self.edge_case_parks["Six Flags Over Georgia"] = "declining"
        self.edge_case_parks["Valleyfair"] = "declining"
        self.edge_case_parks["Dorney Park"] = "declining"
        self.edge_case_parks["Adventureland"] = "declining"
        self.edge_case_parks["Worlds of Fun"] = "declining"

        # Assign some individual ride edge cases
        ride_items = list(self.ride_ids.items())
        random.shuffle(ride_items)

        # 3 disaster rides (<30% uptime)
        for i in range(min(3, len(ride_items))):
            self.edge_case_rides[ride_items[i][1]] = "disaster"

        # 5 high wait rides (90-180 min)
        for i in range(3, min(8, len(ride_items))):
            self.edge_case_rides[ride_items[i][1]] = "high_wait"

        # 10 currently down rides
        for i in range(8, min(18, len(ride_items))):
            self.edge_case_rides[ride_items[i][1]] = "currently_down"

    def _seed_ride_snapshots(self, conn):
        """Insert current ride status snapshots."""
        print("\nSeeding ride snapshots...")
        now = datetime.utcnow()

        for (park_id, ride_name), ride_id in self.ride_ids.items():
            edge_case = self.edge_case_rides.get(ride_id)

            if edge_case == "currently_down":
                is_open = False
                wait_time = 0
            elif edge_case == "high_wait":
                is_open = True
                wait_time = random.randint(90, 180)
            else:
                is_open = random.random() > 0.05  # 95% open
                wait_time = random.randint(5, 60) if is_open else 0

            conn.execute(text("""
                INSERT INTO ride_status_snapshots (ride_id, recorded_at, is_open, wait_time, computed_is_open)
                VALUES (:ride_id, :recorded_at, :is_open, :wait_time, :computed_is_open)
            """), {
                "ride_id": ride_id,
                "recorded_at": now,
                "is_open": is_open,
                "wait_time": wait_time,
                "computed_is_open": is_open and wait_time >= 0,
            })
            self.stats['snapshots_inserted'] += 1

        print(f"  Inserted {self.stats['snapshots_inserted']} snapshots")

    def _get_stats_for_ride(self, ride_id: int, park_name: str, day_offset: int = 0) -> Dict:
        """Generate statistics for a ride based on edge cases."""
        park_edge = self.edge_case_parks.get(park_name)
        ride_edge = self.edge_case_rides.get(ride_id)

        # Base values
        operating_minutes = 720  # 12 hours
        base_uptime = 92.0

        # Adjust for park edge cases
        if park_edge == "perfect":
            base_uptime = random.uniform(98.5, 99.9)
        elif park_edge == "problem":
            base_uptime = random.uniform(50.0, 70.0)
        elif park_edge == "improving":
            # Uptime improves over time: higher now (day_offset=0), lower in the past
            # Factor of 1.2 gives ~8.4% change per week (7 days)
            base_uptime = 95.0 - (day_offset * 1.2)  # Gets worse as we go back in time
            base_uptime = max(base_uptime, 65.0)
        elif park_edge == "declining":
            # Uptime worsens over time: lower now (day_offset=0), higher in the past
            # Factor of 1.2 gives ~8.4% change per week (7 days)
            base_uptime = 75.0 + (day_offset * 1.2)  # Gets better as we go back in time
            base_uptime = min(base_uptime, 95.0)

        # Adjust for ride edge cases
        if ride_edge == "disaster":
            base_uptime = random.uniform(20.0, 35.0)
        elif ride_edge == "high_wait":
            base_uptime = random.uniform(90.0, 98.0)  # Popular rides are usually up

        # Add some randomness
        uptime_pct = max(0, min(100, base_uptime + random.uniform(-3, 3)))
        uptime_minutes = int(operating_minutes * uptime_pct / 100)
        downtime_minutes = operating_minutes - uptime_minutes

        # Wait times
        if ride_edge == "high_wait":
            avg_wait = random.uniform(60, 90)
            peak_wait = random.randint(120, 180)
        elif ride_edge == "disaster" or park_edge == "problem":
            avg_wait = random.uniform(10, 25)
            peak_wait = random.randint(30, 50)
        else:
            avg_wait = random.uniform(15, 45)
            peak_wait = random.randint(45, 90)

        return {
            "uptime_minutes": uptime_minutes,
            "downtime_minutes": downtime_minutes,
            "uptime_percentage": round(uptime_pct, 2),
            "operating_hours_minutes": operating_minutes,
            "avg_wait_time": round(avg_wait, 2),
            "peak_wait_time": peak_wait,
            "status_changes": random.randint(0, 8),
        }

    def _seed_daily_stats(self, conn):
        """Insert daily statistics for the past 35 days."""
        print("\nSeeding daily stats...")
        today = date.today()

        # Create a mapping of park_id -> park_name
        park_id_to_name = {v: k for k, v in self.park_ids.items()}

        for day_offset in range(35):
            stat_date = today - timedelta(days=day_offset)

            # Ride daily stats
            for (park_id, ride_name), ride_id in self.ride_ids.items():
                park_name = park_id_to_name.get(park_id, "")
                stats = self._get_stats_for_ride(ride_id, park_name, day_offset)

                conn.execute(text("""
                    INSERT INTO ride_daily_stats
                    (ride_id, stat_date, uptime_minutes, downtime_minutes, uptime_percentage,
                     operating_hours_minutes, avg_wait_time, peak_wait_time, status_changes)
                    VALUES (:ride_id, :stat_date, :uptime_minutes, :downtime_minutes, :uptime_percentage,
                            :operating_hours_minutes, :avg_wait_time, :peak_wait_time, :status_changes)
                """), {
                    "ride_id": ride_id,
                    "stat_date": stat_date,
                    **stats
                })
                self.stats['daily_stats_inserted'] += 1

            # Park daily stats (aggregate from rides)
            for park_name, park_id in self.park_ids.items():
                park_rides = [(pid, rn, rid) for (pid, rn), rid in self.ride_ids.items() if pid == park_id]
                if not park_rides:
                    continue

                # Generate aggregate stats
                total_downtime_hours = 0
                uptimes = []
                avg_waits = []
                peak_waits = []

                for _, _, ride_id in park_rides:
                    stats = self._get_stats_for_ride(ride_id, park_name, day_offset)
                    total_downtime_hours += stats["downtime_minutes"] / 60
                    uptimes.append(stats["uptime_percentage"])
                    avg_waits.append(stats["avg_wait_time"])
                    peak_waits.append(stats["peak_wait_time"])

                avg_uptime = sum(uptimes) / len(uptimes) if uptimes else 0
                rides_with_downtime = sum(1 for u in uptimes if u < 100)

                conn.execute(text("""
                    INSERT INTO park_daily_stats
                    (park_id, stat_date, total_rides_tracked, avg_uptime_percentage, total_downtime_hours,
                     rides_with_downtime, avg_wait_time, peak_wait_time, operating_hours_minutes)
                    VALUES (:park_id, :stat_date, :total_rides, :avg_uptime, :downtime_hours,
                            :rides_with_downtime, :avg_wait, :peak_wait, :op_minutes)
                """), {
                    "park_id": park_id,
                    "stat_date": stat_date,
                    "total_rides": len(park_rides),
                    "avg_uptime": round(avg_uptime, 2),
                    "downtime_hours": round(total_downtime_hours, 2),
                    "rides_with_downtime": rides_with_downtime,
                    "avg_wait": round(sum(avg_waits) / len(avg_waits), 2) if avg_waits else 0,
                    "peak_wait": max(peak_waits) if peak_waits else 0,
                    "op_minutes": 720,
                })

        print(f"  Inserted {self.stats['daily_stats_inserted']} ride daily stats")

    def _seed_weekly_stats(self, conn):
        """Insert weekly statistics for the past 8 weeks."""
        print("\nSeeding weekly stats...")
        today = date.today()

        # Create a mapping of park_id -> park_name
        park_id_to_name = {v: k for k, v in self.park_ids.items()}

        for week_offset in range(8):
            # Calculate ISO week info
            week_date = today - timedelta(weeks=week_offset)
            year, week_num, _ = week_date.isocalendar()
            week_start = week_date - timedelta(days=week_date.weekday())

            # Ride weekly stats
            for (park_id, ride_name), ride_id in self.ride_ids.items():
                park_name = park_id_to_name.get(park_id, "")
                stats = self._get_stats_for_ride(ride_id, park_name, week_offset * 7)

                # Scale up for weekly
                uptime_weekly = stats["uptime_minutes"] * 7
                downtime_weekly = stats["downtime_minutes"] * 7
                op_minutes_weekly = stats["operating_hours_minutes"] * 7

                # Calculate trend vs previous week
                trend = None
                if week_offset > 0:
                    prev_stats = self._get_stats_for_ride(ride_id, park_name, (week_offset + 1) * 7)
                    if prev_stats["uptime_percentage"] > 0:
                        trend = round(stats["uptime_percentage"] - prev_stats["uptime_percentage"], 2)

                conn.execute(text("""
                    INSERT INTO ride_weekly_stats
                    (ride_id, year, week_number, week_start_date, uptime_minutes, downtime_minutes,
                     uptime_percentage, operating_hours_minutes, avg_wait_time, peak_wait_time,
                     status_changes, trend_vs_previous_week)
                    VALUES (:ride_id, :year, :week_num, :week_start, :uptime, :downtime,
                            :uptime_pct, :op_minutes, :avg_wait, :peak_wait, :status_changes, :trend)
                """), {
                    "ride_id": ride_id,
                    "year": year,
                    "week_num": week_num,
                    "week_start": week_start,
                    "uptime": uptime_weekly,
                    "downtime": downtime_weekly,
                    "uptime_pct": stats["uptime_percentage"],
                    "op_minutes": op_minutes_weekly,
                    "avg_wait": stats["avg_wait_time"],
                    "peak_wait": stats["peak_wait_time"],
                    "status_changes": stats["status_changes"] * 7,
                    "trend": trend,
                })
                self.stats['weekly_stats_inserted'] += 1

            # Park weekly stats
            for park_name, park_id in self.park_ids.items():
                park_rides = [(pid, rn, rid) for (pid, rn), rid in self.ride_ids.items() if pid == park_id]
                if not park_rides:
                    continue

                total_downtime_hours = 0
                uptimes = []

                for _, _, ride_id in park_rides:
                    stats = self._get_stats_for_ride(ride_id, park_name, week_offset * 7)
                    total_downtime_hours += (stats["downtime_minutes"] * 7) / 60
                    uptimes.append(stats["uptime_percentage"])

                avg_uptime = sum(uptimes) / len(uptimes) if uptimes else 0
                rides_with_downtime = sum(1 for u in uptimes if u < 100)

                # Calculate trend
                trend = None
                if week_offset > 0:
                    prev_uptimes = []
                    for _, _, ride_id in park_rides:
                        prev_stats = self._get_stats_for_ride(ride_id, park_name, (week_offset + 1) * 7)
                        prev_uptimes.append(prev_stats["uptime_percentage"])
                    if prev_uptimes:
                        prev_avg = sum(prev_uptimes) / len(prev_uptimes)
                        trend = round(avg_uptime - prev_avg, 2)

                conn.execute(text("""
                    INSERT INTO park_weekly_stats
                    (park_id, year, week_number, week_start_date, total_rides_tracked,
                     avg_uptime_percentage, total_downtime_hours, rides_with_downtime, trend_vs_previous_week)
                    VALUES (:park_id, :year, :week_num, :week_start, :total_rides,
                            :avg_uptime, :downtime_hours, :rides_with_downtime, :trend)
                """), {
                    "park_id": park_id,
                    "year": year,
                    "week_num": week_num,
                    "week_start": week_start,
                    "total_rides": len(park_rides),
                    "avg_uptime": round(avg_uptime, 2),
                    "downtime_hours": round(total_downtime_hours, 2),
                    "rides_with_downtime": rides_with_downtime,
                    "trend": trend,
                })

        print(f"  Inserted {self.stats['weekly_stats_inserted']} ride weekly stats")

    def _seed_monthly_stats(self, conn):
        """Insert monthly statistics for the past 3 months."""
        print("\nSeeding monthly stats...")
        today = date.today()

        # Create a mapping of park_id -> park_name
        park_id_to_name = {v: k for k, v in self.park_ids.items()}

        for month_offset in range(3):
            # Calculate month info
            month_date = today - timedelta(days=month_offset * 30)
            year = month_date.year
            month = month_date.month

            # Ride monthly stats
            for (park_id, ride_name), ride_id in self.ride_ids.items():
                park_name = park_id_to_name.get(park_id, "")
                stats = self._get_stats_for_ride(ride_id, park_name, month_offset * 30)

                # Scale up for monthly (30 days)
                uptime_monthly = stats["uptime_minutes"] * 30
                downtime_monthly = stats["downtime_minutes"] * 30
                op_minutes_monthly = stats["operating_hours_minutes"] * 30

                # Calculate trend vs previous month
                trend = None
                if month_offset > 0:
                    prev_stats = self._get_stats_for_ride(ride_id, park_name, (month_offset + 1) * 30)
                    if prev_stats["uptime_percentage"] > 0:
                        trend = round(stats["uptime_percentage"] - prev_stats["uptime_percentage"], 2)

                conn.execute(text("""
                    INSERT INTO ride_monthly_stats
                    (ride_id, year, month, uptime_minutes, downtime_minutes, uptime_percentage,
                     operating_hours_minutes, avg_wait_time, peak_wait_time, status_changes,
                     trend_vs_previous_month)
                    VALUES (:ride_id, :year, :month, :uptime, :downtime, :uptime_pct,
                            :op_minutes, :avg_wait, :peak_wait, :status_changes, :trend)
                """), {
                    "ride_id": ride_id,
                    "year": year,
                    "month": month,
                    "uptime": uptime_monthly,
                    "downtime": downtime_monthly,
                    "uptime_pct": stats["uptime_percentage"],
                    "op_minutes": op_minutes_monthly,
                    "avg_wait": stats["avg_wait_time"],
                    "peak_wait": stats["peak_wait_time"],
                    "status_changes": stats["status_changes"] * 30,
                    "trend": trend,
                })
                self.stats['monthly_stats_inserted'] += 1

            # Park monthly stats
            for park_name, park_id in self.park_ids.items():
                park_rides = [(pid, rn, rid) for (pid, rn), rid in self.ride_ids.items() if pid == park_id]
                if not park_rides:
                    continue

                total_downtime_hours = 0
                uptimes = []

                for _, _, ride_id in park_rides:
                    stats = self._get_stats_for_ride(ride_id, park_name, month_offset * 30)
                    total_downtime_hours += (stats["downtime_minutes"] * 30) / 60
                    uptimes.append(stats["uptime_percentage"])

                avg_uptime = sum(uptimes) / len(uptimes) if uptimes else 0
                rides_with_downtime = sum(1 for u in uptimes if u < 100)

                # Calculate trend
                trend = None
                if month_offset > 0:
                    prev_uptimes = []
                    for _, _, ride_id in park_rides:
                        prev_stats = self._get_stats_for_ride(ride_id, park_name, (month_offset + 1) * 30)
                        prev_uptimes.append(prev_stats["uptime_percentage"])
                    if prev_uptimes:
                        prev_avg = sum(prev_uptimes) / len(prev_uptimes)
                        trend = round(avg_uptime - prev_avg, 2)

                conn.execute(text("""
                    INSERT INTO park_monthly_stats
                    (park_id, year, month, total_rides_tracked, avg_uptime_percentage,
                     total_downtime_hours, rides_with_downtime, trend_vs_previous_month)
                    VALUES (:park_id, :year, :month, :total_rides, :avg_uptime,
                            :downtime_hours, :rides_with_downtime, :trend)
                """), {
                    "park_id": park_id,
                    "year": year,
                    "month": month,
                    "total_rides": len(park_rides),
                    "avg_uptime": round(avg_uptime, 2),
                    "downtime_hours": round(total_downtime_hours, 2),
                    "rides_with_downtime": rides_with_downtime,
                    "trend": trend,
                })

        print(f"  Inserted {self.stats['monthly_stats_inserted']} ride monthly stats")

    def _print_summary(self):
        """Print seeding summary."""
        print("\n" + "=" * 60)
        print("SEEDING SUMMARY")
        print("=" * 60)
        print(f"Parks inserted:         {self.stats['parks_inserted']}")
        print(f"Rides inserted:         {self.stats['rides_inserted']}")
        print(f"Snapshots inserted:     {self.stats['snapshots_inserted']}")
        print(f"Daily stats inserted:   {self.stats['daily_stats_inserted']}")
        print(f"Weekly stats inserted:  {self.stats['weekly_stats_inserted']}")
        print(f"Monthly stats inserted: {self.stats['monthly_stats_inserted']}")
        print("\nEdge Cases Applied:")
        print(f"  Perfect performer parks: {sum(1 for v in self.edge_case_parks.values() if v == 'perfect')}")
        print(f"  Problem parks:           {sum(1 for v in self.edge_case_parks.values() if v == 'problem')}")
        print(f"  Improving parks:         {sum(1 for v in self.edge_case_parks.values() if v == 'improving')}")
        print(f"  Declining parks:         {sum(1 for v in self.edge_case_parks.values() if v == 'declining')}")
        print(f"  Disaster rides:          {sum(1 for v in self.edge_case_rides.values() if v == 'disaster')}")
        print(f"  High wait rides:         {sum(1 for v in self.edge_case_rides.values() if v == 'high_wait')}")
        print(f"  Currently down rides:    {sum(1 for v in self.edge_case_rides.values() if v == 'currently_down')}")


def main():
    """Main entry point."""
    seeder = TestDataSeeder()
    seeder.run()


if __name__ == '__main__':
    main()
