#!/usr/bin/env python3
"""
Database Connectivity Test - Verifies MySQL connection and schema.
"""

import sys
from pathlib import Path

# Add src to path
backend_src = Path(__file__).parent / 'src'
sys.path.insert(0, str(backend_src.absolute()))

print('=' * 60)
print('DATABASE CONNECTIVITY TEST')
print('=' * 60)
print()

# Test database connection
from database.connection import test_database_connection, get_db_connection
from sqlalchemy import text

print('Testing database connection...')
if not test_database_connection():
    print('❌ Database connection failed!')
    sys.exit(1)

print('✓ Database connection successful')
print()

# Check tables
print('Checking database schema...')
with get_db_connection() as conn:
    # Count tables
    result = conn.execute(text("""
        SELECT COUNT(*) as table_count
        FROM information_schema.tables
        WHERE table_schema = 'themepark_tracker_dev'
    """))
    table_count = result.fetchone()[0]
    print(f'✓ Database has {table_count} tables')

    # List all tables
    result = conn.execute(text("""
        SELECT table_name
        FROM information_schema.tables
        WHERE table_schema = 'themepark_tracker_dev'
        ORDER BY table_name
    """))

    tables = [row[0] for row in result]
    print()
    print('Tables created:')
    for table in tables:
        print(f'  - {table}')

    # Check MySQL Events
    print()
    print('Checking cleanup events...')
    result = conn.execute(text("""
        SELECT event_name, status
        FROM information_schema.events
        WHERE event_schema = 'themepark_tracker_dev'
        ORDER BY event_name
    """))

    events = list(result)
    if events:
        print(f'✓ Found {len(events)} MySQL Events:')
        for event in events:
            print(f'  - {event[0]}: {event[1]}')
    else:
        print('⚠ No MySQL Events found (may need EVENT privilege)')

print()
print('=' * 60)
print('DATABASE TEST PASSED ✓')
print('=' * 60)
print()
print('Summary:')
print('  - MySQL connection: ✓')
print(f'  - Tables created: {table_count}')
print(f'  - Events configured: {len(events) if events else 0}')
print()
print('Next steps:')
print('  - Run data collection: python scripts/collect_data.py (not implemented yet)')
print('  - Run aggregation: python scripts/aggregate_daily.py')
print('  - Start Flask API: python src/api/app.py')
print()
