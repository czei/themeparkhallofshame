# Backend

Flask REST API for Theme Park Hall of Shame.

## Quick Start

```bash
# Create virtual environment
python3 -m venv venv
source venv/bin/activate

# Install dependencies
pip install -r requirements.txt

# Configure environment
cp .env.example .env
# Edit .env with your database credentials

# Run the development server
python -m flask --app src.api.app run --port 5001
```

## Project Structure

```
backend/
├── src/
│   ├── api/           # Flask app, routes, middleware
│   ├── database/      # SQLAlchemy connection, repositories, migrations
│   ├── collector/     # ThemeParks.wiki & Queue-Times API clients
│   ├── processor/     # Status change detection, aggregation
│   ├── classifier/    # Ride tier classification
│   └── scripts/       # Cron job entry points
├── tests/
│   ├── unit/          # Fast tests with SQLite
│   └── integration/   # Tests requiring MySQL
├── data/              # Classification data files
└── requirements.txt
```

## Running Tests

```bash
# Run all tests
./run-all-tests.sh

# Unit tests only (fast, uses SQLite)
pytest tests/unit/ -v

# Integration tests (requires MySQL)
pytest tests/integration/ -v
```

See [TESTING.md](TESTING.md) for detailed testing documentation.

## Scripts

| Script | Purpose |
|--------|---------|
| `src.scripts.collect_parks` | Fetch/update park and ride metadata |
| `src.scripts.collect_snapshots` | Collect current ride status |
| `src.scripts.aggregate_daily` | Compute daily statistics |

Run with:
```bash
python -m src.scripts.collect_parks
```

## API Endpoints

| Endpoint | Description |
|----------|-------------|
| `GET /api/health` | Health check |
| `GET /api/parks/downtime` | Park downtime rankings |
| `GET /api/rides/downtime` | Ride downtime rankings |
| `GET /api/parks/{id}` | Park details |
| `GET /api/live/status-summary` | Current status summary |

## Environment Variables

See `.env.example` for all configuration options. Key variables:

- `DB_HOST`, `DB_NAME`, `DB_USER`, `DB_PASSWORD` - Database connection
- `ENVIRONMENT` - `development`, `server`, or `production`
- `QUEUE_TIMES_API_BASE_URL` - Legacy API endpoint (default: https://queue-times.com)
