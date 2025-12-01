# Theme Park Hall of Shame

A data-driven web application that tracks and ranks theme park ride reliability across North America. See which parks have the most downtime, which rides are the least reliable, and how performance trends over time.

**Live Site**: [themeparkhallofshame.com](http://themeparkhallofshame.com)

## What It Does

- **Collects** ride status data from 80+ North American theme parks every 5 minutes
- **Tracks** when rides go down and calculates downtime statistics
- **Ranks** parks and rides by reliability with daily, weekly, and monthly aggregates
- **Displays** interactive dashboards showing the "Hall of Shame" - parks and rides with the worst downtime

## Screenshots

*Coming soon*

## Tech Stack

| Component | Technology |
|-----------|------------|
| **Backend** | Python 3.11+, Flask |
| **Database** | MySQL/MariaDB |
| **Frontend** | HTML, CSS, JavaScript (no framework) |
| **Data Source** | [ThemeParks.wiki](https://themeparks.wiki) API |
| **Hosting** | Apache + Gunicorn on Linux |

## Project Structure

```
ThemeParkHallOfShame/
├── backend/
│   ├── src/
│   │   ├── api/           # Flask REST API
│   │   ├── database/      # SQLAlchemy models & repositories
│   │   ├── collector/     # ThemeParks.wiki API client
│   │   ├── processor/     # Status detection & aggregation
│   │   └── scripts/       # Cron job entry points
│   └── tests/             # pytest unit & integration tests
├── frontend/
│   ├── index.html         # Main dashboard
│   ├── css/               # Stylesheets
│   └── js/                # API client & UI logic
├── docs/                  # Architecture & deployment docs
└── deployment/            # Server configuration files
```

## Quick Start

### Prerequisites

- Python 3.11+
- MySQL or MariaDB
- Node.js (optional, for frontend dev server)

### Backend Setup

```bash
# Clone the repository
git clone https://github.com/yourusername/ThemeParkHallOfShame.git
cd ThemeParkHallOfShame

# Create virtual environment
cd backend
python3 -m venv venv
source venv/bin/activate  # On Windows: venv\Scripts\activate

# Install dependencies
pip install -r requirements.txt

# Copy and configure environment
cp .env.example .env
# Edit .env with your database credentials

# Initialize database (run migrations)
mysql -u root -p < src/database/migrations/001_initial_schema.sql
# Continue with 002, 003, etc.

# Run the API server
python -m flask --app src.api.app run --port 5001
```

### Frontend Setup

```bash
cd frontend

# Serve locally (Python)
python3 -m http.server 8000

# Or with Node
npx serve .
```

Visit `http://localhost:8000` to see the dashboard.

### Running Tests

```bash
cd backend
./run-all-tests.sh

# Or manually:
pytest tests/unit/ -v
pytest tests/integration/ -v
```

## Data Collection

The system collects data via cron jobs:

| Job | Schedule | Description |
|-----|----------|-------------|
| `collect_snapshots` | Every 5 min | Captures current ride status from ThemeParks.wiki |
| `aggregate_daily` | 1 AM UTC | Calculates daily statistics |
| `collect_parks` | Sunday 2 AM | Refreshes park/ride metadata |

Raw snapshots are retained for 24 hours, then aggregated into permanent daily/weekly/monthly statistics.

## API Endpoints

| Endpoint | Description |
|----------|-------------|
| `GET /api/health` | Health check |
| `GET /api/parks/downtime` | Park rankings by downtime |
| `GET /api/rides/downtime` | Ride rankings by downtime |
| `GET /api/parks/{id}` | Individual park details |
| `GET /api/live/status-summary` | Current system status |

See [API documentation](docs/architecture.md) for full details.

## Documentation

- [Architecture & Technology Decisions](docs/architecture.md)
- [Database Schema](docs/database_schema.md)
- [Deployment Guide](docs/deployment.md)
- [Future Ideas & AI Integration](docs/future-ideas.md)

## How Downtime is Calculated

1. **Data Collection**: Ride status is captured every 5 minutes from ThemeParks.wiki
2. **Status Logic**: A ride is considered "open" if `wait_time > 0` OR `is_open = true`
3. **Operating Hours**: Only time when the park is actually open counts toward uptime calculations
4. **Aggregation**: Daily stats are computed from raw snapshots, then rolled up to weekly/monthly

### Weighted Scoring

Rides are classified into tiers to fairly compare parks with different attraction portfolios:

| Tier | Weight | Examples |
|------|--------|----------|
| Tier 1 | 3x | Major coasters, E-tickets |
| Tier 2 | 2x | Standard attractions |
| Tier 3 | 1x | Kiddie rides, shows |

```
Park Score = Σ(downtime_hours × tier_weight) / Σ(all_ride_weights)
```

## Contributing

Contributions are welcome! Please:

1. Fork the repository
2. Create a feature branch
3. Write tests for new functionality
4. Submit a pull request

## Data Attribution

This project uses the open-source [ThemeParks.wiki](https://themeparks.wiki) API.

> **Powered by [ThemeParks.wiki](https://themeparks.wiki)** — Open-source API providing live wait times & ride status for 50+ parks worldwide.

The ThemeParks.wiki project is available on GitHub: [github.com/ThemeParks/parksapi](https://github.com/ThemeParks/parksapi)

## License

*License to be determined*

## Disclaimer

This project tracks publicly available ride status data for informational purposes. "Hall of Shame" rankings reflect operational data and are not intended as criticism of park maintenance professionals, who work hard to keep attractions running safely.

Theme park operations involve complex factors including weather, maintenance schedules, and safety protocols that may not be reflected in simple uptime metrics.
