# Repository Guidelines

## Project Structure & Module Organization
- `backend/src`: Flask API, data processors, and SQLAlchemy repositories. Key subfolders include `api/`, `processor/`, `database/`, and `utils/`.
- `backend/tests`: Pytest suites split into `integration/`, `unit/`, and `golden_data/`. Integration tests expect a MySQL schema defined in `backend/scripts/setup-test-database.sh`.
- `frontend/`: Static HTML/CSS/JS for the Hall of Shame site. Assets such as `index.html` and JS components live here.
- `specs/` and `docs/`: Product requirements, data-model plans, and architecture references. Consult them before changing schema or API contracts.
- `run-all-tests.sh`: Convenience script for orchestrating backend suites (sets env vars, skips performance tests).

## Build, Test, and Development Commands
- `pip install -r requirements.txt`: Install shared backend dependencies.
- `cd backend && TEST_DB_*=<...> PYTHONPATH=src pytest -q`: Run the full backend test suite (set `PYTEST_ADDOPTS="--cov-fail-under=0"` when targeting individual files).
- `backend/scripts/setup-test-database.sh`: Drops/recreates `themepark_test` using the dev schema dump. Requires MySQL access (`root` pw in script).
- `frontend` work: static, so use `python -m http.server` or similar to preview changes locally.

## Coding Style & Naming Conventions
- Python: PEP8-style 4-space indentation. Favor descriptive snake_case for functions/variables and CamelCase for classes.
- JavaScript: Existing files use 2-space indents and camelCase for functions/variables.
- SQL migrations live under `backend/src/database/migrations/` and follow zero-padded numeric prefixes (e.g., `016_add_shame_score_to_park_daily_stats.sql`). Add SQL comments explaining the purpose.
- Logging uses `utils.logger.logger`; prefer structured, contextual messages (`logger.info("Ride details", extra=...)`).

## Testing Guidelines
- Framework: Pytest with strict async mode (`pytest-asyncio`). Coverage gate defaults to 80% (override via `PYTEST_ADDOPTS="--cov-fail-under=0"` during targeted runs).
- Integration tests rely on MySQL fixtures from `backend/tests/integration/conftest.py`. Always seed data through helper fixtures or scripts; never assume prod rows.
- Test naming: `test_<feature>_<scenario>()` for functions; use `TestSuiteName` classes sparingly (mostly for grouping).

## Commit & Pull Request Guidelines
- Commits in history use clear prefixes (“fix: …”, “feat: …”, “chore: …”). Emulate this style and keep scopes small (touch fewer than ~3 files when possible).
- Pull Requests should describe the user-facing change, list tests executed (command snippets), and mention schema or contract updates explicitly. Link relevant issues/tickets and include screenshots for frontend changes.

## Security & Configuration Tips
- Secrets: Database credentials live in scripts only for local automation; never commit new secrets. Prefer env vars in CI.
- When touching schema or migrations, run `backend/scripts/setup-test-database.sh` locally to ensure the test DB mirrors dev before running integration tests.
