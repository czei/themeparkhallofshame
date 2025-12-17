#!/bin/bash
# Start backend Flask API and frontend static server for local dev.
# Usage: ./run-dev-servers.sh

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
BACKEND_DIR="$SCRIPT_DIR/backend"
FRONTEND_DIR="$SCRIPT_DIR/frontend"
LOG_DIR="$SCRIPT_DIR/tmp"
mkdir -p "$LOG_DIR"

# Ensure virtualenv is active if present
if [ -f "$BACKEND_DIR/venv/bin/activate" ]; then
  source "$BACKEND_DIR/venv/bin/activate"
fi

export PYTHONPATH="$BACKEND_DIR/src"
export FLASK_APP=src.api.app
# Use production mode to avoid debug reloader permission issues in some environments
export FLASK_ENV=production
export FLASK_DEBUG=0

BACKEND_PORT=${BACKEND_PORT:-5001}
FRONTEND_PORT=${FRONTEND_PORT:-8000}

cleanup() {
  echo "[dev] Stopping..."
  [[ -n "${BACKEND_PID:-}" ]] && kill "${BACKEND_PID}" 2>/dev/null || true
  [[ -n "${FRONTEND_PID:-}" ]] && kill "${FRONTEND_PID}" 2>/dev/null || true
}

trap cleanup INT TERM EXIT

check_port_free() {
  local port=$1
  local label=$2
  if lsof -i ":${port}" >/dev/null 2>&1; then
    echo "[dev][error] Port ${port} is already in use for ${label}."
    echo "             Stop the process using it (e.g., lsof -i :${port}) or set ${label}_PORT to another value."
    exit 1
  fi
}

check_port_free "$BACKEND_PORT" BACKEND
check_port_free "$FRONTEND_PORT" FRONTEND

echo "[dev] Starting backend on :$BACKEND_PORT ..."
# Disable the Flask reloader so the parent process stays alive (needed for wait/kill)
( cd "$BACKEND_DIR" && python -m flask run --port "$BACKEND_PORT" --no-reload --no-debugger ) >"$LOG_DIR/backend.log" 2>&1 &
BACKEND_PID=$!
sleep 1
if ! kill -0 "$BACKEND_PID" 2>/dev/null; then
  echo "[dev][error] Backend failed to start. See $LOG_DIR/backend.log"
  exit 1
fi

echo "[dev] Starting frontend static server on :$FRONTEND_PORT ..."
( cd "$FRONTEND_DIR" && python -m http.server "$FRONTEND_PORT" ) >"$LOG_DIR/frontend.log" 2>&1 &
FRONTEND_PID=$!
sleep 1
if ! kill -0 "$FRONTEND_PID" 2>/dev/null; then
  echo "[dev][error] Frontend failed to start. See $LOG_DIR/frontend.log"
  kill "$BACKEND_PID" 2>/dev/null || true
  exit 1
fi

cat <<EOF

Dev servers started:
  Backend : http://localhost:$BACKEND_PORT  (pid $BACKEND_PID)
  Frontend: http://localhost:$FRONTEND_PORT  (pid $FRONTEND_PID)
Logs:
  $LOG_DIR/backend.log
  $LOG_DIR/frontend.log

Press Ctrl+C to stop both.
EOF

wait $BACKEND_PID $FRONTEND_PID
