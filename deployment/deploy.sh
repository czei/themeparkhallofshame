#!/bin/bash
# Theme Park Hall of Shame - Main Deployment Script
# Purpose: Deploy application to production server from local machine
# Usage: ./deploy.sh [all|backend|frontend|migrations|restart|health]

set -euo pipefail

# Configuration
REMOTE_HOST="ec2-user@webperformance.com"
SSH_KEY="${SSH_KEY:-$HOME/.ssh/michael-2.pem}"
REMOTE_APP_DIR="/opt/themeparkhallofshame"
# Frontend is now served directly from git repo (no separate web dir needed)

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "${SCRIPT_DIR}/.." && pwd)"

# Colors
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m'

log() { echo -e "${GREEN}[DEPLOY]${NC} $1"; }
warn() { echo -e "${YELLOW}[WARN]${NC} $1"; }
error() { echo -e "${RED}[ERROR]${NC} $1"; exit 1; }
info() { echo -e "${BLUE}[INFO]${NC} $1"; }

DEPLOY_TARGET="${1:-all}"

# Validate SSH key exists
check_ssh_key() {
    if [ ! -f "$SSH_KEY" ]; then
        error "SSH key not found at $SSH_KEY. Set SSH_KEY env var or use default ~/.ssh/michael-2.pem"
    fi
}

# Test SSH connection
test_connection() {
    log "Testing SSH connection to $REMOTE_HOST..."
    if ! ssh -i "$SSH_KEY" -o ConnectTimeout=10 "$REMOTE_HOST" "echo 'Connection OK'" &>/dev/null; then
        error "Cannot connect to $REMOTE_HOST"
    fi
    log "SSH connection successful"
}

# Execute command on remote server
remote_exec() {
    ssh -i "$SSH_KEY" "$REMOTE_HOST" "$@"
}

# Deploy backend code
deploy_backend() {
    log "Deploying backend code..."

    # Sync backend source code (exclude dev/test files)
    rsync -avz --delete \
        -e "ssh -i $SSH_KEY" \
        --exclude '__pycache__' \
        --exclude '*.pyc' \
        --exclude '.env' \
        --exclude 'tests/' \
        --exclude '.pytest_cache' \
        --exclude '.mypy_cache' \
        "${PROJECT_ROOT}/backend/src/" \
        "${REMOTE_HOST}:${REMOTE_APP_DIR}/backend/src/"

    # Sync requirements.txt
    rsync -avz \
        -e "ssh -i $SSH_KEY" \
        "${PROJECT_ROOT}/backend/requirements.txt" \
        "${REMOTE_HOST}:${REMOTE_APP_DIR}/backend/"

    # Sync wsgi.py
    rsync -avz \
        -e "ssh -i $SSH_KEY" \
        "${PROJECT_ROOT}/backend/wsgi.py" \
        "${REMOTE_HOST}:${REMOTE_APP_DIR}/backend/"

    # Install/update Python dependencies
    log "Installing Python dependencies..."
    remote_exec "cd ${REMOTE_APP_DIR}/backend && ${REMOTE_APP_DIR}/venv/bin/pip install -q -r requirements.txt"

    log "Backend deployed successfully"
}

# Deploy frontend static files (served directly from git repo)
deploy_frontend() {
    log "Deploying frontend..."

    rsync -avz --delete \
        -e "ssh -i $SSH_KEY" \
        --exclude '*.md' \
        --exclude 'netlify.toml' \
        --exclude 'vercel.json' \
        --exclude 'DEPLOYMENT.md' \
        "${PROJECT_ROOT}/frontend/" \
        "${REMOTE_HOST}:${REMOTE_APP_DIR}/frontend/"

    log "Frontend deployed successfully"
}

# Run database migrations
deploy_migrations() {
    log "Running database migrations..."

    # Sync migrations
    rsync -avz \
        -e "ssh -i $SSH_KEY" \
        "${PROJECT_ROOT}/backend/src/database/migrations/" \
        "${REMOTE_HOST}:${REMOTE_APP_DIR}/backend/src/database/migrations/"

    # Run migration script (source .env for DB credentials)
    remote_exec "cd ${REMOTE_APP_DIR}/backend && source .env && cd ${REMOTE_APP_DIR}/deployment && ./scripts/setup-database.sh production"

    log "Migrations completed successfully"
}

# Restart services
restart_services() {
    log "Restarting services..."
    remote_exec "sudo systemctl restart themepark-api && sudo systemctl reload httpd"
    log "Services restarted"
}

# Health check
health_check() {
    log "Running health checks..."
    remote_exec "cd ${REMOTE_APP_DIR}/deployment && ./scripts/health-check.sh"
}

# Show usage
usage() {
    echo "Theme Park Hall of Shame - Deployment Script"
    echo ""
    echo "Usage: $0 [command]"
    echo ""
    echo "Commands:"
    echo "  all         Full deployment (backend + frontend + migrations + restart)"
    echo "  backend     Deploy backend code only"
    echo "  frontend    Deploy frontend static files only"
    echo "  migrations  Run database migrations only"
    echo "  restart     Restart services only"
    echo "  health      Run health checks"
    echo ""
    echo "Environment Variables:"
    echo "  SSH_KEY     Path to SSH private key (default: ~/.ssh/michael-2.pem)"
    echo ""
}

# Main
main() {
    echo -e "${GREEN}======================================${NC}"
    echo -e "${GREEN}Theme Park Hall of Shame - Deploy${NC}"
    echo -e "${GREEN}Target: ${DEPLOY_TARGET}${NC}"
    echo -e "${GREEN}======================================${NC}"
    echo ""

    check_ssh_key
    test_connection

    case "$DEPLOY_TARGET" in
        all)
            deploy_backend
            deploy_frontend
            deploy_migrations
            restart_services
            health_check
            ;;
        backend)
            deploy_backend
            restart_services
            ;;
        frontend)
            deploy_frontend
            ;;
        migrations)
            deploy_migrations
            restart_services
            ;;
        restart)
            restart_services
            ;;
        health)
            health_check
            ;;
        help|--help|-h)
            usage
            exit 0
            ;;
        *)
            error "Unknown command: $DEPLOY_TARGET. Use '$0 help' for usage."
            ;;
    esac

    echo ""
    log "Deployment complete!"
}

main
