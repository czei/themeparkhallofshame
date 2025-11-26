#!/bin/bash
# Theme Park Hall of Shame - Service Setup Script
# Purpose: Install systemd service, Apache config, and cron jobs
# Usage: ./setup-services.sh
# This script is idempotent - safe to run multiple times

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
CONFIG_DIR="${SCRIPT_DIR}/../config"
APP_DIR="/opt/themeparkhallofshame"

# Colors
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m'

log() { echo -e "${GREEN}[SERVICES]${NC} $1"; }
warn() { echo -e "${YELLOW}[WARN]${NC} $1"; }
error() { echo -e "${RED}[ERROR]${NC} $1"; exit 1; }

echo -e "${GREEN}======================================${NC}"
echo -e "${GREEN}Theme Park Hall of Shame - Services Setup${NC}"
echo -e "${GREEN}======================================${NC}"
echo ""

# Install systemd service
install_systemd_service() {
    local SERVICE_FILE="/etc/systemd/system/themepark-api.service"
    local SOURCE_FILE="${CONFIG_DIR}/themepark-api.service"

    if [ ! -f "$SOURCE_FILE" ]; then
        error "Service file not found: $SOURCE_FILE"
    fi

    log "Installing systemd service..."

    # Check if service already exists and is identical
    if [ -f "$SERVICE_FILE" ]; then
        if diff -q "$SOURCE_FILE" "$SERVICE_FILE" &>/dev/null; then
            log "Systemd service already installed (no changes)"
        else
            log "Updating systemd service..."
            sudo cp "$SOURCE_FILE" "$SERVICE_FILE"
            sudo systemctl daemon-reload
        fi
    else
        sudo cp "$SOURCE_FILE" "$SERVICE_FILE"
        sudo systemctl daemon-reload
    fi

    log "Enabling themepark-api service..."
    sudo systemctl enable themepark-api

    log "Systemd service installed"
}

# Setup Basic Auth htpasswd file for soft launch
setup_basic_auth() {
    local HTPASSWD_FILE="/etc/httpd/.htpasswd-themepark"

    log "Setting up Basic Authentication for soft launch..."

    if [ -f "$HTPASSWD_FILE" ]; then
        log "htpasswd file already exists at $HTPASSWD_FILE"
        log "To add users: sudo htpasswd $HTPASSWD_FILE <username>"
        return 0
    fi

    # Prompt for username
    echo ""
    read -p "Enter username for Basic Auth (soft launch access): " AUTH_USER
    if [ -z "$AUTH_USER" ]; then
        warn "No username provided, skipping htpasswd creation"
        warn "You'll need to create it manually before Apache starts:"
        warn "  sudo htpasswd -c $HTPASSWD_FILE <username>"
        return 0
    fi

    # Create htpasswd file
    log "Creating htpasswd file..."
    sudo htpasswd -c "$HTPASSWD_FILE" "$AUTH_USER"

    if [ -f "$HTPASSWD_FILE" ]; then
        log "htpasswd file created at $HTPASSWD_FILE"
    else
        warn "Failed to create htpasswd file"
    fi
}

# Install Apache configuration
install_apache_config() {
    local APACHE_FILE="/etc/httpd/conf.d/themeparkhallofshame.conf"
    local SOURCE_FILE="${CONFIG_DIR}/themeparkhallofshame.conf"

    if [ ! -f "$SOURCE_FILE" ]; then
        error "Apache config not found: $SOURCE_FILE"
    fi

    log "Installing Apache configuration..."

    # Check if config already exists and is identical
    if [ -f "$APACHE_FILE" ]; then
        if diff -q "$SOURCE_FILE" "$APACHE_FILE" &>/dev/null; then
            log "Apache config already installed (no changes)"
        else
            log "Updating Apache config..."
            sudo cp "$SOURCE_FILE" "$APACHE_FILE"
        fi
    else
        sudo cp "$SOURCE_FILE" "$APACHE_FILE"
    fi

    # Setup Basic Auth (required for Apache to start with auth enabled)
    setup_basic_auth

    # Test Apache configuration
    log "Testing Apache configuration..."
    if ! sudo apachectl configtest 2>&1 | grep -q "Syntax OK"; then
        error "Apache configuration test failed. Check $APACHE_FILE"
    fi

    log "Apache configuration installed"
}

# Install cron jobs
install_cron_jobs() {
    local CRON_FILE="${CONFIG_DIR}/crontab.prod"

    if [ ! -f "$CRON_FILE" ]; then
        error "Crontab file not found: $CRON_FILE"
    fi

    log "Installing cron jobs..."

    # Get current crontab (excluding our jobs)
    local CURRENT_CRON=""
    if crontab -l 2>/dev/null; then
        CURRENT_CRON=$(crontab -l 2>/dev/null | grep -v "themeparkhallofshame" | grep -v "Theme Park Hall of Shame" || true)
    fi

    # Combine existing cron (if any) with our jobs
    {
        echo "$CURRENT_CRON"
        echo ""
        cat "$CRON_FILE"
    } | crontab -

    log "Cron jobs installed"
    log "Current crontab:"
    crontab -l | grep -E "(themeparkhallofshame|collect_snapshots|aggregate_daily)" || true
}

# Start services
start_services() {
    log "Starting themepark-api service..."

    # Check if .env exists before starting
    if [ ! -f "${APP_DIR}/backend/.env" ]; then
        warn "Production .env not found at ${APP_DIR}/backend/.env"
        warn "Service will fail to start until .env is created"
        return 0
    fi

    if sudo systemctl start themepark-api; then
        log "themepark-api service started"
    else
        warn "Failed to start themepark-api service"
        sudo systemctl status themepark-api --no-pager || true
    fi

    log "Reloading Apache..."
    sudo systemctl reload httpd
}

# Verify services
verify_services() {
    echo ""
    log "Verifying services..."

    echo -n "  themepark-api.service: "
    if systemctl is-enabled themepark-api &>/dev/null; then
        echo -e "${GREEN}enabled${NC}"
    else
        echo -e "${YELLOW}not enabled${NC}"
    fi

    echo -n "  Apache config: "
    if [ -f /etc/httpd/conf.d/themeparkhallofshame.conf ]; then
        echo -e "${GREEN}installed${NC}"
    else
        echo -e "${RED}missing${NC}"
    fi

    echo -n "  Cron jobs: "
    if crontab -l 2>/dev/null | grep -q "collect_snapshots"; then
        echo -e "${GREEN}installed${NC}"
    else
        echo -e "${RED}missing${NC}"
    fi
}

# Main
main() {
    install_systemd_service
    install_apache_config
    install_cron_jobs
    start_services
    verify_services

    echo ""
    echo -e "${GREEN}======================================${NC}"
    echo -e "${GREEN}Services setup complete!${NC}"
    echo -e "${GREEN}======================================${NC}"
    echo ""
    echo "Next steps:"
    echo "  1. Create production .env: ${APP_DIR}/backend/.env"
    echo "  2. Start API service: sudo systemctl start themepark-api"
    echo "  3. Check status: sudo systemctl status themepark-api"
}

main
