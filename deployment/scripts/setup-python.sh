#!/bin/bash
# Theme Park Hall of Shame - Python Environment Setup
# Purpose: Create Python virtual environment and install base packages
# Usage: ./setup-python.sh
# This script is idempotent - safe to run multiple times

set -euo pipefail

APP_DIR="/opt/themeparkhallofshame"
VENV_DIR="${APP_DIR}/venv"
PYTHON_BIN="python3.11"

# Colors
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m'

log() { echo -e "${GREEN}[PYTHON]${NC} $1"; }
warn() { echo -e "${YELLOW}[WARN]${NC} $1"; }
error() { echo -e "${RED}[ERROR]${NC} $1"; exit 1; }

echo -e "${GREEN}======================================${NC}"
echo -e "${GREEN}Theme Park Hall of Shame - Python Setup${NC}"
echo -e "${GREEN}======================================${NC}"
echo ""

# Check Python 3.11 is available
check_python() {
    if ! command -v $PYTHON_BIN &>/dev/null; then
        error "Python 3.11 not found. Run setup-server.sh first."
    fi
    log "Found $($PYTHON_BIN --version)"
}

# Create virtual environment
create_venv() {
    if [ -d "$VENV_DIR" ]; then
        log "Virtual environment already exists at $VENV_DIR"

        # Verify it's working
        if "${VENV_DIR}/bin/python" --version &>/dev/null; then
            log "Virtual environment is functional"
            return 0
        else
            warn "Virtual environment appears broken, recreating..."
            rm -rf "$VENV_DIR"
        fi
    fi

    log "Creating virtual environment at $VENV_DIR..."
    $PYTHON_BIN -m venv "$VENV_DIR"

    log "Virtual environment created"
}

# Upgrade pip and install base packages
setup_base_packages() {
    log "Upgrading pip..."
    "${VENV_DIR}/bin/pip" install --upgrade pip

    log "Installing wheel and setuptools..."
    "${VENV_DIR}/bin/pip" install --upgrade wheel setuptools

    log "Installing gunicorn..."
    "${VENV_DIR}/bin/pip" install gunicorn
}

# Verify installation
verify_venv() {
    echo ""
    log "Verifying virtual environment..."

    echo -n "  Python: "
    "${VENV_DIR}/bin/python" --version

    echo -n "  pip: "
    "${VENV_DIR}/bin/pip" --version | head -1

    echo -n "  gunicorn: "
    if "${VENV_DIR}/bin/gunicorn" --version &>/dev/null; then
        "${VENV_DIR}/bin/gunicorn" --version
    else
        echo -e "${RED}NOT FOUND${NC}"
        error "gunicorn installation failed"
    fi

    log "Virtual environment verified"
}

# Main
main() {
    check_python
    create_venv
    setup_base_packages
    verify_venv

    echo ""
    echo -e "${GREEN}======================================${NC}"
    echo -e "${GREEN}Python environment setup complete!${NC}"
    echo -e "${GREEN}======================================${NC}"
    echo ""
    echo "To activate: source ${VENV_DIR}/bin/activate"
}

main
