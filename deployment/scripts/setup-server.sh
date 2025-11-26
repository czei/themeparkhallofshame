#!/bin/bash
# Theme Park Hall of Shame - Server Setup Script
# Purpose: Install system packages on Amazon Linux
# Usage: ./setup-server.sh
# This script is idempotent - safe to run multiple times

set -euo pipefail

# Colors
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m'

log() { echo -e "${GREEN}[SETUP]${NC} $1"; }
warn() { echo -e "${YELLOW}[WARN]${NC} $1"; }
error() { echo -e "${RED}[ERROR]${NC} $1"; exit 1; }

echo -e "${GREEN}======================================${NC}"
echo -e "${GREEN}Theme Park Hall of Shame - Server Setup${NC}"
echo -e "${GREEN}======================================${NC}"
echo ""

# Detect OS
if [ -f /etc/os-release ]; then
    . /etc/os-release
    OS=$ID
    VERSION=$VERSION_ID
    log "Detected OS: $PRETTY_NAME"
else
    error "Cannot detect OS. /etc/os-release not found."
fi

# Check if running as root or with sudo
check_sudo() {
    if [ "$EUID" -ne 0 ]; then
        if ! sudo -n true 2>/dev/null; then
            warn "This script requires sudo access for package installation"
        fi
    fi
}

# Install package if not already installed (Amazon Linux / RHEL / CentOS)
install_pkg_rpm() {
    local pkg=$1
    if rpm -q "$pkg" &>/dev/null; then
        log "Package $pkg already installed"
    else
        log "Installing $pkg..."
        sudo dnf install -y "$pkg" || sudo yum install -y "$pkg"
    fi
}

# Install packages for Amazon Linux 2023 / Amazon Linux 2
install_amazon_linux_packages() {
    log "Updating package manager..."
    sudo dnf update -y 2>/dev/null || sudo yum update -y

    log "Installing Python 3.11..."
    # Amazon Linux 2023 has python3.11 available
    # Amazon Linux 2 may need amazon-linux-extras
    if command -v python3.11 &>/dev/null; then
        log "Python 3.11 already installed"
    else
        if [ "$VERSION" = "2" ]; then
            # Amazon Linux 2
            sudo amazon-linux-extras enable python3.11 2>/dev/null || true
            sudo yum install -y python3.11 python3.11-pip python3.11-devel
        else
            # Amazon Linux 2023+
            sudo dnf install -y python3.11 python3.11-pip python3.11-devel
        fi
    fi

    log "Installing MySQL/MariaDB client..."
    install_pkg_rpm mariadb105 2>/dev/null || install_pkg_rpm mariadb || install_pkg_rpm mysql

    log "Installing MySQL development libraries..."
    install_pkg_rpm mariadb105-devel 2>/dev/null || install_pkg_rpm mariadb-devel || install_pkg_rpm mysql-devel

    log "Installing Apache..."
    install_pkg_rpm httpd

    log "Installing Apache modules..."
    # mod_proxy is usually compiled into httpd on Amazon Linux
    # Just ensure mod_ssl is available
    install_pkg_rpm mod_ssl 2>/dev/null || true

    log "Installing development tools..."
    install_pkg_rpm gcc
    install_pkg_rpm gcc-c++
    install_pkg_rpm make

    log "Installing certbot for SSL..."
    install_pkg_rpm certbot 2>/dev/null || sudo pip3 install certbot certbot-apache
}

# Enable and start services
setup_services() {
    log "Enabling Apache..."
    sudo systemctl enable httpd

    log "Starting Apache..."
    sudo systemctl start httpd || warn "Apache may already be running"

    # Check if MariaDB/MySQL is local (optional)
    if systemctl list-unit-files | grep -q mariadb; then
        log "Enabling MariaDB..."
        sudo systemctl enable mariadb
        sudo systemctl start mariadb || warn "MariaDB may already be running"
    fi
}

# Create application directories
create_directories() {
    log "Creating application directories..."

    sudo mkdir -p /opt/themeparkhallofshame/{backend,logs,scripts,config}
    sudo mkdir -p /var/www/themeparkhallofshame

    # Set ownership to ec2-user
    sudo chown -R ec2-user:ec2-user /opt/themeparkhallofshame
    sudo chown -R ec2-user:apache /var/www/themeparkhallofshame
    sudo chmod -R 775 /var/www/themeparkhallofshame

    log "Directories created"
}

# Verify installations
verify_installation() {
    echo ""
    log "Verifying installations..."

    local errors=0

    echo -n "  Python 3.11: "
    if python3.11 --version &>/dev/null; then
        echo -e "${GREEN}$(python3.11 --version)${NC}"
    else
        echo -e "${RED}NOT FOUND${NC}"
        ((errors++))
    fi

    echo -n "  pip: "
    if python3.11 -m pip --version &>/dev/null; then
        echo -e "${GREEN}OK${NC}"
    else
        echo -e "${RED}NOT FOUND${NC}"
        ((errors++))
    fi

    echo -n "  MySQL client: "
    if mysql --version &>/dev/null; then
        echo -e "${GREEN}$(mysql --version | head -1)${NC}"
    else
        echo -e "${RED}NOT FOUND${NC}"
        ((errors++))
    fi

    echo -n "  Apache: "
    if httpd -v &>/dev/null; then
        echo -e "${GREEN}$(httpd -v | head -1)${NC}"
    else
        echo -e "${RED}NOT FOUND${NC}"
        ((errors++))
    fi

    echo -n "  gcc: "
    if gcc --version &>/dev/null; then
        echo -e "${GREEN}OK${NC}"
    else
        echo -e "${RED}NOT FOUND${NC}"
        ((errors++))
    fi

    if [ $errors -gt 0 ]; then
        error "$errors package(s) failed to install"
    fi

    log "All packages verified"
}

# Main
main() {
    check_sudo

    case "$OS" in
        amzn|rhel|centos|fedora)
            install_amazon_linux_packages
            ;;
        *)
            error "Unsupported OS: $OS. This script supports Amazon Linux, RHEL, CentOS, Fedora."
            ;;
    esac

    setup_services
    create_directories
    verify_installation

    echo ""
    echo -e "${GREEN}======================================${NC}"
    echo -e "${GREEN}Server setup complete!${NC}"
    echo -e "${GREEN}======================================${NC}"
}

main
