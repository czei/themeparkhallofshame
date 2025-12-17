#!/bin/bash
# =============================================================================
# Deployment Snapshot Manager
# =============================================================================
#
# Purpose: Create/restore deployment snapshots for rollback capability
#
# Commands:
#   create <name>   - Save current deployment state
#   restore <name>  - Rollback to previous deployment state
#   list            - Show available snapshots with metadata
#   prune           - Remove old snapshots (keep last 10)
#
# Usage:
#   ./snapshot-manager.sh create deploy-20250101-120000-abc1234
#   ./snapshot-manager.sh restore deploy-20250101-120000-abc1234
#   ./snapshot-manager.sh list
#   ./snapshot-manager.sh prune
#
# Notes:
#   - Snapshots stored in /opt/themeparkhallofshame/snapshots/
#   - Each snapshot includes backend/, frontend/, and metadata
#   - Automatically prunes old snapshots after create
#
# =============================================================================

set -euo pipefail

# Configuration
SNAPSHOTS_DIR="/opt/themeparkhallofshame/snapshots"
APP_DIR="/opt/themeparkhallofshame"
MAX_SNAPSHOTS=10

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

log_info() {
    echo -e "${GREEN}✓${NC} $1"
}

log_error() {
    echo -e "${RED}✗${NC} $1" >&2
}

log_warn() {
    echo -e "${YELLOW}⚠${NC} $1"
}

log_step() {
    echo -e "${BLUE}→${NC} $1"
}

# =============================================================================
# Create Snapshot
# =============================================================================
create_snapshot() {
    local snapshot_name="$1"
    local snapshot_dir="${SNAPSHOTS_DIR}/${snapshot_name}"

    if [ -z "$snapshot_name" ]; then
        log_error "Snapshot name required"
        echo "Usage: $0 create <snapshot-name>" >&2
        exit 1
    fi

    if [ -d "$snapshot_dir" ]; then
        log_error "Snapshot already exists: ${snapshot_name}"
        exit 1
    fi

    log_step "Creating snapshot: ${snapshot_name}"

    # Create snapshot directory
    mkdir -p "$snapshot_dir"

    # Copy backend
    if [ -d "${APP_DIR}/backend" ]; then
        log_step "Backing up backend..."
        cp -r "${APP_DIR}/backend" "$snapshot_dir/"
        log_info "Backend backed up"
    else
        log_warn "Backend directory not found (skipping)"
    fi

    # Copy frontend
    if [ -d "${APP_DIR}/frontend" ]; then
        log_step "Backing up frontend..."
        cp -r "${APP_DIR}/frontend" "$snapshot_dir/"
        log_info "Frontend backed up"
    else
        log_warn "Frontend directory not found (skipping)"
    fi

    # Store metadata
    log_step "Storing metadata..."
    {
        echo "snapshot_name=${snapshot_name}"
        echo "created_at=$(date -u +"%Y-%m-%dT%H:%M:%SZ")"
        echo "hostname=$(hostname)"
        echo "user=$(whoami)"

        # Try to get git commit SHA if possible
        if [ -d "${APP_DIR}/backend/.git" ]; then
            cd "${APP_DIR}/backend"
            git_sha=$(git rev-parse --short HEAD 2>/dev/null || echo "unknown")
            echo "git_sha=${git_sha}"
        fi

        # Store directory sizes
        if [ -d "$snapshot_dir/backend" ]; then
            backend_size=$(du -sh "$snapshot_dir/backend" | cut -f1)
            echo "backend_size=${backend_size}"
        fi
        if [ -d "$snapshot_dir/frontend" ]; then
            frontend_size=$(du -sh "$snapshot_dir/frontend" | cut -f1)
            echo "frontend_size=${frontend_size}"
        fi
    } > "${snapshot_dir}/metadata.txt"

    log_info "Metadata stored"

    # Calculate total snapshot size
    total_size=$(du -sh "$snapshot_dir" | cut -f1)
    log_info "Snapshot created: ${snapshot_name} (${total_size})"

    # Prune old snapshots
    prune_snapshots

    return 0
}

# =============================================================================
# Restore Snapshot
# =============================================================================
restore_snapshot() {
    local snapshot_name="$1"
    local snapshot_dir="${SNAPSHOTS_DIR}/${snapshot_name}"

    if [ -z "$snapshot_name" ]; then
        log_error "Snapshot name required"
        echo "Usage: $0 restore <snapshot-name>" >&2
        exit 1
    fi

    if [ ! -d "$snapshot_dir" ]; then
        log_error "Snapshot not found: ${snapshot_name}"
        echo ""
        echo "Available snapshots:"
        list_snapshots
        exit 1
    fi

    log_warn "ROLLBACK: Restoring snapshot: ${snapshot_name}"
    echo ""
    echo "This will replace the current deployment with the snapshot."
    echo "Current deployment will be lost (unless you created a snapshot of it)."
    echo ""
    read -p "Are you sure you want to continue? (yes/no): " confirm

    if [ "$confirm" != "yes" ]; then
        log_info "Rollback cancelled"
        exit 0
    fi

    # Restore backend
    if [ -d "${snapshot_dir}/backend" ]; then
        log_step "Restoring backend..."
        rm -rf "${APP_DIR}/backend"
        cp -r "${snapshot_dir}/backend" "${APP_DIR}/"
        log_info "Backend restored"
    fi

    # Restore frontend
    if [ -d "${snapshot_dir}/frontend" ]; then
        log_step "Restoring frontend..."
        rm -rf "${APP_DIR}/frontend"
        cp -r "${snapshot_dir}/frontend" "${APP_DIR}/"
        log_info "Frontend restored"
    fi

    log_info "Snapshot restored: ${snapshot_name}"
    echo ""
    log_warn "Remember to restart services:"
    echo "  sudo systemctl restart themepark-api"
    echo "  sudo systemctl reload httpd"

    return 0
}

# =============================================================================
# List Snapshots
# =============================================================================
list_snapshots() {
    if [ ! -d "$SNAPSHOTS_DIR" ] || [ -z "$(ls -A $SNAPSHOTS_DIR 2>/dev/null)" ]; then
        echo "No snapshots available"
        return 0
    fi

    echo "Available snapshots (newest first):"
    echo ""

    # List snapshots sorted by modification time (newest first)
    ls -1t "$SNAPSHOTS_DIR" 2>/dev/null | while read snap; do
        local metadata_file="${SNAPSHOTS_DIR}/${snap}/metadata.txt"

        if [ -f "$metadata_file" ]; then
            echo -e "${GREEN}${snap}${NC}"

            # Parse and display metadata
            while IFS='=' read -r key value; do
                case "$key" in
                    created_at)
                        echo "  Created: ${value}"
                        ;;
                    git_sha)
                        echo "  Git SHA: ${value}"
                        ;;
                    backend_size)
                        echo "  Backend: ${value}"
                        ;;
                    frontend_size)
                        echo "  Frontend: ${value}"
                        ;;
                    hostname)
                        echo "  Host: ${value}"
                        ;;
                esac
            done < "$metadata_file"

            echo ""
        else
            echo -e "${YELLOW}${snap}${NC} (no metadata)"
            echo ""
        fi
    done

    # Show total snapshots count and disk usage
    local snapshot_count=$(ls -1 "$SNAPSHOTS_DIR" 2>/dev/null | wc -l | tr -d ' ')
    local total_size=$(du -sh "$SNAPSHOTS_DIR" 2>/dev/null | cut -f1)
    echo "Total: ${snapshot_count} snapshots (${total_size})"

    return 0
}

# =============================================================================
# Prune Old Snapshots
# =============================================================================
prune_snapshots() {
    if [ ! -d "$SNAPSHOTS_DIR" ]; then
        return 0
    fi

    local snapshot_count=$(ls -1 "$SNAPSHOTS_DIR" 2>/dev/null | wc -l | tr -d ' ')

    if [ "$snapshot_count" -le "$MAX_SNAPSHOTS" ]; then
        return 0
    fi

    local to_delete=$((snapshot_count - MAX_SNAPSHOTS))
    log_step "Pruning ${to_delete} old snapshot(s)..."

    # Delete oldest snapshots (keep newest MAX_SNAPSHOTS)
    ls -1t "$SNAPSHOTS_DIR" 2>/dev/null | tail -n "$to_delete" | while read snap; do
        rm -rf "${SNAPSHOTS_DIR}/${snap}"
        log_info "Pruned old snapshot: ${snap}"
    done

    return 0
}

# =============================================================================
# Main
# =============================================================================
main() {
    local command="${1:-}"

    case "$command" in
        create)
            create_snapshot "${2:-}"
            ;;
        restore)
            restore_snapshot "${2:-}"
            ;;
        list)
            list_snapshots
            ;;
        prune)
            prune_snapshots
            log_info "Prune complete"
            ;;
        *)
            echo "Usage: $0 {create|restore|list|prune} [snapshot_name]"
            echo ""
            echo "Commands:"
            echo "  create <name>   - Create a new deployment snapshot"
            echo "  restore <name>  - Restore a previous deployment snapshot"
            echo "  list            - List all available snapshots"
            echo "  prune           - Remove old snapshots (keep last ${MAX_SNAPSHOTS})"
            echo ""
            echo "Examples:"
            echo "  $0 create deploy-20250101-120000-abc1234"
            echo "  $0 restore deploy-20250101-120000-abc1234"
            echo "  $0 list"
            echo "  $0 prune"
            exit 1
            ;;
    esac
}

# Run main
main "$@"
