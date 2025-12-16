#!/bin/bash
# =============================================================================
# Pre-Flight Validation Script
# =============================================================================
#
# Purpose: Validate code on LOCAL machine before deploying to production
#
# Validations:
#   1. Python syntax check (AST parsing)
#   2. Import validation (all modules can be imported)
#   3. WSGI validation (wsgi.py creates Flask app)
#   4. Dependency validation (requirements.txt is valid)
#   5. Environment template check (.env.example has required vars)
#
# Usage:
#   ./pre-flight-validate.sh /path/to/project/root
#
# Exit Codes:
#   0 - All validations passed
#   1 - One or more validations failed
#
# =============================================================================

set -euo pipefail

# Check arguments
if [ $# -ne 1 ]; then
    echo "Usage: $0 /path/to/project/root" >&2
    exit 1
fi

PROJECT_ROOT="$1"
BACKEND_DIR="${PROJECT_ROOT}/backend"
VALIDATION_VENV="${PROJECT_ROOT}/.validation_venv"

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
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

# =============================================================================
# Validation 1: Python Syntax Check
# =============================================================================
validate_syntax() {
    echo ""
    echo "=== Validation 1: Python Syntax Check ==="

    python3 -c "
import sys
import ast
from pathlib import Path

errors = []
backend_src = Path('${BACKEND_DIR}/src')

if not backend_src.exists():
    print(f'ERROR: Backend source directory not found: {backend_src}', file=sys.stderr)
    sys.exit(1)

for py_file in backend_src.rglob('*.py'):
    try:
        with open(py_file, 'r', encoding='utf-8') as f:
            ast.parse(f.read(), filename=str(py_file))
    except SyntaxError as e:
        errors.append(f'{py_file.relative_to(backend_src)}: Line {e.lineno}: {e.msg}')
    except Exception as e:
        errors.append(f'{py_file.relative_to(backend_src)}: {e}')

if errors:
    print('SYNTAX ERRORS FOUND:', file=sys.stderr)
    for err in errors:
        print(f'  {err}', file=sys.stderr)
    sys.exit(1)
else:
    print(f'Checked {len(list(backend_src.rglob(\"*.py\")))} Python files')
"

    if [ $? -eq 0 ]; then
        log_info "Python syntax check passed"
        return 0
    else
        log_error "Python syntax check failed"
        return 1
    fi
}

# =============================================================================
# Validation 2: Import Validation
# =============================================================================
validate_imports() {
    echo ""
    echo "=== Validation 2: Import Validation ==="

    # Create isolated venv for import testing
    log_info "Creating isolated validation environment..."
    python3 -m venv "$VALIDATION_VENV" 2>&1 | grep -v "This may take a few minutes" || true

    # Activate venv
    source "${VALIDATION_VENV}/bin/activate"

    # Upgrade pip quietly
    pip install --upgrade pip --quiet 2>&1 | grep -v "Requirement already satisfied" || true

    # Install dependencies
    log_info "Installing dependencies..."
    if [ -f "${BACKEND_DIR}/requirements.txt" ]; then
        # Install dependencies, capture output
        set +e  # Temporarily allow errors
        local install_output=$(pip install -r "${BACKEND_DIR}/requirements.txt" 2>&1)
        local install_exit=$?
        set -e

        # Check if installation failed
        if [ $install_exit -ne 0 ]; then
            # Check if it's just mysqlclient (expected on macOS)
            if echo "$install_output" | grep -q "mysqlclient"; then
                log_warn "mysqlclient build failed (expected on macOS)"
                log_info "Installing pymysql as fallback..."
                pip install -q pymysql
                log_info "Dependencies installed (using pymysql fallback)"
            else
                log_error "Failed to install dependencies"
                echo "$install_output" >&2
                deactivate
                return 1
            fi
        else
            log_info "Dependencies installed successfully"
        fi
    else
        log_error "requirements.txt not found"
        deactivate
        return 1
    fi

    # Test imports
    log_info "Testing module imports..."
    cd "${BACKEND_DIR}"
    python3 -c "
import sys
import os

# Add src to path
sys.path.insert(0, 'src')

# Set minimal environment for testing
os.environ['ENVIRONMENT'] = 'test'
os.environ['DB_HOST'] = 'localhost'
os.environ['DB_NAME'] = 'test'
os.environ['DB_USER'] = 'test'
os.environ['DB_PASSWORD'] = 'test'
os.environ['LOG_LEVEL'] = 'ERROR'

# Critical modules to test
modules = [
    'api.app',
    'api.routes.health',
    'api.routes.parks',
    'api.routes.rides',
    'api.routes.trends',
    'api.routes.audit',
    'api.routes.search',
    'database.connection',
    'utils.config',
    'utils.timezone',
    'utils.sql_helpers',
]

failed = []
for module in modules:
    try:
        __import__(module)
    except ImportError as e:
        failed.append(f'{module}: Missing import: {e}')
    except Exception as e:
        failed.append(f'{module}: {type(e).__name__}: {e}')

if failed:
    print('IMPORT ERRORS:', file=sys.stderr)
    for err in failed:
        print(f'  {err}', file=sys.stderr)
    sys.exit(1)
else:
    print(f'Successfully imported {len(modules)} critical modules')
"

    local result=$?

    # Deactivate and cleanup
    deactivate
    cd "$PROJECT_ROOT"

    if [ $result -eq 0 ]; then
        log_info "Import validation passed"
        return 0
    else
        log_error "Import validation failed"
        return 1
    fi
}

# =============================================================================
# Validation 3: WSGI Validation
# =============================================================================
validate_wsgi() {
    echo ""
    echo "=== Validation 3: WSGI Validation ==="

    # Activate validation venv (created in previous step)
    source "${VALIDATION_VENV}/bin/activate"

    cd "${BACKEND_DIR}"
    python3 -c "
import sys
import os

# Set minimal environment
os.environ['ENVIRONMENT'] = 'test'
os.environ['DB_HOST'] = 'localhost'
os.environ['DB_NAME'] = 'test'
os.environ['DB_USER'] = 'test'
os.environ['DB_PASSWORD'] = 'test'
os.environ['SECRET_KEY'] = 'test-secret-key'
os.environ['LOG_LEVEL'] = 'ERROR'

try:
    # Execute wsgi.py
    with open('wsgi.py', 'r') as f:
        wsgi_code = f.read()

    # Create execution environment with __file__ defined
    wsgi_globals = {'__file__': os.path.abspath('wsgi.py')}
    exec(wsgi_code, wsgi_globals)

    # Check that application object was created
    if 'application' not in wsgi_globals:
        raise Exception('wsgi.py did not create application object')

    # Check that it's a Flask app
    application = wsgi_globals['application']
    if not hasattr(application, 'route'):
        raise Exception('application object is not a Flask app')

    print('WSGI application created successfully')
    print(f'Flask app name: {application.name}')

except FileNotFoundError:
    print('ERROR: wsgi.py not found', file=sys.stderr)
    sys.exit(1)
except Exception as e:
    print(f'WSGI ERROR: {type(e).__name__}: {e}', file=sys.stderr)
    sys.exit(1)
"

    local result=$?

    deactivate
    cd "$PROJECT_ROOT"

    if [ $result -eq 0 ]; then
        log_info "WSGI validation passed"
        return 0
    else
        log_error "WSGI validation failed"
        return 1
    fi
}

# =============================================================================
# Validation 4: Dependency Validation
# =============================================================================
validate_dependencies() {
    echo ""
    echo "=== Validation 4: Dependency Validation ==="

    # Activate validation venv
    source "${VALIDATION_VENV}/bin/activate"

    # Run pip check for conflicts
    log_info "Checking for dependency conflicts..."
    pip check 2>&1 | tee /tmp/pip-check-output.txt
    local result=${PIPESTATUS[0]}

    deactivate

    if [ $result -eq 0 ]; then
        log_info "Dependency validation passed (no conflicts)"
        return 0
    else
        log_error "Dependency conflicts detected"
        cat /tmp/pip-check-output.txt >&2
        return 1
    fi
}

# =============================================================================
# Validation 5: Environment Template Check
# =============================================================================
validate_env_template() {
    echo ""
    echo "=== Validation 5: Environment Template Check ==="

    python3 -c "
import sys
import re
from pathlib import Path

required_vars = [
    'DB_HOST', 'DB_PORT', 'DB_NAME', 'DB_USER', 'DB_PASSWORD',
    'FLASK_ENV', 'SECRET_KEY', 'LOG_LEVEL', 'ENVIRONMENT'
]

env_example_path = Path('${BACKEND_DIR}/.env.example')

if not env_example_path.exists():
    print('ERROR: .env.example not found', file=sys.stderr)
    sys.exit(1)

with open(env_example_path) as f:
    env_content = f.read()

missing = []
for var in required_vars:
    # Match lines like: VAR_NAME=value or # VAR_NAME=value
    if not re.search(rf'^#?\s*{var}=', env_content, re.MULTILINE):
        missing.append(var)

if missing:
    print('ERROR: Missing required environment variables in .env.example:', file=sys.stderr)
    for var in missing:
        print(f'  {var}', file=sys.stderr)
    sys.exit(1)
else:
    print(f'All {len(required_vars)} required environment variables present')
"

    if [ $? -eq 0 ]; then
        log_info "Environment template check passed"
        return 0
    else
        log_error "Environment template check failed"
        return 1
    fi
}

# =============================================================================
# Cleanup
# =============================================================================
cleanup() {
    if [ -d "$VALIDATION_VENV" ]; then
        log_info "Cleaning up validation environment..."
        rm -rf "$VALIDATION_VENV"
    fi
}

# =============================================================================
# Main Execution
# =============================================================================
main() {
    echo "========================================"
    echo " Pre-Flight Validation"
    echo "========================================"
    echo "Project root: $PROJECT_ROOT"
    echo ""

    # Track failures
    local failed=0

    # Run all validations
    validate_syntax || failed=1
    validate_imports || failed=1
    validate_wsgi || failed=1
    validate_dependencies || failed=1
    validate_env_template || failed=1

    # Cleanup
    cleanup

    # Summary
    echo ""
    echo "========================================"
    if [ $failed -eq 0 ]; then
        log_info "All pre-flight validations passed!"
        echo "========================================"
        return 0
    else
        log_error "Pre-flight validation failed!"
        echo "========================================"
        echo ""
        echo "Fix the errors above before deploying to production."
        return 1
    fi
}

# Run main and exit with its exit code
main
exit $?
