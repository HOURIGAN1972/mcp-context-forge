#!/bin/bash

export PATH=/root/.local/bin:$PATH
source $WORKSPACE/$PIPELINE_CONFIG_REPO_PATH/scripts/utilities/python_utils.sh
install_python3 3.11
pip3.11 install --upgrade pip pytest pytest-cov sqlalchemy
mkdir -p app
echo "############# Python Version #################"
python3 -V
dnf install -y  postgresql-devel

echo "############# Installing Node.js 20.19+ ########"
# Check if nvm is already installed
if [ ! -d "$HOME/.nvm" ]; then
    echo "Installing nvm..."
    # Install nvm (ignore exit code as it may return non-zero after modifying .bashrc)
    curl -o- https://raw.githubusercontent.com/nvm-sh/nvm/v0.40.0/install.sh | bash || true
else
    echo "nvm already installed"
fi

# Load nvm into current shell - disable exit on error for sourcing
export NVM_DIR="$HOME/.nvm"
if [ -s "$NVM_DIR/nvm.sh" ]; then
    echo "Loading nvm from $NVM_DIR/nvm.sh"
    # Temporarily disable exit on error since nvm.sh may return non-zero
    set +e
    source "$NVM_DIR/nvm.sh"
    NVM_SOURCE_EXIT=$?
    set -e
    echo "nvm.sh sourced (exit code: $NVM_SOURCE_EXIT)"
else
    echo "ERROR: nvm.sh not found at $NVM_DIR/nvm.sh"
    ls -la "$NVM_DIR/" || echo "NVM_DIR does not exist"
    exit 1
fi

# Verify nvm is available
if ! command -v nvm &> /dev/null; then
    echo "ERROR: nvm command not found after loading"
    echo "Attempting to load nvm as a function..."
    # Try loading as a bash function
    set +e
    [ -s "$NVM_DIR/bash_completion" ] && source "$NVM_DIR/bash_completion"
    set -e
    if ! command -v nvm &> /dev/null; then
        echo "ERROR: Still cannot find nvm command"
        exit 1
    fi
fi

echo "nvm command is available"

# Install and use Node.js 20.19.0 (minimum required by Vite 7.3.2)
echo "Installing Node.js 20.19.0..."
nvm install 20.19.0 || {
    echo "ERROR: Failed to install Node.js 20.19.0"
    exit 1
}

nvm use 20.19.0 || {
    echo "ERROR: Failed to switch to Node.js 20.19.0"
    exit 1
}

echo "Node.js version: $(node --version)"
echo "npm version: $(npm --version)"

echo "############# Installing UV as a pre-requisite ########"
curl -LsSf https://astral.sh/uv/install.sh | sh

echo "############# Installing Rust for plugin builds ########"
# Install Rust and ensure it's available system-wide
curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh -s -- -y --default-toolchain stable --profile minimal
source "$HOME/.cargo/env"

# Set Rust environment variables for the entire session
export PATH="$HOME/.cargo/bin:$PATH"
export CARGO_HOME="$HOME/.cargo"
export RUSTUP_HOME="$HOME/.rustup"

# Verify Rust installation
echo "Rust version: $(rustc --version)"
echo "Cargo version: $(cargo --version)"
echo "Rustup version: $(rustup --version)"

echo "############# Running Install ################"
make venv install

echo "############# Installing plugins with verbose output ################"
. .venv/bin/activate
echo "Installing plugin packages..."
python3 -m uv pip install -v \
    "cpex-encoded-exfil-detection>=0.2.0" \
    "cpex-pii-filter>=0.2.1" \
    "cpex-rate-limiter>=0.0.4" \
    "cpex-retry-with-backoff>=0.1.0" \
    "cpex-secrets-detection>=0.2.0" \
    "cpex-url-reputation>=0.2.0" || {
    echo "ERROR: Plugin installation failed"
    echo "Checking if Rust is available..."
    rustc --version || echo "Rust not found"
    cargo --version || echo "Cargo not found"
    exit 1
}

echo "############# Verifying plugin installation ################"
echo "Checking installed packages..."
python3 -m pip list | grep cpex || {
    echo "ERROR: No cpex packages found after installation"
    exit 1
}
echo "Attempting to import cpex_retry_with_backoff..."
python3 -c "import cpex_retry_with_backoff; print('✓ cpex_retry_with_backoff imported successfully')" || {
    echo "✗ Failed to import cpex_retry_with_backoff"
    exit 1
}
echo "Attempting to import cpex_pii_filter..."
python3 -c "import cpex_pii_filter; print('✓ cpex_pii_filter imported successfully')" || {
    echo "✗ Failed to import cpex_pii_filter"
    exit 1
}
deactivate

echo "############# Installing dev dependencies ################"
make install-dev

echo "############# Running Linting ##################"
make ruff autoflake isort black
echo "############# Running Install dependencies ################"
. .venv/bin/activate && \
    python3 -m uv pip install 'psycopg[c]' && \
    python3 -m uv pip install 'psycopg2' && \
    python3 -m uv pip install 'openpyxl' && \
    python3 -m uv pip install 'copier' && \
    deactivate
echo "############# Running Install DB ################"
make install-db
echo "############# Running Tests and Coverage ##################"
source .venv/bin/activate && \
        export DATABASE_URL='sqlite:///:memory:' && \
        export TEST_DATABASE_URL='sqlite:///:memory:' && \
        uv run --active pytest -p pytest_cov -n auto --maxfail=0 -v --ignore=tests/fuzz --cov=mcpgateway
coverage xml
coverage report


echo "#############################"
echo "Preparing Evidence for Upload"
echo "#############################"

mkdir -p test/test_result_artifact_content
cp coverage.xml test/test_result_artifact_content/
cp coverage.xml test
