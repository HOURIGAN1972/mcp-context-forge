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
curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh -s -- -y --default-toolchain stable
source "$HOME/.cargo/env"
echo "Rust version: $(rustc --version)"

echo "############# Running Install ################"
make venv install install-dev
echo "############# Running Linting ##################"
make ruff autoflake isort black
echo "############# Running Install dependencies ################"
. $HOME/.venv/mcpgateway/bin/activate && \
    python3 -m uv pip install 'psycopg[c]' && \
    python3 -m uv pip install 'psycopg2' && \
    python3 -m uv pip install 'openpyxl' && \
    python3 -m uv pip install 'copier' && \
    deactivate
echo "############# Running Install DB ################"
make install-db
echo "############# Running Tests and Coverage ##################"
source $HOME/.venv/mcpgateway/bin/activate && \
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
