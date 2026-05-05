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

# Load nvm into current shell
export NVM_DIR="$HOME/.nvm"
[ -s "$NVM_DIR/nvm.sh" ] && \. "$NVM_DIR/nvm.sh" || {
    echo "ERROR: Failed to load nvm"
    exit 1
}

# Verify nvm is available
if ! command -v nvm &> /dev/null; then
    echo "ERROR: nvm command not found after loading"
    exit 1
fi

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
