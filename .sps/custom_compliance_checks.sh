#!/bin/bash

export PATH=/root/.local/bin:$PATH
source $WORKSPACE/$PIPELINE_CONFIG_REPO_PATH/scripts/utilities/python_utils.sh
source $WORKSPACE/$PIPELINE_CONFIG_REPO_PATH/scripts/utilities/go_utils.sh
install_python3 3.11
# Go is not needed for Python dependency scanning and causes Mend failures
# install_go is commented out - Go scanning is disabled in config files
# Remove all Go directories to prevent Mend from attempting Go dependency resolution
rm -rf mcp-servers/go a2a-agents/go mcp-servers/templates/go
pip3.11 install --upgrade pip
mkdir -p app
echo "############# Python Version #################"
python3 -V
