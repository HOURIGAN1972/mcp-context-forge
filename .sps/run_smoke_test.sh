#!/usr/bin/env bash
source $WORKSPACE/$PIPELINE_CONFIG_REPO_PATH/scripts/utilities/python_utils.sh
install_python3 3.11

GH_USER="ISC-REL"
GH_TOKEN="$(get_env git-token)"
GH_URL="https://${GH_USER}:${GH_TOKEN}@github.ibm.com/cyberfraud/cyberfraud-mcp-management-service.git"
git clone $GH_URL

cd cyberfraud-mcp-management-service
echo "#############################"
echo "Running smoke tests"
echo "#############################"
python3 -m pip install --upgrade pip setuptools
python3 -m pip install uv --user
export PATH=/root/.local/bin/:$PATH
FILES_TO_TEST="tests/integration/test_account_user_api.py tests/integration/test_claims_api_v1.py tests/integration/test_gateway_health_api.py tests/integration/test_server_api.py tests/integration/test_tools_api.py tests/integration/test_server_and_tools_apis_destructive.py"
uv run pytest $FILES_TO_TEST -v -s --setup-show --git_token=$(get_env git-token) --mcp_gateway_image_tag="${MCP_GATEWAY_IMAGE_TAG}"
if [ $? != 0 ]; then
  echo "Integration test failed, exiting";
  exit 1;
fi
cd -
