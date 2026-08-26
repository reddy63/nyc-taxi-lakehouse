#!/usr/bin/env bash
# databricks/scripts/configure_auth.sh
# Writes ~/.databrickscfg with your workspace credentials.
# Run this ONCE after you have your Personal Access Token from Databricks UI.
#
# How to get your token:
#   Databricks UI -> top-right avatar -> Settings -> Developer -> Access tokens
#   -> Generate new token -> copy the string
#
# Usage:
#   export DATABRICKS_TOKEN=dapi...
#   bash databricks/scripts/configure_auth.sh

set -euo pipefail

WORKSPACE_HOST="https://adb-7405606284470994.14.azuredatabricks.net"
TOKEN="${DATABRICKS_TOKEN:-}"

if [[ -z "${TOKEN}" ]]; then
    echo "ERROR: Set DATABRICKS_TOKEN before running this script."
    echo "  export DATABRICKS_TOKEN=dapi..."
    exit 1
fi

CFG_FILE="$HOME/.databrickscfg"

# Write/replace the [DEFAULT] profile
python3 - << PYEOF
import configparser, os
path = os.path.expanduser('~/.databrickscfg')
cfg  = configparser.ConfigParser()
if os.path.exists(path):
    cfg.read(path)
if 'DEFAULT' not in cfg:
    cfg['DEFAULT'] = {}
cfg['DEFAULT']['host']  = '${WORKSPACE_HOST}'
cfg['DEFAULT']['token'] = '${TOKEN}'
with open(path, 'w') as f:
    cfg.write(f)
print(f'Written: {path}')
PYEOF

echo ""
echo "Workspace : ${WORKSPACE_HOST}"
echo "Token     : ${TOKEN:0:8}...(redacted)"
echo ""
echo "Test with:  databricks workspace list /"
echo "           (new CLI)  or"
echo "           databricks fs ls  (legacy CLI)"
