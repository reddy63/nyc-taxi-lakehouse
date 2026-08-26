#!/usr/bin/env bash
# databricks/scripts/configure_auth.sh
#
# Authenticates the Databricks CLI using OAuth U2M (browser-based).
# This is the method Databricks now recommends for CLI usage.
# No Personal Access Token (PAT) needed.
#
# What happens:
#   1. The CLI opens a browser tab on your laptop.
#   2. You log in with your Microsoft / Azure AD account
#      (the same one you used to create the workspace).
#   3. The CLI stores an OAuth refresh token in ~/.databrickscfg
#      automatically. You never see or copy a token string.
#
# Prerequisites:
#   - Databricks CLI v1.x installed  (run install_databricks_cli.sh first)
#   - A browser available on this machine
#
# Usage:
#   bash databricks/scripts/configure_auth.sh

set -euo pipefail

WORKSPACE_HOST="https://adb-7405606284470994.14.azuredatabricks.net"

echo ""
echo "=================================================="
echo "  Databricks CLI — OAuth U2M authentication"
echo "  Workspace: ${WORKSPACE_HOST}"
echo "=================================================="
echo ""
echo "A browser tab will open. Sign in with your Microsoft"
echo "account (arunreddyrikkala436@gmail.com)."
echo ""

databricks auth login \
    --host "${WORKSPACE_HOST}" \
    --profile DEFAULT

echo ""
echo "Authentication complete."
echo ""
echo "Verifying connection..."
databricks workspace list / --profile DEFAULT
echo ""
echo "Ready. You can now run:  bash databricks/scripts/deploy.sh dev"
