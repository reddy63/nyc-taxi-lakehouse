#!/usr/bin/env bash
# databricks/scripts/install_databricks_cli.sh
# Downloads the new Databricks CLI v1.x (required for Asset Bundle commands).
# The legacy databricks-cli (pip, v0.18) does not support `bundle` commands.
#
# Usage:  bash databricks/scripts/install_databricks_cli.sh

set -euo pipefail

VERSION="1.13.0"
ARCH="linux_amd64"
BIN_DIR="$HOME/.local/bin"
DOWNLOAD_URL="https://github.com/databricks/cli/releases/download/v${VERSION}/databricks_cli_${VERSION}_${ARCH}.zip"

mkdir -p "${BIN_DIR}"

echo "[1/3] Downloading Databricks CLI v${VERSION}..."
wget -q --show-progress -O /tmp/databricks_cli.zip "${DOWNLOAD_URL}"

echo "[2/3] Extracting..."
unzip -o /tmp/databricks_cli.zip -d /tmp/databricks_extract/
mv /tmp/databricks_extract/databricks "${BIN_DIR}/databricks"
chmod +x "${BIN_DIR}/databricks"
rm -rf /tmp/databricks_cli.zip /tmp/databricks_extract/

echo "[3/3] Adding to PATH..."
grep -qxF 'export PATH=$HOME/.local/bin:$PATH' ~/.bashrc || \
    echo 'export PATH=$HOME/.local/bin:$PATH' >> ~/.bashrc

export PATH="${BIN_DIR}:$PATH"

echo ""
databricks version
echo ""
echo "Done! Restart your terminal or run:  source ~/.bashrc"
