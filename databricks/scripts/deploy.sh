#!/usr/bin/env bash
# databricks/scripts/deploy.sh
# Full deploy + run flow using the new Databricks CLI.
# Prerequisite: run install_databricks_cli.sh and configure_auth.sh first.
#
# Usage:
#   bash databricks/scripts/deploy.sh              # deploy dev target
#   YEAR=2024 MONTHS=01,02,03 bash deploy.sh prod  # deploy + run prod

set -euo pipefail

TARGET="${1:-dev}"
YEAR="${YEAR:-2024}"
MONTHS="${MONTHS:-01}"
BUNDLE_ROOT="$(cd "$(dirname "$0")/.." && pwd)"

echo "========================================"
echo " NYC Taxi Lakehouse — Databricks Deploy"
echo " Target : ${TARGET}"
echo " Year   : ${YEAR}"
echo " Months : ${MONTHS}"
echo " Bundle : ${BUNDLE_ROOT}"
echo "========================================"
echo ""

cd "${BUNDLE_ROOT}"

# 1. Validate the bundle
echo "[1/3] Validating bundle..."
databricks bundle validate -t "${TARGET}"

# 2. Deploy (uploads code + creates/updates the job in Databricks)
echo "[2/3] Deploying bundle to ${TARGET}..."
databricks bundle deploy -t "${TARGET}"

# 3. Run the pipeline with parameters
echo "[3/3] Triggering pipeline run..."
databricks bundle run -t "${TARGET}" nyc_taxi_medallion_pipeline \
    --python-named-params "year=${YEAR},months=${MONTHS}"

echo ""
echo "Done! Monitor the run at:"
echo "  https://adb-7405606284470994.14.azuredatabricks.net/#job/list"
