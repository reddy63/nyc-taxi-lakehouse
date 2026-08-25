#!/usr/bin/env bash
# scripts/download_data.sh
# Download NYC TLC Yellow Taxi Parquet files for a given year + month.
#
# Usage:  bash scripts/download_data.sh 2024 01
#         bash scripts/download_data.sh 2024 01,02,03

set -euo pipefail

YEAR="${1:?Usage: download_data.sh YEAR MONTH[,MONTH,...]}"
RAW_MONTHS="${2:?Usage: download_data.sh YEAR MONTH[,MONTH,...]}"
TAXI_TYPE="${TAXI_TYPE:-yellow}"
DOWNLOAD_DIR="${DOWNLOAD_DIR:-/tmp/nyc_taxi_raw}"
TLC_BASE="https://d37ci6vzurychx.cloudfront.net/trip-data"

mkdir -p "${DOWNLOAD_DIR}"

IFS=',' read -ra MONTHS <<< "${RAW_MONTHS}"

for MONTH in "${MONTHS[@]}"; do
    MONTH=$(printf '%02d' "$((10#${MONTH}))")   # zero-pad
    FILE="${TAXI_TYPE}_tripdata_${YEAR}-${MONTH}.parquet"
    URL="${TLC_BASE}/${FILE}"
    DEST="${DOWNLOAD_DIR}/${FILE}"

    if [[ -f "${DEST}" ]]; then
        echo "[SKIP]  Already downloaded: ${FILE}"
        continue
    fi

    echo "[DL]    Downloading ${URL}"
    curl -fsSL --retry 3 --retry-delay 5 \
         -o "${DEST}" \
         "${URL}" \
    && echo "[OK]    Saved to ${DEST}" \
    || echo "[WARN]  Failed to download ${URL} (TLC may not have published yet)"
done

echo "[DONE]  All downloads complete. Files in ${DOWNLOAD_DIR}"
