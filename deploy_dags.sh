#!/bin/bash
set -euo pipefail

SRC_DIR="/home/fastapi/etfscanner_site/airflow_dags"
TARGET="/opt/airflow/dags"

if [ ! -d "$SRC_DIR" ]; then
  echo "ERROR: source dir not found: $SRC_DIR" >&2
  exit 1
fi

sudo mkdir -p "$TARGET" 2>/dev/null || true

# Copy files from source to target
sudo cp -r "$SRC_DIR"/* "$TARGET/" 2>/dev/null || true

# Fix ownership and permissions
sudo chown -R airflow:airflow "$TARGET" 2>/dev/null || true
sudo chmod -R u+rwX,g+rX,o+rX "$TARGET" 2>/dev/null || true

echo "Deploy complete: $(date -u +'%Y-%m-%dT%H:%M:%SZ')"
