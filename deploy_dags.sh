#!/bin/bash
set -euo pipefail

SRC_DIR="/home/fastapi/etfscanner_site/airflow_dags"
TARGET="/opt/airflow/dags"

if [ ! -d "$SRC_DIR" ]; then
  echo "ERROR: source dir not found: $SRC_DIR" >&2
  exit 1
fi

sudo mkdir -p "$TARGET" 2>/dev/null || true

TMPDIR="$(mktemp -d /tmp/etfdeploy.XXXXXX)"
trap "rm -rf '$TMPDIR'" EXIT

rsync -a --delete --chmod=Du=rwx,Dg=rx,Do=rx,Fu=rw,Fg=r,Fo=r "$SRC_DIR/" "$TMPDIR/"

sudo chown -R airflow:airflow "$TMPDIR" 2>/dev/null || true

rsync -a --delete "$TMPDIR/" "$TARGET/"

sudo chown -R airflow:airflow "$TARGET" 2>/dev/null || true
sudo chmod -R u+rwX,g+rX,o+rX "$TARGET" 2>/dev/null || true

echo "Deploy complete: $(date -u +'%Y-%m-%dT%H:%M:%SZ')"
