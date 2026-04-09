#!/bin/bash
set -euo pipefail

cd "$(dirname "$0")"

if [ ! -d ".venv" ]; then
  python3 -m venv --system-site-packages .venv
elif ! grep -q "include-system-site-packages = true" .venv/pyvenv.cfg; then
  rm -rf .venv
  python3 -m venv --system-site-packages .venv
fi

source .venv/bin/activate
python3 -m pip install --upgrade pip
python3 -m pip install -r requirements.txt

if [ "${POLLER_ONLY:-false}" = "true" ]; then
  exec python3 -m app.poller_only
fi

if [ "${BLE_SITE_ONLY:-false}" = "true" ]; then
  exec python3 -m uvicorn app.ble_site:app --host "${BLE_SITE_HOST:-0.0.0.0}" --port "${BLE_SITE_PORT:-8002}"
fi

exec python3 -m uvicorn app.main:app --host 0.0.0.0 --port 8001
