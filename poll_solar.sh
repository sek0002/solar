#!/bin/bash
set -euo pipefail

cd "$(dirname "$0")"

if [ ! -d ".venv" ]; then
  python3 -m venv .venv
fi

source .venv/bin/activate
python3 -m pip install --upgrade pip
python3 -m pip install -r requirements.txt

GLOBAL_BLEAK_VERSION="$(python3 - <<'PY'
from importlib.metadata import PackageNotFoundError, version
try:
    print(version("bleak"))
except PackageNotFoundError:
    print("")
PY
)"

if [ -n "${GLOBAL_BLEAK_VERSION}" ]; then
  python3 -m pip install "bleak==${GLOBAL_BLEAK_VERSION}"
fi

exec python3 -m uvicorn app.main:app --host 0.0.0.0 --port 8000
