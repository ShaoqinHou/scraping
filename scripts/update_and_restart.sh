#!/usr/bin/env bash
set -euo pipefail

APP_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$APP_DIR"

echo "[1/3] Pull latest code..."
git pull

echo "[2/3] Activate venv and ensure browsers..."
source .venv/bin/activate
export PLAYWRIGHT_BROWSERS_PATH="$VIRTUAL_ENV/playwright-browsers"
python -m playwright install chromium chromium-headless-shell --with-deps

echo "[3/3] Start app (manual run, foreground)..."
export APP_HOST="${APP_HOST:-0.0.0.0}"
export APP_PORT="${APP_PORT:-8000}"
echo "APP_HOST=$APP_HOST APP_PORT=$APP_PORT"
echo "Press Ctrl+C to stop."
python app.py

