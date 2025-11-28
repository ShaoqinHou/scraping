#!/usr/bin/env bash
set -euo pipefail

APP_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$APP_DIR"

echo "[1/4] Pull latest code..."
git pull

echo "[2/4] Activate venv..."
source .venv/bin/activate

echo "[3/4] Ensure Playwright browsers..."
export PLAYWRIGHT_BROWSERS_PATH="$VIRTUAL_ENV/playwright-browsers"
python -m playwright install chromium chromium-headless-shell --with-deps

echo "[4/4] Restart app (systemd service 'scraping' expected)..."
if command -v systemctl >/dev/null 2>&1; then
  sudo systemctl restart scraping
  sudo systemctl status scraping --no-pager
else
  echo "systemctl not found. Start manually with:"
  echo "source .venv/bin/activate && APP_HOST=0.0.0.0 APP_PORT=8000 PLAYWRIGHT_BROWSERS_PATH=$PLAYWRIGHT_BROWSERS_PATH python app.py"
fi

echo "Done."
