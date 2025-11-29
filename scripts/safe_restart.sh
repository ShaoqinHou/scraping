#!/usr/bin/env bash
#
# Safe restart helper:
# - Checks key status endpoints; if any are "running" it exits without restarting.
# - If all are idle (or status endpoints unavailable), it restarts the scraping.service.
#
# Intended to be called by a systemd timer (see instructions in README/ops).

set -euo pipefail

BASE_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
APP_URL="${APP_URL:-http://127.0.0.1:8000}"
SERVICE_NAME="${SERVICE_NAME:-scraping.service}"

STATUS_ENDPOINTS=(
  "${APP_URL}/api/hydrogen/status"
  "${APP_URL}/api/hydrogen/projects/classic/status"
  "${APP_URL}/api/hydrogen/projects/ai/status"
)

log() { echo "[$(date '+%F %T')] $*"; }

is_idle() {
  python3 - "$@" <<'PY'
import json, sys, urllib.request

urls = sys.argv[1:]
running = []
for url in urls:
    try:
        with urllib.request.urlopen(url, timeout=5) as r:
            data = json.load(r)
            stage = str(data.get("stage", "")).lower()
            if stage == "running":
                running.append(url)
    except Exception:
        # Treat unreachable endpoint as "not blocking"
        continue

if running:
    print("busy:", ", ".join(running))
    sys.exit(1)
print("idle")
PY
}

main() {
  log "Checking app status before restart..."
  if is_idle "${STATUS_ENDPOINTS[@]}"; then
    log "All idle. Restarting ${SERVICE_NAME}."
    systemctl restart "${SERVICE_NAME}"
    log "Restart issued."
  else
    log "App is busy; skip restart."
  fi
}

main
