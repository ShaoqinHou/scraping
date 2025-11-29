#!/usr/bin/env bash
# One-time helper: reset AI flags so AI step can rerun on all rows.
# Usage: ./scripts/mark_ai_not_improved.sh

set -euo pipefail

BASE_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
DB_PATH="${BASE_DIR}/qn_hydrogen_monitor.db"

if [[ ! -f "${DB_PATH}" ]]; then
  echo "DB not found: ${DB_PATH}"
  exit 1
fi

echo "Using DB: ${DB_PATH}"
python3 - <<PY
import sqlite3, sys, os
db = os.environ.get("DB_PATH", "${DB_PATH}")
conn = sqlite3.connect(db)
cur = conn.cursor()
cur.execute("UPDATE projects_classic SET is_ai_improved = 0")
conn.commit()
cur.execute("SELECT COUNT(*) FROM projects_classic")
total = cur.fetchone()[0]
conn.close()
print(f"Reset is_ai_improved to 0 for {total} rows.")
PY
