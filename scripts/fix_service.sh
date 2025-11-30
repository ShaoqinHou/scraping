#!/usr/bin/env bash
# Rewrite and reload scraping.service with correct ExecStart
set -euo pipefail

SERVICE_FILE="/etc/systemd/system/scraping.service"

cat > "$SERVICE_FILE" <<'EOF'
[Unit]
Description=Scraping Flask App
After=network.target

[Service]
WorkingDirectory=/root/scraping
Environment=PLAYWRIGHT_BROWSERS_PATH=/root/scraping/.venv/playwright-browsers
ExecStart=/bin/bash -lc 'cd /root/scraping && source .venv/bin/activate && APP_HOST=0.0.0.0 APP_PORT=8000 python app.py'
Restart=always
RestartSec=5
User=root
StandardOutput=journal
StandardError=journal

[Install]
WantedBy=multi-user.target
EOF

systemctl daemon-reload
systemctl restart scraping.service
systemctl status scraping.service
