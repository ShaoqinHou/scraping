# Common Commands (server, bash)

## Update code and restart
```
cd /root/scraping
git pull
sudo bash scripts/fix_service.sh
```

## Reset AI flags (rerun AI from scratch)
```
cd /root/scraping
DB_PATH=/root/scraping/qn_hydrogen_monitor.db ./scripts/mark_ai_not_improved.sh
sudo systemctl restart scraping.service
```

## Set default AI model list
```
cd /root/scraping
python3 scripts/set_ai_models.py
sudo systemctl restart scraping.service
```

## View AI log summary (per-model timing + last 20 entries)
```
cd /root/scraping
python3 scripts/ai_log_summary.py
```

## Tail AI log
```
cd /root/scraping
tail -n 80 ai_project_extractor.log
```

## Service status
```
sudo systemctl status scraping.service
```

## Backup database
```
cd /root/scraping
cp qn_hydrogen_monitor.db qn_hydrogen_monitor.db.bak.$(date +%Y%m%d%H%M%S)
```
