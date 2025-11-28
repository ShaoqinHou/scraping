# Repository Guidelines

## Project Structure & Module Organization
`final_improved_collector_integrated.py` drives the Playwright crawler and hands CAPTCHA images to the Flask helper in `captcha_server.py` plus the UI under `templates/captcha.html`. AI summarization runs through `ai_project_analyzer.py`, which composes helpers like `project_grouper.py`, `document_fetcher.py`, `qwen_api_client.py`, and `output_schema.py`. CSV exports (e.g., `detailed_project_data_20250726_162804.csv`) and logs such as `ai_project_analyzer.log` stay at the repository root for easy pickup by tooling.

## Build, Test, and Development Commands
- `python -m venv .venv && .venv\Scripts\activate` (Windows) or `source .venv/bin/activate` (Unix) — set up an isolated interpreter.
- `pip install -r requirements.txt` then `playwright install chromium` — install Python dependencies and the required browser runtime.
- `python captcha_server.py` and `python final_improved_collector_integrated.py [--retry]` — start the CAPTCHA UI and the scraper; `--retry` replays only failed pages.
- `python ai_project_analyzer.py detailed_project_data_20250726_162804.csv --keywords wind solar -o ai_analysis.json --batch-size 25` — run the AI pipeline on a filtered subset.
- `python check_csv_columns.py` or `python check_keywords.py` — inspect schema and keyword coverage before sharing data.

## Coding Style & Naming Conventions
Follow PEP 8: four-space indentation, snake_case functions, CapWords classes. Keep tunable values (timeouts, rate limits, API URLs) in ALL_CAPS constants near the top of each module. Timestamp CSV filenames as `detailed_project_data_YYYYMMDD_HHMMSS.csv`, keep HTML under `templates/`, and limit inline comments to clarifying tricky Playwright interactions.

## Testing Guidelines
No automated suite exists, so run manual smoke tests per change. Solve a few CAPTCHAs through the UI, confirm the resulting CSV via `check_csv_columns.py`, and open several rows to verify attachment URLs. For analyzer edits, run `ai_project_analyzer.py ... --batch-size 5`, inspect the emitted JSON, and scan `ai_project_analyzer.log` for document fetch or token errors.

## Commit & Pull Request Guidelines
Write imperative, present-tense commit subjects (Conventional Commits such as `feat: improve captcha polling` are welcome). Describe which datasets or logs changed, list the manual commands executed, and mention any new artifacts. PRs should link the driving issue, note scraper scope (page ranges, keyword filters, retry flags), and include screenshots or row deltas when collector output shifts.

## Security & Configuration Tips
Keep secrets like `DASHSCOPE_API_KEY` in your shell environment or a Git-ignored `.env`, never in source. Prefer environment variables or CLI flags for operational overrides, and redact sensitive project identifiers before sharing CSVs externally.
