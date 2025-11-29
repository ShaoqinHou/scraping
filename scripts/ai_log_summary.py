#!/usr/bin/env python3
"""
Summarize ai_project_extractor.log:
- Per-model counts/avg/min/max elapsed (last 2000 lines)
- Last 20 entries with model and elapsed
"""

import re
import pathlib

LOG_PATH = pathlib.Path(__file__).resolve().parent.parent / "ai_project_extractor.log"


def main() -> None:
    if not LOG_PATH.exists():
        print(f"Log not found: {LOG_PATH}")
        return

    text = LOG_PATH.read_text(encoding="utf-8", errors="ignore").splitlines()
    entries = []
    for ln in text[-2000:]:
        m = re.search(r"model=([^ ]+).*?elapsed=([0-9.]+)", ln)
        if m:
            entries.append((m.group(1), float(m.group(2)), ln))

    print("Per-model timing (last 2000 lines):")
    by_model = {}
    for model, t, _ in entries:
        by_model.setdefault(model, []).append(t)
    for model, times in by_model.items():
        avg = sum(times) / len(times)
        print(f"{model}: n={len(times)}, avg={avg:.1f}s, min={min(times):.1f}, max={max(times):.1f}")

    print("\nLast 20 entries:")
    for _, _, ln in entries[-20:]:
        print(ln)


if __name__ == "__main__":
    main()
