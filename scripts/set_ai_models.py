#!/usr/bin/env python3
"""
Set the SILICONFLOW_MODEL list in secrets.json to the five allowed models.
Usage:
  python3 scripts/set_ai_models.py
"""

import json
import pathlib

ALLOWED_MODELS = (
    "Qwen/Qwen2.5-7B-Instruct "
    "Qwen/Qwen2-7B-Instruct "
    "Qwen/Qwen2.5-Coder-7B-Instruct "
    "THUDM/glm-4-9b-chat "
    "THUDM/GLM-4-9B-0414"
)


def main() -> None:
    secrets_path = pathlib.Path(__file__).resolve().parent.parent / "secrets.json"
    data = {}
    if secrets_path.exists():
        try:
            data = json.loads(secrets_path.read_text(encoding="utf-8"))
        except Exception:
            data = {}
    data["SILICONFLOW_MODEL"] = ALLOWED_MODELS
    secrets_path.write_text(json.dumps(data, ensure_ascii=False, indent=2), encoding="utf-8")
    print("Updated SILICONFLOW_MODEL to:")
    print(ALLOWED_MODELS)


if __name__ == "__main__":
    main()
