import csv
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path
from typing import Callable, Dict, List, Optional

from openai import OpenAI


class AIDetailSummarizer:
    """AI 后处理：对详情 CSV 行生成简短中文摘要与要点列表。"""

    def __init__(
        self,
        api_key: str,
        model: str = "deepseek-ai/DeepSeek-V3",
        base_url: str = "https://api.siliconflow.cn/v1",
    ):
        self.client = OpenAI(api_key=api_key, base_url=base_url)
        self.model = model

    def _build_prompt(self, row: Dict[str, str]) -> str:
        parts = []
        fields = [
            "项目名称",
            "审批事项",
            "审批结果",
            "审批日期",
            "审批部门",
            "申报单位",
            "项目代码",
            "项目类型",
            "附件名称",
            "附件链接",
        ]
        for f in fields:
            v = row.get(f, "")
            if v:
                parts.append(f"{f}：{v}")
        body = "\n".join(parts)
        return (
            "你是政府项目审批信息助手。根据下列字段，生成：\n"
            "1) 一个不超过120字的中文摘要，突出项目名称/审批事项/结果/日期/申报单位；\n"
            "2) 2-4 条要点，每条以“- ”开头，涵盖金额/时间/批复/附件等关键词；\n"
            "返回 JSON，形如 {\"ai_summary\": \"...\", \"ai_points\": \"- ...\\n- ...\"}。\n\n"
            f"字段：\n{body}\n"
        )

    def _summarize_row(self, row: Dict[str, str]) -> Dict[str, str]:
        prompt = self._build_prompt(row)
        try:
            resp = self.client.chat.completions.create(
                model=self.model,
                messages=[
                    {"role": "system", "content": "保持输出为简体中文。"},
                    {"role": "user", "content": prompt},
                ],
                temperature=0.2,
                max_tokens=400,
            )
            content = (resp.choices[0].message.content or "").strip()
            if "```" in content:
                # 容错：剥掉 code fence
                if "```json" in content:
                    content = content.split("```json", 1)[-1].split("```", 1)[0].strip()
                else:
                    content = content.split("```", 1)[-1].split("```", 1)[0].strip()
            import json as _json

            parsed = _json.loads(content)
            return {
                "AI摘要": parsed.get("ai_summary") or "",
                "AI要点": parsed.get("ai_points") or "",
            }
        except Exception:
            return {"AI摘要": "", "AI要点": ""}

    def run(
        self,
        csv_path: str,
        max_rows: Optional[int] = None,
        max_workers: int = 4,
        progress_callback: Optional[Callable[..., None]] = None,
    ) -> Optional[str]:
        path = Path(csv_path)
        if not path.exists():
            return None

        with path.open(newline="", encoding="utf-8-sig") as f:
            reader = csv.reader(f)
            metadata_row = next(reader, [])
            header = next(reader, [])
            dict_reader = csv.DictReader(f, fieldnames=header)
            rows = list(dict_reader)

        if max_rows and max_rows > 0:
            rows = rows[:max_rows]

        total = len(rows)
        if progress_callback:
            progress_callback(stage="ai", message="AI 摘要生成中", current=0, total=total)

        enriched: List[Dict[str, str]] = []
        completed = 0
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            future_map = {executor.submit(self._summarize_row, row): row for row in rows}
            for future in as_completed(future_map):
                row = future_map[future]
                extras = future.result()
                row = dict(row)
                row.update(extras)
                enriched.append(row)
                completed += 1
                if progress_callback:
                    progress_callback(stage="ai", message="AI 摘要生成中", current=completed, total=total)

        ai_header = header + ["AI摘要", "AI要点"]
        ai_path = path.with_name(f"{path.stem}_ai{path.suffix}")
        with ai_path.open("w", newline="", encoding="utf-8-sig") as f:
            writer = csv.DictWriter(f, fieldnames=ai_header)
            meta_dict = {ai_header[i]: (metadata_row[i] if i < len(metadata_row) else "") for i in range(len(ai_header))}
            writer.writerow(meta_dict)
            header_row = {h: h for h in ai_header}
            writer.writerow(header_row)
            writer.writerows(enriched)

        if progress_callback:
            progress_callback(stage="idle", message="详情提取+AI 完成", current=total, total=total)
        return str(ai_path)
