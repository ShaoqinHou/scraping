import sqlite3
import os
import json
import concurrent.futures
import time
import threading
import collections
import re
from decimal import Decimal
from pathlib import Path

from openai import OpenAI

BASE_DIR = Path(__file__).resolve().parent
DEFAULT_DB_PATH = str(BASE_DIR / "qn_hydrogen_monitor.db")
LOG_PATH = BASE_DIR / "ai_project_extractor.log"
ALLOWED_MODELS = {
    "Qwen/Qwen2.5-7B-Instruct",
    "Qwen/Qwen2-7B-Instruct",
    "Qwen/Qwen2.5-Coder-7B-Instruct",
    "THUDM/glm-4-9b-chat",
    "THUDM/GLM-4-9B-0414",
    "deepseek-ai/DeepSeek-V3",
}


class RateLimiter:
    def __init__(self, rpm: int):
        self.rpm = rpm
        self.window = 60.0
        self.times = collections.deque()
        self.lock = threading.Lock()

    def wait(self):
        if not self.rpm or self.rpm <= 0:
            return
        while True:
            with self.lock:
                now = time.time()
                while self.times and now - self.times[0] > self.window:
                    self.times.popleft()
                if len(self.times) < self.rpm:
                    self.times.append(now)
                    return
                sleep_time = self.window - (now - self.times[0]) + 0.01
            time.sleep(sleep_time)


class AIProjectExtractor:
    def __init__(
        self,
        api_key,
        base_url="https://api.siliconflow.cn/v1",
        model="THUDM/GLM-4-9B-0414",
        models=None,
        db_path=DEFAULT_DB_PATH,
        request_timeout=180,
        rpm_limit: int | None = None,
    ):
        self.api_key = api_key
        self.base_url = base_url
        # Accept single model or list of models
        candidate_models = []
        candidate_models = []
        if models:
            if isinstance(models, str):
                # split on whitespace or commas (correct regex)
                candidate_models = [m for m in re.split(r"[,\s]+", models.strip()) if m]
            else:
                candidate_models = list(models)
        elif model:
            candidate_models = [model]

        cleaned = []
        for m in candidate_models:
            m = m.strip()
            if not m:
                continue
            if m in ALLOWED_MODELS:
                cleaned.append(m)
            else:
                try:
                    self.log_debug(f"Skip unknown model token: {m}")
                except Exception:
                    pass
        if not cleaned:
            cleaned = ["THUDM/GLM-4-9B-0414"]
        self.models = cleaned
        self._model_idx = 0
        self._model_lock = threading.Lock()
        self._bad_models = set()

        self.db_path = db_path
        self.client = OpenAI(api_key=api_key, base_url=base_url)
        self.request_timeout = request_timeout
        self.running = False
        self.rpm_limit = rpm_limit or int(os.environ.get("AI_RPM_LIMIT", "0") or 0)
        if self.rpm_limit <= 0:
            self.rpm_limit = 20
        # Per-model limiter so different models don't share the same window
        if not hasattr(self.__class__, "_rate_limiters"):
            self.__class__._rate_limiters = {}

    def log_debug(self, msg: str) -> None:
        try:
            ts = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())
            with LOG_PATH.open("a", encoding="utf-8") as f:
                f.write(f"[{ts}] {msg}\n")
        except Exception:
            pass

    def get_db_connection(self):
        conn = sqlite3.connect(self.db_path, check_same_thread=False)
        try:
            conn.execute("PRAGMA journal_mode=WAL;")
            conn.execute("PRAGMA synchronous=NORMAL;")
        except Exception:
            pass
        conn.row_factory = sqlite3.Row
        return conn

    def _next_model(self):
        with self._model_lock:
            attempts = 0
            while attempts < len(self.models):
                model = self.models[self._model_idx % len(self.models)]
                self._model_idx += 1
                attempts += 1
                if model in self._bad_models:
                    continue
                key = f"{self.base_url}::{model}"
                limiter = self.__class__._rate_limiters.setdefault(key, RateLimiter(self.rpm_limit))
                return model, limiter
        raise RuntimeError("No valid models available")

    def extract_project_info(self, title, content, model_name=None):
        raw_response = ""
        system_prompt = """
你是一位氢能项目分析专家。你的任务是从提供的文本中提取结构化的氢能项目数据。

请提取以下字段并以 JSON 格式返回。
**重要提示：所有字符串字段（如摘要、概览、进展等）必须使用简体中文。严禁使用英语。**

1.  **article_type** (文章类型): 将文章分类为以下之一：
    - "项目": 具体氢能项目（规划、建设、运营）。
    - "招标": 招标或采购公告（招标/中标）。
    - "政策": 政府政策、法规或规划。
    - "新闻": 一般行业新闻、市场分析或公司新闻。
    - "市场": 市场数据、统计或报告。
    - "其他": 其他。

2.  **project_name** (项目名称): 项目的具体名称。
    - 如果是项目列表，选择最突出的一个或标题中提到的那个。
    - 去除 "关于"、"拟"、"一期" 等前缀。
    - 优先使用《》内的名称（如果有）。

3.  **stage** (项目阶段): 项目当前阶段。从以下选项中选择**一个**：
    - "备案/获批/核准": 政府已核准/备案。
    - "开工": 开始建设。
    - "在建": 正在建设中。
    - "投运/投产": 已投入运营/投产。
    - "招标": 招标阶段。
    - "中标": 中标结果公示。
    - "签约": 签署协议。
    - "前期/规划": 规划/谅解备忘录/框架协议。
    - "未知": 不明确。严禁输出英文。

4.  **event_date** (事件日期): 事件发生的日期 (YYYY-MM-DD)。如果未找到，使用发布日期。

5.  **location** (地点): 项目地点（城市，省份）。

6.  **capacity_mw** (产能_MW): 项目产能，直接返回原文中的数值+单位（例如 "5.6GW"、"20万千瓦"），不做单位换算，保持原样。
    - 如果只出现“/年产氢”“/产量”而非装机规模，留空。
    - 不返回列表。

7.  **investment_cny** (投资_人民币): 总投资额，直接返回原文中的数值+单位（例如 "5.06亿元"、"8000万元"），不做单位换算，保持原样。
    - 不返回列表。

8.  **owner** (业主/开发商): 项目的主要开发商、投资方或业主。
    - 寻找 "建设单位"、"投资方"、"业主"。

9.  **energy_type** (能源类型): 能源来源类型。
    - 例如："绿氢"、"蓝氢"、"风光氢"、"煤制氢"。
    - 如果提到 "风电" 或 "光伏"，请包含。

10. **classic_quality** (质量评级): 评估此项目的重要性/质量 (A, B, C)。
    - **A**: 重要/大型项目（产能 > 100MW，投资 > 10亿元，或 "开工"/"投产"）。
    - **B**: 中等项目。
    - **C**: 小型或模糊的项目。

11. **province** (省份): 省份名称（例如 "内蒙古"、"山东"）。

12. **city** (城市): 城市名称。

13. **h2_output_tpy** (氢产量_吨/年): 年产氢量，统一为吨/年；“万吨/年”需 *1e4。

14. **h2_output_nm3_per_h** (氢产量_标方/小时): 统一为 Nm3/h；“万标方/小时”需 *1e4。

15. **electrolyzer_count** (电解槽数量): 电解槽台/套数，填入纯数字。

16. **co2_reduction_tpy** (碳减排_吨/年): 统一为吨/年；“万吨/年”需 *1e4。

17. **project_summary** (项目摘要): 项目的简明摘要（最多 200 字，中文）。

18. **project_overview** (项目概况): 项目范围的技术概况（建设内容）。
    - 例如："建设 500MW 风电场和 10,000 吨/年 绿氢工厂..."

19. **project_progress** (项目进展): 文本中提到的具体进展更新。
    - 例如："11月15日签署投资协议"、"开始打桩工程"。

20. **numerical_data** (数值数据汇总): 文本中发现的所有数值数据的汇总列表。
    - 格式为带符号的列表字符串（例如："- 投资: 5亿元\\n- 产能: 200MW\\n- 氢产量: 1万吨/年"）。
    - 必须包含单位。
    - Capture everything: money, capacity, output, land area, dates, counts, etc。

Return ONLY valid JSON.
"""

        user_prompt = f"""
Article Title: {title}
Article Content (truncated 1000 chars):
{content[:1000]}
"""

        try:
            response = self.client.chat.completions.create(
                model=model_name or self.models[0],
                messages=[
                    {"role": "system", "content": system_prompt},
                    {"role": "user", "content": user_prompt},
                ],
                temperature=0.1,
                max_tokens=1500,
                extra_body={"top_k": 5, "top_p": 0.85, "repetition_penalty": 1.05},
                timeout=self.request_timeout,
            )

            raw_response = response.choices[0].message.content.strip()
            parsed = raw_response
            if "```json" in parsed:
                parsed = parsed.split("```json")[1].split("```")[0].strip()
            elif "```" in parsed:
                parsed = parsed.split("```")[1].split("```")[0].strip()
            # Strip line comments like // ... which break json.loads
            parsed_no_comments = "\n".join(
                line for line in parsed.splitlines() if not line.strip().startswith("//")
            )
            return json.loads(parsed_no_comments)
        except Exception as e:
            msg = f"API error: {e}"
            print(msg)
            try:
                self.log_debug(f"{msg} | model={model_name} | raw={raw_response[:500]}")
            except Exception:
                pass
            return {"__error": msg, "__raw": raw_response[:500], "__model": model_name}

    def process_single_project_once(self, project):
        if not self.running:
            return None

        start_time = time.time()
        conn = self.get_db_connection()
        cursor = conn.cursor()
        try:
            cursor.execute("SELECT main_text FROM articles WHERE url = ?", (project["url"],))
            article_row = cursor.fetchone()
            content = article_row["main_text"] if article_row and article_row["main_text"] else project["project_summary"]

            if not content:
                return {"id": project["id"], "status": "skip", "msg": "[AI跳过: 无正文]", "project": project}

            model_name, limiter = self._next_model()
            if limiter:
                limiter.wait()

            result = self.extract_project_info(project["article_title"], content, model_name=model_name)
            # Normalize list responses: take first dict if available
            if isinstance(result, list):
                result = result[0] if result and isinstance(result[0], dict) else {"__error": "invalid_list_result"}
            if result and isinstance(result, dict) and "__error" in result:
                err = result.get("__error", "")
                mdl = result.get("__model")
                status = "fail"
                if "rate" in err.lower():
                    status = "rate_limit"
                if "does not exist" in err.lower() and mdl:
                    self._bad_models.add(mdl)
                    try:
                        self.log_debug(f"Mark bad model: {mdl} due to {err}")
                    except Exception:
                        pass
                return {
                    "id": project["id"],
                    "status": status,
                    "msg": f"[AI失败: {err} @ {mdl or ''}]",
                    "project": project,
                    "model": mdl or model_name,
                    "elapsed": time.time() - start_time,
                }
            if result:
                return {
                    "id": project["id"],
                    "status": "ok",
                    "result": result,
                    "project": project,
                    "model": model_name,
                    "elapsed": time.time() - start_time,
                }
            return {
                "id": project["id"],
                "status": "fail",
                "msg": "[AI失败]",
                "project": project,
                "model": model_name,
                "elapsed": time.time() - start_time,
            }
        except Exception as e:
            print(f"Error processing project {project.get('id')}: {e}")
            return {
                "id": project.get("id"),
                "status": "fail",
                "msg": "[AI失败]",
                "project": project,
                "model": model_name,
                "elapsed": time.time() - start_time,
            }
        finally:
            conn.close()

    def process_single_project(self, project):
        res = None
        for _ in (1, 2):
            if not self.running:
                return None
            res = self.process_single_project_once(project)
            if res and res.get("status") in ("ok", "skip", "fail"):
                return res
        return res

    def run(self, max_projects=10, max_workers=5, progress_callback=None):
        self.running = True
        conn = self.get_db_connection()
        cursor = conn.cursor()
        # Ensure ai_model column exists
        try:
            cur = conn.execute("PRAGMA table_info(projects_classic)")
            cols = {row[1] for row in cur.fetchall()}
            if "ai_model" not in cols:
                conn.execute("ALTER TABLE projects_classic ADD COLUMN ai_model TEXT")
                conn.commit()
        except Exception:
            pass
        total = 0
        completed = 0
        max_workers = max(1, int(max_workers))
        max_projects = max(0, int(max_projects))

        writer_conn = self.get_db_connection()
        writer_cur = writer_conn.cursor()
        hit_rate_limit = False

        def normalize_number(val, field):
            if val is None or val == "":
                return None
            # keep decimals stable for money
            def to_decimal(x):
                try:
                    return Decimal(str(x))
                except Exception:
                    return None

            if isinstance(val, (int, float, Decimal)):
                d = to_decimal(val)
                return float(d) if d is not None else None
            if isinstance(val, str):
                s = val.strip().replace(",", "")
                d = to_decimal(s)
                if d is None:
                    m = re.search(r"([0-9]+(?:\.[0-9]+)?)", s)
                    d = to_decimal(m.group(1)) if m else None
                if d is None:
                    return None
                if "亿" in s:
                    return float(d * Decimal("1e8"))
                if "万元" in s or "万人民币" in s:
                    return float(d * Decimal("1e4"))
                if "万千瓦" in s:
                    return float(d * Decimal("10"))  # 万千瓦 -> MW
                if "千瓦" in s:
                    return float(d / Decimal("1000"))
                if "gw" in s.lower():
                    return float(d * Decimal("1000"))
                return float(d)
            return None

        def normalize_value(v):
            if isinstance(v, (list, dict)):
                try:
                    return json.dumps(v, ensure_ascii=False)
                except Exception:
                    return str(v)
            return v

        def write_result(project_id, payload):
            status = payload.get("status")
            proj = payload.get("project", {}) or {}
            mdl = payload.get("model")
            # Load current row to avoid wiping fields when AI returns empty
            writer_cur.execute(
                """
                SELECT project_name, stage, event_date, location, capacity_mw, investment_cny,
                       owner, energy_type, classic_quality, province, city,
                       h2_output_tpy, h2_output_nm3_per_h, electrolyzer_count,
                       co2_reduction_tpy, project_summary, project_overview, project_progress,
                       article_type, numerical_data, ai_model
                FROM projects_classic WHERE id = ?
                """,
                (project_id,),
            )
            current = writer_cur.fetchone()
            if not current:
                return
            current = dict(current)

            if status == "ok":
                res = payload.get("result", {}) or {}
                # If result is empty, treat as fail to avoid wiping with None
                if not any(res.values()):
                    status = "fail"
                else:
                    # Overwrite existing fields with AI result (clear if missing)
                    merged = {}
                    fields = [
                        "project_name",
                        "stage",
                        "event_date",
                        "location",
                        "capacity_mw",
                        "investment_cny",
                        "owner",
                        "energy_type",
                        "classic_quality",
                        "province",
                        "city",
                        "h2_output_tpy",
                        "h2_output_nm3_per_h",
                        "electrolyzer_count",
                        "co2_reduction_tpy",
                        "project_summary",
                        "project_overview",
                        "project_progress",
                        "article_type",
                        "numerical_data",
                    ]
                    for k in fields:
                        new_val = res.get(k)
                        if k in ("capacity_mw", "investment_cny", "h2_output_tpy", "h2_output_nm3_per_h", "co2_reduction_tpy"):
                            norm = normalize_number(new_val, k)
                            merged[k] = norm if norm is not None else None
                        else:
                            merged[k] = normalize_value(new_val) if (new_val not in (None, "")) else None
                    if merged.get("stage") in ("Unknown", "unknown", "UNKNOWN"):
                        merged["stage"] = "未知"
                    writer_cur.execute(
                        """
                        UPDATE projects_classic 
                        SET project_name = ?, stage = ?, event_date = ?, location = ?,
                            capacity_mw = ?, investment_cny = ?, owner = ?, energy_type = ?,
                            classic_quality = ?, province = ?, city = ?,
                            h2_output_tpy = ?, h2_output_nm3_per_h = ?, electrolyzer_count = ?,
                            co2_reduction_tpy = ?, project_summary = ?,
                            project_overview = ?, project_progress = ?,
                            article_type = ?, numerical_data = ?, ai_model = ?,
                            is_ai_improved = 1
                        WHERE id = ?
                        """,
                        (
                            merged.get("project_name"),
                            merged.get("stage"),
                            merged.get("event_date"),
                            merged.get("location"),
                            merged.get("capacity_mw"),
                            merged.get("investment_cny"),
                            merged.get("owner"),
                            merged.get("energy_type"),
                            merged.get("classic_quality"),
                            merged.get("province"),
                            merged.get("city"),
                            merged.get("h2_output_tpy"),
                            merged.get("h2_output_nm3_per_h"),
                            merged.get("electrolyzer_count"),
                            merged.get("co2_reduction_tpy"),
                            merged.get("project_summary"),
                            merged.get("project_overview"),
                            merged.get("project_progress"),
                            merged.get("article_type"),
                            merged.get("numerical_data"),
                            mdl,
                            project_id,
                        ),
                    )
                    writer_conn.commit()
                    try:
                        self.log_debug(f"OK model={mdl} id={project_id} elapsed={payload.get('elapsed')} title={proj.get('article_title','')}")
                    except Exception:
                        pass
                    return

            # Failure/skip: mark as not improved so it can retry next run.
            # Avoid duplicating the marker.
            note = payload.get("msg") or "[AI失败]"
            writer_cur.execute(
                "SELECT project_progress FROM projects_classic WHERE id = ?", (project_id,)
            )
            row = writer_cur.fetchone()
            existing = row[0] if row else ""
            if note not in (existing or ""):
                new_progress = (existing or "") + note
            else:
                new_progress = existing
            writer_cur.execute(
                """
                UPDATE projects_classic
                SET is_ai_improved = 0,
                    project_progress = ?,
                    ai_model = ?
                WHERE id = ?
                """,
                (new_progress, mdl or payload.get("model"), project_id),
            )
            writer_conn.commit()
            try:
                self.log_debug(f"FAIL model={mdl or payload.get('model')} id={project_id} note={note} elapsed={payload.get('elapsed')}")
            except Exception:
                pass

        try:
            cursor.execute("UPDATE projects_classic SET is_ai_improved = 0 WHERE is_ai_improved = 9")
            conn.commit()

            cursor.execute(
                """
                SELECT id, article_title, project_summary, url 
                FROM projects_classic 
                WHERE is_ai_improved = 0 OR is_ai_improved IS NULL
                ORDER BY id ASC
                LIMIT ?
                """,
                (max_projects,),
            )
            projects = [dict(row) for row in cursor.fetchall()]

            if projects:
                ids = [p["id"] for p in projects]
                cursor.execute(
                    f"UPDATE projects_classic SET is_ai_improved = 9 WHERE id IN ({','.join(['?']*len(ids))})",
                    ids,
                )
                conn.commit()

            total = len(projects)
            if progress_callback:
                progress_callback(stage="running", message=f"发现 {total} 个待处理项目", current=0, total=total)

            if total == 0:
                return

            with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as executor:
                futures = {executor.submit(self.process_single_project, p): p for p in projects}

                for future in concurrent.futures.as_completed(futures):
                    if not self.running:
                        executor.shutdown(wait=False, cancel_futures=True)
                        break

                    payload = None
                    try:
                        payload = future.result()
                    except Exception:
                        payload = {"status": "fail", "msg": "[AI失败]", "project": None, "id": None}

                    completed += 1

                    if payload and payload.get("id") is not None:
                        write_result(payload["id"], payload)

                    if progress_callback:
                        title = ""
                        if payload and payload.get("project"):
                            title = payload["project"].get("article_title", "")
                        progress_callback(
                            stage="running",
                            message=f"已处理: {title}",
                            current=completed,
                            total=total,
                        )
                    if payload and payload.get("status") == "rate_limit":
                        hit_rate_limit = True
                        # Stop submitting more; cancel remaining futures
                        executor.shutdown(wait=False, cancel_futures=True)
                        break

        finally:
            try:
                cursor.execute(
                    "UPDATE projects_classic SET is_ai_improved = CASE WHEN is_ai_improved=9 THEN 0 ELSE is_ai_improved END"
                )
                conn.commit()
            except Exception:
                pass
            conn.close()
            try:
                writer_conn.close()
            except Exception:
                pass
            self.running = False
            if progress_callback:
                if hit_rate_limit:
                    progress_callback(
                        stage="idle",
                        message="AI 提取出错: 触发限速，已暂停，剩余保留待处理",
                        current=completed,
                        total=total,
                    )
                else:
                    progress_callback(stage="idle", message="AI 提取完成", current=completed, total=total)


def main():
    api_key = os.environ.get("SILICONFLOW_API_KEY")
    if not api_key:
        print("Error: SILICONFLOW_API_KEY environment variable not set.")
        return
    extractor = AIProjectExtractor(api_key)
    extractor.run()


if __name__ == "__main__":
    main()
