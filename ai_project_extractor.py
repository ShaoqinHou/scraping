
import sqlite3
import os
import json
import time
import concurrent.futures
from openai import OpenAI
from pathlib import Path

BASE_DIR = Path(__file__).resolve().parent
DEFAULT_DB_PATH = str(BASE_DIR / "qn_hydrogen_monitor.db")

class AIProjectExtractor:
    def __init__(self, api_key, base_url="https://api.siliconflow.cn/v1", model="THUDM/GLM-4-9B-0414", db_path=DEFAULT_DB_PATH, request_timeout=20):
        self.api_key = api_key
        self.base_url = base_url
        self.model = model
        self.db_path = db_path
        self.client = OpenAI(api_key=api_key, base_url=base_url)
        # Per-request timeout to avoid hanging on a single article
        self.request_timeout = request_timeout
        self.running = False

    def get_db_connection(self):
        conn = sqlite3.connect(self.db_path)
        conn.row_factory = sqlite3.Row
        return conn

    def extract_project_info(self, title, content):
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
    - "Unknown": 不明确。

4.  **event_date** (事件日期): 事件发生的日期 (YYYY-MM-DD)。如果未找到，使用发布日期。

5.  **location** (地点): 项目地点（城市，省份）。

6.  **capacity_mw** (产能_MW): 项目产能，必须统一转换为 MW（兆瓦）后的纯数字。
    - GW → *1000；万千瓦 → *10；千瓦(kW) → /1000；瓦(W) → /1,000,000。
    - 如果只出现“/年产氢”“/产量”而非装机规模，留空。
    - **严禁**保留原单位或“万千瓦”字样。

7.  **investment_cny** (投资_人民币): 总投资额，必须统一为“元”后的纯数字。
    - 亿元 → *1e8；万元 → *1e4；百万/千/百 → 相应换算。
    - **严禁**保留“亿元/万元”等单位词。

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
    - 格式为带符号的列表字符串（例如："- 投资: 5亿元\n- 产能: 200MW\n- 氢产量: 1万吨/年"）。
    - 必须包含单位。
    - Capture everything: money, capacity, output, land area, dates, counts, etc.

Return ONLY valid JSON.
"""
        
        user_prompt = f"""
Article Title: {title}
Article Content:
{content[:3000]}
"""
        
        try:
            response = self.client.chat.completions.create(
                model=self.model,
                messages=[
                    {"role": "system", "content": system_prompt},
                    {"role": "user", "content": user_prompt}
                ],
                temperature=0.1,
                max_tokens=1500,
                extra_body={"top_k": 5, "top_p": 0.85, "repetition_penalty": 1.05},
                timeout=self.request_timeout,
            )
            
            content = response.choices[0].message.content.strip()
            # Remove markdown code blocks if present
            if "```json" in content:
                content = content.split("```json")[1].split("```")[0].strip()
            elif "```" in content:
                content = content.split("```")[1].split("```")[0].strip()
            
            return json.loads(content)
        except Exception as e:
            print(f"Error calling API: {e}")
            return None

    def process_single_project(self, project):
        if not self.running:
            return None
            
        conn = self.get_db_connection()
        cursor = conn.cursor()
        try:
            cursor.execute("SELECT main_text FROM articles WHERE url = ?", (project['url'],))
            article_row = cursor.fetchone()
            content = article_row['main_text'] if article_row and article_row['main_text'] else project['project_summary']
            
            if not content:
                cursor.execute("UPDATE projects_classic SET is_ai_improved = 2, project_progress = COALESCE(project_progress, '') || '[AI跳过: 无正文]' WHERE id = ?", (project['id'],))
                conn.commit()
                return None
                
            result = self.extract_project_info(project['article_title'], content)
            
            if result:
                # Update all fields
                cursor.execute("""
                    UPDATE projects_classic 
                    SET project_name = ?, stage = ?, event_date = ?, location = ?,
                        capacity_mw = ?, investment_cny = ?, owner = ?, energy_type = ?,
                        classic_quality = ?, province = ?, city = ?,
                        h2_output_tpy = ?, h2_output_nm3_per_h = ?, electrolyzer_count = ?,
                        co2_reduction_tpy = ?, project_summary = ?,
                        project_overview = ?, project_progress = ?,
                        article_type = ?, numerical_data = ?,
                        is_ai_improved = 1
                    WHERE id = ?
                """, (
                    result.get('project_name'),
                    result.get('stage'),
                    result.get('event_date'),
                    result.get('location'),
                    result.get('capacity_mw'),
                    result.get('investment_cny'),
                    result.get('owner'),
                    result.get('energy_type'),
                    result.get('classic_quality'),
                    result.get('province'),
                    result.get('city'),
                    result.get('h2_output_tpy'),
                    result.get('h2_output_nm3_per_h'),
                    result.get('electrolyzer_count'),
                    result.get('co2_reduction_tpy'),
                    result.get('project_summary'),
                    result.get('project_overview'),
                    result.get('project_progress'),
                    result.get('article_type'),
                    result.get('numerical_data'),
                    project['id']
                ))
                conn.commit()
                return True
            return False
        except Exception as e:
            print(f"Error processing project {project['id']}: {e}")
            try:
                cursor.execute("UPDATE projects_classic SET is_ai_improved = 2, project_progress = COALESCE(project_progress, '') || '[AI失败]' WHERE id = ?", (project['id'],))
                conn.commit()
            except Exception:
                pass
            return False
        finally:
            conn.close()

    def run(self, max_projects=10, max_workers=5, progress_callback=None):
        self.running = True
        conn = self.get_db_connection()
        cursor = conn.cursor()
        total = 0
        completed = 0

        try:
            # Only pick rows not yet improved (0/NULL). Skipped rows (2) are not retried.
            cursor.execute("""
                SELECT id, article_title, project_summary, url 
                FROM projects_classic 
                WHERE is_ai_improved = 0 OR is_ai_improved IS NULL
                ORDER BY id ASC
                LIMIT ?
            """, (max_projects,))
            projects = [dict(row) for row in cursor.fetchall()]
            
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
                    
                    project = futures[future]
                    completed += 1
                    if progress_callback:
                        progress_callback(stage="running", message=f"已处理: {project['article_title']}", current=completed, total=total)
                
        finally:
            conn.close()
            self.running = False
            if progress_callback:
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
