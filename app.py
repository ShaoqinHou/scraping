import asyncio
import pathlib
import sqlite3
import threading
import io
import csv as csv_std
from typing import List

from flask import Flask, jsonify, render_template, request, send_file

from captcha_manager import CaptchaManager, start_standalone_captcha_server
from detailed_project_extractor import DetailedProjectExtractor
from final_improved_collector_integrated import ProjectCollector
from qn_hydrogen_monitor import QNHydrogenMonitor, get_default_hydrogen_channels
from classic_project_extractor import ClassicProjectExtractor
from hydrogen_article_text_fetcher import fetch_missing_article_texts
from ai_project_extractor import AIProjectExtractor
from ai_detail_summarizer import AIDetailSummarizer
import json
import os

BASE_DIR = pathlib.Path(__file__).resolve().parent
DB_PATH = str(BASE_DIR / "qn_hydrogen_monitor.db")

app = Flask(__name__, template_folder="templates", static_folder="static")
captcha_manager = CaptchaManager()
start_standalone_captcha_server(captcha_manager)
app.register_blueprint(captcha_manager.create_blueprint(prefix="captcha"), url_prefix="/captcha")

collector_task = None
extractor_task = None
collector_status = {"stage": "idle", "message": "准备就绪", "current": 0, "total": 0}
extractor_status = {"stage": "idle", "message": "待机", "current": 0, "total": 0}
hydrogen_monitor_task = None
hydrogen_monitor_status = {
    "stage": "idle",
    "message": "待机",
    "current": 0,
    "total": 0,
    "channel_id": None,
    "channel_label": None,
    "page": 0,
    "new_in_page": 0,
    "new_in_run": 0,
    "total_in_db": 0,
}
hydrogen_channels = get_default_hydrogen_channels()
classic_extractor_task = None
classic_extractor_status = {
    "stage": "idle",
    "message": "待机",
    "current": 0,
    "total": 0,
}
ai_extractor_task = None
ai_extractor_status = {
    "stage": "idle",
    "message": "待机",
    "current": 0,
    "total": 0,
}

SECRETS_FILE = "secrets.json"

def load_secrets():
    if os.path.exists(SECRETS_FILE):
        try:
            with open(SECRETS_FILE, 'r') as f:
                return json.load(f)
        except:
            return {}
    return {}

def save_secrets(secrets):
    with open(SECRETS_FILE, 'w') as f:
        json.dump(secrets, f)



def get_db_connection():
    def _open():
        conn = sqlite3.connect(DB_PATH)
        conn.row_factory = sqlite3.Row
        return conn

    try:
        return _open()
    except sqlite3.DatabaseError as exc:
        if "file is not a database" in str(exc):
            backup = f"{DB_PATH}.corrupt"
            try:
                os.replace(DB_PATH, backup)
            except OSError:
                pass
            # Recreate a fresh DB with schema
            extractor = ClassicProjectExtractor(db_path=DB_PATH)
            return extractor._connect()
        raise


def _run_async(coro):
    asyncio.run(coro)


def collector_progress(**info):
    collector_status.update(info)


def extractor_progress(**info):
    extractor_status.update(info)


def hydrogen_progress(**info):
    hydrogen_monitor_status.update(info)


def classic_progress(**info):
    classic_extractor_status.update(info)


@app.route("/")
def index():
    return render_template("hydrogen_dashboard.html")


@app.route("/collector")
def collector_dashboard():
    return render_template("collector_dashboard.html")


@app.route("/api/collector", methods=["POST"])
def start_collector():
    global collector_task
    if collector_task and collector_task.is_alive():
        return jsonify({"ok": False, "message": "已有采集任务运行中"}), 409

    data = request.get_json() or {}
    args = {
        "retry_failed_only": data.get("retry", False),
        "max_pages": data.get("max_pages"),
        "headless": data.get("headless", False),
        "max_open_tabs": data.get("max_open_tabs"),
        "pages_per_second": data.get("pages_per_second"),
    }
    collector = ProjectCollector(progress_callback=collector_progress, captcha_manager=captcha_manager, **args)

    async def task():
        if not await collector.setup_browser():
            return
        if not await collector.get_captcha_token_from_browser():
            return
        await collector.collect_all_projects()
        collector.save_data()
        collector.save_failed_pages()
        await collector.close()
        collector_progress(stage="idle", message="采集完成", current=collector.pages_completed, total=collector.processed_pages)
        collector_progress(stage="idle", message="采集完成", current=collector.pages_completed, total=collector.processed_pages)

    collector_status.update({"stage": "running", "message": "采集中", "current": 0, "total": 0})
    collector_task = threading.Thread(target=_run_async, args=(task(),), daemon=True)
    collector_task.start()
    return jsonify({"ok": True})


@app.route("/api/extractor", methods=["POST"])
def start_extractor():
    global extractor_task
    if extractor_task and extractor_task.is_alive():
        return jsonify({"ok": False, "message": "已有提取任务运行中"}), 409

    data = request.get_json() or {}
    csv_file = data.get("csv_file", "inner_mongolia_projects.csv")
    keywords = data.get("keywords") or []
    cbsnums = data.get("cbsnums")
    extractor = DetailedProjectExtractor(
        csv_file,
        keywords,
        max_projects=data.get("max_projects"),
        headless=data.get("headless", False),
        max_concurrent=data.get("max_concurrent"),
        cbsnums=cbsnums,
        captcha_manager=captcha_manager,
        progress_callback=extractor_progress,
        max_open_tabs=data.get("max_open_tabs"),
        pages_per_second=data.get("pages_per_second"),
    )
    use_ai = bool(data.get("use_ai"))
    ai_max_rows_raw = data.get("ai_max_rows")
    try:
        ai_max_rows = int(ai_max_rows_raw) if ai_max_rows_raw is not None else None
        if ai_max_rows is not None and ai_max_rows <= 0:
            ai_max_rows = None
    except (TypeError, ValueError):
        ai_max_rows = None
    ai_max_workers = data.get("ai_max_workers") or 4
    try:
        ai_max_workers = int(ai_max_workers)
        if ai_max_workers <= 0:
            ai_max_workers = 4
    except (TypeError, ValueError):
        ai_max_workers = 4
    ai_model = data.get("ai_model")

    async def task():
        extractor.load_and_filter_csv()
        if not extractor.filtered_projects:
            extractor_progress(stage="idle", message="无匹配项目", current=0, total=0)
            return
        await extractor.setup_browser()
        if not await extractor.get_captcha_token():
            return
        await extractor.extract_all_projects()
        output_file = extractor.save_extracted_data()
        await extractor.close()
        if use_ai and output_file:
            secrets = load_secrets()
            api_key = secrets.get("SILICONFLOW_API_KEY")
            model = ai_model or secrets.get("SILICONFLOW_MODEL", "deepseek-ai/DeepSeek-V3")
            if not api_key:
                extractor_progress(stage="idle", message="提取完成（AI Key 未配置）", current=len(extractor.filtered_projects), total=len(extractor.filtered_projects))
                return
            summarizer = AIDetailSummarizer(api_key=api_key, model=model)
            try:
                def _cb(**kw):
                    extractor_progress(**kw)
                # AI 摘要生成
                summarizer.run(output_file, max_rows=ai_max_rows, max_workers=int(ai_max_workers), progress_callback=_cb)
            except Exception as e:
                extractor_progress(stage="idle", message=f"提取完成（AI 失败: {e}）", current=len(extractor.filtered_projects), total=len(extractor.filtered_projects))
                return
        extractor_progress(stage="idle", message="提取完成", current=len(extractor.filtered_projects), total=len(extractor.filtered_projects))

    extractor_status.update({"stage": "running", "message": "提取中", "current": 0, "total": 0})
    extractor_task = threading.Thread(target=_run_async, args=(task(),), daemon=True)
    extractor_task.start()
    return jsonify({"ok": True})


@app.route("/api/status")
def get_status():
    return jsonify(
        {
            "collector": collector_status,
            "extractor": extractor_status,
            "hydrogen_monitor": hydrogen_monitor_status,
            "classic_extractor": classic_extractor_status,
        }
    )


@app.route("/api/hydrogen/config", methods=["GET", "POST"])
def hydrogen_config():
    global hydrogen_channels
    if request.method == "GET":
        return jsonify(
            {
                "channels": [ch.to_dict() for ch in hydrogen_channels],
            }
        )

    data = request.get_json() or {}
    updates = {c.get("id"): c for c in data.get("channels") or [] if c.get("id")}
    new_channels = []
    for ch in hydrogen_channels:
        payload = updates.get(ch.id)
        if payload is not None:
            enabled = payload.get("enabled")
            if enabled is not None:
                ch.enabled = bool(enabled)
        new_channels.append(ch)
    hydrogen_channels = new_channels
    return jsonify({"ok": True, "channels": [c.to_dict() for c in hydrogen_channels]})


@app.route("/api/hydrogen/start", methods=["POST"])
def start_hydrogen_monitor():
    global hydrogen_monitor_task
    if hydrogen_monitor_task and hydrogen_monitor_task.is_alive():
        return jsonify({"ok": False, "message": "已有氢能监控任务运行中"}), 409

    data = request.get_json() or {}
    max_new_articles = data.get("max_new_articles")
    try:
        if max_new_articles is not None:
            max_new_articles = int(max_new_articles)
            if max_new_articles <= 0:
                max_new_articles = None
    except (TypeError, ValueError):
        max_new_articles = None

    max_pages_per_channel = data.get("max_pages_per_channel")
    try:
        if max_pages_per_channel is not None:
            max_pages_per_channel = int(max_pages_per_channel)
            if max_pages_per_channel <= 0:
                max_pages_per_channel = None
    except (TypeError, ValueError):
        max_pages_per_channel = None

    enabled_channels = [ch for ch in hydrogen_channels if ch.enabled]
    if not enabled_channels:
        return jsonify({"ok": False, "message": "当前没有启用的氢能频道"}), 400

    monitor = QNHydrogenMonitor(
        channels=enabled_channels,
        progress_callback=hydrogen_progress,
    )

    def task():
        try:
            hydrogen_progress(
                stage="running",
                message="氢能频道监控启动",
                current=0,
                total=max_new_articles or 0,
                channel_id=None,
                channel_label=None,
                page=0,
                new_in_page=0,
                new_in_run=0,
                total_in_db=0,
            )
            monitor.run_once(
                max_new_articles=max_new_articles,
                max_pages_per_channel=max_pages_per_channel,
            )
        except Exception as e:
            hydrogen_progress(stage="idle", message=f"监控任务出错: {e}")
        finally:
            hydrogen_progress(stage="idle", message="监控任务完成")

    hydrogen_monitor_task = threading.Thread(target=task, daemon=True)
    hydrogen_monitor_task.start()
    return jsonify({"ok": True})


@app.route("/api/hydrogen/status")
def get_hydrogen_status():
    return jsonify(hydrogen_monitor_status)


@app.route("/api/hydrogen/articles", methods=["POST"])
def get_hydrogen_articles():
    payload = request.get_json() or {}
    page = payload.get("page") or 1
    page_size = payload.get("page_size") or 20
    channel_ids = payload.get("channel_ids") or None
    try:
        page = int(page)
    except (TypeError, ValueError):
        page = 1
    if page < 1:
        page = 1
    try:
        page_size = int(page_size)
    except (TypeError, ValueError):
        page_size = 20
    if page_size <= 0:
        page_size = 20
    offset = (page - 1) * page_size

    try:
        conn = sqlite3.connect(DB_PATH)
        conn.row_factory = sqlite3.Row
        cur = conn.cursor()
        where = ""
        params = []
        if channel_ids:
            if isinstance(channel_ids, str):
                channel_ids = [channel_ids]
            channel_ids = [str(c) for c in channel_ids if c]
            if channel_ids:
                placeholders = ",".join("?" for _ in channel_ids)
                where = f" WHERE channel_id IN ({placeholders})"
                params.extend(channel_ids)
        cur.execute(f"SELECT COUNT(*) AS c FROM articles{where}", params)
        row = cur.fetchone()
        total = int(row["c"]) if row and row["c"] is not None else 0
        cur.execute(
            f"""
            SELECT url, channel_id, channel_label, title, published_at
            FROM articles
            {where}
            ORDER BY published_at DESC, created_at DESC
            LIMIT ? OFFSET ?
            """,
            params + [page_size, offset],
        )
        items = [
            {
                "url": r["url"],
                "channel_id": r["channel_id"],
                "channel_label": r["channel_label"],
                "title": r["title"],
                "published_at": r["published_at"],
            }
            for r in cur.fetchall()
        ]
    except sqlite3.Error:
        total = 0
        items = []
    finally:
        try:
            conn.close()
        except Exception:
            pass

    return jsonify({"total": total, "items": items, "page": page, "page_size": page_size})


@app.route("/api/hydrogen/articles/reset", methods=["POST"])
def reset_hydrogen_articles():
    conn = None
    try:
        conn = sqlite3.connect(DB_PATH)
        cur = conn.cursor()
        cur.execute("DELETE FROM articles")
        conn.commit()
    except sqlite3.Error as exc:
        if conn is not None:
            conn.rollback()
        return jsonify({"ok": False, "error": str(exc)}), 500
    finally:
        if conn is not None:
            conn.close()

    hydrogen_monitor_status.update(
        {
            "stage": "idle",
            "message": "文章记录已重置",
            "current": 0,
            "total": 0,
            "channel_id": None,
            "channel_label": None,
            "page": 0,
            "new_in_page": 0,
            "new_in_run": 0,
            "total_in_db": 0,
        }
    )
    return jsonify({"ok": True})


@app.route("/api/hydrogen/projects/classic/run", methods=["POST"])
def run_classic_extractor():
    global classic_extractor_task
    if classic_extractor_task and classic_extractor_task.is_alive():
        return jsonify({"ok": False, "message": "已有经典项目提取任务运行中"}), 409

    payload = request.get_json() or {}
    max_articles = payload.get("max_articles")
    score_threshold = payload.get("score_threshold", 2)
    max_workers = payload.get("max_workers", 10)
    headless = payload.get("headless", True)
    try:
        if max_articles is not None:
            max_articles = int(max_articles)
            if max_articles <= 0:
                max_articles = None
    except (TypeError, ValueError):
        max_articles = None
    try:
        score_threshold = int(score_threshold)
    except (TypeError, ValueError):
        score_threshold = 2
    try:
        max_workers = int(max_workers)
    except (TypeError, ValueError):
        max_workers = 10
    if max_workers < 1:
        max_workers = 1
    if max_workers > 32:
        max_workers = 32

    extractor = ClassicProjectExtractor(
        db_path="qn_hydrogen_monitor.db",
        progress_callback=classic_progress,
    )

    def task():
        try:
            # Reset status including error url
            classic_progress(stage="running", message="准备开始...", current=0, total=0, last_error_url=None)
            
            # 先通过 Playwright 抓取缺失或占位的正文，再执行经典规则提取
            fetch_missing_article_texts(
                db_path="qn_hydrogen_monitor.db",
                headless=bool(headless),
                max_concurrent=max_workers,
                max_articles=max_articles,
                progress_callback=classic_progress,
            )
            classic_progress(stage="running", message="经典规则提取启动", current=0, total=0)
            extractor.run(
                max_articles=max_articles,
                score_threshold=score_threshold,
                max_workers=max_workers,
            )
        except Exception as e:
            classic_progress(stage="idle", message=f"经典提取出错: {e}")
        finally:
            if classic_extractor_status.get("stage") != "idle":
                classic_progress(stage="idle", message="经典提取结束")

    classic_extractor_task = threading.Thread(target=task, daemon=True)
    classic_extractor_task.start()
    return jsonify({"ok": True})


@app.route("/api/hydrogen/projects/classic/status")
def get_classic_extractor_status():
    try:
        conn = get_db_connection()
        cur = conn.cursor()
        cur.execute("SELECT COUNT(*) FROM projects_classic")
        total = cur.fetchone()[0]
        cur.execute("SELECT COUNT(*) FROM projects_classic WHERE is_ai_improved = 1")
        ai_done = cur.fetchone()[0]
    except Exception:
        total = 0
        ai_done = 0
    return jsonify({**classic_extractor_status, "classic_total": total, "ai_done": ai_done})


@app.route('/api/hydrogen/projects/classic/list', methods=['GET', 'POST'])
def list_classic_projects():
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        
        # Get parameters from JSON body (POST) or query string (GET)
        data = {}
        if request.method == 'POST' and request.is_json:
            data = request.get_json()
        else:
            data = request.args

        def get_param(key, default=''):
            return str(data.get(key, default)).strip()

        # Get filter parameters
        f_type = get_param('type').lower()
        f_overview = get_param('overview').lower()
        f_progress = get_param('progress').lower()
        f_date_from = get_param('date_from')
        f_date_to = get_param('date_to')
        f_channel = get_param('channel').lower()
        f_title = get_param('title').lower()
        f_name = get_param('name').lower()
        f_stage = get_param('stage').lower()
        f_event_from = get_param('event_from')
        f_event_to = get_param('event_to')
        f_location = get_param('location').lower()
        f_province = get_param('province').lower()
        f_city = get_param('city').lower()
        f_owner = get_param('owner').lower()
        f_product = get_param('product').lower()
        f_energy = get_param('energy').lower()
        f_quality = get_param('quality').upper()
        f_quality = get_param('quality').upper()
        f_link = get_param('link').lower()
        f_article_type = get_param('search_article_type').lower()
        f_numerical = get_param('search_numerical').lower()
        
        # Numeric filters
        def get_float(key):
            v = data.get(key)
            if v is not None and v != '':
                try:
                    return float(v)
                except ValueError:
                    return None
            return None

        f_cap_min = get_float('cap_min')
        f_cap_max = get_float('cap_max')
        f_inv_min = get_float('inv_min')
        f_inv_max = get_float('inv_max')
        f_h2tpy_min = get_float('h2tpy_min')
        f_h2tpy_max = get_float('h2tpy_max')
        f_h2nm3_min = get_float('h2nm3_min')
        f_h2nm3_max = get_float('h2nm3_max')
        f_elec_min = get_float('elec_min')
        f_elec_max = get_float('elec_max')
        f_co2_min = get_float('co2_min')
        f_co2_max = get_float('co2_max')

        # Build query
        where_clauses = []
        params = []

        if f_type:
            if f_type == 'single':
                where_clauses.append("source_type = 'single'")
            elif f_type == 'list':
                where_clauses.append("source_type IN ('list', 'list_item')")
        
        if f_overview:
            where_clauses.append("(project_overview LIKE ? OR project_summary LIKE ?)")
            params.extend([f'%{f_overview}%', f'%{f_overview}%'])
        
        if f_progress:
            where_clauses.append("project_progress LIKE ?")
            params.append(f'%{f_progress}%')

        if f_date_from:
            where_clauses.append("published_at >= ?")
            params.append(f_date_from)
        if f_date_to:
            where_clauses.append("published_at <= ?")
            params.append(f_date_to)

        if f_channel:
            where_clauses.append("channel_label LIKE ?")
            params.append(f'%{f_channel}%')
        
        if f_title:
            where_clauses.append("article_title LIKE ?")
            params.append(f'%{f_title}%')
            
        if f_name:
            where_clauses.append("project_name LIKE ?")
            params.append(f'%{f_name}%')
            
        if f_stage:
            where_clauses.append("stage LIKE ?")
            params.append(f'%{f_stage}%')
            
        if f_event_from:
            where_clauses.append("event_date >= ?")
            params.append(f_event_from)
        if f_event_to:
            where_clauses.append("event_date <= ?")
            params.append(f_event_to)
            
        if f_location:
            where_clauses.append("location LIKE ?")
            params.append(f'%{f_location}%')
            
        if f_province:
            where_clauses.append("province LIKE ?")
            params.append(f'%{f_province}%')
            
        if f_city:
            where_clauses.append("city LIKE ?")
            params.append(f'%{f_city}%')
            
        if f_owner:
            where_clauses.append("owner LIKE ?")
            params.append(f'%{f_owner}%')
            
        if f_product:
            where_clauses.append("product_category LIKE ?")
            params.append(f'%{f_product}%')
            
        if f_energy:
            where_clauses.append("energy_type LIKE ?")
            params.append(f'%{f_energy}%')
            
        if f_link:
            where_clauses.append("url LIKE ?")
            params.append(f'%{f_link}%')
            
        if f_quality:
            where_clauses.append("classic_quality = ?")
            params.append(f_quality)

        if f_article_type:
            where_clauses.append("article_type LIKE ?")
            params.append(f'%{f_article_type}%')
            
        if f_numerical:
            where_clauses.append("numerical_data LIKE ?")
            params.append(f'%{f_numerical}%')

        # Numeric ranges
        if f_cap_min is not None:
            where_clauses.append("capacity_mw >= ?")
            params.append(f_cap_min)
        if f_cap_max is not None:
            where_clauses.append("capacity_mw <= ?")
            params.append(f_cap_max)
            
        if f_inv_min is not None:
            where_clauses.append("investment_cny >= ?")
            params.append(f_inv_min)
        if f_inv_max is not None:
            where_clauses.append("investment_cny <= ?")
            params.append(f_inv_max)
            
        if f_h2tpy_min is not None:
            where_clauses.append("h2_output_tpy >= ?")
            params.append(f_h2tpy_min)
        if f_h2tpy_max is not None:
            where_clauses.append("h2_output_tpy <= ?")
            params.append(f_h2tpy_max)
            
        if f_h2nm3_min is not None:
            where_clauses.append("h2_output_nm3_per_h >= ?")
            params.append(f_h2nm3_min)
        if f_h2nm3_max is not None:
            where_clauses.append("h2_output_nm3_per_h <= ?")
            params.append(f_h2nm3_max)
            
        if f_elec_min is not None:
            where_clauses.append("electrolyzer_count >= ?")
            params.append(f_elec_min)
        if f_elec_max is not None:
            where_clauses.append("electrolyzer_count <= ?")
            params.append(f_elec_max)
            
        if f_co2_min is not None:
            where_clauses.append("co2_reduction_tpy >= ?")
            params.append(f_co2_min)
        if f_co2_max is not None:
            where_clauses.append("co2_reduction_tpy <= ?")
            params.append(f_co2_max)

        where_sql = "WHERE " + " AND ".join(where_clauses) if where_clauses else ""

        # Pagination
        try:
            page = int(get_param('page', 1))
        except ValueError:
            page = 1
        try:
            page_size = int(get_param('page_size', 20))
        except ValueError:
            page_size = 20
            
        offset = (page - 1) * page_size

        # Count total
        cursor.execute(f"SELECT COUNT(*) FROM projects_classic {where_sql}", params)
        total = cursor.fetchone()[0]

        # Query with limit
        cursor.execute(
            f"""
                SELECT url, channel_id, channel_label, article_title, published_at,
                       project_name, stage, event_date, location,
                       capacity_mw, investment_cny, product_category,
                       owner, energy_type, classic_quality,
                       province, city,
                       h2_output_tpy, h2_output_nm3_per_h, electrolyzer_count, co2_reduction_tpy,
                       project_summary, source_type, project_overview, project_progress,
                       user_note, is_ai_improved, article_type, numerical_data, id
                FROM projects_classic
                {where_sql}
                ORDER BY published_at DESC, id DESC
                LIMIT ? OFFSET ?
            """,
            params + [page_size, offset]
        )
        rows = cursor.fetchall()
        
        # Convert to list of dicts
        results = []
        for row in rows:
            results.append({
                "id": row['id'],
                "url": row['url'],
                "channel_id": row['channel_id'],
                "channel_label": row['channel_label'],
                "article_title": row['article_title'],
                "published_at": row['published_at'],
                "project_name": row['project_name'],
                "stage": row['stage'],
                "event_date": row['event_date'],
                "location": row['location'],
                "capacity_mw": row['capacity_mw'],
                "investment_cny": row['investment_cny'],
                "product_category": row['product_category'],
                "owner": row['owner'],
                "energy_type": row['energy_type'],
                "classic_quality": row['classic_quality'],
                "province": row['province'],
                "city": row['city'],
                "h2_output_tpy": row['h2_output_tpy'],
                "h2_output_nm3_per_h": row['h2_output_nm3_per_h'],
                "electrolyzer_count": row['electrolyzer_count'],
                "co2_reduction_tpy": row['co2_reduction_tpy'],
                "project_summary": row['project_summary'],
                "source_type": row['source_type'],
                "project_overview": row['project_overview'],
                "project_progress": row['project_progress'],
                "user_note": row['user_note'],
                "is_ai_improved": bool(row['is_ai_improved']),
                "article_type": row['article_type'],
                "numerical_data": row['numerical_data'],
            })
        
        return jsonify({"total": total, "items": results})
    except Exception as e:
        import traceback
        traceback.print_exc()
        return jsonify({"error": str(e), "trace": traceback.format_exc()}), 500
    finally:
        try:
            conn.close()
        except:
            pass


@app.route('/api/hydrogen/projects/classic/update_note', methods=['POST'])
def update_classic_project_note():
    payload = request.get_json() or {}
    project_id = payload.get('id')
    note = payload.get('note')
    
    if not project_id:
        return jsonify({"error": "Missing project ID"}), 400
        
    conn = get_db_connection()
    try:
        cursor = conn.cursor()
        cursor.execute("UPDATE projects_classic SET user_note = ? WHERE id = ?", (note, project_id))
        conn.commit()
        return jsonify({"ok": True})
    except sqlite3.Error as e:
        return jsonify({"error": str(e)}), 500
    finally:
        conn.close()

@app.route("/classic-projects")
def classic_projects_full():
    return render_template("classic_projects_full.html")


@app.route("/api/hydrogen/projects/classic/reset", methods=["POST"])
def reset_classic_projects():
    conn = None
    try:
        conn = sqlite3.connect(DB_PATH)
        cur = conn.cursor()
        cur.execute("DELETE FROM projects_classic")
        cur.execute(
            "UPDATE articles SET classic_score=0, worth_classic=0, classic_quality=NULL"
        )
        conn.commit()
    except sqlite3.Error as exc:
        if conn is not None:
            conn.rollback()
        classic_extractor_status.update(
            {
                "stage": "idle",
                "message": f"重置失败: {exc}",
                "current": 0,
                "total": 0,
            }
        )
        return jsonify({"ok": False, "error": str(exc)}), 500
    finally:
        if conn is not None:
            conn.close()

    classic_extractor_status.update(
        {
            "stage": "idle",
            "message": "经典项目记录已重置",
            "current": 0,
            "total": 0,
        }
    )
    return jsonify({"ok": True})


@app.route("/details/full")
def details_full_view():
    return render_template("detail_full.html")


@app.route("/api/data", methods=["POST"])
def get_data():
    csv_path = pathlib.Path("inner_mongolia_projects.csv")
    if not csv_path.exists():
        return jsonify({"rows": [], "total": 0})
    import csv as csv_mod
    payload = request.get_json() or {}
    keywords = [k.lower() for k in (payload.get("keywords") or []) if k]
    exclude = [k.lower() for k in (payload.get("exclude") or []) if k]
    cbs_filter = {c.strip() for c in (payload.get("cbsnums") or []) if c.strip()}
    rows: List[dict] = []
    with csv_path.open(newline="", encoding="utf-8-sig") as f:
        reader = csv_mod.reader(f)
        meta = next(reader, None)
        header = next(reader, None)
        if not header:
            return jsonify({"rows": [], "total": 0})
        dict_reader = csv_mod.DictReader(f, fieldnames=header)
        for row in dict_reader:
            cbs = (row.get('cbsnum') or '').strip()
            if '?' in cbs and cbs == 'cbsnum':
                continue
            text = ' '.join((value or '') for value in row.values()).lower()
            # 关键词标签采用“或”逻辑：任一关键词命中即可
            if keywords and not any(k in text for k in keywords):
                continue
            # 排除词仍然是“或”逻辑：包含任一排除词则排除
            if exclude and any(k in text for k in exclude):
                continue
            if cbs_filter and cbs not in cbs_filter:
                continue
            rows.append(row)
    return jsonify({"rows": rows, "total": len(rows)})


@app.route("/api/detail-files")
def get_detail_files():
    import glob, os as _os
    files = glob.glob("detailed_project_data_*.csv")
    files = sorted(files, key=lambda p: _os.path.getmtime(p), reverse=True)
    return jsonify({"files": files})


@app.route("/api/detail-data", methods=["POST"])
def get_detail_data():
    import csv as csv_mod
    payload = request.get_json() or {}
    filename = payload.get("file")
    if not filename or not pathlib.Path(filename).exists():
        return jsonify({"header": [], "rows": [], "total": 0})
    keywords = [k.lower() for k in (payload.get("keywords") or []) if k]
    exclude = [k.lower() for k in (payload.get("exclude") or []) if k]
    rows = []
    header = []
    with open(filename, newline="", encoding="utf-8-sig") as f:
        reader = list(csv_mod.reader(f))
        if not reader:
            return jsonify({"header": [], "rows": [], "total": 0})
        # 兼容旧格式（首行 METADATA）与新格式（首行为表头，元数据行在末尾）
        if reader[0] and reader[0][0] == "METADATA":
            if len(reader) < 2:
                return jsonify({"header": [], "rows": [], "total": 0})
            header = reader[1]
            data_rows = reader[2:]
        else:
            header = reader[0]
            data_rows = reader[1:]
        filtered_data = []
        for r in data_rows:
            if r and r[0] == "METADATA":
                continue
            row_dict = {header[i]: (r[i] if i < len(r) else "") for i in range(len(header))}
            text = " ".join((value or "") for value in row_dict.values()).lower()
            if keywords and not any(k in text for k in keywords):
                continue
            if exclude and any(k in text for k in exclude):
                continue
            filtered_data.append(row_dict)
        rows = filtered_data
    return jsonify({"header": header, "rows": rows, "total": len(rows)})


@app.route("/api/config/siliconflow", methods=["GET", "POST", "DELETE"])
def siliconflow_config():
    if request.method == "GET":
        secrets = load_secrets()
        key = secrets.get("SILICONFLOW_API_KEY")
        model = secrets.get("SILICONFLOW_MODEL", "deepseek-ai/DeepSeek-V3")
        return jsonify({
            "has_key": bool(key),
            "key_masked": f"{key[:4]}...{key[-4:]}" if key else None,
            "model": model
        })
    
    if request.method == "POST":
        data = request.get_json() or {}
        secrets = load_secrets()
        
        if "api_key" in data:
            secrets["SILICONFLOW_API_KEY"] = data["api_key"]
            
        if "model" in data:
            secrets["SILICONFLOW_MODEL"] = data["model"]
            
        save_secrets(secrets)
        return jsonify({"ok": True})
        
    if request.method == "DELETE":
        secrets = load_secrets()
        if "SILICONFLOW_API_KEY" in secrets:
            del secrets["SILICONFLOW_API_KEY"]
            save_secrets(secrets)
        return jsonify({"ok": True})


def ai_progress(**info):
    ai_extractor_status.update(info)


@app.route("/api/hydrogen/projects/ai/run", methods=["POST"])
def run_ai_extractor():
    global ai_extractor_task
    if ai_extractor_task and ai_extractor_task.is_alive():
        return jsonify({"ok": False, "message": "AI提取任务正在运行中"}), 409

    secrets = load_secrets()
    api_key = secrets.get("SILICONFLOW_API_KEY")
    model = secrets.get("SILICONFLOW_MODEL", "deepseek-ai/DeepSeek-V3")
    
    if not api_key:
        return jsonify({"ok": False, "message": "未配置API Key"}), 400

    payload = request.get_json() or {}
    max_projects = payload.get("max_projects", 10)
    # Reuse the UI “每分钟请求上限” field as a per-minute request cap to avoid rate limits.
    rpm_limit = payload.get("max_workers", 20)  # default conservative to stay under TPM
    # Allow a larger worker pool; RPM limiter will enforce overall pace.
    worker_count = max(1, min(int(rpm_limit), 20))

    def task():
        try:
            extractor = AIProjectExtractor(api_key, model=model, rpm_limit=int(rpm_limit))
            ai_progress(stage="running", message="AI提取启动", current=0, total=0)
            extractor.run(
                max_projects=int(max_projects),
                max_workers=worker_count,
                progress_callback=ai_progress,
            )
        except Exception as e:
            ai_progress(stage="idle", message=f"AI提取出错: {e}")
        finally:
            if ai_extractor_status.get("stage") != "idle":
                ai_progress(stage="idle", message="AI提取结束")

    ai_extractor_task = threading.Thread(target=task, daemon=True)
    ai_extractor_task.start()
    return jsonify({"ok": True})


@app.route("/api/hydrogen/projects/ai/status")
def get_ai_extractor_status():
    try:
        conn = get_db_connection()
        cur = conn.cursor()
        cur.execute("SELECT COUNT(*) FROM projects_classic")
        total = cur.fetchone()[0]
        cur.execute("SELECT COUNT(*) FROM projects_classic WHERE is_ai_improved = 1")
        ai_done = cur.fetchone()[0]
    except Exception:
        total = 0
        ai_done = 0
    return jsonify({**ai_extractor_status, "classic_total": total, "ai_done": ai_done})

@app.route("/api/hydrogen/projects/ai/reset", methods=["POST"])
def reset_ai_projects():
    conn = get_db_connection()
    try:
        cur = conn.execute("SELECT url FROM projects_classic WHERE is_ai_improved = 1")
        urls = [row["url"] for row in cur.fetchall()]
        deleted = 0
        if urls:
            with conn:
                conn.execute("DELETE FROM projects_classic WHERE is_ai_improved = 1")
                conn.executemany(
                    "UPDATE articles SET classic_score=0, worth_classic=0, classic_quality=NULL WHERE url=?",
                    [(u,) for u in urls],
                )
            deleted = len(urls)
        return jsonify({"ok": True, "deleted": deleted})
    except Exception as exc:
        return jsonify({"ok": False, "message": str(exc)}), 500
    finally:
        conn.close()


@app.route("/api/hydrogen/projects/classic/export")
def export_classic_projects():
    conn = get_db_connection()
    rows = []
    try:
        cur = conn.execute("SELECT * FROM projects_classic ORDER BY published_at DESC NULLS LAST")
        rows = cur.fetchall()
    except Exception:
        cur = conn.execute("SELECT * FROM projects_classic ORDER BY published_at DESC")
        rows = cur.fetchall()
    finally:
        conn.close()

    if not rows:
        return jsonify({"ok": False, "message": "暂无数据可导出"}), 404

    columns = [
        ("AI标记", "is_ai_improved"),
        ("文章类型", "article_type"),
        ("来源", "channel_label"),
        ("项目名称", "project_name"),
        ("项目概况", "project_overview"),
        ("数值数据", "numerical_data"),
        ("项目进展情况", "project_progress"),
        ("阶段", "stage"),
        ("质量", "classic_quality"),
        ("省份", "province"),
        ("城市", "city"),
        ("产能", "capacity_mw"),
        ("投资", "investment_cny"),
        ("业主/开发商", "owner"),
        ("发布日期", "published_at"),
        ("备注", "user_note"),
        ("链接", "url"),
        ("H2产能(t/y)", "h2_output_tpy"),
        ("H2产能(Nm³/h)", "h2_output_nm3_per_h"),
        ("电解槽数", "electrolyzer_count"),
        ("CO2减排", "co2_reduction_tpy"),
        ("产品类别", "product_category"),
        ("能源类型", "energy_type"),
        ("地点详情", "location"),
        ("事件日期", "event_date"),
        ("频道", "channel_label"),
        ("文章标题", "article_title"),
    ]

    def normalize(row):
        data = dict(row)
        # Boolean to Y/N
        ai_val = data.get("is_ai_improved")
        if ai_val in (1, True, "1", "true", "True"):
            data["is_ai_improved"] = "Y"
        else:
            data["is_ai_improved"] = ""
        return data

    buf = io.StringIO()
    writer = csv_std.writer(buf)
    writer.writerow([cn for cn, _ in columns])
    for r in rows:
        d = normalize(r)
        writer.writerow([d.get(key, "") for _, key in columns])

    mem = io.BytesIO(buf.getvalue().encode("utf-8-sig"))
    mem.seek(0)
    return send_file(mem, mimetype="text/csv", as_attachment=True, download_name="hydrogen_projects_full.csv")


if __name__ == "__main__":
    host = os.environ.get("APP_HOST", "127.0.0.1")
    port = int(os.environ.get("APP_PORT", "8000"))
    app.run(host=host, port=port, debug=False)
