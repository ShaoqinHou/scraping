import dataclasses
import datetime as dt
import json
import os
import re
import sqlite3
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Any, Callable, Dict, List, Optional, Tuple

import requests
from bs4 import BeautifulSoup
from pathlib import Path

BASE_DIR = Path(__file__).resolve().parent
DEFAULT_DB_PATH = str(BASE_DIR / "qn_hydrogen_monitor.db")


ClassicProgressCallback = Callable[..., None]


@dataclasses.dataclass
class ClassicProject:
    url: str
    channel_id: str
    channel_label: str
    article_title: str
    published_at: Optional[str]
    project_name: Optional[str]
    stage: Optional[str]
    event_date: Optional[str]
    location: Optional[str]
    capacity_mw: Optional[float]
    investment_cny: Optional[float]
    product_category: Optional[str]
    owner: Optional[str]
    energy_type: Optional[str]
    classic_quality: Optional[str]
    province: Optional[str] = None
    city: Optional[str] = None
    h2_output_tpy: Optional[float] = None
    h2_output_nm3_per_h: Optional[float] = None
    electrolyzer_count: Optional[int] = None
    h2_output_nm3_per_h: Optional[float] = None
    electrolyzer_count: Optional[int] = None
    co2_reduction_tpy: Optional[float] = None
    project_summary: Optional[str] = None
    source_type: str = "single"  # single, list, bundle
    project_overview: Optional[str] = None
    project_progress: Optional[str] = None
    user_note: Optional[str] = None
    is_ai_improved: Optional[bool] = False
    article_type: Optional[str] = None
    numerical_data: Optional[str] = None


class ClassicProjectExtractor:
    PROJECT_WORDS = ["项目", "工程", "基地", "示范", "园区", "走廊", "产业园", "示范区", "一体化"]
    STAGE_KEYWORDS: Dict[str, List[str]] = {
        "备案/获批/核准": [
            "备案",
            "核准",
            "获批",
            "批复",
            "备案通过",
            "准予备案",
            "获备案",
            "项目备案",
            "审批通过",
        ],
        "开工": [
            "开工",
            "动工",
            "开建",
            "启动建设",
            "开工仪式",
            "开工建设",
            "开工活动",
            "项目启动",
        ],
        "在建": ["在建", "施工中", "施工", "安装高峰期"],
        "投运/投产": [
            "投运",
            "投产",
            "并网",
            "竣工",
            "投用",
            "投入使用",
            "投入运营",
            "投入运行",
            "试运行",
            "竣工投产",
            "竣工投用",
        ],
        "招标": [
            "招标",
            "采购",
            "比选",
            "招标公告",
            "EPC招标",
            "总承包招标",
            "公开招标",
            "采购项目",
        ],
        "中标": [
            "中标",
            "中选",
            "中标结果",
            "中标公示",
            "中标候选人",
            "中标结果公示",
        ],
        "签约": ["签约", "签署协议", "签署合作协议", "合作协议", "签约仪式", "合作签约"],
        "取消": ["取消", "终止", "废标"],
    }
    ENERGY_HINTS: Dict[str, List[str]] = {
        "氢能": [
            "氢能",
            "制氢",
            "绿氢",
            "氢氨",
            "氢气",
            "制氢项目",
            "制氢厂",
            "氢能项目",
        ],
        "风电": [
            "风电",
            "风力发电",
            "风电场",
            "风光制氢",
            "风光氢",
            "风电制氢",
        ],
        "光伏": [
            "光伏",
            "光电",
            "太阳能",
            "光伏发电",
            "光伏制氢",
        ],
        "绿氨/甲醇": [
            "绿氨",
            "绿色甲醇",
            "绿色燃料",
            "绿醇",
            "绿色LNG",
            "氢氨醇",
        ],
    }
    PROVINCE_NAMES = [
        "北京",
        "天津",
        "上海",
        "重庆",
        "内蒙古",
        "新疆",
        "西藏",
        "宁夏",
        "广西",
        "黑龙江",
        "吉林",
        "辽宁",
        "河北",
        "山东",
        "山西",
        "河南",
        "陕西",
        "甘肃",
        "青海",
        "江苏",
        "浙江",
        "安徽",
        "江西",
        "福建",
        "广东",
        "海南",
        "贵州",
        "四川",
        "云南",
        "湖北",
        "湖南",
        "台湾",
        "香港",
        "澳门",
    ]
    LOCATION_PROVINCE_PATTERN = re.compile(
        r"([^\s，。、；:：]{2,10}(?:省|自治区|直辖市))"
    )
    LOCATION_CITY_PATTERN = re.compile(
        r"([^\s，。、；:：]{2,12}(?:市|州|盟|旗|县|区|镇))"
    )

    def __init__(
        self,
        db_path: str = DEFAULT_DB_PATH,
        session: Optional[requests.Session] = None,
        progress_callback: Optional[ClassicProgressCallback] = None,
    ) -> None:
        self.db_path = db_path
        base_session = session or requests.Session()
        # 尽量模拟常见浏览器，避免被 news.bjx.com.cn 拒绝（403）
        base_session.headers["User-Agent"] = (
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:132.0) "
            "Gecko/20100101 Firefox/132.0"
        )
        base_session.headers["Accept"] = "text/html,application/xhtml+xml"
        base_session.headers["Accept-Language"] = "zh-CN,zh;q=0.9"
        base_session.headers["Referer"] = "https://www.bjx.com.cn/"
        self.session = base_session
        self.progress_callback = progress_callback or (lambda **_: None)
        self._conn: Optional[sqlite3.Connection] = None
        # 行政区划索引（从 cn_regions_raw.json 构建）
        self._province_by_token: Dict[str, str] = {}
        self._city_by_token: Dict[str, Tuple[str, str]] = {}
        self._ensure_unique_index()

    def _ensure_unique_index(self) -> None:
        """避免 projects_classic 重复 URL。"""
        with sqlite3.connect(self.db_path) as conn:
            conn.row_factory = sqlite3.Row
            self._ensure_schema(conn)
            conn.execute(
                "CREATE UNIQUE INDEX IF NOT EXISTS ux_projects_classic_url ON projects_classic(url)"
            )
        self._load_region_index()

    def _emit(self, **info: Any) -> None:
        self.progress_callback(**info)

    def _connect(self) -> sqlite3.Connection:
        def _open() -> sqlite3.Connection:
            conn = sqlite3.connect(self.db_path, check_same_thread=False)
            conn.row_factory = sqlite3.Row
            self._ensure_schema(conn)
            return conn

        if self._conn is not None:
            try:
                self._conn.execute("SELECT 1")
                return self._conn
            except sqlite3.Error:
                self._conn = None

        try:
            self._conn = _open()
        except sqlite3.DatabaseError as exc:
            if "file is not a database" in str(exc):
                backup = f"{self.db_path}.corrupt"
                try:
                    os.replace(self.db_path, backup)
                except OSError:
                    pass
                self._conn = _open()
            else:
                raise
        return self._conn

    def _ensure_schema(self, conn: sqlite3.Connection) -> None:
        # Ensure articles table exists
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS articles (
                url TEXT PRIMARY KEY,
                channel_id TEXT NOT NULL,
                channel_label TEXT NOT NULL,
                title TEXT NOT NULL,
                published_at TEXT,
                created_at TEXT NOT NULL,
                updated_at TEXT NOT NULL
            )
            """
        )
        # Add extra columns if missing
        cur = conn.execute("PRAGMA table_info(articles)")
        cols = {row[1] for row in cur.fetchall()}
        alter_stmts = []
        if "main_text" not in cols:
            alter_stmts.append("ALTER TABLE articles ADD COLUMN main_text TEXT")
        if "classic_score" not in cols:
            alter_stmts.append(
                "ALTER TABLE articles ADD COLUMN classic_score INTEGER DEFAULT 0"
            )
        if "worth_classic" not in cols:
            alter_stmts.append(
                "ALTER TABLE articles ADD COLUMN worth_classic INTEGER DEFAULT 0"
            )
        if "classic_quality" not in cols:
            alter_stmts.append(
                "ALTER TABLE articles ADD COLUMN classic_quality TEXT"
            )
        for stmt in alter_stmts:
            try:
                conn.execute(stmt)
            except sqlite3.Error:
                continue

        # Projects table
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS projects_classic (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                url TEXT NOT NULL,
                channel_id TEXT NOT NULL,
                channel_label TEXT NOT NULL,
                article_title TEXT NOT NULL,
                published_at TEXT,
                project_name TEXT,
                stage TEXT,
                event_date TEXT,
                location TEXT,
                capacity_mw REAL,
                investment_cny REAL,
                product_category TEXT,
                owner TEXT,
                energy_type TEXT,
                classic_quality TEXT,
                province TEXT,
                city TEXT,
                h2_output_tpy REAL,
                h2_output_nm3_per_h REAL,
                electrolyzer_count INTEGER,
                co2_reduction_tpy REAL,
                project_summary TEXT,
                source_type TEXT,
                project_overview TEXT,
                project_progress TEXT,
                user_note TEXT,
                is_ai_improved BOOLEAN DEFAULT 0
            )
            """
        )
        
        # Add columns if they don't exist (for migration)
        try:
            conn.execute("ALTER TABLE projects_classic ADD COLUMN project_summary TEXT")
        except sqlite3.OperationalError:
            pass
            
        try:
            conn.execute("ALTER TABLE projects_classic ADD COLUMN source_type TEXT")
        except sqlite3.OperationalError:
            pass

        try:
            conn.execute("ALTER TABLE projects_classic ADD COLUMN project_overview TEXT")
        except sqlite3.OperationalError:
            pass

        try:
            conn.execute("ALTER TABLE projects_classic ADD COLUMN project_progress TEXT")
        except sqlite3.OperationalError:
            pass

        # Add user_note and is_ai_improved columns if they don't exist
        try:
            conn.execute("ALTER TABLE projects_classic ADD COLUMN user_note TEXT")
            conn.execute("ALTER TABLE projects_classic ADD COLUMN is_ai_improved BOOLEAN DEFAULT 0")
        except sqlite3.OperationalError:
            pass

        conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_projects_classic_url ON projects_classic(url)"
        )
        # Optional new columns for older DBs
        cur = conn.execute("PRAGMA table_info(projects_classic)")
        pcols = {row[1] for row in cur.fetchall()}
        alter_proj = []
        if "province" not in pcols:
            alter_proj.append("ALTER TABLE projects_classic ADD COLUMN province TEXT")
        if "city" not in pcols:
            alter_proj.append("ALTER TABLE projects_classic ADD COLUMN city TEXT")
        if "h2_output_tpy" not in pcols:
            alter_proj.append("ALTER TABLE projects_classic ADD COLUMN h2_output_tpy REAL")
        if "h2_output_nm3_per_h" not in pcols:
            alter_proj.append("ALTER TABLE projects_classic ADD COLUMN h2_output_nm3_per_h REAL")
        if "electrolyzer_count" not in pcols:
            alter_proj.append("ALTER TABLE projects_classic ADD COLUMN electrolyzer_count INTEGER")
        if "co2_reduction_tpy" not in pcols:
            alter_proj.append("ALTER TABLE projects_classic ADD COLUMN co2_reduction_tpy REAL")
        if "article_type" not in pcols:
            alter_proj.append("ALTER TABLE projects_classic ADD COLUMN article_type TEXT")
        if "numerical_data" not in pcols:
            alter_proj.append("ALTER TABLE projects_classic ADD COLUMN numerical_data TEXT")
        for stmt in alter_proj:
            try:
                conn.execute(stmt)
            except sqlite3.Error:
                continue
        conn.commit()

    def _load_region_index(self) -> None:
        """从 cn_regions_raw.json 构建 {token -> (province, city)} 索引."""
        try:
            base_dir = os.path.dirname(os.path.abspath(__file__))
            path = os.path.join(base_dir, "cn_regions_raw.json")
            if not os.path.exists(path):
                return
            with open(path, "r", encoding="utf-8") as f:
                data = json.load(f)
        except Exception:
            return

        def canonical_province(name: str) -> str:
            suffixes = [
                "特别行政区",
                "维吾尔自治区",
                "壮族自治区",
                "回族自治区",
                "自治州",
                "自治区",
                "省",
                "市",
            ]
            for suf in suffixes:
                if name.endswith(suf):
                    return name[: -len(suf)]
            return name

        def is_city_like(name: str) -> bool:
            if not name:
                return False
            suffixes = ("市", "州", "盟", "旗", "县", "区", "镇", "地区")
            return name.endswith(suffixes)

        for prov_full, city_dict in data.items():
            if not isinstance(city_dict, dict):
                continue
            prov_canon = canonical_province(prov_full)
            prov_tokens = {prov_full, prov_canon}
            for token in prov_tokens:
                if token:
                    self._province_by_token[token] = prov_canon

            # 直辖市等，将省名本身视为“市”
            self._city_by_token.setdefault(prov_full, (prov_canon, prov_canon))
            self._city_by_token.setdefault(prov_canon, (prov_canon, prov_canon))

            for city_name, districts in city_dict.items():
                # “市辖区”等虚拟层级不单独作为城市
                if city_name in {"市辖区", "县"}:
                    continue
                if is_city_like(city_name):
                    city_canon = city_name
                    tokens = {city_name}
                    # 兼容“张掖市/张掖”这类写法
                    if len(city_name) > 2:
                        tokens.add(city_name.rstrip("市州盟旗县区镇地区"))
                    for token in tokens:
                        if token:
                            self._city_by_token.setdefault(
                                token, (prov_canon, city_canon)
                            )

                # 下一级（区/旗/县等）
                if isinstance(districts, dict):
                    dist_iter = districts.keys()
                else:
                    dist_iter = districts or []
                for dist_name in dist_iter:
                    if not isinstance(dist_name, str):
                        continue
                    if not is_city_like(dist_name):
                        continue
                    dist_canon = dist_name
                    tokens = {dist_name}
                    if len(dist_name) > 2:
                        tokens.add(dist_name.rstrip("市州盟旗县区镇地区"))
                    for token in tokens:
                        if token:
                            # 例如 达拉特旗 -> (内蒙古, 达拉特旗)
                            self._city_by_token.setdefault(
                                token, (prov_canon, dist_canon)
                            )

    def _fetch_article_html(self, url: str) -> Optional[str]:
        try:
            resp = self.session.get(url, timeout=10)
            resp.raise_for_status()
            resp.encoding = resp.apparent_encoding or resp.encoding
            return resp.text
        except Exception:
            return None

    def _extract_main_text(self, html: str) -> str:
        soup = BeautifulSoup(html, "html.parser")
        # Try some common containers used by bjx
        candidates = []
        for selector in [
            "div#content",
            "div.article",
            "div.content",
            "div.main",
            "div.newsText",
        ]:
            node = soup.select_one(selector)
            if node:
                candidates.append(node)
        node = candidates[0] if candidates else soup.body
        if not node:
            return ""
        texts: List[str] = []
        for p in node.find_all("p"):
            t = p.get_text(" ", strip=True)
            if t:
                texts.append(t)
        # Fallback to all text if paragraphs are empty
        if not texts:
            t = node.get_text(" ", strip=True)
            return t
        return "\n".join(texts)

    def _build_text_head(self, title: str, main_text: str, max_chars: int = 600) -> str:
        head = (title or "").strip()
        if head:
            head += "\n"
        head += main_text[:max_chars]
        return head

    def _has_any(self, text: str, words: List[str]) -> bool:
        return any(w in text for w in words)

    def _compute_classic_score(self, title: str, head: str) -> Tuple[int, Dict[str, bool]]:
        score = 0
        flags: Dict[str, bool] = {}
        combined = f"{title}\n{head}"

        # 项目感
        has_project_word = self._has_any(combined, self.PROJECT_WORDS)
        if has_project_word:
            score += 1
        flags["project_word"] = has_project_word

        # 阶段感
        has_stage = any(
            self._has_any(combined, kws) for kws in self.STAGE_KEYWORDS.values()
        )
        if has_stage:
            score += 1
        flags["stage_word"] = has_stage

        # 容量感
        has_capacity = bool(
            re.search(r"\d+(\.\d+)?\s*万?\s*千瓦", combined)
            or re.search(r"\d+(\.\d+)?\s*MW", combined, re.IGNORECASE)
        )
        if has_capacity:
            score += 1
        flags["capacity"] = has_capacity

        # 投资感
        has_investment = bool(
            re.search(r"\d+(\.\d+)?\s*亿元", combined)
            or re.search(r"\d+(\.\d+)?\s*万\s*元", combined)
        )
        if has_investment:
            score += 1
        flags["investment"] = has_investment

        # 地点 + 项目
        has_location_project = bool(
            re.search(
                r"(省|市|自治区|州|盟|旗|县|区).{0,10}(项目|工程|基地|示范|园区)", combined
            )
        )
        if has_location_project:
            score += 1
        flags["location_project"] = has_location_project

        return score, flags

    def _determine_source_type(self, title: str, text: str) -> str:
        # 1. Check title for list indicators (Strong signal)
        if any(x in title for x in ["一览", "汇总", "名单", "统计", "盘点", "周报", "月报", "项目动态"]):
            return "list"
        
        # 2. If title doesn't say it's a list, assume it's single.
        # The previous logic of counting numbered items caused false positives for tender documents
        # which often have "1. 招标条件", "2. 项目概况" etc.
        
        return "single"

    def _generate_summary(self, text: str, max_chars: int = 300) -> str:
        # Split into sentences
        sentences = re.split(r"[。！？\n]+", text)
        
        # Keywords to keep
        keywords = [
            "投资", "亿元", "万元", "产能", "产量", "吨/年", "Nm3/h", "MW", "GW",
        ]
        
        selected = []
        current_len = 0
        
        for s in sentences:
            s = s.strip()
            if not s:
                continue
            
            # Score sentence
            score = 0
            for k in keywords:
                if k in s:
                    score += 1
            
            if score > 0:
                if current_len + len(s) > max_chars:
                    break
                selected.append(s)
                current_len += len(s)
                
        return "。".join(selected) + "。" if selected else ""

    def _extract_list_items(self, text: str) -> List[Dict[str, str]]:
        """
        Extract individual projects from a list article.
        Returns a list of dicts with 'name', 'overview', 'progress'.
        """
        items = []
        # Pattern 1: Numbered list "1. Project Name" or "1、Project Name"
        # We look for a number at start of line, followed by some text, then a newline or end of string
        # The content between numbers is the description.
        
        # Split text by numbered markers
        # Regex to find "1." or "1、" or "(1)" at start of line
        # We use capturing group to keep the delimiter so we know where it starts
        parts = re.split(r"(\n\s*(?:\d+[.、]|\(\d+\)|[一二三四五六七八九十]+[、.]))", text)
        
        current_item = {}
        
        # parts[0] is usually intro text.
        # parts[1] is marker, parts[2] is content, parts[3] is marker, parts[4] is content...
        
        for i in range(1, len(parts), 2):
            marker = parts[i]
            content = parts[i+1] if i+1 < len(parts) else ""
            
            # Extract Name: usually the first line or sentence after marker
            lines = content.strip().split('\n')
            first_line = lines[0].strip()
            
            # Clean name (remove punctuation at end)
            name_match = re.match(r"^([^\s，。；：:！!？?]+(?:项目|工程|基地|示范|园区))", first_line)
            name = name_match.group(1) if name_match else first_line
            if len(name) > 50: # If too long, it's probably not just a name
                 name = name[:50] + "..."

            # Overview: The whole content is the overview for now
            overview = content.strip()
            
            # Progress: Try to find progress keywords in the content
            progress = ""
            progress_keywords = ["签约", "开工", "投产", "中标", "备案", "获批", "公示", "招标"]
            for kw in progress_keywords:
                if kw in overview:
                    # Extract the sentence containing the keyword
                    sentences = re.split(r"[。！？\n]+", overview)
                    for s in sentences:
                        if kw in s:
                            progress = s.strip()
                            break
                    if progress:
                        break
            
            items.append({
                "name": name,
                "overview": overview,
                "progress": progress
            })
            
        return items

    def _extract_project_name(self, title: str, head: str) -> Optional[str]:
        # 1. 优先提取书名号中的内容，通常是项目全称
        # 限制长度，且不允许包含换行符，避免匹配到大段无关文本
        pattern_quote = r"《([^》\n]{2,60}项目[^》\n]*)》"
        
        m_quote = re.search(pattern_quote, title)
        if m_quote:
            return m_quote.group(1)
        
        m_quote_head = re.search(pattern_quote, head)
        if m_quote_head:
            return m_quote_head.group(1)

        # 2. 尝试提取 "XXX项目"
        # 排除 "关于"、"拟" 等前缀
        # 限制长度在 4-30 字之间，避免提取整句话
        # [^\s...] 已经排除换行符，但为了保险，明确长度限制
        pattern = r"([^\s，。；：:！!？?]{2,30}?(?:项目|工程|基地|示范|园区))"
        
        m = re.search(pattern, title)
        if m:
            name = m.group(1)
            # 清理前缀
            name = re.sub(r"^(关于|拟|一期|二期|三期|首期|全省|全市|我省|我市)", "", name)
            if len(name) > 4:
                return name

        m2 = re.search(pattern, head)
        if m2:
            name = m2.group(1)
            name = re.sub(r"^(关于|拟|一期|二期|三期|首期|全省|全市|我省|我市)", "", name)
            if len(name) > 4:
                return name
                
        return None

    def _extract_stage(self, text: str) -> Optional[str]:
        for stage, kws in self.STAGE_KEYWORDS.items():
            if self._has_any(text, kws):
                return stage
        return None

    @staticmethod
    def _parse_number(num_str: str) -> Optional[float]:
        try:
            return float(num_str.replace(",", ""))
        except Exception:
            return None

    def _extract_capacity_mw(self, text: str) -> Optional[float]:
        # 万千瓦 => MW
        m = re.search(r"(\d[\d,]*\.?\d*)\s*万\s*千瓦", text)
        if m:
            v = self._parse_number(m.group(1))
            if v is not None:
                return v * 10.0  # 1万千瓦 = 10 MW
        # GW
        m = re.search(r"(\d[\d,]*\.?\d*)\s*GW", text, re.IGNORECASE)
        if m:
            v = self._parse_number(m.group(1))
            if v is not None:
                return v * 1000.0
        # MW
        m = re.search(r"(\d[\d,]*\.?\d*)\s*MW", text, re.IGNORECASE)
        if m:
            v = self._parse_number(m.group(1))
            if v is not None:
                return v
        # 千瓦/kW
        m = re.search(r"(\d[\d,]*\.?\d*)\s*(千瓦|kW|KW)", text)
        if m:
            v = self._parse_number(m.group(1))
            if v is not None:
                return v / 1_000.0
        # 瓦
        m = re.search(r"(\d[\d,]*\.?\d*)\s*瓦", text)
        # MW
        if m:
            v = self._parse_number(m.group(1))
            if v is not None:
                return v / 1_000_000.0
        return None

    def _extract_investment_cny(self, text: str) -> Optional[float]:
        # 亿元
        m = re.search(r"(\d[\d,]*\.?\d*)\s*亿\s*元?", text)
        if m:
            v = self._parse_number(m.group(1))
            if v is not None:
                return v * 100_000_000.0
        # 万元
        m = re.search(r"(\d[\d,]*\.?\d*)\s*万\s*元", text)
        if m:
            v = self._parse_number(m.group(1))
            if v is not None:
                return v * 10_000.0
        # 元
        m = re.search(r"(\d[\d,]*\.?\d*)\s*元", text)
        if m:
            v = self._parse_number(m.group(1))
            if v is not None:
                return v
        return None

    def _extract_owner(self, text: str) -> Optional[str]:
        # 1. 明确的字段标识
        m = re.search(
            r"(建设单位|项目业主|申报单位|业主单位|建设方|牵头单位|投资方|投资主体)[：:]\s*([^\n。；;，,]{2,30})",
            text,
        )
        if m:
            return m.group(2).strip()

        # 2. "由...投资/建设"
        m_by = re.search(r"由([^\s，。；：:！!？?]{2,30}?)投资", text)
        if m_by:
            return m_by.group(1).strip()
            
        m_build = re.search(r"由([^\s，。；：:！!？?]{2,30}?)建设", text)
        if m_build:
            return m_build.group(1).strip()

        # 3. 签约主体 (e.g. "A公司与B政府签约") - 比较难提取单一业主，暂略

        # 4. 来源字段 (通常比较准确)
        m_source = re.search(r"来源：([^\s\n]+)", text)
        if m_source:
            src = m_source.group(1).strip()
            if "网" not in src and "号" not in src and len(src) > 2: # 排除 "xx网", "xx公众号"
                return src

        # 5. 移除过于宽泛的 fallback (寻找任意“公司”)，因为它导致了大量错误匹配
        # 如果确实需要 fallback，必须非常严格，例如紧跟在“项目”后面
        
        return None

    def _extract_h2_output_tpy(self, text: str) -> Optional[float]:
        """提取年产氢量（吨/年），优先匹配“年产绿氢/氢气X(万)吨”模式。"""
        patterns = [
            r"(年产|年可生产|年可实现)[^。；\n]{0,8}?(\d+(?:\.\d+)?)(万)?\s*吨[^。；\n]{0,6}?(绿氢|氢气|氢)",
            r"(绿氢|氢气|氢)[^。；\n]{0,8}?年产[^。；\n]{0,4}?(\d+(?:\.\d+)?)(万)?\s*吨",
        ]
        for pat in patterns:
            m = re.search(pat, text)
            if not m:
                continue
            if len(m.groups()) == 4:
                num_str = m.group(2)
                wan = m.group(3)
            else:
                num_str = m.group(2)
                wan = m.group(3)
            try:
                val = float(num_str)
            except (TypeError, ValueError):
                continue
            if wan:
                val *= 10_000.0
            return val
        return None

    def _extract_h2_output_nm3_per_h(self, text: str) -> Optional[float]:
        """提取制氢能力 Nm³/h，例如 30000Nm³/h、1万标方/小时。"""
        # 允许“万”+ 标方/Nm³ + /h /小时
        m = re.search(
            r"(\d+(?:\.\d+)?)(万)?\s*(标方|Nm³|Nm3)\s*[/每]?\s*(小时|h)",
            text,
        )
        if not m:
            return None
        try:
            val = float(m.group(1))
        except (TypeError, ValueError):
            return None
        if m.group(2):
            val *= 10_000.0
        return val

    def _extract_electrolyzer_count(self, text: str) -> Optional[int]:
        """提取电解槽台数/套数，例如 46台电解槽、660套电解槽。"""
        if "电解槽" not in text:
            return None
        m = re.search(r"(\d+)\s*(台|套)\s*电解槽", text)
        if not m:
            return None
        try:
            return int(m.group(1))
        except (TypeError, ValueError):
            return None

    def _extract_co2_reduction_tpy(self, text: str) -> Optional[float]:
        """提取年减排二氧化碳量（吨/年）。"""
        m = re.search(
            r"年减排(?:二氧化碳|CO2)?[^。\n]*?(\d+(?:\.\d+)?)(万)?\s*吨",
            text,
            flags=re.IGNORECASE,
        )
        if not m:
            return None
        try:
            val = float(m.group(1))
        except (TypeError, ValueError):
            return None
        if m.group(2):
            val *= 10_000.0
        return val

    def _extract_article_type(self, title: str, text: str) -> str:
        """
        根据标题和正文判断文章类型。
        优先级：招标 > 政策 > 市场 > 项目 > 新闻
        """
        combined = f"{title}\n{text[:500]}" # Check title and beginning of text
        
        # 1. 招标/采购
        if self._has_any(combined, ["招标", "采购", "比选", "中标", "候选人", "询价", "竞谈"]):
            return "招标"
            
        # 2. 政策/规划
        if self._has_any(combined, ["政策", "规划", "通知", "意见", "办法", "标准", "指南", "方案"]):
            # 排除 "建设方案" 等具体项目方案
            if "印发" in combined or "发布" in combined:
                return "政策"
                
        # 3. 市场/分析
        if self._has_any(combined, ["报告", "统计", "分析", "预测", "白皮书", "蓝皮书", "行情"]):
            return "市场"
            
        # 4. 项目 (Default for most collector items)
        # If it has project keywords or stage keywords, it's likely a project
        if self._has_any(combined, self.PROJECT_WORDS) or any(self._has_any(combined, kws) for kws in self.STAGE_KEYWORDS.values()):
            return "项目"
            
        # 5. Default
        return "新闻"



    def _extract_location_fields(self, text: str) -> Tuple[Optional[str], Optional[str], Optional[str]]:
        """基于行政区划字典 + 正则的混合提取."""
        province: Optional[str] = None
        city: Optional[str] = None
        full: Optional[str] = None

        # 1) 先用行政区划字典匹配省
        best_prov = None
        best_prov_pos = -1
        for token, prov in self._province_by_token.items():
            pos = text.find(token)
            if pos == -1:
                continue
            if best_prov is None:
                best_prov = prov
                best_prov_pos = pos
            else:
                # 优先更早出现的；位置相同时，优先 token 更长的
                if pos < best_prov_pos or (pos == best_prov_pos and len(token) >  len(best_prov or "")):
                    best_prov = prov
                    best_prov_pos = pos
        if best_prov:
            province = best_prov

        # 2) 再用行政区划字典匹配市/盟/旗
        best_city = None
        best_city_prov = None
        best_city_pos = -1
        for token, (prov, city_name) in self._city_by_token.items():
            pos = text.find(token)
            if pos == -1:
                continue
            if province and prov != province:
                # 已经识别出省时，只接受同省的城市
                continue
            if best_city is None:
                best_city = city_name
                best_city_prov = prov
                best_city_pos = pos
            else:
                if pos < best_city_pos or (pos == best_city_pos and len(token) > len(best_city or "")):
                    best_city = city_name
                    best_city_prov = prov
                    best_city_pos = pos
        if best_city:
            city = best_city
            if not province:
                province = best_city_prov

        # 3) 如果字典没有识别成功，再退回到原来的正则规则
        if not province or not city:
            m_prov = self.LOCATION_PROVINCE_PATTERN.search(text)
            if m_prov and not province:
                province = m_prov.group(1)
            if province and province in text:
                idx = text.index(province) + len(province)
                search_text = text[idx:]
            else:
                search_text = text
            m_city = self.LOCATION_CITY_PATTERN.search(search_text)
            if m_city and not city:
                city = m_city.group(1)

        # 4) 组装 location 文本
        if province:
            full = province
        if city:
            if full:
                if city not in full:
                    full = full + city
            else:
                full = city
        return province, city, full

    def _extract_energy_type(self, channel_label: str, text: str) -> Optional[str]:
        text_all = (channel_label or "") + " " + text
        hits: List[str] = []
        for etype, kws in self.ENERGY_HINTS.items():
            if self._has_any(text_all, kws):
                hits.append(etype)
        if not hits:
            return None
        if len(hits) == 1:
            return hits[0]
        return "+".join(sorted(set(hits)))

    def _determine_quality(
        self,
        project_name: Optional[str],
        stage: Optional[str],
        capacity_mw: Optional[float],
        investment_cny: Optional[float],
    ) -> str:
        if project_name and stage and (capacity_mw is not None or investment_cny is not None):
            return "A"
        if project_name and (stage or capacity_mw is not None or investment_cny is not None):
            return "B"
        return "C"

    def _process_article_row(self, row: sqlite3.Row, score_threshold: int) -> None:
        url = row["url"]
        title = row["title"] or ""
        channel_id = row["channel_id"] or ""
        channel_label = row["channel_label"] or ""
        published_at = row["published_at"]
        main_text = row["main_text"]

        conn = self._connect()

        if not main_text:
            html = self._fetch_article_html(url)
            if html:
                main_text = self._extract_main_text(html)
                if main_text and main_text.strip():
                    conn.execute(
                        "UPDATE articles SET main_text=?, updated_at=? WHERE url=?",
                        (main_text, dt.datetime.utcnow().isoformat(), url),
                    )
                    conn.commit()
                else:
                    # Empty text after extraction
                    raise ValueError("Empty content after extraction")
            else:
                # Download failed
                raise ValueError("Failed to fetch article HTML (Server Error)")

        if not main_text or not main_text.strip():
             raise ValueError("No content available to process")

        # Validate content length and quality
        text_clean = main_text.strip()
        if len(text_clean) < 50:
            raise ValueError("Content too short (possible error page)")
        
        # Check for error pages
        error_keywords = [
            "403 Forbidden", "404 Not Found", "Nginx", 
            "服务器错误", "Server Error", "500 - "
        ]
        if any(k in text_clean for k in error_keywords):
            raise ValueError("Server Error Content (403/404/500)")

        # Check for "captured footer instead of content"
        # The footer usually starts with "便捷入口" or contains a block of nav links
        if text_clean.startswith("便捷入口") or text_clean.startswith("关于我们"):
            raise ValueError("Invalid Content (Captured Footer/Nav)")
            
        # Check for footer fingerprint (multiple nav keywords appearing together)
        nav_fingerprints = ["关于北极星", "广告服务", "会员服务", "版权所有", "京ICP"]
        if sum(1 for k in nav_fingerprints if k in text_clean) >= 3:
             raise ValueError("Invalid Content (Captured Footer/Nav)")

        head = self._build_text_head(title, main_text or "")
        score, _flags = self._compute_classic_score(title, head)

        worth = 1 if score >= score_threshold else 0

        if not worth:
            conn.execute(
                "UPDATE articles SET classic_score=?, worth_classic=?, classic_quality=? WHERE url=?",
                (score, 0, None, url),
            )
            conn.commit()
            return

        text_for_fields = f"{title}\n{main_text[:1200]}"
        project_name = self._extract_project_name(title, main_text)
        if not project_name:
            conn.execute(
                "UPDATE articles SET classic_score=?, worth_classic=?, classic_quality=? WHERE url=?",
                (score, 0, None, url),
            )
            conn.commit()
            return
        stage = self._extract_stage(text_for_fields)
        capacity_mw = self._extract_capacity_mw(text_for_fields)
        investment_cny = self._extract_investment_cny(text_for_fields)
        owner = self._extract_owner(text_for_fields)
        energy_type = self._extract_energy_type(channel_label, text_for_fields)
        province, city, location = self._extract_location_fields(text_for_fields)
        h2_output_tpy = self._extract_h2_output_tpy(text_for_fields)
        h2_output_nm3_per_h = self._extract_h2_output_nm3_per_h(text_for_fields)
        electrolyzer_count = self._extract_electrolyzer_count(text_for_fields)
        co2_reduction_tpy = self._extract_co2_reduction_tpy(text_for_fields)
        quality = self._determine_quality(project_name, stage, capacity_mw, investment_cny)
        article_type = self._extract_article_type(title, text_for_fields)

        source_type = self._determine_source_type(title, text_for_fields)
        
        projects_to_insert = []

        if source_type == "list":
            # Extract multiple items
            list_items = self._extract_list_items(text_for_fields)
            if not list_items:
                # Fallback to single if extraction fails
                source_type = "single"
            else:
                for item in list_items:
                    # Create a project object for each item
                    # We inherit some fields from the article (url, date, etc.)
                    # But specific fields like capacity/investment might be in the item text
                    # For now, we keep it simple as requested: Name, Overview, Progress
                    
                    # Try to extract capacity/investment from the item text
                    item_cap = self._extract_capacity_mw(item["overview"])
                    item_inv = self._extract_investment_cny(item["overview"])
                    item_stage = self._extract_stage(item["overview"])
                    item_quality = self._determine_quality(item["name"], item_stage, item_cap, item_inv)
                    
                    p = ClassicProject(
                        url=url,
                        channel_id=channel_id,
                        channel_label=channel_label,
                        article_title=title,
                        published_at=published_at,
                        project_name=item["name"],
                        stage=item_stage, # Use item specific stage
                        event_date=published_at,
                        location=location, # Inherit location for now, or extract from item
                        capacity_mw=item_cap,
                        investment_cny=item_inv,
                        product_category=None,
                        owner=None, # Hard to extract owner reliably from short list items
                        energy_type=energy_type,
                        classic_quality=item_quality,
                        province=province,
                        city=city,
                        h2_output_tpy=None,
                        h2_output_nm3_per_h=None,
                        electrolyzer_count=None,
                        co2_reduction_tpy=None,
                        project_summary=None, # Not needed for list items
                        source_type="list_item", # Mark as item of a list
                        project_overview=item["overview"],
                        project_progress=item["progress"],
                        user_note=None,
                        is_ai_improved=False,
                        article_type="项目", # List items are almost always projects
                        numerical_data=None
                    )
                    projects_to_insert.append(p)

        if source_type == "single":
            project_summary = self._generate_summary(text_for_fields)
            # For single project, Overview is the summary, Progress is Stage 
            
            # Initialize numerical_data
            numerical_data = None

            p = ClassicProject(
                url=url,
                channel_id=channel_id,
                channel_label=channel_label,
                article_title=title,
                published_at=published_at,
                project_name=project_name,
                stage=stage,
                event_date=published_at,
                location=location,
                capacity_mw=capacity_mw,
                investment_cny=investment_cny,
                product_category=None,
                owner=owner,
                energy_type=energy_type,
                classic_quality=quality,
                province=province,
                city=city,
                h2_output_tpy=h2_output_tpy,
                h2_output_nm3_per_h=h2_output_nm3_per_h,
                electrolyzer_count=electrolyzer_count,
                co2_reduction_tpy=co2_reduction_tpy,
                project_summary=project_summary,
                source_type="single",
                project_overview=project_summary,
                project_progress=stage,
                user_note=None,
                is_ai_improved=False,
                article_type=article_type,
                numerical_data=numerical_data
            )
            projects_to_insert.append(p)

        with conn:
            for project in projects_to_insert:
                conn.execute(
                    """
                    INSERT OR REPLACE INTO projects_classic (
                        url, channel_id, channel_label, article_title, published_at,
                        project_name, stage, event_date, location,
                        capacity_mw, investment_cny, product_category,
                        owner, energy_type, classic_quality,
                        province, city,
                        h2_output_tpy, h2_output_nm3_per_h, electrolyzer_count, co2_reduction_tpy,
                        project_summary, source_type, project_overview, project_progress,
                        user_note, is_ai_improved, article_type, numerical_data
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        project.url,
                        project.channel_id,
                        project.channel_label,
                        project.article_title,
                        project.published_at,
                        project.project_name,
                        project.stage,
                        project.event_date,
                        project.location,
                        project.capacity_mw,
                        project.investment_cny,
                        project.product_category,
                        project.owner,
                        project.energy_type,
                        project.classic_quality,
                        project.province,
                        project.city,
                        project.h2_output_tpy,
                        project.h2_output_nm3_per_h,
                        project.electrolyzer_count,
                        project.co2_reduction_tpy,
                        project.project_summary,
                        project.source_type,
                        project.project_overview,
                        project.project_progress,
                        project.user_note,
                        project.is_ai_improved,
                        project.article_type,
                        project.numerical_data
                    ),
                )
            conn.execute(
                "UPDATE articles SET classic_score=?, worth_classic=?, classic_quality=? WHERE url=?",
                (score, 1, quality, url),
            )

    def _prefetch_main_text(self, rows: List[Dict[str, Any]], max_workers: int = 10) -> None:
        urls_to_fetch = [
            row["url"]
            for row in rows
            if not (row.get("main_text") or "").strip()
        ]
        if not urls_to_fetch:
            return

        def worker(u: str) -> Tuple[str, Optional[str]]:
            html = self._fetch_article_html(u)
            if not html:
                return u, None
            text = self._extract_main_text(html)
            return u, text or None

        results: Dict[str, str] = {}
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            future_map = {executor.submit(worker, u): u for u in urls_to_fetch}
            for fut in as_completed(future_map):
                url, text = fut.result()
                if text:
                    results[url] = text

        if not results:
            return

        conn = self._connect()
        now = dt.datetime.utcnow().isoformat()
        with conn:
            for url, text in results.items():
                conn.execute(
                    "UPDATE articles SET main_text=?, updated_at=? WHERE url=?",
                    (text, now, url),
                )
        # 更新内存中的行，避免后续再次发起请求
        for row in rows:
            url = row.get("url")
            if url in results:
                row["main_text"] = results[url]

    def run(
        self,
        max_articles: Optional[int] = None,
        score_threshold: int = 2,
        max_workers: int = 10,
    ) -> None:
        conn = self._connect()
        cur = conn.cursor()
        params: List[Any] = []
        sql = """
        SELECT url, channel_id, channel_label, title, published_at,
               COALESCE(main_text, '') AS main_text
        FROM articles
        WHERE worth_classic IS NULL OR worth_classic = 0
        ORDER BY published_at DESC, created_at DESC
        """
        if max_articles and max_articles > 0:
            sql += " LIMIT ?"
            params.append(max_articles)
        cur.execute(sql, params)
        # 将 Row 转为可变 dict，方便预取阶段回填 main_text
        rows = [dict(r) for r in cur.fetchall()]
        total = len(rows)
        # 并发预取正文，避免后续逐篇阻塞
        self._prefetch_main_text(rows, max_workers=max_workers)
        
        server_errors = 0
        sample_error_url = None
        
        for idx, row in enumerate(rows, start=1):
            self._emit(
                stage="running",
                message=f"经典规则提取项目中 (北极星内容源错误: {server_errors})",
                current=idx - 1,
                total=total,
                last_error_url=sample_error_url,
            )
            try:
                self._process_article_row(row, score_threshold=score_threshold)
            except ValueError as e:
                # This catches "Content too short", "Server Error Content", "Invalid Content"
                server_errors += 1
                if not sample_error_url:
                    sample_error_url = row.get("url")
                # Optional: Log detailed error for debugging
                # print(f"Skipping article {row.get('url')}: {e}")
            except Exception as e:
                # Catch unexpected errors too
                server_errors += 1
                if not sample_error_url:
                    sample_error_url = row.get("url")
                import traceback
                traceback.print_exc()
                print(f"Error processing article {row.get('url')}: {e}")
        self._emit(
            stage="idle",
            message=f"经典规则提取完成 (北极星内容源错误: {server_errors})",
            current=total,
            total=total,
            last_error_url=sample_error_url,
        )

if __name__ == "__main__":
    extractor = ClassicProjectExtractor()
    extractor.run(max_articles=0)  # 0 means unlimited
