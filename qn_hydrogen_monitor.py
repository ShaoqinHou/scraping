import dataclasses
import datetime as dt
import re
import sqlite3
import threading
from typing import Any, Callable, Dict, List, Optional

from urllib.parse import urljoin
from pathlib import Path

import requests
from bs4 import BeautifulSoup

BASE_DIR = Path(__file__).resolve().parent
DEFAULT_DB_PATH = str(BASE_DIR / "qn_hydrogen_monitor.db")


HydrogenProgressCallback = Callable[..., None]


@dataclasses.dataclass
class HydrogenChannelConfig:
    id: str
    label: str
    path: str
    content_type: str
    enabled: bool = True

    def to_dict(self) -> Dict[str, Any]:
        return dataclasses.asdict(self)


def get_default_hydrogen_channels() -> List[HydrogenChannelConfig]:
    return [
        HydrogenChannelConfig(
            id="qn_xm",
            label="氢能项目",
            path="xm",
            content_type="project",
            enabled=True,
        ),
        HydrogenChannelConfig(
            id="qn_zb",
            label="氢能招标",
            path="zb",
            content_type="tender",
            enabled=True,
        ),
        HydrogenChannelConfig(
            id="qn_zq",
            label="制氢",
            path="zq",
            content_type="news",
            enabled=False,
        ),
        HydrogenChannelConfig(
            id="qn_qcy",
            label="氢储运",
            path="qcy",
            content_type="news",
            enabled=False,
        ),
        HydrogenChannelConfig(
            id="qn_qnjt",
            label="氢能交通",
            path="qnjt",
            content_type="news",
            enabled=False,
        ),
        HydrogenChannelConfig(
            id="qn_jqz",
            label="加氢站",
            path="jqz",
            content_type="news",
            enabled=False,
        ),
        HydrogenChannelConfig(
            id="qn_qnwzh",
            label="氢能综合利用",
            path="qnwzh",
            content_type="news",
            enabled=False,
        ),
    ]


class QNHydrogenMonitor:
    BASE_URL = "https://qn.bjx.com.cn/"

    def __init__(
        self,
        channels: List[HydrogenChannelConfig],
        db_path: str = DEFAULT_DB_PATH,
        progress_callback: Optional[HydrogenProgressCallback] = None,
        session: Optional[requests.Session] = None,
    ) -> None:
        self.channels = channels
        self.db_path = db_path
        self.progress_callback = progress_callback or (lambda **_: None)
        self.session = session or requests.Session()
        self._lock = threading.Lock()
        self._conn: Optional[sqlite3.Connection] = None

    def _connect(self) -> sqlite3.Connection:
        def _open() -> sqlite3.Connection:
            conn = sqlite3.connect(self.db_path, check_same_thread=False)
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
            # Ensure optional columns used by classic extractor exist
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
            conn.commit()
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

    def _emit(self, **info: Any) -> None:
        with self._lock:
            self.progress_callback(**info)

    def _build_list_url(self, path: str, page: int) -> str:
        if page <= 1:
            return urljoin(self.BASE_URL, f"{path}/")
        return urljoin(self.BASE_URL, f"{path}/{page}/")

    def _fetch(self, url: str) -> Optional[str]:
        try:
            resp = self.session.get(url, timeout=15)
            resp.raise_for_status()
            resp.encoding = resp.apparent_encoding or resp.encoding
            return resp.text
        except Exception as exc:
            self._emit(
                stage="error",
                message=f"请求失败: {url} ({exc})",
                current=0,
                total=0,
            )
            return None

    def _parse_list_page(
        self,
        html: str,
        base_url: str,
    ) -> List[Dict[str, Optional[str]]]:
        soup = BeautifulSoup(html, "html.parser")
        anchors = soup.select('a[href$=".shtml"], a[href*="/html/"]')
        seen = set()
        results: List[Dict[str, Optional[str]]] = []
        for a in anchors:
            href = a.get("href")
            if not href:
                continue
            full_url = urljoin(base_url, href)
            if full_url in seen:
                continue
            seen.add(full_url)
            title = a.get_text(strip=True)
            if not title:
                continue
            container = a.find_parent(["li", "article", "div"]) or a
            text = container.get_text(" ", strip=True)
            m = re.search(r"(\d{4}-\d{2}-\d{2})", text)
            published_at = m.group(1) if m else None
            results.append(
                {
                    "url": full_url,
                    "title": title,
                    "published_at": published_at,
                }
            )
        return results

    def _insert_new_articles(
        self,
        channel: HydrogenChannelConfig,
        items: List[Dict[str, Optional[str]]],
    ) -> int:
        if not items:
            return 0
        conn = self._connect()
        now = dt.datetime.utcnow().isoformat()
        new_count = 0
        with conn:
            for item in items:
                url = item["url"]
                title = item["title"] or ""
                published_at = item.get("published_at")
                try:
                    cur = conn.execute(
                        """
                        INSERT OR IGNORE INTO articles (
                            url, channel_id, channel_label, title,
                            published_at, created_at, updated_at
                        ) VALUES (?, ?, ?, ?, ?, ?, ?)
                        """,
                        (
                            url,
                            channel.id,
                            channel.label,
                            title,
                            published_at,
                            now,
                            now,
                        ),
                    )
                    if cur.rowcount and cur.rowcount > 0:
                        new_count += 1
                except sqlite3.Error:
                    continue
        return new_count

    def _count_articles(self) -> int:
        conn = self._connect()
        cur = conn.execute("SELECT COUNT(*) FROM articles")
        row = cur.fetchone()
        return int(row[0]) if row and row[0] is not None else 0

    def run_once(
        self,
        max_new_articles: Optional[int] = None,
        max_pages_per_channel: Optional[int] = None,
    ) -> None:
        max_new_articles = max_new_articles if (max_new_articles and max_new_articles > 0) else None
        max_pages_per_channel = max_pages_per_channel if (max_pages_per_channel and max_pages_per_channel > 0) else None
        pages_without_new_limit = 3
        self._emit(
            stage="running",
            message="氢能频道监控启动",
            current=0,
            total=0,
            channel_id=None,
            channel_label=None,
            page=0,
            new_in_page=0,
            new_in_run=0,
            total_in_db=self._count_articles(),
        )
        new_in_run = 0
        for channel in self.channels:
            if not channel.enabled:
                continue
            page = 1
            pages_without_new = 0
            while True:
                if max_pages_per_channel and page > max_pages_per_channel:
                    break
                list_url = self._build_list_url(channel.path, page)
                html = self._fetch(list_url)
                if not html:
                    break
                items = self._parse_list_page(html, list_url)
                new_in_page = self._insert_new_articles(channel, items)
                new_in_run += new_in_page
                total_in_db = self._count_articles()
                self._emit(
                    stage="running",
                    message=f"抓取频道【{channel.label}】第 {page} 页",
                    current=new_in_run,
                    total=max_new_articles or 0,
                    channel_id=channel.id,
                    channel_label=channel.label,
                    page=page,
                    new_in_page=new_in_page,
                    new_in_run=new_in_run,
                    total_in_db=total_in_db,
                )
                if max_new_articles and new_in_run >= max_new_articles:
                    return
                if not items or new_in_page == 0:
                    pages_without_new += 1
                else:
                    pages_without_new = 0
                # If user explicitly sets max_pages_per_channel, honor it even when no new items are found.
                # Otherwise stop after pages_without_new_limit consecutive empty pages.
                if not max_pages_per_channel and pages_without_new >= pages_without_new_limit:
                    break
                page += 1
        total_in_db = self._count_articles()
        self._emit(
            stage="idle",
            message="本轮氢能频道监控完成",
            current=new_in_run,
            total=max_new_articles or 0,
            channel_id=None,
            channel_label=None,
            page=0,
            new_in_page=0,
            new_in_run=new_in_run,
            total_in_db=total_in_db,
        )
