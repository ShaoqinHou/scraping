import asyncio
import datetime as dt
import sqlite3
from typing import Any, Callable, Dict, List, Optional

from playwright.async_api import async_playwright


TextProgressCallback = Callable[..., None]


async def _extract_main_text_from_page(page) -> str:
    """Try to extract the main article text from a bjx news page."""
    script = """
    () => {
      const body = document.body;
      if (!body) return null;
      const selectors = [
        'div.article-body',
        'div.article',
        'div#content',
        'div.content',
        'div#ArticleContent',
        'div.newsContent',
        'div#NewsContent',
        'div.detailcon',
        'div.text',
        'article'
      ];
      for (const sel of selectors) {
        const el = document.querySelector(sel);
        if (el) {
          return { selector: sel, text: el.innerText || '' };
        }
      }
      let bestNode = null;
      let bestLen = 0;
      const walker = document.createTreeWalker(body, NodeFilter.SHOW_ELEMENT, null);
      while (walker.nextNode()) {
        const el = walker.currentNode;
        const rect = el.getBoundingClientRect();
        if (rect.width < 200 || rect.height < 50) continue;
        const t = el.innerText;
        if (t && t.trim().length > bestLen) {
          bestLen = t.trim().length;
          bestNode = el;
        }
      }
      if (bestNode) {
        return { selector: bestNode.tagName.toLowerCase(), text: bestNode.innerText || '' };
      }
      return null;
    }
    """
    try:
        result: Optional[Dict[str, Any]] = await page.evaluate(script)
    except Exception:
        result = None
    if isinstance(result, dict):
        text = (result.get("text") or "").strip()
        if text:
            return text
    try:
        full = await page.inner_text("body")
    except Exception:
        return ""
    return (full or "").strip()


async def _run_playwright_fetch(
    urls: List[str],
    headless: bool,
    max_concurrent: int,
    progress_callback: TextProgressCallback,
) -> Dict[str, str]:
    results: Dict[str, str] = {}
    if not urls:
        return results

    async with async_playwright() as pw:
        browser = await pw.chromium.launch(headless=headless)
        context = await browser.new_context(
            user_agent=(
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                "AppleWebKit/537.36 (KHTML, like Gecko) "
                "Chrome/126.0.0.0 Safari/537.36"
            ),
            locale="zh-CN",
        )
        sem = asyncio.Semaphore(max_concurrent)
        total = len(urls)

        async def worker(idx: int, url: str) -> None:
            async with sem:
                try:
                    progress_callback(
                        stage="fetch_text",
                        message="抓取氢能文章正文中",
                        current=idx,
                        total=total,
                    )
                except Exception:
                    pass
                page = await context.new_page()
                try:
                    response = await page.goto(url, wait_until="domcontentloaded", timeout=10000)
                    if response and response.status >= 400:
                        # Skip text extraction for error pages (403, 404, 500, etc.)
                        # This avoids waiting or processing useless content.
                        results[url] = "" # Mark as empty so we don't retry immediately? Or just skip?
                        # If we skip (don't add to results), it might be retried.
                        # But the caller logic in ClassicProjectExtractor handles "Empty content" as an error.
                        # So let's NOT add it to results, so it's treated as "failed to fetch".
                        pass
                    else:
                        text = await _extract_main_text_from_page(page)
                        if text:
                            results[url] = text
                except Exception:
                    # Ignore individual failures; they can be retried later
                    pass
                finally:
                    try:
                        await page.close()
                    except Exception:
                        pass

        tasks = [worker(i + 1, u) for i, u in enumerate(urls)]
        await asyncio.gather(*tasks)
        await context.close()
        await browser.close()
    return results


def fetch_missing_article_texts(
    db_path: str = "qn_hydrogen_monitor.db",
    headless: bool = True,
    max_concurrent: int = 4,
    max_articles: Optional[int] = None,
    progress_callback: Optional[TextProgressCallback] = None,
) -> None:
    """Fill missing or placeholder main_text for bjx hydrogen articles using Playwright."""
    progress = progress_callback or (lambda **_: None)
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    try:
        cur = conn.cursor()
        sql = """
            SELECT url
            FROM articles
            WHERE (main_text IS NULL
                   OR TRIM(main_text) = ''
                   OR main_text LIKE '%登录注册%')
              AND (worth_classic IS NULL OR worth_classic = 0)
            ORDER BY published_at DESC, created_at DESC
        """
        params: List[Any] = []
        if max_articles and max_articles > 0:
            sql += " LIMIT ?"
            params.append(int(max_articles))
        cur.execute(sql, params)
        urls = [row["url"] for row in cur.fetchall()]
    except sqlite3.Error:
        urls = []
    finally:
        conn.close()

    if not urls:
        progress(
            stage="fetch_text",
            message="暂无需要抓取正文的氢能文章",
            current=0,
            total=0,
        )
        return

    try:
        progress(
            stage="fetch_text",
            message=f"准备抓取 {len(urls)} 篇氢能文章正文",
            current=0,
            total=len(urls),
        )
    except Exception:
        pass

    results = asyncio.run(
        _run_playwright_fetch(
            urls=urls,
            headless=headless,
            max_concurrent=max_concurrent,
            progress_callback=progress,
        )
    )

    if not results:
        progress(
            stage="fetch_text",
            message="未成功抓取到新的正文内容",
            current=0,
            total=len(urls),
        )
        return

    now = dt.datetime.utcnow().isoformat()
    conn = sqlite3.connect(db_path)
    try:
        cur = conn.cursor()
        for url, text in results.items():
            try:
                cur.execute(
                    "UPDATE articles SET main_text=?, updated_at=? WHERE url=?",
                    (text, now, url),
                )
            except sqlite3.Error:
                continue
        conn.commit()
    except sqlite3.Error:
        conn.rollback()
    finally:
        conn.close()

    progress(
        stage="fetch_text",
        message=f"正文抓取完成，更新 {len(results)} 篇文章",
        current=len(results),
        total=len(urls),
    )
