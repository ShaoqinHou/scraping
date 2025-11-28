import argparse
import asyncio
import csv
from bs4 import BeautifulSoup
import time
import urllib.parse
from playwright.async_api import async_playwright
import os
from datetime import datetime
from captcha_manager import CaptchaManager, start_standalone_captcha_server

# Configuration
BASE_URL = "https://fgw.nmg.gov.cn"
PROJECT_LIST_URL = f"{BASE_URL}/tzxm/tzsp/VDEdfsef.jspx"
MAX_CONCURRENT_EXTRACTIONS = 5
RETRY_ATTEMPTS = 3
PAGES_PER_SECOND = 5  # Rate of page opening
PAGE_OPEN_INTERVAL = 1.0 / PAGES_PER_SECOND  # Interval between page openings
PAGE_LOAD_WAIT = 3  # Wait time after opening a page before extraction
MAX_OPEN_TABS = 30  # Maximum number of concurrently open tabs
TABLE_SELECTOR_TIMEOUT = 20000  # ms to wait for table
PER_PAGE_TASK_TIMEOUT = 150  # seconds; hard cap per page task to avoid hangs
PAGE_CLOSE_TIMEOUT = 10  # seconds; hard cap when closing a tab

class ProjectCollector:
    def __init__(self, retry_failed_only=False, max_pages=None, headless=False,
                 captcha_manager: CaptchaManager | None = None,
                 progress_callback=None,
                 max_open_tabs=None,
                 pages_per_second=None):
        self.browser = None
        self.context = None
        self.captcha_token = None
        self.total_pages = 0
        self.processed_pages = 0
        self.failed_pages = []
        self.collected_data = []
        self.open_pages_count = 0  # Track number of open pages
        self.page_lock = asyncio.Lock()  # Lock for thread-safe operations on open_pages_count
        self.retry_failed_only = retry_failed_only
        self.max_pages = max_pages
        self.headless = headless
        self.start_time = None
        self.end_time = None
        self.captcha_page = None
        self.captcha_manager = captcha_manager or CaptchaManager()
        self._own_captcha = captcha_manager is None
        self._captcha_thread = None
        if self._own_captcha:
            self._captcha_thread = start_standalone_captcha_server(self.captcha_manager)
        self.progress_callback = progress_callback or (lambda **kwargs: None)
        self.pages_completed = 0
        self.max_open_tabs = max_open_tabs if (max_open_tabs and max_open_tabs > 0) else MAX_OPEN_TABS
        self.pages_per_second = pages_per_second if (pages_per_second and pages_per_second > 0) else PAGES_PER_SECOND
        self.page_open_interval = (1.0 / self.pages_per_second) if self.pages_per_second else 0
        self.table_timeout = TABLE_SELECTOR_TIMEOUT
        # Hard cap for any single page task (network / Playwright safety net)
        self.page_task_timeout = PER_PAGE_TASK_TIMEOUT
        # Hard cap for closing a single page
        self.page_close_timeout = PAGE_CLOSE_TIMEOUT

    async def setup_browser(self):
        """Setup browser with proper configuration"""
        playwright = await async_playwright().start()
        self.browser = await playwright.chromium.launch(headless=self.headless)
        self.context = await self.browser.new_context(
            user_agent='Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/126.0.0.0 Safari/537.36'
        )
        return True

    def _report(self, stage, message, current=None, total=None):
        try:
            self.progress_callback(stage=stage, message=message, current=current, total=total)
        except Exception:
            pass

    async def _run_with_timeout(self, coro, page_num: int):
        """
        Wrap a per-page coroutine with an overall timeout so that a stuck Playwright
        call (e.g. navigation/close) cannot block the whole run forever.
        """
        try:
            if self.page_task_timeout:
                return await asyncio.wait_for(coro, timeout=self.page_task_timeout)
            return await coro
        except asyncio.TimeoutError:
            print(f"Overall timeout for page {page_num} after {self.page_task_timeout} seconds; marking as failed.")
            return []
        except Exception as e:
            # process_page / fetch_page already log their own errors; this is a final safety net
            print(f"Unexpected error in wrapped task for page {page_num}: {e}")
            return []

    async def get_captcha_token_from_browser(self):
        """Open browser for user to solve CAPTCHA and get token from URL"""
        print("Setting up CAPTCHA流程...")
        if self._own_captcha:
            print("本地验证码页面: http://127.0.0.1:5000")
        
        print("Opening browser for CAPTCHA solving...")
        self._report("captcha", "打开登录页面，准备获取验证码", current=0, total=self.total_pages or 0)
        
        # Create a new page
        page = await self.context.new_page()
        self.captcha_page = page  # Store reference for later use
        
        # Navigate to main page (allow extra time for slow overseas connections)
        await page.goto(
            f"{BASE_URL}/tzxm?isYjs=0&record=istrue",
            timeout=90000,
            wait_until="commit"
        )
        # Wait until the "更多>>" links are available before interacting
        await page.wait_for_selector("a[onclick*='click_more']", timeout=60000)
        
        # Find the correct "更多" link by examining the DOM structure
        print("Finding and clicking on '更多>>' link for 项目办理结果公示...")
        
        # Get all links with onclick containing 'click_more'
        more_links = await page.query_selector_all("a[onclick*='click_more']")
        
        # Find the one associated with 项目办理结果公示
        target_link = None
        for link in more_links:
            # Get the parent elements to check if this is the right section
            parent = await link.query_selector("xpath=../..")
            if parent:
                # Check if this parent contains the text "项目办理结果公示"
                inner_html = await parent.inner_html()
                if "项目办理结果公示" in inner_html:
                    target_link = link
                    break
        
        if target_link:
            await target_link.click()
        else:
            # Fallback: try clicking the first one (index 0)
            print("Could not find the specific link, clicking the first '更多>>' link")
            if more_links:
                await more_links[0].click()
            else:
                print("No '更多>>' links found")
                return False
        
        # Wait a bit for the modal to appear
        await page.wait_for_timeout(1000)
        
        # Click on the captcha input field to trigger captcha image display
        print("Clicking on captcha input field...")
        await page.click("#captcha")
        
        # Wait for the CAPTCHA image to load
        await page.wait_for_timeout(2000)
        
        # Try to capture the CAPTCHA image
        print("Capturing CAPTCHA image...")
        captcha_element = await page.query_selector("#captcha-img")
        if not captcha_element:
            # Try alternative selectors
            captcha_element = await page.query_selector("img[src*='captcha']")
        
        if captcha_element:
            captcha_image_bytes = await captcha_element.screenshot()
            self.captcha_manager.set_image(captcha_image_bytes)
            self._report("captcha", "验证码已生成，请在页面输入", current=0, total=self.total_pages or 0)
            print("CAPTCHA image sent to server. Please go to http://localhost:5000 to enter the code.")
        else:
            print("Could not find CAPTCHA image element. Please solve CAPTCHA manually.")
            return await self.manual_captcha_entry(page)
        
        # Wait for CAPTCHA to be solved either via web UI (127.0.0.1)
        # or directly inside the browser (user clicks并完成验证码)
        print("Waiting for CAPTCHA to be solved (网页或浏览器均可)...")
        start_time = time.time()
        submitted_code = False

        while True:
            # 1) Detect successful navigation first (user may have solved directly in browser)
            current_url = page.url
            if "VDEdfsef.jspx" in current_url and "captcha=" in current_url:
                print("Navigation to project list page detected after CAPTCHA.")
                parsed_url = urllib.parse.urlparse(current_url)
                query_params = urllib.parse.parse_qs(parsed_url.query)
                captcha_token = query_params.get('captcha', [None])[0]
                if captcha_token:
                    self.captcha_token = captcha_token
                    print(f"CAPTCHA token extracted: {captcha_token}")
                    break

            # 2) If we have not yet submitted a code, try to get one from the web UI (non‑blocking)
            if not submitted_code:
                captcha_code = self.captcha_manager.wait_for_code(timeout=0.1)
                if captcha_code:
                    print(f"Submitting CAPTCHA code from web UI: {captcha_code}")
                    await page.fill("#captcha", captcha_code)
                    self._report("captcha", "验证码已提交，等待跳转", current=0, total=self.total_pages or 0)
                    try:
                        await page.evaluate("checkCaptcha(0)")
                        print("Executed checkCaptcha(0) function")
                    except Exception as e:
                        print(f"Failed to execute checkCaptcha function: {e}")
                    submitted_code = True

            # 3) Timeout safety
            if time.time() - start_time > 300:
                print("Timeout waiting for CAPTCHA solution. 请确认验证码是否已正确完成。")
                return False

            await asyncio.sleep(0.5)

        # Get total number of pages
        content = await page.content()
        self.total_pages = self.get_total_pages(content)
        if self.total_pages > 0:
            print(f"Total pages detected: {self.total_pages}")
            self._report("detected_pages", f"检测到 {self.total_pages} 页", current=0, total=self.total_pages)
        else:
            # Fallback to default if we can't detect
            self.total_pages = 1811
            print(f"Could not detect total pages, using default: {self.total_pages}")
            self._report("detected_pages", f"使用默认页数 {self.total_pages}", current=0, total=self.total_pages)
        
        return True

    async def manual_captcha_entry(self, page):
        """Fallback method for manual CAPTCHA entry"""
        print("Manual CAPTCHA entry required.")
        print("Please solve the CAPTCHA and click '确定' button.")
        print("Once you've done that, the script will automatically continue.")
        self._report("captcha", "等待手动输入验证码", current=0, total=self.total_pages or 0)
        
        # Instead of waiting for a specific URL pattern, we'll wait for the user to indicate they're done
        # by checking if we're on the project list page
        start_time = time.time()
        while True:
            current_url = page.url
            # Check if we're on the project list page
            if "VDEdfsef.jspx" in current_url and "captcha=" in current_url:
                print("Detected successful CAPTCHA submission!")
                # Extract captcha token from URL
                parsed_url = urllib.parse.urlparse(current_url)
                query_params = urllib.parse.parse_qs(parsed_url.query)
                captcha_token = query_params.get('captcha', [None])[0]
                if captcha_token:
                    self.captcha_token = captcha_token
                    print(f"CAPTCHA token extracted: {captcha_token}")
                    self._report("captcha", "验证码成功，准备采集", current=0, total=self.total_pages or 0)
                break
            
            # Also check for timeout (5 minutes)
            if time.time() - start_time > 300:  # 5 minutes
                print("Timeout waiting for CAPTCHA submission. Please make sure you've completed the CAPTCHA.")
                return False
                
            # Wait a bit before checking again
            await asyncio.sleep(1)
        
        return True

    def get_total_pages(self, html_content):
        """Extract total number of pages from the pagination info"""
        try:
            soup = BeautifulSoup(html_content, 'html.parser')
            # Look for pagination text like "共 18102 条&nbsp;&nbsp;&nbsp;&nbsp; 每页 10 条"
            pagination_text = soup.find(string=lambda text: text and '共' in text and '条' in text)
            if pagination_text:
                # Extract total items and calculate pages
                import re
                match = re.search(r'共\s*(\d+)\s*条', pagination_text)
                if match:
                    total_items = int(match.group(1))
                    # 10 items per page
                    return (total_items + 9) // 10  # Ceiling division
            return 0
        except Exception as e:
            print(f"Error detecting total pages: {e}")
            return 0

    async def collect_all_projects(self):
        """Collect projects from all pages using continuous approach with strict tab limit"""
        if not self.captcha_token:
            print("No CAPTCHA token available.")
            return
        
        self.start_time = time.time()
        
        # Determine which pages to process
        if self.retry_failed_only and os.path.exists("failed_pages.txt"):
            # Read failed pages from file
            with open("failed_pages.txt", "r") as f:
                pages_to_process = [int(line.strip()) for line in f.readlines()]
            print(f"Retrying {len(pages_to_process)} failed pages...")
        else:
            # Process all pages
            pages_to_process = list(range(1, self.total_pages + 1))
            if self.max_pages:
                pages_to_process = pages_to_process[:self.max_pages]
                print(f"Processing first {len(pages_to_process)} pages (max-pages={self.max_pages})...")
            else:
                print(f"Processing all {len(pages_to_process)} pages...")
        
        self.processed_pages = len(pages_to_process)
        
        if not pages_to_process:
            print("No pages to process after applying filters.")
            return

        self._report("running", f"开始采集 {self.processed_pages} 页", current=0, total=self.processed_pages)
        
        # Semaphore to limit concurrent extractions
        extraction_semaphore = asyncio.Semaphore(MAX_CONCURRENT_EXTRACTIONS)
        
        # Track page processing tasks
        task_map = {}
        
        # Start opening pages at the specified rate with strict tab limit
        for i, page_num in enumerate(pages_to_process):
            # Wait until we have fewer than configured tabs open
            while True:
                async with self.page_lock:
                    if self.open_pages_count < self.max_open_tabs:
                        self.open_pages_count += 1
                        break
                await asyncio.sleep(0.05)  # Check every 50ms
            
            # Calculate delay to maintain the desired rate
            elapsed = time.time() - self.start_time
            if self.page_open_interval:
                expected_time = i * self.page_open_interval
                if elapsed < expected_time:
                    await asyncio.sleep(expected_time - elapsed)
            
            # Open page and schedule extraction (with hard timeout wrapper)
            task = asyncio.create_task(
                self._run_with_timeout(self.process_page(extraction_semaphore, page_num), page_num)
            )
            task_map[task] = page_num
            print(f"Opened page {page_num} (Open tabs: {self.open_pages_count})")
        
        # Wait for all tasks to complete, handling them as they finish
        pages_done = 0
        pending = set(task_map.keys())
        while pending:
            done, pending = await asyncio.wait(pending, return_when=asyncio.FIRST_COMPLETED)
            for task in done:
                page_num = task_map.get(task, None)
                if page_num is None:
                    # Should not happen, but log just in case
                    print(f"[DEBUG] Completed task not found in task_map: {task}")
                    continue
                try:
                    result = await task
                    print(f"[DEBUG] Task completed for page {page_num}, "
                          f"type={type(result).__name__}, "
                          f"has_data={bool(result)}")
                    if isinstance(result, Exception):
                        raise result
                    if result:
                        self.collected_data.extend(result)
                    else:
                        self.failed_pages.append(page_num)
                except Exception as exc:
                    print(f"Task for page {page_num} failed with exception: {exc}")
                    self.failed_pages.append(page_num)
                pages_done += 1
                self.pages_completed = pages_done
                self._report("progress", f"采集中...", current=pages_done, total=self.processed_pages)
        
        self.end_time = time.time()
        
        if not self.retry_failed_only:
            print(f"Initial collection complete. Total projects collected: {len(self.collected_data)}")
            print(f"Initial failed pages: {len(self.failed_pages)}")
            
            # Retry failed pages
            if self.failed_pages:
                await self.retry_failed_pages()
        else:
            print(f"Retry of failed pages complete. Total projects collected: {len(self.collected_data)}")
            print(f"Remaining failed pages: {len(self.failed_pages)}")

    async def process_page(self, extraction_semaphore, page_num):
        """Open a page and schedule its extraction after a delay"""
        page = None
        try:
            print(f"[DEBUG] process_page start for page {page_num}")
            if not self.captcha_token:
                print("No CAPTCHA token available.")
                return []
                
            url = f"{PROJECT_LIST_URL}?projectname=&pageNo={page_num}&captcha={self.captcha_token}"
            
            # Create a new page for this request
            page = await self.context.new_page()
            print(f"[DEBUG] New tab opened for page {page_num}")
            
            response = await page.goto(
                url,
                timeout=90000,
                wait_until="commit"
            )
            print(f"[DEBUG] page.goto finished for page {page_num} with status {response.status if response else 'None'} url={page.url}")
            if response.status != 200:
                print(f"Failed to open page {page_num}. Status: {response.status}")
                return []
            if "VDEdfsef" not in page.url:
                print(f"Unexpected redirect for page {page_num}: {page.url}")
                return []
            
            # Wait only for the results table instead of the full load event
            await page.wait_for_selector("table.table_fix", timeout=self.table_timeout)
            print(f"[DEBUG] table selector appeared on page {page_num}")
            
            # Allow a brief pause for stability before scraping
            await asyncio.sleep(PAGE_LOAD_WAIT)
            
            # Extract data with semaphore limiting concurrent extractions
            async with extraction_semaphore:
                content = await page.content()
                print(f"[DEBUG] page.content acquired for page {page_num}")
                return self.parse_page(content, page_num)
        except Exception as e:
            print(f"Error processing page {page_num}: {str(e)}")
            return []
        finally:
            if page:
                print(f"[DEBUG] Closing tab for page {page_num} (close timeout={self.page_close_timeout}s)")
                if self.page_close_timeout:
                    try:
                        await asyncio.wait_for(page.close(), timeout=self.page_close_timeout)
                        print(f"[DEBUG] Tab closed for page {page_num}")
                    except asyncio.TimeoutError:
                        print(f"[DEBUG] Timeout closing tab for page {page_num} after "
                              f"{self.page_close_timeout}s; continuing without waiting")
                    except Exception as e:
                        print(f"[DEBUG] Exception while closing tab for page {page_num}: {e}")
                else:
                    try:
                        await page.close()
                        print(f"[DEBUG] Tab closed for page {page_num} (no timeout)")
                    except Exception as e:
                        print(f"[DEBUG] Exception while closing tab for page {page_num}: {e}")
            async with self.page_lock:
                if self.open_pages_count > 0:
                    self.open_pages_count -= 1
            print(f"[DEBUG] process_page finished for page {page_num}, open_pages_count={self.open_pages_count}")

    def parse_page(self, html_content, page_num):
        """Parse the HTML content of a page and extract project data including cbsnum"""
        try:
            soup = BeautifulSoup(html_content, 'html.parser')
            table = soup.find('table', class_='table_fix')
            
            if not table:
                print(f"No table found on page {page_num}")
                return []
            
            projects = []
            rows = table.find('tbody').find_all('tr')
            
            for row in rows:
                cells = row.find_all('td')
                if len(cells) >= 4:
                    # Extract project code and cbsnum
                    code_element = cells[0].find('a')
                    code = code_element.text.strip() if code_element else ""
                    
                    # Extract cbsnum from onclick attribute
                    cbsnum = ""
                    if code_element and code_element.get('onclick'):
                        onclick = code_element.get('onclick')
                        # Extract cbsnum from onclick="getDetail('cbsnum')"
                        import re
                        match = re.search(r"getDetail\('([^']+)'", onclick)
                        if match:
                            cbsnum = match.group(1)
                    
                    # Extract project name
                    name = cells[1].text.strip() if cells[1] else ""
                    
                    # Extract approval item
                    approval = cells[2].text.strip() if cells[2] else ""
                    
                    # Extract approval result
                    result = cells[3].text.strip() if cells[3] else ""
                    
                    projects.append({
                        'page': page_num,
                        'code': code,
                        'cbsnum': cbsnum,  # Include cbsnum for detailed info access
                        'name': name,
                        'approval': approval,
                        'result': result
                    })
            
            print(f"Parsed {len(projects)} projects from page {page_num}")
            return projects
        except Exception as e:
            print(f"Error parsing page {page_num}: {str(e)}")
            return []

    async def retry_failed_pages(self):
        """Retry failed pages up to RETRY_ATTEMPTS times"""
        remaining_failures = self.failed_pages.copy()
        self.failed_pages.clear()
        
        for attempt in range(RETRY_ATTEMPTS):
            if not remaining_failures:
                break
                
            print(f"Retry attempt {attempt+1}/{RETRY_ATTEMPTS} for {len(remaining_failures)} failed pages...")
            self._report("retry", f"重试 {len(remaining_failures)} 页 (第 {attempt+1} 次)", current=self.pages_completed, total=self.processed_pages)
            
            retry_tasks = {}
            start_time = time.time()
            
            for i, page_num in enumerate(remaining_failures):
                while True:
                    async with self.page_lock:
                        if self.open_pages_count < self.max_open_tabs:
                            self.open_pages_count += 1
                            break
                    await asyncio.sleep(0.05)
                elapsed = time.time() - start_time
                if self.page_open_interval:
                    expected_time = i * self.page_open_interval
                    if elapsed < expected_time:
                        await asyncio.sleep(expected_time - elapsed)
                # Wrap retry fetch in the same hard timeout guard
                task = asyncio.create_task(
                    self._run_with_timeout(self.fetch_page(page_num), page_num)
                )
                retry_tasks[task] = page_num
                print(f"Retrying page {page_num} (Open tabs: {self.open_pages_count})")
            
            still_failed = []
            pending = set(retry_tasks.keys())
            while pending:
                done, pending = await asyncio.wait(pending, return_when=asyncio.FIRST_COMPLETED)
                for task in done:
                    page_num = retry_tasks.get(task, None)
                    if page_num is None:
                        print(f"[DEBUG] Completed retry task not found in retry_tasks: {task}")
                        continue
                    try:
                        result = await task
                        if isinstance(result, Exception):
                            raise result
                        if result:
                            self.collected_data.extend(result)
                        else:
                            still_failed.append(page_num)
                    except Exception as exc:
                        print(f"Retry for page {page_num} failed with exception: {exc}")
                        still_failed.append(page_num)
            
            # Update remaining failures
            remaining_failures = still_failed
            
            # If there are still failures and we have more attempts, wait before next attempt
            if remaining_failures and attempt < RETRY_ATTEMPTS - 1:
                print(f"Still {len(remaining_failures)} failed pages. Waiting 5 seconds before next retry attempt...")
                await asyncio.sleep(5)
        
        # Finalize failed pages
        self.failed_pages = remaining_failures
        print(f"Final retry process complete. Remaining failed pages: {len(self.failed_pages)}")

    async def fetch_page(self, page_num):
        """Fetch a single page of projects (used for retries)"""
        page = None
        try:
            print(f"[DEBUG] fetch_page start for page {page_num}")
            if not self.captcha_token:
                print("No CAPTCHA token available.")
                return []
                
            url = f"{PROJECT_LIST_URL}?projectname=&pageNo={page_num}&captcha={self.captcha_token}"
            
            # Create a new page for this request
            page = await self.context.new_page()
            print(f"[DEBUG] New retry tab opened for page {page_num}")
            
            response = await page.goto(
                url,
                timeout=90000,
                wait_until="commit"
            )
            print(f"[DEBUG] retry page.goto finished for page {page_num} with status {response.status if response else 'None'} url={page.url}")
            if response.status == 200:
                # Wait for table presence instead of the full load event
                await page.wait_for_selector("table.table_fix", timeout=self.table_timeout)
                print(f"[DEBUG] retry table selector appeared on page {page_num}")
                await asyncio.sleep(PAGE_LOAD_WAIT)
                content = await page.content()
                print(f"[DEBUG] retry page.content acquired for page {page_num}")
                return self.parse_page(content, page_num)
            else:
                print(f"Failed to fetch page {page_num}. Status: {response.status}")
                return []
        except Exception as e:
            print(f"Error fetching page {page_num}: {str(e)}")
            return []
        finally:
            if page:
                print(f"[DEBUG] Closing retry tab for page {page_num} (close timeout={self.page_close_timeout}s)")
                if self.page_close_timeout:
                    try:
                        await asyncio.wait_for(page.close(), timeout=self.page_close_timeout)
                        print(f"[DEBUG] Retry tab closed for page {page_num}")
                    except asyncio.TimeoutError:
                        print(f"[DEBUG] Timeout closing retry tab for page {page_num} after "
                              f"{self.page_close_timeout}s; continuing without waiting")
                    except Exception as e:
                        print(f"[DEBUG] Exception while closing retry tab for page {page_num}: {e}")
                else:
                    try:
                        await page.close()
                        print(f"[DEBUG] Retry tab closed for page {page_num} (no timeout)")
                    except Exception as e:
                        print(f"[DEBUG] Exception while closing retry tab for page {page_num}: {e}")
            async with self.page_lock:
                if self.open_pages_count > 0:
                    self.open_pages_count -= 1
            print(f"[DEBUG] fetch_page finished for page {page_num}, open_pages_count={self.open_pages_count}")

    def save_data(self):
        """Save collected data to CSV with metadata"""
        if not self.collected_data:
            print("No data to save.")
            return
        
        output_file = "inner_mongolia_projects.csv"
        header = ['page', 'code', 'cbsnum', 'name', 'approval', 'result']
        
        existing_order = []
        existing_map = {}
        existing_no_cbs = []
        
        if os.path.exists(output_file):
            with open(output_file, newline='', encoding='utf-8-sig') as csvfile:
                reader = csv.reader(csvfile)
                next(reader, None)  # Skip metadata row
                header_row = next(reader, None)
                if header_row:
                    header = header_row
                    data_reader = csv.DictReader(csvfile, fieldnames=header)
                    for row in data_reader:
                        normalized = {field: row.get(field, '') for field in header}
                        cbsnum = normalized.get('cbsnum', '').strip()
                        if cbsnum:
                            if cbsnum not in existing_map:
                                existing_order.append(cbsnum)
                            existing_map[cbsnum] = normalized
                        else:
                            existing_no_cbs.append(normalized)
        
        new_no_cbs = []
        for row in self.collected_data:
            normalized = {field: row.get(field, '') for field in header}
            cbsnum = str(normalized.get('cbsnum', '')).strip()
            if cbsnum:
                if cbsnum not in existing_map:
                    existing_order.append(cbsnum)
                existing_map[cbsnum] = normalized
            else:
                new_no_cbs.append(normalized)
        
        merged_rows = [existing_map[cbs] for cbs in existing_order] + existing_no_cbs + new_no_cbs
        total_records = len(merged_rows)
        
        execution_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        duration = self.end_time - self.start_time if self.end_time and self.start_time else 0
        pages_processed = self.processed_pages or self.total_pages
        
        with open(output_file, 'w', newline='', encoding='utf-8-sig') as csvfile:
            writer = csv.writer(csvfile)
            writer.writerow([
                'METADATA',
                f'Execution time: {execution_time}',
                '',
                f'Total projects collected: {total_records}',
                f'Total pages processed: {pages_processed}',
                f'Duration: {duration:.2f} seconds'
            ])
            
            dict_writer = csv.DictWriter(csvfile, fieldnames=header)
            dict_writer.writeheader()
            for row in merged_rows:
                dict_writer.writerow({field: row.get(field, '') for field in header})
        
        print(f"Data saved to {output_file} (total rows: {total_records})")
        self._report("completed", f"采集完成，共 {total_records} 条记录", current=self.processed_pages, total=self.processed_pages)

    def save_failed_pages(self):
        """Save list of failed pages to a file"""
        if self.failed_pages:
            with open("failed_pages.txt", "w") as f:
                for page in sorted(self.failed_pages):
                    f.write(f"{page}\n")
            print(f"Failed pages saved to failed_pages.txt")
        else:
            print("No failed pages to save.")
            # Remove the file if it exists and there are no failed pages
            if os.path.exists("failed_pages.txt"):
                os.remove("failed_pages.txt")

    async def close(self):
        """Close browser"""
        if self.browser:
            await self.browser.close()

async def main():
    parser = argparse.ArgumentParser(description="Inner Mongolia project collector")
    parser.add_argument('--retry', action='store_true', help='Retry only failed pages from failed_pages.txt')
    parser.add_argument('--max-pages', type=int, help='Limit the number of pages to process (useful for debugging)')
    parser.add_argument('--max-open-tabs', type=int, help=f'Maximum concurrently open tabs (default: {MAX_OPEN_TABS})')
    parser.add_argument('--pages-per-second', type=float, help=f'Page open rate per second (default: {PAGES_PER_SECOND})')
    parser.add_argument('--headless', action='store_true', help='Run browser in headless mode')
    args = parser.parse_args()
    
    collector = ProjectCollector(
        retry_failed_only=args.retry,
        max_pages=args.max_pages,
        headless=args.headless,
        max_open_tabs=args.max_open_tabs,
        pages_per_second=args.pages_per_second
    )
    
    try:
        # Setup browser
        if not await collector.setup_browser():
            print("Failed to setup browser. Exiting.")
            return
        
        # Get CAPTCHA token from browser
        if not await collector.get_captcha_token_from_browser():
            print("Failed to get CAPTCHA token. Exiting.")
            return
        
        # Collect all projects
        print("Starting data collection...")
        await collector.collect_all_projects()
        
        # Save data
        collector.save_data()
        collector.save_failed_pages()
        
        # Print summary
        print(f"\n--- Collection Summary ---")
        print(f"Total projects collected: {len(collector.collected_data)}")
        print(f"Failed pages: {len(collector.failed_pages)}")
        if collector.start_time and collector.end_time:
            print(f"Time taken: {collector.end_time - collector.start_time:.2f} seconds")
        print(f"Output files:")
        print(f"  - inner_mongolia_projects.csv (project data)")
        print(f"  - failed_pages.txt (list of failed pages)")
    finally:
        # Close browser
        await collector.close()

if __name__ == "__main__":
    asyncio.run(main())
