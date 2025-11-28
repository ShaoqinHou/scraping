import asyncio
import pandas as pd
from bs4 import BeautifulSoup
import time
import urllib.parse
from playwright.async_api import async_playwright
import os
from datetime import datetime
import argparse
import csv
from captcha_manager import CaptchaManager, start_standalone_captcha_server

# Configuration
BASE_URL = "https://fgw.nmg.gov.cn"
DETAIL_URL_TEMPLATE = f"{BASE_URL}/tzxm/project/gsSegvDsgger.jspx"
MAX_CONCURRENT_EXTRACTIONS = 5
RETRY_ATTEMPTS = 3
PAGES_PER_SECOND = 5  # Rate of detail page opening
PAGE_OPEN_INTERVAL = 1.0 / PAGES_PER_SECOND
PAGE_LOAD_WAIT = 3
MAX_OPEN_TABS = 30
DETAIL_TABLE_TIMEOUT = 20000

class DetailedProjectExtractor:
    def __init__(self, csv_file, filter_keywords, max_projects=None, headless=False,
                 max_concurrent=None, cbsnums=None, captcha_manager: CaptchaManager | None = None,
                 progress_callback=None, max_open_tabs=None, pages_per_second=None):
        self.csv_file = csv_file
        self.filter_keywords = filter_keywords
        self.browser = None
        self.context = None
        self.filtered_projects = []
        self.extracted_data = []
        self.failed_extractions = []
        self.open_pages_count = 0
        self.page_lock = asyncio.Lock()
        self.start_time = None
        self.end_time = None
        self.captcha_page = None
        self.captcha_manager = captcha_manager or CaptchaManager()
        self._own_captcha = captcha_manager is None
        self._captcha_thread = None
        if self._own_captcha:
            self._captcha_thread = start_standalone_captcha_server(self.captcha_manager)
        self.captcha_token = None
        self.max_projects = max_projects
        self.headless = headless
        self.max_concurrent = max_concurrent or MAX_CONCURRENT_EXTRACTIONS
        self.cbsnum_filters = cbsnums
        self.progress_callback = progress_callback or (lambda **kwargs: None)
        self.max_open_tabs = max_open_tabs if (max_open_tabs and max_open_tabs > 0) else max(MAX_OPEN_TABS, self.max_concurrent)
        self.pages_per_second = pages_per_second if (pages_per_second and pages_per_second > 0) else PAGES_PER_SECOND
        self.page_open_interval = (1.0 / self.pages_per_second) if self.pages_per_second else 0
        self.table_timeout = DETAIL_TABLE_TIMEOUT

    def load_and_filter_csv(self):
        """Load CSV file and filter by keywords in the 'name' column (column D)"""
        print(f"Loading CSV file: {self.csv_file}")
        
        # Read CSV file, skipping metadata row
        df = pd.read_csv(self.csv_file, encoding='utf-8-sig')
        
        # Skip metadata row if it exists (first row contains 'METADATA')
        if len(df) > 0 and str(df.iloc[0, 0]).startswith('METADATA'):
            df = df.iloc[1:].reset_index(drop=True)
        
        print(f"Total rows in CSV: {len(df)}")
        
        if len(df.columns) < 4:
            print("Error: CSV file doesn't have enough columns")
            return
            
        name_column = df.columns[3]  # Column D (0-indexed)
        cbs_column = df.columns[2]
        
        if self.cbsnum_filters:
            filtered_df = df[df[cbs_column].astype(str).isin(self.cbsnum_filters)]
            print(f"Projects matching指定 cbsnum: {len(filtered_df)}")
        elif self.filter_keywords:
            print(f"Filtering by keywords in column: {name_column}")
            filtered_df = df[df[name_column].astype(str).str.contains('|'.join(self.filter_keywords), case=False, na=False)]
            print(f"Projects matching keywords: {len(filtered_df)}")
        else:
            filtered_df = df
            print(f"无关键词过滤，默认选择全部 {len(filtered_df)} 条")
        
        # Convert to list of dictionaries for processing
        projects = filtered_df.to_dict('records')
        if self.max_projects:
            projects = projects[:self.max_projects]
            print(f"Limiting to first {len(projects)} projects (max-projects={self.max_projects})")
        self.filtered_projects = projects
        self._report("prepare", f"共 {len(self.filtered_projects)} 条待提取", current=0, total=len(self.filtered_projects) or 1)
        
        if self.filtered_projects:
            print(f"Sample of filtered projects:")
            for i, project in enumerate(self.filtered_projects[:3]):
                print(f"  {i+1}. {project[name_column]} (cbsnum: {project[cbs_column]})")

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

    async def get_captcha_token(self):
        """Get CAPTCHA token using the same method as the list collector (shared CaptchaManager)."""
        print("Opening browser for CAPTCHA solving...")
        self._report("captcha", "打开列表页面，准备验证码", current=0, total=len(self.filtered_projects) or 0)
        
        # Create a new page
        page = await self.context.new_page()
        self.captcha_page = page  # Store reference for later use
        
        # Navigate to main page (allow slower overseas connections)
        await page.goto(
            f"{BASE_URL}/tzxm?isYjs=0&record=istrue",
            timeout=90000,
            wait_until="commit"
        )
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
        
        # Wait briefly for the modal to appear
        await page.wait_for_timeout(1000)
        
        # Click on the captcha input field to trigger captcha image display
        print("Clicking on captcha input field...")
        await page.click("#captcha")
        
        # Try to capture the CAPTCHA image as soon as it appears
        print("Capturing CAPTCHA image...")
        captcha_element = await page.query_selector("#captcha-img")
        if not captcha_element:
            captcha_element = await page.query_selector("img[src*='captcha']")
        
        if captcha_element:
            captcha_image_bytes = await captcha_element.screenshot()
            self.captcha_manager.set_image(captcha_image_bytes)
            print("CAPTCHA image sent to server. 请在控制台页面输入验证码。")
            self._report("captcha", "验证码已生成，请在页面输入", current=0, total=len(self.filtered_projects) or 0)
        else:
            print("Could not find CAPTCHA image element. Please solve CAPTCHA manually.")
            return await self.manual_captcha_entry(page)
        
        # Wait for CAPTCHA to be solved either via web UI or directly in browser
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
                    self._report("captcha", "验证码成功，准备提取", current=0, total=len(self.filtered_projects) or 0)
                    return True

            # 2) If we have not yet submitted a code, try to get one from the web UI (non‑blocking)
            if not submitted_code:
                captcha_code = self.captcha_manager.wait_for_code(timeout=0.1)
                if captcha_code:
                    print(f"Submitting CAPTCHA code from web UI: {captcha_code}")
                    await page.fill("#captcha", captcha_code)
                    self._report("captcha", "验证码已提交，等待跳转", current=0, total=len(self.filtered_projects) or 0)
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
    
    async def manual_captcha_entry(self, page):
        """Fallback method for manual CAPTCHA entry"""
        print("Manual CAPTCHA entry required.")
        print("Please solve the CAPTCHA and click '确定' button.")
        print("Once you've done that, the script will automatically continue.")
        self._report("captcha", "等待手动输入验证码", current=0, total=len(self.filtered_projects) or 0)
        start_time = time.time()
        while True:
            current_url = page.url
            if "VDEdfsef.jspx" in current_url and "captcha=" in current_url:
                print("Detected successful CAPTCHA submission!")
                parsed_url = urllib.parse.urlparse(current_url)
                query_params = urllib.parse.parse_qs(parsed_url.query)
                captcha_token = query_params.get('captcha', [None])[0]
                if captcha_token:
                    self.captcha_token = captcha_token
                    print(f"CAPTCHA token extracted: {captcha_token}")
                    self._report("captcha", "验证码成功，准备提取", current=0, total=len(self.filtered_projects) or 0)
                    return True
            if time.time() - start_time > 300:
                print("Timeout waiting for CAPTCHA submission. Please make sure you've completed the CAPTCHA.")
                return False
            await asyncio.sleep(1)

    async def extract_project_details(self, project):
        """Extract detailed information for a single project"""
        cbsnum_column = list(project.keys())[2]  # Column C (cbsnum)
        name_column = list(project.keys())[3]    # Column D (name)
        
        cbsnum = project[cbsnum_column]
        project_name = project[name_column]
        
        print(f"Extracting details for: {project_name} (cbsnum: {cbsnum})")
        
        # Craft the detail URL
        detail_url = f"{DETAIL_URL_TEMPLATE}?cbsnum={cbsnum}&captcha={self.captcha_token}"
        
        try:
            while True:
                async with self.page_lock:
                    if self.open_pages_count < self.max_open_tabs:
                        self.open_pages_count += 1
                        break
                await asyncio.sleep(0.05)
            
            # Create new page for this extraction
            page = await self.context.new_page()
            
            try:
                # Navigate to detail page (skip waiting for slow external resources)
                await page.goto(detail_url, timeout=90000, wait_until='commit')
                if "gsSegvDsgger" not in page.url:
                    print(f"Unexpected redirect while opening {project_name}: {page.url}")
                    return None
                await page.wait_for_selector("table.txxx_table_style", timeout=self.table_timeout)
                await page.wait_for_timeout(PAGE_LOAD_WAIT * 1000)
                
                # Get page content
                content = await page.content()
                
                # Parse the content
                soup = BeautifulSoup(content, 'html.parser')
                
                # Extract data from multiple tables
                extracted_data = self.parse_detail_page(soup, project)
                
                await page.close()
                
                # Rate limiting
                if self.page_open_interval:
                    await asyncio.sleep(self.page_open_interval)
                
                return extracted_data
                
            except Exception as e:
                print(f"Error extracting details for {project_name}: {str(e)}")
                await page.close()
                return None
                
        except Exception as e:
            print(f"Error creating page for {project_name}: {str(e)}")
            return None
        finally:
            async with self.page_lock:
                if self.open_pages_count > 0:
                    self.open_pages_count -= 1

    def parse_detail_page(self, soup, original_project):
        """Parse the detail page and extract information from all tables"""
        extracted_data = []
        
        # Get basic project info
        basic_info = {}
        basic_info_table = soup.find('table', class_='txxx_table_style')
        
        if basic_info_table:
            rows = basic_info_table.find_all('tr')
            for row in rows:
                cells = row.find_all('td')
                if len(cells) >= 2:
                    label = cells[0].get_text(strip=True)
                    value = cells[1].get_text(strip=True)
                    basic_info[label] = value
        
        # Extract data from approval tables
        fieldsets = soup.find_all('fieldset', class_='txxx_block')
        
        for fieldset in fieldsets:
            legend = fieldset.find('legend')
            section_name = legend.get_text(strip=True) if legend else "Unknown Section"
            
            table = fieldset.find('table', class_='txxx_table_style')
            if not table:
                continue
                
            tbody = table.find('tbody')
            if not tbody:
                continue
                
            rows = tbody.find_all('tr')
            
            for row in rows:
                cells = row.find_all('td')
                if len(cells) < 6:  # Skip if not enough columns
                    continue
                
                # Extract text and handle embedded links
                row_data = {}
                
                # Copy basic info to each row
                row_data.update(basic_info)
                row_data['section'] = section_name
                
                # Extract table data
                row_data['approval_department'] = cells[0].get_text(strip=True)
                row_data['approval_item'] = cells[1].get_text(strip=True)
                row_data['approval_result'] = cells[2].get_text(strip=True)
                row_data['approval_date'] = cells[3].get_text(strip=True)
                row_data['approval_document_no'] = cells[4].get_text(strip=True)
                
                # Handle attachment cell with embedded links
                attachment_cell = cells[5]
                attachment_links = []
                attachment_texts = []
                attachment_urls = []
                
                links = attachment_cell.find_all('a')
                for link in links:
                    href = link.get('href', '')
                    title = link.get('title', '')
                    text = link.get_text(strip=True)
                    
                    if href:
                        # Make absolute URL if relative
                        if href.startswith('/'):
                            href = BASE_URL + href
                        
                        attachment_texts.append(text)
                        attachment_urls.append(href)
                        attachment_links.append({
                            'text': text,
                            'title': title,
                            'url': href
                        })
                
                # Store data in separate columns for better CSV readability
                if attachment_links:
                    row_data['attachment_text'] = ' | '.join(attachment_texts)  # Separate multiple texts with |
                    row_data['attachment_urls'] = ' | '.join(attachment_urls)   # Separate multiple URLs with |
                    row_data['attachment_full_data'] = str(attachment_links)    # Keep full data as backup
                else:
                    row_data['attachment_text'] = attachment_cell.get_text(strip=True)
                    row_data['attachment_urls'] = ''
                    row_data['attachment_full_data'] = ''
                
                # Add original project data
                for key, value in original_project.items():
                    row_data[f'original_{key}'] = value
                
                extracted_data.append(row_data)
        
        return extracted_data

    async def extract_all_projects(self):
        """Extract details for all filtered projects"""
        if not self.filtered_projects:
            print("No projects to extract")
            return
        
        print(f"Starting extraction of {len(self.filtered_projects)} projects...")
        self.start_time = time.time()
        self._report("running", f"准备提取 {len(self.filtered_projects)} 条", current=0, total=len(self.filtered_projects))
        
        # Create semaphore for concurrent extractions
        extraction_semaphore = asyncio.Semaphore(self.max_concurrent)
        
        async def extract_with_semaphore(project):
            async with extraction_semaphore:
                return project, await self.extract_project_details(project)
        
        tasks = [asyncio.create_task(extract_with_semaphore(project)) for project in self.filtered_projects]
        total = len(tasks)
        completed = 0
        
        for task in asyncio.as_completed(tasks):
            project, result = await task
            completed += 1
            if result:
                self.extracted_data.extend(result)
                print(f"Progress: {completed}/{total} projects processed")
            else:
                self.failed_extractions.append(project)
                print(f"Failed: {completed}/{total} projects processed")
            # UI will append current/total; avoid duplicating the counts in message.
            self._report("progress", "详情提取中", current=completed, total=total)
        
        self.end_time = time.time()
        print(f"Extraction completed. Total data rows: {len(self.extracted_data)}")

    def save_extracted_data(self):
        """Save extracted data to CSV with metadata"""
        if not self.extracted_data:
            print("No data to save.")
            return None

        execution_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        duration = self.end_time - self.start_time if self.end_time and self.start_time else 0
        output_file = f"detailed_project_data_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"

        header_map = {
            'section': '审批环节',
            'approval_department': '审批部门',
            'approval_item': '审批事项',
            'approval_result': '审批结果',
            'approval_date': '审批日期',
            'approval_document_no': '审批文号',
            'attachment_text': '附件名称',
            'attachment_urls': '附件链接',
            'attachment_full_data': '附件原始数据',
            'original_page': '原始页码',
            'original_code': '原始项目代码',
            'original_cbsnum': 'cbsnum',
            'original_name': '原始项目名称',
            'original_approval': '原始审批事项',
            'original_result': '原始审批结果',
            'project_type': '项目类型',
            'project_name': '项目名称',
        }

        translated_rows = []
        all_fields = set()
        for row in self.extracted_data:
            translated = {}
            for k, v in row.items():
                cn = header_map.get(k)
                if not cn and k.startswith('original_'):
                    cn = '原始_' + k[len('original_'):]
                if not cn:
                    cn = k
                translated[cn] = v
                all_fields.add(cn)
            translated_rows.append(translated)

        preferred_order = [
            'AI摘要', 'AI要点',
            '审批日期', '审批部门', '审批文号', '审批事项', '审批结果', '审批环节',
            '申报单位', '项目代码', 'cbsnum', '项目名称', '项目类型',
            '原始项目名称', '原始审批事项', '原始审批结果', '原始项目代码', '原始页码',
            '附件原始数据', '附件名称', '附件链接',
        ]
        fieldnames = [f for f in preferred_order if f in all_fields]
        for f in sorted(all_fields):
            if f not in fieldnames:
                fieldnames.append(f)

        with open(output_file, 'w', newline='', encoding='utf-8-sig') as csvfile:
            writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
            writer.writeheader()
            for row in translated_rows:
                writer.writerow(row)
            metadata_row = {fieldnames[0]: 'METADATA'}
            if len(fieldnames) > 1:
                metadata_row[fieldnames[1]] = f'Execution time: {execution_time}'
            if len(fieldnames) > 2:
                metadata_row[fieldnames[2]] = f'Total rows extracted: {len(self.extracted_data)}'
            if len(fieldnames) > 3:
                metadata_row[fieldnames[3]] = f'Total projects processed: {len(self.filtered_projects)}'
            if len(fieldnames) > 4:
                metadata_row[fieldnames[4]] = f'Duration: {duration:.2f} seconds'
            if len(fieldnames) > 5:
                metadata_row[fieldnames[5]] = f'Keywords: {", ".join(self.filter_keywords)}'
            writer.writerow(metadata_row)

        print(f"Detailed data saved to {output_file}")
        self._report("completed", f"详情提取完成，共 {len(self.extracted_data)} 行", current=len(self.filtered_projects), total=len(self.filtered_projects))
        self.last_output_file = output_file
        return output_file

    async def close(self):
        """Clean up resources"""
        if self.browser:
            await self.browser.close()

async def main():
    parser = argparse.ArgumentParser(description='Extract detailed project information based on keyword filtering')
    parser.add_argument('csv_file', help='Path to the CSV file containing project data')
    parser.add_argument('keywords', nargs='+', help='Keywords to filter projects by (space separated)')
    parser.add_argument('--max-projects', type=int, help='Limit number of matching projects to extract (debugging)')
    parser.add_argument('--headless', action='store_true', help='Run browser in headless mode')
    parser.add_argument('--max-concurrent', type=int, help=f'Max concurrent detail fetches (default: {MAX_CONCURRENT_EXTRACTIONS})')
    parser.add_argument('--max-open-tabs', type=int, help=f'Max open tabs (default: {MAX_OPEN_TABS})')
    parser.add_argument('--pages-per-second', type=float, help=f'Open rate per second (default: {PAGES_PER_SECOND})')
    
    args = parser.parse_args()
    
    # Validate CSV file exists
    if not os.path.exists(args.csv_file):
        print(f"Error: CSV file '{args.csv_file}' not found")
        return
    
    print(f"Starting detailed extraction with keywords: {args.keywords}")
    if args.max_projects:
        print(f"Limiting to first {args.max_projects} matched projects")
    
    # Create extractor
    extractor = DetailedProjectExtractor(
        args.csv_file,
        args.keywords,
        max_projects=args.max_projects,
        headless=args.headless,
        max_concurrent=args.max_concurrent,
        max_open_tabs=args.max_open_tabs,
        pages_per_second=args.pages_per_second
    )
    
    try:
        # Load and filter CSV
        extractor.load_and_filter_csv()
        
        if not extractor.filtered_projects:
            print("No projects match the given keywords")
            return
        
        # Setup browser
        await extractor.setup_browser()
        
        # Get CAPTCHA token
        if not await extractor.get_captcha_token():
            print("Failed to get CAPTCHA token")
            return
        
        # Extract all project details
        await extractor.extract_all_projects()
        
        # Save results
        extractor.save_extracted_data()
        
        # Print summary
        print(f"\n--- Extraction Summary ---")
        print(f"Total projects filtered: {len(extractor.filtered_projects)}")
        print(f"Total data rows extracted: {len(extractor.extracted_data)}")
        print(f"Failed extractions: {len(extractor.failed_extractions)}")
        
        if extractor.failed_extractions:
            print("Failed projects:")
            for project in extractor.failed_extractions:
                name_column = list(project.keys())[3]
                print(f"  - {project[name_column]}")
    
    finally:
        await extractor.close()

if __name__ == "__main__":
    asyncio.run(main())
