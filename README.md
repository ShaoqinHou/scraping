# Inner Mongolia Projects Collector

This project collects project information from the Inner Mongolia Investment Projects Online Approval Platform.

## Components

1. **Main Scraper** (`final_improved_collector_with_captcha_server.py`) - The main scraping script
2. **CAPTCHA Server** (`captcha_server.py`) - A web server that displays CAPTCHA images and collects user input
3. **Web Interface** (`templates/captcha.html`) - HTML page for CAPTCHA entry

## Setup

1. Install required packages:
   ```
   pip install -r requirements.txt
   ```

2. Install Playwright browsers:
   ```
   playwright install chromium
   ```

## Usage

### Normal Operation

1. Start the CAPTCHA server:
   ```
   python captcha_server.py
   ```

2. In a separate terminal, run the main scraper:
   ```
   python final_improved_collector_with_captcha_server.py
   ```

3. Open a web browser and go to `http://localhost:5000` to enter CAPTCHAs when prompted.

### Retry Failed Pages Only

```
python final_improved_collector_with_captcha_server.py --retry
```

## How It Works

1. The main scraper navigates to the website and triggers the CAPTCHA challenge
2. Instead of manually entering the CAPTCHA on the official website, the scraper sends the CAPTCHA image to the CAPTCHA server
3. Users access `http://localhost:5000` to view the CAPTCHA image and enter the code
4. When submitted, the CAPTCHA code is sent back to the scraper
5. The scraper uses the code to continue with data collection

This approach allows for:
- Remote CAPTCHA solving
- Multiple people assisting with CAPTCHA entry
- Cleaner user interface
- Separation of concerns between scraping and user interaction