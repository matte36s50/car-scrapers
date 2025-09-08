import csv
import re
import time
import os
import pandas as pd
import datetime
import shutil
import tempfile
from bs4 import BeautifulSoup
from playwright.sync_api import sync_playwright

def check_and_free_disk_space():
    """Check disk space and clean up if needed"""
    try:
        total, used, free = shutil.disk_usage('.')
        free_gb = free / (1024**3)
        print(f"Available disk space: {free_gb:.1f} GB")
        
        if free_gb < 1:
            print("⚠️ Low disk space! Cleaning temporary files...")
            temp_dir = tempfile.gettempdir()
            for item in os.listdir(temp_dir):
                if any(keyword in item.lower() for keyword in ['playwright', 'chrome', 'chromium']):
                    try:
                        item_path = os.path.join(temp_dir, item)
                        if os.path.isdir(item_path):
                            shutil.rmtree(item_path)
                        else:
                            os.remove(item_path)
                    except:
                        pass
            
            total, used, free = shutil.disk_usage('.')
            free_gb = free / (1024**3)
            print(f"Space after cleanup: {free_gb:.1f} GB")
            
        return free_gb > 0.5
    except:
        return True

def get_sitemap_and_find_new_urls():
    """Get sitemap and find URLs that need processing"""
    print("🌐 Getting sitemap from Cars and Bids...")
    
    SITEMAP_URL = "https://carsandbids.com/cab-sitemap/auctions.xml"
    OUTPUT_CSV = "cnb_sitemap_full_cleaned.csv"
    
    # Get existing URLs from main CSV
    existing_urls = set()
    if os.path.exists(OUTPUT_CSV):
        try:
            df_main = pd.read_csv(OUTPUT_CSV)
            if 'auction_url' in df_main.columns:
                existing_urls = set(df_main['auction_url'].dropna().tolist())
            print(f"Found {len(existing_urls)} URLs already in main CSV")
        except Exception as e:
            print(f"Error reading main CSV: {e}")
    
    # Get sitemap with minimal browser usage
    sitemap_urls = []
    
    with sync_playwright() as p:
        print("Opening browser to get sitemap...")
        browser = p.chromium.launch(
            headless=False,  # Visible so you can handle any challenges
            slow_mo=50,
            args=["--no-sandbox", "--disable-dev-shm-usage"]
        )
        
        try:
            context = browser.new_context()
            page = context.new_page()
            
            print(f"Loading sitemap: {SITEMAP_URL}")
            page.goto(SITEMAP_URL, timeout=60_000)
            
            # Wait for user to handle any challenges
            input("Press Enter when you can see the XML sitemap loaded in the browser...")
            
            xml_string = page.content()
            print("✓ Got sitemap content")
            
        finally:
            browser.close()
    
    # Parse XML to extract URLs
    print("📋 Parsing sitemap XML...")
    
    # Handle browser XML viewer formatting
    if "<pre id=\"webkit-xml-viewer-source-xml\"" in xml_string:
        soup_html = BeautifulSoup(xml_string, "html.parser")
        pre = soup_html.find("pre", id="webkit-xml-viewer-source-xml")
        if pre:
            xml_string = pre.text
    elif "<div id=\"webkit-xml-viewer-source-xml\"" in xml_string:
        soup_html = BeautifulSoup(xml_string, "html.parser")
        div = soup_html.find("div", id="webkit-xml-viewer-source-xml")
        if div:
            xml_string = div.text
    
    try:
        # Try parsing with xml parser first
        soup = BeautifulSoup(xml_string, "xml")
        urls = [loc.text for loc in soup.find_all("loc") if "/auctions/" in loc.text]
        
        # If no URLs found with xml parser, try html parser
        if not urls:
            print("No URLs found with XML parser, trying HTML parser...")
            soup = BeautifulSoup(xml_string, "html.parser")
            urls = [loc.text for loc in soup.find_all("loc") if "/auctions/" in loc.text]
        
        # If still no URLs, try regex extraction
        if not urls:
            print("No URLs found with HTML parser, trying regex extraction...")
            url_pattern = r'https://carsandbids\.com/auctions/[^<\s]+'
            urls = re.findall(url_pattern, xml_string)
            urls = list(set(urls))  # Remove duplicates
        
        print(f"Found {len(urls)} total auction URLs in sitemap!")
        
    except Exception as e:
        print(f"Error parsing XML: {e}")
        return []
    
    if not urls:
        print("❌ No auction URLs found in sitemap!")
        return []
    
    # Filter for URLs that aren't already in main CSV
    new_urls = [url for url in urls if url not in existing_urls]
    print(f"🎯 Found {len(new_urls)} new URLs to process")
    print(f"🚀 Will process ALL {len(new_urls)} URLs for complete auction data")
    
    return new_urls

def extract_engagement_data(page):
    """Extract engagement metrics (views, comments, watchers)"""
    metrics = {'views': '', 'comments': '', 'watchers': ''}
    
    try:
        # JavaScript extraction for engagement data
        engagement_data = page.evaluate('''
            () => {
                const result = {views: '', comments: '', watchers: ''};
                
                // Method 1: Look in stats containers
                const statsSelectors = ['ul.stats', 'ul.bid-stats', '.auction-stats', '.stats'];
                for (let selector of statsSelectors) {
                    const container = document.querySelector(selector);
                    if (container) {
                        const containerText = container.textContent || '';
                        
                        // Views
                        if (!result.views) {
                            const viewsMatch = containerText.match(/Views[^\\d]*([\\d,]+)|([\\d,]+)[^\\d]*Views/i);
                            if (viewsMatch) result.views = (viewsMatch[1] || viewsMatch[2]).replace(/,/g, '');
                        }
                        
                        // Comments
                        if (!result.comments) {
                            const commentsMatch = containerText.match(/Comments?[^\\d]*([\\d,]+)|([\\d,]+)[^\\d]*Comments?/i);
                            if (commentsMatch) result.comments = (commentsMatch[1] || commentsMatch[2]).replace(/,/g, '');
                        }
                        
                        // Watchers/Watching
                        if (!result.watchers) {
                            const watchersMatch = containerText.match(/Watch(?:ing|ers?)[^\\d]*([\\d,]+)|([\\d,]+)[^\\d]*Watch(?:ing|ers?)/i);
                            if (watchersMatch) result.watchers = (watchersMatch[1] || watchersMatch[2]).replace(/,/g, '');
                        }
                    }
                }
                
                // Method 2: Search entire page if still missing data
                const pageText = document.body.textContent || '';
                
                if (!result.views) {
                    const viewsMatch = pageText.match(/Views[:\\s]*([\\d,]+)/i);
                    if (viewsMatch) result.views = viewsMatch[1].replace(/,/g, '');
                }
                
                if (!result.comments) {
                    const commentsMatch = pageText.match(/Comments?[:\\s]*([\\d,]+)/i);
                    if (commentsMatch) result.comments = commentsMatch[1].replace(/,/g, '');
                }
                
                if (!result.watchers) {
                    const watchersMatch = pageText.match(/(?:Watching|Watchers?)[:\\s]*([\\d,]+)/i);
                    if (watchersMatch) result.watchers = watchersMatch[1].replace(/,/g, '');
                }
                
                return result;
            }
        ''')
        
        # Update metrics with what we found
        for key in ['views', 'comments', 'watchers']:
            if engagement_data.get(key):
                metrics[key] = engagement_data[key]
        
    except Exception as e:
        print(f"    ⚠️ Error extracting engagement: {e}")
    
    return metrics

def safe_text(page, selector):
    try:
        el = page.query_selector(selector)
        return el.inner_text().strip() if el else ""
    except Exception:
        return ""

def extract_complete_auction_data(page, auction_url):
    """Extract ALL auction data like the original CNB scraper"""
    print("    📋 Extracting complete auction data...")
    
    # Initialize data structure with all fields from your Google Sheet
    auction_data = {
        'model': '',
        'make': '',
        'vin': '',
        'engine': '',
        'drivetrain': '',
        'transmission': '',
        'body_style': '',
        'exterior_color': '',
        'interior_color': '',
        'title_status': '',
        'location': '',
        'mileage': '',
        'sale_amount': '',
        'sale_date': '',
        'sale_type': '',
        'bids': '',
        'views': '',
        'comments': '',
        'watchers': '',
        'seller': '',
        'auction_url': auction_url,
        'year': ''
    }
    
    try:
        # Extract basic data
        model_raw = safe_text(page, "h1") or safe_text(page, ".auction-title h1")
        
        # Extract sale data using correct selectors
        sale_info = page.evaluate('''
            () => {
                const result = {sale_amount: '', sale_date: '', sale_type: '', bids: ''};
                
                // Sale date from span.time-ended
                const timeEndedElem = document.querySelector('span.time-ended');
                if (timeEndedElem) {
                    result.sale_date = timeEndedElem.textContent.trim();
                }
                
                // Alternative date selectors
                if (!result.sale_date) {
                    const dateSelectors = ['.auction-end-time', '.end-date', '.sale-date'];
                    for (let selector of dateSelectors) {
                        const elem = document.querySelector(selector);
                        if (elem) {
                            result.sale_date = elem.textContent.trim();
                            break;
                        }
                    }
                }
                
                // Sale amount and status
                const pageText = document.body.textContent || '';
                
                // "Sold for $X" pattern
                const soldMatch = pageText.match(/Sold for[^\\d]*\\$?([\\d,]+)/i);
                if (soldMatch) {
                    result.sale_amount = '$' + soldMatch[1];
                    result.sale_type = 'sold';
                }
                
                // Reserve not met
                if (pageText.includes('Reserve not met') || pageText.includes('reserve not met')) {
                    result.sale_type = 'reserve not met';
                }
                
                // Bids
                const bidsMatch = pageText.match(/(?:# Bids|Bids)[^\\d]*([\\d,]+)/i);
                if (bidsMatch) {
                    result.bids = bidsMatch[1].replace(/,/g, '');
                }
                
                return result;
            }
        ''')
        
        # Update auction data with sale info
        for key, value in sale_info.items():
            if value:
                auction_data[key] = value
        
        # Extract engagement data
        engagement_metrics = extract_engagement_data(page)
        for key, value in engagement_metrics.items():
            if value:
                auction_data[key] = value
        
        # Extract other data
        auction_data['seller'] = safe_text(page, ".seller-name") or safe_text(page, ".seller")
        
        # Extract year from URL
        year_match = re.search(r'/(\d{4})-', auction_url)
        if year_match:
            year = int(year_match.group(1))
            if 1900 <= year <= 2030:
                auction_data['year'] = year
        
        # Extract detailed facts (dt/dd pairs) like the original scraper
        facts = {}
        try:
            fact_containers = page.query_selector_all("dl, .auction-facts, .quick-facts")
            
            for container in fact_containers:
                dt_elements = container.query_selector_all("dt")
                for dt in dt_elements:
                    try:
                        key = dt.inner_text().strip().replace(" ", "_").lower()
                        # Find the next dd element
                        dd = dt.evaluate_handle("el => el.nextElementSibling")
                        if dd and dd.as_element():
                            value = dd.as_element().inner_text().strip()
                            if value and key:
                                facts[key] = value
                    except Exception:
                        continue
        except Exception:
            pass
        
        # Map facts to auction data fields
        fact_mapping = {
            'make': 'make',
            'model': 'model',
            'vin': 'vin',
            'engine': 'engine',
            'drivetrain': 'drivetrain',
            'transmission': 'transmission',
            'body_style': 'body_style',
            'exterior_color': 'exterior_color',
            'interior_color': 'interior_color',
            'title_status': 'title_status',
            'location': 'location',
            'mileage': 'mileage'
        }
        
        for fact_key, data_key in fact_mapping.items():
            if fact_key in facts and facts[fact_key]:
                auction_data[data_key] = facts[fact_key]
        
        # Clean model field
        if facts.get('model'):
            auction_data['model'] = clean_model(facts['model'])
        else:
            auction_data['model'] = clean_model(model_raw)
        
        # Show what we extracted
        print(f"    ✓ Model: {auction_data['model']}")
        print(f"    ✓ Make: {auction_data['make']}")
        print(f"    ✓ Year: {auction_data['year']}")
        print(f"    ✓ Sale: {auction_data['sale_amount']} on {auction_data['sale_date']}")
        print(f"    ✓ Engagement: V:{auction_data['views']} C:{auction_data['comments']} W:{auction_data['watchers']}")
        
    except Exception as e:
        print(f"    ❌ Error extracting auction data: {e}")
    
    return auction_data

def clean_model(text):
    """Clean model text by removing 'Save' and newlines/whitespace"""
    if not text:
        return ""
    
    # Remove newlines and normalize whitespace
    text = re.sub(r'\s+', ' ', text)
    
    # Remove 'Save' (case insensitive, with optional whitespace before/after)
    text = re.sub(r'\s*save\s*', '', text, flags=re.IGNORECASE)
    
    # Clean up any remaining whitespace
    return text.strip()

def main():
    print("Complete CNB Scraper - All Data + Engagement")
    print("="*50)
    print("Extracts ALL auction details plus engagement metrics")
    print("="*50)
    
    # Check disk space
    if not check_and_free_disk_space():
        print("❌ Insufficient disk space!")
        return
    
    # Define complete fieldnames matching your Google Sheet
    fieldnames = [
        "model", "make", "vin", "engine", "drivetrain", "transmission", "body_style",
        "exterior_color", "interior_color", "title_status", "location", "mileage",
        "sale_amount", "sale_date", "sale_type", "bids", "views", "comments", "watchers", 
        "seller", "auction_url", "year"
    ]
    
    # Get sitemap and find new URLs
    new_urls = get_sitemap_and_find_new_urls()
    
    if not new_urls:
        print("✅ No new URLs to process!")
        return
    
    print(f"🚀 Processing {len(new_urls)} URLs for complete auction data...")
    
    # Determine CSV mode
    OUTPUT_CSV = "cnb_sitemap_full_cleaned.csv"
    csv_mode = "a" if os.path.exists(OUTPUT_CSV) else "w"
    
    successful_scrapes = 0
    failed_scrapes = 0
    skipped_in_progress = 0
    
    with open(OUTPUT_CSV, csv_mode, newline='', encoding="utf-8") as csvfile:
        writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
        if csv_mode == "w":
            writer.writeheader()
        
        with sync_playwright() as p:
            browser = p.chromium.launch(
                headless=True,
                args=["--no-sandbox", "--disable-dev-shm-usage"]
            )
            context = browser.new_context(
                user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36"
            )
            
            for i, auction_url in enumerate(new_urls):
                print(f"\n[{i+1}/{len(new_urls)}] {auction_url}")
                
                # Check disk space every 10 auctions
                if i % 10 == 0 and i > 0:
                    if not check_and_free_disk_space():
                        print("⚠️ Stopping due to low disk space")
                        break
                
                page = None
                try:
                    page = context.new_page()
                    
                    # Navigate with retries
                    max_retries = 3
                    for attempt in range(max_retries):
                        try:
                            page.goto(auction_url, timeout=30_000, wait_until="networkidle")
                            break
                        except Exception as e:
                            if attempt == max_retries - 1:
                                raise e
                            print(f"  Retry {attempt + 1}/{max_retries}")
                            time.sleep(2)
                    
                    # Wait for page to load completely
                    try:
                        page.wait_for_selector('dl, .auction-facts, .quick-facts', timeout=15000)
                        time.sleep(3)  # Additional wait for dynamic content
                    except Exception:
                        print(f"  Warning: Quick facts container not found, continuing anyway...")
                    
                    # Extract complete auction data
                    auction_data = extract_complete_auction_data(page, auction_url)
                    
                    # Check if this is a completed auction
                    if not auction_data['sale_date'] and not auction_data['sale_amount']:
                        print(f"  ⏳ Skipping - auction appears to be in progress")
                        skipped_in_progress += 1
                        continue
                    
                    # Write to CSV
                    writer.writerow(auction_data)
                    csvfile.flush()
                    successful_scrapes += 1
                    
                    print(f"  ✅ SAVED: Complete data for {auction_data['model']}")
                    
                except Exception as e:
                    failed_scrapes += 1
                    print(f"  ❌ Error: {e}")
                    
                finally:
                    if page:
                        page.close()
                    time.sleep(2)  # Respectful delay
            
            context.close()
            browser.close()
    
    print(f"\n" + "="*60)
    print(f"COMPLETE CNB SCRAPING FINISHED!")
    print(f"✅ Successful: {successful_scrapes}")
    print(f"⏳ In-progress skipped: {skipped_in_progress}")
    print(f"❌ Failed: {failed_scrapes}")
    print(f"📁 Data saved to: {OUTPUT_CSV}")
    print(f"🎯 Total auctions in database: {successful_scrapes + (len(pd.read_csv(OUTPUT_CSV)) if os.path.exists(OUTPUT_CSV) else 0)}")

if __name__ == "__main__":
    main()
