import csv
import re
import time
import os
import boto3
import datetime
import requests
from bs4 import BeautifulSoup
from playwright.sync_api import sync_playwright
from botocore.exceptions import NoCredentialsError

# === CLOUD-READY S3 UPLOAD ===
def upload_to_s3(file_name, bucket, object_name=None):
    """Upload file to S3 bucket"""
    s3 = boto3.client('s3')
    if object_name is None:
        object_name = file_name
    try:
        s3.upload_file(file_name, bucket, object_name)
        print(f"✅ File {file_name} uploaded to s3://{bucket}/{object_name}")
        return True
    except NoCredentialsError:
        print("❌ AWS credentials not available. Check your environment variables.")
        return False
    except Exception as e:
        print(f"❌ Upload failed: {e}")
        return False

SITEMAP_URL = "https://carsandbids.com/cab-sitemap/auctions.xml"
OUTPUT_CSV = f"cnb_data_{datetime.datetime.now().strftime('%Y%m%d_%H%M')}.csv"
S3_BUCKET = "my-mii-reports"
SLEEP_BETWEEN_AUCTIONS = 3.0  # Longer delay for cloud

def get_sitemap_urls():
    """Get sitemap URLs without manual intervention"""
    print("🌐 Fetching sitemap...")
    
    # Method 1: Try direct requests first (fastest)
    try:
        print("📡 Trying direct HTTP request...")
        headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
        }
        response = requests.get(SITEMAP_URL, headers=headers, timeout=30)
        
        if response.status_code == 200:
            xml_string = response.text
            print("✅ Got sitemap via direct request")
        else:
            raise Exception(f"HTTP {response.status_code}")
            
    except Exception as e:
        print(f"📡 Direct request failed: {e}")
        print("🌐 Trying browser method...")
        
        # Method 2: Use browser as backup
        try:
            with sync_playwright() as p:
                browser = p.chromium.launch(headless=True)
                context = browser.new_context(
                    user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"
                )
                page = context.new_page()
                
                # Set longer timeout and try multiple times
                for attempt in range(3):
                    try:
                        print(f"🔄 Browser attempt {attempt + 1}/3...")
                        page.goto(SITEMAP_URL, timeout=45_000, wait_until="networkidle")
                        time.sleep(5)  # Wait for any dynamic content
                        xml_string = page.content()
                        print("✅ Got sitemap via browser")
                        break
                    except Exception as attempt_error:
                        if attempt == 2:  # Last attempt
                            raise attempt_error
                        print(f"⚠️ Attempt {attempt + 1} failed, retrying...")
                        time.sleep(10)
                
                browser.close()
                
        except Exception as browser_error:
            print(f"❌ Both methods failed: {browser_error}")
            return []

    # Extract XML content from browser wrapper if needed
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

    print("📄 XML content preview:")
    print(xml_string[:300] + "...")

    # Parse URLs with multiple fallback methods
    urls = []
    try:
        # Method 1: XML parser
        soup = BeautifulSoup(xml_string, "xml")
        urls = [loc.text for loc in soup.find_all("loc") if "/auctions/" in loc.text]
        
        if not urls:
            # Method 2: HTML parser
            print("🔄 Trying HTML parser...")
            soup = BeautifulSoup(xml_string, "html.parser")
            urls = [loc.text for loc in soup.find_all("loc") if "/auctions/" in loc.text]
        
        if not urls:
            # Method 3: Regex extraction
            print("🔄 Trying regex extraction...")
            url_pattern = r'https://carsandbids\.com/auctions/[^<\s]+'
            urls = re.findall(url_pattern, xml_string)
            urls = list(set(urls))  # Remove duplicates
        
        print(f"🔍 Found {len(urls)} total auction URLs")
        
        if urls:
            print("First 3 URLs found:")
            for i, url in enumerate(urls[:3]):
                print(f"  {i+1}. {url}")
        
    except Exception as e:
        print(f"❌ Error parsing XML: {e}")
        return []

    return urls

def extract_year_from_url(url):
    """Extract year from CNB URL patterns"""
    if not url:
        return None
    
    patterns = [
        r'/auctions/[^/]*-(\d{4})-',
        r'/auctions/(\d{4})-',
        r'-(\d{4})-'
    ]
    
    for pattern in patterns:
        match = re.search(pattern, url)
        if match:
            year = int(match.group(1))
            if 1900 <= year <= 2030:
                return year
    return None

def extract_year_from_title(title):
    """Extract year from title/model text as fallback"""
    if not title:
        return None
    
    match = re.search(r'\b(19|20)\d{2}\b', str(title))
    if match:
        year = int(match.group(0))
        if 1900 <= year <= 2030:
            return year
    return None

def safe_text(page, selector):
    try:
        el = page.query_selector(selector)
        return el.inner_text().strip() if el else ""
    except Exception:
        return ""

def clean_model(text):
    """Clean model text by removing 'Save' and newlines/whitespace"""
    if not text:
        return ""
    
    text = re.sub(r'\s+', ' ', text)
    text = re.sub(r'\s*save\s*', '', text, flags=re.IGNORECASE)
    return text.strip()

def get_existing_urls_from_s3():
    """Get previously scraped URLs from S3"""
    existing_urls = set()
    s3 = boto3.client('s3')
    
    try:
        response = s3.list_objects_v2(Bucket=S3_BUCKET, Prefix='cnb_data_')
        if 'Contents' in response:
            # Get the most recent file
            latest_file = max(response['Contents'], key=lambda x: x['LastModified'])
            print(f"📁 Found previous data file: {latest_file['Key']}")
            
            # Download and read it
            s3.download_file(S3_BUCKET, latest_file['Key'], 'previous_data.csv')
            with open('previous_data.csv', 'r', encoding='utf-8') as f:
                reader = csv.DictReader(f)
                for row in reader:
                    if row.get('auction_url'):
                        existing_urls.add(row['auction_url'])
            print(f"📊 Found {len(existing_urls)} previously scraped auctions")
    except Exception as e:
        print(f"📝 No previous data found (starting fresh): {e}")
    
    return existing_urls

def main():
    print(f"🚀 Starting Cloud CNB Scraper - {datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    
    # 1. Get sitemap URLs (no manual input required)
    urls = get_sitemap_urls()
    
    if not urls:
        print("❌ Failed to get sitemap URLs!")
        return False

    # 2. Get existing URLs from S3
    existing_urls = get_existing_urls_from_s3()

    # 3. Filter new URLs
    new_urls = [url for url in urls if url not in existing_urls]
    print(f"✨ New auctions to scrape: {len(new_urls)}")

    if not new_urls:
        print("✅ No new auctions found - all up to date!")
        return True

    # Limit for cloud efficiency (remove this line to scrape all)
    new_urls = new_urls[:100]  # Process 100 at a time
    print(f"🎯 Processing first {len(new_urls)} new auctions")

    # 4. Scrape new auctions
    with open(OUTPUT_CSV, 'w', newline='', encoding='utf-8') as csvfile:
        fieldnames = [
            "model", "make", "vin", "engine", "drivetrain", "transmission", "body_style",
            "exterior_color", "interior_color", "title_status", "location", "mileage",
            "sale_amount", "sale_date", "sale_type", "bids", "views", "seller", 
            "auction_url", "year", "scraped_date"
        ]
        writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
        writer.writeheader()

        with sync_playwright() as p:
            browser = p.chromium.launch(
                headless=True,
                args=[
                    "--no-sandbox",
                    "--disable-dev-shm-usage",
                    "--disable-web-security",
                    "--disable-features=VizDisplayCompositor"
                ]
            )
            context = browser.new_context(
                user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"
            )
            
            new_auctions = 0
            failed_auctions = 0
            skipped_in_progress = 0
            
            for i, auction_url in enumerate(new_urls):
                print(f"[{i+1}/{len(new_urls)}] Processing: {auction_url}")
                page = None
                
                try:
                    page = context.new_page()
                    year = extract_year_from_url(auction_url)
                    
                    # Navigate with extended retries for cloud
                    for attempt in range(3):
                        try:
                            page.goto(auction_url, timeout=45_000, wait_until="networkidle")
                            break
                        except Exception as e:
                            if attempt == 2:
                                raise e
                            print(f"  🔄 Retry {attempt + 1}/3")
                            time.sleep(5)

                    # Wait for content with longer timeout
                    try:
                        page.wait_for_selector('dl, .auction-facts, .quick-facts', timeout=20000)
                        time.sleep(3)
                    except:
                        print(f"  ⚠️ Page load warning, continuing...")

                    # Extract data (same as before)
                    model_raw = safe_text(page, "h1")
                    if not year:
                        year = extract_year_from_title(model_raw)
                    
                    sale_amount = (safe_text(page, "span.bid-value") or 
                                  safe_text(page, ".bid-value") or 
                                  safe_text(page, ".final-bid"))
                    
                    sale_date = (safe_text(page, "span.time-ended") or 
                               safe_text(page, ".auction-end-time"))
                    
                    # Skip if still in progress
                    if not sale_date or sale_date.strip() == "":
                        print(f"  ⏳ Skipping - auction in progress")
                        skipped_in_progress += 1
                        continue
                    
                    sale_type_raw = safe_text(page, "span.value")
                    if "sold" in sale_type_raw.lower():
                        sale_type = "sold"
                    elif "reserve" in sale_type_raw.lower():
                        sale_type = "reserve not met"
                    else:
                        sale_type = sale_type_raw

                    bids = safe_text(page, "li.num-bids")
                    views = safe_text(page, "li span.views").replace(",", "")
                    seller = safe_text(page, "li.seller")

                    # Extract facts
                    facts = {}
                    try:
                        fact_containers = page.query_selector_all("dl")
                        for container in fact_containers:
                            dt_elements = container.query_selector_all("dt")
                            for dt in dt_elements:
                                try:
                                    key = dt.inner_text().strip().replace(" ", "_").lower()
                                    dd = dt.evaluate_handle("el => el.nextElementSibling")
                                    if dd and dd.as_element():
                                        value = dd.as_element().inner_text().strip()
                                        if value and key:
                                            facts[key] = value
                                except:
                                    continue
                    except Exception as e:
                        print(f"  ⚠️ Facts extraction error: {e}")

                    # Save data
                    row = {
                        "model": clean_model(facts.get("model", model_raw)),
                        "make": facts.get("make", ""),
                        "vin": facts.get("vin", ""),
                        "engine": facts.get("engine", ""),
                        "drivetrain": facts.get("drivetrain", ""),
                        "transmission": facts.get("transmission", ""),
                        "body_style": facts.get("body_style", ""),
                        "exterior_color": facts.get("exterior_color", ""),
                        "interior_color": facts.get("interior_color", ""),
                        "title_status": facts.get("title_status", ""),
                        "location": facts.get("location", ""),
                        "mileage": facts.get("mileage", ""),
                        "sale_amount": sale_amount,
                        "sale_date": sale_date,
                        "sale_type": sale_type,
                        "bids": bids,
                        "views": views,
                        "seller": seller,
                        "auction_url": auction_url,
                        "year": year,
                        "scraped_date": datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                    }
                    
                    writer.writerow(row)
                    csvfile.flush()
                    new_auctions += 1
                    
                    year_display = f"({year})" if year else ""
                    print(f"  ✅ {year_display} {row['model'][:30]} - {sale_amount}")
                    
                except Exception as e:
                    failed_auctions += 1
                    print(f"  ❌ Error: {str(e)[:100]}")
                    
                finally:
                    if page:
                        page.close()
                    time.sleep(SLEEP_BETWEEN_AUCTIONS)

            browser.close()
            
            # Results summary
            print(f"\n📊 Scraping Summary:")
            print(f"✅ Completed auctions: {new_auctions}")
            print(f"⏳ In-progress skipped: {skipped_in_progress}")
            print(f"❌ Failed: {failed_auctions}")

    # 5. Upload to S3
    print(f"\n☁️ Uploading to S3...")
    if upload_to_s3(OUTPUT_CSV, S3_BUCKET):
        print(f"🎉 Successfully uploaded {OUTPUT_CSV}")
        
        # Clean up local files
        try:
            os.remove(OUTPUT_CSV)
            if os.path.exists('previous_data.csv'):
                os.remove('previous_data.csv')
        except:
            pass
        
        return True
    else:
        print("❌ S3 upload failed")
        return False

if __name__ == "__main__":
    success = main()
    print(f"\n{'🎉 Script completed successfully!' if success else '❌ Script failed!'}")
