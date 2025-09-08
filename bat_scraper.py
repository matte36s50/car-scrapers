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

BAT_SITEMAP_URL = "https://bringatrailer.com/sitemap_auctions.xml"
OUTPUT_CSV = f"bat_data_{datetime.datetime.now().strftime('%Y%m%d_%H%M')}.csv"
S3_BUCKET = "my-mii-reports"
SLEEP_BETWEEN_AUCTIONS = 4.0  # Increased for cloud stability

# Known problematic URL patterns to skip
SKIP_PATTERNS = [
    "convertible-67",  # The specific problem URL pattern
    "listing/test-",   # Test listings
    "preview-",        # Preview listings
]

def should_skip_url(url):
    """Check if URL should be skipped due to known issues"""
    for pattern in SKIP_PATTERNS:
        if pattern in url:
            return True
    
    # Skip URLs with excessive dashes (often malformed)
    if url.count('-') > 15:
        return True
    
    # Skip URLs that are too long (often problematic)
    if len(url) > 200:
        return True
    
    return False

def get_sitemap_urls():
    """Get BAT sitemap URLs with fallback methods"""
    print("🌐 Fetching BAT sitemap...")
    
    # Method 1: Direct requests
    try:
        print("📡 Trying direct HTTP request...")
        headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
        }
        response = requests.get(BAT_SITEMAP_URL, headers=headers, timeout=30)
        
        if response.status_code == 200:
            xml_string = response.text
            print("✅ Got BAT sitemap via direct request")
        else:
            raise Exception(f"HTTP {response.status_code}")
            
    except Exception as e:
        print(f"📡 Direct request failed: {e}")
        print("🌐 Trying browser method...")
        
        try:
            with sync_playwright() as p:
                browser = p.chromium.launch(headless=True)
                context = browser.new_context(
                    user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"
                )
                page = context.new_page()
                
                for attempt in range(3):
                    try:
                        print(f"🔄 Browser attempt {attempt + 1}/3...")
                        page.goto(BAT_SITEMAP_URL, timeout=45_000)
                        time.sleep(5)
                        xml_string = page.content()
                        print("✅ Got BAT sitemap via browser")
                        break
                    except Exception as attempt_error:
                        if attempt == 2:
                            raise attempt_error
                        print(f"⚠️ Attempt {attempt + 1} failed, retrying...")
                        time.sleep(10)
                
                browser.close()
                
        except Exception as browser_error:
            print(f"❌ Both methods failed: {browser_error}")
            return []

    # Parse URLs
    try:
        soup = BeautifulSoup(xml_string, "xml")
        urls = [loc.text for loc in soup.find_all("loc") if "/listing/" in loc.text]
        
        if not urls:
            soup = BeautifulSoup(xml_string, "html.parser")
            urls = [loc.text for loc in soup.find_all("loc") if "/listing/" in loc.text]
        
        if not urls:
            url_pattern = r'https://bringatrailer\.com/listing/[^<\s]+'
            urls = re.findall(url_pattern, xml_string)
            urls = list(set(urls))
        
        print(f"🔍 Found {len(urls)} total BAT auction URLs")
        
        # Filter out problematic URLs
        clean_urls = []
        skipped_count = 0
        
        for url in urls:
            if should_skip_url(url):
                skipped_count += 1
                continue
            clean_urls.append(url)
        
        print(f"🧹 Filtered URLs: {len(clean_urls)} usable, {skipped_count} skipped as problematic")
        
        if clean_urls:
            print("First 3 URLs found:")
            for i, url in enumerate(clean_urls[:3]):
                print(f"  {i+1}. {url}")
        
        return clean_urls
        
    except Exception as e:
        print(f"❌ Error parsing XML: {e}")
        return []

def safe_text(page, selector, timeout=5000):
    """Safely extract text with timeout"""
    try:
        element = page.wait_for_selector(selector, timeout=timeout)
        if element:
            return element.inner_text().strip()
        return ""
    except Exception:
        return ""

def quick_check_page_status(page):
    """Quickly determine if page is problematic"""
    try:
        # Check for common problem indicators
        page_content = page.content()
        
        # Check for error pages
        if any(error in page_content.lower() for error in ['404', 'not found', 'error', 'forbidden']):
            return "error_page"
        
        # Check for completed auctions (these can hang)
        if any(completed in page_content.lower() for completed in ['winning bid', 'auction ended', 'sold for']):
            return "completed_auction"
        
        # Check for loading states that might hang
        if 'loading more comments' in page_content.lower():
            return "dynamic_loading"
        
        return "normal"
        
    except Exception:
        return "unknown"

def extract_auction_data_fast(page, auction_url):
    """Fast extraction method for completed/problematic auctions"""
    try:
        # Quick extraction without waiting for dynamic content
        title = safe_text(page, "h1, .listing-title", timeout=3000)
        
        # Try to get basic auction info quickly
        sale_amount = ""
        try:
            # Look for completed sale amount
            amount_selectors = [".winning-bid", ".final-bid", ".sale-price", ".auction-amount"]
            for selector in amount_selectors:
                amount = safe_text(page, selector, timeout=2000)
                if amount and any(char.isdigit() for char in amount):
                    sale_amount = amount
                    break
        except:
            pass
        
        # Basic metrics (set to 0 if not easily found)
        return {
            "title": title or "Unknown",
            "sale_amount": sale_amount,
            "views": "0",
            "comments": "0", 
            "bids": "0",
            "extraction_method": "fast"
        }
        
    except Exception as e:
        print(f"    ⚠️ Fast extraction failed: {e}")
        return None

def extract_auction_data_full(page, auction_url):
    """Full extraction method for normal auctions"""
    try:
        # Wait for main content with reasonable timeout
        page.wait_for_selector("h1, .listing-title", timeout=15000)
        time.sleep(2)  # Brief wait for additional content
        
        # Extract all data
        title = safe_text(page, "h1, .listing-title")
        sale_amount = safe_text(page, ".current-bid, .winning-bid, .final-bid")
        views = safe_text(page, ".view-count, .views")
        comments = safe_text(page, ".comment-count, .comments")
        bids = safe_text(page, ".bid-count, .bids")
        
        return {
            "title": title,
            "sale_amount": sale_amount,
            "views": views,
            "comments": comments,
            "bids": bids,
            "extraction_method": "full"
        }
        
    except Exception as e:
        print(f"    ⚠️ Full extraction failed: {e}")
        return None

def get_existing_urls_from_s3():
    """Get previously scraped URLs from S3"""
    existing_urls = set()
    s3 = boto3.client('s3')
    
    try:
        response = s3.list_objects_v2(Bucket=S3_BUCKET, Prefix='bat_data_')
        if 'Contents' in response:
            latest_file = max(response['Contents'], key=lambda x: x['LastModified'])
            print(f"📁 Found previous BAT data: {latest_file['Key']}")
            
            s3.download_file(S3_BUCKET, latest_file['Key'], 'previous_bat_data.csv')
            with open('previous_bat_data.csv', 'r', encoding='utf-8') as f:
                reader = csv.DictReader(f)
                for row in reader:
                    if row.get('auction_url'):
                        existing_urls.add(row['auction_url'])
            print(f"📊 Found {len(existing_urls)} previously scraped BAT auctions")
    except Exception as e:
        print(f"📝 No previous BAT data found: {e}")
    
    return existing_urls

def main():
    print(f"🚀 Starting Cloud BAT Scraper - {datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    
    # 1. Get sitemap URLs
    urls = get_sitemap_urls()
    
    if not urls:
        print("❌ Failed to get BAT sitemap URLs!")
        return False

    # 2. Get existing URLs from S3
    existing_urls = get_existing_urls_from_s3()

    # 3. Filter new URLs
    new_urls = [url for url in urls if url not in existing_urls]
    print(f"✨ New BAT auctions to scrape: {len(new_urls)}")

    if not new_urls:
        print("✅ No new BAT auctions found - all up to date!")
        return True

    # Limit for cloud efficiency
    new_urls = new_urls[:75]  # Process 75 at a time
    print(f"🎯 Processing first {len(new_urls)} new auctions")

    # 4. Scrape new auctions
    with open(OUTPUT_CSV, 'w', newline='', encoding='utf-8') as csvfile:
        fieldnames = [
            "title", "make", "model", "year", "sale_amount", "sale_date", "sale_type",
            "views", "comments", "bids", "auction_url", "scraped_date", "extraction_method"
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
            fast_extractions = 0
            
            for i, auction_url in enumerate(new_urls):
                print(f"[{i+1}/{len(new_urls)}] Processing: {auction_url}")
                page = None
                
                try:
                    page = context.new_page()
                    
                    # Navigate with timeout protection
                    page.goto(auction_url, timeout=30_000)
                    
                    # Quick page status check
                    page_status = quick_check_page_status(page)
                    print(f"  📊 Page status: {page_status}")
                    
                    # Choose extraction method based on page status
                    if page_status in ["completed_auction", "dynamic_loading", "error_page"]:
                        print(f"  ⚡ Using fast extraction for {page_status}")
                        data = extract_auction_data_fast(page, auction_url)
                        fast_extractions += 1
                    else:
                        print(f"  🔍 Using full extraction")
                        data = extract_auction_data_full(page, auction_url)
                    
                    if not data:
                        print(f"  ❌ No data extracted")
                        failed_auctions += 1
                        continue
                    
                    # Process and clean the data
                    row = {
                        "title": data.get("title", ""),
                        "make": "",  # Extract from title if needed
                        "model": "",  # Extract from title if needed
                        "year": "",   # Extract from title if needed
                        "sale_amount": data.get("sale_amount", ""),
                        "sale_date": "",
                        "sale_type": "completed" if page_status == "completed_auction" else "unknown",
                        "views": data.get("views", "0"),
                        "comments": data.get("comments", "0"),
                        "bids": data.get("bids", "0"),
                        "auction_url": auction_url,
                        "scraped_date": datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
                        "extraction_method": data.get("extraction_method", "unknown")
                    }
                    
                    writer.writerow(row)
                    csvfile.flush()
                    new_auctions += 1
                    
                    print(f"  ✅ {data.get('title', 'Unknown')[:40]} - {data.get('sale_amount', 'No price')}")
                    
                except Exception as e:
                    failed_auctions += 1
                    print(f"  ❌ Error: {str(e)[:100]}")
                    
                finally:
                    if page:
                        page.close()
                    time.sleep(SLEEP_BETWEEN_AUCTIONS)

            browser.close()
            
            # Results summary
            print(f"\n📊 BAT Scraping Summary:")
            print(f"✅ Completed auctions: {new_auctions}")
            print(f"⚡ Fast extractions: {fast_extractions}")
            print(f"❌ Failed: {failed_auctions}")
            print(f"⏱️ Average time per auction: {(len(new_urls) * SLEEP_BETWEEN_AUCTIONS) / 60:.1f} minutes")

    # 5. Upload to S3
    print(f"\n☁️ Uploading to S3...")
    if upload_to_s3(OUTPUT_CSV, S3_BUCKET):
        print(f"🎉 Successfully uploaded {OUTPUT_CSV}")
        
        # Clean up local files
        try:
            os.remove(OUTPUT_CSV)
            if os.path.exists('previous_bat_data.csv'):
                os.remove('previous_bat_data.csv')
        except:
            pass
        
        return True
    else:
        print("❌ S3 upload failed")
        return False

if __name__ == "__main__":
    success = main()
    print(f"\n{'🎉 BAT scraper completed successfully!' if success else '❌ BAT scraper failed!'}")
