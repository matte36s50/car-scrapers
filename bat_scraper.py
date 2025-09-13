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
SLEEP_BETWEEN_AUCTIONS = 2.5  # Optimized timing

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

def safe_text(page, selector, timeout=8000):
    """Safely extract text with timeout"""
    try:
        element = page.wait_for_selector(selector, timeout=timeout)
        if element:
            return element.inner_text().strip()
        return ""
    except Exception:
        return ""

def safe_text_multiple(page, selectors, timeout=5000):
    """Try multiple selectors and return first successful match"""
    for selector in selectors:
        try:
            element = page.wait_for_selector(selector, timeout=timeout)
            if element:
                text = element.inner_text().strip()
                if text:  # Only return non-empty text
                    return text
        except:
            continue
    return ""

def extract_auction_data_comprehensive(page, auction_url):
    """Comprehensive extraction with multiple fallback selectors"""
    try:
        # Wait for page to load
        print(f"    ⏳ Waiting for page content...")
        page.wait_for_selector("h1, .listing-title, .entry-title", timeout=15000)
        time.sleep(3)  # Additional wait for dynamic content
        
        # Extract title with multiple fallbacks
        title_selectors = [
            "h1",
            ".listing-title h1", 
            ".entry-title",
            ".listing-title",
            "h1.entry-title",
            ".post-title h1"
        ]
        title = safe_text_multiple(page, title_selectors)
        print(f"    📝 Title: {title[:50]}...")
        
        # Extract current bid/sale amount with multiple fallbacks
        bid_selectors = [
            ".current-bid",
            ".bid-value", 
            ".winning-bid",
            ".final-bid",
            ".sale-price",
            ".auction-amount",
            ".current-price",
            ".price-value",
            "[data-bid-amount]",
            ".bid-amount"
        ]
        sale_amount = safe_text_multiple(page, bid_selectors)
        print(f"    💰 Sale amount: {sale_amount}")
        
        # Extract view count with multiple fallbacks
        view_selectors = [
            ".view-count",
            ".views",
            ".pageviews", 
            "[data-views]",
            ".stat-views",
            ".listing-views"
        ]
        views = safe_text_multiple(page, view_selectors)
        print(f"    👀 Views: {views}")
        
        # Extract comment count with multiple fallbacks
        comment_selectors = [
            ".comment-count",
            ".comments-count",
            ".comments",
            "[data-comments]",
            ".stat-comments",
            ".listing-comments",
            "#comments .count"
        ]
        comments = safe_text_multiple(page, comment_selectors)
        print(f"    💬 Comments: {comments}")
        
        # Extract bid count with multiple fallbacks  
        bid_count_selectors = [
            ".bid-count",
            ".bids-count", 
            ".bids",
            "[data-bids]",
            ".stat-bids",
            ".listing-bids",
            ".total-bids"
        ]
        bids = safe_text_multiple(page, bid_count_selectors)
        print(f"    🏷️ Bids: {bids}")
        
        # If we got minimal data, try alternative extraction
        if not any([sale_amount, views, comments, bids]):
            print(f"    🔄 Primary extraction failed, trying alternative methods...")
            
            # Try to find data in script tags or data attributes
            try:
                page_content = page.content()
                
                # Look for JSON data in script tags
                if 'views' in page_content or 'bids' in page_content:
                    # Try to extract numbers from page content
                    view_matches = re.findall(r'views?["\s:]+(\d+)', page_content, re.IGNORECASE)
                    bid_matches = re.findall(r'bids?["\s:]+(\d+)', page_content, re.IGNORECASE)
                    
                    if view_matches and not views:
                        views = view_matches[0]
                        print(f"    🔍 Found views in content: {views}")
                    
                    if bid_matches and not bids:
                        bids = bid_matches[0]
                        print(f"    🔍 Found bids in content: {bids}")
                        
            except Exception as e:
                print(f"    ⚠️ Alternative extraction error: {e}")
        
        # Extract additional metadata if available
        make = ""
        model = ""
        year = ""
        
        # Try to parse make/model/year from title
        if title:
            # Look for year (4 digits)
            year_match = re.search(r'\b(19|20)\d{2}\b', title)
            if year_match:
                year = year_match.group(0)
            
            # Basic make extraction (first word often)
            title_words = title.split()
            if len(title_words) > 0:
                make = title_words[0]
        
        result = {
            "title": title or "Unknown Title",
            "make": make,
            "model": model,
            "year": year,
            "sale_amount": sale_amount,
            "views": views or "0",
            "comments": comments or "0",
            "bids": bids or "0",
            "extraction_method": "comprehensive"
        }
        
        print(f"    ✅ Extraction complete: {title[:30]}...")
        return result
        
    except Exception as e:
        print(f"    ❌ Comprehensive extraction failed: {e}")
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
    print(f"🚀 Starting Enhanced BAT Scraper - {datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("🎯 Using FULL EXTRACTION for all auctions to get complete data")
    
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

    # Increased limit to handle BAT's daily volume
    new_urls = new_urls[:250]  # Process 250 at a time
    print(f"🎯 Processing first {len(new_urls)} new auctions")
    print(f"⏱️ Estimated time: ~{(len(new_urls) * SLEEP_BETWEEN_AUCTIONS) / 60:.1f} minutes")

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
                    "--disable-features=VizDisplayCompositor",
                    "--disable-audio-output",
                    "--disable-gpu"
                ]
            )
            context = browser.new_context(
                user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"
            )
            
            successful_extractions = 0
            failed_auctions = 0
            total_views = 0
            total_bids = 0
            
            for i, auction_url in enumerate(new_urls):
                print(f"\n[{i+1}/{len(new_urls)}] Processing: {auction_url}")
                page = None
                
                try:
                    page = context.new_page()
                    
                    # Navigate with timeout protection
                    print(f"  🌐 Loading page...")
                    page.goto(auction_url, timeout=45_000, wait_until="networkidle")
                    
                    # ALWAYS use comprehensive extraction (no fast extraction)
                    print(f"  🔍 Using comprehensive extraction...")
                    data = extract_auction_data_comprehensive(page, auction_url)
                    
                    if not data or not data.get('title') or data['title'] == 'Unknown Title':
                        print(f"  ❌ No meaningful data extracted")
                        failed_auctions += 1
                        continue
                    
                    # Process and clean the data
                    row = {
                        "title": data.get("title", "")[:200],  # Limit length
                        "make": data.get("make", ""),
                        "model": data.get("model", ""),
                        "year": data.get("year", ""),
                        "sale_amount": data.get("sale_amount", ""),
                        "sale_date": "",  # Will be populated if we can extract it
                        "sale_type": "active",  # Assume active unless we detect otherwise
                        "views": data.get("views", "0"),
                        "comments": data.get("comments", "0"),
                        "bids": data.get("bids", "0"),
                        "auction_url": auction_url,
                        "scraped_date": datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
                        "extraction_method": data.get("extraction_method", "comprehensive")
                    }
                    
                    writer.writerow(row)
                    csvfile.flush()
                    successful_extractions += 1
                    
                    # Track metrics for summary
                    try:
                        views_num = int(re.sub(r'[^\d]', '', str(data.get("views", "0"))) or "0")
                        bids_num = int(re.sub(r'[^\d]', '', str(data.get("bids", "0"))) or "0")
                        total_views += views_num
                        total_bids += bids_num
                    except:
                        pass
                    
                    print(f"  ✅ SUCCESS: {data.get('title', 'Unknown')[:40]}...")
                    print(f"       Views: {data.get('views', '0')} | Bids: {data.get('bids', '0')} | Amount: {data.get('sale_amount', 'N/A')}")
                    
                except Exception as e:
                    failed_auctions += 1
                    print(f"  ❌ Error: {str(e)[:150]}...")
                    
                finally:
                    if page:
                        page.close()
                    time.sleep(SLEEP_BETWEEN_AUCTIONS)

            browser.close()
            
            # Results summary
            print(f"\n" + "="*60)
            print(f"📊 BAT SCRAPING RESULTS")
            print(f"="*60)
            print(f"✅ Successful extractions: {successful_extractions}")
            print(f"❌ Failed auctions: {failed_auctions}")
            print(f"📈 Success rate: {successful_extractions/(successful_extractions+failed_auctions)*100:.1f}%")
            print(f"👀 Total views captured: {total_views:,}")
            print(f"🏷️ Total bids captured: {total_bids:,}")
            print(f"⏱️ Total processing time: ~{(len(new_urls) * SLEEP_BETWEEN_AUCTIONS) / 60:.1f} minutes")

    # 5. Upload to S3
    print(f"\n☁️ Uploading to S3...")
    if upload_to_s3(OUTPUT_CSV, S3_BUCKET):
        print(f"🎉 Successfully uploaded {OUTPUT_CSV}")
        print(f"📊 File contains {successful_extractions} auction records with full data")
        
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
    print(f"\n{'🎉 Enhanced BAT scraper completed successfully!' if success else '❌ BAT scraper failed!'}")
