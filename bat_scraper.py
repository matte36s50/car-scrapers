import csv
import re
import time
import os
import boto3
import pandas as pd
import datetime
import requests
from bs4 import BeautifulSoup
from playwright.sync_api import sync_playwright
from botocore.exceptions import NoCredentialsError
import traceback
import json

# === S3 CONFIGURATION ===
S3_BUCKET = "my-mii-reports"
BAT_CSV_FILENAME = "bat.csv"  # Single file to maintain
TEMP_LOCAL_FILE = "temp_bat.csv"
BACKUP_FILE = "bat_backup.csv"

def download_existing_bat_csv():
    """Download existing bat.csv from S3"""
    s3 = boto3.client('s3')
    
    try:
        s3.download_file(S3_BUCKET, BAT_CSV_FILENAME, TEMP_LOCAL_FILE)
        print(f"✅ Downloaded existing bat.csv from S3")
        
        # Load and analyze existing data
        df = pd.read_csv(TEMP_LOCAL_FILE)
        print(f"📊 Existing data: {len(df)} rows, {len(df.columns)} columns")
        
        # Get set of existing URLs for duplicate checking
        existing_urls = set(df['auction_url'].dropna().values)
        print(f"📋 Found {len(existing_urls)} existing auction URLs")
        
        return df, existing_urls
        
    except s3.exceptions.NoSuchKey:
        print(f"⚠️ No existing bat.csv found in S3, will create new one")
        # Return empty dataframe with expected columns
        columns = [
            'auction_url', 'bids', 'category', 'comments', 'end_date', 
            'end_timestamp', 'era', 'location', 'make', 'model', 
            'origin', 'partner', 'sale_amount', 'sale_date', 'sale_type', 
            'seller_type', 'views', 'watchers', 'year'
        ]
        return pd.DataFrame(columns=columns), set()
    except Exception as e:
        print(f"❌ Error downloading bat.csv: {e}")
        raise

def upload_updated_bat_csv(df):
    """Upload updated bat.csv back to S3"""
    s3 = boto3.client('s3')
    
    try:
        # Save dataframe to CSV
        df.to_csv(TEMP_LOCAL_FILE, index=False)
        
        # Create backup first
        try:
            s3.copy_object(
                Bucket=S3_BUCKET,
                CopySource={'Bucket': S3_BUCKET, 'Key': BAT_CSV_FILENAME},
                Key=f"backups/{BAT_CSV_FILENAME}_{datetime.datetime.now().strftime('%Y%m%d_%H%M')}"
            )
            print(f"📦 Created backup of existing bat.csv")
        except:
            pass  # No existing file to backup
        
        # Upload updated file
        s3.upload_file(TEMP_LOCAL_FILE, S3_BUCKET, BAT_CSV_FILENAME)
        print(f"✅ Successfully uploaded updated bat.csv to S3 ({len(df)} total rows)")
        
        # Clean up temp file
        os.remove(TEMP_LOCAL_FILE)
        return True
        
    except Exception as e:
        print(f"❌ Upload failed: {e}")
        return False

BAT_SITEMAP_URL = "https://bringatrailer.com/sitemap_auctions.xml"
SLEEP_BETWEEN_AUCTIONS = 2.5
MAX_AUCTIONS_PER_RUN = 500  # Limit per run to avoid timeouts

def get_sitemap_urls():
    """Get BAT sitemap URLs"""
    print("🌐 Fetching BAT sitemap...")
    
    try:
        headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
        }
        response = requests.get(BAT_SITEMAP_URL, headers=headers, timeout=30)
        
        if response.status_code == 200:
            xml_string = response.text
            print("✅ Got BAT sitemap")
        else:
            raise Exception(f"HTTP {response.status_code}")
            
    except Exception as e:
        print(f"❌ Sitemap fetch failed: {e}")
        return []

    # Parse URLs
    try:
        soup = BeautifulSoup(xml_string, "xml")
        urls = [loc.text for loc in soup.find_all("loc") if "/listing/" in loc.text]
        
        if not urls:
            soup = BeautifulSoup(xml_string, "html.parser")
            urls = [loc.text for loc in soup.find_all("loc") if "/listing/" in loc.text]
        
        print(f"🔍 Found {len(urls)} total auction URLs")
        return urls
        
    except Exception as e:
        print(f"❌ Error parsing XML: {e}")
        return []

def extract_all_auction_data(page, auction_url):
    """Extract comprehensive data matching the bat.csv columns"""
    
    # Initialize with all expected fields
    data = {
        'auction_url': auction_url,
        'bids': None,
        'category': '',
        'comments': None,
        'end_date': '',
        'end_timestamp': None,
        'era': '',
        'location': '',
        'make': '',
        'model': '',
        'origin': '',
        'partner': '',
        'sale_amount': '',
        'sale_date': '',
        'sale_type': '',
        'seller_type': '',
        'views': '',
        'watchers': '',
        'year': None
    }
    
    try:
        # Wait for page to load
        page.wait_for_selector("body", timeout=15000)
        time.sleep(2)
        
        page_html = page.content()
        
        # === TITLE AND MODEL ===
        try:
            title_element = page.query_selector("h1.listing-title") or page.query_selector("h1")
            if title_element:
                full_title = title_element.inner_text().strip()
                data['model'] = full_title
                
                # Extract year from title
                year_match = re.search(r'\b(19|20)\d{2}\b', full_title)
                if year_match:
                    data['year'] = int(year_match.group(0))
                
                # Extract make (usually first word after year)
                words = full_title.split()
                for i, word in enumerate(words):
                    if not word.isdigit() and len(word) > 2 and i > 0:
                        data['make'] = word
                        break
        except:
            pass
        
        # === SALE AMOUNT ===
        try:
            bid_selectors = [
                ".current-bid .bid-value",
                ".current-bid",
                ".bid-value",
                ".winning-bid",
                ".final-bid",
                "span.bid-value"
            ]
            for selector in bid_selectors:
                element = page.query_selector(selector)
                if element:
                    text = element.inner_text().strip()
                    if '$' in text:
                        data['sale_amount'] = text
                        break
        except:
            pass
        
        # === STATS SECTION (Views, Comments, Bids, Watchers) ===
        try:
            stats_section = page.query_selector(".stats-list") or page.query_selector(".auction-stats")
            if stats_section:
                stats_text = stats_section.inner_text()
                
                # Views
                views_match = re.search(r'([\d,]+)\s*views?', stats_text, re.IGNORECASE)
                if views_match:
                    data['views'] = views_match.group(1).replace(',', '')
                
                # Comments
                comments_match = re.search(r'([\d,]+)\s*comments?', stats_text, re.IGNORECASE)
                if comments_match:
                    data['comments'] = int(comments_match.group(1).replace(',', ''))
                
                # Bids
                bids_match = re.search(r'([\d,]+)\s*bids?', stats_text, re.IGNORECASE)
                if bids_match:
                    data['bids'] = int(bids_match.group(1).replace(',', ''))
                
                # Watchers
                watchers_match = re.search(r'([\d,]+)\s*watchers?', stats_text, re.IGNORECASE)
                if watchers_match:
                    data['watchers'] = watchers_match.group(1).replace(',', '')
        except:
            pass
        
        # === ADDITIONAL FALLBACK EXTRACTION FROM HTML ===
        if not data['views'] or data['views'] == '':
            views_patterns = [
                r'class="views"[^>]*>([\d,]+)',
                r'data-views="([\d,]+)"',
                r'>([\d,]+)\s*views?'
            ]
            for pattern in views_patterns:
                match = re.search(pattern, page_html, re.IGNORECASE)
                if match:
                    data['views'] = match.group(1).replace(',', '')
                    break
        
        if data['bids'] is None:
            bids_patterns = [
                r'class="bid-count"[^>]*>([\d,]+)',
                r'data-bids="([\d,]+)"',
                r'>([\d,]+)\s*bids?'
            ]
            for pattern in bids_patterns:
                match = re.search(pattern, page_html, re.IGNORECASE)
                if match:
                    data['bids'] = int(match.group(1).replace(',', ''))
                    break
        
        # === LOCATION ===
        try:
            location_patterns = [
                r'class="location"[^>]*>([^<]+)',
                r'Location:\s*([^<\n]+)',
                r'seller-location[^>]*>([^<]+)'
            ]
            for pattern in location_patterns:
                match = re.search(pattern, page_html, re.IGNORECASE)
                if match:
                    data['location'] = match.group(1).strip()
                    break
        except:
            pass
        
        # === SALE DATE AND TYPE ===
        try:
            # Check if auction ended
            end_time_element = page.query_selector(".time-ended") or page.query_selector(".auction-end-time")
            if end_time_element:
                data['sale_date'] = end_time_element.inner_text().strip()
                data['end_date'] = data['sale_date']
            
            # Sale type (sold, reserve not met, etc.)
            sale_type_element = page.query_selector(".sale-status") or page.query_selector(".auction-status")
            if sale_type_element:
                sale_text = sale_type_element.inner_text().lower()
                if "sold" in sale_text:
                    data['sale_type'] = "sold"
                elif "reserve" in sale_text:
                    data['sale_type'] = "reserve not met"
                else:
                    data['sale_type'] = sale_text
        except:
            pass
        
        # === CATEGORY AND ERA ===
        try:
            # Try to find category/era from tags or breadcrumbs
            category_element = page.query_selector(".category-tag") or page.query_selector(".listing-category")
            if category_element:
                data['category'] = category_element.inner_text().strip()
            
            # Era might be in the title or tags (e.g., "1980s", "Modern", "Classic")
            if data['year']:
                year_int = data['year']
                if year_int < 1950:
                    data['era'] = 'Pre-War'
                elif year_int < 1970:
                    data['era'] = 'Classic'
                elif year_int < 1990:
                    data['era'] = 'Modern Classic'
                elif year_int < 2010:
                    data['era'] = 'Modern'
                else:
                    data['era'] = 'Contemporary'
        except:
            pass
        
        # === SELLER TYPE ===
        try:
            seller_patterns = [
                r'seller[^>]*dealer',
                r'seller[^>]*private',
                r'class="seller-type"[^>]*>([^<]+)'
            ]
            for pattern in seller_patterns:
                match = re.search(pattern, page_html, re.IGNORECASE)
                if match:
                    if 'dealer' in match.group(0).lower():
                        data['seller_type'] = 'Dealer'
                    elif 'private' in match.group(0).lower():
                        data['seller_type'] = 'Private'
                    break
        except:
            pass
        
        # === ORIGIN (Country of manufacture) ===
        if data['make']:
            make_lower = data['make'].lower()
            if make_lower in ['bmw', 'mercedes', 'mercedes-benz', 'porsche', 'audi', 'volkswagen']:
                data['origin'] = 'Germany'
            elif make_lower in ['ferrari', 'lamborghini', 'alfa', 'fiat', 'maserati']:
                data['origin'] = 'Italy'
            elif make_lower in ['toyota', 'honda', 'nissan', 'mazda', 'subaru', 'mitsubishi']:
                data['origin'] = 'Japan'
            elif make_lower in ['ford', 'chevrolet', 'dodge', 'chrysler', 'gmc', 'cadillac']:
                data['origin'] = 'USA'
            elif make_lower in ['jaguar', 'land', 'rover', 'aston', 'bentley', 'rolls']:
                data['origin'] = 'UK'
        
        print(f"    ✅ Extracted: {data['model'][:40] if data['model'] else 'Unknown'}... | "
              f"${data['sale_amount']} | {data['views']} views | {data['bids']} bids")
        
        return data
        
    except Exception as e:
        print(f"    ❌ Extraction error: {str(e)[:100]}")
        traceback.print_exc()
        return data

def main():
    print(f"🚀 Starting BAT Scraper (Append Mode) - {datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    
    # 1. Download existing bat.csv from S3
    existing_df, existing_urls = download_existing_bat_csv()
    
    # 2. Get current sitemap URLs
    all_urls = get_sitemap_urls()
    
    if not all_urls:
        print("❌ Failed to get sitemap URLs!")
        return False
    
    # 3. Filter for new URLs only
    new_urls = [url for url in all_urls if url not in existing_urls]
    print(f"✨ Found {len(new_urls)} new auctions to scrape")
    
    if not new_urls:
        print("✅ No new auctions found - bat.csv is up to date!")
        return True
    
    # 4. Limit to MAX_AUCTIONS_PER_RUN to avoid timeout
    new_urls = new_urls[:MAX_AUCTIONS_PER_RUN]
    print(f"🎯 Processing {len(new_urls)} new auctions (max {MAX_AUCTIONS_PER_RUN} per run)")
    
    # 5. Scrape new auctions
    new_rows = []
    
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
        
        successful = 0
        failed = 0
        
        for i, auction_url in enumerate(new_urls):
            print(f"\n[{i+1}/{len(new_urls)}] Processing: {auction_url}")
            page = None
            
            try:
                page = context.new_page()
                
                # Navigate with retries
                for retry in range(3):
                    try:
                        page.goto(auction_url, timeout=45_000, wait_until="domcontentloaded")
                        break
                    except Exception as nav_error:
                        if retry == 2:
                            raise nav_error
                        print(f"  🔄 Retry {retry + 1}")
                        time.sleep(5)
                
                # Extract comprehensive data
                data = extract_all_auction_data(page, auction_url)
                
                # Add to new rows if we got meaningful data
                if data['model'] or data['views'] or data['bids']:
                    new_rows.append(data)
                    successful += 1
                else:
                    print(f"  ⚠️ Insufficient data extracted")
                    failed += 1
                    
            except Exception as e:
                print(f"  ❌ Error: {str(e)[:150]}")
                failed += 1
                
            finally:
                if page:
                    page.close()
                time.sleep(SLEEP_BETWEEN_AUCTIONS)
                
                # Save progress every 50 auctions
                if len(new_rows) > 0 and len(new_rows) % 50 == 0:
                    print(f"\n💾 Saving progress ({len(new_rows)} new rows)...")
                    temp_df = pd.concat([existing_df, pd.DataFrame(new_rows)], ignore_index=True)
                    upload_updated_bat_csv(temp_df)
        
        browser.close()
        
        print(f"\n📊 Scraping complete: {successful} successful, {failed} failed")
    
    # 6. Append new data to existing dataframe
    if new_rows:
        print(f"\n📝 Adding {len(new_rows)} new rows to bat.csv")
        new_df = pd.DataFrame(new_rows)
        
        # Combine with existing data
        updated_df = pd.concat([existing_df, new_df], ignore_index=True)
        
        # Remove any accidental duplicates
        before_dedup = len(updated_df)
        updated_df = updated_df.drop_duplicates(subset=['auction_url'], keep='first')
        after_dedup = len(updated_df)
        if before_dedup != after_dedup:
            print(f"🧹 Removed {before_dedup - after_dedup} duplicate rows")
        
        # Sort by year (newest first) for better organization
        updated_df = updated_df.sort_values('year', ascending=False, na_position='last')
        
        print(f"📊 Updated bat.csv stats:")
        print(f"   Total rows: {len(updated_df)}")
        print(f"   Total unique auctions: {updated_df['auction_url'].nunique()}")
        print(f"   Years covered: {updated_df['year'].min()} to {updated_df['year'].max()}")
        
        # 7. Upload updated file back to S3
        if upload_updated_bat_csv(updated_df):
            print(f"🎉 Successfully updated bat.csv in S3!")
            return True
        else:
            print(f"❌ Failed to upload updated bat.csv")
            return False
    else:
        print(f"⚠️ No new data to add")
        return True

if __name__ == "__main__":
    try:
        success = main()
        exit(0 if success else 1)
    except Exception as e:
        print(f"❌ Fatal error: {e}")
        traceback.print_exc()
        exit(1)
