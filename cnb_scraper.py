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
from botocore.exceptions import NoCredentialsError, ClientError
import traceback

# === S3 CONFIGURATION ===
S3_BUCKET = "my-mii-reports"
CNB_CSV_FILENAME = "cnb.csv"
TEMP_LOCAL_FILE = "temp_cnb.csv"

def download_existing_cnb_csv():
    """Download existing cnb.csv from S3"""
    s3 = boto3.client('s3')
    
    try:
        s3.download_file(S3_BUCKET, CNB_CSV_FILENAME, TEMP_LOCAL_FILE)
        print(f"Downloaded existing cnb.csv from S3")
        
        df = pd.read_csv(TEMP_LOCAL_FILE)
        print(f"Existing data: {len(df)} rows, {len(df.columns)} columns")
        
        existing_urls = set(df['auction_url'].dropna().values)
        print(f"Found {len(existing_urls)} existing auction URLs")
        
        return df, existing_urls
        
    except ClientError as e:
        if e.response['Error']['Code'] == '404' or e.response['Error']['Code'] == 'NoSuchKey':
            print(f"No existing cnb.csv found in S3, will create new one")
            columns = [
                "model", "make", "vin", "engine", "drivetrain", "transmission", "body_style",
                "exterior_color", "interior_color", "title_status", "location", "mileage",
                "sale_amount", "sale_date", "sale_type", "bids", "views", "comments",
                "seller", "auction_url", "year", "scraped_date"
            ]
            return pd.DataFrame(columns=columns), set()
        else:
            raise
    except Exception as e:
        if "404" in str(e) or "Not Found" in str(e) or "NoSuchKey" in str(e):
            print(f"No existing cnb.csv found in S3 (starting fresh)")
            columns = [
                "model", "make", "vin", "engine", "drivetrain", "transmission", "body_style",
                "exterior_color", "interior_color", "title_status", "location", "mileage",
                "sale_amount", "sale_date", "sale_type", "bids", "views", "comments",
                "seller", "auction_url", "year", "scraped_date"
            ]
            return pd.DataFrame(columns=columns), set()
        else:
            print(f"Error downloading cnb.csv: {e}")
            raise

def upload_updated_cnb_csv(df):
    """Upload updated cnb.csv back to S3"""
    s3 = boto3.client('s3')
    
    try:
        df.to_csv(TEMP_LOCAL_FILE, index=False)
        
        try:
            s3.copy_object(
                Bucket=S3_BUCKET,
                CopySource={'Bucket': S3_BUCKET, 'Key': CNB_CSV_FILENAME},
                Key=f"backups/{CNB_CSV_FILENAME}_{datetime.datetime.now().strftime('%Y%m%d_%H%M')}"
            )
            print(f"Created backup of existing cnb.csv")
        except:
            pass
        
        s3.upload_file(TEMP_LOCAL_FILE, S3_BUCKET, CNB_CSV_FILENAME)
        print(f"Successfully uploaded updated cnb.csv to S3 ({len(df)} total rows)")
        
        os.remove(TEMP_LOCAL_FILE)
        return True
        
    except Exception as e:
        print(f"Upload failed: {e}")
        return False

SLEEP_BETWEEN_AUCTIONS = 3.0
MAX_AUCTIONS_PER_RUN = 300

def get_sitemap_urls():
    """Get CNB auction URLs"""
    print("Fetching CNB sitemap...")
    
    try:
        headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
        }
        
        sitemap_url = "https://carsandbids.com/sitemap.xml"
        response = requests.get(sitemap_url, headers=headers, timeout=30)
        
        if response.status_code == 200:
            soup = BeautifulSoup(response.text, "xml")
            locs = soup.find_all("loc")
            
            auction_sitemap = None
            for loc in locs:
                if "auctions" in loc.text:
                    auction_sitemap = loc.text
                    break
            
            if auction_sitemap:
                print(f"Found auctions sitemap: {auction_sitemap}")
                response = requests.get(auction_sitemap, headers=headers, timeout=30)
                if response.status_code == 200:
                    soup = BeautifulSoup(response.text, "xml")
                    locs = soup.find_all("loc")
                    urls = [loc.text.strip() for loc in locs if "/auctions/" in loc.text]
                    if urls:
                        print(f"Found {len(urls)} auction URLs from sitemap")
                        return urls
    except Exception as e:
        print(f"Sitemap failed: {e}")
    
    print("Trying past auctions page...")
    try:
        with sync_playwright() as p:
            browser = p.chromium.launch(headless=True)
            page = browser.new_page()
            
            page.goto("https://carsandbids.com/past-auctions/", timeout=60_000)
            
            print("Waiting for auction cards to appear...")
            try:
                page.wait_for_selector("a[href*='/auctions/']", timeout=30_000)
                print("Auction links found")
            except:
                print("Timeout waiting for links, trying anyway...")
            
            time.sleep(10)
            
            for i in range(10):
                page.evaluate("window.scrollBy(0, 1000)")
                time.sleep(1)
            
            links = page.query_selector_all("a")
            urls = set()
            
            for link in links:
                href = link.get_attribute("href")
                if href and "/auctions/" in href and href != "/past-auctions/":
                    if href.startswith("/"):
                        href = "https://carsandbids.com" + href
                    urls.add(href)
            
            browser.close()
            
            urls = list(urls)
            print(f"Found {len(urls)} auction URLs from past auctions page")
            return urls
            
    except Exception as e:
        print(f"Past auctions failed: {e}")
        traceback.print_exc()
        return []

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

def clean_text(text):
    """Clean text by removing extra whitespace and 'Save'"""
    if not text:
        return ""
    text = re.sub(r'\s+', ' ', text)
    text = re.sub(r'\s*save\s*', '', text, flags=re.IGNORECASE)
    return text.strip()

def extract_all_auction_data(page, auction_url):
    """Extract comprehensive data from CNB auction page"""
    
    data = {
        "model": "",
        "make": "",
        "vin": "",
        "engine": "",
        "drivetrain": "",
        "transmission": "",
        "body_style": "",
        "exterior_color": "",
        "interior_color": "",
        "title_status": "",
        "location": "",
        "mileage": "",
        "sale_amount": "",
        "sale_date": "",
        "sale_type": "",
        "bids": 0,
        "views": "",
        "comments": 0,
        "seller": "",
        "auction_url": auction_url,
        "year": None,
        "scraped_date": datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    }
    
    try:
        page.wait_for_selector("body", timeout=15000)
        time.sleep(2)
        
        try:
            title_element = page.query_selector("h1")
            if title_element:
                data["model"] = clean_text(title_element.inner_text())
        except:
            pass
        
        data["year"] = extract_year_from_url(auction_url)
        if not data["year"] and data["model"]:
            year_match = re.search(r'\b(19|20)\d{2}\b', data["model"])
            if year_match:
                data["year"] = int(year_match.group(0))
        
        try:
            bid_selectors = [
                "span.bid-value",
                ".bid-value",
                ".final-bid",
                ".current-bid"
            ]
            for selector in bid_selectors:
                element = page.query_selector(selector)
                if element:
                    text = element.inner_text().strip()
                    if text:
                        data["sale_amount"] = text
                        break
        except:
            pass
        
        try:
            date_element = page.query_selector("span.time-ended") or page.query_selector(".auction-end-time")
            if date_element:
                data["sale_date"] = date_element.inner_text().strip()
            
            sale_type_element = page.query_selector("span.value")
            if sale_type_element:
                sale_text = sale_type_element.inner_text().lower()
                if "sold" in sale_text:
                    data["sale_type"] = "sold"
                elif "reserve" in sale_text:
                    data["sale_type"] = "reserve not met"
                else:
                    data["sale_type"] = sale_text
        except:
            pass
        
        try:
            bids_element = page.query_selector("li.num-bids")
            if bids_element:
                bids_text = bids_element.inner_text()
                bids_match = re.search(r'(\d+)', bids_text)
                if bids_match:
                    data["bids"] = int(bids_match.group(1))
        except:
            pass
        
        try:
            views_element = page.query_selector("li span.views")
            if views_element:
                data["views"] = views_element.inner_text().replace(",", "")
        except:
            pass
        
        try:
            comments_element = page.query_selector(".comments-count") or page.query_selector(".comment-count")
            if comments_element:
                comments_text = comments_element.inner_text()
                comments_match = re.search(r'(\d+)', comments_text)
                if comments_match:
                    data["comments"] = int(comments_match.group(1))
        except:
            pass
        
        try:
            seller_element = page.query_selector("li.seller")
            if seller_element:
                data["seller"] = clean_text(seller_element.inner_text())
        except:
            pass
        
        try:
            fact_containers = page.query_selector_all("dl")
            for container in fact_containers:
                dt_elements = container.query_selector_all("dt")
                for dt in dt_elements:
                    try:
                        key = dt.inner_text().strip().replace(" ", "_").lower()
                        dd = dt.evaluate_handle("el => el.nextElementSibling")
                        if dd and dd.as_element():
                            value = clean_text(dd.as_element().inner_text())
                            if value and key:
                                if key == "make":
                                    data["make"] = value
                                elif key == "model":
                                    data["model"] = value if not data["model"] else data["model"]
                                elif key == "vin":
                                    data["vin"] = value
                                elif key == "engine":
                                    data["engine"] = value
                                elif key == "drivetrain":
                                    data["drivetrain"] = value
                                elif key == "transmission":
                                    data["transmission"] = value
                                elif key == "body_style":
                                    data["body_style"] = value
                                elif key == "exterior_color":
                                    data["exterior_color"] = value
                                elif key == "interior_color":
                                    data["interior_color"] = value
                                elif key == "title_status":
                                    data["title_status"] = value
                                elif key == "location":
                                    data["location"] = value
                                elif key == "mileage":
                                    data["mileage"] = value
                    except:
                        continue
        except Exception as e:
            print(f"    Facts extraction error: {e}")
        
        if not data["make"] and data["model"]:
            model_words = data["model"].split()
            if len(model_words) > 0:
                common_makes = ['Toyota', 'Honda', 'Ford', 'Chevrolet', 'BMW', 'Mercedes', 
                               'Audi', 'Volkswagen', 'Nissan', 'Mazda', 'Porsche', 'Ferrari']
                for word in model_words:
                    if any(make.lower() == word.lower() for make in common_makes):
                        data["make"] = word
                        break
        
        print(f"    Extracted: {data['model'][:40] if data['model'] else 'Unknown'}... | "
              f"${data['sale_amount']} | {data['views']} views | {data['bids']} bids")
        
        return data
        
    except Exception as e:
        print(f"    Extraction error: {str(e)[:100]}")
        traceback.print_exc()
        return data

def main():
    print(f"Starting CNB Scraper (Append Mode) - {datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    
    existing_df, existing_urls = download_existing_cnb_csv()
    
    all_urls = get_sitemap_urls()
    
    if not all_urls:
        print("Failed to get sitemap URLs!")
        return False
    
    new_urls = [url for url in all_urls if url not in existing_urls]
    print(f"Found {len(new_urls)} new auctions to scrape")
    
    if not new_urls:
        print("No new auctions found - cnb.csv is up to date!")
        return True
    
    new_urls = new_urls[:MAX_AUCTIONS_PER_RUN]
    print(f"Processing {len(new_urls)} new auctions (max {MAX_AUCTIONS_PER_RUN} per run)")
    
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
        skipped_in_progress = 0
        
        for i, auction_url in enumerate(new_urls):
            print(f"\n[{i+1}/{len(new_urls)}] Processing: {auction_url}")
            page = None
            
            try:
                page = context.new_page()
                
                for retry in range(3):
                    try:
                        page.goto(auction_url, timeout=45_000, wait_until="domcontentloaded")
                        break
                    except Exception as nav_error:
                        if retry == 2:
                            raise nav_error
                        print(f"  Retry {retry + 1}")
                        time.sleep(5)
                
                data = extract_all_auction_data(page, auction_url)
                
                if not data['sale_date'] or data['sale_date'].strip() == "":
                    print(f"  Skipping - auction still in progress")
                    skipped_in_progress += 1
                    continue
                
                if data['model'] or data['views'] or data['bids']:
                    new_rows.append(data)
                    successful += 1
                else:
                    print(f"  Insufficient data extracted")
                    failed += 1
                    
            except Exception as e:
                print(f"  Error: {str(e)[:150]}")
                failed += 1
                
            finally:
                if page:
                    page.close()
                time.sleep(SLEEP_BETWEEN_AUCTIONS)
                
                if len(new_rows) > 0 and len(new_rows) % 50 == 0:
                    print(f"\nSaving progress ({len(new_rows)} new rows)...")
                    temp_df = pd.concat([existing_df, pd.DataFrame(new_rows)], ignore_index=True)
                    upload_updated_cnb_csv(temp_df)
        
        browser.close()
        
        print(f"\nScraping complete:")
        print(f"   Successful: {successful}")
        print(f"   In-progress skipped: {skipped_in_progress}")
        print(f"   Failed: {failed}")
    
    if new_rows:
        print(f"\nAdding {len(new_rows)} new rows to cnb.csv")
        new_df = pd.DataFrame(new_rows)
        
        updated_df = pd.concat([existing_df, new_df], ignore_index=True)
        
        before_dedup = len(updated_df)
        updated_df = updated_df.drop_duplicates(subset=['auction_url'], keep='first')
        after_dedup = len(updated_df)
        if before_dedup != after_dedup:
            print(f"Removed {before_dedup - after_dedup} duplicate rows")
        
        updated_df = updated_df.sort_values('year', ascending=False, na_position='last')
        
        print(f"Updated cnb.csv stats:")
        print(f"   Total rows: {len(updated_df)}")
        print(f"   Total unique auctions: {updated_df['auction_url'].nunique()}")
        if pd.notna(updated_df['year']).any():
            print(f"   Years covered: {updated_df['year'].min():.0f} to {updated_df['year'].max():.0f}")
        
        if upload_updated_cnb_csv(updated_df):
            print(f"Successfully updated cnb.csv in S3!")
            return True
        else:
            print(f"Failed to upload updated cnb.csv")
            return False
    else:
        print(f"No new completed auctions to add")
        return True

if __name__ == "__main__":
    try:
        success = main()
        exit(0 if success else 1)
    except Exception as e:
        print(f"Fatal error: {e}")
        traceback.print_exc()
        exit(1)
