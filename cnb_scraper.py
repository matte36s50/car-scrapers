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

# Incremental mode for GitHub Actions
SLEEP_BETWEEN_AUCTIONS = 2.0 if os.getenv('GITHUB_ACTIONS') == 'true' else 3.0
MAX_AUCTIONS_PER_RUN = int(os.getenv('MAX_AUCTIONS_PER_RUN', '100' if os.getenv('GITHUB_ACTIONS') == 'true' else '300'))

print(f"Running in {'GitHub Actions' if os.getenv('GITHUB_ACTIONS') else 'local'} mode")
print(f"Max auctions per run: {MAX_AUCTIONS_PER_RUN}")
print(f"Sleep between auctions: {SLEEP_BETWEEN_AUCTIONS}s")

def get_sitemap_urls():
    """Get CNB auction URLs - BROWSER-BASED (sitemap is blocked)"""
    print("Fetching CNB auction URLs...")

    # METHOD 1: Try sitemap first (usually blocked but worth trying)
    try:
        headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
            'Accept': 'application/xml,text/xml,*/*',
            'Accept-Encoding': 'gzip, deflate, br',
            'Connection': 'keep-alive'
        }

        print("Trying sitemap (may be blocked)...")
        sitemap_url = "https://carsandbids.com/sitemap.xml"
        response = requests.get(sitemap_url, headers=headers, timeout=15)

        if response.status_code == 200:
            print(f"✓ Got main sitemap ({len(response.text)} chars)")
            soup = BeautifulSoup(response.text, "xml")
            locs = soup.find_all("loc")

            # Find auctions sitemap
            auction_sitemap = None
            for loc in locs:
                if "auctions" in loc.text.lower():
                    auction_sitemap = loc.text
                    break

            if auction_sitemap:
                print(f"✓ Found auctions sitemap: {auction_sitemap}")
                response = requests.get(auction_sitemap, headers=headers, timeout=15)
                if response.status_code == 200:
                    print(f"✓ Got auctions sitemap ({len(response.text)} chars)")
                    soup = BeautifulSoup(response.text, "xml")
                    locs = soup.find_all("loc")
                    urls = [loc.text.strip() for loc in locs if "/auctions/" in loc.text]

                    if urls:
                        print(f"✓ Found {len(urls)} auction URLs via sitemap")
                        return urls
        else:
            print(f"⚠ Sitemap returned {response.status_code}")
    except Exception as e:
        print(f"⚠ Sitemap method failed: {e}")

    # METHOD 2: Fallback to past auctions page (uses Playwright)
    print("\nUsing Playwright to load past auctions page...")
    try:
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
                user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
            )
            page = context.new_page()

            print("Loading past auctions page...")
            page.goto("https://carsandbids.com/past-auctions/", timeout=60_000, wait_until="networkidle")

            print("Waiting for auction cards to appear...")
            try:
                page.wait_for_selector("a[href*='/auctions/']", timeout=30_000)
                print("✓ Auction links found")
            except:
                print("⚠ Timeout waiting for links, trying anyway...")

            time.sleep(3)

            # Scroll multiple times to load more auctions
            print("Scrolling to load more auctions...")
            prev_count = 0
            for i in range(20):
                page.evaluate("window.scrollTo(0, document.body.scrollHeight)")
                time.sleep(1.5)

                # Check if we've loaded more
                links = page.query_selector_all("a[href*='/auctions/']")
                curr_count = len(links)
                if curr_count > prev_count:
                    print(f"  Scroll {i+1}: {curr_count} links loaded")
                    prev_count = curr_count
                elif i > 5:
                    # Stop if no new links after 5 attempts
                    break

            # Collect all auction URLs
            links = page.query_selector_all("a[href*='/auctions/']")
            urls = set()

            for link in links:
                href = link.get_attribute("href")
                if href and "/auctions/" in href:
                    # Skip non-auction pages
                    if any(x in href for x in ['/past-auctions', '/live-auctions', '/search']):
                        continue
                    if href.startswith("/"):
                        href = "https://carsandbids.com" + href
                    # Only include actual auction URLs (should have a slug after /auctions/)
                    if re.match(r'https://carsandbids\.com/auctions/[a-zA-Z0-9-]+', href):
                        urls.add(href)

            browser.close()

            urls = list(urls)
            print(f"✓ Found {len(urls)} auction URLs from past auctions page")
            return urls

    except Exception as e:
        print(f"✗ Past auctions page failed: {e}")
        traceback.print_exc()
        return []

def extract_year_from_url(url):
    """Extract year from CNB URL patterns"""
    if not url:
        return None

    # Allow years up to 5 years in the future (for upcoming model years)
    max_year = datetime.datetime.now().year + 5

    patterns = [
        r'/auctions/[^/]*-(\d{4})-',
        r'/auctions/(\d{4})-',
        r'-(\d{4})-'
    ]

    for pattern in patterns:
        match = re.search(pattern, url)
        if match:
            year = int(match.group(1))
            if 1900 <= year <= max_year:
                return year
    return None

def clean_text(text):
    """Clean text by removing extra whitespace and 'Save'"""
    if not text:
        return ""
    text = re.sub(r'\s+', ' ', text)
    text = re.sub(r'\s*save\s*', '', text, flags=re.IGNORECASE)
    return text.strip()

def extract_number_from_text(text):
    """Extract numeric value from text, handling commas"""
    if not text:
        return 0
    text = str(text).replace(',', '')
    match = re.search(r'(\d+)', text)
    if match:
        return int(match.group(1))
    return 0

def extract_all_auction_data(page, auction_url):
    """Extract comprehensive data from CNB auction page - UPDATED SELECTORS"""

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
        "views": 0,
        "comments": 0,
        "seller": "",
        "auction_url": auction_url,
        "year": None,
        "scraped_date": datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    }

    try:
        page.wait_for_selector("body", timeout=15000)
        time.sleep(2)

        # Extract title from .auction-title h1 or fallback to h1
        try:
            title_element = page.query_selector(".auction-title h1")
            if not title_element:
                title_element = page.query_selector("h1")
            if title_element:
                data["model"] = clean_text(title_element.inner_text())
        except:
            pass

        # Extract year from URL or model text
        data["year"] = extract_year_from_url(auction_url)
        if not data["year"] and data["model"]:
            year_match = re.search(r'\b(19|20)\d{2}\b', data["model"])
            if year_match:
                data["year"] = int(year_match.group(0))

        # Extract sale amount from .bid-value
        try:
            bid_element = page.query_selector(".bid-value")
            if bid_element:
                text = bid_element.inner_text().strip()
                if text:
                    data["sale_amount"] = text
        except:
            pass

        # Extract auction status and sale type from .current-bid.ended or #auction-jump
        is_auction_ended = False
        try:
            # Check for .ended class which indicates auction has ended
            ended_element = page.query_selector(".current-bid.ended")
            if ended_element:
                is_auction_ended = True

            # Check for reserve status
            reserve_element = page.query_selector("#auction-jump h3 span")
            if reserve_element:
                reserve_text = reserve_element.inner_text().lower()
                if "reserve" in reserve_text and "not met" in reserve_text:
                    data["sale_type"] = "reserve not met"
                    is_auction_ended = True
                elif "sold" in reserve_text:
                    data["sale_type"] = "sold"
                    is_auction_ended = True
                else:
                    data["sale_type"] = reserve_text

            # Check status header
            status_container = page.query_selector(".current-bid.ended")
            if status_container:
                status_header = status_container.query_selector("h4")
                if status_header:
                    status_text = status_header.inner_text().lower()
                    if "sold" in status_text:
                        data["sale_type"] = "sold"
                        is_auction_ended = True
                    elif "bid to" in status_text:
                        # On Cars & Bids, "Bid to $X" means the auction ended
                        # WITHOUT selling — the reserve was not met. This is a
                        # no-sale, not a sale.
                        data["sale_type"] = "reserve not met"
                        is_auction_ended = True
                    elif "reserve" in status_text:
                        data["sale_type"] = "reserve not met"
                        is_auction_ended = True
                    elif not data["sale_type"]:
                        data["sale_type"] = status_text
        except:
            pass

        # Store is_ended flag for later use
        data["_is_ended"] = is_auction_ended

        # Extract stats (bids, views, etc.) from ul.stats
        try:
            stats_list = page.query_selector("ul.stats")
            if stats_list:
                stat_items = stats_list.query_selector_all("li:not(.seller)")
                for item in stat_items:
                    try:
                        label_el = item.query_selector(".th")
                        value_el = item.query_selector(".td")
                        if label_el and value_el:
                            label = label_el.inner_text().strip().lower()
                            value_text = value_el.inner_text().strip()

                            if "bid" in label:
                                data["bids"] = extract_number_from_text(value_text)
                            elif "view" in label:
                                data["views"] = extract_number_from_text(value_text)
                            elif "comment" in label:
                                data["comments"] = extract_number_from_text(value_text)
                    except:
                        continue
        except:
            pass

        # Extract seller from li.seller .user
        try:
            seller_element = page.query_selector("li.seller .user")
            if seller_element:
                data["seller"] = clean_text(seller_element.inner_text())
            else:
                # Fallback to just li.seller
                seller_element = page.query_selector("li.seller")
                if seller_element:
                    data["seller"] = clean_text(seller_element.inner_text())
        except:
            pass

        # Extract date from auction end info
        try:
            # Try to get date from the status container
            status_container = page.query_selector(".current-bid.ended")
            if status_container:
                # Look for date text pattern in the container
                full_text = status_container.inner_text()
                date_match = re.search(r'(\d{1,2}/\d{1,2}/\d{2,4}|\w+\s+\d{1,2},?\s+\d{4})', full_text)
                if date_match:
                    data["sale_date"] = date_match.group(1)

            # Also check for time-ended class
            if not data["sale_date"]:
                date_element = page.query_selector(".time-ended, .auction-end-time")
                if date_element:
                    data["sale_date"] = date_element.inner_text().strip()
        except:
            pass

        # Extract quick facts from .quick-facts dl
        try:
            quick_facts = page.query_selector(".quick-facts")
            if quick_facts:
                fact_containers = quick_facts.query_selector_all("dl")
            else:
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
            pass

        # Auto-detect make from model if not found
        if not data["make"] and data["model"]:
            model_words = data["model"].split()
            if len(model_words) > 0:
                common_makes = ['Toyota', 'Honda', 'Ford', 'Chevrolet', 'BMW', 'Mercedes',
                               'Audi', 'Volkswagen', 'Nissan', 'Mazda', 'Porsche', 'Ferrari',
                               'Lamborghini', 'McLaren', 'Lexus', 'Acura', 'Infiniti', 'Jaguar',
                               'Land', 'Range', 'Rover', 'Jeep', 'Dodge', 'Ram', 'Chrysler',
                               'Buick', 'Cadillac', 'GMC', 'Lincoln', 'Volvo', 'Saab', 'Subaru',
                               'Mitsubishi', 'Hyundai', 'Kia', 'Genesis', 'Alfa', 'Fiat', 'Maserati',
                               'Bentley', 'Rolls', 'Royce', 'Aston', 'Martin', 'Lotus', 'Tesla']
                for word in model_words:
                    if any(make.lower() == word.lower() for make in common_makes):
                        data["make"] = word
                        break

        # VALIDATION: Ensure bids is not a year (allow up to 5 years in future)
        max_year = datetime.datetime.now().year + 5
        if data["bids"] >= 1900 and data["bids"] <= max_year:
            print(f"    ⚠ WARNING: Bids value {data['bids']} looks like a year! Setting to 0.")
            data["bids"] = 0

        # Set sale_date to scraped_date if auction has ended but no date found
        if data["sale_type"] and not data["sale_date"]:
            data["sale_date"] = data["scraped_date"].split()[0]  # Just the date part

        print(f"    ✓ {data['model'][:40] if data['model'] else 'Unknown'}... | "
              f"${data['sale_amount']} | {data['views']} views | {data['bids']} bids | Year: {data['year']}")

        return data

    except Exception as e:
        print(f"    ✗ Extraction error: {str(e)[:100]}")
        return data

def main(start_date=None, end_date=None, max_auctions=None, rescrape_urls=None,
         recheck_sold=False, recheck_limit=None):
    print(f"Starting CNB Scraper - {datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")

    effective_max = max_auctions if max_auctions is not None else MAX_AUCTIONS_PER_RUN
    is_backfill = bool(start_date or end_date)
    if is_backfill:
        print(f"[BACKFILL MODE] Date range: {start_date or 'unset'} → {end_date or 'unset'}")
    start_dt = datetime.datetime.strptime(start_date, "%Y-%m-%d").date() if start_date else None
    end_dt   = datetime.datetime.strptime(end_date,   "%Y-%m-%d").date() if end_date   else None

    existing_df, existing_urls = download_existing_cnb_csv()

    # Bulk repair: re-verify every C&B row currently recorded as a sale. The
    # "Bid to $X" (reserve-not-met) bug wrote sale_type='sold' for both real
    # sales and no-sales, so the two are indistinguishable in stored data — the
    # only way to tell them apart is to re-scrape. Genuine sales re-confirm as
    # sold (no change); "Bid to" no-sales flip to reserve_not_met with the
    # corrected scraper, and the canonical upsert clears their stale price.
    if recheck_sold and not rescrape_urls:
        sold_mask = existing_df['sale_type'].astype(str).str.contains('sold', case=False, na=False)
        candidates = existing_df.loc[sold_mask, 'auction_url'].dropna().tolist()
        candidates = [u for u in candidates if isinstance(u, str) and '/auctions/' in u]
        # Preserve order, drop duplicates.
        seen, rescrape_urls = set(), []
        for u in candidates:
            if u not in seen:
                seen.add(u)
                rescrape_urls.append(u)
        if recheck_limit:
            rescrape_urls = rescrape_urls[:recheck_limit]
        print(f"[RECHECK-SOLD] {len(rescrape_urls)} C&B listing(s) currently marked sold "
              f"will be re-scraped to correct any 'Bid to' no-sales"
              + (f" (limited to {recheck_limit})" if recheck_limit else ""))
        if not rescrape_urls:
            print("✓ No sold C&B rows found in cnb.csv — nothing to recheck")
            return True

    if rescrape_urls:
        print(f"[RESCRAPE MODE] {len(rescrape_urls)} URL(s) — these will be "
              f"re-scraped even if already in cnb.csv, and re-pushed to the canonical store")
        # Repair path: no sitemap collection, no already-scraped filter. The
        # dedup below keeps the freshly scraped rows (keep='last') so they
        # replace the old CSV rows, and the canonical push upserts the store's
        # copy — used when an auction's outcome was recorded wrong (e.g. a
        # "Bid to" no-sale previously mislabeled as "sold").
        new_urls = list(rescrape_urls)
        print(f"Re-scraping {len(new_urls)} supplied URL(s)...")
    else:
        all_urls = get_sitemap_urls()

        if not all_urls:
            print("✗ Failed to get sitemap URLs!")
            return False

        new_urls = [url for url in all_urls if url not in existing_urls]
        print(f"Found {len(new_urls)} new auctions to scrape")

        if not new_urls:
            print("✓ No new auctions found - cnb.csv is up to date!")
            return True

        new_urls = new_urls[:effective_max]
        print(f"Processing {len(new_urls)} new auctions (max {effective_max} per run)")
    
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
        consecutive_too_old = 0
        
        for i, auction_url in enumerate(new_urls):
            print(f"\n[{i+1}/{len(new_urls)}] {auction_url}")
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

                # Check if auction has ended using the flag
                is_ended = data.pop('_is_ended', False)

                # Backfill date-range enforcement
                if is_backfill and data.get('sale_date'):
                    try:
                        auction_dt = None
                        for fmt in ("%m/%d/%y", "%m/%d/%Y", "%B %d, %Y", "%b %d, %Y"):
                            try:
                                auction_dt = datetime.datetime.strptime(data['sale_date'].strip(), fmt).date()
                                break
                            except ValueError:
                                continue
                        if auction_dt:
                            if end_dt and auction_dt > end_dt:
                                print(f"  [BACKFILL] Skipping {auction_dt} — after end_date {end_dt}")
                                continue
                            if start_dt and auction_dt < start_dt:
                                consecutive_too_old += 1
                                print(f"  [BACKFILL] Skipping {auction_dt} — before start_date {start_dt} ({consecutive_too_old}/10)")
                                if consecutive_too_old >= 10:
                                    print("  [BACKFILL] 10 consecutive out-of-range — stopping early")
                                    break
                                continue
                            else:
                                consecutive_too_old = 0
                    except Exception as date_err:
                        print(f"  [BACKFILL] Could not parse sale_date '{data.get('sale_date')}': {date_err}")

                if not is_ended and not data['sale_type']:
                    print(f"  ⊘ Skipping - auction in progress")
                    skipped_in_progress += 1
                    continue

                if data['model'] or data['views'] or data['bids']:
                    new_rows.append(data)
                    successful += 1
                else:
                    print(f"  ⚠ Insufficient data")
                    failed += 1
                    
            except Exception as e:
                print(f"  ✗ Error: {str(e)[:150]}")
                failed += 1
                
            finally:
                if page:
                    page.close()
                time.sleep(SLEEP_BETWEEN_AUCTIONS)
                
                if len(new_rows) > 0 and len(new_rows) % 50 == 0:
                    print(f"\n💾 Saving progress ({len(new_rows)} rows)...")
                    temp_df = pd.concat([existing_df, pd.DataFrame(new_rows)], ignore_index=True)
                    upload_updated_cnb_csv(temp_df)
        
        browser.close()
        
        print(f"\n{'='*60}")
        print(f"Scraping complete:")
        print(f"   ✓ Successful: {successful}")
        print(f"   ⊘ In-progress: {skipped_in_progress}")
        print(f"   ✗ Failed: {failed}")
        print(f"{'='*60}")
    
    if new_rows:
        # Dual-write this run's new records to the canonical store (no-op
        # unless SUPABASE_URL/SUPABASE_SERVICE_ROLE_KEY are set; never breaks
        # the CSV path)
        import canonical_store
        canonical_store.push_cnb_records(new_rows)

        print(f"\n💾 Saving {len(new_rows)} new rows...")
        new_df = pd.DataFrame(new_rows)
        
        updated_df = pd.concat([existing_df, new_df], ignore_index=True)
        
        before_dedup = len(updated_df)
        # In rescrape mode the freshly scraped rows are appended last, so
        # keep='last' lets them overwrite the stale CSV rows for the same URL.
        dedup_keep = 'last' if rescrape_urls else 'first'
        updated_df = updated_df.drop_duplicates(subset=['auction_url'], keep=dedup_keep)
        after_dedup = len(updated_df)
        if before_dedup != after_dedup:
            print(f"Removed {before_dedup - after_dedup} duplicates")
        
        updated_df = updated_df.sort_values('year', ascending=False, na_position='last')
        
        print(f"\n📊 Final stats:")
        print(f"   Total rows: {len(updated_df)}")
        print(f"   Unique auctions: {updated_df['auction_url'].nunique()}")
        if pd.notna(updated_df['year']).any():
            print(f"   Years: {updated_df['year'].min():.0f}-{updated_df['year'].max():.0f}")
        
        # Validate bids (allow years up to 5 years in future)
        max_year = datetime.datetime.now().year + 5
        bad_bids = updated_df[(updated_df['bids'] >= 1900) & (updated_df['bids'] <= max_year)]
        if len(bad_bids) > 0:
            print(f"\n⚠  WARNING: {len(bad_bids)} entries with suspicious bids, fixing...")
            updated_df.loc[(updated_df['bids'] >= 1900) & (updated_df['bids'] <= max_year), 'bids'] = 0
        
        if upload_updated_cnb_csv(updated_df):
            print(f"\n✅ Successfully updated cnb.csv in S3!")
            return True
        else:
            print(f"\n✗ Failed to upload")
            return False
    else:
        print(f"\n✓ No new completed auctions")
        return True

if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser(description="Cars & Bids Auction Scraper")
    parser.add_argument("--start-date", metavar="YYYY-MM-DD", default=None,
                        help="Only collect auctions ending on or after this date (backfill mode)")
    parser.add_argument("--end-date", metavar="YYYY-MM-DD", default=None,
                        help="Only collect auctions ending on or before this date (backfill mode)")
    parser.add_argument("--max-auctions", type=int, default=None,
                        help=f"Max auctions per run (overrides MAX_AUCTIONS_PER_RUN, default: {MAX_AUCTIONS_PER_RUN})")
    parser.add_argument("--rescrape-urls", metavar="URL", nargs="+", default=None,
                        help="Re-scrape these specific auction URLs even if already in cnb.csv "
                             "(repair mode for records whose outcome was recorded wrong)")
    parser.add_argument("--recheck-sold", action="store_true",
                        help="Bulk repair: re-scrape every C&B row in cnb.csv currently marked "
                             "sold, correcting any 'Bid to' no-sales to reserve_not_met")
    parser.add_argument("--recheck-limit", type=int, default=None,
                        help="Cap the number of listings re-scraped by --recheck-sold (per run)")
    args = parser.parse_args()
    try:
        success = main(start_date=args.start_date, end_date=args.end_date,
                       max_auctions=args.max_auctions, rescrape_urls=args.rescrape_urls,
                       recheck_sold=args.recheck_sold, recheck_limit=args.recheck_limit)
        exit(0 if success else 1)
    except Exception as e:
        print(f"\n✗ Fatal error: {e}")
        traceback.print_exc()
        exit(1)
