import re
import os
import json
import csv
import pandas as pd
import datetime
from playwright.sync_api import sync_playwright, TimeoutError as PlaywrightTimeoutError
import boto3
from botocore.exceptions import NoCredentialsError

print("=" * 60)
print("BAT SCRAPER STARTING (FIXED MODEL PARSING)")
print(f"Timestamp: {datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
print("=" * 60)

print("\n[1/8] Importing libraries...")
print("All imports successful")

# === S3 UPLOAD CODE ===
def upload_to_s3(file_name, bucket, object_name=None):
    s3 = boto3.client('s3')
    if object_name is None:
        object_name = file_name
    try:
        s3.upload_file(file_name, bucket, object_name)
        print(f"File {file_name} uploaded to s3://{bucket}/{object_name}")
        return True
    except NoCredentialsError:
        print("AWS credentials not available")
        return False
    except Exception as e:
        print(f"Upload failed: {e}")
        return False

def download_existing_bat_csv():
    """Download existing bat.csv from S3 to append to it"""
    print("\n[2/8] Downloading existing data from S3...")
    s3 = boto3.client('s3')
    try:
        s3.download_file('my-mii-reports', 'bat.csv', 'existing_bat.csv')
        print(f"Downloaded existing bat.csv from S3")
        
        # Load existing data
        existing_df = pd.read_csv('existing_bat.csv', low_memory=False)
        print(f"Found {len(existing_df)} existing rows")
        
        # Get existing URLs to avoid duplicates
        existing_urls = set(existing_df['auction_url'].dropna().values) if 'auction_url' in existing_df.columns else set()
        
        return existing_df, existing_urls
    except s3.exceptions.NoSuchKey:
        print("No existing bat.csv in S3 - starting fresh")
        return pd.DataFrame(), set()
    except Exception as e:
        print(f"Could not download existing data: {e}")
        return pd.DataFrame(), set()

def extract_year_from_url(url):
    """Extract year from BAT URL pattern"""
    if not url:
        return None

    try:
        # Allow years up to 5 years in the future (for upcoming model years)
        max_year = datetime.datetime.now().year + 5

        # Primary pattern: /listing/YYYY-make-model/
        match = re.search(r'/listing/(\d{4})-', url)
        if match:
            year = int(match.group(1))
            if 1900 <= year <= max_year:
                return year

        # Secondary pattern: look for YYYY in the URL path after listing/
        match = re.search(r'/listing/[^/]*?(\d{4})', url)
        if match:
            year = int(match.group(1))
            if 1900 <= year <= max_year:
                return year

        return None
    except Exception as e:
        print(f"Error extracting year from URL {url}: {e}")
        return None

def extract_year_from_title(title):
    """Extract year from title"""
    if not title:
        return None

    try:
        # Allow years up to 5 years in the future (for upcoming model years)
        max_year = datetime.datetime.now().year + 5

        # Pattern 1: Year at start "2007 Mercedes-Benz"
        match = re.search(r'^(\d{4})\s+', title)
        if match:
            year = int(match.group(1))
            if 1900 <= year <= max_year:
                return year

        # Pattern 2: Year in parentheses "(2007)"
        match = re.search(r'\((\d{4})\)', title)
        if match:
            year = int(match.group(1))
            if 1900 <= year <= max_year:
                return year

        # Pattern 3: Any 4-digit year in title
        match = re.search(r'\b(\d{4})\b', title)
        if match:
            year = int(match.group(1))
            if 1900 <= year <= max_year:
                return year

        return None
    except Exception as e:
        print(f"Error extracting year from title {title}: {e}")
        return None

def extract_model_from_url(url):
    """
    Extract proper model name from BaT URL slug
    Example: /listing/2007-mercedes-benz-sl65-amg-37/ -> SL65 AMG
    """
    if not url:
        return None
    
    try:
        # Extract the slug between /listing/ and the trailing /
        match = re.search(r'/listing/([^/]+)/', url)
        if not match:
            return None
        
        slug = match.group(1)
        
        # Remove trailing number if present (e.g., "-37" in the example)
        slug = re.sub(r'-\d+$', '', slug)
        
        # Split by hyphens
        parts = slug.split('-')
        
        # Remove year if it's the first part
        if parts and re.match(r'^\d{4}$', parts[0]):
            parts = parts[1:]
        
        # Remove common make names (we already have "make" field)
        common_makes = {'mercedes', 'benz', 'porsche', 'ferrari', 'bmw', 'audi', 
                       'ford', 'chevrolet', 'dodge', 'toyota', 'honda', 'nissan'}
        parts = [p for p in parts if p.lower() not in common_makes]
        
        # Join remaining parts with spaces and title case
        if parts:
            model = ' '.join(parts).upper()
            # Keep common patterns uppercase (AMG, GT, SL, etc.)
            return model
        
        return None
        
    except Exception as e:
        print(f"Error extracting model from URL {url}: {e}")
        return None

def is_generic_model(model_str):
    """
    Check if the model string is too generic and should be replaced
    """
    if not model_str:
        return True
    
    model_lower = model_str.lower().strip()
    
    # Generic patterns that should be replaced
    generic_patterns = [
        'mercedes-benz amg',
        'mercedes amg',
        'bmw m',
        'porsche turbo',
        'ford mustang',  # Too generic without generation
        'chevrolet corvette',  # Too generic without generation
    ]
    
    return any(pattern in model_lower for pattern in generic_patterns)

# CONFIGURATION
BASE_URL = "https://bringatrailer.com"
RESULTS_URL = f"{BASE_URL}/auctions/results/"
MAX_AUCTIONS = 500

SELECTORS = {
    # results page
    "tile": "#auctions-completed-container > div.listings-container.auctions-grid > a",
    "load_more": "button.auctions-footer-button",
    # auction page
    "sale_span": "span.info-value.noborder-tiny",
    "sale_amount": "span.info-value.noborder-tiny > strong",
    "comments": "a > span > span.info-value",
    "bids": "td.listing-stats-value.number-bids-value",
    "views": "#listing-actions-stats > div:nth-child(1) > span",
    "watchers": "#listing-actions-stats > div:nth-child(2) > span",
    "end_span": "#listing-bid > tbody > tr:nth-child(2) > td.listing-stats-value > span",
    "title": "h1.listing-title",
    "seller_type": "div.item.additional",
    "group_items": "div.group-item-wrap > div.group-item",
}

def collect_auction_urls(page, results_url=RESULTS_URL, max_auctions=MAX_AUCTIONS):
    """Collect auction URLs from results page."""
    tile_sel = SELECTORS["tile"]
    btn_sel  = SELECTORS["load_more"]

    print(f"\n[4/8] Navigating to results page: {results_url}")
    page.goto(results_url, timeout=60_000)
    print("Page loaded successfully")

    # Patch fetch + XHR inside the page JS so we capture network calls
    # even during Playwright blocking waits (page.on events miss these)
    page.evaluate("""() => {
        window.__netLog = [];
        const _fetch = window.fetch;
        window.fetch = function(...args) {
            const url = typeof args[0] === 'string' ? args[0] : args[0]?.url;
            const method = args[1]?.method || 'GET';
            window.__netLog.push('fetch ' + method + ' ' + url);
            return _fetch.apply(this, args);
        };
        const _open = XMLHttpRequest.prototype.open;
        XMLHttpRequest.prototype.open = function(method, url) {
            window.__netLog.push('xhr ' + method + ' ' + url);
            return _open.apply(this, arguments);
        };
    }""")

    page.wait_for_selector(tile_sel)
    print("Auction tiles found")

    urls = []
    loaded = 0
    consecutive_failures = 0
    max_failures = 3

    while loaded < max_auctions:
        # Count tiles entirely in JS — never create an ElementHandle
        current = page.evaluate("s => document.querySelectorAll(s).length", tile_sel)
        print(f"Loaded {current}/{max_auctions} listings")

        if current == loaded:
            consecutive_failures += 1
            print(f"No new listings loaded (attempt {consecutive_failures}/{max_failures})")
            if consecutive_failures >= max_failures:
                print("Reached end of listings or load button not working")
                break
        else:
            consecutive_failures = 0

        # Batch-extract hrefs in JS; returns plain strings, no ElementHandle involved
        batch_limit = min(current, max_auctions)
        new_hrefs = page.evaluate(
            """([sel, start, end]) =>
                Array.from(document.querySelectorAll(sel))
                     .slice(start, end)
                     .map(el => el.getAttribute('href'))""",
            [tile_sel, loaded, batch_limit]
        )
        for href in new_hrefs:
            if href:
                urls.append(href if href.startswith("http") else BASE_URL + href)

        loaded = current
        if loaded >= max_auctions:
            break

        # Check button existence + visibility entirely in JS
        btn_state = page.evaluate(
            """sel => {
                const b = document.querySelector(sel);
                if (!b) return 'missing';
                const r = b.getBoundingClientRect();
                return (r.width > 0 && r.height > 0) ? 'visible' : 'hidden';
            }""",
            btn_sel
        )
        if btn_state == 'missing':
            print("Load more button not found - reached end of listings")
            break
        if btn_state == 'hidden':
            print("Load more button not visible - reached end of listings")
            break

        print("Clicking load more button...")
        page.evaluate("() => { window.__netLog = []; }")  # clear before click
        page.locator(btn_sel).scroll_into_view_if_needed()
        page.wait_for_timeout(500)
        page.locator(btn_sel).click()
        page.wait_for_timeout(2000)   # let async requests fire
        # On first two clicks, dump what JS saw on the wire
        if loaded <= 80:
            net_events = page.evaluate("() => window.__netLog.slice()")
            print(f"[NET] {len(net_events)} JS network calls after load-more:")
            for e in net_events:
                print(f"  {e}")

        # Poll for new tiles via JS count. Use a generous timeout — at 10k+
        # DOM nodes the browser renders slowly after each load-more.
        poll_deadline = 60_000
        poll_interval = 500
        elapsed = 0
        loaded_new = False
        while elapsed < poll_deadline:
            page.wait_for_timeout(poll_interval)
            elapsed += poll_interval
            new_count = page.evaluate("s => document.querySelectorAll(s).length", tile_sel)
            if new_count > loaded:
                print("Successfully loaded more listings")
                loaded_new = True
                break

        if not loaded_new:
            print(f"Load more did not respond within {poll_deadline}ms — stopping collection")
            break

    print(f"Collection complete: found {len(urls)} auction URLs")
    return urls

def parse_auction(browser, url):
    """Parse individual auction page - creates fresh page each time"""
    # Create a completely fresh page for this auction
    page = None
    try:
        page = browser.new_page()
        page.set_default_timeout(30000)  # 30 second timeout
        
        page.goto(url, timeout=45_000, wait_until="domcontentloaded")
        page.wait_for_selector(SELECTORS["sale_span"], timeout=20_000)
        
    except PlaywrightTimeoutError:
        print(f"    Timeout loading page")
        if page:
            page.close()
        return {"auction_url": url, "error": "timeout"}
    except Exception as e:
        error_str = str(e)
        if "'dict' object has no attribute" in error_str:
            print(f"    Dict error detected, skipping")
        else:
            print(f"    Failed to load: {error_str[:80]}")
        if page:
            page.close()
        return {"auction_url": url, "error": "load_failed"}
    
    record = {"auction_url": url}

    # Sale Type & optional sale_date
    try:
        if (sale_span := page.query_selector(SELECTORS["sale_span"])):
            text = sale_span.inner_text().strip()
            record["sale_type"] = "sold" if text.lower().startswith("sold for") else "high bid"
            if (date_el := sale_span.query_selector("span.date")):
                record["sale_date"] = date_el.inner_text().replace("on ", "").strip()
    except Exception as e:
        print(f"    Error parsing sale type: {str(e)[:50]}")

    # Simple stats (amount, comments, bids, views, watchers)
    for key in ("sale_amount", "comments", "bids", "views", "watchers"):
        try:
            if (el := page.query_selector(SELECTORS[key])):
                record[key] = el.inner_text().strip()
        except:
            pass

    # Auction end date & timestamp
    try:
        if (end_el := page.query_selector(SELECTORS["end_span"])):
            record["end_date"] = end_el.inner_text().strip()
            record["end_timestamp"] = end_el.get_attribute("data-ends")
    except:
        pass

    # Title
    title = ""
    try:
        if (title_el := page.query_selector(SELECTORS["title"])):
            title = title_el.inner_text().strip()
            record["title"] = title
            record["model"] = title  # Initial fallback
    except:
        pass

    # Year extraction
    year = None
    try:
        year = extract_year_from_url(url)
        if year:
            print(f"    Year from URL: {year}")
        
        if not year and title:
            year = extract_year_from_title(title)
            if year:
                print(f"    Year from title: {year}")
    except:
        pass
    
    record["year"] = year

    # Seller type
    try:
        if (seller_el := page.query_selector(SELECTORS["seller_type"])):
            record["seller_type"] = seller_el.inner_text().split(":", 1)[-1].strip()
    except:
        pass

    # Make, Model, Era, Origin, Category from group items
    group_item_model = None
    try:
        for gi in page.query_selector_all(SELECTORS["group_items"]):
            if lbl_el := gi.query_selector("strong.group-title-label"):
                lbl = lbl_el.inner_text().strip()
                content = gi.inner_text().replace(lbl, "").strip()
                if content:
                    if lbl.lower() == 'model':
                        group_item_model = content
                        # Don't set it yet - we'll validate it first
                    else:
                        record[lbl.lower()] = content
    except:
        pass

    # SMART MODEL SELECTION
    # Priority: URL slug (if group-item is generic) > group-item > title
    url_model = extract_model_from_url(url)
    
    if group_item_model and not is_generic_model(group_item_model):
        # Group item model is good, use it
        record['model'] = group_item_model
        print(f"    Model from group-item: {group_item_model}")
    elif url_model:
        # Use URL slug model (group-item was generic or missing)
        record['model'] = url_model
        if group_item_model:
            print(f"    Model from URL (generic group-item '{group_item_model}' replaced): {url_model}")
        else:
            print(f"    Model from URL: {url_model}")
    elif group_item_model:
        # Use generic model as last resort
        record['model'] = group_item_model
        print(f"    Model from group-item (generic): {group_item_model}")
    # else: Keep the title as model (already set earlier)

    # Close the page before returning
    page.close()
    return record

def run_scraper(start_date=None, end_date=None, max_auctions=MAX_AUCTIONS):
    """Main scraper function"""
    print(f"\nStarting BAT Scraper - {datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")

    is_backfill = bool(start_date or end_date)
    start_dt = datetime.datetime.strptime(start_date, "%Y-%m-%d").date() if start_date else None
    end_dt   = datetime.datetime.strptime(end_date,   "%Y-%m-%d").date() if end_date   else None
    if is_backfill:
        print(f"[BACKFILL MODE] Date range: {start_date or 'unset'} → {end_date or 'unset'}")
        print(f"[BACKFILL MODE] Note: BaT URL date params are not reliable; "
              f"filtering by sale_date extracted from each auction page")

    # Download existing data from S3
    existing_df, existing_urls = download_existing_bat_csv()
    
    new_data = []
    years_extracted = []
    models_from_url = 0
    
    print("\n[3/8] Initializing Playwright browser...")
    with sync_playwright() as pw:
        print("Playwright context created")
        
        print("Launching Chromium browser (headless mode)...")
        browser = pw.chromium.launch(headless=True)
        print("Browser launched successfully")
        
        print("Creating page for URL collection...")
        collection_page = browser.new_page()
        print("Page created successfully")

        try:
            print("\n[5/8] Collecting auction URLs...")
            urls = collect_auction_urls(collection_page, max_auctions=max_auctions)
            
            # Close the collection page
            collection_page.close()
            print("Closed URL collection page")
            
            print(f"\n[6/8] Filtering URLs...")
            # Log a sample of collected URLs so we can verify date range
            if urls:
                print(f"Sample URLs collected (first 3): {urls[:3]}")
            # Filter out URLs we've already scraped
            urls_to_scrape = [url for url in urls if url not in existing_urls]
            print(f"Total URLs collected: {len(urls)}")
            print(f"Already scraped: {len(urls) - len(urls_to_scrape)}")
            print(f"New URLs to scrape: {len(urls_to_scrape)}")

            print(f"\n[7/8] Scraping individual auction pages...")
            consecutive_too_old = 0
            for i, url in enumerate(urls_to_scrape, 1):
                try:
                    print(f"\n[{i}/{len(urls_to_scrape)}] Processing: {url}")
                    # Pass browser instead of page - function creates its own page
                    data = parse_auction(browser, url)

                    # Backfill date-range enforcement
                    if is_backfill and data.get('sale_date'):
                        try:
                            auction_dt = None
                            for fmt in ("%m/%d/%Y", "%m/%d/%y", "%B %d, %Y", "%b %d, %Y"):
                                try:
                                    auction_dt = datetime.datetime.strptime(
                                        data['sale_date'].strip(), fmt
                                    ).date()
                                    break
                                except ValueError:
                                    continue
                            if auction_dt:
                                if end_dt and auction_dt > end_dt:
                                    print(f"  [BACKFILL] Skip {auction_dt} — after end_date {end_dt}")
                                    continue
                                if start_dt and auction_dt < start_dt:
                                    consecutive_too_old += 1
                                    print(f"  [BACKFILL] Skip {auction_dt} — before start_date {start_dt} ({consecutive_too_old}/10)")
                                    if consecutive_too_old >= 10:
                                        print("  [BACKFILL] 10 consecutive out-of-range — stopping early")
                                        break
                                    continue
                                else:
                                    consecutive_too_old = 0
                        except Exception as date_err:
                            print(f"  [BACKFILL] Could not parse sale_date '{data.get('sale_date')}': {date_err}")

                    new_data.append(data)

                    # Track year extraction success
                    if data.get('year'):
                        years_extracted.append(data['year'])

                    # Track URL model extraction
                    if 'Model from URL' in str(data):
                        models_from_url += 1

                    year_display = f"({data.get('year', 'No Year')})"
                    sale_type = data.get('sale_type', 'N/A')
                    sale_amount = data.get('sale_amount', 'N/A')
                    model_display = data.get('model', 'N/A')[:40]
                    print(f"  Result: {year_display} {model_display} - {sale_type} - {sale_amount}")

                except Exception as e:
                    print(f"  Unexpected error: {str(e)[:80]}")
                    new_data.append({"auction_url": url, "error": str(e)[:100]})

        except Exception as e:
            print(f"Error during URL collection: {e}")
            print("Proceeding with any data collected...")
        
        finally:
            print("\nClosing browser...")
            browser.close()
            print("Browser closed")

    if not new_data:
        print("\nNo new data collected.")
        return

    print(f"\n[8/8] Processing and saving data...")
    # Create DataFrame from new data
    new_df = pd.DataFrame(new_data)
    print(f"Created DataFrame with {len(new_df)} new rows")
    
    # Combine with existing data
    if not existing_df.empty:
        combined_df = pd.concat([existing_df, new_df], ignore_index=True)
        # Remove duplicates based on auction_url
        combined_df = combined_df.drop_duplicates(subset=['auction_url'], keep='last')
        print(f"Combined data: {len(combined_df)} total rows")
    else:
        combined_df = new_df
        print(f"New dataset: {len(combined_df)} rows")
    
    # Save to CSV
    combined_df.to_csv("bat.csv", index=False)
    print(f"Saved to bat.csv")

    # Show summary
    print(f"\n" + "=" * 60)
    print("=== SUMMARY ===")
    print(f"Total auctions in file: {len(combined_df)}")
    print(f"New auctions added: {len(new_data)}")
    if years_extracted:
        print(f"Years successfully extracted: {len(years_extracted)}/{len(new_data)}")
        success_rate = len(years_extracted) / len(new_data) * 100
        print(f"Year extraction success rate: {success_rate:.1f}%")
    print(f"Models extracted from URL: {models_from_url}")
    print("=" * 60)

    # Upload to S3
    print("\nUploading bat.csv to S3...")
    if upload_to_s3("bat.csv", "my-mii-reports"):
        print("Upload successful!")
    else:
        print("Upload failed!")

    # Clean up
    if os.path.exists('existing_bat.csv'):
        os.remove('existing_bat.csv')
        print("Cleaned up temporary files")

    print("\n" + "=" * 60)
    print("BAT SCRAPER COMPLETED SUCCESSFULLY")
    print(f"Timestamp: {datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("=" * 60)

if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser(description="BaT Auction Scraper")
    parser.add_argument("--start-date", metavar="YYYY-MM-DD", default=None,
                        help="Filter auctions ending on or after this date (backfill mode)")
    parser.add_argument("--end-date", metavar="YYYY-MM-DD", default=None,
                        help="Filter auctions ending on or before this date (backfill mode)")
    parser.add_argument("--max-auctions", type=int, default=MAX_AUCTIONS,
                        help=f"Max auctions to collect (default: {MAX_AUCTIONS})")
    args = parser.parse_args()
    run_scraper(start_date=args.start_date, end_date=args.end_date, max_auctions=args.max_auctions)
