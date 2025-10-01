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
print("BAT SCRAPER STARTING")
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
        existing_df = pd.read_csv('existing_bat.csv')
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
        # Primary pattern: /listing/YYYY-make-model/
        match = re.search(r'/listing/(\d{4})-', url)
        if match:
            year = int(match.group(1))
            if 1900 <= year <= 2030:
                return year
        
        # Secondary pattern: look for YYYY in the URL path after listing/
        match = re.search(r'/listing/[^/]*?(\d{4})', url)
        if match:
            year = int(match.group(1))
            if 1900 <= year <= 2030:
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
        # Pattern 1: Year at start "2007 Mercedes-Benz"
        match = re.search(r'^(\d{4})\s+', title)
        if match:
            year = int(match.group(1))
            if 1900 <= year <= 2030:
                return year
        
        # Pattern 2: Year in parentheses "(2007)"
        match = re.search(r'\((\d{4})\)', title)
        if match:
            year = int(match.group(1))
            if 1900 <= year <= 2030:
                return year
        
        # Pattern 3: Any 4-digit year in title
        match = re.search(r'\b(\d{4})\b', title)
        if match:
            year = int(match.group(1))
            if 1900 <= year <= 2030:
                return year
                
        return None
    except Exception as e:
        print(f"Error extracting year from title {title}: {e}")
        return None

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

def collect_auction_urls(page):
    """Collect auction URLs from results page"""
    print(f"\n[4/8] Navigating to results page: {RESULTS_URL}")
    page.goto(RESULTS_URL, timeout=60_000)
    print("Page loaded successfully")
    
    print(f"Waiting for auction tiles selector: {SELECTORS['tile']}")
    page.wait_for_selector(SELECTORS["tile"])
    print("Auction tiles found")
    
    urls, loaded = [], 0
    consecutive_failures = 0
    max_failures = 3

    while loaded < MAX_AUCTIONS:
        cards = page.query_selector_all(SELECTORS["tile"])
        current = len(cards)
        print(f"Loaded {current}/{MAX_AUCTIONS} listings")

        # If no new cards loaded, we might be at the end
        if current == loaded:
            consecutive_failures += 1
            print(f"No new listings loaded (attempt {consecutive_failures}/{max_failures})")
            if consecutive_failures >= max_failures:
                print("Reached end of listings or load button not working")
                break
        else:
            consecutive_failures = 0

        for card in cards[loaded: min(current, MAX_AUCTIONS)]:
            href = card.get_attribute("href")
            if href:
                urls.append(href if href.startswith("http") else BASE_URL + href)

        loaded = current
        if loaded >= MAX_AUCTIONS:
            break

        # Look for load more button
        btn = page.query_selector(SELECTORS["load_more"])
        if not btn:
            print("Load more button not found - reached end of listings")
            break
        
        if not btn.is_visible():
            print("Load more button not visible - reached end of listings")
            break
            
        print(f"Clicking load more button...")
        btn.scroll_into_view_if_needed()
        page.wait_for_timeout(1000)
        btn.click()
        
        try:
            page.wait_for_function(
                "([sel, n]) => document.querySelectorAll(sel).length > n",
                arg=[SELECTORS["tile"], loaded],
                timeout=20_000
            )
            print(f"Successfully loaded more listings")
        except Exception as e:
            print(f"Timeout waiting for more listings: {e}")
            page.wait_for_timeout(3000)
            new_cards = page.query_selector_all(SELECTORS["tile"])
            if len(new_cards) > current:
                print(f"Found {len(new_cards) - current} additional listings after timeout")
                continue
            else:
                print("No additional listings found - stopping collection")
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
            record["model"] = title
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

    # Make, Model, Era, Origin, Category
    try:
        for gi in page.query_selector_all(SELECTORS["group_items"]):
            if lbl_el := gi.query_selector("strong.group-title-label"):
                lbl = lbl_el.inner_text().strip()
                content = gi.inner_text().replace(lbl, "").strip()
                if content:
                    if lbl.lower() == 'model':
                        record['model'] = content
                    else:
                        record[lbl.lower()] = content
    except:
        pass

    # Close the page before returning
    page.close()
    return record

def run_scraper():
    """Main scraper function"""
    print(f"\nStarting BAT Scraper - {datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    
    # Download existing data from S3
    existing_df, existing_urls = download_existing_bat_csv()
    
    new_data = []
    years_extracted = []
    
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
            urls = collect_auction_urls(collection_page)
            
            # Close the collection page
            collection_page.close()
            print("Closed URL collection page")
            
            print(f"\n[6/8] Filtering URLs...")
            # Filter out URLs we've already scraped
            urls_to_scrape = [url for url in urls if url not in existing_urls]
            print(f"Total URLs collected: {len(urls)}")
            print(f"Already scraped: {len(urls) - len(urls_to_scrape)}")
            print(f"New URLs to scrape: {len(urls_to_scrape)}")

            print(f"\n[7/8] Scraping individual auction pages...")
            for i, url in enumerate(urls_to_scrape, 1):
                try:
                    print(f"\n[{i}/{len(urls_to_scrape)}] Processing: {url}")
                    # Pass browser instead of page - function creates its own page
                    data = parse_auction(browser, url)
                    new_data.append(data)
                    
                    # Track year extraction success
                    if data.get('year'):
                        years_extracted.append(data['year'])
                    
                    year_display = f"({data.get('year', 'No Year')})"
                    sale_type = data.get('sale_type', 'N/A')
                    sale_amount = data.get('sale_amount', 'N/A')
                    print(f"  Result: {year_display} {sale_type} - {sale_amount}")
                    
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
    run_scraper()
