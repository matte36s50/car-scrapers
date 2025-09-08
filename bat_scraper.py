import re
import os
import json
import csv
import pandas as pd
import datetime
from playwright.sync_api import sync_playwright

# === S3 UPLOAD CODE ===
import boto3
from botocore.exceptions import NoCredentialsError

def upload_to_s3(file_name, bucket, object_name=None):
    s3 = boto3.client('s3')
    if object_name is None:
        object_name = file_name
    try:
        s3.upload_file(file_name, bucket, object_name)
        print(f"File {file_name} uploaded to s3://{bucket}/{object_name}")
    except NoCredentialsError:
        print("AWS credentials not available. Run 'aws configure' to set them up.")

# === GOOGLE SHEETS UPLOAD CODE ===
def upload_to_google_sheets(df, spreadsheet_name='BAT Scraper Results',
                           worksheet_name='BAT_Data', use_oauth=True, append_mode=True, auto_date_sheets=True):
    """
    Upload DataFrame to Google Sheets using OAuth2 (personal account)
    If append_mode=True, appends new data to existing worksheet
    If append_mode=False, replaces all data in worksheet
    """
    try:
        import gspread
        from google.auth.transport.requests import Request
        from google.oauth2.credentials import Credentials
        from google_auth_oauthlib.flow import InstalledAppFlow
        
        SCOPES = ['https://www.googleapis.com/auth/spreadsheets',
                  'https://www.googleapis.com/auth/drive']
        
        creds = None
        base_path = os.path.dirname(os.path.abspath(__file__))
        token_path = os.path.join(base_path, 'token.json')
        
        # Token file stores the user's access and refresh tokens
        if os.path.exists(token_path):
            creds = Credentials.from_authorized_user_file(token_path, SCOPES)
        
        # If there are no (valid) credentials available, let the user log in
        if not creds or not creds.valid:
            if creds and creds.expired and creds.refresh_token:
                creds.refresh(Request())
            else:
                # Look for credentials.json (OAuth2 client ID file)
                cred_path = os.path.join(base_path, 'credentials.json')
                if not os.path.exists(cred_path):
                    print("\n" + "="*60)
                    print("GOOGLE SHEETS SETUP REQUIRED")
                    print("="*60)
                    print("\n1. Go to: https://console.cloud.google.com/")
                    print("2. Create a new project (or select existing)")
                    print("3. Enable Google Sheets API and Google Drive API")
                    print("4. Go to 'Credentials' → 'Create Credentials' → 'OAuth client ID'")
                    print("5. Choose 'Desktop app' as application type")
                    print("6. Download the JSON file")
                    print("7. Rename it to 'credentials.json' and place in your project folder")
                    print("\nThen run this script again!")
                    return None
                
                flow = InstalledAppFlow.from_client_secrets_file(cred_path, SCOPES)
                creds = flow.run_local_server(port=0)
                
            # Save the credentials for the next run
            with open(token_path, 'w') as token:
                token.write(creds.to_json())
        
        # Authorize gspread
        client = gspread.authorize(creds)
        
        # Try to open existing spreadsheet, or create new one
        try:
            spreadsheet = client.open(spreadsheet_name)
        except gspread.SpreadsheetNotFound:
            print(f"Creating new spreadsheet: {spreadsheet_name}")
            spreadsheet = client.create(spreadsheet_name)
        
        # Try to access worksheet, or create new one
        try:
            worksheet = spreadsheet.worksheet(worksheet_name)
            existing_rows = len(worksheet.get_all_values())
            print(f"Found existing worksheet with {existing_rows} rows")
            
            # If worksheet is getting too large (>5000 rows), create a new one with date
            if auto_date_sheets and existing_rows > 5000:
                date_suffix = datetime.datetime.now().strftime("_%Y%m%d")
                new_worksheet_name = f"{worksheet_name}{date_suffix}"
                print(f"Worksheet too large ({existing_rows} rows), creating new sheet: {new_worksheet_name}")
                
                try:
                    worksheet = spreadsheet.worksheet(new_worksheet_name)
                    existing_rows = len(worksheet.get_all_values())
                except gspread.WorksheetNotFound:
                    worksheet = spreadsheet.add_worksheet(title=new_worksheet_name, rows=len(df)+1000, cols=len(df.columns)+5)
                    existing_rows = 0
                    append_mode = False  # Start fresh on new sheet
                    
        except gspread.WorksheetNotFound:
            print(f"Creating new worksheet: {worksheet_name}")
            worksheet = spreadsheet.add_worksheet(title=worksheet_name, rows=len(df)+1000, cols=len(df.columns)+5)
            existing_rows = 0
        
        if append_mode and existing_rows > 0:
            # APPEND MODE: Add new data without disturbing existing data
            print("Appending new data to existing worksheet...")
            
            # Get existing headers to match column order
            existing_headers = worksheet.row_values(1) if existing_rows > 0 else []
            
            if existing_headers:
                # Reorder new data to match existing headers
                df_reordered = df.reindex(columns=existing_headers, fill_value='')
                
                # Find duplicates based on auction_url to avoid adding the same data twice
                if 'auction_url' in existing_headers:
                    existing_data = worksheet.get_all_records()
                    existing_urls = {row.get('auction_url', '') for row in existing_data}
                    
                    # Filter out rows that already exist
                    new_rows = []
                    duplicate_count = 0
                    for _, row in df_reordered.iterrows():
                        if row.get('auction_url', '') not in existing_urls:
                            new_rows.append(row.fillna('').tolist())
                        else:
                            duplicate_count += 1
                    
                    if duplicate_count > 0:
                        print(f"Skipped {duplicate_count} duplicate auctions")
                    
                    if new_rows:
                        # CHECK IF WE NEED TO EXPAND THE SHEET
                        required_rows = existing_rows + len(new_rows)
                        current_rows = worksheet.row_count
                        
                        if required_rows > current_rows:
                            # Expand the sheet to accommodate new data
                            new_row_count = required_rows + 1000  # Add buffer
                            print(f"Expanding sheet from {current_rows} to {new_row_count} rows...")
                            worksheet.resize(rows=new_row_count)
                        
                        # Append new rows
                        start_row = existing_rows + 1
                        cell_range = f"A{start_row}"
                        worksheet.update(values=new_rows, range_name=cell_range)
                        print(f"✓ Added {len(new_rows)} new rows starting at row {start_row}")
                    else:
                        print("No new data to append (all rows already exist)")
                else:
                    # No auction_url column for deduplication, just append all
                    new_data = df_reordered.fillna('').values.tolist()
                    
                    # CHECK IF WE NEED TO EXPAND THE SHEET
                    required_rows = existing_rows + len(new_data)
                    current_rows = worksheet.row_count
                    
                    if required_rows > current_rows:
                        # Expand the sheet to accommodate new data
                        new_row_count = required_rows + 1000  # Add buffer
                        print(f"Expanding sheet from {current_rows} to {new_row_count} rows...")
                        worksheet.resize(rows=new_row_count)
                    
                    start_row = existing_rows + 1
                    cell_range = f"A{start_row}"
                    worksheet.update(values=new_data, range_name=cell_range)
                    print(f"✓ Added {len(new_data)} rows starting at row {start_row}")
            else:
                # No existing headers, treat as new worksheet
                data = [df.columns.tolist()] + df.fillna('').values.tolist()
                
                # Ensure worksheet is large enough
                required_rows = len(data) + 100
                if required_rows > worksheet.row_count:
                    worksheet.resize(rows=required_rows)
                
                worksheet.update(values=data, range_name='A1')
                print(f"✓ Added headers and {len(df)} rows to new worksheet")
        else:
            # REPLACE MODE: Clear and upload all data
            print("Replacing all data in worksheet...")
            worksheet.clear()
            
            # Prepare data for upload
            data = [df.columns.tolist()] + df.fillna('').values.tolist()
            
            # Update the worksheet
            worksheet.update(values=data, range_name='A1')
            print(f"✓ Replaced worksheet with {len(df)} rows")
        
        # Format the header row (always row 1)
        worksheet.format('A1:Z1', {
            "backgroundColor": {"red": 0.8, "green": 0.2, "blue": 0.2},
            "horizontalAlignment": "CENTER",
            "textFormat": {"foregroundColor": {"red": 1, "green": 1, "blue": 1}, "bold": True}
        })
        
        # Get final row count
        final_rows = len(worksheet.get_all_values())
        
        print(f"✓ Successfully updated Google Sheets!")
        print(f"  Spreadsheet: {spreadsheet_name}")
        print(f"  Worksheet: {worksheet_name}")
        print(f"  Total rows: {final_rows}")
        print(f"  URL: {spreadsheet.url}")
        
        return spreadsheet.url
        
    except ImportError:
        print("\nError: Required libraries not installed.")
        print("Please run: pip install gspread google-auth google-auth-oauthlib google-auth-httplib2")
        return None
    except Exception as e:
        print(f"Error uploading to Google Sheets: {e}")
        return None

def create_bat_dashboard(df, spreadsheet_name='BAT Scraper Results'):
    """Create a dashboard worksheet with BAT summary statistics"""
    try:
        import gspread
        from google.auth.transport.requests import Request
        from google.oauth2.credentials import Credentials
        
        base_path = os.path.dirname(os.path.abspath(__file__))
        token_path = os.path.join(base_path, 'token.json')
        
        # Load existing credentials
        if os.path.exists(token_path):
            creds = Credentials.from_authorized_user_file(token_path)
            client = gspread.authorize(creds)
        else:
            print("Error: No authentication token found. Run upload_to_google_sheets first.")
            return
        
        spreadsheet = client.open(spreadsheet_name)
        
        # Create dashboard worksheet
        try:
            dashboard = spreadsheet.worksheet('BAT_Dashboard')
            dashboard.clear()
        except gspread.WorksheetNotFound:
            dashboard = spreadsheet.add_worksheet(title='BAT_Dashboard', rows=50, cols=10)
        
        # Calculate summary statistics
        total_auctions = len(df)
        sold_auctions = len(df[df['sale_type'] == 'sold']) if 'sale_type' in df.columns else 0
        high_bid_auctions = total_auctions - sold_auctions
        unique_models = df['title'].nunique() if 'title' in df.columns else 0
        
        # Year statistics
        year_stats = []
        if 'year' in df.columns and df['year'].notna().sum() > 0:
            years_available = df['year'].notna().sum()
            if years_available > 0:
                year_range = f"{int(df['year'].min())} - {int(df['year'].max())}"
                avg_year = f"{df['year'].mean():.0f}"
                year_stats = [
                    ['Years Available:', f"{years_available:,} ({years_available/total_auctions*100:.1f}%)"],
                    ['Year Range:', year_range],
                    ['Average Year:', avg_year],
                ]
        
        # Sale amount statistics
        sale_stats = []
        if 'sale_amount' in df.columns:
            # Extract numeric values from sale amounts
            df_temp = df.copy()
            df_temp['sale_numeric'] = df_temp['sale_amount'].str.extract(r'[\$]?([\d,]+)')[0].str.replace(',', '').astype(float, errors='ignore')
            valid_sales = df_temp['sale_numeric'].dropna()
            
            if len(valid_sales) > 0:
                avg_sale = f"${valid_sales.mean():,.0f}"
                median_sale = f"${valid_sales.median():,.0f}"
                max_sale = f"${valid_sales.max():,.0f}"
                min_sale = f"${valid_sales.min():,.0f}"
                
                sale_stats = [
                    ['Average Sale:', avg_sale],
                    ['Median Sale:', median_sale],
                    ['Highest Sale:', max_sale],
                    ['Lowest Sale:', min_sale],
                ]
        
        summary_data = [
            ['Bring a Trailer Scraper Dashboard', '', '', ''],
            ['Last Updated:', datetime.datetime.now().strftime("%Y-%m-%d %H:%M"), '', ''],
            ['', '', '', ''],
            ['Summary Statistics', '', '', ''],
            ['Total Auctions:', f"{total_auctions:,}", '', ''],
            ['Sold Auctions:', f"{sold_auctions:,}", '', ''],
            ['High Bid (No Sale):', f"{high_bid_auctions:,}", '', ''],
            ['Unique Titles:', f"{unique_models:,}", '', ''],
            ['', '', '', ''],
        ]
        
        # Add year statistics if available
        if year_stats:
            summary_data.extend([['Year Analysis', '', '', '']] + year_stats + [['', '', '', '']])
        
        # Add sale statistics if available
        if sale_stats:
            summary_data.extend([['Sale Statistics', '', '', '']] + sale_stats + [['', '', '', '']])
        
        # Add top models by views if available
        if 'views' in df.columns and 'title' in df.columns:
            summary_data.extend([
                ['Top 10 Models by Views', 'Views', 'Sale Type', 'Year'],
            ])
            
            # Clean views data
            df_temp = df.copy()
            df_temp['views_numeric'] = pd.to_numeric(df_temp['views'].astype(str).str.replace(',', '').str.replace(' views', ''), errors='coerce')
            top_views = df_temp.nlargest(10, 'views_numeric')
            
            for _, row in top_views.iterrows():
                title_display = str(row['title'])[:30] + '...' if len(str(row['title'])) > 30 else str(row['title'])
                views_display = f"{row['views_numeric']:,.0f}" if pd.notna(row['views_numeric']) else 'N/A'
                sale_type = str(row.get('sale_type', 'Unknown'))
                year_display = str(int(row['year'])) if pd.notna(row.get('year')) else 'N/A'
                
                summary_data.append([
                    title_display,
                    views_display,
                    sale_type,
                    year_display
                ])
        
        # Update dashboard
        dashboard.update(values=summary_data, range_name='A1')
        
        # Format dashboard
        dashboard.format('A1:D1', {
            "backgroundColor": {"red": 0.8, "green": 0.2, "blue": 0.2},
            "textFormat": {"foregroundColor": {"red": 1, "green": 1, "blue": 1}, 
                          "bold": True, "fontSize": 14},
            "horizontalAlignment": "CENTER"
        })
        
        # Format section headers
        dashboard.format('A4:A4', {"textFormat": {"bold": True, "fontSize": 12}})
        if year_stats:
            dashboard.format(f'A{9+len(year_stats)}:A{9+len(year_stats)}', {"textFormat": {"bold": True, "fontSize": 12}})
        
        print("✓ BAT Dashboard created successfully!")
        
    except Exception as e:
        print(f"Error creating BAT dashboard: {e}")

def extract_year_from_url(url):
    """
    FIXED: Extract year from BAT URL pattern
    Examples:
    - https://bringatrailer.com/listing/2007-mercedes-benz-sl65-amg-37/
    - https://bringatrailer.com/listing/1963-jaguar-xke-series-1-coupe-2/
    """
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
    """
    ENHANCED: Extract year from title with better patterns
    """
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

def extract_year_from_group_items(page, selectors):
    """
    Extract year from group items (Make, Model, Era, Origin sections)
    """
    try:
        for gi in page.query_selector_all(selectors["group_items"]):
            if lbl_el := gi.query_selector("strong.group-title-label"):
                lbl = lbl_el.inner_text().strip().lower()
                text = gi.inner_text().strip()
                
                # Look for year in model field
                if 'model' in lbl and text:
                    year = extract_year_from_title(text)
                    if year:
                        return year
                
                # Look for explicit year field
                if 'year' in lbl and text:
                    year = extract_year_from_title(text)
                    if year:
                        return year
                        
                # Look for era field that might contain year
                if 'era' in lbl and text:
                    # Extract year from era like "1960s" -> try to find specific year elsewhere
                    decade_match = re.search(r'(\d{4})s', text)
                    if decade_match:
                        # Era gives us decade, but not specific year
                        # We'll use this as fallback only
                        pass
    except Exception:
        pass
    
    return None

# ————————————————————————————
#  CONFIGURATION
# ————————————————————————————
BASE_URL      = "https://bringatrailer.com"
RESULTS_URL   = f"{BASE_URL}/auctions/results/"
MAX_AUCTIONS  = 500

SELECTORS = {
    # results‐page
    "tile":        "#auctions-completed-container > div.listings-container.auctions-grid > a",
    "load_more":   "button.auctions-footer-button",
    # auction‐page
    "sale_span":   "span.info-value.noborder-tiny",                  # parent span wrapping "Sold for …"
    "sale_amount": "span.info-value.noborder-tiny > strong",        # the <strong>USD $…</strong>
    "comments":    "a > span > span.info-value",
    "bids":        "td.listing-stats-value.number-bids-value",
    "views":       "#listing-actions-stats > div:nth-child(1) > span",
    "watchers":    "#listing-actions-stats > div:nth-child(2) > span",
    "end_span":    "#listing-bid > tbody > tr:nth-child(2) > td.listing-stats-value > span",
    "title":       "h1.listing-title",
    "seller_type": "div.item.additional",
    "group_items": "div.group-item-wrap > div.group-item",
}

def save_outputs(new_data, json_path="bat.json", csv_path="bat.csv"):
    # — JSON: load existing and combine with new —
    if os.path.exists(json_path):
        with open(json_path, "r") as jf:
            existing = json.load(jf)
    else:
        existing = []
    combined = existing + new_data

    # Deduplicate using auction_url as unique key, keep latest
    unique = {}
    for row in combined:
        key = row.get("auction_url")
        if key:
            unique[key] = row  # latest wins

    deduped = list(unique.values())

    # Write JSON
    with open(json_path, "w") as jf:
        json.dump(deduped, jf, indent=2)

    # — CSV: union of all keys for header —
    all_keys = sorted({k for row in deduped for k in row.keys()})
    with open(csv_path, "w", newline="") as cf:
        writer = csv.DictWriter(cf, fieldnames=all_keys)
        writer.writeheader()
        for row in deduped:
            writer.writerow(row)

def collect_auction_urls(page):
    """FIXED VERSION with better timeout handling"""
    page.goto(RESULTS_URL, timeout=60_000)
    page.wait_for_selector(SELECTORS["tile"])
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
            consecutive_failures = 0  # Reset failure counter

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
        
        # Check if button is visible and enabled
        if not btn.is_visible():
            print("Load more button not visible - reached end of listings")
            break
            
        print(f"Clicking load more button...")
        btn.scroll_into_view_if_needed()
        
        # Add small delay before clicking
        page.wait_for_timeout(1000)
        btn.click()
        
        # Wait for new content with shorter timeout and better error handling
        try:
            page.wait_for_function(
                "([sel, n]) => document.querySelectorAll(sel).length > n",
                arg=[SELECTORS["tile"], loaded],
                timeout=20_000  # Reduced from 60 seconds to 20 seconds
            )
            print(f"Successfully loaded more listings")
        except Exception as e:
            print(f"Timeout waiting for more listings: {e}")
            print("Continuing with current listings...")
            
            # Wait a bit and check if any new listings appeared anyway
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

def parse_auction(page, url):
    page.goto(url, timeout=60_000)
    page.wait_for_selector(SELECTORS["sale_span"])
    record = {"auction_url": url}

    # — Sale Type & optional sale_date —
    if (sale_span := page.query_selector(SELECTORS["sale_span"])):
        text = sale_span.inner_text().strip()
        record["sale_type"] = "sold" if text.lower().startswith("sold for") else "high bid"
        if (date_el := sale_span.query_selector("span.date")):
            record["sale_date"] = date_el.inner_text().replace("on ", "").strip()

    # — Simple stats (amount, comments, bids, views, watchers) —
    for key in ("sale_amount", "comments", "bids", "views", "watchers"):
        if (el := page.query_selector(SELECTORS[key])):
            record[key] = el.inner_text().strip()

    # — Auction end date & timestamp —
    if (end_el := page.query_selector(SELECTORS["end_span"])):
        record["end_date"]      = end_el.inner_text().strip()
        record["end_timestamp"] = end_el.get_attribute("data-ends")

    # — Title —
    title = ""
    if (title_el := page.query_selector(SELECTORS["title"])):
        title = title_el.inner_text().strip()
        record["title"] = title

    # — FIXED YEAR EXTRACTION WITH PROPER ERROR HANDLING —
    year = None
    
    try:
        # Method 1: Extract from URL (MOST RELIABLE)
        year = extract_year_from_url(url)
        if year:
            print(f"    ✓ Year from URL: {year}")
        else:
            print(f"    ✗ No year from URL: {url}")
        
        # Method 2: Extract from title (FALLBACK)
        if not year and title:
            year = extract_year_from_title(title)
            if year:
                print(f"    ✓ Year from title: {year}")
            else:
                print(f"    ✗ No year from title: {title}")
        
        # Method 3: Extract from group items (SECOND FALLBACK)
        if not year:
            year = extract_year_from_group_items(page, SELECTORS)
            if year:
                print(f"    ✓ Year from group items: {year}")
            else:
                print(f"    ✗ No year from group items")
                
    except Exception as e:
        print(f"    ✗ Error during year extraction: {e}")
        year = None
    
    # Set the year - this was potentially getting lost before
    record["year"] = year
    print(f"    → Final year set: {record['year']}")

    # — Seller type —
    if (seller_el := page.query_selector(SELECTORS["seller_type"])):
        record["seller_type"] = seller_el.inner_text().split(":",1)[-1].strip()

    # — Make, Model, Era, Origin —
    for gi in page.query_selector_all(SELECTORS["group_items"]):
        if lbl_el := gi.query_selector("strong.group-title-label"):
            lbl = lbl_el.inner_text().strip()
            content = gi.inner_text().replace(lbl, "").strip()
            if content:  # Only add if there's actual content
                record[lbl.lower()] = content

    return record

def run_scraper():
    new_data = []
    years_extracted = []
    
    with sync_playwright() as pw:
        browser = pw.chromium.launch(headless=True)
        page = browser.new_page()

        try:
            urls = collect_auction_urls(page)
            print(f"Scraping details for {len(urls)} auctions…")

            for i, url in enumerate(urls, 1):
                try:
                    print(f"\n[{i}/{len(urls)}] Processing: {url}")
                    data = parse_auction(page, url)
                    new_data.append(data)
                    
                    # Track year extraction success
                    if data.get('year'):
                        years_extracted.append(data['year'])
                    
                    year_display = f"({data.get('year', 'No Year')})" if data.get('year') else "(No Year)"
                    print(f"  → Result: {year_display} {data['sale_type']} – {data.get('sale_amount', 'N/A')}")
                    
                except Exception as e:
                    print(f"  ✗ Error on {url}: {e}")

        except Exception as e:
            print(f"Error during URL collection: {e}")
            print("Proceeding with any URLs that were collected...")
        
        finally:
            browser.close()

    if not new_data:
        print("No data collected. Exiting.")
        return

    save_outputs(new_data)
    print("Done! Appended to bat.json and bat.csv")

    # Show year extraction summary
    print(f"\n=== YEAR EXTRACTION SUMMARY ===")
    print(f"Total auctions scraped: {len(new_data)}")
    print(f"Years successfully extracted: {len(years_extracted)}")
    if len(new_data) > 0:
        success_rate = len(years_extracted) / len(new_data) * 100
        print(f"Year extraction success rate: {success_rate:.1f}%")
    
    if years_extracted:
        print(f"Year range: {min(years_extracted)} - {max(years_extracted)}")
        
        # Show year distribution
        from collections import Counter
        year_counts = Counter(years_extracted)
        print("\nTop 10 years by auction count:")
        for year, count in year_counts.most_common(10):
            print(f"  {year}: {count} auctions")

    # === S3 UPLOAD CODE ===
    print("\nUploading bat.csv to S3...")
    upload_to_s3("bat.csv", "my-mii-reports")

    # === GOOGLE SHEETS UPLOAD CODE ===
    print("\nUploading to Google Sheets...")
    try:
        # Load the CSV data as DataFrame for Google Sheets
        df = pd.read_csv("bat.csv")
        
        # Upload main data with append mode
        sheet_url = upload_to_google_sheets(
            df, 
            spreadsheet_name='BAT Scraper Results',
            worksheet_name='BAT_Data',  # Use consistent worksheet name
            use_oauth=True,
            append_mode=True  # This will append to existing data
        )
        
        if sheet_url:
            # Create dashboard
            create_bat_dashboard(df, spreadsheet_name='BAT Scraper Results')
            print(f"✓ Google Sheets upload complete: {sheet_url}")
        else:
            print("Google Sheets upload skipped. See setup instructions above.")
            
    except Exception as e:
        print(f"Error with Google Sheets upload: {e}")

if __name__ == "__main__":
    run_scraper()
