import csv
import re
import time
import os
import boto3
import pandas as pd
import datetime
import requests
from bs4 import BeautifulSoup
import traceback

# === S3 CONFIGURATION ===
S3_BUCKET = "my-mii-reports"
BAT_CSV_FILENAME = "bat.csv"
TEMP_LOCAL_FILE = "temp_bat.csv"

def download_existing_bat_csv():
    """Download existing bat.csv from S3"""
    s3 = boto3.client('s3')
    
    try:
        s3.download_file(S3_BUCKET, BAT_CSV_FILENAME, TEMP_LOCAL_FILE)
        print(f"✅ Downloaded existing bat.csv from S3")
        
        df = pd.read_csv(TEMP_LOCAL_FILE)
        print(f"📊 Existing data: {len(df)} rows")
        
        existing_urls = set(df['auction_url'].dropna().values)
        return df, existing_urls
        
    except s3.exceptions.NoSuchKey:
        print(f"⚠️ No existing bat.csv found in S3, will create new one")
        columns = [
            'auction_url', 'bids', 'category', 'comments', 'end_date', 
            'end_timestamp', 'era', 'location', 'make', 'model', 
            'origin', 'partner', 'sale_amount', 'sale_date', 'sale_type', 
            'seller_type', 'views', 'watchers', 'year'
        ]
        return pd.DataFrame(columns=columns), set()
    except Exception as e:
        print(f"❌ Error downloading bat.csv: {e}")
        return pd.DataFrame(), set()

def upload_updated_bat_csv(df):
    """Upload updated bat.csv back to S3"""
    s3 = boto3.client('s3')
    
    try:
        df.to_csv(TEMP_LOCAL_FILE, index=False)
        
        # Create backup
        try:
            s3.copy_object(
                Bucket=S3_BUCKET,
                CopySource={'Bucket': S3_BUCKET, 'Key': BAT_CSV_FILENAME},
                Key=f"backups/{BAT_CSV_FILENAME}_{datetime.datetime.now().strftime('%Y%m%d_%H%M')}"
            )
            print(f"📦 Created backup")
        except:
            pass
        
        s3.upload_file(TEMP_LOCAL_FILE, S3_BUCKET, BAT_CSV_FILENAME)
        print(f"✅ Uploaded updated bat.csv ({len(df)} rows)")
        os.remove(TEMP_LOCAL_FILE)
        return True
        
    except Exception as e:
        print(f"❌ Upload failed: {e}")
        return False

def get_sitemap_urls():
    """Get BAT sitemap URLs using requests only"""
    print("🌐 Fetching BAT sitemap...")
    
    try:
        headers = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'}
        response = requests.get("https://bringatrailer.com/sitemap_auctions.xml", 
                              headers=headers, timeout=30)
        
        if response.status_code == 200:
            soup = BeautifulSoup(response.text, 'xml')
            urls = [loc.text for loc in soup.find_all('loc') if '/listing/' in loc.text]
            
            if not urls:
                # Try HTML parser as fallback
                soup = BeautifulSoup(response.text, 'html.parser')
                urls = [loc.text for loc in soup.find_all('loc') if '/listing/' in loc.text]
            
            print(f"🔍 Found {len(urls)} auction URLs")
            return urls
    except Exception as e:
        print(f"❌ Sitemap error: {e}")
    
    return []

def extract_auction_data_simple(url):
    """Extract data using requests and BeautifulSoup only"""
    
    data = {
        'auction_url': url,
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
        headers = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'}
        response = requests.get(url, headers=headers, timeout=15)
        
        if response.status_code != 200:
            return None
        
        soup = BeautifulSoup(response.text, 'html.parser')
        html = response.text
        
        # Extract title/model
        h1 = soup.find('h1', class_='listing-title') or soup.find('h1')
        if h1:
            data['model'] = h1.get_text(strip=True)
            
            # Extract year from title
            year_match = re.search(r'\b(19|20)\d{2}\b', data['model'])
            if year_match:
                data['year'] = int(year_match.group(0))
            
            # Extract make (first word usually)
            words = data['model'].split()
            for word in words:
                if len(word) > 2 and not word.isdigit():
                    data['make'] = word
                    break
        
        # Extract sale amount using regex
        amount_patterns = [
            r'class="bid-value"[^>]*>\s*([^<]+)',
            r'Sold for[^$]*(\$[\d,]+)',
            r'Current Bid[^$]*(\$[\d,]+)',
            r'(\$[\d,]+).*(?:Sold|sold)'
        ]
        
        for pattern in amount_patterns:
            match = re.search(pattern, html, re.IGNORECASE)
            if match:
                data['sale_amount'] = match.group(1).strip()
                break
        
        # Extract views - multiple patterns
        views_patterns = [
            r'([\d,]+)\s*views?',
            r'Views[:\s]*([\d,]+)',
            r'class="views"[^>]*>([\d,]+)'
        ]
        
        for pattern in views_patterns:
            match = re.search(pattern, html, re.IGNORECASE)
            if match:
                data['views'] = match.group(1).replace(',', '')
                break
        
        # Extract bids
        bids_patterns = [
            r'([\d,]+)\s*bids?',
            r'Bids[:\s]*([\d,]+)',
            r'class="bid-count"[^>]*>([\d,]+)'
        ]
        
        for pattern in bids_patterns:
            match = re.search(pattern, html, re.IGNORECASE)
            if match:
                data['bids'] = int(match.group(1).replace(',', ''))
                break
        
        # Extract comments
        comments_patterns = [
            r'([\d,]+)\s*comments?',
            r'Comments[:\s]*([\d,]+)'
        ]
        
        for pattern in comments_patterns:
            match = re.search(pattern, html, re.IGNORECASE)
            if match:
                data['comments'] = int(match.group(1).replace(',', ''))
                break
        
        # Determine if sold
        if 'sold for' in html.lower() or 'winning bid' in html.lower():
            data['sale_type'] = 'sold'
        elif 'reserve not met' in html.lower():
            data['sale_type'] = 'reserve not met'
        elif 'no reserve' in html.lower():
            data['sale_type'] = 'no reserve'
        
        # Set origin based on make
        if data['make']:
            make_lower = data['make'].lower()
            origins = {
                'Germany': ['bmw', 'mercedes', 'porsche', 'audi', 'volkswagen'],
                'Italy': ['ferrari', 'lamborghini', 'alfa', 'fiat', 'maserati'],
                'Japan': ['toyota', 'honda', 'nissan', 'mazda', 'subaru'],
                'USA': ['ford', 'chevrolet', 'dodge', 'chrysler', 'cadillac'],
                'UK': ['jaguar', 'aston', 'bentley', 'rolls', 'lotus']
            }
            for country, brands in origins.items():
                if any(brand in make_lower for brand in brands):
                    data['origin'] = country
                    break
        
        return data
        
    except Exception as e:
        print(f"    ❌ Error scraping {url}: {str(e)[:100]}")
        return None

def main():
    print(f"🚀 Starting BAT Scraper (No Browser) - {datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    
    # Download existing data
    existing_df, existing_urls = download_existing_bat_csv()
    
    # Get all URLs
    all_urls = get_sitemap_urls()
    if not all_urls:
        print("❌ No URLs found!")
        return False
    
    # Filter new URLs
    new_urls = [url for url in all_urls if url not in existing_urls]
    print(f"✨ Found {len(new_urls)} new auctions")
    
    if not new_urls:
        print("✅ No new auctions - already up to date!")
        return True
    
    # Limit to 100 for GitHub Actions (can be increased later)
    new_urls = new_urls[:100]
    print(f"🎯 Processing {len(new_urls)} auctions")
    
    # Scrape new auctions
    new_rows = []
    successful = 0
    failed = 0
    
    for i, url in enumerate(new_urls):
        if i % 10 == 0:
            print(f"\n📊 Progress: {i}/{len(new_urls)}")
        
        data = extract_auction_data_simple(url)
        
        if data and (data['model'] or data['views'] or data['sale_amount']):
            new_rows.append(data)
            successful += 1
            print(f"  ✅ {data['model'][:30] if data['model'] else url[-20:]}")
        else:
            failed += 1
            print(f"  ⚠️ Skipped {url[-30:]}")
        
        # Be polite to the server
        time.sleep(1)
        
        # Save progress every 25 auctions
        if len(new_rows) > 0 and len(new_rows) % 25 == 0:
            print(f"\n💾 Saving progress...")
            temp_df = pd.concat([existing_df, pd.DataFrame(new_rows)], ignore_index=True)
            upload_updated_bat_csv(temp_df)
    
    print(f"\n📊 Scraping complete: {successful} successful, {failed} failed")
    
    # Final save
    if new_rows:
        new_df = pd.DataFrame(new_rows)
        updated_df = pd.concat([existing_df, new_df], ignore_index=True)
        
        # Remove duplicates
        updated_df = updated_df.drop_duplicates(subset=['auction_url'], keep='first')
        
        print(f"📊 Final stats:")
        print(f"   Total rows: {len(updated_df)}")
        print(f"   New additions: {len(new_rows)}")
        
        if upload_updated_bat_csv(updated_df):
            print(f"🎉 Successfully updated bat.csv!")
            return True
    
    return False

if __name__ == "__main__":
    try:
        success = main()
        exit(0 if success else 1)
    except Exception as e:
        print(f"❌ Fatal error: {e}")
        traceback.print_exc()
        exit(1)
