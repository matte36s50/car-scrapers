import pandas as pd
import numpy as np
import datetime
import re
import os
import boto3
from botocore.exceptions import NoCredentialsError
from manufacturer_cleanup import clean_manufacturer_column, get_manufacturer_stats

# ============================================================================
# CONFIGURATION: Set to False to temporarily disable CNB data
# ============================================================================
USE_CNB_DATA = False  # Set to True when CNB scraper is fixed
# ============================================================================

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
        print("❌ AWS credentials not available")
        return False
    except Exception as e:
        print(f"❌ Upload failed: {e}")
        return False

def extract_numeric(value):
    """Extract numeric value from string like 'USD $38,250' or '11,735 views'"""
    if pd.isna(value):
        return 0
    
    # Convert to string
    value_str = str(value)
    
    # Remove currency symbols, commas, and text
    # Extract all digits
    digits = re.sub(r'[^\d]', '', value_str)
    
    if digits:
        return float(digits)
    return 0

def extract_price(value):
    """Extract price from sale_amount string like 'USD $38,250' or '$13,0009'"""
    if pd.isna(value):
        return None
    
    # Convert to string
    value_str = str(value)
    
    # Check if this is USD format (BAT) or $ format (CNB)
    is_usd_format = 'USD' in value_str
    
    if is_usd_format:
        # BAT format: "USD $38,250" - straightforward extraction
        match = re.search(r'\$?([\d,]+)', value_str)
        if match:
            amount_str = match.group(1).replace(',', '')
            try:
                return float(amount_str)
            except:
                return None
    else:
        # CNB format: "$38,75012" where digits after position 3 following the comma are junk
        if ',' in value_str:
            parts = value_str.replace('$', '').split(',')
            if len(parts) >= 2:
                before_comma = parts[0]
                after_comma = parts[1]
                actual_price_digits = after_comma[:3]
                price_str = before_comma + actual_price_digits
                try:
                    return float(price_str)
                except:
                    return None
        else:
            nums = re.sub(r'[^\d]', '', value_str)
            if nums:
                return float(nums)
    
    return None

def extract_proper_model(model_text, make_text=None, original_model=None):
    """
    Extract proper model name, handling special cases like Mercedes AMG
    """
    if not model_text or pd.isna(model_text):
        return None
    
    model_str = str(model_text).strip()
    original_model = original_model or model_str
    
    # Remove year if present at the start (4 digits)
    model_str = re.sub(r'^\d{4}\s+', '', model_str)
    
    # Remove year ranges like "(1990-2018)"
    model_str = re.sub(r'\s*\(\d{4}-\d{4}\)', '', model_str)
    
    # Remove common make names
    common_makes = [
        'Mercedes-Benz', 'Mercedes', 'BMW', 'Porsche', 'Audi', 'Ferrari',
        'Lamborghini', 'McLaren', 'Chevrolet', 'Chevy', 'Ford', 'Dodge', 'Tesla',
        'Toyota', 'Honda', 'Nissan', 'Lexus', 'Acura', 'Infiniti', 'Jaguar',
        'Land Rover', 'Range Rover', 'Alfa Romeo', 'Maserati', 'Bentley',
        'Rolls-Royce', 'Aston Martin', 'Lotus', 'Bugatti'
    ]
    
    common_makes.sort(key=len, reverse=True)
    
    for make in common_makes:
        pattern = rf'^{re.escape(make)}[\s-]+'
        model_str = re.sub(pattern, '', model_str, flags=re.IGNORECASE)
    
    model_str = re.sub(r'\s+', ' ', model_str).strip()
    
    # CRITICAL CHECK: If result is just "AMG", extract more context
    if model_str.upper() == 'AMG' or (len(model_str) < 3 and 'AMG' in str(original_model).upper()):
        amg_match = re.search(r'([A-Z0-9]+)\s+AMG', original_model, re.IGNORECASE)
        if amg_match:
            return f"{amg_match.group(1)} AMG"
        
        amg_model_match = re.search(r'AMG\s+([A-Z][A-Z0-9\s]+)', original_model, re.IGNORECASE)
        if amg_model_match:
            return f"AMG {amg_model_match.group(1)}"
        
        multi_word_match = re.search(r'([A-Z][A-Z0-9]*(?:\s+[A-Z][A-Z0-9]*)*)\s+AMG', original_model, re.IGNORECASE)
        if multi_word_match:
            return f"{multi_word_match.group(1)} AMG"
        
        print(f"  ⚠️  WARNING: Could not extract specific model from '{original_model}' - will be filtered out")
        return None
    
    return model_str if model_str else None

def get_instagram_estimates(all_models):
    """Generate Instagram estimates for models"""
    known_estimates = {
        "bmw": 650000, "m3": 280000, "e30": 18000, "e36": 15000, "e46": 42000,
        "2002": 12000, "z8": 4500, "m5": 14000, "m4": 35000, "z4": 22000,
        "mercedes": 480000, "190e": 18000, "c63": 45000, "amg": 65000,
        "g-class": 55000, "sl": 18000,
        "porsche": 450000, "911": 150000, "turbo": 45000, "gt3": 65000,
        "boxster": 28000, "cayman": 32000,
        "toyota": 180000, "supra": 55000, "mr2": 15000, "ae86": 18000,
        "nissan": 120000, "gtr": 38000, "skyline": 28000, "240z": 22000,
        "honda": 160000, "s2000": 35000, "nsx": 22000, "civic": 45000,
        "mazda": 85000, "rx-7": 28000, "miata": 42000,
        "ford": 180000, "mustang": 85000, "gt40": 12000, "bronco": 25000,
        "chevrolet": 150000, "corvette": 95000, "camaro": 65000,
        "dodge": 95000, "challenger": 45000, "viper": 18000,
        "ferrari": 320000, "lamborghini": 280000, "mclaren": 85000,
        "aston martin": 65000, "bugatti": 55000,
    }
    
    estimates = {}
    for model in all_models:
        if pd.isna(model):
            continue
        
        model_clean = str(model).lower()
        instagram_count = 8000
        
        for key, count in known_estimates.items():
            if key in model_clean:
                instagram_count = max(instagram_count, int(count * 0.3))
                break
        
        if any(brand in model_clean for brand in ['bmw', 'mercedes', 'porsche', 'ferrari']):
            instagram_count = max(instagram_count, 20000)
        elif any(brand in model_clean for brand in ['toyota', 'honda', 'nissan']):
            instagram_count = max(instagram_count, 12000)
        
        estimates[model] = instagram_count
    
    return estimates

def load_scraped_data():
    """Load data from single bat.csv and cnb.csv files in S3"""
    print("📋 Looking for scraped data in S3...")
    
    if not USE_CNB_DATA:
        print("\n" + "="*80)
        print("⚠️  CNB DATA TEMPORARILY DISABLED")
        print("   Only using BAT data for MII calculations")
        print("   Set USE_CNB_DATA = True in the script to re-enable CNB data")
        print("="*80 + "\n")
    
    s3 = boto3.client('s3')
    all_data = []
    
    # Load BAT data from S3
    try:
        print(f"📊 Downloading bat.csv from S3...")
        s3.download_file('my-mii-reports', 'bat.csv', 'temp_bat.csv')
        df = pd.read_csv('temp_bat.csv')
        df['data_source'] = 'BAT'
        
        print(f"   📋 Raw BAT data: {len(df)} records")
        
        # Extract price from sale_amount
        if 'sale_amount' in df.columns:
            df['price'] = df['sale_amount'].apply(extract_price)
        
        # Extract views
        if 'views' in df.columns:
            df['views_numeric'] = df['views'].apply(extract_numeric)
            df['views'] = df['views_numeric']
        
        # Determine if sold
        if 'sale_type' in df.columns:
            df['sold'] = (df['sale_type'] == 'sold').astype(int)
        
        # Clean manufacturer names
        print("🧹 Cleaning BAT manufacturer names...")
        if 'make' in df.columns:
            df['manufacturer'] = df['make']
            unique_before = df['make'].nunique()
            df = clean_manufacturer_column(df, manufacturer_col='manufacturer', model_col='model')
            unique_after = df['manufacturer'].nunique()
            print(f"   Before: {unique_before} unique manufacturers")
            print(f"   After: {unique_after} unique manufacturers")
            print(f"   Reduction: {unique_before - unique_after} duplicates removed")
        
        # CRITICAL FIX: Parse dates with explicit format to handle 2-digit years correctly
        print("📅 Parsing dates...")
        date_parsed = False
        
        if 'sale_date' in df.columns:
            print("   Attempting to parse 'sale_date' column...")
            # Parse dates with dayfirst=False to handle MM/DD/YY format
            # This ensures '5/19/25' is May 19, not invalid date
            df['date'] = pd.to_datetime(df['sale_date'], format='mixed', errors='coerce')
            
            # Fix 2-digit year issue: pandas may interpret as 2069, should be 2025
            # Any date > today must have century adjusted
            current_year = datetime.datetime.now().year
            future_dates = df['date'] > datetime.datetime.now()
            
            if future_dates.sum() > 0:
                print(f"   ⚠️  Found {future_dates.sum()} dates parsed as future - adjusting century...")
                # Subtract 100 years from future dates
                df.loc[future_dates, 'date'] = df.loc[future_dates, 'date'] - pd.DateOffset(years=100)
            
            valid_dates = df['date'].notna().sum()
            print(f"   Parsed {valid_dates} valid dates from sale_date ({valid_dates/len(df)*100:.1f}%)")
            if valid_dates > 0:
                print(f"   📆 Date range: {df['date'].min()} to {df['date'].max()}")
                date_parsed = True
        
        # Fallback to end_timestamp if sale_date didn't work
        if not date_parsed and 'end_timestamp' in df.columns:
            print("   Attempting to parse 'end_timestamp' column...")
            df['date'] = pd.to_datetime(df['end_timestamp'], unit='s', errors='coerce')
            valid_dates = df['date'].notna().sum()
            print(f"   Parsed {valid_dates} valid dates from end_timestamp ({valid_dates/len(df)*100:.1f}%)")
            if valid_dates > 0:
                print(f"   📆 Date range: {df['date'].min()} to {df['date'].max()}")
                date_parsed = True
        
        if not date_parsed:
            print("   ⚠️  WARNING: Could not parse any dates!")
        
        all_data.append(df)
        print(f"  ✅ Loaded {len(df)} BAT records")
        
    except Exception as e:
        print(f"  ⚠️  No BAT data found in S3: {e}")
        import traceback
        traceback.print_exc()
    
    # Load CNB data if enabled
    if USE_CNB_DATA:
        try:
            print(f"📊 Downloading cnb.csv from S3...")
            s3.download_file('my-mii-reports', 'cnb_sitemap_full_cleaned.csv', 'temp_cnb.csv')
            df = pd.read_csv('temp_cnb.csv')
            df['data_source'] = 'CNB'
            
            print(f"   📋 Raw CNB data: {len(df)} records")
            
            if 'sale_amount' in df.columns:
                df['price'] = df['sale_amount'].apply(extract_price)
            
            if 'views' in df.columns:
                df['views_numeric'] = df['views'].apply(extract_numeric)
                df['views'] = df['views_numeric']
            
            if 'bids' in df.columns:
                df['bids_numeric'] = df['bids'].apply(extract_numeric)
                df['bids'] = df['bids_numeric']
            
            df['comments'] = 0
            
            if 'sale_type' in df.columns:
                df['sold'] = (df['sale_type'] == 'sold').astype(int)
            
            print("🧹 Cleaning CNB manufacturer names...")
            if 'make' in df.columns:
                df['manufacturer'] = df['make']
                unique_before = df['make'].nunique()
                df = clean_manufacturer_column(df, manufacturer_col='manufacturer', model_col='model')
                unique_after = df['manufacturer'].nunique()
                print(f"   Before: {unique_before} unique manufacturers")
                print(f"   After: {unique_after} unique manufacturers")
                print(f"   Reduction: {unique_before - unique_after} duplicates removed")
            
            if 'sale_date' in df.columns:
                df['date'] = pd.to_datetime(df['sale_date'], errors='coerce')
            
            all_data.append(df)
            print(f"  ✅ Loaded {len(df)} CNB records")
            
        except Exception as e:
            print(f"  ⚠️  No CNB data found in S3: {e}")
            import traceback
            traceback.print_exc()
    else:
        print(f"  ⏭️  Skipping CNB data (disabled in configuration)")
    
    if not all_data:
        raise Exception("No data loaded from either BAT or CNB!")
    
    # Combine all data
    combined_df = pd.concat(all_data, ignore_index=True)
    print(f"\n✅ Total records loaded: {len(combined_df):,}")
    
    # Show data source breakdown
    print(f"\n📊 Data Source Breakdown:")
    for source in combined_df['data_source'].unique():
        count = len(combined_df[combined_df['data_source'] == source])
        pct = (count / len(combined_df)) * 100
        print(f"   {source:<10} {count:>8,} records ({pct:>5.1f}%)")
    
    return combined_df

def validate_quarter(quarter_str):
    """Validate that quarter is reasonable"""
    if pd.isna(quarter_str):
        return False
    
    try:
        year = int(str(quarter_str).split('-')[0])
        current_year = datetime.datetime.now().year
        
        if 1990 <= year <= current_year:
            return True
        return False
    except:
        return False

def clean_and_process_data(df):
    """Clean and prepare data for MII calculation"""
    print("\n🧹 CLEANING AND PROCESSING DATA")
    print("=" * 80)
    
    initial_count = len(df)
    print(f"Starting records: {initial_count:,}")
    
    # Extract proper model names with AMG fix
    print(f"\n🔧 Extracting proper model names with AMG fix...")
    df['model_original'] = df['model']
    df['model_clean'] = df.apply(
        lambda row: extract_proper_model(
            row['model'], 
            row.get('manufacturer', row.get('make', None)),
            row['model']
        ), 
        axis=1
    )
    
    # Show examples
    print(f"\n📝 Model name transformation examples:")
    examples = df[df['model'] != df['model_clean']].head(10)
    for _, row in examples.iterrows():
        make = row.get('manufacturer', row.get('make', 'Unknown'))
        print(f"  {row['model']:<40} → {row['model_clean']:<40} [{make}]")
    
    # Filter out AMG-only entries
    amg_only_count = df['model_clean'].isna().sum()
    if amg_only_count > 0:
        print(f"\n⚠️  Filtered out {amg_only_count} entries with just 'AMG' as model")
        df = df[df['model_clean'].notna()].copy()
    else:
        print(f"\n✅ No 'AMG-only' Mercedes entries found!")
    
    df['model'] = df['model_clean']
    
    # Filter: Only sold auctions with valid prices
    df = df[
        (df['sold'] == 1) & 
        (df['price'].notna()) & 
        (df['price'] > 100) &
        (df['price'] < 10_000_000)
    ].copy()
    
    print(f"\n🔍 After filtering sold auctions with valid prices:")
    print(f"   Records: {len(df):,} (removed {initial_count - len(df):,})")
    
    # CRITICAL: Add quarter with better error handling
    print(f"\n📅 Creating quarters from dates...")
    valid_dates = df['date'].notna().sum()
    print(f"   Valid dates available: {valid_dates:,} ({valid_dates/len(df)*100:.1f}%)")
    
    if valid_dates == 0:
        raise Exception("❌ CRITICAL ERROR: No valid dates found! Cannot create quarters.")
    
    # Create quarter only for rows with valid dates
    df['quarter'] = df['date'].dt.to_period('Q').astype(str)
    
    # Show quarter distribution
    print(f"\n📊 Quarter distribution:")
    quarter_counts = df['quarter'].value_counts().sort_index()
    print(f"   Total unique quarters: {len(quarter_counts)}")
    if len(quarter_counts) > 0:
        print(f"   Latest quarter: {quarter_counts.index[-1]}")
        print(f"   Earliest quarter: {quarter_counts.index[0]}")
        print(f"\n   Top 5 quarters by record count:")
        for quarter, count in quarter_counts.tail(5).items():
            print(f"      {quarter}: {count:,} records")
    
    # Validate quarters
    df['quarter_valid'] = df['quarter'].apply(validate_quarter)
    invalid_quarters = df[~df['quarter_valid']]
    if len(invalid_quarters) > 0:
        print(f"\n⚠️  Filtering out {len(invalid_quarters)} records with invalid quarters")
        df = df[df['quarter_valid']].copy()
    
    # Add year and age
    current_year = datetime.datetime.now().year
    
    # CRITICAL FIX: Fill missing years with median year from the dataset
    missing_years = df['year'].isna().sum()
    if missing_years > 0:
        median_year = df['year'].median()
        print(f"\n⚠️  Filling {missing_years} missing years with median: {median_year:.0f}")
        df['year'] = df['year'].fillna(median_year)
    
    df['age'] = current_year - df['year']
    df['decade'] = (df['year'] // 10) * 10
    
    # CRITICAL FIX: Only require essential columns, fill optional ones
    # Don't require 'year' since we just filled it
    # Don't require 'bids' since BAT may have some null
    required = ['manufacturer', 'model', 'quarter', 'price']
    
    print(f"\n🔍 Checking required columns: {required}")
    df = df.dropna(subset=required)
    
    # Fill optional columns with 0
    df['views'] = df['views'].fillna(0)
    df['bids'] = df['bids'].fillna(0)
    df['comments'] = df['comments'].fillna(0)
    
    print(f"\n✅ Clean dataset ready:")
    print(f"   Records: {len(df):,}")
    print(f"   Manufacturers: {df['manufacturer'].nunique()}")
    print(f"   Models: {df['model'].nunique()}")
    print(f"   Quarters: {df['quarter'].nunique()}")
    if len(df) > 0:
        print(f"   Date range: {df['date'].min()} to {df['date'].max()}")
    
    return df

def calculate_mii_scores(df):
    """Calculate MII scores for each model/quarter combination"""
    print("\n📊 CALCULATING MII SCORES")
    print("=" * 80)
    
    # Group by manufacturer, model, and quarter
    grouped = df.groupby(['manufacturer', 'model', 'quarter']).agg({
        'price': 'mean',
        'views': 'mean',
        'bids': 'mean',
        'comments': 'mean',
        'year': 'mean',
        'age': 'mean',
        'decade': 'first',
        'sold': 'sum',
        'data_source': 'first'
    }).reset_index()
    
    print(f"✅ Created {len(grouped):,} model-quarter combinations")
    
    # Get Instagram estimates
    print(f"🔍 Estimating Instagram followers for {grouped['model'].nunique()} unique models...")
    instagram_estimates = get_instagram_estimates(grouped['model'].unique())
    grouped['instagram_followers'] = grouped['model'].map(instagram_estimates)
    
    # Normalize within each quarter
    for quarter in grouped['quarter'].unique():
        quarter_mask = grouped['quarter'] == quarter
        
        for metric in ['views', 'bids', 'comments', 'price', 'instagram_followers']:
            if metric in grouped.columns:
                max_val = grouped.loc[quarter_mask, metric].max()
                if max_val > 0:
                    grouped.loc[quarter_mask, f'{metric}_normalized'] = \
                        grouped.loc[quarter_mask, metric] / max_val
                else:
                    grouped.loc[quarter_mask, f'{metric}_normalized'] = 0
        
        # Normalize age
        if 'age' in grouped.columns:
            max_age = grouped.loc[quarter_mask, 'age'].max()
            if max_age > 0:
                grouped.loc[quarter_mask, 'age_normalized'] = \
                    grouped.loc[quarter_mask, 'age'] / max_age
            else:
                grouped.loc[quarter_mask, 'age_normalized'] = 0
    
    # Calculate MII score
    weights = {
        'price_normalized': 0.25,
        'bids_normalized': 0.25,
        'views_normalized': 0.20,
        'comments_normalized': 0.15,
        'instagram_followers_normalized': 0.10,
        'age_normalized': 0.05
    }
    
    grouped['mii_score'] = sum(
        grouped[metric] * weight 
        for metric, weight in weights.items() 
        if metric in grouped.columns
    ) * 100
    
    grouped['mii_score'] = grouped['mii_score'].round(2)
    grouped = grouped.sort_values('mii_score', ascending=False)
    
    print(f"✅ Calculated MII for {len(grouped)} model-quarter combinations")
    print(f"\n💯 MII Score Composition:")
    print(f"   Price:          25%")
    print(f"   Bids:           25%")
    print(f"   Views:          20%")
    print(f"   Comments:       15%")
    print(f"   Instagram:      10%")
    print(f"   Age (classic):   5%")
    
    return grouped

def generate_insights(mii_results):
    """Generate insights and rankings"""
    print("\n📊
