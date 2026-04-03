import pandas as pd
import numpy as np
import datetime
import re
import os
import boto3
from botocore.exceptions import NoCredentialsError
from manufacturer_cleanup import clean_manufacturer_column, get_manufacturer_stats
from social_metrics import collect_social_metrics_for_mii, SocialMetricsCollector

# ============================================================================
# CONFIGURATION
# ============================================================================
USE_CNB_DATA = False  # Set to True when CNB scraper is fixed

# Social Metrics Configuration
USE_SOCIAL_METRICS = True  # Set to True to collect real social metrics
SOCIAL_METRICS_SAMPLE = None  # Set to a number to limit models (for testing), None for all
YOUTUBE_API_KEY = os.environ.get('YOUTUBE_API_KEY')  # Optional: Set for real YouTube data
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
        # Pattern 1: alphanumeric before AMG (e.g., "C63 AMG")
        amg_match = re.search(r'([A-Z0-9]+)\s+AMG', original_model, re.IGNORECASE)
        if amg_match:
            return f"{amg_match.group(1)} AMG"
        
        # Pattern 2: "AMG" followed by model (e.g., "AMG GT")
        amg_model_match = re.search(r'AMG\s+([A-Z][A-Z0-9\s]+)', original_model, re.IGNORECASE)
        if amg_model_match:
            return f"AMG {amg_model_match.group(1)}"
        
        # Pattern 3: Multiple words before AMG
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
        
        # CRITICAL: Parse date properly with multiple formats
        print("📅 Parsing dates...")
        date_parsed = False
        
        # Try sale_date first (most reliable)
        if 'sale_date' in df.columns:
            print("   Attempting to parse 'sale_date' column...")
            df['date'] = pd.to_datetime(df['sale_date'], errors='coerce')
            valid_dates = df['date'].notna().sum()
            print(f"   Parsed {valid_dates} valid dates from sale_date")
            if valid_dates > 0:
                date_parsed = True
        
        # If sale_date didn't work, try end_timestamp
        if not date_parsed and 'end_timestamp' in df.columns:
            print("   Attempting to parse 'end_timestamp' column...")
            df['date'] = pd.to_datetime(df['end_timestamp'], unit='s', errors='coerce')
            valid_dates = df['date'].notna().sum()
            print(f"   Parsed {valid_dates} valid dates from end_timestamp")
            if valid_dates > 0:
                date_parsed = True
        
        # Show date range
        if date_parsed:
            valid_dates_df = df[df['date'].notna()]
            if len(valid_dates_df) > 0:
                print(f"   📆 Date range: {valid_dates_df['date'].min()} to {valid_dates_df['date'].max()}")
            else:
                print("   ⚠️  WARNING: No valid dates found!")
        else:
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
    """Validate that quarter is reasonable - FIXED to handle both formats"""
    if pd.isna(quarter_str):
        return False
    
    try:
        quarter_str = str(quarter_str)
        # Handle both "2025Q3" and "2025-Q3" formats
        if 'Q' in quarter_str:
            year = int(quarter_str.split('Q')[0])
        else:
            year = int(quarter_str.split('-')[0])
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
    df['quarter'] = df['date'].dt.to_period('M').astype(str)
    
    # Show quarter distribution
    print(f"\n📊 Quarter distribution:")
    quarter_counts = df['quarter'].value_counts().sort_index()
    print(f"   Total unique quarters: {len(quarter_counts)}")
    print(f"   Latest quarter: {quarter_counts.index[-1] if len(quarter_counts) > 0 else 'None'}")
    print(f"   Earliest quarter: {quarter_counts.index[0] if len(quarter_counts) > 0 else 'None'}")
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
    df['age'] = current_year - df['year']
    df['decade'] = (df['year'] // 10) * 10
    
    # Required columns
    required = ['manufacturer', 'model', 'year', 'quarter', 'price', 'views', 'bids']
    df = df.dropna(subset=required)
    
    print(f"\n✅ Clean dataset ready:")
    print(f"   Records: {len(df):,}")
    print(f"   Manufacturers: {df['manufacturer'].nunique()}")
    print(f"   Models: {df['model'].nunique()}")
    print(f"   Quarters: {df['quarter'].nunique()}")
    print(f"   Date range: {df['date'].min()} to {df['date'].max()}")
    
    return df

def calculate_mii_scores(df):
    """Calculate MII scores for each model/quarter combination"""
    print("\n📊 CALCULATING MII SCORES")
    print("=" * 80)

    # Group by manufacturer, model, and quarter
 grouped = df.groupby(['manufacturer', 'model', 'quarter']).agg(
    price=('price', 'mean'),
    views=('views', 'mean'),
    bids=('bids', 'mean'),
    comments=('comments', 'mean'),
    year=('year', 'mean'),
    age=('age', 'mean'),
    decade=('decade', 'first'),
    sold=('sold', 'sum'),
    data_source=('data_source', 'first'),
    auction_count=('price', 'count'),
).reset_index()


    print(f"✅ Created {len(grouped):,} model-quarter combinations")

    # Collect social metrics (Google Trends + YouTube)
    if USE_SOCIAL_METRICS:
        print(f"\n🌐 Collecting social metrics for {grouped['model'].nunique()} unique models...")
        try:
            social_df = collect_social_metrics_for_mii(
                grouped[['manufacturer', 'model']].drop_duplicates(),
                youtube_api_key=YOUTUBE_API_KEY,
                sample_size=SOCIAL_METRICS_SAMPLE
            )

            # Merge social metrics with grouped data
            grouped = grouped.merge(
                social_df[['manufacturer', 'model', 'google_trends_interest',
                          'google_trends_pct', 'google_trends_direction',
                          'google_trends_source', 'youtube_total_views',
                          'youtube_source', 'social_score']],
                on=['manufacturer', 'model'],
                how='left'
            )

            # Fill NaN with fallback estimates
            grouped['google_trends_interest'] = grouped['google_trends_interest'].fillna(30)
            grouped['google_trends_pct'] = grouped['google_trends_pct'].fillna(0)
            grouped['google_trends_source'] = grouped['google_trends_source'].fillna('estimate')
            grouped['youtube_total_views'] = grouped['youtube_total_views'].fillna(10000)
            grouped['youtube_source'] = grouped['youtube_source'].fillna('estimate')
            grouped['social_score'] = grouped['social_score'].fillna(25)

            print(f"✅ Social metrics collected and merged")
            print(f"   Google Trends range: {grouped['google_trends_interest'].min():.1f} - {grouped['google_trends_interest'].max():.1f}")
            print(f"   YouTube views range: {grouped['youtube_total_views'].min():,.0f} - {grouped['youtube_total_views'].max():,.0f}")
            print(f"   Social score range: {grouped['social_score'].min():.1f} - {grouped['social_score'].max():.1f}")

            use_social = True
        except Exception as e:
            print(f"⚠️  Social metrics collection failed: {e}")
            print("   Falling back to Instagram estimates")
            use_social = False
    else:
        print("⏭️  Social metrics disabled, using Instagram estimates")
        use_social = False

    # Fallback to Instagram estimates if social metrics failed or disabled
    if not use_social:
        print(f"🔍 Estimating Instagram followers for {grouped['model'].nunique()} unique models...")
        instagram_estimates = get_instagram_estimates(grouped['model'].unique())
        grouped['instagram_followers'] = grouped['model'].map(instagram_estimates)
        grouped['google_trends_interest'] = 0
        grouped['youtube_total_views'] = 0
        grouped['social_score'] = 0

    # Normalize within each quarter
    for quarter in grouped['quarter'].unique():
        quarter_mask = grouped['quarter'] == quarter

        # Metrics to normalize
        if use_social:
            metrics_to_normalize = ['views', 'bids', 'comments', 'price',
                                   'google_trends_interest', 'youtube_total_views', 'social_score']
        else:
            metrics_to_normalize = ['views', 'bids', 'comments', 'price', 'instagram_followers']

        for metric in metrics_to_normalize:
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

    # Calculate MII score with updated weights
    if use_social:
        # New weights with Google Trends and YouTube
        weights = {
            'price_normalized': 0.20,           # 20% - Sale price
            'bids_normalized': 0.20,            # 20% - Auction engagement
            'views_normalized': 0.15,           # 15% - Page views
            'comments_normalized': 0.10,        # 10% - Comments
            'google_trends_interest_normalized': 0.15,  # 15% - Google Trends
            'youtube_total_views_normalized': 0.10,     # 10% - YouTube views
            'social_score_normalized': 0.05,    # 5% - Combined social score
            'age_normalized': 0.05              # 5% - Classic car bonus
        }
        weight_desc = """
   Price:              20%
   Bids:               20%
   Views:              15%
   Comments:           10%
   Google Trends:      15%
   YouTube Views:      10%
   Social Score:        5%
   Age (classic):       5%"""
    else:
        # Legacy weights without social metrics
        weights = {
            'price_normalized': 0.25,
            'bids_normalized': 0.25,
            'views_normalized': 0.20,
            'comments_normalized': 0.15,
            'instagram_followers_normalized': 0.10,
            'age_normalized': 0.05
        }
        weight_desc = """
   Price:          25%
   Bids:           25%
   Views:          20%
   Comments:       15%
   Instagram:      10%
   Age (classic):   5%"""

    grouped['mii_score'] = sum(
        grouped[metric] * weight
        for metric, weight in weights.items()
        if metric in grouped.columns
    ) * 100

    grouped['mii_score'] = grouped['mii_score'].round(2)
    grouped = grouped.sort_values('mii_score', ascending=False)

    print(f"✅ Calculated MII for {len(grouped)} model-quarter combinations")
    print(f"\n💯 MII Score Composition:{weight_desc}")

    return grouped

def generate_insights(mii_results):
    """Generate insights and rankings"""
    print("\n📊 GENERATING INSIGHTS")
    print("=" * 80)
    
    # Get latest quarter
    latest_quarter = mii_results['quarter'].max()
    print(f"\n📅 Latest quarter identified: {latest_quarter}")
    
    if pd.isna(latest_quarter):
        print("❌ ERROR: No valid quarter found in MII results!")
        print("\nAvailable quarters in dataset:")
        print(mii_results['quarter'].value_counts())
        raise Exception("Cannot generate insights without valid quarter data")
    
    latest_data = mii_results[mii_results['quarter'] == latest_quarter].copy()
    print(f"   Records in latest quarter: {len(latest_data):,}")
    
    print(f"\n🏆 TOP 10 MODELS ({latest_quarter})")
    print("-" * 80)
    print(f"{'Rank':<6}{'Model':<30}{'MII':<9}{'Price':<12}{'Bids':<8}{'Views':<10}{'Year':<6}")
    print("-" * 80)
    
    for i, row in latest_data.head(10).iterrows():
        rank = latest_data.index.get_loc(i) + 1
        model_name = row['model'][:28]
        year = int(row['year']) if pd.notna(row['year']) else 'N/A'
        price_str = f"${row['price']:,.0f}" if pd.notna(row['price']) else 'N/A'
        print(f"{rank:<6}{model_name:<30}{row['mii_score']:<9.1f}{price_str:<12}"
              f"{int(row['bids']):<8}{int(row['views']):<10,}{year:<6}")
    
    # Top manufacturers
    print(f"\n\n🏭 TOP MANUFACTURERS BY AVERAGE MII ({latest_quarter})")
    print("-" * 80)
    manufacturer_stats = latest_data.groupby('manufacturer').agg({
        'mii_score': 'mean',
        'model': 'count',
        'price': 'mean',
        'views': 'sum'
    }).round(2).sort_values('mii_score', ascending=False)
    
    print(f"{'Rank':<6}{'Manufacturer':<20}{'Avg MII':<11}{'Models':<9}{'Avg Price':<13}{'Views'}")
    print("-" * 80)
    
    for idx, (manufacturer, row) in enumerate(manufacturer_stats.head(10).iterrows(), 1):
        price_str = f"${row['price']:,.0f}"
        print(f"{idx:<6}{manufacturer:<20}{row['mii_score']:<11.1f}"
              f"{int(row['model']):<9}{price_str:<13}{int(row['views']):,}")
    
    # Price tiers
    print(f"\n\n💎 TOP MODELS BY PRICE TIER ({latest_quarter})")
    print("-" * 80)
    
    price_tiers = [
        (0, 50000, "Under $50K"),
        (50000, 100000, "$50K-$100K"),
        (100000, 200000, "$100K-$200K"),
        (200000, float('inf'), "$200K+")
    ]
    
    for min_price, max_price, label in price_tiers:
        tier_data = latest_data[
            (latest_data['price'] >= min_price) & (latest_data['price'] < max_price)
        ].head(3)
        
        if not tier_data.empty:
            print(f"\n{label}:")
            for _, row in tier_data.iterrows():
                model_name = row['model'][:35]
                price_str = f"${row['price']:,.0f}"
                print(f"  • {model_name:<35} MII: {row['mii_score']:<6.1f} Price: {price_str}")
    
    # Decade analysis
    if 'decade' in latest_data.columns:
        print(f"\n\n📅 TOP MODELS BY DECADE ({latest_quarter})")
        print("-" * 80)
        
        for decade in sorted(latest_data['decade'].dropna().unique(), reverse=True)[:5]:
            decade_data = latest_data[latest_data['decade'] == decade].head(3)
            if not decade_data.empty:
                print(f"\n{int(decade)}s:")
                for _, row in decade_data.iterrows():
                    model_name = row['model'][:30]
                    price_str = f"${row['price']:,.0f}"
                    print(f"  • {model_name:<30} MII: {row['mii_score']:<6.1f} Price: {price_str}")
    
    # Data source breakdown
    print(f"\n\n📈 DATA SOURCE BREAKDOWN")
    print("-" * 80)
    source_counts = mii_results['data_source'].value_counts()
    for source, count in source_counts.items():
        pct = (count / len(mii_results)) * 100
        print(f"{source:<15} {count:>6,} records ({pct:>5.1f}%)")
    
    return latest_data

def save_and_upload_results(mii_results, latest_data):
    """Save results locally and upload to S3"""
    print("\n\n💾 SAVING AND UPLOADING RESULTS")
    print("=" * 80)
    
    timestamp = datetime.datetime.now().strftime("%Y%m%d_%H%M")
    
    # Save complete results
    output_file = 'mii_results_latest.csv'
    mii_results.to_csv(output_file, index=False)
    print(f"✅ Saved: {output_file}")
    
    # Save top models
    top_models_file = f'mii_top_models_{timestamp}.csv'
    latest_data.head(50).to_csv(top_models_file, index=False)
    print(f"✅ Saved: {top_models_file}")
    
    # Upload to S3
    upload_to_s3(output_file, 'my-mii-reports')
    upload_to_s3(top_models_file, 'my-mii-reports')
    
    print("\n✅ All results saved and uploaded!")

def main():
    """Main execution function"""
    print("=" * 80)
    print("🚀 MII Calculator with Manufacturer Name Cleanup")
    print("   Engagement-Focused with Classic Car Bonus (Age Weight)")
    if not USE_CNB_DATA:
        print("   ⚠️  CNB DATA TEMPORARILY DISABLED - BAT DATA ONLY")
    print("=" * 80)
    print(f"⏰ Started at: {datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    
    try:
        # Load data
        df = load_scraped_data()
        
        # Show manufacturer statistics
        print("\n📊 MANUFACTURER STATISTICS AFTER CLEANUP")
        print("=" * 80)
        stats = get_manufacturer_stats(df, 'manufacturer')
        print("\n🏆 Top 15 Manufacturers:")
        for _, row in stats.head(15).iterrows():
            print(f"   {row['Manufacturer']:<20} {int(row['Count']):>6,} ({row['Percentage']:>5.1f}%)")
        
        # Clean and process
        df = clean_and_process_data(df)
        
        # Calculate MII
        mii_results = calculate_mii_scores(df)
        
        # Generate insights
        latest_data = generate_insights(mii_results)
        
        # Save results
        save_and_upload_results(mii_results, latest_data)
        
        print(f"\n⏰ Completed at: {datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        print("=" * 80)
        
    except Exception as e:
        print(f"\n❌ Error: {e}")
        import traceback
        traceback.print_exc()
        return 1
    
    return 0

if __name__ == "__main__":
    exit(main())
