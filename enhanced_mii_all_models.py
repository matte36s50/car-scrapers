import pandas as pd
import numpy as np
import datetime
import re
import os
import boto3
from botocore.exceptions import NoCredentialsError
from manufacturer_cleanup import clean_manufacturer_column, get_manufacturer_stats

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
        # CNB format: "$13,0009" where last digit(s) are appended (not part of price)
        # Pattern: The last 1-2 digits after the final comma group are extra
        # Examples:
        #   $13,0009 → $13,000 (remove "9")
        #   $38,75012 → $38,750 (remove "12")
        #   $7,10010 → $7,100 (remove "10")
        
        # Extract all digits
        nums_only = re.sub(r'[^\d]', '', value_str)
        
        if nums_only and len(nums_only) > 0:
            # CNB prices have 1-2 extra digits appended to the end
            # If the number ends in a pattern like XX,XXXN or XX,XXXNN
            # we need to remove that last N or NN
            
            # Strategy: Remove last digit if result ends in 0 or 5 (round number)
            if len(nums_only) > 3:
                # Try removing last 1 digit
                price_try_1 = int(nums_only[:-1])
                
                # Try removing last 2 digits
                price_try_2 = int(nums_only[:-2]) if len(nums_only) > 4 else 0
                
                # Heuristic: Auction prices typically end in 0, 00, 25, 50, 75
                # Check which removal results in a rounder number
                last_digit_1 = price_try_1 % 10
                last_two_1 = price_try_1 % 100
                
                last_digit_2 = price_try_2 % 10 if price_try_2 > 0 else 99
                last_two_2 = price_try_2 % 100 if price_try_2 > 0 else 99
                
                # Score roundness (0 is most round)
                # Perfect: ends in 00, 25, 50, 75
                # Good: ends in 0
                # OK: ends in 5
                def roundness_score(price):
                    last_two = price % 100
                    last_one = price % 10
                    if last_two in [0, 25, 50, 75]:
                        return 0  # Perfect
                    elif last_one == 0:
                        return 1  # Good
                    elif last_one == 5:
                        return 2  # OK
                    else:
                        return 3  # Not round
                
                score_1 = roundness_score(price_try_1)
                score_2 = roundness_score(price_try_2)
                
                # Use the rounder one, prefer removing fewer digits if tied
                if score_1 <= score_2:
                    return float(price_try_1)
                else:
                    return float(price_try_2)
            
            return float(nums_only)
    
    return None

def get_instagram_estimates(all_models):
    """Generate Instagram estimates for models"""
    known_estimates = {
        # BMW Models
        "bmw": 650000, "m3": 280000, "e30": 18000, "e36": 15000, "e46": 42000,
        "2002": 12000, "z8": 4500, "m5": 14000, "m4": 35000, "z4": 22000,
        
        # Mercedes Models  
        "mercedes": 480000, "190e": 18000, "c63": 45000, "amg": 65000,
        "g-class": 55000, "sl": 18000,
        
        # Porsche Models
        "porsche": 450000, "911": 150000, "turbo": 45000, "gt3": 65000,
        "boxster": 28000, "cayman": 32000,
        
        # Japanese Performance
        "toyota": 180000, "supra": 55000, "mr2": 15000, "ae86": 18000,
        "nissan": 120000, "gtr": 38000, "skyline": 28000, "240z": 22000,
        "honda": 160000, "s2000": 35000, "nsx": 22000, "civic": 45000,
        "mazda": 85000, "rx-7": 28000, "miata": 42000,
        
        # American Muscle
        "ford": 180000, "mustang": 85000, "gt40": 12000, "bronco": 25000,
        "chevrolet": 150000, "corvette": 95000, "camaro": 65000,
        "dodge": 95000, "challenger": 45000, "viper": 18000,
        
        # Supercars
        "ferrari": 320000, "lamborghini": 280000, "mclaren": 85000,
        "aston martin": 65000, "bugatti": 55000,
    }
    
    estimates = {}
    for model in all_models:
        if pd.isna(model):
            continue
        
        model_clean = str(model).lower()
        instagram_count = 8000  # Default
        
        # Check for matches
        for key, count in known_estimates.items():
            if key in model_clean:
                instagram_count = max(instagram_count, int(count * 0.3))
                break
        
        # Brand-based estimation
        if any(brand in model_clean for brand in ['bmw', 'mercedes', 'porsche', 'ferrari']):
            instagram_count = max(instagram_count, 20000)
        elif any(brand in model_clean for brand in ['toyota', 'honda', 'nissan']):
            instagram_count = max(instagram_count, 12000)
        
        estimates[model] = instagram_count
    
    return estimates

def load_scraped_data():
    """Load data from single bat.csv and cnb.csv files in S3"""
    print("📋 Looking for scraped data in S3...")
    
    s3 = boto3.client('s3')
    all_data = []
    
    # Load BAT data from S3
    try:
        print(f"📊 Downloading bat.csv from S3...")
        s3.download_file('my-mii-reports', 'bat.csv', 'temp_bat.csv')
        df = pd.read_csv('temp_bat.csv')
        df['data_source'] = 'BAT'
        
        print(f"   📋 Raw BAT data: {len(df)} records")
        
        # Your actual BAT columns: auction_url, bids, category, comments, end_date, end_timestamp, 
        # era, location, make, model, origin, partner, sale_amount, sale_date, sale_type, 
        # seller_type, views, watchers, year
        
        # Extract price from sale_amount (e.g., "USD $38,250")
        if 'sale_amount' in df.columns:
            df['price'] = df['sale_amount'].apply(extract_price)
        
        # Extract views from views column (e.g., "11,735 views")
        if 'views' in df.columns:
            df['views_numeric'] = df['views'].apply(extract_numeric)
            df['views'] = df['views_numeric']
        
        # Keep bids and comments as is (already numeric)
        # bids, comments are already numeric
        
        # Determine if sold
        if 'sale_type' in df.columns:
            df['sold'] = (df['sale_type'] == 'sold').astype(int)
        
        # Year is already a column
        
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
        
        # Add date/quarter
        if 'sale_date' in df.columns:
            df['date'] = pd.to_datetime(df['sale_date'], errors='coerce')
        elif 'end_date' in df.columns:
            # BAT's end_date is text like "Monday, May 19 at 5:47pm" - not parseable
            # Use sale_date instead
            df['date'] = pd.to_datetime(df.get('sale_date', None), errors='coerce')
        
        all_data.append(df)
        print(f"  ✅ Loaded {len(df)} BAT records")
        
    except Exception as e:
        print(f"  ⚠️  No BAT data found in S3: {e}")
        import traceback
        traceback.print_exc()
    
    # Load CNB data from S3
    try:
        print(f"📊 Downloading cnb.csv from S3...")
        s3.download_file('my-mii-reports', 'cnb.csv', 'temp_cnb.csv')
        df = pd.read_csv('temp_cnb.csv')
        df['data_source'] = 'CNB'
        
        print(f"   📋 Raw CNB data: {len(df)} records")
        
        # Your actual CNB columns: model, make, vin, engine, drivetrain, transmission, body_style,
        # exterior_color, interior_color, title_status, location, mileage, sale_amount, sale_date,
        # sale_type, bids, views, comments, watchers, seller, auction_url, year, bids_original
        
        # Extract price from sale_amount (e.g., "$13,0009")
        if 'sale_amount' in df.columns:
            df['price'] = df['sale_amount'].apply(extract_price)
        
        # Views is already numeric in CNB
        # bids, comments are already numeric
        
        # Determine if sold
        if 'sale_type' in df.columns:
            df['sold'] = (df['sale_type'] == 'sold').astype(int)
        
        # Filter out low-quality CNB entries (views < 50)
        if 'views' in df.columns:
            initial_count = len(df)
            low_view_count = len(df[df['views'] < 50])
            df = df[df['views'] >= 50]
            print(f"  ⚠️  DATA QUALITY FILTERING:")
            print(f"     Found {low_view_count} CNB entries with views < 50")
            print(f"     ✅ Filtered out {initial_count - len(df)} low-quality entries")
        
        # Clean manufacturer names
        print("🧹 Cleaning CNB manufacturer names...")
        if 'make' in df.columns:
            df['manufacturer'] = df['make']
            unique_before = df['make'].nunique()
            df = clean_manufacturer_column(df, manufacturer_col='manufacturer', model_col='model')
            unique_after = df['manufacturer'].nunique()
            print(f"   Before: {unique_before} unique manufacturers")
            print(f"   After: {unique_after} unique manufacturers")
            print(f"   Reduction: {unique_before - unique_after} duplicates removed")
        
        # Add date/quarter
        if 'sale_date' in df.columns:
            df['date'] = pd.to_datetime(df['sale_date'], errors='coerce')
        
        all_data.append(df)
        print(f"  ✅ Loaded {len(df)} CNB records")
        
    except Exception as e:
        print(f"  ⚠️  No CNB data found in S3: {e}")
        import traceback
        traceback.print_exc()
    
    if not all_data:
        raise Exception("No data files found in S3!")
    
    # Combine all data
    combined_df = pd.concat(all_data, ignore_index=True)
    print(f"📈 Combined total: {len(combined_df)} auction records")
    
    return combined_df

def clean_and_process_data(df):
    """Clean and process the combined data"""
    print("\n🧹 Cleaning and processing data...")
    
    # Ensure numeric columns
    numeric_cols = ['views', 'bids', 'comments', 'price']
    for col in numeric_cols:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors='coerce').fillna(0)
    
    # Calculate age
    if 'year' in df.columns:
        current_year = datetime.datetime.now().year
        df['year'] = pd.to_numeric(df['year'], errors='coerce')
        df['age'] = current_year - df['year']
        
        # Add decade
        df['decade'] = (df['year'] // 10) * 10
    
    # Parse dates and create quarters with smart fallback
    current_quarter = pd.Period(datetime.datetime.now(), freq='Q')
    current_quarter_str = str(current_quarter)
    current_year = datetime.datetime.now().year
    
    if 'date' in df.columns:
        df['date'] = pd.to_datetime(df['date'], errors='coerce')
        
        # Filter out bad years (e.g., 2069)
        if df['date'].notna().any():
            bad_years = df['date'].dt.year > current_year + 10
            bad_year_count = bad_years.sum()
            if bad_year_count > 0:
                print(f"   ⚠️  Filtered out {bad_year_count} records with invalid future years")
                df = df[~bad_years]
        
        df['quarter'] = df['date'].dt.to_period('Q').astype(str)
        
        # Replace NaT quarters with current quarter (these are recent auctions without dates)
        nat_mask = (df['quarter'] == 'NaT') | df['quarter'].isna()
        nat_count = nat_mask.sum()
        if nat_count > 0:
            print(f"   ℹ️  Assigned {nat_count} auctions without dates to {current_quarter_str}")
            df.loc[nat_mask, 'quarter'] = current_quarter_str
    else:
        # Default all to current quarter
        df['quarter'] = current_quarter_str
    
    # Remove rows with missing critical data
    df = df.dropna(subset=['model', 'manufacturer'])
    
    # Remove rows with zero price (no sale data)
    if 'price' in df.columns:
        price_before = len(df)
        df = df[df['price'] > 0]
        print(f"   ℹ️  Removed {price_before - len(df)} records with no price data")
    
    print(f"✅ Cleaned data: {len(df)} records with {df['model'].nunique()} unique models")
    if 'views' in df.columns:
        print(f"   Average views: {df['views'].mean():.1f}")
    if 'bids' in df.columns:
        print(f"   Average bids: {df['bids'].mean():.1f}")
    if 'price' in df.columns:
        print(f"   Average price: ${df['price'].mean():,.0f}")
        print(f"   Median price: ${df['price'].median():,.0f}")
    
    # Show quarter distribution
    print(f"\n📅 Quarter Distribution:")
    quarter_dist = df['quarter'].value_counts().sort_index(ascending=False)
    for quarter, count in quarter_dist.head(8).items():
        pct = (count / len(df)) * 100
        print(f"   {quarter}: {count:>6,} auctions ({pct:>5.1f}%)")
    
    return df

def calculate_mii_scores(df):
    """Calculate MII (Market Interest Index) scores with price as key component"""
    print("\n🧮 Calculating MII scores...")
    
    # Group by manufacturer, model, and quarter
    grouped = df.groupby(['manufacturer', 'model', 'quarter']).agg({
        'views': 'sum',
        'bids': 'sum',
        'comments': 'sum',
        'price': 'mean',  # Average price for the model
        'sold': 'sum',
        'year': 'first',
        'age': 'mean',
        'decade': 'first',
        'data_source': lambda x: ','.join(x.unique())
    }).reset_index()
    
    # Get Instagram estimates
    instagram_estimates = get_instagram_estimates(grouped['model'].unique())
    grouped['instagram_followers'] = grouped['model'].map(instagram_estimates)
    
    # Normalize metrics (0-1 scale within each quarter)
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
    
    # Calculate MII score with weighted components
    # PRICE IS THE MOST IMPORTANT COMPONENT!
    weights = {
        'price_normalized': 0.35,           # 35% - Price (most important!)
        'bids_normalized': 0.25,            # 25% - Bidding activity
        'views_normalized': 0.20,           # 20% - Interest/attention
        'comments_normalized': 0.10,        # 10% - Community engagement
        'instagram_followers_normalized': 0.10  # 10% - Social media presence
    }
    
    grouped['mii_score'] = sum(
        grouped[metric] * weight 
        for metric, weight in weights.items() 
        if metric in grouped.columns
    ) * 100
    
    # Round MII score
    grouped['mii_score'] = grouped['mii_score'].round(2)
    
    # Add age-based bonus (classic cars get a small boost)
    if 'age' in grouped.columns:
        grouped.loc[grouped['age'].between(25, 50), 'mii_score'] *= 1.05
        grouped.loc[grouped['age'] > 50, 'mii_score'] *= 1.10
    
    # Sort by MII score
    grouped = grouped.sort_values('mii_score', ascending=False)
    
    print(f"✅ Calculated MII for {len(grouped)} model-quarter combinations")
    print(f"\n💰 MII Score Composition:")
    print(f"   Price:          35% (most important)")
    print(f"   Bids:           25%")
    print(f"   Views:          20%")
    print(f"   Comments:       10%")
    print(f"   Instagram:      10%")
    
    return grouped

def generate_insights(mii_results):
    """Generate insights and rankings"""
    print("\n📊 GENERATING INSIGHTS")
    print("=" * 80)
    
    # Get latest quarter
    latest_quarter = mii_results['quarter'].max()
    latest_data = mii_results[mii_results['quarter'] == latest_quarter].copy()
    
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
    print("   Price-Focused Analysis (35% weight)")
    print("=" * 80)
    print(f"⏰ Started at: {datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    
    try:
        # Load data
        df = load_scraped_data()
        
        # Show manufacturer statistics after cleanup
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
