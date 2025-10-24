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
        
        # Standardize column names
        column_mapping = {
            'listing_title': 'model',
            'final_bid': 'price',
            'bid_count': 'bids',
            'comment_count': 'comments',
            'view_count': 'views',
            'sold_status': 'sold',
            'listing_date': 'date',
            'make': 'make'
        }
        df = df.rename(columns=column_mapping)
        
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
        
        all_data.append(df)
        print(f"  ✅ Loaded {len(df)} BAT records")
        
    except Exception as e:
        print(f"  ⚠️  No BAT data found in S3: {e}")
    
    # Load CNB data from S3
    try:
        print(f"📊 Downloading cnb.csv from S3...")
        s3.download_file('my-mii-reports', 'cnb.csv', 'temp_cnb.csv')
        df = pd.read_csv('temp_cnb.csv')
        df['data_source'] = 'CNB'
        
        # Standardize column names
        column_mapping = {
            'title': 'model',
            'highBid': 'price',
            'numBids': 'bids',
            'numComments': 'comments',
            'numViews': 'views',
            'status': 'sold',
            'startTime': 'date',
            'make': 'make'
        }
        df = df.rename(columns=column_mapping)
        
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
        
        all_data.append(df)
        print(f"  ✅ Loaded {len(df)} CNB records")
        
    except Exception as e:
        print(f"  ⚠️  No CNB data found in S3: {e}")
    
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
    numeric_cols = ['price', 'bids', 'comments', 'views']
    for col in numeric_cols:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors='coerce').fillna(0)
    
    # Extract year from model name
    def extract_year(model_str):
        if pd.isna(model_str):
            return None
        
        # Look for 4-digit year (1900-2099)
        match = re.search(r'\b(19\d{2}|20\d{2})\b', str(model_str))
        if match:
            year = int(match.group(1))
            if 1900 <= year <= 2025:
                return year
        return None
    
    df['year'] = df['model'].apply(extract_year)
    
    # Calculate age
    current_year = datetime.datetime.now().year
    df['age'] = current_year - df['year']
    
    # Add decade
    df['decade'] = (df['year'] // 10) * 10
    
    # Handle sold status
    if 'sold' in df.columns:
        df['sold'] = df['sold'].apply(lambda x: 1 if x in ['sold', 'Sold', True, 1] else 0)
    
    # Parse dates
    if 'date' in df.columns:
        df['date'] = pd.to_datetime(df['date'], errors='coerce')
        df['quarter'] = df['date'].dt.to_period('Q').astype(str)
    else:
        # Default to current quarter
        current_quarter = pd.Period(datetime.datetime.now(), freq='Q')
        df['quarter'] = str(current_quarter)
    
    # Remove rows with missing critical data
    df = df.dropna(subset=['model', 'manufacturer'])
    
    print(f"✅ Cleaned data: {len(df)} records with {df['model'].nunique()} unique models")
    print(f"   Average views: {df['views'].mean():.1f}")
    print(f"   Average bids: {df['bids'].mean():.1f}")
    
    return df

def calculate_mii_scores(df):
    """Calculate MII (Market Interest Index) scores"""
    print("\n🧮 Calculating MII scores...")
    
    # Group by manufacturer, model, and quarter
    grouped = df.groupby(['manufacturer', 'model', 'quarter']).agg({
        'views': 'sum',
        'bids': 'sum',
        'comments': 'sum',
        'price': 'mean',
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
        
        for metric in ['views', 'bids', 'comments', 'instagram_followers']:
            if metric in grouped.columns:
                max_val = grouped.loc[quarter_mask, metric].max()
                if max_val > 0:
                    grouped.loc[quarter_mask, f'{metric}_normalized'] = \
                        grouped.loc[quarter_mask, metric] / max_val
                else:
                    grouped.loc[quarter_mask, f'{metric}_normalized'] = 0
    
    # Calculate MII score with weighted components
    weights = {
        'views_normalized': 0.30,
        'bids_normalized': 0.25,
        'comments_normalized': 0.20,
        'instagram_followers_normalized': 0.25
    }
    
    grouped['mii_score'] = sum(
        grouped[metric] * weight 
        for metric, weight in weights.items() 
        if metric in grouped.columns
    ) * 100
    
    # Round MII score
    grouped['mii_score'] = grouped['mii_score'].round(2)
    
    # Add age-based bonus (classic cars get a small boost)
    grouped.loc[grouped['age'].between(25, 50), 'mii_score'] *= 1.05
    grouped.loc[grouped['age'] > 50, 'mii_score'] *= 1.10
    
    # Sort by MII score
    grouped = grouped.sort_values('mii_score', ascending=False)
    
    print(f"✅ Calculated MII for {len(grouped)} model-quarter combinations")
    
    return grouped

def generate_insights(mii_results):
    """Generate insights and rankings"""
    print("\n📊 GENERATING INSIGHTS")
    print("=" * 60)
    
    # Get latest quarter
    latest_quarter = mii_results['quarter'].max()
    latest_data = mii_results[mii_results['quarter'] == latest_quarter].copy()
    
    print(f"\n🏆 TOP 10 MODELS ({latest_quarter})")
    print("-" * 75)
    print(f"{'Rank':<6}{'Model':<35}{'MII':<9}{'Views':<11}{'Bids':<9}{'Year':<6}")
    print("-" * 75)
    
    for i, row in latest_data.head(10).iterrows():
        rank = latest_data.index.get_loc(i) + 1
        model_name = row['model'][:33]
        year = int(row['year']) if pd.notna(row['year']) else 'N/A'
        print(f"{rank:<6}{model_name:<35}{row['mii_score']:<9.1f}"
              f"{int(row['views']):<11,}{int(row['bids']):<9}{year:<6}")
    
    # Top manufacturers
    print(f"\n\n🏭 TOP MANUFACTURERS BY AVERAGE MII ({latest_quarter})")
    print("-" * 75)
    manufacturer_stats = latest_data.groupby('manufacturer').agg({
        'mii_score': 'mean',
        'model': 'count',
        'views': 'sum'
    }).round(2).sort_values('mii_score', ascending=False)
    
    print(f"{'Rank':<6}{'Manufacturer':<25}{'Avg MII':<12}{'Models':<10}{'Total Views'}")
    print("-" * 75)
    
    for idx, (manufacturer, row) in enumerate(manufacturer_stats.head(10).iterrows(), 1):
        print(f"{idx:<6}{manufacturer:<25}{row['mii_score']:<12.1f}"
              f"{int(row['model']):<10}{int(row['views']):,}")
    
    # Decade analysis
    if 'decade' in latest_data.columns:
        print(f"\n\n📅 TOP MODELS BY DECADE ({latest_quarter})")
        print("-" * 75)
        
        for decade in sorted(latest_data['decade'].dropna().unique(), reverse=True)[:5]:
            decade_data = latest_data[latest_data['decade'] == decade].head(3)
            if not decade_data.empty:
                print(f"\n{int(decade)}s:")
                for _, row in decade_data.iterrows():
                    model_name = row['model'][:40]
                    print(f"  • {model_name:<40} MII: {row['mii_score']:.1f}")
    
    # Data source breakdown
    print(f"\n\n📈 DATA SOURCE BREAKDOWN")
    print("-" * 75)
    source_counts = mii_results['data_source'].value_counts()
    for source, count in source_counts.items():
        pct = (count / len(mii_results)) * 100
        print(f"{source:<15} {count:>6,} records ({pct:>5.1f}%)")
    
    return latest_data

def save_and_upload_results(mii_results, latest_data):
    """Save results locally and upload to S3"""
    print("\n\n💾 SAVING AND UPLOADING RESULTS")
    print("=" * 60)
    
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
    print("=" * 60)
    print("🚀 MII Calculator with Manufacturer Name Cleanup")
    print("=" * 60)
    print(f"⏰ Started at: {datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    
    try:
        # Load data
        df = load_scraped_data()
        
        # Show manufacturer statistics after cleanup
        print("\n📊 MANUFACTURER STATISTICS AFTER CLEANUP")
        print("=" * 60)
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
        print("=" * 60)
        
    except Exception as e:
        print(f"\n❌ Error: {e}")
        import traceback
        traceback.print_exc()
        return 1
    
    return 0

if __name__ == "__main__":
    exit(main())
