import pandas as pd
import numpy as np
import datetime
import re
import os
import boto3
from botocore.exceptions import NoCredentialsError

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
        "toyota": 180000, "supra": 55000, "nissan": 120000, "gtr": 38000,
        "honda": 160000, "s2000": 35000, "nsx": 22000,
        
        # American Muscle
        "ford": 180000, "mustang": 85000, "chevrolet": 150000, "corvette": 95000,
        "camaro": 65000, "challenger": 45000,
        
        # Supercars
        "ferrari": 320000, "lamborghini": 280000, "mclaren": 85000,
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
        
        # Standardize column names for MII calculation
        if 'model' not in df.columns and 'title' in df.columns:
            df['model'] = df['title']
        elif 'model' not in df.columns and 'auction_url' in df.columns:
            # Extract model from URL if needed
            df['model'] = df['auction_url'].str.extract(r'/listing/([^/]+)$')[0]
        
        all_data.append(df)
        print(f"  ✅ Loaded {len(df)} BAT records")
        
        # Clean up temp file
        os.remove('temp_bat.csv')
        
    except Exception as e:
        print(f"  ⚠️ Could not load bat.csv from S3: {e}")
        # Try local file as fallback
        if os.path.exists('bat.csv'):
            df = pd.read_csv('bat.csv')
            df['data_source'] = 'BAT'
            all_data.append(df)
            print(f"  ✅ Loaded {len(df)} BAT records from local file")
    
    # Load CNB data from S3
    try:
        print(f"📊 Downloading cnb.csv from S3...")
        s3.download_file('my-mii-reports', 'cnb.csv', 'temp_cnb.csv')
        df = pd.read_csv('temp_cnb.csv')
        df['data_source'] = 'CNB'
        all_data.append(df)
        print(f"  ✅ Loaded {len(df)} CNB records")
        
        # Clean up temp file
        os.remove('temp_cnb.csv')
        
    except Exception as e:
        print(f"  ⚠️ Could not load cnb.csv from S3: {e}")
        # Try local file as fallback
        if os.path.exists('cnb.csv'):
            df = pd.read_csv('cnb.csv')
            df['data_source'] = 'CNB'
            all_data.append(df)
            print(f"  ✅ Loaded {len(df)} CNB records from local file")
    
    if not all_data:
        print("❌ No scraped data found!")
        return pd.DataFrame()
    
    # Combine all data
    combined_df = pd.concat(all_data, ignore_index=True, sort=False)
    print(f"📈 Combined total: {len(combined_df)} auction records")
    print(f"   BAT records: {len(combined_df[combined_df['data_source'] == 'BAT'])}")
    print(f"   CNB records: {len(combined_df[combined_df['data_source'] == 'CNB'])}")
    
    return combined_df

def clean_and_process_data(df):
    """Clean and standardize the scraped data"""
    print("🧹 Cleaning and processing data...")
    
    # Ensure we have required columns
    required_cols = ['model', 'views', 'bids', 'data_source']
    for col in required_cols:
        if col not in df.columns:
            df[col] = 0 if col in ['views', 'bids'] else 'Unknown'
    
    # Clean model names
    df['model'] = df['model'].astype(str).str.strip()
    df = df[df['model'] != 'nan']
    df = df[df['model'] != '']
    df = df[df['model'].notna()]
    
    # Extract numeric values from text fields
    def extract_number(val):
        if pd.isna(val):
            return 0
        # Handle both string and numeric inputs
        if isinstance(val, (int, float)):
            return int(val)
        matches = re.findall(r'\d+', str(val).replace(',', ''))
        return int(matches[0]) if matches else 0
    
    df['views_numeric'] = df['views'].apply(extract_number)
    df['bids_numeric'] = df['bids'].apply(extract_number)
    
    # Handle comments if the column exists
    if 'comments' in df.columns:
        df['comments_numeric'] = df['comments'].apply(extract_number)
    else:
        df['comments_numeric'] = 0
    
    # Add quarter information
    if 'scraped_date' in df.columns:
        df['quarter'] = pd.to_datetime(df['scraped_date'], errors='coerce').dt.to_period('Q').astype(str)
    elif 'sale_date' in df.columns:
        df['quarter'] = pd.to_datetime(df['sale_date'], errors='coerce').dt.to_period('Q').astype(str)
    else:
        current_quarter = f"{datetime.datetime.now().year}Q{(datetime.datetime.now().month-1)//3 + 1}"
        df['quarter'] = current_quarter
    
    # Extract year from multiple possible sources
    def extract_year(row):
        # Try year column first
        if 'year' in row and pd.notna(row['year']):
            try:
                year = int(row['year'])
                if 1900 <= year <= datetime.datetime.now().year + 2:
                    return year
            except:
                pass
        
        # Try extracting from model name
        if 'model' in row and pd.notna(row['model']):
            matches = re.findall(r'\b(19|20)\d{2}\b', str(row['model']))
            if matches:
                year = int(matches[0])
                if 1900 <= year <= datetime.datetime.now().year + 2:
                    return year
        
        return None
    
    df['year'] = df.apply(extract_year, axis=1)
    df['car_age'] = datetime.datetime.now().year - df['year'].fillna(datetime.datetime.now().year)
    
    # Extract sale amounts if present
    if 'sale_amount' in df.columns:
        def extract_sale_amount(val):
            if pd.isna(val):
                return 0
            # Remove $ and commas, extract number
            val_str = str(val).replace('$', '').replace(',', '')
            matches = re.findall(r'\d+', val_str)
            return int(matches[0]) if matches else 0
        
        df['sale_amount_numeric'] = df['sale_amount'].apply(extract_sale_amount)
    else:
        df['sale_amount_numeric'] = 0
    
    print(f"✅ Cleaned data: {len(df)} records with {df['model'].nunique()} unique models")
    print(f"   Average views: {df['views_numeric'].mean():.0f}")
    print(f"   Average bids: {df['bids_numeric'].mean():.1f}")
    
    return df

def calculate_mii_scores(df):
    """Calculate MII scores for the models"""
    print("🧮 Calculating MII scores...")
    
    # Get Instagram estimates
    all_models = df['model'].unique()
    instagram_estimates = get_instagram_estimates(all_models)
    
    # Create Instagram DataFrame and merge
    instagram_df = pd.DataFrame([
        {'model': model, 'instagram_mentions': count} 
        for model, count in instagram_estimates.items()
    ])
    
    df = df.merge(instagram_df, on='model', how='left')
    df['instagram_mentions'] = df['instagram_mentions'].fillna(8000)
    
    # Group by model and quarter
    agg_dict = {
        'views_numeric': 'mean',
        'bids_numeric': 'mean',
        'comments_numeric': 'mean',
        'sale_amount_numeric': 'mean',
        'data_source': 'count',  # This becomes total_auctions
        'year': 'first',
        'car_age': 'first',
        'instagram_mentions': 'first'
    }
    
    # Add make if it exists
    if 'make' in df.columns:
        agg_dict['make'] = 'first'
    
    grouped = df.groupby(['model', 'quarter']).agg(agg_dict).reset_index()
    
    grouped = grouped.rename(columns={'data_source': 'total_auctions'})
    
    # Calculate z-scores within each quarter
    def calculate_quarter_scores(group):
        metrics = ['views_numeric', 'bids_numeric', 'comments_numeric', 
                  'sale_amount_numeric', 'total_auctions', 'instagram_mentions', 'car_age']
        
        for metric in metrics:
            if metric in group.columns and group[metric].std() > 0:
                group[f'z_{metric}'] = (group[metric] - group[metric].mean()) / group[metric].std()
            else:
                group[f'z_{metric}'] = 0
        
        return group
    
    grouped = grouped.groupby('quarter').apply(calculate_quarter_scores).reset_index(drop=True)
    
    # Calculate MII with weighted scoring
    mii_weights = {
        'z_views_numeric': 3.0,          # Viewer interest
        'z_bids_numeric': 4.0,           # Market competition  
        'z_sale_amount_numeric': 3.5,    # Market value
        'z_comments_numeric': 1.5,       # Community engagement
        'z_total_auctions': 2.0,         # Market activity
        'z_instagram_mentions': 2.0,     # Social presence
        'z_car_age': 1.0                # Classic appeal
    }
    
    total_weight = sum(mii_weights.values())
    
    grouped['MII_Score'] = sum(
        grouped.get(col, 0) * weight for col, weight in mii_weights.items()
    ) / total_weight
    
    # Calculate MII Index (0-100 scale per quarter)
    def calculate_index(group):
        if len(group) > 0:
            max_score = group['MII_Score'].max()
            min_score = group['MII_Score'].min()
            if max_score != min_score:
                group['MII_Index'] = ((group['MII_Score'] - min_score) / (max_score - min_score)) * 100
            else:
                group['MII_Index'] = 50  # Default if all same
        return group
    
    grouped = grouped.groupby('quarter').apply(calculate_index).reset_index(drop=True)
    
    # Add ranking
    grouped['Quarter_Rank'] = grouped.groupby('quarter')['MII_Index'].rank(ascending=False, method='min')
    
    # Calculate momentum (quarter-over-quarter change)
    grouped = grouped.sort_values(['model', 'quarter'])
    grouped['MII_Momentum'] = grouped.groupby('model')['MII_Index'].diff()
    
    # Add metadata
    grouped['calculation_date'] = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    grouped['components_used'] = ', '.join(mii_weights.keys())
    
    # Sort by quarter and MII Index
    grouped = grouped.sort_values(['quarter', 'MII_Index'], ascending=[False, False])
    
    print(f"✅ Calculated MII for {len(grouped)} model-quarter combinations")
    return grouped

def generate_insights(mii_results):
    """Generate insights from MII results"""
    print("\n📊 GENERATING INSIGHTS")
    print("="*60)
    
    latest_quarter = mii_results['quarter'].iloc[0] if len(mii_results) > 0 else 'Unknown'
    latest_data = mii_results[mii_results['quarter'] == latest_quarter]
    
    # Top performers
    print(f"\n🏆 TOP 10 MODELS ({latest_quarter})")
    print("-" * 75)
    print(f"{'Rank':<5} {'Model':<35} {'MII':<8} {'Views':<10} {'Bids':<8} {'Year':<6}")
    print("-" * 75)
    
    for _, row in latest_data.head(10).iterrows():
        model_short = row['model'][:33] + '..' if len(row['model']) > 35 else row['model']
        year_display = str(int(row['year'])) if pd.notna(row['year']) else 'N/A'
        views_display = f"{row['views_numeric']:.0f}" if pd.notna(row['views_numeric']) else 'N/A'
        bids_display = f"{row['bids_numeric']:.0f}" if pd.notna(row['bids_numeric']) else 'N/A'
        
        print(f"{int(row['Quarter_Rank']):<5} {model_short:<35} {row['MII_Index']:<8.1f} "
              f"{views_display:<10} {bids_display:<8} {year_display:<6}")
    
    # Biggest movers (if we have multiple quarters)
    if len(mii_results['quarter'].unique()) > 1:
        movers = mii_results[mii_results['quarter'] == latest_quarter].nlargest(5, 'MII_Momentum')
        if not movers.empty and movers['MII_Momentum'].notna().any():
            print(f"\n📈 TOP GAINERS ({latest_quarter})")
            print("-" * 60)
            for _, row in movers.iterrows():
                if pd.notna(row['MII_Momentum']):
                    print(f"{row['model'][:40]:<40} +{row['MII_Momentum']:.1f} points")
    
    # Category insights
    if 'make' in mii_results.columns:
        make_stats = latest_data.groupby('make').agg({
            'MII_Index': 'mean',
            'model': 'count'
        }).nlargest(5, 'MII_Index')
        
        print(f"\n🚗 TOP MAKES BY AVERAGE MII")
        print("-" * 40)
        for make, stats in make_stats.iterrows():
            print(f"{make:<20} {stats['MII_Index']:.1f} ({int(stats['model'])} models)")
    
    return latest_quarter

def main():
    print("🚀 MII Calculator - Single File Version")
    print(f"⏰ Started at: {datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    
    # Load scraped data from S3
    raw_data = load_scraped_data()
    if raw_data.empty:
        print("❌ No data to process!")
        return False
    
    # Clean and process
    clean_data = clean_and_process_data(raw_data)
    if clean_data.empty:
        print("❌ No clean data to process!")
        return False
    
    # Calculate MII scores
    mii_results = calculate_mii_scores(clean_data)
    
    # Generate insights
    latest_quarter = generate_insights(mii_results)
    
    # Save results
    output_file = f"mii_results_{datetime.datetime.now().strftime('%Y%m%d_%H%M')}.csv"
    mii_results.to_csv(output_file, index=False)
    print(f"\n💾 Saved results to: {output_file}")
    
    # Upload to S3
    print(f"☁️ Uploading to S3...")
    success = upload_to_s3(output_file, "my-mii-reports")
    
    # Also save a "latest" version for easy access
    if success:
        mii_results.to_csv("mii_results_latest.csv", index=False)
        upload_to_s3("mii_results_latest.csv", "my-mii-reports")
    
    # Summary statistics
    print(f"\n📊 FINAL STATISTICS")
    print(f"="*40)
    print(f"Total models analyzed: {mii_results['model'].nunique()}")
    print(f"Total auctions processed: {mii_results['total_auctions'].sum():.0f}")
    print(f"Latest quarter: {latest_quarter}")
    print(f"Average MII Index: {mii_results[mii_results['quarter'] == latest_quarter]['MII_Index'].mean():.1f}")
    
    # Cleanup
    try:
        os.remove(output_file)
        if os.path.exists("mii_results_latest.csv"):
            os.remove("mii_results_latest.csv")
    except:
        pass
    
    print(f"\n{'🎉 MII calculation completed successfully!' if success else '⚠️ MII completed but S3 upload failed'}")
    return success

if __name__ == "__main__":
    main()
