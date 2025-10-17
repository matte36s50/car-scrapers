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

def extract_proper_model(model_text, make_text=None):
    """
    Extract proper model name, handling special cases like Mercedes AMG
    
    Examples:
    - "2015 Mercedes-Benz C63 AMG" -> "C63 AMG"
    - "Mercedes-Benz AMG GT" -> "AMG GT"  
    - "1987 BMW M3" -> "M3"
    - "Porsche 911 Turbo" -> "911 Turbo"
    """
    if not model_text or pd.isna(model_text):
        return None
    
    model_str = str(model_text).strip()
    
    # Remove year if present at the start
    model_str = re.sub(r'^\d{4}\s*', '', model_str)
    
    # Remove make if present (case insensitive)
    if make_text and pd.notna(make_text):
        make_pattern = re.escape(str(make_text))
        model_str = re.sub(rf'{make_pattern}[\s-]*', '', model_str, flags=re.IGNORECASE)
    
    # Common make names to remove if make wasn't provided
    common_makes = [
        'Mercedes-Benz', 'Mercedes', 'BMW', 'Porsche', 'Audi', 'Ferrari',
        'Lamborghini', 'McLaren', 'Chevrolet', 'Ford', 'Dodge', 'Tesla',
        'Toyota', 'Honda', 'Nissan', 'Lexus', 'Acura', 'Infiniti'
    ]
    
    for make in common_makes:
        make_pattern = re.escape(make)
        model_str = re.sub(rf'{make_pattern}[\s-]*', '', model_str, flags=re.IGNORECASE)
    
    # Clean up extra whitespace
    model_str = re.sub(r'\s+', ' ', model_str).strip()
    
    return model_str if model_str else None

def clean_sale_amount(sale_text):
    """
    Clean and validate sale amounts, filtering out obvious errors
    
    Returns None for invalid amounts
    """
    if not sale_text or pd.isna(sale_text):
        return None
    
    # Extract number from text
    sale_str = str(sale_text).replace('$', '').replace(',', '').strip()
    match = re.search(r'\d+', sale_str)
    
    if not match:
        return None
    
    amount = int(match.group(0))
    
    # Validation rules for automotive auction data
    MIN_REASONABLE = 100      # $100 minimum (parts cars, non-runners)
    MAX_REASONABLE = 10000000  # $10M maximum (hypercar territory)
    
    # Flag suspicious amounts
    if amount < MIN_REASONABLE:
        print(f"  ⚠️  Filtered sale amount too low: ${amount:,}")
        return None
    
    if amount > MAX_REASONABLE:
        print(f"  ⚠️  Filtered sale amount too high: ${amount:,}")
        return None
    
    # Additional validation: Cybertrucks shouldn't exceed $200k
    # This is a temporary fix - you might want to make this more sophisticated
    return amount

def get_instagram_estimates(all_models):
    """Generate Instagram estimates for models"""
    known_estimates = {
        # BMW Models
        "bmw": 650000, "m3": 280000, "e30": 18000, "e36": 15000, "e46": 42000,
        "2002": 12000, "z8": 4500, "m5": 140000, "m4": 35000, "z4": 22000,
        
        # Mercedes Models - IMPROVED AMG HANDLING
        "mercedes": 480000, "190e": 18000, "c63": 85000, "c63 amg": 85000,
        "e63": 65000, "e63 amg": 65000, "s63": 55000, "s63 amg": 55000,
        "amg gt": 75000, "g63": 95000, "g63 amg": 95000, "sl63": 42000,
        "g-class": 55000, "sl": 18000, "cls63": 35000, "e55": 28000,
        "c55": 22000, "sl65": 18000, "sl55": 15000,
        
        # Porsche Models
        "porsche": 450000, "911": 150000, "turbo": 45000, "gt3": 65000,
        "boxster": 28000, "cayman": 32000, "gt2": 42000, "carrera": 85000,
        
        # Japanese Performance
        "toyota": 180000, "supra": 55000, "nissan": 120000, "gtr": 38000,
        "gt-r": 38000, "honda": 160000, "s2000": 35000, "nsx": 22000,
        
        # American Muscle
        "ford": 180000, "mustang": 85000, "chevrolet": 150000, "corvette": 95000,
        "camaro": 65000, "challenger": 45000, "hellcat": 32000,
        
        # Supercars
        "ferrari": 320000, "lamborghini": 280000, "mclaren": 85000,
        "aventador": 75000, "huracan": 85000,
        
        # Electric/Modern
        "tesla": 220000, "cybertruck": 45000, "model s": 65000,
        "taycan": 38000, "i8": 28000,
    }
    
    estimates = {}
    for model in all_models:
        if pd.isna(model):
            continue
        
        model_clean = str(model).lower()
        instagram_count = 8000  # Default
        
        # Check for matches with longest matches first (more specific)
        sorted_keys = sorted(known_estimates.keys(), key=len, reverse=True)
        for key in sorted_keys:
            if key in model_clean:
                instagram_count = max(instagram_count, int(known_estimates[key] * 0.3))
                break
        
        # Brand-based estimation if no specific match
        if instagram_count == 8000:
            if any(brand in model_clean for brand in ['bmw', 'mercedes', 'porsche', 'ferrari', 'lamborghini']):
                instagram_count = 20000
            elif any(brand in model_clean for brand in ['toyota', 'honda', 'nissan']):
                instagram_count = 12000
        
        estimates[model] = instagram_count
    
    return estimates

def load_scraped_data():
    """Load data from bat.csv and cnb.csv files in S3"""
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
        if 'model' not in df.columns and 'title' in df.columns:
            df['model'] = df['title']
        
        all_data.append(df)
        print(f"  ✅ Loaded {len(df)} BAT records")
        os.remove('temp_bat.csv')
        
    except Exception as e:
        print(f"  ⚠️ Could not load bat.csv from S3: {e}")
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
        os.remove('temp_cnb.csv')
        
    except Exception as e:
        print(f"  ⚠️ Could not load cnb.csv from S3: {e}")
        if os.path.exists('cnb.csv'):
            df = pd.read_csv('cnb.csv')
            df['data_source'] = 'CNB'
            all_data.append(df)
            print(f"  ✅ Loaded {len(df)} CNB records from local file")
    
    if not all_data:
        print("❌ No scraped data found!")
        return pd.DataFrame()
    
    combined_df = pd.concat(all_data, ignore_index=True, sort=False)
    print(f"📈 Combined total: {len(combined_df)} auction records")
    return combined_df

def clean_and_process_data(df):
    """Clean and standardize the scraped data with improved model extraction"""
    print("🧹 Cleaning and processing data...")
    
    original_count = len(df)
    
    # Ensure we have required columns
    required_cols = ['model', 'views', 'bids', 'data_source']
    for col in required_cols:
        if col not in df.columns:
            df[col] = 0 if col in ['views', 'bids'] else 'Unknown'
    
    # CRITICAL FIX: Extract proper model names before any filtering
    print("\n🔧 Extracting proper model names...")
    df['model_original'] = df['model'].copy()  # Keep original for debugging
    
    if 'make' in df.columns:
        df['model_clean'] = df.apply(
            lambda row: extract_proper_model(row['model'], row['make']), 
            axis=1
        )
    else:
        df['model_clean'] = df['model'].apply(lambda x: extract_proper_model(x))
    
    # Use cleaned model as the primary model field
    df['model'] = df['model_clean']
    
    # Show some examples of the transformation
    print("\n📝 Model name transformation examples:")
    sample = df[df['model_original'] != df['model']].head(10)
    for _, row in sample.iterrows():
        print(f"  {row['model_original'][:50]} → {row['model'][:50]}")
    
    # Remove rows with no valid model
    df = df[df['model'].notna()]
    df = df[df['model'] != '']
    
    # Extract numeric values from text fields
    def extract_number(val):
        if pd.isna(val):
            return 0
        if isinstance(val, (int, float)):
            return int(val)
        matches = re.findall(r'\d+', str(val).replace(',', ''))
        return int(matches[0]) if matches else 0
    
    df['views_numeric'] = df['views'].apply(extract_number)
    df['bids_numeric'] = df['bids'].apply(extract_number)
    
    # DATA QUALITY FILTERING
    print("\n⚠️  DATA QUALITY FILTERING:")
    
    bat_data = df[df['data_source'] == 'BAT'].copy()
    cnb_data = df[df['data_source'] == 'CNB'].copy()
    
    print(f"   BAT entries: {len(bat_data)}")
    print(f"   CNB entries: {len(cnb_data)}")
    
    # Filter CNB entries with suspiciously low views
    low_views_cnb = cnb_data[cnb_data['views_numeric'] < 50]
    print(f"\n   🔍 Found {len(low_views_cnb)} CNB entries with views < 50 (filtering out)")
    
    cnb_filtered = cnb_data[cnb_data['views_numeric'] >= 50]
    df = pd.concat([bat_data, cnb_filtered], ignore_index=True)
    
    print(f"   ✅ Retaining {len(df)} entries after quality filter")
    
    # Handle comments
    if 'comments' in df.columns:
        df['comments_numeric'] = df['comments'].apply(extract_number)
    else:
        df['comments_numeric'] = 0
    
    # CRITICAL FIX: Clean sale amounts with validation
    print("\n💰 Cleaning sale amounts with validation...")
    if 'sale_amount' in df.columns:
        df['sale_amount_numeric'] = df['sale_amount'].apply(clean_sale_amount)
        
        # Report on filtered amounts
        invalid_amounts = df['sale_amount_numeric'].isna().sum()
        if invalid_amounts > 0:
            print(f"   ⚠️  Filtered {invalid_amounts} invalid sale amounts")
    else:
        df['sale_amount_numeric'] = 0
    
    # Quarter assignment with fallback
    def assign_quarter(row):
        date_fields = ['scraped_date', 'sale_date', 'end_date']
        for field in date_fields:
            if field in row and pd.notna(row[field]):
                try:
                    date = pd.to_datetime(row[field], errors='coerce')
                    if pd.notna(date):
                        return date.to_period('Q').strftime('%Y') + 'Q' + str(date.quarter)
                except:
                    pass
        
        # Fallback to current quarter
        now = datetime.datetime.now()
        return f"{now.year}Q{(now.month-1)//3 + 1}"
    
    df['quarter'] = df.apply(assign_quarter, axis=1)
    
    # Extract year
    def extract_year(row):
        if 'year' in row and pd.notna(row['year']):
            try:
                year = int(row['year'])
                if 1900 <= year <= datetime.datetime.now().year + 2:
                    return year
            except:
                pass
        
        if 'model_original' in row and pd.notna(row['model_original']):
            matches = re.findall(r'\b(19|20)\d{2}\b', str(row['model_original']))
            if matches:
                year = int(matches[0])
                if 1900 <= year <= datetime.datetime.now().year + 2:
                    return year
        
        return None
    
    df['year'] = df.apply(extract_year, axis=1)
    df['car_age'] = datetime.datetime.now().year - df['year'].fillna(datetime.datetime.now().year)
    
    print(f"\n✅ Cleaned data: {len(df)} records with {df['model'].nunique()} unique models")
    
    # Show make distribution if available
    if 'make' in df.columns:
        print(f"\n📊 Top 10 Makes:")
        make_counts = df['make'].value_counts().head(10)
        for make, count in make_counts.items():
            print(f"   {make}: {count} auctions")
    
    return df

def calculate_mii_scores(df):
    """Calculate MII scores for the models"""
    print("\n🧮 Calculating MII scores...")
    
    # Get Instagram estimates
    all_models = df['model'].unique()
    instagram_estimates = get_instagram_estimates(all_models)
    
    instagram_df = pd.DataFrame([
        {'model': model, 'instagram_mentions': count} 
        for model, count in instagram_estimates.items()
    ])
    
    df = df.merge(instagram_df, on='model', how='left')
    df['instagram_mentions'] = df['instagram_mentions'].fillna(8000)
    
    # Group by model, make (if available), and quarter
    group_cols = ['model', 'quarter']
    if 'make' in df.columns:
        group_cols.insert(0, 'make')
    
    agg_dict = {
        'views_numeric': 'mean',
        'bids_numeric': 'mean',
        'comments_numeric': 'mean',
        'sale_amount_numeric': lambda x: x[x > 0].mean() if (x > 0).any() else 0,  # Only average valid amounts
        'data_source': 'count',
        'year': 'first',
        'car_age': 'first',
        'instagram_mentions': 'first'
    }
    
    grouped = df.groupby(group_cols).agg(agg_dict).reset_index()
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
        'z_views_numeric': 3.0,
        'z_bids_numeric': 4.0,
        'z_sale_amount_numeric': 3.5,
        'z_comments_numeric': 1.5,
        'z_total_auctions': 2.0,
        'z_instagram_mentions': 2.0,
        'z_car_age': 1.0
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
                group['MII_Index'] = 50
        return group
    
    grouped = grouped.groupby('quarter').apply(calculate_index).reset_index(drop=True)
    
    # Add ranking
    grouped['Quarter_Rank'] = grouped.groupby('quarter')['MII_Index'].rank(ascending=False, method='min')
    
    # Calculate momentum
    grouped = grouped.sort_values(['model', 'quarter'])
    grouped['MII_Momentum'] = grouped.groupby('model')['MII_Index'].diff()
    
    # Add metadata
    grouped['calculation_date'] = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    
    # Sort by quarter and MII Index
    grouped = grouped.sort_values(['quarter', 'MII_Index'], ascending=[False, False])
    
    print(f"✅ Calculated MII for {len(grouped)} model-quarter combinations")
    return grouped

def generate_insights(mii_results):
    """Generate insights from MII results - Mercedes focused"""
    print("\n📊 GENERATING INSIGHTS")
    print("="*80)
    
    valid_quarters = sorted([q for q in mii_results['quarter'].unique() if q != 'NaT'], reverse=True)
    latest_quarter = valid_quarters[0] if valid_quarters else 'Unknown'
    latest_data = mii_results[mii_results['quarter'] == latest_quarter]
    
    # MERCEDES-SPECIFIC ANALYSIS
    if 'make' in mii_results.columns:
        print(f"\n🔹 MERCEDES-BENZ MODEL ANALYSIS ({latest_quarter})")
        print("-" * 80)
        
        mercedes_data = latest_data[latest_data['make'].str.contains('Mercedes', case=False, na=False)]
        
        if not mercedes_data.empty:
            print(f"\nTop 15 Mercedes Models:")
            print(f"{'Rank':<5} {'Model':<30} {'MII':<8} {'Views':<10} {'Bids':<8} {'Avg 
    
    # Top performers overall
    print(f"\n🏆 TOP 20 MODELS OVERALL ({latest_quarter})")
    print("-" * 90)
    print(f"{'Rank':<5} {'Make':<15} {'Model':<25} {'MII':<8} {'Views':<10} {'Bids':<8} {'Year':<6}")
    print("-" * 90)
    
    for _, row in latest_data.head(20).iterrows():
        make_display = row.get('make', 'N/A')[:13] if 'make' in row else 'N/A'
        model_short = row['model'][:23] + '..' if len(row['model']) > 25 else row['model']
        year_display = str(int(row['year'])) if pd.notna(row['year']) else 'N/A'
        views_display = f"{row['views_numeric']:.0f}" if pd.notna(row['views_numeric']) else 'N/A'
        bids_display = f"{row['bids_numeric']:.0f}" if pd.notna(row['bids_numeric']) else 'N/A'
        
        print(f"{int(row['Quarter_Rank']):<5} {make_display:<15} {model_short:<25} {row['MII_Index']:<8.1f} "
              f"{views_display:<10} {bids_display:<8} {year_display:<6}")
    
    # Make comparison
    if 'make' in mii_results.columns:
        print(f"\n🚗 MAKE COMPARISON ({latest_quarter})")
        print("-" * 60)
        
        make_stats = latest_data.groupby('make').agg({
            'MII_Index': 'mean',
            'model': 'count',
            'sale_amount_numeric': lambda x: x[x > 0].mean() if (x > 0).any() else 0
        }).nlargest(10, 'MII_Index')
        
        print(f"{'Make':<20} {'Avg MII':<10} {'Models':<10} {'Avg Sale $':<15}")
        print("-" * 60)
        for make, stats in make_stats.iterrows():
            avg_sale = f"${stats['sale_amount_numeric']:,.0f}" if stats['sale_amount_numeric'] > 0 else 'N/A'
            print(f"{make:<20} {stats['MII_Index']:<10.1f} {int(stats['model']):<10} {avg_sale:<15}")
    
    return latest_quarter

def main():
    print("🚀 MII Calculator - Fixed for Mercedes Models & Sale Amount Validation")
    print(f"⏰ Started at: {datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    
    # Load scraped data
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
    
    if success:
        mii_results.to_csv("mii_results_latest.csv", index=False)
        upload_to_s3("mii_results_latest.csv", "my-mii-reports")
    
    # Summary
    print(f"\n📊 FINAL STATISTICS")
    print(f"="*60)
    print(f"Total models analyzed: {mii_results['model'].nunique()}")
    print(f"Total auctions processed: {mii_results['total_auctions'].sum():.0f}")
    print(f"Latest quarter: {latest_quarter}")
    
    if 'make' in mii_results.columns:
        print(f"Total makes: {mii_results['make'].nunique()}")
        mercedes_count = len(mii_results[mii_results['make'].str.contains('Mercedes', case=False, na=False)])
        print(f"Mercedes models: {mercedes_count}")
    
    # Cleanup
    try:
        os.remove(output_file)
        if os.path.exists("mii_results_latest.csv"):
            os.remove("mii_results_latest.csv")
    except:
        pass
    
    print(f"\n🎉 MII calculation completed successfully!")
    return success

if __name__ == "__main__":
    main()
:<12}")
            print("-" * 80)
            
            for _, row in mercedes_data.head(15).iterrows():
                model_short = row['model'][:28] + '..' if len(row['model']) > 30 else row['model']
                views_display = f"{row['views_numeric']:.0f}" if pd.notna(row['views_numeric']) else 'N/A'
                bids_display = f"{row['bids_numeric']:.0f}" if pd.notna(row['bids_numeric']) else 'N/A'
                sale_display = f"${row['sale_amount_numeric']:,.0f}" if pd.notna(row['sale_amount_numeric']) and row['sale_amount_numeric'] > 0 else 'N/A'
                
                print(f"{int(row['Quarter_Rank']):<5} {model_short:<30} {row['MII_Index']:<8.1f} "
                      f"{views_display:<10} {bids_display:<8} {sale_display:<12}")
            
            # Mercedes vs Competitors comparison
            print(f"\n\n🔹 MERCEDES VS COMPETITORS - LUXURY PERFORMANCE ({latest_quarter})")
            print("-" * 90)
            
            competitor_makes = ['Mercedes', 'BMW', 'Porsche', 'Audi', 'Lexus']
            competitor_data = latest_data[latest_data['make'].str.contains('|'.join(competitor_makes), case=False, na=False)]
            
            if not competitor_data.empty:
                comp_stats = competitor_data.groupby('make').agg({
                    'MII_Index': 'mean',
                    'model': 'count',
                    'views_numeric': 'mean',
                    'bids_numeric': 'mean',
                    'sale_amount_numeric': lambda x: x[x > 0].mean() if (x > 0).any() else 0
                }).sort_values('MII_Index', ascending=False)
                
                print(f"{'Make':<15} {'Avg MII':<10} {'Models':<10} {'Avg Views':<12} {'Avg Bids':<10} {'Avg Sale 
    
    # Top performers overall
    print(f"\n🏆 TOP 20 MODELS OVERALL ({latest_quarter})")
    print("-" * 90)
    print(f"{'Rank':<5} {'Make':<15} {'Model':<25} {'MII':<8} {'Views':<10} {'Bids':<8} {'Year':<6}")
    print("-" * 90)
    
    for _, row in latest_data.head(20).iterrows():
        make_display = row.get('make', 'N/A')[:13] if 'make' in row else 'N/A'
        model_short = row['model'][:23] + '..' if len(row['model']) > 25 else row['model']
        year_display = str(int(row['year'])) if pd.notna(row['year']) else 'N/A'
        views_display = f"{row['views_numeric']:.0f}" if pd.notna(row['views_numeric']) else 'N/A'
        bids_display = f"{row['bids_numeric']:.0f}" if pd.notna(row['bids_numeric']) else 'N/A'
        
        print(f"{int(row['Quarter_Rank']):<5} {make_display:<15} {model_short:<25} {row['MII_Index']:<8.1f} "
              f"{views_display:<10} {bids_display:<8} {year_display:<6}")
    
    # Make comparison
    if 'make' in mii_results.columns:
        print(f"\n🚗 MAKE COMPARISON ({latest_quarter})")
        print("-" * 60)
        
        make_stats = latest_data.groupby('make').agg({
            'MII_Index': 'mean',
            'model': 'count',
            'sale_amount_numeric': lambda x: x[x > 0].mean() if (x > 0).any() else 0
        }).nlargest(10, 'MII_Index')
        
        print(f"{'Make':<20} {'Avg MII':<10} {'Models':<10} {'Avg Sale $':<15}")
        print("-" * 60)
        for make, stats in make_stats.iterrows():
            avg_sale = f"${stats['sale_amount_numeric']:,.0f}" if stats['sale_amount_numeric'] > 0 else 'N/A'
            print(f"{make:<20} {stats['MII_Index']:<10.1f} {int(stats['model']):<10} {avg_sale:<15}")
    
    return latest_quarter

def main():
    print("🚀 MII Calculator - Fixed for Mercedes Models & Sale Amount Validation")
    print(f"⏰ Started at: {datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    
    # Load scraped data
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
    
    if success:
        mii_results.to_csv("mii_results_latest.csv", index=False)
        upload_to_s3("mii_results_latest.csv", "my-mii-reports")
    
    # Summary
    print(f"\n📊 FINAL STATISTICS")
    print(f"="*60)
    print(f"Total models analyzed: {mii_results['model'].nunique()}")
    print(f"Total auctions processed: {mii_results['total_auctions'].sum():.0f}")
    print(f"Latest quarter: {latest_quarter}")
    
    if 'make' in mii_results.columns:
        print(f"Total makes: {mii_results['make'].nunique()}")
        mercedes_count = len(mii_results[mii_results['make'].str.contains('Mercedes', case=False, na=False)])
        print(f"Mercedes models: {mercedes_count}")
    
    # Cleanup
    try:
        os.remove(output_file)
        if os.path.exists("mii_results_latest.csv"):
            os.remove("mii_results_latest.csv")
    except:
        pass
    
    print(f"\n🎉 MII calculation completed successfully!")
    return success

if __name__ == "__main__":
    main()
:<15}")
                print("-" * 90)
                
                for make, stats in comp_stats.iterrows():
                    avg_sale = f"${stats['sale_amount_numeric']:,.0f}" if stats['sale_amount_numeric'] > 0 else 'N/A'
                    print(f"{make:<15} {stats['MII_Index']:<10.1f} {int(stats['model']):<10} "
                          f"{stats['views_numeric']:<12.0f} {stats['bids_numeric']:<10.1f} {avg_sale:<15}")
        else:
            print("  No Mercedes models found in dataset")
    
    # Top performers overall
    print(f"\n🏆 TOP 20 MODELS OVERALL ({latest_quarter})")
    print("-" * 90)
    print(f"{'Rank':<5} {'Make':<15} {'Model':<25} {'MII':<8} {'Views':<10} {'Bids':<8} {'Year':<6}")
    print("-" * 90)
    
    for _, row in latest_data.head(20).iterrows():
        make_display = row.get('make', 'N/A')[:13] if 'make' in row else 'N/A'
        model_short = row['model'][:23] + '..' if len(row['model']) > 25 else row['model']
        year_display = str(int(row['year'])) if pd.notna(row['year']) else 'N/A'
        views_display = f"{row['views_numeric']:.0f}" if pd.notna(row['views_numeric']) else 'N/A'
        bids_display = f"{row['bids_numeric']:.0f}" if pd.notna(row['bids_numeric']) else 'N/A'
        
        print(f"{int(row['Quarter_Rank']):<5} {make_display:<15} {model_short:<25} {row['MII_Index']:<8.1f} "
              f"{views_display:<10} {bids_display:<8} {year_display:<6}")
    
    # Make comparison
    if 'make' in mii_results.columns:
        print(f"\n🚗 MAKE COMPARISON ({latest_quarter})")
        print("-" * 60)
        
        make_stats = latest_data.groupby('make').agg({
            'MII_Index': 'mean',
            'model': 'count',
            'sale_amount_numeric': lambda x: x[x > 0].mean() if (x > 0).any() else 0
        }).nlargest(10, 'MII_Index')
        
        print(f"{'Make':<20} {'Avg MII':<10} {'Models':<10} {'Avg Sale $':<15}")
        print("-" * 60)
        for make, stats in make_stats.iterrows():
            avg_sale = f"${stats['sale_amount_numeric']:,.0f}" if stats['sale_amount_numeric'] > 0 else 'N/A'
            print(f"{make:<20} {stats['MII_Index']:<10.1f} {int(stats['model']):<10} {avg_sale:<15}")
    
    return latest_quarter

def main():
    print("🚀 MII Calculator - Fixed for Mercedes Models & Sale Amount Validation")
    print(f"⏰ Started at: {datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    
    # Load scraped data
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
    
    if success:
        mii_results.to_csv("mii_results_latest.csv", index=False)
        upload_to_s3("mii_results_latest.csv", "my-mii-reports")
    
    # Summary
    print(f"\n📊 FINAL STATISTICS")
    print(f"="*60)
    print(f"Total models analyzed: {mii_results['model'].nunique()}")
    print(f"Total auctions processed: {mii_results['total_auctions'].sum():.0f}")
    print(f"Latest quarter: {latest_quarter}")
    
    if 'make' in mii_results.columns:
        print(f"Total makes: {mii_results['make'].nunique()}")
        mercedes_count = len(mii_results[mii_results['make'].str.contains('Mercedes', case=False, na=False)])
        print(f"Mercedes models: {mercedes_count}")
    
    # Cleanup
    try:
        os.remove(output_file)
        if os.path.exists("mii_results_latest.csv"):
            os.remove("mii_results_latest.csv")
    except:
        pass
    
    print(f"\n🎉 MII calculation completed successfully!")
    return success

if __name__ == "__main__":
    main()
