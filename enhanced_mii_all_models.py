#!/usr/bin/env python3
"""
Enhanced MII (Market Interest Index) Calculator - FIXED VERSION
================================================================

CRITICAL FIX: This version calculates MII for INDIVIDUAL auctions, not aggregated variants.
Each row represents ONE auction event with its unique performance metrics.

Changes from previous version:
- ✅ NO aggregation by variant_id - preserves individual $1M+ sales
- ✅ Integer bids only - no decimal values
- ✅ Preserves high-value sales (Singer, 918 Spyder, etc.)
- ✅ Adds optional manufacturer/model summaries (separate from MII)
- ✅ Validation checks to ensure data quality

Author: MII Analysis Team
Date: October 24, 2025
"""

import pandas as pd
import numpy as np
import re
import os
import sys
from datetime import datetime
import warnings
warnings.filterwarnings('ignore')

# AWS S3 for data storage
import boto3
from botocore.exceptions import NoCredentialsError

# ============================================================================
# CONFIGURATION
# ============================================================================

# Input files (can be local or S3)
BAT_FILE = "bat.csv"
CNB_FILE = "cnb.csv"  # Changed from cnb_sitemap_full_cleaned.csv for GitHub Actions compatibility

# Output files
OUTPUT_INDIVIDUAL = "mii_individual_auctions.csv"
OUTPUT_MODEL_SUMMARY = "mii_model_summary.csv"
OUTPUT_MANUFACTURER_SUMMARY = "mii_manufacturer_summary.csv"

# S3 Configuration
S3_BUCKET = "my-mii-reports"
UPLOAD_TO_S3 = True

# MII Weights (adjust as needed)
MII_WEIGHTS = {
    'views': 0.20,
    'bids': 0.25,
    'comments': 0.15,
    'sale_amount': 0.30,
    'car_age': -0.10  # Negative weight - newer is better
}

# ============================================================================
# UTILITY FUNCTIONS
# ============================================================================

def clean_cnb_price(price_str):
    """
    Clean CNB prices using comma-counting method.
    Fixes issues like "$95,00011" → "$95,000"
    
    Args:
        price_str: Price string from CNB scraper
        
    Returns:
        Cleaned numeric price value
    """
    if pd.isna(price_str) or price_str == "":
        return np.nan
    
    price_str = str(price_str).strip()
    
    # Store original for comma counting
    original_with_commas = price_str.replace('$', '').replace(' ', '')
    
    # Remove dollar signs, commas, whitespace
    price_clean = price_str.replace('$', '').replace(' ', '').replace(',', '')
    
    # If original had commas, check last segment
    if ',' in original_with_commas:
        parts = original_with_commas.split(',')
        last_part = parts[-1]
        
        # Last part should have exactly 3 digits in US format
        if len(last_part) > 3:
            # Too many digits - truncate to 3
            # Reconstruct: everything before last comma + corrected last part
            corrected_parts = parts[:-1] + [last_part[:3]]
            price_clean = ''.join(corrected_parts)
    
    # Convert to float
    try:
        return float(price_clean)
    except:
        return np.nan

def extract_numeric(value):
    """Extract numeric value from string"""
    if pd.isna(value):
        return np.nan
    
    value_str = str(value).replace(',', '').replace('$', '')
    match = re.search(r'\d+\.?\d*', value_str)
    if match:
        try:
            return float(match.group())
        except:
            return np.nan
    return np.nan

def classify_variant(row):
    """
    Classify car into variant categories for analysis.
    This does NOT aggregate data - just adds a classification column.
    """
    model = str(row.get('model', '')).upper()
    year = row.get('year', None)
    
    # Extract generation/variant information
    variant_id = model
    
    # Add year decade if available
    if pd.notna(year):
        try:
            decade = int(float(year)) // 10 * 10
            variant_id += f"_{decade}s"
        except:
            variant_id += "_UNKNOWN_ERA"
    
    return variant_id

def upload_to_s3(file_path, bucket_name, object_name=None):
    """Upload file to S3 bucket"""
    if object_name is None:
        object_name = os.path.basename(file_path)
    
    s3_client = boto3.client('s3')
    try:
        s3_client.upload_file(file_path, bucket_name, object_name)
        print(f"✓ Uploaded {file_path} to s3://{bucket_name}/{object_name}")
        return True
    except FileNotFoundError:
        print(f"✗ File not found: {file_path}")
        return False
    except NoCredentialsError:
        print(f"✗ AWS credentials not available")
        return False
    except Exception as e:
        print(f"✗ Error uploading to S3: {e}")
        return False

def validate_auction_data(df, stage=""):
    """
    Validate that data represents individual auctions, not aggregated variants.
    This is CRITICAL to prevent the aggregation bug from recurring.
    """
    print(f"\n{'='*80}")
    print(f"DATA VALIDATION: {stage}")
    print(f"{'='*80}")
    
    issues = []
    
    # Check 1: No decimal bids (smoking gun for aggregation)
    if 'bids_numeric' in df.columns:
        decimal_bids = df[df['bids_numeric'] % 1 != 0]
        if len(decimal_bids) > 0:
            issues.append(f"❌ CRITICAL: {len(decimal_bids)} rows have decimal bids!")
            issues.append("   This indicates data has been aggregated incorrectly.")
            print(f"   Sample decimal bids: {decimal_bids['bids_numeric'].head().tolist()}")
        else:
            print(f"✓ All bids are integers ({len(df)} rows)")
    
    # Check 2: High-value sales exist
    if 'sale_amount_numeric' in df.columns:
        max_price = df['sale_amount_numeric'].max()
        over_1m = len(df[df['sale_amount_numeric'] > 1000000])
        over_500k = len(df[df['sale_amount_numeric'] > 500000])
        
        print(f"✓ Price ceiling: ${max_price:,.0f}")
        print(f"✓ Sales over $1M: {over_1m}")
        print(f"✓ Sales over $500K: {over_500k}")
        
        if max_price < 500000:
            issues.append(f"⚠️  Warning: Maximum sale price is only ${max_price:,.0f}")
            issues.append("   Expected some sales over $1M for high-value cars")
    
    # Check 3: Realistic distribution
    if 'sale_amount_numeric' in df.columns:
        p99 = df['sale_amount_numeric'].quantile(0.99)
        print(f"✓ 99th percentile price: ${p99:,.0f}")
        
        if p99 < 300000:
            issues.append(f"⚠️  Warning: 99th percentile is only ${p99:,.0f}")
    
    # Check 4: Individual auction structure
    print(f"✓ Total records: {len(df):,}")
    
    if len(issues) == 0:
        print(f"\n{'✅ ALL VALIDATION CHECKS PASSED'}")
        print(f"{'='*80}\n")
        return True
    else:
        print(f"\n{'🚨 VALIDATION ISSUES DETECTED:'}")
        for issue in issues:
            print(issue)
        print(f"{'='*80}\n")
        return False

# ============================================================================
# DATA LOADING AND CLEANING
# ============================================================================

def load_bat_data(file_path):
    """Load and clean BAT (Bring a Trailer) auction data"""
    print(f"\n{'='*80}")
    print("LOADING BAT DATA")
    print(f"{'='*80}")
    
    try:
        df = pd.read_csv(file_path)
        print(f"✓ Loaded {len(df):,} BAT auctions from {file_path}")
        
        # Clean and standardize columns
        df['source'] = 'BAT'
        
        # Numeric conversions
        df['bids_numeric'] = df['bids'].apply(extract_numeric)
        df['comments_numeric'] = df['comments'].apply(extract_numeric)
        df['views_numeric'] = df['views'].apply(extract_numeric)
        df['sale_amount_numeric'] = df['sale_amount'].apply(extract_numeric)
        
        # Year handling
        if 'year' in df.columns:
            df['year'] = pd.to_numeric(df['year'], errors='coerce')
        
        # Current year for age calculation
        current_year = datetime.now().year
        df['car_age'] = current_year - df['year']
        
        print(f"✓ Cleaned BAT data: {len(df):,} auctions")
        return df
        
    except FileNotFoundError:
        print(f"✗ BAT file not found: {file_path}")
        return pd.DataFrame()
    except Exception as e:
        print(f"✗ Error loading BAT data: {e}")
        return pd.DataFrame()

def load_cnb_data(file_path):
    """Load and clean CNB (Cars and Bids) auction data"""
    print(f"\n{'='*80}")
    print("LOADING CNB DATA")
    print(f"{'='*80}")
    
    try:
        df = pd.read_csv(file_path)
        print(f"✓ Loaded {len(df):,} CNB auctions from {file_path}")
        
        # Clean and standardize columns
        df['source'] = 'CNB'
        
        # CNB price cleaning with comma-counting method
        df['sale_amount_numeric'] = df['sale_amount'].apply(clean_cnb_price)
        
        # Other numeric conversions
        df['bids_numeric'] = df.get('bids', pd.Series()).apply(extract_numeric)
        df['comments_numeric'] = df.get('comments', pd.Series()).apply(extract_numeric)
        df['views_numeric'] = df.get('views', pd.Series()).apply(extract_numeric)
        
        # Year handling
        if 'year' in df.columns:
            df['year'] = pd.to_numeric(df['year'], errors='coerce')
        
        # Current year for age calculation
        current_year = datetime.now().year
        df['car_age'] = current_year - df['year']
        
        # Show CNB price statistics
        print(f"\nCNB Price Statistics:")
        print(f"  Mean: ${df['sale_amount_numeric'].mean():,.0f}")
        print(f"  Median: ${df['sale_amount_numeric'].median():,.0f}")
        print(f"  Max: ${df['sale_amount_numeric'].max():,.0f}")
        print(f"  Over $1M: {len(df[df['sale_amount_numeric'] > 1000000])}")
        
        print(f"✓ Cleaned CNB data: {len(df):,} auctions")
        return df
        
    except FileNotFoundError:
        print(f"✗ CNB file not found: {file_path}")
        return pd.DataFrame()
    except Exception as e:
        print(f"✗ Error loading CNB data: {e}")
        return pd.DataFrame()

# ============================================================================
# MII CALCULATION (INDIVIDUAL AUCTION LEVEL)
# ============================================================================

def calculate_mii_individual(df):
    """
    Calculate MII Index for INDIVIDUAL auctions.
    
    CRITICAL: This function does NOT aggregate data.
    Each row in the output represents ONE auction event.
    """
    print(f"\n{'='*80}")
    print("CALCULATING MII SCORES (INDIVIDUAL AUCTIONS)")
    print(f"{'='*80}")
    
    # Validate input
    if not validate_auction_data(df, "Input Data Check"):
        print("⚠️  Proceeding despite validation warnings...")
    
    # Make a copy to avoid modifying original
    mii_df = df.copy()
    
    # Required columns for MII calculation
    required_cols = ['bids_numeric', 'views_numeric', 'sale_amount_numeric', 'car_age']
    
    # Filter to rows with required data
    mii_df = mii_df.dropna(subset=required_cols)
    print(f"✓ Filtered to {len(mii_df):,} auctions with complete data")
    
    # Calculate z-scores for normalization
    # Using robust method with winsorization to handle outliers
    print("\nCalculating z-scores...")
    
    for col in ['bids_numeric', 'views_numeric', 'comments_numeric', 'sale_amount_numeric', 'car_age']:
        if col in mii_df.columns:
            # Winsorize at 1st and 99th percentiles to handle outliers
            p01 = mii_df[col].quantile(0.01)
            p99 = mii_df[col].quantile(0.99)
            
            # Clip extreme values
            winsorized = mii_df[col].clip(lower=p01, upper=p99)
            
            # Calculate z-score
            mean_val = winsorized.mean()
            std_val = winsorized.std()
            
            if std_val > 0:
                mii_df[f'z_{col}'] = (winsorized - mean_val) / std_val
            else:
                mii_df[f'z_{col}'] = 0
            
            print(f"  {col:<25} : μ={mean_val:>10,.1f}, σ={std_val:>10,.1f}")
    
    # Calculate MII Score using weighted combination
    print("\nCalculating MII Score...")
    mii_df['MII_Score'] = 0
    
    for metric, weight in MII_WEIGHTS.items():
        col_name = f'z_{metric}_numeric' if metric != 'car_age' else 'z_car_age'
        if col_name in mii_df.columns:
            mii_df['MII_Score'] += mii_df[col_name] * weight
            print(f"  {metric:<15} weight: {weight:>6.2f}")
    
    # Normalize to 0-100 scale
    min_score = mii_df['MII_Score'].min()
    max_score = mii_df['MII_Score'].max()
    
    if max_score > min_score:
        mii_df['MII_Index'] = ((mii_df['MII_Score'] - min_score) / (max_score - min_score)) * 100
    else:
        mii_df['MII_Index'] = 50  # Default if all scores are the same
    
    # Add tier classification
    mii_df['MII_Tier'] = pd.cut(
        mii_df['MII_Index'],
        bins=[-np.inf, 20, 40, 60, 80, np.inf],
        labels=['D', 'C', 'B', 'A', 'S']
    )
    
    # Add variant classification (for grouping, NOT aggregation)
    mii_df['variant_id'] = mii_df.apply(classify_variant, axis=1)
    
    print(f"\n✓ MII Index calculated for {len(mii_df):,} individual auctions")
    print(f"\nMII Index Distribution:")
    print(f"  Mean:   {mii_df['MII_Index'].mean():>6.2f}")
    print(f"  Median: {mii_df['MII_Index'].median():>6.2f}")
    print(f"  Std:    {mii_df['MII_Index'].std():>6.2f}")
    print(f"  Min:    {mii_df['MII_Index'].min():>6.2f}")
    print(f"  Max:    {mii_df['MII_Index'].max():>6.2f}")
    
    # Show tier distribution
    print(f"\nTier Distribution:")
    tier_counts = mii_df['MII_Tier'].value_counts().sort_index(ascending=False)
    for tier, count in tier_counts.items():
        pct = count / len(mii_df) * 100
        print(f"  {tier}-Tier: {count:>6} auctions ({pct:>5.1f}%)")
    
    # Validate output
    validate_auction_data(mii_df, "Output Data Check")
    
    return mii_df

# ============================================================================
# OPTIONAL AGGREGATION FOR SUMMARIES (SEPARATE FROM MII)
# ============================================================================

def create_model_summary(mii_df):
    """
    Create model-level summary statistics.
    This is SEPARATE from MII calculation - just for reporting.
    """
    print(f"\n{'='*80}")
    print("CREATING MODEL SUMMARY (OPTIONAL AGGREGATION)")
    print(f"{'='*80}")
    print("NOTE: This is for summary reporting only, not for MII calculation")
    
    summary = mii_df.groupby(['make', 'model', 'variant_id']).agg({
        'MII_Index': ['mean', 'median', 'std', 'count'],
        'sale_amount_numeric': ['mean', 'median', 'max', 'min'],
        'bids_numeric': ['mean', 'sum'],
        'views_numeric': ['mean', 'sum'],
        'comments_numeric': ['mean', 'sum'],
        'year': ['min', 'max']
    }).reset_index()
    
    # Flatten column names
    summary.columns = ['_'.join(col).strip('_') if col[1] else col[0] 
                       for col in summary.columns.values]
    
    # Sort by average MII
    summary = summary.sort_values('MII_Index_mean', ascending=False)
    
    print(f"✓ Created summary for {len(summary):,} model variants")
    print(f"\nTop 10 Models by Average MII:")
    for idx, row in summary.head(10).iterrows():
        print(f"  {row['make']} {row['model'][:40]:<40} | Avg MII: {row['MII_Index_mean']:>6.2f} | Auctions: {row['MII_Index_count']:>4.0f}")
    
    return summary

def create_manufacturer_summary(mii_df):
    """
    Create manufacturer-level summary statistics.
    This is SEPARATE from MII calculation - just for reporting.
    """
    print(f"\n{'='*80}")
    print("CREATING MANUFACTURER SUMMARY (OPTIONAL AGGREGATION)")
    print(f"{'='*80}")
    print("NOTE: This is for summary reporting only, not for MII calculation")
    
    summary = mii_df.groupby('make').agg({
        'MII_Index': ['mean', 'median', 'std', 'count'],
        'sale_amount_numeric': ['mean', 'median', 'max', 'sum'],
        'bids_numeric': ['mean', 'sum'],
        'views_numeric': ['mean', 'sum'],
        'comments_numeric': ['mean', 'sum']
    }).reset_index()
    
    # Flatten column names
    summary.columns = ['_'.join(col).strip('_') if col[1] else col[0] 
                       for col in summary.columns.values]
    
    # Sort by average MII
    summary = summary.sort_values('MII_Index_mean', ascending=False)
    
    print(f"✓ Created summary for {len(summary):,} manufacturers")
    print(f"\nTop 10 Manufacturers by Average MII:")
    for idx, row in summary.head(10).iterrows():
        print(f"  {row['make']:<20} | Avg MII: {row['MII_Index_mean']:>6.2f} | Auctions: {row['MII_Index_count']:>5.0f}")
    
    return summary

# ============================================================================
# MAIN EXECUTION
# ============================================================================

def main():
    """Main execution function"""
    print(f"\n{'#'*80}")
    print("# ENHANCED MII CALCULATOR - FIXED VERSION")
    print("# Individual Auction Analysis (No Aggregation)")
    print(f"{'#'*80}")
    print(f"Execution Time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    
    # ========================================================================
    # STEP 1: Load Data
    # ========================================================================
    
    bat_df = load_bat_data(BAT_FILE)
    cnb_df = load_cnb_data(CNB_FILE)
    
    # Combine datasets
    if not bat_df.empty and not cnb_df.empty:
        combined_df = pd.concat([bat_df, cnb_df], ignore_index=True)
    elif not bat_df.empty:
        combined_df = bat_df
    elif not cnb_df.empty:
        combined_df = cnb_df
    else:
        print("✗ No data loaded! Exiting.")
        return
    
    print(f"\n{'='*80}")
    print(f"COMBINED DATASET: {len(combined_df):,} total auctions")
    print(f"{'='*80}")
    
    # ========================================================================
    # STEP 2: Calculate MII for Individual Auctions
    # ========================================================================
    
    mii_df = calculate_mii_individual(combined_df)
    
    # ========================================================================
    # STEP 3: Save Individual Auction Results
    # ========================================================================
    
    print(f"\n{'='*80}")
    print("SAVING RESULTS")
    print(f"{'='*80}")
    
    # Select columns for output
    output_cols = [
        'source', 'make', 'model', 'year', 'variant_id',
        'sale_amount_numeric', 'bids_numeric', 'views_numeric', 'comments_numeric',
        'car_age', 'MII_Score', 'MII_Index', 'MII_Tier'
    ]
    
    # Add any additional columns that exist
    for col in ['auction_url', 'sale_date', 'sale_type', 'location']:
        if col in mii_df.columns:
            output_cols.append(col)
    
    # Save individual auctions
    output_df = mii_df[output_cols].copy()
    output_df.to_csv(OUTPUT_INDIVIDUAL, index=False)
    print(f"✓ Saved individual auction results: {OUTPUT_INDIVIDUAL}")
    print(f"  Records: {len(output_df):,}")
    
    # ========================================================================
    # STEP 4: Create Optional Summary Reports
    # ========================================================================
    
    # Model summary
    model_summary = create_model_summary(mii_df)
    model_summary.to_csv(OUTPUT_MODEL_SUMMARY, index=False)
    print(f"✓ Saved model summary: {OUTPUT_MODEL_SUMMARY}")
    
    # Manufacturer summary
    mfr_summary = create_manufacturer_summary(mii_df)
    mfr_summary.to_csv(OUTPUT_MANUFACTURER_SUMMARY, index=False)
    print(f"✓ Saved manufacturer summary: {OUTPUT_MANUFACTURER_SUMMARY}")
    
    # ========================================================================
    # STEP 5: Upload to S3 (if configured)
    # ========================================================================
    
    if UPLOAD_TO_S3:
        print(f"\n{'='*80}")
        print("UPLOADING TO S3")
        print(f"{'='*80}")
        
        upload_to_s3(OUTPUT_INDIVIDUAL, S3_BUCKET)
        upload_to_s3(OUTPUT_MODEL_SUMMARY, S3_BUCKET)
        upload_to_s3(OUTPUT_MANUFACTURER_SUMMARY, S3_BUCKET)
    
    # ========================================================================
    # STEP 6: Show Top Performers
    # ========================================================================
    
    print(f"\n{'='*80}")
    print("TOP 20 INDIVIDUAL AUCTIONS BY MII INDEX")
    print(f"{'='*80}")
    
    top_20 = mii_df.nlargest(20, 'MII_Index')[['make', 'model', 'year', 'sale_amount_numeric', 'MII_Index', 'MII_Tier']]
    
    for idx, row in top_20.iterrows():
        year_str = f"({int(row['year'])})" if pd.notna(row['year']) else "(Year?)"
        price_str = f"${row['sale_amount_numeric']:,.0f}" if pd.notna(row['sale_amount_numeric']) else "N/A"
        print(f"  {row['MII_Tier']}-Tier | MII {row['MII_Index']:>6.2f} | {year_str} {row['make']} {row['model'][:40]:<40} | {price_str}")
    
    print(f"\n{'='*80}")
    print("✅ MII CALCULATION COMPLETE!")
    print(f"{'='*80}")
    print(f"\nOutput Files:")
    print(f"  1. {OUTPUT_INDIVIDUAL} - Individual auction MII scores")
    print(f"  2. {OUTPUT_MODEL_SUMMARY} - Model-level summary statistics")
    print(f"  3. {OUTPUT_MANUFACTURER_SUMMARY} - Manufacturer-level summary")
    print(f"\nAll files represent individual auction data - no aggregation!")

if __name__ == "__main__":
    main()
