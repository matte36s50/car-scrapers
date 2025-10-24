#!/usr/bin/env python3
"""
Enhanced MII Calculator - FIXED VERSION
Calculates Market Interest Index for INDIVIDUAL auctions (not aggregated)

Key Changes:
1. NO aggregation before MII calculation
2. Price cleaning uses comma-counting method
3. MII calculated per auction
4. Outputs individual auction results
5. Optional variant summary with MAX prices

Date: October 24, 2025
"""

import pandas as pd
import numpy as np
import re
from typing import Optional
import sys
import os
from datetime import datetime

# ============================================================================
# PRICE CLEANING - FIXED VERSION (Comma-Counting Method)
# ============================================================================

def clean_sale_amount(sale_text: str, sale_type: Optional[str] = None) -> Optional[int]:
    """
    Clean and validate sale amount using comma-counting method.
    
    Fixes CNB scraping issues where extra digits are appended:
    - Good: $95,000
    - Bad: $95,00011 (has 5 digits after comma instead of 3)
    
    Solution: Look at digits after last comma, keep only first 3.
    """
    if not sale_text or pd.isna(sale_text):
        return None
    
    # Handle reserve not met
    if sale_type and 'reserve' in str(sale_type).lower():
        return None
    
    # Convert to string and clean
    sale_str = str(sale_text).replace('$', '').replace(' ', '').strip()
    
    # Handle various formats
    if not sale_str or sale_str == '0':
        return None
    
    # Find the last comma
    last_comma_pos = sale_str.rfind(',')
    
    if last_comma_pos == -1:
        # No comma - just parse as integer
        try:
            amount = int(float(sale_str))
            return amount if 100 <= amount <= 10000000 else None
        except:
            return None
    
    # Get everything after the last comma
    after_comma = sale_str[last_comma_pos + 1:]
    
    # Count digits after comma
    digit_count = len(after_comma)
    
    # Standard US format should have exactly 3 digits after final comma
    if digit_count > 3:
        # We have extra digits - keep only the first 3
        corrected = sale_str[:last_comma_pos] + sale_str[last_comma_pos:last_comma_pos + 4]
        sale_str = corrected
        print(f"  🔧 Price correction: {sale_text} → {corrected}")
    
    # Remove all commas and parse
    try:
        amount = int(sale_str.replace(',', ''))
        
        # Sanity check: reasonable range
        if amount < 100 or amount > 10000000:
            print(f"  ⚠️ Price out of range: ${amount:,}")
            return None
            
        return amount
    except:
        return None


# ============================================================================
# DATA VALIDATION
# ============================================================================

def validate_individual_auction_data(df):
    """Ensure data represents individual auctions, not aggregates"""
    
    print("\n" + "="*80)
    print("DATA VALIDATION CHECKS")
    print("="*80)
    
    checks_passed = []
    
    # Check 1: No decimal bids
    if 'bids_numeric' in df.columns:
        has_decimal_bids = (df['bids_numeric'] % 1 != 0).any()
        checks_passed.append(not has_decimal_bids)
        print(f"\n1. Decimal bids check: {'❌ FAIL' if has_decimal_bids else '✅ PASS'}")
        if has_decimal_bids:
            decimal_count = (df['bids_numeric'] % 1 != 0).sum()
            print(f"   WARNING: {decimal_count:,} decimal bid values found!")
            print(f"   Example: {df[df['bids_numeric'] % 1 != 0]['bids_numeric'].iloc[0]}")
        else:
            print(f"   All {len(df):,} auctions have integer bids")
    
    # Check 2: High-value sales exist
    if 'sale_amount_numeric' in df.columns:
        max_sale = df['sale_amount_numeric'].max()
        has_high_values = max_sale > 300000
        checks_passed.append(has_high_values)
        print(f"\n2. High-value sales check: {'✅ PASS' if has_high_values else '⚠️ WARNING'}")
        print(f"   Max sale: ${max_sale:,.0f}")
        
        # Check for million+ sales
        million_plus = len(df[df['sale_amount_numeric'] > 1000000])
        if million_plus > 0:
            print(f"   Million+ sales: {million_plus:,} auctions")
            checks_passed.append(True)
        else:
            print(f"   Million+ sales: 0 (may be expected depending on market)")
    
    # Check 3: Individual auction identifiers
    if 'auction_url' in df.columns:
        has_urls = df['auction_url'].notna().sum()
        all_have_urls = has_urls == len(df)
        checks_passed.append(all_have_urls)
        print(f"\n3. Individual auction URLs: {'✅ PASS' if all_have_urls else '❌ FAIL'}")
        print(f"   URLs present: {has_urls:,} / {len(df):,}")
    
    # Check 4: Reasonable price distribution
    if 'sale_amount_numeric' in df.columns:
        price_stats = df['sale_amount_numeric'].describe()
        median = price_stats['50%']
        mean = price_stats['mean']
        
        reasonable_range = 10000 <= median <= 500000 and 10000 <= mean <= 1000000
        checks_passed.append(reasonable_range)
        print(f"\n4. Price distribution check: {'✅ PASS' if reasonable_range else '⚠️ WARNING'}")
        print(f"   Median: ${median:,.0f}")
        print(f"   Mean: ${mean:,.0f}")
    
    # Summary
    all_passed = all(checks_passed)
    print(f"\n{'✅ ALL CHECKS PASSED' if all_passed else '⚠️ SOME CHECKS FAILED'}")
    print(f"   {sum(checks_passed)}/{len(checks_passed)} checks passed")
    print("="*80 + "\n")
    
    return all_passed


# ============================================================================
# VARIANT CLASSIFICATION
# ============================================================================

def classify_variant(row):
    """Create variant ID from make, model, and generation"""
    make = str(row.get('make', '')).strip().upper()
    model = str(row.get('model', '')).strip().upper()
    
    # Extract generation if available
    generation = 'GEN_OTHER'
    
    # Common generation patterns
    gen_patterns = [
        r'\b(991|992|997|996|993|964|930|993\.2)\b',  # Porsche 911 generations
        r'\b(E\d{2}|F\d{2}|G\d{2})\b',                # BMW generations
        r'\b(W\d{3}|C\d{3}|R\d{3})\b',                # Mercedes generations
        r'\b(MK\d+|MARK\s*\d+|GEN\s*\d+)\b',         # General generations
        r'\b(I{1,3}|IV|V|VI|VII|VIII)\b'              # Roman numerals
    ]
    
    for pattern in gen_patterns:
        match = re.search(pattern, model, re.IGNORECASE)
        if match:
            generation = match.group(0).upper()
            break
    
    return f"{make}_{model}_{generation}"


# ============================================================================
# MII CALCULATION
# ============================================================================

def calculate_mii_for_individual_auctions(df):
    """
    Calculate MII Index for INDIVIDUAL auctions (not aggregated).
    
    This is the CORRECT approach - each row represents one auction event.
    """
    print("\n" + "="*80)
    print("CALCULATING MII FOR INDIVIDUAL AUCTIONS")
    print("="*80 + "\n")
    
    # Ensure we have the required columns
    required_cols = ['sale_amount_numeric', 'bids_numeric', 'views_numeric']
    missing_cols = [col for col in required_cols if col not in df.columns]
    
    if missing_cols:
        print(f"❌ Missing required columns: {missing_cols}")
        return df
    
    # Filter to completed auctions only (those with sale amounts)
    df_complete = df[df['sale_amount_numeric'].notna()].copy()
    print(f"Auctions with sale amounts: {len(df_complete):,} / {len(df):,}")
    
    # Add variant classification (as a column, not aggregating)
    if 'variant_id' not in df_complete.columns:
        print("Classifying variants...")
        df_complete['variant_id'] = df_complete.apply(classify_variant, axis=1)
    
    # Calculate Z-scores for normalization
    print("Calculating Z-scores...")
    metrics = ['views_numeric', 'bids_numeric', 'sale_amount_numeric']
    
    # Optional: Add comments if available
    if 'comments_numeric' in df_complete.columns:
        metrics.append('comments_numeric')
    
    for col in metrics:
        mean = df_complete[col].mean()
        std = df_complete[col].std()
        
        if std > 0:
            df_complete[f'z_{col}'] = (df_complete[col] - mean) / std
        else:
            df_complete[f'z_{col}'] = 0
        
        print(f"  {col}: μ={mean:.2f}, σ={std:.2f}")
    
    # Calculate car age Z-score (newer is better, so negate)
    if 'year' in df_complete.columns:
        current_year = datetime.now().year
        df_complete['car_age'] = current_year - df_complete['year']
        
        mean_age = df_complete['car_age'].mean()
        std_age = df_complete['car_age'].std()
        
        if std_age > 0:
            df_complete['z_car_age'] = -(df_complete['car_age'] - mean_age) / std_age
        else:
            df_complete['z_car_age'] = 0
        
        print(f"  car_age: μ={mean_age:.2f}, σ={std_age:.2f} (negated for scoring)")
    
    # Define weights
    weights = {
        'z_bids_numeric': 0.25,
        'z_views_numeric': 0.20,
        'z_sale_amount_numeric': 0.30,
    }
    
    if 'z_comments_numeric' in df_complete.columns:
        weights['z_comments_numeric'] = 0.15
        weights['z_car_age'] = 0.10
    else:
        weights['z_car_age'] = 0.25
    
    # Ensure weights sum to 1.0
    total_weight = sum(weights.values())
    weights = {k: v/total_weight for k, v in weights.items()}
    
    print(f"\nWeights (normalized):")
    for metric, weight in weights.items():
        print(f"  {metric}: {weight:.3f}")
    
    # Calculate MII Score
    print("\nCalculating MII Score...")
    df_complete['MII_Score'] = 0
    
    for col, weight in weights.items():
        if col in df_complete.columns:
            df_complete['MII_Score'] += df_complete[col] * weight
    
    # Normalize to 0-100 scale
    min_score = df_complete['MII_Score'].min()
    max_score = df_complete['MII_Score'].max()
    
    if max_score > min_score:
        df_complete['MII_Index'] = ((df_complete['MII_Score'] - min_score) / 
                                    (max_score - min_score)) * 100
    else:
        df_complete['MII_Index'] = 50
    
    print(f"\n✓ MII Score calculated for {len(df_complete):,} individual auctions")
    print(f"\nMII Index Distribution:")
    print(f"  Mean: {df_complete['MII_Index'].mean():.2f}")
    print(f"  Median: {df_complete['MII_Index'].median():.2f}")
    print(f"  Min: {df_complete['MII_Index'].min():.2f}")
    print(f"  Max: {df_complete['MII_Index'].max():.2f}")
    print(f"  Std: {df_complete['MII_Index'].std():.2f}")
    
    # Show top 20 individual auctions
    print(f"\n{'='*80}")
    print("TOP 20 INDIVIDUAL AUCTIONS BY MII INDEX")
    print(f"{'='*80}")
    
    top_auctions = df_complete.nlargest(20, 'MII_Index')
    
    for i, (idx, row) in enumerate(top_auctions.iterrows(), 1):
        make = row.get('make', 'Unknown')
        model = row.get('model', 'Unknown')
        year = row.get('year', 0)
        price = row.get('sale_amount_numeric', 0)
        bids = row.get('bids_numeric', 0)
        views = row.get('views_numeric', 0)
        mii = row.get('MII_Index', 0)
        
        year_str = f"({int(year)})" if year > 0 else ""
        print(f"{i:2d}. {make} {model:<35s} {year_str:<6s} | "
              f"${price:>10,.0f} | {int(bids):>3d} bids | "
              f"{int(views):>7,d} views | MII: {mii:>6.2f}")
    
    print(f"{'='*80}\n")
    
    return df_complete


# ============================================================================
# VARIANT SUMMARY (OPTIONAL)
# ============================================================================

def create_variant_summary(df):
    """
    Create variant-level summary with PROPER aggregation.
    
    Key: Use MAX for sale_amount (not MEAN) to show top prices.
    """
    print("\n" + "="*80)
    print("CREATING VARIANT-LEVEL SUMMARY (OPTIONAL)")
    print("="*80 + "\n")
    
    if 'variant_id' not in df.columns:
        print("❌ variant_id column not found")
        return None
    
    # Aggregate properly
    agg_dict = {
        'MII_Index': 'mean',                  # Average MII performance
        'sale_amount_numeric': 'max',         # HIGHEST sale (not average!)
        'bids_numeric': 'mean',               # Average engagement
        'views_numeric': 'mean',
        'make': 'first',                      # Keep metadata
        'model': 'first',
    }
    
    # Optional columns
    if 'comments_numeric' in df.columns:
        agg_dict['comments_numeric'] = 'mean'
    if 'year' in df.columns:
        agg_dict['year'] = 'mean'
    if 'auction_url' in df.columns:
        agg_dict['auction_url'] = 'count'
    
    variant_summary = df.groupby('variant_id').agg(agg_dict)
    
    # Rename count column
    if 'auction_url' in variant_summary.columns:
        variant_summary = variant_summary.rename(columns={'auction_url': 'total_auctions'})
    
    # Reset index to make variant_id a column
    variant_summary = variant_summary.reset_index()
    
    print(f"✓ Created summary for {len(variant_summary):,} variants")
    print(f"\nSummary statistics:")
    print(f"  Average auctions per variant: {variant_summary['total_auctions'].mean():.1f}")
    print(f"  Max auctions per variant: {variant_summary['total_auctions'].max():.0f}")
    
    # Show top 10 variants
    print(f"\nTop 10 Variants by Average MII:")
    top_variants = variant_summary.nlargest(10, 'MII_Index')
    
    for i, (idx, row) in enumerate(top_variants.iterrows(), 1):
        make = row.get('make', 'Unknown')
        model = row.get('model', 'Unknown')
        max_price = row.get('sale_amount_numeric', 0)
        auctions = row.get('total_auctions', 0)
        avg_mii = row.get('MII_Index', 0)
        
        print(f"{i:2d}. {make} {model:<35s} | "
              f"Max: ${max_price:>10,.0f} | "
              f"{int(auctions):>3d} auctions | "
              f"Avg MII: {avg_mii:>6.2f}")
    
    print(f"{'='*80}\n")
    
    return variant_summary


# ============================================================================
# MAIN PIPELINE
# ============================================================================

def main():
    """Main pipeline for MII calculation"""
    
    print("\n" + "="*80)
    print("ENHANCED MII CALCULATOR - FIXED VERSION")
    print("Calculating Market Interest Index for INDIVIDUAL auctions")
    print("="*80 + "\n")
    
    # 1. Load raw data
    input_file = 'cnb_sitemap_full_cleaned.csv'
    
    if not os.path.exists(input_file):
        print(f"❌ Input file not found: {input_file}")
        print("Please provide the raw CNB scraper output CSV")
        sys.exit(1)
    
    print(f"Loading data from: {input_file}")
    df = pd.read_csv(input_file, low_memory=False)
    print(f"✓ Loaded {len(df):,} rows\n")
    
    # 2. Clean sale amounts
    print("💰 Cleaning sale amounts...")
    df['sale_amount_numeric'] = df.apply(
        lambda row: clean_sale_amount(row.get('sale_amount'), row.get('sale_type')),
        axis=1
    )
    
    valid_sales = df['sale_amount_numeric'].notna().sum()
    print(f"✓ {valid_sales:,} valid sale amounts ({valid_sales/len(df)*100:.1f}%)\n")
    
    # 3. Convert other numeric fields
    print("Converting numeric fields...")
    
    if 'bids' in df.columns:
        df['bids_numeric'] = pd.to_numeric(
            df['bids'].astype(str).str.extract(r'(\d+)')[0],
            errors='coerce'
        )
    
    if 'views' in df.columns:
        df['views_numeric'] = pd.to_numeric(
            df['views'].astype(str).str.replace(',', ''),
            errors='coerce'
        )
    
    if 'comments' in df.columns:
        df['comments_numeric'] = pd.to_numeric(
            df['comments'].astype(str).str.extract(r'(\d+)')[0],
            errors='coerce'
        )
    
    if 'year' in df.columns:
        df['year'] = pd.to_numeric(df['year'], errors='coerce')
    
    print("✓ Numeric conversions complete\n")
    
    # 4. Validate data
    validate_individual_auction_data(df)
    
    # 5. Calculate MII for individual auctions
    df_with_mii = calculate_mii_for_individual_auctions(df)
    
    # 6. Save individual auction results
    output_individual = 'mii_individual_auctions.csv'
    df_with_mii.to_csv(output_individual, index=False)
    print(f"\n✅ Individual auction MII data saved to: {output_individual}")
    print(f"   Total records: {len(df_with_mii):,}")
    print(f"   File size: {os.path.getsize(output_individual) / 1024 / 1024:.2f} MB")
    
    # 7. Create variant summary (optional)
    variant_summary = create_variant_summary(df_with_mii)
    
    if variant_summary is not None:
        output_summary = 'mii_variant_summary.csv'
        variant_summary.to_csv(output_summary, index=False)
        print(f"\n✅ Variant summary saved to: {output_summary}")
        print(f"   Total variants: {len(variant_summary):,}")
        print(f"   File size: {os.path.getsize(output_summary) / 1024 / 1024:.2f} MB")
    
    # 8. Final summary
    print("\n" + "="*80)
    print("MII CALCULATION COMPLETE")
    print("="*80)
    print(f"\n✅ Output files:")
    print(f"   1. {output_individual} - Individual auction MII scores")
    if variant_summary is not None:
        print(f"   2. {output_summary} - Variant-level summary")
    print(f"\n✅ Key statistics:")
    print(f"   Total auctions processed: {len(df_with_mii):,}")
    print(f"   Auctions over $1M: {len(df_with_mii[df_with_mii['sale_amount_numeric'] > 1000000]):,}")
    print(f"   MII range: {df_with_mii['MII_Index'].min():.2f} - {df_with_mii['MII_Index'].max():.2f}")
    print("="*80 + "\n")


if __name__ == "__main__":
    main()
