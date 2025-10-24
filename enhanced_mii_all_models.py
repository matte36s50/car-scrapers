# enhanced_mii_all_models.py
# ---------------------------------------------------------------
# Market Interest Index (MII) pipeline v2.0 with:
# - Fixed comma-counting price cleaning (no false positives!)
# - Variant splitting (model_family + generation -> variant_id)
# - Era cohorts
# - Winsorizing per quarter + cohort (2.5/97.5)
# - Robust z-scores (median/MAD) with ±4 cap
# - Weights aligned to report (IG slightly reduced)
# - Min-support/base-floor for % change charts
# - Optional EMA smoothing and S3 upload
# - Comprehensive analytics and insights
# ---------------------------------------------------------------

import os
import re
import datetime
import pandas as pd
import numpy as np
from typing import Dict, Optional, Tuple, List

# Optional S3 support
try:
    import boto3
    from botocore.exceptions import NoCredentialsError
    HAS_BOTO = True
except Exception:
    HAS_BOTO = False

# --------------------------- CONFIG ----------------------------
WINSOR_LO = 0.025
WINSOR_HI = 0.975
Z_CAP = 4.0
EMA_ALPHA = 0.7

# % change stability rules
MIN_SUPPORT_PER_QUARTER = 3
BASE_FLOOR_FOR_PCT = 8.0
SMALL_BASE_CAP = 200.0

# Quality filters
CNB_MIN_VIEWS = 50  # Filter CNB auctions below this view count

# Output
OUTPUT_PREFIX = "mii_results"
S3_BUCKET = "my-mii-reports"

# Instagram estimates metadata
IG_ESTIMATES_VERSION = "2025-Q4"
IG_ESTIMATES_LAST_UPDATED = "2025-10-19"

# ------------------------- UTILITIES ---------------------------
def upload_to_s3(file_name: str, bucket: str, object_name: Optional[str] = None) -> bool:
    """Upload a file to AWS S3 bucket."""
    if not HAS_BOTO:
        print("⚠️  boto3 not installed; skipping S3 upload.")
        print("   Install with: pip install boto3")
        return False
    
    s3 = boto3.client('s3')
    if object_name is None:
        object_name = os.path.basename(file_name)
    
    try:
        s3.upload_file(file_name, bucket, object_name)
        print(f"✅ Uploaded: s3://{bucket}/{object_name}")
        return True
    except NoCredentialsError:
        print("❌ AWS credentials not available")
        print("   Run 'aws configure' to set up credentials")
        return False
    except Exception as e:
        print(f"❌ Upload failed: {e}")
        return False

def extract_proper_model(model_text: str, make_text: Optional[str] = None) -> Optional[str]:
    """Extract clean model name from raw text."""
    if not model_text or pd.isna(model_text):
        return None
    
    model_str = str(model_text).strip()
    original_model = model_str

    # Strip leading year
    model_str = re.sub(r'^\d{4}\s+', '', model_str)

    # Remove common makes from model string
    common_makes = [
        'Mercedes-Benz', 'Mercedes', 'BMW', 'Porsche', 'Audi', 'Ferrari',
        'Lamborghini', 'McLaren', 'Chevrolet', 'Chevy', 'Ford', 'Dodge', 'Tesla',
        'Toyota', 'Honda', 'Nissan', 'Lexus', 'Acura', 'Infiniti', 'Jaguar',
        'Land Rover', 'Range Rover', 'Alfa Romeo', 'Maserati', 'Bentley',
        'Rolls-Royce', 'Aston Martin', 'Lotus', 'Bugatti', 'Mazda', 'Subaru'
    ]
    common_makes.sort(key=len, reverse=True)
    
    for mk in common_makes:
        pattern = rf'^{re.escape(mk)}[\s-]+'
        model_str = re.sub(pattern, '', model_str, flags=re.IGNORECASE)

    # Remove year ranges
    model_str = re.sub(r'\s*\(\d{4}-\d{4}\)\s*$', '', model_str)
    
    # Normalize whitespace
    model_str = re.sub(r'\s+', ' ', model_str).strip()

    # Special handling for AMG models
    if model_str.upper() == 'AMG':
        amg_match = re.search(r'([A-Z]+\d+[A-Z]*)\s*AMG', original_model, re.IGNORECASE)
        if amg_match:
            return f"{amg_match.group(1)} AMG"
        amg_model_match = re.search(r'AMG\s+([A-Z0-9]+(?:\s+[A-Z0-9]+)?)', original_model, re.IGNORECASE)
        if amg_model_match:
            return f"AMG {amg_model_match.group(1)}"
        return None

    return model_str if model_str else None

def clean_sale_amount(sale_text: str, sale_type: Optional[str] = None) -> Optional[int]:
    """
    Clean and validate sale amount using COMMA-COUNTING method.
    
    The smart approach: Count digits after the last comma.
    - Standard US format: $XX,XXX (exactly 3 digits after comma)
    - Broken format: $95,00011 (5 digits after comma - has 2 extra)
    
    This avoids false positives on prices ending in 7, 9, etc.
    
    Examples:
        "$95,00011" → 95000 (removes extra "11")
        "$47,777" → 47777 (preserves, already correct)
        "$89,999" → 89999 (preserves, already correct)
        "$50,000123" → 50000 (removes extra "123")
    
    Args:
        sale_text: Raw sale amount string
        sale_type: Optional sale type ('sold', 'reserve not met', etc.)
        
    Returns:
        Cleaned integer amount or None if invalid
    """
    # Handle reserve not met
    if sale_type and 'reserve' in str(sale_type).lower():
        return 0
    
    if not sale_text or pd.isna(sale_text):
        return None
    
    sale_str = str(sale_text).strip()
    
    # Check for comma and count digits after it
    comma_index = sale_str.rfind(',')
    
    if comma_index != -1:
        # Extract everything after the last comma, keep only digits
        after_comma = sale_str[comma_index + 1:]
        digits_after_comma = len(re.sub(r'[^\d]', '', after_comma))
        
        # Standard US format should have exactly 3 digits after comma
        if digits_after_comma > 3:
            # Extra digits detected
            extra_digits = digits_after_comma - 3
            
            # Parse the full number (remove all non-digits)
            amount = int(re.sub(r'[^\d]', '', sale_str))
            
            # Remove extra digits by dividing
            divisor = 10 ** extra_digits
            amount = amount // divisor
            
            # Validate range
            if amount < 100 or amount > 10_000_000:
                return None
            
            return amount
    
    # No comma or standard format - parse normally
    cleaned = re.sub(r'[^\d]', '', sale_str)
    if not cleaned:
        return None
    
    amount = int(cleaned)
    
    # Validate range
    if amount < 100 or amount > 10_000_000:
        return None
    
    return amount

def validate_quarter(quarter_str: str) -> bool:
    """Validate quarter string format and ensure it's not in the future."""
    if not quarter_str or quarter_str == 'NaT':
        return False
    
    try:
        year = int(quarter_str[:4])
        qnum = int(quarter_str[-1])
        
        now = datetime.datetime.now()
        current_year = now.year
        current_quarter = (now.month - 1) // 3 + 1
        
        if year > current_year:
            return False
        if year == current_year and qnum > current_quarter:
            return False
        if year < 1990:
            return False
        if qnum < 1 or qnum > 4:
            return False
        
        return True
    except:
        return False

def extract_year_from_row(row: pd.Series) -> Optional[int]:
    """Extract year from row, trying multiple sources."""
    current_year = datetime.datetime.now().year
    
    if 'year' in row and pd.notna(row['year']):
        try:
            y = int(row['year'])
            if 1900 <= y <= current_year + 2:
                return y
        except:
            pass
    
    if 'model_original' in row and pd.notna(row['model_original']):
        matches = re.findall(r'\b(19|20)\d{2}\b', str(row['model_original']))
        if matches:
            y = int(matches[0])
            if 1900 <= y <= current_year + 2:
                return y
    
    return None

def era_cohort(year: int) -> str:
    """Classify car into era cohort based on year."""
    if pd.isna(year):
        return 'Unknown'
    
    y = int(year)
    if y < 1970:
        return 'Pre-1970'
    if 1970 <= y < 2000:
        return '1970–1999'
    if 2000 <= y < 2015:
        return '2000–2014'
    return '2015+'

def clean_model(text: str) -> str:
    """Clean model text by removing 'Save' buttons and normalizing whitespace."""
    if not text:
        return ""
    
    # Remove newlines and normalize whitespace
    text = re.sub(r'\s+', ' ', text)
    
    # Remove 'Save' (case insensitive)
    text = re.sub(r'\s*save\s*', '', text, flags=re.IGNORECASE)
    
    return text.strip()

def get_instagram_estimates(all_keys: List[str]) -> Dict[str, int]:
    """Get Instagram follower estimates for models/variants."""
    try:
        ig_data = pd.read_csv('instagram_estimates_latest.csv')
        print(f"✅ Loaded Instagram estimates from file (version: {IG_ESTIMATES_VERSION})")
        return dict(zip(ig_data['key'].str.lower(), ig_data['followers']))
    except:
        print(f"ℹ️  Using baseline Instagram estimates (last updated: {IG_ESTIMATES_LAST_UPDATED})")
    
    # Calibrated baseline estimates
    known = {
        "bmw": 650000, "m3": 280000, "e30": 18000, "e36": 15000, "e46": 42000,
        "e92": 38000, "f80": 45000, "g80": 35000, "2002": 12000, "z8": 4500,
        "m5": 140000, "m4": 35000, "z4": 22000, "m2": 32000, "1m": 15000,
        "mercedes": 480000, "190e": 18000, "c63": 85000, "c63 amg": 85000,
        "e63": 65000, "e63 amg": 65000, "s63": 55000, "s63 amg": 55000,
        "amg gt": 75000, "g63": 95000, "g63 amg": 95000, "sl63": 42000,
        "g-class": 55000, "sl": 18000, "cls63": 35000, "e55": 28000,
        "c55": 22000, "sl65": 18000, "sl55": 15000, "clk63": 22000,
        "porsche": 450000, "911": 150000, "turbo": 45000, "gt3": 65000,
        "gt2": 42000, "boxster": 28000, "cayman": 32000, "carrera": 85000,
        "taycan": 35000, "cayenne": 38000, "panamera": 28000,
        "ferrari": 320000, "lamborghini": 280000, "mclaren": 85000,
        "aventador": 75000, "huracan": 85000, "458": 42000, "488": 55000,
        "f430": 28000, "360": 22000, "720s": 45000, "650s": 32000,
        "toyota": 180000, "supra": 55000, "nissan": 120000, "gtr": 38000,
        "gt-r": 38000, "skyline": 35000, "honda": 160000, "s2000": 35000,
        "nsx": 22000, "civic": 45000, "mazda": 85000, "rx-7": 32000,
        "miata": 42000, "subaru": 95000, "wrx": 48000, "sti": 55000,
        "ford": 180000, "mustang": 85000, "gt": 45000, "bronco": 38000,
        "chevrolet": 150000, "corvette": 95000, "camaro": 55000,
        "dodge": 120000, "viper": 42000, "challenger": 48000,
        "tesla": 280000, "model s": 55000, "lexus": 95000, "lfa": 18000,
        "bentley": 75000, "rolls-royce": 68000, "aston martin": 85000,
    }
    
    out = {}
    for key in all_keys:
        s = str(key).lower()
        val = 8000
        
        for k in sorted(known.keys(), key=len, reverse=True):
            if k in s:
                val = max(val, int(known[k] * 0.3))
                break
        
        if val == 8000:
            if any(b in s for b in ['bmw','mercedes','porsche','ferrari','lamborghini','mclaren']):
                val = 20000
            elif any(b in s for b in ['toyota','honda','nissan','mazda','subaru']):
                val = 12000
            elif any(b in s for b in ['ford','chevrolet','dodge']):
                val = 15000
        
        out[key] = val
    
    return out

def get_model_family(model: str) -> str:
    """Extract model family from full model string."""
    m = str(model).upper()
    
    # Mercedes AMG models
    if 'SL63' in m: return 'SL63'
    if 'C63' in m: return 'C63'
    if 'E63' in m: return 'E63'
    if 'S63' in m: return 'S63'
    if 'CLS63' in m: return 'CLS63'
    if 'AMG GT' in m: return 'AMG GT'
    if 'G63' in m: return 'G63'
    
    # Porsche
    if '911' in m: return '911'
    if 'BOXSTER' in m: return 'Boxster'
    if 'CAYMAN' in m: return 'Cayman'
    if 'TAYCAN' in m: return 'Taycan'
    
    # BMW M cars
    if 'M3' in m: return 'M3'
    if 'M5' in m: return 'M5'
    if 'M2' in m: return 'M2'
    if 'M4' in m: return 'M4'
    
    return m

def get_generation(row: pd.Series) -> str:
    """Determine generation code based on make, model family, and year."""
    make = str(row.get('make', ''))
    fam = get_model_family(row.get('model', ''))
    yr = row.get('year', None)
    
    if pd.isna(yr):
        return 'GEN_UNKNOWN'
    
    yr = int(yr)
    
    # Mercedes-Benz
    if make.startswith('Mercedes'):
        if fam == 'SL63' or 'SL' in fam:
            if 2003 <= yr <= 2011: return 'R230'
            if 2012 <= yr <= 2019: return 'R231'
            if yr >= 2022: return 'R232'
        
        if fam == 'C63' or 'C-CLASS' in fam or fam.startswith('C'):
            if 2001 <= yr <= 2007: return 'W203'
            if 2008 <= yr <= 2014: return 'W204'
            if 2015 <= yr <= 2021: return 'W205'
            if yr >= 2022: return 'W206'
        
        if fam == 'E63' or 'E-CLASS' in fam or fam.startswith('E'):
            if 2003 <= yr <= 2009: return 'W211'
            if 2010 <= yr <= 2016: return 'W212'
            if 2017 <= yr <= 2023: return 'W213'
            if yr >= 2024: return 'W214'
        
        if fam == 'S63' or 'S-CLASS' in fam or fam.startswith('S'):
            if 1999 <= yr <= 2006: return 'W220'
            if 2007 <= yr <= 2013: return 'W221'
            if 2014 <= yr <= 2020: return 'W222'
            if yr >= 2021: return 'W223'
        
        if 'AMG GT' in fam:
            if 2015 <= yr <= 2019: return 'C190'
            if yr >= 2020: return 'C190-FL'
        
        if fam == 'G63' or 'G-CLASS' in fam or 'G-WAGON' in fam:
            if 1990 <= yr <= 2018: return 'W463'
            if yr >= 2019: return 'W464'
    
    # Porsche
    if make == 'Porsche':
        if '911' in fam:
            if 1964 <= yr <= 1973: return 'Original'
            if 1974 <= yr <= 1988: return 'G-Series'
            if 1989 <= yr <= 1994: return '964'
            if 1995 <= yr <= 1998: return '993'
            if 1999 <= yr <= 2004: return '996'
            if 2005 <= yr <= 2012: return '997'
            if 2012 <= yr <= 2019: return '991'
            if yr >= 2019: return '992'
        
        if 'BOXSTER' in fam or 'CAYMAN' in fam:
            if 1997 <= yr <= 2004: return '986'
            if 2005 <= yr <= 2012: return '987'
            if 2013 <= yr <= 2016: return '981'
            if yr >= 2017: return '982'
    
    # BMW
    if make == 'BMW':
        if 'M3' in fam:
            if 1986 <= yr <= 1991: return 'E30'
            if 1992 <= yr <= 1999: return 'E36'
            if 2001 <= yr <= 2006: return 'E46'
            if 2008 <= yr <= 2013: return 'E90/E92'
            if 2014 <= yr <= 2020: return 'F80'
            if yr >= 2021: return 'G80'
        
        if 'M5' in fam:
            if 1985 <= yr <= 1988: return 'E28'
            if 1989 <= yr <= 1995: return 'E34'
            if 1999 <= yr <= 2003: return 'E39'
            if 2005 <= yr <= 2010: return 'E60'
            if 2012 <= yr <= 2016: return 'F10'
            if 2018 <= yr <= 2023: return 'F90'
            if yr >= 2024: return 'G90'
        
        if 'M2' in fam:
            if 2016 <= yr <= 2020: return 'F87'
            if yr >= 2023: return 'G87'
    
    # Ferrari
    if make == 'Ferrari':
        if '458' in fam: return '458'
        if '488' in fam: return '488'
        if 'F430' in fam: return 'F430'
        if '360' in fam: return '360'
        if 'F355' in fam: return 'F355'
    
    # JDM
    if make in ['Toyota', 'Lexus']:
        if 'SUPRA' in fam:
            if 1978 <= yr <= 1981: return 'A40'
            if 1982 <= yr <= 1986: return 'A60'
            if 1986 <= yr <= 1992: return 'A70'
            if 1993 <= yr <= 2002: return 'A80'
            if yr >= 2019: return 'A90'
    
    if make == 'Nissan':
        if 'GT-R' in fam or 'GTR' in fam:
            if yr >= 2008: return 'R35'
            if 1999 <= yr <= 2002: return 'R34'
            if 1995 <= yr <= 1998: return 'R33'
            if 1989 <= yr <= 1994: return 'R32'
    
    if make == 'Honda':
        if 'S2000' in fam:
            if 2000 <= yr <= 2003: return 'AP1'
            if 2004 <= yr <= 2009: return 'AP2'
        if 'NSX' in fam:
            if 1990 <= yr <= 2005: return 'NA1/NA2'
            if yr >= 2016: return 'NC1'
    
    return 'GEN_OTHER'

# --------------------- LOADING / CLEANING ----------------------
def load_scraped_data() -> pd.DataFrame:
    """Load combined auction data from S3 or local files."""
    all_data = []
    s3 = None
    
    if HAS_BOTO:
        try:
            s3 = boto3.client('s3')
        except:
            pass
    
    # Bring a Trailer
    try:
        if s3:
            s3.download_file(S3_BUCKET, 'bat.csv', 'temp_bat.csv')
            df_bat = pd.read_csv('temp_bat.csv')
            os.remove('temp_bat.csv')
        else:
            df_bat = pd.read_csv('bat.csv')
        
        df_bat['data_source'] = 'BAT'
        
        if 'model' not in df_bat.columns and 'title' in df_bat.columns:
            df_bat['model'] = df_bat['title']
        
        all_data.append(df_bat)
        print(f"✅ Loaded {len(df_bat):,} BAT records")
    except Exception as e:
        print(f"⚠️  Could not load BAT: {e}")

    # Cars & Bids
    try:
        if s3:
            s3.download_file(S3_BUCKET, 'cnb.csv', 'temp_cnb.csv')
            df_cnb = pd.read_csv('temp_cnb.csv')
            os.remove('temp_cnb.csv')
        else:
            df_cnb = pd.read_csv('cnb_sitemap_full_cleaned.csv')
        
        df_cnb['data_source'] = 'CNB'
        all_data.append(df_cnb)
        print(f"✅ Loaded {len(df_cnb):,} CNB records")
    except Exception as e:
        print(f"⚠️  Could not load CNB: {e}")

    if not all_data:
        print("❌ No scraped data found!")
        return pd.DataFrame()

    combined = pd.concat(all_data, ignore_index=True, sort=False)
    print(f"📊 Total records loaded: {len(combined):,}")
    return combined

def clean_and_process_data(df: pd.DataFrame) -> pd.DataFrame:
    """Clean and process raw auction data."""
    print("\n🧹 Cleaning and processing data...")
    df = df.copy()
    
    # Ensure required columns
    for col in ['model', 'views', 'bids', 'data_source']:
        if col not in df.columns:
            if col in ['views', 'bids']:
                df[col] = 0
            else:
                df[col] = 'Unknown'

    # Normalize model text
    df['model_original'] = df['model']
    df['model'] = df.apply(lambda r: extract_proper_model(r['model'], r.get('make')), axis=1)
    
    initial_count = len(df)
    df = df[df['model'].notna() & (df['model'] != '')]
    print(f"  Removed {initial_count - len(df):,} rows with invalid models")

    # Extract numeric values
    def extract_num(val):
        if pd.isna(val):
            return 0
        if isinstance(val, (int, float)):
            return int(val)
        m = re.findall(r'\d+', str(val).replace(',', ''))
        return int(m[0]) if m else 0

    df['views_numeric'] = df['views'].apply(extract_num)
    df['bids_numeric'] = df['bids'].apply(extract_num)
    
    if 'comments' in df.columns:
        df['comments_numeric'] = df['comments'].apply(extract_num)
    else:
        df['comments_numeric'] = 0

    # FIXED: Use comma-counting method with sale_type context
    if 'sale_amount' in df.columns:
        df['sale_amount_numeric'] = df.apply(
            lambda row: clean_sale_amount(row.get('sale_amount'), row.get('sale_type')),
            axis=1
        )
    else:
        df['sale_amount_numeric'] = 0

    # Assign quarter
    def assign_quarter(row):
        for field in ['scraped_date', 'sale_date', 'end_date']:
            if field in row and pd.notna(row[field]):
                dt = pd.to_datetime(row[field], errors='coerce')
                if pd.notna(dt) and dt <= pd.Timestamp.now():
                    q = f"{dt.year}Q{dt.quarter}"
                    if validate_quarter(q):
                        return q
        now = pd.Timestamp.now()
        return f"{now.year}Q{((now.month - 1) // 3) + 1}"

    df['quarter'] = df.apply(assign_quarter, axis=1)
    df = df[df['quarter'].apply(validate_quarter)]

    # Year / age / cohort
    df['year'] = df.apply(extract_year_from_row, axis=1)
    df['car_age'] = pd.Timestamp.now().year - pd.Series(df['year']).fillna(pd.Timestamp.now().year)
    df['cohort'] = df['year'].apply(era_cohort)

    # Variant splitting
    df['model_family'] = df['model'].apply(get_model_family)
    df['generation'] = df.apply(get_generation, axis=1)
    df['variant_id'] = (
        df.get('make', '').astype(str) + ' ' +
        df['model_family'].astype(str) + ' ' +
        df['generation'].astype(str)
    ).str.strip()

    # Quality filters
    initial_count = len(df)
    
    if 'data_source' in df.columns:
        mask_cnb_low = (df['data_source'] == 'CNB') & (df['views_numeric'] < CNB_MIN_VIEWS)
        df = df[~mask_cnb_low]
        print(f"  Filtered {mask_cnb_low.sum():,} CNB auctions with <{CNB_MIN_VIEWS} views")

    print(f"✅ Cleaned: {len(df):,} rows, {df['model'].nunique():,} unique models, {df['variant_id'].nunique():,} variants")
    return df

# ----------------- WINSORIZING / ROBUST Z ---------------------
def winsorize_series(s: pd.Series, lower: float = WINSOR_LO, upper: float = WINSOR_HI) -> pd.Series:
    """Winsorize a series by capping values at specified percentiles."""
    if s.empty:
        return s
    lo = s.quantile(lower)
    hi = s.quantile(upper)
    return s.clip(lower=lo, upper=hi)

def winsorize_by_groups(
    df: pd.DataFrame,
    group_cols: List[str],
    metric_cols: List[str],
    lower: float = WINSOR_LO,
    upper: float = WINSOR_HI
) -> pd.DataFrame:
    """Apply winsorizing within groups."""
    df = df.copy()
    for m in metric_cols:
        if m in df.columns:
            df[m] = df.groupby(group_cols, group_keys=False)[m].apply(
                lambda x: winsorize_series(x, lower, upper)
            )
    return df

def robust_z(series: pd.Series) -> pd.Series:
    """Calculate robust z-scores using median and MAD."""
    if series.empty:
        return series
    
    med = series.median()
    mad = (series - med).abs().median()
    
    if mad == 0:
        std = series.std()
        if std == 0:
            return pd.Series(0, index=series.index)
        return (series - series.mean()) / std
    
    z = (series - med) / mad
    return z

# --------------------- CORE CALCULATION -----------------------
def calculate_mii_scores(df: pd.DataFrame) -> pd.DataFrame:
    """Calculate Market Interest Index scores using robust statistical methods."""
    print("\n🧮 Calculating MII scores (winsorized + robust z)...")
    df = df.copy()

    entity_col = 'variant_id' if 'variant_id' in df.columns else 'model'

    # Instagram estimates
    all_keys = df[entity_col].unique()
    ig_map = get_instagram_estimates(all_keys)
    df['instagram_mentions'] = df[entity_col].map(ig_map).fillna(8000)

    # Build grouping columns
    group_cols = [entity_col, 'quarter']
    if 'make' in df.columns:
        group_cols.insert(0, 'make')
    if 'cohort' in df.columns:
        group_cols.append('cohort')

    # Aggregate
    agg_dict = {
        'views_numeric': 'mean',
        'bids_numeric': 'mean',
        'comments_numeric': 'mean',
        'sale_amount_numeric': lambda x: x[x > 0].mean() if (x > 0).any() else 0,
        'data_source': 'count',
        'year': 'first',
        'car_age': 'first',
        'instagram_mentions': 'first'
    }
    
    grouped = df.groupby(group_cols).agg(agg_dict).reset_index()
    grouped = grouped.rename(columns={'data_source': 'total_auctions'})

    print(f"\n📊 Aggregation Summary:")
    print(f"  Total groups: {len(grouped):,}")
    print(f"  Groups with 1 auction: {(grouped['total_auctions'] == 1).sum():,}")
    print(f"  Groups with no sale data: {(grouped['sale_amount_numeric'] == 0).sum():,}")

    # Winsorize per quarter + cohort
    metrics_to_clip = [
        'views_numeric', 'bids_numeric', 'comments_numeric',
        'sale_amount_numeric', 'instagram_mentions'
    ]
    group_for_clip = ['quarter']
    if 'cohort' in grouped.columns:
        group_for_clip.append('cohort')
    
    grouped = winsorize_by_groups(grouped, group_for_clip, metrics_to_clip, WINSOR_LO, WINSOR_HI)

    # Calculate robust z-scores
    def apply_robust_z(g):
        for m in metrics_to_clip + ['total_auctions', 'car_age']:
            zcol = f'z_{m}'
            if m in g.columns:
                g[zcol] = robust_z(g[m])
                g[zcol] = g[zcol].clip(-Z_CAP, Z_CAP)
            else:
                g[zcol] = 0
        return g
    
    grouped = grouped.groupby(group_for_clip, group_keys=False).apply(apply_robust_z)

    # Weighted combination
    weights = {
        'z_bids_numeric':          0.235,
        'z_sale_amount_numeric':   0.206,
        'z_views_numeric':         0.176,
        'z_total_auctions':        0.118,
        'z_instagram_mentions':    0.100,
        'z_comments_numeric':      0.088,
        'z_car_age':               0.059,
    }
    
    total_w = sum(weights.values())
    grouped['MII_Score'] = 0.0
    
    for col, w in weights.items():
        if col in grouped.columns:
            grouped['MII_Score'] += grouped[col] * w
        else:
            print(f"⚠️  Warning: {col} not found in data")
    
    grouped['MII_Score'] /= total_w

    # Scale to 0-100 index
    def to_index(g):
        mx, mn = g['MII_Score'].max(), g['MII_Score'].min()
        if mx > mn:
            g['MII_Index'] = 100 * (g['MII_Score'] - mn) / (mx - mn)
        else:
            g['MII_Index'] = 50
            print(f"⚠️  Quarter {g['quarter'].iloc[0]} has uniform MII scores")
        return g
    
    grouped = grouped.groupby('quarter', group_keys=False).apply(to_index)

    # Calculate ranks
    grouped['Quarter_Rank'] = grouped.groupby('quarter')['MII_Index'].rank(
        ascending=False,
        method='min'
    )

    # Sort for momentum
    grouped = grouped.sort_values([entity_col, 'quarter'])

    # Calculate momentum
    grouped['MII_Momentum'] = grouped.groupby(entity_col)['MII_Index'].diff()

    # EMA smoothing
    grouped['MII_Smoothed'] = grouped.groupby(entity_col)['MII_Index'].transform(
        lambda s: s.ewm(alpha=EMA_ALPHA, adjust=False).mean()
    )

    # Percentile rankings
    grouped['Percentile'] = grouped.groupby('quarter')['MII_Index'].rank(pct=True) * 100

    # Tier classification
    def classify_tier(percentile):
        if percentile >= 90:
            return 'S-Tier'
        if percentile >= 75:
            return 'A-Tier'
        if percentile >= 50:
            return 'B-Tier'
        if percentile >= 25:
            return 'C-Tier'
        return 'D-Tier'
    
    grouped['Tier'] = grouped['Percentile'].apply(classify_tier)

    # Year-over-year comparisons
    grouped['YoY_Change'] = grouped.groupby(entity_col)['MII_Index'].diff(4)

    # Add calculation timestamp
    grouped['calculation_date'] = pd.Timestamp.now().strftime('%Y-%m-%d %H:%M:%S')

    # Final sort
    grouped = grouped.sort_values(['quarter', 'MII_Index'], ascending=[False, False])

    print(f"✅ Calculated MII for {len(grouped):,} variant-quarter combinations")
    print(f"  Entity type: {entity_col}")
    print(f"  Quarters covered: {grouped['quarter'].nunique()}")
    print(f"  Date range: {grouped['quarter'].min()} to {grouped['quarter'].max()}")
    
    return grouped

# --------------- % CHANGE with STABILITY RULES ----------------
def percent_change_table(
    mii_results: pd.DataFrame,
    raw_df: pd.DataFrame,
    from_quarter: str = '2025Q2',
    to_quarter: str = '2025Q3'
) -> pd.DataFrame:
    """Calculate percentage change in MII between two quarters with stability rules."""
    entity_col = 'variant_id' if 'variant_id' in mii_results.columns else 'model'
    
    q_from = mii_results[mii_results['quarter'] == from_quarter].copy()
    q_to = mii_results[mii_results['quarter'] == to_quarter].copy()

    if q_from.empty:
        print(f"⚠️  No data found for {from_quarter}")
        return pd.DataFrame()
    
    if q_to.empty:
        print(f"⚠️  No data found for {to_quarter}")
        return pd.DataFrame()

    merged = pd.merge(
        q_from[[entity_col, 'MII_Index', 'total_auctions']].rename(
            columns={'MII_Index': f'MII_{from_quarter}', 'total_auctions': f'Auctions_{from_quarter}'}
        ),
        q_to[[entity_col, 'MII_Index', 'total_auctions']].rename(
            columns={'MII_Index': f'MII_{to_quarter}', 'total_auctions': f'Auctions_{to_quarter}'}
        ),
        on=entity_col,
        how='inner'
    )

    print(f"\n📊 % Change Analysis ({from_quarter} → {to_quarter}):")
    print(f"  Entities in both quarters: {len(merged):,}")

    initial_count = len(merged)
    merged = merged[merged[f'MII_{from_quarter}'] >= BASE_FLOOR_FOR_PCT].copy()
    print(f"  After base floor filter (>={BASE_FLOOR_FOR_PCT}): {len(merged):,} (removed {initial_count - len(merged):,})")

    merged['Pct_Change'] = 100 * (
        merged[f'MII_{to_quarter}'] - merged[f'MII_{from_quarter}']
    ) / merged[f'MII_{from_quarter}']

    small_base_mask = merged[f'MII_{from_quarter}'] < 12
    if small_base_mask.any():
        merged.loc[small_base_mask, 'Pct_Change'] = merged.loc[small_base_mask, 'Pct_Change'].clip(
            upper=SMALL_BASE_CAP
        )
        print(f"  Capped % change for {small_base_mask.sum():,} entities with base MII < 12")

    if 'make' in raw_df.columns:
        key_map = raw_df[[entity_col, 'make']].drop_duplicates()
        merged = merged.merge(key_map, on=entity_col, how='left')

    merged['Abs_Change'] = merged[f'MII_{to_quarter}'] - merged[f'MII_{from_quarter}']

    return merged

# -------------------- COHORT ANALYSIS -------------------------
def cohort_analysis(mii_results: pd.DataFrame) -> pd.DataFrame:
    """Analyze MII trends by era cohort."""
    if 'cohort' not in mii_results.columns:
        print("⚠️  No cohort data available")
        return pd.DataFrame()
    
    cohort_stats = mii_results.groupby(['quarter', 'cohort']).agg({
        'MII_Index': ['mean', 'median', 'std', 'min', 'max'],
        'total_auctions': 'sum',
        'variant_id': 'count'
    }).round(2)
    
    cohort_stats.columns = ['_'.join(col).strip() for col in cohort_stats.columns.values]
    cohort_stats = cohort_stats.reset_index()
    
    return cohort_stats

# -------------------- TOP MOVERS ------------------------------
def get_top_movers(
    mii_results: pd.DataFrame,
    quarter: str,
    n: int = 20,
    by: str = 'MII_Index'
) -> pd.DataFrame:
    """Get top N entities for a specific quarter."""
    quarter_data = mii_results[mii_results['quarter'] == quarter].copy()
    
    if quarter_data.empty:
        print(f"⚠️  No data for quarter {quarter}")
        return pd.DataFrame()
    
    top = quarter_data.nlargest(n, by)
    return top

# ----------------------------- MAIN ---------------------------
def main():
    """Main execution function for MII pipeline."""
    print("=" * 60)
    print("🚀 MII Calculator v2.0 (Enhanced & Robust)")
    print("=" * 60)
    print(f"⏰ Started at: {datetime.datetime.now():%Y-%m-%d %H:%M:%S}")
    print(f"📍 Instagram estimates: {IG_ESTIMATES_VERSION} ({IG_ESTIMATES_LAST_UPDATED})")
    print(f"⚙️  Config: Winsor={WINSOR_LO}/{WINSOR_HI}, Z-cap=±{Z_CAP}, EMA α={EMA_ALPHA}")
    print("=" * 60)

    # 1) Load raw auctions
    raw = load_scraped_data()
    if raw.empty:
        print("❌ No data to process.")
        return False

    # 2) Clean and process
    clean = clean_and_process_data(raw)
    if clean.empty:
        print("❌ No clean data after processing.")
        return False

    # 3) Calculate MII scores
    mii = calculate_mii_scores(clean)

    # 4) Generate insights
    print("\n" + "=" * 60)
    print("📈 GENERATING INSIGHTS")
    print("=" * 60)

    latest_quarter = mii['quarter'].max()
    print(f"\n🎯 Latest Quarter: {latest_quarter}")

    # Top 15 overall
    top_overall = get_top_movers(mii, latest_quarter, n=15, by='MII_Index')
    print(f"\n🏆 Top 15 by MII Index ({latest_quarter}):")
    display_cols = ['make', 'variant_id', 'MII_Index', 'Tier', 'Quarter_Rank', 'total_auctions']
    display_cols = [col for col in display_cols if col in top_overall.columns]
    print(top_overall[display_cols].to_string(index=False))

    # Top momentum gainers
    if 'MII_Momentum' in mii.columns:
        top_momentum = get_top_movers(mii, latest_quarter, n=10, by='MII_Momentum')
        print(f"\n📈 Top 10 Momentum Gainers ({latest_quarter}):")
        momentum_cols = ['make', 'variant_id', 'MII_Index', 'MII_Momentum', 'Tier']
        momentum_cols = [col for col in momentum_cols if col in top_momentum.columns]
        print(top_momentum[momentum_cols].to_string(index=False))

    # Percentage change analysis
    if '2025Q2' in mii['quarter'].values and '2025Q3' in mii['quarter'].values:
        pct = percent_change_table(mii, clean, from_quarter='2025Q2', to_quarter='2025Q3')
        
        if not pct.empty:
            print(f"\n🔺 Top 15 % Change (Q2 → Q3 2025):")
            pct_sorted = pct.sort_values('Pct_Change', ascending=False)
            pct_cols = ['make', 'variant_id', 'MII_2025Q2', 'MII_2025Q3', 'Pct_Change']
            pct_cols = [col for col in pct_cols if col in pct_sorted.columns]
            print(pct_sorted.head(15)[pct_cols].to_string(index=False))
            
            if 'make' in pct.columns:
                pct_mercedes = pct[pct['make'].str.contains('Mercedes', case=False, na=False)]
                if not pct_mercedes.empty:
                    pct_mercedes_sorted = pct_mercedes.sort_values('Pct_Change', ascending=False)
                    print(f"\n🔺 Top 15 Mercedes % Change (Q2 → Q3 2025):")
                    print(pct_mercedes_sorted.head(15)[pct_cols].to_string(index=False))

    # Cohort analysis
    cohort_stats = cohort_analysis(mii)
    if not cohort_stats.empty:
        print(f"\n📊 Cohort Analysis by Quarter:")
        print(cohort_stats.tail(12).to_string(index=False))

    # Tier distribution
    tier_dist = mii[mii['quarter'] == latest_quarter]['Tier'].value_counts().sort_index()
    print(f"\n🎖️  Tier Distribution ({latest_quarter}):")
    for tier, count in tier_dist.items():
        pct = 100 * count / tier_dist.sum()
        print(f"  {tier}: {count:,} ({pct:.1f}%)")

    # 5) Save results
    print("\n" + "=" * 60)
    print("💾 SAVING RESULTS")
    print("=" * 60)
    
    ts = datetime.datetime.now().strftime('%Y%m%d_%H%M')
    out_csv = f"{OUTPUT_PREFIX}_{ts}.csv"
    latest_csv = f"{OUTPUT_PREFIX}_latest.csv"
    
    mii.to_csv(out_csv, index=False)
    mii.to_csv(latest_csv, index=False)
    print(f"✅ Saved: {out_csv}")
    print(f"✅ Saved: {latest_csv}")

    if not pct.empty:
        pct_csv = f"mii_pct_change_{ts}.csv"
        pct.to_csv(pct_csv, index=False)
        print(f"✅ Saved: {pct_csv}")

    if not cohort_stats.empty:
        cohort_csv = f"mii_cohort_analysis_{ts}.csv"
        cohort_stats.to_csv(cohort_csv, index=False)
        print(f"✅ Saved: {cohort_csv}")

    # 6) Optional S3 upload
    if S3_BUCKET:
        print(f"\n☁️  Uploading to S3 bucket: {S3_BUCKET}")
        if HAS_BOTO:
            upload_to_s3(out_csv, S3_BUCKET)
            upload_to_s3(latest_csv, S3_BUCKET)
            if not pct.empty:
                upload_to_s3(pct_csv, S3_BUCKET)
            if not cohort_stats.empty:
                upload_to_s3(cohort_csv, S3_BUCKET)
        else:
            print("⚠️  S3 upload skipped: boto3 not installed")

    print("\n" + "=" * 60)
    print("🎉 MII CALCULATION COMPLETE")
    print("=" * 60)
    print(f"⏰ Finished at: {datetime.datetime.now():%Y-%m-%d %H:%M:%S}")
    
    return True

if __name__ == "__main__":
    success = main()
    exit(0 if success else 1)
