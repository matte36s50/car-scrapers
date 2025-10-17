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
    """Extract proper model name"""
    if not model_text or pd.isna(model_text):
        return None
    
    model_str = str(model_text).strip()
    original_model = model_str
    
    model_str = re.sub(r'^\d{4}\s+', '', model_str)
    
    common_makes = ['Mercedes-Benz', 'Mercedes', 'BMW', 'Porsche', 'Audi', 'Ferrari',
                   'Lamborghini', 'McLaren', 'Chevrolet', 'Chevy', 'Ford', 'Dodge', 'Tesla',
                   'Toyota', 'Honda', 'Nissan', 'Lexus', 'Acura', 'Infiniti', 'Jaguar']
    
    common_makes.sort(key=len, reverse=True)
    
    for make in common_makes:
        pattern = rf'^{re.escape(make)}[\s-]+'
        model_str = re.sub(pattern, '', model_str, flags=re.IGNORECASE)
    
    model_str = re.sub(r'\s*\(\d{4}-\d{4}\)\s*$', '', model_str)
    model_str = re.sub(r'\s+', ' ', model_str).strip()
    
    if model_str.upper() == 'AMG':
        amg_match = re.search(r'([A-Z]+\d+[A-Z]*)\s*AMG', original_model, re.IGNORECASE)
        if amg_match:
            return f"{amg_match.group(1)} AMG"
        return None
    
    return model_str if model_str else None

def clean_sale_amount(sale_text):
    """Clean and validate sale amounts"""
    if not sale_text or pd.isna(sale_text):
        return None
    
    sale_str = str(sale_text).replace('$', '').replace(',', '').strip()
    
    if '.' in sale_str:
        parts = sale_str.split('.')
        if len(parts) == 2:
            sale_str = parts[0]
    
    match = re.search(r'\d+', sale_str)
    if not match:
        return None
    
    amount = int(match.group(0))
    
    if amount > 500000:
        last_three_digits = amount % 1000
        if last_three_digits in [9, 10, 11, 12]:
            corrected_amount = amount // 100
            print(f"  🔧 Corrected ${amount:,} → ${corrected_amount:,}")
            amount = corrected_amount
    
    if amount < 100 or amount > 10000000:
        return None
    
    return amount
