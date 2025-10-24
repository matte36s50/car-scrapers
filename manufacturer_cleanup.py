"""
Manufacturer Name Standardization Module
Cleans and standardizes manufacturer names from auction data
"""

import re
import pandas as pd


def standardize_manufacturer(name):
    """
    Standardize a manufacturer name to its canonical form.
    
    Args:
        name: Raw manufacturer name string
        
    Returns:
        Standardized manufacturer name
    """
    if pd.isna(name) or name == '':
        return 'Unknown'
    
    # Convert to string and clean
    name = str(name).strip()
    
    # Manufacturer mapping dictionary - maps variations to standard names
    manufacturer_map = {
        # Mercedes variations
        'mercedes-benz': 'Mercedes-Benz',
        'mercedes benz': 'Mercedes-Benz',
        'mercedes': 'Mercedes-Benz',
        'merc': 'Mercedes-Benz',
        'mb': 'Mercedes-Benz',
        'm-b': 'Mercedes-Benz',
        'amg': 'Mercedes-Benz',  # AMG is Mercedes sub-brand
        
        # BMW variations
        'bmw': 'BMW',
        'b.m.w.': 'BMW',
        'b.m.w': 'BMW',
        'bayerische motoren werke': 'BMW',
        
        # Porsche variations
        'porsche': 'Porsche',
        'porche': 'Porsche',  # common misspelling
        
        # Ferrari variations
        'ferrari': 'Ferrari',
        
        # Lamborghini variations
        'lamborghini': 'Lamborghini',
        'lambo': 'Lamborghini',
        
        # Audi variations
        'audi': 'Audi',
        
        # Volkswagen variations
        'volkswagen': 'Volkswagen',
        'vw': 'Volkswagen',
        'v.w.': 'Volkswagen',
        'volks wagen': 'Volkswagen',
        
        # Ford variations
        'ford': 'Ford',
        
        # Chevrolet variations
        'chevrolet': 'Chevrolet',
        'chevy': 'Chevrolet',
        
        # Dodge variations
        'dodge': 'Dodge',
        
        # Jaguar variations
        'jaguar': 'Jaguar',
        'jag': 'Jaguar',
        
        # Land Rover variations
        'land rover': 'Land Rover',
        'landrover': 'Land Rover',
        'land-rover': 'Land Rover',
        
        # Range Rover (sub-brand of Land Rover)
        'range rover': 'Land Rover',
        'range-rover': 'Land Rover',
        
        # Alfa Romeo variations
        'alfa romeo': 'Alfa Romeo',
        'alfa-romeo': 'Alfa Romeo',
        'alfa': 'Alfa Romeo',
        
        # Aston Martin variations
        'aston martin': 'Aston Martin',
        'aston-martin': 'Aston Martin',
        'aston': 'Aston Martin',
        
        # Rolls-Royce variations
        'rolls-royce': 'Rolls-Royce',
        'rolls royce': 'Rolls-Royce',
        'rolls': 'Rolls-Royce',
        'rollsroyce': 'Rolls-Royce',
        
        # Bentley variations
        'bentley': 'Bentley',
        
        # McLaren variations
        'mclaren': 'McLaren',
        'mcclaren': 'McLaren',  # common misspelling
        
        # Lotus variations
        'lotus': 'Lotus',
        
        # Maserati variations
        'maserati': 'Maserati',
        
        # Toyota variations
        'toyota': 'Toyota',
        
        # Nissan variations
        'nissan': 'Nissan',
        'datsun': 'Nissan',  # Datsun was Nissan brand
        
        # Honda variations
        'honda': 'Honda',
        
        # Mazda variations
        'mazda': 'Mazda',
        
        # Subaru variations
        'subaru': 'Subaru',
        
        # Mitsubishi variations
        'mitsubishi': 'Mitsubishi',
        
        # Lexus variations
        'lexus': 'Lexus',
        
        # Acura variations
        'acura': 'Acura',
        
        # Infiniti variations
        'infiniti': 'Infiniti',
        
        # Cadillac variations
        'cadillac': 'Cadillac',
        'caddy': 'Cadillac',
        
        # Buick variations
        'buick': 'Buick',
        
        # GMC variations
        'gmc': 'GMC',
        'g.m.c.': 'GMC',
        
        # Plymouth variations
        'plymouth': 'Plymouth',
        
        # Pontiac variations
        'pontiac': 'Pontiac',
        
        # Oldsmobile variations
        'oldsmobile': 'Oldsmobile',
        'olds': 'Oldsmobile',
        
        # Mercury variations
        'mercury': 'Mercury',
        
        # Lincoln variations
        'lincoln': 'Lincoln',
        
        # Chrysler variations
        'chrysler': 'Chrysler',
        
        # Jeep variations
        'jeep': 'Jeep',
        
        # Ram variations
        'ram': 'Ram',
        
        # Tesla variations
        'tesla': 'Tesla',
        
        # Volvo variations
        'volvo': 'Volvo',
        
        # Saab variations
        'saab': 'Saab',
        
        # Peugeot variations
        'peugeot': 'Peugeot',
        
        # Renault variations
        'renault': 'Renault',
        
        # Citroën variations
        'citroen': 'Citroën',
        'citroën': 'Citroën',
        
        # Fiat variations
        'fiat': 'Fiat',
        
        # Lancia variations
        'lancia': 'Lancia',
        
        # Bugatti variations
        'bugatti': 'Bugatti',
        
        # Koenigsegg variations
        'koenigsegg': 'Koenigsegg',
        
        # Pagani variations
        'pagani': 'Pagani',
        
        # Morgan variations
        'morgan': 'Morgan',
        
        # TVR variations
        'tvr': 'TVR',
        
        # Caterham variations
        'caterham': 'Caterham',
        
        # DeLorean variations
        'delorean': 'DeLorean',
        'de lorean': 'DeLorean',
        
        # Shelby variations (often listed separately from Ford)
        'shelby': 'Shelby',
        
        # MG variations
        'mg': 'MG',
        'm.g.': 'MG',
        
        # Triumph variations
        'triumph': 'Triumph',
        
        # Austin-Healey variations
        'austin-healey': 'Austin-Healey',
        'austin healey': 'Austin-Healey',
        'healey': 'Austin-Healey',
        
        # Mini variations
        'mini': 'Mini',
        
        # Smart variations
        'smart': 'Smart',
    }
    
    # Normalize the name for lookup
    name_lower = name.lower()
    name_lower = re.sub(r'[^\w\s-]', '', name_lower)  # Remove special chars except dash
    name_lower = re.sub(r'\s+', ' ', name_lower).strip()  # Normalize whitespace
    
    # Look up in mapping
    if name_lower in manufacturer_map:
        return manufacturer_map[name_lower]
    
    # If not found, return title case version of cleaned name
    return name.title()


def extract_manufacturer_from_model(model_string):
    """
    Extract manufacturer name from a full model string.
    Handles cases like "Mercedes-Benz 190E" or "BMW M3"
    
    Args:
        model_string: Full model string that may contain manufacturer
        
    Returns:
        Tuple of (manufacturer, model)
    """
    if pd.isna(model_string) or model_string == '':
        return 'Unknown', ''
    
    model_string = str(model_string).strip()
    
    # List of known manufacturers (in order of specificity - longer names first)
    known_manufacturers = [
        'Mercedes-Benz', 'Land Rover', 'Aston Martin', 'Alfa Romeo',
        'Rolls-Royce', 'Austin-Healey', 'Range Rover',
        'BMW', 'Porsche', 'Ferrari', 'Lamborghini', 'Audi', 'Volkswagen',
        'Ford', 'Chevrolet', 'Dodge', 'Jaguar', 'Bentley', 'McLaren',
        'Lotus', 'Maserati', 'Toyota', 'Nissan', 'Honda', 'Mazda',
        'Subaru', 'Mitsubishi', 'Lexus', 'Acura', 'Infiniti', 'Cadillac',
        'Buick', 'GMC', 'Plymouth', 'Pontiac', 'Oldsmobile', 'Mercury',
        'Lincoln', 'Chrysler', 'Jeep', 'Ram', 'Tesla', 'Volvo', 'Saab',
        'Peugeot', 'Renault', 'Citroën', 'Fiat', 'Lancia', 'Bugatti',
        'Koenigsegg', 'Pagani', 'Morgan', 'TVR', 'Caterham', 'DeLorean',
        'Shelby', 'MG', 'Triumph', 'Mini', 'Smart', 'AMG'
    ]
    
    # Check if string starts with a known manufacturer
    for manufacturer in known_manufacturers:
        # Case insensitive check
        pattern = re.compile(r'^' + re.escape(manufacturer) + r'\b', re.IGNORECASE)
        if pattern.search(model_string):
            # Extract the manufacturer and the rest as model
            model = model_string[len(manufacturer):].strip()
            # Remove leading dash or space
            model = re.sub(r'^[\s\-]+', '', model)
            return standardize_manufacturer(manufacturer), model
    
    # If no manufacturer found at start, return as is
    return 'Unknown', model_string


def clean_manufacturer_column(df, manufacturer_col='manufacturer', model_col=None):
    """
    Clean manufacturer names in a dataframe.
    
    Args:
        df: pandas DataFrame
        manufacturer_col: Name of manufacturer column
        model_col: Optional model column to extract manufacturer from if missing
        
    Returns:
        DataFrame with cleaned manufacturer column
    """
    df = df.copy()
    
    # First, standardize existing manufacturer names
    if manufacturer_col in df.columns:
        df[manufacturer_col] = df[manufacturer_col].apply(standardize_manufacturer)
    
    # If model column provided, try to extract manufacturer from it when missing
    if model_col and model_col in df.columns:
        # Find rows with missing or Unknown manufacturer
        missing_mask = (df[manufacturer_col].isna()) | (df[manufacturer_col] == 'Unknown')
        
        if missing_mask.any():
            # Extract manufacturer from model string
            extracted = df.loc[missing_mask, model_col].apply(
                lambda x: extract_manufacturer_from_model(x)[0]
            )
            df.loc[missing_mask, manufacturer_col] = extracted
    
    return df


def get_manufacturer_stats(df, manufacturer_col='manufacturer'):
    """
    Get statistics about manufacturers in the dataset.
    
    Args:
        df: pandas DataFrame
        manufacturer_col: Name of manufacturer column
        
    Returns:
        DataFrame with manufacturer counts and percentages
    """
    stats = df[manufacturer_col].value_counts().reset_index()
    stats.columns = ['Manufacturer', 'Count']
    stats['Percentage'] = (stats['Count'] / len(df) * 100).round(2)
    return stats


# Example usage and testing
if __name__ == '__main__':
    print("🧹 Manufacturer Name Cleanup Module")
    print("=" * 60)
    
    # Test cases
    test_names = [
        'mercedes', 'Mercedes-Benz', 'MERCEDES BENZ', 'MB',
        'bmw', 'B.M.W.', 'BMW',
        'porsche', 'Porche',
        'vw', 'Volkswagen', 'VOLKSWAGEN',
        'Land Rover', 'land-rover', 'LAND ROVER',
        'alfa', 'Alfa Romeo', 'alfa-romeo',
        'Unknown', '', None
    ]
    
    print("\n📋 Testing standardization:")
    print("-" * 60)
    for name in test_names:
        standardized = standardize_manufacturer(name)
        print(f"{str(name):25s} → {standardized}")
    
    # Test manufacturer extraction
    print("\n\n📦 Testing manufacturer extraction from model strings:")
    print("-" * 60)
    test_models = [
        'Mercedes-Benz 190E 2.3-16',
        'BMW M3',
        'Porsche 911 Turbo',
        'Ford Mustang GT',
        'Chevrolet Corvette',
        'M3',  # No manufacturer
    ]
    
    for model in test_models:
        manufacturer, clean_model = extract_manufacturer_from_model(model)
        print(f"{model:30s} → {manufacturer:15s} | {clean_model}")
    
    # Test on sample dataframe
    print("\n\n📊 Testing on sample DataFrame:")
    print("-" * 60)
    sample_data = pd.DataFrame({
        'manufacturer': ['mercedes', 'BMW', 'vw', None, 'porche', 'land rover'],
        'model': ['190E', 'M3', 'Golf GTI', 'Ford Mustang', '911', 'Defender'],
        'price': [15000, 45000, 12000, 30000, 85000, 40000]
    })
    
    print("\nBefore cleaning:")
    print(sample_data[['manufacturer', 'model']])
    
    cleaned_df = clean_manufacturer_column(sample_data, 'manufacturer', 'model')
    
    print("\nAfter cleaning:")
    print(cleaned_df[['manufacturer', 'model']])
    
    print("\n\n📈 Manufacturer Statistics:")
    print("-" * 60)
    stats = get_manufacturer_stats(cleaned_df)
    print(stats.to_string(index=False))
