from rapidfuzz import fuzz, utils
import pandas as pd
import hashlib

def get_composite_score(str1: str, str2: str) -> float:
    """
    Calculate composite similarity score between two strings.
    Uses weighted combination of ratio and token sort ratio.
    
    Args:
        str1: First string to compare
        str2: Second string to compare
        
    Returns:
        Float between 0-100 representing similarity score
    """
    if pd.isna(str1) or pd.isna(str2):
        return 0.0
        
    # Process strings
    str1 = utils.default_process(str1)
    str2 = utils.default_process(str2)
    
    # Calculate scores using RapidFuzz
    basic_score = fuzz.ratio(str1, str2)
    token_score = fuzz.token_sort_ratio(str1, str2)
    
    # Weight the scores (60% basic ratio, 40% token sort)
    composite_score = (basic_score * 0.6) + (token_score * 0.4)
    
    return composite_score

def generate_record_id(row: pd.Series) -> str:
    """
    Generate a unique record ID for each source-target pair.
    
    Args:
        row: DataFrame row containing Source and Target columns
        
    Returns:
        String ID in format 'REC_XXXXXX' where X is a 6-digit number
    """
    # Create deterministic hash of source and target
    combined = f"{str(row['Source'])}_{str(row['Target'])}"
    hash_object = hashlib.md5(combined.encode())
    hash_hex = hash_object.hexdigest()
    
    # Convert first 6 characters of hash to integer and take modulo to get 6 digits
    num = int(hash_hex[:6], 16) % 1000000
    
    # Format as REC_XXXXXX where X is a 6-digit number padded with zeros
    return f"REC_{num:06d}"
