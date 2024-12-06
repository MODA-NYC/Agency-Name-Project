import pandas as pd
from rapidfuzz import fuzz

def analyze_potential_matches(merged_df: pd.DataFrame):
    # Get records with OPS names
    ops_records = merged_df[merged_df['Name - Ops'].notna()]
    
    # Get records with NYC.gov names
    nyc_records = merged_df[merged_df['Name - NYC.gov Redesign'].notna()]
    
    # Compare normalized names
    potential_matches = []
    for _, ops_row in ops_records.iterrows():
        for _, nyc_row in nyc_records.iterrows():
            score = fuzz.ratio(
                ops_row['NameNormalized'],
                nyc_row['NameNormalized']
            )
            if score > 80:  # Threshold for potential matches
                potential_matches.append({
                    'OPS_Name': ops_row['Name - Ops'],
                    'NYC_Name': nyc_row['Name - NYC.gov Redesign'],
                    'Score': score
                })
    
    return pd.DataFrame(potential_matches) 