import pandas as pd

def analyze_unmatched_records(merged_df: pd.DataFrame):
    unmatched = merged_df[
        merged_df['Name - Ops'].isna() & 
        merged_df['Name - NYC.gov Redesign'].isna()
    ]
    
    # Save unmatched records for review
    unmatched.to_csv('data/analysis/unmatched_records.csv', index=False)
    
    # Basic analysis of unmatched
    print("\nUnmatched Records Analysis:")
    print(f"Total unmatched: {len(unmatched)}")
    print("\nSample of unmatched names:")
    print(unmatched['Name'].head()) 