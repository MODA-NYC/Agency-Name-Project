import pandas as pd
import os

def analyze_counts():
    print("=== Analyzing Record Counts ===\n")
    
    # Load raw data (subtract 1 for headers)
    raw_ops = len(pd.read_csv('../data/raw/ops_data.csv')) - 1
    raw_hoo = len(pd.read_csv('../data/raw/nyc_gov_hoo.csv')) - 1
    raw_primary = len(pd.read_csv('../data/processed/nyc_agencies_export.csv')) - 1
    
    print("Raw Data Counts:")
    print(f"OPS: {raw_ops} records")
    print(f"HOO: {raw_hoo} records")
    print(f"Primary: {raw_primary} records")
    print(f"Total Raw: {raw_ops + raw_hoo + raw_primary} records\n")
    
    # Load merged dataset
    merged_df = pd.read_csv('../data/intermediate/merged_dataset.csv')
    
    print("Merged Dataset Analysis:")
    print(f"Total records: {len(merged_df)}")
    
    print("\nSource Distribution:")
    print(merged_df['source'].value_counts())
    
    print("\nData Source Distribution:")
    print(merged_df['data_source'].value_counts())
    
    print("\nNull Values in Key Columns:")
    print(f"Agency Name: {merged_df['Agency Name'].isna().sum()}")
    print(f"NameNormalized: {merged_df['NameNormalized'].isna().sum()}")
    print(f"RecordID: {merged_df['RecordID'].isna().sum()}")
    
    # Check for duplicate RecordIDs
    duplicate_ids = merged_df[merged_df['RecordID'].duplicated(keep=False)]
    if not duplicate_ids.empty:
        print("\nWARNING: Found duplicate RecordIDs:")
        print(duplicate_ids[['RecordID', 'Agency Name', 'source']].to_string())

if __name__ == '__main__':
    analyze_counts() 