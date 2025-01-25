from data_merging import (
    merge_dataframes,
    clean_merged_data,
    track_data_provenance,
    ensure_record_ids,
    deduplicate_merged_data
)
from preprocessing.ops_processor import OpsDataProcessor
from preprocessing.hoo_processor import HooDataProcessor
import pandas as pd
import os
import logging

def setup_logging():
    """Set up logging configuration"""
    logging.basicConfig(
        level=logging.INFO,
        format='%(message)s'
    )

def main():
    # Set up logging
    setup_logging()
    
    # Initialize processors
    ops_processor = OpsDataProcessor()
    hoo_processor = HooDataProcessor()

    # Process data
    ops_df = ops_processor.process('../data/raw/ops_data.csv')
    hoo_df = hoo_processor.process('../data/raw/nyc_gov_hoo.csv')
    
    # Load primary dataset
    primary_df = pd.read_csv('../data/processed/nyc_agencies_export.csv')
    
    # Define secondary dataframes with their fields
    secondary_dfs = [
        (hoo_df, [
            'Agency Name',
            'AgencyNameEnriched',
            'NameNormalized',
            'RecordID',
            'HeadOfOrganizationName',
            'HeadOfOrganizationTitle',
            'HeadOfOrganizationURL'
        ], 'nyc_gov_'),
        (ops_df, [
            'Agency Name',
            'AgencyNameEnriched',
            'NameNormalized',
            'RecordID',
            'Entity type'
        ], 'ops_')
    ]
    
    # Execute merging process
    print("\nExecuting merge process...")
    merged_df = merge_dataframes(primary_df, secondary_dfs)
    print("Basic merge complete.")
    
    merged_df = clean_merged_data(merged_df)
    print("Data cleaning complete.")
    
    merged_df = track_data_provenance(merged_df)
    print("Provenance tracking complete.")
    
    merged_df = ensure_record_ids(merged_df)
    print("Record ID validation complete.")
    
    # Execute deduplication
    print("\nExecuting deduplication process...")
    deduped_df = deduplicate_merged_data(merged_df)
    print("Deduplication complete.")
    
    # Print merge and deduplication results
    print(f'\nMerged Dataset Shape: {merged_df.shape}')
    print(f'Deduplicated Dataset Shape: {deduped_df.shape}')
    
    print('\nColumns:')
    print(deduped_df.columns.tolist())
    
    print('\nSource Distribution:')
    print(deduped_df['source'].value_counts())
    
    print('\nData Source Distribution:')
    print(deduped_df['data_source'].value_counts())
    
    print('\nSample Records:')
    print(deduped_df[['Agency Name', 'NameNormalized', 'source', 'data_source', 'RecordID', 'merged_from']].head().to_string())
    
    # Save merged and deduplicated datasets
    os.makedirs('../data/intermediate', exist_ok=True)
    
    merged_path = '../data/intermediate/merged_dataset.csv'
    merged_df.to_csv(merged_path, index=False)
    print(f'\nMerged dataset saved to: {merged_path}')
    
    deduped_path = '../data/intermediate/dedup_merged_dataset.csv'
    deduped_df.to_csv(deduped_path, index=False)
    print(f'Deduplicated dataset saved to: {deduped_path}')

if __name__ == '__main__':
    main() 