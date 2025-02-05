#!/usr/bin/env python
import os
import logging
import pandas as pd
import argparse

def main(data_dir: str, input_filename: str, output_filename: str):
    # Set up logging
    logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
    logger = logging.getLogger(__name__)
    
    # Define input and output file paths
    input_path = os.path.join(data_dir, 'processed', input_filename)
    output_path = os.path.join(data_dir, 'processed', output_filename)
    
    # Load the final deduplicated dataset
    try:
        df = pd.read_csv(input_path)
        initial_count = len(df)
        logger.info(f"Loaded {initial_count} records from {input_path}")
    except Exception as e:
        logger.error(f"Failed to load dataset from {input_path}: {e}")
        raise

    # Filter out records based on OperationalStatus and PreliminaryOrganizationType
    if 'OperationalStatus' in df.columns:
        df = df[df['OperationalStatus'] != 'Reorganized']
        removed_count = initial_count - len(df)
        logger.info(f"Removed {removed_count} records with OperationalStatus = 'Reorganized'")
    
    if 'PreliminaryOrganizationType' in df.columns:
        exclusion_types = ["Judiciary", "Fiduciary Fund", "Financial Institution"]
        current_count = len(df)
        df = df[~df['PreliminaryOrganizationType'].isin(exclusion_types)]
        removed_count = current_count - len(df)
        logger.info(f"Removed {removed_count} records with PreliminaryOrganizationType in {exclusion_types}")

    # Define the columns to keep in the desired order
    columns_to_keep = [
        'Name',
        'NameAlphabetized',
        'Name - HOO',
        'URL',
        'Description',
        'PrincipalOfficerName',
        'PrincipalOfficerTitle',
        'ParentOrganization',
        'AlternateNames',
        'Acronym',
        'AlternateAcronyms',
        'BudgetCode',
        'OpenDatasetsURL',
        'FoundingYear',
        'Name - Checkbook',
        'Name - Greenbook',
        'Name - NYC Open Data Portal',
        'Name - NYC.gov Agency List',
        'Name - NYC.gov Mayor\'s Office',
        'Name - Ops',
        'Name - WeGov'
    ]

    # Verify all required columns exist
    missing_columns = [col for col in columns_to_keep if col not in df.columns]
    if missing_columns:
        logger.error(f"Missing required columns: {missing_columns}")
        raise ValueError(f"Missing required columns: {missing_columns}")

    # Keep only the specified columns in the desired order
    df = df[columns_to_keep]
    logger.info(f"Kept {len(columns_to_keep)} columns in specified order")
    
    # Save the resulting dataset to a new CSV file
    try:
        df.to_csv(output_path, index=False)
        logger.info(f"Saved initial release dataset with {len(df)} records to {output_path}")
    except Exception as e:
        logger.error(f"Failed to save dataset to {output_path}: {e}")
        raise

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Create initial release output from final deduplicated dataset by keeping specific columns and filtering records.")
    parser.add_argument('--data-dir', type=str, default='data', help='Base directory for data files')
    parser.add_argument('--input', type=str, default='final_deduplicated_dataset.csv', help='Input CSV filename in data/processed')
    parser.add_argument('--output', type=str, default='final_deduplicated_dataset_initial_release.csv', help='Output CSV filename in data/processed')
    
    args = parser.parse_args()
    main(args.data_dir, args.input, args.output) 