#!/usr/bin/env python
import os
import logging
import pandas as pd
import argparse
from pathlib import Path

def load_nyc_gov_descriptions(data_dir: str) -> pd.DataFrame:
    """
    Load the NYC.gov agency descriptions from the scraped data.
    
    Args:
        data_dir (str): Base directory for data files
        
    Returns:
        pd.DataFrame: DataFrame containing agency names and descriptions
    """
    descriptions_path = os.path.join(data_dir, 'raw', 'nyc_gov_agency_list.csv')
    try:
        df_desc = pd.read_csv(descriptions_path)
        return df_desc[['Name - NYC.gov Agency List', 'Description-nyc.gov']]
    except Exception as e:
        logging.warning(f"Could not load NYC.gov descriptions from {descriptions_path}: {e}")
        return pd.DataFrame(columns=['Name - NYC.gov Agency List', 'Description-nyc.gov'])

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

    # Drop the original Description field
    if 'Description' in df.columns:
        df = df.drop('Description', axis=1)
        logger.info("Dropped original Description field")

    # Load NYC.gov descriptions
    df_desc = load_nyc_gov_descriptions(data_dir)
    if not df_desc.empty:
        logger.info(f"Loaded {len(df_desc)} NYC.gov descriptions")
        
        # Merge descriptions based on Name - NYC.gov Agency List
        # Use left merge to keep all records from main dataset
        df = pd.merge(
            df,
            df_desc,
            on='Name - NYC.gov Agency List',
            how='left'
        )
        logger.info(f"Merged in {df['Description-nyc.gov'].notna().sum()} NYC.gov descriptions")
        
        # Rename Description-nyc.gov to Description
        df = df.rename(columns={'Description-nyc.gov': 'Description'})
        logger.info("Renamed Description-nyc.gov field to Description")

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
        
        # Log description statistics
        desc_count = df['Description'].notna().sum()
        logger.info(f"Final dataset has {desc_count} records with descriptions ({desc_count/len(df)*100:.1f}%)")
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