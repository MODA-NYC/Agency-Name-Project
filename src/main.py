import os
import logging
import pandas as pd
import argparse
from typing import List
from preprocessing.ops_processor import OpsDataProcessor
from preprocessing.hoo_processor import HooDataProcessor
from matching.matcher import AgencyMatcher
from analysis.quality_checker import DataQualityChecker
from data_merging import merge_dataframes, clean_merged_data, track_data_provenance, ensure_record_ids

def validate_dataframe_columns(df: pd.DataFrame, required_cols: List[str], df_name: str) -> None:
    """Validate that required columns exist in DataFrame."""
    missing_cols = [col for col in required_cols if col not in df.columns]
    if missing_cols:
        raise ValueError(f"Missing required columns in {df_name}: {missing_cols}")

def main(data_dir: str, log_level: str, display: bool, save: bool):
    # Ensure output directories exist
    os.makedirs(os.path.join(data_dir, 'analysis'), exist_ok=True)
    os.makedirs(os.path.join(data_dir, 'processed'), exist_ok=True)
    os.makedirs(os.path.join(data_dir, 'intermediate'), exist_ok=True)
    
    # Setup logging
    logging.basicConfig(level=log_level)
    logger = logging.getLogger(__name__)
    
    try:
        # Initialize processors
        ops_processor = OpsDataProcessor()
        hoo_processor = HooDataProcessor()
        matcher = AgencyMatcher()
        quality_checker = DataQualityChecker(output_dir=os.path.join(data_dir, 'analysis'))
        
        # Validate input files exist
        required_files = {
            'ops_data': os.path.join(data_dir, 'raw', 'ops_data.csv'),
            'hoo_data': os.path.join(data_dir, 'raw', 'nyc_gov_hoo.csv'),
            'nyc_agencies': os.path.join(data_dir, 'processed', 'nyc_agencies_export.csv')
        }
        
        for name, path in required_files.items():
            if not os.path.exists(path):
                raise FileNotFoundError(f"Required file not found: {path}")
        
        # Process each data source with validation
        logger.info("Processing OPS data...")
        ops_data = ops_processor.process(required_files['ops_data'])
        validate_dataframe_columns(ops_data, ['Agency Name', 'NameNormalized', 'RecordID'], 'ops_data')
        
        logger.info("Processing HOO data...")
        hoo_data = hoo_processor.process(required_files['hoo_data'])
        validate_dataframe_columns(hoo_data, ['Agency Name', 'NameNormalized', 'RecordID'], 'hoo_data')
        
        # Load and validate primary data source
        nyc_agencies_export = pd.read_csv(required_files['nyc_agencies'])
        validate_dataframe_columns(nyc_agencies_export, ['Name'], 'nyc_agencies_export')
        
        # Merge datasets
        logger.info("Merging datasets...")
        
        # Define desired columns but handle missing ones
        hoo_desired_columns = ['Agency Name', 'NameNormalized', 'RecordID']
        hoo_optional_columns = ['HeadOfOrganizationName', 'HeadOfOrganizationTitle', 'HeadOfOrganizationURL']
        
        # Add optional columns that exist
        hoo_columns = hoo_desired_columns + [col for col in hoo_optional_columns if col in hoo_data.columns]
        
        secondary_dfs = [
            (hoo_data, hoo_columns, 'nyc_gov_'),
            (ops_data, ['Agency Name', 'NameNormalized', 'RecordID'], 'ops_')
        ]
        
        # Merge and clean
        merged_df = merge_dataframes(nyc_agencies_export, secondary_dfs)
        merged_df = clean_merged_data(merged_df)
        merged_df = track_data_provenance(merged_df)
        
        # Ensure NameNormalized and RecordID exist
        if 'NameNormalized' not in merged_df.columns:
            logger.warning("'NameNormalized' column missing, attempting to derive from 'Name'")
            if 'Name' in merged_df.columns:
                merged_df['NameNormalized'] = merged_df['Name'].astype(str).str.lower().str.replace('[^\w\s]', '', regex=True).str.strip()
            else:
                raise ValueError("No 'Name' column to derive 'NameNormalized' from.")
        
        merged_df = ensure_record_ids(merged_df, prefix='REC_')
        
        # Remove duplicates based on RecordID to avoid record inflation
        before_dedup = len(merged_df)
        merged_df = merged_df.drop_duplicates(subset=['RecordID'], keep='first')
        after_dedup = len(merged_df)
        if before_dedup != after_dedup:
            logger.info(f"Removed {before_dedup - after_dedup} duplicate records based on RecordID.")
        
        # Save merged dataset
        merged_path = os.path.join(data_dir, 'intermediate', 'merged_dataset.csv')
        merged_df.to_csv(merged_path, index=False)
        logger.info(f"Merged dataset saved to {merged_path}")
        
        # Run quality checks
        logger.info("Running quality checks...")
        quality_checker.analyze_dataset(merged_df, 'NameNormalized', 'merged_dataset')
        
        logger.info("Process completed successfully")
        
        if display:
            logger.info("\nMerged Data Sample:")
            print(merged_df.head())
        
    except Exception as e:
        logger.error(f"Error in main process: {e}")
        raise

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Process NYC organization data.")
    parser.add_argument('--data-dir', type=str, default='data', help='Base directory for data files')
    parser.add_argument('--log-level', type=str, default='INFO', help='Logging level')
    parser.add_argument('--display', action='store_true', help='Display the head of DataFrames')
    parser.add_argument('--save', action='store_true', help='Save intermediate results')
    
    args = parser.parse_args()
    main(args.data_dir, args.log_level, args.display, args.save)