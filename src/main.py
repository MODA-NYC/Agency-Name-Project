import os
import logging
import pandas as pd
import argparse
from typing import List
from preprocessing.ops_processor import OpsDataProcessor
from preprocessing.hoo_processor import HooDataProcessor
from matching.matcher import AgencyMatcher
from analysis.quality_checker import DataQualityChecker
from data_merging import merge_dataframes, clean_merged_data, track_data_provenance

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
        validate_dataframe_columns(ops_data, ['Agency Name', 'NameNormalized'], 'ops_data')
        
        logger.info("Processing HOO data...")
        hoo_data = hoo_processor.process(required_files['hoo_data'])
        validate_dataframe_columns(hoo_data, ['Agency Name', 'NameNormalized'], 'hoo_data')
        
        # Load and validate primary data source
        nyc_agencies_export = pd.read_csv(required_files['nyc_agencies'])
        validate_dataframe_columns(nyc_agencies_export, ['Name'], 'nyc_agencies_export')
        
        # Merge datasets
        logger.info("Merging datasets...")
        
        # Define desired columns but handle missing ones
        hoo_desired_columns = ['Agency Name', 'NameNormalized']  # Start with required columns
        hoo_optional_columns = ['HeadOfOrganizationName', 'HeadOfOrganizationTitle', 'HeadOfOrganizationURL']
        
        # Add optional columns that exist
        hoo_columns = hoo_desired_columns + [col for col in hoo_optional_columns if col in hoo_data.columns]
        
        secondary_dfs = [
            (hoo_data, hoo_columns, 'nyc_gov_'),
            (ops_data, ['Agency Name', 'NameNormalized'], 'ops_')
        ]
        
        # Verify columns exist before merging
        for df, fields, prefix in secondary_dfs:
            missing_cols = [col for col in fields if col not in df.columns]
            if missing_cols:
                logger.warning(f"Missing columns in {prefix} dataset: {missing_cols}")
                # Use only available columns
                fields = [col for col in fields if col in df.columns]
            
        merged_df = merge_dataframes(nyc_agencies_export, secondary_dfs)
        merged_df = clean_merged_data(merged_df)
        merged_df = track_data_provenance(merged_df)
        
        # Save merged dataset
        if save:
            merged_path = os.path.join(data_dir, 'intermediate', 'merged_dataset.csv')
            merged_df.to_csv(merged_path, index=False)
            logger.info(f"Merged dataset saved to {merged_path}")
        
        # Find and save matches
        logger.info("Finding matches...")
        matches = matcher.find_matches(ops_data, hoo_data)
        
        # Load existing matches
        matches_path = os.path.join(data_dir, 'processed', 'consolidated_matches.csv')
        try:
            existing_matches = pd.read_csv(matches_path)
        except FileNotFoundError:
            existing_matches = pd.DataFrame(columns=['Source', 'Target', 'Score', 'Label', 'SourceID', 'TargetID'])
        
        # Convert new matches to DataFrame and append
        if matches:  # Only process if there are new matches
            logger.info(f"Found {len(matches)} potential new matches")
            new_matches = pd.DataFrame(matches)
            logger.info("Converting matches to DataFrame...")
            
            # Add detailed logging for match processing
            logger.info("Processing new matches:")
            logger.info(f"Columns before processing: {new_matches.columns.tolist()}")
            
            new_matches = new_matches.rename(columns={
                'source_id': 'SourceID',
                'target_id': 'TargetID',
                'score': 'Score'
            })
            logger.info(f"Columns after renaming: {new_matches.columns.tolist()}")
            
            # Add Label column
            new_matches['Label'] = new_matches['Score'].apply(lambda x: 'Match' if x >= 95 else '')
            logger.info(f"Added Label column. Sample of matches:\n{new_matches.head()}")
            
            # Validate columns
            new_matches = validate_match_columns(new_matches)
            
            # Combine with existing matches
            combined_matches = pd.concat([existing_matches, new_matches], ignore_index=True)
            combined_matches = validate_match_columns(combined_matches)
            
            # Remove duplicates
            combined_matches = combined_matches.drop_duplicates(
                subset=['Source', 'Target'], 
                keep='first'
            )
            
            # Save updated matches
            combined_matches.to_csv(matches_path, index=False)
            logger.info(f"Updated matches saved to {matches_path}")
        
        # Display if requested
        if display:
            logger.info("\nMerged Data Sample:")
            print(merged_df.head())
            logger.info(f"\nTotal matches found: {len(matches)}")
        
        # Run quality checks
        logger.info("Running quality checks...")
        quality_checker.analyze_dataset(merged_df, 'NameNormalized', 'merged_dataset')
        
        # After merging
        required_columns = [
            'Name',
            'NameNormalized',
            'Name - Ops',
            'Name - NYC.gov Redesign',
            'RecordID'
        ]
        
        missing_cols = [col for col in required_columns if col not in merged_df.columns]
        if missing_cols:
            logger.warning(f"Missing required columns in merged dataset: {missing_cols}")
        
        logger.info("Process completed successfully")
        
    except Exception as e:
        logger.error(f"Error in main process: {e}")
        raise

def validate_match_columns(matches_df):
    """Validate and fix column names in matches DataFrame."""
    required_columns = ['Source', 'Target', 'Score', 'Label', 'SourceID', 'TargetID']
    
    # Check for missing required columns
    missing_cols = [col for col in required_columns if col not in matches_df.columns]
    if missing_cols:
        logging.warning(f"Missing required columns: {missing_cols}")
        for col in missing_cols:
            matches_df[col] = None
    
    # Check for extra columns
    extra_cols = [col for col in matches_df.columns if col not in required_columns]
    if extra_cols:
        logging.warning(f"Removing extra columns: {extra_cols}")
        matches_df = matches_df[required_columns]
    
    return matches_df

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Process NYC organization data.")
    parser.add_argument('--data-dir', type=str, default='data', help='Base directory for data files')
    parser.add_argument('--log-level', type=str, default='INFO', help='Logging level')
    parser.add_argument('--display', action='store_true', help='Display the head of DataFrames')
    parser.add_argument('--save', action='store_true', help='Save intermediate results')
    
    args = parser.parse_args()
    main(args.data_dir, args.log_level, args.display, args.save)
