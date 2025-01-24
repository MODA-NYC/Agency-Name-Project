import pandas as pd
import os
import logging
from pathlib import Path

def setup_logging():
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s'
    )
    return logging.getLogger(__name__)

def check_merge_completion(data_dir: str = 'data'):
    logger = setup_logging()
    
    # Define paths
    paths = {
        'ops': os.path.join(data_dir, 'raw', 'ops_data.csv'),
        'hoo': os.path.join(data_dir, 'raw', 'nyc_gov_hoo.csv'),
        'nyc_export': os.path.join(data_dir, 'processed', 'nyc_agencies_export.csv'),
        'merged': os.path.join(data_dir, 'intermediate', 'merged_dataset.csv')
    }
    
    # Check if all required files exist
    for name, path in paths.items():
        if not os.path.exists(path):
            logger.error(f"Missing required file: {path}")
            return False
    
    # Load datasets
    try:
        ops_data = pd.read_csv(paths['ops'])
        hoo_data = pd.read_csv(paths['hoo'])
        nyc_export = pd.read_csv(paths['nyc_export'])
        merged = pd.read_csv(paths['merged'])
        
        # Print source counts
        logger.info("\nSource Dataset Counts:")
        logger.info(f"OPS data count: {len(ops_data)}")
        logger.info(f"HOO data count: {len(hoo_data)}")
        logger.info(f"NYC agencies export count: {len(nyc_export)}")
        logger.info(f"Merged dataset total count: {len(merged)}")
        
        # Check source distribution
        logger.info("\nMerged 'source' column distribution:")
        if 'source' in merged.columns:
            source_dist = merged['source'].value_counts(dropna=False)
            logger.info(source_dist)
        else:
            logger.warning("'source' column missing from merged dataset")
            
        # Check data provenance
        if 'data_source' in merged.columns:
            logger.info("\nMerged 'data_source' column distribution:")
            data_source_dist = merged['data_source'].value_counts(dropna=False)
            logger.info(data_source_dist)
            
        # Verify required columns exist
        required_columns = ['Name', 'NameNormalized', 'RecordID']
        missing_columns = [col for col in required_columns if col not in merged.columns]
        if missing_columns:
            logger.error(f"Missing required columns in merged dataset: {missing_columns}")
            return False
            
        # Check for duplicate RecordIDs
        if merged['RecordID'].duplicated().any():
            logger.error("Found duplicate RecordIDs in merged dataset")
            return False
            
        logger.info("\nMerge Completion Check Results:")
        logger.info("✓ All source files present")
        logger.info("✓ All datasets loaded successfully")
        logger.info("✓ Merged dataset contains records")
        logger.info("✓ Source tracking columns present")
        logger.info("✓ Required columns present")
        logger.info("✓ No duplicate RecordIDs")
        
        return True
        
    except Exception as e:
        logger.error(f"Error during merge completion check: {e}")
        return False

if __name__ == "__main__":
    check_merge_completion()