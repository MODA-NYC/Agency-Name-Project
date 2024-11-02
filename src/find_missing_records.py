import pandas as pd
import os
import logging

def setup_logging():
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s'
    )
    return logging.getLogger(__name__)

def find_missing_records(data_dir: str):
    logger = setup_logging()
    
    # Load datasets
    ops_data = pd.read_csv(os.path.join(data_dir, 'raw', 'ops_data.csv'))
    hoo_data = pd.read_csv(os.path.join(data_dir, 'raw', 'nyc_gov_hoo.csv'))
    merged = pd.read_csv(os.path.join(data_dir, 'intermediate', 'merged_dataset.csv'))
    
    # Find missing OPS records
    ops_in_merged = set(merged[merged['Name - Ops'].notna()]['Name - Ops'])
    ops_original = set(ops_data['Agency Name'])
    missing_ops = ops_original - ops_in_merged
    
    # Find missing HOO records
    hoo_in_merged = set(merged[merged['Name - NYC.gov Redesign'].notna()]['Name - NYC.gov Redesign'])
    hoo_original = set(hoo_data['Agency Name'])
    missing_hoo = hoo_original - hoo_in_merged
    
    # Check for duplicates in source data
    ops_duplicates = ops_data[ops_data['Agency Name'].duplicated(keep=False)]
    hoo_duplicates = hoo_data[hoo_data['Agency Name'].duplicated(keep=False)]
    
    # Print results
    logger.info("\n=== Missing Records Analysis ===")
    
    logger.info("\nMissing OPS Records:")
    for record in missing_ops:
        logger.info(f"- {record}")
        if record in ops_duplicates['Agency Name'].values:
            logger.info("  (This is a duplicate in OPS data)")
    
    logger.info("\nMissing HOO Records:")
    for record in missing_hoo:
        logger.info(f"- {record}")
        if record in hoo_duplicates['Agency Name'].values:
            logger.info("  (This is a duplicate in HOO data)")
    
    return {
        'missing_ops': missing_ops,
        'missing_hoo': missing_hoo,
        'ops_duplicates': ops_duplicates,
        'hoo_duplicates': hoo_duplicates
    }

def main():
    find_missing_records('data')

if __name__ == "__main__":
    main() 