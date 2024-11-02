import pandas as pd
import os
import logging
from typing import Set, Dict, List

def setup_logging():
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s'
    )
    return logging.getLogger(__name__)

def split_combined_names(name: str) -> List[str]:
    """Split combined names that were joined with commas."""
    if pd.isna(name):
        return []
    return [n.strip() for n in str(name).split(',')]

def analyze_merge_completeness(data_dir: str):
    logger = setup_logging()
    
    # Load datasets
    ops_data = pd.read_csv(os.path.join(data_dir, 'raw', 'ops_data.csv'))
    hoo_data = pd.read_csv(os.path.join(data_dir, 'raw', 'nyc_gov_hoo.csv'))
    final_data = pd.read_csv(os.path.join(data_dir, 'processed', 'final_deduplicated_dataset.csv'))
    
    # Expand combined names in final dataset
    ops_in_final = set()
    for name in final_data[final_data['Name - Ops'].notna()]['Name - Ops']:
        ops_in_final.update(split_combined_names(name))
    
    hoo_in_final = set()
    for name in final_data[final_data['Name - NYC.gov Redesign'].notna()]['Name - NYC.gov Redesign']:
        hoo_in_final.update(split_combined_names(name))
    
    ops_original = set(ops_data['Agency Name'].dropna())
    hoo_original = set(hoo_data['Agency Name'].dropna())
    
    # Find duplicates in source data
    ops_duplicates = ops_data[ops_data['Agency Name'].duplicated(keep=False)]['Agency Name'].unique()
    hoo_duplicates = hoo_data[hoo_data['Agency Name'].duplicated(keep=False)]['Agency Name'].unique()
    
    # Analyze missing records
    missing_ops = ops_original - ops_in_final
    missing_hoo = hoo_original - hoo_in_final
    
    # Print analysis
    logger.info("\n=== Merge Completeness Analysis ===")
    
    logger.info(f"\nOPS Data Statistics:")
    logger.info(f"Total OPS records: {len(ops_original)}")
    logger.info(f"OPS records in final dataset: {len(ops_in_final)}")
    logger.info(f"Missing OPS records: {len(missing_ops)}")
    
    logger.info(f"\nHOO Data Statistics:")
    logger.info(f"Total HOO records: {len(hoo_original)}")
    logger.info(f"HOO records in final dataset: {len(hoo_in_final)}")
    logger.info(f"Missing HOO records: {len(missing_hoo)}")
    
    logger.info("\nMissing OPS Records (with duplicate status):")
    for record in sorted(missing_ops):
        is_duplicate = record in ops_duplicates
        logger.info(f"- {record}")
        if is_duplicate:
            logger.info("  (This is a duplicate in OPS source data)")
    
    logger.info("\nMissing HOO Records (with duplicate status):")
    for record in sorted(missing_hoo):
        is_duplicate = record in hoo_duplicates
        logger.info(f"- {record}")
        if is_duplicate:
            logger.info("  (This is a duplicate in HOO source data)")
    
    # Save detailed analysis to CSV
    detailed_analysis = []
    
    for record in missing_ops:
        detailed_analysis.append({
            'Source': 'OPS',
            'Missing Record': record,
            'Is Duplicate in Source': record in ops_duplicates
        })
    
    for record in missing_hoo:
        detailed_analysis.append({
            'Source': 'HOO',
            'Missing Record': record,
            'Is Duplicate in Source': record in hoo_duplicates
        })
    
    analysis_df = pd.DataFrame(detailed_analysis)
    analysis_path = os.path.join(data_dir, 'analysis', 'missing_records_analysis.csv')
    analysis_df.to_csv(analysis_path, index=False)
    logger.info(f"\nDetailed analysis saved to: {analysis_path}")

if __name__ == "__main__":
    analyze_merge_completeness('data')