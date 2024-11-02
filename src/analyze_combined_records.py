import pandas as pd
import os
import logging

def setup_logging():
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s'
    )
    return logging.getLogger(__name__)

def analyze_dataset_records(df: pd.DataFrame, ops_data: pd.DataFrame, dataset_name: str, logger: logging.Logger):
    """Analyze records in a specific dataset."""
    # Find records with commas in the Name - Ops field
    combined_records = df[df['Name - Ops'].str.contains(',', na=False)]
    
    logger.info(f"\n=== Analysis of {dataset_name} ===")
    logger.info(f"Found {len(combined_records)} records with combined names")
    
    if not combined_records.empty:
        logger.info("\nCombined Records:")
        for _, row in combined_records.iterrows():
            logger.info(f"\nCombined Record:")
            logger.info(f"Name - Ops: {row['Name - Ops']}")
            logger.info(f"Original Name: {row['Name']}")
            logger.info(f"NameNormalized: {row['NameNormalized']}")
            
            # Split the combined names and check against original OPS data
            ops_names = [name.strip() for name in row['Name - Ops'].split(',')]
            logger.info("\nOriginal OPS records for these names:")
            for name in ops_names:
                ops_record = ops_data[ops_data['Agency Name'] == name]
                if not ops_record.empty:
                    logger.info(f"- {name}")
                    if len(ops_record) > 1:
                        logger.info("  (Multiple records in OPS data)")
    
    return combined_records

def analyze_combined_records(data_dir: str):
    logger = setup_logging()
    
    # Load datasets
    ops_data = pd.read_csv(os.path.join(data_dir, 'raw', 'ops_data.csv'))
    merged = pd.read_csv(os.path.join(data_dir, 'intermediate', 'merged_dataset.csv'))
    final = pd.read_csv(os.path.join(data_dir, 'processed', 'final_deduplicated_dataset.csv'))
    
    # Analyze both intermediate and final datasets
    merged_combined = analyze_dataset_records(merged, ops_data, "Intermediate Dataset", logger)
    final_combined = analyze_dataset_records(final, ops_data, "Final Deduplicated Dataset", logger)
    
    # Compare the differences
    if not merged_combined.empty or not final_combined.empty:
        logger.info("\n=== Comparison ===")
        if not merged_combined.empty and not final_combined.empty:
            merged_names = set(merged_combined['Name - Ops'])
            final_names = set(final_combined['Name - Ops'])
            
            new_combinations = final_names - merged_names
            if new_combinations:
                logger.info("\nNew combinations in final dataset:")
                for name in new_combinations:
                    logger.info(f"- {name}")
            
            removed_combinations = merged_names - final_names
            if removed_combinations:
                logger.info("\nCombinations removed in final dataset:")
                for name in removed_combinations:
                    logger.info(f"- {name}")
    
    # Save detailed analysis
    analysis_path = os.path.join(data_dir, 'analysis', 'combined_records_analysis.csv')
    if not final_combined.empty:
        final_combined.to_csv(analysis_path, index=False)
        logger.info(f"\nDetailed analysis saved to: {analysis_path}")
    
    # Also check for missing records
    ops_in_final = set()
    for name in final['Name - Ops'].dropna():
        ops_in_final.update([n.strip() for n in str(name).split(',')])
    
    missing_records = set(ops_data['Agency Name']) - ops_in_final
    if missing_records:
        logger.info("\n=== Missing Records ===")
        logger.info(f"Found {len(missing_records)} records from OPS data that are not in the final dataset:")
        for record in sorted(missing_records):
            logger.info(f"- {record}")

if __name__ == "__main__":
    analyze_combined_records('data')