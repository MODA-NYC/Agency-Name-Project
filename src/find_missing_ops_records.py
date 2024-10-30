import pandas as pd
import logging

def analyze_ops_records():
    # Set up logging
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s'
    )
    
    # Load the datasets
    logging.info("Loading datasets...")
    try:
        ops_data = pd.read_csv('data/raw/ops_data.csv')
        dedup_data = pd.read_csv('data/processed/final_deduplicated_dataset.csv')
        
        logging.info(f"Loaded {len(ops_data)} records from ops_data.csv")
        logging.info(f"Loaded {len(dedup_data)} records from final_deduplicated_dataset.csv")
        
        # Count records with 'Name - Ops' in deduplicated dataset
        ops_count_dedup = dedup_data['Name - Ops'].notna().sum()
        logging.info(f"\nRecords with 'Name - Ops' in deduplicated dataset: {ops_count_dedup}")
        
        # Create set of ops names that appear in deduplicated dataset
        matched_ops_names = set()
        for name in dedup_data['Name - Ops'].dropna():
            matched_ops_names.update([n.strip() for n in name.split(',')])
        
        logging.info(f"Unique Ops names in deduplicated dataset: {len(matched_ops_names)}")
        
        # Find records in ops_data that don't appear in deduplicated dataset
        missing_records = ops_data[~ops_data['Agency Name'].isin(matched_ops_names)]
        
        # Find records that appear multiple times
        duplicate_check = ops_data['Agency Name'].value_counts()
        duplicates = duplicate_check[duplicate_check > 1]
        
        if len(duplicates) > 0:
            logging.info("\nDuplicate agency names in ops_data.csv:")
            print(duplicates)
        
        # Save missing records to CSV
        output_path = 'data/processed/missing_ops_records.csv'
        missing_records.to_csv(output_path, index=False)
        
        logging.info(f"\nFound {len(missing_records)} missing records")
        logging.info(f"Results saved to {output_path}")
        
        # Display all missing records
        if len(missing_records) > 0:
            logging.info("\nAll missing records:")
            print(missing_records[['Agency Name', 'Agency Acronym', 'Entity type']].to_string())
            
        # Sample of matched records for verification
        logging.info("\nSample of matched records (first 5):")
        matched_records = ops_data[ops_data['Agency Name'].isin(matched_ops_names)]
        print(matched_records[['Agency Name', 'Agency Acronym', 'Entity type']].head().to_string())
            
    except Exception as e:
        logging.error(f"Error processing files: {e}")

if __name__ == "__main__":
    analyze_ops_records() 