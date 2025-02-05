#!/usr/bin/env python
import pandas as pd
import logging

# Set up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def main():
    # Load the datasets
    try:
        nyc_gov = pd.read_csv('data/raw/nyc_gov_agency_list.csv')
        final_dataset = pd.read_csv('data/processed/final_deduplicated_dataset_initial_release.csv')
        
        logger.info(f"Loaded {len(nyc_gov)} records from NYC.gov agency list")
        logger.info(f"Loaded {len(final_dataset)} records from final dataset")
        
        # Get list of agencies that have descriptions in final dataset
        matched = final_dataset[final_dataset['Description-nyc.gov'].notna()]
        unmatched = nyc_gov[~nyc_gov['Name - NYC.gov Agency List'].isin(matched['Name - NYC.gov Agency List'])]
        
        logger.info(f"\nFound {len(unmatched)} unmatched descriptions:")
        logger.info("\nUnmatched Agencies:")
        for _, row in unmatched.iterrows():
            logger.info(f"- {row['Name - NYC.gov Agency List']}")
            
        # Save unmatched records to CSV for further analysis
        unmatched.to_csv('data/analysis/unmatched_nyc_gov_descriptions.csv', index=False)
        logger.info(f"\nSaved unmatched records to data/analysis/unmatched_nyc_gov_descriptions.csv")
        
    except Exception as e:
        logger.error(f"Error analyzing matches: {e}")
        raise

if __name__ == "__main__":
    main() 