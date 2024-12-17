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

def main():
    logger = setup_logging()
    
    # Get the project root directory
    project_root = Path(__file__).parent
    
    # Define paths
    merged_path = project_root / 'data' / 'intermediate' / 'merged_dataset.csv'
    
    try:
        if not merged_path.exists():
            raise FileNotFoundError(f"The merged dataset does not exist at: {merged_path}")
        
        merged = pd.read_csv(merged_path)
        logger.info(f"Successfully loaded merged dataset with {len(merged)} records")
        
    except FileNotFoundError as e:
        logger.error(f"Error: {e}")
        logger.error("Please ensure the merged_dataset.csv file exists in the data/intermediate directory")
        exit(1)
    except Exception as e:
        logger.error(f"Unexpected error: {e}")
        exit(1)

if __name__ == "__main__":
    main() 