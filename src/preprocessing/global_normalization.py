import os
import logging
import pandas as pd
from data_normalization import global_normalize_name

"""
Global Normalization Script (Task A3)

This script applies a global normalization pass to the merged dataset, ensuring
consistent normalization of all records now that all sources are integrated.

Steps:
1. Load the merged dataset from data/intermediate/merged_dataset.csv
2. Apply refined normalization rules to 'NameNormalized' column.
3. Save the globally normalized dataset to data/processed/global_normalized_dataset.csv

Refinements include:
- Ensuring 'new york city' is preserved rather than removed.
- Confirming whether certain stopwords (like 'and') should remain to avoid losing essential detail.
- Removing parentheses and certain punctuation uniformly.
- Re-expanding abbreviations if needed.
- Logging actions and leaving TODOs for future adjustments if necessary.
"""

def main():
    logging.basicConfig(level=logging.INFO)
    logger = logging.getLogger(__name__)

    input_path = 'data/intermediate/merged_dataset.csv'
    output_path = 'data/processed/global_normalized_dataset.csv'
    
    if not os.path.exists(input_path):
        logger.error(f"Input file not found: {input_path}")
        return
    
    df = pd.read_csv(input_path)
    
    if 'NameNormalized' not in df.columns:
        logger.warning("'NameNormalized' column not found. Attempting to derive from 'Name' column.")
        if 'Name' in df.columns:
            # If Name exists, attempt to normalize from scratch
            df['NameNormalized'] = df['Name'].fillna('').apply(global_normalize_name)
        else:
            logger.error("No 'Name' or 'NameNormalized' column found. Cannot proceed with global normalization.")
            return
    else:
        # Re-apply global normalization to ensure consistency
        df['NameNormalized'] = df['NameNormalized'].fillna('').apply(global_normalize_name)
    
    # Filter out blank and NaN rows
    df = df[df['NameNormalized'].notna() & (df['NameNormalized'].str.strip() != '')]
    
    # TODO: Future improvement: Additional heuristics or acronym handling if needed.
    
    # Save the globally normalized dataset
    df.to_csv(output_path, index=False)
    logger.info(f"Global normalization complete. Output saved to {output_path}")

if __name__ == "__main__":
    main()