#!/usr/bin/env python
"""
Phase 2.6 Manual Merge Script

This script reads a CSV file containing manual merge instructions (manual_overrides.csv)
and applies manual merge operations on the final deduplicated dataset.

The manual_overrides.csv file should have columns:
  - PrimaryRecordID: The RecordID of the primary record (to keep)
  - SecondaryRecordID: The RecordID of the secondary record (to merge into the primary)

Merge Logic:
  1. Keep the primary record's Name
  2. For each other field (except RecordID and Name):
     - If primary is blank and secondary is not, update primary with secondary's value
  3. For the Acronym field:
     - If secondary has an Acronym, add it to primary's AlternateAcronyms
     - If AlternateAcronyms is blank, set to secondary's Acronym
     - If populated, append with "; " separator
  4. Update manual_merge_history on primary to record secondary's RecordID
  5. Remove secondary record from dataset

The updated dataset is saved as "final_deduplicated_dataset_manual_merged.csv"
"""

import os
import json
import logging
import pandas as pd
from pathlib import Path
from typing import Tuple, Optional, Dict, List

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# File paths
DATA_DIR = Path("data")
PROCESSED_DIR = DATA_DIR / "processed"
INPUT_DATASET = PROCESSED_DIR / "final_deduplicated_dataset.csv"
MANUAL_OVERRIDES = DATA_DIR / "manual_overrides.csv"
OUTPUT_DATASET = PROCESSED_DIR / "final_deduplicated_dataset_manual_merged.csv"

def merge_records(primary: pd.Series, secondary: pd.Series) -> pd.Series:
    """
    Merge secondary record into primary record following the specified merge logic.
    
    Args:
        primary: Primary record to keep and update
        secondary: Secondary record to merge from
        
    Returns:
        Updated primary record with merged information
    """
    # Create a copy of the primary record to modify
    merged = primary.copy()
    
    # Skip these fields during the merge
    skip_fields = {'RecordID', 'Name', 'manual_merge_history'}
    
    # Update blank fields in primary with non-blank values from secondary
    for field in secondary.index:
        if field in skip_fields:
            continue
            
        if field == 'Acronym' and pd.notna(secondary[field]):
            # Special handling for Acronym field
            secondary_acronym = str(secondary[field]).strip()
            if pd.isna(merged.get('AlternateAcronyms')) or merged.get('AlternateAcronyms') == '':
                merged['AlternateAcronyms'] = secondary_acronym
            else:
                # Add secondary acronym if not already present
                existing_acronyms = [a.strip() for a in str(merged['AlternateAcronyms']).split(';')]
                if secondary_acronym not in existing_acronyms:
                    merged['AlternateAcronyms'] = f"{merged['AlternateAcronyms']}; {secondary_acronym}"
        else:
            # For other fields, update if primary is blank and secondary is not
            if pd.isna(merged.get(field)) and pd.notna(secondary[field]):
                merged[field] = secondary[field]
    
    # Update manual merge history
    history = []
    if pd.notna(merged.get('manual_merge_history')):
        try:
            history = json.loads(merged['manual_merge_history'])
        except (json.JSONDecodeError, TypeError):
            history = [str(merged['manual_merge_history'])]
    
    # Add secondary record to history
    history.append(str(secondary['RecordID']))
    merged['manual_merge_history'] = json.dumps(history)
    
    return merged

def validate_input_files() -> Tuple[bool, str]:
    """
    Validate that required input files exist and have required columns.
    
    Returns:
        Tuple of (success: bool, error_message: str)
    """
    # Check input dataset
    if not INPUT_DATASET.exists():
        return False, f"Input dataset not found at {INPUT_DATASET}"
        
    # Check manual overrides file
    if not MANUAL_OVERRIDES.exists():
        return False, f"Manual overrides file not found at {MANUAL_OVERRIDES}"
        
    try:
        # Validate manual overrides format
        overrides = pd.read_csv(MANUAL_OVERRIDES)
        required_columns = {'PrimaryRecordID', 'SecondaryRecordID'}
        missing_columns = required_columns - set(overrides.columns)
        if missing_columns:
            return False, f"Manual overrides file missing required columns: {missing_columns}"
    except Exception as e:
        return False, f"Error reading manual overrides file: {e}"
        
    return True, ""

def apply_manual_merges(df: pd.DataFrame, overrides_df: pd.DataFrame) -> Tuple[pd.DataFrame, Dict]:
    """
    Apply manual merge instructions to the dataset.
    
    Args:
        df: Input DataFrame to update
        overrides_df: DataFrame containing merge instructions
        
    Returns:
        Tuple of (updated DataFrame, statistics dictionary)
    """
    stats = {
        'total_instructions': len(overrides_df),
        'successful_merges': 0,
        'failed_merges': 0,
        'errors': []
    }
    
    # Create working copy of the dataset
    result_df = df.copy()
    
    # Process each merge instruction
    for idx, row in overrides_df.iterrows():
        primary_id = str(row['PrimaryRecordID'])
        secondary_id = str(row['SecondaryRecordID'])
        
        try:
            # Find the records
            primary_mask = result_df['RecordID'] == primary_id
            secondary_mask = result_df['RecordID'] == secondary_id
            
            if not primary_mask.any():
                raise ValueError(f"Primary record {primary_id} not found")
            if not secondary_mask.any():
                raise ValueError(f"Secondary record {secondary_id} not found")
            
            # Get the records
            primary_record = result_df[primary_mask].iloc[0]
            secondary_record = result_df[secondary_mask].iloc[0]
            
            # Merge the records
            merged_record = merge_records(primary_record, secondary_record)
            
            # Update the primary record - fixed the DataFrame update
            for col in merged_record.index:
                result_df.loc[primary_mask, col] = merged_record[col]
            
            # Remove the secondary record
            result_df = result_df[~secondary_mask].reset_index(drop=True)
            
            stats['successful_merges'] += 1
            logger.info(f"Successfully merged records {secondary_id} into {primary_id}")
            
        except Exception as e:
            stats['failed_merges'] += 1
            error_msg = f"Error merging {secondary_id} into {primary_id}: {str(e)}"
            stats['errors'].append(error_msg)
            logger.error(error_msg)
    
    return result_df, stats

def main():
    """Main execution function"""
    logger.info("Starting Phase 2.6 Manual Merge process")
    
    # Validate input files
    valid, error_msg = validate_input_files()
    if not valid:
        logger.error(f"Validation failed: {error_msg}")
        return
    
    try:
        # Load datasets
        logger.info("Loading datasets...")
        df = pd.read_csv(INPUT_DATASET)
        overrides_df = pd.read_csv(MANUAL_OVERRIDES)
        
        logger.info(f"Loaded {len(df)} records from input dataset")
        logger.info(f"Loaded {len(overrides_df)} manual merge instructions")
        
        # Apply manual merges
        updated_df, stats = apply_manual_merges(df, overrides_df)
        
        # Log statistics
        logger.info("\nMerge Statistics:")
        logger.info(f"Total merge instructions: {stats['total_instructions']}")
        logger.info(f"Successful merges: {stats['successful_merges']}")
        logger.info(f"Failed merges: {stats['failed_merges']}")
        logger.info(f"Records in final dataset: {len(updated_df)}")
        
        if stats['errors']:
            logger.info("\nErrors encountered:")
            for error in stats['errors']:
                logger.info(f"- {error}")
        
        # Save updated dataset
        logger.info(f"\nSaving updated dataset to {OUTPUT_DATASET}")
        PROCESSED_DIR.mkdir(parents=True, exist_ok=True)
        updated_df.to_csv(OUTPUT_DATASET, index=False)
        logger.info("Manual merge process completed successfully")
        
    except Exception as e:
        logger.error(f"Error during manual merge process: {e}")
        raise

if __name__ == "__main__":
    main() 