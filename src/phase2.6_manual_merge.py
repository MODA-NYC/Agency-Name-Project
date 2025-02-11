#!/usr/bin/env python
"""
Phase 2.6 Manual Merge and Final Export Script

This script:
1. Reads manual merge instructions from data/manual_overrides.csv.
2. Loads the final deduplicated dataset (from the clean dataset export).
3. Applies manual merge operations:
   - For each pair (PrimaryRecordID, SecondaryRecordID), merge secondary into primary.
   - Use field-level logic: preserve primary name, update blank fields, and merge Acronym into AlternateAcronyms.
   - Track manual merge history.
4. After merging, export the updated dataset in two forms:
   - The full dataset export (all fields) to data/exports/full_dataset.csv.
   - A clean dataset export:
       - Generate a cleaned version using create_clean_export() function.
       - Assign final stable IDs (format: FINAL_REC_XXXXXX).
       - Reorder columns as specified.
       - Save to data/exports/clean_dataset.csv and also to data/processed/final_deduplicated_dataset_manual_merged.csv.
"""

import os
import json
import logging
import pandas as pd
from pathlib import Path
from typing import Tuple, Optional, Dict, List
import argparse

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# File paths
DATA_DIR = Path("data")
PROCESSED_DIR = DATA_DIR / "processed"
EXPORTS_DIR = DATA_DIR / "exports"
INPUT_DATASET = PROCESSED_DIR / "final_deduplicated_dataset.csv"
MANUAL_OVERRIDES = DATA_DIR / "manual_overrides.csv"
OUTPUT_MANUAL_MERGED = PROCESSED_DIR / "final_deduplicated_dataset_manual_merged.csv"
FULL_EXPORT_PATH = EXPORTS_DIR / "full_dataset.csv"
CLEAN_EXPORT_PATH = EXPORTS_DIR / "clean_dataset.csv"

def create_clean_export(df: pd.DataFrame) -> pd.DataFrame:
    """
    Creates a clean export version of the dataset with standardized fields and handling of missing values.
    """
    clean_df = df.copy()
    
    # Fill missing values with empty strings for string columns
    string_columns = [
        'Name', 'NameAlphabetized', 'OperationalStatus', 'PreliminaryOrganizationType',
        'Description', 'URL', 'ParentOrganization', 'NYCReportingLine',
        'AuthorizingAuthority', 'LegalCitation', 'LegalCitationURL', 'LegalCitationText',
        'LegalName', 'AlternateNames', 'Acronym', 'AlternateAcronyms', 'BudgetCode',
        'OpenDatasetsURL', 'Notes', 'URISlug', 'NameWithAcronym', 'NameAlphabetizedWithAcronym',
        'RecordID', 'merged_from', 'data_source', 'Description-nyc.gov'
    ]
    
    # Add optional columns if they exist
    optional_columns = [
        'Ops_PrincipalOfficerName', 'Ops_URL', 'HOO_PrincipalOfficerName',
        'HOO_PrincipalOfficerTitle', 'HOO_PrincipalOfficerContactURL',
        'HOO_URL', 'Suggested_PrincipalOfficerName', 'PO_Name_Status',
        'URL_Status', 'Suggested_URL', 'PO_Notes', 'URL_Notes'
    ]
    
    # Add existing optional columns to string_columns
    for col in optional_columns:
        if col in clean_df.columns:
            string_columns.append(col)
    
    # Fill missing values for string columns that exist in the DataFrame
    for col in string_columns:
        if col in clean_df.columns:
            clean_df[col] = clean_df[col].fillna('')
    
    # Handle numeric columns if they exist
    numeric_columns = ['FoundingYear', 'SunsetYear']
    for col in numeric_columns:
        if col in clean_df.columns:
            clean_df[col] = pd.to_numeric(clean_df[col], errors='coerce')
    
    # Handle date columns if they exist
    date_columns = ['DateCreated', 'DateModified', 'LastVerifiedDate']
    for col in date_columns:
        if col in clean_df.columns:
            clean_df[col] = pd.to_datetime(clean_df[col], errors='coerce')
    
    return clean_df

def assign_final_ids(df: pd.DataFrame, id_prefix: str = "FINAL_REC_") -> pd.DataFrame:
    """
    Generate stable, unique RecordIDs for each record in the final dataset.
    The IDs are formatted as {id_prefix}{6-digit zero-padded index}.
    This function should be called after all merging, cleaning, and matching steps
    are complete, so that the final deduplicated dataset has consistent IDs for further processing.

    Args:
        df (pd.DataFrame): The clean, final deduplicated DataFrame.
        id_prefix (str): The prefix for the ID (default "FINAL_REC_").
        
    Returns:
        pd.DataFrame: The DataFrame with a new 'RecordID' column containing final unique IDs.
    """
    df = df.copy()
    df["RecordID"] = df.index.map(lambda i: f"{id_prefix}{i:06d}")
    return df

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

def export_datasets(df: pd.DataFrame):
    """
    Export the full dataset and the clean export.
    """
    # Create exports directory if it doesn't exist
    EXPORTS_DIR.mkdir(parents=True, exist_ok=True)
    
    # Save the full dataset with all fields
    df.to_csv(FULL_EXPORT_PATH, index=False)
    logger.info(f"Full dataset export saved to {FULL_EXPORT_PATH}. Row count: {len(df)}")
    
    # Create a clean export: process the dataset, assign final IDs, reorder columns
    clean_df = create_clean_export(df)
    clean_df = assign_final_ids(clean_df, id_prefix="FINAL_REC_")
    logger.info(f"Assigned final stable IDs to {len(clean_df)} records")
    
    # Define column order for clean export
    keep_order = [
        "RecordID",  # Add RecordID as first column
        "Name",
        "NameAlphabetized",
        "Name - HOO",
        "OperationalStatus",
        "PreliminaryOrganizationType",
        "Description",
        "URL",
        "AlternateNames",
        "Acronym",
        "AlternateAcronyms",
        "BudgetCode",
        "OpenDatasetsURL",
        "FoundingYear",
        "PrincipalOfficerName",
        "PrincipalOfficerTitle",
        "PrincipalOfficerContactURL",
        "Name - CPO",
        "Name - Checkbook",
        "Name - Greenbook",
        "Name - NYC Open Data Portal",
        "Name - NYC.gov Agency List",
        "Name - NYC.gov Mayor's Office",
        "Name - ODA",
        "Name - Ops",
        "Name - WeGov"
    ]
    clean_df = clean_df.reindex(columns=keep_order)
    
    # Save clean export
    clean_df.to_csv(CLEAN_EXPORT_PATH, index=False)
    logger.info(f"Clean dataset export saved to {CLEAN_EXPORT_PATH}. Row count: {len(clean_df)}")
    
    # Also save the clean export as the final manual merged dataset for backward compatibility
    clean_df.to_csv(OUTPUT_MANUAL_MERGED, index=False)
    logger.info(f"Clean dataset also saved to {OUTPUT_MANUAL_MERGED} for backward compatibility")

def main():
    """Main execution function"""
    logger.info("Starting Phase 2.6 Manual Merge and Export Process")
    
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
        
        # First assign final record IDs
        logger.info("Assigning final record IDs...")
        df = assign_final_ids(df, id_prefix="FINAL_REC_")
        
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
        
        # Export final datasets (full and clean exports)
        export_datasets(updated_df)
        
        logger.info("Phase 2.6 Manual Merge and Export Process completed successfully")
        
    except Exception as e:
        logger.error(f"Error during manual merge process: {e}")
        raise

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Apply manual merge instructions and export final datasets.")
    parser.add_argument('--log-level', type=str, default='INFO', help='Logging level')
    args = parser.parse_args()
    main() 