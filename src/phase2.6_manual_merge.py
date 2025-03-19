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
INPUT_DATASET = EXPORTS_DIR / "clean_dataset.csv"
MANUAL_OVERRIDES = DATA_DIR / "manual_overrides.csv"
OUTPUT_MANUAL_MERGED = PROCESSED_DIR / "final_deduplicated_dataset_manual_merged.csv"
FULL_EXPORT_PATH = EXPORTS_DIR / "full_dataset.csv"
CLEAN_EXPORT_PATH = EXPORTS_DIR / "clean_dataset.csv"

def create_clean_export(df: pd.DataFrame) -> pd.DataFrame:
    """
    Creates a clean export version of the dataset with standardized fields and handling of missing values.
    """
    clean_df = df.copy()
    
    # Add InstanceOf column if it doesn't exist
    if 'InstanceOf' not in clean_df.columns:
        clean_df['InstanceOf'] = ''
        logger.info("Added 'InstanceOf' column with blank values")
    
    # Fill missing values with empty strings for string columns
    string_columns = [
        'Name', 'NameAlphabetized', 'OperationalStatus', 'PreliminaryOrganizationType',
        'Description', 'URL', 'ParentOrganization', 'NYCReportingLine',
        'AuthorizingAuthority', 'LegalCitation', 'LegalCitationURL', 'LegalCitationText',
        'LegalName', 'AlternateNames', 'Acronym', 'AlternateAcronyms', 'BudgetCode',
        'OpenDatasetsURL', 'Notes', 'URISlug', 'NameWithAcronym', 'NameAlphabetizedWithAcronym',
        'RecordID', 'merged_from', 'data_source', 'Description-nyc.gov', 'InstanceOf'
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
    Generate stable, unique RecordIDs for each record in the final dataset, preserving existing IDs.
    The IDs are formatted as {id_prefix}{6-digit zero-padded index}.
    This function preserves existing IDs that match the prefix pattern and only generates
    new IDs for records that don't have them.

    Args:
        df (pd.DataFrame): The clean, final deduplicated DataFrame.
        id_prefix (str): The prefix for the ID (default "FINAL_REC_").
        
    Returns:
        pd.DataFrame: The DataFrame with a 'RecordID' column containing final unique IDs.
    """
    df = df.copy()
    
    # Check if RecordID column exists, if not create it
    if 'RecordID' not in df.columns:
        logger.info("RecordID column not found, creating it...")
        df['RecordID'] = ""
    
    # Helper function to check if a RecordID follows the expected pattern
    def has_valid_id(record_id):
        if pd.isna(record_id) or not isinstance(record_id, str):
            return False
        return record_id.startswith(id_prefix) and len(record_id) == len(id_prefix) + 6 and record_id[len(id_prefix):].isdigit()
    
    # Identify records that already have valid IDs
    has_valid_id_mask = df['RecordID'].apply(has_valid_id)
    valid_id_count = has_valid_id_mask.sum()
    logger.info(f"Found {valid_id_count} records with valid IDs that will be preserved")
    
    # Find the highest existing ID number to continue from
    max_id_num = 0
    if has_valid_id_mask.any():
        existing_ids = df.loc[has_valid_id_mask, 'RecordID']
        existing_nums = [int(id[len(id_prefix):]) for id in existing_ids if has_valid_id(id)]
        if existing_nums:
            max_id_num = max(existing_nums)
            logger.info(f"Highest existing ID number: {max_id_num}")
    
    # Generate new IDs only for records that need them
    next_id = max_id_num + 1
    new_id_count = (~has_valid_id_mask).sum()
    if new_id_count > 0:
        logger.info(f"Generating new IDs for {new_id_count} records starting from {id_prefix}{next_id:06d}")
        for idx in df[~has_valid_id_mask].index:
            df.loc[idx, 'RecordID'] = f"{id_prefix}{next_id:06d}"
            next_id += 1
    
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
    
    # Verify RecordID column exists
    if 'RecordID' not in result_df.columns:
        error_msg = "RecordID column missing from dataset. Cannot apply merges."
        logger.error(error_msg)
        stats['errors'].append(error_msg)
        return result_df, stats
    
    # Process each merge instruction
    for idx, row in overrides_df.iterrows():
        primary_id = str(row['PrimaryRecordID'])
        secondary_id = str(row['SecondaryRecordID'])
        
        try:
            # Find the records
            primary_mask = result_df['RecordID'] == primary_id
            secondary_mask = result_df['RecordID'] == secondary_id
            
            if not primary_mask.any():
                # If we can't find the primary record, log the error and continue
                error_msg = f"Primary record {primary_id} not found by ID, merge will be skipped"
                logger.warning(error_msg)
                stats['failed_merges'] += 1
                stats['errors'].append(error_msg)
                continue
                
            if not secondary_mask.any():
                # If we can't find the secondary record, log the error and continue
                error_msg = f"Secondary record {secondary_id} not found by ID, merge will be skipped"
                logger.warning(error_msg)
                stats['failed_merges'] += 1
                stats['errors'].append(error_msg)
                continue
            
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

def apply_single_record_updates(df: pd.DataFrame, overrides_df: pd.DataFrame) -> Tuple[pd.DataFrame, Dict]:
    """
    Apply single-record updates from the manual overrides file.
    These are records where we want to update fields without merging with another record.
    
    Args:
        df: Input DataFrame to update
        overrides_df: DataFrame containing update instructions
        
    Returns:
        Tuple of (updated DataFrame, statistics dictionary)
    """
    stats = {
        'total_updates': 0,
        'successful_updates': 0,
        'failed_updates': 0,
        'errors': []
    }
    
    # Create working copy of the dataset
    result_df = df.copy()
    
    # Verify RecordID column exists
    if 'RecordID' not in result_df.columns:
        error_msg = "RecordID column missing from dataset. Cannot apply updates."
        logger.error(error_msg)
        stats['errors'].append(error_msg)
        return result_df, stats
    
    # Process each update instruction that has no SecondaryRecordID
    for idx, row in overrides_df[overrides_df['SecondaryRecordID'].isna()].iterrows():
        primary_id = str(row['PrimaryRecordID'])
        stats['total_updates'] += 1
        
        try:
            # Find the record
            record_mask = result_df['RecordID'] == primary_id
            
            if not record_mask.any():
                # If we can't find the record by ID, log the error but continue
                error_msg = f"Record {primary_id} not found by ID, update will be skipped"
                logger.warning(error_msg)
                stats['failed_updates'] += 1
                stats['errors'].append(error_msg)
                continue
            
            # Apply updates based on Notes field
            if pd.notna(row.get('Notes')):
                update_instructions = json.loads(row['Notes'])
                
                for field, value in update_instructions.items():
                    if field == 'AlternateNames':
                        # Special handling for AlternateNames - append to existing
                        current_value = result_df.loc[record_mask, field].iloc[0]
                        if pd.isna(current_value) or current_value == '':
                            new_value = value
                        else:
                            new_value = f"{current_value}; {value}"
                        result_df.loc[record_mask, field] = new_value
                    else:
                        # Direct update for other fields
                        result_df.loc[record_mask, field] = value
            
            stats['successful_updates'] += 1
            logger.info(f"Successfully updated record {primary_id}")
            
        except Exception as e:
            stats['failed_updates'] += 1
            error_msg = f"Error updating {primary_id}: {str(e)}"
            stats['errors'].append(error_msg)
            logger.error(error_msg)
    
    return result_df, stats

def add_nyc_gov_agency_directory_flag(df: pd.DataFrame) -> pd.DataFrame:
    """
    Add a new column 'NYC.gov Agency Directory' with True/False values based on specified criteria.
    
    The criteria for inclusion in the NYC.gov Agency Directory are:
    1. OperationalStatus IS Active
    2. AND PreliminaryOrganizationType IS NOT Non-Governmental Organization
    3. AND PreliminaryOrganizationType IS NOT Division (except for specified divisions)
    4. AND PreliminaryOrganizationType IS NOT Judiciary
    5. AND PreliminaryOrganizationType IS NOT Financial Institution
    6. AND URL DOES NOT CONTAIN "ny.gov"
    7. AND (URL IS NOT empty OR PrincipalOfficerName IS NOT empty OR PrincipalOfficerContactURL IS NOT empty)
    
    Special case: Include Divisions with the following Names:
    - 311
    - Department of Homeless Services
    - Human Resources Administration
    
    Args:
        df: Input DataFrame to update
        
    Returns:
        DataFrame with the new flag column
    """
    # Make a copy to avoid modifying the original
    result_df = df.copy()
    
    # Ensure required columns exist and convert to strings if they're not already
    required_columns = ['OperationalStatus', 'PreliminaryOrganizationType', 'URL', 
                        'PrincipalOfficerName', 'PrincipalOfficerContactURL', 'Name']
    
    for col in required_columns:
        if col not in result_df.columns:
            result_df[col] = ''  # Add empty column if it doesn't exist
        else:
            result_df[col] = result_df[col].fillna('').astype(str)
    
    # Initialize the flag column as False
    result_df['NYC.gov Agency Directory'] = False
    
    # List of division names to include as exceptions
    exception_divisions = ['311', 'Department of Homeless Services', 'Human Resources Administration']
    
    # Apply the criteria to set the flag to True where conditions are met
    mask = (
        (result_df['OperationalStatus'] == 'Active') &
        ~(result_df['PreliminaryOrganizationType'] == 'Non-Governmental Organization') &
        ~(
            (result_df['PreliminaryOrganizationType'] == 'Division') & 
            ~result_df['Name'].isin(exception_divisions)
        ) &
        ~(result_df['PreliminaryOrganizationType'] == 'Judiciary') &
        ~(result_df['PreliminaryOrganizationType'] == 'Financial Institution') &
        ~(result_df['URL'].str.contains('ny.gov', case=False)) &
        (
            (result_df['URL'] != '') |
            (result_df['PrincipalOfficerName'] != '') |
            (result_df['PrincipalOfficerContactURL'] != '')
        )
    )
    
    # Set the flag to True where the mask is True
    result_df.loc[mask, 'NYC.gov Agency Directory'] = True
    
    # Log statistics about the flagged records
    flagged_count = result_df['NYC.gov Agency Directory'].sum()
    logger.info(f"Added NYC.gov Agency Directory flag: {flagged_count} out of {len(result_df)} records flagged for inclusion")
    
    return result_df

def export_datasets(df: pd.DataFrame):
    """
    Export the full dataset and the clean export.
    """
    # Create exports directory if it doesn't exist
    EXPORTS_DIR.mkdir(parents=True, exist_ok=True)
    
    # Add the NYC.gov Agency Directory flag
    df = add_nyc_gov_agency_directory_flag(df)
    
    # Save the full dataset with all fields
    df.to_csv(FULL_EXPORT_PATH, index=False, encoding='utf-8')
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
        "Notes",
        "InstanceOf",  # Add the new InstanceOf column
        "NYC.gov Agency Directory",
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
    
    # Make sure we only include columns that exist in the DataFrame
    keep_order = [col for col in keep_order if col in clean_df.columns]
    
    clean_df = clean_df.reindex(columns=keep_order)
    
    # Save clean export
    clean_df.to_csv(CLEAN_EXPORT_PATH, index=False, encoding='utf-8')
    logger.info(f"Clean dataset export saved to {CLEAN_EXPORT_PATH}. Row count: {len(clean_df)}")
    
    # Also save the clean export as the final manual merged dataset for backward compatibility
    clean_df.to_csv(OUTPUT_MANUAL_MERGED, index=False, encoding='utf-8')
    logger.info(f"Clean dataset also saved to {OUTPUT_MANUAL_MERGED} for backward compatibility")

def add_new_records(df: pd.DataFrame, overrides_df: pd.DataFrame) -> Tuple[pd.DataFrame, Dict]:
    """
    Add new records from the manual overrides file when the record ID doesn't exist in the dataset.
    
    Args:
        df: Input DataFrame to update
        overrides_df: DataFrame containing record instructions
        
    Returns:
        Tuple of (updated DataFrame with new records, statistics dictionary)
    """
    stats = {
        'total_new_records': 0,
        'successful_additions': 0,
        'failed_additions': 0,
        'errors': []
    }
    
    # Create working copy of the dataset
    result_df = df.copy()
    
    # Get all record IDs that exist in overrides but not in the dataset
    existing_record_ids = set(result_df['RecordID'].astype(str))
    potential_new_record_ids = set(overrides_df['PrimaryRecordID'][overrides_df['SecondaryRecordID'].isna()].astype(str))
    new_record_ids = potential_new_record_ids - existing_record_ids
    
    if not new_record_ids:
        logger.info("No new records to add")
        return result_df, stats
    
    stats['total_new_records'] = len(new_record_ids)
    logger.info(f"Found {len(new_record_ids)} new records to add")
    
    # Add each new record
    for record_id in new_record_ids:
        try:
            # Get the override instruction for this record
            override_row = overrides_df[overrides_df['PrimaryRecordID'] == record_id].iloc[0]
            
            # Parse the Notes field to get the record attributes
            if pd.isna(override_row.get('Notes')):
                stats['failed_additions'] += 1
                error_msg = f"No Notes field for new record {record_id}, cannot create record"
                stats['errors'].append(error_msg)
                logger.warning(error_msg)
                continue
                
            try:
                record_attrs = json.loads(override_row['Notes'])
            except json.JSONDecodeError:
                stats['failed_additions'] += 1
                error_msg = f"Invalid JSON in Notes field for record {record_id}"
                stats['errors'].append(error_msg)
                logger.warning(error_msg)
                continue
            
            # Create a new record
            new_record = pd.Series({'RecordID': record_id})
            
            # Add all attributes from the Notes field
            for key, value in record_attrs.items():
                new_record[key] = value
            
            # Append the new record to the DataFrame
            result_df = pd.concat([result_df, pd.DataFrame([new_record])], ignore_index=True)
            
            stats['successful_additions'] += 1
            logger.info(f"Successfully added new record {record_id}")
            
        except Exception as e:
            stats['failed_additions'] += 1
            error_msg = f"Error adding new record {record_id}: {str(e)}"
            stats['errors'].append(error_msg)
            logger.error(error_msg)
    
    return result_df, stats

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
        df = pd.read_csv(INPUT_DATASET, encoding='utf-8')
        overrides_df = pd.read_csv(MANUAL_OVERRIDES, encoding='utf-8')
        
        logger.info(f"Loaded {len(df)} records from input dataset")
        logger.info(f"Loaded {len(overrides_df)} manual override instructions")
        
        # Verify RecordID column exists in the input dataset
        if 'RecordID' not in df.columns:
            logger.error("Error: RecordID column not found in input dataset. Please use a dataset that already has record IDs.")
            return
        
        # First add new records that don't exist yet
        logger.info("Adding new records if they don't exist...")
        df, add_stats = add_new_records(df, overrides_df)
        
        # Then apply single-record updates (using existing record IDs)
        logger.info("Applying single-record updates...")
        df, update_stats = apply_single_record_updates(df, overrides_df)
        
        # Then apply manual merges for records that need to be merged
        logger.info("Applying manual merges...")
        merge_overrides = overrides_df[overrides_df['SecondaryRecordID'].notna()]
        updated_df, merge_stats = apply_manual_merges(df, merge_overrides)
        
        # Last step: ensure all records have valid IDs (preserving existing ones)
        logger.info("Ensuring all records have valid Record IDs (preserving existing IDs)...")
        updated_df = assign_final_ids(updated_df, id_prefix="FINAL_REC_")
        
        # Log statistics
        logger.info("\nAddition Statistics:")
        logger.info(f"Total new records: {add_stats['total_new_records']}")
        logger.info(f"Successfully added: {add_stats['successful_additions']}")
        logger.info(f"Failed additions: {add_stats['failed_additions']}")
        
        logger.info("\nUpdate Statistics:")
        logger.info(f"Total single-record updates: {update_stats['total_updates']}")
        logger.info(f"Successful updates: {update_stats['successful_updates']}")
        logger.info(f"Failed updates: {update_stats['failed_updates']}")
        
        logger.info("\nMerge Statistics:")
        logger.info(f"Total merge instructions: {merge_stats['total_instructions']}")
        logger.info(f"Successful merges: {merge_stats['successful_merges']}")
        logger.info(f"Failed merges: {merge_stats['failed_merges']}")
        logger.info(f"Records in final dataset: {len(updated_df)}")
        
        if add_stats['errors'] or update_stats['errors'] or merge_stats['errors']:
            logger.info("\nErrors encountered:")
            for error in add_stats['errors'] + update_stats['errors'] + merge_stats['errors']:
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