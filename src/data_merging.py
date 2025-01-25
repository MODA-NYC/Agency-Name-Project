import pandas as pd
from matching.normalizer import NameNormalizer
import logging
from typing import List, Tuple, Dict, Optional
import json
import os

def merge_dataframes(primary_df: pd.DataFrame, secondary_dfs: List[Tuple[pd.DataFrame, List[str], str]]) -> pd.DataFrame:
    """Merge primary dataframe with all secondary dataframes.
    
    Args:
        primary_df: Primary DataFrame to merge with
        secondary_dfs: List of tuples (DataFrame, fields_to_keep, prefix)
    """
    
    # Define standard fields to keep from secondary dataframes
    standard_fields = [
        'RecordID',
        'Agency Name',
        'NameNormalized'
    ]

    # Initialize merged_df with primary data
    merged_df = primary_df.copy()
    
    # If primary df doesn't have Agency Name, try to derive it from Name
    if 'Agency Name' not in merged_df.columns and 'Name' in merged_df.columns:
        merged_df['Agency Name'] = merged_df['Name']
    
    # Add source column to primary df
    merged_df['source'] = 'primary'
    
    # Process each secondary dataframe
    for df, additional_fields, source_prefix in secondary_dfs:
        # Combine standard and additional fields
        fields_to_keep = list(set(standard_fields + additional_fields))
        
        # Ensure all required fields exist
        for field in fields_to_keep:
            if field not in df.columns:
                logging.warning(f"Field {field} not found in {source_prefix} dataset")
                fields_to_keep.remove(field)
        
        df_to_merge = df[fields_to_keep].copy()
        
        # Add source column using the prefix
        df_to_merge['source'] = source_prefix.rstrip('_')  # Remove trailing underscore if present
        
        # Concatenate with merged_df
        merged_df = pd.concat([merged_df, df_to_merge], ignore_index=True)
    
    return merged_df

def clean_merged_data(df):
    """Clean the merged dataset by handling missing values and removing invalid records.
    
    Args:
        df (pd.DataFrame): The merged DataFrame to clean
        
    Returns:
        pd.DataFrame: The cleaned DataFrame
    """
    # Remove records with no identifying information
    df = df.dropna(subset=['Name', 'Agency Name'], how='all')
    
    # Handle URL conflicts
    if 'HeadOfOrganizationURL' in df.columns and 'URL' in df.columns:
        df['URL'] = df['URL'].fillna(df['HeadOfOrganizationURL'])
        df = df.drop('HeadOfOrganizationURL', axis=1)
    
    # Fill missing Agency Name from Name if available
    df.loc[df['Agency Name'].isna(), 'Agency Name'] = df.loc[df['Agency Name'].isna(), 'Name']
    
    # Fill missing NameNormalized using normalizer
    normalizer = NameNormalizer()
    df.loc[df['NameNormalized'].isna(), 'NameNormalized'] = df.loc[df['NameNormalized'].isna(), 'Agency Name'].apply(normalizer.normalize)
    
    return df

def track_data_provenance(df: pd.DataFrame) -> pd.DataFrame:
    """Enhanced data source tracking with improved logic"""
    def get_source(row):
        # Check source-specific name columns first
        if pd.notna(row.get('Name - Ops')):
            return 'ops'
        if pd.notna(row.get('Name - NYC.gov Redesign')):
            return 'nyc_gov'
        if pd.notna(row.get('Name')):
            return 'nyc_agencies_export'
        
        # Fall back to source column
        if pd.notna(row.get('source')):
            return row['source']
        
        return 'unknown'
    
    # Add/update data_source column
    df['data_source'] = df.apply(get_source, axis=1)
    
    # Log source distribution
    source_dist = df['data_source'].value_counts()
    logging.info("\nData source distribution:")
    for source, count in source_dist.items():
        logging.info(f"{source}: {count} records")
    
    return df

def ensure_record_ids(df: pd.DataFrame, prefix: str = 'REC_') -> pd.DataFrame:
    """Ensure all records have proper IDs, creating them if necessary."""
    if 'RecordID' not in df.columns:
        df['RecordID'] = [f"{prefix}{i:06d}" for i in range(len(df))]
    else:
        # Fix invalid IDs
        mask = ~df['RecordID'].astype(str).str.match(r'^[A-Z]+_\d{6}$', na=True)
        if mask.any():
            # Reassign these invalid IDs
            invalid_count = mask.sum()
            logging.info(f"Fixing {invalid_count} invalid RecordIDs")
            new_ids = [f"{prefix}{i:06d}" for i in range(invalid_count)]
            df.loc[mask, 'RecordID'] = new_ids
    
    # Verify no duplicate IDs
    duplicates = df['RecordID'].duplicated()
    if duplicates.any():
        logging.warning(f"Found {duplicates.sum()} duplicate RecordIDs")
        # Regenerate IDs for duplicates
        df.loc[duplicates, 'RecordID'] = [f"{prefix}{i:06d}" for i in range(duplicates.sum())]
    
    return df

def deduplicate_merged_data(df: pd.DataFrame) -> pd.DataFrame:
    """
    Deduplicate the merged dataset based on RecordID and NameNormalized.
    Preserves non-null values from merged records.
    
    Args:
        df (pd.DataFrame): The merged DataFrame to deduplicate
        
    Returns:
        pd.DataFrame: The deduplicated DataFrame with additional metadata columns
    """
    # Initialize logging and audit data
    logging.info("\nStarting deduplication process...")
    audit_data = {
        'total_records_before': len(df),
        'duplicate_groups': 0,
        'records_merged': 0,
        'rules_applied': {},
        'fields_preserved': {}
    }
    merged_records = []
    
    # Create working copy
    result_df = df.copy()
    result_df['merged_from'] = None
    result_df['merge_note'] = None
    result_df['dedup_source'] = result_df['source']
    
    # Check for RecordID duplicates (should not happen)
    recordid_dupes = result_df[result_df['RecordID'].duplicated(keep=False)]
    if not recordid_dupes.empty:
        logging.warning(f"Found {len(recordid_dupes)} duplicate RecordIDs!")
        for rid in recordid_dupes['RecordID'].unique():
            logging.warning(f"Duplicate RecordID {rid}:")
            logging.warning(recordid_dupes[recordid_dupes['RecordID'] == rid][['RecordID', 'Agency Name', 'source']].to_string())
    
    # Find duplicates by NameNormalized
    name_dupes = result_df[result_df['NameNormalized'].duplicated(keep=False)].sort_values('NameNormalized')
    if not name_dupes.empty:
        logging.info(f"\nFound {len(name_dupes)} records with duplicate normalized names")
        audit_data['duplicate_groups'] = len(name_dupes['NameNormalized'].unique())
        
        # Process each group of duplicates
        for name in name_dupes['NameNormalized'].unique():
            group = result_df[result_df['NameNormalized'] == name].copy()
            
            # Rule 1: Source Preference
            if len(group['source'].unique()) > 1:
                # Define source preference order
                source_order = ['nyc_agencies_export', 'ops', 'nyc_gov']
                
                # Find the highest priority source present in the group
                for source in source_order:
                    if source in group['source'].values:
                        preferred_source = source
                        break
                else:
                    preferred_source = group['source'].iloc[0]  # fallback
                
                # Keep record from preferred source
                keep_record = group[group['source'] == preferred_source].iloc[0]
                merge_records = group[group['source'] != preferred_source]
                
                # Create a combined record starting with the preferred record
                combined_record = keep_record.copy()
                fields_preserved = []
                
                # For each field in merge_records, fill in nulls in combined_record
                for field in result_df.columns:
                    if field not in ['RecordID', 'source', 'data_source', 'merged_from', 'merge_note', 'dedup_source']:
                        for _, merge_record in merge_records.iterrows():
                            if pd.isna(combined_record[field]) and pd.notna(merge_record[field]):
                                combined_record[field] = merge_record[field]
                                fields_preserved.append(field)
                
                # Update metadata
                merged_ids = merge_records['RecordID'].tolist()
                fields_note = f" (preserved fields: {', '.join(set(fields_preserved))})" if fields_preserved else ""
                note = f"Kept {preferred_source} record over {', '.join(merge_records['source'].unique())}{fields_note}"
                
                # Update the result DataFrame with the combined record
                for field in result_df.columns:
                    result_df.loc[result_df['RecordID'] == keep_record['RecordID'], field] = combined_record[field]
                
                # Update merge metadata
                result_df.loc[result_df['RecordID'] == keep_record['RecordID'], 'merged_from'] = str(merged_ids)
                result_df.loc[result_df['RecordID'] == keep_record['RecordID'], 'merge_note'] = note
                
                # Mark merged records for removal
                result_df = result_df[~result_df['RecordID'].isin(merged_ids)]
                
                # Log the merge
                merged_records.append({
                    'kept_record': keep_record['RecordID'],
                    'merged_records': merged_ids,
                    'normalized_name': name,
                    'rule_applied': 'source_preference',
                    'fields_preserved': fields_preserved,
                    'note': note
                })
                
                # Update audit data
                audit_data['records_merged'] += len(merged_ids)
                audit_data['rules_applied']['source_preference'] = audit_data['rules_applied'].get('source_preference', 0) + 1
                
                # Track preserved fields
                for field in set(fields_preserved):
                    audit_data['fields_preserved'][field] = audit_data['fields_preserved'].get(field, 0) + 1
    
    # Save audit trail
    audit_data['total_records_after'] = len(result_df)
    audit_data['total_records_merged'] = audit_data['total_records_before'] - audit_data['total_records_after']
    
    # Create audit directory if it doesn't exist
    os.makedirs('data/analysis', exist_ok=True)
    
    # Save deduplication summary
    with open('data/analysis/dedup_summary.json', 'w') as f:
        json.dump(audit_data, f, indent=2)
    
    # Save detailed merge log
    if merged_records:
        pd.DataFrame(merged_records).to_csv('data/analysis/merged_records_log.csv', index=False)
    
    # Log summary
    logging.info("\nDeduplication Summary:")
    logging.info(f"Records before: {audit_data['total_records_before']}")
    logging.info(f"Records after: {audit_data['total_records_after']}")
    logging.info(f"Total records merged: {audit_data['total_records_merged']}")
    logging.info(f"Duplicate groups found: {audit_data['duplicate_groups']}")
    logging.info("\nRules applied:")
    for rule, count in audit_data['rules_applied'].items():
        logging.info(f"- {rule}: {count} times")
    if audit_data['fields_preserved']:
        logging.info("\nFields preserved from merged records:")
        for field, count in audit_data['fields_preserved'].items():
            logging.info(f"- {field}: {count} times")
    
    return result_df