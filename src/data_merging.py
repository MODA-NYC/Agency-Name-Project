import pandas as pd
from matching.normalizer import NameNormalizer
import logging
from typing import List, Tuple, Dict, Optional
import json
import os

def merge_dataframes(primary_df: pd.DataFrame, ops_df: pd.DataFrame, hoo_df: pd.DataFrame) -> pd.DataFrame:
    """Merge the three dataframes while preserving original names and tracking provenance."""
    
    # Create copies to avoid modifying originals
    primary = primary_df.copy()
    ops = ops_df.copy()
    hoo = hoo_df.copy()
    
    # Store original names before merging
    ops['OPS_Name'] = ops['Agency Name']
    hoo['HOO_Name'] = hoo['AgencyNameEnriched'] if 'AgencyNameEnriched' in hoo.columns else hoo['Agency Name']
    
    logging.info(f"Sample HOO_Name values before merge: {hoo['HOO_Name'].head().tolist()}")
    logging.info(f"Sample HOO normalized names: {hoo['NameNormalized'].head().tolist()}")
    logging.info(f"Sample primary normalized names: {primary['NameNormalized'].head().tolist()}")
    
    # Count potential matches
    hoo_matches = len(set(hoo['NameNormalized']).intersection(set(primary['NameNormalized'])))
    ops_matches = len(set(ops['NameNormalized']).intersection(set(primary['NameNormalized'])))
    logging.info(f"Potential HOO matches: {hoo_matches}")
    logging.info(f"Potential OPS matches: {ops_matches}")
    
    # Ensure all dataframes have NameNormalized column
    for df in [primary, ops, hoo]:
        if 'NameNormalized' not in df.columns:
            raise ValueError("All dataframes must have NameNormalized column")
    
    # Define columns to keep from each source
    ops_cols = ['RecordID', 'NameNormalized', 'OPS_Name', 'Agency Name', 'Entity type', 'source']
    hoo_cols = ['RecordID', 'NameNormalized', 'HOO_Name', 'Agency Name', 'HeadOfOrganizationName', 'HeadOfOrganizationTitle', 'source', 'AgencyNameEnriched']
    
    # First merge: Primary and OPS (outer join)
    merged = pd.merge(
        primary,
        ops[ops_cols],
        how='outer',
        on='NameNormalized',
        suffixes=('', '_ops'),
        indicator='merge_ops'
    )
    
    # For records only in OPS, copy OPS_Name to Name
    ops_only_mask = merged['merge_ops'] == 'right_only'
    merged.loc[ops_only_mask, 'Name'] = merged.loc[ops_only_mask, 'OPS_Name']
    
    logging.info(f"OPS merge stats:\n{merged['merge_ops'].value_counts()}")
    logging.info(f"Columns after OPS merge: {merged.columns.tolist()}")
    
    # Second merge: Result with HOO (outer join)
    merged = pd.merge(
        merged,
        hoo[hoo_cols],
        how='outer',
        on='NameNormalized',
        suffixes=('', '_hoo'),
        indicator='merge_hoo'
    )
    
    # For records only in HOO, use AgencyNameEnriched or HOO_Name for Name
    hoo_only_mask = merged['merge_hoo'] == 'right_only'
    merged.loc[hoo_only_mask, 'Name'] = (
        merged.loc[hoo_only_mask, 'AgencyNameEnriched'].fillna(
            merged.loc[hoo_only_mask, 'HOO_Name']
        )
    )
    
    logging.info(f"HOO merge stats:\n{merged['merge_hoo'].value_counts()}")
    logging.info(f"Columns after HOO merge: {merged.columns.tolist()}")
    
    # Track merge history
    merged['merged_from'] = merged.apply(
        lambda row: track_merge_history(row, 'RecordID', 'RecordID_ops', 'RecordID_hoo'),
        axis=1
    )
    
    # Clean up merge artifacts but preserve source-specific names and enriched names
    cols_to_drop = [col for col in merged.columns if col.endswith(('_ops', '_hoo')) 
                   and col not in ['OPS_Name', 'HOO_Name', 'AgencyNameEnriched']]
    
    # Log columns being dropped and preserved
    logging.info(f"Columns being dropped: {cols_to_drop}")
    logging.info(f"OPS_Name and HOO_Name present before cleanup: {['OPS_Name' in merged.columns, 'HOO_Name' in merged.columns]}")
    
    result = merged.drop(columns=cols_to_drop + ['merge_ops', 'merge_hoo'])
    
    # Create Name - Ops and Name - HOO columns
    result['Name - Ops'] = result['OPS_Name']
    result['Name - HOO'] = result['AgencyNameEnriched'].fillna(result['HOO_Name'])
    
    # Initialize data_source column if it doesn't exist
    if 'data_source' not in result.columns:
        result['data_source'] = None
    
    # Set data_source based on the actual source of data
    def determine_source(row):
        if pd.notna(row.get('Name - Ops')):
            return 'ops'
        elif pd.notna(row.get('Name - HOO')):
            return 'hoo'
        elif pd.notna(row.get('Name')):
            return 'nyc_agencies_export'
        return 'unknown'
    
    result['data_source'] = result.apply(determine_source, axis=1)
    
    # Remove rows where Name is NaN and no source-specific names exist
    result = result[~(result['Name'].isna() & result['Name - Ops'].isna() & result['Name - HOO'].isna())]
    
    # Fill in Name where it's NaN but we have a source-specific name
    name_fill_mask = result['Name'].isna()
    result.loc[name_fill_mask & result['Name - Ops'].notna(), 'Name'] = result.loc[name_fill_mask & result['Name - Ops'].notna(), 'Name - Ops']
    result.loc[name_fill_mask & result['Name - HOO'].notna(), 'Name'] = result.loc[name_fill_mask & result['Name - HOO'].notna(), 'Name - HOO']
    
    # Drop the original OPS_Name and HOO_Name columns as we now have Name - Ops and Name - HOO
    result = result.drop(columns=['OPS_Name', 'HOO_Name', 'AgencyNameEnriched'], errors='ignore')
    
    # Log presence of columns after cleanup
    logging.info(f"OPS_Name and HOO_Name present after cleanup: {['OPS_Name' in result.columns, 'HOO_Name' in result.columns]}")
    logging.info(f"Columns after cleanup: {result.columns.tolist()}")
    
    # Log source distribution
    logging.info("\nRecord distribution by source:")
    logging.info(result['source'].value_counts().to_string())
    
    return result

def track_merge_history(row, primary_id_col, ops_id_col, hoo_id_col):
    """Track which records were merged together."""
    merged_ids = []
    
    if pd.notna(row.get(ops_id_col)):
        merged_ids.append({"id": row[ops_id_col], "name": row.get('OPS_Name'), "score": None})
    
    if pd.notna(row.get(hoo_id_col)):
        merged_ids.append({"id": row[hoo_id_col], "name": row.get('HOO_Name'), "score": None})
    
    return merged_ids if merged_ids else None

def clean_merged_data(df: pd.DataFrame) -> pd.DataFrame:
    """Clean up merge artifacts and ensure consistent columns."""
    
    # Remove duplicate ID columns but keep OPS_Name and HOO_Name
    cols_to_drop = [col for col in df.columns if col.endswith(('_ops', '_hoo')) 
                    and col not in ['OPS_Name', 'HOO_Name']]
    
    # Log columns being dropped and preserved
    logging.info(f"Columns being dropped: {cols_to_drop}")
    logging.info(f"OPS_Name and HOO_Name present before cleanup: {['OPS_Name' in df.columns, 'HOO_Name' in df.columns]}")
    
    result = df.drop(columns=cols_to_drop)
    
    # Log presence of columns after cleanup
    logging.info(f"OPS_Name and HOO_Name present after cleanup: {['OPS_Name' in result.columns, 'HOO_Name' in result.columns]}")
    
    return result

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
    
    # Ensure OPS_Name and HOO_Name columns exist
    for col in ['OPS_Name', 'HOO_Name']:
        if col not in result_df.columns:
            result_df[col] = None
    
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
                
                # Special handling for OPS_Name and HOO_Name
                for col in ['OPS_Name', 'HOO_Name']:
                    values = [r[col] for r in [keep_record] + [r for _, r in merge_records.iterrows()] if pd.notna(r.get(col))]
                    if values:
                        combined_record[col] = '|'.join(set(values))
                        fields_preserved.append(col)
                
                # For each field in merge_records, fill in nulls in combined_record
                for field in result_df.columns:
                    if field not in ['RecordID', 'source', 'data_source', 'merged_from', 'merge_note', 'dedup_source', 'OPS_Name', 'HOO_Name']:
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