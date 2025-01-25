import pandas as pd
import logging
import json
import os
from pathlib import Path
from typing import Dict, List, Tuple, Set
from difflib import SequenceMatcher
import re

def setup_logging() -> logging.Logger:
    """Configure logging with appropriate format and level"""
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s'
    )
    return logging.getLogger(__name__)

def load_and_validate_inputs() -> Tuple[pd.DataFrame, pd.DataFrame]:
    """
    Load and validate the deduplicated dataset and verified matches.
    
    Returns:
        Tuple[pd.DataFrame, pd.DataFrame]: (deduplicated dataset, verified matches)
    """
    logger = logging.getLogger(__name__)
    
    try:
        # Load deduplicated dataset
        dedup_path = '../data/intermediate/dedup_merged_dataset.csv'
        logger.info(f"Loading deduplicated dataset from {dedup_path}")
        dedup_df = pd.read_csv(dedup_path)
        
        # Load consolidated matches
        matches_path = '../data/processed/consolidated_matches.csv'
        logger.info(f"Loading consolidated matches from {matches_path}")
        matches_df = pd.read_csv(matches_path)
        
        # Filter for verified matches
        matches_df = matches_df[matches_df['Label'] == 'Match'].copy()
        logger.info(f"Found {len(matches_df)} verified matches")
        
        # Validate required fields
        required_fields = ['RecordID', 'Agency Name', 'NameNormalized', 'source']
        missing_fields = [f for f in required_fields if f not in dedup_df.columns]
        if missing_fields:
            raise ValueError(f"Missing required fields in dataset: {missing_fields}")
            
        required_match_fields = ['Source', 'Target', 'Score', 'SourceID', 'TargetID']
        missing_match_fields = [f for f in required_match_fields if f not in matches_df.columns]
        if missing_match_fields:
            raise ValueError(f"Missing required fields in matches: {missing_match_fields}")
        
        return dedup_df, matches_df
        
    except FileNotFoundError as e:
        logger.error(f"Could not find input file: {e}")
        raise
    except Exception as e:
        logger.error(f"Error loading inputs: {e}")
        raise

def merge_record_information(preferred_record: pd.Series, 
                           secondary_record: pd.Series,
                           metadata_fields: Set[str]) -> Tuple[pd.Series, List[str]]:
    """
    Merge information from two records, preserving non-null values from both.
    Special handling for source-specific name fields to ensure original names are preserved.
    
    Args:
        preferred_record: The primary record to keep
        secondary_record: The secondary record to merge information from
        metadata_fields: Set of field names that are metadata (not to be merged)
        
    Returns:
        Tuple[pd.Series, List[str]]: (merged record, list of preserved fields)
    """
    merged_record = preferred_record.copy()
    fields_preserved = []
    
    # Handle source-specific name fields first
    # Map source fields to their corresponding name fields
    source_name_mapping = {
        'hoo': ['Name - HOO', 'Name_HOO'],
        'ops': ['Name - Ops', 'Name_OPS']
    }
    
    # Ensure these fields exist in the merged record
    for fields in source_name_mapping.values():
        for field in fields:
            if field not in merged_record:
                merged_record[field] = None
    
    # Preserve original names based on source
    for record, source_type in [(preferred_record, preferred_record['source']), 
                               (secondary_record, secondary_record['source'])]:
        source_key = source_type.lower()
        if 'hoo' in source_key:
            source_key = 'hoo'
        elif 'ops' in source_key:
            source_key = 'ops'
        
        if source_key in source_name_mapping:
            original_name = record.get('Name') or record.get('Agency Name')
            if pd.notna(original_name):
                for field in source_name_mapping[source_key]:
                    merged_record[field] = original_name
                    fields_preserved.append(field)
    
    # For each remaining field in secondary record
    for field in secondary_record.index:
        if (field not in [f for fields in source_name_mapping.values() for f in fields] and 
            field not in metadata_fields):
            if pd.isna(merged_record[field]) and pd.notna(secondary_record[field]):
                merged_record[field] = secondary_record[field]
                fields_preserved.append(field)
                
    return merged_record, fields_preserved

def update_record_metadata(record: pd.Series,
                         merged_from_record: pd.Series,
                         fields_preserved: List[str]) -> pd.Series:
    """
    Update metadata for a merged record.
    
    Args:
        record: The record to update
        merged_from_record: The record being merged in
        fields_preserved: List of fields preserved from merged record
        
    Returns:
        pd.Series: Updated record with new metadata
    """
    # Update merged_from list
    if pd.isna(record['merged_from']):
        record['merged_from'] = [merged_from_record['RecordID']]
    else:
        # If it's a string representation of a list, eval it and append
        current_merged = eval(record['merged_from'])
        current_merged.append(merged_from_record['RecordID'])
        record['merged_from'] = current_merged
        
    # Update merge note
    fields_note = f" (preserved fields: {', '.join(fields_preserved)})" if fields_preserved else ""
    new_note = f"Merged with {merged_from_record['RecordID']} from {merged_from_record['source']}{fields_note}"
    
    if pd.isna(record['merge_note']):
        record['merge_note'] = new_note
    else:
        record['merge_note'] = f"{record['merge_note']}; {new_note}"
        
    return record

def normalize_name_for_matching(name: str) -> List[str]:
    """
    Generate multiple normalized versions of a name for matching.
    
    Args:
        name: Original name string
        
    Returns:
        List[str]: List of normalized name variations
    """
    name = str(name).lower().strip()
    
    variations = {name}  # Use set to avoid duplicates
    
    # Basic normalization
    basic = re.sub(r'[^\w\s]', ' ', name)
    basic = re.sub(r'\s+', ' ', basic).strip()
    variations.add(basic)
    
    # Common abbreviation expansions
    abbrev_map = {
        'dept': 'department',
        'admin': 'administration',
        'comm': 'commission',
        'nyc': 'new york city',
        'ny': 'new york',
        'tech': 'technology',
        'info': 'information',
        'dev': 'development',
        'svcs': 'services',
        'mgmt': 'management'
    }
    
    expanded = basic
    for abbrev, full in abbrev_map.items():
        expanded = re.sub(r'\b' + abbrev + r'\b', full, expanded)
    variations.add(expanded)
    
    # Different word orders
    words = basic.split()
    if len(words) > 1:
        # Move first word to end
        variations.add(' '.join(words[1:] + [words[0]]))
        # Move last word to front
        variations.add(' '.join([words[-1]] + words[:-1]))
    
    return list(variations)

def find_matching_record(name: str, df: pd.DataFrame, threshold: float = 0.85) -> pd.DataFrame:
    """
    Find matching records using multiple matching strategies.
    
    Args:
        name: Name to match
        df: DataFrame to search in
        threshold: Minimum similarity score for fuzzy matching
        
    Returns:
        pd.DataFrame: Matching records
    """
    # Try exact matches first
    name_variations = normalize_name_for_matching(name)
    for variation in name_variations:
        matches = df[df['NameNormalized'].str.lower() == variation]
        if not matches.empty:
            return matches
            
    # If no exact matches, try fuzzy matching
    best_score = 0
    best_matches = pd.DataFrame()
    
    for _, row in df.iterrows():
        max_score = 0
        normalized = str(row['NameNormalized']).lower()
        
        # Try each variation against the normalized name
        for variation in name_variations:
            score = SequenceMatcher(None, variation, normalized).ratio()
            max_score = max(max_score, score)
        
        if max_score >= threshold:
            if max_score > best_score:
                best_score = max_score
                best_matches = pd.DataFrame([row])
            elif max_score == best_score:
                best_matches = pd.concat([best_matches, pd.DataFrame([row])])
                
    return best_matches

def process_verified_matches(dedup_df: pd.DataFrame,
                           matches_df: pd.DataFrame) -> Tuple[pd.DataFrame, Dict]:
    """
    Process verified matches and merge corresponding records.
    
    Args:
        dedup_df: Deduplicated dataset
        matches_df: Verified matches DataFrame
        
    Returns:
        Tuple[pd.DataFrame, Dict]: (processed DataFrame, audit data)
    """
    logger = logging.getLogger(__name__)
    result_df = dedup_df.copy()
    
    # Ensure source-specific name fields exist
    source_name_fields = ['Name - HOO', 'Name - Ops', 'Name_HOO', 'Name_OPS']
    for field in source_name_fields:
        if field not in result_df.columns:
            result_df[field] = None
    
    # Initialize audit data
    audit_data = {
        'total_matches': len(matches_df),
        'matches_applied': 0,
        'records_affected': set(),
        'fields_preserved': {},
        'source_preferences_applied': 0,
        'match_method': {
            'by_id': 0,
            'by_name': 0,
            'by_fuzzy': 0
        },
        'skipped_matches': {
            'missing_ids': 0,
            'already_processed': 0,
            'records_not_found': 0
        }
    }
    
    # Track processed records to avoid reprocessing
    processed_records = set()
    
    # Define metadata fields that shouldn't be merged
    metadata_fields = {'RecordID', 'source', 'data_source', 'merged_from', 'merge_note', 'dedup_source'}
    
    # Sort matches by score descending
    matches_df = matches_df.sort_values('Score', ascending=False)
    
    # Process each match
    for _, match in matches_df.iterrows():
        source_records = pd.DataFrame()
        target_records = pd.DataFrame()
        
        # Try ID matching first
        if pd.notna(match['SourceID']) and pd.notna(match['TargetID']):
            source_records = result_df[result_df['RecordID'] == match['SourceID']]
            target_records = result_df[result_df['RecordID'] == match['TargetID']]
            if not source_records.empty and not target_records.empty:
                audit_data['match_method']['by_id'] += 1
                
        # Try name matching if ID matching failed
        if source_records.empty or target_records.empty:
            source_records = find_matching_record(match['Source'], result_df)
            target_records = find_matching_record(match['Target'], result_df)
            
            if not source_records.empty and not target_records.empty:
                # Check if it was an exact match or fuzzy match
                source_exact = any(str(match['Source']).lower() == str(r['NameNormalized']).lower() 
                                 for _, r in source_records.iterrows())
                target_exact = any(str(match['Target']).lower() == str(r['NameNormalized']).lower() 
                                 for _, r in target_records.iterrows())
                
                if source_exact and target_exact:
                    audit_data['match_method']['by_name'] += 1
                else:
                    audit_data['match_method']['by_fuzzy'] += 1
            else:
                audit_data['skipped_matches']['records_not_found'] += 1
                logger.warning(f"Could not find records for match: {match['Source']} - {match['Target']}")
                continue
        
        source_record = source_records.iloc[0]
        target_record = target_records.iloc[0]
        
        # Skip if either record was already processed
        if source_record['RecordID'] in processed_records or target_record['RecordID'] in processed_records:
            audit_data['skipped_matches']['already_processed'] += 1
            continue
            
        # Determine which record to keep based on source preference
        source_order = ['nyc_agencies_export', 'ops', 'nyc_gov']
        source_ranks = {source: rank for rank, source in enumerate(source_order)}
        
        source_rank = source_ranks.get(source_record['source'], len(source_order))
        target_rank = source_ranks.get(target_record['source'], len(source_order))
        
        if source_rank <= target_rank:
            keep_record = source_record
            merge_record = target_record
        else:
            keep_record = target_record
            merge_record = source_record
            
        # Merge information and update metadata
        merged_record, fields_preserved = merge_record_information(
            keep_record, merge_record, metadata_fields
        )
        
        merged_record = update_record_metadata(
            merged_record, merge_record, fields_preserved
        )
        
        # Update the result DataFrame
        merged_dict = {field: merged_record[field] for field in result_df.columns if field in merged_record}
        result_df.loc[result_df['RecordID'] == merged_record['RecordID'], merged_dict.keys()] = pd.Series(merged_dict)
        # Remove the merged record and clean up empty rows
        result_df = result_df[result_df['RecordID'] != merge_record['RecordID']].dropna(how='all')
        
        # Update audit data
        audit_data['matches_applied'] += 1
        audit_data['records_affected'].add(merged_record['RecordID'])
        audit_data['source_preferences_applied'] += 1
        
        for field in fields_preserved:
            audit_data['fields_preserved'][field] = audit_data['fields_preserved'].get(field, 0) + 1
            
        # Mark records as processed
        processed_records.add(merged_record['RecordID'])
        processed_records.add(merge_record['RecordID'])
        
        logger.info(f"Processed match: {match['Source']} - {match['Target']}")
        
    # Convert sets to lists for JSON serialization
    audit_data['records_affected'] = list(audit_data['records_affected'])
    
    # Log summary
    logger.info("\nMatch Processing Summary:")
    logger.info(f"Total matches: {audit_data['total_matches']}")
    logger.info(f"Matches applied: {audit_data['matches_applied']}")
    logger.info(f"Records affected: {len(audit_data['records_affected'])}")
    logger.info("\nMatch methods:")
    logger.info(f"- By ID: {audit_data['match_method']['by_id']}")
    logger.info(f"- By name: {audit_data['match_method']['by_name']}")
    logger.info(f"- By fuzzy: {audit_data['match_method']['by_fuzzy']}")
    logger.info("\nSkipped matches:")
    for reason, count in audit_data['skipped_matches'].items():
        logger.info(f"- {reason}: {count}")
    
    return result_df, audit_data

def save_outputs(final_df: pd.DataFrame, audit_data: Dict) -> None:
    """
    Save the final dataset and audit information.
    
    Args:
        final_df: The final deduplicated DataFrame
        audit_data: Dictionary containing audit information
    """
    logger = logging.getLogger(__name__)
    
    try:
        # Create output directories if they don't exist
        os.makedirs('../data/processed', exist_ok=True)
        os.makedirs('../data/analysis', exist_ok=True)
        
        # Ensure source-specific name fields are present
        source_name_fields = ['Name - HOO', 'Name - Ops', 'Name_HOO', 'Name_OPS']
        for field in source_name_fields:
            if field not in final_df.columns:
                final_df[field] = None
        
        # Clean up empty rows but preserve source name fields
        non_empty_mask = ~final_df.drop(columns=source_name_fields).isna().all(axis=1)
        final_df = final_df[non_empty_mask]
        
        # Save final dataset
        output_path = '../data/processed/final_deduplicated_dataset.csv'
        final_df.to_csv(output_path, index=False)
        logger.info(f"Saved final dataset to {output_path}")
        
        # Save audit summary
        summary_path = '../data/analysis/verified_matches_summary.json'
        with open(summary_path, 'w') as f:
            json.dump(audit_data, f, indent=2)
        logger.info(f"Saved audit summary to {summary_path}")
        
    except Exception as e:
        logger.error(f"Error saving outputs: {e}")
        raise

def validate_results(final_df: pd.DataFrame, 
                    original_df: pd.DataFrame,
                    matches_df: pd.DataFrame) -> None:
    """
    Validate the results of the matching process.
    
    Args:
        final_df: The final deduplicated DataFrame
        original_df: The original deduplicated DataFrame
        matches_df: The verified matches DataFrame
    """
    logger = logging.getLogger(__name__)
    
    # Check all verified matches were processed
    for _, match in matches_df.iterrows():
        # At least one of the records should exist in final dataset
        if not (
            (final_df['RecordID'] == match['SourceID']).any() or
            (final_df['RecordID'] == match['TargetID']).any()
        ):
            logger.warning(f"Match not properly processed: {match['SourceID']} - {match['TargetID']}")
    
    # Check for remaining duplicates
    name_dupes = final_df[final_df['NameNormalized'].duplicated(keep=False)]
    if not name_dupes.empty:
        logger.warning(f"Found {len(name_dupes)} records with duplicate normalized names")
        
    # Verify no critical information was lost
    original_records = set(original_df['RecordID'])
    final_records = set(final_df['RecordID'])
    merged_records = original_records - final_records
    
    logger.info(f"Original records: {len(original_records)}")
    logger.info(f"Final records: {len(final_records)}")
    logger.info(f"Merged records: {len(merged_records)}")
    
    # Check metadata integrity
    missing_metadata = final_df[
        pd.isna(final_df['merged_from']) & 
        pd.isna(final_df['merge_note'])
    ]
    if not missing_metadata.empty:
        logger.warning(f"Found {len(missing_metadata)} records with missing metadata")

def main():
    """Main function to execute Step 3.2"""
    logger = setup_logging()
    
    try:
        # Load and validate inputs
        logger.info("Loading and validating inputs...")
        dedup_df, matches_df = load_and_validate_inputs()
        
        # Process verified matches
        logger.info("Processing verified matches...")
        final_df, audit_data = process_verified_matches(dedup_df, matches_df)
        
        # Save outputs
        logger.info("Saving outputs...")
        save_outputs(final_df, audit_data)
        
        # Validate results
        logger.info("Validating results...")
        validate_results(final_df, dedup_df, matches_df)
        
        logger.info("Step 3.2 completed successfully")
        
    except Exception as e:
        logger.error(f"Error in Step 3.2: {e}")
        raise

if __name__ == "__main__":
    main() 