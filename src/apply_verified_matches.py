import pandas as pd
import logging
import json
import os
from pathlib import Path
from typing import Dict, List, Tuple, Set, Optional
from difflib import SequenceMatcher
import re
import gc
import numpy as np

def setup_logging() -> logging.Logger:
    """Configure logging with appropriate format and level"""
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s'
    )
    return logging.getLogger(__name__)

def load_and_validate_inputs() -> Tuple[pd.DataFrame, pd.DataFrame]:
    """Load and validate input datasets"""
    try:
        dedup_path = Path('data/processed/final_deduplicated_dataset.csv')
        matches_path = Path('data/processed/consolidated_matches.csv')
        
        if not dedup_path.exists() or not matches_path.exists():
            raise FileNotFoundError("Required input files not found")
            
        df = pd.read_csv(dedup_path)
        matches_df = pd.read_csv(matches_path)
        
        required_cols = {'RecordID', 'NameNormalized'}
        match_cols = {'Source', 'Target', 'Score', 'Label'}
        
        if not required_cols.issubset(df.columns):
            raise ValueError(f"Missing required columns in deduplicated dataset: {required_cols - set(df.columns)}")
            
        if not match_cols.issubset(matches_df.columns):
            raise ValueError(f"Missing required columns in matches file: {match_cols - set(matches_df.columns)}")
            
        return df, matches_df
        
    except Exception as e:
        logging.error(f"Error loading inputs: {str(e)}")
        raise

def initialize_metadata_columns(df: pd.DataFrame) -> pd.DataFrame:
    """Initialize metadata columns if they don't exist"""
    metadata_cols = ['merged_from', 'merge_note']
    for col in metadata_cols:
        if col not in df.columns:
            df[col] = None
    return df

def safe_json_loads(json_str: str) -> List:
    """Safely load JSON string, returning empty list if invalid"""
    if pd.isna(json_str):
        return []
    try:
        return json.loads(json_str)
    except:
        return []

def update_metadata(record: pd.Series, merged_record: pd.Series, score: float) -> Tuple[str, str]:
    """Update metadata for merged records"""
    # Load existing merged_from list or initialize new one
    merged_from = safe_json_loads(record.get('merged_from', '[]'))
    
    # Add the merged record to the history
    merged_from.append({
        'id': merged_record.get('RecordID', ''),
        'name': merged_record.get('Name', ''),
        'score': score
    })
    
    # Build merge note
    merge_note = f"Merged with {merged_record.get('RecordID', '')} (score: {score})"
    
    # Add any merge conflicts to the note
    if 'merge_conflicts' in record and record['merge_conflicts']:
        conflicts = '; '.join(record['merge_conflicts'])
        merge_note += f"\nConflicts: {conflicts}"
        del record['merge_conflicts']  # Clean up temporary field
    
    # Append to existing merge notes
    if record.get('merge_note'):
        merge_note = f"{record['merge_note']}\n{merge_note}"
    
    return json.dumps(merged_from), merge_note

def build_record_map(df: pd.DataFrame) -> Dict[str, int]:
    """Build a map of RecordID to DataFrame index"""
    return {str(record_id): idx for idx, record_id in enumerate(df['RecordID']) if pd.notna(record_id)}

def merge_record_information(preferred: pd.Series, secondary: pd.Series) -> pd.Series:
    """Merge information from two records, preserving source-specific names"""
    # Convert both Series to use the same index
    preferred = preferred.copy()
    secondary = secondary.copy()
    all_columns = list(set(preferred.index) | set(secondary.index))
    preferred = pd.Series(preferred.values, index=preferred.index).reindex(all_columns)
    secondary = pd.Series(secondary.values, index=secondary.index).reindex(all_columns)
    
    merged = preferred.copy()
    
    # Define source-specific name fields
    source_name_fields = {
        'ops': ['Name - Ops'],
        'hoo': ['Name - HOO'],
        'primary': ['NameNormalized', 'Agency Name']
    }
    
    # Preserve source-specific names
    for source, name_fields in source_name_fields.items():
        for field in name_fields:
            if field in secondary.index and not pd.isna(secondary[field]):
                if field not in merged.index or pd.isna(merged[field]):
                    merged[field] = secondary[field]
                elif merged[field] != secondary[field]:
                    # If both records have different values, combine them
                    merged[field] = f"{merged[field]}|{secondary[field]}"
    
    # Initialize merge conflicts list
    merge_conflicts = []
    
    # Merge other non-null fields from secondary record
    for col in secondary.index:
        if col not in ['merged_from', 'merge_note']:  # Skip metadata fields
            if pd.isna(merged[col]) and not pd.isna(secondary[col]):
                merged[col] = secondary[col]
            elif not pd.isna(merged[col]) and not pd.isna(secondary[col]) and merged[col] != secondary[col]:
                # Record the conflict but keep the preferred record's value
                merge_conflicts.append(f"{col}: preferred='{merged[col]}', secondary='{secondary[col]}'")
    
    # Add merge conflicts to the record if any exist
    if merge_conflicts:
        merged['merge_conflicts'] = merge_conflicts
    
    return merged

def process_verified_matches(df: pd.DataFrame, matches_df: pd.DataFrame) -> Tuple[pd.DataFrame, Dict]:
    """Process verified matches and merge records"""
    stats = {
        'total_matches': len(matches_df),
        'matches_applied': 0,
        'records_affected': set(),
        'skipped_matches': 0,
        'errors': 0,
        'processed_ids': set(),
        'original_records': len(df)
    }
    
    # Initialize metadata columns
    df = initialize_metadata_columns(df)
    
    # Sort matches by score descending
    matches_df = matches_df.sort_values('Score', ascending=False)
    
    # Process matches in batches
    batch_size = 50
    for start_idx in range(0, len(matches_df), batch_size):
        batch_matches = matches_df.iloc[start_idx:start_idx + batch_size]
        
        # Track changes for this batch
        indices_to_drop = []
        batch_updates = []  # List of (index, record) tuples to update
        batch_processed_ids = set()  # Track processed IDs for this batch
        
        # Build record map once for the batch
        record_map = build_record_map(df)
        
        for _, match in batch_matches.iterrows():
            try:
                source_id = str(match['SourceID']) if pd.notna(match['SourceID']) else None
                target_id = str(match['TargetID']) if pd.notna(match['TargetID']) else None
                
                # Skip if either ID is missing
                if not source_id or not target_id:
                    logging.warning(f"Missing ID in match: {match['Source']} - {match['Target']}")
                    stats['skipped_matches'] += 1
                    continue
                
                # Skip self-referential matches
                if source_id == target_id:
                    logging.info(f"Skipping self-referential match: {source_id}")
                    stats['skipped_matches'] += 1
                    continue
                
                # Skip already processed ID pairs
                id_pair = tuple(sorted([source_id, target_id]))
                if id_pair in stats['processed_ids']:
                    logging.info(f"Skipping already processed IDs: {source_id} - {target_id}")
                    stats['skipped_matches'] += 1
                    continue
                
                # Get record indices
                source_idx = record_map.get(source_id)
                target_idx = record_map.get(target_id)
                
                if source_idx is None or target_idx is None:
                    logging.warning(f"Could not find records for match: {match['Source']} - {match['Target']}")
                    stats['skipped_matches'] += 1
                    continue
                
                # Get records
                source_record = df.iloc[source_idx].copy()
                target_record = df.iloc[target_idx].copy()
                
                # Determine which record to keep based on data completeness
                source_completeness = source_record.notna().sum()
                target_completeness = target_record.notna().sum()
                
                if source_completeness >= target_completeness:
                    preferred = source_record
                    secondary = target_record
                    kept_idx = source_idx
                    removed_idx = target_idx
                else:
                    preferred = target_record
                    secondary = source_record
                    kept_idx = target_idx
                    removed_idx = source_idx
                
                # Merge records
                merged_record = merge_record_information(preferred, secondary)
                
                # Update metadata
                merged_from, merge_note = update_metadata(merged_record, secondary, match['Score'])
                merged_record['merged_from'] = merged_from
                merged_record['merge_note'] = merge_note
                
                # Queue updates
                batch_updates.append((kept_idx, merged_record))
                indices_to_drop.append(removed_idx)
                batch_processed_ids.add(id_pair)
                
                logging.info(f"Processed match: {match['Source']} - {match['Target']}")
                
            except Exception as e:
                logging.error(f"Error processing match {match['Source']} - {match['Target']}: {str(e)}")
                stats['errors'] += 1
                continue
        
        # Apply all updates for this batch
        for idx, record in batch_updates:
            for col in record.index:
                if col in df.columns:
                    df.loc[idx, col] = record[col]
        
        # Drop removed indices and reset index
        if indices_to_drop:
            df = df.drop(index=indices_to_drop)
            df = df.reset_index(drop=True)
        
        # Update statistics
        stats['matches_applied'] += len(batch_updates)
        stats['processed_ids'].update(batch_processed_ids)
        stats['records_affected'].update(id for pair in batch_processed_ids for id in pair)
        
        # Force garbage collection after each batch
        gc.collect()
    
    # Convert processed_ids to list for JSON serialization
    stats['processed_ids'] = list(stats['processed_ids'])
    stats['records_affected'] = len(stats['records_affected'])
    stats['final_records'] = len(df)
    stats['records_merged'] = stats['original_records'] - stats['final_records']
    
    return df, stats

def save_outputs(final_df: pd.DataFrame, audit_data: Dict) -> None:
    """Save the final dataset and audit information"""
    try:
        # Create output directories if they don't exist
        Path('data/processed').mkdir(parents=True, exist_ok=True)
        Path('data/analysis').mkdir(parents=True, exist_ok=True)
        
        # Save final dataset
        output_path = Path('data/processed/final_deduplicated_dataset.csv')
        final_df.to_csv(output_path, index=False)
        logging.info(f"Saved final dataset to {output_path}")
        
        # Save audit summary
        audit_path = Path('data/analysis/verified_matches_summary.json')
        with open(audit_path, 'w') as f:
            json.dump(audit_data, f, indent=2)
        logging.info(f"Saved audit summary to {audit_path}")
        
    except Exception as e:
        logging.error(f"Error saving outputs: {str(e)}")
        raise

def validate_results(df: pd.DataFrame, matches_df: pd.DataFrame, processed_ids: Set[Tuple[str, str]], stats: Dict):
    """Validate the results of match processing"""
    validation_stats = {
        'total_matches': len(matches_df),
        'properly_processed': 0,
        'skipped_self_referential': 0,
        'skipped_missing_ids': 0,
        'skipped_already_processed': 0,
        'not_properly_processed': 0,
        'records_not_found': 0,
        'validation_errors': []
    }
    
    # Get set of valid record IDs
    valid_record_ids = set(df['RecordID'].dropna().astype(str))
    
    for _, match in matches_df.iterrows():
        source_id = str(match['SourceID'])
        target_id = str(match['TargetID'])
        
        # Skip if either ID is missing
        if pd.isna(source_id) or pd.isna(target_id):
            validation_stats['skipped_missing_ids'] += 1
            continue
            
        # Skip self-referential matches
        if source_id == target_id:
            validation_stats['skipped_self_referential'] += 1
            continue
            
        id_pair = tuple(sorted([source_id, target_id]))
        
        # Check if match was processed
        if id_pair in processed_ids:
            validation_stats['properly_processed'] += 1
            continue
            
        # Check if records exist in dataset
        if source_id not in valid_record_ids or target_id not in valid_record_ids:
            validation_stats['records_not_found'] += 1
            validation_stats['validation_errors'].append(
                f"Records not found for match: {match['Source']} - {match['Target']} "
                f"(IDs: {source_id} - {target_id})"
            )
            continue
            
        # If we get here, the match should have been processed but wasn't
        validation_stats['not_properly_processed'] += 1
        validation_stats['validation_errors'].append(
            f"Match not properly processed: {source_id} - {target_id}"
        )
    
    # Log validation summary
    logging.info("\nValidation Summary:")
    logging.info(f"Total matches: {validation_stats['total_matches']}")
    logging.info(f"Properly processed: {validation_stats['properly_processed']}")
    logging.info(f"Skipped (self-referential): {validation_stats['skipped_self_referential']}")
    logging.info(f"Skipped (missing IDs): {validation_stats['skipped_missing_ids']}")
    logging.info(f"Skipped (already processed): {validation_stats['skipped_already_processed']}")
    logging.info(f"Records not found: {validation_stats['records_not_found']}")
    logging.info(f"Not properly processed: {validation_stats['not_properly_processed']}")
    
    if validation_stats['validation_errors']:
        logging.warning("\nValidation Errors:")
        for error in validation_stats['validation_errors'][:10]:  # Show first 10 errors
            logging.warning(error)
        if len(validation_stats['validation_errors']) > 10:
            logging.warning(f"...and {len(validation_stats['validation_errors']) - 10} more errors")
    
    stats['validation'] = validation_stats
    return validation_stats

def normalize_name(name: str) -> str:
    """Apply consistent normalization rules to agency names."""
    if pd.isna(name):
        return ""
    
    name = name.lower()
    
    # Common substitutions
    name = name.replace('&', 'and')
    name = name.replace('nyc', 'new york city')
    name = name.replace('ny', 'new york')
    
    # Remove parentheses and their contents
    name = re.sub(r'\([^)]*\)', '', name)
    
    # Remove punctuation except hyphens
    name = re.sub(r'[^\w\s-]', ' ', name)
    
    # Remove extra whitespace
    name = ' '.join(name.split())
    
    # Standardize organizational terms
    org_terms = {
        'dept': 'department',
        'comm': 'commission',
        'auth': 'authority',
        'admin': 'administration',
        'corp': 'corporation',
        'dev': 'development',
        'svcs': 'services',
        'svc': 'service',
        'tech': 'technology',
        'mgmt': 'management',
        'ops': 'operations',
        'bd': 'board',
        'div': 'division',
        'inst': 'institute',
        'org': 'organization',
        'ofc': 'office',
        'ctr': 'center',
        'sys': 'system',
        'dist': 'district',
        'coord': 'coordinator',
        'coord': 'coordination'
    }
    
    words = name.split()
    normalized_words = [org_terms.get(word, word) for word in words]
    name = ' '.join(normalized_words)
    
    # Standardize common patterns
    patterns = [
        (r'department\s+of\s+(\w+)', r'\1 department'),  # "department of X" -> "X department"
        (r'office\s+of\s+(\w+)', r'\1 office'),  # "office of X" -> "X office"
        (r'commission\s+on\s+(\w+)', r'\1 commission'),  # "commission on X" -> "X commission"
        (r"mayor'?s?\s+office\s+of\s+(\w+)", r'\1 office'),  # "mayor's office of X" -> "X office"
        (r'board\s+of\s+(\w+)', r'\1 board'),  # "board of X" -> "X board"
        (r'division\s+of\s+(\w+)', r'\1 division'),  # "division of X" -> "X division"
        (r'bureau\s+of\s+(\w+)', r'\1 bureau'),  # "bureau of X" -> "X bureau"
        (r'center\s+for\s+(\w+)', r'\1 center'),  # "center for X" -> "X center"
        (r'system\s+of\s+(\w+)', r'\1 system'),  # "system of X" -> "X system"
        (r'district\s+of\s+(\w+)', r'\1 district'),  # "district of X" -> "X district"
        (r'coordinator\s+of\s+(\w+)', r'\1 coordinator'),  # "coordinator of X" -> "X coordinator"
        (r'coordination\s+of\s+(\w+)', r'\1 coordination')  # "coordination of X" -> "X coordination"
    ]
    
    for pattern, replacement in patterns:
        name = re.sub(pattern, replacement, name)
    
    return name.strip()

def generate_name_variations(name: str) -> set:
    """Generate common variations of an agency name."""
    variations = {name}
    
    # Add version with 'office of' prefix
    variations.add(f"office of {name}")
    variations.add(f"mayors office of {name}")
    variations.add(f"office of the {name}")
    variations.add(f"the {name}")
    
    # Add version with 'department of' prefix
    variations.add(f"department of {name}")
    
    # Handle reversals (e.g., "education department" <-> "department of education")
    if ' ' in name:
        words = name.split()
        variations.add(f"{words[-1]} {' '.join(words[:-1])}")
        
        # Handle cases with articles
        if words[0] == 'the':
            variations.add(f"{' '.join(words[1:])}")
        else:
            variations.add(f"the {name}")
            
        # Handle hyphenation variations
        if '-' in name:
            variations.add(name.replace('-', ' '))
        else:
            for i in range(len(words) - 1):
                hyphenated = words.copy()
                hyphenated[i] = f"{hyphenated[i]}-{hyphenated[i+1]}"
                del hyphenated[i+1]
                variations.add(' '.join(hyphenated))
    
    return variations

def find_record_by_name(name: str, df: pd.DataFrame, record_map: Dict[str, int]) -> Optional[str]:
    """Find a record ID by trying various name variations."""
    # Try exact match first
    normalized_name = normalize_name(name)
    
    # Check both Name and NameNormalized columns
    for idx, row in df.iterrows():
        if pd.notna(row.get('Name')):
            if normalize_name(row['Name']) == normalized_name:
                return row['RecordID']
        if pd.notna(row.get('NameNormalized')):
            if row['NameNormalized'] == normalized_name:
                return row['RecordID']
    
    # Try variations
    variations = generate_name_variations(normalized_name)
    for variation in variations:
        for idx, row in df.iterrows():
            if pd.notna(row.get('Name')):
                if normalize_name(row['Name']) == variation:
                    return row['RecordID']
            if pd.notna(row.get('NameNormalized')):
                if row['NameNormalized'] == variation:
                    return row['RecordID']
    
    # Try fuzzy matching as a last resort
    best_match = None
    best_score = 0
    
    for idx, row in df.iterrows():
        if pd.notna(row.get('Name')):
            score = SequenceMatcher(None, normalize_name(row['Name']), normalized_name).ratio()
            if score > best_score and score > 0.95:  # Only consider very close matches
                best_score = score
                best_match = row['RecordID']
        if pd.notna(row.get('NameNormalized')):
            score = SequenceMatcher(None, row['NameNormalized'], normalized_name).ratio()
            if score > best_score and score > 0.95:
                best_score = score
                best_match = row['RecordID']
    
    return best_match

def main():
    """Main function to execute Step 3.2"""
    logger = setup_logging()
    
    try:
        # Load and validate inputs
        logger.info("Loading and validating inputs...")
        dedup_df, matches_df = load_and_validate_inputs()
        
        # Initialize metadata columns
        dedup_df = initialize_metadata_columns(dedup_df)
        
        # Process verified matches
        logger.info("Processing verified matches...")
        final_df, stats = process_verified_matches(dedup_df, matches_df)
        
        # Convert processed_ids to set for validation
        processed_ids = set(tuple(pair) for pair in stats['processed_ids'])
        
        # Validate results
        logger.info("Validating results...")
        validation_stats = validate_results(final_df, matches_df, processed_ids, stats)
        stats['validation'] = validation_stats
        
        # Save outputs
        logger.info("Saving outputs...")
        save_outputs(final_df, stats)
        
        logger.info("Step 3.2 completed successfully")
        
    except Exception as e:
        logger.error(f"Error in Step 3.2: {str(e)}")
        raise

if __name__ == "__main__":
    main() 