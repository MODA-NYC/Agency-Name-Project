import pandas as pd
import logging
import re

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(message)s')
logger = logging.getLogger(__name__)

def normalize_name(name):
    """Normalize agency name for comparison."""
    if pd.isna(name):
        return ""
    name = str(name).lower()
    name = name.replace("&", "and")
    name = name.replace("+", "and")
    name = re.sub(r'[^\w\s]', ' ', name)  # Replace punctuation with space
    name = re.sub(r'\s+', ' ', name).strip()  # Normalize whitespace
    return name

def find_record_by_name(name, df, id_col='RecordID'):
    """Find a record in the deduplicated dataset by name."""
    if pd.isna(name):
        return None, None
    
    norm_name = normalize_name(name)
    
    # Try exact match on normalized name
    mask = df['NameNormalized'].apply(normalize_name) == norm_name
    if mask.any():
        match = df[mask].iloc[0]
        return match[id_col], match['Name']
    
    # Try partial match
    for idx, row in df.iterrows():
        row_norm = normalize_name(row['Name'])
        if norm_name in row_norm or row_norm in norm_name:
            return row[id_col], row['Name']
    
    return None, None

def clean_matches(dedup_df, matches_df):
    """Clean matches by validating against deduplicated dataset."""
    logger.info(f"Starting matches cleanup with {len(matches_df)} potential matches")
    logger.info(f"Deduplicated dataset has {len(dedup_df)} records")
    
    cleaned_matches = []
    skipped_matches = 0
    records_not_found = set()
    
    for idx, row in matches_df.iterrows():
        source_name = row['Source']
        target_name = row['Target']
        
        # Skip self-matches
        if normalize_name(source_name) == normalize_name(target_name):
            skipped_matches += 1
            continue
        
        # Try to find records by ID first if available
        source_id = row['SourceID'] if pd.notna(row['SourceID']) else None
        target_id = row['TargetID'] if pd.notna(row['TargetID']) else None
        
        source_found = False
        target_found = False
        
        # If ID is available, verify it exists
        if source_id and source_id in dedup_df['RecordID'].values:
            source_found = True
            source_name = dedup_df[dedup_df['RecordID'] == source_id].iloc[0]['Name']
        else:
            # Try to find by name
            source_id, source_name = find_record_by_name(source_name, dedup_df)
            source_found = source_id is not None
        
        if target_id and target_id in dedup_df['RecordID'].values:
            target_found = True
            target_name = dedup_df[dedup_df['RecordID'] == target_id].iloc[0]['Name']
        else:
            # Try to find by name
            target_id, target_name = find_record_by_name(target_name, dedup_df)
            target_found = target_id is not None
        
        if not source_found:
            records_not_found.add(source_name)
            continue
            
        if not target_found:
            records_not_found.add(target_name)
            continue
        
        # Create cleaned match entry
        cleaned_matches.append({
            'Source': source_name,
            'Target': target_name,
            'Score': row['Score'],
            'Label': row['Label'],
            'SourceID': source_id,
            'TargetID': target_id,
            'Notes': row['Notes'] if 'Notes' in row else ''
        })
    
    logger.info(f"Cleaned matches: {len(cleaned_matches)}")
    logger.info(f"Skipped matches: {skipped_matches}")
    logger.info(f"Records not found: {len(records_not_found)}")
    
    if records_not_found:
        logger.warning("Records not found in deduplicated dataset:")
        for name in sorted(records_not_found):
            logger.warning(f"- {name}")
    
    return pd.DataFrame(cleaned_matches)

def main():
    """Main function to clean up matches."""
    try:
        # Load datasets
        logger.info("Loading datasets...")
        dedup_df = pd.read_csv('data/processed/final_deduplicated_dataset.csv', encoding='utf-8')
        matches_df = pd.read_csv('data/processed/consolidated_matches.csv', encoding='utf-8')
        
        logger.info(f"Loaded {len(dedup_df)} records from deduplicated dataset")
        logger.info(f"Loaded {len(matches_df)} matches from matches file")
        
        # Clean matches
        cleaned_matches_df = clean_matches(dedup_df, matches_df)
        
        # Save cleaned matches
        output_path = 'data/processed/cleaned_matches.csv'
        cleaned_matches_df.to_csv(output_path, index=False, encoding='utf-8')
        logger.info(f"Saved cleaned matches to {output_path}")
        
    except Exception as e:
        logger.error(f"Error processing matches: {str(e)}")
        raise

if __name__ == "__main__":
    main() 