import pandas as pd
import ast
import logging
from collections import defaultdict

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def safe_eval(expr):
    """Safely evaluate a string expression, handling NaN values."""
    if pd.isna(expr):
        return []
    try:
        import ast
        return ast.literal_eval(expr)
    except:
        return []

def merge_records(primary_record, secondary_record, merge_note):
    """Merge two records, preserving fields from both and handling conflicts."""
    merged = primary_record.copy()
    
    # Track conflicts for merge note
    conflicts = []
    
    # Handle source-specific name fields
    # Initialize fields if they don't exist
    for field in ['Name - HOO', 'Name - Ops']:
        if field not in merged:
            merged[field] = None
            
    # Helper function to combine name fields
    def combine_names(existing, new_value):
        if pd.isna(existing) and pd.isna(new_value):
            return None
        if pd.isna(existing):
            return new_value
        if pd.isna(new_value):
            return existing
        # Split existing names and new value, then combine unique values
        existing_names = str(existing).split('|')
        new_names = str(new_value).split('|')
        combined = list(set(existing_names + new_names))
        return '|'.join(sorted(combined))
    
    # Process source-specific fields from secondary record
    if 'Agency Name' in secondary_record:
        if secondary_record['source'] == 'ops':
            old_value = merged.get('Name - Ops')
            merged['Name - Ops'] = combine_names(old_value, secondary_record['Agency Name'])
            if old_value != merged['Name - Ops']:
                logging.info(f"Updated Name - Ops for {merged.get('RecordID', 'unknown')}: {old_value} -> {merged['Name - Ops']}")
        elif secondary_record['source'] == 'nyc_gov':
            old_value = merged.get('Name - HOO')
            merged['Name - HOO'] = combine_names(old_value, secondary_record['Agency Name'])
            if old_value != merged['Name - HOO']:
                logging.info(f"Updated Name - HOO for {merged.get('RecordID', 'unknown')}: {old_value} -> {merged['Name - HOO']}")
    
    # Also preserve any existing source-specific fields from secondary record
    if 'Name - HOO' in secondary_record and pd.notna(secondary_record['Name - HOO']):
        old_value = merged.get('Name - HOO')
        merged['Name - HOO'] = combine_names(old_value, secondary_record['Name - HOO'])
        if old_value != merged['Name - HOO']:
            logging.info(f"Preserved Name - HOO from secondary record {secondary_record.get('RecordID', 'unknown')}: {old_value} -> {merged['Name - HOO']}")
    if 'Name - Ops' in secondary_record and pd.notna(secondary_record['Name - Ops']):
        old_value = merged.get('Name - Ops')
        merged['Name - Ops'] = combine_names(old_value, secondary_record['Name - Ops'])
        if old_value != merged['Name - Ops']:
            logging.info(f"Preserved Name - Ops from secondary record {secondary_record.get('RecordID', 'unknown')}: {old_value} -> {merged['Name - Ops']}")
    
    # Merge other fields
    for col in secondary_record.index:
        if col not in ['Name - HOO', 'Name - Ops']:  # Skip source-specific fields as they're handled above
            if col not in merged or pd.isna(merged[col]):
                merged[col] = secondary_record[col]
            elif pd.notna(secondary_record[col]) and merged[col] != secondary_record[col]:
                conflicts.append(f"{col}: preferred='{merged[col]}', secondary='{secondary_record[col]}'")
    
    if conflicts:
        merge_note += f"\nConflicts: {'; '.join(conflicts)}"
    
    merged['merge_note'] = merge_note
    return merged

def apply_matches(df, matches_df):
    """Apply matches to merge records."""
    logging.info(f"Starting with {len(df)} records")
    
    # Initialize source-specific name fields if they don't exist
    if 'Name - HOO' not in df.columns:
        df['Name - HOO'] = None
    if 'Name - Ops' not in df.columns:
        df['Name - Ops'] = None
    
    # Track processed records
    processed_ids = set()
    merge_groups = []
    total_merged = 0
    
    for _, row in matches_df.iterrows():
        source_id = row['SourceID']
        target_id = row['TargetID']
        score = row['Score']
        
        # Skip if either record has been processed
        if source_id in processed_ids or target_id in processed_ids:
            logging.info(f"Skipping self-match: {source_id}")
            continue
            
        # Get records
        source_mask = df['RecordID'] == source_id
        target_mask = df['RecordID'] == target_id
        
        if not source_mask.any():
            logging.warning(f"Record not found - source_id: {source_id}, target_id: {target_id}")
            continue
        if not target_mask.any():
            logging.warning(f"Record not found - source_id: {source_id}, target_id: {target_id}")
            continue
            
        # Convert to series
        source_record = df[source_mask].iloc[0]
        target_record = df[target_mask].iloc[0]
        
        # Create merge note
        merge_note = f"Merged with {target_id} (score: {score})"
        
        # Merge records
        merged_record = merge_records(source_record, target_record, merge_note)
        
        # Update dataframe
        df = df[~target_mask]  # Remove secondary record
        for col in df.columns:
            df.loc[source_mask, col] = merged_record[col]  # Update primary record
        
        # Track processed records
        processed_ids.add(source_id)
        processed_ids.add(target_id)
        merge_groups.append([source_id, target_id])
        total_merged += 1
        
    logging.info(f"Applied {len(merge_groups)} merge groups")
    logging.info(f"Total records merged: {total_merged}")
    logging.info(f"Final dataset has {len(df)} records")
    
    # Define name-related columns for reordering
    name_columns = [
        'Name - NYC.gov Agency List',
        'Name - NYC.gov Mayor\'s Office',
        'Name - NYC Open Data Portal',
        'Name - ODA',
        'Name - CPO',
        'Name - WeGov',
        'Name - Greenbook',
        'Name - Checkbook',
        'Name - HOO',
        'Name - Ops',
        'NameWithAcronym',
        'NameAlphabetizedWithAcronym',
        'Agency Name',
        'AgencyNameEnriched',
        'LegalName',
        'AlternateNames'
    ]
    
    # Get all other columns (excluding Name, NameAlphabetized, and name-related columns)
    other_columns = [col for col in df.columns if col not in ['Name', 'NameAlphabetized'] + name_columns]
    
    # Create new column order
    new_column_order = ['Name', 'NameAlphabetized'] + name_columns + other_columns
    
    # Reorder columns
    df = df[new_column_order]
    logging.info("Reordered columns to group name-related fields together")
    
    # Save final dataset
    output_path = 'data/processed/final_deduplicated_dataset.csv'
    df.to_csv(output_path, index=False)
    logging.info(f"Saved final deduplicated dataset to {output_path}")
    logging.info(f"Total records merged: {total_merged}")
    
    return df

def main():
    """Main function to apply matches and generate final output."""
    try:
        # Load datasets
        logger.info("Loading datasets...")
        df = pd.read_csv('data/processed/final_merged_dataset.csv', dtype=str)
        matches_df = pd.read_csv('data/processed/cleaned_matches.csv', dtype=str)
        
        logger.info(f"Loaded {len(df)} records from deduplicated dataset")
        logger.info(f"Loaded {len(matches_df)} matches from cleaned matches file")
        
        # Populate source-specific name fields based on source
        for idx, row in df.iterrows():
            if row['source'] == 'nyc_gov':
                df.at[idx, 'Name - HOO'] = row['Agency Name']
            elif row['source'] == 'ops':
                df.at[idx, 'Name - Ops'] = row['Agency Name']
        
        # Apply matches
        final_df = apply_matches(df, matches_df)
        
        # Generate merge statistics
        total_merges = sum(1 for x in final_df['merged_from'] if pd.notna(x))
        logger.info(f"Total records merged: {total_merges}")
        
    except Exception as e:
        logger.error(f"Error applying matches: {str(e)}")
        raise

if __name__ == "__main__":
    main() 