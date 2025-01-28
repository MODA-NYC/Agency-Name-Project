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

def combine_names(primary_name, secondary_name):
    """Combine two name values, handling None values appropriately."""
    if primary_name is None or pd.isna(primary_name):
        return secondary_name
    if secondary_name is None or pd.isna(secondary_name):
        return primary_name
    if primary_name == secondary_name:
        return primary_name
    return f"{primary_name}; {secondary_name}"

def merge_records(primary_record, secondary_record, merge_note=None):
    """
    Merge two records, with the primary record taking precedence.
    The Name field follows hierarchical priority:
    1. nyc_agencies_export
    2. ops
    3. nyc_gov (HOO)
    4. Agency Name as fallback
    """
    merged = primary_record.copy()
    
    # Handle source-specific name fields first
    for name_field in ['Name - HOO', 'Name - Ops', 'Name - NYC.gov Agency List', 'Name - NYC.gov Mayor\'s Office']:
        old_value = merged.get(name_field)
        new_value = secondary_record.get(name_field)
        merged[name_field] = combine_names(old_value, new_value)
    
    # Apply hierarchical priority for Name field
    if pd.isna(merged['Name']):
        # Try to get name from source-specific fields in priority order
        if not pd.isna(merged['Name - NYC.gov Agency List']):
            merged['Name'] = merged['Name - NYC.gov Agency List']
        elif not pd.isna(merged['Name - Ops']):
            merged['Name'] = merged['Name - Ops']
        elif not pd.isna(merged['Name - HOO']):
            merged['Name'] = merged['Name - HOO']
        elif not pd.isna(merged['Agency Name']):
            merged['Name'] = merged['Agency Name']
    
    # Handle other fields
    for col in secondary_record.index:
        if col not in ['Name', 'Name - HOO', 'Name - Ops', 'Name - NYC.gov Agency List', 'Name - NYC.gov Mayor\'s Office']:
            if pd.isna(merged[col]) and not pd.isna(secondary_record[col]):
                merged[col] = secondary_record[col]
    
    # Update merge note
    if merge_note:
        old_note = merged['MergeNote'] if not pd.isna(merged['MergeNote']) else ''
        merged['MergeNote'] = f"{old_note}; {merge_note}" if old_note else merge_note
    
    return merged

def apply_matches(df, matches_df):
    """Apply matches to merge records while preserving unmatched records (outer join behavior)."""
    logging.info(f"Starting with {len(df)} records")
    
    # Track processed records and their merge groups
    processed_ids = set()
    merge_groups = []
    total_merged = 0
    
    # Create a copy of the dataframe to avoid modifying it during iteration
    result_df = df.copy()
    
    for _, row in matches_df.iterrows():
        source_id = row['SourceID']
        target_id = row['TargetID']
        score = row['Score']
        
        # Skip if both records have been processed
        if source_id in processed_ids and target_id in processed_ids:
            logging.info(f"Skipping already processed match: {source_id} - {target_id}")
            continue
            
        # Get records
        source_mask = result_df['RecordID'] == source_id
        target_mask = result_df['RecordID'] == target_id
        
        if not source_mask.any() or not target_mask.any():
            logging.warning(f"Record not found - source_id: {source_id}, target_id: {target_id}")
            continue
            
        # Convert to series
        source_record = result_df[source_mask].iloc[0]
        target_record = result_df[target_mask].iloc[0]
        
        # Create merge note
        merge_note = f"Merged with {target_id} (score: {score})"
        
        # Merge records
        merged_record = merge_records(source_record, target_record, merge_note)
        
        # Update dataframe
        result_df = result_df[~target_mask]  # Remove secondary record
        for col in result_df.columns:
            result_df.loc[source_mask, col] = merged_record[col]  # Update primary record
        
        # Track processed records
        processed_ids.add(source_id)
        processed_ids.add(target_id)
        merge_groups.append([source_id, target_id])
        total_merged += 1
    
    # At this point, result_df contains all merged records and unmatched records
    logging.info(f"Applied {len(merge_groups)} merge groups")
    logging.info(f"Total records merged: {total_merged}")
    logging.info(f"Final dataset has {len(result_df)} records")
    
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
    other_columns = [col for col in result_df.columns if col not in ['Name', 'NameAlphabetized'] + name_columns]
    
    # Create new column order
    new_column_order = ['Name', 'NameAlphabetized'] + name_columns + other_columns
    
    # Reorder columns
    result_df = result_df[new_column_order]
    logging.info("Reordered columns to group name-related fields together")
    
    # Save final dataset
    output_path = 'data/processed/final_deduplicated_dataset.csv'
    result_df.to_csv(output_path, index=False)
    logging.info(f"Saved final deduplicated dataset to {output_path}")
    logging.info(f"Total records merged: {total_merged}")
    
    return result_df

def main():
    """Main function to load data, apply matches, and save results."""
    logging.info("Loading datasets...")
    
    # Load the deduplicated dataset and matches
    df = pd.read_csv('data/processed/final_deduplicated_dataset.csv')
    matches_df = pd.read_csv('data/processed/cleaned_matches.csv')
    
    logging.info(f"Loaded {len(df)} records from deduplicated dataset")
    logging.info(f"Loaded {len(matches_df)} matches from cleaned matches file")
    
    # Initialize source-specific name fields if they don't exist
    name_fields = ['Name - HOO', 'Name - Ops', 'Name - NYC.gov Agency List', 'Name - NYC.gov Mayor\'s Office']
    for field in name_fields:
        if field not in df.columns:
            df[field] = None
    
    # Initialize Name field based on source
    logging.info(f"Starting with {len(df)} records")
    
    # Apply source priority for initial Name field population
    for idx, row in df.iterrows():
        if pd.isna(row['Name']):
            if not pd.isna(row['Name - NYC.gov Agency List']):
                df.at[idx, 'Name'] = row['Name - NYC.gov Agency List']
            elif not pd.isna(row['Name - Ops']):
                df.at[idx, 'Name'] = row['Name - Ops']
            elif not pd.isna(row['Name - HOO']):
                df.at[idx, 'Name'] = row['Name - HOO']
            elif not pd.isna(row['Agency Name']):
                df.at[idx, 'Name'] = row['Agency Name']
    
    # Apply matches
    final_df = apply_matches(df, matches_df)
    
    # Final check for any remaining blank Names
    for idx, row in final_df.iterrows():
        if pd.isna(row['Name']) and not pd.isna(row['Agency Name']):
            final_df.at[idx, 'Name'] = row['Agency Name']
    
    # Save the final dataset
    final_df.to_csv('data/processed/final_deduplicated_dataset.csv', index=False)
    logging.info(f"Saved final deduplicated dataset with {len(final_df)} records")

if __name__ == "__main__":
    main() 