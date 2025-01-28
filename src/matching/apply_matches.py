import os
import sys
import pandas as pd
import logging

def apply_matches(df, matches_path, output_path):
    """Apply matches to merge records in the dataset.
    
    Args:
        df (pd.DataFrame): Input dataset from final_merged_dataset.csv
        matches_path (str): Path to matches CSV file with Source, Target columns
        output_path (str): Path to save final deduplicated dataset
        
    Returns:
        pd.DataFrame: Dataset with matches applied and records merged
    """
    logging.info(f"Loading matches from {matches_path}")
    matches = pd.read_csv(matches_path)
    
    # Remove matches where either Source or Target is NaN
    matches = matches.dropna(subset=['Source', 'Target'])
    
    # Remove self-matches (same name matched to itself)
    matches = matches[matches['Source'] != matches['Target']]
    logging.info(f"Found {len(matches)} non-self matches to apply")
    
    # Create a copy of df to track which records have been processed
    df_copy = df.copy()
    df_copy['processed'] = False
    
    # Track merged records for logging
    total_merged = 0
    
    # Process each match
    for _, match in matches.iterrows():
        source_name = match['Source']
        target_name = match['Target']
        score = match['Score']
        
        # Find records matching source and target using both HOO and Ops name columns
        source_mask = (
            (df_copy['Name - HOO'] == source_name) |
            (df_copy['Name - Ops'] == source_name)
        )
        target_mask = (
            (df_copy['Name - HOO'] == target_name) |
            (df_copy['Name - Ops'] == target_name)
        )
        
        source_records = df_copy[source_mask]
        target_records = df_copy[target_mask]
        
        if len(source_records) == 0 and len(target_records) == 0:
            logging.warning(f"Neither source nor target found for match: {source_name} -> {target_name}")
            continue
            
        if len(source_records) == 0:
            logging.warning(f"Source not found: {source_name}")
            continue
            
        if len(target_records) == 0:
            logging.warning(f"Target not found: {target_name}")
            continue
        
        # Skip if both records have already been processed
        if source_records['processed'].all() and target_records['processed'].all():
            continue
            
        # Merge the records
        source_idx = source_records.index[0]
        target_idx = target_records.index[0]
        
        # Mark records as processed
        df_copy.loc[source_idx, 'processed'] = True
        df_copy.loc[target_idx, 'processed'] = True
        
        # Merge values from target into source where source is null
        for col in df.columns:
            if pd.isna(df_copy.loc[source_idx, col]) and not pd.isna(df_copy.loc[target_idx, col]):
                df_copy.loc[source_idx, col] = df_copy.loc[target_idx, col]
        
        # Remove target record
        df_copy = df_copy.drop(target_idx)
        total_merged += 1
        logging.info(f"Merged: {source_name} <- {target_name}")
        
    logging.info(f"Total records merged: {total_merged}")
    logging.info(f"Final dataset has {len(df_copy)} records")
    
    # Save final dataset
    df_copy = df_copy.drop('processed', axis=1)
    df_copy.to_csv(output_path, index=False)
    
    return df_copy

if __name__ == '__main__':
    # Set up logging
    logging.basicConfig(level=logging.INFO)
    
    # Get absolute paths relative to script location
    script_dir = os.path.dirname(os.path.abspath(__file__))
    project_root = os.path.abspath(os.path.join(script_dir, '../..'))
    data_dir = os.path.join(project_root, 'data', 'processed')
    intermediate_dir = os.path.join(project_root, 'data', 'intermediate')
    
    # Load dataset
    df = pd.read_csv(os.path.join(intermediate_dir, 'merged_dataset.csv'))
    
    # Apply matches
    apply_matches(
        df=df,
        matches_path=os.path.join(data_dir, 'consolidated_matches.csv'),
        output_path=os.path.join(data_dir, 'final_deduplicated_dataset.csv')
    ) 