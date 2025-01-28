import os
import sys
import pandas as pd
import logging

logging.basicConfig(level=logging.INFO)

def apply_matches(input_path, matches_path, output_path):
    # Read input data and matches
    df = pd.read_csv(input_path)
    matches = pd.read_csv(matches_path)
    
    # Create a copy of the original dataframe
    result_df = df.copy()
    
    # Track which records have been processed
    result_df['processed'] = False
    
    # Process each match
    for _, match in matches.iterrows():
        source_name = match['Source']
        target_name = match['Target']
        
        # Skip invalid matches
        if pd.isna(source_name) or pd.isna(target_name):
            continue
            
        # Try original direction first
        source_mask = (
            (result_df['Name'].str.lower() == source_name.lower()) |
            (result_df['Name - Ops'].str.lower() == source_name.lower()) |
            (result_df['Name - HOO'].str.lower() == source_name.lower())
        )
        target_mask = (
            (result_df['Name'].str.lower() == target_name.lower()) |
            (result_df['Name - Ops'].str.lower() == target_name.lower()) |
            (result_df['Name - HOO'].str.lower() == target_name.lower())
        )
        
        # If no matches found, try reverse direction
        if not (source_mask.any() and target_mask.any()):
            source_mask = (
                (result_df['Name'].str.lower() == target_name.lower()) |
                (result_df['Name - Ops'].str.lower() == target_name.lower()) |
                (result_df['Name - HOO'].str.lower() == target_name.lower())
            )
            target_mask = (
                (result_df['Name'].str.lower() == source_name.lower()) |
                (result_df['Name - Ops'].str.lower() == source_name.lower()) |
                (result_df['Name - HOO'].str.lower() == source_name.lower())
            )
            if source_mask.any() and target_mask.any():
                # Swap names for logging
                source_name, target_name = target_name, source_name
        
        # Get the records
        source_records = result_df[source_mask]
        target_records = result_df[target_mask]
        
        if not source_records.empty and not target_records.empty:
            # Get records with Ops or HOO data
            source_with_ops = source_records[source_records['Name - Ops'].notna()]
            target_with_ops = target_records[target_records['Name - Ops'].notna()]
            source_with_hoo = source_records[source_records['Name - HOO'].notna()]
            target_with_hoo = target_records[target_records['Name - HOO'].notna()]
            
            # If both have Ops or HOO data, keep both
            if (not source_with_ops.empty and not target_with_ops.empty) or \
               (not source_with_hoo.empty and not target_with_hoo.empty):
                source_record = source_with_ops.iloc[0] if not source_with_ops.empty else source_with_hoo.iloc[0]
                target_record = target_with_ops.iloc[0] if not target_with_ops.empty else target_with_hoo.iloc[0]
                
                # Mark both as processed but keep both
                result_df.loc[source_record.name, 'processed'] = True
                result_df.loc[target_record.name, 'processed'] = True
                continue
            
            # If only one has Ops or HOO data, use that as the base record
            if not source_with_ops.empty:
                source_record = source_with_ops.iloc[0]
                target_record = target_records.iloc[0]
            elif not target_with_ops.empty:
                source_record = source_records.iloc[0]
                target_record = target_with_ops.iloc[0]
            elif not source_with_hoo.empty:
                source_record = source_with_hoo.iloc[0]
                target_record = target_records.iloc[0]
            elif not target_with_hoo.empty:
                source_record = source_records.iloc[0]
                target_record = target_with_hoo.iloc[0]
            else:
                # Neither has Ops or HOO data, use unprocessed records if available
                source_record = source_records[~source_records['processed']].iloc[0] if not source_records[~source_records['processed']].empty else source_records.iloc[0]
                target_record = target_records[~target_records['processed']].iloc[0] if not target_records[~target_records['processed']].empty else target_records.iloc[0]
            
            # Start with the record that has Ops or HOO data
            if (pd.isna(source_record['Name - Ops']) and not pd.isna(target_record['Name - Ops'])) or \
               (pd.isna(source_record['Name - HOO']) and not pd.isna(target_record['Name - HOO'])):
                merged_record = target_record.copy()
                # Only copy non-null values from source
                for col in source_record.index:
                    if not pd.isna(source_record[col]) and (pd.isna(merged_record[col]) or not col.startswith('Name')):
                        merged_record[col] = source_record[col]
            else:
                merged_record = source_record.copy()
                # Only copy non-null values from target
                for col in target_record.index:
                    if not pd.isna(target_record[col]) and (pd.isna(merged_record[col]) or not col.startswith('Name')):
                        merged_record[col] = target_record[col]
            
            # Mark both records as processed
            result_df.loc[source_record.name, 'processed'] = True
            result_df.loc[target_record.name, 'processed'] = True
            
            # Update the record with merged data
            if (pd.isna(source_record['Name - Ops']) and not pd.isna(target_record['Name - Ops'])) or \
               (pd.isna(source_record['Name - HOO']) and not pd.isna(target_record['Name - HOO'])):
                # Keep target record if it has Ops or HOO data
                result_df.loc[target_record.name] = merged_record
                if source_record.name != target_record.name:
                    result_df = result_df.drop(source_record.name)
            else:
                # Keep source record
                result_df.loc[source_record.name] = merged_record
                # Only drop target if it doesn't have Ops or HOO data
                if target_record.name != source_record.name and pd.isna(target_record['Name - Ops']) and pd.isna(target_record['Name - HOO']):
                    result_df = result_df.drop(target_record.name)
        else:
            logging.warning(f"Neither source nor target found for match: {source_name} -> {target_name}")
    
    # Drop the processed flag
    result_df = result_df.drop('processed', axis=1)
    
    # Reorder columns to put Name columns first
    name_cols = ['Name', 'NameAlphabetized'] + sorted([col for col in result_df.columns if col.startswith('Name - ')])
    other_cols = [col for col in result_df.columns if col not in name_cols]
    ordered_cols = name_cols + other_cols
    result_df = result_df[ordered_cols]
    
    # Save the result
    result_df.to_csv(output_path, index=False)
    logging.info(f"Total records merged: {len(df) - len(result_df)}")
    logging.info(f"Final dataset has {len(result_df)} records")

if __name__ == "__main__":
    # Set up logging
    logging.basicConfig(level=logging.INFO)
    
    # Get absolute paths relative to script location
    script_dir = os.path.dirname(os.path.abspath(__file__))
    project_root = os.path.abspath(os.path.join(script_dir, '../..'))
    data_dir = os.path.join(project_root, 'data', 'processed')
    intermediate_dir = os.path.join(project_root, 'data', 'intermediate')
    
    # Apply matches
    apply_matches(
        input_path=os.path.join(intermediate_dir, 'merged_dataset.csv'),
        matches_path=os.path.join(data_dir, 'consolidated_matches.csv'),
        output_path=os.path.join(data_dir, 'final_deduplicated_dataset.csv')
    ) 