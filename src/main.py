import argparse
import logging
import os
import sys
import pandas as pd
import networkx as nx

# Add the project root to the Python path
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from src.data_loading import get_file_path, load_csv_data
from src.data_normalization import standardize_name
from src.data_merging import merge_dataframes, clean_merged_data, track_data_provenance
from src.preprocess_nyc_gov_hoo import preprocess_nyc_gov_hoo
from src.data_preprocessing import preprocess_agency_data, differentiate_mayors_office
from src.match_generator import PotentialMatchGenerator
from src.string_matching import generate_record_id

def setup_logging(log_level):
    logging.basicConfig(level=log_level, format='%(asctime)s - %(levelname)s - %(message)s')

def display_head(df, name, n=5):
    logging.info(f"Displaying the first {n} rows of {name}:")
    print(df.head(n))

def save_intermediate(df, name, output_dir):
    os.makedirs(output_dir, exist_ok=True)  # Ensure the directory exists
    output_path = os.path.join(output_dir, f"{name}.csv")
    df.to_csv(output_path, index=False)
    logging.info(f"Saved {name} to {output_path}")

def remove_redundancies(merged_df, deduplicated_df, match_groups):
    if not match_groups:  # If no matches were found
        logging.warning("No redundancies to remove. Returning original dataset.")
        return merged_df
        
    # Create a set of all RecordIDs in match groups
    matched_record_ids = set(record_id for group in match_groups for record_id in group)
    
    # Identify non-duplicate records
    non_duplicate_records = merged_df[~merged_df['RecordID'].isin(matched_record_ids)]
    
    # Append non-duplicate records to the deduplicated DataFrame
    final_df = pd.concat([deduplicated_df, non_duplicate_records], ignore_index=True)
    
    return final_df

def main(data_dir, log_level, display, save):
    setup_logging(log_level)
    
    logging.info("Starting data processing...")

    # Load the existing primary data source
    nyc_agencies_export_path = get_file_path(data_dir, 'processed', 'nyc_agencies_export.csv')
    nyc_agencies_export = load_csv_data(nyc_agencies_export_path)

    # Load the new data sources for Phase 2
    nyc_gov_hoo_path = get_file_path(data_dir, 'raw', 'nyc_gov_hoo.csv')
    nyc_gov_hoo = load_csv_data(nyc_gov_hoo_path)

    ops_data_path = get_file_path(data_dir, 'raw', 'ops_data.csv')
    ops_data = load_csv_data(ops_data_path)

    # Remove rows where 'Name' is null in nyc_agencies_export
    nyc_agencies_export = nyc_agencies_export[nyc_agencies_export['Name'].notnull()]

    # Remove rows where 'Agency Name' is null in nyc_gov_hoo and ops_data
    nyc_gov_hoo = nyc_gov_hoo[nyc_gov_hoo['Agency Name'].notnull()]
    ops_data = ops_data[ops_data['Agency Name'].notnull()]

    # Preprocess nyc_gov_hoo
    nyc_gov_hoo = preprocess_nyc_gov_hoo(nyc_gov_hoo)

    # Preprocess ops_data
    ops_data = preprocess_agency_data(ops_data)

    # Check if all datasets are loaded
    if nyc_agencies_export is not None and nyc_gov_hoo is not None and ops_data is not None:
        # Normalize the Name fields after preprocessing
        nyc_agencies_export['NameNormalized'] = nyc_agencies_export['Name'].apply(standardize_name)
        nyc_gov_hoo['NameNormalized'] = nyc_gov_hoo['Agency Name'].apply(standardize_name)
        ops_data['NameNormalized'] = ops_data['Agency Name'].apply(standardize_name)
        
        # Ensure 'Name' column exists
        nyc_gov_hoo['Name'] = nyc_gov_hoo['Agency Name']
        ops_data['Name'] = ops_data['Agency Name']

        # Differentiate Mayor's Office entries
        nyc_agencies_export = differentiate_mayors_office(nyc_agencies_export)
        nyc_gov_hoo = differentiate_mayors_office(nyc_gov_hoo)
        ops_data = differentiate_mayors_office(ops_data)
        
        logging.info("All datasets loaded and normalized successfully.")
        
        # Prepare secondary dataframes for merging
        nyc_gov_fields = [
            'Agency Name',
            'Head of Organization ',
            'HoO Title',
            'HoO Contact Link',
            'Agency Link (URL)',
            'Name - NYC.gov Redesign - Original Value'  # Include the new field here
        ]
        ops_data_fields = ['Agency Name']
        
        secondary_dfs = [
            (nyc_gov_hoo, nyc_gov_fields, 'nyc_gov_'),
            (ops_data, ops_data_fields, 'ops_')
        ]
        
        # Merge the datasets
        merged_df = merge_dataframes(nyc_agencies_export, secondary_dfs)
        
        # Rename columns as specified
        column_mapping = {
            'nyc_gov_Agency Name': 'Name - NYC.gov Redesign',
            'nyc_gov_Head of Organization ': 'HeadOfOrganizationName',
            'nyc_gov_HoO Title': 'HeadOfOrganizationTitle',
            'nyc_gov_HoO Contact Link': 'HeadOfOrganizationURL',
            'ops_Agency Name': 'Name - Ops'
        }
        merged_df = merged_df.rename(columns=column_mapping)
        
        # Clean the merged data
        merged_df = clean_merged_data(merged_df)
        
        # Track data provenance
        merged_df = track_data_provenance(merged_df)
        
        # Add RecordID to merged dataset
        merged_df['RecordID'] = merged_df.apply(lambda row: generate_record_id({
            'Source': row['Name'],
            'Target': row['NameNormalized']
        }), axis=1)
        
        logging.info("Datasets merged, cleaned, and IDs generated successfully.")
        
        # Display the head of the merged DataFrame if requested
        if display:
            display_head(merged_df, 'merged_dataset')
        
        # Save intermediate DataFrame if requested
        if save:
            intermediate_dir = os.path.join(data_dir, 'intermediate')
            save_intermediate(merged_df, 'merged_dataset', intermediate_dir)
        
        # Initialize match generator
        match_generator = PotentialMatchGenerator(
            matches_file=os.path.join(data_dir, 'processed', 'consolidated_matches.csv')
        )
        
        # Process new matches if merged dataset was created successfully
        if merged_df is not None:
            logging.info("Generating potential matches...")
            try:
                match_generator.process_new_matches(
                    df=merged_df,
                    name_column='NameNormalized',
                    min_score=75.0,  # Lower threshold for normalized names
                    batch_size=1000
                )
                logging.info("Completed generating potential matches")
            except Exception as e:
                logging.error(f"Error generating matches: {e}")
        
        # Load the confirmed matches
        matches_path = os.path.join(data_dir, 'processed', 'consolidated_matches.csv')
        if not os.path.exists(matches_path):
            logging.warning(f"No matches file found at {matches_path}")
            matches_df = pd.DataFrame(columns=['Source', 'Target', 'Score', 'Label', 'SourceID', 'TargetID'])
        else:
            matches_df = pd.read_csv(matches_path)
        
        # Build match groups
        match_groups = build_match_groups(matches_df)
        logging.info(f"Found {len(match_groups)} match groups")
        
        # Deduplicate the dataset
        deduplicated_df = deduplicate_dataset(merged_df, match_groups)
        
        # Remove redundancies and finalize the dataset
        final_df = remove_redundancies(merged_df, deduplicated_df, match_groups)
        
        # Save the final deduplicated dataset
        final_deduplicated_path = os.path.join(data_dir, 'processed', 'final_deduplicated_dataset.csv')
        final_df.to_csv(final_deduplicated_path, index=False)
        logging.info(f"Final deduplicated dataset saved to {final_deduplicated_path}")
    else:
        logging.error("One or more datasets failed to load.")

def process_matches(merged_df):
    """Process matches and clean up the consolidated_matches.csv file"""
    # Generate new potential matches
    generate_potential_matches(merged_df)
    
    # Clean up any duplicates in consolidated_matches.csv
    cleanup_consolidated_matches()
    
    # Log summary of matches
    matches_df = pd.read_csv('data/processed/consolidated_matches.csv')
    auto_matches = matches_df[matches_df['Score'] == 100].shape[0]
    pending_review = matches_df[matches_df['Label'] == ''].shape[0]
    
    logging.info(f"Generated matches summary:")
    logging.info(f"- Auto-labeled matches (100% score): {auto_matches}")
    logging.info(f"- Pending manual review: {pending_review}")

def build_match_groups(matches_df):
    # Create graph from confirmed matches
    G = nx.Graph()
    
    # Check if SourceID and TargetID columns exist
    if 'SourceID' not in matches_df.columns or 'TargetID' not in matches_df.columns:
        logging.warning("SourceID or TargetID columns missing from matches file")
        # Add empty SourceID and TargetID columns if they don't exist
        matches_df['SourceID'] = None
        matches_df['TargetID'] = None
    
    # Add edges for matches with valid IDs
    for _, row in matches_df[matches_df['Label'].str.lower() == 'match'].iterrows():
        if pd.notna(row['SourceID']) and pd.notna(row['TargetID']):
            G.add_edge(row['SourceID'], row['TargetID'])
    
    # Get connected components (groups of related matches)
    components = list(nx.connected_components(G))
    logging.info(f"Found {len(components)} match groups")
    return components

def merge_duplicate_records(group, merged_df):
    records = merged_df[merged_df['RecordID'].isin(group)]
    
    # Check for edge cases
    check_for_conflicts(records)
    
    # Get the actual column names from the DataFrame
    nyc_gov_col = 'Name - NYC.gov Redesign'  # Note the capital R in Redesign
    ops_col = 'Name - Ops'
    
    # Merge fields according to rules
    merged = {
        'NameNormalized': records['NameNormalized'].iloc[0],  # Arbitrary choice
        'Name - Ops': ', '.join(records[ops_col].dropna().unique()) if ops_col in records.columns else '',
        'Name - NYC.gov Redesign': ', '.join(records[nyc_gov_col].dropna().unique()) if nyc_gov_col in records.columns else '',
        'PrincipalOfficerName': records['PrincipalOfficerName'].fillna(records['HeadOfOrganizationName']).iloc[0] if 'PrincipalOfficerName' in records.columns else None,
        'PrincipalOfficerTitle': records['PrincipalOfficerTitle'].fillna(records['HeadOfOrganizationTitle']).iloc[0] if 'PrincipalOfficerTitle' in records.columns else None,
        'PrincipalOfficerContactURL': records['HeadOfOrganizationURL'].iloc[0] if 'HeadOfOrganizationURL' in records.columns else None
    }
    
    return merged

def check_for_conflicts(records):
    names = records['HeadOfOrganizationName'].dropna().unique()
    if len(names) > 1:
        logging.warning(f"Conflict detected in HeadOfOrganizationName: {names}")

def deduplicate_dataset(merged_df, match_groups):
    if not match_groups:  # If no matches were found
        logging.warning("No match groups found. Returning original dataset with minimal processing.")
        # Create a copy of merged_df with only the columns we need
        result_df = merged_df.copy()
        # Ensure all required columns exist
        required_columns = [
            'NameNormalized',
            'Name - Ops',
            'Name - NYC.gov Redesign',  # Updated column name
            'PrincipalOfficerName',
            'PrincipalOfficerTitle',
            'HeadOfOrganizationName',
            'HeadOfOrganizationTitle',
            'HeadOfOrganizationURL'
        ]
        for col in required_columns:
            if col not in result_df.columns:
                result_df[col] = None
        return result_df
        
    deduplicated_records = []
    
    for group in match_groups:
        merged_record = merge_duplicate_records(group, merged_df)
        deduplicated_records.append(merged_record)
    
    # Convert to DataFrame
    deduplicated_df = pd.DataFrame(deduplicated_records)
    
    return deduplicated_df

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Process NYC organization data.")
    parser.add_argument('--data-dir', type=str, default='data', help='Base directory for data files')
    parser.add_argument('--log-level', type=str, default='INFO', help='Logging level (DEBUG, INFO, WARNING, ERROR, CRITICAL)')
    parser.add_argument('--display', action='store_true', help='Display the head of the DataFrames')
    parser.add_argument('--save', action='store_true', help='Save intermediate DataFrames to CSV files')
    args = parser.parse_args()
    
    main(args.data_dir, args.log_level, args.display, args.save)
