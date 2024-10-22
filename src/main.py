import argparse
import logging
import os
import sys

# Add the project root to the Python path
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from src.data_loading import get_file_path, load_csv_data
from src.data_normalization import standardize_name
from src.data_merging import merge_dataframes, clean_merged_data, track_data_provenance
from src.preprocess_nyc_gov_hoo import preprocess_nyc_gov_hoo
from src.data_preprocessing import preprocess_agency_data, differentiate_mayors_office
from src.match_generator import PotentialMatchGenerator

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
        
        logging.info("Datasets merged and cleaned successfully.")
        
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
                    min_score=82.0,
                    batch_size=1000
                )
                logging.info("Completed generating potential matches")
            except Exception as e:
                logging.error(f"Error generating matches: {e}")
    else:
        logging.error("One or more datasets failed to load.")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Process NYC organization data.")
    parser.add_argument('--data-dir', type=str, default='data', help='Base directory for data files')
    parser.add_argument('--log-level', type=str, default='INFO', help='Logging level (DEBUG, INFO, WARNING, ERROR, CRITICAL)')
    parser.add_argument('--display', action='store_true', help='Display the head of the DataFrames')
    parser.add_argument('--save', action='store_true', help='Save intermediate DataFrames to CSV files')
    args = parser.parse_args()
    
    main(args.data_dir, args.log_level, args.display, args.save)
