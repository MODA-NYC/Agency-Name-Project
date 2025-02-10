import os
import logging
import pandas as pd
import argparse
from typing import List
from preprocessing.ops_processor import OpsDataProcessor
from preprocessing.hoo_processor import HooDataProcessor
from preprocessing.data_normalization import standardize_name as full_standardize_name
from matching.matcher import AgencyMatcher
from analysis.quality_checker import DataQualityChecker
from data_merging import merge_dataframes, clean_merged_data, track_data_provenance, ensure_record_ids
from preprocessing.global_normalization import apply_global_normalization

# Import the apply_matches function from its module.
from apply_matches import apply_matches

def validate_dataframe_columns(df: pd.DataFrame, required_cols: List[str], df_name: str) -> None:
    """Validate that required columns exist in DataFrame."""
    missing_cols = [col for col in required_cols if col not in df.columns]
    if missing_cols:
        raise ValueError(f"Missing required columns in {df_name}: {missing_cols}")

def apply_manual_overrides(final_df: pd.DataFrame) -> pd.DataFrame:
    """
    Applies manual override mappings to the final deduplicated dataset.
    For each record whose current Name (normalized to lowercase and stripped)
    matches one of the override keys, update the Name and Acronym fields as specified.
    """
    # Define override mappings (keys are lowercased old names)
    overrides = {
        'atlantic yards community development corporation (aycdc)': {
            'Name': 'Atlantic Yards Community Development Corporation',
            'Acronym': ''
        },
        'center for brooklyn history (formerly known brooklyn historical society)': {
            'Name': 'Center for Brooklyn History',
            'Acronym': ''
        },
        'boro president bronx': {
            'Name': 'Borough President - Bronx',
            'Acronym': ''
        },
        'boro president brooklyn': {
            'Name': 'Borough President - Brooklyn',
            'Acronym': ''
        },
        'boro president manhattan': {
            'Name': 'Borough President - Manhattan',
            'Acronym': ''
        },
        'boro president queens': {
            'Name': 'Borough President - Queens',
            'Acronym': ''
        },
        'boro president staten island': {
            'Name': 'Borough President - Staten Island',
            'Acronym': ''
        },
        'cig - brooklyn academy of music (bam)': {
            'Name': 'CIG - Brooklyn Academy of Music',
            'Acronym': 'BAM'
        },
        'cig - snug harbor cultural center & botanical garden (shcc)': {
            'Name': 'CIG - Snug Harbor Cultural Center & Botanical Garden',
            'Acronym': 'SHCC'
        },
        'cig - staten island museum (also known as siias)': {
            'Name': 'CIG - Staten Island Museum',
            'Acronym': 'SIIAS'
        },
        "citizens' advisory committee (dcla)": {
            'Name': "Citizens' Advisory Committee",
            'Acronym': ''
        },
        'community services board (formerly known as mental hygiene advisory board)': {
            'Name': 'Community Services Board',
            'Acronym': ''
        },
        'creative communications': {
            'Name': 'Office of Creative Communications',
            'Acronym': ''
        },
        'district attorney - kings county (brooklyn)': {
            'Name': 'District Attorney - Kings County',
            'Acronym': ''
        },
        'district attorney - new york county (manhattan)': {
            'Name': 'District Attorney - New York County',
            'Acronym': ''
        },
        'district attorney - queens county': {
            'Name': 'District Attorney - Queens County',
            'Acronym': ''
        },
        'district attorney - richmond county (staten island)': {
            'Name': 'District Attorney - Richmond County',
            'Acronym': ''
        },
        'counsel to the mayor': {
            'Name': 'Chief Counsel to the Mayor and City Hall',
            'Acronym': ''
        },
        "mayor's office -- media and research analysis": {
            'Name': "Mayor's Office - Media and Research Analysis",
            'Acronym': ''
        },
        "mayor's office -- speechwriting": {
            'Name': "Mayor's Office - Speechwriting",
            'Acronym': ''
        },
        'media, nyc': {
            'Name': 'NYC Media',
            'Acronym': ''
        },
        "nonprofit services, mayor's office of (mons)": {
            'Name': "Mayor's Office of Nonprofit Services",
            'Acronym': 'MONS'
        },
        'queens county public administrator': {
            'Name': 'Public Administrator - Queens County',
            'Acronym': ''
        },
        'richmond county public administrator': {
            'Name': 'Public Administrator - Richmond County',
            'Acronym': ''
        },
        'school construction authority- board of trustees': {
            'Name': 'School Construction Authority - Board of Trustees',
            'Acronym': ''
        },
        'sustainability advisory board (formerly oneync)': {
            'Name': 'Sustainability Advisory Board',
            'Acronym': ''
        },
        'temporary commercial incentive area boundary commission (icap)': {
            'Name': 'Temporary Commercial Incentive Area Boundary Commission',
            'Acronym': 'ICAP'
        },
        'traffic mobility review board (aka congestion pricing)': {
            'Name': 'Traffic Mobility Review Board',
            'Acronym': ''
        }
    }
    
    for idx, row in final_df.iterrows():
        current_name = str(row.get('Name', '')).strip().lower()
        if current_name in overrides:
            override = overrides[current_name]
            final_df.at[idx, 'Name'] = override['Name']
            if 'Acronym' in final_df.columns:
                final_df.at[idx, 'Acronym'] = override['Acronym']
            else:
                final_df['Acronym'] = override['Acronym']
            logging.info(f"Applied override for '{current_name}': set Name to '{override['Name']}' and Acronym to '{override['Acronym']}'")
    return final_df

def create_clean_export(df: pd.DataFrame) -> pd.DataFrame:
    """
    Creates a clean export by removing specified fields and renaming others.
    """
    # Create a copy to avoid modifying the original
    clean_df = df.copy()
    
    # Fields to remove
    fields_to_remove = [
        'PrincipalOfficerName',
        'PrincipalOfficerTitle',
        'PrincipalOfficerGivenName',
        'PrincipalOfficerFamilyName',
        'NameNormalized',
        'Agency Name',
        'Entity type',
        'source'
    ]
    
    # Remove specified fields if they exist
    for field in fields_to_remove:
        if field in clean_df.columns:
            clean_df = clean_df.drop(columns=[field])
    
    # Rename fields
    field_renames = {
        'HeadOfOrganizationName': 'HOO_PrincipalOfficerName',
        'HeadOfOrganizationTitle': 'HOO_PrincipalOfficerTitle',
        'PrincipalOfficerContactURL': 'HOO_PrincipalOfficerContactURL'
    }
    
    # Rename fields if they exist
    clean_df = clean_df.rename(columns={old: new for old, new in field_renames.items() if old in clean_df.columns})
    
    return clean_df

def main(data_dir: str, log_level: str, display: bool, save: bool, apply_matches_flag: bool):
    # Ensure necessary directories exist
    os.makedirs(os.path.join(data_dir, 'analysis'), exist_ok=True)
    os.makedirs(os.path.join(data_dir, 'processed'), exist_ok=True)
    os.makedirs(os.path.join(data_dir, 'intermediate'), exist_ok=True)
    
    logging.basicConfig(level=log_level)
    logger = logging.getLogger(__name__)
    
    try:
        # Initialize processors and other components
        ops_processor = OpsDataProcessor()
        hoo_processor = HooDataProcessor()
        matcher = AgencyMatcher()
        quality_checker = DataQualityChecker(output_dir=os.path.join(data_dir, 'analysis'))
        
        required_files = {
            'ops_data': os.path.join(data_dir, 'raw', 'ops_data.csv'),
            'hoo_data': os.path.join(data_dir, 'raw', 'nyc_gov_hoo.csv'),
            'nyc_agencies': os.path.join(data_dir, 'processed', 'nyc_agencies_export.csv')
        }
        
        for name, path in required_files.items():
            if not os.path.exists(path):
                raise FileNotFoundError(f"Required file not found: {path}")
        
        logger.info("Processing OPS data...")
        ops_data = ops_processor.process(required_files['ops_data'])
        validate_dataframe_columns(ops_data, ['Agency Name', 'NameNormalized', 'RecordID'], 'ops_data')
        logger.info(f"OPS data records: {len(ops_data)}")

        logger.info("Processing HOO data...")
        hoo_data = hoo_processor.process(required_files['hoo_data'])
        validate_dataframe_columns(hoo_data, ['Agency Name', 'NameNormalized', 'RecordID'], 'hoo_data')
        logger.info(f"HOO data records: {len(hoo_data)}")

        nyc_agencies_export = pd.read_csv(required_files['nyc_agencies'])
        validate_dataframe_columns(nyc_agencies_export, ['Name'], 'nyc_agencies_export')
        logger.info(f"NYC agencies export records: {len(nyc_agencies_export)}")
        
        nyc_agencies_export['NameNormalized'] = nyc_agencies_export['Name'].apply(full_standardize_name)
        
        logger.info("Merging datasets...")
        merged_df = merge_dataframes(nyc_agencies_export, ops_data, hoo_data)
        logger.info(f"Merged dataset row count (pre-clean): {len(merged_df)}")
        
        merged_df = clean_merged_data(merged_df)
        logger.info(f"Merged dataset row count (post-clean): {len(merged_df)}")
        
        merged_df = track_data_provenance(merged_df)
        
        if 'NameNormalized' not in merged_df.columns:
            logger.warning("'NameNormalized' column missing, attempting to derive from 'Name'")
            if 'Name' in merged_df.columns:
                merged_df['NameNormalized'] = merged_df['Name'].astype(str).str.lower().str.replace('[^\w\s]', '', regex=True).str.strip()
            else:
                raise ValueError("No 'Name' column to derive 'NameNormalized' from.")
        
        merged_df = ensure_record_ids(merged_df, prefix='REC_')
        merged_df = apply_global_normalization(merged_df)
        
        before_dedup = len(merged_df)
        merged_df = merged_df.drop_duplicates(subset=['RecordID'], keep='first')
        after_dedup = len(merged_df)
        logger.info(f"Removed {before_dedup - after_dedup} duplicate records based on RecordID.")
        
        # Apply manual override mappings to adjust final names and acronyms
        merged_df = apply_manual_overrides(merged_df)
        
        # Create exports directory if it doesn't exist
        exports_dir = os.path.join(data_dir, 'exports')
        os.makedirs(exports_dir, exist_ok=True)
        
        # First, save the full dataset with all fields
        full_export_path = os.path.join(exports_dir, 'full_dataset.csv')
        merged_df.to_csv(full_export_path, index=False)
        logger.info(f"Full dataset saved to {full_export_path}. Row count: {len(merged_df)}")
        
        # By default, apply matches unless explicitly skipped
        if apply_matches_flag:
            matches_path = os.path.join(data_dir, 'processed', 'consolidated_matches.csv')
            logger.info("Applying verified matches to full dataset...")
            apply_matches(
                input_path=full_export_path,
                matches_path=matches_path,
                output_path=full_export_path
            )
            # Reload the dataset after applying matches
            merged_df = pd.read_csv(full_export_path)
        else:
            logger.info("Skipping match application (--skip-matches flag was set)")
        
        # Create and save the clean export
        clean_df = create_clean_export(merged_df)
        clean_export_path = os.path.join(exports_dir, 'clean_dataset.csv')
        clean_df.to_csv(clean_export_path, index=False)
        logger.info(f"Clean dataset saved to {clean_export_path}. Row count: {len(clean_df)}")
        
        # For backward compatibility, also save the clean version as final_deduplicated_dataset.csv
        final_path = os.path.join(data_dir, 'processed', 'final_deduplicated_dataset.csv')
        clean_df.to_csv(final_path, index=False)
        logger.info(f"Clean dataset also saved to {final_path} for backward compatibility")
        
        logger.info("Running quality checks...")
        quality_checker.analyze_dataset(merged_df, 'Name', 'final_deduplicated_dataset')
        
        if display:
            logger.info("\nFinal Dataset Sample:")
            print(clean_df.head())
        
    except Exception as e:
        logger.error(f"Error in main process: {e}")
        raise

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Process NYC organization data.")
    parser.add_argument('--data-dir', type=str, default='data', help='Base directory for data files')
    parser.add_argument('--log-level', type=str, default='INFO', help='Logging level')
    parser.add_argument('--display', action='store_true', help='Display the head of DataFrames')
    parser.add_argument('--save', action='store_true', help='Save intermediate results')
    parser.add_argument('--skip-matches', action='store_true', help='Skip applying verified matches (by default matches are applied)')
    
    args = parser.parse_args()
    # Invert the skip-matches flag to get apply_matches_flag
    main(args.data_dir, args.log_level, args.display, args.save, not args.skip_matches)