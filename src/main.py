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
import traceback

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
    Creates a clean export version of the dataset with standardized fields and handling of missing values.
    """
    clean_df = df.copy()
    
    # Fill missing values with empty strings for string columns
    string_columns = [
        'Name', 'NameAlphabetized', 'OperationalStatus', 'PreliminaryOrganizationType',
        'Description', 'URL', 'ParentOrganization', 'NYCReportingLine',
        'AuthorizingAuthority', 'LegalCitation', 'LegalCitationURL', 'LegalCitationText',
        'LegalName', 'AlternateNames', 'Acronym', 'AlternateAcronyms', 'BudgetCode',
        'OpenDatasetsURL', 'Notes', 'URISlug', 'NameWithAcronym', 'NameAlphabetizedWithAcronym',
        'RecordID', 'merged_from', 'data_source', 'Description-nyc.gov'
    ]
    
    # Add optional columns if they exist
    optional_columns = [
        'Ops_PrincipalOfficerName', 'Ops_URL', 'HOO_PrincipalOfficerName',
        'HOO_PrincipalOfficerTitle', 'HOO_PrincipalOfficerContactURL',
        'HOO_URL', 'Suggested_PrincipalOfficerName', 'PO_Name_Status',
        'URL_Status', 'Suggested_URL', 'PO_Notes', 'URL_Notes'
    ]
    
    # Add existing optional columns to string_columns
    for col in optional_columns:
        if col in clean_df.columns:
            string_columns.append(col)
    
    # Fill missing values for string columns that exist in the DataFrame
    for col in string_columns:
        if col in clean_df.columns:
            clean_df[col] = clean_df[col].fillna('')
    
    # Handle numeric columns if they exist
    numeric_columns = ['FoundingYear', 'SunsetYear']
    for col in numeric_columns:
        if col in clean_df.columns:
            clean_df[col] = pd.to_numeric(clean_df[col], errors='coerce')
    
    # Handle date columns if they exist
    date_columns = ['DateCreated', 'DateModified', 'LastVerifiedDate']
    for col in date_columns:
        if col in clean_df.columns:
            clean_df[col] = pd.to_datetime(clean_df[col], errors='coerce')
    
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
        
        # --- New code block to merge NYC.gov descriptions ---
        nyc_gov_agency_list_path = os.path.join(data_dir, 'raw', 'nyc_gov_agency_list.csv')
        try:
            nyc_gov_df = pd.read_csv(nyc_gov_agency_list_path)
            if 'Name - NYC.gov Agency List' in nyc_gov_df.columns and 'Description-nyc.gov' in nyc_gov_df.columns:
                nyc_gov_subset = nyc_gov_df[['Name - NYC.gov Agency List', 'Description-nyc.gov']]
                merged_df = merged_df.merge(nyc_gov_subset, on='Name - NYC.gov Agency List', how='left')
                logger.info("Merged NYC.gov descriptions into the dataset")
            else:
                logger.warning("NYC.gov agency list does not contain expected columns")
        except Exception as e:
            logger.error(f"Error merging NYC.gov descriptions: {e}")
        # --- End of NYC.gov descriptions block ---

        # --- New code block: Merge Phase2 Manual Adjustments ---
        adjustments_path = os.path.join(data_dir, 'raw', 'phase2_manual_adjustments.csv')
        try:
            # Try different encodings
            try:
                adjustments_df = pd.read_csv(adjustments_path, encoding='utf-8')
            except UnicodeDecodeError:
                try:
                    adjustments_df = pd.read_csv(adjustments_path, encoding='latin1')
                except UnicodeDecodeError:
                    adjustments_df = pd.read_csv(adjustments_path, encoding='cp1252')
            
            logger.info(f"Loaded {len(adjustments_df)} records from phase2_manual_adjustments.csv")
            
            # Normalize names for matching by converting to lowercase and stripping whitespace
            merged_df['Name_normalized'] = merged_df['Name'].str.lower().str.strip()
            adjustments_df['Name_normalized'] = adjustments_df['Name'].str.lower().str.strip()
            
            # Define fields to update from adjustments
            fields_to_update = {
                'OperationalStatus_Confirmed': 'OperationalStatus',
                'PreliminaryOrganizationType_Confirmed': 'PreliminaryOrganizationType',
                'NameAlphabetized_Confirmed': 'NameAlphabetized',
                'URL_Confirmed': 'URL',
                'PrincipalOfficerName_Confirmed': 'PrincipalOfficerName',
                'HOO_URL': 'HOO_URL',  # Direct field mapping
                'Ops_URL': 'Ops_URL',  # Direct field mapping
                'HOO_PrincipalOfficerName': 'HOO_PrincipalOfficerName',  # Direct field mapping
                'Ops_PrincipalOfficerName': 'Ops_PrincipalOfficerName',  # Direct field mapping
                'PO_Notes': 'PO_Notes',  # Direct field mapping
                'URL Notes': 'URL_Notes'  # Note the space in source column
            }
            
            # Get all columns from adjustments_df that end with _Confirmed and aren't already mapped
            additional_confirmed_fields = [col for col in adjustments_df.columns 
                                        if col.endswith('_Confirmed') 
                                        and col not in fields_to_update]
            
            for field in additional_confirmed_fields:
                target_field = field.replace('_Confirmed', '')
                fields_to_update[field] = target_field
            
            logger.info(f"Fields to update from adjustments: {', '.join(fields_to_update.keys())}")
            
            # Prepare columns for merge
            merge_columns = ['Name_normalized']
            for source_field in fields_to_update.keys():
                if source_field in adjustments_df.columns:
                    merge_columns.append(source_field)
                else:
                    logger.warning(f"Column {source_field} not found in adjustments file")
            
            # Join adjustments_df with merged_df on the normalized name field
            before_merge = len(merged_df)
            merged_df = merged_df.merge(
                adjustments_df[merge_columns],
                on='Name_normalized',
                how='left'
            )
            
            # Update fields with confirmed values where available
            updates_made = 0
            for source_field, target_field in fields_to_update.items():
                if source_field not in merged_df.columns:
                    continue
                    
                # Create target field if it doesn't exist
                if target_field not in merged_df.columns:
                    merged_df[target_field] = None
                    logger.info(f"Created new field: {target_field}")
                
                # Convert values to strings for comparison, handling NaN values
                mask = merged_df[source_field].notna() & (merged_df[source_field].astype(str).str.strip() != '')
                if mask.any():
                    # Update the target field where we have confirmed values
                    merged_df.loc[mask, target_field] = merged_df.loc[mask, source_field]
                    updates_made += mask.sum()
                    logger.info(f"Updated {mask.sum()} records for {target_field} from {source_field}")
            
            logger.info(f"Total updates made from phase2_manual_adjustments.csv: {updates_made}")
            
            # Drop the temporary columns
            columns_to_drop = ['Name_normalized'] + [col for col in fields_to_update.keys() if col in merged_df.columns]
            merged_df = merged_df.drop(columns_to_drop, axis=1)
            
        except Exception as e:
            logger.error(f"Error merging phase2 manual adjustments: {e}")
            logger.error(traceback.format_exc())
        # --- End of Phase2 Manual Adjustments block ---

        # Apply manual override mappings to adjust final names and acronyms
        merged_df = apply_manual_overrides(merged_df)
        
        # First, save the full dataset with all fields
        full_export_path = os.path.join(data_dir, 'exports', 'full_dataset.csv')
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
            
        # Create and save the clean export (now includes computing new fields)
        clean_df = create_clean_export(merged_df)
        
        # Remove unwanted fields and reorder columns for clean export
        keep_order = [
            "Name",
            "NameAlphabetized",
            "Name - HOO",
            "OperationalStatus",
            "PreliminaryOrganizationType",
            "Description",
            "URL",
            "AlternateNames",
            "Acronym",
            "AlternateAcronyms",
            "BudgetCode",
            "OpenDatasetsURL",
            "FoundingYear",
            "PrincipalOfficerName",
            "PrincipalOfficerTitle",
            "PrincipalOfficerContactURL",
            "Name - CPO",
            "Name - Checkbook",
            "Name - Greenbook",
            "Name - NYC Open Data Portal",
            "Name - NYC.gov Agency List",
            "Name - NYC.gov Mayor's Office",
            "Name - ODA",
            "Name - Ops",
            "Name - WeGov"
        ]
        clean_df = clean_df.reindex(columns=keep_order)
        
        clean_export_path = os.path.join(data_dir, 'exports', 'clean_dataset.csv')
        clean_df.to_csv(clean_export_path, index=False)
        logger.info(f"Clean dataset saved to {clean_export_path}. Row count: {len(clean_df)}")
        
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