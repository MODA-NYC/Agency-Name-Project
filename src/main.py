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
from apply_matches import apply_matches
import conflict_resolution_fields

logger = logging.getLogger(__name__)

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

def final_cleanup(final_df: pd.DataFrame) -> pd.DataFrame:
    """
    Perform final cleanup of the deduplicated dataset.
    
    Steps:
    1. Remove unwanted columns
    2. Add OPS data with deduplication
    3. Add HOO data with deduplication
    4. Add NYC.gov Agency List data with deduplication
    5. Drop temporary join key columns
    6. Add conflict resolution fields
    7. Rename and reorder columns for final export
    """
    # Step 1: Remove unwanted columns
    cols_to_remove = [
        "PrincipalOfficerGivenName", "PrincipalOfficerFamilyName",
        "Agency Name", "Entity type", "source", "PrincipalOfficerName"
    ]
    final_df = final_df.drop(columns=[col for col in cols_to_remove if col in final_df.columns])
    
    # Helper function to create join keys
    def create_join_key(series: pd.Series) -> pd.Series:
        return series.astype(str).str.lower().str.strip()
    
    # Step 2: Join OPS data using "Name - Ops"
    try:
        ops_raw = pd.read_csv(os.path.join("data", "raw", "ops_data.csv"))
        # Create new OPS columns
        ops_raw["Ops_PrincipalOfficerName"] = ops_raw["Agency Head First Name"].fillna("").str.strip() + " " + ops_raw["Agency Head Last Name"].fillna("").str.strip()
        ops_raw["Ops_PrincipalOfficerTitle"] = ops_raw["Agency Head Title"].fillna("") if "Agency Head Title" in ops_raw.columns else ""
        ops_raw["Ops_URL"] = ops_raw["Agency/Board Website"].fillna("")
        ops_raw["Ops_Acronum"] = ops_raw["Agency Acronym"].fillna("")
        # Create join key in OPS data using "Agency Name"
        ops_raw["join_key_ops"] = create_join_key(ops_raw["Agency Name"])
        # Deduplicate OPS data on join_key_ops
        ops_dedup = ops_raw.drop_duplicates(subset="join_key_ops")
        # Create join key in final_df using "Name - Ops"
        final_df["join_key_ops"] = create_join_key(final_df["Name - Ops"])
        # Merge OPS columns based on join_key_ops
        ops_subset = ops_dedup[["join_key_ops", "Ops_PrincipalOfficerName", "Ops_PrincipalOfficerTitle", "Ops_URL", "Ops_Acronum"]]
        final_df = final_df.merge(ops_subset, on="join_key_ops", how="left")
        logging.info("OPS data joined successfully using deduplicated join key.")
    except Exception as e:
        logging.error(f"Error processing OPS data: {e}")
        # Initialize empty columns if OPS data processing fails
        final_df["Ops_PrincipalOfficerName"] = ""
        final_df["Ops_PrincipalOfficerTitle"] = ""
        final_df["Ops_URL"] = ""
        final_df["Ops_Acronum"] = ""
    
    # Step 3: Join HOO data using "Name - HOO"
    try:
        # Initialize HOO processor
        hoo_processor = HooDataProcessor()
        # Process HOO data
        hoo_data = hoo_processor.process(os.path.join("data", "raw", "nyc_gov_hoo.csv"))
        # Create join key in HOO data
        hoo_data["join_key_hoo"] = create_join_key(hoo_data["AgencyNameEnriched"] if "AgencyNameEnriched" in hoo_data.columns else hoo_data["Agency Name"])
        final_df["join_key_hoo"] = create_join_key(final_df["Name - HOO"])
        # Deduplicate HOO data on join_key_hoo
        hoo_dedup = hoo_data.drop_duplicates(subset="join_key_hoo")
        # Include HOO_URL, HOO_PrincipalOfficerName, and HOO_PrincipalOfficerContactLink
        hoo_subset = hoo_dedup[["join_key_hoo", "HOO_URL", "HOO_PrincipalOfficerName", "HOO_PrincipalOfficerContactLink"]]
        
        # Drop existing HOO columns if they exist to avoid duplicates
        hoo_cols_to_drop = ["HOO_PrincipalOfficerContactLink", "HOO_PrincipalOfficerName", "HOO_URL"]
        final_df = final_df.drop(columns=[col for col in hoo_cols_to_drop if col in final_df.columns])
        
        final_df = final_df.merge(hoo_subset, on="join_key_hoo", how="left")
        logging.info("HOO data joined successfully using deduplicated join key.")
    except Exception as e:
        logging.error(f"Error processing HOO data: {e}")
    
    # Step 4: Join NYC.gov Agency List for Description_NYCWebsite using "Name - NYC.gov Agency List"
    try:
        nyc_list = pd.read_csv(os.path.join("data", "raw", "nyc_gov_agency_list.csv"))
        if "Name - NYC.gov Agency List" in nyc_list.columns and "Description-nyc.gov" in nyc_list.columns:
            nyc_list["join_key_nyc"] = create_join_key(nyc_list["Name - NYC.gov Agency List"])
            final_df["join_key_nyc"] = create_join_key(final_df["Name - NYC.gov Agency List"])
            # Deduplicate nyc_list on join_key_nyc
            nyc_dedup = nyc_list.drop_duplicates(subset="join_key_nyc")
            nyc_subset = nyc_dedup[["join_key_nyc", "Description-nyc.gov"]]
            final_df = final_df.merge(nyc_subset, on="join_key_nyc", how="left")
            final_df = final_df.rename(columns={"Description-nyc.gov": "Description_NYCWebsite"})
            logging.info("NYC.gov Agency List data joined successfully for Description_NYCWebsite using deduplicated join key.")
        else:
            logging.warning("Required columns not found in nyc_gov_agency_list.csv for Description_NYCWebsite.")
    except Exception as e:
        logging.error(f"Error processing nyc_gov_agency_list.csv for Description_NYCWebsite: {e}")
    
    # Step 5: Drop temporary join key columns
    final_df = final_df.drop(columns=["join_key_ops", "join_key_hoo", "join_key_nyc"], errors="ignore")
    
    # Step 6: Add conflict resolution fields
    logging.info("Starting conflict resolution...")
    logging.info(f"Available columns before conflict resolution: {final_df.columns.tolist()}")
    
    # Add conflict resolution fields for URLs - only compute URL_Status, Suggested_URL will be set later
    url_cols = [col for col in ['Ops_URL', 'HOO_URL', 'URL'] if col in final_df.columns]
    logging.info(f"URL columns found for conflict resolution: {url_cols}")
    if url_cols:
        final_df[['URL_Status', '_']] = final_df.apply(
            lambda row: conflict_resolution_fields.resolve_conflict(row, url_cols, url_cols),
            axis=1
        )
        final_df = final_df.drop('_', axis=1)  # Drop the temporary column
        logging.info("URL status resolution completed")
    else:
        logging.warning("No URL columns found for conflict resolution")
        final_df['URL_Status'] = ''
        final_df['Suggested_URL'] = ''

    # Add conflict resolution fields for Principal Officer Titles
    po_title_cols = [col for col in ['Ops_PrincipalOfficerTitle', 'HeadOfOrganizationTitle'] if col in final_df.columns]
    logging.info(f"Principal Officer Title columns found for conflict resolution: {po_title_cols}")
    if po_title_cols:
        final_df[['PO_Title_Status', 'Suggested_PO_Title']] = final_df.apply(
            lambda row: conflict_resolution_fields.resolve_conflict(row, po_title_cols, po_title_cols),
            axis=1
        )
        logging.info("Principal Officer Title conflict resolution completed")
        # Log a sample of the results
        sample_titles = final_df[['PO_Title_Status', 'Suggested_PO_Title']].head()
        logging.info(f"Sample PO Title resolution results:\n{sample_titles}")
    else:
        logging.warning("No Principal Officer Title columns found for conflict resolution")
        final_df['PO_Title_Status'] = ''
        final_df['Suggested_PO_Title'] = ''
    
    logging.info(f"Available columns after conflict resolution: {final_df.columns.tolist()}")
    logging.info("Conflict resolution completed")
    
    # Step 7: Rename and reorder columns for final export
    # Rename columns as specified
    if 'PrincipalOfficerTitle' in final_df.columns:
        final_df = final_df.rename(columns={'PrincipalOfficerTitle': 'HOO_PrincipalOfficerTitle'})
    if 'Ops_Acronum' in final_df.columns:
        final_df = final_df.rename(columns={'Ops_Acronum': 'Ops_Acronym'})

    # Define the desired final column order
    desired_order = [
        "Name",
        "NameAlphabetized",
        "Name - CPO",
        "Name - Checkbook",
        "Name - Greenbook",
        "Name - HOO",
        "Name - NYC Open Data Portal",
        "Name - NYC.gov Agency List",
        "Name - NYC.gov Mayor's Office",
        "Name - ODA",
        "Name - Ops",
        "Name - WeGov",
        "OperationalStatus",
        "PreliminaryOrganizationType",
        "Description",
        "URL",
        "URL_Status",
        "Suggested_URL",
        "ParentOrganization",
        "NYCReportingLine",
        "AuthorizingAuthority",
        "LegalCitation",
        "LegalCitationURL",
        "LegalCitationText",
        "LegalName",
        "AlternateNames",
        "Acronym",
        "AlternateAcronyms",
        "BudgetCode",
        "OpenDatasetsURL",
        "Notes",
        "FoundingYear",
        "SunsetYear",
        "URISlug",
        "DateCreated",
        "DateModified",
        "LastVerifiedDate",
        "NameWithAcronym",
        "NameAlphabetizedWithAcronym",
        "NameNormalized",
        "RecordID",
        "merged_from",
        "data_source",
        "HOO_PrincipalOfficerName",
        "HOO_PrincipalOfficerTitle",
        "HOO_URL",
        "HOO_PrincipalOfficerContactLink",
        "Ops_PrincipalOfficerName",
        "Ops_PrincipalOfficerTitle",
        "Ops_URL",
        "Ops_Acronym",
        "PO_Name_Status",
        "Suggested_PO_Name",
        "PO_Title_Status",
        "Suggested_PO_Title"
    ]

    # Reorder columns, only including those that exist in the DataFrame
    existing_columns = [col for col in desired_order if col in final_df.columns]
    
    # Log any columns in the DataFrame that weren't in our desired order
    extra_columns = [col for col in final_df.columns if col not in desired_order]
    if extra_columns:
        logging.warning(f"Additional columns found that weren't in desired order: {extra_columns}")
        existing_columns.extend(extra_columns)
    
    # Apply the column ordering
    final_df = final_df[existing_columns]
    
    return final_df

def main(data_dir: str, log_level: str, display: bool, save: bool, skip_apply_matches: bool):
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
        
        # Apply final cleanup with deduplication of external data
        final_df = final_cleanup(merged_df)
        final_path = os.path.join(data_dir, 'processed', 'final_deduplicated_dataset.csv')
        final_df.to_csv(final_path, index=False)
        logger.info(f"Final deduplicated dataset saved to {final_path}. Final row count: {len(final_df)}")
        
        logger.info("Running quality checks...")
        quality_checker.analyze_dataset(final_df, 'NameNormalized', 'final_deduplicated_dataset')
        
        # Default behavior: run apply_matches unless --skip-apply-matches is specified
        if not skip_apply_matches:
            intermediate_dir = os.path.join(data_dir, 'intermediate')
            matches_path = os.path.join(data_dir, 'processed', 'consolidated_matches.csv')
            logger.info("Running apply_matches (default behavior)...")
            apply_matches(input_path=final_path, matches_path=matches_path,
                          output_path=os.path.join(data_dir, 'processed', 'final_deduplicated_dataset.csv'))
            
            # Reload the dataset after apply_matches
            final_df = pd.read_csv(os.path.join(data_dir, 'processed', 'final_deduplicated_dataset.csv'))
            
            # Find Principal Officer Name columns
            po_name_cols = [col for col in final_df.columns if 'PrincipalOfficerName' in col]
            logger.info(f"Found Principal Officer Name columns: {po_name_cols}")

            # Log NYPD row values before processing
            nypd_rows = final_df[final_df['Name'].str.contains('Police Department', na=False)]
            for _, row in nypd_rows.iterrows():
                logger.info("NYPD Principal Officer values:")
                for col in po_name_cols:
                    logger.info(f"{col}: {row[col]}")

            # Process each row for conflict resolution for Principal Officer Name
            for idx, row in final_df.iterrows():
                # Get the conflict resolution result for this row
                result = conflict_resolution_fields.resolve_conflict(
                    row, 
                    po_name_cols,
                    ['HOO_PrincipalOfficerName', 'Ops_PrincipalOfficerName']  # Priority order
                )
                
                # result is a pandas Series with [status, suggested]
                final_df.at[idx, 'PO_Name_Status'] = result.iloc[0]
                final_df.at[idx, 'Suggested_PO_Name'] = result.iloc[1]
            
            # New Block: Set Suggested_URL based on priority order
            logger.info("Setting Suggested_URL based on priority order (HOO_URL > URL > Ops_URL)...")
            for idx, row in final_df.iterrows():
                # Handle NaN/None values and strip whitespace
                hoo_url = str(row.get('HOO_URL', '')).strip() if pd.notna(row.get('HOO_URL')) else ''
                url = str(row.get('URL', '')).strip() if pd.notna(row.get('URL')) else ''
                ops_url = str(row.get('Ops_URL', '')).strip() if pd.notna(row.get('Ops_URL')) else ''
                
                # Apply priority logic
                if hoo_url:  # If HOO_URL has a non-empty value
                    suggested_url = hoo_url
                elif url:    # If URL has a non-empty value
                    suggested_url = url
                else:       # Otherwise use Ops_URL (even if empty)
                    suggested_url = ops_url
                
                final_df.at[idx, 'Suggested_URL'] = suggested_url
            
            logger.info("Completed setting Suggested_URL values")
            
            # Save the updated DataFrame
            final_df.to_csv(final_path, index=False)
            
        else:
            logger.info("Skipping apply_matches as per flag.")
        
        if display:
            logger.info("\nFinal Dataset Sample:")
            print(final_df.head())
        
    except Exception as e:
        logger.error(f"Error in main process: {e}")
        raise

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Process NYC organization data.")
    parser.add_argument('--data-dir', type=str, default='data', help='Base directory for data files')
    parser.add_argument('--log-level', type=str, default='INFO', help='Logging level')
    parser.add_argument('--display', action='store_true', help='Display the head of DataFrames')
    parser.add_argument('--save', action='store_true', help='Save intermediate results')
    # New flag: --skip-apply-matches. By default, apply_matches is run.
    parser.add_argument('--skip-apply-matches', action='store_true', help='Skip running apply_matches')
    
    args = parser.parse_args()
    main(args.data_dir, args.log_level, args.display, args.save, args.skip_apply_matches)