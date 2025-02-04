import pandas as pd
from .base_processor import BaseDataProcessor
from .data_normalization import standardize_name as full_standardize_name
import logging

class HooDataProcessor(BaseDataProcessor):
    """Processor for nyc_gov_hoo.csv with source-specific logic."""

    def __init__(self):
        super().__init__('hoo')
        self.known_duplicates = {
            "Mayor's Office": 'keep_first',
            'Office of the Mayor': 'keep_latest',
            'Department of Social Services': 'keep_first',
            'Human Resources Administration': 'keep_latest',
            'Department of Homeless Services': 'keep_latest',
            'NYC Health + Hospitals': 'keep_first',
            'Health and Hospitals Corporation': 'keep_latest'
        }

        self.column_mappings = {
            'Head of Organization': 'HeadOfOrganizationName',
            'HoO Title': 'HeadOfOrganizationTitle',
            'Agency Link (URL)': 'HeadOfOrganizationURL',
            'Agency Name': 'Agency Name'
        }

    def process(self, input_path: str) -> pd.DataFrame:
        """Complete processing pipeline for HOO data."""
        df = pd.read_csv(input_path)
        logging.info(f"HOO data columns before processing: {df.columns.tolist()}")

        # Strip whitespace from column names
        df.columns = df.columns.str.strip()
        logging.info(f"HOO data columns after stripping whitespace: {df.columns.tolist()}")

        # Remove duplicate entries
        df = df[~df['Agency Name'].str.strip().isin([
            "Counsel to the Mayor, Office of",
            "Public Schools,New York City"
        ])]

        # Check raw duplicates
        raw_dupes = self.check_raw_duplicates(df, ['Agency Name'])

        # Handle known duplicates based on rules
        df = self.handle_known_duplicates(df)

        # Rename columns according to mapping
        for old_col, new_col in self.column_mappings.items():
            if old_col in df.columns:
                df[new_col] = df[old_col]

        # Create normalized name directly from Agency Name
        df['NameNormalized'] = df['Agency Name'].apply(full_standardize_name)
        
        # Special handling for "Mayor, Office of the" records
        def enrich_agency_name(row):
            if pd.isna(row['Agency Name']):
                return row['Agency Name']  # Return NaN as is
            agency_name = str(row['Agency Name']).strip()
            if agency_name == "Mayor, Office of the" and pd.notna(row['HeadOfOrganizationTitle']):
                # For Mayor's Office records, combine with HoO Title and normalize
                title = str(row['HeadOfOrganizationTitle']).strip()
                enriched = f"Mayor's Office - {title}"
                logging.info(f"Enriching Mayor's Office record: {enriched}")
                return enriched
            return agency_name

        # Create AgencyNameEnriched using the new enrichment logic
        if 'HeadOfOrganizationTitle' in df.columns:
            df['AgencyNameEnriched'] = df.apply(enrich_agency_name, axis=1)
            
            # Update NameNormalized for Mayor's Office records to include the title
            mayors_office_mask = df['Agency Name'].str.strip() == "Mayor, Office of the"
            df.loc[mayors_office_mask & df['HeadOfOrganizationTitle'].notna(), 'NameNormalized'] = (
                df.loc[mayors_office_mask & df['HeadOfOrganizationTitle'].notna(), 'AgencyNameEnriched']
                .apply(full_standardize_name)
            )
        else:
            df['AgencyNameEnriched'] = df['Agency Name']
        
        # Preserve original name in HOO_Name column
        df['HOO_Name'] = df['AgencyNameEnriched']  # Now using enriched name to preserve the full context
        
        # Add source column
        df['source'] = 'nyc_gov'

        # Add RecordID column if it doesn't exist
        if 'RecordID' not in df.columns:
            df['RecordID'] = df.index.map(lambda x: f'HOO_{x:06d}')
        
        logging.info(f"HOO data columns after processing: {df.columns.tolist()}")
        logging.info(f"Sample HOO_Name values: {df['HOO_Name'].head().tolist()}")
        logging.info(f"Sample normalized names: {df['NameNormalized'].head().tolist()}")
        
        # Log specific info about Mayor's Office records
        mayors_office_records = df[df['Agency Name'].str.strip() == "Mayor, Office of the"]
        logging.info(f"Found {len(mayors_office_records)} Mayor's Office records")
        logging.info("Sample Mayor's Office enriched names:")
        for _, row in mayors_office_records.head().iterrows():
            logging.info(f"Original: {row['Agency Name']} | Enriched: {row['AgencyNameEnriched']}")

        # Handle OTI duplicates - drop the redundant record
        oti_mask = df['Agency Name'].str.contains('Technology and Innovation', case=False, na=False)
        if oti_mask.sum() > 1:
            # Keep the record with "Office of Technology and Innovation" and drop others
            keep_mask = df['Agency Name'].str.startswith('Office of Technology and Innovation', na=False)
            drop_mask = oti_mask & ~keep_mask
            logging.info(f"Dropping {drop_mask.sum()} redundant OTI records")
            df = df[~drop_mask]

        return df