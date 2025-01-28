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
        
        # Create AgencyNameEnriched by combining Agency Name and HoO Title if available
        if 'HeadOfOrganizationTitle' in df.columns and df['HeadOfOrganizationTitle'].notna().any():
            df['AgencyNameEnriched'] = df.apply(
                lambda row: f"{row['Agency Name']} - {row['HeadOfOrganizationTitle']}"
                if pd.notna(row['HeadOfOrganizationTitle']) else row['Agency Name'],
                axis=1
            )
        else:
            df['AgencyNameEnriched'] = df['Agency Name']
        
        # Preserve original name in HOO_Name column
        df['HOO_Name'] = df['Agency Name']
        
        # Add source column
        df['source'] = 'nyc_gov'

        # Add RecordID column if it doesn't exist
        if 'RecordID' not in df.columns:
            df['RecordID'] = df.index.map(lambda x: f'HOO_{x:06d}')
        
        logging.info(f"HOO data columns after processing: {df.columns.tolist()}")
        logging.info(f"Sample HOO_Name values: {df['HOO_Name'].head().tolist()}")
        logging.info(f"Sample normalized names: {df['NameNormalized'].head().tolist()}")

        return df