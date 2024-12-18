import pandas as pd
from .base_processor import BaseDataProcessor
from .data_normalization import standardize_name as full_standardize_name

class OpsDataProcessor(BaseDataProcessor):
    """Processor for ops_data.csv with source-specific logic."""

    def __init__(self):
        super().__init__('ops')
        self.known_duplicates = {
            'Gracie Mansion Conservancy': 'keep_first',
            'Commission on Gender Equity': 'keep_latest',
            'Commission on Racial Equity': 'keep_first',
            'Hudson Yards Infrastructure Corporation': 'keep_first',
            'Financial Information Services Agency': 'keep_first'
        }

    def process(self, input_path: str) -> pd.DataFrame:
        """Complete processing pipeline for ops data."""
        df = pd.read_csv(input_path)

        # Check raw duplicates
        raw_dupes = self.check_raw_duplicates(df, ['Agency Name'])

        # Handle known duplicates based on rules
        df = self.handle_known_duplicates(df)

        # Create AgencyNameEnriched by combining Agency Name and Entity type if available
        if 'Entity type' in df.columns and df['Entity type'].notna().any():
            df['AgencyNameEnriched'] = df.apply(
                lambda row: f"{row['Agency Name']} - {row['Entity type']}"
                if pd.notna(row['Entity type']) else row['Agency Name'],
                axis=1
            )
        else:
            df['AgencyNameEnriched'] = df['Agency Name']

        # Handle normalization duplicates - now use AgencyNameEnriched for normalization
        df['NameNormalized'] = df['AgencyNameEnriched'].apply(full_standardize_name)

        # Add RecordID column if it doesn't exist
        if 'RecordID' not in df.columns:
            df['RecordID'] = df.index.map(lambda x: f'OPS_{x:06d}')

        return df