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

        # Create normalized name column directly from Agency Name
        df['NameNormalized'] = df['Agency Name'].apply(full_standardize_name)
        
        # Preserve original name in OPS_Name column
        df['OPS_Name'] = df['Agency Name']
        
        # Add source column
        df['source'] = 'ops'

        # Add RecordID column if it doesn't exist
        if 'RecordID' not in df.columns:
            df['RecordID'] = df.index.map(lambda x: f'OPS_{x:06d}')

        return df