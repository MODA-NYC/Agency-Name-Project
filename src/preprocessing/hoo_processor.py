import pandas as pd
from .base_processor import BaseDataProcessor

class HooDataProcessor(BaseDataProcessor):
    """Processor for nyc_gov_hoo.csv with source-specific logic."""
    
    def __init__(self):
        super().__init__('hoo')
        # Restore known duplicate handling rules
        self.known_duplicates = {
            "Mayor's Office": 'keep_first',
            'Office of the Mayor': 'keep_latest',
            'Department of Social Services': 'keep_first',
            'Human Resources Administration': 'keep_latest',
            'Department of Homeless Services': 'keep_latest',
            'NYC Health + Hospitals': 'keep_first',
            'Health and Hospitals Corporation': 'keep_latest'
        }
        
        # Update column mappings to match actual HOO data columns
        self.column_mappings = {
            'Head of Organization': 'HeadOfOrganizationName',
            'HoO Title': 'HeadOfOrganizationTitle',
            'Agency Link (URL)': 'HeadOfOrganizationURL',
            'Agency Name': 'Agency Name'
        }
    
    def process(self, input_path: str) -> pd.DataFrame:
        """Complete processing pipeline for HOO data."""
        df = pd.read_csv(input_path)
        
        # Check raw duplicates
        raw_dupes = self.check_raw_duplicates(df, ['Agency Name'])
        
        # Handle known duplicates based on rules
        df = self.handle_known_duplicates(df)
        
        # Rename columns according to mapping
        for old_col, new_col in self.column_mappings.items():
            if old_col in df.columns:
                df[new_col] = df[old_col]
        
        # Handle normalization duplicates
        df = self.handle_normalization_duplicates(df, 'Agency Name')
        
        return df 