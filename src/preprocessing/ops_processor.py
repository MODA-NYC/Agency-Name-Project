import pandas as pd
from .base_processor import BaseDataProcessor

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
        
        # Handle normalization duplicates
        df = self.handle_normalization_duplicates(df, 'Agency Name')
        
        return df
    
    def handle_known_duplicates(self, df: pd.DataFrame) -> pd.DataFrame:
        """Apply predefined rules for known duplicates."""
        for name, rule in self.known_duplicates.items():
            mask = df['Agency Name'] == name
            if rule == 'keep_first':
                df = df[~(mask & df.duplicated(['Agency Name'], keep='first'))]
            elif rule == 'keep_latest':
                df = df[~(mask & df.duplicated(['Agency Name'], keep='last'))]
        return df 