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

        # Create Ops_PrincipalOfficerName from first and last name fields
        # First ensure the fields exist and handle NaN values properly
        df['Agency Head First Name'] = df.get('Agency Head First Name', pd.Series(dtype='object')).fillna('')
        df['Agency Head Last Name'] = df.get('Agency Head Last Name', pd.Series(dtype='object')).fillna('')
        
        # Combine first and last names, handling various edge cases
        df['Ops_PrincipalOfficerName'] = df.apply(
            lambda row: ' '.join(filter(None, [
                str(row['Agency Head First Name']).strip(),
                str(row['Agency Head Last Name']).strip()
            ])),
            axis=1
        )
        
        # Convert empty strings to NaN
        df.loc[df['Ops_PrincipalOfficerName'] == '', 'Ops_PrincipalOfficerName'] = pd.NA
        
        # Add Ops_URL from Agency/Board Website
        df['Ops_URL'] = df['Agency/Board Website']

        # Add RecordID column if it doesn't exist
        if 'RecordID' not in df.columns:
            df['RecordID'] = df.index.map(lambda x: f'OPS_{x:06d}')

        # Rename specific library entries for consistency
        df.loc[df['Agency Name'].str.strip() == "Brooklyn Public Library Board of Trustees", 'Agency Name'] = "Brooklyn Public Library"
        
        # Update normalized name for renamed records
        mask = df['Agency Name'] == "Brooklyn Public Library"
        df.loc[mask, 'NameNormalized'] = full_standardize_name("Brooklyn Public Library")
        df.loc[mask, 'OPS_Name'] = "Brooklyn Public Library"

        return df