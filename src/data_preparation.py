import pandas as pd
import unicodedata
import re
from typing import Optional, Dict, List

class AgencyNamePreprocessor:
    """
    Handles data preparation and cleaning for NYC agency names before matching.
    Implements standardization rules specific to NYC government organizations.
    """
    
    def __init__(self):
        """Initialize preprocessor with standard replacement patterns."""
        self.std_replacements = {
            # Department variations
            r'\bdept\.?\b': 'department',
            r'\bdepart\.?\b': 'department',
            
            # Administration variations
            r'\badmin\.?\b': 'administration',
            r'\badm\.?\b': 'administration',
            
            # Commission variations
            r'\bcomm\.?\b': 'commission',
            r'\bcom\.?\b': 'commission',
            
            # Common abbreviations
            r'\bsvcs\.?\b': 'services',
            r'\bsvc\.?\b': 'service',
            r'\bmgmt\.?\b': 'management',
            r'\bdev\.?\b': 'development',
            
            # NYC specific patterns
            r'\bny\b': 'new york',
            r'\bnyc\b': 'new york city',
            r'(?<!of\s)manhattan': 'borough of manhattan',
            r'(?<!of\s)brooklyn': 'borough of brooklyn',
            r'(?<!of\s)queens': 'borough of queens',
            r'(?<!of\s)bronx': 'borough of bronx',
            r'(?<!of\s)staten island': 'borough of staten island',
            
            # Special characters
            r'&': 'and',
            r'@': 'at',
            r'/': ' and ',
            
            # Common typos
            r'mayor office': "mayor's office",
            r'mayors office': "mayor's office",
            r'majors office': "mayor's office"
        }
        
        # Special handling for possessives
        self.possessive_fixes = {
            "mayors": "mayor's",
            "commissioners": "commissioner's",
            "childrens": "children's",
            "veterans": "veterans'",
            "peoples": "people's"
        }

    def clean_name_for_matching(self, name: str) -> Optional[str]:
        """
        Clean and standardize agency name for matching.
        
        Args:
            name: Raw agency name
            
        Returns:
            Cleaned and standardized name, or None if invalid
        """
        if pd.isna(name):
            return None
            
        # Convert to string if needed
        name = str(name)
        
        # Convert to lowercase
        name = name.lower().strip()
        
        # Normalize unicode characters
        name = unicodedata.normalize('NFKD', name)
        name = name.encode('ascii', 'ignore').decode('ascii')
        
        # Apply standard replacements
        for pattern, replacement in self.std_replacements.items():
            name = re.sub(pattern, replacement, name, flags=re.IGNORECASE)
        
        # Fix possessives
        for wrong, right in self.possessive_fixes.items():
            name = re.sub(r'\b' + wrong + r'\b', right, name)
        
        # Remove extra whitespace
        name = ' '.join(name.split())
        
        return name if name else None

    def standardize_nyc_prefixes(self, name: str) -> str:
        """
        Standardize NYC-related prefixes in agency names.
        
        Args:
            name: Agency name
            
        Returns:
            Name with standardized NYC prefixes
        """
        if pd.isna(name):
            return name
            
        patterns = [
            (r'^(?:nyc|new york city)\s*[-:]?\s*', 'nyc '),  # Standardize prefix
            (r'\s*[-:]?\s*(?:nyc|new york city)$', ''),      # Remove suffix
        ]
        
        result = name.lower()
        for pattern, replacement in patterns:
            result = re.sub(pattern, replacement, result)
            
        # Ensure NYC prefix if it's a city agency
        if not result.startswith('nyc ') and any(term in result for term in 
            ['department', 'commission', 'authority', 'agency', 'office']):
            result = f'nyc {result}'
            
        return result

    def prepare_dataset_for_matching(
        self,
        df: pd.DataFrame,
        name_column: str,
        additional_columns: Optional[List[str]] = None
    ) -> pd.DataFrame:
        """
        Prepare entire dataset for matching by cleaning and standardizing names.
        
        Args:
            df: Input DataFrame
            name_column: Name of column containing agency names
            additional_columns: Optional list of additional columns to include
            
        Returns:
            DataFrame with cleaned and standardized names
        """
        # Create copy to avoid modifying original
        result_df = df.copy()
        
        # Clean names
        result_df['NameCleaned'] = result_df[name_column].apply(self.clean_name_for_matching)
        
        # Standardize NYC prefixes
        result_df['NameStandardized'] = result_df['NameCleaned'].apply(self.standardize_nyc_prefixes)
        
        # Remove rows with invalid names
        result_df = result_df.dropna(subset=['NameStandardized'])
        
        # Select columns to keep
        columns_to_keep = ['NameStandardized', name_column]
        if additional_columns:
            columns_to_keep.extend(additional_columns)
        
        return result_df[columns_to_keep]

    def extract_agency_components(self, name: str) -> Dict[str, str]:
        """
        Extract components from agency name for advanced matching.
        
        Args:
            name: Standardized agency name
            
        Returns:
            Dictionary containing agency name components
        """
        if pd.isna(name):
            return {}
            
        components = {
            'prefix': '',
            'core_name': '',
            'type': '',
            'borough': '',
            'acronym': ''
        }
        
        # Extract acronym
        acronym_match = re.search(r'\(([^)]+)\)', name)
        if acronym_match:
            components['acronym'] = acronym_match.group(1)
            name = re.sub(r'\s*\([^)]+\)', '', name)
        
        # Extract borough
        for borough in ['manhattan', 'brooklyn', 'queens', 'bronx', 'staten island']:
            if borough in name:
                components['borough'] = borough
                name = name.replace(f"borough of {borough}", '').replace(borough, '').strip()
        
        # Extract prefix
        if name.startswith('nyc '):
            components['prefix'] = 'nyc'
            name = name[4:]
        
        # Extract agency type
        agency_types = ['department', 'commission', 'authority', 'office', 'agency', 'board']
        for agency_type in agency_types:
            if agency_type in name:
                components['type'] = agency_type
                name = name.replace(agency_type, '').strip()
                break
        
        components['core_name'] = name.strip()
        
        return components
