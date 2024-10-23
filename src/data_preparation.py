import pandas as pd
import unicodedata
import re
from typing import Optional, List
import logging

class AgencyNamePreprocessor:
    """
    Handles data preparation and cleaning for NYC agency names before matching.
    Implements standardization rules specific to NYC government organizations.
    """
    
    def __init__(self):
        """Initialize preprocessor with standard replacement patterns."""
        self.logger = logging.getLogger(__name__)
        
        # Standard abbreviation replacements
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
            r'\bauth\.?\b': 'authority',
            r'\boff\.?\b': 'office',
            
            # Special characters
            r'&': 'and',
            r'@': 'at',
            r'/': ' and ',
        }
        
        # NYC-specific patterns
        self.nyc_patterns = {
            'prefixes': [
                r'\bnyc\s+|,?\s+nyc\b',
                r'\bnew york city\s+|,?\s+new york city\b'
            ],
            'office_patterns': [
                r"mayor'?s?\s+office\s+of\b",
                r'office\s+of\s+the\s+mayor\b'
            ],
            'boroughs': [
                r'\b(brooklyn|queens|staten island|manhattan|bronx)\b'
            ]
        }
        
        # Special handling for possessives
        self.possessive_fixes = {
            "mayors": "mayor's",
            "commissioners": "commissioner's",
            "childrens": "children's",
            "veterans": "veterans'",
            "peoples": "people's",
            "womens": "women's"
        }

    def _generate_record_id(self, index: int) -> str:
        """
        Generate a unique RecordID.
        
        Args:
            index: Index number for the record
            
        Returns:
            RecordID in format 'REC_XXXXXX'
        """
        return f'REC_{index:06d}'

    def _clean_name(self, name: str) -> Optional[str]:
        """
        Clean and standardize agency name.
        
        Args:
            name: Raw agency name
            
        Returns:
            Cleaned name or None if invalid
        """
        if pd.isna(name):
            return None
            
        # Convert to string and lowercase
        name = str(name).lower().strip()
        
        # Normalize unicode characters to ASCII
        name = unicodedata.normalize('NFKD', name)
        name = name.encode('ascii', 'ignore').decode('ascii')
        
        # Apply standard abbreviation replacements
        for pattern, replacement in self.std_replacements.items():
            name = re.sub(pattern, replacement, name, flags=re.IGNORECASE)
        
        # Fix possessives
        for wrong, right in self.possessive_fixes.items():
            name = re.sub(r'\b' + wrong + r'\b', right, name)
        
        # Remove extra whitespace
        name = ' '.join(name.split())
        
        return name if name else None

    def _apply_nyc_standardization(self, name: str) -> str:
        """
        Apply NYC-specific standardization rules.
        
        Args:
            name: Pre-cleaned agency name
            
        Returns:
            Name with NYC-specific standardization applied
        """
        if pd.isna(name):
            return name
            
        # Remove NYC prefix/suffix variations
        for pattern in self.nyc_patterns['prefixes']:
            name = re.sub(pattern, '', name)
        
        # Standardize mayor's office patterns
        for pattern in self.nyc_patterns['office_patterns']:
            if re.search(pattern, name):
                name = re.sub(pattern, "mayor's office of", name)
                break
        
        # Remove borough names
        for pattern in self.nyc_patterns['boroughs']:
            name = re.sub(pattern, '', name)
        
        # Handle special characters (O'Neill, etc.)
        name = re.sub(r"o'", "o", name)
        
        return name.strip()

    def prepare_dataset_for_matching(
        self,
        df: pd.DataFrame,
        name_column: str,
        additional_columns: Optional[List[str]] = None
    ) -> pd.DataFrame:
        """
        Prepare dataset for matching by cleaning and standardizing names.
        
        Args:
            df: Input DataFrame
            name_column: Name of column containing agency names
            additional_columns: Optional list of additional columns to include
            
        Returns:
            DataFrame with cleaned and standardized names
        """
        self.logger.info("Starting dataset preparation for matching...")
        
        # Create copy to avoid modifying original
        result_df = df.copy()
        
        # Generate RecordIDs
        result_df['RecordID'] = [self._generate_record_id(i) for i in range(len(df))]
        
        # Clean names
        self.logger.info("Cleaning agency names...")
        result_df['NameCleaned'] = result_df[name_column].apply(self._clean_name)
        
        # Apply NYC-specific standardization
        self.logger.info("Applying NYC-specific standardization...")
        result_df['NameStandardized'] = result_df['NameCleaned'].apply(
            self._apply_nyc_standardization
        )
        
        # Remove rows with invalid names
        initial_count = len(result_df)
        result_df = result_df.dropna(subset=['NameStandardized'])
        removed_count = initial_count - len(result_df)
        
        if removed_count > 0:
            self.logger.warning(f"Removed {removed_count} rows with invalid names")
        
        # Select columns to keep
        columns_to_keep = ['RecordID', 'NameStandardized', name_column]
        if additional_columns:
            columns_to_keep.extend(additional_columns)
        
        result_df = result_df[columns_to_keep]
        
        self.logger.info(f"Dataset preparation complete. {len(result_df)} records processed.")
        
        return result_df
