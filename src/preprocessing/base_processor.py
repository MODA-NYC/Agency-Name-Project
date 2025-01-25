import pandas as pd
from typing import List
import logging
from matching.normalizer import NameNormalizer
import re

class BaseDataProcessor:
    """Base class for data source processing with common functionality."""
    
    def __init__(self, source_name: str):
        self.source_name = source_name
        self.duplicate_path = f'../data/analysis/raw_duplicates_{source_name}.csv'
        self.normalizer = NameNormalizer()
        
    def standardize_name(self, name: str) -> str:
        """Wrapper for name normalization"""
        return self.normalizer.normalize(name)
    
    def check_raw_duplicates(self, df: pd.DataFrame, key_columns: List[str]) -> pd.DataFrame:
        """Check for duplicates in raw data and save report."""
        duplicates = df[df.duplicated(subset=key_columns, keep=False)]
        if not duplicates.empty:
            duplicates.to_csv(self.duplicate_path)
            logging.info(f"Found {len(duplicates)} duplicates in {self.source_name}")
        return duplicates
    
    def clean_agency_name(self, name: str) -> str:
        """Enhanced name cleaning to prevent splitting."""
        if pd.isna(name):
            return name
            
        # Preserve full names of known patterns
        preserve_patterns = [
            "Mayor's Office of",
            "Department of",
            "Board of",
            "Office of",
            ", Inc.",
            " & ",
            " - "
        ]
        
        result = str(name)
        for pattern in preserve_patterns:
            if pattern in result:
                # Mark pattern to prevent splitting
                placeholder = f"___{pattern.strip().replace(' ', '_')}___"
                result = result.replace(pattern, placeholder)
        
        # Clean other parts of the name
        result = ' '.join(result.split())
        
        # Restore original patterns
        for pattern in preserve_patterns:
            placeholder = f"___{pattern.strip().replace(' ', '_')}___"
            result = result.replace(placeholder, pattern)
            
        return result.strip()
    
    def handle_normalization_duplicates(self, df: pd.DataFrame, name_column: str) -> pd.DataFrame:
        """Handle duplicates that would be created by normalization."""
        # Clean names first
        df[name_column] = df[name_column].apply(self.clean_agency_name)
        
        # Then normalize
        df['NameNormalized'] = df[name_column].apply(self.standardize_name)
        
        # Log any potential issues
        self._log_normalization_issues(df, name_column)
        
        return df
    
    def _log_normalization_issues(self, df: pd.DataFrame, name_column: str):
        """Log potential normalization issues for review."""
        # Check for suspicious patterns with escaped regex
        patterns = {
            'ampersand': r'\s*&\s*',
            'comma': r',\s+',
            'parentheses': r'\(',
            'plus': r'\+',
            'slash': r'/'
        }
        
        for pattern_name, pattern in patterns.items():
            try:
                matches = df[df[name_column].str.contains(pattern, na=False, regex=True)]
                if not matches.empty:
                    logging.warning(f"\nFound {pattern_name} in names that might need review:")
                    for name in matches[name_column]:
                        logging.warning(f"  - {name}")
            except Exception as e:
                logging.error(f"Error checking pattern {pattern_name}: {e}")
    
    def handle_known_duplicates(self, df: pd.DataFrame) -> pd.DataFrame:
        """Default implementation for handling known duplicates."""
        if hasattr(self, 'known_duplicates'):
            for name, rule in self.known_duplicates.items():
                mask = df['Agency Name'] == name
                if rule == 'keep_first':
                    df = df[~(mask & df.duplicated(['Agency Name'], keep='first'))]
                elif rule == 'keep_latest':
                    df = df[~(mask & df.duplicated(['Agency Name'], keep='last'))]
        return df
    
    def validate_name_integrity(self, df: pd.DataFrame, name_column: str) -> pd.DataFrame:
        """Ensure organization names aren't inadvertently split."""
        original_names = set(df[name_column].dropna())
        processed_names = set(
            name.strip() 
            for names in df[name_column].dropna() 
            for name in names.split(',')
        )
        
        if len(processed_names) > len(original_names):
            logging.warning("Name splitting detected. Validating name integrity...")
            # Log any split names for review
            for name in original_names:
                if ',' in str(name) or '&' in str(name) or ' - ' in str(name):
                    logging.warning(f"Potential split name: {name}")
        
        return df