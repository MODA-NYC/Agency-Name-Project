from typing import List, Dict, Tuple, Optional
import pandas as pd
from .string_similarity import StringSimilarityScorer

class AgencyMatcher:
    """
    Implements matching algorithm for NYC agency names with specific handling
    for NYC agency naming patterns and optimizations for performance.
    """
    
    def __init__(self):
        """Initialize the matcher with default settings and similarity scorer."""
        self.scorer = StringSimilarityScorer()
        self.nyc_patterns = {
            'prefixes': ['nyc', 'new york city'],
            'office_patterns': ["mayor's office of", "office of"],
            'boroughs': ['manhattan', 'brooklyn', 'queens', 'bronx', 'staten island'],
            'common_abbrev': {
                'dept': 'department',
                'comm': 'commission',
                'admin': 'administration',
                'dev': 'development',
                'svcs': 'services',
                'mgmt': 'management'
            }
        }

    def _standardize_nyc_patterns(self, name: str) -> str:
        """
        Standardize NYC-specific naming patterns.
        
        Args:
            name: Agency name to standardize
            
        Returns:
            Standardized agency name
        """
        name = name.lower().strip()
        
        # Standardize NYC prefix
        for prefix in self.nyc_patterns['prefixes']:
            if name.startswith(prefix):
                name = name.replace(prefix, 'nyc')
                break
            if name.endswith(prefix):
                name = name.replace(prefix, '')
                name = f"nyc {name}"
                break
                
        # Standardize office patterns
        for pattern in self.nyc_patterns['office_patterns']:
            if pattern in name:
                name = name.replace(pattern, "mayors office of")
                break
                
        # Standardize borough names
        for borough in self.nyc_patterns['boroughs']:
            if borough in name:
                name = name.replace(f"borough of {borough}", borough)
                name = name.replace(f"{borough} borough", borough)
        
        return name.strip()

    def _extract_acronym(self, name: str) -> Tuple[str, Optional[str]]:
        """
        Extract parenthetical acronyms from agency names.
        
        Args:
            name: Agency name possibly containing acronym
            
        Returns:
            Tuple of (cleaned_name, acronym)
        """
        import re
        
        # Find parenthetical content
        match = re.search(r'\(([^)]+)\)', name)
        if not match:
            return name, None
            
        acronym = match.group(1)
        cleaned_name = re.sub(r'\s*\([^)]+\)', '', name).strip()
        
        return cleaned_name, acronym

    def create_blocks(self, df: pd.DataFrame, column: str) -> Dict[str, pd.DataFrame]:
        """
        Create blocks of records for comparison based on first letter.
        
        Args:
            df: DataFrame containing agency names
            column: Name of column containing normalized agency names
            
        Returns:
            Dictionary mapping first letters to DataFrames
        """
        blocks = {}
        df['first_letter'] = df[column].str[0]
        
        # Create main blocks
        for letter in df['first_letter'].unique():
            if pd.notna(letter):
                blocks[letter] = df[df['first_letter'] == letter]
        
        # Create NYC block separately (common prefix)
        nyc_mask = df[column].str.startswith('nyc', na=False)
        if nyc_mask.any():
            blocks['nyc'] = df[nyc_mask]
        
        return blocks

    def find_potential_matches(
        self,
        df: pd.DataFrame,
        name_column: str,
        min_score: float = 82.0
    ) -> List[Dict[str, any]]:
        """
        Find potential matches in the dataset using blocking and similarity scoring.
        
        Args:
            df: DataFrame containing agency names
            name_column: Name of column containing agency names
            min_score: Minimum composite score to consider a match
            
        Returns:
            List of dictionaries containing match information
        """
        potential_matches = []
        blocks = self.create_blocks(df, name_column)
        
        # Process each block
        for block_key, block_df in blocks.items():
            # Compare records within block
            for idx1, row1 in block_df.iterrows():
                name1 = row1[name_column]
                
                # Skip if name is missing
                if pd.isna(name1):
                    continue
                    
                # Standardize first name
                std_name1 = self._standardize_nyc_patterns(name1)
                name1_clean, acronym1 = self._extract_acronym(std_name1)
                
                # Compare with other records in block
                for idx2, row2 in block_df.iterrows():
                    # Skip self-comparisons and already processed pairs
                    if idx1 >= idx2:
                        continue
                        
                    name2 = row2[name_column]
                    if pd.isna(name2):
                        continue
                        
                    # Standardize second name
                    std_name2 = self._standardize_nyc_patterns(name2)
                    name2_clean, acronym2 = self._extract_acronym(std_name2)
                    
                    # Get similarity score
                    score = self.scorer.get_composite_score(name1_clean, name2_clean)
                    
                    # Check acronym match if present
                    acronym_match = False
                    if acronym1 and acronym2:
                        acronym_match = acronym1.lower() == acronym2.lower()
                    
                    # Add to potential matches if score is high enough
                    if score and score >= min_score:
                        potential_matches.append({
                            'source_id': idx1,
                            'target_id': idx2,
                            'source_name': name1,
                            'target_name': name2,
                            'similarity_score': score,
                            'acronym_match': acronym_match
                        })
        
        return potential_matches
