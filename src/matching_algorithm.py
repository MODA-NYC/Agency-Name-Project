from typing import List, Dict, Tuple, Optional
import pandas as pd
from .string_similarity import StringSimilarityScorer

class AgencyMatcher:
    """
    Implements matching algorithm for NYC agency names with specific handling
    for NYC agency naming patterns and optimizations for performance.
    """
    
    def __init__(self, scorer: Optional[StringSimilarityScorer] = None):
        """
        Initialize matcher with similarity scorer.
        
        Args:
            scorer: StringSimilarityScorer instance (creates new one if None)
        """
        self.scorer = scorer or StringSimilarityScorer()
        self.nyc_patterns = {
            'prefixes': ['nyc', 'new york city'],
            'office_patterns': ["mayor's office of", "office of"],
            'boroughs': ['manhattan', 'brooklyn', 'queens', 'bronx', 'staten island']
        }

    def _create_blocks(self, df: pd.DataFrame, name_column: str) -> Dict[str, pd.DataFrame]:
        """
        Create blocks of records for comparison based on first letter.
        
        Args:
            df: DataFrame containing agency names
            name_column: Name of column containing normalized agency names
            
        Returns:
            Dictionary mapping first letters to DataFrames
        """
        blocks = {}
        
        # Add first letter column for blocking
        df['first_letter'] = df[name_column].str[0]
        
        # Create main blocks by first letter
        for letter in df['first_letter'].unique():
            if pd.notna(letter):
                blocks[letter] = df[df['first_letter'] == letter]
        
        # Create special NYC block
        nyc_mask = df[name_column].str.startswith('nyc', na=False)
        if nyc_mask.any():
            blocks['nyc'] = df[nyc_mask]
            
        # Create special Mayor's Office block
        mayors_mask = df[name_column].str.contains("mayor's office", na=False, case=False)
        if mayors_mask.any():
            blocks['mayors'] = df[mayors_mask]
            
        return blocks

    def _standardize_name_for_comparison(self, name: str) -> str:
        """
        Apply NYC-specific standardization for comparison.
        
        Args:
            name: Agency name to standardize
            
        Returns:
            Standardized name
        """
        if pd.isna(name):
            return name
            
        name = name.lower()
        
        # Handle NYC prefix/suffix
        for prefix in self.nyc_patterns['prefixes']:
            if name.startswith(prefix):
                name = name.replace(prefix, 'nyc')
                break
            if name.endswith(prefix):
                name = name.replace(prefix, '')
                name = f"nyc {name}"
                break
        
        # Handle office patterns
        for pattern in self.nyc_patterns['office_patterns']:
            if pattern in name:
                name = name.replace(pattern, "mayor's office of")
                break
        
        # Handle borough variations
        for borough in self.nyc_patterns['boroughs']:
            if borough in name:
                name = name.replace(f"borough of {borough}", borough)
                name = name.replace(f"{borough} borough", borough)
        
        return name.strip()

    def _extract_acronym(self, name: str) -> tuple[str, Optional[str]]:
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
        
        # Create blocks for efficient comparison
        blocks = self._create_blocks(df, name_column)
        
        # Process each block
        for block_key, block_df in blocks.items():
            # Compare records within block
            for idx1, row1 in block_df.iterrows():
                name1 = row1[name_column]
                
                # Skip if name is missing
                if pd.isna(name1):
                    continue
                    
                # Standardize first name
                std_name1 = self._standardize_name_for_comparison(name1)
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
                    std_name2 = self._standardize_name_for_comparison(name2)
                    name2_clean, acronym2 = self._extract_acronym(std_name2)
                    
                    # Get similarity score
                    score = self.scorer.get_composite_score(name1_clean, name2_clean)
                    
                    # Check acronym match if present
                    acronym_match = False
                    if acronym1 and acronym2:
                        acronym_match = acronym1.lower() == acronym2.lower()
                        
                    # Boost score for acronym matches
                    if acronym_match and score:
                        score = min(100, score + 5)
                    
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
