from typing import List, Dict, Optional
import pandas as pd
from .normalizer import NameNormalizer

class AgencyMatcher:
    """Handles matching logic between normalized agency names."""
    
    def __init__(self, normalizer: Optional[NameNormalizer] = None):
        self.normalizer = normalizer or NameNormalizer()
    
    def validate_match(self, row1: pd.Series, row2: pd.Series, score: float) -> bool:
        """Enhanced match validation."""
        # Don't match if names are identical (unless they're from different sources)
        if str(row1['Agency Name']) == str(row2['Agency Name']):
            return False
            
        # Don't match if either name is too short
        if len(str(row1['Agency Name'])) < 4 or len(str(row2['Agency Name'])) < 4:
            return False
            
        # Don't match if one name is completely contained in the other
        if str(row1['Agency Name']).lower() in str(row2['Agency Name']).lower():
            if len(str(row1['Agency Name'])) / len(str(row2['Agency Name'])) < 0.5:
                return False
        
        return True
    
    def find_matches(self, df1: pd.DataFrame, df2: pd.DataFrame,
                    threshold: float = 82.0) -> List[Dict]:
        """Enhanced match finding with better ID management."""
        matches = []
        seen_pairs = set()  # Track unique pairs
        
        for idx1, row1 in df1.iterrows():
            for idx2, row2 in df2.iterrows():
                # Create unique pair identifier
                pair_id = tuple(sorted([str(row1['Agency Name']), str(row2['Agency Name'])]))
                if pair_id in seen_pairs:
                    continue
                    
                score = self.normalizer.get_similarity_score(
                    row1['NameNormalized'],
                    row2['NameNormalized']
                )
                
                if score >= threshold and self.validate_match(row1, row2, score):
                    # Ensure consistent ID format
                    source_id = (row1.get('RecordID') or f'REC_{idx1:06d}')
                    target_id = (row2.get('RecordID') or f'REC_{idx2:06d}')
                    
                    matches.append({
                        'Source': row1['Agency Name'],
                        'Target': row2['Agency Name'],
                        'SourceID': source_id,
                        'TargetID': target_id,
                        'score': score
                    })
                    seen_pairs.add(pair_id)
        
        return matches