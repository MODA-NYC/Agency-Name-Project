import re
from typing import Optional
from rapidfuzz import fuzz
import pandas as pd

class NameNormalizer:
    """Handles name normalization and similarity scoring."""
    
    def __init__(self):
        self.patterns = {
            'nyc': re.compile(r'\b(nyc|new york city)\s+'),
            'dept': re.compile(r'department\s+of\s+(\w+)'),
            'office': re.compile(r'office\s+of\s+(\w+)')
        }
    
    def normalize(self, name: str) -> Optional[str]:
        """Apply all normalization rules to a name."""
        if pd.isna(name):
            return None
            
        name = str(name).lower().strip()
        name = self.patterns['nyc'].sub('', name)
        name = self.patterns['dept'].sub(r'\1 department', name)
        name = self.patterns['office'].sub(r'\1 office', name)
        
        return name
    
    def get_similarity_score(self, name1: str, name2: str) -> float:
        """Calculate similarity score between two names."""
        if pd.isna(name1) or pd.isna(name2):
            return 0.0
            
        return fuzz.ratio(self.normalize(name1), self.normalize(name2)) 