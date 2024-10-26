import re
import logging
from typing import Dict, List, Optional, Set, Tuple
from rapidfuzz import fuzz, utils

class EnhancedMatcher:
    """
    Enhanced matching algorithm with NYC-specific patterns and rules.
    """
    
    def __init__(self):
        # Initialize pattern maps
        self.parent_child_map = {
            'department social services': [
                'human resources administration',
                'department homeless services'
            ]
        }
        
        self.canonical_names = {
            'education department': [
                'department education',
                'public schools new york city'
            ]
        }
        
        self.abbreviation_map = {
            'dept': 'department',
            'comm': 'commission',
            'svcs': 'services',
            'admin': 'administration',
            'auth': 'authority',
            'dev': 'development',
            'mgmt': 'management'
        }
        
        # Compile regex patterns
        self.nyc_patterns = {
            'prefix': re.compile(r'\b(nyc|new york city)\s+'),
            'suffix': re.compile(r',?\s+(nyc|new york city)\b'),
            'dept': re.compile(r'department\s+of\s+(\w+)'),
            'office': re.compile(r'office\s+of\s+(\w+)'),
            'commission': re.compile(r'commission\s+on\s+(\w+)'),
            'mayors_office': re.compile(r"mayor'?s?\s+office\s+of\s+(\w+)"),
            'mayors_x': re.compile(r"mayor'?s?\s+(\w+)\s+office")
        }

    def standardize_nyc_references(self, name: str) -> str:
        """Remove NYC variations consistently."""
        if not name:
            return name
            
        name = self.nyc_patterns['prefix'].sub('', name)
        name = self.nyc_patterns['suffix'].sub('', name)
        return name.strip()

    def standardize_org_type(self, name: str) -> str:
        """Standardize department/office/commission patterns."""
        if not name:
            return name
            
        # Convert "Department of X" to "X Department"
        name = self.nyc_patterns['dept'].sub(r'\1 department', name)
        
        # Convert "Office of X" to "X Office"
        name = self.nyc_patterns['office'].sub(r'\1 office', name)
        
        # Convert "Commission on X" to "X Commission"
        name = self.nyc_patterns['commission'].sub(r'\1 commission', name)
        
        return name.strip()

    def standardize_mayors_office(self, name: str) -> str:
        """Standardize mayor's office variations."""
        if not name:
            return name
            
        # Convert "Mayor's Office of X" to "Office of X"
        name = self.nyc_patterns['mayors_office'].sub(r'office of \1', name)
        
        # Convert "Mayor's X Office" to "X Office"
        name = self.nyc_patterns['mayors_x'].sub(r'\1 office', name)
        
        return name.strip()

    def expand_abbreviations(self, name: str) -> str:
        """Expand common abbreviations."""
        if not name:
            return name
            
        words = name.split()
        expanded = []
        for word in words:
            expanded.append(self.abbreviation_map.get(word.lower(), word))
        return ' '.join(expanded)

    def check_org_type_match(self, str1: str, str2: str) -> bool:
        """Check if organization types match (e.g., both are departments)."""
        org_types = {'department', 'office', 'commission', 'authority', 'board'}
        words1 = set(str1.lower().split())
        words2 = set(str2.lower().split())
        
        common_types = org_types.intersection(words1).intersection(words2)
        return len(common_types) > 0

    def check_acronym_match(self, str1: str, str2: str) -> bool:
        """Check if acronyms match."""
        def extract_acronym(s: str) -> Optional[str]:
            # Look for parenthetical acronyms
            match = re.search(r'\(([A-Z]+)\)', s)
            if match:
                return match.group(1)
            return None
            
        acr1 = extract_acronym(str1)
        acr2 = extract_acronym(str2)
        
        if acr1 and acr2:
            return acr1 == acr2
        return False

    def check_canonical_names(self, name1: str, name2: str) -> bool:
        """Check if names are canonical equivalents."""
        for canonical, variants in self.canonical_names.items():
            if name1.lower() == canonical:
                return name2.lower() in variants
            if name2.lower() == canonical:
                return name1.lower() in variants
        return False

    def get_enhanced_score(self, str1: str, str2: str) -> float:
        """Calculate enhanced similarity score."""
        # Get base similarity score
        base_score = fuzz.ratio(str1, str2) * 0.6 + fuzz.token_sort_ratio(str1, str2) * 0.4
        
        # Add bonuses
        if self.check_org_type_match(str1, str2):
            base_score += 5
        
        if self.check_acronym_match(str1, str2):
            base_score += 10
            
        return min(100, base_score)

    def preprocess_agency_name(self, name: str) -> str:
        """Apply full preprocessing pipeline."""
        if not name:
            return name
            
        name = name.lower()
        name = self.standardize_nyc_references(name)
        name = self.standardize_org_type(name)
        name = self.standardize_mayors_office(name)
        name = self.expand_abbreviations(name)
        return name.strip()

    def find_matches(self, name1: str, name2: str) -> float:
        """Find matches using enhanced matching pipeline."""
        # Apply preprocessing
        std1 = self.preprocess_agency_name(name1)
        std2 = self.preprocess_agency_name(name2)
        
        # Check for exact matches after standardization
        if std1 == std2:
            return 100
            
        # Check canonical name mappings
        if self.check_canonical_names(std1, std2):
            return 100
            
        # Calculate enhanced similarity score
        return self.get_enhanced_score(std1, std2)
