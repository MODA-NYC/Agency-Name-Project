from rapidfuzz import fuzz, utils
from typing import Optional, Tuple
import pandas as pd

class StringSimilarityScorer:
    """
    Handles string similarity comparisons using multiple metrics optimized for NYC agency names.
    Uses a composite scoring system combining Jaro-Winkler and Token Sort Ratio.
    """
    
    def __init__(
        self,
        jw_weight: float = 0.6,
        token_weight: float = 0.4,
        jw_threshold: float = 85.0,
        token_threshold: float = 80.0,
        composite_threshold: float = 82.0
    ):
        """
        Initialize the scorer with configurable weights and thresholds.
        
        Args:
            jw_weight: Weight for Jaro-Winkler score (default: 0.6)
            token_weight: Weight for Token Sort Ratio score (default: 0.4)
            jw_threshold: Minimum Jaro-Winkler similarity threshold (default: 85.0)
            token_threshold: Minimum Token Sort Ratio threshold (default: 80.0)
            composite_threshold: Minimum composite score threshold (default: 82.0)
        """
        self.jw_weight = jw_weight
        self.token_weight = token_weight
        self.jw_threshold = jw_threshold
        self.token_threshold = token_threshold
        self.composite_threshold = composite_threshold

    def get_composite_score(
        self,
        str1: str,
        str2: str
    ) -> Optional[float]:
        """
        Calculate composite similarity score between two strings.
        
        Args:
            str1: First string to compare
            str2: Second string to compare
            
        Returns:
            Composite similarity score between 0 and 100, or None if below thresholds
        """
        if pd.isna(str1) or pd.isna(str2):
            return None
            
        # Process strings with rapidfuzz utils
        str1 = utils.default_process(str1)
        str2 = utils.default_process(str2)
        
        # Use ratio and token_sort_ratio from rapidfuzz
        basic_score = fuzz.ratio(str1, str2, score_cutoff=self.jw_threshold)
        token_score = fuzz.token_sort_ratio(str1, str2, score_cutoff=self.token_threshold)
        
        # If either score is below threshold, return None
        if basic_score < self.jw_threshold or token_score < self.token_threshold:
            return None
            
        # Calculate weighted composite score
        composite_score = (basic_score * self.jw_weight) + (token_score * self.token_weight)
        
        # Return None if below composite threshold
        return composite_score if composite_score >= self.composite_threshold else None

    def get_detailed_scores(
        self,
        str1: str,
        str2: str
    ) -> Optional[Tuple[float, float, float]]:
        """
        Get detailed breakdown of similarity scores.
        
        Args:
            str1: First string to compare
            str2: Second string to compare
            
        Returns:
            Tuple of (jaro_winkler_score, token_sort_score, composite_score)
            or None if any score is below its threshold
        """
        if pd.isna(str1) or pd.isna(str2):
            return None
            
        # Process strings with rapidfuzz utils
        str1 = utils.default_process(str1)
        str2 = utils.default_process(str2)
        
        # Use ratio_normalized instead of jaro_winkler
        jw_score = fuzz.ratio_normalized(str1, str2)
        token_score = fuzz.token_sort_ratio(str1, str2)
        
        if (jw_score < self.jw_threshold or 
            token_score < self.token_threshold):
            return None
            
        composite_score = (jw_score * self.jw_weight) + (token_score * self.token_weight)
        
        if composite_score < self.composite_threshold:
            return None
            
        return (jw_score, token_score, composite_score)
