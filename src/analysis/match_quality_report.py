import pandas as pd
import logging
from typing import Dict, List

def analyze_match_quality(matches_df: pd.DataFrame) -> Dict:
    """Generate detailed match quality report."""
    report = {
        'total_matches': len(matches_df),
        'score_distribution': matches_df['Score'].describe().to_dict(),
        'matches_by_label': matches_df['Label'].value_counts().to_dict(),
        'suspicious_matches': [],
        'id_issues': []
    }
    
    # Check for suspicious matches
    same_name = matches_df[matches_df['Source'] == matches_df['Target']]
    report['suspicious_matches'] = same_name[['Source', 'Target', 'Score']].values.tolist()
    
    # Check ID format
    id_pattern = r'REC_\d{6}$'
    invalid_ids = matches_df[
        ~matches_df['SourceID'].str.match(id_pattern) |
        ~matches_df['TargetID'].str.match(id_pattern)
    ]
    report['id_issues'] = invalid_ids[['Source', 'Target', 'SourceID', 'TargetID']].values.tolist()
    
    return report 