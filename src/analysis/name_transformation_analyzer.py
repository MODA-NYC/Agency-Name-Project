import pandas as pd
import logging
from typing import Dict, List

class NameTransformationAnalyzer:
    def __init__(self):
        self.logger = logging.getLogger(__name__)
        
    def analyze_transformations(self, original_df: pd.DataFrame, 
                              processed_df: pd.DataFrame,
                              original_col: str,
                              processed_col: str) -> Dict:
        """Analyze how names were transformed during processing."""
        results = {
            'split_names': [],
            'merged_names': [],
            'modified_names': []
        }
        
        original_names = set(original_df[original_col].dropna())
        processed_names = set(processed_df[processed_col].dropna())
        
        # Find split names
        for orig_name in original_names:
            splits = [proc_name for proc_name in processed_names 
                     if proc_name in orig_name and proc_name != orig_name]
            if splits:
                results['split_names'].append({
                    'original': orig_name,
                    'splits': splits
                })
        
        # Find merged names
        for proc_name in processed_names:
            if ',' in proc_name or ' and ' in proc_name:
                results['merged_names'].append(proc_name)
        
        return results

if __name__ == "__main__":
    analyzer = NameTransformationAnalyzer()
    
    # Load datasets
    ops_data = pd.read_csv('data/raw/ops_data.csv')
    final_data = pd.read_csv('data/processed/final_deduplicated_dataset.csv')
    
    results = analyzer.analyze_transformations(
        ops_data, final_data,
        'Agency Name', 'Name - Ops'
    )
    
    print("\nSplit Names Analysis:")
    for item in results['split_names']:
        print(f"Original: {item['original']}")
        print(f"Split into: {item['splits']}\n")
        
    print("\nMerged Names:")
    for name in results['merged_names']:
        print(f"- {name}") 