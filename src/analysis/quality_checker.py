import pandas as pd
import logging
from typing import Dict, List

class DataQualityChecker:
    """Analyzes data quality and generates reports."""
    
    def __init__(self, output_dir: str = 'data/analysis'):
        self.output_dir = output_dir
        
    def analyze_dataset(self, df: pd.DataFrame, 
                       name_column: str,
                       dataset_name: str) -> Dict:
        """Analyze a dataset for quality issues."""
        results = {
            'total_records': len(df),
            'unique_names': df[name_column].nunique(),
            'null_values': df[name_column].isna().sum(),
            'duplicates': df[df.duplicated(subset=[name_column], keep=False)]
        }
        
        # Save detailed reports
        if not results['duplicates'].empty:
            results['duplicates'].to_csv(
                f'{self.output_dir}/duplicates_{dataset_name}.csv'
            )
        
        return results 