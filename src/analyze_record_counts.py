import pandas as pd
import logging
from typing import Set, Dict, List

class RecordAnalyzer:
    def __init__(self):
        self.logger = logging.getLogger(__name__)
        logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
    
    def analyze_record_counts(self):
        """Analyze record counts and identify discrepancies."""
        # Load datasets
        ops_data = pd.read_csv('data/raw/ops_data.csv')
        final_data = pd.read_csv('data/processed/final_deduplicated_dataset.csv')
        
        # Basic counts
        ops_count = len(ops_data)
        final_ops_count = final_data['Name - Ops'].notna().sum()
        
        self.logger.info(f"OPS Data Records: {ops_count}")
        self.logger.info(f"Final Dataset Records with Name - Ops: {final_ops_count}")
        
        # Get unique names from both datasets
        ops_names = set(ops_data['Agency Name'].str.strip().unique())
        final_ops_names = set(
            name.strip() 
            for names in final_data['Name - Ops'].dropna() 
            for name in (names.split(',') if isinstance(names, str) else [names])
        )
        
        # Find missing and extra names
        missing_from_final = ops_names - final_ops_names
        extra_in_final = final_ops_names - ops_names
        
        # Analyze duplicates
        self.logger.info("\nDuplicate Analysis:")
        self._analyze_duplicates(ops_data)
        
        # Report missing records with context
        if missing_from_final:
            self.logger.info("\nRecords in OPS data missing from final dataset:")
            missing_records = ops_data[ops_data['Agency Name'].isin(missing_from_final)]
            for _, record in missing_records.iterrows():
                self.logger.info(f"- {record['Agency Name']} ({record['Entity type']})")
        
        if extra_in_final:
            self.logger.info("\nExtra records in final dataset:")
            for name in sorted(extra_in_final):
                self.logger.info(f"- {name}")
    
    def _analyze_duplicates(self, df: pd.DataFrame):
        """Analyze duplicate records in detail."""
        duplicates = df[df.duplicated(subset=['Agency Name'], keep=False)]
        if not duplicates.empty:
            self.logger.info("\nDuplicate records found:")
            for name, group in duplicates.groupby('Agency Name'):
                self.logger.info(f"\n{name}:")
                for _, record in group.iterrows():
                    self.logger.info(f"  - Entity type: {record['Entity type']}")
                    self.logger.info(f"  - Reports to: {record['Reports to Deputy Mayor/Executive']}")

if __name__ == "__main__":
    analyzer = RecordAnalyzer()
    analyzer.analyze_record_counts() 