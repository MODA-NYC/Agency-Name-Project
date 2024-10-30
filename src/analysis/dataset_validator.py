import pandas as pd
import logging

class DatasetValidator:
    def __init__(self, output_dir='data/analysis'):
        self.output_dir = output_dir
        self.logger = logging.getLogger(__name__)
    
    def validate_final_dataset(self, df: pd.DataFrame) -> dict:
        """Validate the final deduplicated dataset."""
        results = {
            'total_records': len(df),
            'missing_ids': df['RecordID'].isna().sum(),
            'duplicate_ids': df['RecordID'].duplicated().sum(),
            'invalid_id_format': sum(~df['RecordID'].str.match(r'REC_\d{6}$', na=False)),
            'missing_required_fields': self._check_required_fields(df),
            'column_stats': self._get_column_stats(df),
            'id_continuity': self._check_id_continuity(df)
        }
        
        self._log_validation_results(results)
        return results
    
    def _check_required_fields(self, df):
        required_fields = [
            'NameNormalized',
            'Name - Ops',
            'Name - NYC.gov Redesign',
            'RecordID'
        ]
        return {field: df[field].isna().sum() for field in required_fields}
    
    def _get_column_stats(self, df):
        return {col: {
            'null_count': df[col].isna().sum(),
            'unique_values': df[col].nunique()
        } for col in df.columns}
    
    def _log_validation_results(self, results):
        self.logger.info(f"Total records: {results['total_records']}")
        self.logger.info(f"Records with missing IDs: {results['missing_ids']}")
        self.logger.info(f"Duplicate IDs: {results['duplicate_ids']}")
        self.logger.info(f"Invalid ID format: {results['invalid_id_format']}")
        self.logger.info("\nMissing required fields:")
        for field, count in results['missing_required_fields'].items():
            self.logger.info(f"  {field}: {count}")
    
    def _check_id_continuity(self, df):
        """Check if RecordIDs form a continuous sequence."""
        if 'RecordID' not in df.columns:
            return {'has_ids': False}
            
        # Extract numeric portions of IDs
        numeric_ids = df['RecordID'].str.extract(r'REC_(\d{6})')[0].astype(int)
        min_id = numeric_ids.min()
        max_id = numeric_ids.max()
        expected_count = max_id - min_id + 1
        
        return {
            'has_ids': True,
            'min_id': f"REC_{min_id:06d}",
            'max_id': f"REC_{max_id:06d}",
            'gaps_in_sequence': expected_count != len(numeric_ids),
            'duplicate_ids': len(numeric_ids) != len(numeric_ids.unique())
        }