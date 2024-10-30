import pandas as pd
from typing import Dict
import logging

def validate_matches(matches_df: pd.DataFrame) -> Dict:
    """Validate matches and generate detailed report."""
    report = {
        'duplicate_pairs': [],
        'missing_ids': [],
        'suspicious_matches': []
    }
    
    # Check for duplicate pairs
    pair_counts = matches_df.groupby(['Source', 'Target']).size()
    report['duplicate_pairs'] = pair_counts[pair_counts > 1].index.tolist()
    
    # Check ID format - handle NaN values
    id_pattern = r'REC_\d{6}'
    invalid_ids = matches_df[
        matches_df['SourceID'].notna() & 
        matches_df['TargetID'].notna() & 
        (~matches_df['SourceID'].astype(str).str.match(id_pattern) | 
         ~matches_df['TargetID'].astype(str).str.match(id_pattern))
    ]
    
    # Also count NaN IDs as missing
    missing_ids = matches_df[
        matches_df['SourceID'].isna() | 
        matches_df['TargetID'].isna()
    ]
    
    report['missing_ids'] = (
        invalid_ids[['Source', 'Target']].values.tolist() + 
        missing_ids[['Source', 'Target']].values.tolist()
    )
    
    # Check for suspicious matches
    report['suspicious_matches'] = matches_df[
        matches_df['Source'] == matches_df['Target']
    ][['Source', 'Target', 'Score']].values.tolist()
    
    return report

if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    logger = logging.getLogger(__name__)
    
    try:
        matches_df = pd.read_csv('data/processed/consolidated_matches.csv')
        report = validate_matches(matches_df)
        
        logger.info("\nValidation Report:")
        
        if report['duplicate_pairs']:
            logger.info("\nDuplicate Pairs:")
            for pair in report['duplicate_pairs']:
                logger.info(f"- {pair}")
        else:
            logger.info("No duplicate pairs found")
            
        logger.info(f"\nMissing/Invalid IDs: {len(report['missing_ids'])} pairs")
        if report['missing_ids']:
            logger.info("Sample of records with missing/invalid IDs:")
            for pair in report['missing_ids'][:5]:  # Show first 5
                logger.info(f"- {pair}")
        
        if report['suspicious_matches']:
            logger.info("\nSuspicious Matches (same source/target):")
            for match in report['suspicious_matches']:
                logger.info(f"- Source/Target: {match[0]}, Score: {match[2]}")
        else:
            logger.info("\nNo suspicious matches found")
            
    except Exception as e:
        logger.error(f"Error validating matches: {e}")