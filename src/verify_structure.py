import os
from typing import List, Dict
import logging

def setup_logging():
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s'
    )
    return logging.getLogger(__name__)

def get_expected_structure() -> Dict[str, List[str]]:
    """Define expected file structure."""
    return {
        'data/analysis': [
            'combined_records_analysis.csv',
            'match_issues_analysis.csv',
            'missing_records_analysis.csv'
        ],
        'data/intermediate': [
            'merged_dataset.csv'
        ],
        'data/processed': [
            'consolidated_matches.csv',
            'final_deduplicated_dataset.csv',
            'nyc_agencies_export.csv'
        ],
        'data/raw': [
            'ops_data.csv',
            'nyc_gov_hoo.csv'
        ],
        'project_plans': [
            'EntityMatchingPlan.md',
            'OriginalPlan.md'
        ],
        'src/analysis': [
            'dataset_validator.py',
            'match_analyzer.py',
            'match_quality_report.py',
            'match_validator.py'
        ],
        'src/matching': [
            '__init__.py',
            'matcher.py',
            'normalizer.py'
        ],
        'src/preprocessing': [
            '__init__.py',
            'base_processor.py',
            'hoo_processor.py',
            'ops_processor.py'
        ],
        'src': [
            'analyze_combined_records.py',
            'analyze_match_issues.py',
            'analyze_merge_completeness.py',
            'analyze_record_counts.py',
            'data_merging.py',
            'main.py'
        ]
    }

def verify_structure(base_dir: str = '.'):
    """Verify all files are in their correct locations."""
    logger = setup_logging()
    expected = get_expected_structure()
    
    # Track issues
    missing_files = []
    missing_dirs = []
    extra_files = []
    
    # Check each directory
    for dir_path, expected_files in expected.items():
        full_path = os.path.join(base_dir, dir_path)
        
        # Check if directory exists
        if not os.path.exists(full_path):
            missing_dirs.append(dir_path)
            continue
        
        # Check for missing files
        actual_files = os.listdir(full_path)
        for expected_file in expected_files:
            if expected_file not in actual_files:
                missing_files.append(os.path.join(dir_path, expected_file))
        
        # Check for extra files
        for actual_file in actual_files:
            if actual_file not in expected_files and not actual_file.startswith('.'):
                extra_files.append(os.path.join(dir_path, actual_file))
    
    # Report findings
    logger.info("\n=== File Structure Verification ===")
    
    if missing_dirs:
        logger.warning("\nMissing Directories:")
        for dir_path in missing_dirs:
            logger.warning(f"- {dir_path}")
    
    if missing_files:
        logger.warning("\nMissing Files:")
        for file_path in missing_files:
            logger.warning(f"- {file_path}")
    
    if extra_files:
        logger.info("\nExtra Files (not in expected structure):")
        for file_path in extra_files:
            logger.info(f"- {file_path}")
    
    if not (missing_dirs or missing_files):
        logger.info("\nAll expected directories and files are present!")

if __name__ == "__main__":
    verify_structure() 