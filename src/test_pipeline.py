import os
import logging
import pandas as pd
from pathlib import Path

def test_file_structure():
    """Test that all required directories and files exist"""
    required_dirs = [
        'data/raw',
        'data/processed',
        'data/intermediate',
        'data/analysis'
    ]
    
    required_files = [
        'data/raw/ops_data.csv',
        'data/raw/nyc_gov_hoo.csv',
        'data/processed/nyc_agencies_export.csv'
    ]
    
    # Check directories
    for dir_path in required_dirs:
        assert os.path.exists(dir_path), f"Missing directory: {dir_path}"
        
    # Check files
    for file_path in required_files:
        assert os.path.exists(file_path), f"Missing file: {file_path}"

def test_processors():
    """Test that processors can load and process data"""
    from preprocessing.ops_processor import OpsDataProcessor
    from preprocessing.hoo_processor import HooDataProcessor
    
    # Test OPS processor
    ops_processor = OpsDataProcessor()
    ops_data = ops_processor.process('data/raw/ops_data.csv')
    assert 'NameNormalized' in ops_data.columns, "OPS processor failed to add NameNormalized"
    
    # Test HOO processor
    hoo_processor = HooDataProcessor()
    hoo_data = hoo_processor.process('data/raw/nyc_gov_hoo.csv')
    assert 'NameNormalized' in hoo_data.columns, "HOO processor failed to add NameNormalized"

def test_matcher():
    """Test that matcher can find matches"""
    from matching.matcher import AgencyMatcher
    
    matcher = AgencyMatcher()
    # Create test data with complex names
    test_df1 = pd.DataFrame({
        'Agency Name': [
            'Test Department',
            'Office of Example, Inc.',
            'Board of Testing & Research'
        ],
        'NameNormalized': [
            'test department',
            'office of example inc',
            'board of testing and research'
        ]
    })
    test_df2 = pd.DataFrame({
        'Agency Name': [
            'Department of Test',
            'Example Office Inc',
            'Testing and Research Board'
        ],
        'NameNormalized': [
            'department of test',
            'example office inc',
            'testing and research board'
        ]
    })
    
    matches = matcher.find_matches(test_df1, test_df2)
    assert len(matches) > 0, "Matcher failed to find any matches"

def main():
    logging.basicConfig(level=logging.INFO)
    logger = logging.getLogger(__name__)
    
    try:
        logger.info("Testing file structure...")
        test_file_structure()
        
        logger.info("Testing processors...")
        test_processors()
        
        logger.info("Testing matcher...")
        test_matcher()
        
        logger.info("All tests passed successfully!")
        logger.info("You can now run main.py safely.")
        
    except AssertionError as e:
        logger.error(f"Test failed: {e}")
    except Exception as e:
        logger.error(f"Unexpected error: {e}")

if __name__ == "__main__":
    main() 