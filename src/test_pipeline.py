import os
import logging
import pandas as pd
import tempfile
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

def test_normalization_thoroughness():
    """
    Test that the normalization logic truly normalizes agency names as expected.
    We will create a temporary CSV with known inputs and expected normalized outputs,
    run the processor on it, and then check if the actual normalized results match expectations.
    """
    from preprocessing.ops_processor import OpsDataProcessor

    # Known input-to-expected-output pairs.
    test_cases = [
        (
            "NYC Health + Hospitals",
            "new york city health and hospitals"
        ),
        (
            "Mayor's Office of Citywide Events Coordination & Management",
            "mayors office of citywide events coordination and management"
        ),
        (
            "Jamaica Bay - Rockaway Parks Conservancy, Inc.",
            "jamaica bay rockaway parks conservancy inc"
        ),
        (
            "Atlantic Yards Community Development Corporation (AYCDC)",
            "atlantic yards community development corporation"
        ),
        (
            "Technology and Innovation, NYC Office of (OTI)",
            "technology and innovation new york city office of"
        )
    ]

    df_input = pd.DataFrame({"Agency Name": [tc[0] for tc in test_cases]})
    with tempfile.NamedTemporaryFile(suffix=".csv", delete=False) as tmp_file:
        temp_path = tmp_file.name
        df_input.to_csv(temp_path, index=False)
    
    try:
        # Process using OPS processor (as a representative test)
        ops_processor = OpsDataProcessor()
        df_processed = ops_processor.process(temp_path)

        # Ensure NameNormalized is present
        assert 'NameNormalized' in df_processed.columns, "NameNormalized column not found after processing."

        # Check each expected normalization
        for i, (_, expected) in enumerate(test_cases):
            actual = df_processed.iloc[i]['NameNormalized']
            assert actual == expected, f"Normalization mismatch.\nInput: {test_cases[i][0]}\nExpected: {expected}\nGot: {actual}"

    finally:
        # Clean up temp file
        if os.path.exists(temp_path):
            os.remove(temp_path)

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

        logger.info("Testing normalization thoroughness...")
        test_normalization_thoroughness()
        
        logger.info("All tests passed successfully!")
        logger.info("You can now run main.py safely.")
        
    except AssertionError as e:
        logger.error(f"Test failed: {e}")
    except Exception as e:
        logger.error(f"Unexpected error: {e}")


if __name__ == "__main__":
    main()
