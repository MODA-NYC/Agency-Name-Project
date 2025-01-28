import pandas as pd
import pytest
import logging
from pathlib import Path
import sys
import os

# Add src directory to path
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from generate_matches import generate_potential_matches, EnhancedMatcher
from matching.enhanced_matching import EnhancedMatcher

# Configure logging for tests
logging.basicConfig(level=logging.INFO)

@pytest.fixture
def sample_data():
    """Create sample data for testing"""
    return pd.DataFrame({
        'RecordID': ['REC_000001', 'REC_000002', 'REC_000003', 'REC_000004', 'REC_000005'],
        'Agency Name': [
            'Department of Education',
            'DOE',
            "Mayor's Office of Operations",
            'Office of Operations',
            'Brooklyn Public Library'
        ],
        'NameNormalized': [
            'department of education',
            'doe',
            'mayors office of operations',
            'office of operations',
            'brooklyn public library'
        ]
    })

@pytest.fixture
def existing_pairs():
    """Create sample existing pairs"""
    return {
        ('department of education', 'education department'),
        ('education department', 'department of education')
    }

def test_match_generation_basic(sample_data, existing_pairs):
    """Test basic match generation functionality"""
    matcher = EnhancedMatcher()
    logger = logging.getLogger('test')
    
    matches = generate_potential_matches(sample_data, existing_pairs, matcher, logger)
    
    # Should find matches between:
    # - Department of Education <-> DOE (acronym)
    # - Mayor's Office of Operations <-> Office of Operations (mayor's office variation)
    assert len(matches) >= 2
    
    # Verify match properties
    for match in matches:
        assert 'Source' in match
        assert 'Target' in match
        assert 'Score' in match
        assert 'Label' in match
        assert 'SourceID' in match
        assert 'TargetID' in match
        assert 'Notes' in match
        assert match['Score'] >= 82.0

def test_pattern_detection(sample_data, existing_pairs):
    """Test special pattern detection"""
    matcher = EnhancedMatcher()
    logger = logging.getLogger('test')
    
    matches = generate_potential_matches(sample_data, existing_pairs, matcher, logger)
    
    # Check for specific patterns
    for match in matches:
        if 'DOE' in [match['Source'], match['Target']]:
            assert 'Potential acronym match' in match['Notes']
            
        if "Mayor's Office" in match['Source'] or "Mayor's Office" in match['Target']:
            assert "Mayor's Office variation" in match['Notes']
            
        if 'Brooklyn' in match['Source'] or 'Brooklyn' in match['Target']:
            assert 'Borough variation' in match['Notes']

def test_score_thresholds(sample_data, existing_pairs):
    """Test score thresholds and auto-labeling"""
    matcher = EnhancedMatcher()
    logger = logging.getLogger('test')
    
    matches = generate_potential_matches(sample_data, existing_pairs, matcher, logger)
    
    for match in matches:
        # All matches should meet minimum threshold
        assert match['Score'] >= 82.0
        
        # Check auto-labeling
        if match['Score'] >= 95.0:
            assert match['Label'] == 'Match'
        else:
            assert match['Label'] == ''

def test_existing_pairs_exclusion(sample_data, existing_pairs):
    """Test that existing pairs are excluded"""
    matcher = EnhancedMatcher()
    logger = logging.getLogger('test')
    
    matches = generate_potential_matches(sample_data, existing_pairs, matcher, logger)
    
    # Check that no existing pairs are in the results
    for match in matches:
        pair = (match['Source'].lower(), match['Target'].lower())
        assert pair not in existing_pairs
        assert (pair[1], pair[0]) not in existing_pairs

def test_record_id_preservation(sample_data, existing_pairs):
    """Test that RecordIDs are properly preserved"""
    matcher = EnhancedMatcher()
    logger = logging.getLogger('test')
    
    matches = generate_potential_matches(sample_data, existing_pairs, matcher, logger)
    
    for match in matches:
        # Verify that SourceID and TargetID exist in the original dataset
        source_id = match['SourceID']
        target_id = match['TargetID']
        assert source_id in sample_data['RecordID'].values
        assert target_id in sample_data['RecordID'].values
        
        # Verify they point to the correct records
        source_record = sample_data[sample_data['RecordID'] == source_id].iloc[0]
        target_record = sample_data[sample_data['RecordID'] == target_id].iloc[0]
        assert source_record['Agency Name'] == match['Source']
        assert target_record['Agency Name'] == match['Target'] 