import pandas as pd
import logging
from pathlib import Path
import re
from difflib import SequenceMatcher

# Set up logging
logging.basicConfig(level=logging.INFO, format='%(levelname)s: %(message)s')
logger = logging.getLogger(__name__)

def load_dedup_dataset():
    """Load the deduplicated dataset."""
    df = pd.read_csv("data/intermediate/dedup_merged_dataset.csv")
    return df

def load_matches():
    """Load the consolidated matches file."""
    df = pd.read_csv("data/processed/consolidated_matches.csv")
    return df

def normalize_name(name):
    """Apply consistent normalization rules to agency names."""
    if pd.isna(name):
        return ""
    
    name = name.lower()
    
    # Common substitutions
    name = name.replace('&', 'and')
    name = name.replace('nyc', 'new york city')
    name = name.replace('ny', 'new york')
    
    # Remove parentheses and their contents
    name = re.sub(r'\([^)]*\)', '', name)
    
    # Remove punctuation except hyphens
    name = re.sub(r'[^\w\s-]', ' ', name)
    
    # Remove extra whitespace
    name = ' '.join(name.split())
    
    # Standardize organizational terms
    org_terms = {
        'dept': 'department',
        'comm': 'commission',
        'auth': 'authority',
        'admin': 'administration',
        'corp': 'corporation',
        'dev': 'development',
        'svcs': 'services',
        'svc': 'service',
        'tech': 'technology',
        'mgmt': 'management',
        'ops': 'operations',
        'bd': 'board',
        'div': 'division',
        'inst': 'institute',
        'org': 'organization',
        'ofc': 'office',
        'ctr': 'center',
        'sys': 'system',
        'dist': 'district',
        'coord': 'coordinator',
        'coord': 'coordination',
        'cncl': 'council',
        'cmte': 'committee',
        'assoc': 'association',
        'fdn': 'foundation'
    }
    
    words = name.split()
    normalized_words = [org_terms.get(word, word) for word in words]
    name = ' '.join(normalized_words)
    
    # Standardize common patterns
    patterns = [
        (r'department\s+of\s+(\w+)', r'\1 department'),  # "department of X" -> "X department"
        (r'office\s+of\s+(\w+)', r'\1 office'),  # "office of X" -> "X office"
        (r'commission\s+on\s+(\w+)', r'\1 commission'),  # "commission on X" -> "X commission"
        (r'board\s+of\s+(\w+)', r'\1 board'),  # "board of X" -> "X board"
        (r'division\s+of\s+(\w+)', r'\1 division'),  # "division of X" -> "X division"
        (r'center\s+for\s+(\w+)', r'\1 center'),  # "center for X" -> "X center"
        (r'system\s+of\s+(\w+)', r'\1 system'),  # "system of X" -> "X system"
        (r'committee\s+on\s+(\w+)', r'\1 committee'),  # "committee on X" -> "X committee"
        (r'authority\s+for\s+(\w+)', r'\1 authority'),  # "authority for X" -> "X authority"
    ]
    
    for pattern, replacement in patterns:
        name = re.sub(pattern, replacement, name)
    
    return name.strip()

def generate_name_variations(name):
    """Generate common variations of an agency name."""
    variations = {name}
    
    # Add version with 'office of' prefix
    variations.add(f"office of {name}")
    variations.add(f"mayors office of {name}")
    variations.add(f"office of the {name}")
    
    # Add version with 'department of' prefix
    variations.add(f"department of {name}")
    
    # Add version with 'the' prefix and without
    variations.add(f"the {name}")
    variations.add(name.replace("the ", ""))
    
    # Handle hyphenated names
    if '-' in name:
        variations.add(name.replace('-', ' '))
        variations.add(name.replace('-', ' and '))
    
    # Handle reversals (e.g., "education department" <-> "department of education")
    if ' ' in name:
        words = name.split()
        # Try reversing with organizational terms
        org_terms = ['department', 'office', 'commission', 'board', 'division', 'center', 'system', 'committee', 'authority']
        for term in org_terms:
            if term in words:
                idx = words.index(term)
                before = ' '.join(words[:idx])
                after = ' '.join(words[idx+1:])
                variations.add(f"{term} of {before} {after}".strip())
                variations.add(f"{before} {after} {term}".strip())
    
    return variations

def fuzzy_match_score(str1, str2):
    """Calculate fuzzy match score between two strings."""
    return SequenceMatcher(None, str1, str2).ratio()

def create_name_to_id_mapping(dedup_df):
    """Create a mapping of normalized names to record IDs."""
    name_to_id = {}
    fuzzy_matches = {}  # For storing potential fuzzy matches
    
    # First pass: exact matches on normalized names
    for _, row in dedup_df.iterrows():
        record_id = row['RecordID']
        
        # Try all available name fields
        name_fields = [
            'NameNormalized',
            'Agency Name',
            'Name',
            'NameAlphabetized',
            'NameWithAcronym'
        ]
        
        for field in name_fields:
            if pd.notna(row.get(field)):
                norm_name = normalize_name(row[field])
                if norm_name:
                    name_to_id[norm_name] = record_id
                    
                    # Generate and store variations
                    for variation in generate_name_variations(norm_name):
                        name_to_id[variation] = record_id
    
    return name_to_id

def fix_match_ids(matches_df, name_to_id):
    """Fix missing IDs in matches using the name to ID mapping."""
    updated_count = {'source': 0, 'target': 0}
    missing_count = {'source': 0, 'target': 0}
    fuzzy_threshold = 0.85  # Minimum similarity score for fuzzy matches
    
    # Only process verified matches
    verified_matches = matches_df[matches_df['Label'] == 'Match'].copy()
    logger.info(f"Processing {len(verified_matches)} verified matches")
    
    for idx, row in verified_matches.iterrows():
        # Skip if both IDs are null (legacy match)
        if pd.isna(row['SourceID']) and pd.isna(row['TargetID']):
            logger.debug(f"Skipping legacy match: {row['Source']} - {row['Target']}")
            continue
            
        # Try to fix Source ID if missing
        if pd.isna(row['SourceID']):
            source_name = normalize_name(row['Source'])
            
            # Try exact match first
            if source_name in name_to_id:
                matches_df.at[idx, 'SourceID'] = name_to_id[source_name]
                updated_count['source'] += 1
                logger.info(f"Fixed SourceID for: {row['Source']} -> {name_to_id[source_name]}")
            else:
                # Try variations
                found_match = False
                for variation in generate_name_variations(source_name):
                    if variation in name_to_id:
                        matches_df.at[idx, 'SourceID'] = name_to_id[variation]
                        updated_count['source'] += 1
                        logger.info(f"Fixed SourceID using variation: {row['Source']} -> {name_to_id[variation]}")
                        found_match = True
                        break
                
                if not found_match:
                    # Try fuzzy matching as last resort
                    best_match = None
                    best_score = 0
                    for known_name in name_to_id:
                        score = fuzzy_match_score(source_name, known_name)
                        if score > fuzzy_threshold and score > best_score:
                            best_score = score
                            best_match = known_name
                    
                    if best_match:
                        matches_df.at[idx, 'SourceID'] = name_to_id[best_match]
                        updated_count['source'] += 1
                        logger.info(f"Fixed SourceID using fuzzy match: '{row['Source']}' -> '{best_match}' (score: {best_score:.2f})")
                    else:
                        missing_count['source'] += 1
                        logger.debug(f"Could not find ID for source: {row['Source']}")
                
        # Try to fix Target ID if missing
        if pd.isna(row['TargetID']):
            target_name = normalize_name(row['Target'])
            
            # Try exact match first
            if target_name in name_to_id:
                matches_df.at[idx, 'TargetID'] = name_to_id[target_name]
                updated_count['target'] += 1
                logger.info(f"Fixed TargetID for: {row['Target']} -> {name_to_id[target_name]}")
            else:
                # Try variations
                found_match = False
                for variation in generate_name_variations(target_name):
                    if variation in name_to_id:
                        matches_df.at[idx, 'TargetID'] = name_to_id[variation]
                        updated_count['target'] += 1
                        logger.info(f"Fixed TargetID using variation: {row['Target']} -> {name_to_id[variation]}")
                        found_match = True
                        break
                
                if not found_match:
                    # Try fuzzy matching as last resort
                    best_match = None
                    best_score = 0
                    for known_name in name_to_id:
                        score = fuzzy_match_score(target_name, known_name)
                        if score > fuzzy_threshold and score > best_score:
                            best_score = score
                            best_match = known_name
                    
                    if best_match:
                        matches_df.at[idx, 'TargetID'] = name_to_id[best_match]
                        updated_count['target'] += 1
                        logger.info(f"Fixed TargetID using fuzzy match: '{row['Target']}' -> '{best_match}' (score: {best_score:.2f})")
                    else:
                        missing_count['target'] += 1
                        logger.debug(f"Could not find ID for target: {row['Target']}")
    
    logger.info(f"Updated {updated_count['source']} source IDs and {updated_count['target']} target IDs")
    logger.info(f"Still missing {missing_count['source']} source IDs and {missing_count['target']} target IDs")
    
    return matches_df

def main():
    # Load datasets
    dedup_df = load_dedup_dataset()
    matches_df = load_matches()
    
    # Create name to ID mapping
    name_to_id = create_name_to_id_mapping(dedup_df)
    
    # Fix missing IDs
    updated_matches = fix_match_ids(matches_df, name_to_id)
    
    # Save updated matches
    updated_matches.to_csv("data/processed/consolidated_matches.csv", index=False)
    logging.info("Saved updated matches file")

if __name__ == "__main__":
    main() 