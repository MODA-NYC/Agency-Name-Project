import pandas as pd
import logging
from pathlib import Path
from matching.enhanced_matching import EnhancedMatcher

def setup_logging():
    """Configure logging with appropriate format and level"""
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s'
    )
    return logging.getLogger(__name__)

def load_dedup_dataset():
    """Load the deduplicated dataset"""
    try:
        df = pd.read_csv('../data/intermediate/dedup_merged_dataset.csv')
        return df
    except FileNotFoundError as e:
        raise FileNotFoundError(f"Could not find deduplicated dataset: {e}")

def load_existing_matches():
    """Load existing matches to avoid duplicates"""
    try:
        matches_df = pd.read_csv('../data/processed/consolidated_matches.csv')
        return matches_df
    except FileNotFoundError:
        # If file doesn't exist, return empty DataFrame with required columns
        return pd.DataFrame(columns=['Source', 'Target', 'Score', 'Label', 'SourceID', 'TargetID'])

def get_existing_pairs(matches_df):
    """Get set of existing source-target pairs to avoid duplicates"""
    pairs = set()
    for _, row in matches_df.iterrows():
        # Add both directions to avoid duplicates
        pairs.add((str(row['Source']).lower(), str(row['Target']).lower()))
        pairs.add((str(row['Target']).lower(), str(row['Source']).lower()))
    return pairs

def generate_potential_matches(df, existing_pairs, matcher, logger):
    """Generate potential matches using EnhancedMatcher"""
    new_matches = []
    total_comparisons = 0
    match_count = 0
    
    # Get unique agency names
    agency_names = df['Agency Name'].dropna().unique()
    logger.info(f"Found {len(agency_names)} unique agency names to compare")
    
    # Compare each pair of names
    for i, name1 in enumerate(agency_names):
        for name2 in agency_names[i+1:]:
            total_comparisons += 1
            
            # Skip if pair already exists
            if (name1.lower(), name2.lower()) in existing_pairs:
                continue
                
            # Get similarity score
            score = matcher.find_matches(name1, name2)
            
            # Only keep matches >= 82.0
            if score >= 82.0:
                match_count += 1
                logger.info(f"Found match: '{name1}' - '{name2}' (score: {score})")
                
                # Get record IDs
                source_id = df[df['Agency Name'] == name1]['RecordID'].iloc[0]
                target_id = df[df['Agency Name'] == name2]['RecordID'].iloc[0]
                
                new_matches.append({
                    'Source': name1,
                    'Target': name2,
                    'Score': round(score, 1),
                    'Label': 'Match' if score >= 95 else '',  # Auto-label high confidence matches
                    'SourceID': source_id,
                    'TargetID': target_id
                })
            
            if total_comparisons % 1000 == 0:
                logger.info(f"Processed {total_comparisons} comparisons, found {match_count} matches")
    
    logger.info(f"Completed comparisons. Total: {total_comparisons}, Matches found: {match_count}")
    return new_matches

def save_matches(new_matches, logger):
    """Append new matches to consolidated_matches.csv"""
    if not new_matches:
        logger.info("No new matches to save")
        return
        
    # Convert to DataFrame and sort by score
    new_matches_df = pd.DataFrame(new_matches)
    new_matches_df = new_matches_df.sort_values('Score', ascending=False)
    
    # Append to existing file
    output_file = '../data/processed/consolidated_matches.csv'
    new_matches_df.to_csv(output_file, mode='a', header=False, index=False)
    logger.info(f"Saved {len(new_matches)} new matches to {output_file}")

def main():
    # Setup
    logger = setup_logging()
    matcher = EnhancedMatcher()
    
    try:
        # Load data
        logger.info("Loading deduplicated dataset...")
        df = load_dedup_dataset()
        
        logger.info("Loading existing matches...")
        existing_matches = load_existing_matches()
        existing_pairs = get_existing_pairs(existing_matches)
        logger.info(f"Found {len(existing_pairs)} existing match pairs")
        
        # Generate and save new matches
        logger.info("Generating potential matches...")
        new_matches = generate_potential_matches(df, existing_pairs, matcher, logger)
        
        logger.info("Saving new matches...")
        save_matches(new_matches, logger)
        
        logger.info("Match generation completed successfully")
        
    except Exception as e:
        logger.error(f"Error during match generation: {e}")
        raise

if __name__ == '__main__':
    main() 