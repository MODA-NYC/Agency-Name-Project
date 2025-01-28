import pandas as pd
import logging
from pathlib import Path
from typing import List, Dict, Set, Tuple
from src.matching.enhanced_matching import EnhancedMatcher

def setup_logging() -> logging.Logger:
    """Configure logging with appropriate format and level"""
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s'
    )
    return logging.getLogger(__name__)

def load_merged_dataset() -> pd.DataFrame:
    """Load the merged dataset with proper encoding"""
    try:
        df = pd.read_csv('data/processed/final_deduplicated_dataset.csv', encoding='utf-8')
        required_cols = {'RecordID', 'Agency Name', 'NameNormalized'}
        
        if not required_cols.issubset(df.columns):
            missing = required_cols - set(df.columns)
            raise ValueError(f"Missing required columns: {missing}")
            
        return df
        
    except Exception as e:
        logging.error(f"Error loading merged dataset: {e}")
        raise

def generate_potential_matches(
    df: pd.DataFrame,
    existing_pairs: Set[Tuple[str, str]],
    matcher: EnhancedMatcher,
    logger: logging.Logger
) -> List[Dict]:
    """
    Generate potential matches using enhanced matching logic.
    
    Args:
        df: DataFrame containing agency records
        existing_pairs: Set of existing matches to avoid duplicates
        matcher: Configured EnhancedMatcher instance
        logger: Logger instance
        
    Returns:
        List of potential matches with metadata
    """
    matches = []
    comparison_count = 0
    match_count = 0
    
    # Get unique normalized names to compare
    names = df['NameNormalized'].unique()
    total_comparisons = len(names) * (len(names) - 1) // 2
    
    logger.info(f"Starting match generation with {len(names)} unique names")
    logger.info(f"Total comparisons to process: {total_comparisons}")
    
    for i, name1 in enumerate(names):
        for name2 in names[i+1:]:
            comparison_count += 1
            
            # Progress logging
            if comparison_count % 1000 == 0:
                progress = (comparison_count / total_comparisons) * 100
                logger.info(f"Progress: {progress:.1f}% ({comparison_count}/{total_comparisons})")
            
            # Skip if already matched
            pair = tuple(sorted([name1, name2]))
            if pair in existing_pairs:
                continue
            
            # Get similarity score
            score = matcher.find_matches(name1, name2)
            
            if score >= 82.0:  # Using threshold from project plan
                match_count += 1
                logger.info(f"Found match: '{name1}' - '{name2}' (score: {score})")
                
                # Get record details
                record1 = df[df['NameNormalized'] == name1].iloc[0]
                record2 = df[df['NameNormalized'] == name2].iloc[0]
                
                # Determine match type
                notes = []
                if score == 100:
                    notes.append("Exact match after normalization")
                if any(term in name1.lower() or term in name2.lower() 
                      for term in ["dept", "department"]):
                    notes.append("Department variation")
                if "nyc" in name1.lower() or "nyc" in name2.lower():
                    notes.append("NYC prefix/suffix variation")
                
                matches.append({
                    'Source': name1,
                    'Target': name2,
                    'Score': round(score, 1),
                    'Label': 'Match' if score >= 95 else '',
                    'SourceID': record1['RecordID'],
                    'TargetID': record2['RecordID'],
                    'Notes': '; '.join(notes) if notes else ''
                })
    
    logger.info(f"Completed match generation:")
    logger.info(f"- Processed {comparison_count} comparisons")
    logger.info(f"- Found {match_count} potential matches")
    logger.info(f"- Auto-labeled {len([m for m in matches if m['Label'] == 'Match'])} high-confidence matches")
    
    return matches

def save_matches(matches: List[Dict], output_path: Path, logger: logging.Logger) -> None:
    """Save generated matches to CSV, preserving existing matches"""
    try:
        # Convert new matches to DataFrame and sort by score
        new_matches_df = pd.DataFrame(matches)
        new_matches_df = new_matches_df.sort_values('Score', ascending=False)
        
        if output_path.exists():
            # Load existing matches
            existing_matches = pd.read_csv(output_path)
            logger.info(f"Loaded {len(existing_matches)} existing matches")
            
            # Combine existing and new matches
            combined_matches = pd.concat([existing_matches, new_matches_df], ignore_index=True)
            
            # Remove any duplicates based on Source-Target pairs
            combined_matches = combined_matches.drop_duplicates(
                subset=['Source', 'Target'], 
                keep='first'  # Keep existing matches in case of duplicates
            )
            
            # Sort all matches by score
            combined_matches = combined_matches.sort_values('Score', ascending=False)
            
            # Save combined matches
            combined_matches.to_csv(output_path, index=False)
            logger.info(f"Added {len(new_matches_df)} new matches to existing {len(existing_matches)} matches")
            logger.info(f"Total matches after deduplication: {len(combined_matches)}")
        else:
            # If no existing file, create new one
            new_matches_df.to_csv(output_path, index=False)
            logger.info(f"Created new matches file with {len(new_matches_df)} matches")
        
    except Exception as e:
        logger.error(f"Error saving matches: {e}")
        raise

def main():
    # Setup
    logger = setup_logging()
    matcher = EnhancedMatcher()
    output_path = Path('data/processed/consolidated_matches.csv')
    
    try:
        # Load merged dataset
        logger.info("Loading merged dataset...")
        df = load_merged_dataset()
        logger.info(f"Loaded {len(df)} records")
        
        # Load existing matches if any
        existing_pairs = set()
        if output_path.exists():
            existing_matches = pd.read_csv(output_path)
            existing_pairs = {
                tuple(sorted([row['Source'], row['Target']]))
                for _, row in existing_matches.iterrows()
            }
            logger.info(f"Loaded {len(existing_pairs)} existing matches")
        
        # Generate new matches
        logger.info("Generating potential matches...")
        matches = generate_potential_matches(df, existing_pairs, matcher, logger)
        
        # Save results
        save_matches(matches, output_path, logger)
        
    except Exception as e:
        logger.error(f"Error in match generation pipeline: {e}")
        raise

if __name__ == "__main__":
    main() 
