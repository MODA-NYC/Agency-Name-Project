import pandas as pd
import re
import logging

logger = logging.getLogger(__name__)

def is_library_name(name):
    """
    Check if the given name refers to a library by looking for the keyword "library".
    """
    if pd.isna(name):
        return False
    return 'library' in name.lower()

def normalize_library_name(name):
    """
    Normalize a library name by converting to lower case and removing common extra phrases.
    Specifically handles the three main NYC library systems and their variations.
    """
    if pd.isna(name):
        return ""
    
    name = name.lower().strip()
    
    # Handle specific NYC library systems first
    if 'brooklyn' in name and 'library' in name:
        return 'brooklyn public library'
    if 'new york public' in name or 'nypl' in name:
        return 'new york public library'
    if any(x in name for x in ['queens borough public', 'queens public', 'queens library']):
        return 'queens public library'
    
    # Remove common extra phrases
    phrases_to_remove = [
        r'\bboard of trustees\b',
        r'\bpublic\b',
        r'\bborough\b',
        r'\bsystem\b',
        r'\bbranch\b',
        r'\bdivision\b',
        r'\bof the\b',
        r'\(.*?\)',  # Remove anything in parentheses
    ]
    
    for phrase in phrases_to_remove:
        name = re.sub(phrase, '', name, flags=re.IGNORECASE)
    
    # Remove punctuation and extra whitespace
    name = re.sub(r'[^\w\s]', ' ', name)
    name = " ".join(name.split())
    
    return name

def consolidate_library_matches(matches_df):
    """
    Consolidate library name variants in the consolidated matches DataFrame.
    """
    df = matches_df.copy()
    
    # Identify rows where both Source and Target are library names
    library_mask = df['Source'].apply(is_library_name) & df['Target'].apply(is_library_name)
    logger.info(f"Found {library_mask.sum()} library match pairs in consolidated_matches.csv")
    
    # Track specific library systems
    brooklyn_count = 0
    nypl_count = 0
    queens_count = 0
    other_count = 0
    
    # Process each row in these library match pairs
    for idx, row in df[library_mask].iterrows():
        source_norm = normalize_library_name(row['Source'])
        target_norm = normalize_library_name(row['Target'])
        
        if source_norm == target_norm and source_norm != "":
            # Mark the pair as a confirmed match
            df.at[idx, 'Label'] = 'Match'
            
            # Ensure consistent IDs
            if pd.isna(row['SourceID']) and pd.notna(row['TargetID']):
                df.at[idx, 'SourceID'] = row['TargetID']
            elif pd.isna(row['TargetID']) and pd.notna(row['SourceID']):
                df.at[idx, 'TargetID'] = row['SourceID']
            elif pd.notna(row['SourceID']) and pd.notna(row['TargetID']) and row['SourceID'] != row['TargetID']:
                df.at[idx, 'TargetID'] = row['SourceID']
            
            # Track which library system was matched
            if 'brooklyn' in source_norm:
                brooklyn_count += 1
            elif 'new york public' in source_norm:
                nypl_count += 1
            elif 'queens' in source_norm:
                queens_count += 1
            else:
                other_count += 1
            
            logger.info(f"Consolidated library match: '{row['Source']}' -> '{row['Target']}' (normalized: {source_norm})")
    
    # Log summary statistics
    logger.info(f"Library consolidation summary:")
    logger.info(f"- Brooklyn Public Library matches: {brooklyn_count}")
    logger.info(f"- New York Public Library matches: {nypl_count}")
    logger.info(f"- Queens Public Library matches: {queens_count}")
    logger.info(f"- Other library matches: {other_count}")
    
    return df

def update_consolidated_matches(file_path):
    """
    Load the consolidated_matches.csv file from the given file path, apply library consolidation,
    and save the updated DataFrame back to the same file.
    """
    try:
        df = pd.read_csv(file_path)
        logger.info(f"Loaded {len(df)} rows from {file_path}")
    except Exception as e:
        logger.error(f"Error loading {file_path}: {e}")
        return
    
    updated_df = consolidate_library_matches(df)
    updated_df.to_csv(file_path, index=False)
    logger.info(f"Updated consolidated matches saved to {file_path}")

if __name__ == "__main__":
    # Example usage when running this module directly:
    import sys
    if len(sys.argv) < 2:
        print("Usage: python library_consolidation.py path/to/consolidated_matches.csv")
    else:
        update_consolidated_matches(sys.argv[1]) 