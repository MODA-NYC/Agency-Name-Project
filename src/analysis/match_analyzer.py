import pandas as pd
import logging

def analyze_matches():
    """Analyze the matches in consolidated_matches.csv"""
    matches_df = pd.read_csv('data/processed/consolidated_matches.csv')
    
    # Analyze score distribution
    score_stats = matches_df['Score'].describe()
    print("\nScore Distribution:")
    print(score_stats)
    
    # Analyze match types
    match_types = matches_df['Label'].value_counts()
    print("\nMatch Types:")
    print(match_types)
    
    # Look for potential issues
    print("\nPotential Issues:")
    # Check for matches with same source
    same_source = matches_df[matches_df['Source'] == matches_df['Target']]
    if not same_source.empty:
        print(f"Found {len(same_source)} matches with identical source/target")
        
    # Check for missing IDs
    missing_ids = matches_df[matches_df['SourceID'].isna() | matches_df['TargetID'].isna()]
    if not missing_ids.empty:
        print(f"Found {len(missing_ids)} matches with missing IDs")

if __name__ == "__main__":
    analyze_matches() 