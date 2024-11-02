import pandas as pd
import logging
from typing import List, Dict

def setup_logging():
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s'
    )
    return logging.getLogger(__name__)

def find_problematic_matches(matches_df: pd.DataFrame, 
                           ops_data: pd.DataFrame,
                           final_data: pd.DataFrame) -> Dict[str, List]:
    """Find matches that might be causing incorrect combinations."""
    
    # Find all combined records in final dataset
    combined_records = final_data[final_data['Name - Ops'].str.contains(',', na=False)]
    
    problematic_matches = {
        'incorrect_combinations': [],
        'suspicious_matches': [],
        'chain_matches': []  # matches that form chains (A->B->C)
    }
    
    # Check each combined record
    for _, row in combined_records.iterrows():
        combined_names = [name.strip() for name in row['Name - Ops'].split(',')]
        
        # Find matches that led to this combination
        related_matches = matches_df[
            matches_df['Source'].isin(combined_names) | 
            matches_df['Target'].isin(combined_names)
        ]
        
        if not related_matches.empty:
            # Check if these entities should really be combined
            if len(combined_names) > 1:
                problematic_matches['incorrect_combinations'].append({
                    'combined_names': combined_names,
                    'matches_involved': related_matches[['Source', 'Target', 'Score', 'Label']].to_dict('records')
                })
        
        # Look for chain matches (A->B->C becoming A,B,C)
        if len(combined_names) > 2:
            problematic_matches['chain_matches'].append({
                'combined_names': combined_names,
                'matches_involved': related_matches[['Source', 'Target', 'Score', 'Label']].to_dict('records')
            })
    
    return problematic_matches

def analyze_match_issues(data_dir: str = 'data'):
    logger = setup_logging()
    
    # Load datasets
    matches_df = pd.read_csv(f'{data_dir}/processed/consolidated_matches.csv')
    ops_data = pd.read_csv(f'{data_dir}/raw/ops_data.csv')
    final_data = pd.read_csv(f'{data_dir}/processed/final_deduplicated_dataset.csv')
    
    # Find problematic matches
    issues = find_problematic_matches(matches_df, ops_data, final_data)
    
    # Log findings
    logger.info("\n=== Match Issues Analysis ===")
    
    if issues['incorrect_combinations']:
        logger.info("\nPotentially Incorrect Combinations:")
        for combo in issues['incorrect_combinations']:
            logger.info(f"\nCombined Names: {', '.join(combo['combined_names'])}")
            logger.info("Matches that led to this combination:")
            for match in combo['matches_involved']:
                logger.info(f"- {match['Source']} -> {match['Target']} (Score: {match.get('Score', 'N/A')}, Label: {match.get('Label', 'N/A')})")
    
    if issues['chain_matches']:
        logger.info("\nChain Matches (potential transitive combinations):")
        for chain in issues['chain_matches']:
            logger.info(f"\nCombined Chain: {', '.join(chain['combined_names'])}")
            logger.info("Matches in chain:")
            for match in chain['matches_involved']:
                logger.info(f"- {match['Source']} -> {match['Target']}")
    
    # Save detailed analysis
    analysis_path = f'{data_dir}/analysis/match_issues_analysis.csv'
    
    # Convert issues to a flat DataFrame for CSV
    rows = []
    for issue_type, items in issues.items():
        for item in items:
            for match in item['matches_involved']:
                rows.append({
                    'issue_type': issue_type,
                    'combined_names': ', '.join(item['combined_names']),
                    'source': match['Source'],
                    'target': match['Target'],
                    'score': match.get('Score', 'N/A'),
                    'label': match.get('Label', 'N/A')
                })
    
    if rows:
        pd.DataFrame(rows).to_csv(analysis_path, index=False)
        logger.info(f"\nDetailed analysis saved to: {analysis_path}")

if __name__ == "__main__":
    analyze_match_issues() 