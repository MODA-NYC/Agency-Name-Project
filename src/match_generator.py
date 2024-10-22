import pandas as pd
from typing import List, Dict, Optional
import logging
from pathlib import Path
from .matching_algorithm import AgencyMatcher
from .data_preparation import AgencyNamePreprocessor

class PotentialMatchGenerator:
    """
    Generates and manages potential matches for agency names,
    handling the interaction with consolidated_matches.csv
    """
    
    def __init__(
        self,
        matcher: Optional[AgencyMatcher] = None,
        preprocessor: Optional[AgencyNamePreprocessor] = None,
        matches_file: str = 'data/processed/consolidated_matches.csv'
    ):
        """
        Initialize the match generator.
        
        Args:
            matcher: AgencyMatcher instance (creates new one if None)
            preprocessor: AgencyNamePreprocessor instance (creates new if None)
            matches_file: Path to consolidated_matches.csv
        """
        self.matcher = matcher or AgencyMatcher()
        self.preprocessor = preprocessor or AgencyNamePreprocessor()
        self.matches_file = Path(matches_file)
        
        # Set up logging
        logging.basicConfig(level=logging.INFO)
        self.logger = logging.getLogger(__name__)

    def _load_existing_matches(self) -> pd.DataFrame:
        """
        Load existing matches from consolidated_matches.csv.
        Creates file with headers if it doesn't exist.
        
        Returns:
            DataFrame containing existing matches
        """
        try:
            if not self.matches_file.exists():
                # Create new file with headers
                df = pd.DataFrame(columns=['Source', 'Target', 'Score', 'Label'])
                df.to_csv(self.matches_file, index=False)
                return df
            return pd.read_csv(self.matches_file)
        except Exception as e:
            self.logger.error(f"Error loading matches file: {e}")
            raise

    def _get_existing_pairs(self) -> set:
        """
        Get set of existing source-target pairs to avoid duplicates.
        
        Returns:
            Set of (source, target) tuples
        """
        existing_matches = self._load_existing_matches()
        # Create pairs in both directions to ensure no duplicates
        pairs = set(zip(existing_matches['Source'], existing_matches['Target']))
        reverse_pairs = set(zip(existing_matches['Target'], existing_matches['Source']))
        return pairs.union(reverse_pairs)

    def generate_new_matches(
        self,
        df: pd.DataFrame,
        name_column: str,
        min_score: float = 82.0
    ) -> List[Dict[str, any]]:
        """
        Generate new potential matches not already in consolidated_matches.csv.
        
        Args:
            df: DataFrame containing agency names
            name_column: Name of column containing agency names
            min_score: Minimum similarity score to consider
            
        Returns:
            List of new potential matches
        """
        # Prepare dataset
        prepared_df = self.preprocessor.prepare_dataset_for_matching(df, name_column)
        
        # Get existing pairs
        existing_pairs = self._get_existing_pairs()
        
        # Find potential matches
        all_matches = self.matcher.find_potential_matches(
            prepared_df,
            'NameStandardized',
            min_score=min_score
        )
        
        # Filter out existing matches
        new_matches = []
        for match in all_matches:
            source = match['source_name'].lower()
            target = match['target_name'].lower()
            
            if (source, target) not in existing_pairs and \
               (target, source) not in existing_pairs:
                new_matches.append({
                    'Source': source,
                    'Target': target,
                    'Score': round(match['similarity_score'], 1),
                    'Label': ''
                })
        
        return new_matches

    def append_matches_to_file(
        self,
        new_matches: List[Dict[str, any]],
        batch_size: int = 1000
    ) -> None:
        """
        Append new matches to consolidated_matches.csv in batches.
        
        Args:
            new_matches: List of new matches to append
            batch_size: Number of matches to write at once
        """
        try:
            # Convert to DataFrame
            matches_df = pd.DataFrame(new_matches)
            
            # Sort by score descending
            matches_df = matches_df.sort_values('Score', ascending=False)
            
            # Write in batches
            for i in range(0, len(matches_df), batch_size):
                batch = matches_df[i:i + batch_size]
                batch.to_csv(
                    self.matches_file,
                    mode='a',
                    header=False,
                    index=False
                )
                self.logger.info(f"Wrote batch of {len(batch)} matches")
                
            self.logger.info(f"Successfully added {len(matches_df)} new potential matches")
            
        except Exception as e:
            self.logger.error(f"Error appending matches: {e}")
            raise

    def process_new_matches(
        self,
        df: pd.DataFrame,
        name_column: str,
        min_score: float = 82.0,
        batch_size: int = 1000
    ) -> None:
        """
        Generate and append new matches in one operation.
        
        Args:
            df: DataFrame containing agency names
            name_column: Name of column containing agency names
            min_score: Minimum similarity score to consider
            batch_size: Number of matches to write at once
        """
        # Generate new matches
        new_matches = self.generate_new_matches(df, name_column, min_score)
        
        if not new_matches:
            self.logger.info("No new matches found")
            return
            
        # Append to file
        self.append_matches_to_file(new_matches, batch_size)
