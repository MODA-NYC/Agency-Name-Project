import pandas as pd
from typing import List, Dict, Optional, Set, Tuple
import logging
from pathlib import Path
from .matching_algorithm import AgencyMatcher
from .data_preparation import AgencyNamePreprocessor
from .string_matching import get_composite_score  # Add this import

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
        self.existing_matches = self._load_existing_matches()  # Initialize existing_matches
        
        # Set up logging
        logging.basicConfig(level=logging.INFO)
        self.logger = logging.getLogger(__name__)

    def _load_existing_matches(self) -> Set[Tuple[str, str]]:
        """Load existing matches to avoid duplicates"""
        try:
            df = pd.read_csv(self.matches_file)
            # Keep only required columns if they exist
            required_cols = ['Source', 'Target', 'Score', 'Label']
            existing_cols = [col for col in required_cols if col in df.columns]
            df = df[existing_cols]
            return {(row['Source'], row['Target']) for _, row in df.iterrows()}
        except FileNotFoundError:
            return set()
            
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
        
        # Filter out existing matches and format with RecordIDs
        new_matches = []
        for match in all_matches:
            source = match['source_name'].lower()
            target = match['target_name'].lower()
            score = round(match['similarity_score'], 1)
            
            if (source, target) not in existing_pairs and \
               (target, source) not in existing_pairs:
                # Get RecordIDs from prepared dataset
                source_record = prepared_df.loc[match['source_id']]
                target_record = prepared_df.loc[match['target_id']]
                
                new_matches.append({
                    'Source': source,
                    'Target': target,
                    'Score': score,
                    'Label': 'Match' if score == 100 else '',  # Auto-label 100% matches
                    'SourceID': source_record['RecordID'],
                    'TargetID': target_record['RecordID']
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
            if not new_matches:
                self.logger.info("No new matches to append")
                return
                
            # Convert to DataFrame
            matches_df = pd.DataFrame(new_matches)
            
            # Sort by score descending
            matches_df = matches_df.sort_values('Score', ascending=False)
            
            # Validate RecordIDs exist
            if not all(matches_df['SourceID'].notna()) or not all(matches_df['TargetID'].notna()):
                raise ValueError("Missing RecordIDs in matches")
            
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
            
            # Log summary
            auto_labeled = len(matches_df[matches_df['Label'] == 'Match'])
            pending_review = len(matches_df[matches_df['Label'] == ''])
            self.logger.info(f"Successfully added {len(matches_df)} new potential matches:")
            self.logger.info(f"- Auto-labeled matches (100% score): {auto_labeled}")
            self.logger.info(f"- Pending manual review: {pending_review}")
            
        except Exception as e:
            self.logger.error(f"Error appending matches: {e}")
            raise

    def process_new_matches(
        self,
        df: pd.DataFrame,
        name_column: str,
        min_score: float = 82.0,
        batch_size: int = 1000
    ):
        """
        Generate and save potential matches from the dataset.
        """
        self.logger.info(f"Starting match generation with {len(df)} records")
        names = df[name_column].unique()
        self.logger.info(f"Found {len(names)} unique names to compare")
        
        new_matches = []
        comparison_count = 0
        match_count = 0
        
        for i, name1 in enumerate(names):
            for name2 in names[i+1:]:
                comparison_count += 1
                if comparison_count % 1000 == 0:
                    self.logger.info(f"Processed {comparison_count} comparisons, found {match_count} matches")
                
                # Skip if this pair is already in existing matches
                if (name1, name2) in self.existing_matches or \
                   (name2, name1) in self.existing_matches:
                    continue
                    
                score = get_composite_score(name1, name2)
                
                # Debug log for scores near threshold
                if score > 70:  # Log scores that are close to matching
                    self.logger.debug(f"Near match: '{name1}' - '{name2}' = {score}")
                
                if score >= min_score:
                    match_count += 1
                    self.logger.info(f"Found match: '{name1}' - '{name2}' = {score}")
                    
                    # Find the RecordIDs from merged_dataset for this pair
                    try:
                        record1 = df[df[name_column] == name1].iloc[0]
                        record2 = df[df[name_column] == name2].iloc[0]
                        
                        new_matches.append({
                            'Source': name1,
                            'Target': name2,
                            'Score': round(float(score), 1),
                            'Label': 'Match' if score >= 95 else '',  # Only auto-label very high confidence matches
                            'SourceID': record1['RecordID'],
                            'TargetID': record2['RecordID']
                        })
                        
                        if len(new_matches) >= batch_size:
                            self.logger.info(f"Saving batch of {len(new_matches)} matches")
                            self._save_matches(new_matches)
                            new_matches = []
                    except Exception as e:
                        self.logger.error(f"Error processing match {name1} - {name2}: {e}")
        
        # Save any remaining matches
        if new_matches:
            self.logger.info(f"Saving final batch of {len(new_matches)} matches")
            self._save_matches(new_matches)
            
        self.logger.info(f"Completed match generation. Processed {comparison_count} comparisons, found {match_count} matches")
    
    def _save_matches(self, new_matches: List[Dict]):
        """Save new matches to the CSV file"""
        if not new_matches:
            return
            
        new_df = pd.DataFrame(new_matches)
        
        try:
            if self.matches_file.exists():
                existing_df = pd.read_csv(self.matches_file)
                # Keep only the columns we want
                cols = ['Source', 'Target', 'Score', 'Label', 'SourceID', 'TargetID']
                existing_df = existing_df.reindex(columns=cols)
                
                # Ensure Score column is numeric
                existing_df['Score'] = pd.to_numeric(existing_df['Score'], errors='coerce')
                
                # Combine with new matches
                combined_df = pd.concat([existing_df, new_df], ignore_index=True)
            else:
                combined_df = new_df
            
            # Ensure proper formatting of Score column
            combined_df['Score'] = combined_df['Score'].round(1)
            
            # Remove duplicates
            combined_df = combined_df.drop_duplicates(subset=['Source', 'Target'])
            
            # Save with proper quoting to avoid the single quote issue
            combined_df.to_csv(self.matches_file, index=False, float_format='%.1f')
            self.logger.info(f"Saved {len(new_df)} new matches to {self.matches_file}")
            
        except Exception as e:
            self.logger.error(f"Error saving matches: {e}")
            raise

    def validate_record_ids(self, merged_df: pd.DataFrame) -> None:
        """
        Validate all RecordIDs in matches exist in merged dataset.
        
        Args:
            merged_df: The merged dataset containing valid RecordIDs
        """
        matches_df = self._load_existing_matches()
        valid_ids = set(merged_df['RecordID'])
        
        source_ids = set(matches_df['SourceID'])
        target_ids = set(matches_df['TargetID'])
        
        invalid_ids = (source_ids | target_ids) - valid_ids
        if invalid_ids:
            raise ValueError(f"Invalid RecordIDs found: {invalid_ids}")

    def generate_potential_matches(merged_df):
        """Generate potential matches and append to consolidated_matches.csv"""
        # Get existing matches to avoid duplicates
        existing_matches = pd.read_csv('data/processed/consolidated_matches.csv')
        existing_pairs = set(zip(existing_matches['Source'], existing_matches['Target']))
        
        # Generate new potential matches
        new_matches = []
        for idx1, row1 in merged_df.iterrows():
            block = merged_df[merged_df['NameNormalized'].str[0] == row1['NameNormalized'][0]]
            for idx2, row2 in block.iterrows():
                if idx1 < idx2:  # Avoid self-matches and duplicates
                    score = get_composite_score(row1['NameNormalized'], row2['NameNormalized'])
                    if score >= 82:  # Using threshold from section 1.2
                        pair = (row1['NameNormalized'], row2['NameNormalized'])
                        if pair not in existing_pairs:
                            new_matches.append({
                                'Source': pair[0],
                                'Target': pair[1],
                                'Score': score,
                                'Label': 'Match' if score == 100 else ''  # Auto-label 100% matches
                            })
        
        if new_matches:
            # Convert to DataFrame and sort by score descending
            new_matches_df = pd.DataFrame(new_matches)
            new_matches_df = new_matches_df.sort_values('Score', ascending=False)
            
            # Append new matches to existing file
            new_matches_df.to_csv('data/processed/consolidated_matches.csv', 
                                mode='a', header=False, index=False)
