import pandas as pd
import numpy as np

print('\nChecking dedup dataset...')
dedup_df = pd.read_csv('data/intermediate/dedup_merged_dataset.csv')
print(f'Dedup dataset shape: {dedup_df.shape}')
print('\nSample RecordIDs:')
print(dedup_df['RecordID'].head())

print('\nChecking matches...')
matches_df = pd.read_csv('data/processed/consolidated_matches.csv')
print(f'Matches shape: {matches_df.shape}')

print('\nLabel distribution:')
print(matches_df['Label'].value_counts(dropna=False))

# Only look at matches labeled as "Match"
verified_matches = matches_df[matches_df['Label'] == 'Match']
print(f'\nVerified matches: {len(verified_matches)}')

print('\nID Status in verified matches:')
source_null = verified_matches['SourceID'].isna().sum()
target_null = verified_matches['TargetID'].isna().sum()
print(f'Null SourceIDs: {source_null}')
print(f'Null TargetIDs: {target_null}')

# Check non-null IDs against dedup dataset
all_record_ids = set(dedup_df['RecordID'].unique())
valid_source = verified_matches[~verified_matches['SourceID'].isna()]['SourceID'].isin(all_record_ids).sum()
valid_target = verified_matches[~verified_matches['TargetID'].isna()]['TargetID'].isin(all_record_ids).sum()

print(f'\nValid SourceIDs: {valid_source}')
print(f'Valid TargetIDs: {valid_target}')

# Sample of matches with valid IDs
valid_matches = verified_matches[
    verified_matches['SourceID'].isin(all_record_ids) & 
    verified_matches['TargetID'].isin(all_record_ids)
]
print('\nSample of verified matches with valid IDs:')
print(valid_matches[['Source', 'Target', 'SourceID', 'TargetID']].head()) 