import pandas as pd

print('\nChecking dedup dataset...')
dedup_df = pd.read_csv('../data/intermediate/dedup_merged_dataset.csv')
print(f'Dedup dataset shape: {dedup_df.shape}')
print('\nSample RecordIDs:')
print(dedup_df['RecordID'].head())

print('\nChecking matches...')
matches_df = pd.read_csv('../data/processed/consolidated_matches.csv')
print(f'Matches shape: {matches_df.shape}')
print('\nSample SourceIDs and TargetIDs:')
print(matches_df[['SourceID', 'TargetID']].head())

print('\nChecking for missing IDs...')
all_source_ids = set(matches_df['SourceID'].unique())
all_target_ids = set(matches_df['TargetID'].unique())
all_record_ids = set(dedup_df['RecordID'].unique())

missing_source = [id for id in all_source_ids if id not in all_record_ids]
missing_target = [id for id in all_target_ids if id not in all_record_ids]

print(f'\nMissing source IDs: {len(missing_source)}')
if missing_source:
    print('First few missing source IDs:', missing_source[:5])

print(f'\nMissing target IDs: {len(missing_target)}')
if missing_target:
    print('First few missing target IDs:', missing_target[:5]) 