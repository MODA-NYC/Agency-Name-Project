import pandas as pd

# Read the datasets
final_df = pd.read_csv('data/processed/final_deduplicated_dataset.csv')
try:
    phase2_df = pd.read_csv('data/raw/phase2_manual_adjustments.csv', encoding='utf-8')
except UnicodeDecodeError:
    phase2_df = pd.read_csv('data/raw/phase2_manual_adjustments.csv', encoding='latin1')

# Get unique names from each dataset
final_names = set(final_df['Name'].str.lower().str.strip())
phase2_names = set(phase2_df['Name'].str.lower().str.strip())

# Calculate matches and differences
matches = final_names.intersection(phase2_names)
only_in_final = final_names - phase2_names
only_in_phase2 = phase2_names - final_names

print(f'\nComparison Results:')
print(f'Records in final deduplicated dataset: {len(final_names)}')
print(f'Records in phase two adjustments: {len(phase2_names)}')
print(f'Exact matches: {len(matches)}')
print(f'\nDelta Analysis:')
print(f'Records only in final dataset: {len(only_in_final)}')
print(f'Records only in phase two: {len(only_in_phase2)}')

print('\nSample of records only in final dataset (first 5):')
for name in sorted(list(only_in_final))[:5]:
    print(f'- {name}')

print('\nSample of records only in phase two (first 5):')
for name in sorted(list(only_in_phase2))[:5]:
    print(f'- {name}') 