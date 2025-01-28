import pandas as pd

# Read the deduplicated dataset
df = pd.read_csv('data/processed/final_deduplicated_dataset.csv')

# Define name-related columns (excluding Name and NameAlphabetized which will be first)
name_columns = [
    'Name - NYC.gov Agency List',
    'Name - NYC.gov Mayor\'s Office',
    'Name - NYC Open Data Portal',
    'Name - ODA',
    'Name - CPO',
    'Name - WeGov',
    'Name - Greenbook',
    'Name - Checkbook',
    'Name - HOO',
    'Name - Ops',
    'NameWithAcronym',
    'NameAlphabetizedWithAcronym',
    'Agency Name',
    'AgencyNameEnriched',
    'LegalName',
    'AlternateNames'
]

# Get all other columns (excluding Name, NameAlphabetized, and name-related columns)
other_columns = [col for col in df.columns if col not in ['Name', 'NameAlphabetized'] + name_columns]

# Create new column order
new_column_order = ['Name', 'NameAlphabetized'] + name_columns + other_columns

# Reorder columns
df = df[new_column_order]

# Save back to the original file
df.to_csv('data/processed/final_deduplicated_dataset.csv', index=False)

print("Columns have been reordered in final_deduplicated_dataset.csv")
print("\nFirst few columns in new order:")
print(", ".join(new_column_order[:10])) 