import pandas as pd
import os

# Use an absolute path to ensure the file is found
base_path = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
file_path = os.path.join(base_path, 'data', 'intermediate', 'merged_dataset.csv')

# Check if the file exists before attempting to read
if not os.path.exists(file_path):
    raise FileNotFoundError(f"The file '{file_path}' does not exist.")

df = pd.read_csv(file_path)

print(f"Total number of records: {len(df)}")
print(f"Columns in the dataset: {df.columns.tolist()}")

# Function to safely print non-null counts
def print_non_null_count(column_name):
    if column_name in df.columns:
        non_null_count = df[column_name].notna().sum()
        print(f"Number of non-null values in '{column_name}': {non_null_count}")
    else:
        print(f"Column '{column_name}' not found in the dataset.")

# List of key columns to analyze
key_columns = ['Name', 'NameNormalized', 'Name - NYC.gov Redesign', 'Name - Ops']

# Print non-null counts for key columns
print("\nNon-null counts for key columns:")
for column in key_columns:
    print_non_null_count(column)

# Investigate entries from different sources
print("\nValue counts for key columns:")
for column in key_columns:
    if column in df.columns:
        print(f"\nTop entries in '{column}':")
        print(df[column].value_counts().head())
    else:
        print(f"\nColumn '{column}' not found in the dataset.")

# Identify records with null values in 'Name' or 'NameNormalized'
for column in ['Name', 'NameNormalized']:
    if column in df.columns:
        null_records = df[df[column].isna()]
        print(f"\nRecords with null '{column}' values: {len(null_records)}")
        if not null_records.empty:
            print(null_records[key_columns + ['OperationalStatus']].head())
    else:
        print(f"\nColumn '{column}' not found in the dataset.")

# Check for non-null duplicate entries in 'NameNormalized' and 'Name'
for column in ['Name', 'NameNormalized']:
    if column in df.columns:
        # Exclude null values
        non_null_df = df[df[column].notna()]
        duplicates = non_null_df[non_null_df.duplicated(subset=column, keep=False)]
        print(f"\nNon-null duplicate entries in '{column}': {duplicates.shape[0]}")
        if not duplicates.empty:
            # Group duplicates for better visualization
            grouped_duplicates = duplicates.groupby(column)
            print(f"Sample duplicate groups in '{column}':")
            for name_value, group in list(grouped_duplicates)[0:5]:  # Show first 5 groups
                print(f"\nValue: {name_value}")
                print(group[key_columns + ['OperationalStatus']])
    else:
        print(f"\nColumn '{column}' not found in the dataset.")

# Identify discrepancies between 'Name' and 'NameNormalized'
if 'Name' in df.columns and 'NameNormalized' in df.columns:
    discrepancies = df[df['Name'].str.lower().str.strip() != df['NameNormalized'].str.lower().str.strip()]
    discrepancies = discrepancies.dropna(subset=['Name', 'NameNormalized'])
    print(f"\nDiscrepancies between 'Name' and 'NameNormalized': {len(discrepancies)}")
    if not discrepancies.empty:
        print(discrepancies[['Name', 'NameNormalized']].head())
else:
    print("\nColumns 'Name' and/or 'NameNormalized' not found in the dataset.")

# Additional checks for missing data in other important columns
important_columns = ['OperationalStatus', 'Description', 'URL']
print("\nNon-null counts for important columns:")
for column in important_columns:
    print_non_null_count(column)

# Check for inconsistent data types
print("\nData types of key columns:")
print(df[key_columns].dtypes)

# Identify unexpected values in 'OperationalStatus'
if 'OperationalStatus' in df.columns:
    expected_statuses = ['Active', 'Reorganized', 'Merged', 'Inactive']
    unexpected_statuses = df[~df['OperationalStatus'].isin(expected_statuses) & df['OperationalStatus'].notna()]
    print(f"\nRecords with unexpected 'OperationalStatus' values: {len(unexpected_statuses)}")
    if not unexpected_statuses.empty:
        print(unexpected_statuses[['Name', 'OperationalStatus']].head())
else:
    print("\nColumn 'OperationalStatus' not found in the dataset.")
