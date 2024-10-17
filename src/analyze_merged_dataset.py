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

# Load the ops_data.csv and nyc_gov_hoo.csv datasets
ops_data_path = os.path.join(base_path, 'data', 'raw', 'ops_data.csv')
nyc_gov_hoo_path = os.path.join(base_path, 'data', 'raw', 'nyc_gov_hoo.csv')

ops_data_df = pd.read_csv(ops_data_path)
nyc_gov_hoo_df = pd.read_csv(nyc_gov_hoo_path)

# Standardize the agency names for comparison
def standardize_name(name):
    if pd.isna(name):
        return name
    return ' '.join(name.strip().lower().split())

df['Name - Ops'] = df['Name - Ops'].apply(standardize_name)
ops_data_df['Agency Name'] = ops_data_df['Agency Name'].apply(standardize_name)

df['Name - NYC.gov Redesign'] = df['Name - NYC.gov Redesign'].apply(standardize_name)
nyc_gov_hoo_df['Agency Name'] = nyc_gov_hoo_df['Agency Name'].apply(standardize_name)

# Find mismatches between merged_dataset.csv and ops_data.csv
ops_mismatches = df[~df['Name - Ops'].isin(ops_data_df['Agency Name']) & df['Name - Ops'].notna()]
print(f"\nRecords in merged_dataset.csv with 'Name - Ops' not matching 'Agency Name' in ops_data.csv: {len(ops_mismatches)}")
if not ops_mismatches.empty:
    print(ops_mismatches[['Name', 'Name - Ops']].head())

# Find mismatches between ops_data.csv and merged_dataset.csv
ops_data_mismatches = ops_data_df[~ops_data_df['Agency Name'].isin(df['Name - Ops']) & ops_data_df['Agency Name'].notna()]
print(f"\nRecords in ops_data.csv with 'Agency Name' not matching 'Name - Ops' in merged_dataset.csv: {len(ops_data_mismatches)}")
if not ops_data_mismatches.empty:
    print(ops_data_mismatches[['Agency Name']].head())

# Find mismatches between merged_dataset.csv and nyc_gov_hoo.csv
nyc_gov_mismatches = df[~df['Name - NYC.gov Redesign'].isin(nyc_gov_hoo_df['Agency Name']) & df['Name - NYC.gov Redesign'].notna()]
print(f"\nRecords in merged_dataset.csv with 'Name - NYC.gov Redesign' not matching 'Agency Name' in nyc_gov_hoo.csv: {len(nyc_gov_mismatches)}")
if not nyc_gov_mismatches.empty:
    print(nyc_gov_mismatches[['Name', 'Name - NYC.gov Redesign']])

# Find mismatches between nyc_gov_hoo.csv and merged_dataset.csv
nyc_gov_hoo_mismatches = nyc_gov_hoo_df[~nyc_gov_hoo_df['Agency Name'].isin(df['Name - NYC.gov Redesign']) & nyc_gov_hoo_df['Agency Name'].notna()]
print(f"\nRecords in nyc_gov_hoo.csv with 'Agency Name' not matching 'Name - NYC.gov Redesign' in merged_dataset.csv: {len(nyc_gov_hoo_mismatches)}")
if not nyc_gov_hoo_mismatches.empty:
    print(nyc_gov_hoo_mismatches[['Agency Name']].head())
