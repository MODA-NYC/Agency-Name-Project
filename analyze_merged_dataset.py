import pandas as pd
import os

file_path = os.path.join('data', 'intermediate', 'merged_dataset.csv')
df = pd.read_csv(file_path)

print(f"Total number of records: {len(df)}")
print(f"Number of non-null values in 'Name - NYC.gov Redesign': {df['Name - NYC.gov Redesign'].notna().sum()}")
print(f"Number of non-null values in 'ops_Agency Name': {df['ops_Agency Name'].notna().sum()}")
print(f"Number of null values in 'NameNormalized': {df['NameNormalized'].isna().sum()}")

# Investigate extra 'ops_Agency Name' entries
print("\nInvestigating 'ops_Agency Name' entries:")
print(df['ops_Agency Name'].value_counts().head())

# Identify records with null 'NameNormalized' values
print("\nRecords with null 'NameNormalized' values:")
null_name_normalized = df[df['NameNormalized'].isna()]
print(null_name_normalized[['Name', 'Name - NYC.gov Redesign', 'ops_Agency Name']])

# Check for duplicate entries in 'NameNormalized'
print("\nDuplicate entries in 'NameNormalized':")
duplicates = df[df.duplicated(subset=['NameNormalized'], keep=False)]
print(duplicates[['NameNormalized', 'Name', 'Name - NYC.gov Redesign', 'ops_Agency Name']])

# Check for any remaining duplicates in 'Name'
print("\nChecking for any remaining duplicates in 'Name':")
agency_duplicates = df[df.duplicated(subset=['Name'], keep=False)]
print(agency_duplicates[['Name', 'Name - NYC.gov Redesign', 'ops_Agency Name']])

# Print unique values in 'Name' column
print("\nUnique values in 'Name' column:")
print(df['Name'].nunique())
print(df['Name'].value_counts())
