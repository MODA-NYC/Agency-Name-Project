#!/usr/bin/env python
# coding: utf-8

import pandas as pd
import os

print("Starting schema.py")

# Update BASE_PATH to point to the project root directory
BASE_PATH = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
print(f"Project root directory (BASE_PATH): {BASE_PATH}")

def get_file_path(*paths):
    full_path = os.path.join(BASE_PATH, *paths)
    print(f"Attempting to access: {full_path}")
    return full_path

print(f"Contents of project root: {os.listdir(BASE_PATH)}")

# Load the NYC Agencies Export Data
df_path = get_file_path('data', 'processed', 'nyc_agencies_export.csv')
try:
    df = pd.read_csv(df_path)
    print("Existing fields:", df.columns.tolist())
    print("Data types:\n", df.dtypes)
    print("Sample values:\n", df.head())
except FileNotFoundError:
    print(f"Error: The file '{df_path}' does not exist.")
    exit(1)

# 1. Update Field Names (Terminology Transition)
agency_columns = [col for col in df.columns if 'Agency' in col]
rename_mapping = {col: col.replace('Agency', 'Organization') for col in agency_columns}
df.rename(columns=rename_mapping, inplace=True)
print("Updated column names:", df.columns.tolist())

# 2. Adjust Data Types
date_fields = ['DateCreated', 'DateModified', 'FoundingYear', 'SunsetYear']
for field in date_fields:
    if field in df.columns:
        df[field] = pd.to_datetime(df[field], errors='coerce')

# Format 'FoundingYear' as a four-digit year
if 'FoundingYear' in df.columns:
    df['FoundingYear'] = df['FoundingYear'].dt.year
    print("'FoundingYear' formatted as four-digit year.")

# Format 'SunsetYear' as a four-digit year
if 'SunsetYear' in df.columns:
    df['SunsetYear'] = df['SunsetYear'].dt.year
    print("'SunsetYear' formatted as four-digit year.")

# Ensure 'Budget Code' preserves leading zeros
if 'Budget Code' in df.columns:
    df['Budget Code'] = df['Budget Code'].astype(str).str.zfill(df['Budget Code'].str.len().max())
    print("'Budget Code' formatted to preserve leading zeros.")

print("Data types after conversion:\n", df.dtypes)

# 3. Remove Obsolete or Redundant Fields
obsolete_fields = ['Description_y', 'Contact Name_y']  # Replace with actual field names as needed
existing_obsolete_fields = [field for field in obsolete_fields if field in df.columns]
if existing_obsolete_fields:
    df.drop(columns=existing_obsolete_fields, inplace=True)
    print("Remaining columns after removal:", df.columns.tolist())
else:
    print("No obsolete fields found to remove.")

# 4. Ensure Consistency in Key Fields
if 'Name' in df.columns:
    df['Name'] = df['Name'].astype(str).str.strip()
    # Optionally, apply additional normalization functions here
    print("Standardized 'Name' field.")

# Remove 'PrincipalOfficerGivenName' and 'PrincipalOfficerFamilyName' from DataFrame
fields_to_remove = ['PrincipalOfficerGivenName', 'PrincipalOfficerFamilyName']
existing_fields_to_remove = [field for field in fields_to_remove if field in df.columns]
if existing_fields_to_remove:
    df.drop(columns=existing_fields_to_remove, inplace=True)
    print(f"Removed fields from DataFrame: {', '.join(existing_fields_to_remove)}")
else:
    print("No fields to remove from DataFrame.")

# 5. Add New Fields to the Schema
# Add new fields if they don't exist
new_fields = [
    'PrincipalOfficerContactURL',
    'HeadOfOrganizationTitle',
    'HeadOfOrganizationURL',
    'MMRChapter',
    'RulemakingEntity'
]
for field in new_fields:
    if field not in df.columns:
        df[field] = ''
print(f"New fields added: {', '.join(new_fields)}")

# 6. Update the Data Dictionary
# Load old data dictionary
old_data_dict_path = get_file_path('docs', 'data_dictionary_old.csv')
try:
    old_data_dict = pd.read_csv(old_data_dict_path)
    print(f"Loaded old data dictionary from '{old_data_dict_path}'.")
except FileNotFoundError:
    print(f"Old data dictionary not found at '{old_data_dict_path}'. Creating a new one.")
    # Initialize an empty DataFrame with expected columns if old data dictionary doesn't exist
    old_data_dict = pd.DataFrame(columns=['FieldName', 'Definition'])

# Remove 'PrincipalOfficerGivenName' and 'PrincipalOfficerFamilyName' from data dictionary
data_dict = old_data_dict.copy()
data_dict = data_dict[~data_dict['FieldName'].isin(['PrincipalOfficerGivenName', 'PrincipalOfficerFamilyName'])]
print("Removed obsolete fields from data dictionary.")

# Add definitions for new fields
new_fields_definitions = {
    'PrincipalOfficerContactURL': "The contact URL for the principal officer of the organization.",
    'HeadOfOrganizationTitle': "The title of the head of the organization.",
    'HeadOfOrganizationURL': "The contact URL for the head of the organization.",
    'MMRChapter': "Indicates if the organization has a chapter in the Mayor's Management Report.",
    'RulemakingEntity': "Indicates whether the organization is a rulemaking entity.",
    'Name - NYC.gov Redesign - Original Value': "The original 'Agency Name' from the NYC.gov dataset before any modifications."
}

# Prepare new definitions as a DataFrame
new_definitions = pd.DataFrame([
    {'FieldName': field, 'Definition': desc}
    for field, desc in new_fields_definitions.items()
])

# Concatenate the new definitions to the data dictionary
data_dict = pd.concat([data_dict, new_definitions], ignore_index=True)
print("Added new field definitions to the data dictionary.")

# Remove the 'Description' column if it exists
if 'Description' in data_dict.columns:
    data_dict.drop(columns=['Description'], inplace=True)
    print("Removed 'Description' column from the data dictionary.")

# 7. Re-generate and Save the Data Dictionary

# Define the desired order of fields
desired_order = [
    'Name',
    'NameAlphabetized',
    'OperationalStatus',
    'PreliminaryOrganizationType',
    'Description',
    'URL',
    'ParentOrganization',
    'NYCReportingLine',
    'AuthorizingAuthority',
    'LegalCitation',
    'LegalCitationURL',
    'LegalCitationText',
    'LegalName',
    'AlternateNames',
    'Acronym',
    'AlternateAcronyms',
    'BudgetCode',
    'PrincipalOfficerName',
    'PrincipalOfficerTitle',
    'PrincipalOfficerContactURL',
    'HeadOfOrganizationName',
    'HeadOfOrganizationTitle',
    'HeadOfOrganizationURL',
    'MMRChapter',
    'RulemakingEntity',
    'OpenDatasetsURL',
    'FoundingYear',
    'SunsetYear',
    'URISlug',
    'DateCreated',
    'DateModified',
    'LastVerifiedDate',
    'Name - NYC.gov Organization List',
    "Name - NYC.gov Mayor's Office",
    'Name - NYC Open Data Portal',
    'Name - ODA',
    'Name - CPO',
    'Name - WeGov',
    'Name - Greenbook',
    'Name - Checkbook',
    'NameWithAcronym',
    'NameAlphabetizedWithAcronym',
    'Notes'
]

# Create a new data dictionary DataFrame with updated FieldNames
new_data_dict = pd.DataFrame({'FieldName': desired_order})

# Merge the new data dictionary with the existing one to retain definitions
data_dict = new_data_dict.merge(data_dict[['FieldName', 'Definition']], on='FieldName', how='left')

# Add DataType and ExampleValues
data_dict['DataType'] = data_dict['FieldName'].apply(lambda field: str(df[field].dtype) if field in df.columns else '')
data_dict['ExampleValues'] = data_dict['FieldName'].apply(
    lambda field: '; '.join(df[field].dropna().astype(str).unique()[:3]) if field in df.columns else ''
)

# Fill missing definitions with an empty string or a placeholder
data_dict['Definition'].fillna('', inplace=True)

# Save the updated data dictionary by overwriting the existing file
data_dict_path = get_file_path('docs', 'data_dictionary.csv')
try:
    data_dict.to_csv(data_dict_path, index=False)
    print(f"Data dictionary re-generated and saved to '{data_dict_path}'.")
except Exception as e:
    print(f"Error saving data dictionary: {e}")
    exit(1)

# Save the Updated DataFrame
# Save the updated DataFrame to a new CSV file
output_path = get_file_path('data', 'processed', 'nyc_organizations_updated.csv')
try:
    df.to_csv(output_path, index=False)
    print(f"Updated DataFrame saved to '{output_path}'.")
except Exception as e:
    print(f"Error saving updated DataFrame: {e}")
    exit(1)

print("Finished schema.py")
