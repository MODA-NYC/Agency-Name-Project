import pandas as pd
import os

def main():
    # Define the path to the merged dataset
    base_path = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
    file_path = os.path.join(base_path, 'data', 'intermediate', 'merged_dataset.csv')

    # Load the dataset
    try:
        df = pd.read_csv(file_path)
        print(f"Loaded '{file_path}' successfully.")
    except FileNotFoundError:
        print(f"The file '{file_path}' does not exist.")
        return

    # Display basic information
    print("\nBasic Information:")
    print(df.info())

    # Check for duplicates based on 'Name' and 'NameNormalized'
    print("\nDuplicate Analysis:")
    for column in ['Name', 'NameNormalized']:
        if column in df.columns:
            duplicates = df[df.duplicated(subset=column, keep=False)].sort_values(by=column)
            num_duplicates = duplicates.shape[0]
            print(f"Number of duplicate entries based on '{column}': {num_duplicates}")
            if num_duplicates > 0:
                print(duplicates[[column]].drop_duplicates().head())
        else:
            print(f"Column '{column}' not found in the dataset.")

    # Analyze key fields for matching
    key_fields = [
        'Name',
        'NameNormalized',
        'Acronym',
        'AlternateNames',
        'AlternateAcronyms',
        'Name - NYC.gov Redesign',
        'HeadOfOrganizationName'
    ]

    print("\nAnalysis of Key Fields:")
    for field in key_fields:
        if field in df.columns:
            unique_values = df[field].nunique(dropna=True)
            missing_values = df[field].isna().sum()
            print(f"Field: {field}")
            print(f"  - Unique values: {unique_values}")
            print(f"  - Missing values: {missing_values}")
        else:
            print(f"Field '{field}' not found in the dataset.")

    # Analyze variations in duplicate entries
    print("\nVariations in Duplicate Entries:")
    if 'NameNormalized' in df.columns:
        duplicate_groups = df.groupby('NameNormalized')
        for name_normalized, group in duplicate_groups:
            if len(group) > 1:
                print(f"\nDuplicate Group: {name_normalized}")
                print(group[['Name', 'Acronym', 'AlternateNames', 'AlternateAcronyms']])

if __name__ == "__main__":
    main()
