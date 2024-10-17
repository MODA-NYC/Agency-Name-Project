# Project Plan: Addressing Discrepancies in Merged Dataset

## Summary of the Issue

1. **`nyc_gov_hoo.csv`** contains **177 records**, but only **62 records** have non-null values in the `"Name - NYC.gov Redesign"` field in `merged_dataset.csv`.

2. **`ops_data.csv`** contains **418 records**, but only **154 records** have non-null values in the `"Name - Ops"` field in `merged_dataset.csv`.

## Objective

Modify the data merging process to ensure that all records from `nyc_gov_hoo.csv` and `ops_data.csv` are properly included in `merged_dataset.csv`, without breaking existing functionality. Additionally, include rows in the merged_dataset file based on NameNormalized values from the merge sources that don't exist in the nyc_agencies_export.csv base file.

## Plan of Action

1. **Review the Current Merging Process**
2. **Identify Potential Causes for the Discrepancies**
3. **Adjust the Merging Strategy**
4. **Implement NameNormalized Creation**
5. **Ensure Inclusion of New NameNormalized Entries**
6. **Implement and Test Changes**

## Detailed Steps

### 1. Review the Current Merging Process
- Locate the `merge_dataframes` function in the codebase.

### 2. Examine Merge Keys and Methods
- Check which columns are being used to merge the datasets.
- Determine the current merge method (inner, left, right, or outer join).

### 3. Standardize Merge Keys
- Implement a function to standardize agency names across datasets.

### 4. Implement NameNormalized Creation
- Create a function to generate NameNormalized values for each dataset.
- Apply this function to nyc_gov_hoo.csv, ops_data.csv, and nyc_agencies_export.csv.

### 5. Ensure Inclusion of New NameNormalized Entries
- Modify the merge process to create new rows in the merged_dataset for NameNormalized values from nyc_gov_hoo.csv and ops_data.csv that don't exist in nyc_agencies_export.csv.
- Implement a method to flag these new entries for review.

### 6. Adjust the Merging Strategy
- Modify the merge to use an outer join to include all records, including those with new NameNormalized values.
- Ensure that the merge keys include the newly created NameNormalized field.

### 7. Handle Missing and Duplicate Values
- Decide on a strategy for handling NaN values and potential duplicates.

### 8. Verify Data Types and Formats
- Ensure consistency in data types for merge columns.

## Potential Issues and Resolutions

### Issue 1: Inconsistent Agency Names
- Create a mapping or crosswalk table for agency names.
- Use data cleaning techniques to standardize names.

### Issue 2: Merge Method Excluding Records
- Use outer joins to include all records from both datasets.

### Issue 3: Data Cleaning Steps Removing Records
- Review and adjust data cleaning steps to prevent unintended record removal.

## Example Code Updates

### Standardizing Agency Names

```python
def standardize_agency_name(name):
    if pd.isna(name):
        return name
    return ' '.join(name.strip().lower().split())

# Apply to each dataframe
nyc_gov_hoo_df['Agency Name'] = nyc_gov_hoo_df['Agency Name'].apply(standardize_agency_name)
ops_data_df['Agency Name'] = ops_data_df['Agency Name'].apply(standardize_agency_name)
main_df['Agency Name'] = main_df['Agency Name'].apply(standardize_agency_name)
```

### Creating and Including New NameNormalized Entries

```python
def create_name_normalized(name):
    # Implementation of NameNormalized creation logic
    pass

# Apply to each dataframe
for df in [nyc_gov_hoo_df, ops_data_df, main_df]:
    df['NameNormalized'] = df['Agency Name'].apply(create_name_normalized)

# Merge with inclusion of new NameNormalized entries
merged_df = pd.merge(
    left=main_df,
    right=nyc_gov_hoo_df,
    how='outer',
    on='NameNormalized',
    indicator=True
)

# Flag new entries
merged_df['New_Entry'] = merged_df['_merge'] == 'right_only'

# Repeat similar process for ops_data_df
```

### Merging with Outer Joins

```python
# Merge nyc_gov_hoo_df into main_df
merged_df = pd.merge(
    left=main_df,
    right=nyc_gov_hoo_df[['NameNormalized', 'Desired Columns...']],
    how='outer',
    on='NameNormalized'
)

# Merge ops_data_df into merged_df
merged_df = pd.merge(
    left=merged_df,
    right=ops_data_df[['NameNormalized', 'Desired Columns...']],
    how='outer',
    on='NameNormalized'
)
```

## Validating the Results

1. **Check Record Counts**
   - Verify that the number of records in merged_dataset.csv matches or exceeds the sum of unique records from all source files.

2. **Spot-Check Data**
   - Randomly select records from each source file and ensure they are correctly represented in merged_dataset.csv.

3. **Handle Unmatched Records**
   - Review any records that didn't match during the merge process and determine appropriate actions (e.g., manual review, additional data cleaning).

4. **Review New Entries**
   - Examine the rows flagged as new entries to ensure they are correctly identified and contain appropriate information.

## Next Steps

1. Implement the Code Changes
   - Update the merging script with the new standardization and merging logic.
   - Implement the NameNormalized creation function.
   - Add logic to include and flag new entries based on NameNormalized values.

2. Run the Merging Process
   - Execute the updated script on the source data files.

3. Verify the Output
   - Perform the validation steps outlined above.
   - Check for any unexpected issues or anomalies in the merged dataset.

4. Review for Side Effects
   - Ensure that the changes haven't negatively impacted any existing functionality or data integrity.

5. Document Changes
   - Update any relevant documentation to reflect the new merging process and the inclusion of new NameNormalized entries.

6. Consider Automation
   - Evaluate the possibility of automating this process for future updates to the dataset.