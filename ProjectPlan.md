# Project Plan: Probabilistic Matching and Deduplication of Merged Dataset

## Objective
Implement a probabilistic matching process to identify and consolidate duplicate records in `merged_dataset.csv` by utilizing the NameNormalized field. The goal is to generate a list of potential matches for manual confirmation and then deduplicate the dataset based on these confirmations.

## Overview
This project aims to create a standardized list of Agency Names for the NYC Open Data portal. The primary goal is to enhance data legibility and interoperability by providing official, consistently formatted agency names, which will serve as a canonical source for agency name formatting and improve data quality when joining datasets.

After merging `nyc_agencies_export.csv`, `nyc_gov_hoo.csv`, and `ops_data.csv` into `merged_dataset.csv`, we've identified duplicate entries with slight variations in their names and attributes. To address this, we'll implement a probabilistic matching algorithm focusing on the NameNormalized field to find probable duplicates, compile them for manual review in `consolidated_matches.csv`, and then deduplicate the merged dataset accordingly.

Note: The word "Agency" is colloquially used to mean a government organization that includes a New York City Agency, a Mayoral Office, or a Commission.

## Plan of Action

### 1. Implement Probabilistic Matching Based on NameNormalized

**Action:**

* 1.1. String Similarity Metrics Selection:
  * Primary Metric: Jaro-Winkler Distance
    - Optimal for short strings like organization names
    - Weights matching characters at string start higher
    - Effectively handles typos and minor variations
  * Secondary Metric: Token Sort Ratio
    - Handles word reordering in agency names
    - Normalizes whitespace and case differences
  * Implementation using RapidFuzz library for better performance
    ```python
    from rapidfuzz import fuzz
    
    def get_composite_score(str1, str2):
        jw_score = fuzz.jaro_winkler(str1, str2, score_cutoff=0)
        token_score = fuzz.token_sort_ratio(str1, str2, score_cutoff=0)
        return (jw_score * 0.6) + (token_score * 0.4)
    ```

* 1.2. Matching Algorithm Development:
  * Add specific NYC agency name patterns:
    - Handle "NYC" prefix/suffix variations
    - Account for "Mayor's Office of" vs "Office of" patterns
    - Special handling for borough-specific variations
    - Handle parenthetical acronyms
  * Create composite scoring function combining both metrics:
    - 60% weight on Jaro-Winkler (better for agency names)
    - 40% weight on Token Sort Ratio (handles word reordering)
  * Set threshold scores:
    - Jaro-Winkler: 85% minimum similarity
    - Token Sort: 80% minimum similarity
    - Composite: 82% minimum similarity
  * Implement blocking by first letter to improve performance
    ```python
    # Example blocking implementation
    merged_df['NameFirstLetter'] = merged_df['NameNormalized'].str[0]
    for letter in merged_df['NameFirstLetter'].unique():
        block = merged_df[merged_df['NameNormalized'].str[0] == letter]
        # Compare records within block
    ```

* 1.3. Data Preparation for Matching:
  * Add unique identifier:
    - Generate RecordID for each entry in merged_dataset.csv
    - Format: 'REC_XXXXXX' (where X is a zero-padded number)
  * Clean NameNormalized field:
    - Convert to lowercase
    - Remove extra whitespace
    - Handle NYC-specific patterns
    - Standardize abbreviations
    - Remove special characters
  * Filter dataset:
    - Remove null or empty NameNormalized entries
    - Ensure consistent character encoding (ASCII)
  * NYC-specific standardization rules:
    - Remove "NYC" prefix/suffix variations
    - Standardize mayor's office naming patterns
    - Remove borough names
    - Handle special characters in O'Neill, etc.
  * Example preprocessing:
    ```python
    def prepare_merged_dataset(df):
        # Add unique identifier
        df['RecordID'] = [f'REC_{i:06d}' for i in range(len(df))]
        
        def clean_name_for_matching(name):
            if pd.isna(name): return None
            
            # NYC-specific preprocessing
            name = re.sub(r'\bnyc\s+|,?\s+nyc\b', '', name.lower())  # Handle NYC variations
            name = re.sub(r"mayor'?s?\s+office\s+of\b", 'office of', name)  # Standardize mayor's office
            name = re.sub(r'\b(brooklyn|queens|staten island|manhattan|bronx)\b', '', name)  # Remove borough names
            
            # Then proceed with general cleaning
            name = unicodedata.normalize('NFKD', name)
            name = name.encode('ascii', 'ignore').decode('ascii')
            name = re.sub(r'\bdept\b', 'department', name, flags=re.IGNORECASE)
            name = re.sub(r'[^\w\s]', '', name)
            return name.strip()
        
        df['NameNormalized'] = df['Name'].apply(clean_name_for_matching)
        return df
    ```


### 2. Generate Potential Matches for Manual Review

**Action:**

* 2.1. Append New Potential Matches to `consolidated_matches.csv`:
   * Match existing format in consolidated_matches.csv:
    - Source: Always in lowercase
    - Target: Always in lowercase
    - Score: Optional numeric score (0-100)
    - Label: "Match" or "No Match" (case-insensitive)
    - SourceID: RecordID from merged_dataset.csv
    - TargetID: RecordID from merged_dataset.csv
  * Format new potential matches to match existing structure:
    - Source: Original normalized name
    - Target: Potential match normalized name
    - Score: Similarity score (0-100)
    - Label: Auto-labeled "Match" for 100% scores, empty for others
    - SourceID: Source record's RecordID
    - TargetID: Target record's RecordID
  * Example structure:
    ```csv
    Source,Target,Score,Label,SourceID,TargetID
    department of education,education department,92.5,,REC_000123,REC_000456
    office of the mayor,mayors office,100,Match,REC_000789,REC_000012
    ```
  * Implementation approach:
    ```python
    def generate_potential_matches(merged_df):
        # Get existing matches to avoid duplicates
        existing_matches = pd.read_csv('data/processed/consolidated_matches.csv')
        existing_pairs = set(zip(existing_matches['Source'], existing_matches['Target']))
        
        # Generate new potential matches
        new_matches = []
        for idx1, row1 in merged_df.iterrows():
            block = merged_df[merged_df['NameNormalized'].str[0] == row1['NameNormalized'][0]]
            for idx2, row2 in block.iterrows():
                if idx1 < idx2:  # Avoid self-matches and duplicates
                    score = get_composite_score(row1['NameNormalized'], row2['NameNormalized'])
                    if score >= 82:  # Using threshold from section 1.2
                        pair = (row1['NameNormalized'], row2['NameNormalized'])
                        if pair not in existing_pairs:
                            new_matches.append({
                                'Source': pair[0],
                                'Target': pair[1],
                                'Score': score,
                                'Label': 'Match' if score == 100 else '',
                                'SourceID': row1['RecordID'],
                                'TargetID': row2['RecordID']
                            })
        
        if new_matches:
            # Convert to DataFrame and sort by score descending
            new_matches_df = pd.DataFrame(new_matches)
            new_matches_df = new_matches_df.sort_values('Score', ascending=False)
            
            # Append new matches to existing file
            new_matches_df.to_csv('data/processed/consolidated_matches.csv', 
                                mode='a', header=False, index=False)
    ```
  * Quality checks:
    - Ensure no duplicate pairs are added
    - Verify score calculations are consistent
    - Maintain consistent string normalization
    - Validate RecordID references exist in merged_dataset.csv
    - Confirm automatic labeling of 100% matches
    - Monitor distribution of similarity scores
    - Track coverage of manual reviews

### 3. Manually Review and Confirm Matches

**Action:**
* 3.1. Review Process:
  * Manually examine each pair in `consolidated_matches.csv`
  * Determine whether records are true duplicates based on domain knowledge and additional attributes
* 3.2. Label Matches:
  * Assign a label of "Match" or "No Match" to each pair
  * Save the reviewed pairs and labels in `consolidated_matches.csv`

### 4. Deduplicate the Merged Dataset

**Action:**

#### 4.1. Map Confirmed Matches
* Create Connected Components:
  - Build graph from confirmed matches in `consolidated_matches.csv`
  - Identify all connected components (groups of related matches)
  - Handle transitive relationships
  ```python
  def build_match_groups(matches_df):
      # Create graph from confirmed matches
      G = nx.Graph()
      
      # Add edges for matches
      for _, row in matches_df[matches_df['Label'] == 'Match'].iterrows():
          G.add_edge(row['SourceID'], row['TargetID'])
      
      # Get connected components (groups of related matches)
      return list(nx.connected_components(G))
  ```

#### 4.2. Merge Duplicates
* Canonical Record Selection:
  - Use any NameNormalized value as canonical (arbitrary choice)
  - Merge all values from nyc_agencies_export, nyc_gov_hoo, and ops_data
  - Store "Name - Ops" and "Name - NYC.gov redesign" in separate columns

* Field Merging Rules:
  1. Principal Officer Fields:
     - Use `HeadOfOrganizationName` for `PrincipalOfficerName` if blank
     - Use `HeadOfOrganizationTitle` for `PrincipalOfficerTitle` if blank
     - Create `PrincipalOfficerContactURL` from `HeadOfOrganizationURL`
  2. Alert for Edge Cases:
     - Implement alert for multiple `HeadOfOrganizationName` values in a match group
     ```python
     def check_for_conflicts(records):
         names = records['HeadOfOrganizationName'].dropna().unique()
         if len(names) > 1:
             print(f"Conflict detected: {names}")
     ```

* Implementation:
  ```python
  def merge_duplicate_records(group, merged_df):
      records = merged_df[merged_df['RecordID'].isin(group)]
      
      # Check for edge cases
      check_for_conflicts(records)
      
      # Merge fields according to rules
      merged = {
          'NameNormalized': records['NameNormalized'].iloc[0],  # Arbitrary choice
          'Name - Ops': records['Name - Ops'].dropna().unique(),
          'Name - NYC.gov redesign': records['Name - NYC.gov redesign'].dropna().unique(),
          'PrincipalOfficerName': records['PrincipalOfficerName'].fillna(records['HeadOfOrganizationName']).iloc[0],
          'PrincipalOfficerTitle': records['PrincipalOfficerTitle'].fillna(records['HeadOfOrganizationTitle']).iloc[0],
          'PrincipalOfficerContactURL': records['HeadOfOrganizationURL'].iloc[0]
      }
      
      return merged
  ```

#### 4.3. Remove Redundancies
* Process:
  1. Create new deduplicated dataset
  2. Add merged records
  3. Add non-duplicate records
  4. Verify no information loss
  5. Update all references to old RecordIDs

* Quality Checks:
  - Verify all required fields present
  - Validate merged data consistency
  - Check referential integrity
  - Document merge decisions

### 5. Validate and Document the Deduplication Process

**Action:**
* 5.1. Validation:
  * Verify that all duplicates have been appropriately merged and removed
  * Check for any remaining inconsistencies or errors in the dataset
  * NYC-specific validation:
    - Verify borough consistency
    - Check mayoral office naming standards
    - Validate acronym usage
    - Cross-reference against official NYC.gov listings
  * Schema validation:
    - Verify data types for all fields
    - Ensure Budget Code preserves leading zeros
    - Validate date fields (FoundingYear, SunsetYear)
    - Check for obsolete or redundant fields
* 5.2. Documentation:
  * Record the methodologies used for probabilistic matching and deduplication
  * Note any challenges faced and how they were resolved
  * Include code snippets and explanations as reference for future maintenance

## Deliverables
* `consolidated_matches.csv`: List of potential duplicate pairs with similarity scores and manual review labels
* `deduplicated_dataset.csv`: The final dataset with duplicates merged and removed
* Documentation: Detailed report explaining the matching algorithm, deduplication steps, and decision-making processes

**Dependencies:**
- Add to requirements.txt:
  ```
  rapidfuzz>=3.0.0
  ```

**File Structure:**

The project has the following file structure:

```
agency-name-project-main/
├── data/
│   ├── exports/
│   ├── intermediate/
│   │   └── merged_dataset.csv
│   ├── processed/
│   │   ├── .gitkeep
│   │   ├── consolidated_matches.csv
│   │   └── nyc_agencies_export.csv
│   └── raw/
│       ├── .gitkeep
│       ├── CPO Data.csv
│       ├── nyc_gov_hoo.csv
│       ├── ODA Data.csv
│       ├── ops_data.csv
│       └── WeGov Data.csv
├── docs/
│   └── data_dictionary.csv
├── src/
│   ├── __init__.py
│   ├── analyze_merged_dataset.py
│   ├── data_loading.py
│   ├── data_merging.py
│   ├── data_normalization.py
│   ├── data_preprocessing.py
│   ├── main.py
│   ├── preprocess_nyc_gov_hoo.py
│   └── schema.py
├── .gitignore
├── FileStructure.md
├── ProjectPlan.md
├── README.md
├── requirements.txt
└── .cursorrules

```

