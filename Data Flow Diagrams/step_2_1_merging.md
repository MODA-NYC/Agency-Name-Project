# Step 2.1 - Merging Flow

## Entry Point (main.py)
```
main.py
├── Load Datasets
│   ├── Load primary dataset (nyc_agencies_export.csv)
│   ├── Load OPS processed data
│   └── Load HOO processed data
├── Prepare for Merging
│   ├── Define standard fields to keep
│   │   ├── RecordID
│   │   ├── Agency Name
│   │   └── NameNormalized
│   └── Set up source prefixes
│       ├── 'nyc_gov' for HOO data
│       └── 'ops' for OPS data
└── Execute Merge Process
    └── Call merge_dataframes()
```

## Merging Flow (data_merging.py)
```
merge_dataframes()
├── Initialize merged_df with primary data
│   ├── Copy primary DataFrame
│   ├── Derive 'Agency Name' from 'Name' if missing
│   └── Add 'source' = 'primary'
│
├── Process Secondary DataFrames
│   └── For each secondary DataFrame:
│       ├── Combine standard and additional fields
│       ├── Validate required fields exist
│       ├── Remove missing fields with warning
│       ├── Add source column (ops/nyc_gov)
│       └── Concatenate with merged_df
│
├── Clean Merged Data
│   ├── Remove records with no identifying info
│   │   └── Drop if both 'Name' and 'Agency Name' are null
│   ├── Handle URL conflicts
│   │   ├── Fill primary URL with HOO URL if missing
│   │   └── Drop HeadOfOrganizationURL column
│   ├── Fill missing Agency Name from Name
│   └── Fill missing NameNormalized using normalizer
│
├── Track Data Provenance
│   └── Add 'data_source' based on:
│       ├── Check source-specific name columns first:
│       │   ├── 'ops' if has 'Name - Ops'
│       │   ├── 'nyc_gov' if has 'Name - NYC.gov Redesign'
│       │   └── 'nyc_agencies_export' if has 'Name'
│       └── Fall back to 'source' column value
│
└── Ensure Record IDs
    ├── Create RecordID if missing (REC_XXXXXX)
    ├── Fix invalid IDs not matching pattern [A-Z]+_\d{6}
    ├── Log count of fixed invalid IDs
    └── Handle duplicate IDs by regenerating
```

## Data Flow
```
Input
├── Primary Dataset (260 records)
│   └── nyc_agencies_export.csv
│
├── OPS Processed Data (417 → 413 records)
│   ├── RecordID (OPS_XXXXXX)
│   ├── Agency Name
│   ├── NameNormalized
│   └── [Other OPS columns]
│
└── HOO Processed Data (178 → 177 records)
    ├── RecordID (HOO_XXXXXX)
    ├── Agency Name
    ├── NameNormalized
    ├── AgencyNameEnriched
    └── [Other HOO columns]

Output (merged_dataset.csv)
└── Merged DataFrame (851 total records)
    ├── All primary columns
    ├── Standard fields from secondary sources
    ├── Source tracking columns:
    │   ├── source distribution:
    │   │   ├── ops: 413 records
    │   │   ├── primary: 261 records
    │   │   └── nyc_gov: 177 records
    │   └── data_source (detailed provenance)
    └── Validated RecordIDs (100% coverage)
```

## Key Files
1. `main.py`: Orchestrates the merging process
2. `src/data_merging.py`: Core merging logic
3. `data/processed/nyc_agencies_export.csv`: Primary dataset
4. `data/intermediate/merged_dataset.csv`: Output file

## Validation Points
1. Verify all records have valid RecordIDs (✓ Confirmed)
   - All IDs match pattern [A-Z]+_\d{6}
   - No duplicate IDs
2. Check source and data_source columns are properly populated (✓ Confirmed)
   - All records have a valid source
   - Data source derived from name columns or source
3. Confirm record counts match expectations:
   - Raw total: 855 records
   - After processing: 851 records
   - Delta explained by:
     - Removed 2 HOO records with no identifying info
     - OPS deduplication (-4) from previous step
4. Validate data quality:
   - No null values in Agency Name (filled from Name)
   - No null values in NameNormalized (filled using normalizer)
   - No null RecordIDs
5. Ensure proper handling of URL conflicts (✓ Confirmed)
   - HeadOfOrganizationURL merged into URL field
   - Redundant URL columns dropped
6. Check column name consistency across sources (✓ Confirmed)
   - Standard fields present: RecordID, Agency Name, NameNormalized
   - Source-specific fields preserved as needed 