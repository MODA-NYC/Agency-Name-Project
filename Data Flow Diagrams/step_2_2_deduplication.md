# Step 2.2 - Clean & Deduplicate

## Entry Point (main.py)
```
main.py
├── Load Merged Dataset
│   └── Load merged_dataset.csv
├── Run Deduplication
│   └── Call deduplicate_merged_data()
└── Save Results
    └── Save to dedup_merged_dataset.csv
```

## Deduplication Flow (data_merging.py)
```
deduplicate_merged_data()
├── Check for Exact Duplicates
│   ├── By RecordID
│   │   └── Log warning if found (should not happen)
│   └── By NameNormalized
│       ├── Group by NameNormalized
│       └── Log potential duplicates
│
├── Apply Deduplication Rules
│   ├── Rule 1: Keep Primary Source
│   │   └── If same name exists in multiple sources, prefer:
│   │       1. nyc_agencies_export (primary)
│   │       2. ops
│   │       3. nyc_gov
│   │
│   ├── Rule 2: Keep Most Information
│   │   └── When merging duplicates:
│   │       ├── Keep record with most non-null fields
│   │       ├── Combine unique information from duplicates
│   │       └── Track merged records in 'merged_from' column
│   │
│   └── Rule 3: Special Cases
│       └── Handle known special cases:
│           ├── Department of Social Services/HRA
│           ├── Mayor's Office variations
│           └── Health + Hospitals/HHC
│
├── Create Audit Trail
│   ├── Save deduplication summary
│   │   ├── Total duplicates found
│   │   ├── Records merged
│   │   └── Rules applied
│   └── Save detailed log of merged records
│
└── Return Deduplicated DataFrame
    └── Include new columns:
        ├── dedup_source: Source of final record
        ├── merged_from: List of merged RecordIDs
        └── merge_note: Explanation of merge decision
```

## Data Flow
```
Input (851 records)
└── merged_dataset.csv
    ├── Standard fields
    │   ├── RecordID
    │   ├── Agency Name
    │   ├── NameNormalized
    │   └── [Other fields...]
    └── Source tracking
        ├── source
        └── data_source

Output
└── dedup_merged_dataset.csv
    ├── All original fields
    ├── Deduplication metadata
    │   ├── dedup_source
    │   ├── merged_from
    │   └── merge_note
    └── Audit files
        ├── dedup_summary.json
        └── merged_records_log.csv
```

## Key Files
1. `main.py`: Orchestrates the process
2. `src/data_merging.py`: Core deduplication logic
3. `data/intermediate/merged_dataset.csv`: Input file
4. `data/intermediate/dedup_merged_dataset.csv`: Output file
5. `data/analysis/dedup_summary.json`: Deduplication summary
6. `data/analysis/merged_records_log.csv`: Detailed merge log

## Validation Points
1. Verify no duplicate RecordIDs (✓ Confirmed)
2. Check for remaining NameNormalized duplicates
3. Validate source preferences were applied correctly
4. Confirm special cases were handled properly
5. Verify all merged records are properly documented
6. Check that no critical information was lost during merging
7. Validate deduplication audit trail is complete and accurate 