# Step 2.6: Final Cleanup and ID Assignment

## Overview
This step performs final cleanup on the merged dataset and assigns stable unique IDs to each record.

## Input
- `data/processed/final_deduplicated_dataset.csv`
  - Contains merged and initially deduplicated records
  - May have remaining duplicates or naming inconsistencies
  - Uses various source-specific ID formats

## Process Flow

1. **Final Deduplication**
   ```
   [final_deduplicated_dataset.csv] --> [cleanup_matches.py]
                                   --> [Resolve remaining duplicates]
                                   --> [Fix naming conflicts]
                                   --> [Temporary cleaned dataset]
   ```

2. **ID Assignment**
   ```
   [Temporary cleaned dataset] --> [assign_final_ids.py]
                               --> [Generate FINAL_REC_XXXXXX IDs]
                               --> [Create ID crosswalk]
                               --> [final_cleaned_dataset.csv]
   ```

3. **Validation & Analysis**
   ```
   [final_cleaned_dataset.csv] --> [id_assignment_report.py]
                               --> [final_cleanup_report.py]
                               --> [Validation reports]
   ```

## Output
- Primary Output:
  - `data/processed/final_cleaned_dataset.csv`
    - Clean, deduplicated records
    - New stable FINAL_REC_XXXXXX IDs
    - Standardized agency names

- Supporting Files:
  - `data/processed/id_crosswalk.csv`
    - Maps original source IDs to new FINAL_REC_XXXXXX IDs
    - Enables traceability back to source records

- Analysis Reports:
  - `data/analysis/id_assignment_report.csv`
  - `data/analysis/final_cleanup_report.csv`

## Key Metrics
- Input Record Count: [Previous total] records
- Output Record Count: [New total] records
- Duplicates Resolved: [Count] records
- Naming Conflicts Fixed: [Count] issues
- ID Format: FINAL_REC_XXXXXX (6-digit sequential number)

## Notes
- This is an interim solution until a more systematic ID assignment mechanism is implemented
- The FINAL_REC_XXXXXX format is designed to be stable but will be replaced in a future phase
- ID crosswalk ensures traceability during the transition to a new ID system 