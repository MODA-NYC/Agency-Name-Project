# Step 2.6: Final Cleanup and ID Assignment

## Overview
This step performs final cleanup on the merged dataset, assigns stable unique IDs to each record, and adds the NYC.gov Agency Directory flag.

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
                               --> [intermediate cleaned dataset]
   ```

3. **Manual Merge and Flag Generation**
   ```
   [intermediate cleaned dataset] --> [phase2.6_manual_merge.py]
                                  --> [Apply manual merge instructions]
                                  --> [Add NYC.gov Agency Directory flag]
                                  --> [final_cleaned_dataset.csv]
   ```

4. **Validation & Analysis**
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
    - NYC.gov Agency Directory flag

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
- NYC.gov Agency Directory Flagged: [Count] agencies

## Notes
- This is an interim solution until a more systematic ID assignment mechanism is implemented
- The FINAL_REC_XXXXXX format is designed to be stable but will be replaced in a future phase
- ID crosswalk ensures traceability during the transition to a new ID system
- NYC.gov Agency Directory flag criteria: 
  1. Active operational status
  2. Not a Non-Governmental Organization
  3. Not a Division (except for 311, DHS, and HRA)
  4. Not Judiciary or Financial Institution
  5. URL doesn't contain "ny.gov"
  6. Has at least one of: URL, principal officer name, or contact URL 