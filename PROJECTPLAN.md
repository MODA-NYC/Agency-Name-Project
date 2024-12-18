## Overview

This project aims to produce a single, standardized, authoritative dataset of New York City government organization names. We will integrate multiple sources, apply robust normalization and deduplication, and perform fuzzy matching to ensure data quality. Quality checks, documentation, and iterative improvements guide this process.

## Project History & Current Work

**Phase 1 (Completed in Colab):**  
- Integrated initial data sources and produced a preliminary standardized export (`nyc_agencies_export.csv`).
- Developed initial normalization and basic deduplication logic.

**Phase 2 (In Progress, Modular Refactoring & Additional Data Sources):**  
- Completed:
  - Integrated new sources (`nyc_gov_hoo.csv`, `ops_data.csv`).
  - Established source-level normalization and merged datasets into a single intermediate dataset.
- Challenges Discovered:
  - Certain data sources (e.g., HOO data) rely on multiple fields (Agency Name, HoO Title) to distinguish unique entities.
  - Using Agency Name alone leads to ambiguous merges and loss of detail.
  - Relying solely on one column for normalization and deduplication may collapse distinct entities.

## Project Plan

**Data Preprocessing & Integration**

- **Task A1: Source-Level Normalization** ✓  
  Implement per-source normalization in `ops_processor.py` and `hoo_processor.py` to produce a `NameNormalized` column.

- **Task A2: Merge Preprocessed Datasets** ✓  
  Combine all preprocessed data (including new sources) into a single intermediate dataset.  
  Ensure consistent schema (including `NameNormalized` and `RecordID`) to enable global normalization.

- **Task A3: Global Normalization & Multi-Field Enrichment** (Current Focus)  
  **Goal:** Ensure that the `NameNormalized` field incorporates additional distinguishing attributes before global normalization.
  
  **Actions:**
  1. Adjust Source Processing Scripts:
     - Enhance `hoo_processor.py` to create `AgencyNameEnriched` field combining AgencyName and HoOTitle
     - Update `ops_processor.py` and other processors as needed
  2. Update Normalization Logic:
     - Normalize `AgencyNameEnriched` fields instead of just Agency Name
     - Ensure `NameNormalized` reflects these enriched fields
  3. Re-run Merging & Global Normalization:
     - Re-merge datasets with updated processors
     - Apply global normalization script on enriched results
  4. Validate Counts & Spot Checks:
     - Verify source record counts (e.g., HOO's 179 records)
     - Ensure alignment between source and integrated counts

**Matching & Deduplication**

- **Task B1: Fuzzy Matching Logic & Thresholds**  
  Leverage improved global normalization to tune fuzzy matching thresholds and heuristics.  
  Document logic and thresholds for reproducibility.

- **Task B2: Deduplication & Provenance**  
  Deduplicate records using enriched normalization results.  
  Track provenance and maintain `RecordID` fields across merges and transformations.

**Analysis & Validation**

- **Task C1: Validation & Reporting**  
  Produce reports on duplicates, missing records, and suspicious matches.  
  Confirm integrity and correctness of the final dataset.

- **Task C2: Data Dictionary & Documentation**  
  Update `data_dictionary.csv` with finalized fields and normalization rules.  
  Record naming conventions and transformations.

## Current Status & Recommendations

- **Source-Level Processing Complete:**  
  Initial source-specific normalization (A1) and merging (A2) are complete.

- **Next Steps (Task A3):**  
  Implement multi-field enrichment strategy to prevent unintended record collapses.  
  Re-run global normalization with enriched fields to improve matching accuracy.

**Conclusion:**  
The project has identified key challenges in maintaining distinct entity records. The revised approach using multi-field enrichment will provide a more accurate foundation for subsequent fuzzy matching and deduplication tasks, ultimately producing a more reliable standardized dataset.
