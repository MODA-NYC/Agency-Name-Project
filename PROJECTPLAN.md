## Overview

This project aims to produce a single, standardized, authoritative dataset of New York City government organization names. We will integrate multiple sources, apply robust normalization and deduplication, and perform fuzzy matching to ensure data quality. Quality checks, documentation, and iterative improvements guide this process.

## Project History & Current Work

**Phase 1 (Completed in Colab):**  
- Integrated initial data sources and produced a preliminary standardized export (`nyc_agencies_export.csv`).
- Developed initial normalization and basic deduplication logic.

**Phase 2 (In Progress, Modular Refactoring & Additional Data Sources):**  
- Introduced new sources (`nyc_gov_hoo.csv`, `ops_data.csv`).
- Restructured code into a modular Python codebase (`src/` directories).
- Established source-level normalization and successfully merged datasets into a single intermediate dataset.
- Updated tests to ensure correctness of initial normalization and merging.

## Project Plan

**Data Preprocessing & Integration**

- **Task A1: Source-Level Normalization**  
  Implement per-source normalization in `ops_processor.py` and `hoo_processor.py` to produce a `NameNormalized` column.

- **Task A2: Merge Preprocessed Datasets**  
  Combine all preprocessed data (including new sources) into a single intermediate dataset.  
  Ensure consistent schema (including `NameNormalized` and `RecordID`) to enable global normalization.

- **Task A3: Global Normalization & Standardization** (Next Step)  
  Re-apply and refine normalization rules across the merged dataset.  
  Address edge cases (e.g., parentheses, special abbreviations, final stopword decisions).  
  Prepare the dataset for accurate fuzzy matching by preserving necessary terms and ensuring consistent formatting.

**Matching & Deduplication**

- **Task B1: Fuzzy Matching Logic & Thresholds**  
  After global normalization, tune fuzzy matching thresholds and heuristics.  
  Document logic and thresholds for reproducibility.

- **Task B2: Deduplication & Provenance**  
  Deduplicate records to ensure each organization is unique.  
  Track provenance and maintain `RecordID` fields across merges and transformations.

**Analysis & Validation**

- **Task C1: Validation & Reporting**  
  Produce reports on duplicates, missing records, and suspicious matches.  
  Confirm integrity and correctness of the final dataset.

- **Task C2: Data Dictionary & Documentation**  
  Update `data_dictionary.csv` with finalized fields and normalization rules.  
  Record naming conventions and transformations.

## Current Status & Recommendations

- **Source-Level Tests Passed:**  
  Source-specific normalization (A1) and initial merging (A2) are complete.

- **Next Steps (Task A3):**  
  Perform a global normalization pass on the merged dataset, resolving edge cases and ensuring uniformity.  
  Fine-tune stopwords and final transformations to support accurate fuzzy matching and deduplication in the following tasks.

**Conclusion:**  
You have a stable intermediate dataset. Now proceed with Task A3: apply global normalization, refine naming conventions, and finalize transformations. This will pave the way for fuzzy matching and deduplication, ultimately leading to a standardized and authoritative dataset.
