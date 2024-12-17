# PROJECTPLAN.md

## Overview

This project’s ultimate goal is to create a single, standardized, and authoritative dataset of New York City government organization names by integrating and cleaning multiple sources. The process includes initial per-source preprocessing, merging datasets, normalization and standardization, fuzzy matching for deduplication, and comprehensive quality checks.

## Project History & Current Work

**Phase 1 (Completed in Colab):**  
- Integrated initial data sources into a single combined dataset.
- Developed a preliminary standardized export called `nyc_agencies_export.csv`.
- Created initial normalization logic and basic deduplication.

**Phase 2 (In Progress, Modular Refactoring):**  
- Introducing new data sources (`nyc_gov_hoo.csv`, `ops_data.csv`).
- Migrating to a structured, modular Python codebase with `src/` directories.
- Enhancing normalization and fuzzy matching techniques specific to NYC agencies.
- Updating tests to ensure normalization correctness and robust data preprocessing.

## Project Plan

Below is an outline of major tasks, milestones, and their purposes. Where relevant, tasks have been refined based on insights from recent testing and strategy sessions.

### Data Preprocessing & Integration

**Task A1: Refine Source-Specific Normalization Rules in Preprocessing**   
- Implement initial, source-specific normalization rules in `ops_processor.py` and `hoo_processor.py`. These rules should handle straightforward transformations (lowercasing, basic punctuation removal, replacing `&` with `and`, handling common abbreviations) and produce a `NameNormalized` column.  

**Task A2: Comprehensive Merging of Preprocessed Datasets**  
- Merge all preprocessed datasets (including the newly integrated `nyc_gov_hoo.csv` and `ops_data.csv`) into a single intermediate dataset.  
- Handle any schema alignment, ensuring that all essential fields (e.g., `NameNormalized`, `RecordID`) are present.  
- This merged intermediate dataset sets the stage for a global normalization pass if needed.

**Task A3 (Future): Secondary Normalization & Standardization**  
- Once merged, re-apply normalization logic at a global scale to ensure consistency across all sources.  
- This may include handling edge cases discovered after merging (e.g., removing parentheses consistently, unifying abbreviations, applying final transformations now that all data is visible in one place).

### Matching & Deduplication

**Task B1: Fuzzy Matching Thresholds & Logic**  
- Fine-tune fuzzy matching logic and thresholds after the merged dataset is fully normalized.  
- Document thresholds and heuristics.

**Task B2: Deduplication & Provenance Tracking**  
- Deduplicate the combined dataset, ensuring that each unique organization is represented once.  
- Preserve `RecordID` fields and ensure that source origin (provenance) is tracked.

### Analysis & Validation

**Task C1: Validation & Reporting**  
- Produce validation reports on duplicates, missing records, suspicious matches.  
- Implement scripts to check final dataset integrity and correctness.

**Task C2: Data Dictionary & Schema Updates**  
- Update `data_dictionary.csv` with finalized fields and normalization rules.
- Document final naming conventions and any transformations applied at each stage.

## Current Status & Recommendations

- **Normalization Test Improvements:**  
  We’ve introduced new tests verifying that source-level normalization (A1) is functioning correctly and producing expected outputs for representative test cases.
  
- **Warnings & Future Refinement:**  
  Current warnings from the processors highlight potential normalization improvements. However, the tests are passing, confirming that we’ve met the initial normalization requirements for each source.
  
- **Readiness for Task A2:**  
  With source-specific normalization (A1) now verified and a clearer strategy in place—performing a global normalization pass after merging—it’s reasonable to proceed to Task A2. In A2, you’ll merge the datasets, see them in aggregate, and then determine whether additional normalization steps are needed before matching and deduplication.

**Conclusion:**  
You have a solid baseline for initial normalization and thorough testing at the source level. The next logical step is Task A2: merging datasets. After merging, you can revisit normalization for a global standard, guided by the warnings and patterns observed so far. This approach ensures continuous improvement while maintaining a steady forward momentum in the project.
