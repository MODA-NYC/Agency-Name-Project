# PROJECTPLAN.md

## Overview

This project plan details the key milestones, tasks, script consolidation approach, and timelines for completing the NYC Agency Name Standardization Project. The goal is to establish a well-structured and maintainable pipeline that integrates multiple government datasets, applies standardized naming conventions, performs robust fuzzy matching, and generates high-quality deduplicated datasets.

## Objectives

1. **Data Integration & Normalization:**  
   - Integrate new data sources (e.g., `nyc_gov_hoo.csv`, `ops_data.csv`) into the existing standardized dataset.
   - Ensure consistent normalization, such as removing "NYC", standardizing acronyms, and harmonizing organizational hierarchy references.

2. **Fuzzy Matching & Deduplication:**  
   - Implement probabilistic matching thresholds (e.g., 82.0) with well-documented heuristics.
   - Deduplicate records post-merge while preserving data provenance and record IDs.
   - Improve fuzzy matching logic by refactoring into independent, testable modules.

3. **Analysis & Validation:**  
   - Produce validation reports (duplicates, missing records, suspicious matches).
   - Validate dataset integrity and continuous ID sequences.
   - Maintain data dictionaries and track transformations accurately.

4. **Maintainable Codebase Structure:**  
   - Achieve a DRY (Don’t Repeat Yourself) approach by using a common base processor class.
   - Segregate responsibilities into `preprocessing`, `matching`, and `analysis` directories.
   - Build robust logging, error handling, and inline documentation to facilitate future maintenance.

## Milestones & High-Level Timeline

| Milestone | Description | Estimated Completion |
|-----------|-------------|---------------------|
| M1: Data Loading & Preprocessing  | Finish integrating `nyc_gov_hoo.csv` and `ops_data.csv` processors; ensure standardized schemas | Week 1 |
| M2: Enhanced Normalization Rules  | Apply advanced normalization (NYC references, abbreviations, mayor’s office formatting) | Week 2 |
| M3: Fuzzy Matching & Threshold Tuning | Refine similarity scoring (Levenshtein/Jaro-Winkler), document thresholds, and run initial match tests | Week 3 |
| M4: Deduplication & Provenance Tracking | Deduplicate integrated dataset, retain `RecordID` and source columns; perform final merges | Week 4 |
| M5: Analysis & Reporting  | Generate final validation reports, missing record analyses, and confirm final dataset integrity | Week 5 |
| M6: Schema & Data Dictionary Updates | Finalize schema adjustments, update `data_dictionary.csv`, and ensure continuous CI testing | Week 6 |

## Detailed Tasks

### Data Preprocessing (Weeks 1–2)
- **Task A1:** Update `ops_processor.py` and `hoo_processor.py` to fully normalize agency names.
- **Task A2:** Implement common cleaning utilities in `base_processor.py` to reduce code duplication.
- **Task A3:** Confirm `NameNormalized` fields are consistently generated for all sources.

### Matching & Deduplication (Weeks 2–4)
- **Task B1:** Integrate `NameNormalizer` and `EnhancedMatcher` in `matcher.py` to handle edge cases.
- **Task B2:** Apply a configurable similarity threshold (e.g., 82.0) in `string_similarity.py` and `string_matching.py`.
- **Task B3:** Deduplicate final dataset in `data_merging.py` ensuring no record inflation and preserving provenance fields.

### Analysis & Validation (Weeks 4–5)
- **Task C1:** Use `match_analyzer.py`, `dataset_validator.py`, and `match_validator.py` to identify duplicate pairs, missing IDs, suspicious matches.
- **Task C2:** Generate `missing_records_analysis.csv` and `match_issues_analysis.csv` reports.
- **Task C3:** Document analysis results and update `data/analysis/` with final validation metrics.

### Schema & Documentation Updates (Weeks 5–6)
- **Task D1:** Update `data_dictionary.csv` to reflect new fields and transformations in `schema.py`.
- **Task D2:** Improve inline documentation and docstrings in all processors, matchers, and analysis scripts.
- **Task D3:** Create or update READMEs in `src/preprocessing/`, `src/matching/`, and `src/analysis/` directories.

## Script Consolidation Plans
- **Consolidate Preprocessing Logic:** Ensure all source-specific cleaning methods derive from `base_processor.py` to avoid duplication. Source-specific files (`ops_processor.py`, `hoo_processor.py`) should only implement unique logic.
- **Fuzzy Matching Modularization:** Move fuzzy matching logic into well-defined functions within `matcher.py` and `string_similarity.py`, enabling easy tuning and testing.
- **Analysis Scripts Streamlining:** Each analysis script should handle one reporting function. For example, `match_analyzer.py` only for match scoring checks, `dataset_validator.py` only for final schema checks.

## Testing & Quality Assurance
- Run `test_pipeline.py` regularly to ensure that recent changes have not introduced regressions.
- Add unit tests for normalization and fuzzy matching utilities.
- Conduct periodic reviews to ensure all transformations are documented in `data_dictionary.csv`.

## Communication & Review
- Weekly progress reviews to confirm milestone completion and to adjust timeline if necessary.
- Stakeholder demos after completing critical phases (e.g., after M3 to show improved match accuracy).

## Final Deliverables
- A deduplicated, standardized dataset with proper `RecordID` fields.
- A robust codebase with clear documentation, logging, and error handling.
- Comprehensive analysis reports and a maintained `data_dictionary.csv`.