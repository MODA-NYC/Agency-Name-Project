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
  - Successfully implemented source-specific name preservation in `merged_dataset.csv`.
  - Created centralized pipeline orchestration through `main.py`.
  - Added NYC.gov agency list scraping script to gather additional agency information.
- Challenges Discovered:
  - Certain data sources (e.g., HOO data) rely on multiple fields (Agency Name, HoO Title) to distinguish unique entities.
  - Using Agency Name alone leads to ambiguous merges and loss of detail.
  - Relying solely on one column for normalization and deduplication may collapse distinct entities.

## Project Plan

### Pipeline Orchestration

The project uses `main.py` as the central orchestrator for all data processing steps. This script:
1. Coordinates the execution of all preprocessing, merging, and analysis tasks
2. Maintains consistent logging and error handling
3. Generates intermediate and final outputs in the appropriate directories
4. Ensures proper sequencing of data transformation steps

### Data Processing Pipeline

1. **Source Processing & Normalization** (Orchestrated by `main.py`)
   - Process each source through dedicated processors
   - Generate normalized intermediate files
   - Track processing statistics and data quality metrics

2. **Merging & Integration** (Orchestrated by `main.py`)
   - Combine processed sources into `merged_dataset.csv`
   - Preserve source-specific names in dedicated columns
   - Track data provenance and merge decisions

3. **Matching & Deduplication** (Future work)
   - Use `merged_dataset.csv` as input for fuzzy matching
   - Generate and verify potential matches
   - Produce final deduplicated dataset

## 1. Preprocessing & Source-Level Normalization

**Objective**: Convert raw CSV data into consistently normalized DataFrames, each with a clear `RecordID`, a normalized name column (`NameNormalized`), and any necessary source-specific fields.

1. **OPS Processor** (`src/preprocessing/ops_processor.py`)  
   - Load `ops_data.csv` from `data/raw/`.
   - Deduplicate, handle known duplicates (if any remain).
   - Normalize names (`NameNormalized`) with relevant expansions/abbreviations.

2. **HOO Processor** (`src/preprocessing/hoo_processor.py`)  
   - Load `nyc_gov_hoo.csv` from `data/raw/`.
   - Apply `AgencyNameEnriched` logic (e.g., combine AgencyName + HoO Title).
   - Deduplicate, handle known duplicates.
   - Normalize `NameNormalized` using source-specific expansions.

3. **Global Normalization** (`src/preprocessing/global_normalization.py`)  
   - Once each source is processed, optionally run a final "global" pass to ensure consistent `NameNormalized` across all sources.  
   - Add or refine expansions for NYC-specific terms not fully handled in each processor.

## 2. Merging & Integration

**Objective**: Combine the preprocessed source DataFrames into a single master dataset.

1. **Initial Merge** (`src/data_merging.py`)  
   - Load the primary dataset (e.g., `nyc_agencies_export.csv`) plus each processed DataFrame
   - Use consistent field mappings and ensure each record has a `RecordID`
   - Generate `merged_dataset.csv` with:
     - Source-specific name columns (`Name - Ops`, `Name - HOO`)
     - Clear data provenance tracking
     - Preserved original names from each source
   - Store in `data/intermediate/` for subsequent processing

2. **Clean & Deduplicate**  
   - Eliminate empty rows, handle column collisions
   - Remove duplicates based on `RecordID`
   - Prepare dataset for fuzzy matching phase

## 3. Matching & Deduplication

**Objective**: Identify and merge matching records using fuzzy matching and specialized NYC agency logic, with manual verification.

### 3.1 Generate Potential Matches
1. **Setup**
   - Load deduplicated dataset from Step 2.2
   - Initialize enhanced matcher with NYC-specific rules
   - Configure similarity threshold (82.0)

2. **Match Generation**
   - Use `AgencyMatcher` to compute similarity scores
   - Filter matches above threshold
   - Auto-label perfect matches (score = 100)
   - Store in `consolidated_matches.csv`

3. **Manual Review**
   - Review generated matches
   - Apply labels ("Match" or "No Match")
   - Document any special cases or patterns

### 3.2 Apply Verified Matches

**Objective**: Apply manually verified matches from `consolidated_matches.csv` to produce a final, fully deduplicated dataset that **preserves source-specific columns** (e.g., `Name - Ops` and `Name - NYC.gov Redesign`).

1. **Load & Filter**
   - Load deduplicated dataset (output of Step 2.2).
   - Load verified matches from `consolidated_matches.csv`.
   - Filter for "Match" labels.

2. **Process Matches**
   - Sort matches by score (descending) or follow user-labeled order.
   - For each verified match:
     - Attempt a direct RecordID-based match first (if both sides have valid IDs).
     - If no direct ID match, attempt name or fuzzy matching on the final dataset.
     - **Preserve `Name - Ops` and `Name - HOO` Fields**:
       - Retrieve non-null values from both matched records.
       - If multiple distinct values exist for either field, combine them (e.g., comma-separated).
       - If only one record has a value, copy it over to the merged record.
       - Ensure the final merged record retains all relevant `Name - Ops` and `Name - HOO` data.
     - Apply source preference rules (e.g., prefer `nyc_agencies_export` over `ops` over `nyc_gov` if conflicts arise).
     - Merge record information (including other columns) and preserve non-null values.
     - Update provenance and metadata to indicate which fields came from each record.

3. **Generate Output**
   - Save the final merged (deduplicated) dataset to a new file (e.g., `final_deduplicated_dataset.csv`).
   - Each record should now have:
     - A single `RecordID`.
     - A unified `NameNormalized`.
     - Possibly combined (or unioned) `Name - Ops` and `Name - NYC.gov Redesign` fields from all matched sources.
   - Maintain an audit trail that shows how each record was formed.

4. **Validate Results**
   - Verify match application:
     - Check that each pair labeled "Match" was merged correctly.
   - Check remaining duplicates:
     - Confirm no unintended additional duplicates remain.
   - Validate information preservation:
     - Confirm that `Name - Ops` and `Name - NYC.gov Redesign` fields are populated where applicable.
     - Ensure no critical source data was overwritten or lost during the merge.

### 3.3 Quality Assurance
1. **Completeness Check**
   - Verify all required fields
   - Check for missing records
   - Validate metadata integrity

2. **Consistency Check**
   - Review source preferences
   - Verify merge decisions
   - Validate field values

3. **Documentation**
   - Update data dictionary
   - Document merge decisions
   - Note special cases

4. **Final Review**
   - Stakeholder review
   - Address any concerns
   - Prepare for publication

## 4. Analysis & Validation

**Objective**: Perform post-merge QA checks to ensure data completeness, identify missing records, duplicates, or suspicious merges.

1. **Analysis Scripts** (`src/analysis/*.py`)  
   - **`dataset_validator.py`**: Validate final deduplicated dataset for missing IDs, required fields, etc.  
   - **`match_validator.py`**: Check `consolidated_matches.csv` for duplicates, suspicious matches.  
   - **`quality_checker.py`**: Summaries of data quality (null counts, duplicates, etc.).  
   - Potential additional scripts for verifying record counts, analyzing unmatched entities, or generating summary reports.

2. **Testing** (`src/test_pipeline.py`)  
   - Run the test suite to confirm that all processors, matchers, and merges function as expected.  
   - If needed, unify structural checks (like `verify_structure.py`) or simply remove them.

## 5. Final Output & Publishing

- **Finalize**: Produce `final_deduplicated_dataset.csv` once all matching and QA steps are complete.  
- **Document**: Update `data_dictionary.csv` with any new fields or changes.  
- **Publish**: Provide the final dataset to NYC Open Data or internal consumers.

## 6. Next Steps & Maintenance

1. **Enrichment of Additional Sources**  
   - If more data sources appear in the future, replicate the pattern: write a `*_processor.py`, produce normalized output, merge, match, and analyze.

2. **Automate & Schedule**  
   - Wrap the entire pipeline (`main.py`) into a scheduled job or CI pipeline, ensuring periodic updates.

3. **Ongoing QA**  
   - Plan for periodic reviews to address new name variants, updated acronyms, or new agencies.

## Milestones & High-Level Timeline

| Milestone | Description | Estimated Completion |
|-----------|-------------|---------------------|
| M1: Data Loading & Preprocessing  | Finish integrating `nyc_gov_hoo.csv` and `ops_data.csv` processors; ensure standardized schemas | Week 1 |
| M2: Enhanced Normalization Rules  | Apply advanced normalization (NYC references, abbreviations, mayor's office formatting) | Week 2 |
| M3: Fuzzy Matching & Threshold Tuning | Refine similarity scoring (Levenshtein/Jaro-Winkler), document thresholds, and run initial match tests | Week 3 |
| M4: Deduplication & Provenance Tracking | Deduplicate integrated dataset, retain `RecordID` and source columns; perform final merges | Week 4 |
| M5: Analysis & Reporting  | Generate final validation reports, missing record analyses, and confirm final dataset integrity | Week 5 |
| M6: Schema & Data Dictionary Updates | Finalize schema adjustments, update `data_dictionary.csv`, and ensure continuous CI testing | Week 6 |
| M7: Final Cleanup & ID Assignment | Implement final cleanup phase, assign stable unique IDs, and prepare for future systematic ID assignment | Week 7 |

## Detailed Tasks

### Data Preprocessing (Weeks 1–2)
- **Task A1:** Integrate `nyc_gov_hoo.csv` and `ops_data.csv` processors.
- **Task A2:** Ensure standardized schemas for all processed sources.

### Matching & Deduplication (Weeks 2–4)
- **Task B1:** Refine similarity scoring (Levenshtein/Jaro-Winkler).
- **Task B2:** Document thresholds and run initial match tests.
- **Task B3:** Deduplicate integrated dataset, retain `RecordID` and source columns.
- **Task B4:** Perform final merges.

### Analysis & Validation (Weeks 4–5)
- **Task C1:** Generate final validation reports.
- **Task C2:** Analyze missing records and confirm final dataset integrity.

### Schema & Documentation Updates (Weeks 5–6)
- **Task D1:** Finalize schema adjustments.
- **Task D2:** Update `data_dictionary.csv`.
- **Task D3:** Ensure continuous CI testing.

### Final Cleanup & ID Assignment (Week 7)
- **Task E1:** Implement final deduplication pass in `cleanup_matches.py` to resolve remaining duplicates and naming conflicts.
- **Task E2:** Create new ID assignment logic in `assign_final_ids.py` to generate stable "FINAL_REC_XXXXXX" IDs.
- **Task E3:** Update export process to use new IDs and document ID assignment methodology.
- **Task E4:** Create crosswalk between original source IDs and new final IDs for traceability.
- **Task E5:** Document limitations of current ID system and requirements for future systematic ID assignment.
- **Task E6:** Implement manual merge process in `phase2.6_manual_merge.py` to handle special cases requiring manual intervention, including the ability to add new records via the manual_overrides.csv file.
- **Task E7:** Enhance `phase2.6_manual_merge.py` to add the NYC.gov Agency Directory flag column based on operational status, organization type, URL, and principal officer criteria.

## Script Consolidation Plans
- **Plan P1:** Consolidate all scripts into a single main.py file for easier management and execution.
- **Plan P2:** Implement a modular architecture for easy addition of new sources or changes to existing ones.