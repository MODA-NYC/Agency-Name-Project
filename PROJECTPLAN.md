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

**Objective**: Combine the preprocessed source DataFrames into a single master `merged_dataset.csv`.

1. **Merging** (`src/data_merging.py`)  
   - Load the primary dataset (e.g., `nyc_agencies_export.csv`) plus each processed DataFrame.
   - Use consistent field mappings (e.g. `AgencyNameEnriched`) and ensure each record has a `RecordID`.
   - Concatenate them with provenance tracking (e.g., `source` column).

2. **Clean & Deduplicate**  
   - Eliminate empty rows, handle column collisions (URL fields, etc.), and remove duplicates based on `RecordID` or `NameNormalized` as appropriate.

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
1. **Load & Filter**
   - Load deduplicated dataset
   - Load verified matches from `consolidated_matches.csv`
   - Filter for "Match" labels

2. **Process Matches**
   - Apply source preference rules
   - Merge record information
   - Preserve non-null values
   - Update metadata and tracking

3. **Generate Output**
   - Save final deduplicated dataset
   - Create merge summary
   - Maintain audit trail

4. **Validate Results**
   - Verify match application
   - Check remaining duplicates
   - Validate information preservation

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