# Project File Inventory

## Core Configuration Files
- `requirements.txt` - Python package dependencies and versions
- `PROJECTPLAN.md` - Project roadmap and implementation details
- `.cursorrules` - IDE-specific configuration
- `.repo_ignore` - Repository-specific ignore rules
- `IMPROVEMENT_IDEAS.md` - Documented suggestions for future enhancements
- `README.md` - Project overview and setup instructions
- `.github/dependabot.yml` - Dependabot configuration for security updates

## Documentation
- `docs/data_dictionary.csv` - Definitions and descriptions of data fields
- `Data Flow Diagrams/` - Step-by-step documentation of data processing pipeline
  - `step_1_1_ops_processor.md` - OPS data preprocessing workflow
  - `step_1_2_hoo_processor.md` - HOO data preprocessing workflow
  - `step_1_3_global_normalization.md` - Global name normalization process
  - `step_2_1_merging.md` - Dataset merging workflow
  - `step_2_2_initial_deduplication.md` - Initial deduplication process
  - `Step_3.1_Potential_Match_Generation.md` - Match generation workflow
  - `Step_3.2_Apply_Verified_Matches.md` - Verified match application process

## Source Code

### Core Pipeline
- `src/main.py` - Main pipeline orchestrator
- `src/data_merging.py` - Core data merging functionality
- `src/data_preparation.py` - Data preparation utilities
- `src/data_loading.py` - Data loading utilities

### Preprocessing
- `src/preprocessing/base_processor.py` - Base class for data processors
- `src/preprocessing/ops_processor.py` - OPS-specific data processor
- `src/preprocessing/hoo_processor.py` - HOO-specific data processor
- `src/preprocessing/global_normalization.py` - Global name normalization
- `src/preprocessing/data_preprocessing.py` - General preprocessing utilities
- `src/preprocessing/data_normalization.py` - Name normalization utilities
- `src/preprocessing/schema.py` - Data schema definitions

### Matching
- `src/matching/matcher.py` - Core matching functionality
- `src/matching/enhanced_matching.py` - Enhanced matching algorithms
- `src/matching/string_similarity.py` - String similarity functions
- `src/matching/string_matching.py` - String matching utilities
- `src/matching/normalizer.py` - Name normalization for matching

### Analysis
- `src/analysis/dataset_validator.py` - Dataset validation utilities
- `src/analysis/match_validator.py` - Match validation utilities
- `src/analysis/match_analyzer.py` - Match analysis tools
- `src/analysis/quality_checker.py` - Data quality checking
- `src/analysis/analyze_merged_dataset.py` - Merged dataset analysis
- `src/analysis/analyze_data_quality.py` - Data quality analysis
- `src/analysis/match_quality_report.py` - Match quality reporting
- `src/analysis/name_transformation_analyzer.py` - Name transformation analysis

### Testing
- `src/tests/test_match_generation.py` - Match generation tests
- `src/tests/test_global_norm.py` - Global normalization tests
- `src/tests/test_pipeline.py` - Pipeline integration tests
- `src/tests/test_merging.py` - Data merging tests

### Utility Scripts
- `src/fix_match_ids.py` - Fixes inconsistent match IDs
- `src/cleanup_matches.py` - Match cleanup utilities
- `src/add_manual_matches.py` - Manual match addition utility
- `src/match_generator.py` - Match generation orchestrator
- `src/apply_verified_matches.py` - Applies verified matches
- `src/create_initial_release.py` - Generates initial release dataset by removing specified fields and filtering records

## Data Files

### Raw Data
- `data/raw/ops_data.csv` - Original OPS dataset
- `data/raw/nyc_gov_hoo.csv` - Original HOO dataset
- `data/raw/WeGov Data.csv` - WeGov source data
- `data/raw/ODA Data.csv` - ODA source data
- `data/raw/CPO Data.csv` - CPO source data

### Intermediate Data
- `data/intermediate/merged_dataset.csv` - Combined dataset after merging

### Processed Data
- `data/processed/consolidated_matches.csv` - Current consolidated matches
- `data/processed/nyc_agencies_export.csv` - Final agency export
- `data/processed/global_normalized_dataset.csv` - Globally normalized dataset
- `data/processed/final_deduplicated_dataset.csv` - Final deduplicated output

### Analysis Data
- `data/analysis/verified_matches_summary.json` - Match verification summary
- `data/analysis/merged_records_log.csv` - Merge operation log
- `data/analysis/dedup_summary.json` - Deduplication summary 