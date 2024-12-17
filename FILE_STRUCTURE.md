```md
Agency-Name-Project-main/
├── data/
│   ├── analysis/                    # Analysis outputs
│   │   ├── combined_records_analysis.csv
│   │   ├── duplicates_hoo.csv
│   │   ├── duplicates_merged_dataset.csv
│   │   ├── match_issues_analysis.csv
│   │   ├── missing_records_analysis.csv
│   │   ├── raw_duplicates_hoo.csv
│   │   └── raw_duplicates_ops.csv
│   ├── intermediate/               # Intermediate processing results
│   │   └── merged_dataset.csv
│   ├── processed/                  # Final and processed datasets
│   │   ├── consolidated_matches.csv
│   │   ├── dedup_data_with_normalized.csv
│   │   ├── final_deduplicated_dataset.csv
│   │   ├── missing_ops_records.csv
│   │   ├── nyc_agencies_export.csv
│   │   ├── ops_data_with_normalized.csv
│   │   └── potential_matches.csv
│   └── raw/                        # Original source data
│       ├── CPO Data.csv
│       ├── ODA Data.csv
│       ├── WeGov Data.csv
│       ├── nyc_gov_hoo.csv
│       └── ops_data.csv
├── docs/
│   └── data_dictionary.csv
├── project_plans_old/              # Old project documentation
│   ├── EntityMatchingPlan.md       # Previously focused plan
│   └── OriginalPlan.md             # Original broader plan
├── src/
│   ├── analysis/                   # Analysis tools
│   │   ├── dataset_validator.py
│   │   ├── match_analyzer.py
│   │   ├── match_quality_report.py
│   │   ├── match_validator.py
│   │   ├── name_transformation_analyzer.py
│   │   └── quality_checker.py
│   ├── matching/                   # Matching logic
│   │   ├── __init__.py
│   │   ├── enhanced_matching.py
│   │   ├── matcher.py             # Core matching algorithm
│   │   ├── normalizer.py          # Name normalization
│   │   ├── string_matching.py
│   │   └── string_similarity.py
│   ├── preprocessing/              # Data preprocessing
│   │   ├── __init__.py
│   │   ├── base_processor.py      # Base preprocessing class
│   │   ├── data_normalization.py
│   │   ├── data_preprocessing.py
│   │   ├── hoo_processor.py       # HOO-specific processing
│   │   ├── ops_processor.py       # OPS-specific processing
│   │   └── schema.py
│   ├── add_manual_matches.py
│   ├── analyze_combined_records.py # Analysis scripts
│   ├── analyze_match_issues.py
│   ├── analyze_merge_completeness.py
│   ├── analyze_missing_records.py
│   ├── analyze_match_rate.py
│   ├── analyze_record_counts.py
│   ├── create_entity_mapping.py
│   ├── data_loading.py            # Core data operations
│   ├── data_merging.py
│   ├── data_preparation.py
│   ├── find_missing_ops_records.py
│   ├── find_missing_records.py
│   ├── main.py                    # Main execution script
│   ├── match_generator.py
│   ├── matching_algorithm.py
│   ├── preprocess_nyc_gov_hoo.py
│   ├── test_pipeline.py
│   ├── verify_structure.py
└── FILE_STRUCTURE.md              # This file

## Key Components

### Analysis Tools
- **analyze_match_issues.py**: Identifies problematic entity combinations
- **analyze_combined_records.py**: Analyzes combined records
- **analyze_merge_completeness.py**: Checks for missing records
- **analyze_record_counts.py**: Validates record counts
- **name_transformation_analyzer.py**: Analyzes name transformations
- **quality_checker.py**: Checks data quality
- **dataset_validator.py**: Validates dataset structure
- **match_validator.py**: Validates matches

### Core Processing
- **matcher.py**: Entity matching logic
- **normalizer.py**: Name normalization rules
- **data_merging.py**: Data merging operations
- **enhanced_matching.py**: Advanced matching algorithms
- **string_matching.py**: String similarity functions
- **string_similarity.py**: Additional similarity scoring methods

### Data Flow
1. Raw data loaded from `/data/raw`
2. Processed through source-specific processors (e.g., `ops_processor.py`, `hoo_processor.py`)
3. Merged into intermediate dataset
4. Matches identified and applied
5. Final deduplicated dataset produced

### Old Project Plans
- **EntityMatchingPlan.md**: Previously focused matching plan
- **OriginalPlan.md**: Original broader plan documentation