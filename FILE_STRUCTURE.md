Root Path: /Users/nathanstorey/Documents/Code/Agency-Name-Project-main
/Users/nathanstorey/Documents/Code/Agency-Name-Project-main
├── FILE_STRUCTURE.md
├── requirements.txt
├── PROJECTPLAN.md
├── PROJECT_INVENTORY.md
├── docs
├── └── data_dictionary.csv
├── .cursorrules
├── IMPROVEMENT_IDEAS.md
├── README.md
├── Data Flow Diagrams
├── ├── step_1_3_global_normalization.md
├── ├── step_2_2_initial_deduplication.md
├── ├── step_2_1_merging.md
├── ├── step_1_1_ops_processor.md
├── ├── Step_3.1_Potential_Match_Generation.md
├── ├── Step_3.2_Apply_Verified_Matches.md
├── └── step_1_2_hoo_processor.md
├── data
├── ├── analysis
├── ├── ├── merged_records_log.csv
├── ├── ├── dedup_summary.json
├── ├── └── verified_matches_summary.json
├── ├── intermediate
├── ├── └── merged_dataset.csv
├── ├── processed
├── ├── ├── consolidated_matches.csv
├── ├── ├── nyc_agencies_export.csv
├── ├── ├── global_normalized_dataset.csv
├── ├── └── final_deduplicated_dataset.csv
├── └── raw
├── └── ├── ops_data.csv
├── └── ├── WeGov Data.csv
├── └── ├── ODA Data.csv
├── └── ├── CPO Data.csv
├── └── └── nyc_gov_hoo.csv
├── .repo_ignore
└── src
└── ├── matching
└── ├── ├── matcher.py
└── ├── ├── __init__.py
└── ├── ├── string_similarity.py
└── ├── ├── string_matching.py
└── ├── ├── enhanced_matching.py
└── ├── └── normalizer.py
└── ├── fix_match_ids.py
└── ├── analysis
└── ├── ├── dataset_validator.py
└── ├── ├── analyze_merge_completeness.py
└── ├── ├── check_merge_completion.py
└── ├── ├── analyze_merge_counts.py
└── ├── ├── analyze_record_counts.py
└── ├── ├── match_validator.py
└── ├── ├── __init__.py
└── ├── ├── match_analyzer.py
└── ├── ├── quality_checker.py
└── ├── ├── analyze_merged_dataset.py
└── ├── ├── check_data.py
└── ├── ├── analyze_data_quality.py
└── ├── ├── match_quality_report.py
└── ├── └── name_transformation_analyzer.py
└── ├── data_loading.py
└── ├── tests
└── ├── ├── test_match_generation.py
└── ├── ├── __init__.py
└── ├── ├── test_global_norm.py
└── ├── ├── test_pipeline.py
└── ├── └── test_merging.py
└── ├── cleanup_matches.py
└── ├── add_manual_matches.py
└── ├── match_generator.py
└── ├── preprocessing
└── ├── ├── global_normalization.py
└── ├── ├── data_preprocessing.py
└── ├── ├── hoo_processor.py
└── ├── ├── data_normalization.py
└── ├── ├── __init__.py
└── ├── ├── base_processor.py
└── ├── ├── ops_processor.py
└── ├── └── schema.py
└── ├── data_preparation.py
└── ├── main.py
└── ├── apply_verified_matches.py
└── └── data_merging.py