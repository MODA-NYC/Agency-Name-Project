Root Path: /Users/nathanstorey/Documents/Code/Agency-Name-Project-main
/Users/nathanstorey/Documents/Code/Agency-Name-Project-main
├── FILE_STRUCTURE.md
├── requirements.txt
├── PROJECTPLAN.md
├── docs
├── └── data_dictionary.csv
├── .cursorrules
├── README.md
├── Data Flow Diagrams
├── ├── step_1_3_global_normalization.md
├── ├── step_2_1_merging.md
├── ├── step_1_1_ops_processor.md
├── ├── Step_3.1_Potential_Match_Generation.md
├── ├── Step_3.2_Apply_Verified_Matches.md
├── ├── step_2_2_deduplication.md
├── └── step_1_2_hoo_processor.md
├── data
├── ├── analysis
├── ├── ├── raw_duplicates_ops.csv
├── ├── ├── combined_records_analysis.csv
├── ├── ├── missing_records_analysis.csv
├── ├── ├── match_issues_analysis.csv
├── ├── ├── duplicates_hoo.csv
├── ├── ├── duplicates_merged_dataset.csv
├── ├── ├── verified_matches_summary.json
├── ├── └── raw_duplicates_hoo.csv
├── ├── intermediate
├── ├── ├── merged_dataset.csv
├── ├── └── dedup_merged_dataset.csv
├── ├── processed
├── ├── ├── consolidated_matches.csv
├── ├── ├── nyc_agencies_export.csv
├── ├── ├── potential_matches.csv
├── ├── ├── .gitkeep
├── ├── ├── global_normalized_dataset.csv
├── ├── ├── final_deduplicated_dataset.csv
├── ├── ├── dedup_data_with_normalized.csv
├── ├── ├── ops_data_with_normalized.csv
├── ├── └── missing_ops_records.csv
├── └── raw
├── └── ├── ops_data.csv
├── └── ├── WeGov Data.csv
├── └── ├── .gitkeep
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
└── ├── find_missing_records.py
└── ├── analysis
└── ├── ├── dataset_validator.py
└── ├── ├── match_validator.py
└── ├── ├── __init__.py
└── ├── ├── match_analyzer.py
└── ├── ├── quality_checker.py
└── ├── ├── analyze_merged_dataset.py
└── ├── ├── analyze_data_quality.py
└── ├── ├── match_quality_report.py
└── ├── └── name_transformation_analyzer.py
└── ├── analyze_merge_completeness.py
└── ├── check_merge_completion.py
└── ├── data_loading.py
└── ├── analyze_merge_counts.py
└── ├── analyze_record_counts.py
└── ├── preprocess_nyc_gov_hoo.py
└── ├── find_missing_ops_records.py
└── ├── analyze_match_rate.py
└── ├── analyze_missing_records.py
└── ├── generate_matches.py
└── ├── check_data.py
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
└── ├── analyze_combined_records.py
└── ├── analyze_unmatched_records.py
└── ├── analyze_match_issues.py
└── ├── data_preparation.py
└── ├── main.py
└── ├── apply_verified_matches.py
└── ├── data
└── ├── └── analysis
└── ├── └── ├── merged_records_log.csv
└── ├── └── └── dedup_summary.json
└── ├── test_global_norm.py
└── ├── test_pipeline.py
└── ├── test_merging.py
└── └── data_merging.py