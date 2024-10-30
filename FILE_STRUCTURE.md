# Project File Structure

```
agency-name-project/
├── data/
│   ├── analysis/
│   │   ├── duplicates_hoo.csv
│   │   ├── raw_duplicates_hoo.csv
│   │   ├── raw_duplicates_ops.csv
│   │   └── raw_duplicates_ops.csv
│   ├── exports/
│   │   └── merged_dataset.csv
│   ├── intermediate/
│   │   └── merged_dataset.csv
│   ├── processed/
│   │   ├── consolidated_matches.csv
│   │   ├── dedup_data_with_normalized.csv
│   │   ├── final_deduplicated_dataset.csv
│   │   ├── missing_ops_records.csv
│   │   ├── nyc_agencies_export.csv
│   │   ├── ops_data_with_normalized.csv
│   │   └── potential_matches.csv
│   └── raw/
│       ├── CPO Data.csv
│       ├── nyc_gov_hoo.csv
│       ├── ODA Data.csv
│       ├── ops_data.csv
│       └── WeGov Data.csv
├── docs/
│   └── data_dictionary.csv
├── src/
│   ├── analysis/
│   │   ├── __init__.py
│   │   └── quality_checker.py
│   ├── matching/
│   │   ├── __init__.py
│   │   ├── matcher.py
│   │   └── normalizer.py
│   ├── preprocessing/
│   │   ├── __init__.py
│   │   ├── base_processor.py
│   │   ├── hoo_processor.py
│   │   └── ops_processor.py
│   ├── add_manual_matches.py
│   ├── analyze_data_quality.py
│   ├── analyze_merged_dataset.py
│   ├── data_loading.py
│   ├── data_merging.py
│   ├── data_normalization.py
│   ├── enhanced_matching.py
│   ├── find_missing_ops_records.py
│   ├── main.py
│   ├── preprocess_nyc_gov_hoo.py
│   ├── schema.py
│   └── test_pipeline.py
├── .cursorrules
├── .gitignore
├── FILE_STRUCTURE.md
├── ProjectPlan.md
└── requirements.txt
```

## Directory Structure Overview

### /data
- **analysis/**: Contains duplicate detection reports and data quality analysis
  - `duplicates_hoo.csv`: HOO dataset duplicate records
  - `raw_duplicates_*.csv`: Raw duplicate records from source datasets
- **exports/**: Final output datasets for external use
- **intermediate/**: Temporary processing results and merged datasets
- **processed/**: Cleaned and transformed datasets
  - `consolidated_matches.csv`: Confirmed matches between datasets
  - `final_deduplicated_dataset.csv`: Final deduplicated agency records
  - `nyc_agencies_export.csv`: Primary dataset
- **raw/**: Original unmodified input data files

### /docs
- Documentation including data dictionary

### /src
- **analysis/**: Data quality and analysis modules
  - `quality_checker.py`: Data quality validation
- **matching/**: Name matching and normalization logic
  - `matcher.py`: Core matching algorithm
  - `normalizer.py`: Name standardization rules
- **preprocessing/**: Data source-specific processors
  - `base_processor.py`: Common processing functionality
  - `hoo_processor.py`: HOO dataset processor
  - `ops_processor.py`: OPS dataset processor

## Data Flow
1. Raw data loaded from `/data/raw`
2. Processed by source-specific processors in `/src/preprocessing`
3. Merged and normalized in intermediate steps
4. Matches identified and confirmed
5. Final deduplicated dataset generated

## Key Files
- `main.py`: Primary execution script
- `data_merging.py`: Dataset merging logic
- `data_normalization.py`: Name standardization rules
- `test_pipeline.py`: Pipeline validation tests