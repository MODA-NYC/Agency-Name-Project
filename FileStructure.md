## Project Structure

The project has the following file structure:

```
agency-name-project-main/
├── data/
│   ├── exports/
│   ├── intermediate/
│   │   └── merged_dataset.csv
│   ├── processed/
│   │   ├── .gitkeep
│   │   ├── consolidated_matches.csv
│   │   └── nyc_agencies_export.csv
│   └── raw/
│       ├── .gitkeep
│       ├── CPO Data.csv
│       ├── nyc_gov_hoo.csv
│       ├── ODA Data.csv
│       ├── ops_data.csv
│       └── WeGov Data.csv
├── docs/
│   ├── data_dictionary_old.csv
│   └── data_dictionary.csv
├── src/
│   ├── __init__.py
│   ├── data_loading.py
│   ├── data_merging.py
│   ├── data_normalization.py
│   ├── data_preprocessing.py
│   ├── main.py
│   ├── preprocess_nyc_gov_hoo.py
│   └── schema.py
├── .gitignore
├── analyze_merged_dataset.py
├── FileStructure.md
├── ProjectPlan.md
├── README.md
├── requirements.txt
└── TODO.md
```

When implementing new functions or modifying existing ones, please ensure they are placed in the appropriate files within the `src/` directory.