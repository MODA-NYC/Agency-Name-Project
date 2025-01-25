# Step 1.1 - OPS Processor Flow

## Entry Point (main.py)
```
main.py
├── Initialize OpsDataProcessor
├── Validate input files exist
│   └── Check 'data/raw/ops_data.csv'
├── Process OPS data
│   └── Call ops_processor.process()
└── Validate output columns
    └── Check ['Agency Name', 'NameNormalized', 'RecordID']
```

## OPS Processing Flow (ops_processor.py)
```
OpsDataProcessor.process()
├── Load Data
│   └── pd.read_csv('ops_data.csv')
│
├── Check Raw Duplicates
│   ├── Call check_raw_duplicates(['Agency Name'])
│   ├── Find 10 duplicate records (5 pairs)
│   └── Save to 'data/analysis/raw_duplicates_ops.csv'
│
├── Handle Known Duplicates
│   ├── Rules from self.known_duplicates:
│   │   ├── 'Gracie Mansion Conservancy': keep_first
│   │   ├── 'Commission on Gender Equity': keep_latest
│   │   ├── 'Commission on Racial Equity': keep_first
│   │   ├── 'Hudson Yards Infrastructure Corp': keep_first
│   │   └── 'Financial Information Services Agency': keep_first
│   └── Apply rules to remove 5 duplicate records
│
├── Create NameNormalized
│   └── Apply standardize_name() to 'Agency Name'
│       ├── Convert to lowercase
│       ├── Expand 'NYC' to 'new york city'
│       ├── Replace '&' with 'and'
│       ├── Remove parentheses and contents
│       ├── Remove punctuation
│       ├── Remove extra whitespace
│       ├── Expand abbreviations
│       └── Remove minimal stopwords (excluding 'of')
│
└── Add RecordID
    └── Generate 'OPS_XXXXXX' format if missing
```

## Data Flow
```
Input (418 records)
└── ops_data.csv
    ├── Agency Acronym
    ├── Agency Name
    ├── Entity type
    ├── MMR chapter
    ├── Rulemaking Entity
    ├── Agency Head First Name
    ├── Agency Head Last Name
    ├── Reports to Deputy Mayor/Executive
    └── Agency/Board Website

Output (413 records)
└── Processed DataFrame
    ├── [Original columns...]
    ├── NameNormalized (standardized names)
    └── RecordID (OPS_XXXXXX format)
```

## Key Files
1. `main.py`: Orchestrates the process
2. `src/preprocessing/ops_processor.py`: OPS-specific logic
3. `src/preprocessing/base_processor.py`: Base processing functionality
4. `src/preprocessing/data_normalization.py`: Name standardization logic

## Output Files
1. `data/analysis/raw_duplicates_ops.csv`: List of found duplicates
2. Processed DataFrame (413 records) passed to next pipeline stage 