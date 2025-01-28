# Step 1.2 - HOO Processor Flow

## Entry Point (main.py)
```
main.py
├── Initialize HooDataProcessor
├── Validate input files exist
│   └── Check 'data/raw/nyc_gov_hoo.csv'
├── Process HOO data
│   └── Call hoo_processor.process()
└── Validate output columns
    └── Check ['Agency Name', 'NameNormalized', 'RecordID']
```

## HOO Processing Flow (hoo_processor.py)
```
HooDataProcessor.process()
├── Load Data
│   └── pd.read_csv('nyc_gov_hoo.csv')
│
├── Strip whitespace from column names
│   └── df.columns = df.columns.str.strip()
│
├── Check Raw Duplicates
│   ├── Call check_raw_duplicates(['Agency Name'])
│   ├── Find 19 duplicate records
│   └── Save to 'data/analysis/raw_duplicates_hoo.csv'
│
├── Handle Known Duplicates
│   ├── Rules from self.known_duplicates:
│   │   ├── "Mayor's Office": keep_first
│   │   ├── 'Office of the Mayor': keep_latest
│   │   ├── 'Department of Social Services': keep_first
│   │   ├── 'Human Resources Administration': keep_latest
│   │   ├── 'Department of Homeless Services': keep_latest
│   │   ├── 'NYC Health + Hospitals': keep_first
│   │   └── 'Health and Hospitals Corporation': keep_latest
│   └── Apply rules to remove duplicate records
│
├── Rename Columns (Column Mappings)
│   ├── 'Head of Organization' → 'HeadOfOrganizationName'
│   ├── 'HoO Title' → 'HeadOfOrganizationTitle'
│   ├── 'Agency Link (URL)' → 'HeadOfOrganizationURL'
│   └── 'Agency Name' → 'Agency Name' (unchanged)
│
├── Create AgencyNameEnriched
│   ├── IF HeadOfOrganizationTitle exists:
│   │   └── Combine: "Agency Name - HeadOfOrganizationTitle"
│   └── ELSE:
│       └── Use Agency Name as is
│
├── Create NameNormalized
│   └── Apply standardize_name() to 'AgencyNameEnriched'
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
    └── Generate 'HOO_XXXXXX' format if missing
```

## Data Flow
```
Input (Raw Data)
└── nyc_gov_hoo.csv
    ├── Agency Name
    ├── Head of Organization
    ├── HoO Title
    ├── Agency Link (URL)
    └── [Other columns...]

Output (Processed DataFrame)
├── Original columns (renamed):
│   ├── HeadOfOrganizationName
│   ├── HeadOfOrganizationTitle
│   └── HeadOfOrganizationURL
├── Enhanced columns:
│   ├── AgencyNameEnriched (Agency Name + Title)
│   ├── NameNormalized (standardized names)
│   └── RecordID (HOO_XXXXXX format)
```

## Key Files
1. `main.py`: Orchestrates the process
2. `src/preprocessing/hoo_processor.py`: HOO-specific logic
3. `src/preprocessing/base_processor.py`: Base processing functionality
4. `src/preprocessing/data_normalization.py`: Name standardization logic

## Output Files
1. `data/analysis/raw_duplicates_hoo.csv`: List of found duplicates
2. Processed DataFrame passed to next pipeline stage 