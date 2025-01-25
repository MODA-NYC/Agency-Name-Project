# Step 1.3 - Global Normalization Flow

## Entry Point (main.py)
```
main.py
├── Load processed DataFrames
│   ├── Load OPS processed data
│   └── Load HOO processed data
├── Apply global normalization
│   └── Call apply_global_normalization()
└── Validate output
    └── Check ['NameNormalized'] column consistency
```

## Global Normalization Flow (global_normalization.py)
```
apply_global_normalization()
├── Check for source column (priority order)
│   ├── Try 'AgencyNameEnriched'
│   ├── Try 'Name'
│   └── Try 'Agency Name'
│
├── Apply global_normalize_name() to source column
│   ├── Start with base standardization:
│   │   ├── Convert to lowercase
│   │   ├── Expand 'NYC' to 'new york city'
│   │   ├── Replace '&' with 'and'
│   │   ├── Remove parentheses and contents
│   │   ├── Remove punctuation
│   │   ├── Remove extra whitespace
│   │   ├── Expand base abbreviations
│   │   └── Remove minimal stopwords (excluding 'of')
│   │
│   └── Apply global refinements:
│       ├── Standardize organizational terms:
│       │   ├── dept → department
│       │   ├── comm → commission
│       │   ├── auth → authority
│       │   ├── admin → administration
│       │   ├── corp → corporation
│       │   ├── dev → development
│       │   ├── svcs/svc → service(s)
│       │   ├── tech → technology
│       │   ├── mgmt → management
│       │   └── ops → operations
│       │
│       ├── Handle special cases:
│       │   └── "mayors office" → "office of the mayor"
│       │
│       └── Final cleanup:
│           ├── Ensure 'new york city' is preserved
│           └── Remove any extra whitespace
│
└── Update NameNormalized column
    └── Overwrite with globally normalized values
```

## Data Flow
```
Input
├── OPS Processed Data (413 records)
│   ├── Original columns
│   ├── NameNormalized (source-specific)
│   └── RecordID (OPS_XXXXXX)
│
└── HOO Processed Data (179 records)
    ├── Original columns
    ├── AgencyNameEnriched
    ├── NameNormalized (source-specific)
    └── RecordID (HOO_XXXXXX)

Output
└── Globally Normalized DataFrames
    ├── All original columns preserved
    └── NameNormalized (globally consistent)
        Examples:
        ├── "NYC Dept of Tech Mgmt" → "new york city department of technology management"
        ├── "Mayors Office of Admin Svcs" → "office of the mayor of administration services"
        ├── "Corp Counsel" → "corporation counsel"
        ├── "Auth for Dev" → "authority development"
        └── "Comm on Human Rights" → "commission human rights"
```

## Key Files
1. `main.py`: Orchestrates the process
2. `src/preprocessing/global_normalization.py`: Global normalization logic
3. `src/preprocessing/data_normalization.py`: Core normalization functions

## Validation Points
1. Check that 'new york city' is preserved where appropriate
2. Verify consistent handling of common terms across sources
3. Ensure no information loss from source-specific normalization
4. Validate that key organizational terms are retained
5. Confirm special case handling (e.g., "mayors office" transformation)
6. Verify abbreviation expansion is consistent 