# ⚠️ Archived — superseded by NYC Governance Organizations (NYC GO)
# Active code and data now live here: [[new repo link]](https://github.com/MODA-NYC/nyc-governance-organizations). This repo remains for historical context and prototypes.

# NYC Agency Name Standardization Project

## Overview

This project's ultimate goal is to create a single, standardized, and authoritative dataset of New York City government organization names. "Agencies," as referred to here, include not only formal city agencies but also mayoral offices, commissions, and other city government entities. The standardized dataset will improve data quality and interoperability across various NYC data resources and help ensure consistent naming in official datasets published on the NYC Open Data portal.

## Project History & Current Work

**Phase 1 (Completed in Google Colab):**  
- Integrated several data sources listed in the original README (e.g., NYC.gov Agencies page, NYC Open Data listings, Greenbook data, etc.) into a single combined dataset.
- Developed initial cleaning and normalization logic in a Jupyter/Colab notebook.
- Produced a preliminary standardized export called `nyc_agencies_export.csv`.

**Phase 2 (In Progress, Refactoring and New Sources):**  
- Introducing two additional data sources: `nyc_gov_hoo.csv` and `ops_data.csv`.
- Migrating from a single Colab notebook approach to a more modular Python codebase.
- Improving maintainability and clarity by reorganizing code into `src/` directories with separate modules for preprocessing, matching, and analysis.
- Enhancing name normalization and matching techniques to handle NYC-specific patterns and improve deduplication.
- Plans to strengthen testing, add detailed documentation, and finalize a maintenance process for ongoing updates.

**Phase 2.6 (Final Cleanup and Unique ID Assignment):**
- Implementing a final cleanup phase to remove remaining duplicates and resolve naming conflicts.
- Introducing stable, unique record IDs (format: "FINAL_REC_000001") at the export stage.
- Serving as an interim solution until a more systematic, source-level ID assignment mechanism is implemented.
- Ensuring data consistency and traceability while maintaining flexibility for future updates.

## Data Sources

The project draws from multiple NYC data sources. Initially integrated sources (Phase 1) included:

- NYC.gov Agencies:  
  [NYC Government Agencies Page](https://www.nyc.gov/nyc-resources/agencies.page)
  
- NYC Open Data Portal:  
  [NYC Open Data](https://opendata.cityofnewyork.us/data/)
  
- Greenbook:  
  [Greenbook (NYC Official Directory) Dataset](https://data.cityofnewyork.us/resource/mdcw-n682.json)
  
- Checkbook NYC:  
  [Checkbook NYC Agency Codes](https://www.checkbooknyc.com/agency_codes/newwindow)

- Additional Lists:  
  ODA (Office of Data and Analytics) internal lists, Chief Privacy Officer's sanitized agency list, and the WeGov compiled list.

During Phase 2, we are incorporating:

- `nyc_gov_hoo.csv` (NYC Mayor's Office "Head of Organization" data)
- `ops_data.csv` (Operational perspective data from NYC agencies)

**Note:** The integration of these two new sources into the standardized dataset is still underway.

## Methodology

1. **Initial Aggregation & Standardization (Phase 1):**  
   - Collected and combined multiple datasets into one master list.
   - Implemented basic name normalization and removal of duplicates.
   - Produced `nyc_agencies_export.csv` as an initial standardized output.

2. **Refactoring & Enhanced Matching (Phase 2):**  
   - Transitioning from ad hoc notebook code to a structured, modular codebase.
   - Developing improved normalization and fuzzy matching logic (e.g., handling "Department of X" vs. "X Department").
   - Incorporating `nyc_gov_hoo.csv`
