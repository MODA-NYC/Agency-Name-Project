# NYC Agency Name Standardization Project

## Overview

This project’s ultimate goal is to create a single, standardized, and authoritative dataset of New York City government organization names. “Agencies,” as referred to here, include not only formal city agencies but also mayoral offices, commissions, and other city government entities. The standardized dataset will improve data quality and interoperability across various NYC data resources and help ensure consistent naming in official datasets published on the NYC Open Data portal.

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
  ODA (Office of Data and Analytics) internal lists, Chief Privacy Officer’s sanitized agency list, and the WeGov compiled list.

During Phase 2, we are incorporating:

- `nyc_gov_hoo.csv` (NYC Mayor’s Office "Head of Organization" data)
- `ops_data.csv` (Operational perspective data from NYC agencies)

**Note:** The integration of these two new sources into the standardized dataset is still underway.

## Methodology

1. **Initial Aggregation & Standardization (Phase 1):**  
   - Collected and combined multiple datasets into one master list.
   - Implemented basic name normalization and removal of duplicates.
   - Produced `nyc_agencies_export.csv` as an initial standardized output.

2. **Refactoring & Enhanced Matching (Phase 2):**  
   - Transitioning from ad hoc notebook code to a structured, modular codebase.
   - Developing improved normalization and fuzzy matching logic (e.g., handling “Department of X” vs. “X Department”).
   - Incorporating `nyc_gov_hoo.csv` and `ops_data.csv` into the pipeline.
   - Planning to produce a fully deduplicated dataset that aligns with DCAT-US-3 standards, including acronyms, alternate names, and organizational hierarchies.

3. **Future Maintenance & Publishing:**  
   - Once Phase 2 is complete, publish the fully standardized dataset on NYC Open Data.
   - Document procedures for ongoing maintenance (adding/removing agencies, updating titles, etc.).
   - Consider creating a crosswalk for different name formats and ensure current commissioner, URL, and other details are up-to-date.

## Code Structure (Ongoing Refactoring)

The code is being refactored into a modular structure:

```txt
src/
  preprocessing/     # Data cleaning & normalization scripts
  matching/          # Fuzzy matching, NYC-specific normalization, and deduplication logic
  analysis/          # Quality checks, validation, and diagnostic reports
  main.py            # Orchestrates the entire workflow
data/
  raw/               # Original input files (including nyc_gov_hoo.csv, ops_data.csv)
  intermediate/      # Merged intermediate results
  processed/         # Final standardized datasets (including nyc_agencies_export.csv)
  analysis/          # Outputs from validation and quality checks
docs/
  data_dictionary.csv # Definitions for fields and formats


Key Files:
main.py: Entry point to run the entire pipeline.
preprocessing/*.py: Scripts to clean, normalize, and prepare raw data sources.
matching/*.py: Implements name matching, fuzzy logic, and canonicalization of agency names.
analysis/*.py: Validates results, generates reports on duplicates, missing records, and match quality.
Getting Started
Install Dependencies:
bash
Copy code
pip install -r requirements.txt


Run the Pipeline (Current Status):
Since the refactor is still in progress, exact steps may vary. Typically:
bash
Copy code
cd src
python main.py --data-dir ../data --save
Adjust paths as needed. Note that the final outputs and new datasets (integrating nyc_gov_hoo.csv and ops_data.csv) are not yet complete.
Check Outputs:
Phase 1 Output: data/processed/nyc_agencies_export.csv
Work in Progress (Phase 2): Expect intermediate merged files in data/intermediate/.
Diagnostic reports under data/analysis/ once the analysis scripts are completed and run.
Future Plans
Additional Documentation:
Separate READMEs for each subdirectory (preprocessing/, matching/, analysis/) explaining the logic and usage of scripts in more detail.
Project Plan Document:
A separate PROJECT_PLAN.md (or similar) will outline remaining tasks, milestones, and a roadmap for completing the refactor and fully integrating the new sources.
Testing & Validation:
Introduce a robust test suite and CI/CD to ensure stability as changes are made.

In summary, this README reflects the overall project goals, the initial completed phase of work, and the ongoing refactoring and integration efforts. As the project progresses, additional documentation and project plan documents will provide more granular guidance.

