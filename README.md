# NYC Agency Name Project

## Project Overview

This project aims to create a standardized list of Agency Names* and publish this as a dataset on the NYC Open Data portal. The primary goal is to enhance data legibility and interoperability by providing official, consistently formatted agency names. This will provide a clear canonical source for how to format Agency Names, improving data quality and saving time when joining datasets on the Agency Name field.

This project is being developed by the Data Governance team in the Office of Data and Analytics.

*The word “Agency” is colloquially used to mean a government organization that includes a New York City Agency, a Mayoral Office, or a Commission.

Original Project Plan document: [NYC Agency Name Project](https://docs.google.com/document/d/1u9-sZXUWdand1yIRmmKGbq7D5RAgD2puWoYvbP06a4g/edit?usp=sharing)
Code Notebook originally developed as a Google CoLab project: [Agency Name Project.ipynb](https://colab.research.google.com/drive/1BzU2_8sAOsIZWr_9fS5JaM2-MUrPt7eg?usp=sharing)

## Objectives

1. Collect all relevant lists of Agency names from various sources.
2. Standardize Agency Names: Develop a standardized list of agency names and acronyms.
3. Possibly publish a “crosswalk” between different common formats of Agency Names (e.g. alphabetized or not alphabetized, with or without acronym).
4. Publish a Standardized list to the Open Data portal.
5. Document a process for maintaining this data asset as new agencies are added or removed, or agencies change their names.

## Data Sources

- Agency list from https://www.nyc.gov/nyc-resources/agencies.page
- Agency list from https://www.nyc.gov/office-of-the-mayor/admin-officials.page
- Agency list from https://opendata.cityofnewyork.us/data/
- Checkbook https://www.checkbooknyc.com/agency_codes/newwindow
- Geenbook https://data.cityofnewyork.us/resource/mdcw-n682.json
- Office of Data and Analytics (ODA) List
- Chief Privacy Officer List, with sensitive information removed.
- WeGov compiled list of NYC agency/organization names: https://airtable.com/appBU3zLf0ORYqKjk/shr1wInsFi70OU8qj
- NYC 2022 Bill Drafting Manual, Appendix D, TITLES OF SELECT CITY AGENCIES AND AGENCY HEADS https://council.nyc.gov/legislation/wp-content/uploads/sites/55/2023/03/NYC-Bill-Drafting-Manual-2022-FINAL.pdf
- December 2023 Org Chart (https://www.nyc.gov/assets/home/downloads/pdf/office-of-the-mayor/misc/NYC-Organizational-Chart.pdf)

## Methodology

1. Data Collection: Aggregate data from the aforementioned sources.
2. Data Standardization:
  - Create several columns for each agency name in various formats (e.g., with and without acronyms, alphabetized).
  - Deduplicate names to create a list of unique agency names.
  - Manually evaluate outliers.
3. Classification:
  - Classify each entity as an "Organization" per the DCAT-US-3 standard.
  - Assign "Organization Type" (e.g., City Agency, Mayoral Office, Commission).
  - Add alternate names and legal authorizing authority as needed.
4. Additional Information:
  - Include fields for the current commissioner, website URL, and other relevant details.
  - Ensure all field names align with DCAT-US-3 standards, adding extensions where necessary.

## Field Names and Definitions and Field Values Definitions

See Data Dictionary.

## Maintenance Plan

- Responsibility: The dataset will be maintained by ODA.
- Update Process: Document and establish a procedure for adding, removing, or modifying organization names in the dataset.

## Stakeholder Engagement

- Internal Collaboration: Engage with NYC government employees for verification and feedback where needed.
- Community Engagement: Update and involve the BetaNYC community, soliciting feedback on project plans and output formats.
