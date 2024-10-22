Project Plan: Probabilistic Matching and Deduplication of Merged Dataset


Objective


Implement a probabilistic matching process to identify and consolidate duplicate records in merged_dataset.csv. The goal is to generate a list of potential matches for manual confirmation and then deduplicate the dataset based on these confirmations.


Overview


After merging nyc_agencies_export.csv, nyc_gov_hoo.csv, and ops_data.csv into merged_dataset.csv, we've identified duplicate entries with slight variations in their names and attributes. To address this, we'll implement a probabilistic matching algorithm to find probable duplicates, compile them for manual review in consolidated_matches.csv, and then deduplicate the merged dataset accordingly.


Plan of Action


1. Review the Merged Dataset


Action: Examine merged_dataset.csv to understand the nature and extent of duplicates.


Goal: Identify key fields that can be used for matching and understand the variations in duplicate entries.


2. Identify Key Matching Fields


Action: Determine which fields are most suitable for matching. Potential fields include:


Name


NameNormalized


Acronym


AlternateNames


AlternateAcronyms


Name - NYC.gov Redesign


HeadOfOrganizationName


Goal: Select fields that are most indicative of record identity to improve matching accuracy.


3. Select a Probabilistic Matching Algorithm


Action: Research and choose appropriate string similarity algorithms such as:


Levenshtein Distance


Jaro-Winkler Similarity


FuzzyWuzzy (Token Sort Ratio, Token Set Ratio)


RapidFuzz library for efficient computation


Goal: Pick an algorithm that effectively handles slight variations and typos.


4. Implement the Matching Script


Action:


Create a new script (e.g., probabilistic_matching.py).


Read merged_dataset.csv into a DataFrame.


Generate pairwise comparisons of records based on the selected fields.


Compute similarity scores using the chosen algorithm.


Goal: Calculate similarity scores for potential duplicate pairs.


5. Set a Similarity Threshold


Action:


Analyze similarity scores to determine an appropriate threshold (e.g., 85%).


Test different thresholds to balance between false positives and false negatives.


Goal: Establish a threshold that identifies most true duplicates without overwhelming manual review.


6. Generate Potential Matches


Action:


Filter pairs exceeding the similarity threshold.


Create a DataFrame or CSV file (potential_matches.csv) with columns:


RecordID_1


RecordID_2


Name_1


Name_2


Field_Compared


Similarity_Score


Goal: Compile a list of probable duplicates for manual confirmation.


7. Prepare consolidated_matches.csv for Manual Review


Action:


Ensure consolidated_matches.csv exists with columns:


RecordID_1


RecordID_2


Confirmed_Match (Yes/No)


Comments (optional)


Initially populate it with pairs from potential_matches.csv.


Goal: Set up a file to document manual confirmations.


8. Conduct Manual Confirmation


Action:


Review each pair in potential_matches.csv.


For each pair, determine if they are duplicates.


Update consolidated_matches.csv with confirmations.


Goal: Create an authoritative list of confirmed duplicates.


9. Update the Deduplication Script


Action:


Modify main.py or create a new script (e.g., deduplicate_dataset.py).


Read merged_dataset.csv and consolidated_matches.csv.


For confirmed matches:


Merge records according to predefined rules.


Remove duplicate entries.


Goal: Produce a clean, deduplicated dataset.


10. Define Merging Rules


Action:


Establish rules for merging fields, such as:


Prefer non-null over null values.


Use the most recently updated record.


Combine lists of alternate names or acronyms.


Goal: Ensure consistent and accurate data consolidation.


11. Validate the Deduplicated Dataset


Action:


Perform quality checks on the deduplicated dataset.


Spot-check records to ensure correct merging.


Verify that no unique records were erroneously removed.


Goal: Confirm the integrity and accuracy of the deduplicated data.


12. Document the Process and Findings


Action:


Update ProjectPlan.md with:


Details of the algorithms used.


Thresholds selected and rationale.


Any challenges and solutions encountered.


Note any insights or patterns observed during manual review.


Goal: Maintain comprehensive documentation for transparency and future reference.


Additional Considerations


Performance Optimization:


Pairwise comparisons can be computationally intensive.


Implement blocking techniques to reduce comparisons (e.g., only compare records with the same first letter).


Data Storage:


Ensure that all intermediate files are saved appropriately.


Use version control (e.g., Git) to track changes to scripts.


Future Updates:


Consider automating the matching process for new data additions.


Explore machine learning approaches for improved matching accuracy over time.


Next Steps


Begin implementing the matching script as outlined.


Schedule time for manual review and confirmation.


Plan for potential updates or iterations based on initial outcomes.
