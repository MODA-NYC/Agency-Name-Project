import pandas as pd
import logging
import os

def add_manual_matches():
    # Set up logging with more visible output
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s'
    )
    
    # Print working directory and target file path
    current_dir = os.getcwd()
    target_file = os.path.join(current_dir, 'data/processed/consolidated_matches.csv')
    print(f"Current directory: {current_dir}")
    print(f"Target file: {target_file}")

    # Define new matches
    new_matches = [
        {
            'Source': 'transportation department',
            'Target': 'department transportation',
            'Score': 100,
            'Label': 'Match',
            'SourceID': None,
            'TargetID': None
        },
        {
            'Source': 'transitional finance authority',
            'Target': 'transitional finance authority new york city',
            'Score': 100,
            'Label': 'Match',
            'SourceID': None,
            'TargetID': None
        },
        {
            'Source': 'tax commission',
            'Target': 'tax commission new york city',
            'Score': 100,
            'Label': 'Match',
            'SourceID': None,
            'TargetID': None
        },
        {
            'Source': 'tax appeals tribunal',
            'Target': 'tax appeals tribunal new york city',
            'Score': 100,
            'Label': 'Match',
            'SourceID': None,
            'TargetID': None
        },
        {
            'Source': 'public schools new york city',
            'Target': 'education department',
            'Score': 100,
            'Label': 'Match',
            'SourceID': None,
            'TargetID': None
        },
        {
            'Source': 'education department',
            'Target': 'department education',
            'Score': 100,
            'Label': 'Match',
            'SourceID': None,
            'TargetID': None
        },
        {
            'Source': 'police department new york city',
            'Target': 'police department',
            'Score': 100,
            'Label': 'Match',
            'SourceID': None,
            'TargetID': None
        },
        {
            'Source': 'new york city sheriff',
            'Target': 'city sheriff',
            'Score': 100,
            'Label': 'Match',
            'SourceID': None,
            'TargetID': None
        },
        {
            'Source': 'fire department',
            'Target': 'fire department new york city',
            'Score': 100,
            'Label': 'Match',
            'SourceID': None,
            'TargetID': None
        },
        {
            'Source': 'department social services',
            'Target': 'department social services department homeless services',
            'Score': 100,
            'Label': 'Match',
            'SourceID': None,
            'TargetID': None
        },
        {
            'Source': 'department social services',
            'Target': 'department social services human resources administration',
            'Score': 100,
            'Label': 'Match',
            'SourceID': None,
            'TargetID': None
        },
        {
            'Source': 'city council',
            'Target': 'city council new york',
            'Score': 100,
            'Label': 'Match',
            'SourceID': None,
            'TargetID': None
        }
    ]

    print(f"Prepared {len(new_matches)} new matches to add")

    # Load existing matches
    try:
        existing_matches = pd.read_csv('data/processed/consolidated_matches.csv')
        print(f"Successfully loaded {len(existing_matches)} existing matches")
    except FileNotFoundError as e:
        print(f"Error: Could not find consolidated_matches.csv: {e}")
        print("Creating new matches file...")
        existing_matches = pd.DataFrame(columns=['Source', 'Target', 'Score', 'Label', 'SourceID', 'TargetID'])

    # Convert new matches to DataFrame
    new_matches_df = pd.DataFrame(new_matches)
    print(f"Created DataFrame with {len(new_matches_df)} new matches")

    # Combine existing and new matches
    combined_matches = pd.concat([existing_matches, new_matches_df], ignore_index=True)
    print(f"Combined DataFrame has {len(combined_matches)} total matches")

    # Remove duplicates (keeping first occurrence)
    before_dedup = len(combined_matches)
    combined_matches = combined_matches.drop_duplicates(subset=['Source', 'Target'], keep='first')
    after_dedup = len(combined_matches)
    print(f"Removed {before_dedup - after_dedup} duplicate matches")

    # Save back to CSV
    try:
        # Ensure the directory exists
        os.makedirs('data/processed', exist_ok=True)
        
        combined_matches.to_csv('data/processed/consolidated_matches.csv', index=False)
        print(f"Successfully saved {len(combined_matches)} matches to consolidated_matches.csv")
    except Exception as e:
        print(f"Error saving file: {e}")

if __name__ == "__main__":
    add_manual_matches()
