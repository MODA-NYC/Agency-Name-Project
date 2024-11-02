import pandas as pd
import os
from rapidfuzz import fuzz

def analyze_missing_records(data_dir: str):
    # Load datasets
    ops_data = pd.read_csv(os.path.join(data_dir, 'raw', 'ops_data.csv'))
    hoo_data = pd.read_csv(os.path.join(data_dir, 'raw', 'nyc_gov_hoo.csv'))
    merged = pd.read_csv(os.path.join(data_dir, 'intermediate', 'merged_dataset.csv'))
    
    # Function to find potential matches
    def find_potential_matches(name, df, threshold=80):
        matches = []
        for idx, row in df.iterrows():
            # Check against Name and NameNormalized
            for field in ['Name', 'NameNormalized']:
                if pd.notna(row[field]):
                    score = fuzz.ratio(name.lower(), row[field].lower())
                    if score >= threshold:
                        matches.append({
                            'potential_match': row[field],
                            'score': score,
                            'source': field
                        })
        return sorted(matches, key=lambda x: x['score'], reverse=True)[:3]  # Top 3 matches
    
    # Analyze OPS missing records
    print("\n=== OPS Missing Records Analysis ===")
    ops_in_merged = set(merged[merged['Name - Ops'].notna()]['Name - Ops'])
    ops_original = set(ops_data['Agency Name'])
    missing_ops = ops_original - ops_in_merged
    
    for record in missing_ops:
        print(f"\nMissing: {record}")
        matches = find_potential_matches(record, merged)
        if matches:
            print("Potential matches in merged dataset:")
            for match in matches:
                print(f"- {match['potential_match']} (Score: {match['score']})")
    
    # Analyze HOO missing records
    print("\n=== HOO Missing Records Analysis ===")
    hoo_in_merged = set(merged[merged['Name - NYC.gov Redesign'].notna()]['Name - NYC.gov Redesign'])
    hoo_original = set(hoo_data['Agency Name'])
    missing_hoo = hoo_original - hoo_in_merged
    
    for record in missing_hoo:
        if pd.isna(record):  # Skip nan values
            continue
        print(f"\nMissing: {record}")
        matches = find_potential_matches(record, merged)
        if matches:
            print("Potential matches in merged dataset:")
            for match in matches:
                print(f"- {match['potential_match']} (Score: {match['score']})")

if __name__ == "__main__":
    analyze_missing_records('data') 