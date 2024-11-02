import pandas as pd
import logging
import re  # Add this in case standardize_name import fails

# Try to import standardize_name, if not available, define a simple version
try:
    from data_preprocessing import standardize_name
except ImportError:
    def standardize_name(name):
        """Simple name standardization if import fails"""
        if pd.isna(name):
            return name
        name = str(name).lower()
        name = re.sub(r'[^\w\s]', '', name)
        return ' '.join(name.split())

def analyze_dataset_quality(df: pd.DataFrame, name_column: str, dataset_name: str):
    """Analyze a dataset for potential duplicate issues after normalization."""
    logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
    
    # Original duplicate check
    original_dupes = df[name_column].value_counts()
    original_dupes = original_dupes[original_dupes > 1]
    
    # Add normalized names
    df['NameNormalized'] = df[name_column].apply(standardize_name)
    
    # Check for duplicates after normalization
    normalized_dupes = df['NameNormalized'].value_counts()
    normalized_dupes = normalized_dupes[normalized_dupes > 1]
    
    # Find new duplicates created by normalization
    new_dupes = set(normalized_dupes.index) - set(original_dupes.index)
    
    logging.info(f"\nAnalysis for {dataset_name}:")
    logging.info(f"Total records: {len(df)}")
    logging.info(f"Unique {name_column}: {df[name_column].nunique()}")
    logging.info(f"Unique normalized names: {df['NameNormalized'].nunique()}")
    
    if len(original_dupes) > 0:
        logging.info(f"\nOriginal duplicates in {name_column}:")
        print(original_dupes)
    
    if len(normalized_dupes) > 0:
        logging.info(f"\nDuplicates after normalization:")
        print(normalized_dupes)
    
    if new_dupes:
        logging.info("\nNew duplicates created by normalization:")
        for name in new_dupes:
            original_names = df[df['NameNormalized'] == name][name_column].tolist()
            print(f"\nNormalized name: {name}")
            print(f"Original names: {original_names}")
    
    return df

def main():
    # Load datasets
    ops_data = pd.read_csv('data/raw/ops_data.csv')
    dedup_data = pd.read_csv('data/processed/final_deduplicated_dataset.csv')
    
    # Analyze ops_data
    ops_analysis = analyze_dataset_quality(ops_data, 'Agency Name', 'ops_data.csv')
    
    # Analyze deduplicated dataset
    dedup_analysis = analyze_dataset_quality(dedup_data, 'Name - Ops', 'final_deduplicated_dataset.csv')
    
    # Save analysis results
    ops_analysis.to_csv('data/processed/ops_data_with_normalized.csv', index=False)
    dedup_analysis.to_csv('data/processed/dedup_data_with_normalized.csv', index=False)

if __name__ == "__main__":
    main() 