import pandas as pd
import re

def preprocess_agency_data(df, source_column='Agency Name'):
    # Create a copy to avoid SettingWithCopyWarning
    df = df.copy()

    # Fill NaN values with a placeholder
    df[source_column] = df[source_column].fillna('Unknown Agency')
    
    # Remove duplicates based on the source column, keeping the first occurrence
    df = df.drop_duplicates(subset=[source_column], keep='first')
    
    # Create NameNormalized column
    df['NameNormalized'] = df[source_column].apply(standardize_name)
    
    return df

def standardize_name(name):
    # Convert to lowercase
    name = str(name).lower()
    # Remove punctuation and extra spaces
    name = re.sub(r'[^\w\s]', '', name)
    name = ' '.join(name.split())
    return name

def differentiate_mayors_office(df):
    # Identify rows where 'NameNormalized' is 'mayor office'
    mayors_office_mask = df['NameNormalized'] == 'mayor office'

    # Check if 'Name' column exists
    if 'Name' not in df.columns:
        raise KeyError("'Name' column is missing in the DataFrame.")

    # Update 'NameNormalized' using the standardized 'Name' values for these rows
    df.loc[mayors_office_mask, 'NameNormalized'] = df.loc[mayors_office_mask, 'Name'].apply(standardize_name)

    return df

def cleanup_consolidated_matches():
    """Clean up and deduplicate entries in consolidated_matches.csv"""
    matches_df = pd.read_csv('data/processed/consolidated_matches.csv')
    
    # Sort by Score descending, then by Label (so 'Match' comes before empty labels)
    matches_df = matches_df.sort_values(['Score', 'Label'], ascending=[False, True])
    
    # Drop duplicates keeping first occurrence (highest score and labeled entries)
    matches_df = matches_df.drop_duplicates(subset=['Source', 'Target'], keep='first')
    
    # Sort again for final output
    matches_df = matches_df.sort_values(['Label', 'Score'], ascending=[False, False])
    
    # Save back to file
    matches_df.to_csv('data/processed/consolidated_matches.csv', index=False)

def preprocess_matches(df: pd.DataFrame) -> pd.DataFrame:
    """
    Preprocess the matches dataframe by adding record IDs and normalizing strings.
    
    Args:
        df: Input DataFrame with Source and Target columns
        
    Returns:
        Preprocessed DataFrame with RecordID column added
    """
    # Add RecordID column
    df['RecordID'] = df.apply(generate_record_id, axis=1)
    
    # Normalize string columns
    df['Source'] = df['Source'].str.lower().str.strip()
    df['Target'] = df['Target'].str.lower().str.strip()
    
    return df
