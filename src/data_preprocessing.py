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
