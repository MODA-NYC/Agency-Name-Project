import pandas as pd
from matching.normalizer import NameNormalizer
import logging

def merge_dataframes(primary_df, secondary_dfs, on_column='NameNormalized', how='outer'):
    """
    Merge multiple dataframes based on a specified column.
    
    Args:
    primary_df (pandas.DataFrame): The primary dataframe to merge onto
    secondary_dfs (list): List of tuples (dataframe, fields_to_keep, prefix)
    on_column (str): Column to merge on (default: 'NameNormalized')
    how (str): Type of merge to perform (default: 'outer')
    
    Returns:
    pandas.DataFrame: Merged dataframe
    """
    result = primary_df.copy()
    normalizer = NameNormalizer()
    
    # Standard column mappings
    column_mappings = {
        'ops': {
            'Agency Name': 'Name - Ops',
            'Agency Acronym': 'Acronym - Ops'
        },
        'nyc_gov': {
            'Agency Name': 'Name - NYC.gov Redesign',
            'Head of Organization': 'HeadOfOrganizationName',
            'HoO Title': 'HeadOfOrganizationTitle',
            'Agency Link (URL)': 'HeadOfOrganizationURL'
        }
    }
    
    # Ensure primary_df has NameNormalized
    if 'NameNormalized' not in result.columns and 'Name' in result.columns:
        result['NameNormalized'] = result['Name'].apply(normalizer.normalize)
    
    # Ensure RecordID exists
    if 'RecordID' not in result.columns:
        result['RecordID'] = [f'REC_{i:06d}' for i in range(len(result))]
    
    def clean_agency_name(name: str) -> str:
        """Clean agency name and prevent splitting on common separators."""
        if pd.isna(name):
            return name
        # Don't split on these characters
        name = name.replace(' & ', ' and ')
        name = name.replace(', Inc.', ' Inc')
        name = name.replace(' - ', ' ')
        return name.strip()
    
    for df, fields_to_keep, prefix in secondary_dfs:
        df_to_merge = df[fields_to_keep].copy()
        
        # Clean agency names before processing
        if 'Agency Name' in df_to_merge.columns:
            df_to_merge['Agency Name'] = df_to_merge['Agency Name'].apply(clean_agency_name)
        
        # Add RecordID if not present
        if 'RecordID' not in df_to_merge.columns:
            df_to_merge['RecordID'] = [f'REC_{i:06d}' for i in range(len(df_to_merge))]
        
        # Verify all required columns exist
        missing_cols = [col for col in fields_to_keep if col not in df_to_merge.columns]
        if missing_cols:
            logging.warning(f"Missing columns in dataset with prefix {prefix}: {missing_cols}")
            # Use only available columns
            fields_to_keep = [col for col in fields_to_keep if col not in missing_cols]
            df_to_merge = df[fields_to_keep].copy()
        
        # Apply standard column mappings
        source = prefix.rstrip('_')
        if source in column_mappings:
            for old_col, new_col in column_mappings[source].items():
                if old_col in df_to_merge.columns:
                    df_to_merge[new_col] = df_to_merge[old_col]
                    df_to_merge = df_to_merge.drop(columns=[old_col])
        
        result = pd.merge(result, df_to_merge, on=on_column, how=how)
    
    return result

def clean_merged_data(df):
    """
    Clean the merged dataframe by removing blank rows and handling URL conflicts.
    
    Args:
    df (pandas.DataFrame): The merged dataframe
    
    Returns:
    pandas.DataFrame: Cleaned dataframe
    """
    # Remove blank rows
    df = df.dropna(how='all')
    
    # Handle URL conflicts
    if 'URL' in df.columns and 'nyc_gov_Agency Link (URL)' in df.columns:
        df['URL'] = df['URL'].fillna(df['nyc_gov_Agency Link (URL)'])
        df = df.drop(columns=['nyc_gov_Agency Link (URL)'])
    
    return df

def track_data_provenance(merged_df):
    """Enhanced data source tracking"""
    def get_source(row):
        if pd.notna(row.get('Name - Ops')):
            return 'ops'
        if pd.notna(row.get('Name - NYC.gov Redesign')):
            return 'nyc_gov'
        if pd.notna(row.get('Name')):
            return 'nyc_agencies_export'
        return 'unknown'
    
    merged_df['data_source'] = merged_df.apply(get_source, axis=1)
    return merged_df

def ensure_record_ids(df: pd.DataFrame, prefix: str = 'REC_') -> pd.DataFrame:
    """Ensure all records have proper IDs."""
    if 'RecordID' not in df.columns:
        df['RecordID'] = [f"{prefix}{i:06d}" for i in range(len(df))]
    else:
        # Fix invalid IDs
        mask = ~df['RecordID'].str.match(r'REC_\d{6}$', na=True)
        df.loc[mask, 'RecordID'] = [f"{prefix}{i:06d}" for i in range(sum(mask))]
    return df
