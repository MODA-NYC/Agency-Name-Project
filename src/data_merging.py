import pandas as pd
from matching.normalizer import NameNormalizer
import logging
from typing import List, Tuple

def merge_dataframes(primary_df: pd.DataFrame, secondary_dfs: List[Tuple[pd.DataFrame, List[str], str]]) -> pd.DataFrame:
    """Merge primary dataframe with all secondary dataframes.
    
    Args:
        primary_df: Primary DataFrame to merge with
        secondary_dfs: List of tuples (DataFrame, fields_to_keep, prefix)
    """
    
    # Define standard fields to keep from secondary dataframes
    standard_fields = [
        'RecordID',
        'Agency Name',
        'NameNormalized'
    ]

    merged_df = primary_df.copy()
    # Add source column to primary df
    merged_df['source'] = 'primary'
    
    for df, _, source_prefix in secondary_dfs:  # Use the prefix as source
        df_to_merge = df[standard_fields].copy()
        # Add source column using the prefix
        df_to_merge['source'] = source_prefix.rstrip('_')  # Remove trailing underscore if present
        merged_df = pd.concat([merged_df, df_to_merge], ignore_index=True)
    
    return merged_df

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
    """Ensure all records have proper IDs, creating them if necessary."""
    if 'RecordID' not in df.columns:
        df['RecordID'] = [f"{prefix}{i:06d}" for i in range(len(df))]
    else:
        # Fix invalid IDs
        mask = ~df['RecordID'].astype(str).str.match(r'^REC_\d{6}$', na=True)
        if mask.any():
            # Reassign these invalid IDs
            invalid_count = mask.sum()
            logging.info(f"Fixing {invalid_count} invalid RecordIDs.")
            new_ids = [f"{prefix}{i:06d}" for i in range(invalid_count)]
            df.loc[mask, 'RecordID'] = new_ids
    return df