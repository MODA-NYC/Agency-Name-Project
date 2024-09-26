import pandas as pd

def merge_dataframes(primary_df, secondary_dfs, on_column='NameNormalized', how='left'):
    """
    Merge multiple dataframes based on a specified column.
    
    Args:
    primary_df (pandas.DataFrame): The primary dataframe to merge onto
    secondary_dfs (list): List of tuples (dataframe, fields_to_keep, prefix)
    on_column (str): Column to merge on (default: 'NameNormalized')
    how (str): Type of merge to perform (default: 'left')
    
    Returns:
    pandas.DataFrame: Merged dataframe
    """
    result = primary_df.copy()
    
    for df, fields_to_keep, prefix in secondary_dfs:
        df_to_merge = df[fields_to_keep + [on_column]].copy()
        df_to_merge.columns = [f"{prefix}{col}" if col != on_column else col for col in df_to_merge.columns]
        
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
    """
    Track the source of each piece of data in the merged dataframe.
    
    Args:
    merged_df (pandas.DataFrame): The merged dataframe
    
    Returns:
    pandas.DataFrame: Dataframe with an additional 'data_source' column
    """
    sources = ['nyc_agencies_export', 'nyc_gov', 'ops']
    
    def get_source(row):
        row_sources = []
        for source in sources:
            if any(col.startswith(source) for col in row.index if pd.notna(row[col])):
                row_sources.append(source)
        return ', '.join(row_sources) if row_sources else 'unknown'
    
    merged_df['data_source'] = merged_df.apply(get_source, axis=1)
    return merged_df