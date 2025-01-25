import pandas as pd
import logging
from .data_normalization import global_normalize_name

logger = logging.getLogger(__name__)

def apply_global_normalization(df: pd.DataFrame) -> pd.DataFrame:
    """
    Apply global normalization to the dataset, ensuring 'NameNormalized' is derived
    from 'AgencyNameEnriched' field if present.
    """

    # Define column priority for normalization
    priority_columns = ['AgencyNameEnriched', 'Name', 'Agency Name']
    source_col = None

    for col in priority_columns:
        if col in df.columns:
            source_col = col
            logger.info(f"Using {col} as source for global normalization")
            break

    if source_col is None:
        raise ValueError("No suitable column found for global normalization (tried: AgencyNameEnriched, Name, Agency Name)")

    # Apply global normalization
    df['NameNormalized'] = df[source_col].apply(global_normalize_name)

    return df