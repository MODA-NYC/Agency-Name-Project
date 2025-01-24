import pandas as pd
import logging
from .data_normalization import global_normalize_name

logger = logging.getLogger(__name__)

def apply_global_normalization(df: pd.DataFrame) -> pd.DataFrame:
    """
    Apply global normalization to the dataset, ensuring 'NameNormalized' is derived
    from 'AgencyNameEnriched' field if present.
    """

    if 'AgencyNameEnriched' not in df.columns:
        logger.warning("AgencyNameEnriched column missing. Using Name column as fallback for global normalization.")
        source_col = 'Name' if 'Name' in df.columns else None
    else:
        source_col = 'AgencyNameEnriched'

    if source_col is None:
        raise ValueError("No suitable column found for global normalization (missing AgencyNameEnriched and Name).")

    # Apply global normalization
    df['NameNormalized'] = df[source_col].apply(global_normalize_name)

    return df