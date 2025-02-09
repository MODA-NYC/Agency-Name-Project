import pandas as pd
import logging

def resolve_po_name(row: pd.Series) -> pd.Series:
    """
    Resolve the principal officer name according to specific business rules:
    1. If Ops_PrincipalOfficerName exists and is non-empty, use that
    2. Otherwise, use HOO_PrincipalOfficerName if it exists and is non-empty
    3. If neither exists or both are empty, return empty string
    
    Args:
        row (pd.Series): A row from the DataFrame containing principal officer name fields
        
    Returns:
        pd.Series: A two-element series containing [status, suggested_value] where:
            - status is either "match" if values are identical or "conflict: val1, val2" if different
            - suggested_value follows the priority order specified above
    """
    ops_name = str(row.get('Ops_PrincipalOfficerName', '')).strip()
    hoo_name = str(row.get('HOO_PrincipalOfficerName', '')).strip()
    
    # Determine suggested value based on priority
    suggested = ops_name if ops_name else hoo_name
    
    # Determine status
    if not ops_name and not hoo_name:
        status = "match"  # Both empty is considered a match
    elif not ops_name or not hoo_name:
        status = "match"  # One empty and one value is considered a match
    elif ops_name.lower() == hoo_name.lower():
        status = "match"
    else:
        status = f"conflict: '{ops_name}' (from Ops), '{hoo_name}' (from HOO)"
    
    return pd.Series([status, suggested])

def resolve_conflict(row, candidate_cols, priority_order):
    """
    Given a row and candidate columns, compute:
      - A status string: "match" if all non-empty candidate values (after lowercasing and stripping) are identical,
        otherwise "conflict: val1, val2, ..." listing the distinct non-empty values.
      - A suggested value: the first non-empty value from the priority_order.
    
    This version uses direct equality checking (after lowercasing and stripping) rather than fuzzy matching.
    This ensures that even minor differences in principal officer names are flagged as conflicts.
    
    Parameters:
        row (pd.Series): a row from the DataFrame.
        candidate_cols (list): list of column names to compare.
        priority_order (list): list of column names in priority order (highest first).
    
    Returns:
        pd.Series: A two-element Series: [status, suggested_value].
    """
    # Special case for principal officer name resolution
    if set(candidate_cols) == {'Ops_PrincipalOfficerName', 'HOO_PrincipalOfficerName'}:
        return resolve_po_name(row)
    
    # Special debug for NYPD
    is_nypd = False
    if 'Name' in row and isinstance(row['Name'], str) and 'Police Department' in row['Name']:
        is_nypd = True
        logging.info("\n=== Processing NYPD Row ===")
        logging.info(f"Full row name: {row['Name']}")
        logging.info(f"Candidate columns: {candidate_cols}")
        logging.info(f"Priority order: {priority_order}")
        logging.info("\nRow data for all columns:")
        for col in row.index:
            logging.info(f"  {col}: {repr(row[col])} (type: {type(row[col])})")
    
    # Validate input columns exist
    missing_cols = [col for col in candidate_cols if col not in row.index]
    if missing_cols:
        logging.error(f"Missing columns in row: {missing_cols}")
        if is_nypd:
            logging.error("NYPD Debug - Missing columns detected!")
        return pd.Series(["error: missing columns", ""])
    
    # Log input values for debugging
    if is_nypd:
        logging.info("\nProcessing candidate columns:")
        for col in candidate_cols:
            raw_val = row[col]
            val_type = type(raw_val)
            is_na = pd.isna(raw_val)
            str_val = str(raw_val).strip() if not is_na else ''
            logging.info(f"  Column: {col}")
            logging.info(f"    Raw value (repr): {repr(raw_val)}")
            logging.info(f"    Raw value (str): {str(raw_val)}")
            logging.info(f"    Type: {val_type}")
            logging.info(f"    Is NA: {is_na}")
            logging.info(f"    Stripped string (repr): {repr(str_val)}")
    
    # Gather non-empty values, keeping both raw and normalized versions
    values = []  # List of tuples (raw_value, normalized_value, source_column)
    for col in candidate_cols:
        if pd.notna(row[col]):
            raw_val = str(row[col]).strip()
            if raw_val != '':
                norm_val = raw_val.lower()
                values.append((raw_val, norm_val, col))
                if is_nypd:
                    logging.info(f"\nAdded value from {col}:")
                    logging.info(f"  Raw value (repr): {repr(raw_val)}")
                    logging.info(f"  Raw value (str): {str(raw_val)}")
                    logging.info(f"  Normalized value (repr): {repr(norm_val)}")
                    logging.info(f"  Source column: {col}")
    
    if is_nypd and not values:
        logging.warning("NYPD Debug - No valid values found from any candidate column!")
    
    # Get distinct values based on normalized versions
    distinct_values = []
    seen_normalized = set()
    value_sources = {}  # Track which columns contributed each value
    
    for raw_val, norm_val, source_col in values:
        if norm_val not in seen_normalized:
            distinct_values.append(raw_val)
            seen_normalized.add(norm_val)
            value_sources[raw_val] = source_col
            if is_nypd:
                logging.info(f"\nAdded distinct value:")
                logging.info(f"  Raw value (repr): {repr(raw_val)}")
                logging.info(f"  Normalized value (repr): {repr(norm_val)}")
                logging.info(f"  Source column: {source_col}")
                logging.info(f"  Current seen_normalized: {seen_normalized}")
                logging.info(f"  Current distinct_values: {distinct_values}")
    
    if is_nypd:
        logging.info("\nValue summary:")
        logging.info(f"  All values (raw): {[repr(v[0]) for v in values]}")
        logging.info(f"  All values (normalized): {[repr(v[1]) for v in values]}")
        logging.info(f"  Distinct values: {[repr(v) for v in distinct_values]}")
        logging.info(f"  Value sources: {value_sources}")
        logging.info(f"  Number of distinct values: {len(distinct_values)}")
    
    # Determine status based on number of distinct values
    if len(distinct_values) <= 1:
        status = "match"
        if is_nypd:
            logging.info("\nStatus: MATCH")
            if distinct_values:
                logging.info(f"  Single value (repr): {repr(distinct_values[0])}")
            else:
                logging.info("  No values found")
    else:
        value_details = [f"'{val}' (from {value_sources[val]})" for val in distinct_values]
        status = "conflict: " + ", ".join(value_details)
        if is_nypd:
            logging.info("\nStatus: CONFLICT")
            for val in distinct_values:
                logging.info(f"  Value (repr): {repr(val)}")
                logging.info(f"  Source: {value_sources[val]}")
    
    # Determine suggested value based on priority order
    suggested = ""
    for col in priority_order:
        if pd.notna(row[col]):
            val = str(row[col]).strip()
            if val != '':
                suggested = val
                if is_nypd:
                    logging.info(f"\nSelected suggested value:")
                    logging.info(f"  Value (repr): {repr(suggested)}")
                    logging.info(f"  Source: {col}")
                break
    
    if is_nypd:
        logging.info("\nFinal result:")
        logging.info(f"  Status: {status}")
        logging.info(f"  Suggested (repr): {repr(suggested)}")
        logging.info("=== End NYPD Processing ===\n")
    
    return pd.Series([status, suggested])

if __name__ == "__main__":
    # Configure logging
    logging.basicConfig(level=logging.INFO, format='%(message)s')
    
    # Example usage for testing
    import pandas as pd

    # Test case for NYPD principal officer conflict
    print("\n=== Testing NYPD Case ===")
    data_nypd = {
        'Name': ['New York City Police Department'],
        'Ops_PrincipalOfficerName': ['Jessica Tisch'],
        'HOO_PrincipalOfficerName': ['Edward A. Caban'],
        'RecordID': ['NYPD_TEST']
    }
    df_nypd = pd.DataFrame(data_nypd)
    print("\nInput DataFrame:")
    print(df_nypd)
    print("\nRaw values before processing:")
    print(f"Ops_PrincipalOfficerName: {repr(df_nypd['Ops_PrincipalOfficerName'].iloc[0])}")
    print(f"HOO_PrincipalOfficerName: {repr(df_nypd['HOO_PrincipalOfficerName'].iloc[0])}")
    
    df_nypd[['PO_Name_Status', 'Suggested_PO_Name']] = df_nypd.apply(
        lambda row: resolve_conflict(row, ['Ops_PrincipalOfficerName', 'HOO_PrincipalOfficerName'],
                                     ['Ops_PrincipalOfficerName', 'HOO_PrincipalOfficerName']),
        axis=1
    )
    print("\nResults:")
    print(df_nypd)
    print("=== End NYPD Test ===\n")

    # Test case for URL conflict resolution
    data_url = {
        'URL': ['https://phaseone.example.com', 'https://phaseone.example.com', ''],
        'HOO_URL': ['https://hoo.example.com', 'https://hoo.example.com', 'https://hoo.example.com'],
        'Ops_URL': ['https://ops.example.com', '', '']
    }
    df_url = pd.DataFrame(data_url)
    df_url[['URL_Status', 'Suggested_URL']] = df_url.apply(
        lambda row: resolve_conflict(row, ['Ops_URL', 'HOO_URL', 'URL'], ['Ops_URL', 'HOO_URL', 'URL']),
        axis=1
    )
    print("\nURL Conflict Resolution:")
    print(df_url)

    # Test case for Principal Officer Name conflict resolution (direct equality check)
    data_po = {
        'Ops_PrincipalOfficerName': ['John Doe', 'John Doe', 'Jonathan Doe'],
        'HOO_PrincipalOfficerName': ['John Doe', 'Jon Doe', 'Jonathan A. Doe']
    }
    df_po = pd.DataFrame(data_po)
    df_po[['PO_Name_Status', 'Suggested_PO_Name']] = df_po.apply(
        lambda row: resolve_conflict(row, ['Ops_PrincipalOfficerName', 'HOO_PrincipalOfficerName'],
                                     ['Ops_PrincipalOfficerName', 'HOO_PrincipalOfficerName']),
        axis=1
    )
    print("\nPrincipal Officer Name Conflict Resolution:")
    print(df_po)
    
    # Test case for Principal Officer Title conflict resolution
    data_pt = {
        'Ops_PrincipalOfficerTitle': ['Chief Inspector', 'Chief Inspector', 'Inspector'],
        'HOO_PrincipalOfficerTitle': ['Chief Inspector', 'Chief Insp.', 'Inspector General']
    }
    df_pt = pd.DataFrame(data_pt)
    df_pt[['PO_Title_Status', 'Suggested_PO_Title']] = df_pt.apply(
        lambda row: resolve_conflict(row, ['Ops_PrincipalOfficerTitle', 'HOO_PrincipalOfficerTitle'],
                                     ['Ops_PrincipalOfficerTitle', 'HOO_PrincipalOfficerTitle']),
        axis=1
    )
    print("\nPrincipal Officer Title Conflict Resolution:")
    print(df_pt) 