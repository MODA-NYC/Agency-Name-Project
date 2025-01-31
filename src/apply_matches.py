import os
import sys
import pandas as pd
import logging
import json

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def get_preferred_record(rec1, rec2):
    """
    Determine the preferred record based on source priority.
    Priority: nyc_agencies_export > ops > nyc_gov.
    Returns a tuple: (preferred_record, secondary_record)
    """
    priority = {"nyc_agencies_export": 3, "ops": 2, "nyc_gov": 1}
    # Handle NaN/float values in source field
    src1 = str(rec1.get("source", "")).strip().lower() if pd.notna(rec1.get("source")) else ""
    src2 = str(rec2.get("source", "")).strip().lower() if pd.notna(rec2.get("source")) else ""
    p1 = priority.get(src1, 0)
    p2 = priority.get(src2, 0)
    if p1 >= p2:
        return rec1, rec2
    else:
        return rec2, rec1

def merge_records(primary, secondary):
    """
    Merge two records.
    For each column:
      - For columns starting with "Name -", if both values exist and are different, join with " | ".
      - Otherwise, use the primary value if not null; if null, use the secondary.
    Also update merged_from and merge_note fields.
    """
    merged = primary.copy()
    merged_from = []
    # If primary has a merged_from field, load it (assumed JSON list)
    try:
        if pd.notna(primary.get("merged_from")):
            merged_from = json.loads(primary["merged_from"])
    except Exception:
        merged_from = []
    # Always add both record IDs if not already included
    for rec in [primary, secondary]:
        rec_id = rec.get("RecordID")
        if rec_id and rec_id not in merged_from:
            merged_from.append(rec_id)
    
    # Merge each column
    for col in primary.index:
        val_primary = primary[col]
        val_secondary = secondary[col]
        # For source-specific name columns, combine if both exist and are different
        if col.startswith("Name -"):
            if pd.notna(val_primary) and pd.notna(val_secondary):
                # If they are different and not already merged
                if str(val_primary).strip().lower() != str(val_secondary).strip().lower():
                    merged[col] = f"{val_primary} | {val_secondary}"
                # Else keep one value
            elif pd.isna(val_primary) and pd.notna(val_secondary):
                merged[col] = val_secondary
        # For the 'merged_from' and 'merge_note' columns, we handle after loop
        elif col in ["merged_from", "merge_note"]:
            continue
        else:
            # For other columns, if primary is null, use secondary
            if pd.isna(val_primary) and pd.notna(val_secondary):
                merged[col] = val_secondary
    # Update merged_from and merge_note metadata
    merged["merged_from"] = json.dumps(merged_from)
    note = f"Merged records: {', '.join(merged_from)}"
    # Append any existing merge_note from primary and secondary
    notes = []
    if pd.notna(primary.get("merge_note")):
        notes.append(str(primary["merge_note"]))
    if pd.notna(secondary.get("merge_note")):
        notes.append(str(secondary["merge_note"]))
    notes.append(note)
    merged["merge_note"] = " | ".join(notes)
    return merged

def apply_matches(input_path, matches_path, output_path):
    # Read input data and matches
    df = pd.read_csv(input_path)
    matches = pd.read_csv(matches_path)
    
    # Create a working copy of the dataframe and add a processed flag
    result_df = df.copy()
    result_df["processed"] = False
    
    # Process each match from the consolidated matches file
    for _, match in matches.iterrows():
        source_name = match["Source"]
        target_name = match["Target"]
        
        # Skip if names are missing
        if pd.isna(source_name) or pd.isna(target_name):
            continue
        
        # Build masks for matching records (case-insensitive on Name, Name - Ops, Name - HOO)
        def build_mask(name):
            name_lower = name.lower().strip()
            # Handle potential NaN values in string columns
            mask = (
                (result_df["Name"].fillna("").str.lower().str.strip() == name_lower) |
                (result_df["Name - Ops"].fillna("").str.lower().str.strip() == name_lower) |
                (result_df["Name - HOO"].fillna("").str.lower().str.strip() == name_lower)
            )
            return mask
        
        source_mask = build_mask(source_name)
        target_mask = build_mask(target_name)
        
        # If no records found in one direction, try swapping names
        if not (source_mask.any() and target_mask.any()):
            source_mask = build_mask(target_name)
            target_mask = build_mask(source_name)
            # Also swap for later logging
            source_name, target_name = target_name, source_name
        
        # Get the matching records
        source_records = result_df[source_mask]
        target_records = result_df[target_mask]
        
        if source_records.empty or target_records.empty:
            logger.warning(f"Match not found for: {source_name} -> {target_name}")
            continue
        
        # For simplicity, take the first unprocessed record from each group
        source_record = source_records[~source_records["processed"]].iloc[0] if not source_records[~source_records["processed"]].empty else source_records.iloc[0]
        target_record = target_records[~target_records["processed"]].iloc[0] if not target_records[~target_records["processed"]].empty else target_records.iloc[0]
        
        # IMPORTANT: Do not merge if both records already come from the same source group
        # i.e. if both have a non-null "Name - HOO" OR both have a non-null "Name - Ops", then skip merging.
        source_HOO = source_record.get("Name - HOO")
        target_HOO = target_record.get("Name - HOO")
        source_OPS = source_record.get("Name - Ops")
        target_OPS = target_record.get("Name - Ops")
        # Check if both records already have a value in the same source-specific field.
        if (pd.notna(source_HOO) and pd.notna(target_HOO)) or (pd.notna(source_OPS) and pd.notna(target_OPS)):
            # Mark both as processed but do not merge; they remain separate rows.
            result_df.loc[source_record.name, "processed"] = True
            result_df.loc[target_record.name, "processed"] = True
            continue
        
        # Otherwise, merge the records (from different source groups)
        primary_record, secondary_record = get_preferred_record(source_record, target_record)
        merged_record = merge_records(primary_record, secondary_record)
        preferred_index = primary_record.name
        secondary_index = secondary_record.name
        result_df.loc[preferred_index] = merged_record
        result_df.loc[preferred_index, "processed"] = True
        result_df.loc[secondary_index, "processed"] = True
        # Remove the secondary record from the dataframe
        result_df = result_df.drop(index=secondary_index)
    
    # Drop the temporary "processed" flag column
    if "processed" in result_df.columns:
        result_df = result_df.drop(columns=["processed"])
    
    # Reorder columns: place Name columns first
    name_cols = ["Name", "NameAlphabetized"] + sorted([col for col in result_df.columns if col.startswith("Name - ")])
    other_cols = [col for col in result_df.columns if col not in name_cols]
    ordered_cols = name_cols + other_cols
    result_df = result_df[ordered_cols]
    
    # Save the final deduplicated dataset
    result_df.to_csv(output_path, index=False)
    logger.info(f"Final dataset has {len(result_df)} records")
    # Log number of merged records (difference between original and final count)
    logger.info(f"Total records merged: {len(pd.read_csv(input_path)) - len(result_df)}")

if __name__ == "__main__":
    # Set up logging and determine paths relative to the project root
    logging.basicConfig(level=logging.INFO)
    script_dir = os.path.dirname(os.path.abspath(__file__))
    project_root = os.path.abspath(os.path.join(script_dir, ".."))
    data_dir = os.path.join(project_root, "data", "processed")
    intermediate_dir = os.path.join(project_root, "data", "intermediate")
    
    apply_matches(
        input_path=os.path.join(intermediate_dir, "merged_dataset.csv"),
        matches_path=os.path.join(data_dir, "consolidated_matches.csv"),
        output_path=os.path.join(data_dir, "final_deduplicated_dataset.csv")
    )