# Script Consolidation TODOs

## Data Quality Issues
1. Fix column naming inconsistency in merged_dataset.csv
   - Change 'ops_Agency Name' to 'Name - Ops'
   - Standardize all column names to match established format

2. Record ID Management
   - Ensure consistent RecordID format (REC_XXXXXX)
   - Add RecordIDs to merged dataset
   - Verify RecordID continuity
   - Handle RecordID conflicts during merges

3. Validation Enhancements
   - Add column format validation
   - Verify required fields presence
   - Check data type consistency
   - Add source data validation

4. Logging Improvements
   - Add statistics for records before/after deduplication
   - Log successful merges
   - Track conflict resolutions
   - Report on data source contributions

5. Testing
   - Create comprehensive test suite
   - Add validation tests
   - Test edge cases in matching
   - Verify duplicate handling

## Analysis Tools Needed
1. Script to analyze NaN values and unknown sources
2. Tool to verify merged dataset quality
3. Report generator for new matches
4. Duplicate analysis tool

## Documentation Updates
1. Update data dictionary with new columns
2. Document matching rules
3. Create data lineage documentation
4. Add validation rules documentation 