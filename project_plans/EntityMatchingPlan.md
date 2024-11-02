# NYC Agency Name Matching Project Plan

## Current Status
- Base functionality works but has issues with incorrect entity combinations
- Analysis tools have identified specific problems to fix
- Need to preserve correct matches while preventing incorrect combinations

## Immediate Goals (Phase 1)
1. Fix Entity Combinations
   - Prevent Development Corporation combinations
   - Prevent Board combinations
   - Maintain all source records
   - Preserve NYC.gov Redesign column

2. Success Criteria
   - All OPS records (413) present in final dataset
   - All HOO records (179) present where appropriate
   - No incorrect combinations of Development Corporations
   - No incorrect combinations of Boards
   - NYC.gov Redesign column preserved

## Implementation Steps

### 1. Revert and Preserve (Day 1)
- Keep analysis tools:
  - src/analyze_match_issues.py
  - src/analyze_combined_records.py
  - src/analyze_merge_completeness.py
- Revert other changes to last working version
- Document current matches in consolidated_matches.csv

### 2. Fix Matching Logic (Day 2)
a. Update consolidated_matches.csv
   - Review all Development Corporation matches
   - Review all Board matches
   - Document each change

b. Add Entity Protection
   - Implement entity type protection in normalizer
   - Test with Development Corporations first
   - Test with Boards second
   - Validate no loss of records

### 3. Validation Process (Day 3)
For each change:
1. Run main.py
2. Verify record counts:
   - Total records
   - OPS records
   - HOO records
3. Check for incorrect combinations:
   - Development Corporations
   - Boards
4. Document results

## Files to Maintain
1. Analysis Tools
   - src/analyze_match_issues.py
   - src/analyze_combined_records.py
   - src/analyze_merge_completeness.py

2. Core Processing
   - src/main.py (revert to last working version)
   - src/data_merging.py (revert to last working version)
   - src/matching/matcher.py (revert to last working version)

3. Data Files
   - data/processed/consolidated_matches.csv (keep current version)
   - All other data files (revert to last working version)

## Success Metrics
1. Record Counts
   - Final dataset should have all source records
   - No unexplained missing records

2. Entity Integrity
   - Development Corporations remain distinct
   - Boards remain distinct
   - Other entity types properly handled

3. Data Quality
   - All columns preserved
   - No data loss in merging
   - Matches properly applied

## Next Phase Planning
After completing Phase 1:
1. Review other entity types for similar issues
2. Implement additional entity protections as needed
3. Consider automation of match validation 