# Agency Name Matching Improvements

## Address Missing Records

### Fuzzy Matching Enhancements
- Implement more sophisticated fuzzy matching for department name variations
  - Handle "Dept." vs "Department" variations
  - Account for word order differences (e.g., "Department of Education" vs "Education Department")
  - Consider using improved Levenshtein distance thresholds for longer agency names

### Pattern Recognition
- Add common abbreviation patterns to matching logic
- Implement n-gram based matching for partial name matches
- Consider phonetic matching for similar sounding agency names

### Flexible Matching Rules
- Allow for partial matches when full name isn't found
- Implement word-by-word matching for long agency names
- Consider semantic similarity for related terms

## Enhance Normalization

### Standardization Rules
- Standardize department/office prefixes consistently
  - Normalize "Dept.", "Department", "Office of", "Bureau of"
  - Handle possessive forms ("Children's" vs "Childrens")
  - Standardize spacing in multi-word agency names

### Text Processing
- Improve handling of special characters and punctuation
- Normalize capitalization patterns
- Standardize whitespace and hyphenation

### Name Components
- Break down agency names into components
- Handle common prefixes and suffixes consistently
- Normalize directional terms (e.g., "New York" vs "NY")

### Future Considerations
- Consider maintaining a lookup table of common variations
- Implement machine learning for pattern recognition
- Add context-aware normalization based on agency type

## Future ID Assignment System

### Source-Level ID Management
- Implement unique ID assignment at data source ingestion
- Create consistent ID format across all data sources
- Maintain ID history for tracking changes over time

### ID Format Improvements
- Consider hierarchical ID structure to reflect agency relationships
- Add metadata in ID format (e.g., source identifier prefix)
- Implement checksum/validation in ID structure

### ID Management System
- Build centralized ID registry for all agency records
- Implement ID reservation system for new agencies
- Create API for ID lookup and validation

### Data Quality Controls
- Automated ID sequence validation
- Duplicate ID detection
- ID format validation rules

### Migration Strategy
- Plan gradual transition from FINAL_REC_XXXXXX format
- Create mapping between old and new ID systems
- Maintain backward compatibility during transition

### Future Considerations
- Consider UUID or other globally unique ID formats
- Plan for integration with NYC Open Data ID systems
- Design for scalability and future agency reorganizations 