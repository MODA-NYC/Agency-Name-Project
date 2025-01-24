import re

def standardize_name(name):
    """
    Standardizes a raw agency name string by:
      - Converting to lowercase
      - Expanding 'NYC' to 'new york city'
      - Replacing '+'/'&' with 'and'
      - Removing parentheses and any text inside them (including acronyms)
      - Removing punctuation
      - Removing extra whitespace
      - Expanding known abbreviations (dept -> department, etc.)
      - Preserving core words (e.g. 'new', 'york', 'city') for further normalization

    This function ensures that any acronyms present within parentheses
    (e.g., "(OTI)") are removed from the final `NameNormalized` string.
    """
    if not isinstance(name, str):
        return ''
    # Convert to lowercase
    name = name.lower()

    # Expand NYC early to preserve "new york city"
    name = re.sub(r'\bnyc\b', 'new york city', name)

    # Replace '+' and '&' with 'and'
    name = name.replace('+', 'and')
    name = name.replace('&', 'and')

    # Remove parentheses and their contents (this drops acronyms)
    name = re.sub(r'\(.*?\)', '', name)

    # Remove punctuation (other than spaces)
    name = re.sub(r'[^\w\s]', '', name)

    # Remove extra whitespace
    name = ' '.join(name.split())

    # Expand common abbreviations
    abbreviations = {
        'dept': 'department',
        'govt': 'government',
        'svc': 'services',
        'comm': 'commission',
        'admin': 'administration',
        'mgmt': 'management',
        'dev': 'development',
        'ed': 'education',
        'eng': 'engineering',
        'env': 'environmental',
        'hr': 'human resources',
        'acad': 'academy',
        'cuny': 'city university new york'
        # TODO: Add more abbreviations as needed
    }

    words = name.split()
    expanded_words = [abbreviations.get(word, word) for word in words]
    name = ' '.join(expanded_words)

    # Baseline stopwords removal
    # Remove minimal stopwords, excluding 'of' to preserve agency name structure
    stopwords = {'the', 'for', 'to', 'a', 'in', 'on'}  # 'of' purposely excluded
    words = [w for w in name.split() if w not in stopwords]
    name = ' '.join(words)

    return name.strip()

def global_normalize_name(name: str) -> str:
    """
    Applies global normalization rules to ensure consistent NameNormalized values
    across the entire dataset. Builds on standardize_name but makes more 
    globally-aware decisions now that all data is integrated.

    Enhancements for global step:
    - Ensures 'new york city' is preserved if present.
    - Retains relevant words like 'office', 'department', 'commission'.
    - Also removes acronyms in parentheses as part of the baseline standardization.

    Returns:
        str: A globally normalized name string.
    """
    if not isinstance(name, str):
        return ''

    # Start with the base standardization
    normalized = standardize_name(name)

    # Additional global refinements:
    # (Currently no extra global changes beyond what's in standardize_name().
    # Future steps might handle cross-source canonical forms or advanced expansions.)
    return normalized