import re

def standardize_name(name):
    if not isinstance(name, str):
        return ''
    # Convert to lowercase
    name = name.lower()

    # Expand NYC early to preserve "new york city"
    name = re.sub(r'\bnyc\b', 'new york city', name)

    # Replace '+' and '&' with 'and'
    name = name.replace('+', 'and')
    name = name.replace('&', 'and')

    # Remove parentheses and their contents
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
    # Previously we removed 'the', 'of', 'for', 'and', 'to', 'a', 'in', 'on', 'office'
    # For global normalization, let's consider retaining 'and' and 'office' 
    # to preserve structural meaning for matching.
    # Remove minimal stopwords: 'the', 'of', 'for', 'to', 'a', 'in', 'on'
    stopwords = {'the', 'of', 'for', 'to', 'a', 'in', 'on'}
    words = [w for w in name.split() if w not in stopwords]
    name = ' '.join(words)

    return name.strip()

def global_normalize_name(name: str) -> str:
    """
    Applies global normalization rules to ensure consistent NameNormalized values
    across the entire dataset. Builds on standardize_name but makes more 
    globally-aware decisions now that all data is integrated.

    Enhancements for global step:
    - Ensure 'new york city' is preserved if present.
    - Retain 'and' and 'office' to preserve essential structure.
    - Confirm special terms like 'commission', 'department', 'authority' remain.
    - Remove extraneous punctuation and parentheses again if any remain.
    - Consider future improvements if needed.

    Returns:
        str: A globally normalized name string.
    """
    if not isinstance(name, str):
        return ''

    # Start with the base standardization
    normalized = standardize_name(name)

    # Additional global refinements:
    # Ensure 'new york city' remains as a single unit if present
    # Already handled by early replacements, but let's ensure no further splitting:
    # If after standardization 'new', 'york', 'city' appear separately, it's fine 
    # as long as they remain together. No special handling needed now.

    # TODO: Future steps: handle acronyms and canonical forms across all data sources.
    # e.g., If we find 'hhs' frequently standing for 'health and human services',
    # we could expand it here in a future iteration.

    return normalized