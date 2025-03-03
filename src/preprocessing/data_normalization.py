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
    - Ensures 'new york city' is preserved if present
    - Retains relevant words like 'office', 'department', 'commission'
    - Standardizes organizational terms (e.g., 'dept' -> 'department')
    - Handles special cases like 'mayors office' vs 'office of the mayor'
    - Removes redundant terms while preserving meaning

    Returns:
        str: A globally normalized name string.
    """
    if not isinstance(name, str):
        return ''

    # Start with the base standardization
    normalized = standardize_name(name)

    # Global organizational term standardization
    org_terms = {
        'dept': 'department',
        'comm': 'commission',
        'auth': 'authority',
        'admin': 'administration',
        'corp': 'corporation',
        'dev': 'development',
        'svcs': 'services',
        'svc': 'service',
        'tech': 'technology',
        'mgmt': 'management',
        'ops': 'operations'
    }

    # Split into words for processing
    words = normalized.split()
    
    # Process words
    processed_words = []
    i = 0
    while i < len(words):
        word = words[i]
        
        # Handle special cases
        if i < len(words) - 1:
            two_word = f"{word} {words[i+1]}"
            if two_word == "mayors office":
                processed_words.extend(["office", "of", "the", "mayor"])
                i += 2
                continue
        
        # Expand organizational terms
        if word in org_terms:
            processed_words.append(org_terms[word])
        else:
            processed_words.append(word)
        i += 1

    # Rejoin words
    normalized = ' '.join(processed_words)

    # Ensure 'new york city' is preserved if present
    if 'new york city' in normalized:
        normalized = normalized.replace('nyc', 'new york city')

    return normalized.strip()