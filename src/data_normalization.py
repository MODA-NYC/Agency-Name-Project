
import re

def standardize_name(name):
    if not isinstance(name, str):
        return ''
    # Convert to lowercase
    name = name.lower()
    # Remove content in parentheses
    name = re.sub(r'\(.*?\)', '', name)
    # Remove possessives, e.g., "mayor's" -> "mayor"
    name = re.sub(r"'s\b", '', name)
    # Replace '&' with 'and'
    name = name.replace('&', 'and')
    # Remove punctuation
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
        'cuny': 'city university new york',
        'nyc': 'new york city',
        # Add more abbreviations as needed
    }
    # Expand abbreviations
    words = name.split()
    expanded_words = [abbreviations.get(word, word) for word in words]
    name = ' '.join(expanded_words)
    # Remove common stopwords
    stopwords = {'the', 'of', 'for', 'and', 'to', 'a', 'in', 'on'}
    words = [word for word in name.split() if word not in stopwords]
    name = ' '.join(words)
    return name


