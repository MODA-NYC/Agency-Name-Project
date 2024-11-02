import pandas as pd

mappings = {
    'development_entities': {
        'source_names': [
            'Brooklyn Bridge Park Corporation',
            'Brooklyn Bridge Park Development Corporation',
            'Convention Center Development Corporation',
            'Convention Center Operating Corporation (Javits)',
            'Land Development Corporation'
        ],
        'final_name': 'Brooklyn Bridge Park Corporation',  # or appropriate consolidated name
        'reason': 'Related development entities consolidated'
    },
    'advisory_boards': {
        'source_names': [
            'Gender and Racial Equity Advisory Board',
            'Medical Equity Advisory Board'
        ],
        'parent_org': 'Commission on Gender Equity',  # or appropriate parent org
        'reason': 'Advisory boards tracked under parent organization'
    },
    'duplicate_variations': {
        'Brooklyn Public Library (BPL)': 'Brooklyn Public Library',
        'Library, Brooklyn Public (BPL)': 'Brooklyn Public Library',
        'Office of Technology and Innovation, New York City': 'Office of Technology and Innovation',
        'Technology and Innovation, NYC Office of (OTI)': 'Office of Technology and Innovation'
    }
} 