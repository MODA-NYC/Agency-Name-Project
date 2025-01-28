from preprocessing.global_normalization import apply_global_normalization
from preprocessing.ops_processor import OpsDataProcessor
from preprocessing.hoo_processor import HooDataProcessor
import pandas as pd

def test_specific_cases():
    # Create test cases
    test_data = {
        'Agency Name': [
            'NYC Dept of Tech Mgmt',
            'Mayors Office of Admin Svcs',
            'Corp Counsel',
            'Auth for Dev',
            'Comm on Human Rights'
        ]
    }
    df = pd.DataFrame(test_data)
    
    # Apply global normalization
    df_global = apply_global_normalization(df)
    
    print('\nTesting Global Normalization Rules:\n')
    for i in range(len(df)):
        print(f'Before: {df.iloc[i]["Agency Name"]}')
        print(f'After:  {df_global.iloc[i]["NameNormalized"]}\n')

def main():
    # Test specific cases first
    test_specific_cases()
    
    # Original tests
    ops_processor = OpsDataProcessor()
    hoo_processor = HooDataProcessor()

    ops_df = ops_processor.process('../data/raw/ops_data.csv')
    hoo_df = hoo_processor.process('../data/raw/nyc_gov_hoo.csv')

    ops_global = apply_global_normalization(ops_df)
    hoo_global = apply_global_normalization(hoo_df)

    print('\nOPS Sample (Before/After):\n')
    for i in range(3):
        print(f'Before: {ops_df.iloc[i]["NameNormalized"]}')
        print(f'After:  {ops_global.iloc[i]["NameNormalized"]}\n')

    print('\nHOO Sample (Before/After):\n')
    for i in range(3):
        print(f'Before: {hoo_df.iloc[i]["NameNormalized"]}')
        print(f'After:  {hoo_global.iloc[i]["NameNormalized"]}\n')

if __name__ == '__main__':
    main() 