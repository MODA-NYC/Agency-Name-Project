import os
import pandas as pd

# Set BASE_PATH to the project root directory
BASE_PATH = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))

def get_file_path(*paths):
    full_path = os.path.join(BASE_PATH, *paths)
    print(f"Attempting to access: {full_path}")
    return full_path

def load_csv_data(file_path):
    try:
        df = pd.read_csv(file_path)
        print(f"Loaded data from {file_path}")
        return df
    except FileNotFoundError:
        print(f"Error: The file '{file_path}' does not exist.")
        return None