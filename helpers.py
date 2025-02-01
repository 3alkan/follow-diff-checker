import pandas as pd
from constants import *
import json
import os

def json2df(name, record_path=None, meta=None, max_level=2):
    """
    Converts a JSON file into a pandas DataFrame.
    
    Args:
        name (str): Name of the JSON file.
        record_path (str or list, optional): Path in the JSON to the records to normalize.
        meta (list, optional): Fields to use as metadata when flattening nested JSON.
        max_level (int, optional): Maximum level to flatten nested JSON. Default is 2.
        
    Returns:
        pd.DataFrame: A flattened DataFrame from the JSON file.
    """
    # Load JSON file
    path=os.path.join(data_folder,f"{name}.json")
    with open(path, 'r') as file:
        data = json.load(file)
    
    # Normalize nested JSON structure
    df = pd.json_normalize(
        data,
        record_path=record_path,  # Path to the records to normalize
        meta=meta,                # Metadata to include
        max_level=max_level       # Maximum level to flatten
    )
    
    return df

def saveDF_As_PKL(df:pd.DataFrame, name:str):
    """Saves a DataFrame as a Pickle file with the given name."""
    path=os.path.join(pkl_export,f"{name}.pkl")
    df.to_pickle(path)

def loadDF_From_PKL(name:str):
    """Loads a DataFrame from a Pickle file with the given name."""
    path=os.path.join(pkl_export,f"{name}.pkl")
    df = pd.read_pickle(path)
    return df

def saveDF_As_CSV(df:pd.DataFrame, name:str):
    """Saves a DataFrame as a Pickle file with the given name."""
    path=os.path.join(csv_export,f"{name}.csv")
    df.to_csv(path)

def loadDF_From_CSV(name:str):
    """Loads a DataFrame from a Pickle file with the given name."""
    path=os.path.join(csv_export,f"{name}.csv")
    df = pd.read_csv(path)
    return df
