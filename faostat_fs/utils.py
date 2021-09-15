import json
import pandas as pd
import tempfile
import requests

# Get metadata
def read_xlsx_from_url(url):
    headers = {"User-Agent": "Mozilla/5.0 (X11; Linux i686)"}
    response = requests.get(url, headers=headers)
    with tempfile.NamedTemporaryFile() as tmp:
        with open(tmp.name, "wb") as f:
            f.write(response.content)
        return pd.read_excel(tmp.name, sheet_name=None)


def load_data(input_path: str) -> pd.DataFrame:
    """Load input CSV data.

    Args:
        input_path (str): Path to input data (csv).

    Returns:
        pd.DataFrame: Input data
    """
    df = pd.read_csv(input_path)
    # Drop nulls
    df = df.dropna(subset=["Value", "Area"])
    return df


def load_metadata_walden(input_path: str):
    """Load metadata.

    Args:
        input_path (str): Path to metadata file (JSON). Assumed to have Walden format.

    Returns:
        pd.DataFrame: Input data
    """
    with open(input_path, "r") as f:
        return json.load(f)


def load_metadata_fao(input_path: str) -> pd.DataFrame:
    """Load FAO metadata.

    Args:
        input_path (str): Path to FAO metadata (csv).

    Returns:
        pd.DataFrame: FAO metadata
    """
    df = pd.read_csv(input_path)
    return df
