"""Converts a folder's output to a single csv for manual import to 
https://owid.cloud/admin/import.

Usage::

    python -m standard_importer.create_manual_import
"""

import os
import re
import logging
from glob import glob
import pandas as pd
from tqdm import tqdm

logging.basicConfig()
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

DATASET_DIR = "who_gho"  # directory of dataset to import.

def main():
    create_manual_csv_import(DATASET_DIR)

def create_manual_csv_import(dataset_dir: str) -> None:
    """Converts a folder's output to a single csv for manual import to
    https://owid.cloud/admin/import.
    """
    datapoint_files = glob(os.path.join(dataset_dir, "output", "datapoints/datapoints_*.csv"))
    df_variables = pd.read_csv(os.path.join(dataset_dir, "output", "variables.csv"))
    variable_id2name = df_variables.set_index('id')['name'].to_dict()
    dataframes = []
    for datapoint_file in tqdm(datapoint_files):
        variable_id = int(re.search("\\d+", datapoint_file)[0])
        df_data = pd.read_csv(datapoint_file) \
                    .rename(columns={'value': variable_id2name[variable_id]}) \
                    .set_index(['country', 'year'])
        dataframes.append(df_data)
    df_data = pd.concat(dataframes, axis=1).reset_index()
    assert df_data.shape[0] > 0
    assert df_data.shape[1] == (2 + len(datapoint_files))
    outfpath = os.path.join(dataset_dir, "output", "manual_import.csv")
    df_data.to_csv(outfpath, index=False)
    logger.info(f'Dataset CSV saved to {outfpath}')

if __name__ == '__main__':
    main()
