"""Creates a `variable_replacements.json` file of {old variable id} to
{new variable id} key-value pairs.
"""

import logging
import os

import numpy as np
import pandas as pd
import simplejson as json

from bp_statreview import CONFIGPATH, OUTPATH, DATASET_FULL_NAME_PREVIOUS, DATASET_FULL_NAME
from db import get_connection

logging.basicConfig()
logger = logging.getLogger(__name__)


def get_dataset_id(dataset_name, cursor):
    query = f"""
            SELECT id
            FROM datasets
            WHERE name = "{dataset_name}";
            """
    cursor.execute(query)
    result = cursor.fetchall()
    assert len(result) == 1, f"Dataset {dataset_name} not found."
    dataset_id = result[0][0]

    return dataset_id


def get_variable_id(dataset_id, variable_name, cursor):
    query = f"""
            SELECT *
            FROM variables
            WHERE datasetId = {dataset_id}
            AND name = '{variable_name}'
            """
    cursor.execute(query)
    result = cursor.fetchall()
    if len(result) == 1:
        variable_id = result[0][0]
    elif len(result) == 0:
        logger.warning(f"Variable {variable_name} not found.")
        variable_id = None
    else:
        logger.warning(f"Multiple variables named {variable_name} found.")
        variable_id = None
    return variable_id


def get_all_variable_names_in_dataset(dataset_id, cursor):
    query = f"""
        SELECT
        DISTINCT(name)
        FROM variables v
        WHERE datasetId = {dataset_id}
        """
    cursor.execute(query)
    result = sorted(np.squeeze(cursor.fetchall()))

    return result


def main():
    with open(os.path.join(CONFIGPATH, "variable_replacements_by_name.json"), "r") as f:
        df_replacements = pd.DataFrame(json.load(f)["replacements"])

    # Initialise dictionary mapping old variable ids to new variable ids.
    map_old_to_new_variable_ids = {}
    with get_connection() as conn:
        cursor = conn.cursor()
        # Get dataset id of the previous version of the dataset (the one currently published).
        old_dataset_id = get_dataset_id(dataset_name=DATASET_FULL_NAME_PREVIOUS, cursor=cursor)
        # Get dataset id of the new version of the dataset (the one to be published).
        new_dataset_id = get_dataset_id(dataset_name=DATASET_FULL_NAME, cursor=cursor)
        # Get all variable names in the old dataset.
        old_variable_names = get_all_variable_names_in_dataset(dataset_id=old_dataset_id, cursor=cursor)

        for old_variable_name in old_variable_names:
            # If the old variable is cited in the `variable_replacements_by_name.json` file, take the new variable name
            # from there. Otherwise, assume the new variable name is the same as the old.
            renaming = df_replacements[df_replacements["old_name"] == old_variable_name]
            if len(renaming) == 1:
                new_variable_name = renaming["new_name"].item()
            else:
                new_variable_name = old_variable_name

            # Get old and new variable ids.
            old_variable_id = get_variable_id(dataset_id=old_dataset_id, variable_name=old_variable_name, cursor=cursor)
            new_variable_id = get_variable_id(dataset_id=new_dataset_id, variable_name=new_variable_name, cursor=cursor)

            # Add to the mapping of old to new variables only those pairs where both variables where found.
            if (old_variable_id is not None) and (new_variable_id is not None):
                map_old_to_new_variable_ids[str(old_variable_id)] = str(new_variable_id)

    # Save mapping of old to new variable ids into a json file.
    if not os.path.exists(OUTPATH):
        os.makedirs(OUTPATH)
    with open(os.path.join(OUTPATH, "variable_replacements.json"), "w") as f:
        json.dump(map_old_to_new_variable_ids, f, indent=2)
