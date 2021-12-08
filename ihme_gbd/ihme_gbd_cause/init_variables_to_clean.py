import os
import simplejson as json
import pandas as pd
from typing import List
import re

from db import get_connection
from db_utils import DBUtils

from ihme_gbd.ihme_gbd_cause import (
    CONFIGPATH,
    FILTER_FIELDS,
    INPATH,
    NAMESPACE,
    OUTPATH,
)
from ihme_gbd.match_variables import get_datasets
from ihme_gbd.gbd_tools import get_variable_names

CUSTOM_FNAME = "custom_variable_replacements.json"


def main():
    variables_to_clean = get_variables_to_clean_from_string_matches()

    assert len(variables_to_clean) == len(set(variables_to_clean)), (
        "There are one or more duplicate variable names in the constructed "
        "array of variables to clean. Expected 0 duplicate variable names."
    )

    variables_to_clean = sorted(variables_to_clean)

    with open(os.path.join(CONFIGPATH, "variables_to_clean.json"), "w") as f:
        json.dump(
            {
                "meta": {
                    "notes": "This file contains an array of IHME GBD "
                    "variables to upsert to SQL. Any variables NOT in this file will be "
                    "ignored."
                },
                "variables": variables_to_clean,
            },
            f,
            ignore_nan=True,
            indent=4,
        )


def get_variables_to_clean_from_string_matches() -> List[dict]:
    """retrieves an array of variables to clean by retrieving all "old" variables
    that are used in at least one existing OWID chart, and then matching each of
    these old variables to a variable in the new dataset using exact string
    matching.
    """

    df_old_vars = get_old_variables(
        outpath=OUTPATH, namespace=re.sub("ihme_", "", NAMESPACE)
    ).tolist()
    df_new_vars = get_variable_names(
        inpath=INPATH, filter_fields=FILTER_FIELDS
    ).tolist()

    variables_to_clean = list(set(df_old_vars) & set(df_new_vars))

    return variables_to_clean


def get_old_variables(outpath: str, namespace: str):
    connection = get_connection()
    with connection.cursor() as cursor:
        db = DBUtils(cursor)
        # retrieves old and new datasets
        df_old_datasets = get_datasets(
            outpath=outpath, db=db, new=False, namespace=namespace
        )
        query = f"""
            SELECT *
            FROM variables
            WHERE id IN (
                SELECT DISTINCT(variableId)
                FROM chart_dimensions
            )
            AND datasetId IN ({','.join([str(_id) for _id in df_old_datasets['id']])})
            ORDER BY updatedAt DESC
        """
        df = pd.read_sql(query, connection).drop_duplicates(
            subset=["name"], keep="first"
        )

        df_old_vars = df["name"]

    return df_old_vars


if __name__ == "__main__":
    main()
