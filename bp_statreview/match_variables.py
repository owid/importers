"""Creates a `variable_replacements.json` file of {old variable id} to
{new variable id} key-value pairs.
"""

import os
import simplejson as json
import pandas as pd

from db import get_connection
from bp_statreview import CONFIGPATH, OUTPATH, DATASET_NAMESPACE


def main():
    with open(os.path.join(CONFIGPATH, "variable_replacements_by_name.json"), "r") as f:
        df_replacements = pd.DataFrame(json.load(f)["replacements"])

    with get_connection() as conn:
        old_rows = []
        for datasetId, gp in df_replacements.groupby("oldDatasetId"):
            old_rows += pd.read_sql(
                f"""
                SELECT id, name, datasetId
                FROM variables
                WHERE name IN ({", ".join([f'"{nm}"' for nm in gp['oldName'].tolist()])})
                    AND datasetId={datasetId}
            """,
                conn,
            ).to_dict(orient="records")
        old_name2id = {row["name"]: row["id"] for row in old_rows}

    with get_connection() as conn:
        new_rows = pd.read_sql(
            f"""
            SELECT id, name, datasetId
            FROM variables
            WHERE name IN ({", ".join([f'"{nm}"' for nm in gp['newName'].tolist()])})
                AND datasetId IN (
                    SELECT id
                    FROM datasets
                    WHERE namespace="{DATASET_NAMESPACE}"
                )
        """,
            conn,
        ).to_dict(orient="records")
        new_name2id = {row["name"]: row["id"] for row in new_rows}

    old_id2new_id = {}
    for _, row in df_replacements.iterrows():
        old_id = old_name2id.get(row.oldName)
        new_id = new_name2id.get(row.newName)
        if pd.notnull(old_id) and pd.notnull(new_id):
            old_id2new_id[old_id] = new_id

    if not os.path.exists(OUTPATH):
        os.makedirs(OUTPATH)
    with open(os.path.join(OUTPATH, "variable_replacements.json"), "w") as f:
        json.dump(old_id2new_id, f, indent=2)
