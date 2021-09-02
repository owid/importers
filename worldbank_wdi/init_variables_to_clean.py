import os
import simplejson as json
import pandas as pd
import logging
from typing import List

from db import get_connection

from worldbank_wdi import CLEAN_ALL_VARIABLES, CONFIGPATH, INPATH, OUTPATH
from worldbank_wdi.match_variables import get_datasets

logging.basicConfig()
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

CUSTOM_FNAME = "custom_variable_replacements.json"


def main():
    variables_to_clean = get_variables_to_clean_from_custom_matches()
    variables_to_clean2 = get_variables_to_clean_from_string_matches()
    if CLEAN_ALL_VARIABLES:
        variables_to_clean2 += get_unmatched_variables_to_clean()

    uniq_var_names = {var["name"] for var in variables_to_clean}
    for var in variables_to_clean2:
        if var["name"] not in uniq_var_names:
            variables_to_clean.append(var)
            uniq_var_names.add(var["name"])

    uniq_var_names = [var["name"] for var in variables_to_clean]
    assert len(uniq_var_names) == len(set(uniq_var_names)), (
        "There are one or more duplicate variable names in the constructed "
        "array of variables to clean. Expected 0 duplicate variable names."
    )

    variables_to_clean = sorted(variables_to_clean, key=lambda x: x["name"])
    with open(os.path.join(OUTPATH, "variables_to_clean.json"), "w") as f:
        json.dump(
            {
                "meta": {
                    "notes": "This file contains an array of WB WDI "
                    "variables to upsert to SQL. Also contains old metadata "
                    "for these variables, as copied from previous versions of "
                    "these variables. Any variables NOT in this file will be "
                    "ignored."
                },
                "variables": variables_to_clean,
            },
            f,
            ignore_nan=True,
            indent=4,
        )


def get_variables_to_clean_from_custom_matches() -> List[dict]:
    """retrieves an array of variables to clean based on the variables matches
    that have been manually identified in {CUSTOM_FNAME}.
    """
    if not os.path.exists(os.path.join(CONFIGPATH, CUSTOM_FNAME)):
        return []
    with open(os.path.join(CONFIGPATH, CUSTOM_FNAME), "r") as f:
        custom_variable_replacements: List[dict] = json.load(f)
        old_var_ids = [d["oldId"] for d in custom_variable_replacements]
        old_name2new_name = {
            d["oldName"]: d["newName"] for d in custom_variable_replacements
        }
    df_old_vars = pd.read_sql(
        f"""
        SELECT *
        FROM variables
        WHERE id IN ({','.join([str(_id) for _id in old_var_ids])})
        """,
        get_connection(),
    )
    df_variables = get_new_variables()

    assert df_old_vars["name"].isin(old_name2new_name).all(), (
        f"One or more `oldId` values in {CUSTOM_FNAME} "
        "does not correspond to the `oldName` value."
    )
    assert (
        len(set(old_var_ids).difference(df_old_vars["id"].unique())) == 0
    ), f"Failed to retrieve one or more `oldId` values in {CUSTOM_FNAME} from database."
    assert df_old_vars["name"].duplicated().sum() == 0, (
        "Expected 0 duplicate variable names. Something is wrong with "
        f"{CUSTOM_FNAME}."
    )

    var_name2code = df_variables.set_index("indicator_name")["indicator_code"].to_dict()
    variables_to_clean = []
    for _, row in df_old_vars.iterrows():
        new_name = old_name2new_name[row["name"]]
        new_code = var_name2code[new_name].upper()
        if row["code"] != new_code:
            logger.warning(
                f"old code != new code ({row['code']} != {new_code}). Are "
                "you sure this is the correct match?"
            )
        meta = {
            "name": new_name,
            "code": new_code,
            "old": {
                "unit": row["unit"],
                "shortUnit": row["shortUnit"],
                "display": json.loads(row["display"]),
            },
        }
        variables_to_clean.append(meta)
    return variables_to_clean


def get_variables_to_clean_from_string_matches() -> List[dict]:
    """retrieves an array of variables to clean by retrieving all "old" variables
    that are used in at least one existing OWID chart, and then matching each of
    these old variables to a variable in the new dataset using exact string
    matching.
    """
    df_old_vars = get_old_variables()
    df_variables = get_new_variables()
    var_name2code = df_variables.set_index("indicator_name")["indicator_code"].to_dict()
    variables_to_clean = []
    for _, row in df_old_vars.iterrows():
        if row["name"] in df_variables["indicator_name"].tolist():
            new_code = var_name2code[row["name"]].upper()
            assert (
                row["code"] == new_code
            ), f"old code != new code ({row['code']} != {var_name2code[row['name']]}"
            meta = {
                "name": row["name"],
                "code": new_code,
                "old": {
                    "unit": row["unit"],
                    "shortUnit": row["shortUnit"],
                    "display": json.loads(row["display"]),
                },
            }
            variables_to_clean.append(meta)
        else:
            logger.warning(
                f'Failed to find new variable to clean and replace old variable: {row["name"]}'
            )
    return variables_to_clean


def get_unmatched_variables_to_clean() -> List[dict]:
    """retrieves an array of variables to clean by retrieving all "old" variables
    that are used in at least one existing OWID chart, and then matching each of
    these old variables to a variable in the new dataset using exact string
    matching.
    """
    df_variables = get_new_variables()
    df_variables = df_variables[["indicator_name", "indicator_code"]].rename(
        columns={"indicator_name": "name", "indicator_code": "code"}
    )
    df_variables["code"] = df_variables["code"].str.upper()
    assert (
        df_variables["name"].duplicated().sum() == 0
        and df_variables["code"].duplicated().sum() == 0
    ), "There are one or more duplicate variable codes and/or variable names."
    variables_to_clean = df_variables.to_dict(orient="records")
    return variables_to_clean


def get_old_variables():
    with get_connection() as conn:
        df_old_datasets = get_datasets(conn, new=False)
        if os.path.exists(os.path.join(CONFIGPATH, CUSTOM_FNAME)):
            with open(os.path.join(CONFIGPATH, CUSTOM_FNAME), "r") as f:
                custom_variable_replacements: List[dict] = json.load(f)
                old_var_ids = [d["oldId"] for d in custom_variable_replacements]
        query = f"""
            SELECT *
            FROM variables
            WHERE id IN (
                SELECT DISTINCT(variableId)
                FROM chart_dimensions
            ) 
            AND datasetId IN ({','.join([str(_id) for _id in df_old_datasets['id']])})
            AND id NOT IN ({','.join([str(_id) for _id in old_var_ids])})
            ORDER BY updatedAt DESC
        """
        df = pd.read_sql(query, conn).drop_duplicates(subset=["name"], keep="first")
    return df


def get_new_variables():
    infpath = os.path.join(INPATH, "WDIData.csv.zip")
    df_data = pd.read_csv(infpath, compression="gzip")
    df_data.columns = df_data.columns.str.lower().str.replace(
        r"[\s/-]+", "_", regex=True
    )
    df_variables = df_data[["indicator_name", "indicator_code"]].drop_duplicates()
    return df_variables


if __name__ == "__main__":
    main()
