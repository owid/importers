"""Creates a `variable_replacements.json` file of {old variable id} to
{new variable id} key-value pairs.

`variable_replacements.json` is used in `suggest_chart_revisions.py` for
determining which charts to update.

Usage:

    python -m gcp_gcb.match_variables
"""

import os
import simplejson as json
from typing import List, Dict
import pandas as pd

from db import get_connection
from gcp_gcb import OUTPATH, CONFIGPATH, DATASET_NAMESPACE

CUSTOM_FNAME = "custom_variable_replacements.json"


def main():
    old_var_id2new_var_id = get_variable_replacements_from_string_matches()
    custom_old_var_id2new_var_id = get_variable_replacements_from_custom_matches()
    old_var_id2new_var_id.update(custom_old_var_id2new_var_id)

    assert all([isinstance(k, int) for k in old_var_id2new_var_id.keys()])
    assert all([isinstance(v, int) for v in old_var_id2new_var_id.values()])

    if not os.path.exists(OUTPATH):
        os.makedirs(OUTPATH)

    with open(os.path.join(OUTPATH, "variable_replacements.json"), "w") as f:
        json.dump(old_var_id2new_var_id, f, indent=2, sort_keys=True)


def get_variable_replacements_from_string_matches() -> Dict[int, int]:
    # retrieves old and new datasets
    df_old_datasets = get_datasets(new=False)
    df_new_datasets = get_datasets(new=True)
    assert df_old_datasets.shape[0] > 0
    assert df_new_datasets.shape[0] > 0

    # retrieves old and new variables
    df_old_vars = get_variables(dataset_ids=df_old_datasets["id"])
    df_new_vars = get_variables(dataset_ids=df_new_datasets["id"])

    # merges old and new variables on name
    df_vars = pd.merge(
        df_old_vars,
        df_new_vars,
        on="name",
        how="inner",
        suffixes=["_old", "_new"],
        validate="m:1",
    )
    assert df_vars.id_old.notnull().all() and df_vars.id_new.notnull().all()
    old_var_id2new_var_id = (
        df_vars.dropna(subset=["id_old", "id_new"])
        .set_index("id_old")["id_new"]
        .squeeze()
        .to_dict()
    )
    return old_var_id2new_var_id


def get_variable_replacements_from_custom_matches() -> Dict[int, int]:
    # retrieves old and new datasets
    df_old_datasets = get_datasets(new=False)
    df_new_datasets = get_datasets(new=True)
    assert df_old_datasets.shape[0] > 0
    assert df_new_datasets.shape[0] > 0

    # retrieves old and new variables
    df_old_vars = get_variables(dataset_ids=df_old_datasets["id"])
    df_new_vars = get_variables(dataset_ids=df_new_datasets["id"])

    if not os.path.exists(os.path.join(CONFIGPATH, CUSTOM_FNAME)):
        return {}

    with open(os.path.join(CONFIGPATH, CUSTOM_FNAME), "r") as f:
        df_custom_replacements = pd.DataFrame(json.load(f))
        n_expected = df_custom_replacements.shape[0]

    df_custom_replacements = df_custom_replacements.merge(
        df_new_vars[["name", "id"]],
        left_on="newName",
        right_on="name",
        how="left",
        validate="1:1",
    ).merge(
        df_old_vars[["name", "id"]],
        left_on="oldId",
        right_on="id",
        how="left",
        validate="1:1",
        suffixes=["_new", "_old"],
    )

    assert (
        df_custom_replacements["oldName"] == df_custom_replacements["name_old"]
    ).all(), (
        f"One or more old names in {CUSTOM_FNAME} do not match the "
        "name of the variable with the corresponding `oldId`."
    )
    assert df_custom_replacements.shape[0] == n_expected, (
        "Something went wrong in constructing custom replacements. "
        f"Expected {n_expected} custom replacements, but only "
        f"{df_custom_replacements.shape[0]} match a new variable name "
        "and old variable id."
    )
    assert df_custom_replacements[["oldId", "oldName"]].duplicated().sum() == 0, (
        "Expected 0 duplicate old variable id-name combinations. Something is "
        f"wrong with {CUSTOM_FNAME}."
    )
    assert df_custom_replacements["id_new"].notnull().all(), (
        "Something went wrong in constructing custom replacements. Some new Ids "
        "are NaN."
    )

    if df_custom_replacements.shape[0] == 1:
        custom_old_var_id2new_var_id = df_custom_replacements.set_index("id_old")[
            "id_new"
        ].to_dict()
    else:
        custom_old_var_id2new_var_id = (
            df_custom_replacements.set_index("id_old")["id_new"].squeeze().to_dict()
        )
    assert len(custom_old_var_id2new_var_id) == df_custom_replacements.shape[0]
    return custom_old_var_id2new_var_id


def get_datasets(new: bool = True) -> pd.DataFrame:
    """retrieves new datasets if `new=True`, else retrieves old datasets.

    Arguments:

        new: bool = True. If True, retrieves new datasets. Else retrieves
            old datasets.

    Returns:

        pd.DataFrame: dataframe of old or new datasets.
    """
    columns = ["id", "name", "createdAt", "updatedAt"]
    if new:
        datasets = pd.read_csv(os.path.join(OUTPATH, "datasets.csv"))
        if datasets.shape[0] > 1:
            raise NotImplementedError(
                "This function is not implemented for multiple new datasets in datasets.csv./"
            )
        dataset_name = datasets.iloc[0]["name"]
        query = f"""
            SELECT {','.join(columns)}
            FROM datasets
            WHERE name="{dataset_name}" AND namespace="{DATASET_NAMESPACE}"
        """
    else:
        with open(os.path.join(CONFIGPATH, "old_datasets.json"), "r") as f:
            _ids = [str(d["id"]) for d in json.load(f)["datasets"]]
        query = f"""
            SELECT {','.join(columns)}
            FROM datasets
            WHERE id IN ({','.join(_ids)})
        """
    df_datasets = pd.read_sql(query, get_connection())
    return df_datasets


def get_variables(dataset_ids: List[int]) -> pd.DataFrame:
    """retrieves all variables in dataset(s).

    Also retrieves the min year and max year of available data for each variable.

    Arguments:

        dataset_ids: List[Union[int, str]]. List of dataset ids for which
            to retrieve variables.

    Returns:

        pd.DataFrame. Dataframe of variables.
    """
    # retrieves all variables in old dataset(s)
    dataset_ids_str = ",".join([str(_id) for _id in dataset_ids])
    columns = [
        "id",
        "name",
        "description",
        "unit",
        "display",
        "createdAt",
        "updatedAt",
        "datasetId",
        "sourceId",
    ]
    df_vars = pd.read_sql(
        f"""
        SELECT {','.join(columns)}
        FROM variables
        WHERE datasetId IN ({dataset_ids_str})
    """,
        get_connection(),
    )
    return df_vars


if __name__ == "__main__":
    main()
