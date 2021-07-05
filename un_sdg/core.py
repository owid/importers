import os
import pandas as pd
import json
import itertools
import math
import numpy as np
import requests

from typing import List, Tuple
from un_sdg import OUTPATH


def extract_datapoints(df: pd.DataFrame) -> pd.DataFrame:
    return (
        pd.DataFrame(
            {"country": df["country"], "year": df["TimePeriod"], "value": df["Value"]}
        )
        .drop_duplicates(subset=["country", "year"])
        .dropna()
    )


def get_distinct_entities() -> List[str]:
    """retrieves a list of all distinct entities that contain at least
    one non-null data point that was saved to disk from the
    `create_variables_datapoints()` method.
    Returns:
        entities: List[str]. List of distinct entity names.
    """
    fnames = [
        fname
        for fname in os.listdir(os.path.join(OUTPATH, "datapoints"))
        if fname.endswith(".csv")
    ]
    entities = set({})
    for fname in fnames:
        df_temp = pd.read_csv(os.path.join(OUTPATH, "datapoints", fname))
        entities.update(df_temp["country"].unique().tolist())

    entities = list(entities)
    assert pd.notnull(entities).all(), (
        "All entities should be non-null. Something went wrong in "
        "`create_variables_datapoints()`."
    )
    return entities


def clean_datasets(
    DATASET_NAME: str, DATASET_AUTHORS: str, DATASET_VERSION: str
) -> pd.DataFrame:
    """Constructs a dataframe where each row represents a dataset to be
    upserted.
    Note: often, this dataframe will only consist of a single row.
    """
    data = [
        {"id": 0, "name": f"{DATASET_NAME} - {DATASET_AUTHORS} ({DATASET_VERSION})"}
    ]
    df = pd.DataFrame(data)
    return df


def dimensions_description() -> pd.DataFrame:
    base_url = "https://unstats.un.org/sdgapi"
    # retrieves all goal codes
    url = f"{base_url}/v1/sdg/Goal/List"
    res = requests.get(url)
    assert res.ok
    goals = json.loads(res.content)
    goal_codes = [int(goal["code"]) for goal in goals]
    # retrieves all area codes
    d = []
    for goal in goal_codes:
        url = f"{base_url}/v1/sdg/Goal/{goal}/Dimensions"
        res = requests.get(url)
        assert res.ok
        dims = json.loads(res.content)
        for dim in dims:
            for code in dim["codes"]:
                d.append(
                    {
                        "id": dim["id"],
                        "code": code["code"],
                        "description": code["description"],
                    }
                )
    dim_dict = pd.DataFrame(d).drop_duplicates()
    return dim_dict


def attributes_description() -> dict:
    base_url = "https://unstats.un.org/sdgapi"
    # retrieves all goal codes
    url = f"{base_url}/v1/sdg/Goal/List"
    res = requests.get(url)
    assert res.ok
    goals = json.loads(res.content)
    goal_codes = [int(goal["code"]) for goal in goals]
    # retrieves all area codes
    a = []
    for goal in goal_codes:
        url = f"{base_url}/v1/sdg/Goal/{goal}/Attributes"
        res = requests.get(url)
        assert res.ok
        attr = json.loads(res.content)
        for att in attr:
            for code in att["codes"]:
                a.append(
                    {
                        "code": code["code"],
                        "description": code["description"],
                    }
                )
    att_dict = pd.DataFrame(a).drop_duplicates().set_index("code").squeeze().to_dict()
    return att_dict


def create_short_unit(long_unit: pd.Series) -> np.ndarray:

    conditions = [
        (long_unit.str.contains("PERCENT")) | (long_unit.str.contains("Percentage")),
        (long_unit.str.contains("KG")) | (long_unit.str.contains("Kilograms")),
        (long_unit.str.contains("USD")) | (long_unit.str.contains("usd")),
    ]

    choices = ["%", "kg", "$"]

    short_unit = np.select(conditions, choices, default=None)
    return short_unit


def get_series_with_relevant_dimensions(
    data_filtered: pd.DataFrame, DIMENSIONS: tuple, NON_DIMENSIONS: tuple
) -> Tuple[pd.DataFrame, list, list]:
    """For a given indicator and series, return a tuple:

    - data filtered to that indicator and series
    - names of relevant dimensions
    - unique values for each relevant dimension
    """
    non_null_dimensions_columns = [
        col for col in DIMENSIONS if data_filtered.loc[:, col].notna().any()
    ]
    dimension_names = []
    dimension_unique_values = []

    for c in non_null_dimensions_columns:
        uniques = data_filtered[c].unique()
        if (
            len(uniques) > 1
        ):  # Means that columns where the value doesn't change aren't included e.g. Nature is typically consistent across a dimension whereas Age and Sex are less likely to be.
            dimension_names.append(c)
            dimension_unique_values.append(list(uniques))
    return (
        data_filtered[
            data_filtered.columns.intersection(
                list(NON_DIMENSIONS) + list(dimension_names)
            )
        ],
        dimension_names,
        dimension_unique_values,
    )


def generate_tables_for_indicator_and_series(
    data_filtered: pd.DataFrame,
    DIMENSIONS: tuple,
    NON_DIMENSIONS: tuple,
    dim_dict: dict,
) -> pd.DataFrame:
    tables_by_combination = {}
    # dim_dict = dimensions_description()
    data_filtered, dimensions, dimension_values = get_series_with_relevant_dimensions(
        data_filtered, DIMENSIONS, NON_DIMENSIONS
    )
    if (len(dimensions) == 0) | (
        data_filtered[dimensions].isna().sum().sum() > 0
    ):  # not the best solution.
        # no additional dimensions
        export = data_filtered
        return export
    else:
        dim_desc = (
            dim_dict.set_index("id")
            .loc[dimensions]
            .set_index("code")
            .squeeze()
            .to_dict()
        )
        i = 0
        for i in range(len(dimension_values)):
            dimension_values[i] = [dim_desc[k] for k in dimension_values[i]]
        for dim in dimensions:
            data_filtered[dim] = data_filtered[dim].apply(lambda x: dim_desc[x])
        for dimension_value_combination in itertools.product(*dimension_values):
            # build filter by reducing, start with a constant True boolean array
            filt = [True] * len(data_filtered)
            for dim_idx, dim_value in enumerate(dimension_value_combination):
                dimension_name = dimensions[dim_idx]
                value_is_nan = type(dim_value) == float and math.isnan(dim_value)
                filt = filt & (
                    data_filtered[dimension_name].isnull()
                    if value_is_nan
                    else data_filtered[dimension_name] == dim_value
                )
                tables_by_combination[dimension_value_combination] = data_filtered[
                    filt
                ].drop(dimensions, axis=1)
                tables_by_combination = {
                    k: v for (k, v) in tables_by_combination.items() if not v.empty
                }  # removing empty combinations
    return tables_by_combination
