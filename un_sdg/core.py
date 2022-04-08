import os
from xxlimited import new
import pandas as pd
import json
import itertools
import math
import numpy as np
import requests
import pdfminer.high_level
import pdfminer.layout
import io
import re
import datetime


from typing import List, Tuple
from un_sdg import CONFIGPATH, OUTPATH, METAPATH
from pdfminer.high_level import extract_text


def extract_datapoints(df: pd.DataFrame) -> pd.DataFrame:
    if df.duplicated(subset=["country", "TimePeriod"]).sum() > 0:
        df.to_csv("un_sdg/output/duplicate_country_year.csv")
    assert df.duplicated(subset=["country", "TimePeriod"]).sum() == 0
    return pd.DataFrame(
        {"country": df["country"], "year": df["TimePeriod"], "value": df["Value"]}
    ).dropna()


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
    # adding an nan code for each id - a problem for the Coverage dimension
    nan_data = {
        "id": dim_dict.id.unique(),
        "code": np.repeat(np.nan, len(dim_dict.id.unique()), axis=0),
        "description": np.repeat("", len(dim_dict.id.unique()), axis=0),
    }
    nan_df = pd.DataFrame(nan_data)
    dim_dict = pd.concat([dim_dict, nan_df])
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


def generate_tables_for_indicator_and_series(
    data_series: pd.DataFrame,
    init_dimensions: tuple,
    init_non_dimensions: tuple,
    dim_dict: dict,
) -> pd.DataFrame:
    tables_by_combination = {}
    data_dimensions, dimensions, dimension_values = get_series_with_relevant_dimensions(
        data_series, init_dimensions, init_non_dimensions
    )
    if len(dimensions) == 0:  # not the best solution.
        # no additional dimensions
        export = data_dimensions
        return export
    else:
        dim_desc = (
            dim_dict.set_index("id")
            .loc[dimensions]
            .set_index("code")
            .squeeze()
            .to_dict()
        )
        dim_desc["nan"] = ""
        i = 0
        # Mapping the dimension value codes to more meaningful descriptions
        for i in range(len(dimension_values)):
            df = pd.DataFrame({"value": dimension_values[i]})
            df["value"] = df["value"].astype(str)
            dimension_values[i] = [dim_desc[k] for k in df["value"].to_list()]
        # Mapping the descriptions into the dataframe
        for dim in dimensions:
            data_dimensions[dim] = data_dimensions[dim].astype(str)
            data_dimensions[dim] = [dim_desc[k] for k in data_dimensions[dim]]
        # Create each combination of dimension values, e.g. each age group & sex combination. Not all combinations will have associated data.
        for dimension_value_combination in itertools.product(*dimension_values):
            # build filter by reducing, start with a constant True boolean array
            filt = [True] * len(data_dimensions)
            for dim_idx, dim_value in enumerate(dimension_value_combination):
                dimension_name = dimensions[dim_idx]
                value_is_nan = type(dim_value) == float and math.isnan(dim_value)
                # Boolean identifying which rows contain the dimension combination
                filt = filt & (
                    data_dimensions[dimension_name].isnull()
                    if value_is_nan
                    else data_dimensions[dimension_name] == dim_value
                )
                # Pulling out the data for a given combination
                tables_by_combination[dimension_value_combination] = data_dimensions[
                    filt
                ].drop(dimensions, axis=1)
                # Removing tables for the combinations that don't exist
                tables_by_combination = {
                    k: v for (k, v) in tables_by_combination.items() if not v.empty
                }  # removing empty combinations
    return tables_by_combination


def get_series_with_relevant_dimensions(
    data_series: pd.DataFrame, init_dimensions: tuple, init_non_dimensions: tuple
) -> Tuple[pd.DataFrame, list, list]:
    """For a given indicator and series, return a tuple:

    - data filtered to that indicator and series
    - names of relevant dimensions
    - unique values for each relevant dimension
    """
    non_null_dimensions_columns = [
        col for col in init_dimensions if data_series.loc[:, col].notna().any()
    ]
    dimension_names = []
    dimension_unique_values = []

    for c in non_null_dimensions_columns:
        uniques = data_series[c].unique()
        if (
            len(uniques) > 1
        ):  # Means that columns where the value doesn't change aren't included e.g. Nature is typically consistent across a dimension whereas Age and Sex are less likely to be.
            dimension_names.append(c)
            dimension_unique_values.append(list(uniques))
    return (
        data_series[
            data_series.columns.intersection(
                list(init_non_dimensions) + list(dimension_names)
            )
        ],
        dimension_names,
        dimension_unique_values,
    )


def extract_description(pdf_path):
    laparams = pdfminer.layout.LAParams()
    for param in (
        "all_texts",
        "detect_vertical",
        "word_margin",
        "char_margin",
        "line_margin",
        "boxes_flow",
    ):
        paramv = locals().get(param, None)
        if paramv is not None:
            setattr(laparams, param, paramv)

    inputf = open(pdf_path, "rb")
    ff = io.StringIO()
    pdfminer.high_level.extract_text_to_fp(inputf, ff, laparams=laparams)
    inputf.close()

    converted_text = ff.getvalue()

    return converted_text


def extract_meta_text(file_path: str) -> dict:
    now = datetime.datetime.now()
    ext_txt = extract_text(file_path)
    file_name = re.sub("Metadata-", "", re.sub(".pdf", "", os.path.basename(file_path)))

    # There are a few different formats for the metadata pdfs, meaning that we need to use slightly different splits.
    ext_txt_meth = ext_txt.split("Methodology")[0]
    ext_txt_2b = ext_txt.split("2.b. Unit of measure")[0]

    # We want to know which split has worked, this should be the shorter one.
    test = [ext_txt_meth, ext_txt_2b]
    ind = np.argmin([len(item) for item in test])

    # Pass the shorter source description onto the rest of the function.
    if ind == 0:
        ext_txt_sub = ext_txt_meth
    if ind == 1:
        ext_txt_sub = ext_txt_2b

    # Remove anything that is before 'Definition:' in the metadata. Mostly empty space and details on goal, target, indicator.
    sd = ext_txt_sub[ext_txt_sub.find("Definition:") :]
    s = re.sub(
        r"\n\n\x0cLast updated:*.*(now.year-9|now.year-8|now.year-7|now.year-6||now.year-5||now.year-4|now.year-3|now.year-2|now.year-1|now.year|now.year+1) \n\n",
        "",
        sd,
        flags=re.I,
    )
    metadata_update = {"indicator": file_name, "metadata": s}
    return metadata_update


def get_metadata_link(indicator: str) -> None:
    url = os.path.join(
        "https://unstats.un.org/sdgs/metadata/files/", "Metadata-%s.pdf"
    ) % "-".join([part.rjust(2, "0") for part in indicator.split(".")])
    r = requests.head(url)
    ctype = r.headers["Content-Type"]
    if ctype == "application/pdf":
        url_out = url
    elif ctype == "text/html":
        url_a = os.path.join(
            "https://unstats.un.org/sdgs/metadata/files/", "Metadata-%sa.pdf"
        ) % "-".join([part.rjust(2, "0") for part in indicator.split(".")])
        url_b = os.path.join(
            "https://unstats.un.org/sdgs/metadata/files/", "Metadata-%sb.pdf"
        ) % "-".join([part.rjust(2, "0") for part in indicator.split(".")])
        url_out = url_a + " and " + url_b
        url_check = requests.head(url_a)
        ctype_a = url_check.headers["Content-Type"]
        assert ctype_a == "application/pdf", url_a + "does not link to a pdf"
    return url_out


def create_comb_omm(
    variables: pd.DataFrame, var_stub: str, new_var_name: str
) -> pd.DataFrame:
    variable_str = var_stub
    vars = variables[variables["name"].str.startswith(variable_str)]
    vars = vars[
        vars["name"]
        != "15.9.1 - Countries that established national targets in accordance with Aichi Biodiversity Target 2 of the Strategic Plan for Biodiversity 2011-2020 in their National Biodiversity Strategy and Action Plans (1 = YES; 0 = NO) - ER_BDY_ABT2NP - No breakdown"
    ]
    vars_to_comb_id = vars["id"].to_list()
    vars_to_comb_name = (
        vars["name"].str.replace(variable_str, "", regex=False).to_list()
    )
    new_var_df = []
    new_var_id = variables["id"].max() + 1
    for var_id in vars_to_comb_id:
        df_var_id = pd.read_csv(
            os.path.join(OUTPATH, "datapoints", "datapoints_%d.csv" % var_id)
        )
        df_var_id["value"] = vars_to_comb_name[vars_to_comb_id.index(var_id)]
        new_var_df.append(df_var_id)
    pd.concat(new_var_df, ignore_index=True).to_csv(
        os.path.join(OUTPATH, "datapoints", "datapoints_%d.csv" % new_var_id),
        index=False,
    )
    new_var = vars.head(1)
    new_var["id"] = new_var_id
    new_var["name"] = new_var_name
    variables = pd.concat([variables, new_var], ignore_index=True)
    return variables


def create_omms(variables: pd.DataFrame) -> pd.DataFrame:
    variables = create_comb_omm(
        variables,
        var_stub="12.7.1 - Number of countries implementing sustainable public procurement policies and action plans - SG_SCP_PROCN - ",
        new_var_name="12.7.1 - Number of countries implementing sustainable public procurement policies and action plans - SG_SCP_PROCN - OMM",
    )

    variables = create_comb_omm(
        variables,
        var_stub="15.9.1 - Countries that established national targets in accordance with Aichi Biodiversity Target 2 of the Strategic Plan for Biodiversity 2011-2020 in their National Biodiversity Strategy and Action Plans (1 = YES; 0 = NO) - ER_BDY_ABT2NP - ",
        new_var_name="15.9.1 - Countries that established national targets in accordance with Aichi Biodiversity Target 2 of the Strategic Plan for Biodiversity 2011-2020 in their National Biodiversity Strategy and Action Plans (1 = YES; 0 = NO) - ER_BDY_ABT2NP - OMM",
    )
    return variables
