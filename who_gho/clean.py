"""Cleans data from the World Health Organization Global Health Observatory (WHO GHO).

https://www.who.int/data/gho/info/gho-odata-api

Usage:

    python -m who_gho.clean

Instructions for manually standardizing entity names:

0. Retrieve all unique entity names in the dataset:

1. Open the OWID Country Standardizer Tool
   (https://owid.cloud/admin/standardize);

2. Change the "Input Format" field to "Non-Standard Country Name";

3. Change the "Output Format" field to "Our World In Data Name";

4. In the "Choose CSV file" field, upload {outfpath};

5. For any country codes that do NOT get matched, enter a custom name on
   the webpage (in the "Or enter a Custom Name" table column);

    * NOTE: For this dataset, you will most likely need to enter custom
      names for regions/continents;

6. Click the "Download csv" button;

7. Replace {outfpath} with the downloaded CSV;

"""
import os
import pandas as pd
from ihme_gbd.gbd_tools import make_dirs


from who_gho import (
    CONFIGPATH,
    INPATH,
    SELECTED_VARS_ONLY,
    OUTPATH,
    FIX_VAR_CODE,
    DELETE_OUTPUT,
    DATASET_SOURCENAME,
)

from who_gho.core import (
    clean_datasets,
    get_metadata_url,
    get_variable_codes,
    clean_sources,
    load_all_data_and_add_variable_name,
    get_distinct_entities,
    get_metadata,
    remove_empty_rows,
    standardise_country_name,
    clean_variables,
    delete_output,
)


def main() -> None:
    if DELETE_OUTPUT:
        delete_output(
            keep_paths=[],
            outpath=OUTPATH,
        )
    make_dirs(inpath=INPATH, outpath=OUTPATH, configpath=CONFIGPATH)

    df_datasets = clean_datasets()
    assert (
        df_datasets.shape[0] == 1
    ), f"Only expected one dataset in {os.path.join(OUTPATH, 'datasets.csv')}."

    df_sources = clean_sources(
        dataset_id=df_datasets["id"].iloc[0],
        dataset_name=DATASET_SOURCENAME,
    )

    variable_codes = get_variable_codes(selected_vars_only=SELECTED_VARS_ONLY)

    code2url, code2name = get_metadata_url(fix_var_code=FIX_VAR_CODE)

    var_code2meta = get_metadata(var_code2url=code2url)

    df = load_all_data_and_add_variable_name(
        variables=variable_codes, var_code2name=code2name
    )

    df = remove_empty_rows(df)

    df["country"] = standardise_country_name(country_col=df["SpatialDim"])

    assert df[df["country"].isnull()].shape[0] == 0
    df_variables = clean_variables(df, var_code2meta)

    df_distinct_entities = pd.DataFrame(get_distinct_entities(), columns=["name"])

    # saves datasets, sources, variables, and distinct entities to disk.
    df_datasets.to_csv(os.path.join(OUTPATH, "datasets.csv"), index=False)
    df_sources.to_csv(os.path.join(OUTPATH, "sources.csv"), index=False)
    df_variables.to_csv(os.path.join(OUTPATH, "variables.csv"), index=False)

    df_distinct_entities.to_csv(
        os.path.join(OUTPATH, "distinct_countries_standardized.csv"), index=False
    )


if __name__ == "__main__":
    main()
