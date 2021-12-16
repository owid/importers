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
import logging
import pandas as pd
from pandas.api.types import is_numeric_dtype

from who_gho import (
    CONFIGPATH,
    SELECTED_VARS_ONLY,
    OUTPATH,
    KEEP_PATHS,
    DOWNLOAD_INPUTS,
    DELETE_EXISTING_INPUTS,
    INPATH,
)

from who_gho.core import (
    clean_datasets,
    get_metadata_url,
    get_variable_codes,
    clean_and_create_datapoints,
    clean_sources,
    clean_variables,
    get_distinct_entities,
    get_metadata,
)


logging.basicConfig()
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)


def main() -> None:

    # cleans datasets, datapoints, variables, and sources.
    df_datasets = clean_datasets()
    assert (
        df_datasets.shape[0] == 1
    ), f"Only expected one dataset in {os.path.join(OUTPATH, 'datasets.csv')}."

    df_sources = clean_sources(
        dataset_id=df_datasets["id"].iloc[0],
        dataset_name=df_datasets["name"].iloc[0],
    )

    variable_codes = get_variable_codes(selected_vars_only=SELECTED_VARS_ONLY)

    code2url, code2name = get_metadata_url()

    var_code2meta = get_metadata(var_code2url=code2url)

    df_variables = clean_variables(
        variables=variable_codes,
        var_code2meta=var_code2meta,
        var_code2name = code2name,
    )

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
