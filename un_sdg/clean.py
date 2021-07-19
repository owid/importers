"""
After running load_and_clean() to create $ENTFILE use the country standardiser tool to standardise $ENTFILE
1. Open the OWID Country Standardizer Tool
   (https://owid.cloud/admin/standardize);
2. Change the "Input Format" field to "Non-Standard Country Name";
3. Change the "Output Format" field to "Our World In Data Name"; 
4. In the "Choose CSV file" field, upload $ENTFILE;
5. For any country codes that do NOT get matched, enter a custom name on
   the webpage (in the "Or enter a Custom Name" table column);
    * NOTE: For this dataset, you will most likely need to enter custom
      names for regions/continents (e.g. "Arab World", "Lower middle
      income");
6. Click the "Download csv" button;
7. Name the downloaded csv 'standardized_entity_names.csv' and save in the output folder;
8. Rename the "Country" column to "country_code".
"""

import pandas as pd
import os
import shutil
import json
import numpy as np
import re
from pathlib import Path
from tqdm import tqdm

from un_sdg import (
    INFILE,
    ENTFILE,
    OUTPATH,
    CONFIGPATH,
    DATASET_NAME,
    DATASET_AUTHORS,
    DATASET_VERSION,
    DATASET_RETRIEVED_DATE,
    DATASET_NAMESPACE,
)

from un_sdg.core import (
    ihr_capacity_clean,
    create_short_unit,
    extract_datapoints,
    get_distinct_entities,
    clean_datasets,
    dimensions_description,
    attributes_description,
    create_short_unit,
    get_series_with_relevant_dimensions,
    generate_tables_for_indicator_and_series,
)

"""
load_and_clean():
- Loads in the raw data 
- Keeps rows where values in the "Value" column are not Null
- Creates $ENTFILE, a list of unique geographical entities from the "GeoAreaName" column
- Creates the output/datapoints folder
- Outputs cleaned data
"""


def load_and_clean() -> pd.DataFrame:
    # Load and clean the data
    print("Reading in original data...")
    original_df = pd.read_csv(INFILE, low_memory=False, compression="gzip")
    original_df = original_df[original_df["Value"].notnull()]
    # Clean the IHR Capacity column, duplicate labelling of some attributes which doesn't work well with the grapher
    original_df["[IHR Capacity]"] = original_df["[IHR Capacity]"].replace(
        [
            "IHR02",
            "IHR03",
            "IHR06",
            "IHR07",
            "IHR08",
            "IHR09",
            "IHR10",
            "IHR11",
            "IHR12",
        ],
        [
            "SPAR02",
            "SPAR06",
            "SPAR10",
            "SPAR07",
            "SPAR05",
            "SPAR11",
            "SPAR03",
            "SPAR04",
            "SPAR12",
        ],
    )
    # original_df["[IHR Capacity]"] = ihr_capacity_clean(original_df["[IHR Capacity]"])
    print("Extracting unique entities to " + ENTFILE + "...")
    original_df[["GeoAreaName"]].drop_duplicates().dropna().rename(
        columns={"GeoAreaName": "Country"}
    ).to_csv(ENTFILE, index=False)
    # Make the datapoints folder
    Path(OUTPATH, "datapoints").mkdir(parents=True, exist_ok=True)
    return original_df


"""
create_datasets():
- Creates very simple one line csv with name of dataset and dataset id
"""


def create_datasets() -> pd.DataFrame:
    df_datasets = clean_datasets(DATASET_NAME, DATASET_AUTHORS, DATASET_VERSION)
    assert (
        df_datasets.shape[0] == 1
    ), f"Only expected one dataset in {os.path.join(OUTPATH, 'datasets.csv')}."
    print("Creating datasets csv...")
    df_datasets.to_csv(os.path.join(OUTPATH, "datasets.csv"), index=False)
    return df_datasets


"""
create_sources():
- Creates a csv where each row represents a source for each unique series code in the database
- Each indicator can have multiple series codes associated with it
- Each series code may be associated with multiple indicators
- Each series code may be made up of multiple sources ('dataPublisherSource')
- For each series we extract the 'dataPublisherSource', if there are two or fewer we record all of them,
 if there are more we state that '"Data from multiple sources compiled by UN Global SDG Database - https://unstats.un.org/sdgs/indicators/database/"'
"""


def create_sources(original_df: pd.DataFrame, df_datasets: pd.DataFrame) -> None:
    df_sources = pd.DataFrame(columns=["id", "name", "description", "dataset_id"])
    source_description_template = {
        "dataPublishedBy": "United Nations Statistics Division",
        "dataPublisherSource": None,
        "link": "https://unstats.un.org/sdgs/indicators/database/",
        "retrievedDate": DATASET_RETRIEVED_DATE,
        "additionalInfo": None,
    }
    all_series = (
        original_df[["SeriesCode", "SeriesDescription", "[Units]"]]
        .drop_duplicates()
        .reset_index()
    )
    source_description = source_description_template.copy()
    print("Extracting sources from original data...")
    for i, row in tqdm(all_series.iterrows(), total=len(all_series)):
        dp_source = original_df[
            original_df.SeriesCode == row["SeriesCode"]
        ].Source.drop_duplicates()
        if len(dp_source) <= 2:
            source_description["dataPublisherSource"] = dp_source.str.cat(sep="; ")
        else:
            source_description[
                "dataPublisherSource"
            ] = "Data from multiple sources compiled by UN Global SDG Database - https://unstats.un.org/sdgs/indicators/database/"
        try:
            source_description["additionalInfo"] = None
        except:
            pass
        df_sources = df_sources.append(
            {
                "id": i,
                "name": "%s %s" % (row["SeriesDescription"], DATASET_NAMESPACE),
                "description": json.dumps(source_description),
                "dataset_id": df_datasets.iloc[0][
                    "id"
                ],  # this may need to be more flexible!
                "series_code": row["SeriesCode"],
            },
            ignore_index=True,
        )
    print("Saving sources csv...")
    df_sources.to_csv(os.path.join(OUTPATH, "sources.csv"), index=False)


"""
create_variables_datapoints():
- Outputs a csv where each variables is a row

"""


def create_variables_datapoints(original_df: pd.DataFrame) -> None:
    variable_idx = 0
    variables = pd.DataFrame(columns=["id", "name", "unit", "dataset_id", "source_id"])

    new_columns = []
    for k in original_df.columns:
        new_columns.append(re.sub(r"[\[\]]", "", k))

    original_df.columns = new_columns

    entity2owid_name = (
        pd.read_csv(os.path.join(CONFIGPATH, "standardized_entity_names.csv"))
        .set_index("country_code")
        .squeeze()
        .to_dict()
    )

    sources = pd.read_csv(os.path.join(OUTPATH, "sources.csv"))
    sources = sources[["id", "series_code"]]

    series2source_id = sources.set_index("series_code").squeeze().to_dict()

    unit_description = attributes_description()

    dim_description = dimensions_description()

    original_df["country"] = original_df["GeoAreaName"].apply(
        lambda x: entity2owid_name[x]
    )
    original_df["Units_long"] = original_df["Units"].apply(
        lambda x: unit_description[x]
    )

    init_dimensions = tuple(dim_description.id.unique())
    init_non_dimensions = tuple(
        [c for c in original_df.columns if c not in set(init_dimensions)]
    )
    all_series = (
        original_df[["Indicator", "SeriesCode", "SeriesDescription", "Units_long"]]
        .drop_duplicates()
        .reset_index()
    )
    all_series["short_unit"] = create_short_unit(all_series.Units_long)
    print("Extracting variables from original data...")
    for i, row in tqdm(all_series.iterrows(), total=len(all_series)):
        data_filtered = pd.DataFrame(
            original_df[
                (original_df.Indicator == row["Indicator"])
                & (original_df.SeriesCode == row["SeriesCode"])
            ]
        )

        _, dimensions, dimension_members = get_series_with_relevant_dimensions(
            data_filtered, init_dimensions, init_non_dimensions
        )
        if len(dimensions) == 0:
            # no additional dimensions
            table = generate_tables_for_indicator_and_series(
                data_filtered, init_dimensions, init_non_dimensions, dim_description
            )
            variable = {
                "dataset_id": 0,
                "source_id": series2source_id[row["SeriesCode"]],
                "id": variable_idx,
                "name": "%s - %s - %s"
                % (row["Indicator"], row["SeriesDescription"], row["SeriesCode"]),
                "description": None,
                "code": row["SeriesCode"],
                "unit": row["Units_long"],
                "short_unit": row["short_unit"],
                "timespan": "%s - %s"
                % (
                    int(np.min(data_filtered["TimePeriod"])),
                    int(np.max(data_filtered["TimePeriod"])),
                ),
                "coverage": None,
                "display": None,
                "original_metadata": None,
            }
            variables = variables.append(variable, ignore_index=True)
            extract_datapoints(table).to_csv(
                os.path.join(OUTPATH, "datapoints", "datapoints_%d.csv" % variable_idx),
                index=False,
            )
            variable_idx += 1
        else:
            # has additional dimensions
            for member_combination, table in generate_tables_for_indicator_and_series(
                data_filtered, init_dimensions, init_non_dimensions, dim_description
            ).items():
                variable = {
                    "dataset_id": 0,
                    "source_id": series2source_id[row["SeriesCode"]],
                    "id": variable_idx,
                    "name": "%s - %s - %s - %s"
                    % (
                        row["Indicator"],
                        row["SeriesDescription"],
                        row["SeriesCode"],
                        " - ".join(map(str, member_combination)),
                    ),
                    "description": None,
                    "code": None,
                    "unit": row["Units_long"],
                    "short_unit": row["short_unit"],
                    "timespan": "%s - %s"
                    % (
                        int(np.min(data_filtered["TimePeriod"])),
                        int(np.max(data_filtered["TimePeriod"])),
                    ),
                    "coverage": None,
                    # "display": None,
                    "original_metadata": None,
                }
                variables = variables.append(variable, ignore_index=True)
                extract_datapoints(table).to_csv(
                    os.path.join(
                        OUTPATH, "datapoints", "datapoints_%d.csv" % variable_idx
                    ),
                    index=False,
                )
                variable_idx += 1
    print("Saving variables csv...")
    variables.to_csv(os.path.join(OUTPATH, "variables.csv"), index=False)


def create_distinct_entities() -> None:
    df_distinct_entities = pd.DataFrame(
        get_distinct_entities(), columns=["name"]
    )  # Goes through each datapoints to get the distinct entities
    df_distinct_entities.to_csv(
        os.path.join(OUTPATH, "distinct_countries_standardized.csv"), index=False
    )


def compress_output(outpath) -> None:
    outpath = os.path.realpath(outpath)
    zip_loc = os.path.join(outpath, "datapoints")
    zip_dest = os.path.join(outpath, "datapoints")
    shutil.make_archive(
        base_dir=zip_loc, root_dir=zip_loc, format="zip", base_name=zip_dest
    )


def main() -> None:
    original_df = load_and_clean()
    df_datasets = create_datasets()
    create_sources(original_df, df_datasets)
    create_variables_datapoints(original_df)
    create_distinct_entities()
    compress_output(OUTPATH)


if __name__ == "__main__":
    main()
