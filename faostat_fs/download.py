import re
import os
import requests
import zipfile
import io
import json

import pandas as pd
from faostat_fs.utils import read_xlsx_from_url
from faostat_fs import CONFIG_DIR


URL = "http://walden.nyc3.digitaloceanspaces.com/faostat/2021/faostat_FS.zip"
URL_METADATA = (
    "https://github.com/owid/walden/raw/master/index/faostat/2021/faostat_FS.json"
)
FILENAME = "Food_Security_Data_E_All_Data_(Normalized).csv"


def main(output_path) -> str:
    path_data = download_data(output_path)
    path_metadata_walden = download_metadata_walden(output_path)
    path_metadata_fao = download_metadata_fao(output_path)
    return path_data, path_metadata_walden, path_metadata_fao


def download_data(output_path: str) -> str:
    r = requests.get(URL)
    z = zipfile.ZipFile(io.BytesIO(r.content))
    z.extractall(output_path)
    return os.path.join(output_path, FILENAME)


def download_metadata_walden(output_path: str) -> str:
    metadata = requests.get(URL_METADATA).json()
    path = os.path.join(output_path, "metadata-walden.json")
    with open(path, "w") as f:
        json.dump(metadata, f)
    return path


def download_metadata_fao(output_path: str) -> str:
    mc = MetadataCleaner()
    df = mc.run()
    path = os.path.join(output_path, "metadata-fao.csv")
    df.to_csv(path, index=False)
    return path


class MetadataCleaner:
    METADATA_URL: str = "http://fenixservices.fao.org/faostat/static/documents/FS/Descriptions_and_Metadata.xlsx"
    VARIABLES_CONFIG: str = (
        os.path.join(  # Manually created. Mapping FAO data varname -> FAO meta varname
            CONFIG_DIR, "variables.json"
        )
    )
    SOURCES_CONFIG: str = (
        os.path.join(  # Manually created. Mapping FAO source name -> OWID source name
            CONFIG_DIR, "sources.json"
        )
    )

    def __init__(self):
        with open(self.VARIABLES_CONFIG, "r") as f:
            self.variables_mapping = json.load(f)
        with open(self.SOURCES_CONFIG, "r") as f:
            self.sources_mapping = json.load(f)

    def get_df(self):
        # Read data
        metadata_raw = read_xlsx_from_url(self.METADATA_URL)
        # Build df metadata
        tab_names = [
            x for x in metadata_raw.keys() if x not in ["Home", "Table of Contents"]
        ]
        metadata = _clean_metadata(metadata_raw, tab_names)
        return pd.DataFrame.from_dict(metadata, orient="index")

    def pipe_variables(self, df):
        # Replace with correct variable name
        df = df.assign(variable_name=df.name.replace(self.variables_mapping))
        df = df.dropna(subset=["variable_name"])
        return df

    def pipe_sources(self, df):
        # Fill sources in rows with "See ..."
        df["Source data"] = df["Source data"].apply(
            lambda x: df.loc[_extract_varname(x), "Source data"]
            if _extract_varname(x)
            else x
        )
        df["Original source data"] = df["Original source data"].apply(
            lambda x: df.loc[_extract_varname(x), "Original source data"]
            if _extract_varname(x)
            else x
        )
        # Combine `Source data` and `Original source data`
        msk = df["Original source data"].isna()
        df["source_name"] = df["Original source data"]
        df.loc[msk, "source_name"] = df.loc[msk, "Source data"]
        df = df.assign(source_name=df.source_name.replace(self.sources_mapping))
        return df

    def pipe_description(self, df):
        # Fill sources in rows with "See ..."
        return df.assign(description=df.apply(_build_variable_description, axis=1))

    def pipe_sources_id(self, df):
        sources = set(df.source_name)
        sources = dict(zip(sources, range(len(sources))))
        return df.assign(source_id=df.source_name.map(sources))

    def pipe_index(self, df):
        return df.reset_index(drop=True)

    def pipeline(self, df):
        return (
            df.pipe(self.pipe_variables)
            .pipe(self.pipe_sources)
            .pipe(self.pipe_description)
            .pipe(self.pipe_sources_id)
            .pipe(self.pipe_index)[
                ["variable_name", "source_name", "description", "source_id"]
            ]
        )

    def run(self):
        df = self.get_df()
        return df.pipe(self.pipeline)


def _clean_metadata_variable_df(df):
    """Cleans a metadata data frame.

    Columns with all NaNs are removed. Headers in data frames are fixed.
    """
    varname = df.columns[1]
    df = df.dropna(axis=1, how="all")
    df = df.dropna(subset=["(table of contents)"])
    df = pd.DataFrame(df.loc[2:].values, columns=["field", "value"])
    df = df.append([{"field": "name", "value": varname}], ignore_index=True)
    return df


def _clean_metadata(dfs, tab_names):
    """Clean variable metadata.

    Convert raw data frames into cleaned dictionaries. Format is {"variable_name": {key: value, ...}}.
    """
    metadata = {}
    for name in tab_names:
        df = dfs.get(name, dfs.get(name + " ", None))
        if df is None:
            raise ValueError(f"Invalid variable name {name}")
        try:
            df = _clean_metadata_variable_df(df)
        except:
            print(name)
        metadata[name] = dict(zip(df.field, df.value))
    return metadata


def _extract_varname(x):
    if pd.isnull(x):
        return None
    if re.search(r"See (.)+", x):
        return re.search(r"See (.*)", x).group(1) + " Metadata"
    return None


def _build_variable_description(ds):
    meta_variables = [
        "Statistical concepts and definitions",
        "Relevance",
        "Time coverage",
        "Sector coverage",
        "Data compilation",
    ]
    x = ds[meta_variables]
    text = "\n".join(f"{k}: {v}" for k, v in x.items() if not pd.isnull(v))
    return text
