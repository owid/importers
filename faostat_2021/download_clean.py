"""
TODO: Make it terminal-friendly

Currently only works in Python shell:

>>> from download_clean import main
>>> main()
"""


import shutil
import os
import requests
import zipfile
import io
import tempfile
import pandas as pd
from datetime import datetime


class FAOTableGenerator:
    def __init__(self, metadata_url: str):
        self.metadata_url = metadata_url
        self.metadata = requests.get(self.metadata_url).json()
        self.metadata_original = requests.get(self.metadata_original_url).json()

    @property
    def dataset_id(self):
        return self.metadata["short_name"].split("_")[1]

    @property
    def metadata_original_url(self):
        return (
            f"http://fenixservices.fao.org/faostat/api/v1/en/metadata/{self.dataset_id}"
        )

    @property
    def base_url_descriptions(self):
        return f"http://fenixservices.fao.org/faostat/api/v1/en/definitions/domain/{self.dataset_id}/{{field}}?output_type=objects"

    def _download_data(self, output_path: str) -> str:
        r = requests.get(self.metadata["owid_data_url"])
        z = zipfile.ZipFile(io.BytesIO(r.content))
        z.extractall(output_path)
        files_matched = list(
            filter(lambda x: "(Normalized)" in x, os.listdir(output_path))
        )
        if len(files_matched) == 1:
            return os.path.join(output_path, files_matched[0])
        raise ValueError(
            "Downloaded file not found with current logic. Note that multiple files may"
            " have been downloaded. Program looks for the one containing string"
            " '(Normalized)'."
        )

    def load_df(self):
        with tempfile.TemporaryDirectory() as tmp_dir:
            path = self._download_data(tmp_dir)
            df = pd.read_csv(path, encoding="latin-1")
            return self.process(df)

    def process(self, df):
        self.sanity_checks(df)
        return df

    def sanity_checks(self, df):
        # Check field and field code pair uniqueness
        fields = ["Area", "Item", "Element", "Year"]
        for field in fields:
            check_unique_pairs(df, field, f"{field} Code")
        # Check units for Item + Element
        if (df.groupby(["Item", "Element"]).Unit.nunique() != 1).sum():
            raise ValueError("Variable (Item+Element) appears with different units")
        # Check no NaNs are present (besides in Flag and Value fields)
        ds = df.isnull().sum().drop(["Flag", "Value"])
        if ds.sum() != 0:
            raise ValueError("NaN values in non expected fields!")

    def create_datasets(self):
        df_dataset = pd.DataFrame(
            [
                {
                    "id": self.dataset_id,
                    "name": self.metadata["name"],
                }
            ]
        )
        return df_dataset

    def create_sources(self):
        df_sources = pd.DataFrame(
            [
                {
                    "id": 0,
                    "name": self.metadata["source_name"],
                    "dataset_id": self.dataset_id,
                    "description": {
                        "dataPublishedBy": self.metadata["source_name"],
                        "dataPublisherSource": (
                            "Annual production questionaires, national publications,"
                            " official FAO member country websites, semi-official"
                            " sources (including commodity-specific trade"
                            " publications), data imputation (no data available)"
                        ),
                        "link": self.metadata["url"],
                        "retrievedDate": datetime.strptime(
                            self.metadata["date_accessed"], "%Y-%m-%d"
                        ).strftime("%d-%B-%Y"),
                        "additionalInfo": None,
                    },
                }
            ]
        )
        return df_sources

    def create_variables(self, df):
        # Keep relevant rows
        df_variables = (
            df.groupby(
                ["Item", "Item Code", "Element", "Element Code", "Unit"], as_index=False
            )
            .agg(timespan=("Year", lambda x: f"{min(x)}-{max(x)}"))
            .astype(str)
        )
        # Build df
        df_variables = df_variables.assign(
            name=self._get_variables_name(df_variables),
            code=self._get_variables_code(df_variables),
            dataset_id=self.dataset_id,
            source_id=0,
            description=self._get_variables_description(df_variables),
            display=pd.NA,
            coverage=pd.NA,
            original_metadata=pd.NA,
            short_unit=self._get_variables_short_unit(df_variables),
        )
        # Column renaming + selection
        df_variables = df_variables.reset_index().rename(
            columns={"Unit": "unit", "index": "id"}
        )
        df_variables = df_variables[
            [
                "id",
                "name",
                "code",
                "dataset_id",
                "source_id",
                "unit",
                "short_unit",
                "description",
                "display",
                "coverage",
                "original_metadata",
            ]
        ]
        # Sanity check
        if df_variables.name.value_counts().max() != 1:
            raise ValueError("Variable name is not unique!")
        return df_variables

    def _get_variables_name(self, df):
        return df["Item"] + " - " + df["Element"]

    def _get_variables_code(self, df_variables):
        return (
            f"FAO.{self.dataset_id}."
            + df_variables["Item Code"].astype(str)
            + "."
            + df_variables["Element Code"].astype(str)
        )

    def _get_variables_description(self, df_variables):
        df_item_desc = pd.DataFrame(
            requests.get(self.base_url_descriptions.format(field="items")).json()[
                "data"
            ]
        ).astype(str)
        df_elem_desc = pd.DataFrame(
            requests.get(self.base_url_descriptions.format(field="element")).json()[
                "data"
            ]
        ).astype(str)
        _aux = df_variables.merge(
            df_item_desc[["Item Code", "Description"]], on="Item Code"
        )
        _aux = _aux.merge(
            df_elem_desc[["Element Code", "Description"]], on="Element Code"
        )
        return (_aux["Description_x"] + " " + _aux["Description_y"]).str.strip()

    def _get_variables_short_unit(self, df_variables):
        return df_variables["Unit"]

    def create_datapoints(self, df, df_variables):
        df["varname"] = self._get_variables_name(df)
        df_dp = df[["Area", "Year", "Value", "varname"]]
        datapoints = []
        for dfg in df_dp.groupby("varname"):
            datapoints.append(
                {
                    "df": dfg[1],
                    "id": df_variables.loc[df_variables["name"] == dfg[0], "id"].item(),
                }
            )
        return datapoints

    def create_tables(self, output_path):
        delete_dir(output_path)
        ensure_mkdir(output_path)
        df = self.load_df()
        # Datasets
        # print("datasets")
        df_datasets = self.create_datasets()
        df_datasets.to_csv(os.path.join(output_path, "datasets.csv"), index=False)
        # Sources
        # print("sources")
        df_sources = self.create_sources()
        df_sources.to_csv(os.path.join(output_path, "sources.csv"), index=False)
        # Variables
        # print("variables")
        df_variables = self.create_variables(df)
        df_variables.to_csv(os.path.join(output_path, "variables.csv"), index=False)
        # Datapoints
        # print("datapoints")
        datapoints_all = self.create_datapoints(df, df_variables)
        for datapoints in datapoints_all:
            datapoints["df"].to_csv(
                os.path.join(
                    output_path, "datapoints", f"datapoints_{datapoints['id']}.csv"
                ),
                index=False,
            )


def check_unique_pairs(df, name_1, name_2):
    if not (
        (df.groupby(name_1)[name_2].nunique() == 1).sum()
        and (df.groupby(name_2)[name_1].nunique() == 1).sum()
    ):
        raise ValueError(
            f"Some `{name_1}` may have multiple `{name_2}` values (or opposite)."
        )


def ensure_mkdir(path):
    if not os.path.isdir(path):
        os.makedirs(path)
    if not os.path.exists(os.path.join(path, "datapoints")):
        os.makedirs(os.path.join(path, "datapoints"))


def delete_dir(path) -> None:
    if os.path.exists(path):
        shutil.rmtree(path)
        os.makedirs(path)


def main():
    url = "https://github.com/owid/walden/raw/master/index/faostat/2021-03-18/faostat_QCL.json"
    output_path = "./output"
    table_generator = FAOTableGenerator(url)
    # Load data
    table_generator.create_tables(output_path)
