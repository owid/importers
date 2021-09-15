#
#  formats.py
#
#  Load and save datasets and tables in different on-disk formats.
#


from dataclasses import dataclass
from typing import Any, NoReturn, Dict, List
import atexit
import tempfile
import json
from os import path
from collections import defaultdict

from . import dtypes

import frictionless
import pandas as pd

# ---------------------------------------------------------------------------------------------#
# Frictionless data format
# ---------------------------------------------------------------------------------------------#


def error_on_set(self: Any, v: Any) -> NoReturn:
    raise Exception("setter not implemented")


class Frictionless:
    @staticmethod
    def save(ds: dtypes.Dataset, dirname: str) -> None:
        # export dataset metadata to dict
        # prefix custom fields with underscore
        # for each table:
        #    - export metadata to dict
        #    - remove dataset metadata
        #    - prefix custom fields with underscore
        #    - remap field names like primaryKey
        #    - write table to csv
        #
        # save datapackage.json
        pass

    @staticmethod
    def load(dirname: str) -> dtypes.Dataset:
        return dtypes.InMemoryDataset()

    @staticmethod
    def encode_metadata(metadata: dtypes.AboutThisDataset) -> dict:
        return {
            "_namespace": metadata.namespace,
            "name": metadata.short_name,
            "title": metadata.title,
            "description": metadata.description,
            "licenses": [{"name": metadata.license_name, "path": metadata.license_url}],
            "sources": [
                {
                    "title": metadata.source_name,
                    "path": metadata.source_url,
                    "_description": metadata.source_description,
                    "_orig_data_url": metadata.source_data_url,
                    "_owid_data_url": metadata.owid_data_url,
                }
            ],
        }

    @staticmethod
    def decode_metadata(metadata: dict) -> dtypes.AboutThisDataset:
        license = defaultdict(lambda: None)
        if metadata.get("licenses"):
            license.update(metadata["licenses"][0])

        source = defaultdict(lambda: None)
        if metadata.get("sources"):
            source.update(metadata["sources"][0])

        return dtypes.AboutThisDataset(
            namespace=metadata.get("_namespace"),
            short_name=metadata.get("name"),
            title=metadata.get("title"),
            description=metadata.get("description"),
            license_name=(license["name"]),
            license_url=(license["path"]),
            source_data_url=source["_orig_data_url"],
            owid_data_url=source["_owid_data_url"],
            source_name=source["title"],
            source_description=source["_description"],
            source_url=source["path"],
        )


@dataclass
class FrictionlessTable:
    """
    A frictionless dataset stored as a CSV table, with metadata kept in the dataset's
    datapackage.json file.
    """

    resource: frictionless.Resource
    primary_key: List[str]
    indicators: List[str]
    metadata: Dict[str, Any]

    @classmethod
    def from_resource(cls, resource: frictionless.Resource):
        columns = [f["name"] for f in resource.schema["fields"]]  # type: ignore
        primary_key = resource.schema.get("primaryKey", [])
        indicators = [c for c in columns if c not in primary_key]
        metadata = resource.get("_owid_metadata", {})

        return FrictionlessTable(resource, primary_key, indicators, metadata)

    def to_frame(self) -> pd.DataFrame:
        return self.resource.to_pandas()  # type: ignore

    @classmethod
    def from_frame(cls, df: pd.DataFrame) -> "FrictionlessTable":
        # XXX currently writes the frame to disk, but if we use this more
        #     we should work out how to just do it in memory

        temp_dir = tempfile.TemporaryDirectory()
        atexit.register(temp_dir.cleanup)  # delete on exit

        csv_file = path.join(temp_dir.name, "data.csv")
        json_file = path.join(temp_dir.name, "datapackage.json")

        # work out the primary key, if there is one
        primary_key = _get_primary_key(df)
        if primary_key:
            df = df.reset_index()

        df.to_csv(csv_file, index=False)

        schema: Dict[str, Any] = {"fields": [{"name": col} for col in df.columns]}
        if primary_key:
            schema["primaryKey"] = primary_key
        data_pkg = {"resources": [{"path": "data.csv", "schema": schema}]}

        with open(json_file, "w") as ostream:
            json.dump(data_pkg, ostream)

        pkg = frictionless.Package(json_file)
        return FrictionlessTable.from_resource(pkg.resources[0])  # type: ignore


@dataclass
class FrictionlessDataset:
    """
    A dataset is a folder in Frictionless data format, containing many CSV files and one
    huge datapackage.json files containing schemas and metadata for them all.
    """

    pkg: frictionless.Package

    def __iter__(self):
        for resource in self.pkg.resources:  # type: ignore
            yield FrictionlessTable.from_resource(resource)

    @classmethod
    def load(cls, dir_name: str) -> "FrictionlessDataset":
        datapackage_file = path.join(dir_name, "datapackage.json")
        pkg = frictionless.Package(datapackage_file)
        return FrictionlessDataset(pkg)


def _get_primary_key(df: pd.DataFrame) -> List[str]:
    primary_key: List[str] = list(df.index.names)
    if primary_key[0] is not None:
        return primary_key

    return []
