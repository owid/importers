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

from . import dtypes

import frictionless
import pandas as pd

# ---------------------------------------------------------------------------------------------#
# Frictionless data attempt
# ---------------------------------------------------------------------------------------------#


def error_on_set(self: Any, v: Any) -> NoReturn:
    raise Exception("setter not implemented")


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
