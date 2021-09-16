#
#  formats.py
#
#  Load and save datasets and tables in different on-disk formats.
#


from dataclasses import dataclass
from typing import Any, Iterator, NoReturn, Dict, List, Optional
import atexit
import tempfile
import json
from os import path
import os
from collections import defaultdict
import shutil

from . import dtypes

import frictionless
import pandas as pd

# ---------------------------------------------------------------------------------------------#
# Frictionless data format
# ---------------------------------------------------------------------------------------------#


def error_on_set(self: Any, v: Any) -> NoReturn:
    raise Exception("setter not implemented")


class Frictionless:
    """
    Encoding and decoding according to the frictionless standard.

    See: https://specs.frictionlessdata.io/schemas/data-package.json
    """

    @staticmethod
    def save(ds: dtypes.Dataset, dirname: str) -> None:
        package_file = path.join(dirname, "datapackage.json")

        if path.exists(dirname):
            # save over the top of an existing dataset, but be careful about it
            if not path.exists(package_file):
                raise Exception(
                    f"refuse to save over the top of a non-dataset folder: {dirname}"
                )

            shutil.rmtree(dirname)

        os.mkdir(dirname)

        resources = []
        for table in ds:
            resource = Frictionless.save_table(table, dirname)
            resources.append(resource)

        m = Frictionless.encode_metadata(ds.metadata)
        m["resources"] = resources
        with open(package_file, "w") as ostream:
            json.dump(m, ostream, indent=2)

    @staticmethod
    def save_table(table: dtypes.RichDataFrame, dirname: str) -> dict:
        metadata: dtypes.AboutThisTable = table.metadata

        if not metadata.short_name:
            raise Exception("cannot serialise a table without a short name for it")

        # save the data
        dest_file = path.join(dirname, f"{metadata.short_name}.feather")
        table.to_feather(dest_file)

        # return the resource metadata
        schema: Dict[str, dtypes.AboutThisSeries] = table.metadata.schema  # type: ignore
        d = {
            "name": metadata.short_name,
            "path": path.basename(dest_file),
            "schema": {
                "primaryKey": table.primary_key,
                "fields": {
                    col: Frictionless.encode_series_metadata(schema[col])
                    for col in schema
                },
            },
        }
        return d

    @staticmethod
    def load(dirname: str) -> "FrictionlessDataset":
        pkg_file = path.join(dirname, "datapackage.json")
        return FrictionlessDataset(pkg_file)

    @staticmethod
    def encode_metadata(metadata: dtypes.AboutThisDataset) -> dict:
        d = {
            "_namespace": metadata.namespace,
            "name": metadata.short_name,
            "title": metadata.title,
            "description": metadata.description,
            "licenses": [{"name": metadata.license_name, "path": metadata.license_url}],
            "sources": [
                {
                    "title": source.name,
                    "path": source.url,
                    "_date_accessed": source.date_accessed,
                    "_publication_date": source.publication_date,
                    "_publication_year": source.publication_year,
                    "_description": source.description,
                    "_source_data_url": source.source_data_url,
                    "_owid_data_url": source.owid_data_url,
                }
                for source in metadata.sources
            ],
        }
        return pruned(d)

    @staticmethod
    def decode_metadata(metadata: dict) -> dtypes.AboutThisDataset:
        # we only support one license, but frictionless allows many
        licenses: List[dict] = metadata.get("licenses", defaultdict(lambda: None))
        if len(licenses) > 1:
            raise ValueError("OWID datasets only support one license per dataset")
        license = licenses[0]

        sources = [defaultdict(lambda: None, source) for source in metadata["sources"]]

        return dtypes.AboutThisDataset(
            namespace=metadata.get("_namespace"),
            short_name=metadata.get("name"),
            title=metadata.get("title"),
            description=metadata.get("description"),
            license_name=license["name"],
            license_url=license["path"],
            sources=[
                dtypes.Source(
                    name=source["title"],
                    description=source["_description"],
                    url=source["path"],
                    source_data_url=source["_source_data_url"],
                    owid_data_url=source["_owid_data_url"],
                    date_accessed=source["_date_accessed"],
                    publication_date=source["_publication_date"],
                    publication_year=source["_publication_year"],
                )
                for source in sources
            ],
        )

    @staticmethod
    def table_from_resource(
        resource: frictionless.Resource, base_dir: str, dataset: dtypes.AboutThisDataset
    ):
        metadata: dtypes.AboutThisTable = Frictionless.decode_table_metadata(
            resource, dataset
        )

        filename = path.join(base_dir, resource.path)  # type: ignore
        df = pd.read_feather(filename)
        df = dtypes.RichDataFrame(df, metadata=metadata)

        if df.primary_key:
            df.set_index(df.primary_key, inplace=True)

        return df

    @staticmethod
    def decode_table_metadata(
        resource: frictionless.Resource,
        dataset: Optional[dtypes.AboutThisDataset] = None,
    ) -> dtypes.AboutThisTable:
        fields: Dict[str, Any] = resource.schema["fields"]  # type: ignore
        return dtypes.AboutThisTable(
            short_name=resource.name,  # type: ignore
            primary_key=resource.schema.get("primaryKey"),
            schema={
                col: Frictionless.decode_series_metadata(
                    resource.schema.fields[col], dataset
                )
                for col in fields
            },
            dataset=dataset,
        )

    @staticmethod
    def encode_series_metadata(metadata: dtypes.AboutThisSeries) -> Dict[str, Any]:
        return {
            "name": metadata.name,
            "title": metadata.title,
            "description": metadata.description,
            "_source_name": metadata.source_name,
        }

    @staticmethod
    def decode_series_metadata(
        schema: Dict[str, Any], dataset: Optional[dtypes.AboutThisDataset] = None
    ) -> dtypes.AboutThisSeries:
        return dtypes.AboutThisSeries(
            name=schema["name"],
            title=schema.get("title"),
            description=schema.get("description"),
            source_name=schema.get("_source_name"),
            dataset=dataset,
        )


class FrictionlessDataset:
    """
    A dataset is a folder in Frictionless data format, containing many CSV files and one
    huge datapackage.json files containing schemas and metadata for them all.
    """

    def __init__(self, pkg_file: str):
        self.pkg_file = pkg_file
        self.pkg = frictionless.Package(pkg_file)
        self.metadata = Frictionless.decode_metadata(self.pkg)

    def __len__(self) -> int:
        return len(self.pkg.resources)  # type: ignore

    def __iter__(self) -> Iterator[dtypes.RichDataFrame]:
        base_dir = path.dirname(self.pkg_file)
        for resource in self.pkg.resources:  # type: ignore
            table = Frictionless.table_from_resource(resource, base_dir, self.metadata)
            yield table

    def __getitem__(self, table_name: str) -> dtypes.RichDataFrame:
        base_dir = path.dirname(self.pkg_file)
        (t,) = [r for r in self.pkg.resources if r["name"] == table_name]  # type: ignore
        return Frictionless.table_from_resource(t, base_dir, self.metadata)

    def save(self, path: str) -> None:
        Frictionless.save(self, path)

    @staticmethod
    def load(path: str) -> "FrictionlessDataset":
        return Frictionless.load(path)

    def add(self, table: dtypes.RichDataFrame) -> None:
        raise Exception("not yet implemented")


def _get_primary_key(df: pd.DataFrame) -> List[str]:
    primary_key: List[str] = list(df.index.names)
    if primary_key[0] is not None:
        return primary_key

    return []


def pruned(v: Any) -> Any:
    "Prune a JSON-like document to remove any (k, v) pairs where the value is None."
    if isinstance(v, dict):
        v = v.copy()
        for k in list(v):
            v[k] = pruned(v[k])
            if v[k] is None:
                del v[k]

    elif isinstance(v, list):
        return [pruned(x) for x in v]

    return v
