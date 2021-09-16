# -*- coding: utf-8 -*-
#
#  __init__.py
#  importers
#

"""
A first cut at making a Pythonic API that can handle data and metadata together.

Philosophy:

- Incremental: you should still be able to use with some or all metadata missing
- General: where possible, aim for things many people would want, not just OWID
"""

from collections import defaultdict
from os import path
from typing import Protocol, Iterator, List, Dict, Any, NoReturn, Optional
from dataclasses import dataclass, field
import datetime as dt
import uuid
import re

import pandas as pd
from dataclasses_json import dataclass_json


@dataclass_json
@dataclass
class Source:
    name: Optional[str] = None
    description: Optional[str] = None
    url: Optional[str] = None
    source_data_url: Optional[str] = None
    owid_data_url: Optional[str] = None
    date_accessed: Optional[str] = None
    publication_date: Optional[str] = None
    publication_year: Optional[int] = None


@dataclass_json
@dataclass
class AboutThisDataset:
    """
    Metadata for an entire dataset, meant to be shared by all tables in this dataset.
    Most of this comes directly from Walden.

    Goal: you can build an addressing scheme from this metadata.
    """

    namespace: Optional[str] = None
    short_name: Optional[str] = None
    title: Optional[str] = None
    description: Optional[str] = None
    sources: List[Source] = field(default_factory=list)
    license_name: Optional[str] = None
    license_url: Optional[str] = None


@dataclass
class AboutThisTable:
    """
    Metadata for a table within a broader dataset.
    """

    # every dataset needs a name, use a UUID if we have nothing better
    short_name: str = field(default_factory=lambda: str(uuid.uuid4()))

    primary_key: Optional[List[str]] = None
    schema: Optional[Dict[str, "AboutThisSeries"]] = None

    # only used if all fields in the table share the same dataset
    dataset: Optional[AboutThisDataset] = None

    def is_empty(self) -> bool:
        return _is_uuid4(self.short_name) and all(
            getattr(self, f) is None
            for f in AboutThisTable.__dataclass_fields__
            if f != "short_name"
        )


@dataclass
class AboutThisSeries:
    """
    Metadata for an individual field in a table.
    """

    name: Optional[str] = None
    title: Optional[str] = None
    description: Optional[str] = None
    source_name: Optional[str] = None
    dataset: Optional[AboutThisDataset] = None

    # XXX add units, type, etc

    def is_empty(self) -> bool:
        return all(
            getattr(self, f) is None for f in AboutThisSeries.__dataclass_fields__
        )


class RichDataFrame(pd.DataFrame):
    """
    A data frame that contains metadata about where it came from and about
    the columns it has. Use it like a normal data frame, except that you can also
    add metadata to it, field by field, or using the `metadata` attribute.
    """

    def __init__(self, *args, metadata: Optional[AboutThisTable] = None, **kwargs):
        super().__init__(*args, **kwargs)
        self.metadata = metadata or AboutThisTable()
        if not self.primary_key:
            self.primary_key = _detect_primary_key(self)

    @property
    def _constructor(self):
        return RichDataFrame

    @property
    def _constructor_sliced(self):
        return RichSeries

    _metadata = list(AboutThisTable.__dataclass_fields__)

    def set_metadata(self, metadata: Optional[AboutThisTable]) -> None:
        metadata = metadata or AboutThisTable()
        for field in AboutThisTable.__dataclass_fields__:
            value = getattr(metadata, field)
            setattr(self, field, value)

    def get_metadata(self) -> Optional[AboutThisTable]:
        return AboutThisTable(
            **{f: getattr(self, f, None) for f in AboutThisTable.__dataclass_fields__}
        )

    def ensure_named(self) -> None:
        """
        Make sure this table has a name we can refer to it with.
        """
        self.metadata.short_name = str(uuid.uuid4())

    def to_feather(self, *args, **kwargs) -> None:
        if self.has_index():
            pd.DataFrame.to_feather(self.reset_index(), *args, **kwargs)

    def has_index(self) -> bool:
        names = self.index.names
        return len(names) != 1 or names[0] is not None

    def all_columns(self):
        "Return all column names, including those in the index."
        cols = [col for col in self.index.names if col]
        cols += list(self.columns)
        return cols

    metadata = property(get_metadata, set_metadata)


class RichSeries(pd.Series):
    """
    A pandas Series with optional metadata about this column and where it came from.
    Use it like a normal series, or enrich it with fields from AboutThisSeries.
    """

    def __init__(self, *args, metadata: Optional[AboutThisSeries] = None, **kwargs):
        super().__init__(*args, **kwargs)
        self.metadata = metadata or AboutThisSeries(name=kwargs.get("name"))

    @property
    def _constructor(self):
        return RichSeries

    @property
    def _constructor_expanddim(self):
        return RichDataFrame

    _metadata = list(AboutThisSeries.__dataclass_fields__)

    def set_metadata(self, metadata: Optional[AboutThisSeries]) -> None:
        for field in AboutThisSeries.__dataclass_fields__:
            if metadata is not None:
                value = getattr(metadata, field)
            else:
                value = None
            setattr(self, field, value)

    def get_metadata(self) -> Optional[AboutThisSeries]:
        return AboutThisSeries(
            **{f: getattr(self, f, None) for f in AboutThisSeries.__dataclass_fields__}
        )

    metadata = property(get_metadata, set_metadata)


class Dataset(Protocol):
    metadata: AboutThisDataset

    def __iter__(self) -> Iterator[RichDataFrame]:
        ...

    def __len__(self) -> int:
        ...

    def __getitem__(self, table_name: str) -> RichDataFrame:
        ...

    def add(self, table: RichDataFrame) -> None:
        ...


class SerializableDataset(Dataset, Protocol):
    def save(self, path: str) -> None:
        ...

    @staticmethod
    def load(path: str) -> "Dataset":
        ...


def _detect_primary_key(df: pd.DataFrame) -> Optional[List[str]]:
    primary_key: List[str] = list(df.index.names)
    if primary_key[0] is not None:
        return primary_key

    return None


@dataclass
class InMemoryDataset(SerializableDataset):
    tables: Dict[str, RichDataFrame] = field(default_factory=dict)
    metadata: AboutThisDataset = field(default_factory=AboutThisDataset)

    def __len__(self) -> int:
        return len(self.tables)

    def __iter__(self) -> Iterator[RichDataFrame]:
        yield from self.tables.values()

    def __getitem__(self, table_name: str) -> RichDataFrame:
        return self.tables[table_name]

    def add(self, table: RichDataFrame) -> None:
        # link the table's metadata to that of the entire dataset
        table.dataset = self.metadata

        # add the table to our collection
        self.tables[table.metadata.short_name] = table  # type: ignore


def _is_uuid4(name: str) -> bool:
    return bool(
        re.match("^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$", name)
    )
