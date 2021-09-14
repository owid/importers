# -*- coding: utf-8 -*-
#
#  __init__.py
#  importers
#

"""
A first cut at making a Pythonic API that can handle data and metadata together.

Philosophy:

- Incremental: you should still be able to use with some or all metadata missing
- General: aim for
"""

from os import path
from typing import Protocol, Iterator, List, Dict, Any, NoReturn, Optional
from dataclasses import dataclass, field
import datetime as dt

import pandas as pd


@dataclass
class Provenance:
    source: Optional[str] = None
    source_data_url: Optional[str] = None
    owid_data_url: Optional[str] = None
    date_accessed: Optional[dt.date] = None
    publication_date: Optional[str] = None
    publication_year: Optional[int] = None
    license_name: Optional[str] = None
    license_url: Optional[str] = None


@dataclass
class AboutThisDataset:
    """
    Metadata for an entire dataset, meant to be shared by all tables in this dataset.
    """

    namespace: Optional[str] = None
    short_name: Optional[str] = None
    name: Optional[str] = None
    description: Optional[str] = None
    provenance: Provenance = field(default_factory=Provenance)


@dataclass
class AboutThisSeries:
    """
    Metadata for an individual field in a table.
    """

    name: Optional[str] = None
    long_name: Optional[str] = None
    description: Optional[str] = None
    uri: Optional[str] = None  # addressing scheme
    dataset: Optional["AboutThisDataset"] = None

    # XXX add units, type, etc

    def is_empty(self) -> bool:
        return all(
            getattr(self, f) is None for f in AboutThisSeries.__dataclass_fields__
        )


@dataclass
class AboutThisTable:
    """
    Metadata for a table within a broader dataset.
    """

    primary_key: Optional[List[str]] = None
    schema: Optional[Dict[str, AboutThisSeries]] = None

    # only used if all fields in the table share the same dataset
    dataset: Optional[AboutThisDataset] = None

    def is_empty(self) -> bool:
        return all(
            getattr(self, f) is None for f in AboutThisTable.__dataclass_fields__
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

    def __size__(self) -> int:
        ...

    def save(self, path: str) -> None:
        ...

    @staticmethod
    def load(path: str) -> "Dataset":
        ...
