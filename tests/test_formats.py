#
#  test_formats.py
#

from inspect import trace
import tempfile
import random
from typing import Any, Dict
import typing
import datetime as dt
import os
from os import path

from owid import dtypes
from owid.formats import Frictionless

import frictionless
import pandas as pd


def test_encode_metadata_to_frictionless():
    metadata = mock_dataset_metadata()
    d = Frictionless.encode_metadata(metadata)

    # the frictionless standard requires at least one resources, so add a dummy one
    d["resources"] = [
        {
            "path": "https://owid-test.nyc3.digitaloceanspaces.com/importers/01-minimal.csv"
        }
    ]

    assert metadata.namespace == d["_namespace"]
    assert metadata.short_name == d["name"]
    assert metadata.title == d["title"]
    assert metadata.description == d["description"]
    assert metadata.license_name == d["licenses"][0]["name"]
    assert metadata.license_url == d["licenses"][0]["path"]
    for i, source in enumerate(metadata.sources):
        s = d["sources"][i]
        assert source.name == s["title"]
        assert source.description == s["_description"]
        assert source.url == s["path"]
        assert source.source_data_url == s["_source_data_url"]
        assert source.owid_data_url == s["_owid_data_url"]

    # validate against the frictionless standard
    assert frictionless.validate(d).errors == []


def test_decode_metadata_from_frictionless():
    d = {
        "_namespace": "drinks",
        "name": "very_fancy",
        "title": "All cocktails known to mankind",
        "description": "Long markdown doc...",
        "sources": [
            {
                "title": "Bartender's guide 2040",
                "path": "https://dev.null/",
                "_description": "An extremely long markdown description...",
                "_source_data_url": "https://dev.null/example.csv",
                "_owid_data_url": "https://fake.ourworldindata.org/example.csv",
            }
        ],
        "licenses": [
            {
                "name": "CC-BY-NC-4.0",
                "path": "https://creativecommons.org/licenses/by-nc/4.0/",
            }
        ],
    }
    metadata = Frictionless.decode_metadata(d)
    assert metadata.namespace == d["_namespace"]
    assert metadata.short_name == d["name"]
    assert metadata.title == d["title"]
    assert metadata.description == d["description"]
    assert metadata.license_name == d["licenses"][0]["name"]
    assert metadata.license_url == d["licenses"][0]["path"]
    for i, s in enumerate(d["sources"]):
        source = metadata.sources[i]
        assert source.name == s["title"]
        assert source.description == s["_description"]
        assert source.url == s["path"]
        assert source.source_data_url == s["_source_data_url"]
        assert source.owid_data_url == s["_owid_data_url"]


def test_frictionless_series_metadata_roundtrip():
    m1: dtypes.AboutThisSeries = attr_updated(
        mock(dtypes.AboutThisSeries), dataset=None
    )

    s = Frictionless.encode_series_metadata(m1)

    m2 = Frictionless.decode_series_metadata(s)

    assert m1 == m2


def test_frictionless_round_trip():
    "Check that we can encode data to frictionless in a lossless way."
    # set up dataset
    metadata = mock(dtypes.AboutThisDataset)
    df = dtypes.RichDataFrame(
        {
            "ice_cream": ["black sesame", "marshmallow", "pepparkakor"],
        },
        index=pd.Index(["AUS", "USA", "SWE"], name="country"),
        metadata=dtypes.AboutThisTable(
            short_name="best_flavours",
            primary_key=["country"],
            schema={
                "country": attr_updated(mock(dtypes.AboutThisSeries), dataset=metadata),
                "ice_cream": attr_updated(
                    mock(dtypes.AboutThisSeries), dataset=metadata
                ),
            },
        ),
    )
    ds: dtypes.SerializableDataset = dtypes.InMemoryDataset(metadata=metadata)
    ds.add(df)

    with tempfile.TemporaryDirectory() as temp_dir:
        # get rid of the auto-created directory
        os.rmdir(temp_dir)

        # save to disk
        Frictionless.save(ds, temp_dir)

        # check that the package validates clean
        package_file = path.join(temp_dir, "datapackage.json")
        assert path.exists(package_file)

        # XXX failing validation with internal error in frictionless
        # assert frictionless.validate(package_file).errors == []

        # read from disk
        ds2 = Frictionless.load(temp_dir)

        print(ds["best_flavours"].metadata.schema)
        print(ds2["best_flavours"].metadata.schema)

        assert_ds_eq(ds, ds2)


def assert_dataclass_eq(lhs: Any, rhs: Any, _type: Any) -> None:
    for f in _type.__dataclass_fields__:
        assert getattr(lhs, f) == getattr(rhs, f), f


def assert_df_eq(lhs: dtypes.RichDataFrame, rhs: dtypes.RichDataFrame) -> None:
    # assert lhs.metadata == rhs.metadata
    assert_dataclass_eq(lhs.metadata, rhs.metadata, type(lhs.metadata))
    assert lhs.to_dict() == rhs.to_dict()


def assert_ds_eq(lhs: dtypes.Dataset, rhs: dtypes.Dataset) -> None:
    # assert lhs.metadata == rhs.metadata
    assert_dataclass_eq(lhs.metadata, rhs.metadata, type(lhs.metadata))
    assert len(lhs) == len(rhs)
    for lhs_t, rhs_t in zip(lhs, rhs):
        assert_df_eq(lhs_t, rhs_t)


_MOCK_STRINGS = None


def mock_dataset_metadata() -> dtypes.AboutThisDataset:
    return dtypes.AboutThisDataset(
        **{
            f.name: mock(f.type)
            for f in dtypes.AboutThisDataset.__dataclass_fields__.values()
        }
    )


def is_optional_type(_type: type) -> bool:
    return (
        getattr(_type, "__origin__", None) == typing.Union
        and len(getattr(_type, "__args__", ())) == 2
        and getattr(_type, "__args__")[1] == type(None)
    )


def strip_option(_type: type) -> type:
    return _type.__args__[0]  # type: ignore


def mock(_type: type) -> Any:
    global _MOCK_STRINGS

    if is_optional_type(_type):
        _type = strip_option(_type)

    if hasattr(_type, "__forward_arg__"):
        raise ValueError(_type)

    if _type == int:
        return random.randint(0, 1000)

    elif _type == float:
        return 10 * random.random() / random.random()

    elif _type == dt.date:
        return dt.date.fromordinal(
            dt.date.today().toordinal() - random.randint(0, 1000)
        )

    elif _type == str:
        if not _MOCK_STRINGS:
            _MOCK_STRINGS = [l.strip() for l in open("/usr/share/dict/words")]

        # some strings in the frictionless standard must be lowercase with no spaces
        return random.choice(_MOCK_STRINGS).lower()

    elif getattr(_type, "_name", None) == "List":
        # e.g. List[int]
        return [mock(_type.__args__[0]) for i in range(random.randint(1, 4))]  # type: ignore

    elif getattr(_type, "_name", None) == "Dict":
        # e.g. Dict[str, int]
        _from, _to = _type.__args__  # type: ignore
        return {mock(_from): mock(_to) for i in range(random.randint(1, 8))}

    elif hasattr(_type, "__dataclass_fields__"):
        # all dataclasses
        return _type(
            **{
                f.name: mock(f.type)
                for f in _type.__dataclass_fields__.values()  # type: ignore
            }
        )

    raise ValueError(f"don't know how to mock type: {_type}")


T = typing.TypeVar("T")


def attr_updated(obj: T, **kwargs) -> T:
    for k, v in kwargs.items():
        setattr(obj, k, v)
    return obj
