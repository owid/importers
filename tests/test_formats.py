#
#  test_formats.py
#

import tempfile
import random
from typing import Any
import typing
import datetime as dt

from owid import dtypes
from owid.formats import Frictionless

import frictionless
import pandas as pd


def test_encode_metadata_to_frictionless():
    metadata = mock_dataset_metadata()
    d = Frictionless.encode_metadata(metadata)
    assert frictionless.validate(d).valid


def test_decode_metadata_from_frictionless():
    d = {
        "_namespace": "drinks",
        "name": "very_fancy",
        "title": "All cocktails known to mankind",
        "description": "Long markdown doc...",
        "sources": [
            {
                "name": "Bartender's guide 2040",
                "_description": "An extremely long markdown description...",
                "path": "https://dev.null/",
                "_orig_data_url": "https://dev.null/example.csv",
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
    assert metadata.source_name == d["sources"][0]["name"]
    assert metadata.source_description == d["sources"][0]["_description"]
    assert metadata.source_url == d["sources"][0]["path"]
    assert metadata.source_data_url == d["sources"][0]["_orig_data_url"]
    assert metadata.owid_data_url == d["sources"][0]["_owid_data_url"]


# def test_frictionless_round_trip():
#     # set up dataset
#     metadata = mock_dataset_metadata()
#     df = dtypes.RichDataFrame(
#         {
#             "ice_cream": ["black sesame", "marshmallow", "pepparkakor"],
#         },
#         index=pd.Index(["AUS", "USA", "SWE"], name="country"),
#     )
#     ds: dtypes.Dataset = dtypes.InMemoryDataset(metadata=metadata)
#     ds.add_table(df)

#     with tempfile.TemporaryDirectory() as temp_dir:
#         # save to disk
#         Frictionless.save(ds, temp_dir)

#         # read from disk
#         ds2 = Frictionless.load(temp_dir)

#         assert_ds_eq(ds, ds2)


# def test_frictionless_table_with_primary_key():
#     df = pd.DataFrame(
#         {
#             "country": ["AUS", "USA", "SWE"],
#             "ice_cream": ["black sesame", "marshmallow", "pepparkakor"],
#         }
#     ).set_index("country")

#     t: dtypes.Table = formats.FrictionlessTable.from_frame(df)
#     assert t.primary_key == ["country"]

#     df2 = t.to_frame()
#     assert_df_eq(df2, df)


# def test_frictionless_dataset():
#     dataset = generate_dataset()


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

        return random.choice(_MOCK_STRINGS)

    raise ValueError(f"don't know how to mock type: {_type}")
