#
#  test_formats.py
#

from owid import dtypes, formats

import pandas as pd


def test_fricitonless_tables():
    df = pd.DataFrame(
        {
            "country": ["AUS", "USA", "SWE"],
            "ice_cream": ["black sesame", "marshmallow", "pepparkakor"],
        }
    )

    t: dtypes.Table = formats.FrictionlessTable.from_frame(df)
    assert t.primary_key == []

    df2 = t.to_frame()
    assert_df_eq(df2, df)


def test_frictionless_table_with_primary_key():
    df = pd.DataFrame(
        {
            "country": ["AUS", "USA", "SWE"],
            "ice_cream": ["black sesame", "marshmallow", "pepparkakor"],
        }
    ).set_index("country")

    t: dtypes.Table = formats.FrictionlessTable.from_frame(df)
    assert t.primary_key == ["country"]

    df2 = t.to_frame()
    assert_df_eq(df2, df)


# def test_frictionless_dataset():
#     dataset = generate_dataset()


def assert_df_eq(lhs: pd.DataFrame, rhs: pd.DataFrame) -> None:
    assert list(lhs.columns) == list(rhs.columns)
    assert lhs.to_dict() == rhs.to_dict()
