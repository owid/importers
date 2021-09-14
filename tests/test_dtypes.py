from owid import dtypes
import pandas as pd


def test_rich_series_no_metadata():
    # no metadata case
    s1 = dtypes.RichSeries([1, 2, 3])
    assert s1.metadata.is_empty()
    assert s1.metadata == dtypes.AboutThisSeries()


def test_rich_series_keeps_name():
    # compatibility of metadata with name
    s2 = dtypes.RichSeries([1, 2, 3], name="numbers")
    assert s2.metadata == dtypes.AboutThisSeries(name="numbers")


def test_rich_series_slicing_and_access():
    m3 = dtypes.AboutThisSeries(
        name="gdp", long_name="GDP per capita in 2011 international dollars"
    )
    s3 = dtypes.RichSeries(
        [53015, 54008, 55335],
        index=pd.MultiIndex.from_tuples(
            [("usa", 2016), ("usa", 2017), ("usa", 2018)], names=["country", "year"]
        ),
        metadata=m3,
    )
    assert s3.metadata == m3  # construction
    assert s3.iloc[:2].metadata == m3  # slicing
    assert s3.long_name == m3.long_name  # individual access


def test_rich_dataframe_no_metadata():
    d1 = dtypes.RichDataFrame({"a": [1, 2, 3], "b": ["dog", "sheep", "pig"]})
    assert d1.metadata.is_empty()
    assert d1.metadata == dtypes.AboutThisTable()


# def test_rich_dataset():
#     df = dtypes.RichDataFrame(
#         {
#             "country": ["AUS", "USA", "SWE"],
#             "ice_cream": ["black sesame", "marshmallow", "pepparkakor"],
#         },
#         metadata=AboutThisTable,
#     )
#     assert df.metadata == metadata
