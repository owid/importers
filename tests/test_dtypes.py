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
        name="gdp", title="GDP per capita in 2011 international dollars"
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
    assert s3.title == m3.title  # individual access


def test_rich_dataframe_no_metadata():
    d = dtypes.RichDataFrame({"a": [1, 2, 3], "b": ["dog", "sheep", "pig"]})
    assert d.metadata.is_empty()


def test_rich_dataframe_detect_primary_key():
    d = dtypes.RichDataFrame(
        {"c": ["dog", "sheep", "pig"]},
        index=pd.MultiIndex.from_tuples(
            [(1, 2020), (2, 2020), (2, 2021)], names=["a", "b"]
        ),
        metadata=dtypes.AboutThisTable(short_name="example"),
    )
    assert d.primary_key == ["a", "b"]
    assert d.metadata == dtypes.AboutThisTable(
        short_name="example", primary_key=["a", "b"]
    )


def test_rich_dataframe_creates_rich_series():
    gho = dtypes.AboutThisDataset(short_name="GHO")
    d = dtypes.RichDataFrame(
        {"c": ["dog", "sheep", "pig"]},
        index=pd.MultiIndex.from_tuples(
            [(1, 2020), (2, 2020), (2, 2021)], names=["a", "b"]
        ),
        metadata=dtypes.AboutThisTable(dataset=gho),
    )
    s = d.c
    assert isinstance(s, dtypes.RichSeries)
    assert s.dataset == gho
    assert s.metadata.dataset == gho
    assert s.metadata.name == "c"


def test_rich_dataframe_metadata_survives_copying():
    metadata = dtypes.AboutThisTable(dataset=dtypes.AboutThisDataset(short_name="GHO"))
    d = dtypes.RichDataFrame(
        {"c": ["dog", "sheep", "pig"]},
        index=pd.MultiIndex.from_tuples(
            [(1, 2020), (2, 2020), (2, 2021)], names=["a", "b"]
        ),
        metadata=metadata,
    )
    assert not d.metadata.is_empty()

    # try slicing and copying
    assert d.iloc[:1].metadata == d.metadata  # type: ignore
    assert d.copy().metadata == d.metadata
