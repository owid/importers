import os
import re
import pandas as pd

from bp_statreview import INPATH, DATASET_VERSION

LAST_YEAR_OF_DATA = int(DATASET_VERSION) - 1
DATA_PATH = os.path.join(INPATH, "data.xlsx")


def clean_excel_datapoints(var: dict) -> pd.DataFrame:
    """cleans data points by extracting them from an xlsx file.

    This is only used when the data points are not available in the provided
    source csv file.
    """
    cleaning_meta = var["cleaningMetadata"]
    if cleaning_meta["sheetName"].lower() == "coal - reserves":
        df = process_coal_reserves(
            cleaning_meta["sheetName"],
            cleaning_meta["skipRows"],
            cleaning_meta["subvariable"],
        )
    elif cleaning_meta["sheetName"].lower() == "coal prices":
        df = process_coal_prices(cleaning_meta["sheetName"], cleaning_meta["skipRows"])
    elif cleaning_meta["sheetName"].lower() == "gas prices ":
        df = process_gas_prices(cleaning_meta["sheetName"], cleaning_meta["skipRows"])
    elif cleaning_meta["sheetName"].lower() == "oil crude prices since 1861":
        df = process_countryless_sheet(
            cleaning_meta["sheetName"],
            cleaning_meta["skipRows"],
            cleaning_meta["subvariable"],
        )
    elif cleaning_meta["sheetName"].lower() == "oil - spot crude prices":
        df = process_crude_prices(cleaning_meta["sheetName"], cleaning_meta["skipRows"])
    else:
        df = process_sheet(cleaning_meta["sheetName"], cleaning_meta["skipRows"])
    df.rename(
        columns={"country": "Country", "year": "Year", "value": "Value"}, inplace=True
    )
    assert "Year" in df.columns, "dataframe is missing a required column: 'Year'."
    assert "Country" in df.columns, "dataframe is missing a required column: 'Country'."
    assert "Value" in df.columns, "dataframe is missing a required column: 'Value'."
    df["Year"] = df["Year"].astype(int)
    df["Value"] = df["Value"].astype(float)
    df.dropna(how="any", inplace=True)
    return df


def process_sheet(sheet_name: str, skiprows: int) -> pd.DataFrame:
    data = (
        pd.read_excel(
            DATA_PATH, na_values=["n/a"], sheet_name=sheet_name, skiprows=skiprows
        )
        .dropna(how="all", axis=0)
        .dropna(how="all", axis=1)
    )
    col0 = data.columns[0]
    data = (
        data.set_index(col0)
        .loc[:"Total World", :LAST_YEAR_OF_DATA]
        .stack()
        .reset_index()
        .rename(
            columns={col0: "country", "level_1": "year", 0: "value"}, errors="raise"
        )
    )
    data["country"] = data["country"].apply(_rm_nonalpha_end_chars)
    data["year"] = (
        data["year"].astype(str).str.replace("at end ", "", regex=False).str.lstrip()
    )
    return data.dropna(how="any")


def process_countryless_sheet(
    sheet_name: str, skiprows: int, subvariable: str
) -> pd.DataFrame:
    data = (
        pd.read_excel(
            DATA_PATH, na_values=["n/a"], sheet_name=sheet_name, skiprows=skiprows
        )
        .dropna(how="all", axis=0)
        .dropna(how="all", axis=1)
    )
    col0 = data.columns[0]
    data = (
        data.set_index(col0)
        .filter(regex=r"^\d{4}$", axis=0)[subvariable]
        .reset_index()
        .rename(columns={col0: "year", subvariable: "value"}, errors="raise")
    )
    data["country"] = "Total World"

    return data.dropna(how="any")


def process_coal_prices(sheet_name: str, skiprows: int) -> pd.DataFrame:
    data = pd.read_excel(
        DATA_PATH, na_values=["n/a", "-"], sheet_name=sheet_name, skiprows=skiprows
    ).dropna(how="all")
    keep_cols = [
        x for x in data.columns if not re.search(r"^\s*$|unnamed", x, re.IGNORECASE)
    ]
    col0 = data.columns[0]
    data = (
        data[keep_cols]
        .set_index(col0)
        .dropna(how="any")
        .stack()
        .reset_index()
        .rename(
            columns={
                "level_1": "country",
                col0: "year",
                0: "value",
            },
            errors="raise",
        )
    )
    data["country"] = data["country"].apply(_rm_nonalpha_end_chars)
    data["year"] = (
        data["year"].astype(str).str.replace("at end ", "", regex=False).str.lstrip()
    )
    return data.dropna(how="any")


def process_coal_reserves(
    sheet_name: str, skiprows: int, subvariable: str
) -> pd.DataFrame:
    data = (
        pd.read_excel(
            DATA_PATH, na_values=["n/a"], sheet_name=sheet_name, skiprows=skiprows
        )
        .dropna(how="all")
        .rename(
            columns={
                "and bituminous": "Anthracite and bituminous",
                "and lignite": "Sub-bituminous and lignite",
            }
        )
    )
    data["year"] = LAST_YEAR_OF_DATA
    col0 = data.columns[0]
    data = (
        data.set_index(col0)
        .loc[:"Total World"]
        .reset_index()
        .rename(columns={col0: "country", subvariable: "value"}, errors="raise")
    )
    data["country"] = data["country"].apply(_rm_nonalpha_end_chars)
    return data[["country", "year", "value"]].dropna(how="any")


def process_gas_prices(sheet_name: str, skiprows: int) -> pd.DataFrame:
    data = (
        pd.read_excel(
            DATA_PATH,
            na_values=["n/a", "-"],
            sheet_name=sheet_name,
            skiprows=2,
            header=[0, 1, 2],
        )
        .dropna(how="all", axis=0)
        .dropna(how="all", axis=1)
    )
    data.set_index(data.columns[0], inplace=True)
    data.index.name = None
    data.columns = [f"{lvl0} - {lvl1} {lvl2}" for lvl0, lvl1, lvl2 in data.columns]
    data = (
        data.dropna(how="all")
        .stack()
        .reset_index()
        .rename(
            columns={
                "level_0": "year",
                "level_1": "country",
                0: "value",
            },
            errors="raise",
        )
    )
    data["country"] = data["country"].apply(_rm_nonalpha_end_chars)
    data["year"] = (
        data["year"].astype(str).str.replace("at end ", "", regex=False).str.lstrip()
    )
    return data.dropna(how="any")


def process_crude_prices(sheet_name: str, skiprows: int) -> pd.DataFrame:
    data = (
        pd.read_excel(
            DATA_PATH,
            na_values=["n/a", "-"],
            sheet_name=sheet_name,
            skiprows=skiprows,
            header=[0, 1, 2],
        )
        .dropna(how="all", axis=0)
        .dropna(how="all", axis=1)
    )
    columns = []
    for lvls in data.columns:
        col = ""
        for lvl in lvls[:-1]:
            if not re.search(r"^unnamed", lvl, re.IGNORECASE):
                col += f" {lvl}"
        columns.append(col.strip())
    data.columns = columns
    data = (
        data.set_index(data.columns[0])
        .filter(regex=r"^\d{4}$", axis=0)
        .dropna(how="all")
        .stack()
        .reset_index()
        .rename(
            columns={
                "": "year",
                "level_1": "country",
                0: "value",
            },
            errors="raise",
        )
    )
    data["value"] = data["value"].astype(float)
    data["country"] = data["country"].apply(_rm_nonalpha_end_chars)
    data["year"] = (
        data["year"].astype(str).str.replace("at end ", "", regex=False).str.lstrip()
    )
    return data.dropna(how="any")


def _rm_nonalpha_end_chars(s: str) -> str:
    """removes any characters at the end of a string that are not in A-z or an end
    parentheses ")"."""
    return re.sub(r"[^A-z\)]*$", "", s).strip()
