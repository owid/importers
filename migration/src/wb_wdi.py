import requests, zipfile, io
import pandas as pd
import os.path
from migration.src.utils import standardise_countries, five_year_moving_window


def remittances_received_share_gdp() -> pd.DataFrame:
    if not os.path.exists(
        "migration/input/wb_wdi/remittances_received_share_gdp/API_BX.TRF.PWKR.DT.GD.ZS_DS2_en_csv_v2_3732529.csv"
    ):
        res = requests.get(
            "https://api.worldbank.org/v2/en/indicator/BX.TRF.PWKR.DT.GD.ZS?downloadformat=csv"
        )
        assert res.ok
        z = zipfile.ZipFile(io.BytesIO(res.content))
        z.extractall("migration/input/wb_wdi/remittances_received_share_gdp/")

    df = pd.read_csv(
        "migration/input/wb_wdi/remittances_received_share_gdp/API_BX.TRF.PWKR.DT.GD.ZS_DS2_en_csv_v2_3732529.csv",
        skiprows=4,
    )

    years = [str(x) for x in range(1960, 2021)]
    df = pd.melt(
        df,
        id_vars=["Country Name"],
        value_vars=years,
    )
    df = df.dropna()
    df["Country Name"] = standardise_countries(df["Country Name"])
    df = df.rename(
        columns={
            "Country Name": "Country",
            "variable": "Year",
            "value": "wdi_remittances_received_share_gdp",
        }
    )
    df = five_year_moving_window(df)
    df.to_csv("migration/ready/wdi_remittances_received_share_gdp.csv", index=False)


def average_cost_sending_remittances_from_country() -> pd.DataFrame:
    if not os.path.exists(
        "migration/input/wb_wdi/average_cost_sending_remittances_from_country/API_SI.RMT.COST.OB.ZS_DS2_en_csv_v2_3759305.csv"
    ):
        res = requests.get(
            "https://api.worldbank.org/v2/en/indicator/SI.RMT.COST.OB.ZS?downloadformat=csv"
        )
        assert res.ok
        z = zipfile.ZipFile(io.BytesIO(res.content))
        z.extractall(
            "migration/input/wb_wdi/average_cost_sending_remittances_from_country/"
        )
    df = pd.read_csv(
        "migration/input/wb_wdi/average_cost_sending_remittances_from_country/API_SI.RMT.COST.OB.ZS_DS2_en_csv_v2_3759305.csv",
        skiprows=4,
    )

    years = [str(x) for x in range(1960, 2021)]
    df = pd.melt(
        df,
        id_vars=["Country Name"],
        value_vars=years,
    )
    df = df.dropna()
    df["Country Name"] = standardise_countries(df["Country Name"])
    df = df.rename(
        columns={
            "Country Name": "Country",
            "variable": "Year",
            "value": "wdi_average_cost_sending_remittances_from_country",
        }
    )
    df = five_year_moving_window(df)
    df.to_csv(
        "migration/ready/wdi_average_cost_sending_remittances_from_country.csv",
        index=False,
    )


def average_cost_sending_remittances_to_country() -> pd.DataFrame:
    if not os.path.exists(
        "migration/input/wb_wdi/average_cost_sending_remittances_to_country/API_SI.RMT.COST.IB.ZS_DS2_en_csv_v2_3758354.csv"
    ):
        res = requests.get(
            "https://api.worldbank.org/v2/en/indicator/SI.RMT.COST.IB.ZS?downloadformat=csv"
        )
        assert res.ok
        z = zipfile.ZipFile(io.BytesIO(res.content))
        z.extractall(
            "migration/input/wb_wdi/average_cost_sending_remittances_to_country/"
        )
    df = pd.read_csv(
        "migration/input/wb_wdi/average_cost_sending_remittances_to_country/API_SI.RMT.COST.IB.ZS_DS2_en_csv_v2_3758354.csv",
        skiprows=4,
    )

    years = [str(x) for x in range(1960, 2021)]
    df = pd.melt(
        df,
        id_vars=["Country Name"],
        value_vars=years,
    )
    df = df.dropna()
    df["Country Name"] = standardise_countries(df["Country Name"])
    df = df.rename(
        columns={
            "Country Name": "Country",
            "variable": "Year",
            "value": "wdi_average_cost_sending_remittances_to_country",
        }
    )
    df = five_year_moving_window(df)
    df.to_csv(
        "migration/ready/wdi_average_cost_sending_remittances_to_country.csv",
        index=False,
    )
