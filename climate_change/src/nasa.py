import datetime
import requests

from bs4 import BeautifulSoup
import pandas as pd


def process_file(loc: str, source_url: str) -> pd.DataFrame:
    df = (
        pd.read_csv(
            source_url,
            skiprows=1,
            na_values="***",
            usecols=[
                "Year",
                "Jan",
                "Feb",
                "Mar",
                "Apr",
                "May",
                "Jun",
                "Jul",
                "Aug",
                "Sep",
                "Oct",
                "Nov",
                "Dec",
            ],
        )
        .assign(location=loc)
        .rename(columns={"Year": "year"})
        .melt(
            id_vars=["year", "location"],
            var_name="month",
            value_name="temperature_anomaly",
        )
    )
    df["date"] = pd.to_datetime(df.year.astype(str) + df.month + "15", format="%Y%b%d")
    return df[["location", "date", "temperature_anomaly"]].dropna(
        subset=["temperature_anomaly"]
    )


def global_temperature_anomaly() -> pd.DataFrame:
    df = pd.concat(
        [
            process_file(
                "World",
                "https://data.giss.nasa.gov/gistemp/tabledata_v4/GLB.Ts+dSST.csv",
            ),
            process_file(
                "Northern Hemisphere",
                "https://data.giss.nasa.gov/gistemp/tabledata_v4/NH.Ts+dSST.csv",
            ),
            process_file(
                "Southern Hemisphere",
                "https://data.giss.nasa.gov/gistemp/tabledata_v4/SH.Ts+dSST.csv",
            ),
        ]
    )
    df = df[df.date < datetime.datetime.now()]
    df.to_csv("ready/nasa_global-temperature-anomaly.csv", index=False)


def arctic_sea_ice_extent():
    source_url = "https://climate.nasa.gov/vital-signs/arctic-sea-ice/"
    soup = BeautifulSoup(requests.get(source_url).content, "html.parser")
    file_url = soup.find(class_="download_links").find("a").get("href")
    df = pd.read_excel("https://climate.nasa.gov" + file_url)
    (
        df[["year", "extent"]]
        .rename(columns={"extent": "arctic_sea_ice_nasa"})
        .assign(location="World")
        .to_csv("ready/nasa_arctic-sea-ice.csv", index=False)
    )


def main():
    global_temperature_anomaly()
    arctic_sea_ice_extent()


if __name__ == "__main__":
    main()
