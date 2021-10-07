import requests

from bs4 import BeautifulSoup
import pandas as pd


def process_concentration_file(
    gas: str, source_url: str, skiprows: int
) -> pd.DataFrame:
    df = pd.read_fwf(source_url, skiprows=skiprows)
    df["date"] = pd.to_datetime(
        df.year.astype(str) + "-" + df.month.astype(str) + "-15"
    )
    return (
        df[["date", "average", "trend"]]
        .rename(
            columns={
                "average": f"average_{gas}_concentrations",
                "trend": f"trend_{gas}_concentrations",
            }
        )
        .assign(location="World")
    )


def monthly_concentrations() -> pd.DataFrame:
    gases = {
        "co2": ("https://gml.noaa.gov/webdata/ccgg/trends/co2/co2_mm_gl.txt", 57),
        "ch4": ("https://gml.noaa.gov/webdata/ccgg/trends/ch4/ch4_mm_gl.txt", 60),
        "n2o": ("https://gml.noaa.gov/webdata/ccgg/trends/n2o/n2o_mm_gl.txt", 60),
    }
    for k, v in gases.items():
        process_concentration_file(gas=k, source_url=v[0], skiprows=v[1]).to_csv(
            f"ready/noaa_monthly-{k}-concentrations.csv", index=False
        )


def get_sea_level_url(source_page: str) -> str:
    soup = BeautifulSoup(requests.get(source_page).content, "html.parser")
    links = soup.find_all("a")
    for link in links:
        if link.text == "download":
            source_url = link["href"]
            break
    return source_url


def sea_level_rise():
    source_page = "https://www.climate.gov/news-features/understanding-climate/climate-change-global-sea-level"
    source_url = get_sea_level_url(source_page)
    df = (
        pd.read_csv(
            source_url,
            sep="\t",
            encoding_errors="ignore",
            usecols=["Date", "CW_2011", "UHSLC_FD"],
        )
        .rename(
            columns={
                "Date": "date",
                "CW_2011": "sea_level_rise_cw2011",
                "UHSLC_FD": "sea_level_rise_uhslcfd",
            }
        )
        .assign(location="World")
    )

    df["sea_level_rise_average"] = (
        df[["sea_level_rise_cw2011", "sea_level_rise_uhslcfd"]].mean(axis=1).round(5)
    )

    # Fix dates
    start = df.index[df.date == "1/15/00"][0]
    end = df.index[df.date == "1/15/00"][1]
    df.loc[(df.index >= start) & (df.index < end), "date"] = (
        df.date.str.slice(0, -2) + "19" + df.date.str.slice(-2)
    )
    df.loc[df.index >= end, "date"] = (
        df.date.str.slice(0, -2) + "20" + df.date.str.slice(-2)
    )
    df.loc[df.index >= start, "date"] = pd.to_datetime(df.date).dt.date

    df.to_csv("ready/noaa_sea-level-rise.csv", index=False)


def main():
    monthly_concentrations()
    sea_level_rise()


if __name__ == "__main__":
    main()
