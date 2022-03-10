from functools import reduce
import json
import pdb
import re
import requests
import yaml

from bs4 import BeautifulSoup
import pandas as pd


def get_config():
    with open("config.yml") as file:
        config = yaml.load(file, Loader=yaml.FullLoader)
    return config


def get_original_data(chart_id: int) -> pd.DataFrame:
    url = f"https://www.macrotrends.net/assets/php/chart_iframe_comp.php?id={chart_id}"
    soup = BeautifulSoup(requests.get(url).content, "html.parser")
    for script in soup.find_all("script"):
        if script.string:
            if "originalData" in script.string:
                data = re.search(r"originalData = ([^;]+)", script.string).group(1)
                break
    return pd.DataFrame.from_records(json.loads(data))


def clean_data(df: pd.DataFrame, name: str, conversion: float = None) -> pd.DataFrame:
    if "close1" in df.columns:
        df = df.drop(columns="close").rename(columns={"close1": "close"})
    df["close"] = df.close.astype(float)
    if conversion:
        df.loc[df.close.notnull(), "close"] = df.close.mul(conversion).round(2)
    return df.dropna(subset="close").drop(columns="id").rename(columns={"close": name})


def main():
    commodities = get_config()

    dataframes = []
    for com in commodities:
        print(com["name"])
        df = get_original_data(com["chart_id"]).pipe(
            clean_data, com["name"], com["conversion"]
        )
        dataframes.append(df)

    df = reduce(
        lambda df1, df2: pd.merge(
            df1, df2, on="date", how="outer", validate="one_to_one"
        ),
        dataframes,
    ).sort_values("date")

    df.to_csv("output/commodities.csv", index=False)


if __name__ == "__main__":
    main()
