import requests, zipfile, io
import pandas as pd
from owid import catalog


def owid_population() -> pd.DataFrame:
    population = (
        catalog.find("population", namespace="owid")
        .load()
        .reset_index()
        .rename(
            columns={"country": "Country", "year": "Year", "population": "Population"}
        )[["Country", "Year", "Population"]]
    )
    return population


def refugees_by_destination() -> pd.DataFrame:
    res = requests.get(
        "https://api.unhcr.org/population/v1/population/?limit=20&dataset=population&displayType=totals&columns%5B%5D=refugees&columns%5B%5D=stateless&columns%5B%5D=ooc&yearFrom=1951&yearTo=2021&coa_all=true&download=true#_ga=2.204253328.180802795.1646058128-293029033.1646058128"
    )
    assert res.ok
    z = zipfile.ZipFile(io.BytesIO(res.content))
    z.extractall("migration/input/unhcr")

    df = pd.read_csv("migration/input/unhcr/population.csv", skiprows=14)
    df = df[["Year", "Country of asylum", "Refugees under UNHCR's mandate"]]
    df.to_csv("migration/output/unhcr_refugees.csv", index=False)
    return df


def refugees_by_destination_per_capita() -> pd.DataFrame:
    population = owid_population()
    refugees = refugees_by_destination()
