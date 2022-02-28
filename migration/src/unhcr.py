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
    z.extractall("migration/input/unhcr/refugees_by_destination/")

    df = pd.read_csv(
        "migration/input/unhcr/refugees_by_destination/population.csv", skiprows=14
    )
    df = df[["Year", "Country of asylum", "Refugees under UNHCR's mandate"]]
    df["Country of asylum"] = standardise_countries(df["Country of asylum"])
    df.to_csv("migration/output/unhcr_refugees_by_destination.csv", index=False)
    return df


def standardise_countries(country=pd.Series) -> pd.DataFrame:
    owid_countries = (
        pd.read_csv("migration/countries_to_standardise_country_standardized.csv")
        .set_index("Country")
        .squeeze()
        .to_dict()
    )

    countries_standardised = country.apply(lambda x: owid_countries[x])
    return countries_standardised


def refugees_by_destination_per_capita() -> pd.DataFrame:
    population = owid_population()
    refugees = refugees_by_destination()
    # refugees["Country of asylum"] = standardise_countries(refugees["Country of asylum"])
    refugees = refugees.merge(
        population,
        how="inner",
        left_on=["Country of asylum", "Year"],
        right_on=["Country", "Year"],
    )
    refugees["refugees_per_capita"] = (
        refugees["Refugees under UNHCR's mandate"] / refugees["Population"]
    )
    refugees.to_csv(
        "migration/output/omm_unhcr_refugees_by_destination_per_capita.csv", index=False
    )
    return refugees


def refugees_by_origin() -> pd.DataFrame:
    res = requests.get(
        "https://api.unhcr.org/population/v1/population/?limit=20&dataset=population&displayType=totals&columns%5B%5D=refugees&yearFrom=1951&yearTo=2021&coo_all=true&download=true#_ga=2.95743388.180802795.1646058128-293029033.1646058128"
    )
    assert res.ok
    z = zipfile.ZipFile(io.BytesIO(res.content))
    z.extractall("migration/input/unhcr/refugees_by_origin/")

    df = pd.read_csv(
        "migration/input/unhcr/refugees_by_origin/population.csv", skiprows=14
    )
    df = df[["Year", "Country of origin", "Refugees under UNHCR's mandate"]]
    df["Country of origin"] = standardise_countries(df["Country of origin"])
    df.to_csv("migration/output/unhcr_refugees_by_origin.csv", index=False)
    return df


def refugees_by_origin_per_capita() -> pd.DataFrame:
    population = owid_population()
    refugees = refugees_by_origin()
    # refugees["Country of origin"] = standardise_countries(refugees["Country of origin"])
    refugees = refugees.merge(
        population,
        how="inner",
        left_on=["Country of asylum", "Year"],
        right_on=["Country", "Year"],
    )
    refugees["refugees_per_capita"] = (
        refugees["Refugees under UNHCR's mandate"] / refugees["Population"]
    )
    refugees.to_csv(
        "migration/output/omm_unhcr_refugees_by_destination_per_capita.csv", index=False
    )
    return refugees


rbd = refugees_by_destination()
rbo = refugees_by_origin()

list(
    dict.fromkeys(
        rbd["Country of asylum"].to_list() + rbo["Country of origin"].to_list()
    )
)

df = pd.DataFrame(
    list(
        dict.fromkeys(
            rbd["Country of asylum"].to_list() + rbo["Country of origin"].to_list()
        )
    ),
    columns=["Country"],
)
