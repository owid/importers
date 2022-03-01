import requests, zipfile, io
import pandas as pd

from migration.src.utils import standardise_countries, owid_population


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
    df.rename(
        columns={
            "Country of asylum": "Country",
            "Refugees under UNHCR's mandate": "unhcr_refugees_by_destination",
        },
        inplace=True,
    )

    df.to_csv("migration/ready/unhcr_refugees_by_destination.csv", index=False)
    return df


def refugees_by_destination_per_capita() -> pd.DataFrame:
    population = owid_population()
    refugees = refugees_by_destination()
    # refugees["Country of asylum"] = standardise_countries(refugees["Country of asylum"])
    refugees = refugees.merge(
        population,
        how="inner",
        left_on=["Country", "Year"],
        right_on=["Country", "Year"],
    )
    refugees["unhcr_refugees_by_destination_per_capita"] = (
        refugees["refugees_by_destination"] / refugees["Population"]
    )
    refugees[["Year", "Country", "unhcr_refugees_by_destination_per_capita"]].to_csv(
        "migration/ready/omm_unhcr_refugees_by_destination_per_capita.csv", index=False
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
    df.rename(
        columns={
            "Country of origin": "Country",
            "Refugees under UNHCR's mandate": "unhcr_refugees_by_origin",
        },
        inplace=True,
    )
    df.to_csv("migration/ready/unhcr_refugees_by_origin.csv", index=False)
    return df


def refugees_by_origin_per_capita() -> pd.DataFrame:
    population = owid_population()
    refugees = refugees_by_origin()
    refugees = refugees.merge(
        population,
        how="inner",
        left_on=["Country", "Year"],
        right_on=["Country", "Year"],
    )
    refugees["refugees_by_origin_per_capita"] = (
        refugees["refugees_by_origin"] / refugees["Population"]
    )
    refugees[["Year", "Country", "unhcr_refugees_by_origin_per_capita"]].to_csv(
        "migration/ready/omm_unhcr_refugees_by_destination_per_capita.csv", index=False
    )
    return refugees


def main():
    refugees_by_destination()
    refugees_by_destination_per_capita()
    refugees_by_origin()
    refugees_by_origin_per_capita()


if __name__ == "__main__":
    main()
