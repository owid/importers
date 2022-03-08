import requests, zipfile, io
import pandas as pd

from migration.src.utils import (
    standardise_countries,
    owid_population,
    five_year_moving_window,
)


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
    df = five_year_moving_window(df)

    df.to_csv("migration/ready/unhcr_refugees_by_destination.csv", index=False)
    return df


def refugees_by_destination_per_1000() -> pd.DataFrame:
    population = owid_population()
    refugees = refugees_by_destination()
    # refugees["Country of asylum"] = standardise_countries(refugees["Country of asylum"])
    refugees = refugees.merge(
        population,
        how="inner",
        left_on=["Country", "Year"],
        right_on=["Country", "Year"],
    )
    refugees["unhcr_refugees_by_destination_per_1000"] = (
        refugees["unhcr_refugees_by_destination"] / refugees["Population"]
    ) * 1000
    refugees = refugees[["Year", "Country", "unhcr_refugees_by_destination_per_1000"]]
    refugees.to_csv(
        "migration/ready/omm_unhcr_refugees_by_destination_per_1000.csv", index=False
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
    df = five_year_moving_window(df)
    df.to_csv("migration/ready/unhcr_refugees_by_origin.csv", index=False)
    return df


def refugees_by_origin_per_1000() -> pd.DataFrame:
    population = owid_population()
    refugees = refugees_by_origin()
    refugees = refugees.merge(
        population,
        how="inner",
        left_on=["Country", "Year"],
        right_on=["Country", "Year"],
    )
    refugees["unhcr_refugees_by_origin_per_1000"] = (
        refugees["unhcr_refugees_by_origin"] / refugees["Population"]
    ) * 1000
    refugees = refugees[["Year", "Country", "unhcr_refugees_by_origin_per_1000"]]
    refugees.to_csv(
        "migration/ready/omm_unhcr_refugees_by_destination_per_1000.csv", index=False
    )
    return refugees


def asylum_seekers_by_origin() -> pd.DataFrame:
    res = requests.get(
        "https://api.unhcr.org/population/v1/population/?limit=20&dataset=population&displayType=totals&columns%5B%5D=asylum_seekers&yearFrom=1951&yearTo=2021&coo_all=true&download=true#_ga=2.172262819.180802795.1646058128-293029033.1646058128"
    )
    assert res.ok
    z = zipfile.ZipFile(io.BytesIO(res.content))
    z.extractall("migration/input/unhcr/asylum_seekers_by_origin/")
    df = pd.read_csv(
        "migration/input/unhcr/asylum_seekers_by_origin/population.csv", skiprows=14
    )
    df = df[["Country of origin", "Year", "Asylum-seekers"]]
    df["Country of origin"] = standardise_countries(df["Country of origin"])
    df.rename(
        columns={
            "Country of origin": "Country",
            "Asylum-seekers": "unhcr_asylum_seekers_by_origin",
        },
        inplace=True,
    )
    df = five_year_moving_window(df)
    df.to_csv("migration/ready/unhcr_asylum_seekers_by_origin.csv", index=False)
    return df


def asylum_seekers_by_origin_per_100000() -> pd.DataFrame:
    asylum = asylum_seekers_by_origin()
    population = owid_population()
    asylum = asylum.merge(
        population,
        how="inner",
        left_on=["Country", "Year"],
        right_on=["Country", "Year"],
    )
    asylum["unhcr_asylum_seekers_by_origin_per_100000"] = (
        asylum["unhcr_asylum_seekers_by_origin"] / asylum["Population"]
    ) * 100000
    asylum = asylum[["Year", "Country", "unhcr_asylum_seekers_by_origin_per_100000"]]
    asylum.to_csv(
        "migration/ready/omm_unhcr_asylum_seekers_by_origin_per_100000.csv", index=False
    )
    return asylum


def asylum_seekers_by_destination() -> pd.DataFrame:
    res = requests.get(
        "https://api.unhcr.org/population/v1/population/?limit=20&dataset=population&displayType=totals&columns%5B%5D=asylum_seekers&yearFrom=1951&yearTo=2021&coa_all=true&download=true#_ga=2.143090997.180802795.1646058128-293029033.1646058128"
    )
    assert res.ok
    z = zipfile.ZipFile(io.BytesIO(res.content))
    z.extractall("migration/input/unhcr/asylum_seekers_by_destination/")
    df = pd.read_csv(
        "migration/input/unhcr/asylum_seekers_by_destination/population.csv",
        skiprows=14,
    )
    df = df[["Country of asylum", "Year", "Asylum-seekers"]]
    df["Country of asylum"] = standardise_countries(df["Country of asylum"])
    df.rename(
        columns={
            "Country of asylum": "Country",
            "Asylum-seekers": "unhcr_asylum_seekers_by_destination",
        },
        inplace=True,
    )
    df = five_year_moving_window(df)
    df.to_csv("migration/ready/unhcr_asylum_seekers_by_destination.csv", index=False)
    return df


def asylum_seekers_by_destination_per_100000() -> pd.DataFrame:
    asylum = asylum_seekers_by_destination()
    population = owid_population()
    asylum = asylum.merge(
        population,
        how="inner",
        left_on=["Country", "Year"],
        right_on=["Country", "Year"],
    )
    asylum["unhcr_asylum_seekers_by_destination_per_100000"] = (
        asylum["unhcr_asylum_seekers_by_destination"] / asylum["Population"]
    ) * 100000
    asylum = asylum[
        ["Year", "Country", "unhcr_asylum_seekers_by_destination_per_100000"]
    ]
    asylum.to_csv(
        "migration/ready/omm_unhcr_asylum_seekers_by_destination_per_100000.csv",
        index=False,
    )
    return asylum


def resettlement_arrivals_by_destination() -> pd.DataFrame:
    res = requests.get(
        "https://api.unhcr.org/population/v1/solutions/?limit=20&dataset=solutions&displayType=totals&columns%5B%5D=resettlement&yearFrom=1951&yearTo=2021&coa_all=true&populationType=unrwa&download=true#_ga=2.204318096.180802795.1646058128-293029033.1646058128"
    )
    assert res.ok
    z = zipfile.ZipFile(io.BytesIO(res.content))
    z.extractall("migration/input/unhcr/resettlement_arrivals_by_destination/")
    df = pd.read_csv(
        "migration/input/unhcr/resettlement_arrivals_by_destination/solutions.csv",
        skiprows=15,
    )
    df = df[["Country of asylum", "Year", "Resettlement arrivals"]]
    df["Country of asylum"] = standardise_countries(df["Country of asylum"])
    df.rename(
        columns={
            "Country of asylum": "Country",
            "Resettlement arrivals": "unhcr_resettlement_arrivals_by_destination",
        },
        inplace=True,
    )
    df.to_csv(
        "migration/ready/unhcr_resettlement_arrivals_by_destination.csv", index=False
    )
    return df


def resettlement_arrivals_by_destination_per_100000() -> pd.DataFrame:
    resettle = resettlement_arrivals_by_destination()
    population = owid_population()
    resettle = resettle.merge(
        population,
        how="inner",
        left_on=["Country", "Year"],
        right_on=["Country", "Year"],
    )
    resettle["unhcr_resettlement_arrivals_by_destination_per_100000"] = (
        resettle["unhcr_resettlement_arrivals_by_destination"] / resettle["Population"]
    ) * 100000

    resettle = resettle[
        ["Year", "Country", "unhcr_resettlement_arrivals_by_destination_per_100000"]
    ]

    resettle.to_csv(
        "migration/ready/omm_unhcr_resettlement_arrivals_by_destination_per_100000.csv",
        index=False,
    )
    return resettle


def resettlement_arrivals_by_origin() -> pd.DataFrame:
    res = requests.get(
        "https://api.unhcr.org/population/v1/solutions/?limit=20&dataset=solutions&displayType=totals&columns%5B%5D=resettlement&yearFrom=1951&yearTo=2021&coo_all=true&populationType=unrwa&download=true#_ga=2.133475342.180802795.1646058128-293029033.1646058128"
    )
    assert res.ok
    z = zipfile.ZipFile(io.BytesIO(res.content))
    z.extractall("migration/input/unhcr/resettlement_arrivals_by_origin/")
    df = pd.read_csv(
        "migration/input/unhcr/resettlement_arrivals_by_origin/solutions.csv",
        skiprows=15,
    )
    df = df[["Country of origin", "Year", "Resettlement arrivals"]]
    df["Country of origin"] = standardise_countries(df["Country of origin"])
    df.rename(
        columns={
            "Country of origin": "Country",
            "Resettlement arrivals": "unhcr_resettlement_arrivals_by_origin",
        },
        inplace=True,
    )
    df.to_csv("migration/ready/unhcr_resettlement_arrivals_by_origin.csv", index=False)
    return df


def resettlement_arrivals_by_origin_per_100000() -> pd.DataFrame:
    resettle = resettlement_arrivals_by_origin()
    population = owid_population()
    resettle = resettle.merge(
        population,
        how="inner",
        left_on=["Country", "Year"],
        right_on=["Country", "Year"],
    )
    resettle["unhcr_resettlement_arrivals_by_origin_per_100000"] = (
        resettle["unhcr_resettlement_arrivals_by_origin"] / resettle["Population"]
    ) * 100000
    resettle = resettle[
        ["Year", "Country", "unhcr_resettlement_arrivals_by_origin_per_100000"]
    ]
    resettle.to_csv(
        "migration/ready/omm_unhcr_resettlement_arrivals_by_origin_per_100000.csv",
        index=False,
    )
    return resettle
