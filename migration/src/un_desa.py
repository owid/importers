import pandas as pd

from migration.src.utils import is_number, standardise_countries, owid_population


def international_migrants_by_destination() -> pd.DataFrame:
    df = pd.read_excel(
        "https://www.un.org/development/desa/pd/sites/www.un.org.development.desa.pd/files/undesa_pd_2020_ims_stock_by_sex_and_destination.xlsx",
        sheet_name="Table 1",
        skiprows=10,
        usecols="B:L",
    )
    df["Region, development group, country or area"] = standardise_countries(
        df["Region, development group, country or area"]
    )
    df = df[
        [
            "Region, development group, country or area",
            1990,
            1995,
            2000,
            2005,
            2010,
            2015,
            2020,
        ]
    ]
    df = pd.melt(
        df,
        id_vars=["Region, development group, country or area"],
        value_vars=[1990, 1995, 2000, 2005, 2010, 2015, 2020],
    )
    df.rename(
        columns={
            "Region, development group, country or area": "Country",
            "variable": "Year",
            "value": "international_migrants_by_destination",
        },
        inplace=True,
    )
    df = df[df["international_migrants_by_destination"].apply(is_number)]
    df.to_csv(
        "migration/ready/undesa_international_migrants_by_destination.csv", index=False
    )
    return df


def share_of_pop_international_migrants_by_destination() -> pd.DataFrame:
    df = pd.read_excel(
        "https://www.un.org/development/desa/pd/sites/www.un.org.development.desa.pd/files/undesa_pd_2020_ims_stock_by_sex_and_destination.xlsx",
        sheet_name="Table 3",
        skiprows=10,
        usecols="B:L",
    )
    df["Region, development group, country or area"] = standardise_countries(
        df["Region, development group, country or area"]
    )
    df = df[
        [
            "Region, development group, country or area",
            1990,
            1995,
            2000,
            2005,
            2010,
            2015,
            2020,
        ]
    ]
    df = pd.melt(
        df,
        id_vars=["Region, development group, country or area"],
        value_vars=[1990, 1995, 2000, 2005, 2010, 2015, 2020],
    )
    df.rename(
        columns={
            "Region, development group, country or area": "Country",
            "variable": "Year",
            "value": "share_of_population_that_are_international_migrants_by_destination",
        },
        inplace=True,
    )
    df = df[
        df["share_of_population_that_are_international_migrants_by_destination"].apply(
            is_number
        )
    ]
    df.to_csv(
        "migration/ready/undesa_share_of_population_that_are_international_migrants_by_destination.csv",
        index=False,
    )
    return df


def international_migrants_by_origin() -> pd.DataFrame:
    df = pd.read_excel(
        "https://www.un.org/development/desa/pd/sites/www.un.org.development.desa.pd/files/undesa_pd_2020_ims_stock_by_sex_and_origin.xlsx",
        sheet_name="Table 1",
        skiprows=10,
        usecols="B:L",
    )
    df["Region, development group, country or area"] = standardise_countries(
        df["Region, development group, country or area"]
    )
    df = df[
        [
            "Region, development group, country or area",
            1990,
            1995,
            2000,
            2005,
            2010,
            2015,
            2020,
        ]
    ]
    df = pd.melt(
        df,
        id_vars=["Region, development group, country or area"],
        value_vars=[1990, 1995, 2000, 2005, 2010, 2015, 2020],
    )
    df.rename(
        columns={
            "Region, development group, country or area": "Country",
            "variable": "Year",
            "value": "international_migrants_by_origin",
        },
        inplace=True,
    )
    df = df[df["international_migrants_by_origin"].apply(is_number)]
    df.to_csv(
        "migration/ready/undesa_international_migrants_by_origin.csv", index=False
    )
    return df


def refugees_by_destination() -> None:
    df = pd.read_excel(
        "https://www.un.org/development/desa/pd/sites/www.un.org.development.desa.pd/files/undesa_pd_2020_ims_stock_by_sex_and_destination.xlsx",
        sheet_name="Table 6",
        skiprows=10,
        usecols="B:L",
    )
    df["Region, development group, country or area"] = standardise_countries(
        df["Region, development group, country or area"]
    )
    df = df[
        [
            "Region, development group, country or area",
            1990,
            1995,
            2000,
            2005,
            2010,
            2015,
            2020,
        ]
    ]
    df = pd.melt(
        df,
        id_vars=["Region, development group, country or area"],
        value_vars=[1990, 1995, 2000, 2005, 2010, 2015, 2020],
    )
    df.rename(
        columns={
            "Region, development group, country or area": "Country",
            "variable": "Year",
            "value": "un_desa_refugees_by_destination",
        },
        inplace=True,
    )
    df = df[df["un_desa_refugees_by_destination"].apply(is_number)]
    df.to_csv("migration/ready/undesa_refugees_by_destination.csv", index=False)
    return df


def refugees_by_destination_per_capita() -> None:
    refugees = refugees_by_destination()
    population = owid_population()
    refugees = refugees.merge(
        population,
        how="inner",
        left_on=["Country", "Year"],
        right_on=["Country", "Year"],
    )
    refugees["un_desa_refugees_by_destination_per_capita"] = (
        refugees["un_desa_refugees_by_destination"] / refugees["Population"]
    )
    refugees[["Year", "Country", "un_desa_refugees_by_destination_per_capita"]].to_csv(
        "migration/ready/omm_un_desa_refugees_by_destination_per_capita.csv",
        index=False,
    )
