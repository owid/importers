import pandas as pd
import numpy as np

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


def average_annual_change_international_migrants_by_destination() -> None:
    migrants = international_migrants_by_destination()
    migrants.groupby("Country")
    years = range(1990, 2021)

    x = np.array([(x, y) for x in migrants["Country"].drop_duplicates() for y in years])
    annual_df = pd.DataFrame(x, columns=["Country", "Year"])

    annual_df.Country = annual_df.Country.astype(str)
    annual_df.Year = annual_df.Year.astype(int)
    migrants.Country = migrants.Country.astype(str)
    migrants.Year = migrants.Year.astype(int)
    migrants.international_migrants_by_destination = (
        migrants.international_migrants_by_destination.astype(int)
    )

    annual_df = annual_df.merge(
        migrants,
        how="left",
        left_on=["Country", "Year"],
        right_on=["Country", "Year"],
    )
    interp_df = annual_df.groupby("Country").apply(
        lambda x: x.interpolate(method="linear")
    )
    shifted = interp_df.groupby("Country").shift(+1)
    interp_df_lead = interp_df.join(shifted.rename(columns=lambda x: x + "_lead"))
    interp_df_lead["annual_average_change_in_international_migrants_by_destination"] = (
        interp_df_lead["international_migrants_by_destination"]
        - interp_df_lead["international_migrants_by_destination_lead"]
    )
    interp_df_lead = interp_df_lead[
        [
            "Country",
            "Year",
            "annual_average_change_in_international_migrants_by_destination",
        ]
    ]
    interp_df_lead.dropna(
        subset=["annual_average_change_in_international_migrants_by_destination"],
        inplace=True,
    )
    interp_df_lead[
        "annual_average_change_in_international_migrants_by_destination"
    ] = round(
        interp_df_lead["annual_average_change_in_international_migrants_by_destination"]
    )
    interp_df_lead[
        "annual_average_change_in_international_migrants_by_destination"
    ] = interp_df_lead[
        "annual_average_change_in_international_migrants_by_destination"
    ].astype(
        int
    )
    interp_df_lead.to_csv(
        "migration/ready/omm_annual_average_change_in_international_migrants_by_destination.csv"
    )
    return interp_df_lead


def average_annual_change_international_migrants_by_destination_per_capita() -> None:

    annual_change = average_annual_change_international_migrants_by_destination()
    population = owid_population()
    annual_change = annual_change.merge(
        population,
        how="inner",
        left_on=["Country", "Year"],
        right_on=["Country", "Year"],
    )
    annual_change[
        "annual_average_change_in_international_migrants_by_destination_per_capita"
    ] = (
        annual_change["annual_average_change_in_international_migrants_by_destination"]
        / annual_change["Population"]
    )
    annual_change[
        [
            "Year",
            "Country",
            "annual_average_change_in_international_migrants_by_destination_per_capita",
        ]
    ].to_csv(
        "migration/ready/omm_unhcr_annual_average_change_in_international_migrants_by_destination_per_capita.csv",
        index=False,
    )
    return annual_change


def average_annual_change_international_migrants_by_origin() -> None:
    migrants = international_migrants_by_origin()
    migrants.groupby("Country")
    years = range(1990, 2021)

    x = np.array([(x, y) for x in migrants["Country"].drop_duplicates() for y in years])
    annual_df = pd.DataFrame(x, columns=["Country", "Year"])

    annual_df.Country = annual_df.Country.astype(str)
    annual_df.Year = annual_df.Year.astype(int)
    migrants.Country = migrants.Country.astype(str)
    migrants.Year = migrants.Year.astype(int)
    migrants.international_migrants_by_destination = (
        migrants.international_migrants_by_destination.astype(int)
    )

    annual_df = annual_df.merge(
        migrants,
        how="left",
        left_on=["Country", "Year"],
        right_on=["Country", "Year"],
    )
    interp_df = annual_df.groupby("Country").apply(
        lambda x: x.interpolate(method="linear")
    )
    shifted = interp_df.groupby("Country").shift(+1)
    interp_df_lead = interp_df.join(shifted.rename(columns=lambda x: x + "_lead"))
    interp_df_lead["annual_average_change_in_international_migrants_by_origin"] = (
        interp_df_lead["international_migrants_by_origin"]
        - interp_df_lead["international_migrants_by_origin_lead"]
    )
    interp_df_lead = interp_df_lead[
        [
            "Country",
            "Year",
            "annual_average_change_in_international_migrants_by_origin",
        ]
    ]
    interp_df_lead.dropna(
        subset=["annual_average_change_in_international_migrants_by_origin"],
        inplace=True,
    )
    interp_df_lead["annual_average_change_in_international_migrants_by_origin"] = round(
        interp_df_lead["annual_average_change_in_international_migrants_by_origin"]
    )
    interp_df_lead[
        "annual_average_change_in_international_migrants_by_origin"
    ] = interp_df_lead[
        "annual_average_change_in_international_migrants_by_origin"
    ].astype(
        int
    )
    interp_df_lead.to_csv(
        "migration/ready/omm_annual_average_change_in_international_migrants_by_origin.csv"
    )
    return interp_df_lead


def average_annual_change_international_migrants_by_origin_per_capita() -> None:

    annual_change = average_annual_change_international_migrants_by_origin()
    population = owid_population()
    annual_change = annual_change.merge(
        population,
        how="inner",
        left_on=["Country", "Year"],
        right_on=["Country", "Year"],
    )
    annual_change[
        "annual_average_change_in_international_migrants_by_origin_per_capita"
    ] = (
        annual_change["annual_average_change_in_international_migrants_by_origin"]
        / annual_change["Population"]
    )
    annual_change[
        [
            "Year",
            "Country",
            "annual_average_change_in_international_migrants_by_origin_per_capita",
        ]
    ].to_csv(
        "migration/ready/omm_unhcr_annual_average_change_in_international_migrants_by_origin_per_capita.csv",
        index=False,
    )
    return annual_change
