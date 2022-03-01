from operator import is_
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
    refugees = refugees[
        refugees["international_migrants_by_destination"].apply(is_number)
    ]
    refugees[["Year", "Country", "un_desa_refugees_by_destination_per_capita"]].to_csv(
        "migration/ready/omm_un_desa_refugees_by_destination_per_capita.csv",
        index=False,
    )


def average_annual_change_international_migrants_by_destination() -> None:
    migrants = international_migrants_by_destination()
    # migrants.groupby("Country")
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
    interp_df_lead = interp_df_lead[
        interp_df_lead[
            "annual_average_change_in_international_migrants_by_destination"
        ].apply(is_number)
    ]

    interp_df_lead.to_csv(
        "migration/ready/omm_annual_average_change_in_international_migrants_by_destination.csv"
    )
    return interp_df_lead


def change_in_international_migrants_by_destination() -> None:
    migrants = international_migrants_by_destination()
    shifted = migrants.groupby("Country").shift(+1)
    migrants_lead = migrants.join(shifted.rename(columns=lambda x: x + "_lead"))
    migrants_lead[
        "undesa_five_year_change_in_international_migrants_by_destination"
    ] = (
        migrants_lead["international_migrants_by_destination"]
        - migrants_lead["international_migrants_by_destination_lead"]
    )
    migrants_lead = migrants_lead[
        [
            "Country",
            "Year",
            "undesa_five_year_change_in_international_migrants_by_destination",
        ]
    ]
    migrants_lead.dropna(
        subset=["undesa_five_year_change_in_international_migrants_by_destination"],
        inplace=True,
    )
    migrants_lead[
        "undesa_five_year_change_in_international_migrants_by_destination"
    ] = migrants_lead[
        "undesa_five_year_change_in_international_migrants_by_destination"
    ].astype(
        int
    )
    migrants_lead = migrants_lead[
        migrants_lead[
            "undesa_five_year_change_in_international_migrants_by_destination"
        ].apply(is_number)
    ]

    migrants_lead = migrants_lead[migrants_lead.Year > 1990]

    migrants_lead.to_csv(
        "migration/ready/undesa_five_year_change_in_international_migrants_by_destination.csv"
    )
    return migrants_lead


def change_in_international_migrants_by_origin() -> None:
    migrants = international_migrants_by_origin()
    shifted = migrants.groupby("Country").shift(+1)
    migrants_lead = migrants.join(shifted.rename(columns=lambda x: x + "_lead"))
    migrants_lead["undesa_five_year_change_in_international_migrants_by_origin"] = (
        migrants_lead["international_migrants_by_origin"]
        - migrants_lead["international_migrants_by_origin_lead"]
    )
    migrants_lead = migrants_lead[
        [
            "Country",
            "Year",
            "undesa_five_year_change_in_international_migrants_by_origin",
        ]
    ]
    migrants_lead.dropna(
        subset=["undesa_five_year_change_in_international_migrants_by_origin"],
        inplace=True,
    )
    migrants_lead[
        "undesa_five_year_change_in_international_migrants_by_origin"
    ] = migrants_lead[
        "undesa_five_year_change_in_international_migrants_by_origin"
    ].astype(
        int
    )
    migrants_lead = migrants_lead[
        migrants_lead[
            "undesa_five_year_change_in_international_migrants_by_origin"
        ].apply(is_number)
    ]

    migrants_lead = migrants_lead[migrants_lead.Year > 1990]

    migrants_lead.to_csv(
        "migration/ready/undesa_five_year_change_in_international_migrants_by_origin.csv"
    )
    return migrants_lead


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

    annual_change = annual_change[
        annual_change[
            "annual_average_change_in_international_migrants_by_destination_per_capita"
        ].apply(is_number)
    ]

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


def change_in_international_migrants_by_destination_per_capita() -> pd.DataFrame:
    migrants = change_in_international_migrants_by_destination()
    population = owid_population()
    migrants = migrants.merge(
        population,
        how="inner",
        left_on=["Country", "Year"],
        right_on=["Country", "Year"],
    )
    migrants[
        "undesa_five_year_change_in_international_migrants_by_destination_per_capita"
    ] = (
        migrants["undesa_five_year_change_in_international_migrants_by_destination"]
        / migrants["Population"]
    )

    migrants = migrants[
        migrants[
            "undesa_five_year_change_in_international_migrants_by_destination_per_capita"
        ].apply(is_number)
    ]

    migrants[
        [
            "Year",
            "Country",
            "undesa_five_year_change_in_international_migrants_by_destination_per_capita",
        ]
    ].to_csv(
        "migration/ready/undesa_five_year_change_in_international_migrants_by_destination_per_capita.csv",
        index=False,
    )
    return migrants


def change_in_international_migrants_by_destination_per_capita() -> None:
    migrants = international_migrants_by_destination()
    shifted = migrants.groupby("Country").shift(+1)
    migrants_lead = migrants.join(shifted.rename(columns=lambda x: x + "_lead"))
    migrants_lead[
        "undesa_five_year_change_in_international_migrants_by_destination"
    ] = (
        migrants_lead["international_migrants_by_destination"]
        - migrants_lead["international_migrants_by_destination_lead"]
    )
    migrants_lead = migrants_lead[
        [
            "Country",
            "Year",
            "undesa_five_year_change_in_international_migrants_by_destination",
        ]
    ]
    migrants_lead.dropna(
        subset=["undesa_five_year_change_in_international_migrants_by_destination"],
        inplace=True,
    )
    migrants_lead[
        "undesa_five_year_change_in_international_migrants_by_destination"
    ] = migrants_lead[
        "undesa_five_year_change_in_international_migrants_by_destination"
    ].astype(
        int
    )
    migrants_lead = migrants_lead[
        migrants_lead[
            "undesa_five_year_change_in_international_migrants_by_destination"
        ].apply(is_number)
    ]

    migrants_lead = migrants_lead[migrants_lead.Year > 1990]

    migrants_lead.to_csv(
        "migration/ready/undesa_five_year_change_in_international_migrants_by_destination.csv"
    )
    return migrants_lead


def change_in_international_migrants_by_origin_per_capita() -> None:
    migrants = international_migrants_by_origin()
    shifted = migrants.groupby("Country").shift(+1)
    migrants_lead = migrants.join(shifted.rename(columns=lambda x: x + "_lead"))
    migrants_lead["undesa_five_year_change_in_international_migrants_by_origin"] = (
        migrants_lead["international_migrants_by_origin"]
        - migrants_lead["international_migrants_by_origin_lead"]
    )
    migrants_lead = migrants_lead[
        [
            "Country",
            "Year",
            "undesa_five_year_change_in_international_migrants_by_origin",
        ]
    ]
    migrants_lead.dropna(
        subset=["undesa_five_year_change_in_international_migrants_by_origin"],
        inplace=True,
    )
    migrants_lead[
        "undesa_five_year_change_in_international_migrants_by_origin"
    ] = migrants_lead[
        "undesa_five_year_change_in_international_migrants_by_origin"
    ].astype(
        int
    )
    migrants_lead = migrants_lead[
        migrants_lead[
            "undesa_five_year_change_in_international_migrants_by_origin"
        ].apply(is_number)
    ]

    migrants_lead = migrants_lead[migrants_lead.Year > 1990]

    migrants_lead.to_csv(
        "migration/ready/undesa_five_year_change_in_international_migrants_by_origin.csv"
    )
    return migrants_lead


def average_annual_change_international_migrants_by_origin() -> pd.DataFrame:
    migrants = international_migrants_by_origin()
    migrants.groupby("Country")
    years = range(1990, 2021)

    x = np.array([(x, y) for x in migrants["Country"].drop_duplicates() for y in years])
    annual_df = pd.DataFrame(x, columns=["Country", "Year"])

    annual_df.Country = annual_df.Country.astype(str)
    annual_df.Year = annual_df.Year.astype(int)
    migrants.Country = migrants.Country.astype(str)
    migrants.Year = migrants.Year.astype(int)
    migrants.international_migrants_by_origin = (
        migrants.international_migrants_by_origin.astype(int)
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
    interp_df_lead = interp_df_lead[
        interp_df_lead[
            "annual_average_change_in_international_migrants_by_origin"
        ].apply(is_number)
    ]
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

    annual_change = annual_change[
        annual_change[
            "annual_average_change_in_international_migrants_by_origin_per_capita"
        ].apply(is_number)
    ]
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


def net_migration_rate() -> None:
    df = pd.read_excel(
        "https://population.un.org/wpp/Download/Files/1_Indicators%20(Standard)/EXCEL_FILES/4_Migration/WPP2019_MIGR_F01_NET_MIGRATION_RATE.xlsx",
        sheet_name="ESTIMATES",
        skiprows=16,
        usecols="B:U",
    )

    df["Region, subregion, country or area *"] = standardise_countries(
        df["Region, subregion, country or area *"]
    )
    df = df[
        [
            "Region, subregion, country or area *",
            "1950-1955",
            "1955-1960",
            "1960-1965",
            "1965-1970",
            "1970-1975",
            "1975-1980",
            "1980-1985",
            "1985-1990",
            "1990-1995",
            "1995-2000",
            "2000-2005",
            "2005-2010",
            "2010-2015",
            "2015-2020",
        ]
    ]

    df = pd.melt(
        df,
        id_vars=["Region, subregion, country or area *"],
        value_vars=[
            "1950-1955",
            "1955-1960",
            "1960-1965",
            "1965-1970",
            "1970-1975",
            "1975-1980",
            "1980-1985",
            "1985-1990",
            "1990-1995",
            "1995-2000",
            "2000-2005",
            "2005-2010",
            "2010-2015",
            "2015-2020",
        ],
    )
    df.rename(
        columns={
            "Region, subregion, country or area *": "Country",
            "variable": "Year",
            "value": "net_migration_rate_per_1000",
        },
        inplace=True,
    )
    df["Year"] = df["Year"].str[-4:].astype(int)
    df = df[df["net_migration_rate_per_1000"].apply(is_number)]
    df.to_csv(
        "migration/ready/undesa_net_migration_rate_per_1000.csv",
        index=False,
    )
    return df


def net_number_migrants() -> None:
    df = pd.read_excel(
        "https://population.un.org/wpp/Download/Files/1_Indicators%20(Standard)/EXCEL_FILES/4_Migration/WPP2019_MIGR_F02_NET_NUMBER_OF_MIGRANTS.xlsx",
        sheet_name="ESTIMATES",
        skiprows=16,
        usecols="B:U",
    )
    df["Region, subregion, country or area *"] = standardise_countries(
        df["Region, subregion, country or area *"]
    )
    df = pd.melt(
        df,
        id_vars=["Region, subregion, country or area *"],
        value_vars=[
            "1950-1955",
            "1955-1960",
            "1960-1965",
            "1965-1970",
            "1970-1975",
            "1975-1980",
            "1980-1985",
            "1985-1990",
            "1990-1995",
            "1995-2000",
            "2000-2005",
            "2005-2010",
            "2010-2015",
            "2015-2020",
        ],
    )
    df.rename(
        columns={
            "Region, subregion, country or area *": "Country",
            "variable": "Year",
            "value": "net_number_of_migrants",
        },
        inplace=True,
    )
    df["Year"] = df["Year"].str[-4:].astype(int)
    df = df[df["net_number_of_migrants"].apply(is_number)]
    df["net_number_of_migrants"] = (df["net_number_of_migrants"] * 1000).astype(int)
    df.to_csv(
        "migration/ready/undesa_net_number_of_migrants.csv",
        index=False,
    )
    return df


def child_migrants_by_destination() -> pd.DataFrame:
    df = pd.read_excel(
        "https://www.un.org/development/desa/pd/sites/www.un.org.development.desa.pd/files/undesa_pd_2020_ims_stock_by_age_sex_and_destination.xlsx",
        sheet_name="Table 1",
        skiprows=10,
        usecols="B:K",
    )
    df["Region, development group, country or area"] = standardise_countries(
        df["Region, development group, country or area"]
    )

    df_u15 = df[
        ["Year", "Region, development group, country or area", "0-4", "5-9", "10-14"]
    ]
    df_u20 = df[
        [
            "Year",
            "Region, development group, country or area",
            "0-4",
            "5-9",
            "10-14",
            " 15-19",
        ]
    ]

    df_u15["undesa_child_migrants_by_destination_under_15"] = df_u15[
        ["0-4", "5-9", "10-14"]
    ].sum(axis=1)
    df_u20["undesa_child_migrants_by_destination_under_20"] = df_u20[
        ["0-4", "5-9", "10-14", " 15-19"]
    ].sum(axis=1)

    df_u15 = df_u15[
        [
            "Year",
            "Region, development group, country or area",
            "undesa_child_migrants_by_destination_under_15",
        ]
    ]
    df_u20 = df_u20[
        [
            "Year",
            "Region, development group, country or area",
            "undesa_child_migrants_by_destination_under_20",
        ]
    ]

    df_u15.rename(
        columns={
            "Region, development group, country or area": "Country",
        },
        inplace=True,
    )

    df_u20.rename(
        columns={
            "Region, development group, country or area": "Country",
        },
        inplace=True,
    )
    df_u15 = df_u15[
        df_u15["undesa_child_migrants_by_destination_under_15"].apply(is_number)
    ]
    df_u20 = df_u20[
        df_u20["undesa_child_migrants_by_destination_under_20"].apply(is_number)
    ]

    df_u15.to_csv(
        "migration/ready/undesa_child_migrants_by_destination_under_15.csv", index=False
    )
    df_u20.to_csv(
        "migration/ready/undesa_child_migrants_by_destination_under_20.csv", index=False
    )
    return df_u15, df_u20


def child_migrants_by_destination_per_capita() -> None:
    df_u15, df_u20 = child_migrants_by_destination()
    population = owid_population()

    df_u15 = df_u15.merge(
        population,
        how="inner",
        left_on=["Country", "Year"],
        right_on=["Country", "Year"],
    )

    df_u20 = df_u20.merge(
        population,
        how="inner",
        left_on=["Country", "Year"],
        right_on=["Country", "Year"],
    )

    df_u15["undesa_child_migrants_by_destination_under_15_per_capita"] = (
        df_u15["undesa_child_migrants_by_destination_under_15"] / df_u15["Population"]
    )
    df_u20["undesa_child_migrants_by_destination_under_20_per_capita"] = (
        df_u20["undesa_child_migrants_by_destination_under_20"] / df_u20["Population"]
    )

    df_u15 = df_u15[
        df_u15["undesa_child_migrants_by_destination_under_15_per_capita"].apply(
            is_number
        )
    ]
    df_u20 = df_u20[
        df_u20["undesa_child_migrants_by_destination_under_20_per_capita"].apply(
            is_number
        )
    ]

    df_u15[
        ["Year", "Country", "undesa_child_migrants_by_destination_under_15_per_capita"]
    ].to_csv(
        "migration/ready/omm_undesa_child_migrants_by_destination_under_15_per_capita.csv",
        index=False,
    )
    df_u20[
        ["Year", "Country", "undesa_child_migrants_by_destination_under_20_per_capita"]
    ].to_csv(
        "migration/ready/omm_undesa_child_migrants_by_destination_under_20_per_capita.csv",
        index=False,
    )
    return df_u15, df_u20
