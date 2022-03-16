import pandas as pd
import os.path
import numpy as np
from migration.src.utils import standardise_countries


def add_selected_country_value(
    df: pd.DataFrame, total_origin: pd.DataFrame, total_dest: pd.DataFrame
) -> pd.DataFrame:

    countries = df["Entity"].drop_duplicates()
    for country in countries:
        print(country)
        df.loc[df["Entity"] == country, country + "_origin"] = total_origin.loc[
            total_origin["Entity"] == country, "total_origin"
        ].to_list()
        df.loc[df["Entity"] == country, country + "_origin"] = (
            df.loc[df["Entity"] == country, country + "_origin"] * -1
        )

        df.loc[df["Entity"] == country, country + "_destination"] = total_dest.loc[
            total_dest["Entity"] == country, "total_destination"
        ].to_list()
        df.loc[df["Entity"] == country, country + "_destination"] = (
            df.loc[df["Entity"] == country, country + "_destination"]
        ) * -1

    return df


def migration_matrix():
    if os.path.exists(
        "migration/input/undesa_pd_2020_ims_stock_by_sex_destination_and_origin.csv"
    ):
        df = pd.read_csv(
            "migration/input/undesa_pd_2020_ims_stock_by_sex_destination_and_origin.csv"
        )
    else:
        df = pd.read_excel(
            "https://www.un.org/development/desa/pd/sites/www.un.org.development.desa.pd/files/undesa_pd_2020_ims_stock_by_sex_destination_and_origin.xlsx",
            sheet_name="Table 1",
            skiprows=10,
            usecols="B:N",
        )
        df.to_csv(
            "migration/input/undesa_pd_2020_ims_stock_by_sex_destination_and_origin.csv"
        )
    df[
        "Region, development group, country or area of destination"
    ] = standardise_countries(
        df["Region, development group, country or area of destination"]
    )

    df["Region, development group, country or area of origin"] = standardise_countries(
        df["Region, development group, country or area of origin"]
    )
    orig_total, dest_total = get_total_origin_destination(df)
    df = remove_regions(df)

    df["destination_origin"] = (
        df["Region, development group, country or area of destination"]
        + " _ "
        + df["Region, development group, country or area of origin"]
    )

    df.drop(
        [
            "Notes of destination",
            "Location code of destination",
            "Type of data of destination",
            "Location code of origin",
            "Region, development group, country or area of destination",
            "Region, development group, country or area of origin",
        ],
        axis=1,
        inplace=True,
    )

    df_melt = pd.melt(
        df,
        id_vars=["destination_origin"],
        value_vars=["1990", "1995", "2000", "2005", "2010", "2015", "2020"],
    )

    split = df_melt["destination_origin"].str.split(" _ ", n=1, expand=True)
    df_melt["destination"] = split[0]
    df_melt["origin"] = split[1]
    df_melt = df_melt.drop("destination_origin", axis=1)

    # add _destination to column names in df_wide_origin
    df_wide_origin = df_melt.pivot_table(
        index=["origin", "variable"], columns="destination", values="value"
    ).reset_index()
    cols = df_wide_origin.columns.drop(["origin", "variable"])
    new_cols = cols + "_destination"
    df_wide_origin.rename(columns=dict(zip(cols, new_cols)), inplace=True)
    df_wide_origin = df_wide_origin.rename(
        columns={"origin": "Entity", "variable": "Year"}
    )

    # add _origin to column names in df_wide_destination
    df_wide_destination = df_melt.pivot_table(
        index=["destination", "variable"], columns="origin", values="value"
    ).reset_index()
    cols = df_wide_destination.columns.drop(["destination", "variable"])
    new_cols = cols + "_origin"
    df_wide_destination.rename(columns=dict(zip(cols, new_cols)), inplace=True)
    df_wide_destination = df_wide_destination.rename(
        columns={"destination": "Entity", "variable": "Year"}
    )

    # df_wide_destination.loc[
    #    :, ~df_wide_destination.columns.isin(["Entity", "Year"])
    # ] = df_wide_destination.loc[
    #    :, ~df_wide_destination.columns.isin(["Entity", "Year"])
    # ].mul(
    #    -1, fill_value=np.nan
    # )

    df_both = pd.merge(
        df_wide_origin, df_wide_destination, on=["Entity", "Year"], how="outer"
    )

    df_both = add_selected_country_value(
        df=df_both, total_origin=orig_total, total_dest=dest_total
    )

    dest_cols = [col for col in df_both.columns if "_destination" in col]
    df_both[dest_cols] = df_both[dest_cols].mul(-1, fill_value=np.NaN)

    res = df_both.apply(lambda x: x.fillna(""))
    res.columns = res.columns.str.replace(" ", "").str.lower()

    res.to_csv("migration/output/Migration_matrix_new.csv", index=False)


def get_total_origin_destination(df: pd.DataFrame):
    orig_df = df[
        [
            "Region, development group, country or area of destination",
            "Region, development group, country or area of origin",
            "1990",
            "1995",
            "2000",
            "2005",
            "2010",
            "2015",
            "2020",
        ]
    ]
    orig_df_tot = orig_df[
        orig_df["Region, development group, country or area of destination"] == "World"
    ]
    orig_total = orig_df_tot.melt(
        id_vars=["Region, development group, country or area of origin"],
        value_vars=["1990", "1995", "2000", "2005", "2010", "2015", "2020"],
    )
    orig_total.rename(
        columns={
            "Region, development group, country or area of origin": "Entity",
            "variable": "Year",
            "value": "total_origin",
        },
        inplace=True,
    )

    dest_df = df[
        [
            "Region, development group, country or area of destination",
            "Region, development group, country or area of origin",
            "1990",
            "1995",
            "2000",
            "2005",
            "2010",
            "2015",
            "2020",
        ]
    ]
    dest_df_tot = dest_df[
        dest_df["Region, development group, country or area of origin"] == "World"
    ]
    dest_total = dest_df_tot.melt(
        id_vars=["Region, development group, country or area of destination"],
        value_vars=["1990", "1995", "2000", "2005", "2010", "2015", "2020"],
    )
    dest_total.rename(
        columns={
            "Region, development group, country or area of destination": "Entity",
            "variable": "Year",
            "value": "total_destination",
        },
        inplace=True,
    )

    return orig_total, dest_total


def remove_regions(df: pd.DataFrame) -> pd.DataFrame:

    regions = [
        "World",
        "Africa",
        "Asia",
        "Australia and New Zealand",
        "Central America",
        "Central Asia",
        "Central and Southern Asia",
        "Developed regions",
        "Eastern Africa",
        "Eastern Asia",
        "Eastern Europe",
        "Eastern and South-Eastern Asia",
        "Europe",
        "Europe and Northern America",
        "High-income countries",
        "Land-locked Developing Countries (LLDC)",
        "Latin America and the Caribbean",
        "Least developed countries",
        "Less developed regions",
        "Less developed regions, excluding China",
        "Less developed regions, excluding least developed countries",
        "Low-income countries",
        "Lower-middle-income countries",
        "Melanesia",
        "Micronesia (region)",
        "Middle Africa",
        "Middle-income countries",
        "Northern Africa",
        "Northern Africa and Western Asia",
        "Northern America",
        "Northern Europe",
        "Oceania",
        "Oceania (excluding Australia and New Zealand)",
        "Other",
        "Small island developing States (SIDS)",
        "South America",
        "South-Eastern Asia",
        "Southern Africa",
        "Southern Asia",
        "Southern Europe",
        "Sub-Saharan Africa",
        "Upper-middle-income countries",
        "Western Africa",
        "Western Asia",
        "Western Europe",
    ]
    df = df[
        ~df["Region, development group, country or area of destination"].isin(regions)
    ]
    df = df[~df["Region, development group, country or area of origin"].isin(regions)]

    return df
