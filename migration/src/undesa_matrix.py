import pandas as pd


from migration.src.utils import standardise_countries


def add_selected_country_value(df: pd.DataFrame) -> pd.DataFrame:
    if "origin" in df.columns:
        countries = df["origin"].drop_duplicates()
        for country in countries:
            df.loc[df["origin"] == country, country] = "Selected country"
    if "destination" in df.columns:
        countries = df["destination"].drop_duplicates()
        for country in countries:
            df.loc[df["destination"] == country, country] = "Selected country"
    return df


def migration_matrix_by_destination():
    df = pd.read_excel(
        "https://www.un.org/development/desa/pd/sites/www.un.org.development.desa.pd/files/undesa_pd_2020_ims_stock_by_sex_destination_and_origin.xlsx",
        sheet_name="Table 1",
        skiprows=10,
        usecols="B:N",
    )
    df[
        "Region, development group, country or area of destination"
    ] = standardise_countries(
        df["Region, development group, country or area of destination"]
    )
    df["Region, development group, country or area of origin"] = standardise_countries(
        df["Region, development group, country or area of origin"]
    )
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
        value_vars=[1990, 1995, 2000, 2005, 2010, 2015, 2020],
    )

    split = df_melt["destination_origin"].str.split(" _ ", n=1, expand=True)
    df_melt["destination"] = split[0]
    df_melt["origin"] = split[1]
    df_melt = df_melt.drop("destination_origin", axis=1)

    # add _destination to column names in df_wide_origin
    df_wide_origin = df_melt.pivot_table(
        index=["origin", "variable"], columns="destination", values="value"
    ).reset_index()

    df_wide_origin = add_selected_country_value(df=df_wide_origin)

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

    df_wide_destination = add_selected_country_value(df=df_wide_destination)

    cols = df_wide_destination.columns.drop(["destination", "variable"])
    new_cols = cols + "_origin"
    df_wide_destination.rename(columns=dict(zip(cols, new_cols)), inplace=True)
    df_wide_destination = df_wide_destination.rename(
        columns={"destination": "Entity", "variable": "Year"}
    )

    df_both = pd.merge(
        df_wide_origin, df_wide_destination, on=["Entity", "Year"], how="outer"
    )

    res = df_both.apply(lambda x: x.fillna(""))

    res.to_csv("migration/output/Migration_matrix.csv", index=False)
