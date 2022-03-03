import pandas as pd


from migration.src.utils import standardise_countries


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

    df_wide = df_melt.pivot_table(
        index=["origin", "variable"], columns="destination", values="value"
    ).reset_index()

    res = df_wide.apply(lambda x: x.fillna(""))
    res = res.rename(columns={"origin": "Entity", "variable": "Year"})
    # cols = res.columns.drop(["origin", "variable"]).to_list()
    # df_wide = res[cols].apply(np.int64)
    res.to_csv("migration/output/Migration_matrix.csv", index=False)
