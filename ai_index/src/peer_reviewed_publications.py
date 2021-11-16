import pandas as pd

COUNTRY_MAPPING = pd.read_csv("input/countries_standardized.csv")


def main():
    df = pd.read_excel(
        "input/Elsevier - 2021 AI Index Reprot.xlsx",
        sheet_name="Raw Data",
        usecols=[
            "Collaboration Level",
            "Year",
            "Country Code",
            "Country Name",
            "Number of AI Publications",
        ],
    )

    df.loc[df["Country Code"] == "WLD", "Country Name"] = "World"

    df = (
        df[(df["Collaboration Level"] == "ALL") & (df["Country Name"].notnull())]
        .drop(columns=["Collaboration Level", "Country Code"])
        .rename(
            columns={
                "Country Name": "Entity",
                "Number of AI Publications": "ai_publications_absolute",
            }
        )
    )

    # Standardize counties
    df = pd.merge(
        df,
        COUNTRY_MAPPING,
        left_on="Entity",
        right_on="Country",
        how="left",
        validate="many_to_one",
    )
    if df["Our World In Data Name"].isna().any():
        print(df[df["Our World In Data Name"].isna()]["Entity"].drop_duplicates())
        raise Exception("Missing country mappings!")
    df = df.drop(columns=["Entity", "Country"]).rename(
        columns={"Our World In Data Name": "Entity"}
    )

    world_total = (
        df[df.Entity == "World"]
        .rename(columns={"ai_publications_absolute": "world"})
        .drop(columns="Entity")
    )

    eu_countries = pd.read_csv("input/eu_countries.csv")
    eu = (
        df[df.Entity.isin(eu_countries.Entity)]
        .groupby("Year")
        .sum()
        .reset_index()
        .assign(Entity="European Union")
    )
    df = pd.concat([df, eu])

    df = df.merge(world_total, on="Year", how="outer", validate="many_to_one")
    df["ai_publications_share_world"] = (df.ai_publications_absolute / df.world).round(
        4
    )
    df.loc[df.Entity == "World", "ai_publications_share_world"] = pd.NA
    df = df.drop(columns="world")

    df.to_csv("transformed/peer_reviewed_publications.csv", index=False)


if __name__ == "__main__":
    main()
