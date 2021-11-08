import pandas as pd

COUNTRY_MAPPING = pd.read_csv("input/countries_standardized.csv")


def main():

    df = pd.read_excel(
        "input/NetBase Quid - 2021 AI Index Report.xlsx",
        sheet_name="Funding Event",
        usecols=[
            "Year of Funding Event",
            "Funding in USD",
            "Target Location (Country)",
            "Event Type",
        ],
    )

    df = (
        df.dropna()
        .rename(
            columns={
                "Year of Funding Event": "Year",
                "Funding in USD": "dollars",
                "Target Location (Country)": "Entity",
                "Event Type": "investment_type",
            }
        )
        .groupby(["Year", "Entity", "investment_type"], as_index=False)
        .sum()
        .pivot_table(
            index=["Year", "Entity"], columns="investment_type", values="dollars"
        )
        .reset_index()
        .rename(
            columns={
                "Merger/Acquisition": "funding_ma",
                "Private Investment": "funding_private",
                "Public Offering": "funding_public",
            }
        )
        .fillna(0)
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

    world = df.groupby("Year").sum().reset_index().assign(Entity="World")
    df = pd.concat([df, world])

    eu_countries = pd.read_csv("input/eu_countries.csv")
    eu = (
        df[df.Entity.isin(eu_countries.Entity)]
        .groupby("Year")
        .sum()
        .reset_index()
        .assign(Entity="European Union")
    )
    df = pd.concat([df, eu])

    df["Year"] = df.Year.astype(int)

    df.to_csv("transformed/corporate_investment.csv", index=False)


if __name__ == "__main__":
    main()
