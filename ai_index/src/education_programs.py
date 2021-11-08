import datetime

import pandas as pd

COUNTRY_MAPPING = pd.read_csv("input/countries_standardized.csv")


def main():

    df = pd.read_excel(
        "input/Academic Offer tables_text_FINAL.xlsx",
        sheet_name="tableA1",
        skiprows=3,
    )

    df = (
        df.rename(columns={"Unnamed: 1": "Entity"})
        .assign(
            education_programs_bachelor=df["Broad"] + df["Specialised"],
            education_programs_master=df["Broad.1"] + df["Specialised.1"],
            education_programs_short=df["Broad.2"] + df["Specialised.2"],
        )
        .dropna(subset=["Unnamed: 0"])[
            [
                "Entity",
                "education_programs_bachelor",
                "education_programs_master",
                "education_programs_short",
            ]
        ]
        .assign(Year=datetime.date.today().year)
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

    eu_countries = pd.read_csv("input/eu_countries.csv")
    eu = (
        df[df.Entity.isin(eu_countries.Entity)]
        .groupby("Year")
        .sum()
        .reset_index()
        .assign(Entity="European Union")
    )
    df = pd.concat([df, eu])

    df.to_csv("transformed/education_programs.csv", index=False)


if __name__ == "__main__":
    main()
