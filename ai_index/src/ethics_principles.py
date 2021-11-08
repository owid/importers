import pandas as pd

COUNTRY_MAPPING = pd.read_csv("input/countries_standardized.csv")


def main():

    df = pd.read_excel(
        "input/AI Ethics Lab - 2021 AI Index Report.xlsx",
        sheet_name="Raw Data",
        usecols=["Type of Document", "Year"],
    ).rename(columns={"Type of Document": "type"})

    df = (
        df.groupby(["type", "Year"], as_index=False)
        .size()
        .pivot(index=["Year"], columns="type", values="size")
        .fillna(0)
        .rename(
            columns={
                "Research/Professional Organization": "research_ethics_principles",
                "Private Company": "private_ethics_principles",
                "Intergovernmental Organization/Agency": "intergov_ethics_principles",
                "Government Agency": "gov_ethics_principles",
            }
        )
        .reset_index()
        .sort_values("Year")
    )

    df[
        [
            "gov_ethics_principles",
            "intergov_ethics_principles",
            "private_ethics_principles",
            "research_ethics_principles",
        ]
    ] = df[
        [
            "gov_ethics_principles",
            "intergov_ethics_principles",
            "private_ethics_principles",
            "research_ethics_principles",
        ]
    ].cumsum()

    df = df.assign(Year=df.Year.astype(int), Entity="World")

    df.to_csv("transformed/ethics_principles.csv", index=False)


if __name__ == "__main__":
    main()
