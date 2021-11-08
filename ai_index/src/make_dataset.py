from functools import reduce

import pandas as pd

PARTS = [
    "peer_reviewed_publications",
    "job_postings",
    "imagenet_top1",
    "corporate_investment",
    "education_programs",
    "ethics_principles",
    "gender_skill_ratio",
]

COUNTRY_MAPPING = pd.read_csv("input/countries_standardized.csv")


def import_part(name: str) -> pd.DataFrame:
    print(name)
    df = pd.read_csv(f"transformed/{name}.csv")

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

    return df


def main():

    # Merge all datasets
    dataframes = [import_part(part) for part in PARTS]
    df = reduce(
        lambda left, right: pd.merge(
            left, right, on=["Year", "Entity"], how="outer", validate="one_to_one"
        ),
        dataframes,
    )
    df["Year"] = df.Year.astype(int)

    # Reorder columns
    first_column = df.pop("Entity")
    df.insert(0, "Entity", first_column)

    df.to_csv("output/AI Index.csv", index=False)


if __name__ == "__main__":
    main()
