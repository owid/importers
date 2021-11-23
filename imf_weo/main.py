import pandas as pd

SOURCE_FILE = "WEOOct2021all.csv"


def read(source_url: str) -> pd.DataFrame:
    df = (
        pd.read_csv(f"input/{source_url}", low_memory=False)
        .drop(
            columns=[
                "WEO Country Code",
                "WEO Subject Code",
                "Estimates Start After",
                "Country",
                "Country/Series-specific Notes",
            ]
        )
        .dropna(subset=["ISO"])
    )
    df = df.loc[:, ~df.columns.str.contains("Unnamed")]
    return df


def pipe_make_variables(df: pd.DataFrame) -> pd.DataFrame:
    return df.assign(variable=df["Subject Descriptor"] + " - " + df["Units"]).drop(
        columns=["Subject Descriptor", "Units"]
    )


def pipe_choose_vars(df: pd.DataFrame) -> pd.DataFrame:
    return df[
        df.variable.isin(["Gross domestic product, constant prices - Percent change"])
    ]


def pipe_output_metadata(df: pd.DataFrame) -> pd.DataFrame:
    df[["variable", "Scale", "Subject Notes"]].drop_duplicates().to_csv(
        "output/metadata.csv", index=False
    )
    df[["ISO"]].drop_duplicates().rename(columns={"ISO": "Country"}).to_csv(
        "output/entities.csv", index=False
    )
    return df.drop(columns=["Subject Notes", "Scale"])


def pipe_reshape_A(df: pd.DataFrame) -> pd.DataFrame:
    return df.melt(id_vars=["ISO", "variable"], var_name="year")


def pipe_clean_values(df: pd.DataFrame) -> pd.DataFrame:
    return df.assign(value=pd.to_numeric(df.value, errors="coerce")).dropna(
        subset=["value"]
    )


def pipe_reshape_B(df: pd.DataFrame) -> pd.DataFrame:
    return df.pivot(
        index=["ISO", "year"], columns="variable", values="value"
    ).reset_index()


def pipe_translate_countries(df: pd.DataFrame) -> pd.DataFrame:
    mapping = pd.read_csv("input/entities_country_standardized.csv")
    return (
        pd.merge(
            mapping,
            df,
            how="inner",
            left_on="Country",
            right_on="ISO",
            validate="one_to_many",
        )
        .drop(columns=["Country", "ISO"])
        .rename(columns={"Our World In Data Name": "entity"})
    )


def pipeline(df: pd.DataFrame) -> pd.DataFrame:
    return (
        df.pipe(pipe_make_variables)
        .pipe(pipe_choose_vars)
        .pipe(pipe_output_metadata)
        .pipe(pipe_reshape_A)
        .pipe(pipe_clean_values)
        .pipe(pipe_reshape_B)
        .pipe(pipe_translate_countries)
    )


def main():
    read(SOURCE_FILE).pipe(pipeline).to_csv(
        "output/International Monetary Fund - World Economic Outlook.csv", index=False
    )


if __name__ == "__main__":
    main()
