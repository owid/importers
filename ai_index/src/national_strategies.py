import pandas as pd


def main():

    df = pd.read_csv(
        "input/ai-national-strategies_ai-index-ch7_country_standardized.csv"
    )

    df = (
        df.rename(columns={"Country": "Entity"})
        .pivot(index="Entity", columns="Year", values="Published")
        .reset_index()
        .melt(id_vars="Entity", var_name="Year", value_name="national_strategies")
        .sort_values(["Entity", "Year"])
    )

    df["national_strategies"] = (
        df[["national_strategies", "Entity"]].groupby("Entity").ffill()
    )
    df = df.dropna()
    df["national_strategies"] = df.national_strategies.astype(int).replace({3: 2})

    df.to_csv("transformed/national_strategies.csv", index=False)


if __name__ == "__main__":
    main()
