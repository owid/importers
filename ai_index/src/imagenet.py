import pandas as pd


def main():

    df = pd.read_excel(
        "input/ImageNet Top 1% - 2021 AI Index Report.xlsx",
        sheet_name="2.1.1",
        usecols=["Date", "Without extra training data", "With extra training data"],
    )

    df["Year"] = df.Date.dt.year
    df = (
        df.drop(columns="Date")
        .melt(id_vars="Year", var_name="Entity", value_name="imagenet_top1")
        .dropna()
        .groupby(["Year", "Entity"], as_index=False)
        .max()
    )

    df.to_csv("transformed/imagenet_top1.csv", index=False)


if __name__ == "__main__":
    main()
