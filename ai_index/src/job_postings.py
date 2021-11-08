import pandas as pd


def main():
    df = (
        pd.read_excel(
            "input/Burning Glass - 2021 AI Index Report.xlsx",
            sheet_name="3.1.3",
        )
        .melt(id_vars="Year", value_name="job_postings_share", var_name="Entity")
        .dropna()
    )
    df["job_postings_share"] = df.job_postings_share.round(5)
    df.to_csv("transformed/job_postings.csv", index=False)


if __name__ == "__main__":
    main()
