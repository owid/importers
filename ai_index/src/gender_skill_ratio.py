import pandas as pd


def main():

    df = pd.read_excel(
        "input/LinkedIn - 2021 AI Index Report (Diversity in AI).xlsx",
        sheet_name="6.1.1",
    ).rename(columns={"Country": "Entity"})

    df = df.assign(gender_skill_ratio=df.Female / df.Male, Year=2020).drop(
        columns=["Female", "Male"]
    )

    df.to_csv("transformed/gender_skill_ratio.csv", index=False)


if __name__ == "__main__":
    main()
