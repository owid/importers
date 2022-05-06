import pandas as pd

INPUT_FILE = "input/NMC_Documentation 6.0/NMC-60-abridged/NMC-60-abridged.csv"
USD_CPI = "input/USCPI_1914-2022.csv"
COUNTRY_MAPPING = "config/country_standardized.csv"


def main():

    df = pd.read_csv(
        INPUT_FILE,
        usecols=["stateabb", "year", "milex", "milper", "tpop"],
        na_values=-9,
    )

    df = df.rename(
        columns={
            "milex": "military_expenditure",
            "milper": "military_personnel",
            "tpop": "population",
        }
    )

    # Expenditure and personnel are expressed in thousands
    df[["military_personnel", "population", "military_expenditure"]] = (
        df[["military_personnel", "population", "military_expenditure"]] * 1000
    )

    # We only keep expenditures for 1914+, in current year US Dollars
    df["military_expenditure_post1914"] = df.loc[
        df.year >= 1914, "military_expenditure"
    ]
    # Adjust for inflation
    cpi = pd.read_csv(USD_CPI, comment="#")
    cpi_2020 = cpi.loc[cpi.year == 2020, "cpi"].values[0]
    df = df.merge(cpi, on="year", validate="many_to_one", how="left")
    df["military_expenditure_post1914"] = (
        df.military_expenditure_post1914.mul(cpi_2020).div(df.cpi).round(0)
    )

    df["military_personnel_share"] = 100 * df.military_personnel / df.population

    # Country mapping
    mapping = pd.read_csv(COUNTRY_MAPPING)
    df = pd.merge(mapping, df, on="stateabb", validate="one_to_many").rename(
        columns={"owid_name": "country"}
    )

    df = df.drop(
        columns=["military_expenditure", "population", "stateabb", "cow_name", "cpi"]
    )

    df.to_csv(
        "output/Correlates of War (CoW) - National Material Capabilities.csv",
        index=False,
    )


if __name__ == "__main__":
    main()
