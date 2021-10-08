import datetime
import pandas as pd

COUNTRY_MAPPING = "input/country_mapping.csv"

# https://dataportaal.pbl.nl/downloads/HYDE/
# select version -> download baseline.zip -> extract -> popc_c.txt
# select version -> download general_files.zip -> extract -> HYDE_country_codes.xlsx
HYDE_DATA = "input/popc_c.txt"
HYDE_CODES = "input/HYDE_country_codes.xlsx"

# http://gapm.io/dl_popv6
GAPMINDER_DATA = "input/GM-Population - Dataset - v6.xlsx"

# https://population.un.org/wpp/Download/Standard/CSV/
UNWPP_DATA = "input/WPP2019_TotalPopulationBySex.csv"

# https://ourworldindata.org/grapher/continents-according-to-our-world-in-data
CONTINENT_LIST = "input/continents-according-to-our-world-in-data.csv"


def load_hyde(country_path: str, code_path: str) -> pd.DataFrame:
    codes = pd.read_excel(code_path, sheet_name="country", usecols="A:B").rename(
        columns={"ISO-CODE": "region", "Country": "Entity"}
    )
    codes["Entity"] = codes["Entity"].str.strip()
    codes = codes.drop_duplicates(subset="region", keep="first")

    countries = pd.read_csv(country_path, sep=" ").melt(
        id_vars="region", var_name="Year", value_name="Population"
    )
    countries = countries[-countries.region.isin(["Total"])]
    countries["region"] = countries.region.astype(int)

    hyde = (
        pd.merge(codes, countries, on="region", how="inner", validate="one_to_many")
        .drop(columns="region")
        .assign(source="hyde")
    )
    hyde[["Population", "Year"]] = hyde[["Population", "Year"]].astype(int)
    return hyde


def load_gapminder(path: str) -> pd.DataFrame:
    return (
        pd.read_excel(
            path,
            sheet_name="data-for-countries-etc-by-year",
            usecols=["name", "time", "Population"],
        )
        .rename(columns={"name": "Entity", "time": "Year"})
        .assign(source="gapminder")
    )


def load_unwpp(path: str) -> pd.DataFrame:
    df = pd.read_csv(path, usecols=["Location", "Variant", "Time", "PopTotal"])
    df = (
        df[df.Variant == "Medium"]
        .drop(columns="Variant")
        .rename(
            columns={"Location": "Entity", "Time": "Year", "PopTotal": "Population"}
        )
        .assign(source="unwpp")
    )
    df["Population"] = df.Population.mul(1000).astype(int)
    return df


def rename_entities(df: pd.DataFrame) -> pd.DataFrame:
    mapping = pd.read_csv(COUNTRY_MAPPING).drop_duplicates()
    df = df.merge(mapping, left_on="Entity", right_on="Country", how="left")

    missing = df[pd.isnull(df["Our World In Data Name"])]
    if len(missing) > 0:
        missing = "\n".join(missing.Entity.unique())
        raise Exception(f"Missing entities in mapping:\n{missing}")

    df = df.drop(columns=["Entity", "Country"]).rename(
        columns={"Our World In Data Name": "Entity"}
    )

    df = df[-(df.Entity == "DROPENTITY")]
    return df


def select_source(df: pd.DataFrame) -> pd.DataFrame:

    df = df[df.Population > 0]

    # If a country has UN data, then remove all non-UN data after 1949
    has_un_data = set(df.loc[df.source == "unwpp", "Entity"])
    df = df[
        -((df.Entity.isin(has_un_data)) & (df.Year >= 1950) & (df.source != "unwpp"))
    ]

    # If a country has Gapminder data, then remove all non-Gapminder data between 1800 and 1949
    has_gapminder_data = set(df.loc[df.source == "gapminder", "Entity"])
    df = df[
        -(
            (df.Entity.isin(has_gapminder_data))
            & (df.Year >= 1800)
            & (df.Year <= 1949)
            & (df.source != "gapminder")
        )
    ]

    # Test if all countries have only one row per year
    assert df.shape == df.drop_duplicates(subset=["Entity", "Year"]).shape

    return df.drop(columns=["source"])


def calculate_aggregates(df: pd.DataFrame) -> pd.DataFrame:
    """
    Aggregate our own totals according to OWID continent definitions.
    """
    df = df[
        -df.Entity.isin(
            [
                "North America",
                "South America",
                "Europe",
                "Africa",
                "Asia",
                "Oceania",
                "World",
            ]
        )
    ]

    continent_list = pd.read_csv(CONTINENT_LIST, usecols=["Entity", "Continent"])
    continents = (
        df.merge(continent_list, on="Entity")
        .groupby(["Continent", "Year"], as_index=False)
        .sum()
        .rename(columns={"Continent": "Entity"})
    )

    world = (
        df[["Year", "Population"]]
        .groupby("Year")
        .sum()
        .reset_index()
        .assign(Entity="World")
    )

    return pd.concat([df, continents, world], ignore_index=True)


def prepare_dataset(df: pd.DataFrame) -> pd.DataFrame:
    df = df[df.Population > 0].copy()
    df.loc[
        df.Year <= datetime.date.today().year, "Total population (Gapminder, HYDE & UN)"
    ] = df.Population
    df[["Total population (Gapminder, HYDE & UN)", "Population", "Year"]] = df[
        ["Total population (Gapminder, HYDE & UN)", "Population", "Year"]
    ].astype("Int64")
    df = df.rename(
        columns={
            "Population": "Population by country and region, historic and projections (Gapminder, HYDE & UN)"
        }
    )
    df = df[
        [
            "Entity",
            "Year",
            "Total population (Gapminder, HYDE & UN)",
            "Population by country and region, historic and projections (Gapminder, HYDE & UN)",
        ]
    ]

    # Add a metric "% of world population"
    world_pop = df[df.Entity == "World"][
        ["Year", "Total population (Gapminder, HYDE & UN)"]
    ].rename(columns={"Total population (Gapminder, HYDE & UN)": "world_population"})
    df = df.merge(world_pop, on="Year", how="left")
    df["Share of world population"] = (
        df["Total population (Gapminder, HYDE & UN)"].div(df.world_population)
    ).round(4)

    df = df.drop(columns="world_population").sort_values(["Entity", "Year"])
    return df


def population_pipeline(
    hyde: pd.DataFrame, gapminder: pd.DataFrame, unwpp: pd.DataFrame
) -> pd.DataFrame:
    return (
        pd.concat([hyde, gapminder, unwpp], ignore_index=True)
        .pipe(rename_entities)
        .pipe(select_source)
        .pipe(calculate_aggregates)
        .pipe(prepare_dataset)
    )


def main():
    unwpp = load_unwpp(UNWPP_DATA)
    hyde = load_hyde(HYDE_DATA, HYDE_CODES)
    gapminder = load_gapminder(GAPMINDER_DATA)
    population_pipeline(hyde, gapminder, unwpp).to_csv(
        "output/Population (Gapminder, HYDE & UN).csv", index=False
    )


if __name__ == "__main__":
    main()
