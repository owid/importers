import datetime
import pandas as pd

MAPPING_PATH = "country_mapping.csv"

# https://population.un.org/wpp/Download/Standard/CSV/
UN_PATH = "WPP2019_TotalPopulationBySex.csv"

# https://www.gapminder.org/data/documentation/gd003/
GAPMINDER_PATH = "_GM-Population - Dataset - v6.xlsx"

# https://owid.cloud/admin/datasets/70
PRE1800_PATH = "pre1800_global_pop.csv"

# https://ourworldindata.org/grapher/continents-according-to-our-world-in-data
CONTINENTS_PATH = "continents-according-to-our-world-in-data.csv"


def load_pre1800(path: str) -> pd.DataFrame:
    pre1800 = pd.read_csv(f"input/{path}")
    pre1800["Source"] = "HYDE"
    return pre1800


def load_un(path: str) -> pd.DataFrame:
    un = pd.read_csv(f"input/{path}")
    un = un[un.Variant == "Medium"]
    un = un[["Location", "Time", "PopTotal"]]
    un = un.rename(
        columns={"Location": "Entity", "Time": "Year", "PopTotal": "Population"}
    )
    un["Population"] = un.Population.mul(1000).astype(int)
    un["Source"] = "UN"
    return un


def load_gapminder(path: str) -> pd.DataFrame:
    countries = pd.read_excel(
        f"input/{path}", sheet_name="data-for-countries-etc-by-year"
    )
    world = pd.read_excel(f"input/{path}", sheet_name="data-for-world-by-year")
    gap = pd.concat([countries, world], ignore_index=True)
    gap = gap[["name", "time", "Population"]]
    gap = gap.rename(columns={"name": "Entity", "time": "Year"})
    gap[["Year", "Population"]] = gap[["Year", "Population"]].astype(int)
    gap["Source"] = "Gapminder"
    return gap


def rename_entities(df: pd.DataFrame) -> pd.DataFrame:
    mapping = pd.read_csv(f"input/{MAPPING_PATH}").drop_duplicates()
    df = df.merge(mapping, left_on="Entity", right_on="Country", how="outer")

    missing = df[pd.isnull(df["Our World In Data Name"])]
    if len(missing) > 0:
        raise Exception(f"Missing entities in mapping: {missing.Entity.unique()}")

    df = df.drop(columns=["Entity", "Country"]).rename(
        columns={"Our World In Data Name": "Entity"}
    )

    df = df[-(df.Entity == "DROPENTITY")]
    return df


def select_source(df: pd.DataFrame) -> pd.DataFrame:
    return (
        df.sort_values("Source", ascending=False)
        .groupby(["Entity", "Year"])
        .head(1)
        .drop(columns=["Source"])
    )


def aggregate_continents(df: pd.DataFrame) -> pd.DataFrame:
    df["Source"] = "original"
    continents = pd.read_csv(
        f"input/{CONTINENTS_PATH}", usecols=["Entity", "Continent"]
    )
    aggregated = (
        df.merge(continents, on="Entity")
        .groupby(["Continent", "Year"], as_index=False)
        .sum()
        .rename(columns={"Continent": "Entity"})
    )
    aggregated[["Year", "Population"]] = aggregated[["Year", "Population"]].astype(
        "Int64"
    )
    aggregated["Source"] = "aggregated"

    df = pd.concat([df, aggregated], ignore_index=True)

    return (
        df.sort_values("Source", ascending=False)
        .groupby(["Entity", "Year"])
        .head(1)
        .drop(columns=["Source"])
    )


def prepare_dataset(df: pd.DataFrame) -> pd.DataFrame:
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
    pre1800: pd.DataFrame, un: pd.DataFrame, gap: pd.DataFrame
) -> pd.DataFrame:
    return (
        pd.concat([pre1800, un, gap], ignore_index=True)
        .pipe(rename_entities)
        .pipe(select_source)
        .pipe(aggregate_continents)
        .pipe(prepare_dataset)
    )


def main():
    pre1800 = load_pre1800(PRE1800_PATH)
    un = load_un(UN_PATH)
    gap = load_gapminder(GAPMINDER_PATH)
    df = population_pipeline(pre1800, un, gap)
    df.to_csv("output/Population (Gapminder, HYDE & UN).csv", index=False)


if __name__ == "__main__":
    main()
