import pandas as pd
import numpy as np
from owid import catalog


def standardise_countries(country=pd.Series) -> pd.DataFrame:
    owid_countries = pd.read_csv(
        "migration/countries_to_standardise_country_standardized.csv",
        usecols=["Country", "Our World In Data Name"],
    )
    owid_countries["Country"] = owid_countries["Country"].apply(lambda x: x.strip())
    country = country.apply(lambda x: x.strip())
    owid_countries = owid_countries.set_index("Country").squeeze().to_dict()
    countries_standardised = country.apply(lambda x: owid_countries[x])
    return countries_standardised


def owid_population() -> pd.DataFrame:
    population = (
        catalog.find("population", namespace="owid")
        .load()
        .reset_index()
        .rename(
            columns={"country": "Country", "year": "Year", "population": "Population"}
        )[["Country", "Year", "Population"]]
    )
    countries = ["Mayotte", "Turks and Caicos"]
    population.drop(population[population.Country.isin(countries)].index, inplace=True)
    return population


def is_number(s):
    try:
        float(s)
        return True
    except (ValueError, TypeError):
        return False


def five_year_moving_window(df: pd.DataFrame) -> pd.DataFrame:
    var_name = df.columns[2]
    countries = df.Country.drop_duplicates()
    years = df.Year.drop_duplicates()
    country_year_combos = pd.DataFrame(
        [(x, y) for x in countries for y in years], columns=["Country", "Year"]
    )
    country_year_combos[var_name] = np.NaN
    dfm = df.merge(country_year_combos, on=["Country", "Year"], how="outer")
    dfm[var_name] = dfm.groupby("Country")[var_name + "_x"].transform(
        lambda x: x.rolling(5, center=True).mean()
    )
    dfm = dfm[["Year", "Country", var_name]].sort_values(by=["Country", "Year"])
    dfm = dfm[dfm[var_name].notna()]
    return dfm
