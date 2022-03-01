import pandas as pd
from owid import catalog


def standardise_countries(country=pd.Series) -> pd.DataFrame:
    owid_countries = (
        pd.read_csv(
            "migration/countries_to_standardise_country_standardized.csv",
            usecols=["Country", "Our World In Data Name"],
        )
        .set_index("Country")
        .squeeze()
        .to_dict()
    )

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
    return population


def is_number(s):
    try:
        float(s)
        return True
    except (ValueError, TypeError):
        return False
