import requests
import pandas as pd
import io

from migration.src.utils import standardise_countries, owid_population, is_number


def under_eighteen_migrants_by_destination() -> pd.DataFrame:
    url = "https://sdmx.data.unicef.org/ws/public/sdmxapi/rest/data/UNICEF,MG,1.0/.MG_INTNL_MG_CNTRY_DEST.Y0T17.?format=csv&labels=both"
    r = requests.get(url)
    assert r.ok
    r = r.content
    df = pd.read_csv(io.StringIO(r.decode("utf-8")))
    df = df[["Geographic area", "TIME_PERIOD", "OBS_VALUE"]]
    df["Geographic area"] = standardise_countries(df["Geographic area"])
    df.replace("<1", 0, inplace=True)
    df["OBS_VALUE"] = df["OBS_VALUE"].astype(int) * 1000
    df.rename(
        columns={
            "Geographic area": "Country",
            "TIME_PERIOD": "Year",
            "OBS_VALUE": "unicef_under_eighteen_migrants_by_destination",
        },
        inplace=True,
    )
    df.to_csv(
        "migration/ready/unicef_under_eighteen_migrants_by_destination.csv", index=False
    )
    return df


def under_eighteen_migrants_by_destination_per_1000() -> pd.DataFrame:
    migrants = under_eighteen_migrants_by_destination()
    population = owid_population()
    migrants = migrants.merge(
        population,
        how="inner",
        left_on=["Country", "Year"],
        right_on=["Country", "Year"],
    )
    migrants["unicef_under_eighteen_migrants_by_destination_per_1000"] = (
        migrants["unicef_under_eighteen_migrants_by_destination"]
        / migrants["Population"]
    ) * 1000
    migrants = migrants[
        migrants["unicef_under_eighteen_migrants_by_destination"].apply(is_number)
    ]
    migrants = migrants[
        ["Year", "Country", "unicef_under_eighteen_migrants_by_destination_per_1000"]
    ]
    migrants.to_csv(
        "migration/ready/omm_unicef_under_eighteen_migrants_by_destination_per_1000.csv",
        index=False,
    )
    return migrants
