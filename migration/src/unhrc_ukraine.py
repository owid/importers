import requests, zipfile, io
import pandas as pd
import json
from migration.src.utils import standardise_countries, owid_population


def get_ukraine_refugees() -> pd.pd.DataFrame:
    res = requests.get(
        "https://data2.unhcr.org/population/get/sublocation?geo_id=0&forcesublocation=1&widget_id=283573&sv_id=54&color=%233c8dbc&color2=%23303030&population_group=5460"
    )
    res = json.loads(res.content)
    df = pd.json_normalize(res["data"])
    df["Country"] = standardise_countries(df.geomaster_name)
    df_out = df[["Country", "date", "individuals"]]
