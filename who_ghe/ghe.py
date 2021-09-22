import requests
import json
import pandas as pd
from rich import inspect

base_url = "https://ghoapi.azureedge.net/api"

requests.get("https://ghoapi.azureedge.net/api/Indicator")


url = f"{base_url}/Indicator"
res = requests.get(url)
assert res.ok

indicators = json.loads(res.content)
indicators = indicators["value"]
GHE_codes = [x for x in indicators if x["IndicatorCode"].startswith("GHE_")]
GHE_codes = pd.DataFrame(GHE_codes)["IndicatorCode"].tolist()

data = []
for code in GHE_codes:
    url = base_url + "/" + code
    res = requests.get(url)
    data += json.loads(res.content)["value"]


data_df = pd.DataFrame(data)

data_df["TimeDimensionValue"].max()


url = "https://frontdoor-l4uikgap6gz3m.azurefd.net/DEX_CMS/GHE_FULL_Test"

url = "https://frontdoor-l4uikgap6gz3m.azurefd.net/DEX_CMS/GHE_FULL_Test?&$select=DIM_COUNTRY_CODE,DIM_GHECAUSE_CODE,DIM_GHECAUSE_TITLE,DIM_YEAR_CODE,DIM_SEX_CODE,DIM_AGEGROUP_CODE,VAL_DALY_COUNT_NUMERIC"

url = "https://frontdoor-l4uikgap6gz3m.azurefd.net/DEX_CMS/GHE_FULL_Test?&$select=DIM_COUNTRY_CODE,DIM_GHECAUSE_CODE,DIM_GHECAUSE_TITLE,DIM_YEAR_CODE,DIM_SEX_CODE,DIM_AGEGROUP_CODE,VAL_DALY_COUNT_NUMERIC&$filter=DIM_YEAR_CODE%20eq%20%272000%27"
res = requests.get(url)
assert res.ok
ghe_full = json.loads(res.content)["value"]
ghe_df = pd.DataFrame(ghe_full)
ghe_df["DIM_YEAR_CODE"].max()


url = "https://frontdoor-l4uikgap6gz3m.azurefd.net/DEX_CMS/GHE_COD_COMPLETE?&$select=COUNTRY_CODE,GHE_CAUSE_CODE,YEAR"
res = requests.get(url)
assert res.ok
ghe_cause = json.loads(res.content)["value"]
ghe_df = pd.DataFrame(ghe_cause)
ghe_df["YEAR"].min()


url = "https://frontdoor-l4uikgap6gz3m.azurefd.net/DEX_CMS/GHE_COD_COMPLETE?&$orderby=DEATHS_100K desc&$select=COUNTRY_CODE,GHE_CAUSE_CODE,GHE_CAUSE_TITLE,YEAR,SEX_CODE,AGEGROUP_CODE,DALY,DEATHS,POPULATION,DALY_RATE,DEATHS_RATE,DALY_100K,DEATHS_100K&$filter=COUNTRY_CODE eq 'AFG'"
res = requests.get(url)
assert res.ok
ghe_cause = json.loads(res.content)["value"]


url = "https://frontdoor-l4uikgap6gz3m.azurefd.net/DEX_CMS/GHE_COD_COMPLETE?&$orderby=DEATHS_100K%20desc&$filter=FL_RANKABLE%20eq%201%20and%20COUNTRY_CODE%20eq%20%27AFG%27%20and%20SEX_CODE%20eq%20%27BTSX%27%20and%20AGEGROUP_CODE%20eq%20%27ALLAges%27%20and%20YEAR%20eq%202019"
res = requests.get(url)
assert res.ok
ghe_cause = json.loads(res.content)["value"]

url = "https://frontdoor-l4uikgap6gz3m.azurefd.net/DEX_CMS/GHE_COD_COMPLETE?&$select=COUNTRY_CODE"
res = requests.get(url)
assert res.ok
ghe_cause = json.loads(res.content)["value"]


url = "https://frontdoor-l4uikgap6gz3m.azurefd.net/DEX_CMS/GHE_COD_COMPLETE?$apply=groupby((COUNTRY_CODE))"
res = requests.get(url)
assert res.ok
value_json = json.loads(res.content)["value"]
countries = pd.DataFrame.from_records(value_json)


