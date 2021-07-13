import pandas as pd
from io import StringIO
from numpy import arange

# import grequests
import pdb
import requests
import concurrent.futures
from time import sleep
import glob
import os.path

df = pd.read_csv("summary.csv", index_col=0, header=0)
dfg = df.sort_values(by=["HeadCount"]).groupby(["CountryName", "RequestYear"])
median_poverty_line_by_country_year = {}
for country_year_tuple in dfg.groups.keys():
    country_year_df = dfg.get_group(country_year_tuple)
    median_poverty_line = country_year_df.iloc[
        [country_year_df.HeadCount.searchsorted(0.5)]
    ]
    median_poverty_line_by_country_year[country_year_tuple] = median_poverty_line.iloc[
        0
    ].poverty_line

df = pd.DataFrame.from_dict(
    median_poverty_line_by_country_year, orient="index"
).reset_index()
df = df.rename(columns={"index": "country_year", 0: "poverty_line"})
df[["country", "year"]] = pd.DataFrame(df["country_year"].tolist(), index=df.index)
df.drop(columns=["country_year"])
df = df[["country", "year", "poverty_line"]]

pdb.set_trace()


# path = "data_by_poverty_line"
# all_files = glob.glob(path + "/*.csv")

# li = []

# for filename in all_files:
#     df = pd.read_csv(filename, index_col=0, header=0)
#     df["poverty_line"] = os.path.basename(os.path.splitext(filename)[0])
#     li.append(df)

# frame = pd.concat(li, axis=0, ignore_index=True)
# frame = frame[frame.CountryName.isin(["United States", "China"])]
# frame.to_csv("summary.csv")

# df = pd.read_csv(StringIO(result.text))
# df.to_html('temp.html')
