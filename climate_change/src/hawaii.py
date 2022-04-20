"""Functions to fetch data from the University of Hawaii at Manoa.

"""

import argparse
import os

import pandas as pd

from climate_change.src import READY_DIR


def ocean_ph():
    output_file = os.path.join(READY_DIR, "hawaii_ocean-ph.csv")
    source_url = "https://hahana.soest.hawaii.edu/hot/hotco2/HOT_surface_CO2.txt"
    df = (
        pd.read_csv(
            source_url,
            skiprows=8,
            sep="\t",
            usecols=["date", "pHcalc_insitu"],
            na_values=[-999],
        )
        .rename(columns={"pHcalc_insitu": "ocean_ph"})
        .assign(location="World")
    )

    df["date"] = pd.to_datetime(df.date)
    desired_index = pd.date_range(start=df.date.min(), end=df.date.max(), freq="1D")
    df = df.set_index("date")
    df = df.reindex(df.index.union(desired_index))
    df["ocean_ph_yearly_average"] = (
        df.ocean_ph.interpolate(method="time").rolling(365).mean().round(4)
    )

    df = df.dropna(subset=["ocean_ph"]).reset_index().rename(columns={"index": "date"})
    df.to_csv(output_file, index=False)


def main():
    ocean_ph()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    args = parser.parse_args()
    main()
