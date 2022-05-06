"""Functions to fetch data from the Ocean and Sea Ice Satellite Application Facilities (OSISAF).

"""

import argparse
import os

import pandas as pd

from climate_change.src import READY_DIR


def arctic_sea_ice():
    source_url = "http://osisaf.met.no/quicklooks/sie_graphs/figs_v2p1/nh/osisaf_nh_sia_monthly.txt"
    output_file = os.path.join(READY_DIR, "osisaf_arctic-sea-ice.csv")
    df = pd.read_csv(
        source_url,
        skiprows=7,
        header=None,
        names=["date", "year", "month", "day", "arctic_sea_ice_osisaf"],
        sep=" ",
        na_values=-999,
    ).assign(location="World")
    df["date"] = pd.to_datetime(
        df.year.astype(str) + "-" + df.month.astype(str) + "-" + df.day.astype(str)
    ).dt.date
    df[["date", "location", "arctic_sea_ice_osisaf"]].to_csv(output_file, index=False)


def main():
    arctic_sea_ice()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    args = parser.parse_args()
    main()
