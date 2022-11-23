"""Functions to fetch data from the National Aeronautics and Space Administration (NASA).

"""

import argparse
import datetime
import os
import requests
import tempfile
from ftplib import FTP

import pandas as pd
from bs4 import BeautifulSoup
from tqdm.auto import tqdm

from climate_change.src import READY_DIR


def process_file(loc: str, source_url: str) -> pd.DataFrame:
    df = (
        pd.read_csv(
            source_url,
            skiprows=1,
            na_values="***",
            usecols=[
                "Year",
                "Jan",
                "Feb",
                "Mar",
                "Apr",
                "May",
                "Jun",
                "Jul",
                "Aug",
                "Sep",
                "Oct",
                "Nov",
                "Dec",
            ],
        )
        .assign(location=loc)
        .rename(columns={"Year": "year"})
        .melt(
            id_vars=["year", "location"],
            var_name="month",
            value_name="temperature_anomaly",
        )
    )
    df["date"] = pd.to_datetime(df.year.astype(str) + df.month + "15", format="%Y%b%d")
    return df[["location", "date", "temperature_anomaly"]].dropna(
        subset=["temperature_anomaly"]
    )


def global_temperature_anomaly():
    output_file = os.path.join(READY_DIR, "nasa_global-temperature-anomaly.csv")
    df = pd.concat(
        [
            process_file(
                "World",
                "https://data.giss.nasa.gov/gistemp/tabledata_v4/GLB.Ts+dSST.csv",
            ),
            process_file(
                "Northern Hemisphere",
                "https://data.giss.nasa.gov/gistemp/tabledata_v4/NH.Ts+dSST.csv",
            ),
            process_file(
                "Southern Hemisphere",
                "https://data.giss.nasa.gov/gistemp/tabledata_v4/SH.Ts+dSST.csv",
            ),
        ]
    )
    df = df[df.date < datetime.datetime.now()]
    df.to_csv(output_file, index=False)


def fetch_sea_ice_extent_data_from_ftp():
    # FTP server and remote directory of data files.
    ftp_dir = 'sidads.colorado.edu'
    remote_directory = '/DATASETS/NOAA/G02135/north/monthly/data'
    # Name of data file for the month of September.
    # We download only September because it is the month with the minimum extent.
    september_file = 'N_09_extent_v3.0.csv'

    # Connect and log in to the FTP server.
    ftp = FTP(ftp_dir)
    ftp.login('anonymous')
    ftp.cwd(remote_directory)

    # List all files in remote FTP directory.
    remote_files = ftp.nlst()[2:]

    # # Download all files within the FTP directory into a temporary folder, and store them in memory.
    # with tempfile.TemporaryDirectory() as temp_dir:
    #     data = []
    #     for remote_file in tqdm(remote_files):
    #         temp_file = os.path.join(temp_dir, remote_file)
    #         with open(temp_file, "wb") as _temp_file:
    #             ftp.retrbinary('RETR ' + remote_file, _temp_file.write)
    #         data.append(pd.read_csv(temp_file))
    # # Gather all data files into a single dataframe.
    # df = pd.concat(data)

    # Instead of downloading all files, get only the one for September (which has every year the minimum extent).
    error = "Data file for September not found; it may have changed name."
    assert september_file in remote_files, error

    # Download data file for September from the FTP directory into a temporary folder, and store data in memory.
    remote_file = september_file
    with tempfile.TemporaryDirectory() as temp_dir:
        temp_file = os.path.join(temp_dir, remote_file)
        with open(temp_file, "wb") as _temp_file:
            ftp.retrbinary(f'RETR {remote_file}', _temp_file.write)
        df = pd.read_csv(temp_file)

    # Close the FTP connection.
    ftp.quit()

    return df


def arctic_sea_ice_extent():
    output_file = os.path.join(READY_DIR, "nasa_arctic-sea-ice.csv")

    # Fetch data from FTP server.
    df = fetch_sea_ice_extent_data_from_ftp()

    # Clean column names (that have spurious spaces).
    df.columns = [column.strip() for column in df.columns]

    # Remove rows with -9999, which should be interpreted as missing data.
    df = df[df["extent"] > 0].reset_index(drop=True)

    # Check that there is only one region (North).
    assert df["region"].str.strip().unique().tolist() == ["N"]

    # Prepare output dataframe.
    df = df[["year", "extent"]].sort_values("year").reset_index(drop=True).\
        rename(columns={"extent": "arctic_sea_ice_nasa"}).assign(**{"location": "World"})

    # Save data to output file.
    df.to_csv(output_file, index=False)


def main():
    global_temperature_anomaly()
    arctic_sea_ice_extent()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    args = parser.parse_args()
    main()
