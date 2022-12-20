"""Functions to fetch data from the National Aeronautics and Space Administration (NASA).

"""

import argparse
import datetime
import os
import tempfile
from ftplib import FTP

import pandas as pd

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
    ftp_dir = "sidads.colorado.edu"
    remote_directory = "/DATASETS/NOAA/G02135/north/monthly/data"
    # Name of data files for the months of February and September, corresponding to the maximum and minimum sea
    # ice extent, respectively.
    # The minimum seems to be happening consistently in September, whereas the maximum occurs between February and
    # March. In fact, March is on average little larger than February, however, given that the difference is small,
    # and for consistency with the months used for the Antarctic sea ice extent (February and September, as chosen by
    # EPA), we will use February and September for the Arctic too.
    files_to_download = [
        # February.
        "N_02_extent_v3.0.csv",
        # September.
        "N_09_extent_v3.0.csv",
    ]

    # Connect and log in to the FTP server.
    ftp = FTP(ftp_dir)
    ftp.login("anonymous")
    ftp.cwd(remote_directory)

    # List all files in remote FTP directory.
    remote_files = ftp.nlst()[2:]

    # Instead of downloading all files, get only the files for maximum and minimum extent.
    error = "Data files for February or September not found; they may have changed names."
    assert set(files_to_download) < set(remote_files), error

    # Download all files within the FTP directory into a temporary folder, and store them in memory.
    with tempfile.TemporaryDirectory() as temp_dir:
        data = []
        for remote_file in files_to_download:
            temp_file = os.path.join(temp_dir, remote_file)
            with open(temp_file, "wb") as _temp_file:
                ftp.retrbinary('RETR ' + remote_file, _temp_file.write)
            data.append(pd.read_csv(temp_file))
    # Gather all data files into a single dataframe.
    df = pd.concat(data)

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

    # Create a column for each month, and rename them appropriately.
    df["mo"] = df["mo"].replace({2: "arctic_sea_ice_february", 9: "arctic_sea_ice_september"})
    df = df.pivot(index="year", columns=["mo"], values=["extent"]).droplevel(0, axis=1).reset_index()

    # Prepare output dataframe.
    df = df.sort_values("year").reset_index(drop=True).assign(**{"location": "World"})

    # Save data to output file.
    df.to_csv(output_file, index=False)


def main():
    global_temperature_anomaly()
    arctic_sea_ice_extent()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    args = parser.parse_args()
    main()
