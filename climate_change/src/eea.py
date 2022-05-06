"""Functions to fetch data from the European Environment Agency (EEA) and create various datasets.

Concentration data is obtained from:
https://www.eea.europa.eu/data-and-maps/daviz/atmospheric-concentration-of-carbon-dioxide-5
This data contains CO2, CH4 and N2O concentrations from 1750 to 2017/2018.
* Their CH4 & N2O concentration data is claimed to be provided by AGAGE. However, I could not manage to find data
  prior to 1986, so I do not know where EEA's CH4 and N2O concentration data between 1750 and 1986 comes from. But I
  will combine the EEA data between 1750 and 1986 from EEA with data > 1986 from NOAA.
* Their CO2 concentration data is claimed to be provided by NOAA. Therefore, we can ignore it, since we already
  extract a more up-to-date version using the noaa module).

"""

import argparse
import os

import pandas as pd

from climate_change.src import READY_DIR


def ghg_concentrations():
    # Define paths to input and output files.
    eea_data_file = (
        "https://www.eea.europa.eu/data-and-maps/daviz/atmospheric-concentration-of-carbon-dioxide-5/"
        "download.csv"
    )
    output_co2_file = os.path.join(READY_DIR, "eea_yearly-co2-concentrations.csv")
    output_ch4_file = os.path.join(READY_DIR, "eea_yearly-ch4-concentrations.csv")
    output_n2o_file = os.path.join(READY_DIR, "eea_yearly-n2o-concentrations.csv")
    # Names for output columns of concentrations.
    co2_column = "yearly_co2_concentrations"
    ch4_column = "yearly_ch4_concentrations"
    n2o_column = "yearly_n2o_concentrations"

    # Load data.
    eea_data = pd.read_csv(eea_data_file)
    eea_data = eea_data.rename(
        columns={
            eea_data.columns[0]: "year",
            eea_data.columns[1]: "value",
            eea_data.columns[2]: "polutant",
        }
    )[["year", "value", "polutant"]]

    # Reshape data.
    eea_data = eea_data.pivot(index="year", columns=["polutant"], values=["value"])
    eea_data.columns = eea_data.columns.droplevel(0)
    eea_data = eea_data.reset_index().rename_axis(None, axis=0)

    # Add location column.
    eea_data = eea_data.assign(location="World")

    # Split data for different gases.
    co2_concentration = (
        eea_data.rename(columns={"CO2 (ppm)": co2_column})[
            ["location", "year", co2_column]
        ]
        .dropna()
        .reset_index(drop=True)
    )
    ch4_concentration = (
        eea_data.rename(columns={"CH4 (ppb)": ch4_column})[
            ["location", "year", ch4_column]
        ]
        .dropna()
        .reset_index(drop=True)
    )
    n2o_concentration = (
        eea_data.rename(columns={"N2O (ppb)": n2o_column})[
            ["location", "year", n2o_column]
        ]
        .dropna()
        .reset_index(drop=True)
    )

    # Save data to output files.
    co2_concentration.to_csv(output_co2_file, index=False)
    ch4_concentration.to_csv(output_ch4_file, index=False)
    n2o_concentration.to_csv(output_n2o_file, index=False)


def main():
    ghg_concentrations()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    args = parser.parse_args()
    main()
