"""Functions to fetch data from the Scripps Institution of Oceanography and create various datasets.

Monthly CO2 concentration from 1959 until present can be found here:
https://keelingcurve.ucsd.edu/permissions-and-data-sources/
Specifically, a file can be downloaded from here:
https://scrippsco2.ucsd.edu/assets/data/atmospheric/stations/in_situ_co2/monthly/monthly_in_situ_co2_mlo.csv

Documentation extracted from the header of the data file:

    The data file below contains 10 columns.  Columns 1-4 give the dates in several redundant
    formats. Column 5 below gives monthly Mauna Loa CO2 concentrations in micro-mol CO2 per
    mole (ppm), reported on the 2012 SIO manometric mole fraction scale.  This is the
    standard version of the data most often sought.  The monthly values have been adjusted
    to 24:00 hours on the 15th of each month.  Column 6 gives the same data after a seasonal
    adjustment to remove the quasi-regular seasonal cycle.  The adjustment involves
    subtracting from the data a 4-harmonic fit with a linear gain factor.  Column 7 is a
    smoothed version of the data generated from a stiff cubic spline function plus 4-harmonic
    functions with linear gain.  Column 8 is the same smoothed version with the seasonal
    cycle removed.  Column 9 is identical to Column 5 except that the missing values from
    Column 5 have been filled with values from Column 7.  Column 10 is identical to Column 6
    except missing values have been filled with values from Column 8.  Missing values are
    denoted by -99.99

"""

import os

import pandas as pd

from climate_change.src import READY_DIR


def co2_concentrations():
    # Define input and output files.
    scripps_data_file = "https://scrippsco2.ucsd.edu/assets/data/atmospheric/stations/in_situ_co2/monthly/" \
                        "monthly_in_situ_co2_mlo.csv"
    output_file = os.path.join(READY_DIR, "scripps_monthly-co2-concentrations.csv")
    # Name for output column of CO2 concentrations.
    co2_column = "monthly_co2_concentrations"

    # Load data and give it a convenient format.
    scripps_data = pd.read_csv(scripps_data_file, skiprows=54)
    scripps_data = scripps_data.rename(columns={
        scripps_data.columns[0]: 'year',
        scripps_data.columns[1]: 'month',
        scripps_data.columns[-1]: co2_column,
    })[['year', 'month', co2_column]].dropna(how='all')
    for column in scripps_data.columns:
        scripps_data[column] = pd.to_numeric(scripps_data[column], errors='coerce')

    # Missing data is denoted with an arbitrary -99.99. Ignore those rows.
    scripps_data = scripps_data[scripps_data[co2_column] > 0].reset_index(drop=True)

    # Assume that each month corresponds to the 15th of each month (as explained above).
    scripps_data['date'] = scripps_data['year'].astype(int).astype(str) + \
        '-' + scripps_data['month'].astype(int).astype(str).str.zfill(2) + \
        '-' + '15'
    scripps_data = scripps_data.assign(location="World")[['location', 'date', co2_column]]

    # Save data to file.
    scripps_data.to_csv(output_file, index=False)


def main():
    co2_concentrations()


if __name__ == "__main__":
    main()
