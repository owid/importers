"""Download and generate all individual datasets related to climate change impacts, combine them, and export two
datasets (one with yearly, and one with monthly data).

"""

import argparse
import os

import pandas as pd

import eea
import epa
import hawaii
import metoffice
import nasa
import noaa
import osisaf
import scripps
from climate_change.src import READY_DIR, OUTPUT_DIR

# Define paths to output files.
OUTPUT_YEARLY_FILE = os.path.join(OUTPUT_DIR, "climate_change_impacts_yearly.csv")
OUTPUT_MONTHLY_FILE = os.path.join(OUTPUT_DIR, "climate_change_impacts_monthly.csv")


def generate_individual_datasets():
    """Generate individual datasets for all institutions as csv files.

    """
    eea.ghg_concentrations()
    epa.ocean_heat_content()
    epa.antarctic_sea_ice()
    epa.mass_balance_global_glaciers()
    epa.snow_cover_north_america()
    epa.antarctica_greenland_ice_sheet_loss()
    hawaii.ocean_ph()
    metoffice.annual_sea_surface_temperature()
    metoffice.monthly_sea_surface_temperature()
    nasa.global_temperature_anomaly()
    nasa.arctic_sea_ice_extent()
    noaa.monthly_concentrations()
    noaa.sea_level_rise()
    noaa.yearly_long_run_co2_concentration()
    osisaf.arctic_sea_ice()
    scripps.co2_concentrations()


def resample_monthly_to_yearly_data(df, aggregations=None, date_column='date', year_column='year'):
    """Resample a dataframe of monthly data (with a column of dates) into another of yearly data.

    Parameters
    ----------
    df : pd.DataFrame
        Dataframe to be resampled.
    aggregations : dict
        Type of aggregation to apply to each column in the dataframe. If not specified, 'mean' will be applied.
    date_column : str
        Name of date column, that should be present in the input dataframe.
    year_column : str
        Name of year column, that will be added to the output dataframe.

    Returns
    -------
    df_resampled : pd.DataFrame
        Original dataframe, after resampling.

    """
    df_resampled = df.copy()
    # Define dictionary of aggregations.
    if aggregations is None:
        # By default, if nothing specified, assume that all columns should be averaged.
        resampling = {col: 'mean' for col in df_resampled.columns if col not in [date_column]}
    else:
        # Ensure columns in aggregations dictionary exist in dataset.
        resampling = {col: aggregations[col] for col in aggregations
                      if col in df_resampled.columns if col != date_column}
    # Convert date column into a datetime column, and resample.
    df_resampled[date_column] = pd.to_datetime(df_resampled[date_column])
    df_resampled = df_resampled.resample('Y', on=date_column).agg(resampling).reset_index()
    # Create year column and delete original date column.
    df_resampled[year_column] = df_resampled[date_column].dt.year
    df_resampled = df_resampled.drop(columns='date').reset_index(drop=True)

    return df_resampled


def generate_long_run_ghg_concentrations_data(yearly_file, monthly_file, gas):
    """Combine yearly and (resampled) monthly data of greenhouse gas concentrations to generate a long-run yearly
    dataset.

    Parameters
    ----------
    yearly_file : str
        Path to file containing yearly data (which must contain a 'year' column).
    monthly_file : str
        Path to file containing monthly data (which must contain a 'date' column).
    gas : str
        Name of the gas.

    Returns
    -------

    """
    yearly = pd.read_csv(yearly_file)
    monthly = pd.read_csv(monthly_file)

    aggregations = {
        'location': 'first',
        f'monthly_{gas}_concentrations': "mean",
    }
    monthly_resampled = resample_monthly_to_yearly_data(df=monthly, aggregations=aggregations).\
        rename(columns={f'monthly_{gas}_concentrations': f'yearly_{gas}_concentrations'})
    monthly_first_year = monthly_resampled['year'].min()
    combined = pd.concat([yearly[yearly['year'] < monthly_first_year], monthly_resampled], ignore_index=True).\
        sort_values(['location', 'year']).reset_index(drop=True)

    return combined


def generate_monthly_dataset():
    """Combine all monthly data to generate a unique dataset.

    Returns
    -------
    monthly_data : pd.DataFrame
        Monthly data.

    """
    monthly_dataset_files = [
        os.path.join(READY_DIR, "epa_antarctica-greenland-ice-sheet-loss.csv"),
        os.path.join(READY_DIR, "noaa_monthly-n2o-concentrations.csv"),
        os.path.join(READY_DIR, "metoffice_monthly-sea-surface-temperature.csv"),
        os.path.join(READY_DIR, "hawaii_ocean-ph.csv"),
        os.path.join(READY_DIR, "nasa_global-temperature-anomaly.csv"),
        os.path.join(READY_DIR, "noaa_sea-level-rise.csv"),
        os.path.join(READY_DIR, "osisaf_arctic-sea-ice.csv"),
        os.path.join(READY_DIR, "noaa_monthly-ch4-concentrations.csv"),
        # os.path.join(READY_DIR, "scripps_monthly-co2-concentrations.csv"),
        os.path.join(READY_DIR, "noaa_monthly-co2-concentrations.csv"),
    ]
    monthly_data = pd.DataFrame({'entity': [], 'date': []})
    for dataset_file in monthly_dataset_files:
        dataset = pd.read_csv(dataset_file).rename(columns={'location': 'entity'})
        monthly_data = pd.merge(monthly_data, dataset, on=['entity', 'date'], how='outer')

    # Sort values and columns conveniently.
    monthly_data = monthly_data.sort_values(['entity', 'date']).reset_index(drop=True)
    monthly_columns = ['entity', 'date'] + [col for col in monthly_data.columns if col not in ['entity', 'date']]
    monthly_data = monthly_data[monthly_columns]

    return monthly_data


def generate_yearly_dataset():
    """Combine all yearly data to generate a unique dataset.

    Returns
    -------
    yearly_data : pd.DataFrame
        Yearly data.

    """
    # Gather gas yearly concentration files.
    gas_concentration_files = {
        'co2': {
            'yearly': os.path.join(READY_DIR, "noaa_yearly-long-run-co2-concentrations.csv"),
            'monthly': os.path.join(READY_DIR, "scripps_monthly-co2-concentrations.csv"),
        },
        'ch4': {
            'yearly': os.path.join(READY_DIR, "eea_yearly-ch4-concentrations.csv"),
            'monthly': os.path.join(READY_DIR, "noaa_monthly-ch4-concentrations.csv"),
        },
        'n2o': {
            'yearly': os.path.join(READY_DIR, "eea_yearly-n2o-concentrations.csv"),
            'monthly': os.path.join(READY_DIR, "noaa_monthly-n2o-concentrations.csv"),
        },
    }
    yearly_data = pd.DataFrame({'entity': [], 'year': []})
    for gas in gas_concentration_files:
        dataset = generate_long_run_ghg_concentrations_data(
            yearly_file=gas_concentration_files[gas]['yearly'],
            monthly_file=gas_concentration_files[gas]['monthly'],
            gas=gas
        ).rename(columns={'location': 'entity'})
        yearly_data = pd.merge(yearly_data, dataset, on=['entity', 'year'], how='outer')

    # Include all other yearly data (not related to concentrations).
    yearly_dataset_files = [
        os.path.join(READY_DIR, "epa_antarctic-sea-ice.csv"),
        # os.path.join(READY_DIR, "eea_yearly-co2-concentrations.csv"),
        os.path.join(READY_DIR, "epa_mass-balance-global-glaciers.csv"),
        # os.path.join(READY_DIR, "noaa_yearly-long-run-co2-concentrations.csv"),
        # os.path.join(READY_DIR, "eea_yearly-ch4-concentrations.csv"),
        # os.path.join(READY_DIR, "eea_yearly-n2o-concentrations.csv"),
        os.path.join(READY_DIR, "epa_700m-ocean-heat-content.csv"),
        os.path.join(READY_DIR, "epa_snow-cover-north-america.csv"),
        os.path.join(READY_DIR, "metoffice_annual-sea-surface-temperature.csv"),
        os.path.join(READY_DIR, "epa_2000m-ocean-heat-content.csv"),
        os.path.join(READY_DIR, "nasa_arctic-sea-ice.csv"),
    ]
    for dataset_file in yearly_dataset_files:
        dataset = pd.read_csv(dataset_file).rename(columns={'location': 'entity'})
        yearly_data = pd.merge(yearly_data, dataset, on=['entity', 'year'], how='outer')

    # Sort values and columns conveniently.
    yearly_data = yearly_data.sort_values(['entity', 'year']).reset_index(drop=True)
    yearly_columns = ['entity', 'year'] + [col for col in yearly_data.columns if col not in ['entity', 'year']]
    yearly_data = yearly_data[yearly_columns]

    return yearly_data


def main():
    print(f"Generate individual datasets in folder: {READY_DIR}")
    generate_individual_datasets()

    print("Generate output monthly dataset.")
    monthly_data = generate_monthly_dataset()

    print("Generate output yearly dataset.")
    yearly_data = generate_yearly_dataset()

    print(f"Save data into two output files:\n* {OUTPUT_YEARLY_FILE}\n* {OUTPUT_MONTHLY_FILE}")
    yearly_data.to_csv(OUTPUT_YEARLY_FILE, index=False)
    monthly_data.to_csv(OUTPUT_MONTHLY_FILE, index=False)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    args = parser.parse_args()
    main()
