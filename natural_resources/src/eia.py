"""Load and process EIA data and generate a file with yearly and a file with monthly data.

"""

# +
import os
from datetime import datetime

import numpy as np
import pandas as pd

from natural_resources.src import INPUT_DIR, READY_DIR
# -

OUTPUT_YEARLY_FILE = os.path.join(READY_DIR, "eia_natural-resources-yearly.csv")
OUTPUT_MONTHLY_FILE = os.path.join(READY_DIR, "eia_natural-resources-monthly.csv")

# +
# TODO: For the next update, apply for an API key to be able to use the API, instead of manually downloading files.

# TODO: Rename accronyms to something more meaningful, and check all units.
# Conversion from billion cubit feet to million cubic meters.
BCF_TO_MMCM = 28.32
# Conversion from trillion cubit feet to million cubic meters.
TCF_TO_MMCM = 28320
# Conversion from thousand short tons (MST) to million (metric) tons.
MST_TO_MT = 0.9071847 * 1e-3
# Convert from thousand barrels per day (Mb/d) to million cubic meters per year (mm3/yr).
MBD_TO_MILLION_M3_PER_YEAR = 0.058
# Convert from billion barrels (billion b) to billion cubic meters.
BB_TO_BILLION_CUBIC_METERS = 0.159


# -

def find_last_data_file(variable_name, input_dir=INPUT_DIR):
    data_dir = os.path.join(input_dir, variable_name)
    assert os.path.isdir(data_dir)
    files_found = np.array([file.path for file in os.scandir(data_dir) if file.path.lower().endswith('.csv')])
    timestamps = np.array([datetime.strptime(os.path.basename(file).split('.')[0],
                                    "INT-Export-%m-%d-%Y_%H-%M-%S").strftime("%Y-%m-%d_%H-%M-%S")
                  for file in files_found])
    last_file = files_found[np.array(timestamps).argsort()][-1]
    assert os.path.isfile(last_file)

    return last_file


def load_single_gas_dataset(variable_name, output_column_name, conversion_factor):
    data_file = find_last_data_file(variable_name=variable_name)
    variable_name_in_file = pd.read_csv(data_file, skiprows=1, na_values='--').iloc[0, 1]
    print(variable_name_in_file)
    data = pd.read_csv(data_file, skiprows=1, na_values='--').drop(0).drop(columns='API').\
        rename(columns={'Unnamed: 1': 'Country'})
    data_melt = data.melt(id_vars='Country', var_name='Year')
    data_melt[output_column_name] = data_melt['value'].astype(float) * conversion_factor
    data_melt['Year'] = data_melt['Year'].astype(int)
    data_melt = data_melt.drop(columns=['value'])
    # Remove appended spaces on country names.
    data_melt['Country'] = data_melt['Country'].str.lstrip()

    return data_melt


def combine_single_datasets(data_dict):
    # Merge all dataframes into one.
    combined = pd.DataFrame({'Country': [], 'Year': []})
    for variable_name in data_dict:
        combined = pd.merge(combined, data_dict[variable_name], on=('Country', 'Year'), how='outer')

    return combined


def load_gas_data():
    gas_data = {
        "gas_production": load_single_gas_dataset(
            variable_name="gas_production",
            output_column_name="Dry natural gas production (MMCM)",
            conversion_factor=BCF_TO_MMCM),
        "gas_consumption": load_single_gas_dataset(
            variable_name="gas_consumption",
            output_column_name="Dry natural gas consumption (MMCM)",
            conversion_factor=BCF_TO_MMCM),
        "gas_imports": load_single_gas_dataset(
            variable_name="gas_imports",
            output_column_name="Dry natural gas imports (MMCM)",
            conversion_factor=BCF_TO_MMCM),
        "gas_exports": load_single_gas_dataset(
            variable_name="gas_exports",
            output_column_name="Dry natural gas exports (MMCM)",
            conversion_factor=BCF_TO_MMCM),
        "gas_reserves": load_single_gas_dataset(
            variable_name="gas_reserves",
            output_column_name="Natural gas reserves (MMCM)",
            conversion_factor=TCF_TO_MMCM),
    }

    # Merge all dataframes into one.
    combined = combine_single_datasets(data_dict=gas_data)

    return combined


def assert_that_coal_types_add_up_to_total_coal(data):
    coal_types = ["Anthracite (Mst)", "Metallurgical coal (Mst)", "Bituminous (Mst)", "Subbituminous (Mst)",
                  "Lignite (Mst)"]
    coal_types_sum = data[(data['Country'] == 'World') & (data['mixed'].isin(coal_types))].\
        drop(columns=['mixed', 'Country']).sum(axis=0)
    coal_sum = data[(data['Country'] == 'World') & (data['mixed'] == 'Coal (Mst)')].\
        drop(columns=['mixed', 'Country']).sum(axis=0)

    assert all(abs(coal_types_sum - coal_sum) < 1e-7)


def load_single_coal_dataset(variable_name, output_column_name, conversion_factor, relevant_entity='Coal (Mst)'):
    data_file = find_last_data_file(variable_name=variable_name)
    data = pd.read_csv(data_file, skiprows=1, na_values='--').rename(columns={'Unnamed: 1': 'mixed'}).\
        drop(columns=['API'])
    print(data.loc[1]['mixed'].lstrip())
    # Add a column for country. To do so, assume that country names are not prepended by spaces.
    data['Country'] = data['mixed'].copy()
    data.loc[data['Country'].str.startswith(' '), 'Country'] = np.nan
    data['Country'] = data['Country'].ffill()
    data['mixed'] = data['mixed'].str.lstrip()
    # We only care about coal data.
    assert_that_coal_types_add_up_to_total_coal(data=data)
    data = data[data['mixed'] == relevant_entity].drop(columns='mixed').reset_index(drop=True)
    data_melt = data.melt(id_vars='Country', var_name='Year')
    data_melt[output_column_name] = data_melt['value'] * conversion_factor
    data_melt = data_melt.drop(columns='value')
    data_melt['Year'] = data_melt['Year'].astype(int)

    return data_melt


def load_coal_data():
    coal_data = {
        "coal_production": load_single_coal_dataset(
            variable_name="coal_production",
            output_column_name="Coal production (Mt)",
            conversion_factor=MST_TO_MT),
        "coal_consumption": load_single_coal_dataset(
            variable_name="coal_consumption",
            output_column_name="Coal consumption (Mt)",
            conversion_factor=MST_TO_MT),
        "coal_imports": load_single_coal_dataset(
            variable_name="coal_imports",
            output_column_name="Coal imports (Mt)",
            conversion_factor=MST_TO_MT),
        "coal_exports": load_single_coal_dataset(
            variable_name="coal_exports",
            output_column_name="Coal exports (Mt)",
            conversion_factor=MST_TO_MT),
        "coal_reserves": load_single_gas_dataset(
            variable_name="coal_reserves",
            output_column_name="Coal reserves (Mt)",
            conversion_factor=MST_TO_MT),
    }

    # Merge all dataframes into one.
    combined = combine_single_datasets(data_dict=coal_data)

    return combined


def load_oil_data():
    oil_data = {
        "oil_imports": load_single_gas_dataset(
            variable_name="oil_imports",
            output_column_name="Crude oil imports, including lease condensate (million cubic meters)",
            conversion_factor=MBD_TO_MILLION_M3_PER_YEAR),
        "oil_exports": load_single_gas_dataset(
            variable_name="oil_exports",
            output_column_name="Crude oil exports, including lease condensate (million cubic meters)",
            conversion_factor=MBD_TO_MILLION_M3_PER_YEAR),
        "oil_reserves": load_single_gas_dataset(
            variable_name="oil_reserves",
            output_column_name="Crude oil reserves, including lease condensate (million cubic meters)",
            conversion_factor=BB_TO_BILLION_CUBIC_METERS),
        "oil_production": load_single_coal_dataset(
            variable_name="oil_production",
            output_column_name="Production of petroleum and other liquids (million cubic meters)",
            conversion_factor=MBD_TO_MILLION_M3_PER_YEAR,
            relevant_entity="Total petroleum and other liquids (Mb/d)"),
        "oil_consumption": load_single_coal_dataset(
            variable_name="oil_consumption",
            output_column_name="Consumption of refined petroleum products (million cubic meters)",
            conversion_factor=MBD_TO_MILLION_M3_PER_YEAR,
            relevant_entity="Consumption (Mb/d)"),
    }

    # Merge all dataframes into one.
    combined = combine_single_datasets(data_dict=oil_data)

    return combined


# +
# At https://www.eia.gov/international/data/world they access to these selections of data:
# Monthly petroleum and other liquids production - Used as "oil_production_monthly" 
# Quarterly petroleum and other liquids production - Ignored.
# Annual petroleum and other liquids production - Used as "oil_production".
# Annual refined petroleum products production - Ignored.
# Annual refined petroleum products consumption - Used as "oil_consumption".
# Annual crude and lease condensate imports - Used as "oil_imports".
# Annual crude and lease condensate exports - Used as "oil_exports".
# Annual crude and lease condensate reserves - Used as "oil_reserves".
# Annual bunker refined petroleum products - Ignored.
# More Petroleum and Other Liquids data - Ignored.

# +
# From the EIA web:
# Unit Abbreviation - Unit Definition
# billion b - billion barrels
# bcf - billion cubic feet
# bcm - billion cubic meters
# Billion 2015$ PPP - billion dollars at purchasing power parities
# billion kWh - billion kilowatthours
# Btu/kWh - Btu per kilowatthour
# BtupUSdm - Btu per year 2005 U.S. dollars (market exchange rates)
# BtupUSdp - Btu per year 2005 U.S. dollars (purchasing power parities)
# bUSd - billions of 2005 U.S. dollars
# MMb - millions barrels
# MMBtu/person - million Btu per person
# million kW - million kilowatts
# millions - millions
# MMtonnes CO2 - million metric tonnes carbon dioxide
# million st - million short tons
# Mmt - 1000 metric tons
# tons/person - metric tons of carbon dioxide per person
# mtcdpUSd - metric tons of carbon dioxide per thousand year 2005 U.S. dollars
# MMTOE - million metric tons of oil equivalent
# quad Btu - quadrillion Btu
# quad Btu/st - quadrillion Btu per short ton
# Mb/d - thousand barrels per day
# 1000 Btu/2015$ GDP PPP - thousand Btu per USD at purchasing power parities
# tcf - trillion cubic feet
# Mperson - people in thousands
# terajoules - terajoules
# Mst - thousand short tons
# -

def add_percentage_columns(combined):
    percentage_columns = {
        "Share of coal consumption that comes from imports (%)": {
            "numerator": "Coal imports (Mt)",
            "denominator": "Coal consumption (Mt)",
        },
        "Share of gas consumption that comes from imports (%)": {
            "numerator": "Dry natural gas imports (MMCM)",
            "denominator": "Dry natural gas consumption (MMCM)",
        },
        "Share of oil consumption that comes from imports (%)": {
            "numerator": "Crude oil imports, including lease condensate (million cubic meters)",
            "denominator": "Consumption of refined petroleum products (million cubic meters)",
        },
        "Share of coal production that is exported (%)": {
            "numerator": "Coal exports (Mt)",
            "denominator": "Coal production (Mt)",
        },
        "Share of gas production that is exported (%)": {
            "numerator": "Dry natural gas exports (MMCM)",
            "denominator": "Dry natural gas production (MMCM)",
        },
        "Share of oil production that is exported (%)": {
            "numerator": "Crude oil exports, including lease condensate (million cubic meters)",
            "denominator": "Production of petroleum and other liquids (million cubic meters)",
        },
    }
    combined_added = combined.copy()
    for new_column in percentage_columns:
        numerator = percentage_columns[new_column]["numerator"]
        denominator = percentage_columns[new_column]["denominator"]
        print(f"\n* Adding column: {new_column}")
        combined_added[new_column] = 100 * combined_added[numerator] / combined_added[denominator]

        negative_pct_rows = combined_added[combined_added[new_column].fillna(0) < 0]
        if len(negative_pct_rows) > 0:
            print(f"WARNING: {len(negative_pct_rows)} rows with negative percentages.")

        above_101_pct_rows = combined_added[combined_added[new_column].fillna(0) >= 101]
        if len(above_101_pct_rows) > 0:
            print(f"WARNING: {len(above_101_pct_rows)} rows with above 100 percentages.")
    
    return combined_added


def generate_yearly_dataset():
    print("* Loading gas data.")
    gas_data = load_gas_data()
    print("* Loading coal data.")
    coal_data = load_coal_data()
    print("* Loading oil data.")
    oil_data = load_oil_data()

    combined = pd.DataFrame({'Country': [], 'Year': []})
    combined = pd.merge(combined, gas_data, on=('Country', 'Year'), how='outer')
    combined = pd.merge(combined, coal_data, on=('Country', 'Year'), how='outer')
    combined = pd.merge(combined, oil_data, on=('Country', 'Year'), how='outer')

    combined_added = add_percentage_columns(combined=combined)

    print(f"* Saving data to file: {OUTPUT_YEARLY_FILE}")
    combined_added.to_csv(OUTPUT_YEARLY_FILE, index=False)


def generate_monthly_dataset():
    # TODO.
    pass


def main():
    print("* Generating yearly dataset.")
    yearly_data = generate_yearly_dataset()

    print("* Generating monthly data.")
    # monthly_data = generate_monthly_dataset()


# +
# TODO: Gather all descriptions from the EIA for each table.
# TODO: Rename load_single_gas_dataset and load_single_coal_dataset.
# -

if __name__ == "__main__":
    main()
