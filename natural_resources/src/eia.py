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

# Conversion from billion cubit feet to million cubic meters.
BCF_TO_MMCM = 28.32
# Conversion from trillion cubit feet to million cubic meters.
TCF_TO_MMCM = 28320
# Conversion from million short tons to million (metric) tons.
MST_TO_MT = 0.9071847


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


def load_single_coal_dataset(variable_name, output_column_name, conversion_factor):
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
    data = data[data['mixed'] == 'Coal (Mst)'].drop(columns='mixed').reset_index(drop=True)
    data_melt = data.melt(id_vars='Country', var_name='Year')
    data_melt[output_column_name] = data_melt['value'] * conversion_factor
    data_melt = data_melt.drop(columns='value')

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
        "coal_reserves": load_single_coal_dataset(
            variable_name="coal_reserves",
            output_column_name="Coal reserves (Mt)",
            conversion_factor=MST_TO_MT),
    }

    # Merge all dataframes into one.
    combined = combine_single_datasets(data_dict=coal_data)

    return coal_data


def generate_yearly_data():
    gas_data = load_gas_data()
    coal_data = load_coal_data()


def generate_monthly_data():
    pass


def main():
    generate_yearly_data()


if __name__ == "__main__":
    main()
