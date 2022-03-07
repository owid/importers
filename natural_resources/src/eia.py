"""Load and process EIA data and generate a file with yearly and a file with monthly data.

TODO: For the next update, apply for an API key to be able to use the API, instead of manually downloading files.

"""

import os
from datetime import datetime

import numpy as np
import pandas as pd
from owid import catalog

from natural_resources.src import CONFIG_DIR, INPUT_DIR, READY_DIR

# Path to file translating country names from EIA to OWID country names.
COUNTRIES_FILE = os.path.join(CONFIG_DIR, "country_standardized.csv")
# Path to output file of yearly data.
OUTPUT_YEARLY_FILE = os.path.join(READY_DIR, "eia_natural-resources-yearly.csv")
# Path to output file of monthly data.
OUTPUT_MONTHLY_FILE = os.path.join(READY_DIR, "eia_natural-resources-monthly.csv")
# Number of significant figures to assume for the values of all variables in output data.
N_SIGNIFICANT_FIGURES = 4
# Conversion of units, following the definitions at: https://www.eia.gov/state/seds/sep_prices/notes/pr_metric.pdf
# * Conversion factor from billion cubic feet (bcf) to cubic meters.
#   Given that 1 feet = 0.3048 meters,
#   1 bcf = 1 billion cubic feet * (0.3048 meters / feet)**3 * (1e9 / billion) =
#   = 2.832e7 cubic meters
BCF_TO_CUBIC_METERS = 2.832e7
# * Conversion factor from trillion cubic feet (tcf) to cubic meters.
#   Given that 1 feet = 0.3048 cubic meters,
#   1 tcf = 1 trillion cubic feet * (0.3048 meters / feet)**3 * (1e12 / trillion) =
#   = 2.832e10 cubic meters
TCF_TO_CUBIC_METERS = 2.832e10
# * Conversion factor from thousand short tons (MST) to tonnes.
#   Given that 1 US (short) ton = 0.9071847 (metric) tonnes,
#   1 MST = 1 thousand short tons * (0.9071847 tonnes / short tons) * (1e3 / thousand) =
#   = 9.072e2 tonnes
MST_TO_TONNES = 9.072e2
# * Conversion factor from thousand barrels per day (Mb/d) to cubic meters per year.
#   Given that 1 barrel = 0.1589873 cubic meters, and given that 1 (Julian) year = 365.25 days,
#   1 Mb/d = 1 thousand barrels / day * (0.1589873 cubic meters / barrel) * (1e3 / thousand) * (365.25 days / year) =
#   = 5.807e+04 cubic meters / year
MBD_TO_CUBIC_METERS_PER_YEAR = 5.807e+04
# * Conversion factor from billion barrels (billion b) to cubic meters.
#   Given that 1 barrel = 0.1589873 cubic meters,
#   1 billion b = 1 billion barrels * (0.1589873 cubic meters / barrel) * (1e9 / billion) =
#   = 1.590e+08 cubic meters
BB_TO_CUBIC_METERS = 1.590e+08
# * Conversion factor from thousand barrels per day (Mb/d) to cubic meters per month.
#   Given that 1 barrel = 0.1589873 cubic meters, and given that 1 average month in a (Julian) year = 365.25 / 12 days,
#   1 Mb/d = 1 thousand barrels per day * (0.1589873 cubic meters / barrel) * (1e3 / thousand) * (365.25 days / year) *
#   * (1 year / 12 months) =
#   = 4.839e+03 cubic meters / month
MBD_TO_CUBIC_METERS_PER_MONTH = 4.839e+03


def find_last_data_file(variable_name):
    """Find last data file inside a folder of a certain variable (within the input folder).

    Assume that input folder contains a sub-folder for each variable, e.g. 'oil_production'.

    Parameters
    ----------
    variable_name : str
        Variable name (which coincides with the name of a sub-folder inside input folder).

    Returns
    -------
    last_file : str
        Path to latest input data file added for considered variable.

    """
    data_dir = os.path.join(INPUT_DIR, variable_name)
    assert os.path.isdir(data_dir)
    files_found = np.array([file.path for file in os.scandir(data_dir) if file.path.lower().endswith('.csv')])
    timestamps = np.array([datetime.strptime(os.path.basename(file).split('.')[0],
                                             "INT-Export-%m-%d-%Y_%H-%M-%S").strftime("%Y-%m-%d_%H-%M-%S")
                           for file in files_found])
    last_file = files_found[np.array(timestamps).argsort()][-1]
    assert os.path.isfile(last_file)

    return last_file


def load_population_dataset():
    """Load OWID population dataset.

    Returns
    -------
    population : pd.DataFrame
        Population data.

    """
    # Load OWID population dataset.
    population = catalog.find("population", namespace="owid").load().reset_index()[["country", "population", "year"]].\
        rename(columns={"country": "Entity", "population": "Population", "year": "Year"})

    return population


def load_simple_dataset(variable_name, conversion_factor):
    """Load a simple dataset (one with an easy structure) that has been manually downloaded from the EIA website.

    Parameters
    ----------
    variable_name : str
        Variable name (which coincides with the name of a sub-folder inside input folder).
    conversion_factor : float
        Factor to convert EIA units into OWID units.

    Returns
    -------
    data_melt : pd.DataFrame
        Data extracted from the dataset, in a convenient format and in convenient units.

    """
    data_file = find_last_data_file(variable_name=variable_name)
    variable_name_in_file = pd.read_csv(data_file, skiprows=1, na_values='--').iloc[0, 1]
    print(variable_name_in_file)
    data = pd.read_csv(data_file, skiprows=1, na_values='--').drop(0).drop(columns='API').\
        rename(columns={'Unnamed: 1': 'Entity'})
    data_melt = data.melt(id_vars='Entity', var_name='Year')
    data_melt[variable_name] = data_melt['value'].astype(float) * conversion_factor
    data_melt['Year'] = data_melt['Year'].astype(int)
    data_melt = data_melt.drop(columns=['value'])
    # Remove appended spaces on country names.
    data_melt['Entity'] = data_melt['Entity'].str.lstrip()

    return data_melt


def load_dataset_with_indented_entities(variable_name, conversion_factor, relevant_entity):
    """Load a dataset that has been manually downloaded from the EIA website, where one of the columns has entity levels
    defined by their indentation.

    Parameters
    ----------
    variable_name : str
        Variable name (which coincides with the name of a sub-folder inside input folder).
    conversion_factor : float
        Factor to convert EIA units into OWID units.
    relevant_entity : str
        Name of the relevant entity to be extracted from the original file.

    Returns
    -------
    data_melt : pd.DataFrame
        Data extracted from the dataset, in a convenient format and in convenient units.

    """
    data_file = find_last_data_file(variable_name=variable_name)
    data = pd.read_csv(data_file, skiprows=1, na_values='--').rename(columns={'Unnamed: 1': 'mixed'}).\
        drop(columns=['API'])
    print(data.loc[1]['mixed'].lstrip())
    # Add a column for country. To do so, assume that country names are not prepended by spaces.
    data['Entity'] = data['mixed'].copy()
    data.loc[data['Entity'].str.startswith(' '), 'Entity'] = np.nan
    data['Entity'] = data['Entity'].ffill()
    data['mixed'] = data['mixed'].str.lstrip()
    # We only care about coal data.
    data = data[data['mixed'] == relevant_entity].drop(columns='mixed').reset_index(drop=True)
    data_melt = data.melt(id_vars='Entity', var_name='Year')
    data_melt[variable_name] = data_melt['value'] * conversion_factor
    data_melt = data_melt.drop(columns='value')
    data_melt['Year'] = data_melt['Year'].astype(int)

    return data_melt


def merge_dataframes(list_of_dataframes):
    """Outer merge multiple dataframes.

    Parameters
    ----------
    list_of_dataframes : list
        Dataframes to merge.

    Returns
    -------
    combined : pd.DataFrame
        Combined data.

    """
    # Merge all dataframes into one.
    combined = pd.DataFrame({'Entity': [], 'Year': []})
    for dataframe in list_of_dataframes:
        combined = pd.merge(combined, dataframe, on=('Entity', 'Year'), how='outer')

    return combined


def load_gas_data():
    """Load all natural gas data.

    Returns
    -------
    combined : pd.DataFrame
        Combined natural gas data.

    """
    gas_data = [
        # Dry natural gas production (cubic meters).
        load_simple_dataset(
            variable_name="natural_gas_production",
            conversion_factor=BCF_TO_CUBIC_METERS),
        # Dry natural gas consumption (cubic meters).
        load_simple_dataset(
            variable_name="natural_gas_consumption",
            conversion_factor=BCF_TO_CUBIC_METERS),
        # Dry natural gas imports (cubic meters).
        load_simple_dataset(
            variable_name="natural_gas_imports",
            conversion_factor=BCF_TO_CUBIC_METERS),
        # Dry natural gas exports (cubic meters).
        load_simple_dataset(
            variable_name="natural_gas_exports",
            conversion_factor=BCF_TO_CUBIC_METERS),
        # Dry natural gas reserves (cubic meters).
        load_simple_dataset(
            variable_name="natural_gas_reserves",
            conversion_factor=TCF_TO_CUBIC_METERS),
    ]

    # Merge all dataframes into one.
    combined = merge_dataframes(list_of_dataframes=gas_data)

    return combined


def load_coal_data():
    """Load all coal data.

    Returns
    -------
    combined : pd.DataFrame
        Combined coal data.

    """
    coal_data = [
        # Coal production (tonnes).
        load_dataset_with_indented_entities(
            variable_name="coal_production",
            conversion_factor=MST_TO_TONNES,
            relevant_entity='Coal (Mst)',
        ),
        # Coal consumption (tonnes).
        load_dataset_with_indented_entities(
            variable_name="coal_consumption",
            conversion_factor=MST_TO_TONNES,
            relevant_entity='Coal (Mst)',
        ),
        # Coal imports (tonnes).
        load_dataset_with_indented_entities(
            variable_name="coal_imports",
            conversion_factor=MST_TO_TONNES,
            relevant_entity='Coal (Mst)',
            ),
        # Coal exports (tonnes).
        load_dataset_with_indented_entities(
            variable_name="coal_exports",
            conversion_factor=MST_TO_TONNES,
            relevant_entity='Coal (Mst)',
        ),
        # Coal reserves (tonnes).
        load_simple_dataset(
            variable_name="coal_reserves",
            conversion_factor=MST_TO_TONNES),
    ]

    # Merge all dataframes into one.
    combined = merge_dataframes(list_of_dataframes=coal_data)

    return combined


def load_oil_data():
    """Load all oil data.

    Returns
    -------
    combined : pd.DataFrame
        Combined oil data.

    """
    oil_data = [
        # Crude oil production, including lease condensate (cubic meters).
        load_dataset_with_indented_entities(
            variable_name="oil_production",
            conversion_factor=MBD_TO_CUBIC_METERS_PER_YEAR,
            relevant_entity="Crude oil including lease condensate (Mb/d)"),
        # Consumption of refined petroleum products (cubic meters).
        load_dataset_with_indented_entities(
            variable_name="oil_consumption",
            conversion_factor=MBD_TO_CUBIC_METERS_PER_YEAR,
            relevant_entity="Consumption (Mb/d)"),
        # Crude oil imports, including lease condensate (cubic meters).
        load_simple_dataset(
            variable_name="oil_imports",
            conversion_factor=MBD_TO_CUBIC_METERS_PER_YEAR),
        # Crude oil exports, including lease condensate (cubic meters).
        load_simple_dataset(
            variable_name="oil_exports",
            conversion_factor=MBD_TO_CUBIC_METERS_PER_YEAR),
        # Crude oil reserves, including lease condensate (cubic meters).
        load_simple_dataset(
            variable_name="oil_reserves",
            conversion_factor=BB_TO_CUBIC_METERS),
    ]

    # Merge all dataframes into one.
    combined = merge_dataframes(list_of_dataframes=oil_data)

    return combined


def add_percentage_columns(combined):
    """Add percentage columns (e.g. share of coal consumption that is imported) to data.

    NOTE: We decided to not use these columns, since we are ignoring the effect of trading. For example, the percentage
    of consumed gas that is imported may be larger than 100% (because part of that imported gas is exported).

    Parameters
    ----------
    combined : pd.DataFrame
        Combined (natural gas/coal/oil) data.

    Returns
    -------
    combined_added : pd.DataFrame
        Input data with additional percentage columns.

    """
    percentage_columns = {
        # Share of coal consumption that comes from imports (%)
        "share_of_coal_consumption_imported": {
            "numerator": "coal_imports",
            "denominator": "coal_consumption",
        },
        # Share of gas consumption that comes from imports (%).
        "share_of_gas_consumption_imported": {
            "numerator": "natural_gas_imports",
            "denominator": "natural_gas_consumption",
        },
        # Share of coal production that is exported (%).
        "share_of_coal_production_exported": {
            "numerator": "coal_exports",
            "denominator": "coal_production",
        },
        # Share of gas production that is exported (%).
        "share_of_gas_production_exported": {
            "numerator": "natural_gas_exports",
            "denominator": "natural_gas_production",
        },
        # Share of oil production that is exported (%).
        "share_of_oil_production_exported": {
            "numerator": "oil_exports",
            "denominator": "oil_production",
        },
    }
    combined_added = combined.copy()
    for new_column in percentage_columns:
        numerator = percentage_columns[new_column]["numerator"]
        denominator = percentage_columns[new_column]["denominator"]
        print(f"\n  * Adding column: {new_column}")
        combined_added[new_column] = 100 * combined_added[numerator] / combined_added[denominator]

        negative_pct_rows = combined_added[combined_added[new_column].fillna(0) < 0]
        if len(negative_pct_rows) > 0:
            print(f"  WARNING: {len(negative_pct_rows)} rows with negative percentages.")

        above_101_pct_rows = combined_added[combined_added[new_column].fillna(0) >= 101]
        if len(above_101_pct_rows) > 0:
            print(f"  WARNING: {len(above_101_pct_rows)} rows with above 100 percentages.")
    
    return combined_added


def add_net_imports(data):
    """Add net imports to data.

    Parameters
    ----------
    data : pd.DataFrame
        Original data.

    Returns
    -------
    data : pd.DataFrame
        Original data after including columns on net imports.

    """
    data = data.copy()

    for variable in ['natural_gas', 'coal', 'oil']:
        data[f'{variable}_net_imports'] = data[f'{variable}_imports'] - data[f'{variable}_exports']

    return data


def add_per_capita_columns(data):
    """Add per-capita columns to data.

    Parameters
    ----------
    data : pd.DataFrame
        Combined (natural gas/coal/oil) data.

    Returns
    -------
    data : pd.DataFrame
        Input data with additional per-capita columns.

    """
    data = data.copy()

    # Create a per-capita column for each relevant variable.
    per_capita_columns = [column for column in data.columns if column not in ['Entity', 'Year', 'Date']
                          if not column.startswith('share_')]

    # Standardize country names.
    country_remapping = pd.read_csv(COUNTRIES_FILE).set_index('eia_name').to_dict()['owid_name']
    data['Entity'] = data['Entity'].replace(country_remapping)

    # Add population data.
    population = load_population_dataset()

    # Temporarily add a year column to monthly data be able to merge with population dataset.
    if 'Date' in data.columns:
        data['Year'] = pd.to_datetime(data['Date']).dt.year

    # Add population to data.
    data = pd.merge(data, population, on=['Entity', 'Year'], how='left')

    # Add per capita variables one by one.
    for column in per_capita_columns:
        new_column = column + '_per_capita'
        data[new_column] = data[column] / data['Population']

    # Remove unnecessary columns.
    if 'Date' in data.columns:
        del data['Year']

    first_columns = ['Entity', 'Year', 'Date', 'Population']
    columns = [column for column in data.columns if column in first_columns] + \
              [column for column in data.columns if column not in first_columns]
    data = data[columns]

    return data


def clean_dataset(data, fixed_columns):
    """Execute a basic cleaning of the data.

    Cleaning steps:
    * Replace infinities by nan.
    * Drop rows where all relevant columns are nan.
    * Sort rows in a convenient way.

    Parameters
    ----------
    data : pd.DataFrame
        Combined (natural gas/coal/oil) data, which can contain additional percentage and per-capita columns.
    fixed_columns : list
        Fixed columns in data (usually 'Entity', 'Year' or 'Date').

    Returns
    -------
    clean : pd.DataFrame
        Clean data.

    """
    variables = [col for col in data.columns if col not in fixed_columns]
    clean = data.copy()
    # Replace infinities by nan.
    clean = clean.replace([np.inf, -np.inf], np.nan)
    # Remove rows where all columns are nan.
    clean = clean.dropna(subset=variables, how='all')
    clean = clean.sort_values(fixed_columns).reset_index(drop=True)

    return clean


def load_oil_monthly_dataset():
    """Load monthly dataset for oil production.

    Returns
    -------
    data_melt : pd.DataFrame
        Monthly data of oil production in a convenient format and units.

    """
    # Monthly production of crude oil including lease condensate (cubic meters).
    conversion_factor = MBD_TO_CUBIC_METERS_PER_MONTH
    variable_name = "oil_production_monthly"
    relevant_entity = "Crude oil including lease condensate (Mb/d)"
    data_file = find_last_data_file(variable_name=variable_name)
    data = pd.read_csv(data_file, skiprows=1, na_values='--').rename(columns={'Unnamed: 1': 'mixed'}).\
        drop(columns=['API'])
    print(data.loc[1]['mixed'].lstrip())
    # Add a column for country. To do so, assume that country names are not prepended by spaces.
    data['Entity'] = data['mixed'].copy()
    data.loc[data['Entity'].str.startswith(' '), 'Entity'] = np.nan
    data['Entity'] = data['Entity'].ffill()
    data['mixed'] = data['mixed'].str.lstrip()
    data = data[data['mixed'] == relevant_entity].drop(columns='mixed').reset_index(drop=True)
    data_melt = data.melt(id_vars='Entity', var_name='Date')
    data_melt[variable_name] = data_melt['value'] * conversion_factor
    data_melt = data_melt.drop(columns='value')
    data_melt['Date'] = pd.to_datetime(data_melt['Date']).astype(str)

    return data_melt


def save_data_in_a_convenient_format(data, output_file, columns_to_format, n_significant_figures=N_SIGNIFICANT_FIGURES):
    """Save output data in a file using a convenient format.

    To save data using scientific notation, one could simply use:
    >>> data.to_csv(output_file, float_format="%.3e", index=False)
    However, this would add "e00" to all numbers between 0 and 10, which also implies saving all zeros as "0.000e00",
    which makes the file significantly larger.
    To avoid this, convert values to strings and format them in a more convenient way.

    Parameters
    ----------
    data : pd.DataFrame
        Data to be stored in a file.
    output_file : str
        Path to output file.
    columns_to_format : list
        Variables whose format has to be changed to a more convenient format.
    n_significant_figures : int
        Number of significant figures for all float variables.

    """
    data = data.copy()
    format_rule = f"{{:.{n_significant_figures -1}e}}"
    for column in columns_to_format:
        data[column] = data[column].map(format_rule.format).\
            str.replace('e+00', '', regex=False).str.replace('nan', '', regex=False)
        data.loc[data[column] == "0." + "0" * (n_significant_figures - 1), column] = "0"

    data.to_csv(output_file, index=False)


def generate_yearly_dataset():
    """Generate yearly dataset and store it in a file.

    """
    print("* Loading yearly data.")
    all_data = [
        load_gas_data(),
        load_coal_data(),
        load_oil_data(),
    ]

    print("* Combining yearly data.")
    combined = merge_dataframes(all_data)

    # print("* Adding percentage columns.")
    # combined = add_percentage_columns(combined=combined)

    print("* Adding net imports.")
    combined = add_net_imports(data=combined)

    print("* Add per-capita columns.")
    combined = add_per_capita_columns(data=combined)

    print("* Cleaning yearly data.")
    clean_data = clean_dataset(data=combined, fixed_columns=['Entity', 'Year', 'Population'])

    print(f"* Saving data to file: {OUTPUT_YEARLY_FILE}")
    save_data_in_a_convenient_format(
        data=clean_data,
        output_file=OUTPUT_YEARLY_FILE,
        columns_to_format=[column for column in clean_data.columns if column not in ['Entity', 'Year']])


def generate_monthly_dataset():
    """Generate yearly dataset and store it in a file.

    """
    print("* Loading oil monthly production data.")
    monthly_data = load_oil_monthly_dataset()

    print("* Add per-capita columns.")
    monthly_data = add_per_capita_columns(data=monthly_data)

    print("* Cleaning monthly data.")
    clean_data = clean_dataset(data=monthly_data, fixed_columns=['Entity', 'Date', 'Population'])

    print(f"* Saving data to file: {OUTPUT_MONTHLY_FILE}")
    save_data_in_a_convenient_format(
        data=clean_data,
        output_file=OUTPUT_MONTHLY_FILE,
        columns_to_format=[column for column in clean_data.columns if column not in ['Entity', 'Date']])


def main():
    print("* Generating yearly dataset.")
    generate_yearly_dataset()

    print("* Generating monthly data.")
    generate_monthly_dataset()


if __name__ == "__main__":
    main()
