"""Run sanity checks on the EIA datasets.

"""

import argparse
import os
import webbrowser
from datetime import datetime

import numpy as np
import pandas as pd

from cait import sanity_checks
from natural_resources.src import READY_DIR, SANITY_CHECKS_DIR
from natural_resources.src.eia import load_population_dataset

# TODO: Move sanity checks module outside of cait folder, to a shared folder.

# Date tag and output file for visual inspection of potential issues with the dataset.
DATE_TAG = datetime.now().strftime("%Y-%m-%d")
OUTPUT_FILE = os.path.join(SANITY_CHECKS_DIR, f"eia_sanity_checks_{DATE_TAG}.html")
YEARLY_DATA_FILE = os.path.join(READY_DIR, "eia_natural-resources-yearly.csv")
MONTHLY_DATA_FILE = os.path.join(READY_DIR, "eia_natural-resources-monthly.csv")
# Latest acceptable value for the minimum year.
MIN_YEAR_LATEST_POSSIBLE = 2000
# Maximum delay (from current year) that maximum year can have
MAX_YEAR_MAXIMUM_DELAY = 2
# True to include interactive plots in output HTML file (which can make the inspection slow if there are many figures);
# False to include plots as static images.
EXPORT_INTERACTIVE_PLOTS = False
# Maximum number of plots (of potentially problematic cases) to show in output file.
MAX_NUM_PLOTS = 150

# Define default entity names.
NAME = {
    'country': 'Entity',
    'year': 'Year',
}
# Define minimum, maximum, and minimum relevant values for all variables.
default_range = {
    'min': 0,
    'max': 'World',
    'min_relevant': 1,
}
default_range_percentage = {
    'min': 0,
    'max': 100,
}
RANGES = {
    'natural_gas_production': default_range,
    'natural_gas_consumption': default_range,
    'natural_gas_imports': default_range,
    'natural_gas_exports': default_range,
    'natural_gas_reserves': default_range,
    'natural_gas_net_imports': {
        'min': -1e12,
        'max': 1e12,
    },
    'coal_production': default_range,
    'coal_consumption': default_range,
    'coal_imports': default_range,
    'coal_exports': default_range,
    'coal_reserves': default_range,
    'coal_net_imports': {
        'min': -1e9,
        'max': 1e9,
    },
    'oil_production': default_range,
    'oil_consumption': default_range,
    'oil_imports': default_range,
    'oil_exports': default_range,
    'oil_reserves': default_range,
    'oil_net_imports': {
        'min': -1e9,
        'max': 1e9,
    },
    'natural_gas_production_per_capita': {
        'min': 0,
        'max': 15000,
    },
    'natural_gas_consumption_per_capita': {
        'min': 0,
        'max': 10000,
    },
    'natural_gas_imports_per_capita': {
        'min': 0,
        'max': 5000,
    },
    'natural_gas_exports_per_capita': {
        'min': 0,
        'max': 10000,
    },
    'natural_gas_reserves_per_capita': {
        'min': 0,
        'max': 500000,
    },
    'natural_gas_net_imports_per_capita': {
        'min': -6000,
        'max': 5000,
    },
    'coal_production_per_capita': {
        'min': 0,
        'max': 10,
    },
    'coal_consumption_per_capita': {
        'min': 0,
        'max': 10,
    },
    'coal_imports_per_capita': {
        'min': 0,
        'max': 10,
    },
    'coal_exports_per_capita': {
        'min': 0,
        'max': 10,
    },
    'coal_reserves_per_capita': {
        'min': 0,
        'max': 10,
    },
    'coal_net_imports_per_capita': {
        'min': -10,
        'max': 10,
    },
    'oil_production_per_capita': {
        'min': 0,
        'max': 100,
    },
    'oil_consumption_per_capita': {
        'min': 0,
        'max': 100,
    },
    'oil_imports_per_capita': {
        'min': 0,
        'max': 100,
    },
    'oil_exports_per_capita': {
        'min': 0,
        'max': 100,
    },
    'oil_reserves_per_capita': {
        'min': 0,
        'max': 2000,
    },
    'oil_net_imports_per_capita': {
        'min': -100,
        'max': 100,
    },
}
# Define ranges for variables created when comparing consumption + exports with production + imports.
RANGES_ON_TOTAL_ENERGY = {
    'natural_gas_total': {
        'min_relevant': 1,
    },
    'coal_total': {
        'min_relevant': 1e6,
    },
}


def resample_monthly_data(monthly):
    """Convert monthly data into yearly data.

    Parameters
    ----------
    monthly : pd.DataFrame
        Monthly data.

    Returns
    -------
    monthly_resampled : pd.DataFrame
        Monthly data resampled to be in yearly intervals.

    """
    monthly_resampled = monthly.copy()
    monthly_resampled['Date'] = pd.to_datetime(monthly_resampled['Date'])
    monthly_resampled = monthly_resampled.groupby('Entity').resample('Y', on='Date').sum().reset_index()
    monthly_resampled['Year'] = monthly_resampled['Date'].dt.year.astype(int)
    monthly_resampled = monthly_resampled.drop(columns='Date')

    return monthly_resampled


def _save_output_file(summary, output_file):
    # Ensure output folder exists.
    output_dir = os.path.abspath(os.path.dirname(output_file))
    if not os.path.isdir(output_dir):
        os.makedirs(output_dir)
    print(f"Saving summary to file {output_file}.")
    with open(output_file, "w") as _output_file:
        _output_file.write(summary)


def max_absolute_percentage_error(old, new, epsilon=1e-6):
    """Maximum absolute percentage error (MaxAPE).

    Parameters
    ----------
    old : pd.Series
        Old values.
    new : pd.Series
        New values.
    epsilon : float
        Small number that avoids divisions by zero.

    Returns
    -------
    error : float
        MaxAPE.

    """
    error = np.max(abs(new - old) / (old + epsilon)) * 100

    return error


# Default error metric.
ERROR_METRIC = {
    'function': max_absolute_percentage_error,
    'name': 'maxape',
    'min_relevant': 1,
}
# Error metric to use when comparing consumption + exports with production + imports.
ERROR_METRIC_ON_TOTAL_ENERGY = {
    'function': sanity_checks.mean_absolute_percentage_error,
    'name': 'mape',
    'min_relevant': 10,
}


def run_sanity_checks_on_new_dataset(yearly, population):
    checks_on_single_dataset = sanity_checks.SanityChecksOnSingleDataset(
        data=yearly,
        name=NAME,
        variable_ranges=RANGES,
        population=population,
        min_year_latest_possible=MIN_YEAR_LATEST_POSSIBLE,
        max_year_maximum_delay=MAX_YEAR_MAXIMUM_DELAY,
    )
    # Gather all warnings from checks.
    warnings_single_dataset = checks_on_single_dataset.apply_all_checks()
    summary = checks_on_single_dataset.summarize_warnings_in_html(
        all_warnings=warnings_single_dataset
    )
    return summary


def compare_monthly_and_yearly_datasets(monthly_resampled, yearly, common_columns):
    checks_comparing_datasets = sanity_checks.SanityChecksComparingTwoDatasets(
        data_old=yearly[common_columns],
        data_new=monthly_resampled[common_columns],
        error_metric=ERROR_METRIC,
        name=NAME,
        variable_ranges=RANGES,
        data_label_old='yearly',
        data_label_new='monthly_resampled',
        export_interactive_plots=EXPORT_INTERACTIVE_PLOTS,
        max_num_plots=MAX_NUM_PLOTS,
    )
    warnings_comparing_datasets = checks_comparing_datasets.apply_all_checks()
    summary = checks_comparing_datasets.summarize_warnings_in_html(
        all_warnings=warnings_comparing_datasets
    )
    # Add graphs to be visually inspected.
    summary += checks_comparing_datasets.summarize_figures_to_inspect_in_html(
        warnings=warnings_comparing_datasets
    )
    return summary


def compare_consumption_and_imports_with_production_and_exports(yearly):
    consumption_and_exports = yearly.copy()
    production_and_imports = yearly.copy()

    for variable in ['natural_gas', 'coal']:
        consumption_and_exports[f'{variable}_total'] = \
            consumption_and_exports[f"{variable}_consumption"] + consumption_and_exports[f"{variable}_exports"]
        production_and_imports[f'{variable}_total'] = \
            production_and_imports[f"{variable}_production"] + production_and_imports[f"{variable}_imports"]

    checks_comparing_datasets = sanity_checks.SanityChecksComparingTwoDatasets(
        data_old=consumption_and_exports,
        data_new=production_and_imports,
        error_metric=ERROR_METRIC_ON_TOTAL_ENERGY,
        name=NAME,
        variable_ranges=RANGES_ON_TOTAL_ENERGY,
        data_label_old='consumption_and_exports',
        data_label_new='production_and_imports',
        export_interactive_plots=EXPORT_INTERACTIVE_PLOTS,
        max_num_plots=MAX_NUM_PLOTS,
    )
    warnings_comparing_datasets = checks_comparing_datasets.apply_all_checks()
    summary = checks_comparing_datasets.summarize_warnings_in_html(
        all_warnings=warnings_comparing_datasets
    )
    # Add graphs to be visually inspected.
    summary += checks_comparing_datasets.summarize_figures_to_inspect_in_html(
        warnings=warnings_comparing_datasets
    )
    return summary


def main(output_file=OUTPUT_FILE):
    print("Loading data.")
    # Load yearly and monthly data.
    yearly = pd.read_csv(YEARLY_DATA_FILE)
    monthly = pd.read_csv(MONTHLY_DATA_FILE)
    # Rename columns to be consistent with yearly data.
    monthly = monthly.rename(columns={col: col.replace('_monthly', '') for col in monthly.columns})
    # Load OWID population dataset.
    population = load_population_dataset()
    # Resample monthly data to become yearly.
    monthly_resampled = resample_monthly_data(monthly=monthly)

    print(f"Execute sanity checks on {len(RANGES)} variables of the new dataset.")
    summary = run_sanity_checks_on_new_dataset(yearly=yearly, population=population)

    common_columns = [col for col in list(set(yearly.columns) & set(monthly_resampled.columns))
                      if 'capita' not in col]
    print(f"Execute sanity checks comparing yearly and monthly resampled datasets on {len(common_columns)} variables.")
    summary += compare_monthly_and_yearly_datasets(
        monthly_resampled=monthly_resampled, yearly=yearly, common_columns=common_columns)

    print(f"Execute sanity checks comparing consumption and imports with production and exports.")
    summary += compare_consumption_and_imports_with_production_and_exports(yearly=yearly)

    # Save all warnings to a HTML file for visual inspection.
    _save_output_file(summary=summary, output_file=output_file)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Perform sanity checks on the EIA datasets."
    )
    parser.add_argument(
        "-f",
        "--output_file",
        default=OUTPUT_FILE,
        help=f"Path to output HTML file to be visually inspected. Default: {OUTPUT_FILE}",
    )
    parser.add_argument(
        "-s",
        "--show_in_browser",
        default=False,
        action="store_true",
        help="If given, display output file in browser.",
    )
    args = parser.parse_args()

    main(output_file=args.output_file)
    if args.show_in_browser:
        webbrowser.open("file://" + os.path.abspath(args.output_file))
