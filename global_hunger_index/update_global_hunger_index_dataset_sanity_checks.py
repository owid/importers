"""Sanity checks on the Global Hunger Index dataset.

"""

import argparse
import base64
import os
from datetime import datetime

import pandas as pd
import plotly
import plotly.express as px
from tqdm.auto import tqdm

# Define common paths.
CURRENT_DIR = os.path.dirname(os.path.abspath(__file__))
INPUT_DIR = os.path.join(CURRENT_DIR, "input")
GRAPHER_DIR = os.path.join(CURRENT_DIR, "grapher")
OLD_DATA_URL = "https://github.com/owid/owid-datasets/raw/master/datasets/Global%20Hunger%20Index-%20IFPRI%20(2018)/" \
               "Global%20Hunger%20Index-%20IFPRI%20(2018).csv"
GHI_NAME = 'Global Hunger Index (GHI, 2021)'

# Date tag and output file for visual inspection of potential issues with the dataset.
DATE_TAG = datetime.now().strftime("%Y-%m-%d")
OUTPUT_FILE = os.path.join(CURRENT_DIR, f"global_hunger_index_sanity_checks_{DATE_TAG}.html")
NEW_DATA_FILE = os.path.join(GRAPHER_DIR, GHI_NAME + '.csv')


def create_comparison_dataframe(old, new):
    old_melt = old.melt(id_vars=['Country', 'Year'])
    old_melt['source'] = 'old'
    new_melt = new.melt(id_vars=['Country', 'Year'])
    new_melt['source'] = 'new'

    comparison = pd.concat([old_melt, new_melt], ignore_index=True)

    return comparison


def plot_country(comparison, country):
    # x_range = [comparison["Year"].min() - 1, comparison["Year"].max() + 1]
    # y_range = [comparison["value"].min() * 0.9, comparison["value"].max() * 1.1]
    x_range = [1992, 2021]
    y_range = [0, 100]
    plot_data = comparison.dropna(how='any').reset_index(drop=True)
    fig = px.line(plot_data[plot_data['Country'] == country], x='Year', y='value', color='source',
                  color_discrete_map={'old': 'red', 'new': 'blue'}, markers=True)
    fig.update_layout({'title': country}).\
        update_xaxes(showgrid=True, title="Year", autorange=False, range=x_range).\
        update_yaxes(showgrid=True, title="Global Hunger Index", autorange=False, range=y_range)
    return fig


def generate_figures_for_all_countries(comparison):
    figures = ""
    for country in tqdm(comparison['Country'].unique()):
        fig = plot_country(comparison, country=country)
        img = plotly.io.to_image(fig, scale=1.2)
        img_base64 = base64.b64encode(img).decode("utf8")
        figures += f"<br><img class='icon' src='data:image/png;base64,{img_base64}'>"

    return figures


def save_summary_to_html_file(summary, output_file):
    with open(output_file, "w") as output_file_:
        output_file_.write(summary)


def main():
    old = pd.read_csv(OLD_DATA_URL).rename(columns={'Entity': 'Country'})[[
        'Country', 'Year', 'Global Hunger Index (IFPRI (2016))']]
    new = pd.read_csv(NEW_DATA_FILE)
    comparison = create_comparison_dataframe(old=old, new=new)
    figures = generate_figures_for_all_countries(comparison=comparison)
    save_summary_to_html_file(summary=figures, output_file=OUTPUT_FILE)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Generate figures for all countries to visually inspect changes in the data.")
    args = parser.parse_args()

    main()
