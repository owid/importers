"""executes bulk dataset import + chart updates for the UN_SDGs dataset.
Usage:
    python -m un_sdg.main

To run the code without downloading the data again: 
    python -m un_sdg.main --skip_download

To run the code without downloading or cleaning the data again: 
    python -m un_sdg.main --skip_download --skip_clean 

To just run the chart revision suggester again:

    python -m un_sdg.main --skip_download --skip_clean --skip_import --skip_match
"""
import os
import click

from un_sdg import CONFIGPATH, DATASET_DIR, DATASET_NAMESPACE

from un_sdg import download, clean

from standard_importer import import_dataset
from standard_revisions import match_variables_from_two_versions_of_a_dataset
from standard_importer.chart_revision_suggester import ChartRevisionSuggester


@click.command()
@click.option(
    "--download_data/--skip_download",
    default=True,
    help="Whether or not to download the data from the source as it often takes quite some time.",
)
@click.option(
    "--clean_data/--skip_clean",
    default=True,
    help="Whether or not to clean the data, useful for just upserting previously cleaned data",
)
@click.option(
    "--import_data/--skip_import",
    default=True,
    help="Whether or not to import the data to the database",
)
@click.option(
    "--match_vars/--skip_match",
    default=True,
    help="Whether or not to match the imported variables to existing variables in database",
)
@click.option(
    "--suggest_charts/--skip_suggest",
    default=True,
    help="Whether or not to suggest chart revisions",
)
def main(download_data, clean_data, import_data, match_vars, suggest_charts):
    if download_data:
        download.main()
    if clean_data:
        clean.main()
    if import_data:
        import_dataset.main(DATASET_DIR, DATASET_NAMESPACE)
    if match_vars:
        match_variables_from_two_versions_of_a_dataset.main(
            old_dataset_name="United Nations Sustainable Development Goals - United Nations (2021-10)",
            new_dataset_name="United Nations Sustainable Development Goals - United Nations (2022-02)",
            output_file=os.path.join(CONFIGPATH, "variable_replacements.json"),
        )
    if suggest_charts:
        suggester = ChartRevisionSuggester(DATASET_DIR)
        suggester.suggest()


if __name__ == "__main__":
    main()
