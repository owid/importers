"""Executes bulk dataset import + chart updates for the IHME GBD Cause of death dataset.

The download step is fairly manual and needs some setting up before running. More details in download.py.

Usage:
    python -m ihme_gbd.ihme_gbd_child_mortality.main

    python -m ihme_gbd.ihme_gbd_child_mortality.main --skip_download --skip_clean --skip_import
"""
import click
import re
from ihme_gbd.ihme_gbd_child_mortality import (
    CONFIGPATH,
    DATASET_DIR,
    DATASET_NAMESPACE,
    FILTER_FIELDS,
    INPATH,
    NAMESPACE,
    OUTPATH,
    CLEAN_ALL_VARIABLES,
    UPDATE_EXISTING_DATA_VERSION,
)

from ihme_gbd.ihme_gbd_child_mortality import download, clean
from ihme_gbd import init_variables_to_clean, match_variables

from standard_importer import import_dataset
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
    help="Whether or not to import the data",
)
@click.option(
    "--match_vars/--skip_match",
    default=True,
    help="Whether or not to match the imported variables to existing variables in database",
)
def main(download_data, clean_data, import_data, match_vars):
    if download_data:
        download.main()
    if not CLEAN_ALL_VARIABLES:
        init_variables_to_clean.main(
            configpath=CONFIGPATH,
            inpath=INPATH,
            outpath=OUTPATH,
            namespace=NAMESPACE,
            fields=FILTER_FIELDS,
            update_existing_data=UPDATE_EXISTING_DATA_VERSION,
        )
    if clean_data:
        clean.main()
    if import_data:
        import_dataset.main(DATASET_DIR, DATASET_NAMESPACE)
    if match_vars:
        match_variables.main(outpath=OUTPATH, namespace=re.sub("ihme_", "", NAMESPACE))

    suggester = ChartRevisionSuggester(DATASET_DIR)
    suggester.suggest()


if __name__ == "__main__":
    main()
