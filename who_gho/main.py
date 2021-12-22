"""executes bulk dataset import + chart updates for the UN_SDGs dataset.
Usage:
    python -m who_gho.main --skip_download --skip_clean --skip_import

"""
import click

from who_gho import DATASET_DIR, DATASET_NAMESPACE, OUTPATH

from who_gho import download, clean, match_variables

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
        match_variables.main(outpath=OUTPATH, namespace=("who_gho"))
    if suggest_charts:
        suggester = ChartRevisionSuggester(DATASET_DIR)
        suggester.suggest()


if __name__ == "__main__":
    main()
