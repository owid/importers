"""executes bulk dataset import + chart updates for the WHO GHE dataset.
Usage:
    python -m who_ghe.main

To run the code without downloading the data again: 
    python -m who_ghe.main --skip_download

To run the code without downloading or cleaning the data again: 
    python -m who_ghe.main --skip_download --skip_clean
"""
import click

from who_ghe import DATASET_DIR, DATASET_NAMESPACE

from who_ghe import download, clean

from standard_importer import import_dataset


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
def main(download_data, clean_data):
    if download_data:
        download.main()
    if clean_data:
        clean.main()
    import_dataset.main(DATASET_DIR, DATASET_NAMESPACE)


if __name__ == "__main__":
    main()
