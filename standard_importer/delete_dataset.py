"""Deletes a dataset from SQL, along with any associated sources,
variables, and data values.

This script is only intended for use after `import_dataset` for
debugging purposes and is only intended for use in a local/dev
environment (and should NOT be used in production or staging).

This script does not alter any of the chart SQL tables (e.g. `charts`,
`chart_dimensions`, or `chart_revisions`).

Usage::

    >>> from standard_importer import delete_dataset
    >>> dataset_id = {DATASET ID}
    >>> delete_dataset.main(dataset_id)
"""

import os
import logging

from db import get_connection
from db_utils import DBUtils

logging.basicConfig()
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)


def main(dataset_id: int) -> None:

    assert os.getenv("DB_HOST") == "localhost", (
        "This script is only intended for use in local/dev environments and "
        "should NOT be used in production or staging."
    )
    with get_connection().cursor() as cursor:
        db = DBUtils(cursor)
        # deletes data points
        logger.info("Deleting data values...")
        n_datavalues_deleted = db.cursor.execute(
            f"""
            DELETE FROM data_values
            WHERE variableId IN (
                SELECT id 
                FROM variables
                WHERE datasetId={dataset_id}
            )
        """
        )

        # deletes variables
        logger.info("Deleting variables...")
        n_variables_deleted = db.cursor.execute(
            f"""
            DELETE FROM variables
            WHERE datasetId={dataset_id}
        """
        )

        # deletes sources
        logger.info("Deleting sources...")
        n_sources_deleted = db.cursor.execute(
            f"""
            DELETE FROM sources
            WHERE datasetId={dataset_id}
        """
        )

        # deletes dataset
        logger.info("Deleting dataset...")
        n_datasets_deleted = db.cursor.execute(
            f"""
            DELETE FROM datasets
            WHERE id={dataset_id}
        """
        )

        logger.info(
            f"Deleted {n_datasets_deleted} datasets, "
            f"{n_sources_deleted} sources, "
            f"{n_variables_deleted} variables, "
            f"{n_datavalues_deleted} data values."
        )


if __name__ == "__main__":
    main()
