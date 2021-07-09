"""executes bulk dataset import + chart updates for the BP statistical review
or world energy dataset.

Usage:

    python -m bp_statreview.main

"""

from bp_statreview import (
    DATASET_NAMESPACE,
    DATASET_DIR,
    download,
    clean,
    match_variables,
)
from standard_importer import import_dataset
from standard_importer.chart_revision_suggester import ChartRevisionSuggester


def main():
    # insert data into sql
    download.main()
    clean.main()
    import_dataset.main(DATASET_DIR, DATASET_NAMESPACE)

    # constructs and upserts suggested chart revisions
    match_variables.main()
    suggester = ChartRevisionSuggester(DATASET_DIR)
    suggester.suggest()


if __name__ == "__main__":
    main()
