"""Write the new dataset to grapher database, and add charts to be reviewed using the chart approval tool.

"""

import argparse

from bp_statreview import (
    DATASET_NAMESPACE,
    DATASET_DIR,
    match_variables,
)
from standard_importer import import_dataset
from standard_importer.chart_revision_suggester import ChartRevisionSuggester


def main():
    # insert data into sql
    import_dataset.main(DATASET_DIR, DATASET_NAMESPACE)

    # constructs and upserts suggested chart revisions
    match_variables.main()
    suggester = ChartRevisionSuggester(DATASET_DIR)
    suggester.suggest()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    args = parser.parse_args()
    main()
