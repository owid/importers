"""executes bulk dataset import + chart updates for the UN_SDGs dataset.
The download step is quite manual at the moment so will not be included in main.py

Usage:
    python -m ihme_gbd_cause.main
"""
from ihme_gbd_cause import DATASET_DIR, DATASET_NAMESPACE

from ihme_gbd_cause import clean, match_variables

from standard_importer import import_dataset
from standard_importer.chart_revision_suggester import ChartRevisionSuggester


def main():
    clean.main()
    import_dataset.main(DATASET_DIR, DATASET_NAMESPACE)
    match_variables.main()

    suggester = ChartRevisionSuggester(DATASET_DIR)
    suggester.suggest()


if __name__ == "__main__":
    main()
