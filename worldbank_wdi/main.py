# type: ignore
"""executes bulk dataset import + chart updates for the World Bank World
Development Indicators dataset.

Usage:

    python -m worldbank_wdi.main
"""

from ..worldbank_wdi import DATASET_NAMESPACE, DATASET_DIR

from ..worldbank_wdi import download, init_variables_to_clean, clean, match_variables
from ..standard_importer import import_dataset
from ..standard_importer.chart_revision_suggester import ChartRevisionSuggester


def main():
    # insert data into sql
    download.main()
    init_variables_to_clean.main()
    clean.main()
    import_dataset.main(DATASET_DIR, DATASET_NAMESPACE)

    # constructs and upserts suggested chart revisions
    match_variables.main()
    suggester = ChartRevisionSuggester(DATASET_DIR)
    suggester.suggest()


if __name__ == "__main__":
    main()
