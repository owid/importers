"""
Usage:
python -m wid.main
"""

from wid import DATASET_NAMESPACE, DATASET_DIR

from standard_importer import import_dataset

# from standard_importer.chart_revision_suggester import ChartRevisionSuggester


def main():
    # insert data into sql
    import_dataset.main(DATASET_DIR, DATASET_NAMESPACE)

    # constructs and upserts suggested chart revisions
    # suggester = ChartRevisionSuggester(DATASET_DIR)
    # suggester.suggest()


if __name__ == "__main__":
    main()
