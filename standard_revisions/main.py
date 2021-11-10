"""
Triggers ad hoc suggested revisions in https://owid.cloud/admin/suggested-chart-revisions
based on config/variable_replacements.json
Usage:
python -m standard_revisions.main
"""

from standard_revisions import DATASET_DIR
from standard_importer.chart_revision_suggester import ChartRevisionSuggester


def main():
    suggester = ChartRevisionSuggester(DATASET_DIR)
    suggester.suggest()


if __name__ == "__main__":
    main()
