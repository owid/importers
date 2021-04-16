"""executes bulk dataset import + chart updates for the World Bank World
Development Indicators dataset.

Usage:

    python -m worldbank_wdi.main
"""

from worldbank_wdi import DATASET_VERSION, DATASET_DIR

from worldbank_wdi import (
    download, 
    init_variables_to_upsert, 
    clean, 
    match_variables, 
    prepare_chart_updates
)
from standard_importer import import_dataset, upsert_suggested_chart_revisions

def main():
    download.main()
    init_variables_to_upsert.main()
    clean.main()
    import_dataset.main(DATASET_DIR, DATASET_VERSION)

    match_variables.main()
    prepare_chart_updates.main()
    upsert_suggested_chart_revisions.main(DATASET_DIR)

if __name__ == '__main__':
    main()
