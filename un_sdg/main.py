"""executes bulk dataset import + chart updates for the UN_SDGs dataset.
Usage:
    python -m un_sdg.main
"""
from un_sdg import DATASET_NAMESPACE, DATASET_DIR,  OUTPATH, DATASET_VERSION

from un_sdg import (
    download, 
    clean,
    match_variables
)

from standard_importer import import_dataset
from standard_importer.chart_revision_suggester import ChartRevisionSuggester

def main():
    download.main()
    clean.main()
    import_dataset.main(DATASET_DIR, DATA_PATH, DATASET_VERSION, USER_ID)
    match_variables.main()

    suggester = ChartRevisionSuggester('un_sdg')
    suggester.suggest()
    
if __name__ == '__main__':
    main()