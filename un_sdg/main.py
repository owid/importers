"""executes bulk dataset import + chart updates for the UN_SDGs dataset.
Usage:
    python -m un_sdg.main
"""
from un_sdg import DATASET_NAMESPACE, DATASET_DIR,  DATA_PATH, DATASET_VERSION, USER_ID

from un_sdg import (
    download, 
    clean,
    match_variables
)

from standard_importer import import_dataset

def main():
    download.main()
    clean.main()
    import_dataset.main(DATASET_DIR, DATA_PATH, DATASET_VERSION, USER_ID)
    match_variables.main()

if __name__ == '__main__':
    main()