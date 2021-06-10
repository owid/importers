# Bulk dataset import for World Development Indicators

This folder contains all scripts required to execute a bulk dataset import + update of existing charts for the World Bank - World Development Indicators (WDI) dataset. 

Instructions for executing a bulk dataset import + update of existing charts for a new version of the World Bank - WDI dataset:

1. Update `__init__.py` with the appropriate `DATASET_VERSION`, `DATASET_RETRIEVED_DATE`, and other constants as needed.

2. Make any necessary changes to the scripts in this folder, which may require small modifications to work with the new dataset version. (e.g. you may need to change `download.py` to make sure the newest dataset version is being downloaded.)

3. Execute `python -m wordlbank_wdi.main`.
