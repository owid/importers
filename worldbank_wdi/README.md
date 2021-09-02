# Bulk dataset import for World Development Indicators

This folder contains all scripts required to execute a bulk dataset import + update of existing charts for the World Bank - World Development Indicators (WDI) dataset. 

Instructions for executing a bulk dataset import + update of existing charts for a new version of the World Bank - WDI dataset:

1. Update `__init__.py` with the appropriate `DATASET_VERSION`, `DATASET_RETRIEVED_DATE`, and other constants as needed.

2. Make any necessary changes to the scripts in this folder, which may require small modifications to work with the new dataset version. (e.g. you may need to change `download.py` to make sure the newest dataset version is being downloaded.)

3. Update standardized_entity_names.csv by executing:

```python
import os
import pandas as pd
from worldbank_wdi import OUTPATH
from worldbank_wdi.entities import get_unstandardized_entity_names
pd.DataFrame(get_unstandardized_entity_names(), columns=["Country"]).to_csv(os.path.join(OUTPATH, "distinct_countries_unstandardized.csv"), index=False)
```

Then upload this distinct_countries_unstandardized.csv file to the [OWID country standardizer tool](https://owid.cloud/admin/standardize). In the country standardizer tool, change "Input Format" field to "ISO 3166-1 ALPHA-3 CODE" and change "Output Format" field to "Our World In Data Name". After using the tool, download the csv file of standardized entity names and save it to `config/standardized_entity_names.csv`.

4. Execute `python -m wordlbank_wdi.main`.
