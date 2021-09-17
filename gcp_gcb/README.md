# Bulk dataset import for the Global Carbon Project's Global Carbon Budget

## How to update

This folder contains all scripts required to execute a bulk dataset import + update of existing charts for the Global Carbon Project - Global Carbon Budget dataset. 

Instructions for executing a bulk dataset import + update of existing charts for a new version of the dataset:

1. Update `__init__.py` with the appropriate `DATASET_VERSION`, `DATASET_RETRIEVED_DATE`, and other constants as needed.

2. Make any necessary changes to the scripts and config files in this folder, which may require small modifications to work with the new dataset version. (e.g. you may need to change `download.py` to make sure the newest dataset version is being downloaded.)

3. Update `config/standardized_entity_names.csv` by executing:

```python
import os
import pandas as pd
from gcp_gcb import OUTPATH
from gcp_gcb.entities import get_unstandardized_entity_names
df = pd.DataFrame(get_unstandardized_entity_names(), columns=["Country"])
df.to_csv(os.path.join(OUTPATH, "distinct_countries_unstandardized.csv"), index=False)
```

Then upload this distinct_countries_unstandardized.csv file to the [OWID country standardizer tool](https://owid.cloud/admin/standardize). After using the tool, download the csv file of standardized entity names and save it to `config/standardized_entity_names.csv`.

4. Execute `python -m gcp_gcb.main`.

## Data sources

The Global Carbon Budget dataset (v2020) is available at https://folk.universitetetioslo.no/roberan/GCB2020.shtml (1750-2019) and https://doi.org/10.18160/GCP-2020 (1959-2019).
