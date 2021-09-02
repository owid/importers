# Bulk dataset import and chart update for BP Statistical Review of World Energy

This folder contains all scripts required to execute a bulk dataset import + update of existing charts for the BP Statistical Review of World Energy dataset. 

Instructions for executing a bulk dataset import + update of existing charts for a new version of the dataset:

1. Update `__init__.py` with the appropriate `DATASET_VERSION`, `DATASET_RETRIEVED_DATE`, and other constants as needed.

2. Make any necessary changes to the scripts and `config/` files in this folder, which may require small modifications to work with the new dataset version. (e.g. you may need to change `download.py` to make sure the newest dataset version is being downloaded.)

3. Update `standardized_entity_names.csv` by executing:

```python
import os
import pandas as pd
from bp_statreview import OUTPATH
from bp_statreview.entities import get_unstandardized_entity_names
pd.DataFrame(get_unstandardized_entity_names(), columns=["Country"]).to_csv(os.path.join(OUTPATH, "distinct_countries_unstandardized.csv"), index=False)
```

Then upload this `distinct_countries_unstandardized.csv` file to the [OWID country standardizer tool](https://owid.cloud/admin/standardize). After using the tool, download the csv file of standardized entity names and save it to `config/standardized_entity_names.csv`.

3. Execute `python -m bp_statreview.main`.

## Other useful things to know

* The `clean.py` script constructs variables from both the csv and xlsx input files, in addition to constructing variables through unit conversion. The reason for this reliance on both the csv and xlsx files is that the csv input file does not contain all of the variables available in the xlsx file. But the xlsx file is much more cumbersome to clean, so we opt for variables in the csv file when available.
* In the `config/standardized_entity_names.csv` file, we override the country standardizer tool's assignment of "USSR" (unstandardized) -> "Russia" (standardized) by instead assigning "USSR"->"USSR". This is because "Russia" already exists in the list of standardized entity names ("Russian Federation"->"Russia"), which would lead to duplicate variable-country-year observations if both "USSR" and "Russian Federation" -> "Russia".
