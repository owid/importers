# Bulk dataset import and chart update for BP Statistical Review of World Energy

This folder contains all scripts required to execute a bulk dataset import + update of existing charts for the BP Statistical Review of World Energy dataset. 

Instructions for executing a bulk dataset import + update of existing charts for a new version of the dataset:

0. Go to the `importers`' root folder and activate the virtual environment (which should have been previously created).
```
source venv/activate/bin
export PYTHONPATH=${PWD}
```
1. From the same root folder, download the new data, by running:
```
python bp_statreview/download.py
```
If anything fails, you may have to manually edit some of the variables defined in `__init__.py` (maybe the download
links to the new BP files have changed).
2. Ensure all variables can be read and cleaned from the new files.
It is advisable to open the old and new `.xlsx` data files and visually inspect them to detect any changes.
Manually update the names of some variables in `variables_to_clean.json` and `variable_replacements_by_name.json` that
have a hardcoded year in the name (e.g. "Oil - Crude prices since 1861 (2021 $)").
Then run:
```
python bp_statreview/clean.py
```
This script is likely to fail because various things have changed in the new BP data files with respect to the old one.
The best solution (which is far from ideal) is to run the script in debug mode, and find out the source of the error.
Then, adjust the content of `variables_to_clean.json`, and, if that is not enough, adjust the code in `clean.py` and
`clean_excel.py`.
Once it runs until the end without errors, it may still raise a warning, saying that entities have not been
standardized, which is solved in the next steps.
3. Retrieve all country/region names from raw BP data, by running:
```
python bp_statreview/entities.py
```
This will create the file `distinct_countries_unstandardized.csv` in the folder of outputs.
The same warning as in the previous step will be raised (ignore it).
4. Harmonize country names.
To do this, you can either use the `harmonize` tool from `etl` (and adjust the format of the output file), or upload
the `distinct_countries_unstandardized.csv` file to the
[OWID country standardizer tool](https://owid.cloud/admin/standardize).
The resulting file must be saved as `config/standardized_entity_names.csv`, with columns `Country` and
`Our World In Data Name`.
5. Execute again the cleaning script, now that countries are harmonized, by running:
```
python bp_statreview/clean.py --countries_are_standardized
```
This time it should work without raising any warnings.
6. Update the data in the grapher database, by running:
```
python bp_statreview/write_to_grapher.py
```
7. Use the chart approval tool to compare old and new charts.
