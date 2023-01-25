> **Warning**
> This repository is deprecated. See https://docs.owid.io/projects/etl/en/latest/

# Importers

_Bulk import scripts for ingesting large external datasets into OWID's master dataset._

## Overview

OWID keeps a master MySQL database of the charts that appear on our website, as well as the datasets used to create these charts (see [owid-grapher](https://github.com/owid/owid-grapher)). 

The `importers` repository aids in the maintenance of this database by:

**1. Importing datasets:** The folders in this repository contain scripts for uploading external datasets to the database at regular intervals, such as the World Bank World Development Indicators. Only some of the datasets in our database are updated in this way. Most are instead manually added to the database by OWID researchers using the [grapher](https://github.com/owid/owid-grapher) admin interface. 

**2. Suggesting chart revisions:** Once a new version of a dataset has been uploaded to the database, the next task is to update the corresponding OWID charts to display the newly available data in place of the old data. Because dataset imports create a _new_ version of an existing dataset rather than overwriting an old version of the same dataset, the relevant charts must be amended to display the new data. The scripts in this repository _suggest_ these chart revisions, which are then manually approved or rejected by an OWID researcher using the [grapher](https://github.com/owid/owid-grapher) admin interface.

## Development

1.  install Python 3.8+ and required packages:

```
pip install -r requirements.txt
```

2. Copy `.env.example` to `.env` and change any variables as needed. If you are unsure what to change, ask a member of the OWID data team.

```
cp .env.example .env
```

3. Follow the [setup instructions in the owid-grapher repository](https://github.com/owid/owid-grapher#initial-development-setup) to initialize a local version of the OWID MySQL database.

> Note: After following the setup instructions, you must initialize the `suggested_chart_revisions` MySQL table by switching to the [feature/admin-suggested-chart-revision-approver](https://github.com/owid/owid-grapher/tree/feature/admin-suggested-chart-revision-approver) branch of the owid-grapher repository and running `yarn buildTsc && yarn typeorm migration:run`.

### Folder structure

Each dataset has its own folder `{institution}_{dataset}/` (e.g. `worldbank_wdi/` for the World Bank World Development Indicators), containing all code and configuration files required to execute the dataset import and suggest the chart revisions.

Typical folder structure:

```
__init__.py # used for storing dataset constants (e.g. {DATASET_NAME}, {DATASET_VERSION})
main.py     # executes all 6 steps in sequence
...         # helper scripts (e.g. `download.py`, `clean.py`)
input/      # original copy of the dataset
output/     # the cleaned data to be imported + other *generated* files for steps 1-6
config/     # *manually* constructed files for steps 1-6.
```

See [worldbank_wdi/](worldbank_wdi) for a recent example to follow.

## Conventions to follow

Each `{institution}_{dataset}/` folder executes the same 6 steps to import a dataset and suggest chart revisions:

1. Download the data.
   - Example: [worldbank_wdi/download.py](worldbank_wdi/download.py).

2. Specify which variables in the dataset are to be cleaned and imported into the database.
   - This information is typically stored in a `variables_to_clean.json` file. Example: [worldbank_wdi/output/variables_to_clean.json](worldbank_wdi/output/variables_to_clean.json). 
   - For some datasets, it makes sense to generate `variables_to_clean.json` programmatically (as in [worldbank_wdi/init_variables_to_clean.py](worldbank_wdi/init_variables_to_clean.py)), in which case `variables_to_clean.json` should be stored in the `output/` sub-folder. For other datasets, it may make more sense for you to generate `variables_to_clean.json` manually, in which case it should be stored in the `config/` sub-folder.

3. Clean/transform/manipulate the selected variables prior to import.
   - This step involves the construction of metadata for each variable (name, description, ...), as well as any required sanity checks on data observations, the removal or correction of problematic data observations, data transformations (e.g. per capita), et cetera.
   - The cleaned variables must be saved in CSV files in preparation for import into MySQL. See [standard_importer/README.md](standard_importer/README.md) for the required CSV format.
   - Example: [worldbank_wdi/clean.py](worldbank_wdi/clean.py).

> Note: This step generally requires usage of OWID's Country Standardizer Tool ([owid.cloud/admin/standardize](https://owid.cloud/admin/standardize), or [localhost:3030/admin/standardize](http://localhost:3030/admin/standardize) if you are running the grapher locally). This step requires you to upload a list of all unique country/entity names in the dataset to the country standardizer tool and then save the resulting downloaded csv file to `{DATASET_DIR}/config/standardized_entity_names.csv` (e.g. [worldbank_wdi/config/standardized_entity_names.csv](worldbank_wdi/config/standardized_entity_names.csv)) for use in your cleaning script to harmonize all entity names with OWID entity names.

4. Import the dataset into MySQL.
   - The [standard_importer/import_dataset.py](standard_importer/import_dataset.py) module exists to implement this step for any dataset, as long as you have saved the cleaned variables from step 3 in the required CSV format.

5. For each variable in the new dataset, specify which variable in the old dataset is its equivalent.
   - This information is typically stored in a `variable_replacements.json` file. Example: [worldbank_wdi/output/variable_replacements.json](worldbank_wdi/output/variable_replacements.json).
   - For some datasets, it makes sense to generate `variable_replacements.json` programmatically (as in [worldbank_wdi/match_variables.py](worldbank_wdi/match_variables.py)), in which case `variable_replacements.json` should be stored in the `output/` sub-folder. For other datasets, it may make more sense for you to generate `variable_replacements.json` manually, in which case it should be stored in the `config/` sub-folder.

6. Suggest the chart revisions using the `oldVariable -> newVariable` key-value pairs from `variable_replacements.json`.
   - The [standard_importer/chart_revision_suggester.py](standard_importer/chart_revision_suggester.py) module exists to implement this step for any dataset. 

Historical data transformations might have used Jupyter notebooks, but all recent ones use Python or R scripts.
