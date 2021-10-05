# OWID standard importer

Imports a cleaned dataset and associated data sources, variables, and data points into the MySQL database.

Example usage:

```
from standard_importer import import_dataset
dataset_dir = "worldbank_wdi"
dataset_namespace = "worldbank_wdi@2021.05.25"
import_dataset.main(dataset_dir, dataset_namespace)
```

`import_dataset.main(...)` expects a set of CSV files to exist in `{DATASET_DIR}/output/` (e.g. `worldbank_wdi/output`): 

- `distinct_countries_standardized.csv`
- `datasets.csv`
- `sources.csv`
- `variables.csv`
- `datapoints/data_points_{VARIABLE_ID}.csv` (one `data_points_{VARIABLE_ID}.csv` file for each variable in `variables.csv`)

## Expected format of CSV files

Inside the dataset directory (e.g. `vdem`), data must be located in an `output` directory, with the following structure:

(see [worldbank_wdi/output](../worldbank_wdi/output) for an example)


### Entities file (`distinct_countries_standardized.csv`)

This file lists all entities present in the data, so that new entities can be created if necessary. Located in `output/distinct_countries_standardized.csv`:

* `name`: name of the entity.


### Datasets file (`datasets.csv`)

Located in `output/datasets.csv`:

* `id`: temporary dataset ID for loading process
* `name`: name of the Grapher dataset


### Sources file (`sources.csv`)

Located in `output/sources.csv`:

* `id`: temporary source ID for loading process
* `name`: name of the source
* `description`: JSON object with `dataPublishedBy` (string), `dataPublisherSource` (string), `link` (string), `retrievedDate` (string), `additionalInfo` (string)
* `dataset_id`: foreign key matching each source with a dataset ID


### Variables file (`variables.csv`)

Located in `output/variables.csv`:

* `dataset_id`: foreign key matching each variable with a dataset ID
* `source_id`: foreign key matching each variable with a source ID
* `id`: temporary variable ID for loading process
* `name`: name of the variable
* `description`: long description of the variable
* `code`: original variable code used by the data source
* `unit`: unit of measurement
* `short_unit`: short unit of measurement, for chart axis display
* `timespan`: timespan covered by the variable
* `coverage`: type of geographical coverage
* `display`: JSON object that defines how the variable should be displayed
* `original_metadata`: JSON object representing original uncleaned metadata from the data source


### Datapoint files (`datapoints/data_points_{VARIABLE_ID}.csv`)

Located in `output/datapoints/datapoints_{VARIABLE_ID}.csv`:

* `{VARIABLE_ID}` in the file name is a foreign key matching values with a temporary variable ID in `variables.csv`
* `country`: location of the observation
* `year`: year of the observation
* `value`: value of the observation
