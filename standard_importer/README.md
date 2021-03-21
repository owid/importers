# OWID standard importer

This is a standard importer to load data into the OWID database. Can be run with `python3 -m standard_importer.import_dataset` after setting the right values at the top of the script:

```
DATASET_DIR = "vdem"  # Directory in this Git repo where data is located
USER_ID = 46          # ID of OWID user loading the data
```


## Expected format

Inside the dataset directory (e.g. `vdem`), data must be located in an `output` directory, with the following structure:


### Entities file

This file lists all entities present in the data, so that new entities can be created if necessary. Located in `output/distinct_countries_standardized.csv`:

* `name`: name of the entity.


### Datasets file

Located in `output/datasets.csv`:

* `id`: temporary dataset ID for loading process
* `name`: name of the Grapher dataset


### Sources file

Located in `output/sources.csv`:

* `name`: name of the source
* `description`: JSON string with `dataPublishedBy` (string), `dataPublisherSource` (string), `link` (string), `retrievedDate` (string), `additionalInfo` (string)
* `dataset_id`: foreign key matching each source with a dataset ID


### Variables file

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


### Datapoint files

Located in `output/datapoints/datapoints_NNN.csv`:

* `NNN` in the file name is a foreign key matching values with a variable ID
* `country`: location of the observation
* `year`: year of the observation
* `value`: value of the observation
