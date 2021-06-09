# Importers

_Bulk import scripts for ingesting large external datasets into OWID's master dataset._

## Overview

OWID keeps a master Postgres database of all known data sets. Whilst some are manually added by researchers using the [grapher](https://github.com/owid/owid-grapher) admin interface, the bulk of the data comes from importing large external datasets, which is the focus of this repository. Datasets are often updated versions of older datasets; new versions do not overwrite old data, but are enabled with their own versioned `namespace`. This codebase also proposes which `grapher` charts should be updated to use data from a fresher dataset version.

## Rough convention

Each large dataset has its own folder `<dataset>/`, and inside that you can find:

- Scripts that transform it
- `input/`: an original(ish) copy
- `output/`: the transformed copy that will be uploaded
- `standardisation/`: any country name transformations that were needed

## Development

You should install Python 3.8+ and install required packages:

```
pip install -r requirements.txt
```

Historical data transformations might have used Jupyter notebooks, but all recent ones use Python scripts.

## Updating data

Depending on the dataset, it may get uploaded as new variables, or the existing variables might have additional values added. In both cases, existing charts do not necessarily get new data, but might need their JSON blob to be updated.

Ask the Data Team for more info!
