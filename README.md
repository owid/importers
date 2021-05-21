# Importers

_Bulk import scripts for getting large datasets into grapher._

## Overview

Each large dataset has its own folder, which includes scripts to pull the data, transform it and upload it as a new version.

Historical data transformations might have used Jupyter notebooks, but all recent ones use Python scripts.

## Rough convention

Each major dataset has its own folder `dataset/`, and inside that you can find:

- Scripts that transform it
- `input/`: an original(ish) copy
- `output/`: the transformed copy that will be uploaded
- `standardisation/`: any country name transformations that were needed

## Development

You should install Python 3.8+ and install required packages:

```
pip install -r requirements.txt
```

## Updating data

Depending on the dataset, it may get uploaded as new variables, or the existing variables might have additional values added. In both cases, existing charts do not necessarily get new data, but might need their JSON blob to be updated.

Ask the Data Team for more info!
