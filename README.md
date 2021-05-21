# Importers

_Bulk import scripts for getting large datasets into grapher._

## Overview

Each large dataset has its own folder, which includes scripts to pull the data, transform it and upload it as a new version.

## Development

Data transformations are written in Python and sometimes use Jupyter notebooks.

You should install Python 3.8+ and install required packages:

```
pip install -r requirements.txt
```

## Updating data

Depending on the dataset, it may get uploaded as new variables, or the existing variables might have additional values added. In both cases, existing charts do not necessarily get new data, but might need their JSON blob to be updated.

Ask the Data Team for more info!
