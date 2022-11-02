# Climate change impacts

Here is the code that generates the datasets used in the climate change impacts data explorer.
For the moment we do not have a fully automated pipeline to update the datasets.

TODO: During last update, `nasa.arctic_sea_ice_extent()` failed.
On the next update, we may need to adapt the ingestion code, and change units of EPA antarctic ice extent from miles to km (and adapt explorer accordingly).

To update the datasets:
1. Update the files in folders `ready` and `output`, by executing, from the root directory of this repository:
```bash
source venv/bin/activate
export PYTHONPATH=${PWD}
python climate_change/src/main.py
```
2. Run sanity checks.
3. Commit and push changes.

The [Climate Change Impacts Data Explorer](https://ourworldindata.org/explorers/climate-change) will automatically be
updated after merging the changes to master, since the explorer points to the files inside the `output` folder.
