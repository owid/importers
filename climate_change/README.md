# Climate change impacts

Here is the code that generates the datasets used in the climate change impacts data explorer.
For the moment we do not have a fully automated pipeline to update the datasets.

To update the datasets:
1. To update the files in folders `ready` and `output`, execute, from the root directory of this repository:
```bash
source venv/bin/activate
export PYTHONPATH=${PWD}
python climate_change/src/main.py
```
2. Commit and push changes.
3. Manually upload the datasets inside `output` onto grapher, and replace previous versions.
