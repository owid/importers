"""Retrieve all unstandardized entity names from the raw BP files.

"""

import argparse
import os
from typing import List

import pandas as pd

from bp_statreview import OUTPATH
from bp_statreview.clean import clean_variables_and_datapoints


def get_unstandardized_entity_names() -> List[str]:
    _, df_data = clean_variables_and_datapoints(0, 0, std_entities=False)
    entities = sorted(df_data["Country"].unique().tolist())
    return entities


def main():
    # Load all countries and regions in BP data.
    unstandardized_countries = pd.DataFrame(get_unstandardized_entity_names(), columns=["Country"])

    # Save all country/region names (as given in BP data) to file.
    unstandardized_countries.to_csv(os.path.join(OUTPATH, "distinct_countries_unstandardized.csv"), index=False)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    args = parser.parse_args()
    main()
