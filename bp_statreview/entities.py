"""Retrieves unstandardized entity names from raw downloaded dataset.

Usage:
    >>> import os
    >>> import pandas as pd
    >>> from bp_statreview import OUTPATH
    >>> from bp_statreview.entities import get_unstandardized_entity_names
    >>> pd.DataFrame(get_unstandardized_entity_names(), columns=["Country"]).to_csv(os.path.join(OUTPATH, "distinct_countries_unstandardized.csv"), index=False)
"""

from typing import List
from bp_statreview.clean import clean_variables_and_datapoints


def get_unstandardized_entity_names() -> List[str]:
    _, df_data = clean_variables_and_datapoints(0, 0, std_entities=False)
    entities = sorted(df_data["Country"].unique().tolist())
    return entities
