"""Retrieves unstandardized entity names from raw downloaded dataset.

Usage:
    >>> from bp_statreview import OUTPATH
    >>> from bp_statreview.entities import get_unstandardized_entity_names
    >>> pd.DataFrame(get_unstandardized_entity_names(), columns=["Country"]).to_csv(os.path.join(OUTPATH, "distinct_countries_unstandardized.csv"), index=False)
"""

import os
import pandas as pd
from typing import List

from bp_statreview import INPATH


def get_unstandardized_entity_names() -> List[str]:
    data = pd.read_csv(os.path.join(INPATH, "data_panel.csv"))
    return sorted(data["Country"].dropna().unique())
