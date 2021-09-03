"""Utilities for helping with World Bank WDI source name standardization.

Usage:

    >>> import os
    >>> import json
    >>> from worldbank_wdi import CONFIGPATH
    >>> from worldbank_wdi.sources import get_unstandardized_source_names
    >>> l = get_unstandardized_source_names()
    >>> with open(os.path.join(CONFIGPATH, "unstandardized_source_names.json"), "w") as f:
    >>>     json.dump([{"rawName": src, "name": "", "dataPublisherSource": ""} for src in l], f, indent=2)
"""

import os
from typing import List
import pandas as pd
from worldbank_wdi import INPATH


def get_unstandardized_source_names() -> List[str]:
    df = pd.read_csv(os.path.join(INPATH, "WDISeries.csv.zip"), compression="gzip")
    l = df["Source"].dropna().drop_duplicates().sort_values().tolist()
    return l
