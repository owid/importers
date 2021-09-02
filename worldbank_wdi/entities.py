"""Retrieves unstandardized entity names from raw downloaded dataset.

Usage:

    >>> import os
    >>> import pandas as pd
    >>> from worldbank_wdi import OUTPATH
    >>> from worldbank_wdi.entities import get_unstandardized_entity_names
    >>> pd.DataFrame(get_unstandardized_entity_names(), columns=["Country"]).to_csv(os.path.join(OUTPATH, "distinct_countries_unstandardized.csv"), index=False)

"""

import os
import pandas as pd
from typing import List

from worldbank_wdi import INPATH


def get_unstandardized_entity_names() -> List[str]:
    df_entities = pd.read_csv(
        os.path.join(INPATH, "WDICountry.csv.zip"), compression="gzip"
    )
    df_entities.columns = df_entities.columns.str.lower().str.strip()
    assert not df_entities["country code"].duplicated().any()
    entities = (
        df_entities[["country code"]]
        .drop_duplicates()
        .dropna()
        .rename(columns={"Country code": "Country"})
        .squeeze()
        .sort_values()
        .tolist()
    )
    return entities
