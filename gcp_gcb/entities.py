"""Retrieves unstandardized entity names from raw downloaded dataset.

Usage:

    >>> import os
    >>> import pandas as pd
    >>> from gcp_gcb import OUTPATH
    >>> from gcp_gcb.entities import get_unstandardized_entity_names
    >>> pd.DataFrame(get_unstandardized_entity_names(), columns=["Country"]).to_csv(os.path.join(OUTPATH, "distinct_countries_unstandardized.csv"), index=False)

"""

from typing import List
from gcp_gcb.clean import DataValuesCleaner


def get_unstandardized_entity_names() -> List[str]:
    dvc = DataValuesCleaner()
    co2_prod = dvc.load_production_emissions(standardize_entities=False)
    co2_cons = dvc.load_consumption_emissions(standardize_entities=False)
    entities = sorted(
        set(
            co2_prod[dvc.entity_col].drop_duplicates().tolist()
            + co2_cons[dvc.entity_col].drop_duplicates().tolist()
        )
    )
    return entities
