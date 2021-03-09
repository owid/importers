import sys

sys.path.append("..")

from db import connection
from db_utils import DBUtils
import requests
import pdb
import csv
import pandas as pd

VARIABLE_METADATA_SPREADSHEET_URL = "https://docs.google.com/spreadsheets/d/e/2PACX-1vRkfC-f9XCIjzyhv8yVAtbWZJD10XMjT15FCH9QAe0c9FLg1QeeKHGNlR5u07oaPSBgYAAv1WQtfY5f/pub?gid=0&single=true&output=csv"
DATASET_METADATA_SPREADSHEET_URL = "https://docs.google.com/spreadsheets/d/e/2PACX-1vTyfEzCh3_9jz0Rygxq-67CBRVkpr4Wfj1oMbxVQOwEKa8VTCSb1CbGinMjNEm-gJJ7ChudFjOHMplk/pub?gid=600801085&single=true&output=csv"
SOURCES_METADATA_SPREADSHEET_URL = "https://docs.google.com/spreadsheets/d/e/2PACX-1vQ0m_gu6qI5XgyIu3JcIOo1T_HAzVR-sUKvTEBWsppr7YpxFBiMCQatY7TOQaCjDB3JXZ_hom3tHOnm/pub?gid=935021276&single=true&output=csv"
USER_ID = 44


# TODO: VALIDATE GOOGLE SHEET INPUT


def get_mega_csv():
    return pd.read_csv("output/mega.csv")


def map_of_entity_name_to_DB_entity_name(dbUtil):
    entities = pd.read_csv(
        "entities/standardized_entities.csv",
    )

    entities["code"] = entities["Our World In Data Name"].map(
        dbUtil.get_or_create_entity
    )

    return {row["Country"]: row["code"] for _, row in entities.iterrows()}


def main():
    variable_metadata = pd.read_csv(VARIABLE_METADATA_SPREADSHEET_URL).fillna("")
    dataset_metadata = pd.read_csv(DATASET_METADATA_SPREADSHEET_URL).iloc[0].fillna("")
    sources_metadata = pd.read_csv(SOURCES_METADATA_SPREADSHEET_URL).iloc[0].fillna("")

    with connection.cursor() as c:
        db = DBUtils(c)
        entity_name_map = map_of_entity_name_to_DB_entity_name(db)

        print(f"Inserting dataset: {dataset_metadata.Name}")
        db_dataset_id = db.upsert_dataset(
            name=dataset_metadata.Name,
            description=dataset_metadata.Description,
            namespace=dataset_metadata.Namespace,
            user_id=USER_ID,
        )

        print(f"Inserting source: {sources_metadata.Name}")
        db_source_id = db.upsert_source(
            name=sources_metadata.Name,
            description=sources_metadata.Description or "{}",
            dataset_id=db_dataset_id,
        )

        data_df = get_mega_csv()

        pdb.set_trace()
        for variable in variable_metadata:
            # insert row in variables table
            print("Inserting variable: %s" % variable["name"])
            db_variable_id = db.upsert_variable(
                name=variable["name"],
                code=variable["code"],
                unit=variable["unit"],
                description=variable["description"],
                short_unit=None,
                source_id=db_source_id,
                dataset_id=db_dataset_id,
            )


if __name__ == "__main__":
    main()