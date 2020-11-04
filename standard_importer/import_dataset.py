import re
import json
from glob import glob
import sys
import os

from tqdm import tqdm
import pandas as pd

sys.path.append("/mnt/importers/scripts/importers")
from db import connection
from db_utils import DBUtils


DATASET_DIR = "vdem"
DATASET_VERSION = 10
USER_ID = 46

CURRENT_DIR = os.path.dirname(__file__)
DATA_PATH = os.path.join(CURRENT_DIR, f"../{DATASET_DIR}/output/")


def main():

    with connection.cursor() as cursor:
        db = DBUtils(cursor)


        # Upsert entities
        print("---\nUpserting entities...")
        entities = pd.read_csv(os.path.join(DATA_PATH, "distinct_countries_standardized.csv"))
        for entity_name in tqdm(entities.name):
            db_entity_id = db.get_or_create_entity(entity_name)
            entities.loc[entities.name == entity_name, "db_entity_id"] = db_entity_id
        print(f"Upserted {len(entities)} entities.")


        # Upsert datasets
        print("---\nUpserting datasets...")
        datasets = pd.read_csv(os.path.join(DATA_PATH, "datasets.csv"))
        for i, dataset_row in tqdm(datasets.iterrows()):
            db_dataset_id = db.upsert_dataset(
                name=dataset_row["name"],
                namespace=f"{DATASET_DIR}{DATASET_VERSION}",
                user_id=USER_ID
            )
            datasets.at[i, "db_dataset_id"] = db_dataset_id
        print(f"Upserted {len(datasets)} datasets.")


        # Upsert sources
        print("---\nUpserting sources...")
        sources = pd.read_csv(os.path.join(DATA_PATH, "sources.csv"))
        sources = pd.merge(sources, datasets, left_on="dataset_id", right_on="id")
        for i, source_row in tqdm(sources.iterrows()):
            db_source_id = db.upsert_source(
                name=source_row.name_x,
                description=json.dumps(eval(source_row.description)),
                dataset_id=source_row.db_dataset_id
            )
            sources.at[i, "db_source_id"] = db_source_id
        print(f"Upserted {len(sources)} sources.")


        # Upsert variables
        print("---\nUpserting variables...")
        variables = pd.read_csv(os.path.join(DATA_PATH, "variables.csv"))
        variables = pd.merge(variables, sources, left_on="dataset_id", right_on="dataset_id")
        for i, variable_row in tqdm(variables.iterrows()):
            db_variable_id = db.upsert_variable(
                name=variable_row["name"],
                code=None,
                unit=variable_row["unit"],
                short_unit=None,
                source_id=variable_row["db_source_id"],
                dataset_id=variable_row["db_dataset_id"],
                description=variables["notes"],
                timespan="",
                coverage="",
                display={}
            )
            variables.at[i, "db_variable_id"] = db_variable_id
        print(f"Upserted {len(variables)} variables.")


        # Upserting datapoints
        print("---\nUpserting datapoints...")
        datapoint_files = glob("output/datapoints/datapoints_*.csv")
        for datapoint_file in tqdm(datapoint_files):

            variable_id = int(re.search("\\d+", datapoint_file)[0])
            db_variable_id = variables.iloc[variable_id]["db_variable_id"]

            data = pd.read_csv(os.path.join(DATA_PATH, d)
            data = pd.merge(data, entities, left_on="country", right_on="name")

            data_tuples = zip(
                data["value"],
                data["year"].astype(int),
                data["db_entity_id"].astype(int),
                [int(db_variable_id)] * len(data)
            )

            query = f"""
                INSERT INTO data_values
                    (value, year, entityId, variableId)
                VALUES (%s, %s, %s, %s)
                ON DUPLICATE KEY UPDATE
                    value = VALUES(value),
                    year = VALUES(year),
                    entityId = VALUES(entityId),
                    variableId = VALUES(variableId)
            """

            db.upsert_many(query, data_tuples)
        print(f"Upserted {len(datapoint_files)} datapoint files.")


if __name__ == "__main__":
    main()
