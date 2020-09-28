import re
import json
from glob import glob
import sys
sys.path.append("/home/owid/importers/importers")

from tqdm import tqdm
import pandas as pd
from db import connection
from db_utils import DBUtils

def main():

    with connection.cursor() as cursor:
        db = DBUtils(cursor)

        # Upsert entities
        print("---\nUpserting entities...")
        entities = pd.read_csv("output/distinct_countries_standardized.csv")
        for entity_name in entities.name:
            db_entity_id = db.get_or_create_entity(entity_name)
            entities.loc[entities.name == entity_name, "db_entity_id"] = db_entity_id
        print(f"Upserted {len(entities)} entities.")

        # Upsert datasets
        print("---\nUpserting datasets...")
        datasets = pd.read_csv("output/datasets.csv")
        for i, dataset_row in datasets.iterrows():
            db_dataset_id = db.upsert_dataset(
                name=dataset_row["name"],
                namespace="unwpp",
                user_id=46
            )
            datasets.at[i, "db_dataset_id"] = db_dataset_id
        print(f"Upserted {len(datasets)} datasets.")

        # Upsert sources
        print("---\nUpserting sources...")
        sources = pd.read_csv("output/sources.csv")
        sources = pd.merge(sources, datasets, left_on="dataset_id", right_on="id")
        for i, source_row in sources.iterrows():
            db_source_id = db.upsert_source(
                name=source_row.name_x,
                description=json.dumps(source_row.description),
                dataset_id=source_row.db_dataset_id
            )
            sources.at[i, "db_source_id"] = db_source_id
        print(f"Upserted {len(sources)} sources.")

        # Upsert variables
        print("---\nUpserting variables...")
        variables = pd.read_csv("output/variables.csv")
        variables = pd.merge(variables, sources, left_on="dataset_id", right_on="dataset_id")
        for i, variable_row in variables.iterrows():
            db_variable_id = db.upsert_variable(
                name=variable_row["name"],
                code=None,
                unit=variable_row["unit"],
                short_unit=None,
                source_id=variable_row["db_source_id"],
                dataset_id=variable_row["db_dataset_id"],
                description=None,
                timespan="",
                coverage="",
                display={}
            )
            variables.at[i, "db_variable_id"] = db_variable_id
        print(f"Upserted {len(variables)} variables.")

        # Upserting datapoints
        print("---\nUpserting datapoint files...")
        for datapoint_file in tqdm(glob("output/datapoints/datapoints_*.csv")):

            variable_id = int(re.search("\\d+", datapoint_file)[0])
            db_variable_id = variables.iloc[variable_id]["db_variable_id"]

            data = pd.read_csv(datapoint_file)
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
