import datetime
import os
import json
from glob import glob
import sys
sys.path.append("/home/owid/importers/importers")

import pandas as pd
from db import connection
from db_utils import DBUtils

with connection.cursor() as cursor:
    db = DBUtils(cursor)

    # Upsert entities
    entities = pd.read_csv("output/distinct_countries_standardized.csv")
    for entity_name in entities.name:
        db_entity_id = db.get_or_create_entity(entity_name)
        entities.loc[entities.name == entity_name, "db_entity_id"] = db_entity_id
    print(entities)

    # Upsert datasets
    datasets = pd.read_csv("output/datasets.csv")
    for i, dataset_row in datasets.iterrows():
        db_dataset_id = db.upsert_dataset(name=dataset_row["name"], namespace="unwpp", user_id=46)
        datasets.at[i, "db_dataset_id"] = db_dataset_id
    print(datasets)

    # Upsert sources
    sources = pd.read_csv("output/sources.csv")
    sources = pd.merge(sources, datasets, left_on="dataset_id", right_on="id")
    for i, source_row in sources.iterrows():
        db_source_id = db.upsert_source(
            name=source_row.name_x,
            description=json.dumps(source_row.description),
            dataset_id=source_row.db_dataset_id
        )
        sources.at[i, "db_source_id"] = db_source_id
    print(sources)

    # Upsert variables
    variables = pd.read_csv("output/variables.csv")
    print(variables.shape)
    variables = pd.merge(variables, sources, left_on="dataset_id", right_on="dataset_id")
    print(variables.shape)
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
    print(variables)

    # Upserting datapoints
    datapoints_files = glob("output/datapoints/datapoints_*.csv")
    for datapoint_file in datapoints_files: 
        import pdb; pdb.set_trace()

        # to get variable id
        v_id = int(datapoint_file.split("_")[1].split(".")[0])

        # to get variable name
        variable_name = variables[variables["id"]==v_id]["name"].values[0]

        # to get variable id from db
        variable_id = names_to_ids[variable_name]
        data = pd.read_csv(datapoint_file)

        for i, row in data.iterrows():
            entity_id = entities[entities["name"] == row["country"]]["db_entity_id"].values[0]

            year = row["year"]
            val = row["value"]

            db.upsert_one("""
                INSERT INTO data_values
                    (value, year, entityId, variableId)
                VALUES
                    (%s, %s, %s, %s)
                ON DUPLICATE KEY UPDATE
                    value = VALUES(value),
                    year = VALUES(year),
                    entityId = VALUES(entityId),
                    variableId = VALUES(variableId)
            """, [val, int(year), str(int(entity_id)), str(variable_id)])
