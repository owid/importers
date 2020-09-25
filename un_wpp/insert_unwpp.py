import datetime
import os
from glob import glob
import sys
sys.path.append("/home/owid/importers/importers")

import pandas as pd
from db import connection
from db_utils import DBUtils

with connection as cnx:
    db = DBUtils(cnx)
    
    # upsert datasets
    dataset_name_ids = {}
    for f in os.listdir("sources/"):
        if f == ".DS_Store":
            continue
        data = pd.read_excel("sources/"+f)
        val = data[data.columns[0]][8]
        index_to_remove = val.find(":")
        res = "UN WPP - " + val[index_to_remove+2:]
        dataset_id = db.upsert_dataset(name=res, namespace="unwpp", user_id=15)
        dataset_name_ids[res] = dataset_id
        print(f, dataset_id)
        
    # upsert sources
    dataset_to_source_ids = {}
    source_name = "United Nations – Population Division (2019 Revision)"
    for additional_info, dataset_name in datasets_dict.items():
        description = {}
        description["dataPublishedBy"] = "United Nations, Department of Economic and Social Affairs, Population Division (2019). World Population Prospects: The 2019 Revision, DVD Edition."
        description["dataPublisherSource"] = None
        description["link"] = "https://population.un.org/wpp2019/Download/Standard/Interpolated/"
        description["retrievedDate"] = datetime.datetime.now().strftime("%d-%b-%Y")
        description["additionalInfo"] = additional_info
        
        source_id = db.upsert_source(name=source_name, description=json.dumps(description), dataset_id=dataset_name_ids[dataset_name])
        dataset_to_source_ids[dataset_name_ids[dataset_name]] = source_id
        print(dataset_name, source_id)
    
    # entities = pd.read_csv("distinct_countries_standardized.csv")
    # datasets = pd.read_csv("datasets.csv")
    # sources = pd.read_csv("sources.csv")
    # variables = pd.read_csv("variables.csv")
    
    # new_entities = entities[entities["db_entity_id"].isnull()]
    # for _, entity in new_entities.iterrows():
    #     entity_id = entity.name
    #     entity_name = entity["name"]
    #     db_entity_id = db.get_or_create_entity(entity_name)
    #     entities.loc[entity_id, "db_entity_id"] = db_entity_id
    
    # # upsert datasets
    # dataset_name_ids = {}
    # for i, row in datasets.iterrows():
    #     dataset_id = db.upsert_dataset(name=row["name"], namespace="unwpp", user_id=15)
    #     dataset_name_ids[row["name"]] = dataset_id
        
    # # upsert sources
    # dataset_to_source_ids = {}
    # for i, row in sources.iterrows():

    #     dataset_name = datasets[datasets["id"] == row["dataset_id"]]["name"].values[0]
    #     source_id = db.upsert_source(name=row["name"], description=json.dumps(row["description"]), dataset_id=dataset_name_ids[dataset_name])

    #     dataset_to_source_ids[dataset_name] = source_id
        
    # # upsert variables
    # names_to_ids = {}
    # for i, row in variables.iterrows():
        
    #     dataset_name = datasets[datasets["id"] == row["dataset_id"]]["name"].values[0]
    #     dataset_id = dataset_name_ids[dataset_name]
    #     source_id = dataset_to_source_ids[dataset_name]<‡
        
    #     variable_id = db.upsert_variable(
    #                                     name=row["name"], 
    #                                     code=None, 
    #                                     unit=row["unit"], 
    #                                     short_unit=None, 
    #                                     source_id=source_id, 
    #                                     dataset_id=dataset_id, 
    #                                     description=None, 
    #                                     timespan=", 
    #                                     coverage=", 
    #                                     display={}
    #                                     )
    #     names_to_ids[row["name"]] = variable_id
        
    # # Inserting datapoints
    # datapoints_files = glob("datapoints/*.csv")
    # for x in datapoints_files: 
    #     # to get variable is
    #     v_id = int(x.split("_")[1].split(".")[0])
       
    #     # to get variable name
    #     variable_name = variables[variables["id"]==v_id]["name"].values[0]
       
    #     # to get variable id from db
    #     variable_id = names_to_ids[variable_name]
    #     data = pd.read_csv(x)

    #     for i, row in data.iterrows():
    #         entity_id = entities[entities["name"] == row["country"]]["db_entity_id"].values[0]

    #         year = row["year"]
    #         val = row["value"]

    #         db.upsert_one("""
    #             INSERT INTO data_values
    #                 (value, year, entityId, variableId)
    #             VALUES
    #                 (%s, %s, %s, %s)
    #             ON DUPLICATE KEY UPDATE
    #                 value = VALUES(value),
    #                 year = VALUES(year),
    #                 entityId = VALUES(entityId),
    #                 variableId = VALUES(variableId)
    #         """, [val, int(year), str(int(entity_id)), str(variable_id)])
    # 
