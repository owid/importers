#!/usr/bin/env python
# coding: utf-8

# # Inserting the CSVs into the database

# ## Dependencies and globals

# In[8]:


import sys
sys.path.append("..")

import os
import pandas as pd
import numpy as np
from tqdm import tqdm
from glob import glob

from db import connection
from db_utils import DBUtils

# ID of user who imported the data
USER_ID = 29

# Dataset namespace
NAMESPACE = 'faostat_2020'

OUTPUT_PATH = 'output/'
STANDARDIZATION_PATH = 'standardization/'


# ## Load datasets, entities, variables & sources

# In[9]:


entities = pd.read_csv(
    os.path.join(STANDARDIZATION_PATH, './entities_standardized.csv'), 
    index_col='name'
)


# In[10]:


db_entity_id_by_name = { 
    row.name: int(row['db_entity_id']) for _, row in entities.iterrows() 
}


# In[41]:


# We replace nan's with "" (empty string) because every column is a string type, 
# there should be no numeric values
variables = pd.read_csv(os.path.join(OUTPUT_PATH, 'variables.csv')).fillna("")
datasets = pd.read_csv(os.path.join(OUTPUT_PATH, 'datasets.csv')).fillna("")
sources = pd.read_csv(os.path.join(OUTPUT_PATH, 'sources.csv')).fillna("")


# ## Integrity checks

# In[13]:


def print_err(*args, **kwargs):
    print(*args, file=sys.stderr, **kwargs)

def assert_unique(df, subset, message="Duplicate row found"):
    duplicate_mask = df.duplicated(subset=subset)
    if duplicate_mask.any() == True:
        print_err(message)
        print_err(df[duplicate_mask])
        return 1
    return 0


# In[14]:


print("Running integrity checks...")


# In[15]:


errors = 0

# Dataset IDs should be unique
errors += assert_unique(datasets, ['id'])

# Dataset names should be unique
errors += assert_unique(datasets, ['name'])

# Variable names should be unique
errors += assert_unique(variables, ['name'])

# Variable codes should be unique
errors += assert_unique(variables, ['code'])

# all entities should have a db_entity_id
if entities['db_entity_id'].isnull().any() == True:
    print_err("Entities are missing database ID")
    print_err(entities[entities['db_entity_id'].isnull()])
    errors += 1

# all entities in the data should exist in standardization file
for filepath in tqdm(sorted(glob(os.path.join(OUTPUT_PATH, 'datapoints/*.csv')))):
    df = pd.read_csv(filepath)
    # UNIQUE (entity, year) constraint
    errors += assert_unique(df, ['entity', 'year'], "Duplicate row in %s" % filepath)
    # No empty values
    if df['value'].isnull().any():
        print("%s contains empty values in 'value' column" % filepath)
        errors += 1
    # No non-numeric values
    if not df['value'].map(np.isreal).all():
        print("Non-numeric values in %s" % filepath)
        print(df[pd.to_numeric(df['value'], errors='coerce').isnull()])
        errors += 1

if errors != 0:
    print_err("\nIntegrity checks failed. There were %s errors.\n" % str(errors))
    sys.exit(1)
else:
    print("\nIntegrity checks passed.\n")


# ## Insert database rows

# In[1]:


with connection as c:
    db = DBUtils(c)
    
    for _, dataset in tqdm(datasets.iterrows(), total=len(datasets)):
        
        # Insert the dataset
        print("Inserting dataset: %s" % dataset['name'])
        db_dataset_id = db.upsert_dataset(
            name=dataset['name'],
            description=dataset['description'],
            namespace=NAMESPACE, 
            user_id=USER_ID)
        
        # Insert the source
        source = sources[sources['dataset_id'] == dataset.id].iloc[0]
        print("Inserting source: %s" % source['name'])
        db_source_id = db.upsert_source(
            name=source['name'], 
            description=source['description'], 
            dataset_id=db_dataset_id)
        
        # Insert variables associated with this dataset
        for j, variable in variables[variables.dataset_id == dataset['id']].iterrows():
            # insert row in variables table
            print("Inserting variable: %s" % variable['name'])
            db_variable_id = db.upsert_variable(
                name=variable['name'], 
                code=variable['code'], 
                unit=variable['unit'], 
                description=variable['description'],
                short_unit=None, 
                source_id=db_source_id, 
                dataset_id=db_dataset_id)

            # read datapoints
            data_values = pd.read_csv(os.path.join(OUTPUT_PATH, 'datapoints', '%s.csv' % variable.id))

            values = [(float(row['value']), int(row['year']), db_entity_id_by_name[row['entity']], db_variable_id)
                      for _, row in data_values.iterrows()]

            print("Inserting values...")
            db.upsert_many("""
                INSERT INTO 
                    data_values (value, year, entityId, variableId)
                VALUES 
                    (%s, %s, %s, %s)
                ON DUPLICATE KEY UPDATE
                    year = VALUES(year)
            """, values)
            
            # We have a dummy ON DUPLICATE handler that updates the year which is essentially 
            # a no update operation. We do this only to avoid a duplicate key error. It occurs 
            # when FAO uses an Item Group and Item with the same name. For example, 'Eggs' is 
            # both an Item Group and a standalone Item in: 
            # Commodity Balances - Livestock and Fish Primary Equivalent
            
            # This is not ideal because we could be masking other duplication issues, we should 
            # ideally have the differentiation between groups and itemsin the database, but this 
            # requires effort and time, both of which are currently in short supply.
            
            print("Inserted %d values for variable" % len(values))

print("All done. Phew!")


# ## SQL to delete all data

# ```sql
# DELETE data_values
# FROM   data_values
#        INNER JOIN variables
#                ON variables.id = data_values.variableid
#        INNER JOIN sources
#                ON sources.id = variables.sourceid
#        INNER JOIN datasets
#                ON datasets.id = sources.datasetid
# WHERE  datasets.namespace = 'faostat_2020';
# 
# DELETE variables
# FROM   variables
#        INNER JOIN sources
#                ON sources.id = variables.sourceid
#        INNER JOIN datasets
#                ON datasets.id = sources.datasetid
# WHERE  datasets.namespace = 'faostat_2020';
# 
# DELETE sources
# FROM   sources
#        INNER JOIN datasets
#                ON datasets.id = sources.datasetid
# WHERE  datasets.namespace = 'faostat_2020';
# 
# DELETE FROM datasets
# WHERE  namespace = 'faostat_2020';
# ```

# In[ ]:




