#!/usr/bin/env python
# coding: utf-8

# # Inserting the CSVs into the database

# ## Dependencies and globals

# In[1]:


import sys
sys.path.append("..")

import os
import pandas as pd
from tqdm import tqdm

from db import connection
from db_utils import DBUtils

# ID of user who imported the data
USER_ID = 29

# Dataset namespace
NAMESPACE = 'faostat_2020'

OUTPUT_PATH = 'output/'
STANDARDIZATION_PATH = 'standardization/'


# ## Load entities

# In[2]:


entities = pd.read_csv(
    os.path.join(STANDARDIZATION_PATH, './entities_standardized.csv'), 
    index_col='name'
)


# In[3]:


db_entity_id_by_name = { 
    row.name: int(row['db_entity_id']) for _, row in entities.iterrows() 
}


# In[4]:


variables = pd.read_csv(os.path.join(OUTPUT_PATH, 'variables.csv'))
datasets = pd.read_csv(os.path.join(OUTPUT_PATH, 'datasets.csv'))
sources = pd.read_csv(os.path.join(OUTPUT_PATH, 'sources.csv'))


# In[5]:


datasets


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




