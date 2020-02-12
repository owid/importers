#!/usr/bin/env python
# coding: utf-8

# # Extract the data into CSV files

# ## Dependencies & global variables

# In[1]:


import sys
sys.path.append("..")

import pandas as pd
import os
from tqdm import tqdm
import datetime
import json
from glob import glob
import zipfile

DATA_PATH = 'input/FAOSTAT'
DATA_UNZIP_PATH = 'input/tmp'
METADATA_PATH = 'input/metadata'
OUTPUT_PATH = 'output/'
STANDARDIZATION_PATH = 'standardization/'


# ## Load the metadata

# In[2]:


def read_json_file(file_path):
    with open(file_path, 'r') as json_file:
        return json.load(json_file)
    


# In[3]:


def get_name_from_path(file_path):
    return file_path.split("/")[-1]


# In[4]:


metadata_by_code = read_json_file(os.path.join(METADATA_PATH, 'metadata_by_code.json'))
datasets = read_json_file(os.path.join(METADATA_PATH, 'datasets.json'))

# In order to match the metadata against our bulk downloaded files, we need to 
for dataset in datasets:
    dataset['FileName'] = get_name_from_path(dataset['FileLocation'])


# ### Exclude the datasets we can't support

# We don't support these because they either include a **monthly breakdown** or because they have **too many breakdowns** and are not worth the effort.

# In[5]:


codes_to_exclude = ['PM', 'OA', 'CP', 'ET', 'PA', 'HS', 'TM', 'EA', 'FA', 'FT']
datasets = [d for d in datasets if d['DatasetCode'] not in codes_to_exclude]


# ### Ensure the metadata is correctly loaded

# In[6]:


pd.DataFrame(datasets)


# In[7]:


pd.DataFrame(metadata_by_code['QC'])


# ## Check whether we have all datasets

# In[8]:


files_we_have = set(map(get_name_from_path, glob(os.path.join(DATA_PATH, "*.zip"))))
files_in_metadata = set(map(lambda d: d['FileName'], datasets))


# In[9]:


files_in_metadata - files_we_have


# ## Metadata extraction utilities

# In[10]:


def find(fn, lst):
    for item in lst:
        if fn(item):
            return item

def get_metadata_field(code, label):
    try:
        return find(
            lambda row: row['metadata_label'] == label, 
            metadata_by_code[code]
        )['metadata_text']
    except:
        return ""


# ## **Extract datasets** & **sources**

# In[11]:


pd.DataFrame(datasets).head()
    


# In[12]:


owid_datasets = [] # pd.DataFrame(columns=['id', 'name', 'code', 'zip_file'])
owid_sources = [] # pd.DataFrame(columns=['id', 'name', 'description', 'dataset_id'])

for dataset in datasets:
    
    # DATASET
    
    name = dataset['DatasetName']
    code = dataset['DatasetCode']
    
    owid_datasets.append({
        'id': code, 
        'code': code,
        'name': name,
        'description': dataset['DatasetDescription'],
        'zip_filename': dataset['FileName']
    })
    
    # SOURCE
    
    source_desc = {}
    source_desc['dataPublishedBy'] = "Food and Agriculture Organization of the United Nations (FAO) (2019)"
    source_desc['dataPublisherSource'] = get_metadata_field(code, 'Source data')
    source_desc['link'] = "http://www.fao.org/faostat/en/?#data/"
    source_desc['retrievedDate'] = datetime.datetime.now().strftime("%d-%b-%Y")
    source_desc['additionalInfo'] = get_metadata_field(code, 'Statistical concepts and definitions')
    
    owid_sources.append({
        'id': code,
        'dataset_id': code,
        'name': name,
        'description': json.dumps(source_desc)
    })
    
    


# In[15]:


pd.DataFrame(owid_datasets)[['id', 'name', 'description']].to_csv(
    os.path.join(OUTPUT_PATH, 'datasets.csv'),
    index=False
)


# In[16]:


pd.DataFrame(owid_sources)[['id', 'name', 'description', 'dataset_id']].to_csv(
    os.path.join(OUTPUT_PATH, 'sources.csv'),
    index=False
)


# ## **Extract variables** & **datapoints** to CSVs

# The unique sets of headers extracted from the **used datasets only**, without the excluded.

# ### File loading utilities

# In[17]:


def load_dataset(code):
    dataset = find(lambda d: d['code'] == code, owid_datasets)
    zip_filepath = os.path.join(DATA_PATH, dataset['zip_filename'])
    csv_filepath = unzip_csv(zip_filepath)
    df = pd.read_csv(csv_filepath, encoding='latin-1', low_memory=False)
    os.remove(csv_filepath)
    return df

def unzip_csv(zip_filepath):
    """Returns the path of the unzipped CSV"""
    zip_ref = zipfile.ZipFile(zip_filepath, 'r')
    
    # Some ZIP files contain multiple files, usually one is a codebook for Flags
    # used in the dataset. We only want the CSVs that have 'All_Data' in their
    # filename.
    csv_filename = find(lambda name: 'All_Data' in name, zip_ref.namelist())
    
    zip_ref.extract(csv_filename, DATA_UNZIP_PATH)
    zip_ref.close()
    
    csv_filepath = os.path.join(DATA_UNZIP_PATH, csv_filename)
    
    return csv_filepath


# #### Extract the column types for each dataset (for diagnostic purposes)

# In[18]:


# all_dataset_columns = pd.DataFrame({
#     'code': [d['code'] for d in owid_datasets],
#     'columns': [list(load_dataset(d['code']).columns) for d in owid_datasets]
# })


# In[19]:


# all_dataset_columns.merge(pd.DataFrame(owid_datasets), on='code')


# #### Extract all unique dimension values (for diagnostic purposes)

# In[22]:


# for dataset in owid_datasets:
#     df = load_dataset(dataset['code'])
#     dim_cols = [col for col in df.columns if col not in ['Value', 'Note']]
#     pd.concat(
#         [pd.DataFrame({ col: df[col].unique() }) for col in dim_cols], 
#         axis=1
#     ).to_csv(os.path.join('dimensions', dataset['name'] + '.csv'))


# ### Year conversion utility

# In[20]:


def year_to_int(year):
    if isinstance(year, str) and '-' in year:
        start, end = year.split('-')
        return (int(end) + int(start)) // 2
    else:
        return year


# In[22]:


# pd.Series(['2012-2014', '2013-2015']).map(year_to_int)


# ### Column types parameters

# In[23]:


params_by_cols = {
    tuple(['Area Code', 'Area', 'Item Code', 'Item', 'Element Code', 'Element', 'Year Code', 'Year', 'Unit', 'Value', 'Flag']): {
        'entity': 'Area',
        'breakdown': ['Item', 'Element', 'Unit'],
        'code': ['Item Code', 'Element Code', 'Unit']
    },
    tuple(['Area Code', 'Area', 'Item Code', 'Item', 'Element Code', 'Element', 'Year Code', 'Year', 'Unit', 'Value', 'Flag', 'Note']): {
        'entity': 'Area',
        'breakdown': ['Item', 'Element', 'Unit'],
        'code': ['Item Code', 'Element Code', 'Unit']
    },
    tuple(['Country Code', 'Country', 'Item Code', 'Item', 'Element Code', 'Element', 'Year Code', 'Year', 'Unit', 'Value', 'Flag']): {
        'entity': 'Country',
        'breakdown': ['Item', 'Element', 'Unit'],
        'code': ['Item Code', 'Element Code', 'Unit']
    },
    tuple(['Area Code', 'Area', 'Item Code', 'Item', 'ISO Currency Code', 'Currency', 'Year Code', 'Year', 'Unit', 'Value', 'Flag', 'Note']): {
        'entity': 'Area',
        'breakdown': ['Item', 'Currency'],
        'code': ['Item Code', 'ISO Currency Code']
    },
    tuple(['Area Code', 'Area', 'Source Code', 'FAO Source', 'Indicator Code', 'Indicator', 'Year Code', 'Year', 'Unit', 'Value', 'Flag', 'Note']): {
        'entity': 'Area',
        'breakdown': ['Indicator', 'FAO Source', 'Unit'],
        'code': ['Indicator Code', 'Source Code', 'Unit']
    }
}


# #### Old column types (left just for reference)

# In[26]:


# column_types = [
#     # 11 columns
#     tuple(["Area Code", "Area", "Item Code", "Item", "ISO Currency Code", "Currency", "Year Code", "Year", "Unit", "Value", "Flag"]),
#     tuple(["CountryCode", "Country", "ItemCode", "Item", "ElementGroup", "ElementCode", "Element", "Year", "Unit", "Value", "Flag"]),
#     tuple(["Area Code", "Area", "Item Code", "Item", "Element Code", "Element", "Year Code", "Year", "Unit", "Value", "Flag"]),
#     tuple(["Country Code", "Country", "Item Code", "Item", "Element Code", "Element", "Year Code", "Year", "Unit", "Value", "Flag"]),
#     tuple(["Country Code", "Country", "Source Code", "Source", "Indicator Code", "Indicator", "Year Code", "Year", "Unit", "Value", "Flag"]),
#     tuple(["Recipient Country Code", "Recipient Country", "Item Code", "Item", "Donor Country Code", "Donor Country", "Year Code", "Year", "Unit", "Value", "Flag"]),
#     # 13 columns
#     tuple(["Reporter Country Code", "Reporter Countries", "Partner Country Code", "Partner Countries", "Item Code", "Item", "Element Code", "Element", "Year Code", "Year", "Unit", "Value", "Flag"]),
#     # 15 columns
#     tuple(["Donor Code", "Donor", "Recipient Country Code", "Recipient Country", "Item Code", "Item", "Element Code", "Element", "Purpose Code", "Purpose", "Year Code", "Year", "Unit", "Value", "Flag"]),
#     # for Indicators_from_Household_Surveys_E_All_Data_(Normalized)
#     tuple(['Survey Code','Survey','Breakdown Variable Code','Breakdown Variable','Breadown by Sex of the Household Head Code','Breadown by Sex of the Household Head','Indicator Code','Indicator','Measure Code','Measure','Unit','Value','Flag']),
#     tuple(['Area Code', 'Area','Source Code','FAO Source','Indicator Code','Indicator','Year Code','Year','Unit','Value','Flag','Note']),
#     # ConsumerPriceIndices_E_All_Data_(Normalized).zip
#     tuple(['Area Code','Area','Item Code','Item','Months Code','Months','Year Code','Year','Unit','Value','Flag','Note']),
#     # Exchange_rate_E_All_Data_(Normalized).zip
#     tuple(['Area Code','Area','Item Code','Item','ISO Currency Code','Currency','Year Code','Year','Unit','Value','Flag','Note']),
#     # Environment_Temperature_change_E_All_Data_(Normalized).zip
#     tuple(['Area Code','Area','Months Code','Months','Element Code','Element','Year Code','Year','Unit','Value','Flag'])
# ]


# ### Data extraction procedure

# Need to specify:
# 
# - Variable name prefix
# - Columns to break down by
# - Columns to use for constructing the `code` value
# - What to use as the entity field
# - That we should break down by `Unit` (all tables have it, it's just good if it's explicit)
# 
# What we know:
# 
# - Every dataset has a `Year` field (which we might need to transform to a single year from a range)

# In[24]:


def process_dataset(code):
    """Returns the variables inserted"""
    
    idx = 0
    
    dataset = find(lambda d: d['code'] == code, owid_datasets)
    df_dataset = load_dataset(code)
    cols = tuple(df_dataset.columns)
    
    if cols not in params_by_cols:
        print('ERROR: processing dataset %s: could not find %s in the list of column sets' % (code, str(cols)))
        return
    
    params = params_by_cols[cols]
    
    variables = []
    
    for key, df in df_dataset.groupby(params['breakdown']):
        idx += 1
        var_id = dataset['code'] + '-' + str(idx)
        name_prefix = dataset['name'].split(":", 1)[1]
        name_suffix = " - ".join(map(str, df[params['breakdown']].iloc[0].to_list()))
        name = " - ".join([name_prefix, name_suffix]).strip()
        code = " - ".join([dataset['code'], *map(str, df[params['code']].iloc[0].to_list())])
        unit = df.iloc[0].to_dict()['Unit'] if 'Unit' in df.columns else ""
        
        variables.append({
            'id': var_id,
            'name': name,
            'code': code,
            'unit': unit,
            'description': dataset['description'],
            'dataset_id': dataset['id'],
            'source_id': dataset['id'] # not a nice way to do it, but we know that source id == dataset id
        })
        
        datapoints = df.rename(columns={"Value": "value", params['entity']: "entity", "Year": "year"})
        
        # Some years are defined as 3-year ranges, e.g. "2013-2015"
        # We want to convert these to a single year
        datapoints['year'] = datapoints['year'].map(year_to_int)
        
        output_path = os.path.join(OUTPUT_PATH, 'datapoints/%s.csv' % str(var_id))
        datapoints[['entity', 'year', 'value']].to_csv(output_path, index=False)
    
    return variables


# ### Process the data files

# In[28]:


get_ipython().system('mkdir -p $OUTPUT_PATH/datapoints')


# In[ ]:


print("Extracting datapoints & variables...")


# In[30]:


variables = []

for dataset in tqdm(owid_datasets):
    variables += process_dataset(dataset['code'])

pd.DataFrame(variables)[['id', 'name', 'code', 'unit', 'description', 'dataset_id', 'source_id']]     .to_csv(os.path.join(OUTPUT_PATH, 'variables.csv'), index=False)


# ## Extract **entities** (for standardization)

# In[ ]:


print("Extracting unique entities...")


# In[2]:


entities = set()

for filepath in tqdm(glob(os.path.join(OUTPUT_PATH, 'datapoints/*.csv'))):
    df = pd.read_csv(filepath)
    entities |= set(df['entity'].tolist())

res = pd.DataFrame()
res['name'] = list(entities)
res.sort_values(by='name')     .to_csv(os.path.join(STANDARDIZATION_PATH, "entities.csv"), index=False)


# In[ ]:




