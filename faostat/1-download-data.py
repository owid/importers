#!/usr/bin/env python
# coding: utf-8

# In[2]:


import sys
sys.path.append("..")

import pandas as pd
import os
import json
import requests

from utils import write_file

METADATA_PATH = 'input/metadata/'
DATA_PATH = 'input/'


# ### Downloading the raw data

# In[3]:


get_ipython().system('curl -Lo $DATA_PATH/FAOSTAT.zip http://fenixservices.fao.org/faostat/static/bulkdownloads/FAOSTAT.zip')


# In[3]:


get_ipython().system('unzip $DATA_PATH/FAOSTAT.zip -d $DATA_PATH/FAOSTAT')


# In[ ]:


get_ipython().system('rm FAOSTAT.zip')


# ### Downloading the metadata

# In[4]:


def write_metadata_file(file_path, content):
    write_file(os.path.join(METADATA_PATH, file_path), content)


# In[5]:


def fetch_metadata(domain_code):
    url = "http://fenixservices.fao.org/faostat/api/v1/en/metadata/%s" % domain_code
    return requests.get(url).json()['data']


# In[6]:


groups = requests.get("http://fenixservices.fao.org/faostat/api/v1/en/groupsanddomains?section=download").json()['data']


# In[7]:


datasets = requests.get("http://fenixservices.fao.org/faostat/static/bulkdownloads/datasets_E.json").json()['Datasets']['Dataset']


# In[8]:


metadata_by_code = { group['domain_code']: fetch_metadata(group['domain_code']) for group in groups }


# In[9]:


write_metadata_file('groups.json', json.dumps(groups))
write_metadata_file('datasets.json', json.dumps(datasets))
write_metadata_file('metadata_by_code.json', json.dumps(metadata_by_code))


# In[ ]:




