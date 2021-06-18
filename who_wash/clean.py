import pandas as pd
import os
import shutil
import glob
from datetime import datetime
import json
import io
import itertools
import functools
import math
import pdfminer.high_level
import pdfminer.layout
import lxml.html
import numpy as np
import re

from pathlib import Path
from tqdm import tqdm
from typing import List, Tuple, Dict

from who_wash import (
    INFILE,
    ENTFILE,
    OUTPATH,
    DATASET_NAME,
    DATASET_AUTHORS,
    DATASET_VERSION
)

from who_wash.core import (
    create_short_unit,
    extract_datapoints,
    get_distinct_entities,
    clean_datasets,
    dimensions_description,
    attributes_description,
    create_short_unit,
    get_series_with_relevant_dimensions,
    generate_tables_for_indicator_and_series,
    str_to_float, 
    extract_description
)

"""
Now use the country standardiser tool to standardise $ENTFILE
1. Open the OWID Country Standardizer Tool
   (https://owid.cloud/admin/standardize);
2. Change the "Input Format" field to "Non-Standard Country Name";
3. Change the "Output Format" field to "Our World In Data Name"; 
4. In the "Choose CSV file" field, upload {outfpath};
5. For any country codes that do NOT get matched, enter a custom name on
   the webpage (in the "Or enter a Custom Name" table column);
    * NOTE: For this dataset, you will most likely need to enter custom
      names for regions/continents (e.g. "Arab World", "Lower middle
      income");
6. Click the "Download csv" button;
7. Replace {outfpath} with the downloaded CSV;
8. Rename the "Country" column to "country_code".
"""

def load_and_clean():
    # Load and clean the data 
    
    xls = pd.ExcelFile(INFILE)

    df_wat = pd.read_excel(xls, 'Water Data')
    filter_wat = df_wat.columns[df_wat.columns.str.startswith(('wat_', 'arc_wat_'))]
    wat_melt = pd.melt(df_wat, id_vars = ["name", "year", "pop_n"], value_vars = filter_wat)

    df_san = pd.read_excel(xls, 'Sanitation Data')
    filter_san = df_san.columns[df_san.columns.str.startswith(('san_', 'arc_san'))]
    san_melt = pd.melt(df_san, id_vars = ["name", "year", "pop_n"], value_vars = filter_san)

    df_hyg = pd.read_excel(xls, 'Hygiene Data')
    filter_hyg = df_hyg.columns[df_hyg.columns.str.startswith(('hyg_', 'arc_hyg'))]
    hyg_melt = pd.melt(df_hyg, id_vars = ["name", "year", "pop_n"], value_vars = filter_hyg)
 
    df = pd.concat([wat_melt, san_melt, hyg_melt])

    df = df[df['value'].notnull()]
    df[['name']].drop_duplicates() \
                                .dropna() \
                                .rename(columns={'name': 'Country'}) \
                                .to_csv(ENTFILE, index=False)
    # Make the datapoints folder
    Path(OUTPATH, 'datapoints').mkdir(parents=True, exist_ok=True)
    return df

### Datasets
def create_datasets():
    df_datasets = clean_datasets(DATASET_NAME, DATASET_AUTHORS, DATASET_VERSION)
    assert df_datasets.shape[0] == 1, f"Only expected one dataset in {os.path.join(OUTPATH, 'datasets.csv')}."
    df_datasets.to_csv(os.path.join(OUTPATH, 'datasets.csv'), index=False)
    return df_datasets


### Getting dimensions from variable name 

def create_dimensions(df):

        ds =  df['variable'].astype(str).str[0:7]

        conditions_data = [
            ds == "wat_bas",
            ds == "wat_lim", 
            ds == "wat_uni",
            ds == "wat_sur",
            ds == "arc_wat",
            ds == "wat_sm_",
            ds == "wat_pre",
            ds == "wat_ava",
            ds == "wat_qua",
            ds == "wat_pip",
            ds == "wat_npi",
            ds == "wat_imp",
            ds == "san_bas",
            ds == "san_lim",
            ds == "san_uni",
            ds == "san_od_",
            ds == "arc_san",
            ds == "arc_san",
            ds == "san_sm_",
            ds == "san_sdo",
            ds == "san_fst",
            ds == "san_sew",
            ds == "san_lat",
            ds == "san_sep",
            ds == "san_sew",
            ds == "san_imp",
            ds == "hyg_bas",
            ds == "hyg_lim",
            ds == "hyg_nfa"
        ]

        choices_data = np.repeat(["Drinking water", "Sanitation", "Hygiene"], [12,14,3])
        
        df['dataset'] = np.select(conditions_data, choices_data)

        location = df['variable'].astype(str).str[-1]

        conditions_loc = [
            location == "n",
            location == "r",
            location == "u"
        ]

        choices_loc = ["National", "Rural", "Urban"]

        df['location'] = np.select(conditions_loc, choices_loc)

        var =  df['variable'].str[:-2]
        
        conditions_var = [
            var == "wat_bas",
            var == "wat_lim", 
            var == "wat_unimp",
            var == "wat_sur",
            var == "arc_wat_bas",
            var == "wat_sm",
            var == "wat_premises",
            var == "wat_available",
            var == "wat_quality",
            var == "wat_pip",
            var == "wat_npip",
            var == "wat_imp",
            var == "san_bas",
            var == "san_lim",
            var == "san_unimp",
            var == "san_od",
            var == "arc_san_bas",
            var == "arc_san_od",
            var == "san_sm",
            var == "san_sdo_sm",
            var == "san_fst_sm",
            var == "san_sew_sm",
            var == "san_lat",
            var == "san_sep",
            var == "san_sew",
            var == "san_imp",
            var == "hyg_bas",
            var == "hyg_lim",
            var == "hyg_nfac"
        ]        

        choices_var = ["At least basic", "Limited (more than 30 mins)", "Unimproved", 
        "Surface water", "Annual rate of change in basic", "Safely managed", 
        "Accessible on premises", "Available when needed", 
        "Free from contamination", "Piped", "Non-piped", "Improved",
        "At least basic", "Limited (shared)", "Unimproved", "Open defecation",
        "Annual rate of change in basic", "Annual rate of change in open defecation",
        "Safely managed","Disposed in situ", "Emptied and treated", "Wastewater treated", 
        "Latrines and other", "Septic tanks", "Sewer connections", "Improved",
        "Basic", "Limited (without water or soap)", "No facility"]

        df['variable_desc'] = np.select(conditions_var, choices_var)

        return(df)




### Sources

def create_sources(df, df_datasets):
    df_sources = pd.DataFrame(columns=['id', 'name', 'description', 'dataset_id'])
    source_description_template = {
        'dataPublishedBy': DATASET_AUTHORS,
        'dataPublisherSource': None,
        'link': "https://washdata.org/data/",
        'retrievedDate': datetime.now().strftime("%d-%B-%y"),
        'additionalInfo': None
    }
    all_series = df[['dataset']].groupby(by=['dataset']).count().reset_index()
    source_description = source_description_template.copy()
    for i, row in tqdm(all_series.iterrows(), total=len(all_series)):
        df_sources = df_sources.append({
            'id': i,
            #'name': "%s (UN SDG, 2021)" % row['Source'],
            'name': "%s (WHO UNICEF, 2021)" % row['dataset'],
            'description': json.dumps(source_description),
            'dataset_id': df_datasets.iloc[0]['id'], # this may need to be more flexible! 
            'series_code': None
        }, ignore_index=True)
    df_sources.to_csv(os.path.join(OUTPATH, 'sources.csv'), index=False)

    
### Variables

def create_variables_datapoints(original_df):
    variable_idx = 0
    variables = pd.DataFrame(columns=['id', 'name', 'unit', 'dataset_id', 'source_id'])
    
    new_columns = [] 
    for k in original_df.columns:
        new_columns.append(re.sub(r"[\[\]]", '',k))

    original_df.columns = new_columns

    entity2owid_name = pd.read_csv(os.path.join(OUTPATH, 'standardized_entity_names.csv')) \
                              .set_index('country_code') \
                              .squeeze() \
                              .to_dict()

    series2source_id = pd.read_csv(os.path.join(OUTPATH, 'sources.csv'))\
                            .drop(['name','description', 'dataset_id'], 1)\
                            .set_index('series_code')\
                            .squeeze() \
                            .to_dict()
 
    unit_description = attributes_description()

    dim_description = dimensions_description()

    original_df['country'] = original_df['GeoAreaName'].apply(lambda x: entity2owid_name[x])
    original_df['Units_long'] = original_df['Units'].apply(lambda x: unit_description[x])

    DIMENSIONS = tuple(dim_description.id.unique())
    NON_DIMENSIONS = tuple([c for c in original_df.columns if c not in set(DIMENSIONS)])# not sure if units should be in here
    
    all_series = original_df[['Indicator', 'SeriesCode', 'SeriesDescription', 'Units_long']]   .groupby(by=['Indicator', 'SeriesCode', 'SeriesDescription', 'Units_long'])   .count()   .reset_index()
    all_series = create_short_unit(all_series)

    for i, row in tqdm(all_series.iterrows(), total=len(all_series)): 
        data_filtered =  pd.DataFrame(original_df[(original_df.Indicator == row['Indicator']) & (original_df.SeriesCode == row['SeriesCode'])])
        _, dimensions, dimension_members = get_series_with_relevant_dimensions(data_filtered, DIMENSIONS, NON_DIMENSIONS)
        print(i)
        if len(dimensions) == 0|(data_filtered[dimensions].isna().sum().sum() > 0):
            # no additional dimensions
            table = generate_tables_for_indicator_and_series(data_filtered, DIMENSIONS, NON_DIMENSIONS)
            variable = {
                'dataset_id': 0,
                'source_id': series2source_id[row['SeriesCode']],
                'id': variable_idx,
                'name': "%s - %s - %s" % (row['Indicator'], row['SeriesDescription'], row['SeriesCode']),
                'description': None,
                'code': row['SeriesCode'],
                'unit': row['Units_long'],
                'short_unit': row['short_unit'],
                'timespan': "%s - %s" % (int(np.min(data_filtered['TimePeriod'])), int(np.max(data_filtered['TimePeriod']))),
                'coverage': None,
                'display': None,
                'original_metadata': None
            }
            variables = variables.append(variable, ignore_index=True)
            extract_datapoints(table).to_csv(os.path.join(OUTPATH,'datapoints','datapoints_%d.csv' % variable_idx), index=False)
            variable_idx += 1
        else:
        # has additional dimensions
            for member_combination, table in generate_tables_for_indicator_and_series(data_filtered, DIMENSIONS, NON_DIMENSIONS).items():
                variable = {
                    'dataset_id': 0,
                    'source_id': series2source_id[row['SeriesCode']],
                    'id': variable_idx,
                    'name': "%s - %s - %s - %s" % (
                        row['Indicator'], 
                        row['SeriesDescription'], 
                        row['SeriesCode'],
                        ' - '.join(map(str, member_combination))),
                    'description': None,
                    'code': row['SeriesCode'],
                    'unit': row['Units_long'],
                    'short_unit': row['short_unit'],
                    'timespan': "%s - %s" % (int(np.min(data_filtered['TimePeriod'])), int(np.max(data_filtered['TimePeriod']))),
                    'coverage': None,
                    'display': None,
                    'original_metadata': None  
                }
                print(member_combination)
                variables = variables.append(variable, ignore_index=True)
                extract_datapoints(table).to_csv(os.path.join(OUTPATH,'datapoints','datapoints_%d.csv' % variable_idx), index=False)
                variable_idx += 1
                print(table)
    variables.to_csv(os.path.join(OUTPATH,'variables.csv'), index=False)

def create_distinct_entities(): 
    df_distinct_entities = pd.DataFrame(get_distinct_entities(), columns=['name']) # Goes through each datapoints to get the distinct entities
    df_distinct_entities.to_csv(os.path.join(OUTPATH, 'distinct_countries_standardized.csv'), index=False)




# Max length of source name.
MAX_SOURCE_NAME_LEN = 256


def main():
    df = load_and_clean() 
    df = create_dimensions(df)
    df_datasets = create_datasets()
    create_sources(df, df_datasets)
    create_variables_datapoints(df) #numexpr can't be installed for this function to work - need to formalise this somehow
    create_distinct_entities()

if __name__ == '__main__':
    main()