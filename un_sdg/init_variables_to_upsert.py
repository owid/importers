import os
import simplejson as json
import pandas as pd

from db import connection
from db_utils import DBUtils

from un_sdg import INPATH, CONFIGPATH
from un_sdg.match_variables import get_datasets


def main():
    df_old_vars = get_old_variables()
    df_variables = get_new_variables()
    var_name2code = df_variables.set_index('indicator_name')['indicator_code'].to_dict()
    variables_to_clean = []
    for _, row in df_old_vars.iterrows():
        if row['name'] in df_variables['indicator_name'].tolist():
            new_code = var_name2code[row['name']].upper()
            assert row['code'] == new_code, f"old code != new code ({row['code']} != {var_name2code[row['name']]}"
            meta = {
                "originalMetadata": row['originalMetadata'],
                "name": row['name'],
                "unit": row['unit'],
                "shortUnit": row['shortUnit'],
                "description": None,
                "code": new_code,
                "coverage": row['coverage'],
                "timespan": row['timespan'],
                "display": json.loads(row['display']),
            }
            variables_to_clean.append(meta)
        else:
            print(f'Failed to find new variable for: {row["name"]}')
    # pd.DataFrame(variables_to_clean).to_csv(os.path.join(CURRENT_DIR, 'config', 'variables_to_clean.csv'), index=False)
    with open(os.path.join(CONFIGPATH, 'variables_to_clean.json'), 'w') as f:
        json.dump({
            'meta': {'notes': 'This file contains an array of UN SDG '
                              'variables to upsert to SQL. Any variables NOT '
                              'in this array will be ignored.'},
            'variables': variables_to_clean
        }, f, ignore_nan=True, indent=4)


def get_old_variables():
    with connection.cursor() as cursor:
        db = DBUtils(cursor)
        df_old_datasets = get_datasets(db=db, new=False)
        columns = ['id', 'name', 'originalMetadata', 'unit', 'shortUnit', 
                   'description', 'code', 'coverage', 'timespan', 'display']
        rows = db.fetch_many(f"""
            SELECT {','.join(columns)}
            FROM variables
            WHERE id IN (
                SELECT DISTINCT(variableId)
                FROM chart_dimensions
            ) AND datasetId IN ({','.join([str(_id) for _id in df_old_datasets['id'].tolist()])})
        """)
        df_old_used_vars = pd.DataFrame(rows, columns=columns)
    return df_old_used_vars


def get_new_variables():
    infpath = os.path.join(INPATH, 'un_sdg.csv')
    df_data = pd.read_csv(infpath)
    df_data.columns = df_data.columns.str.lower().str.replace(r'[\s/-]+', '_', regex=True)
    df_variables = df_data[['indicator', 'seriescode']].drop_duplicates()
    return df_variables

if __name__ == '__main__':
    main()