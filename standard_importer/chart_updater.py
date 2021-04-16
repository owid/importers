
import os
import re
import simplejson as json
import logging
import traceback
from typing import List, Tuple
from copy import deepcopy

import pandas as pd
from tqdm import tqdm
from pandas.api.types import is_numeric_dtype

from db_utils import DBUtils
from worldbank_wdi import DATASET_NAME, DATASET_VERSION

DEBUG = True

logging.basicConfig()
logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG if DEBUG else logging.INFO)


class ChartUpdater(object):
    """Implements methods for updating many charts at once.
    """
    var_id2year_range = None
    created_reason = f"{DATASET_NAME} (v{DATASET_VERSION}) bulk dataset update"
    
    def __init__(self, db: DBUtils, old_var_id2new_var_id: dict):
        self.db = db
        self.old_var_id2new_var_id = old_var_id2new_var_id

    def prepare_updates(self):
        self.var_id2year_range = self._get_variable_year_ranges()
        df_charts, df_chart_dims, _ = self._get_charts_from_old_variables()
        charts_to_update = []
        chart_dims_to_update = []
        for row in tqdm(df_charts.itertuples(), total=df_charts.shape[0]):
            # if row.id == 4477:
            #     break
            try:
                chart_id = row.id
                # retrieves chart dimensions to be updated.
                chart_dims = df_chart_dims[df_chart_dims['chartId'] == chart_id].to_dict(orient='records')
                chart_dims_orig = deepcopy(chart_dims)
                chart_config = json.loads(row.config)

                self._modify_chart_config_map(chart_config)
                self._modify_chart_config_time(chart_id, chart_config)
                self._modify_chart_config_fastt(chart_id, chart_config)
            
                self._modify_chart_dimensions(chart_id, chart_dims, chart_config)
                
                
                config_has_changed = json.dumps(chart_config, ignore_nan=True) != row.config
                dims_have_changed = any([dim != chart_dims_orig[i] for i, dim in enumerate(chart_dims)])
                assert config_has_changed == dims_have_changed, (
                    f'Chart {chart_id}: Chart config and chart dimensions must '
                     'have either BOTH changed or NEITHER changed, but only '
                     'one has changed. Something went wrong.'
                )
                if config_has_changed:
                    # update version
                    # if 'version' in chart_config and isinstance(chart_config['version'], int):
                    chart_config['version'] += 1
                    chart_config_str = json.dumps(chart_config, ignore_nan=True)
                    charts_to_update.append({
                        'id': chart_id, 
                        'old': {'config': row.config}, 
                        'new': {'config': chart_config_str},
                        'createdReason': self.created_reason
                    })
                    for i, dim in enumerate(chart_dims):
                        dim_has_changed = dim != chart_dims_orig[i]
                        if dim_has_changed:
                            chart_dims_to_update.append({
                                'id': chart_dims_orig[i]['id'],
                                'old': chart_dims_orig[i],
                                'new': dim
                            })

            except Exception as e:
                logger.error(f'Error encountered for chart {row.id}: {e}')
                if DEBUG:
                    traceback.print_exc()
        return charts_to_update, chart_dims_to_update

    # def preview_one_update(self, path_to_index_html, chart_to_update: dict):
    #     with open(os.path.join(os.path.dirname(path_to_index_html), 'grapher.js'), 'w') as f:
    #         f.write(f'const sandboxGrapherLeft = {chart_to_update["old"]["config"]}\n'
    #                 f'const sandboxGrapherRight = {chart_to_update["new"]["config"]}')
        
    #     webbrowser.open(f'file://{os.path.abspath(path_to_index_html)}')

    def _get_charts_from_old_variables(self) -> Tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
        """retrieves all charts, chart_dimensions, and chart_revisions rows 
        for old variables.

        Returns:

            (df_charts, 
             df_chart_dimensions, 
             df_chart_revisions): Tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame].
                df_charts: dataframe of charts rows.
                df_chart_dimensions: dataframe of chart_dimensions rows.
                df_chart_revisions: dataframe of chart_revisions rows.
        """
        # retrieves chart_dimensions
        variable_ids = list(self.old_var_id2new_var_id.keys())
        variable_ids_str = ','.join([str(_id) for _id in variable_ids])
        columns = ['id', 'chartId', 'variableId', 'property', 'order']
        rows = self.db.fetch_many(f"""
            SELECT {','.join([f'`{col}`' for col in columns])}
            FROM chart_dimensions
            WHERE variableId IN ({variable_ids_str})
        """)
        df_chart_dimensions = pd.DataFrame(rows, columns=columns)

        # retrieves charts
        chart_ids_str = ','.join([str(_id) for _id in df_chart_dimensions['chartId'].unique().tolist()])
        columns = ['id', 'config', 'createdAt', 'updatedAt', 'lastEditedAt', 'publishedAt']
        rows = self.db.fetch_many(f"""
            SELECT {','.join(columns)}
            FROM charts
            WHERE id IN ({chart_ids_str})
        """)
        df_charts = pd.DataFrame(rows, columns=columns)
        
        # retrieves chart_revisions
        columns = ['id', 'chartId', 'userId', 'config', 'createdAt', 'updatedAt']
        rows = self.db.fetch_many(f"""
            SELECT {','.join(columns)}
            FROM chart_revisions
            WHERE chartId IN ({chart_ids_str})
        """)
        df_chart_revisions = pd.DataFrame(rows, columns=columns)
        return df_charts, df_chart_dimensions, df_chart_revisions

    def _get_variable_year_ranges(self):
        all_var_ids = list(self.old_var_id2new_var_id.keys()) + list(self.old_var_id2new_var_id.values())
        variable_ids_str = ','.join([str(_id) for _id in all_var_ids])
        columns = []
        rows = self.db.fetch_many(f"""
            SELECT variableId, MIN(year) AS minYear, MAX(year) AS maxYear
            FROM data_values
            WHERE variableId IN ({variable_ids_str})
            GROUP BY variableId
        """)
        var_id2year_range = {}
        for row in rows:
            var_id2year_range[row[0]] = [row[1], row[2]]
        return var_id2year_range

    def _modify_chart_config_map(self, chart_config: dict) -> None:
        """modifies chart config map."""
        if 'map' in chart_config and 'variableId' in chart_config['map'] and \
            chart_config['map']['variableId'] in self.old_var_id2new_var_id:

            old_var_id = chart_config['map']['variableId']
            new_var_id = self.old_var_id2new_var_id[old_var_id]
            chart_config['map']['variableId'] = new_var_id
            min_year_old, max_year_old = self.var_id2year_range[old_var_id]
            min_year_new, max_year_new = self.var_id2year_range[new_var_id]
            
            # update targetYear
            if 'targetYear' in chart_config['map']:
                if pd.notnull(min_year_new) and chart_config['map']['targetYear'] == min_year_old:
                    chart_config['map']['targetYear'] = int(min_year_new)
                elif pd.notnull(max_year_new):
                    chart_config['map']['targetYear'] = int(max_year_new)
            
            # update time
            if 'time' in chart_config['map']:
                if pd.notnull(min_year_new) and chart_config['map']['time'] == min_year_old:
                    chart_config['map']['time'] = int(min_year_new)
                elif pd.notnull(max_year_new):
                    chart_config['map']['time'] = int(max_year_new)
    
    def _modify_chart_config_time(self, chart_id: int, chart_config: dict) -> None:
        """modifies chart config maxTime and minTime"""
        old_variable_ids = set([dim['variableId'] for dim in chart_config['dimensions']])
        if 'map' in chart_config and 'variableId' in chart_config['map']:
            old_variable_ids.add(chart_config['map']['variableId'])
        old_variable_ids = [_id for _id in old_variable_ids if _id in self.old_var_id2new_var_id]
        new_variable_ids = [self.old_var_id2new_var_id[_id] for _id in old_variable_ids]
        min_year_old = min([self.var_id2year_range[_id][0] for _id in old_variable_ids])
        max_year_old = max([self.var_id2year_range[_id][1] for _id in old_variable_ids])
        min_year_new = min([self.var_id2year_range[_id][0] for _id in new_variable_ids])
        max_year_new = max([self.var_id2year_range[_id][1] for _id in new_variable_ids])
        
        # Is the min year hard-coded in the chart's title or subtitle?
        min_year_hardcoded = (
            (
                'minTime' in chart_config and 
                'title' in chart_config and 
                bool(re.search(rf"{chart_config['minTime']}", chart_config['title']))
            ) or
            (
                'minTime' in chart_config and 
                'subtitle' in chart_config and 
                bool(re.search(rf"{chart_config['minTime']}", chart_config['subtitle']))
            )
        )
        # Is the min year hard-coded in the chart's title or subtitle?
        max_year_hardcoded = (
            (
                'maxTime' in chart_config and 
                'title' in chart_config and 
                bool(re.search(rf"{chart_config['maxTime']}", chart_config['title']))
            ) or
            (
                'maxTime' in chart_config and 
                'subtitle' in chart_config and 
                bool(re.search(rf"{chart_config['maxTime']}", chart_config['subtitle']))
            )
        )
        if min_year_hardcoded or max_year_hardcoded:
            title = chart_config['title'] if 'title' in chart_config else None
            subtitle = chart_config['subtitle'] if 'subtitle' in chart_config else None
            min_time = chart_config['minTime'] if 'minTime' in chart_config else None
            max_time = chart_config['maxTime'] if 'maxTime' in chart_config else None
            logger.warning(
                f'Chart {chart_id} title or subtitle may contain a hard-coded '
                 'year, so the minTime and maxTime fields will not be changed.'
                f'\nTitle: {title}'
                f'\nSubtitle: {subtitle}'
                f'\nminTime: {min_time}; maxTime: {max_time}'
            )
        else:
            times_are_eq = (
                'minTime' in chart_config and 
                'maxTime' in chart_config and 
                (
                    (chart_config['minTime'] == chart_config['maxTime']) or
                    (chart_config['minTime'] == 'earliest' and chart_config['maxTime'] == min_year_old) or
                    (chart_config['minTime'] == min_year_old and chart_config['maxTime'] == 'earliest') or
                    (chart_config['minTime'] == max_year_old and chart_config['maxTime'] == 'latest') or 
                    (chart_config['minTime'] == 'latest' and chart_config['maxTime'] == max_year_old)
                )
            )
            if times_are_eq:
                use_min_year = (
                    chart_config['minTime'] == 'earliest' or
                    chart_config['maxTime'] == 'earliest' or 
                    (
                        is_numeric_dtype(chart_config['minTime']) and
                        is_numeric_dtype(chart_config['maxTime']) and
                        abs(chart_config['minTime'] - min_year_old) < abs(chart_config['maxTime'] - max_year_old)
                    )
                )
                if use_min_year:
                    chart_config['minTime'] = min_year_new
                    chart_config['maxTime'] = min_year_new
                else:
                    chart_config['minTime'] = max_year_new
                    chart_config['maxTime'] = max_year_new
            else:
                replace_min_time = (
                    'minTime' in chart_config and 
                    chart_config['minTime'] != 'earliest' and
                    pd.notnull(min_year_new)
                )
                if replace_min_time: 
                    min_year_new = int(min_year_new)
                    if pd.notnull(min_year_old) and (min_year_new > min_year_old):
                        logger.warning(
                            f'For chart {chart_id}, min year of new variable(s) > '
                            'min year of old variable(s). New variable(s): '
                            f'{new_variable_ids}'
                        )
                    chart_config['minTime'] = min_year_new
                replace_max_time = (
                    'maxTime' in chart_config and 
                    chart_config['maxTime'] != 'latest' and
                    pd.notnull(max_year_new)
                )
                if replace_max_time:
                    max_year_new = int(max_year_new)
                    if pd.notnull(max_year_old) and (max_year_new < max_year_old):
                        logger.warning(
                            f'For chart {chart_id}, max year of new variable(s) < '
                            'max year of old variable(s). New variable(s): '
                            f'{new_variable_ids}'
                        )
                    chart_config['maxTime'] = max_year_new

    
    def _modify_chart_config_fastt(self, chart_id: int, chart_config: dict) -> None:
        """modifies chart config FASTT.
        
        update/check text fields: slug, note, title, subtitle, sourceDesc.
        """
        if 'title' in chart_config and re.search(r'\b\d{4}\b', chart_config['title']):
            logger.warning(
                f'Chart {chart_id} title may have a hard-coded year in it that '
                f'will not be updated: "{chart_config["title"]}"'
            )
        if 'subtitle' in chart_config and re.search(r'\b\d{4}\b', chart_config['subtitle']):
            logger.warning(
                f'Chart {chart_id} subtitle may have a hard-coded year in it '
                f'that will not be updated: "{chart_config["subtitle"]}"'
            )
        if 'note' in chart_config and re.search(r'\b\d{4}\b', chart_config['note']):
            logger.warning(
                f'Chart {chart_id} note may have a hard-coded year in it that '
                f'will not be updated: "{chart_config["note"]}"'
            )
        if re.search(r'\b\d{4}\b', chart_config['slug']):
            logger.warning(
                f'Chart {chart_id} slug may have a hard-coded year in it that '
                f'will not be updated: "{chart_config["slug"]}"'
            )

    def _modify_chart_dimensions(self, chart_id: int, chart_dimensions: List[dict], chart_config: dict) -> None:
        """modifies each chart dimension (in both chart_dimensions and chart config)."""
        for dim in chart_dimensions:
            if dim['variableId'] in self.old_var_id2new_var_id:
                dim['variableId'] = self.old_var_id2new_var_id[dim['variableId']]
                config_dim = chart_config['dimensions'][dim['order']]
                config_dim['variableId'] = self.old_var_id2new_var_id[config_dim['variableId']]
