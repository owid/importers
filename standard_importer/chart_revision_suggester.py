import os
import re
from utils import import_from
import simplejson as json
import logging
import traceback
from typing import List, Tuple, Dict
from copy import deepcopy
from dotenv import load_dotenv
import pandas as pd
from tqdm import tqdm
from pandas.api.types import is_numeric_dtype
from pymysql.err import IntegrityError

from db_utils import DBUtils
from db import get_connection
from utils import import_from

load_dotenv()

logging.basicConfig()
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

DEBUG = os.getenv('DEBUG') == "True"
USER_ID = int(os.getenv('USER_ID'))

class ChartRevisionSuggester(object):
    """Implements methods for suggesting revisions to one or more
    charts, to be approved using the chart approval tool.

    This class is intended for use after a new dataset has been imported
    into the MySQL database and you wish to update the corresponding
    OWID charts to display the newly available data in place of the old data.

    Attributes:
        dataset_dir: str. Name of dataset directory. Example: "worldbank_wdi". 
            There *must* be a `variable_replacements.json` file located in 
            either `{dataset_dir}/config/` or `{dataset_dir}/output/`. The 
            `variable_replacements.json` file contains a dictionary of 
            old_variable_id->new_variable_id key-value pairs. Example:

                {"2032": 147395, "2033": 147396, ...}
    
    Usage:
        >>> from standard_importer.chart_revision_suggester import ChartRevisionSuggester
        >>> dataset_dir = "worldbank_wdi"
        >>> suggester = ChartRevisionSuggester(dataset_dir)
        >>> suggester.suggest()
    """
    
    def __init__(self, dataset_dir: str):
        self.var_id2year_range = None
        self.dataset_dir = dataset_dir
        self.old_var_id2new_var_id = self.load_variable_replacements()

    @property
    def status(self) -> str:
        return "pending"

    def suggest(self, *args, **kwargs) -> None:
        kwargs['suggested_chart_revisions'] = self.prepare()
        self.insert(*args, **kwargs)
    
    def load_variable_replacements(self) -> Dict[int, int]:
        try:
            with open(os.path.join(self.dataset_dir, 'config', 'variable_replacements.json'), 'r') as f:
                data = {int(k): int(v) for k, v in json.load(f).items()}
        except FileNotFoundError:
            with open(os.path.join(self.dataset_dir, 'output', 'variable_replacements.json'), 'r') as f:
                data = {int(k): int(v) for k, v in json.load(f).items()}
        return data

    def prepare(self) -> List[dict]:
        self.var_id2year_range = self._get_variable_year_ranges()
        df_charts, df_chart_dims, _ = self._get_charts_from_old_variables()
        suggested_chart_revisions = []
        for row in tqdm(df_charts.itertuples(), total=df_charts.shape[0]):
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
                    suggested_chart_revisions.append({
                        'chartId': chart_id, 
                        'originalConfig': row.config, 
                        'suggestedConfig': chart_config_str
                    })

            except Exception as e:
                logger.error(f'Error encountered for chart {row.id}: {e}')
                if DEBUG:
                    traceback.print_exc()
        return suggested_chart_revisions
    
    def insert(self, suggested_chart_revisions: List[dict], suggested_reason: str = None) -> None:
        if suggested_reason is None:
            dataset_name = import_from(self.dataset_dir, 'DATASET_NAME')
            dataset_version = import_from(self.dataset_dir, 'DATASET_VERSION')
            suggested_reason = f"{dataset_name} (v{dataset_version}) bulk dataset update"
        try:
            connection = get_connection()
            connection.autocommit(False)
            cursor = connection.cursor()
            db = DBUtils(cursor)

            n_before = db.fetch_one("SELECT COUNT(id) FROM suggested_chart_revisions")[0]
            
            res = db.fetch_many("""
                SELECT *
                FROM (
                    SELECT chartId, COUNT(chartId) as c
                    FROM suggested_chart_revisions
                    WHERE status IN ("pending", "flagged")
                    GROUP BY chartId
                    ORDER BY c DESC
                    ) as grouped
                WHERE grouped.c > 1
            """)
            if len(res):
                raise RuntimeError(
                    "Two or more suggested chart revisions with status IN "
                    "('pending', 'flagged') share an identical chart id. These "
                    "must be resolved before inserting more suggested "
                   f"chart revisions. Affected chart IDs: {[r[0] for r in res]}"
                )

            tuples = []
            for rev in suggested_chart_revisions:
                t = (
                    int(rev['chartId']), 
                    rev['suggestedConfig'], 
                    rev['originalConfig'], 
                    suggested_reason, 
                    self.status, 
                    USER_ID, 
                )
                tuples.append(t)

            chart_ids = [t[0] for t in tuples]
            assert len(chart_ids) == len(set(chart_ids)), "`suggested_chart_revisions` contains duplicate chart ids."

            query = f"""
                INSERT INTO suggested_chart_revisions
                    (chartId, suggestedConfig, originalConfig, suggestedReason, status, createdBy, createdAt, updatedAt)
                VALUES 
                    (%s, %s, %s, %s, %s, %s, NOW(), NOW())
            """
            db.upsert_many(query, tuples)

            # checks if any of the affected chartIds now has multiple
            # pending suggested revisions. If so, then rejects the whole
            # insert and tell the user which suggested chart revisions need
            # to be approved/rejected.
            res = db.fetch_many(f"""
                SELECT id, scr.chartId, c, createdAt
                FROM (
                    SELECT chartId, COUNT(chartId) as c
                    FROM suggested_chart_revisions
                    WHERE status IN ("pending", "flagged") AND chartId IN ({", ".join([str(_id) for _id in chart_ids])})
                    GROUP BY chartId
                    ORDER BY c DESC
                ) as grouped
                LEFT JOIN (
                    SELECT * 
                    FROM suggested_chart_revisions 
                    WHERE status IN ("pending", "flagged")
                ) as scr ON grouped.chartId = scr.chartId
                WHERE grouped.c > 1
                ORDER BY createdAt ASC
            """)
            if len(res):
                df = pd.DataFrame(res, columns=['id', 'chart_id', 'count', 'created_at'])
                df['drop'] = df.groupby('chart_id')['created_at'].transform(lambda gp: gp == gp.max())
                df = df[~df['drop']]
                # problem_chart_ids = [r[0] for r in res]
                s = ''
                for nm, gp in df.groupby('chart_id'):
                    s += f"Chart ID: {nm}. Suggested chart revision IDs: {gp['id'].tolist()}\n"
                raise RuntimeError(
                    "For one or more of the suggested chart revisions that you are "
                    "trying to insert, a suggested chart revision already exists for "
                    "the same chartId with status IN ('pending', 'flagged'). You "
                    "must approve/reject these suggested chart revisions before new "
                    "suggested revisions for the same charts can be created. "
                    f"Affected charts:\n{s}"
                )
            connection.commit()
        except IntegrityError as e:
            connection.rollback()
            logger.error(
                'INSERT operation into `suggested_chart_revisions` cancelled. '
                'Failed to insert suggested chart revisions because one or '
                'more of the suggested revisions that you are trying to insert '
                'already exists with an equivalent chartId, originalVersion, '
               f'suggestedVersion, and suggestedReason. Error: {e}'
            )
        except Exception as e:
            connection.rollback()
            logger.error(f'INSERT operation into `suggested_chart_revisions` cancelled. Error: {e}')
            if DEBUG:
                traceback.print_exc()
        finally:
            n_after = db.fetch_one("SELECT COUNT(id) FROM suggested_chart_revisions")[0]
            cursor.close()
            connection.close()
            logger.info(f'{n_after - n_before} of {len(suggested_chart_revisions)} suggested chart revisions inserted.')

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
        with get_connection().cursor() as cursor:
            # retrieves chart_dimensions
            db = DBUtils(cursor)
            variable_ids = list(self.old_var_id2new_var_id.keys())
            variable_ids_str = ','.join([str(_id) for _id in variable_ids])
            columns = ['id', 'chartId', 'variableId', 'property', 'order']
            rows = db.fetch_many(f"""
                SELECT {','.join([f'`{col}`' for col in columns])}
                FROM chart_dimensions
                WHERE variableId IN ({variable_ids_str})
            """)
            df_chart_dimensions = pd.DataFrame(rows, columns=columns)

            # retrieves charts
            chart_ids_str = ','.join([str(_id) for _id in df_chart_dimensions['chartId'].unique().tolist()])
            columns = ['id', 'config', 'createdAt', 'updatedAt', 'lastEditedAt', 'publishedAt']
            rows = db.fetch_many(f"""
                SELECT {','.join(columns)}
                FROM charts
                WHERE id IN ({chart_ids_str})
            """)
            df_charts = pd.DataFrame(rows, columns=columns)
        
            # retrieves chart_revisions
            columns = ['id', 'chartId', 'userId', 'config', 'createdAt', 'updatedAt']
            rows = db.fetch_many(f"""
                SELECT {','.join(columns)}
                FROM chart_revisions
                WHERE chartId IN ({chart_ids_str})
            """)
            df_chart_revisions = pd.DataFrame(rows, columns=columns)
        return df_charts, df_chart_dimensions, df_chart_revisions

    def _get_variable_year_ranges(self) -> Dict[int, List[int]]:
        with get_connection().cursor() as cursor:
            db = DBUtils(cursor)
            all_var_ids = list(self.old_var_id2new_var_id.keys()) + list(self.old_var_id2new_var_id.values())
            variable_ids_str = ','.join([str(_id) for _id in all_var_ids])
            columns = []
            rows = db.fetch_many(f"""
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
