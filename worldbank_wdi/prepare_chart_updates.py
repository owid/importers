"""

Usage:

    python -m worldbank_wdi.suggest_chart_revisions
"""

import os
import simplejson as json
import logging
from typing import Dict

from db import connection
from db_utils import DBUtils
from standard_importer.chart_updater import ChartUpdater
from worldbank_wdi import CONFIGPATH, OUTPATH, CONFIGPATH

DEBUG = True

logging.basicConfig()
logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG if DEBUG else logging.INFO)


def main():
    # cursor = connection.cursor()
    with connection.cursor() as cursor:
        db = DBUtils(cursor)
        old_var_id2new_var_id = load_variable_replacements()

        updater = ChartUpdater(db=db, old_var_id2new_var_id=old_var_id2new_var_id)
        charts_to_update, chart_dims_to_update = updater.prepare_updates()

        with open(os.path.join(OUTPATH, 'charts_to_update.json'), 'w') as f:
            json.dump(charts_to_update, f)

        # with open(os.path.join(OUTPATH, 'chart_dimensions_to_update.json'), 'w') as f:
        #     json.dump(chart_dims_to_update, f)


def load_variable_replacements() -> Dict[int, int]:
    with open(os.path.join(CONFIGPATH, 'variable_replacements.json'), 'r') as f:
        data = {int(k): int(v) for k, v in json.load(f).items()}
    return data


if __name__ == '__main__':
    main()
