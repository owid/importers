"""

Usage:

    python -m standard_importer.upsert_suggested_chart_revisions
"""

import os
import json
from dotenv import load_dotenv

from db import connection
from db_utils import DBUtils

load_dotenv()

USER_ID = os.getenv('USER_ID')
assert USER_ID is not None, "USER_ID not found in .env file"
USER_ID = int(USER_ID)

CURRENT_DIR = os.path.dirname(__file__)
# CURRENT_DIR = os.path.join(os.getcwd(), 'standard_importer')


def main(dataset_dir: str):
    output_dir = os.path.join(dataset_dir, 'output')
    with open(os.path.join(output_dir, 'charts_to_update.json'), 'r') as f:
        data = json.load(f)
    
    with connection.cursor() as cursor:
        db = DBUtils(cursor)
        tuples = []
        for d in data:
            t = (int(d['id']), d['new']['config'], d['createdReason'], int(USER_ID))
            tuples.append(t)
            
        query = f"""
            INSERT INTO suggested_chart_revisions
                (chartId, config, createdReason, userId, status, createdAt, updatedAt)
            VALUES 
                (%s, %s, %s, %s, "pending", NOW(), NOW())
            ON DUPLICATE KEY UPDATE
                chartId = VALUES(chartId),
                config = VALUES(config),
                createdReason = VALUES(createdReason),
                userId = VALUES(userId),
                status = VALUES(status),
                updatedAt = VALUES(updatedAt)
        """
        db.upsert_many(query, tuples)


if __name__ == '__main__':
    main()
