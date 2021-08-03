"""Exports a csv of all variables in the MySQL table (enriched with data from
other tables, such as dataset name, chart IDs, etc).
"""

import pandas as pd

from db import get_connection


def main():
    df = get_variables()
    df.to_csv("variables.csv", index=False)


def get_variables() -> pd.DataFrame:
    df = pd.read_sql(
        """
        SELECT 
            # variable-level columns
            v.id as variableId, v.name as variableName, 
            v.createdAt as variableCreatedAt, v.updatedAt as variableUpdatedAt, 
            v.description as variableDescription,
            v.unit as variableUnit, v.shortUnit as variableShortUnit, 
            v.display as variableDisplay,
            
            # dataset-level columns
            v.datasetId, d.name as datasetName, d.namespace as datasetNamespace,
            d.createdAt as datasetCreatedAt, d.updatedAt as datasetUpdatedAt,
            d.dataEditedAt as datasetDataEditedAt, 
            uc.fullName as datasetCreatedBy, ue.fullName as datasetdataEditedBy, 
            
            # source-level columns
            v.sourceId, s.name as sourceName,

            # chart-level columns
            cd.chartIds
        
        FROM variables as v
        LEFT JOIN datasets as d
        ON v.datasetId = d.id
        LEFT JOIN sources as s
        ON v.sourceId = s.id
        LEFT JOIN users as uc
        ON d.createdByUserId = uc.id
        LEFT JOIN users as ue
        ON d.dataEditedByUserId = ue.id
        LEFT JOIN (
            SELECT variableId, JSON_ARRAYAGG(chartId) as chartIds
            FROM chart_dimensions
            GROUP BY variableId
        ) as cd
        ON v.id = cd.variableId
        ORDER BY datasetDataEditedAt DESC
    """,
        get_connection(),
    )
    return df


if __name__ == "__main__":
    main()
