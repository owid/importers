"""Removes hard-coded year "2011" from subtitles of charts containing the 
GDP per capita, PPP (constant 2017 international $) variable.
"""

import os
import re
import logging
import pandas as pd
from dotenv import load_dotenv
from typing import List, Union

from db import get_connection
from db_utils import DBUtils

logging.basicConfig()
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

load_dotenv()

VAR_ID = 147776  # GDP per capita, PPP (constant 2017 international $)
YEAR_TO_REPLACE = 2011
USER_ID = int(os.getenv("USER_ID"))  # type: ignore


def main() -> None:
    df = get_fastt_to_fix()
    df.iloc[0].subtitle
    for col in df:
        df[col] = replace_year(df[col], year=YEAR_TO_REPLACE)
    df["updatedBy"] = USER_ID
    records = df.reset_index().to_dict(orient="records")
    with get_connection() as conn:
        cursor = conn.cursor()
        db = DBUtils(cursor)
        db.cursor.executemany(
            """
            UPDATE suggested_chart_revisions
            SET suggestedConfig = JSON_REPLACE(
                    suggestedConfig, 
                    '$.title', JSON_UNQUOTE(%(title)s), 
                    '$.subtitle', JSON_UNQUOTE(%(subtitle)s), 
                    '$.note', JSON_UNQUOTE(%(note)s)
                ),
                updatedAt = NOW(),
                updatedBY = %(updatedBy)s
            WHERE id=%(id)s
            """,
            records,
        )

    # sanity check
    df = get_fastt_to_fix()
    if df.shape[0] > 0:
        logger.warning(
            f"A title/subtitle/note in {df.shape[0]} record(s) still contain the year {YEAR_TO_REPLACE}"
        )


def get_fastt_to_fix() -> pd.DataFrame:
    assert (
        pd.read_sql(
            f"""SELECT name FROM variables WHERE id={VAR_ID}""", get_connection()
        ).iloc[0]["name"]
        == "GDP per capita, PPP (constant 2017 international $)"
    ), f'Expected variable with id={VAR_ID} to have name="GDP per capita, PPP (constant 2017 international $)".'
    df = pd.read_sql(
        f"""
        SELECT id, 
            suggestedConfig->"$.title" as title, 
            suggestedConfig->"$.subtitle" as subtitle, 
            suggestedConfig->"$.note" as note
        FROM suggested_chart_revisions
        WHERE status="pending" 
            AND JSON_CONTAINS(suggestedConfig->"$.dimensions[*].variableId", '{VAR_ID}', '$')
            AND (
                suggestedConfig->"$.title" REGEXP "\s*2011\s*" OR
                suggestedConfig->"$.subtitle" REGEXP "\s*2011\s*" OR
                suggestedConfig->"$.note" REGEXP "\s*2011\s*"
            )
        ORDER BY updatedAt DESC
    """,
        get_connection(),
    )
    return df.set_index("id")


def replace_year(values: List[str], year: Union[int, str]) -> List[str]:
    regexes = [
        {
            "pattern": re.compile(rf"\s*(constant)?\s*{year}\s*international"),
            "repl": " constant international",
        },
        {
            "pattern": re.compile(rf"\s*(constant)?\s*{year}\s*int\b"),
            "repl": " constant int",
        },
        {
            "pattern": re.compile(rf"constant\s*{year}\s*prices"),
            "repl": "constant prices",
        },
        {
            "pattern": re.compile(
                rf"in\s*international-\$\s*(in|at)\s*{year}\s*prices"
            ),
            "repl": "in constant international-$",
        },
    ]
    result = []
    for s in values:
        changed = False
        i = 0
        if not isinstance(s, str):
            new_s = s
        else:
            while not changed and i < len(regexes):
                regex = regexes[i]
                new_s = re.sub(regex["pattern"], regex["repl"], s)
                if new_s != s:
                    changed = True
                i += 1
            if not changed and re.search(f"{year}", s):
                print(f"Failed to replace year in string {s}")
        result.append(new_s)
    return result


if __name__ == "__main__":
    main()
