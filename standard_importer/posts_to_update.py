"""Constructs a dataframe that counts the number of charts per OWID page that
have been updated.

Each row is in the dataframe is an OWID page.

The exported csv file is intended to make it easy for OWID staff to see which
public OWID pages need to be checked for textual inconsistencies after one or
more suggested chart revisions have been approved as part of a bulk dataset
update.

Example:

    >>> from standard_importer import posts_to_update
    >>> df = posts_to_update.main(dataset_id=5357, since="2021-08-01", include_google_analytics=True)
    >>> print(df.head())
                             post_title                                          post_slug  num_charts_updated  pageviews_from_20210608_to_20210608
    1                Women's employment     https://ourworldindata.org/female-labor-supply                   2                               9755.0
    2  Working women: Key facts and ...  https://ourworldindata.org/female-labor-force-...                   2                               6250.0
    0           Teachers and Professors  https://ourworldindata.org/teachers-and-profes...                   2                               1894.0

"""

import os
import re
import time
import datetime as dt
import grequests
import json
import pandas as pd
import numpy as np
import logging
from tqdm import tqdm
from dotenv import load_dotenv

from db import get_connection
from utils import assert_admin_api_connection, batchify
from site_analytics.request_report import execute_report_request

load_dotenv()

logging.basicConfig()
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)


# config for google analytics report request
GA_CONFIG = {
    "metric": "pageviews",
    "dimension": "pagePath",
    "filters_expression": "ga:dimension1==0",  # exclude embedded page views
    "start_date": (dt.datetime.utcnow() - dt.timedelta(days=90)).strftime("%Y-%m-%d"),
    "end_date": (dt.datetime.utcnow() - dt.timedelta(days=1)).strftime("%Y-%m-%d"),
}

SITE_SESSION_ID = os.getenv("SITE_SESSION_ID")
SITE_HOST = os.getenv("SITE_HOST")
DEBUG = os.getenv("DEBUG") == "True"


def main(
    dataset_id: int, since: str, include_google_analytics: bool = False
) -> pd.DataFrame:
    """Constructs a dataframe that counts the number of charts from a dataset
    per OWID page that have been updated.
    """
    if include_google_analytics:
        df_ga = get_ga_data()

    df = get_charts_updated_data(dataset_id, since)

    if include_google_analytics:
        metric = GA_CONFIG["metric"]
        dimension = GA_CONFIG["dimension"]
        start_date = GA_CONFIG["start_date"]
        end_date = GA_CONFIG["start_date"]
        df = df.merge(
            df_ga[[dimension, metric]],
            left_on="post_slug",
            right_on=dimension,
            how="left",
            validate="1:1",
        ).drop(columns=[dimension])
        df.sort_values(by=[metric, "num_charts_updated"], ascending=False, inplace=True)
        df.rename(
            columns={
                metric: f"{metric}_from_{re.sub('-', '', start_date)}_to_{re.sub('-', '', end_date)}"
            },
            inplace=True,
        )
    else:
        df.sort_values(by=["num_charts_updated"], ascending=False, inplace=True)

    df["post_slug"] = "https://ourworldindata.org/" + df["post_slug"]
    return df


def get_ga_data() -> pd.DataFrame:
    df_ga = execute_report_request(**GA_CONFIG)
    assert (df_ga[GA_CONFIG["dimension"]] == "(other)").sum() == 0
    if "pagePath" in df_ga.columns:
        df_ga["pagePath"] = (
            df_ga["pagePath"]
            .str.replace(r"\?.*", "", regex=True)
            .str.replace(r"/?\.?$", "", regex=True)
            .str.replace(r"^/", "", regex=True)
        )
        df_ga = (
            df_ga.groupby(["start_date", "end_date", GA_CONFIG["dimension"]])[
                GA_CONFIG["metric"]
            ]
            .sum()
            .reset_index()
        )

    df_ga.sort_values(GA_CONFIG["metric"], ascending=False, inplace=True)
    return df_ga


def get_charts_updated_data(dataset_id: int, since: str) -> pd.DataFrame:
    """returns a dataframe of the number of charts updated since YYYY-MM-DD, by page."""
    # retrieves all charts that use a variable from {dataset} and have been updated.
    assert_admin_api_connection()
    logger.info("Retrieving updated charts...")
    df = pd.read_sql(
        f"""
        SELECT
            charts.id, charts.updatedAt, charts.createdAt, charts.lastEditedAt, charts.publishedAt
        FROM charts
        INNER JOIN chart_dimensions
        ON charts.id = chart_dimensions.chartId
        WHERE variableId IN (
            SELECT id
            FROM variables
            WHERE datasetId = {dataset_id}
        )
            AND charts.updatedAt >= "{since}"
        ORDER BY updatedAt DESC
    """,
        get_connection(),
    )
    if DEBUG:
        logger.warning(
            "DEBUG mode is on. Only retrieving post references for the first "
            "10 updated charts."
        )
        df = df.iloc[:10]

    logger.info("Retrieving chart references to OWID pages...")
    wait = 2
    batch_size = 50
    n_batches = int(np.ceil(df.shape[0] / batch_size))

    responses = []
    for batch in tqdm(batchify(df, batch_size=batch_size), total=n_batches):
        requests = []
        for _, row in batch.iterrows():
            # refs = get_references_by_chart_id(row.id)["references"]
            url = f"{SITE_HOST}/admin/api/charts/{row.id}.references.json"
            res = grequests.get(url, cookies={"sessionid": SITE_SESSION_ID})
            requests.append(res)
        responses += grequests.map(requests)
        time.sleep(wait)

    references = []
    for resp in responses:
        refs = json.loads(resp.content)["references"]
        for ref in refs:
            ref["chartId"] = row.id
        references += refs

    # df_refs = pd.read_csv('refs.csv')
    df_refs = pd.DataFrame(references).rename(columns={"id": "refId"})
    df = (
        df_refs.value_counts(["title", "slug"])
        .reset_index()
        .rename(
            columns={
                0: "num_charts_updated",
                "title": "post_title",
                "slug": "post_slug",
            }
        )
    )
    return df


if __name__ == "__main__":
    main()
