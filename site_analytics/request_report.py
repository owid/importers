import os
import pandas as pd
from typing import Optional
from dotenv import load_dotenv
from apiclient.discovery import build
from google.oauth2.credentials import Credentials

from site_analytics.utils import google_analytics_authenticate

load_dotenv()

GA_ACCOUNT_ID = os.getenv("GA_ACCOUNT_ID")
GA_PROPERTY_ID = os.getenv("GA_PROPERTY_ID")
GA_VIEW_ID = os.getenv("GA_VIEW_ID")


def execute_report_request(
    metric: str,
    dimension: str,
    start_date: str,
    end_date: str,
    filters_expression: Optional[str] = None,
    credentials: Optional[Credentials] = None,
) -> pd.DataFrame:
    """Request a single simple Google Analytics report with one metric and one
    dimension in a single time range.

    Uses the Google Analytics Reporting API v4. See
    https://developers.google.com/analytics/devguides/reporting/core/v4/rest/v4/reports/batchGet
    for valid values of the `metric`, `dimension`, `start_date`, etc parameters.
    """
    if not credentials:
        credentials = google_analytics_authenticate()
    analytics = build("analyticsreporting", "v4", credentials=credentials)
    ga_rows = []
    report_request = {
        "viewId": f"ga:{GA_VIEW_ID}",
        "dateRanges": [{"startDate": start_date, "endDate": end_date}],
        "metrics": [{"expression": f"ga:{metric}"}],
        "dimensions": [
            {"name": f"ga:{dimension}"},
        ],
        "pageSize": "100000",
        "orderBys": [
            {
                "fieldName": f"ga:{metric}",
                "sortOrder": "DESCENDING",
            }
        ],
    }
    if filters_expression:
        report_request["filtersExpression"] = filters_expression
    response = (
        analytics.reports()
        .batchGet(body={"reportRequests": [report_request]})
        .execute()
    )
    for row in response["reports"][0]["data"]["rows"]:
        dims = row["dimensions"]
        assert "(other)" not in dims
        val = row["metrics"][0]["values"][0]
        ga_rows.append([start_date, end_date] + dims + [val])
    while response["reports"][0].get("nextPageToken"):
        report_request["pageToken"] = response["reports"][0].get("nextPageToken")
        response = (
            analytics.reports()
            .batchGet(body={"reportRequests": [report_request]})
            .execute()
        )
        for row in response["reports"][0]["data"]["rows"]:
            dims = row["dimensions"]
            assert "(other)" not in dims
            val = row["metrics"][0]["values"][0]
            ga_rows.append([start_date, end_date] + dims + [val])
    df = pd.DataFrame(
        ga_rows,
        columns=["start_date", "end_date", dimension, metric],
    )
    try:
        df[metric] = df[metric].astype(float)
    except:
        pass
    df = df.sort_values(by=metric).reset_index(drop=True)
    return df
