import pytest

import os
import pandas as pd
from apiclient.discovery import build

from site_analytics.utils import google_analytics_authenticate
from site_analytics.request_report import execute_report_request

from dotenv import load_dotenv

load_dotenv()

GA_ACCOUNT_ID = os.getenv("GA_ACCOUNT_ID")
GA_PROPERTY_ID = os.getenv("GA_PROPERTY_ID")


@pytest.fixture(scope="module")
def credentials():
    yield google_analytics_authenticate()


def test_dimension1_name(credentials):
    nm = (
        build("analytics", "v3", credentials=credentials)
        .management()
        .customDimensions()
        .get(
            accountId=GA_ACCOUNT_ID,
            webPropertyId=GA_PROPERTY_ID,
            customDimensionId="ga:dimension1",
        )
        .execute()["name"]
    )
    assert nm == "Page is embedded"


@pytest.fixture(scope="module")
def df_simple_request(credentials):
    df = execute_report_request(
        metric="pageviews",
        dimension="pagePath",
        start_date="yesterday",
        end_date="yesterday",
        filters_expression=None,
        credentials=credentials,
    )
    yield df


def test_is_frame(df_simple_request):
    assert type(df_simple_request) == pd.DataFrame


def test_gt_zero_rows(df_simple_request):
    assert df_simple_request.shape[0] > 0
