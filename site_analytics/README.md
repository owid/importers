# Site analytics

Exposes simple site analytics, such as page views by slug URL on the [ourworldindata.org](https://ourworldindata.org) website. For internal use by OWID staff only.

This service uses the [Google Analytics Reporting API v4](https://developers.google.com/analytics/devguides/reporting/core/v4). 

## Setup

In order to use this service, you must:

1. Have a client secrets file located in `site_analytics/config/credentials/owid-analytics-client-secrets.json`. 
2. Add the following variables to `.env` at the root of this repository:

```bash
GA_VIEW_ID = "{GA_VIEW_ID}"
GA_ACCOUNT_ID = "{GA_ACCOUNT_ID}"
GA_PROPERTY_ID = "{GA_PROPERTY_ID}"
```

Ask an OWID developer for these files/variables (available to OWID staff only).


## Examples

Execute a single simple [Google Analytics report request](https://developers.google.com/analytics/devguides/reporting/core/v4/rest/v4/reports/batchGet) with one metric and one dimension over one time range:

```python
from site_analytics.request_report import execute_report_request

df = execute_report_request(
  metric="pageviews",
  dimension="pagePath",
  start_date="7daysAgo",
  end_date="yesterday",
  filters_expression="ga:dimension1==0"  # excludes views of embedded pages
)
print(df.tail(5))
#       start_date   end_date             pagePath  pageviews
# 41932   7daysAgo  yesterday         /covid-cases    90053.0
# 41933   7daysAgo  yesterday  /covid-vaccinations   123601.0
# 41934   7daysAgo  yesterday         /coronavirus   167887.0
# 41935   7daysAgo  yesterday                    /   176392.0
# 41936   7daysAgo  yesterday  /covid-vaccinations   711477.0

```

For acceptable values of the `metric`, `dimension`, etc parameters, check out the [Google Analytics UA query explorer](https://ga-dev-tools.web.app/query-explorer/).

> Note: set `filters_expression="ga:dimension1==0"` to exclude visits to embedded OWID pages.
