################################################################################
# This dataset download is a bit hard to automate, a human needs to follow the
# steps below instead:
#
# 1. Go to the GHDx results tool: http://ghdx.healthdata.org/gbd-results-tool
# 2. Select the following:
#
#    Measure:
#    - Deaths
#    - DALYs (Disability-Adjusted Life Years)
#
#    Age:
#    - All Ages
#    - Age-standardized
#    - Under 5
#    - 5-14 years
#    - 15-49 years
#    - 50-69 years
#    - 70+ years
#
#    Metric:
#    - Number
#    - Rate
#
#    Year: select all
#
#    Risks: select all
#
#    Cause: Total All Causes
#
#    Context: Risk
#
#    Location:
#    - select all countries
#    and then also:
#    - All "higher level" districts, e.g. Sub-Saharan Africa
#    - Also England, Scotland, Wales & Northern Ireland
#
#    Sex:
#    - Both
#

from ihme_gbd.ihme_gbd_risk import INPATH, OUTPATH, CONFIGPATH, URL_STUB
from ihme_gbd.gbd_tools import make_dirs, download_data

# names of columns we are interested in
fields = [
    "measure_name",
    "location_name",
    "sex_name",
    "age_name",
    "rei_name",
    "metric_name",
    "year",
    "val",
]


def main() -> None:
    make_dirs(inpath=INPATH, outpath=OUTPATH, configpath=CONFIGPATH)
    download_data(url=URL_STUB, inpath=INPATH)


if __name__ == "__main__":
    main()
