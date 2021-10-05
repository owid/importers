################################################################################
# This dataset download is a bit hard to automate, a human needs to follow the
# steps below instead:
#
# 1. Go to the GHDx results tool: http://ghdx.healthdata.org/gbd-results-tool
# 2. Select the following:
#
#    Measure:
#    - Prevalence
#    - Incidence
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
#    - Percent
#
#    Year: select all
#
#    Cause: select all
#
#    Context: Cause
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


from ihme_gbd.ihme_gbd_prevalence import INPATH, OUTPATH, ENTFILE, CONFIGPATH, URL_STUB
from ihme_gbd.gbd_tools import make_dirs, download_data, load_and_filter

# names of columns we are interested in
fields = [
    "measure_name",
    "location_name",
    "sex_name",
    "age_name",
    "cause_name",
    "metric_name",
    "year",
    "val",
]


def main() -> None:
    make_dirs(inpath=INPATH, outpath=OUTPATH, configpath=CONFIGPATH)
    download_data(url=URL_STUB, inpath=INPATH)
    load_and_filter(inpath=INPATH, entfile=ENTFILE, column_fields=fields)


if __name__ == "__main__":
    main()
