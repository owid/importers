###############################################################################
### Causes                                                                   ###
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
#    - Early Neonatal (0-6 days)
#    - Late Neonatal (7-27 days) - calculate 0-28 days
#    - Post Neonatal (28-364 days)
#    - <1 year
#    - 1-4 years
#
#    Metric:
#    - Number
#    - Rate
#    - Percent
#
#    Year: select all
#
#    Cause:
#    - Total All causes
#    - A.1.1: HIV/AIDS
#    - A.1.2: Sexually transmitted infections excluding HIV
#    - A.1.2.1: Syphillis
#    - A.2.1: Tuberculosis
#    - A.2.2: Lower respiratory infections
#    - A.2.3: Upper respiratory infections
#    - A.3: Enteric infections
#    - A.3.1: Diarrheal diseases
#    - A.3.2: Typhoid and paratyphoid
#    - A.3.2.1: Typhoid fever
#    - A.4.1: Malaria
#    - A.5: Other infectious diseases and all under this
#    - A.6.2: Neonatal disorders and all under
#    - A.7: Nutritional deficiencies
#    - A.7.1: Protein-energy malnutrition
#    - B.3: Chronic respiratory diseases
#    - B.4: Digestive diseases
#    - B.4.1 Cirrhosis and chronic liver diseases
#    - B.8 Diabetes and kidney diseases
#    - B.12.1: Congenital birth defects and all under this
#    - B.12.7: Sudden infant death syndrome
#
#    Context: Cause
#
#    Sex:
#    - Both
#    - Male
#    - Female
#
#    Location:
#    - select all countries
#    and then also:
#    - All "higher level" districts, e.g. Sub-Saharan Africa
#    - Also England, Scotland, Wales & Northern Ireland
#
#    Permalink: http://ghdx.healthdata.org/gbd-results-tool?params=gbd-api-2019-permalink/fc24dcf060876044fe3d5702a101ec3f

from ihme_gbd.ihme_gbd_child_mortality import INPATH, OUTPATH, CONFIGPATH, URL_STUB
from ihme_gbd.gbd_tools import make_dirs, download_data

# names of columns we are interested in


def main() -> None:
    make_dirs(inpath=INPATH, outpath=OUTPATH, configpath=CONFIGPATH)
    download_data(url=URL_STUB, inpath=INPATH)


if __name__ == "__main__":
    main()
