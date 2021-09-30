import requests
import os
import time
from pathlib import Path

CURRENT_PATH = os.path.dirname(os.path.realpath(__file__))

################################################################################
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
# 3. The tool will then create a dataset for you in chunks. Once it's finished
#    (which may take several hours) this command might be helpful to download
#    them all:
#
#         for i in {1..<number of files>}; do
#             wget http://s3.healthdata.org/gbd-api-2017-public/<hash of a file...>-$i.zip;
#         done
#
# 4. Then, unzip them all and put them in a single folder. This should be the
#    `csv_dir` specified below. Helpful command:
#
#         unzip \*.zip -x citation.txt -d csv/
#

Path("ihme_gbd/input/causes").mkdir(parents=True, exist_ok=True)


for i in range(1, 20):
    fname = (
        "https://s3.healthdata.org/gbd-api-2019-public/6bc81b0cecef147c44df55608fe573f3_files/IHME-GBD_2019_DATA-6bc81b0c-%s.zip"
        # "https://s3.healthdata.org/gbd-api-2019-public/f01933bdc3867c8239be6fa3f87c9485_files/IHME-GBD_2019_DATA-f01933bd-%s.zip"
        % i
    )
    print(fname)
    trycnt = 3
    while trycnt > 0:
        try:
            r = requests.get(fname)
            assert r.ok
            zname = os.path.join("ihme_gbd/input/causes", os.path.basename(fname))
            print(zname)
            zfile = open(zname, "wb")
            zfile.write(r.content)
            zfile.close()
            break
        except requests.exceptions.ChunkedEncodingError:
            if trycnt <= 0:
                print("Failed to retrieve: " + fname + "\n")  # done retrying
            else:
                trycnt -= 1  # retry
            time.sleep(0.5)
