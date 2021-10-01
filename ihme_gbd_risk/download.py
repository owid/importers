import requests
import os
import time
from pathlib import Path

CURRENT_PATH = os.path.dirname(os.path.realpath(__file__))

from ihme_gbd_risk import INPATH

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

Path(INPATH).mkdir(parents=True, exist_ok=True)


for i in range(1, 71):
    fname = (
        "https://s3.healthdata.org/gbd-api-2019-public/707c49b3fd7207a33699db633359dbd5_files/IHME-GBD_2019_DATA-707c49b3-%s.zip"
        % i
    )
    print(fname)
    trycnt = 3
    while trycnt > 0:
        try:
            r = requests.get(fname)
            assert r.ok
            zname = os.path.join(INPATH, os.path.basename(fname))
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
