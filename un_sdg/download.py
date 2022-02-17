"""
Functions for downloading UN SDG data in CSV format from the SDG API.
"""
import glob
import json
import os
import pandas as pd
import requests
import zipfile
import shutil

from PyPDF2 import PdfFileMerger
from io import BytesIO
from pathlib import Path
from un_sdg import INFILE, OUTPATH, METAPATH, METADATA_LOC, INPATH, DATASET_VERSION
from typing import List

base_url = "https://unstats.un.org/sdgapi"
keep_paths = []  # files not to be deleted


def main():
    delete_output(keep_paths)
    download_data()
    combine_data()
    delete_metadata()
    download_metadata()
    clean_metadata(METAPATH)


"""
delete_output():
* Function used to delete all files in OUTPATH except for those in 'keep_paths'
* Function gets base file name from file path
* Then gets the index of the files which are in both 'base_files' and 'keep_paths'
* Creates selection of filepaths from index
* Deletes files not in selection
"""


def delete_output(keep_paths: List[str]) -> None:
    output_files = glob.glob(os.path.join(OUTPATH, "**/*.*"), recursive=True)
    base_files = []
    for file in output_files:
        bf = os.path.basename(file)
        base_files.append(bf)
    ind_file = []
    for file in keep_paths:
        ind = base_files.index(file)
        ind_file.append(ind)
    file_sel = [output_files[i] for i in ind_file]
    clean_up = [x for x in output_files if x not in file_sel]
    for del_file in clean_up:
        os.remove(del_file)


def download_data() -> None:
    # retrieves all goal codes
    print("Retrieving SDG goal codes...")
    url = f"{base_url}/v1/sdg/Goal/List"
    res = requests.get(url)
    assert res.ok

    goals = json.loads(res.content)
    goal_codes = [str(goal["code"]) for goal in goals]
    # retrieves all area codes
    print("Retrieving area codes...")
    url = f"{base_url}/v1/sdg/GeoArea/List"
    res = requests.get(url)
    assert res.ok

    areas = json.loads(res.content)
    area_codes = [str(area["geoAreaCode"]) for area in areas]
    # retrieves csv with data for all codes and areas
    print("Retrieving data...")
    url = f"{base_url}/v1/sdg/Goal/DataCSV"
    for goal in goal_codes:
        res = requests.post(url, data={"goal": goal, "areaCodes": area_codes})
        print(f"Goal {goal} downloaded {res.ok}")
        assert res.ok
        df = pd.read_csv(BytesIO(res.content), low_memory=False)
        infile = os.path.join(INPATH, goal + "_un-sdg-" + DATASET_VERSION + ".csv.zip")
        df.to_csv(infile, index=False, compression="gzip")


def combine_data() -> None:
    all_files = glob.glob(f"{INPATH}/*{os.path.basename(os.path.normpath(INFILE))}")
    df_from_each_file = (
        pd.read_csv(f, sep=",", low_memory=False, compression="gzip") for f in all_files
    )
    df_merged = pd.concat(df_from_each_file, ignore_index=True)
    df_merged.to_csv(INFILE, compression="gzip")
    for file in all_files:
        os.remove(file)


def delete_metadata() -> None:
    if Path(METAPATH).is_dir():
        shutil.rmtree(METAPATH)


def download_metadata() -> None:
    # Download metadata
    zip_url = METADATA_LOC
    r = requests.get(zip_url)
    Path(METAPATH).mkdir(parents=True, exist_ok=True)
    with open(os.path.join(METAPATH, "sdg-metadata.zip"), "wb") as f:
        f.write(r.content)

    # Unzip metadata
    with zipfile.ZipFile(os.path.join(METAPATH, "sdg-metadata.zip"), "r") as zip_ref:
        zip_ref.extractall(METAPATH)

    # docx metadata is downloaded as well as pdf, this deletes the docx
    files_in_directory = os.listdir(METAPATH)
    filtered_files = [file for file in files_in_directory if not file.endswith(".pdf")]
    for file in filtered_files:
        path_to_file = os.path.join(METAPATH, file)
        os.remove(path_to_file)


def clean_metadata(metapath) -> None:
    # Some indicators have multiple associated pdfs, this combines these PDFs into one, e.g. Metadata-01-01-01a.pdf and Metadata-01-01-01b.pdf. If not done then un_sdg.core.extract_description() will not work.
    pdf_in_directory = os.listdir(metapath)
    for pdf in pdf_in_directory:
        pref = pdf[0:17]
        dup_pdf = [x for x in pdf_in_directory if x.startswith(pref)]
        if len(dup_pdf) > 1:
            merger = PdfFileMerger()
            for pdf in dup_pdf:
                merger.append(os.path.join(metapath, pdf))
            merger.write(os.path.join(metapath, pref + ".pdf"))
            merger.close()

    for i in pdf_in_directory[:]:
        if len(i) >= 22:
            os.remove(os.path.join(metapath, i))


if __name__ == "__main__":
    main()
