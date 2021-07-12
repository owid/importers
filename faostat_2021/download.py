"""
This script downloads selected datasets from FAO and uploads them into Walden.

Additionally, it creates metadata files and adds them to the local project Walden directory (clone it from
https://github.com/owid/walden).

Run it as `python -m faostat_2021.download`
"""


from datetime import datetime
import hashlib
import os
import json
import tempfile

import requests


DATASET_NAMES = [
    "Food Security: Suite of Food Security Indicators",
    "Production: Crops",
]


class FAODataset:
    namespace: str = "faostat"
    url: str =  "http://www.fao.org/faostat/en/#data"
    source_name: str = "Food and Agriculture Organization of the United Nations"
    _extra_metadata = {}

    def __init__(self, dataset_metadata, groups):
        self._dataset_metadata = dataset_metadata
        self.publication_date = dataset_metadata['DateUpdate']
        self.publication_year = datetime.strptime(dataset_metadata['DateUpdate'], "%Y-%m-%d").year
        self.owid_folder = f"http://walden.nyc3.digitaloceanspaces.com/{self.namespace}/{self.publication_year}"
        self.source_data_url = dataset_metadata['FileLocation']
        self.description = dataset_metadata['DatasetDescription']
        self.original_name = dataset_metadata['DatasetName']
        self.name = f"{self.original_name} - FAO ({self.publication_year})"
        self.short_name =  f"{self.namespace}_{self.build_shortname_suffix(groups)}"

    @property
    def owid_data_url(self):
        """URL to file hosted at Walden."""
        return f"{self.owid_folder}/{(self.filename)}"

    @property
    def filename(self):
        """Filename.
        
        E.g.: From `some/url/file.csv` to `file.csv`.
        """
        return os.path.basename(self._dataset_metadata['FileLocation'])
    
    def build_shortname_suffix(self, groups):
        """Build short name for dataset.
        
        Uses namespace and group name to create a short name.
        """
        return self._dataset_metadata["DatasetCode"]
        # shortname_map = {
        #     group['domain_code']: re.sub(r'\s?&\s?', '', re.sub(r'(\s|,\s?)', '-', group['group_name'].lower()))
        #     for group in groups
        # }
        # return shortname_map[self._dataset_metadata['DatasetCode']]

    @property
    def metadata(self):
        """Metadata file.
        
        Required by the dataset index catalog (more info at https://github.com/owid/walden).
        """
        return {
            "namespace": self.namespace,
            "short_name": self.short_name,
            "name": self.name,
            "description": self.description,
            "source_name": self.source_name,
            "publication_year": self.publication_year,
            "publication_date": self.publication_date,
            "url": self.source_data_url,
            "owid_data_url": self.owid_data_url,
            "source_data_url": self.source_data_url,
            **self._extra_metadata
        }

    def add_metadata(self, key: str, value: str):
        """Add extra field to metadata dictionary.

        Useful to add, for instance, the md5 hash after downloading the dataset.

        Args:
            key (str): Dictionary key.
            value (str): Dictionary value.
        """
        self._extra_metadata[key] = value
        
    def catalog_path(self, catalog_folder: str):
        """Path to the folder where the dataset metadata file should be added.

        Args:
            catalog_folder (str): walden project local folder (clone project from https://github.com/owid/walden).
        """
        return f"{catalog_folder}/{self.namespace}/{self.publication_year}/"
    
    def download(self, output_folder: str):
        """Download dataset.

        Args:
            output_folder (str): Folder where to store downloaded dataset.

        Returns:
            str: Complete path to dataset in the local folder.
        """
        print(f"==> Downloading data")
        r = requests.get(self.source_data_url)
        output_path = os.path.join(output_folder, self.filename)
        print(f"{self.source_data_url} -> {output_path}")
        with open(output_path, 'wb') as f:
            f.write(r.content)
        return output_path

    def upload_to_walden(self, filepath: str):
        """Upload dataset to Walden.

        Args:
            filepath (str): Path to Walden remote folder.
        """
        print("==> Uploading data to Walden (NOT IMPLEMENTED)")
        print(f"{filepath} -> {self.owid_data_url}")

    def add_md5_to_metadata(self, filepath: str):
        """Add md5 hash of dataset file to metadata.

        Args:
            filepath (str): Path to dataset in the local folder.
        """
        print("==> Uploading metadata (md5)")
        with open(filepath, "rb") as f:
            md5 = hashlib.md5(f.read()).hexdigest()
        self.add_metadata("md5", md5)

    def add_metadata_to_catalog(self, catalog_folder: str):
        """Add metadata to walden index catalog.

        Args:
            catalog_folder (str): walden project local folder (clone project from https://github.com/owid/walden).
        """
        print("==> Adding metadata to Walden repository")
        catalog_path = self.catalog_path(catalog_folder)
        os.makedirs(catalog_path, exist_ok=True)
        metadata_file = os.path.join(catalog_path, f"{self.short_name}.json")
        print(f" -> {metadata_file}")
        with open(metadata_file, 'w') as f:
            json.dump(self.metadata, f, indent=2)
            f.write('\n')

    def download_pipeline(self, catalog_folder: str):
        """Run download pipeline.

        This downloads the dataset from source, uploads it to Walden, creates the corresponding metadata file and
        places it in the walden local project repository.

        Args:
            catalog_folder (str): walden project local folder (clone project from https://github.com/owid/walden).
            tmp_dir (str): Folder to use to store downloaded dataset. By default creates a temporary one.
        """
        with tempfile.TemporaryDirectory() as f:
            tmp_path = self.download(f)
            # Upload data to Walden
            self.upload_to_walden(tmp_path)
            # Create metadata
            self.add_md5_to_metadata(tmp_path)
            # Upload metadata to Walden catalog
            self.add_metadata_to_catalog(catalog_folder)


def get_groups_metadata():
    url_groups_and_domains = "http://fenixservices.fao.org/faostat/api/v1/en/groupsanddomains?section=download"
    groups = requests.get(url_groups_and_domains).json()['data']
    return groups

def get_datasets_metadata():
    url_datasets = "http://fenixservices.fao.org/faostat/static/bulkdownloads/datasets_E.json"
    datasets = requests.get(url_datasets).json()['Datasets']['Dataset']
    return datasets

def download_data(catalog_folder):
    groups = get_groups_metadata()
    datasets = get_datasets_metadata()
    for _, dataset in enumerate(datasets):
        # Build FAODataset instance
        ds = FAODataset(dataset, groups)
        # print(_, ds.original_name)
        if ds.original_name in DATASET_NAMES:
            # Run download pipeline
            ds.download_pipeline(catalog_folder)


def main(catalog_folder):
    download_data(catalog_folder)
