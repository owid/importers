import requests
import zipfile
import io

from who_ghe import INPATH


def main():
    download_data()


# Get data from Walden - gist for downloading data is here - https://gist.github.com/spoonerf/9646dce7452583472dc2ac8ddf210835


def download_data() -> None:
    url = "https://nyc3.digitaloceanspaces.com/walden/who/2021-07-01/ghe.zip"
    res = requests.get(url)
    assert res.ok
    z = zipfile.ZipFile(io.BytesIO(res.content))
    z.extractall(INPATH)
