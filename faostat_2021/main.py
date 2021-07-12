from faostat_2021 import download
from faostat_2021._parsers import get_parser_args


def main(catalog_folder: str):
    download.main(catalog_folder)


if __name__ == "__main__":
    args = get_parser_args()
    main(args.catalog_folder)
