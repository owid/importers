import argparse


def get_parser_args():
    parser = argparse.ArgumentParser(
        description="Download, process and upload FAO datasets.",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter
    )
    parser.add_argument(
        "catalog_folder",
        help=(
            "Path to local directory of walden project (clone it from https://github.com/owid/walden)."
        )
    )
    args = parser.parse_args()
    return args
