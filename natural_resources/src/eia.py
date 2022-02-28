"""Load and process EIA data and generate a file with yearly and a file with monthly data.

"""

import os

from natural_resources.src import READY_DIR

OUTPUT_YEARLY_FILE = os.path.join(READY_DIR, "eia_natural-resources-yearly.csv")
OUTPUT_MONTHLY_FILE = os.path.join(READY_DIR, "eia_natural-resources-monthly.csv")


def generate_yearly_data():
    pass


def generate_monthly_data():
    pass


def main():
    generate_yearly_data()


if __name__ == "__main__":
    main()
