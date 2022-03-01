"""Load all intermediate datasets and generate natural resources dataset.

"""
import eia

from natural_resources.src import eia
from natural_resources.src import OUTPUT_DIR


def extract():
    eia.generate_yearly_dataset()
    eia.generate_monthly_dataset()


def transform():
    pass


def load():
    pass


def main():
    extract()
    # year_df, date_df = transform()
    # load(year_df, date_df)


if __name__ == "__main__":
    main()
