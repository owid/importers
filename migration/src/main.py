# to run: python -m migration.src.main

import glob
import pandas as pd
from functools import reduce

import migration.src.unhcr as unhcr
import migration.src.un_desa as un_desa
import migration.src.unicef as unicef
import migration.src.idmc as idmc
import migration.src.wb_wdi as wdi
import migration.src.undesa_matrix as mig_matrix


def main():
    extract_migration_data()
    transform()
    mig_matrix.migration_matrix_by_destination()


def extract_migration_data() -> None:
    un_desa.international_migrants_by_destination()
    unhcr.refugees_by_destination()
    unhcr.refugees_by_destination_per_1000()
    unhcr.refugees_by_origin()
    unhcr.refugees_by_origin_per_1000()
    unhcr.asylum_seekers_by_origin()
    unhcr.asylum_seekers_by_origin_per_100000()
    unhcr.asylum_seekers_by_destination()
    unhcr.asylum_seekers_by_destination_per_100000()
    unhcr.resettlement_arrivals_by_destination()
    unhcr.resettlement_arrivals_by_destination_per_100000()
    unhcr.resettlement_arrivals_by_origin()
    unhcr.resettlement_arrivals_by_origin_per_100000()
    un_desa.share_of_pop_international_migrants_by_destination()
    un_desa.international_migrants_by_origin()
    un_desa.refugees_by_destination()
    un_desa.refugees_by_destination_per_1000()
    un_desa.net_migration_rate()
    un_desa.net_number_migrants()
    un_desa.child_migrants_by_destination()
    un_desa.child_migrants_by_destination_per_1000()
    un_desa.change_in_international_migrants_by_destination()
    un_desa.change_in_international_migrants_by_destination_per_1000()
    un_desa.change_in_international_migrants_by_origin()
    un_desa.change_in_international_migrants_by_origin_per_1000()
    unicef.under_eighteen_migrants_by_destination()
    unicef.under_eighteen_migrants_by_destination_per_1000()
    idmc.annual_internal_displacement_conflict()
    idmc.share_annual_internal_displacement_conflict()
    idmc.annual_internal_displacement_disaster()
    idmc.share_annual_internal_displacement_disaster()
    idmc.total_internal_displacement_conflict()
    idmc.share_total_internal_displacement_conflict()
    idmc.total_internal_displacement_disaster()
    idmc.share_total_internal_displacement_disaster()
    wdi.remittances_received_share_gdp()
    wdi.average_cost_sending_remittances_from_country()
    wdi.average_cost_sending_remittances_to_country()


def transform():
    year_dataframes = []
    for file in glob.glob("migration/ready/*.csv"):
        print(file)
        tmp_df = pd.read_csv(file)
        # print(tmp_df.shape)
        year_dataframes.append(tmp_df)

    year_df = reduce(
        lambda left, right: pd.merge(left, right, on=["Country", "Year"], how="outer"),
        year_dataframes,
    )

    year_df = year_df.rename(columns={"Country": "entity", "Year": "year"})

    cols = [
        "wdi_remittances_received_share_gdp",
        "wdi_average_cost_sending_remittances_to_country",
        "wdi_average_cost_sending_remittances_from_country",
        "undesa_child_migrants_by_destination_under_20_per_1000",
        "undesa_five_year_change_in_international_migrants_by_origin_per_1000",
        "undesa_five_year_change_in_international_migrants_by_destination_per_1000",
        "undesa_refugees_by_destination_per_1000",
        "share_idmc_total_internal_displacement_disaster",
        "unhcr_resettlement_arrivals_by_destination_per_100000",
        "idmc_share_annual_internal_displacement_conflict",
        "unhcr_asylum_seekers_by_origin_per_100000",
        "unicef_under_eighteen_migrants_by_destination_per_1000",
        "share_idmc_total_internal_displacement_conflict",
        "undesa_share_of_population_that_are_international_migrants_by_destination",
        "idmc_share_annual_internal_displacement_disaster",
        "undesa_child_migrants_by_destination_under_15_per_1000",
        "unhcr_refugees_by_origin_per_1000",
        "unhcr_resettlement_arrivals_by_origin_per_100000",
        "unhcr_asylum_seekers_by_destination_per_100000",
    ]
    year_df[cols] = year_df[cols].round(3)

    year_df.to_csv("migration/output/Migration.csv", index=False)


if __name__ == "__main__":
    main()
