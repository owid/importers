import glob
import pandas as pd
from functools import reduce

import migration.src.unhcr as unhcr
import migration.src.un_desa as un_desa
import migration.src.unicef as unicef
import migration.src.idmc as idmc
import migration.src.wb_wdi as wdi


def main():
    extract_migration_data()
    transform()


def extract_migration_data() -> None:
    un_desa.international_migrants_by_destination()
    unhcr.refugees_by_destination()
    unhcr.refugees_by_destination_per_capita()
    unhcr.refugees_by_origin()
    unhcr.refugees_by_origin_per_capita()
    unhcr.asylum_applications_by_origin()
    unhcr.asylum_applications_by_origin_per_capita()
    unhcr.asylum_applications_by_destination()
    unhcr.asylum_applications_by_destination_per_capita()
    unhcr.resettlement_arrivals_by_destination()
    unhcr.resettlement_arrivals_by_destination_per_capita()
    unhcr.resettlement_arrivals_by_origin()
    unhcr.resettlement_arrivals_by_origin_per_capita()
    un_desa.share_of_pop_international_migrants_by_destination()
    un_desa.international_migrants_by_origin()
    un_desa.refugees_by_destination()
    un_desa.refugees_by_destination_per_capita()
    un_desa.average_annual_change_international_migrants_by_destination()
    un_desa.average_annual_change_international_migrants_by_destination_per_capita()
    un_desa.average_annual_change_international_migrants_by_origin()
    un_desa.average_annual_change_international_migrants_by_origin_per_capita()
    un_desa.net_migration_rate()
    un_desa.net_number_migrants()
    un_desa.child_migrants_by_destination()
    un_desa.child_migrants_by_destination_per_capita()
    un_desa.change_in_international_migrants_by_destination()
    un_desa.change_in_international_migrants_by_destination_per_capita()
    un_desa.change_in_international_migrants_by_origin()
    un_desa.change_in_international_migrants_by_origin_per_capita()
    unicef.under_eighteen_migrants_by_destination()
    unicef.under_eighteen_migrants_by_destination_per_capita()
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

    cols = ["wdi_remittances_received_share_gdp"]
    year_df[cols] = year_df[cols].round(3)

    year_df.to_csv("migration/output/Migration.csv", index=False)


if __name__ == "__main__":
    main()
