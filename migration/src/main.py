import migration.src.unhcr as unhcr
import migration.src.un_desa as un_desa
import migration.src.unicef as unicef
import migration.src.idmc as idmc


def extract() -> None:
    unhcr.refugees_by_destination()
    unhcr.refugees_by_destination_per_capita()
    unhcr.refugees_by_origin()
    unhcr.refugees_by_origin_per_capita()
    un_desa.international_migrants_by_destination()
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
