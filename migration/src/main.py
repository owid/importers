import migration.src.unhcr as unhcr
import migration.src.un_desa as un_desa


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
