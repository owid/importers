from ihme_gbd.ihme_gbd_risk import (
    INPATH,
    DATASET_NAME,
    DATASET_AUTHORS,
    DATASET_VERSION,
    OUTPATH,
    DATASET_RETRIEVED_DATE,
    CONFIGPATH,
    ENTFILE,
    CURRENT_PATH,
    DATAPOINTS_DIR,
)

from ihme_gbd.gbd_tools import (
    create_datapoints,
    create_datasets,
    create_sources,
    create_variables,
    create_distinct_entities,
    find_countries,
    delete_datapoints,
)

FILTER_FIELDS = [
    "measure_name",
    "location_name",
    "sex_name",
    "age_name",
    "cause_name",
    "metric_name",
    "year",
    "val",
]

COUNTRY_COL = "location_name"


def main() -> None:
    print(CURRENT_PATH)
    print(INPATH)
    delete_datapoints(DATAPOINTS_DIR)
    find_countries(country_col=COUNTRY_COL, inpath=INPATH, entfile=ENTFILE)
    create_datasets(
        dataset_name=DATASET_NAME,
        dataset_authors=DATASET_AUTHORS,
        dataset_version=DATASET_VERSION,
        outpath=OUTPATH,
    )
    create_sources(dataset_retrieved_date=DATASET_RETRIEVED_DATE, outpath=OUTPATH)
    vars = create_variables(inpath=INPATH, filter_fields=FILTER_FIELDS, outpath=OUTPATH)
    create_datapoints(vars, inpath=INPATH, configpath=CONFIGPATH, outpath=OUTPATH)
    create_distinct_entities(configpath=CONFIGPATH, outpath=OUTPATH)


if __name__ == "__main__":
    main()
