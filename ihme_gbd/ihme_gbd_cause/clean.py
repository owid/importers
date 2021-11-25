from ihme_gbd.ihme_gbd_cause import (
    INPATH,
    DATASET_NAME,
    DATASET_AUTHORS,
    DATASET_VERSION,
    OUTPATH,
    DATASET_RETRIEVED_DATE,
    CONFIGPATH,
    ENTFILE,
    CURRENT_PATH,
)

from ihme_gbd.gbd_tools import (
    create_datasets,
    create_sources,
    create_variables_datapoints,
    create_distinct_entities,
    load_and_filter,
)

filter_fields = [
    "measure_name",
    "location_name",
    "sex_name",
    "age_name",
    "cause_name",
    "metric_name",
    "year",
    "val",
]


def main() -> None:
    print(CURRENT_PATH)
    print(INPATH)
    load_and_filter(inpath=INPATH, entfile=ENTFILE, column_fields=filter_fields)
    create_datasets(
        dataset_name=DATASET_NAME,
        dataset_authors=DATASET_AUTHORS,
        dataset_version=DATASET_VERSION,
        outpath=OUTPATH,
    )
    create_sources(dataset_retrieved_date=DATASET_RETRIEVED_DATE, outpath=OUTPATH)
    create_variables_datapoints(inpath=INPATH, configpath=CONFIGPATH, outpath=OUTPATH)
    create_distinct_entities(configpath=CONFIGPATH, outpath=OUTPATH)


if __name__ == "__main__":
    main()
