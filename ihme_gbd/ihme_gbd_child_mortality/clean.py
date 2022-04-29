from ihme_gbd.ihme_gbd_child_mortality import (
    CLEAN_ALL_VARIABLES,
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
    FILTER_FIELDS,
    COUNTRY_COL,
    CALCULATE_OWID_VARS,
    PARENT_DIR,
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
    vars = create_variables(
        inpath=INPATH,
        filter_fields=FILTER_FIELDS,
        outpath=OUTPATH,
        clean_all_vars=CLEAN_ALL_VARIABLES,
        configpath=CONFIGPATH,
        calculate_owid_vars=CALCULATE_OWID_VARS,
    )
    create_datapoints(
        vars,
        inpath=INPATH,
        parent_dir=PARENT_DIR,
        configpath=CONFIGPATH,
        outpath=OUTPATH,
        calculate_owid_vars=CALCULATE_OWID_VARS,
    )
    create_distinct_entities(parent_dir=PARENT_DIR, outpath=OUTPATH)


if __name__ == "__main__":
    main()
