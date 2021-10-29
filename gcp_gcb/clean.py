"""Cleans a dataset, sources, variables, and datavalues and exports to csv
in preparation for upsert to the database.

Usage:

    >>> from gcp_gcb import clean
    >>> clean.main()

Command line usage:

    python -m gcp_gcb.clean

"""

import os
import re
import logging
from copy import deepcopy
from typing import Dict, List
import simplejson as json
import numpy as np
import pandas as pd
from pandas.api.types import is_integer_dtype, is_numeric_dtype
from tqdm import tqdm

from gcp_gcb import (
    CONFIGPATH,
    INPATH,
    OUTPATH,
    DATASET_DIR,
    DATASET_NAME,
    DATASET_AUTHORS,
    DATASET_VERSION,
    DATASET_LINK,
    DATASET_RETRIEVED_DATE,
)
from utils import (
    camel_case2snake_case,
    delete_output,
    get_owid_variable,
    get_owid_variable_source,
    get_distinct_entities,
)

logging.basicConfig()
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)


def main():
    c = Cleaner()
    c.clean()
    c.write()


class Cleaner:
    """Cleans a dataset, sources, variables, and datavalues and exports to csv
    in preparation for upsert to the database.

    Usage:

        >>> from gcp_gcb.clean import Cleaner
        >>> c = Cleaner()
        >>> c.clean()
        INFO:gcp_gcb.clean:dataset has been updated
        INFO:gcp_gcb.clean:sources have been updated
        INFO:gcp_gcb.clean:variables have been updated
        INFO:gcp_gcb.clean:datavalues have been updated
        >>> # the `dataset`, `sources`, `variables`, and `datavalues` properties
        >>> # are now non-null:
        >>> c.dataset
        {'id': 0, 'name': 'Global Carbon Budget - Global Carbon Project (2020)'}
        >>> c.variables.head()
           id                              name    unit  ...  source_id
        0   0              Annual CO2 emissions  tonnes  ...          0
        1   1    Annual CO2 emissions from coal  tonnes  ...          0
        2   2     Annual CO2 emissions from oil  tonnes  ...          0
        3   3     Annual CO2 emissions from gas  tonnes  ...          0
        4   4  Annual CO2 emissions from cement  tonnes  ...          0
        >>> c.write()
        INFO:gcp_gcb.clean:Wrote datasets to gcp_gcb/output/datasets.csv. (n=1)
        INFO:gcp_gcb.clean:Wrote sources to gcp_gcb/output/sources.csv. (n=4)
        INFO:gcp_gcb.clean:Wrote variables to gcp_gcb/output/variables.csv.
            (n=66)
        INFO:gcp_gcb.clean:Saving data values to csv for each variable...
        INFO:gcp_gcb.clean:Wrote data values to csv for 66 variables. CSVs
            written to: gcp_gcb/output/datapoints.
        INFO:gcp_gcb.clean:Wrote distinct entities to
            gcp_gcb/output/distinct_countries_standardized.csv.
    """

    def __init__(self, delete_existing_output: bool = True):
        self.delete_existing_output = delete_existing_output

    @property
    def dataset(self) -> dict:
        return self._dataset

    @property
    def sources(self) -> Dict[str, dict]:
        return self._sources

    @property
    def variables(self) -> pd.DataFrame:
        return self._variables

    @property
    def datavalues(self) -> pd.DataFrame:
        return self._datavalues

    @property
    def delete_existing_output(self) -> bool:
        """if true, deletes existing files in {DATASET_DIR}/output prior to
        execution."""
        return self._delete_existing_output

    @dataset.setter
    def dataset(self, value: dict) -> None:
        assert isinstance(value, dict)
        self._dataset = value
        logger.info("dataset has been updated")

    @sources.setter
    def sources(self, value: Dict[str, dict]) -> None:
        assert isinstance(value, dict)
        self._sources = value
        logger.info("sources have been updated")

    @variables.setter
    def variables(self, value: pd.DataFrame) -> None:
        assert isinstance(value, pd.DataFrame)
        self._variables = value
        logger.info("variables have been updated")

    @datavalues.setter
    def datavalues(self, value: pd.DataFrame) -> None:
        assert isinstance(value, pd.DataFrame)
        self._datavalues = value
        logger.info("datavalues have been updated")

    @delete_existing_output.setter
    def delete_existing_output(self, value: bool) -> None:
        assert isinstance(value, bool)
        self._delete_existing_output = value

    def clean(self) -> None:
        """constructs dataset, sources, variables, and datavalues."""
        if not os.path.exists(OUTPATH):
            os.makedirs(OUTPATH)

        if self.delete_existing_output:
            delete_output(DATASET_DIR)

        self.construct_dataset()
        self.construct_sources()
        self.construct_variables()
        self.construct_datavalues()

    def write(self) -> None:
        """writes dataset, sources, variables, and datavalues to disk as csv
        files."""
        self.check_keys()
        self.write_dataset()
        self.write_sources()
        self.write_variables()
        self.write_datavalues()
        self.write_distinct_entities()

    def check_keys(self):
        """checks ids/names for consistent usage across the dataset, sources,
        variables, and datavalues properties.

        e.g. all source ids should appear in the `source_id` column of
        self.variables, and vice versa.
        """
        assert (
            self.dataset is not None
            and self.sources is not None
            and self.variables is not None
            and self.datavalues is not None
        )

        # checks that keys are unique
        source_ids = [src["id"] for src in self.sources.values()]
        var_ids = self.variables["id"].tolist()
        var_names = self.variables["name"].tolist()
        assert len(source_ids) == len(set(source_ids))
        assert len(var_ids) == len(set(var_ids))
        assert len(var_names) == len(set(var_names))

        # all dataset ids should be in the sources list, and vice versa.
        dataset_ids = set({self.dataset["id"]})
        source_dataset_ids = set([src["dataset_id"] for src in self.sources.values()])
        assert len(dataset_ids.difference(source_dataset_ids)) == 0
        assert len(source_dataset_ids.difference(dataset_ids)) == 0

        # all dataset ids should be in the variables dataframe, and vice versa.
        dataset_ids = set({self.dataset["id"]})
        var_dataset_ids = set(self.variables["dataset_id"].unique())
        assert len(dataset_ids.difference(var_dataset_ids)) == 0
        assert len(var_dataset_ids.difference(dataset_ids)) == 0

        # all source ids should be in the variables dataframe, and vice versa.
        source_ids = set([src["id"] for src in self.sources.values()])
        var_source_ids = set(self.variables["source_id"].unique())
        assert len(source_ids.difference(var_source_ids)) == 0
        assert len(var_source_ids.difference(source_ids)) == 0

        # all variable names should be in the datavalues dataframe, and vice versa.
        var_names = set(self.variables["name"].unique())
        dv_var_names = set(self.datavalues["variable"].unique())
        assert len(var_names.difference(dv_var_names)) == 0
        assert len(dv_var_names.difference(var_names)) == 0

    def construct_dataset(self) -> None:
        """constructs the dataset property in preparation for uploading the
        sources to the `datasets` database table."""
        d = {"id": 0, "name": f"{DATASET_NAME} - {DATASET_AUTHORS} ({DATASET_VERSION})"}
        self.dataset = d

    def construct_sources(self) -> None:
        """Constructs a list of sources in preparation for uploading the
        sources to the `sources` database table.
        """
        assert self.dataset is not None
        default_data_publisher_source = "Friedlingstein et al. (2020)"
        gdp_source = get_owid_variable_source(DataValuesCleaner().gdp_id)
        population_source = get_owid_variable_source(DataValuesCleaner().population_id)
        # primary_energy_source = get_owid_variable_source(DataValuesCleaner().primary_energy_id)
        # note: we hard-code primary_energy_source_* name and link variables
        # because the name and link retrieved from OWID are either null or
        # "ugly".
        primary_energy_source_name = "BP and Shift Energy Data Portal"
        primary_energy_source_link = (
            "https://www.bp.com/en/global/corporate/energy-economics/"
            "statistical-review-of-world-energy, "
            "https://www.theshiftdataportal.org/energy"
        )
        co2_consumption_additional_info = (
            "Consumption-based CO2 emissions have been constructed by "
            f"the {DATASET_AUTHORS} using the methodology described in:\n\n"
            "Peters, GP, Minx, JC, Weber, CL and Edenhofer, O 2011. Growth in "
            "emission transfers via international trade from 1990 to 2008. "
            "Proceedings of the National Academy of Sciences 108, 8903-8908. "
            "http://www.pnas.org/content/108/21/8903.abstract"
        )

        def _construct_source(
            name: str,
            dataPublishedBy: str,
            dataPublisherSource: str,
            appendAdditionalInfo: str = "",
        ) -> dict:
            return {
                "id": 0,
                "dataset_id": self.dataset["id"],
                "name": name,
                "description": {
                    "link": DATASET_LINK,
                    "retrievedDate": DATASET_RETRIEVED_DATE,
                    "additionalInfo": (
                        f"The {DATASET_NAME} dataset is available at "
                        f"{DATASET_LINK}."
                        "\n\n"
                        f"Full reference for the {self.dataset['name']} "
                        "dataset: Friedlingstein et al, Global Carbon Budget "
                        "2020, Earth Syst. Sci. Data, 12, 3269–3340, "
                        "https://doi.org/10.5194/essd-12-3269-2020, 2020."
                        "\n\n"
                        "Our World in Data have renamed the category "
                        '"bunker fuels" as "International transport" '
                        "for improved clarity, which includes emissions "
                        "from international aviation and shipping."
                        + appendAdditionalInfo
                    ),
                    "dataPublishedBy": dataPublishedBy,
                    "dataPublisherSource": dataPublisherSource,
                },
            }

        sources = {
            "primary": _construct_source(
                name=DATASET_AUTHORS,
                dataPublishedBy=self.dataset["name"],
                dataPublisherSource=default_data_publisher_source,
            ),
            "derived": _construct_source(
                name=f"Our World in Data based on the {DATASET_AUTHORS}",
                dataPublishedBy=f"Our World in Data based on the {DATASET_AUTHORS}",
                dataPublisherSource=default_data_publisher_source,
            ),
            "derived_population": _construct_source(
                name=f"Our World in Data based on the {DATASET_AUTHORS}",
                dataPublishedBy=f"Our World in Data based on the {DATASET_AUTHORS}",
                dataPublisherSource=default_data_publisher_source,
                appendAdditionalInfo=(
                    "\n\nPopulation figures are sourced from "
                    f"{population_source['source']['dataPublishedBy']}. "
                    f"Links: {population_source['source']['link']}"
                ),
            ),
            "derived_gdp": _construct_source(
                name=f"Our World in Data based on the {DATASET_AUTHORS} and {gdp_source['s_name']}",
                dataPublishedBy=f"Our World in Data based on the {DATASET_AUTHORS} and {gdp_source['s_name']}",
                dataPublisherSource=f"{default_data_publisher_source} and {gdp_source['s_name']}",
                appendAdditionalInfo=(
                    f"\n\nGDP figures are sourced from "
                    f"{gdp_source['source']['dataPublishedBy']} "
                    f"Link: {gdp_source['source']['link']}"
                ),
            ),
            "derived_primary_energy": _construct_source(
                name=f"Our World in Data based on the {DATASET_AUTHORS}, {primary_energy_source_name}",
                dataPublishedBy=f"Our World in Data based on the {DATASET_AUTHORS}, {primary_energy_source_name}",
                dataPublisherSource=f"{default_data_publisher_source}, {primary_energy_source_name}",
                appendAdditionalInfo=(
                    "\n\nPrimary energy consumption figures are sourced from "
                    f"{primary_energy_source_name}. "
                    f"Link: {primary_energy_source_link}"
                ),
            ),
        }

        cons_sources = {}
        for k, source in sources.items():
            cons_sources[f"{k}_co2consumption"] = deepcopy(source)
            cons_sources[f"{k}_co2consumption"]["description"][
                "additionalInfo"
            ] += f"\n\n{co2_consumption_additional_info}"

        sources.update(cons_sources)

        with open(os.path.join(CONFIGPATH, "variables_to_clean.json")) as f:
            source_keys = [
                v.get("cleaningMetadata", {}).get("sourceKey")
                for v in json.load(f)["variables"]
                if v.get("cleaningMetadata", {}).get("sourceKey")
            ]
            sources = {k: v for k, v in sources.items() if k in source_keys}

        for i, source in enumerate(sources.values()):
            source["description"] = json.dumps(source["description"], ignore_nan=True)
            source["id"] = i

        self.sources = sources

    def construct_variables(self) -> None:
        """constructs a dataframe of variables in preparation for uploading to
        the `variables` database table."""
        assert self.dataset is not None
        assert self.sources is not None
        with open(os.path.join(CONFIGPATH, "variables_to_clean.json"), "r") as f:
            vars_to_clean = json.load(f)["variables"]
            zero_filled_vars = []
            for var in vars_to_clean:
                if pd.notnull(var.get("cleaningMetadata", {}).get("fillna")):
                    zero_filled_var = deepcopy(var)
                    zero_filled_var["name"] = f"{var['name']} (zero filled)"
                    zero_filled_var["cleaningMetadata"][
                        "rawName"
                    ] = f"{var['cleaningMetadata']['rawName']} (zero filled)"
                    zero_filled_vars.append(zero_filled_var)
            vars_to_clean += zero_filled_vars
        df = pd.DataFrame(vars_to_clean)
        df["dataset_id"] = self.dataset["id"]

        df["source_id"] = df["cleaningMetadata"].apply(
            lambda x: self.sources[x["sourceKey"]]["id"]
        )

        if "display" in df.columns:
            df["display"] = df["display"].apply(
                lambda x: json.dumps(x, ignore_nan=True) if pd.notnull(x) else None
            )

        if "id" not in df.columns:
            df["id"] = list(range(0, df.shape[0]))

        df.drop(columns=["old", "cleaningMetadata"], errors="ignore", inplace=True)

        # converts column names to snake case b/c this is what is expected in the
        # `standard_importer.import_dataset` module.
        df.columns = df.columns.map(camel_case2snake_case)

        required_fields = ["id", "name", "dataset_id", "source_id"]
        for field in required_fields:
            assert field in df.columns, f"`{field}` does not exist."
            assert df[field].notnull().all(), (
                f"The following variables have a null `{field}` field:\n"
                f"{df.loc[df[field].isnull(), required_fields]}"
            )

        df = df.set_index(["id", "name"]).reset_index().sort_values("name")
        self.variables = df

    def construct_datavalues(self) -> None:
        """constructs a dataframe of entity-date datavalues in preparation for
        uploading to the `data_values` database table."""
        self.datavalues = DataValuesCleaner().clean()

    def write_dataset(self) -> None:
        """writes dataset to csv."""
        assert self.dataset is not None
        fpath = os.path.join(OUTPATH, "datasets.csv")
        df = pd.DataFrame([self.dataset])
        df.to_csv(fpath, index=False)
        logger.info(f"Wrote datasets to {fpath}. (n={df.shape[0]})")

    def write_sources(self) -> None:
        """writes sources to csv."""
        assert self.sources is not None
        fpath = os.path.join(OUTPATH, "sources.csv")
        self.variables.to_csv(fpath, index=False)
        df = pd.DataFrame(self.sources.values())
        df.to_csv(fpath, index=False)
        logger.info(f"Wrote sources to {fpath}. (n={df.shape[0]})")

    def write_variables(self) -> None:
        """writes variables to csv."""
        assert self.variables is not None
        fpath = os.path.join(OUTPATH, "variables.csv")
        self.variables.to_csv(fpath, index=False)
        logger.info(f"Wrote variables to {fpath}. (n={self.variables.shape[0]})")

    def write_datavalues(self) -> None:
        """writes datavalues to csv (one csv for each variable)."""
        logger.info("Saving data values to csv for each variable...")
        assert self.variables is not None
        var_name2id: Dict[str, int] = (
            self.variables.set_index("name")["id"].squeeze().to_dict()
        )

        df = self.datavalues
        # writes a csv to disk for each.
        out_path = os.path.join(OUTPATH, "datapoints")
        if not os.path.exists(out_path):
            os.makedirs(out_path)

        grouped = df.groupby("variable")
        for var_name, gp in tqdm(grouped, total=len(grouped)):
            # gp_tmp = gp[[self.entity_col, self.date_col, "Value"]].dropna()
            fpath = os.path.join(out_path, f"datapoints_{var_name2id[var_name]}.csv")
            assert not os.path.exists(fpath), (
                f"{fpath} already exists. This should not be possible, because "
                "each variable is supposed to be assigned its own unique "
                "file name."
            )

            # saves datapoints to disk.
            gp.columns = gp.columns.str.lower()
            gp.to_csv(fpath, index=False)

        logger.info(
            f"Wrote data values to csv for {df['variable'].nunique()} variables. CSVs written to: {out_path}."
        )

    def write_distinct_entities(self) -> None:
        """writes distinct entities to csv."""
        fpath = os.path.join(OUTPATH, "distinct_countries_standardized.csv")
        pd.DataFrame(get_distinct_entities(DATASET_DIR), columns=["name"]).to_csv(
            fpath, index=False
        )
        logger.info(f"Wrote distinct entities to {fpath}.")


class DataValuesCleaner:
    """Constructs a tidy (long) dataframe of entity-date-variable data
    observations from raw input dataset files downloaded from the original
    source.

    Usage:

        >>> from gcp_gcb.clean import DataValuesCleaner
        >>> dvc = DataValuesCleaner()
        >>> df = dvc.clean()
        >>> df.head()
                 Country  Year              variable     value
        199  Afghanistan  1949  Annual CO2 emissions   14656.0
        200  Afghanistan  1950  Annual CO2 emissions   84272.0
        201  Afghanistan  1951  Annual CO2 emissions   91600.0
        202  Afghanistan  1952  Annual CO2 emissions   91600.0
        203  Afghanistan  1953  Annual CO2 emissions  106256.0
        ...
    """

    def __init__(self):
        self.variables_to_drop = []

    @property
    def date_col(self) -> str:
        return "year"

    @property
    def entity_col(self) -> str:
        return "country"

    @property
    def c2co2_conversion_factor(self) -> float:
        """conversion factor for (million) tonnes of carbon to (million) tonnes
        of CO2."""
        return 3.664

    @property
    def twh2kwh_conversion_factor(self) -> int:
        """conversion factor for twh to kwh."""
        return 1e9

    @property
    def emission_sources(self) -> List[str]:
        return ["total", "coal", "oil", "gas", "flaring", "cement", "other"]

    @property
    def production_emissions_fname(self) -> str:
        return "GCB2021v32_MtCO2_flat.csv"

    @property
    def consumption_emissions_fname(self) -> str:
        return "National_Carbon_Emissions_2020v1.0.xlsx"

    @property
    def population_id(self) -> int:
        """OWID variable ID for the latest Gapminder et al. population series"""
        return 72

    @property
    def gdp_id(self) -> int:
        """OWID variable ID for the latest Maddison GDP series"""
        return 146201

    @property
    def primary_energy_id(self) -> int:
        """OWID variable ID for the latest primary energy consumption series"""
        return 143360

    @property
    def variables_to_drop(self) -> List[str]:
        """names of temporary variables to drop before returning cleaned dataset."""
        return self._variables_to_drop

    @variables_to_drop.setter
    def variables_to_drop(self, value: List[str]) -> None:
        assert isinstance(value, list)
        assert all([isinstance(v, str) for v in value])
        self._variables_to_drop = value

    def clean(self) -> pd.DataFrame:
        """Returns a tidy (long) dataframe of entity-date-variable data
        observations from raw input dataset files downloaded from the original
        source."""
        # retrieves national and global CO2 emissions data
        co2_prod = self.load_production_emissions()
        co2_cons = self.load_consumption_emissions()

        index_cols = [self.entity_col, self.date_col]
        # merges in national consumption emissions, population, gdp, and primary energy data
        # note: how="left" drops entities in co2_cons that do not appear in
        # co2_prod. This is intentional, since co2_cons contains regions that we
        # wish to drop (b/c regional sums are constructed later). But this left
        # merge may be prone to error if for some reason co2_prod does not contain
        # all country entities that appear in co2_cons.
        df = (
            pd.merge(
                co2_prod,
                co2_cons,
                on=index_cols,
                how="left",
                validate="1:1",
            )
            .merge(self.get_population(), on=index_cols, how="left", validate="1:1")
            .merge(self.get_gdp(), on=index_cols, how="left", validate="1:1")
            .merge(self.get_primary_energy(), on=index_cols, how="left", validate="1:1")
        )

        assert not df.duplicated(index_cols).any(), (
            "CO2 emissions dataset contains one or more duplicated "
            f"{self.entity_col}-{self.date_col} rows."
        )

        # aggregates country data into regional sums
        df_regions = self.aggregate_regions(df)
        if "Antarctica" in df_regions[self.entity_col].unique():
            df = df[df[self.entity_col] != "Antarctica"]

        # constructs 'geo_lvl' variable, which is used only for sanity checks.
        df["geo_lvl"] = df[self.entity_col].apply(
            lambda x: "country" if x != "World" else "World"
        )
        assert (df["geo_lvl"] == "World").sum() == df[self.date_col].dropna().nunique()
        df_regions["geo_lvl"] = "region"
        self.variables_to_drop.append("geo_lvl")

        df = pd.concat([df, df_regions]).reset_index(drop=True)

        df = (
            df.pipe(self.mk_full_time_series)
            .pipe(self.mk_growth_variables)
            .pipe(self.mk_traded_variables)
            .pipe(self.mk_per_capita_variables)
            .pipe(self.mk_global_variables)
            .pipe(self.mk_share_variables)
            .pipe(self.mk_cumul_variables)
            .pipe(self.mk_intensity_variables)
            .pipe(self.convert_units)
            .pipe(self.replace_with_nan)
            .pipe(self.mk_nan_filled_variables)
            .pipe(self.round_variables)
        )

        # Clean up dataset for grapher
        df = (
            df.dropna(subset=index_cols, how="any")
            .set_index(index_cols)
            .reset_index()
            .pipe(self.drop_temp_variables)
            .pipe(self.drop_nan_variables)
            .pipe(self.rename_variables)
            .pipe(self.to_long)
            .dropna(how="any")
        )

        self.sanity_checks(df)

        return df

    def load_production_emissions(
        self, standardize_entities: bool = True
    ) -> pd.DataFrame:
        """loads production emissions dataframe."""
        df = pd.read_csv(
            os.path.join(INPATH, self.production_emissions_fname), encoding="latin1"
        ).drop(columns=["ISO 3166-1 alpha-3", "Per Capita"])
        df.columns = df.columns.str.lower()
        if standardize_entities:
            df[self.entity_col] = self.standardize_entity_names(df[self.entity_col])

        # drops all rows with only NaN and drops all values <= 0.
        # note: we drop values == 0 b/c these seem to be synonymous with NaN.
        # e.g. in the raw data, Rwanda has 0 total emissions for each year
        # between 1751 and 1949, which then suddenly jumps to .016854 million
        # tonnes in 1950. We opt to drop values of 0 instead of leaving 0 values
        # as they are b/c the use of 0 vs. NaN to represent an NaN value is
        # inconsistent in the raw dataset, so we treat all as NaN for consistency.
        df = (
            df.set_index([self.entity_col, self.date_col])
            .dropna(how="all")
            .stack()
            .where(lambda x: x > 0)
            .dropna()
            .unstack()
            .reset_index()
        )
        assert (
            df.set_index([self.entity_col, self.date_col]) <= 0
        ).sum().sum() == 0, "Found one or more entity-date-variable values <= 0"
        return df

    def load_consumption_emissions(
        self, standardize_entities: bool = True
    ) -> pd.DataFrame:
        """loads consumption emissions dataframe."""
        df = pd.read_excel(
            os.path.join(INPATH, self.consumption_emissions_fname),
            sheet_name="Consumption Emissions",
            skiprows=8,
        )
        df = (
            df.rename(columns={df.columns[0]: self.date_col})
            .drop(columns=["Bunkers", "Statistical Difference"])
            .dropna(how="all", axis=0)
            .dropna(how="all", axis=1)
            .melt(
                id_vars=[self.date_col],
                var_name=[self.entity_col],
                value_name="consumption_emissions",
            )
            .dropna(subset=["consumption_emissions"])
        )
        if standardize_entities:
            df[self.entity_col] = self.standardize_entity_names(df[self.entity_col])

        # drops all values <= 0.
        df = df[df["consumption_emissions"] > 0]

        # Convert from million tonnes of carbon to million tonnes of CO2
        converted_columns = ["consumption_emissions"]
        df[converted_columns] = (
            df[converted_columns].astype(float).mul(self.c2co2_conversion_factor)
        )

        assert (
            df.set_index([self.entity_col, self.date_col]) <= 0
        ).sum().sum() == 0, "Found one or more entity-date-variable values <= 0"
        return df

    def standardize_entity_names(self, entities: List[str]) -> List[str]:
        """standardizes entity names to OWID entities."""
        country2std_name: Dict[str, str] = (
            pd.read_csv(os.path.join(CONFIGPATH, "standardized_entity_names.csv"))
            .set_index("Country")
            .squeeze()
            .to_dict()
        )
        unstd_entities = set([ent for ent in entities if ent not in country2std_name])
        if len(unstd_entities) > 0:
            logger.warning(
                "The following country names have not been not standardized to an "
                "OWID entity name because they are not in `standardized_entity_names.csv`: "
                f"{unstd_entities}"
            )
        return [country2std_name.get(ent, ent) for ent in entities]

    def get_population(self) -> pd.DataFrame:
        """retrieve OWID population dataframe."""
        df = (
            get_owid_variable(self.population_id, to_frame=True)
            .rename(
                columns={
                    "entity": self.entity_col,
                    "year": self.date_col,
                    "value": "population",
                }
            )
            .drop(columns=["variable"])
        )
        self.variables_to_drop.append("population")
        return df

    def get_gdp(self) -> pd.DataFrame:
        """retrieve OWID gdp dataframe."""
        df = (
            get_owid_variable(self.gdp_id, to_frame=True)
            .rename(
                columns={
                    "entity": self.entity_col,
                    "year": self.date_col,
                    "value": "real_gdp",
                }
            )
            .drop(columns=["variable"])
        )
        self.variables_to_drop.append("real_gdp")
        return df

    def get_primary_energy(self) -> pd.DataFrame:
        """retrieve OWID primary energy consumption dataframe."""
        df = (
            get_owid_variable(self.primary_energy_id, to_frame=True)
            .rename(
                columns={
                    "entity": self.entity_col,
                    "year": self.date_col,
                    "value": "primary_energy_consumption",
                }
            )
            .drop(columns=["variable"])
        )
        df["primary_energy_consumption"] *= self.twh2kwh_conversion_factor
        self.variables_to_drop.append("primary_energy_consumption")
        return df

    def aggregate_regions(self, df: pd.DataFrame) -> pd.DataFrame:
        """aggregate entity-date observations into region-date observations."""
        df_regions = pd.read_csv(
            os.path.join(CONFIGPATH, "standardized_entity_regions.csv"),
            usecols=["Country", "Region"],
        )
        df_regions.columns = df_regions.columns.str.lower()
        df_regions = (
            df_regions.merge(df, on=self.entity_col, how="left", validate="m:m")
            .groupby(["region", self.date_col], as_index=False)
            .sum(min_count=1)
            .rename(columns={"region": self.entity_col})
        )
        return df_regions

    def mk_full_time_series(self, df: pd.DataFrame) -> pd.DataFrame:
        """for each entity time series, adds any missing {date} values so that
        each entity time series has the full range of unique {date} values."""
        uniq_dates = df[self.date_col].drop_duplicates().sort_values().tolist()
        df = (
            df.groupby(self.entity_col)
            .apply(lambda gp: gp.set_index(self.date_col).reindex(uniq_dates))
            .drop(columns=[self.entity_col])  # gets re-inserted on reset_index()
            .reset_index()
            .sort_values([self.entity_col, self.date_col])
        )
        return df

    def mk_growth_variables(self, df: pd.DataFrame) -> pd.DataFrame:
        """constructs annual growth variables."""
        assert not df.duplicated(
            [self.entity_col, self.date_col]
        ).any(), f"dataframe contains one or more duplicated {self.entity_col}-{self.date_col} rows."
        self.check_all_time_series_full(df)
        df.sort_values([self.entity_col, self.date_col], inplace=True)
        df["emissions_growth_pct"] = (
            df.groupby(self.entity_col)["total"].pct_change() * 100
        )
        df["emissions_growth_abs"] = df.groupby(self.entity_col)["total"].diff()
        return df

    def mk_traded_variables(self, df: pd.DataFrame) -> pd.DataFrame:
        """constructs traded emissions variables."""
        df["traded_emissions"] = df["consumption_emissions"] - df["total"]
        df["traded_emissions_pct"] = df["traded_emissions"] / df["total"] * 100
        return df

    def mk_per_capita_variables(self, df: pd.DataFrame) -> pd.DataFrame:
        """constructs per capita variables."""
        for src in self.emission_sources + [
            "consumption_emissions",
            "traded_emissions",
        ]:
            df[f"{src}_percap"] = df[src] / df["population"]
        return df

    def mk_global_variables(self, df: pd.DataFrame) -> pd.DataFrame:
        """constructs "global {source} emissions" variables."""
        self.check_world_sums(df)
        df_world = (
            df[df["geo_lvl"] == "country"]
            .groupby(self.date_col)[self.emission_sources]
            .sum(min_count=1)
            .sort_index()
            .reset_index()
            .rename(columns={src: f"{src}_global" for src in self.emission_sources})
        )
        df = df.merge(df_world, on=self.date_col, how="left", validate="m:1")
        return df

    def mk_share_variables(self, df: pd.DataFrame) -> pd.DataFrame:
        """constructs "share of global emissions" variables."""
        for src in self.emission_sources:
            df[f"{src}_pct"] = df[src] / df[f"{src}_global"] * 100
            self.variables_to_drop.append(f"{src}_global")

        return df

    def mk_cumul_variables(self, df: pd.DataFrame) -> pd.DataFrame:
        """constructs cumulative emissions variables."""
        df.sort_values([self.entity_col, self.date_col], inplace=True)
        for src in self.emission_sources:
            df[f"{src}_cumul"] = df.groupby(self.entity_col)[src].cumsum()
            df[f"{src}_global_cumul"] = df.groupby(self.entity_col)[
                f"{src}_global"
            ].cumsum()
            df[f"{src}_cumul_pct"] = (
                df[f"{src}_cumul"] / df[f"{src}_global_cumul"] * 100
            )
            self.variables_to_drop.append(f"{src}_global_cumul")
        return df

    def mk_intensity_variables(self, df: pd.DataFrame) -> pd.DataFrame:
        """Constructs carbon intensity variables (per unit GDP and per unit
        energy)."""
        df["emissions_per_gdp"] = df["total"] / df["real_gdp"]
        df["consumption_emissions_per_gdp"] = (
            df["consumption_emissions"] / df["real_gdp"]
        )
        df["emissions_per_energy"] = df["total"] / df["primary_energy_consumption"]
        return df

    def convert_units(self, df: pd.DataFrame) -> pd.DataFrame:
        """converts units based on `conversionFactor` field provided in
        `variables_to_clean.json`"""
        with open(os.path.join(CONFIGPATH, "variables_to_clean.json"), "r") as f:
            vars_to_clean = json.load(f)["variables"]
        for var in vars_to_clean:
            var_name = var["cleaningMetadata"]["rawName"]
            conv_factor = var.get("cleaningMetadata", {}).get("conversionFactor")
            if pd.notnull(conv_factor):
                df[var_name] *= conv_factor
        return df

    def replace_with_nan(self, df: pd.DataFrame) -> pd.DataFrame:
        """replaces inf values with nan."""
        return df.replace([np.inf, -np.inf], np.nan)

    def drop_temp_variables(self, df: pd.DataFrame) -> pd.DataFrame:
        """drops temporary variables"""
        return df.drop(columns=self.variables_to_drop)

    def drop_nan_variables(self, df: pd.DataFrame) -> pd.DataFrame:
        """drops variables that do not contain any non-NaN values."""
        # drops any variables with all NaN entity-year observations
        return df.dropna(how="all", axis=1)

    def mk_nan_filled_variables(self, df: pd.DataFrame) -> pd.DataFrame:
        """constructs "NaN-filled" variables, which have NaN values filled with
        non-NaN values using pandas.Series.fillna

        These variables are used in stacked area charts.
        """
        with open(os.path.join(CONFIGPATH, "variables_to_clean.json"), "r") as f:
            raw_name2fillna_kwargs: Dict[str, str] = {
                v["cleaningMetadata"]["rawName"]: v["cleaningMetadata"]["fillna"]
                for v in json.load(f)["variables"]
                if pd.notnull(v.get("cleaningMetadata", {}).get("fillna"))
            }
        df.sort_values([self.entity_col, self.date_col], inplace=True)
        for nm, fillna_kwargs in raw_name2fillna_kwargs.items():
            df[f"{nm}_zero_filled"] = df.groupby(self.entity_col)[nm].fillna(
                **fillna_kwargs
            )
            # fills any remaining NaNs with 0.
            # e.g. in fillna(method='ffill'), NaNs at the beginning of the
            # time series will still be NaN.
            if df[f"{nm}_zero_filled"].isnull().any():
                df[f"{nm}_zero_filled"] = df[f"{nm}_zero_filled"].fillna(0)
        return df

    def round_variables(self, df: pd.DataFrame) -> pd.DataFrame:
        """rounds variables.

        Rounds to 2 digits if variable is a percentage, otherwise rounds to 4
        digits.
        """
        with open(os.path.join(CONFIGPATH, "variables_to_clean.json"), "r") as f:
            var2unit: Dict[str, str] = {}
            for var in json.load(f)["variables"]:
                raw_name = var["cleaningMetadata"]["rawName"]
                var2unit[raw_name] = var["unit"]
                if pd.notnull(var.get("cleaningMetadata", {}).get("fillna")):
                    var2unit[f"{raw_name}_zero_filled"] = var["unit"]

        for var_name, unit in var2unit.items():
            if unit == "%":
                digits = 2
            else:
                digits = 4
            df[var_name] = df[var_name].round(digits)
        return df

    def rename_variables(self, df: pd.DataFrame) -> pd.DataFrame:
        """renames variables to "clean" names."""
        raw_name2clean_name = self.load_standardized_variable_names()
        return df.rename(columns=raw_name2clean_name)

    def load_standardized_variable_names(self) -> Dict[str, str]:
        with open(os.path.join(CONFIGPATH, "variables_to_clean.json"), "r") as f:
            vars_to_clean: List[dict] = json.load(f)["variables"]
            raw_name2clean_name = {}
            for var in vars_to_clean:
                raw_name = var.get("cleaningMetadata", {}).get("rawName")
                name = var["name"]
                if raw_name:
                    raw_name2clean_name[raw_name] = name
                if pd.notnull(var.get("cleaningMetadata", {}).get("fillna")):
                    raw_name2clean_name[
                        f"{raw_name}_zero_filled"
                    ] = f"{name} (zero filled)"
        return raw_name2clean_name

    def to_long(self, df: pd.DataFrame) -> pd.DataFrame:
        """reshapes dataframe from wide to long."""
        df_long = df.melt(
            id_vars=[self.entity_col, self.date_col],
            var_name="variable",
            value_name="value",
        )
        if not is_integer_dtype(df_long[self.date_col]):
            df_long[self.date_col] = df_long[self.date_col].astype(int)
        return df_long

    def sanity_checks(self, df_long: pd.DataFrame) -> None:
        """executes sanity checks on long dataframe."""
        self.check_dtypes(df_long)
        self.check_no_duplicates(df_long)
        self.check_all_time_series_full(df_long)
        self.check_values_gte_zero(df_long)
        self.check_pct_minmax(df_long)
        self.check_cumulative_is_monotonic(df_long)

    def check_world_sums(self, df: pd.DataFrame) -> None:
        for src in self.emission_sources:
            world_sums_computed = (
                df[df["geo_lvl"] == "country"]
                .groupby(self.date_col)[src]
                .sum(min_count=1)
                .sort_index()
            )
            world_sums_raw = (
                df[df[self.entity_col] == "World"]
                .set_index(self.date_col)[src]
                .sort_index()
            )
            one_nan = (
                pd.concat([world_sums_computed, world_sums_raw], axis=1)
                .isnull()
                .sum(axis=1)
                == 1
            )
            if one_nan.any():
                logger.warning(
                    f"For one or more years in the '{src}' variable, EITHER "
                    "the global emissions value provided in the raw data is null "
                    "OR the global emissions value computed by summing all "
                    "country values is null (but not both). You may want to "
                    "investigate this discrepancy.\nYears affected: "
                    f"{world_sums_computed.index[one_nan].astype(int).tolist()}"
                )
            max_diff = (
                (world_sums_raw - world_sums_computed).abs() / world_sums_raw
            ).max()
            assert max_diff < 0.01, (
                f"For one or more years in the '{src}' variable, the global "
                "emissions value provided in the raw data differs from the "
                "global emissions value computed by summing all country values "
                f"by more than 1% (difference={max_diff.round(4)*100}%). This "
                "should be investigated."
            )
            if max_diff > 0.001:
                logger.warning(
                    f"For one or more years in the '{src}' variable, the "
                    "global emissions value provided in the raw data differs "
                    "from the global emissions value computed by summing all "
                    "country values by more than 0.1% (difference="
                    f"{max_diff.round(4)*100}%). You may want to investigate "
                    "this discrepancy."
                )

    def check_dtypes(self, df_long: pd.DataFrame) -> None:
        """checks that column dtypes are as expected."""
        assert is_integer_dtype(df_long[self.date_col])
        assert is_numeric_dtype(df_long["value"])

    def check_no_duplicates(self, df_long: pd.DataFrame) -> None:
        """checks there are no entity-date-variable duplicate rows in long
        dataframe."""
        assert not df_long.duplicated(
            subset=[self.entity_col, self.date_col, "variable"]
        ).any()

    def check_all_time_series_full(self, df: pd.DataFrame) -> None:
        """checks that each time series has all the same date values.

        Note: this method works when dataset is in either wide or long format.
        """
        date_min = df[self.date_col].min()
        date_max = df[self.date_col].max()
        assert is_numeric_dtype(date_min) and is_numeric_dtype(date_max)
        n_periods = date_max - date_min + 1
        assert (
            df.groupby(self.entity_col)[self.date_col]
            .apply(
                lambda gp: (gp.min() == date_min)
                & (gp.max() == date_max)
                & (gp.nunique() == n_periods)
            )
            .all()
        )

    def check_values_gte_zero(self, df_long: pd.DataFrame) -> None:
        """checks that all values are > 0 for all variables
        (except "growth" and "net" variables).
        """
        vars_exclude = [
            "emissions_growth_pct",
            "emissions_growth_abs",
            "traded_emissions",
            "traded_emissions_pct",
            "traded_emissions_percap",
        ]
        raw_name2clean_name = self.load_standardized_variable_names()
        vars_exclude_clean = [raw_name2clean_name[v] for v in vars_exclude]
        vars_with_negatives = (
            df_long.where(lambda df: ~df["variable"].isin(vars_exclude_clean))
            .where(lambda df: df["value"].round(4) < 0)
            .dropna()["variable"]
            .drop_duplicates()
            .tolist()
        )

        assert (
            len(vars_with_negatives) == 0
        ), f"The following variables have one or more negative values: {vars_with_negatives}"

    def check_pct_minmax(self, df_long: pd.DataFrame) -> None:
        """checks that percentage variables have values between 0 and 100 (inclusive).

        Note: "growth" and "...embedded in trade" (i.e. "net") variables can
        have values below zero, so they are excluded from this check.
        """
        with open(os.path.join(CONFIGPATH, "variables_to_clean.json"), "r") as f:
            vars_to_check: List[str] = []
            for v in json.load(f)["variables"]:
                if (
                    v["shortUnit"] == "%"
                    and v.get("cleaningMetadata", {}).get("rawName")
                    and not re.search(
                        r"emissions_growth|traded_emissions",
                        v["cleaningMetadata"]["rawName"],
                        re.I,
                    )
                ):
                    vars_to_check.append(v["name"])
            assert len(vars_to_check) > 0

        vars_with_invalid = (
            df_long.where(lambda df: df["variable"].isin(vars_to_check))
            .where(lambda df: (df["value"].round(4) < 0) | (df["value"].round(4) > 100))
            .dropna()["variable"]
            .drop_duplicates()
            .tolist()
        )

        assert (
            len(vars_with_invalid) == 0
        ), f"The following variables have one or more values > 100 or < 0: {vars_with_invalid}"

    def check_cumulative_is_monotonic(self, df_long: pd.DataFrame) -> None:
        """Checks that each "cumulative" variable is monotonically increasing
        for each country time series."""
        with open(os.path.join(CONFIGPATH, "variables_to_clean.json"), "r") as f:
            cumul_variables: List[str] = []
            for v in json.load(f)["variables"]:
                if re.search(r"cumul", v["name"], re.I) and not v["shortUnit"] == "%":
                    cumul_variables.append(v["name"])
                    if pd.notnull(v.get("cleaningMetadata", {}).get("fillna")):
                        cumul_variables.append(f"{v['name']} (zero filled)")
            assert len(cumul_variables) > 0
        assert (
            df_long.where(lambda df: df["variable"].isin(cumul_variables))
            .dropna()
            .groupby(["variable", self.entity_col])["value"]
            .apply(lambda gp: gp.is_monotonic)
            .all()
        ), (
            "One or more 'cumulative' variables contain a country time series "
            "that is NOT monotonically increasing."
        )


if __name__ == "__main__":
    main()
