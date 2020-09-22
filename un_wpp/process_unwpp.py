import datetime
from glob import glob
import pandas as pd


NAME_TO_PREFIX = {
    "input/WPP2019_INT_F03_3_POPULATION_BY_AGE_ANNUAL_FEMALE.xlsx": \
        "Female population by age (thousands)",
    "input/WPP2019_INT_F03_2_POPULATION_BY_AGE_ANNUAL_MALE.xlsx": \
        "Male population by age (thousands)",
    "input/WPP2019_INT_F03_1_POPULATION_BY_AGE_ANNUAL_BOTH_SEXES.xlsx": \
        "Total population by age, both sexes combined (thousands)",
    "input/WPP2019_INT_F02C_3_ANNUAL_POPULATION_INDICATORS_DEPENDENCY_RATIOS_FEMALE.xlsx": \
        "Female dependency ratios for different age groups",
    "input/WPP2019_INT_F02C_2_ANNUAL_POPULATION_INDICATORS_DEPENDENCY_RATIOS_MALE.xlsx": \
        "Male dependency ratios for different age groups",
    "input/WPP2019_INT_F02C_1_ANNUAL_POPULATION_INDICATORS_DEPENDENCY_RATIOS_BOTH_SEXES.xlsx": \
        "Dependency ratios (both sexes combined) for different age groups",
    "input/WPP2019_INT_F02B_3_ANNUAL_POPULATION_INDICATORS_PERCENTAGE_FEMALE.xlsx": \
        "Percentage of female population by broad age group (per 100 female total population)",
    "input/WPP2019_INT_F02B_2_ANNUAL_POPULATION_INDICATORS_PERCENTAGE_MALE.xlsx": \
        "Percentage of male population by broad age group (per 100 male total population)",
    "input/WPP2019_INT_F02B_1_ANNUAL_POPULATION_INDICATORS_PERCENTAGE_BOTH_SEXES.xlsx": \
        "Percentage of total population by broad age group, both sexes combined (per 100 total population)",
    "input/WPP2019_INT_F02A_3_ANNUAL_POPULATION_INDICATORS_FEMALE.xlsx": \
        "Female population by broad age group (thousands)",
    "input/WPP2019_INT_F02A_2_ANNUAL_POPULATION_INDICATORS_MALE.xlsx": \
        "Male population by broad age group (thousands)",
    "input/WPP2019_INT_F02A_1_ANNUAL_POPULATION_INDICATORS_BOTH_SEXES.xlsx": \
        "Total population by broad age group, both sexes combined (thousands)"
}


def create_datasets():

    names = []
    for filename in glob("input/*.xlsx"):
        print(filename)
        data = pd.read_excel(filename, usecols=[0], nrows=9)
        names.append(data.iloc[8, 0])
    datasets = pd.DataFrame({"name": names})
    datasets.loc[:, "name"] = "UN WPP - " + datasets["name"].str.replace("^[^:]+: ", "")
    datasets.to_csv("output/datasets.csv", index_label="id")


def create_description(row, additional_info):
    return {
        "dataPublishedBy": "United Nations, Department of Economic and Social Affairs, Population Division (2019). World Population Prospects: The 2019 Revision, DVD Edition.",
        "dataPublisherSource": None,
        "link": "https://population.un.org/wpp2019/Download/Standard/Interpolated/",
        "retrievedDate": datetime.datetime.now().strftime("%d %B %Y"),
        "additionalInfo": additional_info[row["dataset_name"]],
    }


def create_sources():

    ## Sources
    keys = [
        "UN WPP - Total population (both sexes combined) by single age, region, subregion and country, annually for 1950-2100 (thousands)",
        "UN WPP - Male population by single age, region, subregion and country, annually for 1950-2100 (thousands)",
        "UN WPP - Female population by single age, region, subregion and country, annually for 1950-2100 (thousands)",
        "UN WPP - Interpolated demographic indicators by region, subregion and country, annually for 1950-2099",
        "UN WPP - Interpolated total population by broad age group, region, subregion and country, annually for 1950-2100 (thousands)",
        "UN WPP - Interpolated male population by broad age group, region, subregion and country, annually for 1950-2100 (thousands)",
        "UN WPP - Interpolated female population by broad age group, region, subregion and country, annually for 1950-2100 (thousands)",
        "UN WPP - Percentage of total population by broad age group, region, subregion and country, annually interpolated for 1950-2100",
        "UN WPP - Percentage of male total population by broad age group, region, subregion and country, annually interpolated for 1950-2100",
        "UN WPP - Percentage of female total population by broad age group, region, subregion and country, annually interpolated for 1950-2100",
        "UN WPP - Dependency ratios (total, child, old-age) for different age groups and for both sexes combined by region, subregion and country, annually interpolated for 1950-2100",
        "UN WPP - Male dependency ratios (total, child, old-age) for different age groups by region, subregion and country, annually interpolated for 1950-2100",
        "UN WPP - Female dependency ratios (total, child, old-age) for different age groups by region, subregion and country, annually interpolated for 1950-2100"
    ]

    vals = [
        "Annual population by single age - Both Sexes. De facto population as of 1 July of the year indicated classified by single age (0, 1, 2, ..., 99, 100+). Data are presented in thousands.",
        "Annual population by single age - Male. De facto population as of 1 July of the year indicated classified by single age (0, 1, 2, ..., 99, 100+). Data are presented in thousands.",
        "Annual population by single age - Female. De facto population as of 1 July of the year indicated classified by single age (0, 1, 2, ..., 99, 100+). Data are presented in thousands.",
        "Annually interpolated demographic indicators.",
        "Annual total population (both sexes combined) by broad age groups. Data are presented in thousands.",
        "Annual male population by broad age groups. Data are presented in thousands.",
        "Annual female population by broad age groups. Data are presented in thousands.",
        "Percentage of annual total population (both sexes combined) by broad age group.",
        "Percentage of annual male total population by broad age groups.",
        "Percentage of annual female total population by broad age groups.",
        "Annual dependency ratios (total, child, old-age) for different age groups and for both sexes combined.",
        "Annual male dependency ratios (total, child, old-age) for different age groups.",
        "Annual female dependency ratios (total, child, old-age) for different age groups."
    ]

    additional_info = dict(zip(keys, vals))

    sources = pd.read_csv("output/datasets.csv").rename(columns={
        "id": "dataset_id", "name": "dataset_name"
    })
    sources["name"] = "United Nations – Population Division (2019 Revision)"
    sources["description"] = sources.apply(create_description, 1, additional_info=additional_info)
    sources = sources[["name", "description", "dataset_id"]]
    sources.to_csv("output/sources.csv", index=False)


class DataVariables():

    def __init__(self):

        self.i = 0
        self.ids = []
        self.names = []
        self.units = []
        self.dataset_ids = []
        self.doc_to_unit = {
            "UN WPP - Female population by single age, region, subregion and country, annually for 1950-2100 (thousands)": "Thousands",
            "UN WPP - Male population by single age, region, subregion and country, annually for 1950-2100 (thousands)": "Thousands",
            "UN WPP - Total population (both sexes combined) by single age, region, subregion and country, annually for 1950-2100 (thousands)": "Thousands",
            "UN WPP - Female dependency ratios (total, child, old-age) for different age groups by region, subregion and country, annually interpolated for 1950-2100": "Female dependency ratios for different age groups",
            "UN WPP - Male dependency ratios (total, child, old-age) for different age groups by region, subregion and country, annually interpolated for 1950-2100": "Male dependency ratios for different age groups",
            "UN WPP - Dependency ratios (total, child, old-age) for different age groups and for both sexes combined by region, subregion and country, annually interpolated for 1950-2100": "Dependency ratios (both sexes combined) for different age groups",
            "UN WPP - Percentage of female total population by broad age group, region, subregion and country, annually interpolated for 1950-2100": "Percentage",
            "UN WPP - Percentage of male total population by broad age group, region, subregion and country, annually interpolated for 1950-2100": "Percentage",
            "UN WPP - Percentage of total population by broad age group, region, subregion and country, annually interpolated for 1950-2100": "Percentage",
            "UN WPP - Interpolated female population by broad age group, region, subregion and country, annually for 1950-2100 (thousands)": "Thousands",
            "UN WPP - Interpolated male population by broad age group, region, subregion and country, annually for 1950-2100 (thousands)": "Thousands",
            "UN WPP - Interpolated total population by broad age group, region, subregion and country, annually for 1950-2100 (thousands)": "Thousands"
        }
        self.datasets = pd.read_csv("output/datasets.csv")
        self.datavars = pd.DataFrame()

    def get_variables(self, path, skiprows=8, prefix=None):

        for sheet_name in ["ESTIMATES", "MEDIUM VARIANT"]:
            data = pd.read_excel(path, skiprows=skiprows, sheet_name=sheet_name)

            val = data[data.columns[0]][0]
            index_to_remove = val.find(":")
            res = "UN WPP - " + val[index_to_remove+2:]

            title = data[data.columns[0]][1]
            print(title)

            for item in data.loc[7, data.columns[8]: data.columns[-1]].values:
                if prefix:
                    self.names.append(title + ": " + prefix + " - " + item)
                else:
                    self.names.append(title + ": " + item)
                self.ids.append(self.i)
                self.i+=1
                self.units.append(self.doc_to_unit[res])
                self.dataset_ids.append(self.datasets[self.datasets["name"] == res]["id"].values[0])

    def get_custom_variable(self, path, skiprows=8, prefix=None):

        column_unit = {
            8: "Thousands",
            9: "Thousands",
            10: "Thousands",
            11: "Percentage",
            12: "Years",
            13: "Years",
            14: "Years",
            15: "Thousands",
            16: "Infant deaths per 1,000 live births",
            17: "Deaths under age 5 per 1,000 live births",
            18: "Thousands",
            19: "Births per 1,000 population",
            20: "Live births per woman",
            21: "Thousands",
            22: "Per 1,000 population",
            23: "Thousands",
            24: "Percentage"
        }

        for sheet_name in ["ESTIMATES", "MEDIUM VARIANT"]:
            data = pd.read_excel(path, skiprows=skiprows, sheet_name=sheet_name)

            val = data[data.columns[0]][0]
            index_to_remove = val.find(":")
            res = "UN WPP - " + val[index_to_remove+2:]

            title = data[data.columns[0]][1]
            print(title)

            col_index = 8
            for item in data.loc[7, data.columns[8]: data.columns[-1]].values:
                if prefix:
                    self.names.append(title + ": " + prefix + " - " + item)
                else:
                    self.names.append(title + ": " + item)
                self.ids.append(self.i)
                self.i+=1
                self.units.append(column_unit[col_index])
                self.dataset_ids.append(self.datasets[self.datasets["name"] == res]["id"].values[0])
                col_index += 1

    def get_df(self):

        self.datavars["id"] = self.ids
        self.datavars["name"] = self.names
        self.datavars["unit"] = self.units
        self.datavars["dataset_id"] = self.dataset_ids

        return self.datavars


def create_variables():

    datavars = DataVariables()
    for filename in glob("input/*.xlsx"):
        if filename == "input/WPP2019_INT_F01_ANNUAL_DEMOGRAPHIC_INDICATORS.xlsx":
            datavars.get_custom_variable(
                filename, prefix="Annually interpolated demographic indicators"
            )
        else:
            datavars.get_variables(filename, prefix=NAME_TO_PREFIX[filename])

    variables = datavars.get_df()
    variables.to_csv("output/variables.csv", index=False)


def normalize_country(series):
    series = series.str.replace(r"\s*[^A-Za-z\s]*$", "")
    return series


def standardize_country(series, country_mapping):
    series = series.replace(country_mapping)
    return series


def get_datapoints(path, variables, country_mapping, skiprows=8, prefix=None):

    for sheet_name in ["ESTIMATES", "MEDIUM VARIANT"]:
        data = pd.read_excel(path, skiprows=skiprows, sheet_name=sheet_name)
        title = data[data.columns[0]][1]
        index_col = 8

        for item in data.loc[7, data.columns[8]: data.columns[-1]].values:

            if prefix:
                var_name = title + ": " + prefix + " - " + item
            else:
                var_name = title + ": " + item

            var_id = variables[variables["name"] == var_name]["id"].values[0]

            data2 = data.iloc[8:]

            data_res = pd.DataFrame()
            data_res["country"] = data2[data2.columns[2]]
            data_res["year"] = data2[data2.columns[7]]
            data_res["value"] = data2["Unnamed: %s" % str(index_col)]

            data_res["country"] = normalize_country(data_res["country"])
            data_res["country"] = standardize_country(data_res["country"], country_mapping)
            data_res = data_res[data_res["value"] != "..."]
            data_res.to_csv("output/datapoints/datapoints_%s.csv" % str(var_id), index=False)

            index_col += 1


def create_datapoints():

    country_mapping = pd.read_csv("standardization/entities_standardized.csv")
    country_mapping = dict(zip(country_mapping.name, country_mapping.standardized_name))

    variables = pd.read_csv("output/variables.csv")
    for filename in glob("input/*.xlsx"):
        if filename == "input/WPP2019_INT_F01_ANNUAL_DEMOGRAPHIC_INDICATORS.xlsx":
            get_datapoints(
                filename, variables, prefix="Annually interpolated demographic indicators",
                country_mapping=country_mapping
            )
        else:
            get_datapoints(
                filename, variables, prefix=NAME_TO_PREFIX[filename],
                country_mapping=country_mapping
            )


def create_country_list():

    countries = set()
    for filename in glob("output/datapoints/*.csv"):
        data = pd.read_csv(filename)
        for j in data["country"]:
            countries.add(j)
    countries = pd.DataFrame({"name": [*countries]}).sort_values("name")
    countries.to_csv("output/distinct_countries_standardized.csv", index=False)


def main():
    create_datasets()
    create_sources()
    create_variables()
    create_datapoints()
    create_country_list()

if __name__ == "__main__":
    main()
