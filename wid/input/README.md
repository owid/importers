World Inequality Database Dataset
---------------------------------

# Content of this Folder

In addition to the current README file, this folder contains the following CSV files:

* "WID_countries.csv" which contains the list and meaning of all country and region codes.

* file(s) named "WID_data_XX.csv" which contains the data for the country/region XX.

* file(s) named "WID_metadata_XX.csv" which contains the metadata for the country/region XX.

# Structure and Format of the CSV files

The CSV files use the semicolon ";" as a separator. Strings are quoted when required. The first row corresponds to variable names.

## Structure of WID_countries.csv file

The WID_countries.csv file contains five variables:

* alpha2: the 2-letter country/region code. It mostly follows the ISO 3166-1 alpha-2 nomenclature, with some additions to account for former countries, regions and subregions. Regions within country XX are indicated as XX-YY, where YY is the region code. World regions are indicated as XX and XX-MER, the first one using purchasing power parities (the default) and the second one using market exchange rates. See [the technical note "Prices and currency conversions in WID.world"](https://wid.world/document/convert-wid-world-series/) for details.

* titlename: the name of the country/region as it would appear in an English sentence (i.e. including the definite article, if any).

* shortname: the name of the country/region as it would appear on its own in English (i.e. excluding the definite article).

* region: broad world region to which the country belongs (similar to the first-level division of the United Nations geoscheme).

* region2: detailed world region to which the country belongs (similar to the second-level division of the United Nations geoscheme).

## Structure of the WID_data_XX.csv files

The WID_data_XX.csv files contain seven variables:

* country: country/region code (see WID_countries.csv).

* variable: WID variable code (see below for details).

* percentile: WID percentile code (see below for details).

* year: the year of the data point.

* value: the value of the data point.

* age: code indicating the age group to which the data point refers to.

* pop: code indicating the population unit to which the data point refers to.

## Structure of the WID_metadata_XX.csv

The WID_metadata_XX.csv contains seventeen variables:

* country: the country/region code.

* variable: the variable code to which the metadata refer.

* age: the code of the age group to which the population refer.

* pop: the code of the population unit to which the population refer.

* countryname: the name of the country/region as it would appear in an English sentence.

* shortname: the name of the country/region as it would appear on its own in English.

* simpledes: decription of the variable in plain English.

* technicaldes: description of the variable via accounting identities.

* shorttype: short description of the variable type (average, aggregate, share, index, etc.) in plain English.

* longtype: longer, more detailed description of the variable type in plain English.

* shortpop: short description of the population unit (individuals, tax units, equal-split, etc.) in plain English.

* longpop: longer, more detailed description of the population unit in plain English.

* shortage: short description of the age group (adults, full population, etc.) in plain English.

* longage: longer, more detailed description of the age group in plain English.

* unit: unit of the variable (the 3-letter currency code for monetary amounts).

* source: The source(s) used to compute the data.

* method: Methological details describing how the data was constructed and/or caveats.

# How to Interpret Variable Codes

The meaning of each variable is described in the metadata files. The complete WID variable codes (i.e. sptinc992j) obey to the following logic:

* the first letter indicates the variable type (i.e. "s" for share).

* the next five letters indicate the income/wealth/other concept (i.e. "ptinc" for pre-tax national income).

* the next three digits indicate the age group (i.e. "992" for adults).

* the last letter indicate the population unit (i.e. "j" for equal-split).

# How to Interpret Percentile Codes

There are two types of percentiles on WID.world : (1) group percentiles and (2) generalized percentiles. The interpretation of income (or wealth) average, share or threshold series depends on which type of percentile is looked at.

## Group Percentiles

Group percentiles are defined as follows : p0p50 (bottom 50% of the population), p50p90 (next 40%), p90p100 (top 10%), p99p100 (top 1%), p0p10 (bottom 10% of the population, i.e. first decile), p10p20 (next 10%, i.e. second decile), p20p30 (next 10%, i.e. third decile), p30p40 (next 10%, i.e. fourth decile), p40p50 (next 10%, i.e. fifth decile), p50p60 (next 10%, i.e. sixth decile), p60p70 (next 10%, i.e. seventh decile), p70p80 (next 10%, i.e. eighth decile), p80p90 (next 10%, i.e. ninth decile), p0p90 (bottom 90%), p0p99 (bottom 99% of the population), p99.9p100 (top 0.1%), p99.99p100 (top 0.01%).

For each group percentiles, we provide the associated income or wealth shares, averages and thresholds.

* group percentile shares correspond to the income (or wealth) share held by a given group percentile. For instance, the fiscal income share of group p0p50 is the share of total fiscal income captured by the bottom 50% group.

* group percentile averages correspond to the income or wealth annual income (or wealth) average within a given group percentile group. For instance, the fiscal income average of group p0p50 is the average annual fiscal income of the bottom 50% group.

* group percentile thresholds correspond to the minimum income (or wealth) level required to belong to a given group percentile. For instance, the fiscal income threshold of group p90p100 is the minimum annual fiscal income required to belong to the top 10% group.

When the data allows, the WID.world website makes it possible to produce shares, averages and thresholds for any group percentile (say, for instance, average income of p43p99.92). These are not stored in bulk data tables.

For certain countries, because of data limitations, we are not able to provide the list of group percentiles described above. We instead store specific group percentiles (these can be, depending on the countries p90p95, p95p100, p95p99, p99.5p100, p99.5p99.9, p99.75p100, p99.95p100, p99.95p99.99, p99.995p100, p99.9p99.95, p99.9p99.99 or p99p99.5).

## Generalized Percentiles

Generalized percentiles (g-percentiles) are defined to as follows: p0, p1, p2, ..., p99, p99.1, p99.2, ..., p99.9, p99.91, p99.92, ..., p99.99, p99.991, p99.992 ,..., p99.999. There are 127 g-percentiles in total.

For each g-percentiles, we provide shares, averages, top averages and thresholds.

* g-percentiles shares correspond to the income (or wealth) share captured by the population group above a given g-percentile value. For example, the fiscal income share of g-percentile p90 corresponds to the fiscal income share held by the top 10% group; the fiscal income share of g-percentile p99.9 corresponds to the fiscal income share of the top 0.1% income group and so on. By construction, the fiscal income share of g-percentile p0 corresponds to the share held by 100% of the population and is equal to 100%. Formally, the g-percentile share at g-percentile pX corresponds to the share of the top (100-X)% group.

* g-percentile averages correspond to the average income or wealth between two consecutive g-percentiles. Average income of g-percentile p0 corresponds to the average annual income of the bottom 1% group, p2 corresponds to the next 1% group and so on until p98 (the 1% population group below the top 1%). Average income of g-percentile p99 corresponds to average annual of group percentile p99p99.1 (i.e. the bottom 10% group of earners within the top 1% group of earners), p99.1 corresponds to the next 0.1% group, p99.2 corresponds to the next 0.1% group and so on until p99.8. Average income of p99.9 corresponds to the average annual income of group percentile p99.9p99.91 (i.e. the bottom 10% group of earners within the top 0.1% group of earners), p99.91 corresponds to the next 0.01% group, p99.92 corresponds to the next 0.01% group and so on until p99.98. Average income of p99.99, corresponds to the average annual income of group percentile p99.99p99.991 (i.e. the bottom 10% group within the top 0.01% group of earners), p99.991 corresponds to the next 0.001%, p99.992 corresponds to the next 0.001% group and so on until p99.999 (average income of the top 0.001% group). For instance, average fiscal income of g-percentile p50 is equal to the average annual fiscal income of the p50p51 group percentile (i.e. the average annual income of the population group earning more than 50% of the population and less than the top 49% of the population). The average fiscal income of g-percentile p99 is equal to the average annual fiscal income within group percentile p99p99.1 (i.e. a group representing 0.1% of the total population earning more than 99% of the population but less than the top 0.9% of the population).

* g-percentile top-averages correspond to the average income or wealth above a given g-percentile threshold. For instance the top average fiscal income at g-percentile p50 corresponds to the average annual fiscal income of individuals earning more than 50% of the population. The top average fiscal income at g-percentile p90 corresponds to the average annual fiscal income of the top 10% group.

* g-percentile thresholds correspond to minimum income (or wealth) level  required to belong to the population group above a given g-percentile value. For instance, the fiscal income threshold at g-percentile p90 corresponds to the minimum annual fiscal income required to belong to the top 10% group. Fiscal income threshold at g-percentile p99.9 corresponds to the minimum annual fiscal income required to belong to the top 0.1% group. Formally, the g-percentile threshold at g-percentile pX corresponds to the threshold of the top (100-X)% group.
