library(data.table)
library(stringr)
library(plyr)
rm(list = setdiff(ls(), c("codebook", "vdem")))

# Download and load data
download.file(url = "https://github.com/vdeminstitute/vdemdata/raw/master/data/codebook.RData", destfile = "input/codebook.RData")
load("input/codebook.RData")
setDT(codebook)

download.file(url = "https://github.com/vdeminstitute/vdemdata/raw/master/data/vdem.RData", destfile = "input/vdem.RData")
load("input/vdem.RData")
setDT(vdem)

standard_countries <- fread("input/vdem_country_standardized.csv")

create_dataset <- function() {
    name <- "V-Dem Dataset Version 11.1 - V-Dem Institute"
    df <- data.table(id = 0, name)
    fwrite(df, "output/datasets.csv")
}

create_sources <- function() {
    link <- "https://www.v-dem.net/en/data/data"
    retrieved <- Sys.Date()
    publisher <- "V-Dem (Varieties of Democracy) Institute"
    additional_info <- "The Varieties of Democracy Project (V-Dem) provides a multidimensional and disaggregated dataset with indicators of democracy for all countries since 1900. The data covers five high-level concepts of democracy (electoral, liberal, participatory, deliberative, and egalitarian) and collects data to measure them. V-Dem draws on theoretical and methodological expertise from a global team of experts. Approximately half of the indicators in the V-Dem dataset are based on factual information obtainable from official documents such as constitutions and government records. The other half consists of subjective assessments on topics where multiple country experts provide ratings."
    publisher_source <- "Coppedge, Michael, John Gerring, Carl Henrik Knutsen, Staffan I. Lindberg, Jan Teorell, Nazifa Alizada, David Altman, Michael Bernhard, Agnes Cornell, M. Steven Fish, Lisa Gastaldi, Haakon Gjerløw, Adam Glynn, Allen Hicken, Garry Hindle, Nina Ilchenko, Joshua Krusell, Anna Luhrmann, Seraphine F. Maerz, Kyle L. Marquardt, Kelly McMann, Valeriya Mechkova, Juraj Medzihorsky, Pamela Paxton, Daniel Pemstein, Joseﬁne Pernes, Johannes von Römer, Brigitte Seim, Rachel Sigman, Svend-Erik Skaaning, Jeffrey Staton, Aksel Sundström, Ei-tan Tzelgov, Yi-ting Wang, Tore Wig, Steven Wilson and Daniel Ziblatt. 2021. ”V-Dem [Country–Year/Country–Date] Dataset v11.1” Varieties of Democracy Project. https://doi.org/10.23696/vdemds21"
    desc <- sprintf(
        '{"dataPublishedBy": "%s", "dataPublisherSource": "%s", "link": "%s", "retrievedDate": "%s", "additionalInfo": "%s"}',
        publisher,
        publisher_source,
        link,
        retrieved,
        additional_info
    )
    df <- data.table(
        name = "V-Dem Dataset Version 11.1 (2021)",
        description = desc,
        dataset_id = 0,
        id = 0
    )
    fwrite(df, "output/sources.csv")
}

create_variables <- function() {
    varbook <- copy(codebook)
    varbook[!is.na(source) & source != "", source := sprintf("Source: %s", source)]
    for (col in c("tag", "name", "question", "clarification", "notes", "aggregation", "source", "responses", "scale")) {
        varbook[[col]] <- varbook[[col]] %>%
            str_replace_na("") %>%
            str_replace_all("<br>", "\n") %>%
            str_squish()
    }
    description <- paste(
        varbook$question,
        varbook$responses,
        varbook$clarification,
        varbook$notes,
        varbook$aggregation,
        varbook$source,
        sep = "\n"
    ) %>% str_replace_all("(\\\n)+", "\n") %>% str_replace("\\\n$", "") %>% str_replace_all('"', "'")
    varbook[
        scale == "We provide two versions of this index. The first is the normalized output from the the hierarchical latent variable analysis. It is on an unbounded interval scale. The second, denoted by *_osp, is a version of this output which we scale using a standard normal cumulative distribution function. It is thus scaled low to high (0-1).",
        scale := "Normalized output from the the hierarchical latent variable analysis on an unbounded interval scale"
    ]
    unit <- paste(
        varbook$scale,
        sep = "\n"
    ) %>% str_replace_all("(\\\n)+", "\n") %>% str_replace("\\\n$", "") %>% str_replace_all('"', "'")
    tag <- varbook$tag
    name <- varbook$name
    id <- seq_along(name)
    dataset_id <- 0
    df <- data.table(tag, dataset_id, id, name, unit, description)
    fwrite(df, "output/variables.csv")
}

standardize_countries <- function(countries) {
    return(mapvalues(
        countries,
        from = standard_countries$Country,
        to = standard_countries$`Our World In Data Name`,
        warn_missing = FALSE
    ))
}

create_datapoints <- function() {
    variables <- fread("output/variables.csv")
    unprocessed <- c()
    processed <- c()
    # country_names <- c()
    for (col in names(vdem)) {
        message(col)
        if (str_detect(col, "_(osp|codelow|codehigh|sd|ord|nr|mean|[3-5]C)$")) {
            next
        } else if (any(is.character(vdem[[col]]))) {
            next
        } else if (col %in% variables$tag) {
            df <- data.table(
                country = vdem$country_name,
                year = vdem$year,
                value = vdem[[col]])
            df <- df[!is.na(value) & as.character(value) != ""]
            df$country <- standardize_countries(df$country)

            # Sanity check
            dup <- df[, .N, c("country", "year")][N > 1]
            if (nrow(dup) > 0) stop(dup)

            fwrite(df, sprintf("output/datapoints/datapoints_%s.csv", variables[tag == col, id]))
            processed <- c(processed, col)
            # country_names <- unique(c(country_names, vdem$country_name))
        } else {
            unprocessed <- c(unprocessed, col)
        }
    }
    writeLines(unprocessed, "output/unprocessed_variables.txt")
    variables <- variables[tag %in% processed]

    # Pre-filled short units for variable that have percentages
    variables[str_detect(name, "(%)|([Pp]ercent)|([Ss]hare of)"), short_unit := "%"]

    # Set zero decimal places for categorical and/or binary vars
    variables[
        unit %in% c("Nominal", "Dichotomous", "Ordinal.", "Dichotomous."),
        display := "{'numDecimalPlaces': 0}"
    ]

    fwrite(variables, "output/variables.csv")
    message(nrow(variables), " variables ready for DB import.")
    # fwrite(data.table(Country = country_names), "output/non_standard_country_names.csv")
}

create_entities <- function() {
    files <- list.files("output/datapoints", pattern = "csv$", full.names = TRUE)
    entities <- c()
    for (f in files) {
        message(f)
        tmp <- fread(f, showProgress = FALSE, select = "country")
        entities <- unique(c(entities, tmp$country))
    }
    entities <- data.table(name = entities)
    setorder(entities, name)
    fwrite(entities, "output/distinct_countries_standardized.csv")
}

# file.remove(list.files("output", full.names = TRUE, recursive = TRUE))
create_dataset()
create_sources()
create_variables()
create_datapoints()
create_entities()
