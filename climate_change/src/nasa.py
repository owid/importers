import pandas as pd


def process_file(loc: str, source_url: str) -> pd.DataFrame:
    df = (
        pd.read_csv(
            source_url,
            skiprows=1,
            na_values="***",
            usecols=[
                "Year",
                "Jan",
                "Feb",
                "Mar",
                "Apr",
                "May",
                "Jun",
                "Jul",
                "Aug",
                "Sep",
                "Oct",
                "Nov",
                "Dec",
            ],
        )
        .assign(location=loc)
        .rename(columns={"Year": "year"})
        .melt(
            id_vars=["year", "location"],
            var_name="month",
            value_name="temperature_anomaly",
        )
    )
    df["date"] = pd.to_datetime(df.year.astype(str) + df.month + "15", format="%Y%b%d")
    return df[["location", "date", "temperature_anomaly"]]


def global_temperature_anomaly() -> pd.DataFrame:
    df = pd.concat(
        [
            process_file(
                "World",
                "https://data.giss.nasa.gov/gistemp/tabledata_v4/GLB.Ts+dSST.csv",
            ),
            process_file(
                "Northern Hemisphere",
                "https://data.giss.nasa.gov/gistemp/tabledata_v4/NH.Ts+dSST.csv",
            ),
            process_file(
                "Southern Hemisphere",
                "https://data.giss.nasa.gov/gistemp/tabledata_v4/SH.Ts+dSST.csv",
            ),
        ]
    )
    df.to_csv("ready/nasa_global-temperature-anomaly.csv", index=False)


def arctic_sea_ice_extent():
    source_url = "https://climate.nasa.gov/system/internal_resources/details/original/2264_N_09_extent_v3.0.csv"
    df = pd.read_csv(source_url)
    df.columns = df.columns.str.strip()
    (
        df[["year", "extent"]]
        .rename(columns={"extent": "arctic_sea_ice_nasa"})
        .assign(location="Arctic")
        .to_csv("ready/nasa_arctic-sea-ice.csv", index=False)
    )


def main():
    global_temperature_anomaly()
    arctic_sea_ice_extent()


if __name__ == "__main__":
    main()
