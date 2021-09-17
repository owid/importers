import pandas as pd


def ocean_ph():
    source_url = "https://hahana.soest.hawaii.edu/hot/products/HOT_surface_CO2.txt"
    df = (
        pd.read_csv(
            source_url,
            skiprows=8,
            sep="\t",
            usecols=["date", "pHcalc_insitu"],
            na_values=[-999],
        )
        .rename(columns={"pHcalc_insitu": "ocean_ph"})
        .assign(location="Hawaii")
    )
    df["date"] = pd.to_datetime(df.date).dt.date
    df.to_csv("ready/hawaii_ocean-ph.csv", index=False)


def main():
    ocean_ph()


if __name__ == "__main__":
    main()
