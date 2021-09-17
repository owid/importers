import pandas as pd


def arctic_sea_ice():
    source_url = "http://osisaf.met.no/quicklooks/sie_graphs/figs_v2p1/nh/osisaf_nh_sia_monthly.txt"
    df = pd.read_csv(
        source_url,
        skiprows=7,
        header=None,
        names=["date", "year", "month", "day", "arctic_sea_ice_osisaf"],
        sep=" ",
        na_values=-999,
    ).assign(location="Arctic")
    df["date"] = pd.to_datetime(
        df.year.astype(str) + "-" + df.month.astype(str) + "-" + df.day.astype(str)
    ).dt.date
    df[["date", "location", "arctic_sea_ice_osisaf"]].to_csv(
        "ready/osisaf_arctic-sea-ice.csv", index=False
    )


def main():
    arctic_sea_ice()


if __name__ == "__main__":
    main()
