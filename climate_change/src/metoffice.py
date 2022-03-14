import datetime
import io
import os

import pandas as pd
import requests

from climate_change.src import READY_DIR


def process_file(loc: str, source_url: str, period: str) -> pd.DataFrame:
    data = requests.get(source_url).content
    df = pd.read_csv(io.StringIO(data.decode("utf-8"))).assign(location=loc)

    if period == "monthly":
        period_col = "date"
        df[period_col] = pd.to_datetime(
            df.year.astype(str) + "-" + df.month.astype(str) + "-15"
        )
    else:
        period_col = "year"

    return df[
        [
            period_col,
            "location",
            "anomaly",
            "lower_bound_95pct_bias_uncertainty_range",
            "upper_bound_95pct_bias_uncertainty_range",
        ]
    ].rename(
        columns={
            "anomaly": f"{period}_sea_surface_temperature_anomaly",
            "lower_bound_95pct_bias_uncertainty_range": f"{period}_sea_surface_temperature_anomaly_lower_bound_95pct",
            "upper_bound_95pct_bias_uncertainty_range": f"{period}_sea_surface_temperature_anomaly_upper_bound_95pct",
        }
    )


def annual_sea_surface_temperature():
    output_file = os.path.join(
        READY_DIR, "metoffice_annual-sea-surface-temperature.csv"
    )
    files = {
        "World": "https://www.metoffice.gov.uk/hadobs/hadsst4/data/csv/HadSST.4.0.1.0_annual_GLOBE.csv",
        "Northern Hemisphere": "https://www.metoffice.gov.uk/hadobs/hadsst4/data/csv/HadSST.4.0.1.0_annual_NHEM.csv",
        "Southern Hemisphere": "https://www.metoffice.gov.uk/hadobs/hadsst4/data/csv/HadSST.4.0.1.0_annual_SHEM.csv",
        "Tropics": "https://www.metoffice.gov.uk/hadobs/hadsst4/data/csv/HadSST.4.0.1.0_annual_TROP.csv",
    }
    df = pd.concat([process_file(k, v, period="annual") for k, v in files.items()])
    df = df[df.year < datetime.date.today().year]
    df.to_csv(output_file, index=False)


def monthly_sea_surface_temperature():
    output_file = os.path.join(
        READY_DIR, "metoffice_monthly-sea-surface-temperature.csv"
    )
    files = {
        "World": "https://www.metoffice.gov.uk/hadobs/hadsst4/data/csv/HadSST.4.0.1.0_monthly_GLOBE.csv",
        "Northern Hemisphere": "https://www.metoffice.gov.uk/hadobs/hadsst4/data/csv/HadSST.4.0.1.0_monthly_NHEM.csv",
        "Southern Hemisphere": "https://www.metoffice.gov.uk/hadobs/hadsst4/data/csv/HadSST.4.0.1.0_monthly_SHEM.csv",
        "Tropics": "https://www.metoffice.gov.uk/hadobs/hadsst4/data/csv/HadSST.4.0.1.0_monthly_TROP.csv",
    }
    df = pd.concat([process_file(k, v, period="monthly") for k, v in files.items()])
    df.to_csv(output_file, index=False)


def main():
    annual_sea_surface_temperature()
    monthly_sea_surface_temperature()


if __name__ == "__main__":
    main()
