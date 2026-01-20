import pandas as pd
from pathlib import Path


def over_100_occupancy(df: pd.DataFrame, year: int):
    """
    Returns:
      - summary DataFrame with counts (N_over, N_total, year)
      - trends DataFrame with geo, YEAR, OCC_ABS for countries with OCC_ABS > 100 in 2022
    """
    
    # Filter target year
    df_year = df[df["YEAR"] == year].copy()

    # Countries above 100% in that year
    over = df_year[df_year["OCC_ABS"] > 100]

    n_over = over["geo"].nunique()
    n_total = df_year["geo"].nunique()

    print(f"Number of countries with occupancy above 100% in {year}: {n_over} out of {n_total}")

    # Time series for those overcrowded countries (for plotting)
    over_countries = over["geo"].unique().tolist()
    trends = (
        df[df["geo"].isin(over_countries)][["geo", "YEAR", "OCC_ABS"]]
        .sort_values(["geo", "YEAR"])
        .reset_index(drop=True)
    )

    return trends
