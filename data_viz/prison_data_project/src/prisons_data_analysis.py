import pandas as pd


def over_100_occupancy(df: pd.DataFrame, year: int) -> pd.DataFrame:
    """
    Identifies countries with occupancy rate > 100% in the target year and extracts their time trends.
    
    Args:
        df (pd.DataFrame): Input dataframe with columns ['geo', 'YEAR', 'OCC_ABS']
        year (int): Target year for filtering countries above 100%
    
    Returns:
        pd.DataFrame: Trends dataframe with columns ['geo', 'YEAR', 'OCC_ABS'] for overcrowded countries,
                      sorted by geo and YEAR
        
    Example:
        trends = over_100_occupancy(df, 2022)
        trends.to_csv('overcrowded_trends.csv', index=False)
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


def top_worst_countries(df: pd.DataFrame, year: int, n: int = 10, direction: str = "top"):
    """
    Selects top-N or worst-N countries by occupancy rate in target year and extracts their time trends.
    
    Args:
        df (pd.DataFrame): Input dataframe with columns ['geo', 'YEAR', 'OCC_ABS']
        year (int): Target year for selecting extreme countries
        n (int, optional): Number of extreme countries to select. Defaults to 10.
        direction (str, optional): "top" for highest OCC_ABS (overcrowded), "worst" for lowest OCC_ABS (underutilized).
                                   Defaults to "top".
    
    Returns:
        pd.DataFrame: Trends dataframe with columns ['geo', 'YEAR', 'OCC_ABS'] for extreme countries,
                      sorted by geo and YEAR
        
    Raises:
        ValueError: If direction is not "top" or "worst"
        
    Example:
        top10_trends = top_n_occupancy_extremes(df, 2022, n=10, direction="top")
        worst5_trends = top_n_occupancy_extremes(df, 2022, n=5, direction="worst")
        top10_trends.to_csv('top10_occupancy_trends.csv', index=False)
    """
    # Filter target year
    df_year = df[df["YEAR"] == year].copy()
    
    # Validate direction
    if direction not in ["top", "worst"]:
        raise ValueError('direction must be "worst" (highest OCC_ABS) or "top" (lowest OCC_ABS)')
    
    # Select top-N or worst-N countries by OCC_ABS
    if direction == "worst":
        extreme_countries = df_year.nlargest(n, "OCC_ABS")["geo"].unique().tolist()
        extreme_label = "worst"
    else:
        extreme_countries = df_year.nsmallest(n, "OCC_ABS")["geo"].unique().tolist()
        extreme_label = "top"
    
    n_extreme = len(extreme_countries)
    n_total = df_year["geo"].nunique()
    
    print(f"Number of {extreme_label} {n} occupancy countries in {year}: {n_extreme} out of {n_total}")
    
    # Time series for those extreme countries (for plotting trends)
    extreme_trends = (
        df[df["geo"].isin(extreme_countries)][["geo", "YEAR", "OCC_ABS"]]
        .sort_values(["geo", "YEAR"])
        .reset_index(drop=True)
    )
    
    # columns outcome: geo, YEAR, OCC_ABS
    return extreme_trends


def compare_regions(df: pd.DataFrame, region1, region2, year: int) -> pd.DataFrame:
    """
    Compares occupancy statistics between North and South regions for target year.
    
    Args:
        df (pd.DataFrame): Input dataframe with columns ['geo', 'YEAR', 'OCC_ABS']
        region1 (str or list): North region countries as comma-separated string OR list
        region2 (str or list): South region countries as comma-separated string OR list
        year (int): Target year for comparison
        
    Returns:
        pd.DataFrame: Region statistics with columns ['REGION', 'mean', 'min', 'max', 'count']
        
    Example:
        # With lists (tuo caso)
        north = ["Norway", "Sweden", ...]
        south = ["Italy", "Spain", ...]
        stats = compare_regions(df, north, south, 2022)
        
        # O con stringhe
        stats = compare_regions(df, "Norway,Sweden", "Italy,Spain", 2022)
    """
    # Convert to lists if strings, use directly if lists
    if isinstance(region1, str):
        north_countries = [c.strip() for c in region1.split(",")]
    else:  # list
        north_countries = [c.strip() for c in region1]
        
    if isinstance(region2, str):
        south_countries = [c.strip() for c in region2.split(",")]
    else:  # list
        south_countries = [c.strip() for c in region2]

    def assign_region(country):
        if country in north_countries:
            return "North"
        if country in south_countries:
            return "South"
        return "Other"

    df_assigned = df.copy()
    df_assigned["REGION"] = df_assigned["geo"].apply(assign_region)

    # Filter latest year
    latest = df_assigned[df_assigned["YEAR"] == year].copy()

    region_stats = (
        latest.groupby("REGION")["OCC_ABS"]
        .agg(["mean","min","max","count"])
        .round(2)
    )

    print(f"Region stats for {year}:")
    print(region_stats)

    # columns outcome: REGION, mean, min, max, count
    return region_stats


def trend_nations(df: pd.DataFrame, countries_focus: list) -> pd.DataFrame:
    """
    Extracts time trends for a specific list of focus countries.
    
    Args:
        df (pd.DataFrame): Input dataframe with columns ['geo', 'YEAR', 'OCC_ABS']
        countries_focus (list): List of country names to extract trends for
        
    Returns:
        pd.DataFrame: Trends dataframe with columns ['geo', 'YEAR', 'OCC_ABS'] for focus countries,
                      sorted by geo and YEAR
        
    Example:
        focus_trends = trend_nations(df, ["Iceland", "Malta", "Liechtenstein", "Cyprus"])
        focus_trends.to_csv('focus_countries_trends.csv', index=False)
    """
    # countries_focus = ["Iceland", "Malta", "Liechtenstein", "Cyprus"]
    
    focus_trends = df[df["geo"].isin(countries_focus)][["geo", "YEAR", "OCC_ABS"]].copy()
    
    n_countries = focus_trends["geo"].nunique()
    print(f"Trends extracted for {n_countries} focus countries: {countries_focus}")
    
    focus_trends = focus_trends.sort_values(["geo", "YEAR"]).reset_index(drop=True)
    
    # columns outcome: geo, YEAR, OCC_ABS
    return focus_trends
