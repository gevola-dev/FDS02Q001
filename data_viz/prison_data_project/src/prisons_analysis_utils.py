import pandas as pd


def load_and_clean_prison_data(filepath: str, max_year: int = 2022) -> pd.DataFrame:
    """
    Loads Eurostat prison statistics CSV, performs type casting and year filtering.
    
    Args:
        filepath (str): Path to CSV file (e.g., 'crim_pris_cap-defaultview_linear.csv')
        max_year (int, optional): Maximum year to include. Defaults to 2022 (complete data).
    
    Returns:
        pd.DataFrame: Cleaned dataset with ALL original columns + numeric 'YEAR' and 'VALUE'
        
        Columns include:
        - 'geo': Country names
        - 'YEAR': Numeric year (from TIME_PERIOD)
        - 'VALUE': Numeric values (from OBS_VALUE)
        - 'unit', 'indic_cr', and all other original Eurostat columns
    """
    # Load raw CSV data
    df = pd.read_csv(filepath, sep=',')
    
    # Type casting for numeric columns (handle parsing errors)
    df['YEAR'] = pd.to_numeric(df['TIME_PERIOD'], errors='coerce')
    df['VALUE'] = pd.to_numeric(df['OBS_VALUE'], errors='coerce')
    
    # Filter to complete years only (up to max_year)
    df = df[df['YEAR'] <= max_year].copy()
    
    # Remove rows with missing critical YEAR data
    df = df.dropna(subset=['YEAR'])
    
    # Summary statistics
    n_rows, n_countries = len(df), df['geo'].nunique()
    print(f"Loaded {n_rows:,} observations for {n_countries} countries (YEAR <= {max_year})")
    print(f"Columns: {len(df.columns)} total | Sample: {df.columns.tolist()[:6]}...")
    
    return df


def compute_occupancy_rates(df_clean: pd.DataFrame) -> pd.DataFrame:
    """
    Computes absolute and per-100k occupancy rates from raw Eurostat prison data.
    
    Args:
        df_clean (pd.DataFrame): Output from load_and_clean_prison_data()
                                Must contain columns: ['geo', 'YEAR', 'unit', 'indic_cr', 'VALUE']
    
    Returns:
        pd.DataFrame: Final occupancy dataset with columns:
            ['geo', 'YEAR', 'PRISONERS_NUM', 'CAPACITY_NUM', 'OCC_ABS',
             'PRISONERS_100K', 'CAPACITY_100K', 'OCC_PER_100K']
        - All occupancy rates rounded to 2 decimals
        - Only complete observations (non-null numerators/denominators)
    """
    # 1) ABSOLUTE NUMBERS (Number unit)
    df_num = df_clean[df_clean["unit"] == "Number"].copy()
    
    pivot_num = df_num.pivot_table(
        index=["geo", "YEAR"],
        columns="indic_cr",
        values="VALUE",
        aggfunc="first"
    )
    
    pivot_num = pivot_num.rename(columns={
        "Actual number of persons held in prison": "PRISONERS_NUM",
        "Official prison capacity - persons": "CAPACITY_NUM"
    })
    
    # Keep only complete rows
    pivot_num = pivot_num.dropna(subset=["PRISONERS_NUM", "CAPACITY_NUM"])
    
    # Absolute occupancy (%)
    pivot_num["OCC_ABS"] = (pivot_num["PRISONERS_NUM"] / pivot_num["CAPACITY_NUM"] * 100).round(2)
    
    # 2) PER 100K INHABITANTS
    df_100k = df_clean[df_clean["unit"] == "Per hundred thousand inhabitants"].copy()
    
    pivot_100k = df_100k.pivot_table(
        index=["geo", "YEAR"],
        columns="indic_cr",
        values="VALUE",
        aggfunc="first"
    )
    
    pivot_100k = pivot_100k.rename(columns={
        "Actual number of persons held in prison": "PRISONERS_100K",
        "Official prison capacity - persons": "CAPACITY_100K"
    })
    
    # Keep only complete rows
    pivot_100k = pivot_100k.dropna(subset=["PRISONERS_100K", "CAPACITY_100K"])
    
    # Per-100k occupancy (%)
    pivot_100k["OCC_PER_100K"] = (pivot_100k["PRISONERS_100K"] / pivot_100k["CAPACITY_100K"] * 100).round(2)
    
    # 3) MERGE BOTH VERSIONS
    occupancy = pivot_num.join(pivot_100k, how="inner")
    
    # Final dataframe
    occupancy_df = occupancy.reset_index()
    
    # Info
    n_rows, n_countries = len(occupancy_df), occupancy_df['geo'].nunique()
    print(f"Occupancy dataset: {n_rows} observations, {n_countries} countries")
    print(f"Years: {occupancy_df['YEAR'].min()} → {occupancy_df['YEAR'].max()}")
    print("Columns:", occupancy_df.columns.tolist())
    
    return occupancy_df


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
    Compares occupancy statistics between regions for target year.
    
    Args:
        df (pd.DataFrame): Input with ['geo', 'YEAR', 'OCC_ABS']
        region1 (str/list): North countries
        region2 (str/list): South countries  
        year (int): Target year
    
    Returns:
        pd.DataFrame: Flat table with columns ['REGION', 'mean', 'min', 'max', 'count']
                      Ready for CSV export and plotting!
    """
    # Convert inputs to lists
    north_countries = [c.strip() for c in (region1.split(",") if isinstance(region1, str) else region1)]
    south_countries = [c.strip() for c in (region2.split(",") if isinstance(region2, str) else region2)]
    
    def assign_region(country):
        if country in north_countries: return "North"
        if country in south_countries: return "South" 
        return "Other"
    
    # Assign regions
    df_assigned = df.copy()
    df_assigned["REGION"] = df_assigned["geo"].apply(assign_region)
    
    # Filter target year
    latest = df_assigned[df_assigned["YEAR"] == year].copy()
    
    # Groupby with REGION as column (not index!)
    region_stats = (
        latest.groupby("REGION", as_index=False)["OCC_ABS"]  # ✅ as_index=False!
        .agg(["mean", "min", "max", "count"])
        .round(2)
        .reset_index(drop=True)  # Pulizia finale
    )
    
    # Flatten column names (optional, cleaner)
    region_stats.columns = ['REGION', 'mean', 'min', 'max', 'count']
    
    print(f"Region stats for {year}:")
    print(region_stats)
    
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
    
    focus_trends = df[df["geo"].isin(countries_focus)][["geo", "YEAR", "OCC_ABS"]].copy()
    
    n_countries = focus_trends["geo"].nunique()
    print(f"Trends extracted for {n_countries} focus countries: {countries_focus}")
    
    focus_trends = focus_trends.sort_values(["geo", "YEAR"]).reset_index(drop=True)
    
    # columns outcome: geo, YEAR, OCC_ABS
    return focus_trends
