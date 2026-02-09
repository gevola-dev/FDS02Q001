import feedparser
import json
import pandas as pd
import sqlite3

from utils.sqlite_db import (
    SCHEMAS,
    create_table,
    insert_df_to_db
)


RSS_FEEDS = [
    {
        "url": "https://medium.com/feed/tag/data-quality",
    },
    {
        "url": "https://medium.com/feed/tag/data-observability",
    },
    {
        "url": "https://medium.com/feed/tag/data-governance",
    },
    {
        "url": "https://medium.com/feed/tag/data-lineage",
    },
    {
        "url": "https://medium.com/feed/tag/data-engineer",
    },
]


def get_feed_df(rss_url: str) -> pd.DataFrame:
    """Parses RSS/Atom feed and returns raw entries.

    Logs bozo exceptions, returns empty list on parse failure.

    Args:
        rss_url (str): RSS feed URL.

    Returns:
        pd.DataFrame: DataFrame of feedparser entry dicts, or empty on error.
    """
    feed = feedparser.parse(rss_url)
    if feed.bozo:
        print(f"Error parsing {rss_url}: {feed.bozo_exception}")
        return pd.DataFrame()
    return pd.DataFrame(feed.entries)


def transform_medium(df: pd.DataFrame) -> pd.DataFrame:
    """Transforms Medium RSS DataFrame for staging table.

    Flattens nested dicts (title_detail, tags, authors) to JSON strings using vectorized apply.
    Deduplicates by link/id_rss. Handles missing values safely with fillna.

    Args:
        df: Input DataFrame from parse_rss_feed (feed.entries as rows).

    Returns:
        pd.DataFrame: Cleaned DataFrame ready for insert (deduplicated, flattened).
    """
    if df.empty:
        print("Input DataFrame is empty.")
        return df

    # Select and rename core columns (safe if missing)
    flat_df = df[['title', 'title_detail', 'summary', 'summary_detail', 
                  'link', 'id', 'published', 'published_parsed', 'updated', 
                  'tags', 'authors']].copy()
    flat_df.rename(columns={'id': 'id_rss'}, inplace=True)

    # Vectorized JSON dump for nested fields (dict/list -> string)
    json_cols = ['title_detail', 'summary_detail', 'tags', 'authors']
    for col in json_cols:
        flat_df[col] = flat_df[col].apply(
            lambda x: json.dumps(x) if isinstance(x, (dict, list)) else json.dumps({})
        )

    # Safe string cleaning for scalar fields
    scalar_cols = ['title', 'summary', 'link', 'id_rss', 'published', 
                   'published_parsed', 'updated']
    for col in scalar_cols:
        flat_df[col] = flat_df[col].fillna('').astype(str)

    # Deduplicate prioritizing latest by link/id_rss
    initial_count = len(flat_df)
    flat_df = flat_df.drop_duplicates(subset=['link', 'id_rss'], keep='last')
    print(f"Deduplicated: {initial_count} -> {len(flat_df)} rows")

    if len(flat_df) == 0:
        print("No entries after deduplication.")

    return flat_df


def ingest_rss_to_db(rss: dict, conn: sqlite3.Connection, table_name: str, 
                     transform_func=None) -> bool:
    """

    """
    try:
        # Retrieve schema dynamically from SCHEMAS
        schema = SCHEMAS.get(table_name)
        if not schema:
            print(f"Schema not found for table '{table_name}'")
            return False

        # Ensure table exists
        if not create_table(conn, table_name, schema):
            return False
        
        df = get_feed_df(rss["url"])
        if transform_func:
            df = transform_func(df)
        
        # Bulk insert (AUTOINCREMENT handles id)
        insert_df_to_db(df, 'stg_medium_articles', conn)

        print(f"Ingested {len(df)} rows into '{table_name}'")
        return True
        
    except Exception as e:
        print(f"Error ingesting to '{table_name}': {e}")
        return False
