import pandas as pd
from utils.sqlite_db import (
    create_database,
    ingest_csv_to_db,
    transform_gfg,
    delete_database,
    query_to_df,
)
from utils.rss import RSS_FEEDS, ingest_rss_to_db, transform_medium
from utils.dq import dq_pandera, extract_failed_records_general
from utils.dimensions import staging_to_dim_articles_gfg, staging_to_dim_articles_medium


def main() -> None:
    """
    Entry point for the project.
    """

    # db setup
    _= delete_database('data/dg_articles.db')
    conn = create_database('data/dg_articles.db')


    # GeeksforGeeks
    ingest_csv_to_db('data/GeeksforGeeks_articles.csv', conn, 'stg_gfg_articles', transform_gfg)
    df = query_to_df(conn, 'SELECT * FROM stg_gfg_articles')
    print(df.head())


    # Medium - Iterate over all RSS feeds
    for feed in RSS_FEEDS:
        ingest_rss_to_db(feed, conn, 'stg_medium_articles', transform_medium)
    df = query_to_df(conn, 'SELECT * FROM stg_medium_articles')
    print(df.head())


    # DQ checks GeeksforGeeks
    df_gfg = pd.read_sql("SELECT * FROM stg_gfg_articles WHERE processed = false", conn)
    if not df_gfg.empty:
        errors = dq_pandera(df_gfg, "stg_gfg_articles")
        clean_df, quarantined_count = extract_failed_records_general(errors, df_gfg, conn, "stg_gfg_articles", "id")
        if staging_to_dim_articles_gfg(clean_df, conn):
            print(f"GFG data loaded, {quarantined_count} quarantined")
        else:
            print(f"GFG data didnt load, {quarantined_count} quarantined")
    else:
        print("No unprocessed GFG records found")


    # DQ checks Medium
    df_medium = pd.read_sql("SELECT * FROM stg_medium_articles WHERE processed = false", conn)
    if not df_medium.empty:
        errors = dq_pandera(df_medium, "stg_medium_articles")
        clean_df, quarantined_count = extract_failed_records_general(errors, df_medium, conn, "stg_medium_articles", "id")
        if staging_to_dim_articles_medium(clean_df, conn):
            print(f"Medium data loaded, {quarantined_count} quarantined")
        else:
            print(f"Medium data didnt load, {quarantined_count} quarantined")
    else:
        print("No unprocessed Medium records found")


    # Monitoring
    df = query_to_df(conn, 'SELECT * FROM dq_quarantine')
    print(df.head())
    df = query_to_df(conn, 'SELECT * FROM dim_articles')
    print(df.head())


if __name__ == "__main__":
    main()