from dotenv import load_dotenv
import pandas as pd
from utils.sqlite_db import (
    create_database,
    ingest_csv_to_db,
    transform_gfg,
    delete_database,
    query_to_df,
)
from utils.rss import (
    RSS_FEEDS,
    ingest_rss_to_db,
    transform_medium,
)
from utils.dq import dq_pandera, extract_failed_records_general


# C:\Users\g.evola\repo\UNI\FDS02Q001\.env
# C:\Users\Work\Documents\GitHub\UNI\FDS02Q001\.env
dotenv_path = r"C:\Users\Work\Documents\GitHub\UNI\FDS02Q001\.env"
load_dotenv(dotenv_path, override=True)


# MAIN SCRIPT

if __name__ == "__main__":

    # db setup
    _= delete_database('data_man/project/data/dg_articles.db')
    conn = create_database('data_man/project/data/dg_articles.db')


    # GeeksforGeeks
    ingest_csv_to_db('data_man/project/data/GeeksforGeeks_articles.csv', conn, 'stg_gfg_articles', transform_gfg)
    df = query_to_df(conn, 'SELECT * FROM stg_gfg_articles')
    print(df.head())


    # Medium - Iterate over all RSS feeds
    for feed in RSS_FEEDS:
        ingest_rss_to_db(feed, conn, 'stg_medium_articles', transform_medium)
    df = query_to_df(conn, 'SELECT * FROM stg_medium_articles')
    print(df.head())

    # DQ checks GeeksforGeeks
    df_gfg = pd.read_sql("SELECT * FROM stg_gfg_articles WHERE processed = false", conn)
    errors = dq_pandera(df_gfg, "stg_gfg_articles")

    if errors is not None:
        print("\n=== Error Analysis ===")
        print(f"Total validation errors: {len(errors.failure_cases)}")
        print(f"Unique failed records: {errors.failure_cases['index'].dropna().nunique()}")
        print("\nTop failed columns:")
        print(errors.failure_cases['column'].value_counts())
        print("\nTop failed checks:")
        print(errors.failure_cases['check'].value_counts())
        print("\nSample failure cases:")
        print(errors.failure_cases[['column', 'check', 'failure_case']].head(20))

    clean_df, quarantined_count = extract_failed_records_general(
        errors, df_gfg, conn, "stg_gfg_articles", "id"
    )
    df = query_to_df(conn, 'SELECT * FROM dq_quarantine')
    print(df.head())

    # 3. Usa solo dati puliti
    #staging_to_dim_articles(clean_df, conn)
