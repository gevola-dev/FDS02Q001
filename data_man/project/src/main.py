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
from utils.dq import dq_pandera, dq_profiling



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

    # DQ checks
    df_gfg = pd.read_sql("SELECT * FROM stg_gfg_articles WHERE processed = false", conn)
    if dq_pandera(df_gfg, "GFG", max_dupes=100):
        print("✅ GFG DQ PASS!")
        #staging_to_dim_articles(conn)  # Procedi!
        

    # GFG pipeline
    all_clean, clean_gfg = dq_pipeline_general(
        df_gfg, conn, "stg_gfg_articles", "article_id", SCHEMA_GFG_ARTICLES
    )

    # Medium pipeline  
    all_clean_medium, clean_medium = dq_pipeline_general(
        df_medium, conn, "stg_medium_articles", "id_rss", SCHEMA_MEDIUM_ARTICLES
    )

    # Load solo dati puliti (usa insert_df_to_db internamente)
    pd.concat([clean_gfg, clean_medium]).to_sql(
        'dim_articles_temp', conn, if_exists='replace', index=False
    )
