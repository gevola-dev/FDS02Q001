from dotenv import load_dotenv
import pandas as pd
from utils.sqlite_db import (
    create_database,
    ingest_csv_to_db,
    transform_gfg,
    delete_database,
    query_to_df,
    SCHEMAS,
    create_table,
    insert_df_to_db
)
from utils.medium import (
    RSS_FEEDS,
    parse_rss_feed,
    ingest_rss_to_stg_medium,
)
import sys

# C:\Users\g.evola\repo\UNI\FDS02Q001\.env
# C:\Users\Work\Documents\GitHub\UNI\FDS02Q001\.env
dotenv_path = r"C:\Users\Work\Documents\GitHub\UNI\FDS02Q001\.env"
load_dotenv(dotenv_path, override=True)


# MAIN SCRIPT

if __name__ == "__main__":

    _= delete_database('data_man/project/data/dg_articles.db')

    conn = create_database('data_man/project/data/dg_articles.db')


    # GeeksforGeeks

    #ingest_csv_to_db('data_man/project/data/GeeksforGeeks_articles.csv', conn, 'stg_gfg_articles', transform_gfg)
    #df = query_to_df(conn, 'SELECT * FROM stg_gfg_articles')
    #print(df.head())


    # Medium

    # Retrieve schema dynamically from SCHEMAS
    schema = SCHEMAS.get('stg_medium_articles')
    if not schema:
        print("Schema not found for table stg_medium_articles")
        sys.exit(1)

    # Ensure table exists
    if create_table(conn, 'stg_medium_articles', schema):

        # Iterate over all RSS feeds
        for feed in RSS_FEEDS:

            # Extract parameters for Notion
            feed_url = feed["url"]

            feed = parse_rss_feed(feed_url)
            df = ingest_rss_to_stg_medium(feed)

            insert_df_to_db(df, 'stg_medium_articles', conn)
            
        df_query = query_to_df(conn, 'SELECT count(1) FROM stg_medium_articles')
        print(df_query.head(100))