import os
import json
from dotenv import load_dotenv
import pandas as pd
from utils.sqlite_db import (
    create_database,
    ingest_csv_to_db,
    transform_gfg,
    drop_table,
    delete_database,
)

# C:\Users\g.evola\repo\my_notion\.env
# C:\Users\Work\Documents\GitHub\my_notion\.env
dotenv_path = r"C:\Users\g.evola\repo\my_notion\.env"
load_dotenv(dotenv_path, override=True)

COOKIES_PATH = r"C:\Users\g.evola\repo\my_notion\src\conf\medium_cookies.json"
OAI_API_KEY = os.getenv("OAI_API_KEY")

if not OAI_API_KEY:
    raise RuntimeError("Missing OAI_API_KEY")

raw_cookies = []
if os.path.exists(COOKIES_PATH):
    with open(COOKIES_PATH, encoding="utf-8") as f:
        raw_cookies = json.load(f)
    print(f"Cookies: {len(raw_cookies)}")

# MAIN SCRIPT

if __name__ == "__main__":

    _= delete_database('data_man/project/data/dg_articles.db')

    conn = create_database('data_man/project/data/dg_articles.db')

    table_name = 'stg_gfg_articles'
    #_= drop_table(conn, 'stg_gfg_articles')

    ingest_csv_to_db('data_man/project/data/GeeksforGeeks_articles.csv', conn, 'stg_gfg_articles', transform_gfg)
    
    df_cols = pd.read_sql_query(f"PRAGMA table_info({table_name})", conn)
    print(f"Columns of {table_name}")
    print(df_cols[['name', 'type', 'pk']].to_string(index=False))