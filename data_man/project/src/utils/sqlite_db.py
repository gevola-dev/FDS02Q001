import sqlite3
import os
import pandas as pd
from typing import Optional


SCHEMAS = {
    'stg_gfg_articles': '''
        id INTEGER,
        ingested_at DATETIME DEFAULT CURRENT_TIMESTAMP,
        processed BOOLEAN DEFAULT false,
        article_id TEXT,
        title TEXT,
        author_id TEXT,
        last_updated TEXT,
        link TEXT,
        category TEXT,
        PRIMARY KEY("id" AUTOINCREMENT)
    ''',
    'stg_medium_articles': '''
        id INTEGER,
        ingested_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        processed BOOLEAN DEFAULT false,
        title TEXT,
        title_detail TEXT,  -- JSON o serialized dict
        summary TEXT,
        summary_detail TEXT,
        link TEXT,
        id_rss TEXT,  -- entry['id']
        published TEXT,
        published_parsed TEXT,
        updated TEXT,
        tags TEXT,           -- JSON array of dicts
        authors TEXT,        -- JSON array of dicts
        PRIMARY KEY("id" AUTOINCREMENT)
    ''',
    'dim_articles': '''
        id INTEGER,
        article_id TEXT UNIQUE NOT NULL,
        source_platform TEXT NOT NULL CHECK(source_platform IN ('GFG', 'Medium')),
        title TEXT NOT NULL,
        author TEXT,
        pub_date DATE,
        link TEXT,
        category TEXT,
        is_valid BOOLEAN DEFAULT 1,
        created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
        updated_at DATETIME DEFAULT CURRENT_TIMESTAMP,
        PRIMARY KEY("id" AUTOINCREMENT)
    ''',
    'dq_quarantine': '''
        id INTEGER,
        run_timestamp DATETIME DEFAULT CURRENT_TIMESTAMP,
        source_table TEXT NOT NULL,           -- stg_gfg_articles, stg_medium_articles
        pk_column_name TEXT NOT NULL,         -- 'article_id', 'id_rss'
        pk_value TEXT NOT NULL,               -- valore PK del record sporco
        total_columns INTEGER NOT NULL,       -- numero colonne record
        validation_error TEXT,                -- errore Pandera dettagliato
        PRIMARY KEY("id" AUTOINCREMENT)
    '''
}


def create_database(db_path: str = 'data/demo.db') -> Optional[sqlite3.Connection]:
    """
    Create database directory and return connection safely.

    Args:
        db_path (str): Relative/absolute path to .db file. Defaults to 'data/articles.db'.

    Returns:
        Tuple[bool, Optional[sqlite3.Connection]]: (success, connection). 
        True + conn if OK, False + None if error.

    """
    try:
        # Create directory
        os.makedirs(os.path.dirname(db_path) or '.', exist_ok=True)
        
        # Create db Connection
        conn = sqlite3.connect(db_path)
        print(f"DB connencted: {os.path.abspath(db_path)}")
        return conn
        
    except (sqlite3.Error, OSError, PermissionError) as e:
        print(f"Connection or creation database error '{db_path}': {e}")
        return None


def delete_database(db_path: str = 'data/demo.db') -> bool:
    """
    Safely delete database file and empty parent directory.

    Args:
        db_path (str): Path to .db file. Defaults to 'data/articles.db'.

    Returns:
        bool: True if deleted/success (or not found), False if error.
    """
    try:
        if not os.path.exists(db_path):
            print(f"Database '{db_path}' does not exist (already clean)")
            return True
        
        # Close any open connections
        try:
            conn = sqlite3.connect(db_path, timeout=1)
            conn.close()
        except:
            pass
        
        # Delete DB file
        os.remove(db_path)
        print(f"Database '{db_path}' deleted successfully")
        
        # Clean empty parent directory (optional)
        db_dir = os.path.dirname(db_path)
        if os.path.exists(db_dir) and not os.listdir(db_dir):
            os.rmdir(db_dir)
        
        return True
        
    except PermissionError:
        print(f"Permission denied on '{db_path}' (close DB apps/GUI)")
        return False
    except OSError as e:
        print(f"OS error deleting '{db_path}': {e}")
        return False


def create_table(conn: sqlite3.Connection, table_name: str, schema: str) -> bool:
    """
    Create table if not exists with custom schema.

    Args:
        conn (sqlite3.Connection): Active database connection.
        table_name (str): Name of the table (e.g., 'articles', 'authors').
        schema (str): SQL CREATE TABLE statement without 'CREATE TABLE' prefix.

    Returns:
        bool: True if table created or already exists, False if error.
    """
    cursor = conn.cursor()
    full_sql = f"CREATE TABLE IF NOT EXISTS {table_name} ({schema})"
    
    # Check if table already exists
    cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name=?", (table_name,))
    table_exists = cursor.fetchone() is not None
    
    try:
        cursor.execute(full_sql)
        conn.commit()
        
        if table_exists:
            print(f"Table '{table_name}' already exists")
        else:
            print(f"Table '{table_name}' created successfully")
            print("\n")
            df_cols = pd.read_sql_query(f"PRAGMA table_info({table_name})", conn)
            print(df_cols[['name', 'type', 'pk']].to_string(index=False))
            print("\n")

        return True
            
    except sqlite3.Error as e:
        print(f"Error creating table '{table_name}': {e}")
        conn.rollback()
        return False


def drop_table(conn: sqlite3.Connection, table_name: str) -> bool:
    """
    Drop table if exists safely.

    Args:
        conn (sqlite3.Connection): Active database connection.
        table_name (str): Name of table to drop (e.g., 'stg_gfg_articles').

    Returns:
        bool: True if table dropped or did not exist, False if error.

    Example:
        if drop_table(conn, 'stg_gfg_articles'):
            print("Table dropped successfully")
    """
    try:
        cursor = conn.cursor()

        cursor.execute(f"DROP TABLE IF EXISTS {table_name}")
        conn.commit()
        if cursor.rowcount > 0:
            print(f"Table '{table_name}' dropped")
        else:
            print(f"Table '{table_name}' did not exist")
        return True
    except sqlite3.Error as e:
        print(f"Error dropping table '{table_name}': {e}")
        conn.rollback()
        return False


def transform_gfg(df: pd.DataFrame) -> pd.DataFrame:
    """
    Transform GeeksforGeeks CSV data.

    Args:
        df (pd.DataFrame): Raw CSV DataFrame.

    Returns:
        pd.DataFrame: Transformed DataFrame ready for staging.
    """
    # ID parsing
    df['article_id'] = df['link'].str.split('/').str[-2]

    # Date parsing
    def parse_date(date_str):
        if pd.isna(date_str):
            return pd.NaT
        try:
            return pd.to_datetime(date_str, format='%d %b, %Y')
        except:
            return pd.NaT
    
    df['last_updated'] = df['last_updated'].apply(parse_date)
    
    # YYYY-MM-DD format for staging
    df['last_updated_str'] = df['last_updated'].dt.strftime('%Y-%m-%d')

    return df[['article_id', 'title', 'author_id', 'last_updated', 'link', 'category']]


def ingest_csv_to_db(csv_path: str, conn: sqlite3.Connection, table_name: str, 
                     transform_func=None) -> bool:
    """
    Generic CSV ingestion with dynamic schema lookup and optional transforms.

    Args:
        csv_path (str): Path to input CSV file.
        conn (sqlite3.Connection): Active database connection.
        table_name (str): Target table name (must exist in SCHEMAS).
        transform_func (callable, optional): Transformation function(df) -> df.

    Returns:
        bool: True if ingestion succeeded, False otherwise.
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
        
        # Load and transform CSV
        df = pd.read_csv(csv_path)
        if transform_func:
            df = transform_func(df)
        
        # Bulk insert (AUTOINCREMENT handles id)
        insert_df_to_db(df, 'stg_gfg_articles', conn)
        
        print(f"Ingested {len(df)} rows into '{table_name}'")
        return True
        
    except Exception as e:
        print(f"Error ingesting '{csv_path}' to '{table_name}': {e}")
        return False


def query_to_df(conn: sqlite3.Connection, statement: str, params: Optional[tuple] = None) -> pd.DataFrame:
    """
    Execute custom SQL query and return Pandas DataFrame.

    Args:
        conn (sqlite3.Connection): Active database connection.
        query (str): SQL query string.
        params (Optional[tuple]): Query parameters (safe from SQL injection).

    Returns:
        pd.DataFrame: Query results.
    """
    return pd.read_sql_query(statement, conn, params=params)


def insert_df_to_db(
    df: pd.DataFrame,
    table_name: str,
    conn,
    chunksize: int = 500,
    if_exists: str = 'append'
) -> None:
    """Inserts a pandas DataFrame into SQLite database table with chunking.
    Uses `df.to_sql()` with optimized parameters to avoid SQLite's "too many SQL variables"
    error on large DataFrames. Supports safe append operations while preserving table schema.

    Args:
        df: Input DataFrame to insert into database.
        table_name: Target table name in the database.
        conn: SQLAlchemy connection or engine object.
        chunksize: Number of rows per chunk (default: 500, safe for SQLite).
        if_exists: What to do if table exists: {'fail', 'replace', 'append', 'delete_rows'}.
            Default is 'append' to preserve schema.

    Raises:
        Exception: If insertion fails, falls back to single-row inserts.
    """
    try:
        df.to_sql(
            name=table_name,
            con=conn,
            if_exists=if_exists,
            index=False,
            method='multi',
            chunksize=chunksize
        )
        print(f"Successfully inserted {len(df)} rows into '{table_name}'.")
    except Exception as e:
        print(f"Multi-insert failed for '{table_name}': {e}")
        # Fallback: single-row inserts without method='multi'
        df.to_sql(
            name=table_name,
            con=conn,
            if_exists=if_exists,
            index=False,
            chunksize=chunksize
        )
        print(f"Fallback insert completed: {len(df)} rows into '{table_name}'.")

