import json
import pandas as pd
import sqlite3
from typing import Optional
from utils.sqlite_db import insert_df_to_db


def mark_records_as_processed(conn: sqlite3.Connection, table_name: str, 
                              pk_col: str, pk_values: list) -> int:
    """
    Mark staging records as processed after successful load to dimension table.
    
    Updates processed flag to True (1) for specified primary key values. Only updates
    records that were successfully validated and loaded. Failed/quarantined records
    remain with processed = False for potential reprocessing.
    
    Args:
        conn: Active SQLite database connection.
        table_name: Name of staging table to update.
        pk_col: Primary key column name (article_id or id_rss).
        pk_values: List of primary key values to mark as processed.
        
    Returns:
        Number of records marked as processed.
    """
    if not pk_values:
        print(f"No records to mark as processed in {table_name}")
        return 0
    
    try:
        cursor = conn.cursor()
        
        # Use parameterized query with IN clause
        placeholders = ','.join('?' * len(pk_values))
        update_query = f"""
            UPDATE {table_name}
            SET processed = 1
            WHERE {pk_col} IN ({placeholders})
        """
        
        cursor.execute(update_query, pk_values)
        conn.commit()
        
        rows_updated = cursor.rowcount
        print(f"Marked {rows_updated} records as processed in {table_name}")
        return rows_updated
        
    except Exception as e:
        print(f"Error marking records as processed in {table_name}: {e}")
        conn.rollback()
        return 0


def extract_first_tag(tags_json: str) -> Optional[str]:
    """
    Extract first tag name from JSON array string.
    
    Parses JSON array from Medium RSS feed tags field and extracts first tag.
    Handles both dict format with 'term' key and direct string format.
    
    Args:
        tags_json: JSON string containing array of tag objects or strings.
        
    Returns:
        First tag name as string, or None if parsing fails or empty.
    """
    try:
        tags = json.loads(tags_json)
        if isinstance(tags, list) and len(tags) > 0:
            if isinstance(tags[0], dict) and 'term' in tags[0]:
                return tags[0]['term']
            elif isinstance(tags[0], str):
                return tags[0]
    except (json.JSONDecodeError, TypeError, KeyError):
        pass
    return None


def extract_first_author(authors_json: str) -> Optional[str]:
    """
    Extract first author name from JSON array string.
    
    Parses JSON array from Medium RSS feed authors field and extracts first author.
    Handles both dict format with 'name' key and direct string format.
    
    Args:
        authors_json: JSON string containing array of author objects or strings.
        
    Returns:
        First author name as string, or None if parsing fails or empty.
    """
    try:
        authors = json.loads(authors_json)
        if isinstance(authors, list) and len(authors) > 0:
            if isinstance(authors[0], dict) and 'name' in authors[0]:
                return authors[0]['name']
            elif isinstance(authors[0], str):
                return authors[0]
    except (json.JSONDecodeError, TypeError, KeyError):
        pass
    return None


def staging_to_dim_articles_gfg(df_clean: pd.DataFrame, conn: sqlite3.Connection) -> bool:
    """
    Load cleaned GFG articles from staging to dim_articles dimension table.
    
    Transforms GFG-specific fields to dimensional model schema. Maps article_id 
    to article_id, extracts publication date from last_updated, sets source_platform 
    to 'GFG'. Handles duplicates with ON CONFLICT strategy. Updates updated_at 
    timestamp on existing records, preserves created_at. After successful load,
    marks source records as processed in staging table.
    
    Args:
        df_clean: Cleaned DataFrame from stg_gfg_articles after DQ validation.
        conn: Active SQLite database connection.
        
    Returns:
        Number of records successfully inserted/updated in dim_articles.
    """
    if df_clean.empty:
        print("No clean GFG records to load")
        return 0
    
    # Transform to dim_articles schema
    dim_df = pd.DataFrame({
        'article_id': df_clean['article_id'],
        'source_platform': 'GFG',
        'title': df_clean['title'],
        'author': df_clean['author_id'],
        'pub_date': pd.to_datetime(df_clean['last_updated'], errors='coerce').dt.strftime('%Y-%m-%d'),
        'link': df_clean['link'],
        'category': df_clean['category'],
        'is_valid': 1
    })
    
    # Clean pub_date NaT values
    dim_df['pub_date'] = dim_df['pub_date'].replace('NaT', None)
    
    try:   
        insert_df_to_db(dim_df, 'dim_articles', conn)
        
        # Mark successfully loaded records as processed
        loaded_ids = df_clean['id'].tolist()
        _= mark_records_as_processed(conn, 'stg_gfg_articles', 'id', loaded_ids)
        
        return True
        
    except Exception as e:
        print(f"Error loading GFG articles to dim_articles: {e}")
        conn.rollback()
        return False


def staging_to_dim_articles_medium(df_clean: pd.DataFrame, conn: sqlite3.Connection) -> bool:
    """
    Load cleaned Medium articles from staging to dim_articles dimension table.
    
    Transforms Medium RSS-specific fields to dimensional model schema. Maps id_rss 
    to article_id, extracts first tag as category, parses authors from JSON array.
    Sets source_platform to 'Medium'. Handles duplicates with ON CONFLICT strategy.
    Updates updated_at timestamp on existing records, preserves created_at. After 
    successful load, marks source records as processed in staging table.
    
    Args:
        df_clean: Cleaned DataFrame from stg_medium_articles after DQ validation.
        conn: Active SQLite database connection.
        
    Returns:
        Number of records successfully inserted/updated in dim_articles.
    """
    if df_clean.empty:
        print("No clean Medium records to load")
        return 0
    
    # Transform to dim_articles schema
    dim_df = pd.DataFrame({
        'article_id': df_clean['id_rss'],
        'source_platform': 'Medium',
        'title': df_clean['title'],
        'author': df_clean['authors'].apply(extract_first_author),
        'pub_date': df_clean['published'],
        'link': df_clean['link'],
        'category': df_clean['tags'].apply(extract_first_tag),
        'is_valid': 1
    })
    
    try:
        insert_df_to_db(dim_df, 'dim_articles', conn)
        
        # Mark successfully loaded records as processed
        loaded_ids = df_clean['id'].tolist()
        _= mark_records_as_processed(conn, 'stg_medium_articles', 'id', loaded_ids)
        
        return True
        
    except Exception as e:
        print(f"Error loading Medium articles to dim_articles: {e}")
        conn.rollback()
        return False


def get_dimension_stats(conn: sqlite3.Connection) -> dict:
    """
    Retrieve summary statistics for dim_articles table.
    
    Provides counts by source platform, total records, and date range of publications.
    Useful for pipeline monitoring and data quality reporting.
    
    Args:
        conn: Active SQLite database connection.
        
    Returns:
        Dictionary containing statistics:
        - total: Total records in dim_articles
        - gfg_count: Number of GFG articles
        - medium_count: Number of Medium articles
        - earliest_pub: Earliest publication date
        - latest_pub: Latest publication date
    """
    cursor = conn.cursor()
    
    stats = {}
    
    # Total records
    cursor.execute("SELECT COUNT(*) FROM dim_articles")
    stats['total'] = cursor.fetchone()[0]
    
    # Counts by platform
    cursor.execute("SELECT source_platform, COUNT(*) FROM dim_articles GROUP BY source_platform")
    for platform, count in cursor.fetchall():
        stats[f'{platform.lower()}_count'] = count
    
    # Date range
    cursor.execute("SELECT MIN(pub_date), MAX(pub_date) FROM dim_articles WHERE pub_date IS NOT NULL")
    earliest, latest = cursor.fetchone()
    stats['earliest_pub'] = earliest
    stats['latest_pub'] = latest
    
    return stats
