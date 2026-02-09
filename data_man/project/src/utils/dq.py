import pandera as pa
import pandas as pd
import sqlite3
from typing import Dict, Any
from datetime import datetime
from ydata_profiling import ProfileReport
from utils.sqlite_db import SCHEMAS, create_table, insert_df_to_db



# GFG Articles Schema with comprehensive format validation
SCHEMA_GFG_ARTICLES = pa.DataFrameSchema({
    "article_id": pa.Column(
        str, 
        required=True,  # Cannot be NULL or NaN
        checks=[
            pa.Check(lambda s: s.str.len() > 0)  # Cannot be empty string
        ]
    ),
    "title": pa.Column(
        str, 
        required=True,  # Cannot be NULL or NaN
        checks=[
            pa.Check(lambda s: (s.str.len() > 5).all()),  # Minimum 6 characters
            pa.Check(lambda s: (s.str.len() < 200).all())  # Maximum 199 characters
        ]
    ),
    "author_id": pa.Column(
        str, 
        nullable=True,  # Can be NULL
        checks=[
            pa.Check.str_matches(r"^[a-z0-9]+$")  # Alphanumeric only, no special chars
        ]
    ),
    "last_updated": pa.Column(
        str, 
        nullable=True,  # Can be NULL
        checks=[
            pa.Check.str_matches(r"^\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}$")  # Exact format: YYYY-MM-DD HH:MM:SS
        ]
    ),
    "link": pa.Column(
        str, 
        nullable=True,  # Can be NULL
        checks=[
            pa.Check.str_matches(r"^https?://[^\s<>\"{}|\\^`\\[\\]]+(\.[a-zA-Z0-9.-]+)+/?$")  # Valid URL pattern
        ]
    ),
    "category": pa.Column(
        str, 
        nullable=True,  # Can be NULL
        checks=[
            pa.Check.isin(["easy", "medium", "hard"])  # GFG difficulty levels only
        ]
    )
}, strict=False)  # Allow extra columns (id, ingested_at)


def dq_pandera(df: pd.DataFrame, table_name: str, max_dupes: int = 100) -> bool:
    """
    Perform comprehensive data quality validation using Pandera schema.
    
    Args:
        df: Input DataFrame from staging table
        table_name: Name of staging table ('stg_gfg_articles' or 'stg_medium_articles')
        max_dupes: Maximum allowed duplicate article_id values
        
    Returns:
        bool: True if all checks pass, False otherwise
        
    Raises:
        KeyError: If required columns missing
    """
    print(f"\n=== Data Quality Check: {table_name} (n={len(df):,}) ===")
    
    # Quick statistics for key metrics
    dup_id = df['article_id'].duplicated().sum()
    null_title = df['title'].isna().sum()
    short_title = (df['title'].str.len() < 5).sum()
    
    print(f"  Null titles: {null_title}")
    print(f"  Short titles (<6 chars): {short_title}")
    print(f"  Duplicate article_id: {dup_id}")
    
    # Fail-fast checks
    if dup_id > max_dupes:
        print(f"  FAIL: Excessive duplicates ({dup_id} > {max_dupes})")
        return False
    
    if null_title > 0 or short_title > 0:
        print(f"  FAIL: Invalid titles (null: {null_title}, short: {short_title})")
        return False
    
    # Schema validation with format checks
    try:
        SCHEMA_GFG_ARTICLES.validate(df, lazy=False)
        print("  PASS: Schema validation (format, types, required fields)")
        return True
    except pa.errors.SchemaError as e:
        print(f"  FAIL: Schema validation errors:")
        print(f"    {e.failure_cases.head(3)}")
        return False


def save_dq_results(conn: sqlite3.Connection, table_name: str, 
                   df: pd.DataFrame, validation_passed: bool) -> None:
    """
    Save data quality results to audit table in database.
    
    Args:
        conn: SQLite database connection
        table_name: Staging table name
        df: Input DataFrame
        validation_passed: Result of dq_pandera validation
        
    Creates 'dq_audit_log' table if not exists.
    """
    cursor = conn.cursor()
    
    # Create audit table if not exists
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS dq_audit_log (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            table_name TEXT NOT NULL,
            run_timestamp DATETIME DEFAULT CURRENT_TIMESTAMP,
            total_rows INTEGER,
            null_titles INTEGER,
            duplicate_ids INTEGER,
            validation_passed BOOLEAN,
            validation_errors TEXT
        )
    """)
    
    # Log metrics
    cursor.execute("""
        INSERT INTO dq_audit_log 
        (table_name, total_rows, null_titles, duplicate_ids, validation_passed)
        VALUES (?, ?, ?, ?, ?)
    """, (table_name, len(df), df['title'].isna().sum(), 
          df['article_id'].duplicated().sum(), validation_passed))
    
    conn.commit()
    print(f"  Logged DQ results to dq_audit_log")


def dq_profiling(df: pd.DataFrame, filename: str = "dq_report") -> str:
    """
    Generate comprehensive HTML profiling report (sampled for performance).
    
    Args:
        df: Input DataFrame
        filename: Base filename for HTML report
        
    Returns:
        str: Path to generated HTML report
    """
    try:
        sample_size = min(1000, len(df))
        sample_df = df.sample(n=sample_size, random_state=42)
        
        profile = ProfileReport(
            sample_df, 
            title=f"{filename} - Data Quality Report",
            explorative=True
        )
        
        report_path = f"{filename}_{sample_size}rows.html"
        profile.to_file(report_path)
        
        print(f"  Generated profiling report: {report_path}")
        return report_path
    except ImportError:
        print("  ydata-profiling not available - install with: pip install ydata-profiling")
        return ""
    except Exception as e:
        print(f"  Profiling failed: {e}")
        return ""


def quarantine_failed_records(df: pd.DataFrame, conn: sqlite3.Connection, 
                             table_name: str, pk_col: str, 
                             error_msg: str) -> int:
    """
    Quarantine failed records using optimized bulk insert.
    
    Args:
        df: DataFrame containing failed records
        conn: SQLite connection
        table_name: Source staging table
        pk_col: Primary key column name
        error_msg: Validation error description
        
    Returns:
        int: Number of quarantined records
    """
    if len(df) == 0:
        return 0
    
    # Prepare quarantine DataFrame
    quarantine_df = pd.DataFrame({
        'source_table': table_name,
        'pk_column_name': pk_col,
        'pk_value': df[pk_col].astype(str),
        'total_columns': len(df.columns),
        'validation_error': error_msg[:1000]  # Truncate long errors
    })
    
    try:
        insert_df_to_db(quarantine_df, 'dq_quarantine_general', conn, chunksize=1000)
        print(f"  Quarantined {len(quarantine_df)} records via optimized bulk insert")
        return len(quarantine_df)
    except Exception as e:
        print(f"  Bulk quarantine failed: {e}")
        return 0


def extract_failed_records_general(df: pd.DataFrame, conn: sqlite3.Connection, 
                                  table_name: str, pk_col: str, 
                                  dq_schema: pa.DataFrameSchema) -> tuple[pd.DataFrame, int]:
    """
    Extract validation failures and quarantine using optimized insert_df_to_db.
    """
    # Retrieve schema dynamically from SCHEMAS
    schema = SCHEMAS.get(table_name)
    if not schema:
        print(f"Schema not found for table '{table_name}'")
        return False

    # Ensure table exists
    if not create_table(conn, table_name, schema):
        return False
    
    quarantined_count = 0
    
    try:
        # Validate and collect all errors
        errors = dq_schema.validate(df, lazy=True)
        
        if hasattr(errors, 'failure_cases') and len(errors.failure_cases) > 0:
            failed_df = errors.failure_cases
            
            # Quarantine failed records
            quarantined_count = quarantine_failed_records(
                failed_df, conn, table_name, pk_col, str(errors)
            )
            
            # Return clean records only
            clean_df = df.drop(failed_df.index)
            return clean_df, quarantined_count
            
    except pa.errors.SchemaError as e:
        # All records failed
        quarantined_count = quarantine_failed_records(
            df, conn, table_name, pk_col, f"SchemaError: {str(e)}"
        )
        return pd.DataFrame(), quarantined_count
    
    return df, 0


def dq_pipeline_general(df: pd.DataFrame, conn: sqlite3.Connection, 
                       table_name: str, pk_col: str, 
                       schema: pa.DataFrameSchema) -> tuple[bool, pd.DataFrame]:
    """
    Complete generalized DQ pipeline with optimized quarantine.
    
    Args:
        df: Staging DataFrame
        conn: SQLite connection
        table_name: Staging table name
        pk_col: Primary key column
        schema: Pandera schema
        
    Returns:
        tuple[bool, pd.DataFrame]: (all_passed, clean_data)
    """
    print(f"\n=== Optimized DQ Pipeline: {table_name} ===")
    
    # PK duplicate check
    dupes = df[pk_col].duplicated().sum()
    print(f"  PK duplicates ({pk_col}): {dupes}")
    
    if dupes > 100:
        print("  FAIL: Excessive duplicates")
        return False, df
    
    # Extract failures + quarantine
    clean_df, quarantined = extract_failed_records_general(df, conn, table_name, pk_col, schema)
    
    all_clean = len(clean_df) == len(df)
    status = "ALL PASS" if all_clean else f"{quarantined} quarantined"
    
    print(f"  Pipeline complete: {status}")
    print(f"  Input: {len(df):,} → Output clean: {len(clean_df):,}")
    
    return all_clean, clean_df
