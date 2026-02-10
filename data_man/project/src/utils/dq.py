import pandera as pa
import pandas as pd
import sqlite3
from typing import Tuple, Optional
from utils.sqlite_db import SCHEMAS, create_table, insert_df_to_db


SCHEMA_GFG_ARTICLES = pa.DataFrameSchema({
    "article_id": pa.Column(
        str, 
        required=True,
        checks=[
            pa.Check(lambda s: s.str.len() > 0, error="article_id cannot be empty")
        ]
    ),
    "title": pa.Column(
        str, 
        required=True,
        checks=[
            pa.Check(lambda s: s.str.len() >= 6, error="title too short"),
            pa.Check(lambda s: s.str.len() <= 200, error="title too long")
        ]
    ),
    "author_id": pa.Column(
        str, 
        nullable=True,
        checks=[
            # Allow alphanumeric, underscores, hyphens, mixed case
            pa.Check.str_matches(r"^[a-zA-Z0-9_-]+$", error="invalid author_id format")
        ]
    ),
    "last_updated": pa.Column(
        str, 
        nullable=True,
        checks=[
            pa.Check.str_matches(
                r"^\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}$",
                error="invalid datetime format"
            )
        ]
    ),
    "link": pa.Column(
        str, 
        nullable=True,
        checks=[
            # Validate URL exists in string (allows markdown wrapping)
            pa.Check.str_contains(
                r"https?://[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}",
                error="missing valid URL"
            )
        ]
    ),
    "category": pa.Column(
        str, 
        nullable=True
    )
}, strict=False)


SCHEMA_MEDIUM_ARTICLES = pa.DataFrameSchema({
    "id_rss": pa.Column(
        str, 
        required=True,
        checks=[
            pa.Check(lambda s: s.str.len() > 0, error="id_rss cannot be empty")
        ]
    ),
    "title": pa.Column(
        str, 
        required=True,
        checks=[
            pa.Check(lambda s: s.str.len() >= 3, error="title too short"),
            pa.Check(lambda s: s.str.len() <= 500, error="title too long")
        ]
    ),
    "title_detail": pa.Column(
        str, 
        nullable=True,
        checks=[
            pa.Check(
                lambda s: s.isna() | s.str.startswith('{') | s.str.startswith('['),
                error="title_detail must be valid JSON format"
            )
        ]
    ),
    "summary": pa.Column(
        str, 
        nullable=True,
        checks=[
            pa.Check(lambda s: s.str.len() <= 5000, error="summary too long")
        ]
    ),
    "summary_detail": pa.Column(
        str, 
        nullable=True,
        checks=[
            pa.Check(
                lambda s: s.isna() | s.str.startswith('{') | s.str.startswith('['),
                error="summary_detail must be valid JSON format"
            )
        ]
    ),
    "link": pa.Column(
        str, 
        nullable=True,
        checks=[
            pa.Check.str_contains(
                r"https?://[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}",
                error="missing valid URL"
            )
        ]
    ),
    "published": pa.Column(
        str, 
        nullable=True,
        checks=[
            pa.Check.str_matches(
                r"^\d{4}-\d{2}-\d{2}$",
                error="published must be in format YYYY-MM-DD"
            )
        ]
    ),
    "published_parsed": pa.Column(
        str, 
        nullable=True,
        checks=[
            pa.Check(lambda s: s.str.len() <= 200, error="published_parsed too long")
        ]
    ),
    "updated": pa.Column(
        str, 
        nullable=True,
        checks=[
            pa.Check.str_matches(
                r"^\d{4}-\d{2}-\d{2}$",
                error="updated must be in format YYYY-MM-DD"
            )
        ]
    ),
    "tags": pa.Column(
        str, 
        nullable=True,
        checks=[
            pa.Check(
                lambda s: s.isna() | s.str.startswith('['),
                error="tags must be valid JSON array"
            )
        ]
    ),
    "authors": pa.Column(
        str, 
        nullable=True,
        checks=[
            pa.Check(
                lambda s: s.isna() | s.str.startswith('['),
                error="authors must be valid JSON array"
            )
        ]
    )
}, strict=False)


def dq_pandera(df: pd.DataFrame, table_name: str) -> Optional[pa.errors.SchemaErrors]:
    """
    Perform comprehensive data quality validation using Pandera schema.
    
    Validates DataFrame against predefined schema
    and comprehensive field validation. Uses lazy validation to collect all errors.
    
    Args:
        df: Input DataFrame from staging table to validate.
        table_name: Name of staging table for logging purposes.
        
    Returns:
        SchemaErrors object containing all validation failures if errors exist,
        None if validation passes.
    """    
    try:
        # Use lazy validation to collect all errors in a single pass
        if table_name == 'stg_medium_articles':
            SCHEMA_MEDIUM_ARTICLES.validate(df, lazy=True)
        elif table_name == 'stg_gfg_articles':
            SCHEMA_GFG_ARTICLES.validate(df, lazy=True)
        else:
            raise ValueError (f"No schema configuration for table: {table_name}")
        print(f"PASS: Schema validation {table_name}")
        return None
        
    except pa.errors.SchemaErrors as e:
        print(f"FAIL: {len(e.failure_cases)} validation errors {table_name}")
        return e


def quarantine_failed_records(df_failed: pd.DataFrame, 
                             conn: sqlite3.Connection, 
                             table_name: str, 
                             pk_col: str, 
                             errors: pa.errors.SchemaErrors) -> int:
    """
    Insert failed validation records into quarantine table using bulk operations.
    
    Creates quarantine records with metadata about validation failures including
    source table, primary key values, and error descriptions. Uses chunked inserts
    for performance with large datasets.
    
    Args:
        df_failed: DataFrame containing complete failed records from original data.
        conn: Active SQLite database connection.
        table_name: Source staging table name for tracking.
        pk_col: Primary key column name in source table.
        errors: Pandera SchemaErrors object containing validation failure details.
        
    Returns:
        Number of records successfully quarantined, 0 if operation fails.
    """
    if len(df_failed) == 0:
        return 0
    
    # Aggregate error details by record index for detailed tracking
    error_summary = (
        errors.failure_cases
        .groupby('index')['column']
        .apply(lambda x: ', '.join(x.unique()))
        .to_dict()
    )
    
    # Prepare quarantine DataFrame with standardized structure
    quarantine_df = pd.DataFrame({
        'source_table': table_name,
        'pk_column_name': pk_col,
        'pk_value': df_failed[pk_col].astype(str),
        'total_columns': len(df_failed.columns),
        'validation_error': df_failed.index.map(
            lambda idx: f"Failed columns: {error_summary.get(idx, 'unknown')}"
        )
    })
    
    try:
        # Use bulk insert with chunking for performance
        insert_df_to_db(quarantine_df, 'dq_quarantine', conn, chunksize=1000)
        print(f"  Quarantined {len(quarantine_df)} records")
        return len(quarantine_df)
    except Exception as e:
        print(f"  Quarantine bulk insert failed: {e}")
        return 0


def extract_failed_records_general(errors: Optional[pa.errors.SchemaErrors], 
                                  df_original: pd.DataFrame, 
                                  conn: sqlite3.Connection, 
                                  table_name: str, 
                                  pk_col: str) -> Tuple[pd.DataFrame, int]:
    """
    Separate clean and failed records based on Pandera validation results.
    
    Extracts unique failed record indices from validation errors, retrieves complete
    records from original DataFrame, sends them to quarantine, and returns clean subset.
    Ensures quarantine table exists before attempting inserts.
    
    Args:
        errors: Pandera SchemaErrors object from validation, None if no errors.
        df_original: Original DataFrame before validation.
        conn: Active SQLite database connection.
        table_name: Source staging table name for quarantine tracking.
        pk_col: Primary key column name for record identification.
        
    Returns:
        Tuple containing (clean_dataframe, quarantined_count). Clean DataFrame has
        failed records removed and index reset. Quarantined count is 0 if no errors
        or operation fails.
    """
    # Retrieve quarantine table schema
    schema = SCHEMAS.get('dq_quarantine')
    if not schema:
        print("Schema not found for table dq_quarantine")
        raise ValueError

    # Ensure quarantine table exists
    if not create_table(conn, 'dq_quarantine', schema):
        print("Failed to create quarantine table")
        raise ValueError
    
    # Handle no errors case
    if errors is None or not hasattr(errors, 'failure_cases') or len(errors.failure_cases) == 0:
        print("No validation failures")
        return df_original, 0
    
    # Extract unique failed record indices, filtering out None values
    # None indices indicate DataFrame-level errors (e.g., schema-wide issues)
    failed_indices = errors.failure_cases['index'].dropna().unique()
    
    # Handle case where all errors are DataFrame-level (no specific row indices)
    if len(failed_indices) == 0:
        error_types = errors.failure_cases['check'].unique()
        print("  CRITICAL: DataFrame-level validation errors detected")
        print(f"  Error types: {error_types.tolist()}")
        print(f"  Failed checks: {errors.failure_cases[['column', 'check', 'failure_case']].to_dict('records')}")
        raise ValueError(
            f"Critical schema validation failure for {table_name}. "
            f"DataFrame-level errors: {error_types.tolist()}. "
        )
    
    print(f"Found {len(failed_indices)} unique failed records (from {len(errors.failure_cases)} total validation errors)")
    
    # Retrieve complete failed records using indices
    df_failed_complete = df_original.loc[failed_indices].copy()
    
    # Send failed records to quarantine
    quarantined_count = quarantine_failed_records(
        df_failed_complete, conn, table_name, pk_col, errors
    )
    
    # Remove failed records from original DataFrame
    clean_df = df_original.drop(failed_indices).reset_index(drop=True)
    print(f"Clean records returned: {len(clean_df)}")
    
    return clean_df, quarantined_count

