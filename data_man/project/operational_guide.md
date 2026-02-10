# Multi-Source Article ETL Pipeline - Operational Guide

This guide provides comprehensive instructions for reproducing, configuring, and running the Multi-Source Article ETL Pipeline with Data Quality validation.

## Overview

### Project Structure
```
project/
├── operational_guide.md
├── README.md     
├── requirements-dev.txt
└── src/  
│   ├── main.py
│   └── utils/              
│       ├── dimensions.py   
│       ├── dq.py       
│       ├── sqlite_db.py
│       ├── llm.py 
│       ├── scraping.py
│       └── rss.py
├── data/
│   ├── articles.db
│   └── GeeksforGeeks_articles.csv
└── docs/
    ├── project_presentation.pptx 
    ├── architecture.drawio
    ├── project_report.md
    └── project_report.pdf
```

## Setup

### Prerequisites
- **Python**: Version 3.10 or 3.11
- **Git**: For version control
- **SQLite**: Bundled with Python (no separate installation needed)

### Environment Setup

#### 1. Clone Repository
```bash
git clone https://github.com/gevola-dev/FDS02Q001.git
cd FDS02Q001
```

#### 2. Create Virtual Environment
```bash
# Create virtual environment
python -m venv venv

# Activate virtual environment
venv\Scripts\activate

```

#### 3. Install Dependencies
```bash
pip install -r requirements-dev.txt
```

**Core dependencies:**
- `pandas`: Data manipulation
- `pandera`: Schema validation
- `beautifulsoup4`: Web scraping
- `requests`: HTTP requests
- `feedparser`: RSS feed parsing
- `pytest`: unit test

### Database Initialization

#### Create Database Schema
```bash
cd data_man/project
python -c "from src.utils.sqlite_db import create_database; create_database('data_man/project/data/dg_articles.db')"
```
## Data Acquisition

### Download GFG file
```python
import kagglehub

# Download latest version
path = kagglehub.dataset_download("ashishjangra27/geeksforgeeks-articles")

print("Path to dataset files:", path)
```

Put the CSV file into data_man/project/data/ path.

## ETL Pipeline Execution

### Full Pipeline Run
```bash
python src/main.py
```

This orchestrates the complete ETL flow:
1. **Database Initialization**: Drop existing database and create fresh schema
2. **Data Ingestion**: Load GFG CSV and Medium RSS feeds into staging tables
3. **Extract Unprocessed**: Query records with `processed = false`
4. **Data Quality Validation**: Apply Pandera schema validation
5. **Quarantine Management**: Isolate failed records with detailed error tracking
6. **Dimensional Loading**: Transform and load clean records to `dim_articles`
7. **Flag Management**: Mark successfully loaded records as `processed = true`
8. **Monitoring**: Display quarantine and dimensional table summaries

### Pipeline Architecture

```
┌─────────────────────────────────────────────────────────────┐
│ 1. DATABASE SETUP                                           │
│    - Drop existing DB (if exists)                           │
│    - Create fresh schema (staging, dimension, quarantine)   │
└─────────────────────────────────────────────────────────────┘
                            ↓
┌─────────────────────────────────────────────────────────────┐
│ 2. DATA INGESTION                                           │
│    GFG: CSV → transform_gfg() → stg_gfg_articles           │
│    Medium: RSS feeds → transform_medium() → stg_medium_*    │
└─────────────────────────────────────────────────────────────┘
                            ↓
┌─────────────────────────────────────────────────────────────┐
│ 3. ETL PROCESSING (per source)                              │
│    ┌─────────────────────────────────────────────────────┐ │
│    │ Extract: SELECT * WHERE processed = false           │ │
│    └─────────────────────────────────────────────────────┘ │
│                        ↓                                     │
│    ┌─────────────────────────────────────────────────────┐ │
│    │ Validate: dq_pandera() - Pandera schema checks     │ │
│    └─────────────────────────────────────────────────────┘ │
│                        ↓                                     │
│    ┌─────────────────────────────────────────────────────┐ │
│    │ Split: extract_failed_records_general()             │ │
│    │   → clean_df (valid records)                        │ │
│    │   → quarantine (failed records to dq_quarantine)    │ │
│    └─────────────────────────────────────────────────────┘ │
│                        ↓                                     │
│    ┌─────────────────────────────────────────────────────┐ │
│    │ Load: staging_to_dim_articles_*()                   │ │
│    │   → Insert/update dim_articles                      │ │
│    │   → Mark records as processed = true                │ │
│    └─────────────────────────────────────────────────────┘ │
└─────────────────────────────────────────────────────────────┘
                            ↓
┌─────────────────────────────────────────────────────────────┐
│ 4. MONITORING                                               │
│    - Display dq_quarantine summary                          │
│    - Display dim_articles sample                            │
└─────────────────────────────────────────────────────────────┘
```

### Pipeline Stages in Detail

#### Stage 0: Database Setup
```python
# Drop existing database for clean slate (development mode)
delete_database('data_man/project/data/dg_articles.db')

# Create fresh database with all schemas
conn = create_database('data_man/project/data/dg_articles.db')
```

**Tables Created:**
- `stg_gfg_articles`: GFG staging with processed flag
- `stg_medium_articles`: Medium staging with processed flag  
- `dim_articles`: Unified dimensional table
- `dq_quarantine`: Failed records tracking

#### Stage 1: Data Ingestion

**GFG CSV Ingestion:**
```python
ingest_csv_to_db(
    'data_man/project/data/GeeksforGeeks_articles.csv', 
    conn, 
    'stg_gfg_articles', 
    transform_gfg
)
```

**Medium RSS Ingestion:**
```python
for feed in RSS_FEEDS:
    ingest_rss_to_db(feed, conn, 'stg_medium_articles', transform_medium)
```

**RSS_FEEDS:**
- `https://medium.com/feed/tag/data-quality`
- `https://medium.com/feed/tag/data-observability`
- `https://medium.com/feed/tag/data-governance`
- `https://medium.com/feed/tag/data-lineage`
- `https://medium.com/feed/tag/data-engineer`

#### Stage 2: Extract Unprocessed Records

**GFG:**
```python
df_gfg = pd.read_sql(
    "SELECT * FROM stg_gfg_articles WHERE processed = false", 
    conn
)
```

**Medium:**
```python
df_medium = pd.read_sql(
    "SELECT * FROM stg_medium_articles WHERE processed = false", 
    conn
)
```

**Note:** Only records with `processed = false` are processed. This enables:
- **Idempotent pipeline**: Safe reruns without duplicates
- **Incremental processing**: Only new/failed records processed
- **Quarantine persistence**: Failed records remain `processed = false`

#### Stage 3: Data Quality Validation

```python
# Apply source-specific validation schemas
errors = dq_pandera(df_gfg, "stg_gfg_articles")
```

**Validation Checks:**
- **Schema compliance**: Column types, nullability
- **Format validation**: URLs, dates, IDs (regex patterns)
- **Business rules**: Category whitelists, length constraints
- **JSON validation**: Medium nested structures (tags, authors)

**Output:** List of validation errors with:
- Schema name
- Failed check details
- Row indices of failed records

#### Stage 4: Quarantine Management

```python
clean_df, quarantined_count = extract_failed_records_general(
    errors, 
    df_gfg, 
    conn, 
    "stg_gfg_articles", 
    "id"
)
```

**Process:**
1. **Split data**: Separate clean vs failed records based on error indices
2. **Insert to quarantine**: Failed records → `dq_quarantine` table with:
   - `source_table`: Origin table name
   - `pk_value`: Primary key of failed record
   - `validation_error`: Detailed error message
   - `quarantine_timestamp`: When quarantined
3. **Return clean data**: Valid records proceed to dimensional loading

**Quarantine Table Structure:**
```sql
CREATE TABLE dq_quarantine (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    source_table TEXT NOT NULL,
    pk_value TEXT NOT NULL,
    validation_error TEXT,
    quarantine_timestamp DATETIME DEFAULT CURRENT_TIMESTAMP
);
```

#### Stage 5: Dimensional Loading

**GFG Loading:**
```python
loaded_count = staging_to_dim_articles_gfg(clean_df, conn)
```

**Medium Loading:**
```python
loaded_count = staging_to_dim_articles_medium(clean_df, conn)
```

**Transformation Logic:**

| **GFG Field** | **dim_articles Field** | **Transformation** |
|---------------|------------------------|-------------------|
| `article_id` | `article_id` | Direct mapping |
| `title` | `title` | Direct mapping |
| `author_id` | `author` | Direct mapping |
| `last_updated` | `pub_date` | Parse to YYYY-MM-DD |
| `link` | `link` | Direct mapping |
| `category` | `category` | Direct mapping |
| - | `source_platform` | Set to 'GFG' |

| **Medium Field** | **dim_articles Field** | **Transformation** |
|------------------|------------------------|-------------------|
| `id_rss` | `article_id` | Direct mapping |
| `title` | `title` | Direct mapping |
| `authors` | `author` | Extract first from JSON array |
| `published` | `pub_date` | RFC 2822 → YYYY-MM-DD |
| `link` | `link` | Direct mapping |
| `tags` | `category` | Extract first tag from JSON |
| - | `source_platform` | Set to 'Medium' |

**Duplicate Handling:**
```sql
INSERT INTO dim_articles (...) 
VALUES (...)
ON CONFLICT(article_id) DO UPDATE SET
    source_platform = excluded.source_platform,
    title = excluded.title,
    ...
    updated_at = CURRENT_TIMESTAMP
```

**Processed Flag Update:**
After successful load, marks source records as `processed = true`:
```python
mark_records_as_processed(conn, 'stg_gfg_articles', 'id', loaded_ids)
```

#### Stage 6: Monitoring Output

```python
# Display quarantine summary
df = query_to_df(conn, 'SELECT * FROM dq_quarantine')
print(df.head())

# Display loaded articles
df = query_to_df(conn, 'SELECT * FROM dim_articles')
print(df.head())
```

### Expected Console Output

```
Successfully inserted 34123 rows into 'stg_gfg_articles'.
   id article_id              title  ...
0   1  gfg-12345  Python Tutorial  ...

Successfully inserted 87 rows into 'stg_medium_articles'.
Successfully inserted 156 rows into 'stg_medium_articles'.
...

Extracted 34123 unprocessed records from stg_gfg_articles
Successfully loaded 34000 GFG articles to dim_articles
Marked 34000 records as processed in stg_gfg_articles
GFG data loaded, 123 quarantined

Extracted 445 unprocessed records from stg_medium_articles
Successfully loaded 430 Medium articles to dim_articles
Marked 430 records as processed in stg_medium_articles
Medium data loaded, 15 quarantined

   id source_table pk_value           validation_error  ...
0   1  stg_gfg_...  gfg-999  Column 'link' failed ...
1   2  stg_medium_... mid-123 Column 'pub_date' ...

   id article_id source_platform        title  ...
0   1  gfg-12345  GFG          Python Tutorial ...
1   2  mid-67890  Medium       Data Quality ...
```

### Incremental Runs

The pipeline supports **idempotent execution**:

```bash
# First run: Process all records
python src/main.py
# Output: 34123 GFG + 445 Medium processed

# Second run (without new data): Nothing to process
python src/main.py
# Output: No unprocessed GFG/Medium records found
```

**To reprocess specific records:**
```sql
-- Reset processed flag for remediation
UPDATE stg_gfg_articles 
SET processed = false 
WHERE id IN (123, 456, 789);
```

Then rerun pipeline:
```bash
python src/main.py
# Output: 3 records reprocessed
```

### Error Handling

Each stage includes error handling:

```python
if not df_gfg.empty:
    errors = dq_pandera(df_gfg, "stg_gfg_articles")
    clean_df, quarantined_count = extract_failed_records_general(...)
    
    if staging_to_dim_articles_gfg(clean_df, conn):
        print(f"GFG data loaded, {quarantined_count} quarantined")
    else:
        print(f"GFG data didn't load, {quarantined_count} quarantined")
else:
    print("No unprocessed GFG records found")
```

**Transaction Safety:**
- All database operations use try-except with rollback
- Failed loads don't affect processed flag
- Quarantine records preserved for debugging


## Testing

### Unit Tests
To be implemented


## Monitoring & Reporting
To be implemented


### Query Examples

#### Articles by Source Platform
```sql
SELECT source_platform, COUNT(*) as count, 
       MIN(pub_date) as earliest, 
       MAX(pub_date) as latest
FROM dim_articles
GROUP BY source_platform;
```

#### Top Categories
```sql
SELECT category, COUNT(*) as article_count
FROM dim_articles
WHERE category IS NOT NULL
GROUP BY category
ORDER BY article_count DESC
LIMIT 10;
```

#### Recent Additions
```sql
SELECT title, author, source_platform, created_at
FROM dim_articles
ORDER BY created_at DESC
LIMIT 20;
```

### Indexing
Create indexes for frequently queried columns:
```sql
CREATE INDEX idx_source_platform ON dim_articles(source_platform);
CREATE INDEX idx_pub_date ON dim_articles(pub_date);
CREATE INDEX idx_category ON dim_articles(category);
```

## Contact & Support

For issues or questions:
- Repository: `https://github.com/gevola-dev/FDS02Q001`
- Documentation: Docs folder
- Author: Giorgio Evola

***

**Last Updated:** February 10, 2026