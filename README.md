# Sparkify Data Warehouse on Amazon Redshift

An ETL pipeline that moves raw event and metadata JSON from S3 into an Amazon Redshift
data warehouse, modeled as a star schema so the analytics team can answer questions like
*"which songs are users listening to?"* with simple aggregate queries instead of scanning
raw logs.

The pipeline has two stages: a `COPY` from S3 into flat staging tables, then a set of
`INSERT ... SELECT` statements that reshape staging data into one fact table and four
dimension tables.

---

## Architecture

```text
   S3 (raw JSON)                 Redshift (staging)              Redshift (star schema)
┌────────────────────┐        ┌─────────────────────┐        ┌────────────────────────┐
│ log-data/          │  COPY  │ staging_events      │ INSERT │ songplays  (fact)      │
│ log_json_path.json │ ─────► │                     │ ─────► │ users      (dim)       │
│ song-data/         │        │ staging_songs       │ SELECT │ songs      (dim)       │
└────────────────────┘        └─────────────────────┘        │ artists    (dim)       │
                                                             │ time       (dim)       │
                                                             └────────────────────────┘
```

Staging tables are intentionally untyped and unconstrained — they mirror the source JSON
one-to-one. All cleaning, deduplication, and type casting happens in the second step.

---

## Data Sources

| Source | S3 path | Format | Contents |
|---|---|---|---|
| Event logs | `s3://udacity-dend/log-data` | JSON, one object per line | User activity from the music app (page views, song plays, auth events) |
| JSONPaths | `s3://udacity-dend/log_json_path.json` | JSONPaths file | Maps log fields to `staging_events` columns, since the keys are camelCase |
| Song metadata | `s3://udacity-dend/song-data` | Nested JSON, partitioned by first 3 letters of track ID | One file per song, with artist and track attributes |

Song data is loaded with `FORMAT AS JSON 'auto'` because the keys already match the
column names. Event data needs the explicit JSONPaths file.

---

## Star Schema

### Fact table

**`songplays`** — one row per song play event (`page = 'NextSong'` only; all other page
types are filtered out during the insert).

| Column | Type | Notes |
|---|---|---|
| `songplay_id` | `INT IDENTITY(0,1)` | Surrogate primary key |
| `start_time` | `TIMESTAMP` | Derived from the epoch-millis `ts` field |
| `user_id` | `INT` | FK → `users` |
| `level` | `VARCHAR` | `free` or `paid` at time of play |
| `song_id` | `VARCHAR` | FK → `songs`, `NULL` when no match found |
| `artist_id` | `VARCHAR` | FK → `artists`, `NULL` when no match found |
| `session_id` | `INT` | |
| `location` | `VARCHAR` | |
| `user_agent` | `VARCHAR` | |

`song_id` and `artist_id` are resolved by joining staging on song title, artist name, and
track duration. Most events do not match a song in the metadata subset, so a large share
of these columns will be `NULL` — that is expected, not a bug.

### Dimension tables

| Table | Grain | Columns |
|---|---|---|
| `users` | One row per user | `user_id`, `first_name`, `last_name`, `gender`, `level` |
| `songs` | One row per song | `song_id`, `title`, `artist_id`, `year`, `duration` |
| `artists` | One row per artist | `artist_id`, `name`, `location`, `latitude`, `longitude` |
| `time` | One row per distinct timestamp | `start_time`, `hour`, `day`, `week`, `month`, `year`, `weekday` |

`users.level` is deduplicated to the most recent event per user, so the dimension holds
the current subscription level rather than a historical one.

---

## Distribution and Sort Keys

Redshift performance here depends almost entirely on avoiding data shuffles during the
fact-to-dimension joins:

| Table | Distribution | Sort key | Reasoning |
|---|---|---|---|
| `songplays` | `DISTKEY(song_id)` | `start_time` | Colocates the fact table with `songs`; time-range filters are the most common access pattern |
| `songs` | `DISTKEY(song_id)` | `song_id` | Matches the fact table so song joins stay local to each slice |
| `artists` | `DISTSTYLE ALL` | `artist_id` | Small table, replicated to every node to make the join free |
| `users` | `DISTSTYLE ALL` | `user_id` | Same reasoning — small and joined constantly |
| `time` | `DISTSTYLE ALL` | `start_time` | Small, and joined on every time-based aggregation |

> Adjust this table if `sql_queries.py` uses a different strategy — it should document
> what the code actually does.

---

## Project Structure

```text
.
├── create_tables.py    # Drops and recreates all staging and analytics tables
├── etl.py              # COPYs S3 data into staging, then loads the star schema
├── sql_queries.py      # All DDL and DML, imported by both scripts
├── dwh.cfg             # Cluster, IAM, and S3 configuration (not committed)
└── README.md
```

---

## Prerequisites

- Python 3.8+ with `psycopg2-binary` installed
- A running Redshift cluster, publicly accessible, with an inbound security group rule on
  port `5439`
- An IAM role attached to the cluster with `AmazonS3ReadOnlyAccess`
- The cluster in the same region as the source bucket (`us-west-2`) to avoid cross-region
  transfer costs and slower `COPY` throughput

---

## Setup

Fill in `dwh.cfg` with your cluster details:

```ini
[CLUSTER]
HOST=your-cluster.abc123.us-west-2.redshift.amazonaws.com
DB_NAME=dev
DB_USER=awsuser
DB_PASSWORD=yourpassword
DB_PORT=5439

[IAM_ROLE]
ARN=arn:aws:iam::your-account-id:role/your-iam-role

[S3]
LOG_DATA='s3://udacity-dend/log-data'
LOG_JSONPATH='s3://udacity-dend/log_json_path.json'
SONG_DATA='s3://udacity-dend/song-data'
REGION='us-west-2'
```

`dwh.cfg` holds live credentials — add it to `.gitignore` and commit a `dwh.cfg.example`
with placeholder values instead.

---

## Running the Pipeline

```bash
python create_tables.py   # Drops and recreates every table — destructive
python etl.py             # COPYs from S3, then loads the star schema
```

`create_tables.py` is idempotent but destructive: it runs `DROP TABLE IF EXISTS` before
every `CREATE`. Run it only when you want to rebuild from scratch.

The `COPY` of `song-data` is the slow step, since it reads a large number of small files.
Expect it to dominate total runtime.

---

## Verification

After `etl.py` finishes, confirm the loads landed:

```sql
SELECT 'staging_events' AS table, COUNT(*) FROM staging_events
UNION ALL SELECT 'staging_songs', COUNT(*) FROM staging_songs
UNION ALL SELECT 'songplays',     COUNT(*) FROM songplays
UNION ALL SELECT 'users',         COUNT(*) FROM users
UNION ALL SELECT 'songs',         COUNT(*) FROM songs
UNION ALL SELECT 'artists',       COUNT(*) FROM artists
UNION ALL SELECT 'time',          COUNT(*) FROM time;
```

If a `COPY` fails or loads zero rows, the error detail is in the system tables:

```sql
SELECT starttime, filename, line_number, colname, err_reason
FROM stl_load_errors
ORDER BY starttime DESC
LIMIT 10;
```

---

## Example Analytical Queries

Top 10 most-played songs:

```sql
SELECT s.title, a.name AS artist, COUNT(*) AS plays
FROM songplays sp
JOIN songs s   ON sp.song_id = s.song_id
JOIN artists a ON sp.artist_id = a.artist_id
GROUP BY s.title, a.name
ORDER BY plays DESC
LIMIT 10;
```

Listening volume by hour of day:

```sql
SELECT t.hour, COUNT(*) AS plays
FROM songplays sp
JOIN time t ON sp.start_time = t.start_time
GROUP BY t.hour
ORDER BY t.hour;
```

Paid versus free activity:

```sql
SELECT level, COUNT(*) AS plays, COUNT(DISTINCT user_id) AS users
FROM songplays
GROUP BY level;
```

---

## Notes

- Pause or delete the Redshift cluster when you are not using it. An idle `dc2.large`
  cluster still bills by the hour.
- Rerunning `etl.py` without `create_tables.py` will duplicate rows — there is no
  upsert logic, so the load is not idempotent on its own.
