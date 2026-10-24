# RAG Ecosystem Activity Analytics: Medallion Pipeline (Phase 2)

PySpark pipeline on Databricks that pulls GitHub repository data for five RAG-stack projects and builds the **Bronze** and **Silver** layers of a Lakehouse.

**Author:** M. Yaseen (24L-2515), Taimur Amir (24L-2518)

**Repos tracked:** langchain-ai/langchain, run-llama/llama_index, chroma-core/chroma, qdrant/qdrant, deepset-ai/haystack

---

## 1. Architecture

```
GitHub REST API
      |  01_ingest_raw        (Source -> Raw)
      v
Landing files (newline-delimited JSON in a Unity Catalog volume)
      |  02_raw_to_bronze     (Raw -> Bronze, explicit schemas)
      v
Bronze Delta tables  (bronze_commits, bronze_issues, bronze_quarantine)
      |  03_bronze_to_silver  (Bronze -> Silver, cast + clean + MERGE INTO)
      v
Silver Delta tables  (silver_commits, silver_issues, silver_issue_labels, silver_quarantine)

pipeline_execution_logs  <- one audit row for every step above
```

Everything lives in catalog `workspace`, schema `rag`. Raw files live under `/Volumes/workspace/rag/raw/landing/{load_type}/{endpoint}/{repo}/{ingest_date}/`.

## 2. Repository layout

| Path | What it is |
|---|---|
| `notebooks/00_config_and_helpers` | Widgets (parameters), catalog/schema setup, audit log table and `log_run()` helper |
| `notebooks/01_ingest_raw` | GitHub API to raw JSON files (cursor pagination, rate-limit handling) |
| `notebooks/02_raw_to_bronze` | Explicit `StructType` schemas, Raw to Bronze, quarantine of bad records |
| `notebooks/03_bronze_to_silver` | Casting, PII hashing, dedupe, `MERGE INTO` into Silver |
| `notebooks/04_run_pipeline` | Runner that calls 01, 02 and 03 in order |
| `sample_data/` | Small sample JSON files from the proposal |
| `docs/` | Proposal and phase documentation |

## 3. Requirements checklist

| Requirement | How it is done |
|---|---|
| No schema inference | Every read uses an explicit `StructType`/`StructField` schema (`02_raw_to_bronze`). `inferSchema` is never used. |
| Type casting | ISO timestamp strings are cast to `TimestampType`, counts to `INT`, flags to `BOOLEAN` when moving to Silver. |
| `load_timestamp` everywhere | Present on every Bronze and Silver table, set with `current_timestamp()` at write time. |
| Idempotency with `MERGE INTO` | Silver is written only through `MERGE INTO` on business keys. Re-running on the same data inserts and updates 0 rows. Bronze skips files it has already loaded. |
| Parameterized backfills | No hardcoded paths or dates. Dates, load type, repos, ingest date and batch are notebook widgets (see section 6). |
| Schema drift | Reads use `PERMISSIVE` mode with a corrupt-record column. Records that fail the schema go to `bronze_quarantine`. Records with null keys or failed timestamp casts go to `silver_quarantine`. Writes use `mergeSchema`, and `_raw_json` keeps the full original record so new source columns are never lost. |
| Audit logging | `pipeline_execution_logs` gets one row per file/table per layer (see section 5). |

## 4. Data models

Primary keys are logical. Delta tables do not enforce them, and the `MERGE` keys below are what keep them unique in Silver.

### 4.1 Bronze

Bronze is append-only and keeps the raw structure. The same record can appear more than once if a window is pulled twice. Silver removes those duplicates.

Partitioned by `_source_repo`, `_ingest_date`.

#### `bronze_commits`  (logical key: `_source_repo`, `sha`)

| Column | Type | Notes |
|---|---|---|
| sha | STRING | commit hash |
| html_url | STRING | |
| commit | STRUCT | `author` and `committer` (`name`, `email`, `date` as STRING), `message` STRING, `comment_count` BIGINT, `verification` (`verified` BOOLEAN, `reason` STRING) |
| author | STRUCT | GitHub user: `login`, `id` BIGINT, `type`, `site_admin` BOOLEAN |
| committer | STRUCT | same shape as `author` |
| parents | ARRAY&lt;STRUCT&lt;sha STRING&gt;&gt; | |
| _raw_json | STRING | original JSON line, unchanged |
| _source_file | STRING | file the record came from |
| load_timestamp | TIMESTAMP | when the record was loaded into Bronze |
| _source_repo | STRING | repo name from the file path |
| _ingest_date | STRING | ingest date from the file path |
| _load_type | STRING | full / incremental / backfill |
| _batch_id | STRING | run identifier |

#### `bronze_issues`  (logical key: `_source_repo`, `id`)

Contains issues and pull requests (GitHub returns both from the issues endpoint).

| Column | Type | Notes |
|---|---|---|
| id | BIGINT | |
| number | BIGINT | |
| state | STRING | open / closed |
| state_reason | STRING | |
| title | STRING | |
| body | STRING | free text |
| html_url | STRING | |
| user | STRUCT | `login`, `id`, `type`, `site_admin` |
| labels | ARRAY&lt;STRUCT&gt; | `id` BIGINT, `name`, `color`, `description` |
| assignee | STRUCT | same shape as `user` |
| assignees | ARRAY&lt;STRUCT&gt; | same shape as `user` |
| locked | BOOLEAN | |
| comments | BIGINT | |
| created_at | STRING | cast in Silver |
| updated_at | STRING | cast in Silver |
| closed_at | STRING | cast in Silver |
| author_association | STRING | |
| draft | BOOLEAN | |
| pull_request | STRUCT | `url`, `merged_at` (present only for PRs) |
| _raw_json, _source_file, load_timestamp, _source_repo, _ingest_date, _load_type, _batch_id | | same metadata columns as `bronze_commits` |

#### `bronze_quarantine`

Records that did not match the Bronze schema.

| Column | Type |
|---|---|
| endpoint | STRING |
| raw_record | STRING |
| reason | STRING |
| _source_file | STRING |
| load_timestamp | TIMESTAMP |
| _batch_id | STRING |

### 4.2 Silver

Flattened, typed and deduplicated. Every table has `_row_hash` (SHA-256 of the business columns, used so MERGE only updates rows that really changed), `load_timestamp` and `_batch_id`.

#### `silver_commits`  (primary key: `repo_name`, `sha`)

| Column | Type | Notes |
|---|---|---|
| sha | STRING | |
| repo_name | STRING | |
| author_ts | TIMESTAMP | cast from ISO string |
| committer_ts | TIMESTAMP | cast from ISO string |
| author_name | STRING | kept in Silver only |
| author_login | STRING | kept in Silver only |
| contributor_hash | STRING | SHA-256 of the lower-cased, trimmed author email |
| commit_message | STRING | |
| comment_count | INT | |
| parent_count | INT | |
| is_verified | BOOLEAN | |
| html_url | STRING | |
| event_date | DATE | date of `author_ts` |
| _row_hash | STRING | |
| load_timestamp | TIMESTAMP | |
| _batch_id | STRING | |

#### `silver_issues`  (primary key: `repo_name`, `issue_id`)

| Column | Type | Notes |
|---|---|---|
| issue_id | BIGINT | |
| repo_name | STRING | |
| number | BIGINT | |
| state | STRING | |
| title | STRING | |
| body | STRING | free text, Silver only |
| author_login | STRING | kept in Silver only |
| author_hash | STRING | SHA-256 of the lower-cased login |
| is_pull_request | BOOLEAN | true when `pull_request.url` exists |
| is_merged | BOOLEAN | true when `pull_request.merged_at` exists |
| comment_count | INT | |
| created_ts | TIMESTAMP | cast from ISO string |
| updated_ts | TIMESTAMP | cast from ISO string |
| closed_ts | TIMESTAMP | null while open |
| author_association | STRING | |
| html_url | STRING | |
| event_date | DATE | date of `created_ts` |
| _row_hash | STRING | |
| load_timestamp | TIMESTAMP | |
| _batch_id | STRING | |

#### `silver_issue_labels`  (primary key: `repo_name`, `issue_id`, `label_name`)

| Column | Type |
|---|---|
| issue_id | BIGINT |
| repo_name | STRING |
| label_id | BIGINT |
| label_name | STRING |
| label_color | STRING |
| _row_hash | STRING |
| load_timestamp | TIMESTAMP |
| _batch_id | STRING |

#### `silver_quarantine`

| Column | Type |
|---|---|
| entity | STRING |
| record_key | STRING |
| repo_name | STRING |
| reason | STRING |
| load_timestamp | TIMESTAMP |
| _batch_id | STRING |

**Planned for the next phase:** `silver_contributors` and `silver_pull_requests`. Pull requests are already identifiable in `silver_issues` through `is_pull_request` and `is_merged`.

### 4.3 PII handling

- Commit author emails are never stored in Silver. They are hashed with SHA-256 into `contributor_hash`.
- `author_name` and `author_login` stay in Silver for traceability and are dropped before anything goes to Gold.
- GitHub does not return an email for issue authors, so `author_hash` in `silver_issues` is a hash of the login. It is a different value from `contributor_hash` in commits, so the two cannot be joined to each other directly.

## 5. Audit table: `pipeline_execution_logs`

| Column | Type | Meaning |
|---|---|---|
| run_id | STRING | unique id of the log row |
| batch_id | STRING | id of the whole run |
| layer | STRING | `Source-to-Raw`, `Raw-to-Bronze` or `Bronze-to-Silver` |
| load_type | STRING | full / incremental / backfill |
| parameter_or_file | STRING | repo and endpoint, path pattern, or target table with parameters |
| start_time | TIMESTAMP | |
| end_time | TIMESTAMP | |
| status | STRING | Success or Failure |
| rows_inserted | BIGINT | rows written (from MERGE metrics in Silver) |
| rows_updated | BIGINT | rows updated by MERGE |
| error_message | STRING | filled on failure |

Quick look at recent runs:

```sql
SELECT layer, parameter_or_file, status, rows_inserted, rows_updated, start_time, end_time
FROM workspace.rag.pipeline_execution_logs
ORDER BY start_time DESC LIMIT 20;
```

## 6. Execution guide

### 6.1 One-time setup

1. Clone this repo into Databricks (Workspace, Create, Git folder).
2. Create a GitHub personal access token (read-only, public repos). **Do not commit it.** Type it into the `github_token` widget box when running.
3. Run `00_config_and_helpers` once. It creates schema `workspace.rag`, the volume `workspace.rag.raw` and the log table.

### 6.2 Parameters (widgets)

| Widget | Used in | Meaning | Default |
|---|---|---|---|
| `load_type` | all | `full`, `incremental` or `backfill`. It names the landing folder, so 02 must use the same value as 01. | full |
| `start_date` / `end_date` | 01 | date range to pull | 2024-10-10 / 2026-10-10 |
| `repos` | 01 | comma-separated `owner/repo` list | the 5 repos |
| `endpoints` | 01 | `commits`, `issues` or both | commits,issues |
| `max_pages` | 01 | pages per repo and endpoint, `0` means all | 3 (test) |
| `github_token` | 01 | API token | empty |
| `base_path` | all | root volume path | /Volumes/workspace/rag/raw |
| `ingest_date` | 02, 03 | `*` for all, or one folder date like `2026-10-11` | * |
| `force_reload` | 02 | `true` deletes and reloads files already in Bronze | false |
| `bronze_batch_id` | 03 | `*` for all, or one Bronze batch | * |
| `batch_id` | all | optional fixed id, otherwise generated | generated |

### 6.3 Standard incremental load

Pull only the recent window (the proposal uses a rolling 3-day window), then run the next two layers.

1. `01_ingest_raw` with `load_type = incremental`, `start_date` = 3 days ago, `end_date` = today, `max_pages = 0`.
2. `02_raw_to_bronze` with `load_type = incremental`, `ingest_date = *`.
3. `03_bronze_to_silver` with defaults.

### 6.4 Backfill (any historical window)

1. `01_ingest_raw` with `load_type = backfill` (or `full`), `start_date` and `end_date` set to the window to reload, and `repos` / `endpoints` narrowed if needed.
2. `02_raw_to_bronze` with the same `load_type`.
3. `03_bronze_to_silver`. To reprocess only part of Bronze, set `ingest_date` or `bronze_batch_id`.

To rebuild Silver from Bronze without pulling from GitHub again, run only step 3. To re-read raw files into Bronze, run step 2 with `force_reload = true`.

### 6.5 Runner

`04_run_pipeline` runs the three layers in order, one repo at a time, so one failed repo does not stop the rest. It prints the failed repos at the end. Re-run it with `repos` set to only those repos.

Note: values passed through the runner are saved in Databricks run history. Run `01_ingest_raw` directly if you do not want the token stored there.

### 6.6 Proving idempotency

1. Run `03_bronze_to_silver` twice with the same parameters.
2. The second run logs `rows_inserted = 0` and `rows_updated = 0`, and the Silver row counts stay the same.
3. Run `02_raw_to_bronze` twice. The second run inserts 0 rows because files already loaded are skipped.

## 7. Data volume

| Table | Unique rows | Unique raw JSON (MB) |
|---|---|---|
| bronze_commits | [fill in] | [fill in] |
| bronze_issues | [fill in] | [fill in] |
| **Total** | | **[fill in] MB** |

Window pulled: [start date] to [end date], all 5 repos.

## 8. Notes and limits

- GitHub allows 5,000 authenticated requests per hour. `01_ingest_raw` sleeps and retries when the limit is hit.
- The issues endpoint no longer supports `?page=N` on large repos, so ingestion follows GitHub's `next` links instead.
- Free serverless compute allows one notebook run at a time. Run the notebooks one after another.
- Repeated pulls of the same window create extra raw files and Bronze duplicates. Silver stays correct because of the `MERGE` keys.
