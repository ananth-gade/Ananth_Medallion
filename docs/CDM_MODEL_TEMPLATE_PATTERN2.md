# CDM Incremental Model Template — Pattern 2 (Single-Source + Enrichment)

> **Purpose**: Reference template for AI agents generating Silver-layer CDM models with **one primary source** and optional enrichment lookups.  
> **When to use**: Model has a **single primary source table** that drives the entity, with optional LEFT JOINs to lookup/reference tables for enrichment.  
> **See also**: [Pattern 1 — Multi-Source Filter-Before-Join](CDM_MODEL_TEMPLATE.md) for models joining 3+ source tables with independent change tracking.  
> **Platform**: Snowflake · dbt 1.9+ · dbt_utils 1.3+  
> **Last Updated**: 2026-04-07  

---

## Table of Contents

1. [Architecture Overview](#1-architecture-overview)
2. [File Structure](#2-file-structure)
3. [SQL Model Template](#3-sql-model-template)
4. [YAML Schema Template](#4-yaml-schema-template)
5. [dbt_project.yml Configuration](#5-dbt_projectyml-configuration)
6. [Macro Reference](#6-macro-reference)
7. [Pattern Rules & Conventions](#7-pattern-rules--conventions)
8. [Real Example: EMPLOYEE](#8-real-example-employee)

---

## 1. Architecture Overview

### Pattern 2: Single-Source + Enrichment

```
┌───────────────────────────────────────┐
│  CTE_source_data                      │
│  Primary source with watermark filter │
│  (get_max_event_time or MAX subquery) │
└──────────────┬────────────────────────┘
               │
     ┌─────────┼─────────────┐
     ▼         │             ▼
┌──────────┐   │      ┌──────────────────────┐
│(Optional)│   │      │ CTE_enrichment_table │  ← Full scan of
│Reprocess │   │      │ (lookup / reference) │    small reference
│null vals │   │      └──────────┬───────────┘    table
└────┬─────┘   │                 │
     │         │                 │
     ▼         ▼                 │
┌─────────────────────┐          │
│  CTE_combined_source│          │
│  (UNION incremental │          │
│   + reprocess keys) │          │
└──────────┬──────────┘          │
           │                     │
           ▼                     │
┌──────────────────────┐         │
│  CTE_source_enriched │         │
│  (INNER JOIN back to │         │
│   full ODS source)   │         │
└──────────┬───────────┘         │
           │                     │
           └─── LEFT JOIN ───────┘
                     │
              CTE_transformed
              (all CASTs + logic)
                     │
              ┌──────┴──────┐
              │ Final SELECT │
              │ • *          │
              │ • hash_change│
              │ • hash_full  │
              └──────┬──────┘
                     │
               MERGE into {{ this }}
               (where HASH_CHANGE differs)
```

### When to Use Pattern 2 vs Pattern 1

| Criteria | Pattern 1 (Multi-Source) | Pattern 2 (Single-Source + Enrichment) |
|----------|-------------------------|----------------------------------------|
| Primary source tables | 3+ tables of similar importance | 1 dominant source table |
| Change tracking | Independent watermarks per source | Single watermark on primary source |
| Enrichment tables | Each has its own change detection | Static or slowly-changing lookups |
| Join behavior | LEFT JOIN all filtered CTEs | LEFT JOIN to unfiltered lookup tables |
| Reprocessing | Not needed (all sources tracked) | Optional: reprocess rows with null enrichment |
| Complexity | Higher (CTE_CHANGED_KEYS union) | Lower (simple incremental filter) |
| Example | ACCOUNT (12 InfoPro tables) | EMPLOYEE (Workday + ES Users lookup) |

### Key Principles

| Principle | Description |
|-----------|-------------|
| **Single watermark** | Only the primary source table drives incremental filtering via `get_max_event_time()` |
| **Enrichment joins are unfiltered** | Lookup tables (e.g., ES_USERS) are scanned fully — they're small reference tables |
| **Optional reprocessing** | Re-include previously loaded rows where enrichment was NULL, to fill in data that arrived late in the lookup table |
| **`SELECT *` from CTE_transformed** | All CASTs and logic happen in the transformation CTE; final SELECT adds only hashes |

---

## 2. File Structure

```
models/silver/cdm/<domain>/
├── <ENTITY>.sql                        # The model (e.g., EMPLOYEE.sql)
└── <ENTITY>.yml                        # Column contracts, tests, meta tags
```

**Naming Conventions:**
- Model file name: `<ENTITY>.sql` (e.g., `EMPLOYEE.sql`, `SITE.sql`)
- Surrogate primary key: `CDM_<ENTITY>_PK` (retains `CDM_` prefix)
- YAML model name: `<ENTITY>` (matches file name without extension)
- Tags: `["silver", "cdm", "<source_system>", "<domain>", "data-observe"]`

---

## 3. SQL Model Template

```sql
-- ==========================================================================
-- <ENTITY_NAME> Model (CDM Silver Layer)
-- Source: <ODS_PRIMARY_SOURCE> (Bronze Layer)
-- Target: <SCHEMA>.<ENTITY> (Silver CDM Layer)
-- Pattern 2: Single-source incremental with enrichment lookups
--            + hash-based change detection
-- ==========================================================================

{{
    config(
        materialized='incremental',
        unique_key='CDM_<ENTITY>_PK',
        merge_no_update_columns = ['CDM_INS_BATCH_ID', 'CDM_INSERT_TIMESTAMP'],
        merge_condition="DBT_INTERNAL_SOURCE.HASH_CHANGE != DBT_INTERNAL_DEST.HASH_CHANGE",
        tags=["silver","cdm","<source_system>","<domain>","data-observe"]
    )
}}


-- ============================================
-- Step 1: Get incremental delta from PRIMARY source
-- ============================================
WITH CTE_source_data AS (
    SELECT *
    FROM {{ ref('<ODS_PRIMARY_SOURCE>') }}
    {% if is_incremental() %}
        WHERE ODS_UPDATE_TIMESTAMP >= '{{ get_max_event_time('UPDATE_TIMESTAMP_<ODS_PRIMARY_SOURCE>') }}'
    {% endif %}
),


-- ============================================
-- Step 2 (Optional): Reprocess rows with NULL enrichment
-- ============================================
-- Use this when an enrichment lookup may not have had data at first load
-- but may have it now. Only include if reprocessing is needed.

{% if is_incremental() %}
CTE_reprocess_null_<enrichment> AS (
    -- Find rows in target where enrichment column is still NULL
    SELECT DISTINCT
        target.<BUSINESS_KEY>
    FROM {{ this }} target
    WHERE target.<ENRICHMENT_COLUMN> IS NULL
      AND target.<ENRICHMENT_JOIN_KEY> IS NOT NULL
      -- Only reprocess if there's now data in the enrichment table
      AND EXISTS (
          SELECT 1
          FROM {{ ref('<ODS_ENRICHMENT_TABLE>') }} enrichment
          WHERE enrichment.<ENRICHMENT_KEY> = target.<ENRICHMENT_JOIN_KEY>
      )
),
{% endif %}


-- ============================================
-- Step 3: Combine incremental + reprocess records
-- ============================================
CTE_combined_source AS (
    -- Regular incremental records
    SELECT
        <PRIMARY_KEY>,
        'NEW' AS SOURCE_TYPE
    FROM CTE_source_data

    {% if is_incremental() %}
    UNION ALL

    -- Reprocess records to fill NULL enrichment
    SELECT
        ods.<PRIMARY_KEY>,
        'REPROCESS' AS SOURCE_TYPE
    FROM {{ ref('<ODS_PRIMARY_SOURCE>') }} ods
    INNER JOIN CTE_reprocess_null_<enrichment> rp
        ON ods.<PRIMARY_KEY> = rp.<BUSINESS_KEY>
    {% endif %}
),


-- ============================================
-- Step 4: Get full source data for combined keys
-- ============================================
CTE_source_enriched AS (
    SELECT
        ods.*,
        cs.SOURCE_TYPE
    FROM {{ ref('<ODS_PRIMARY_SOURCE>') }} ods
    INNER JOIN CTE_combined_source cs
        ON ods.<PRIMARY_KEY> = cs.<PRIMARY_KEY>
),


-- ============================================
-- Step 5: Enrichment lookup tables (full scan — small tables)
-- ============================================
CTE_<enrichment_table> AS (
    SELECT
        <key_col>,
        <enrichment_col_1>,
        <enrichment_col_2>,
        <ODS_ENRICHMENT_TABLE>_PK,
        ODS_INSERT_TIMESTAMP,
        ODS_UPDATE_TIMESTAMP
    FROM {{ ref('<ODS_ENRICHMENT_TABLE>') }}
),


-- ============================================
-- Step 6: Audit batch ID
-- ============================================
CTE_audits AS (
    SELECT
        TO_NUMBER(TO_VARCHAR(CURRENT_TIMESTAMP, 'YYYYMMDDHH24MISSFF3')) AS batch_id,
        CAST(CURRENT_TIMESTAMP AS TIMESTAMP_NTZ) AS batch_timestamp
),


-- ============================================
-- Step 7: Transformation — all CASTs, CASE logic, joins
-- ============================================
CTE_transformed AS (
    SELECT
        -- Surrogate key
        {{ generate_surrogate_key(['<BUSINESS_KEY>']) }} AS CDM_<ENTITY>_PK,

        -- Business key
        CAST(src.<PRIMARY_KEY> AS VARCHAR(<len>)) AS <BUSINESS_KEY>,

        -- Enrichment columns (from lookup join)
        CAST(enrichment.<enrichment_col> AS <type>) AS <ENRICHED_COLUMN_NAME>,

        -- Source columns (with business logic)
        CAST(src.<col> AS VARCHAR(<len>)) AS <COLUMN_NAME>,
        CAST(src.<col> AS DATE) AS <DATE_COLUMN>,

        -- Flag conversions (Y/N → 1/0 or keep as Y/N)
        CASE WHEN src.<flag_col> = 'Y' THEN 1 ELSE 0 END AS IS_<FLAG>,

        -- Computed columns
        CAST(
            TRIM(CONCAT_WS(' ', src.<first>, src.<last>))
            AS VARCHAR(<len>)
        ) AS <COMPUTED_COLUMN>,

        -- Data lineage
        '<SOURCE_SYSTEM>' AS SOURCE_SYSTEM,
        '<ODS_PRIMARY_SOURCE>' || CASE WHEN enrichment.<key> IS NOT NULL
            THEN ', <ODS_ENRICHMENT_TABLE>' ELSE '' END AS SOURCE_TABLES,
        '<ODS_PRIMARY_SOURCE>' AS PRIMARY_DATA_SOURCE,

        -- ODS primary keys (one per source)
        src.<ODS_PRIMARY_SOURCE>_PK,
        enrichment.<ODS_ENRICHMENT_TABLE>_PK,

        -- ODS timestamps (one pair per source)
        src.ODS_INSERT_TIMESTAMP AS INSERT_TIMESTAMP_<ODS_PRIMARY_SOURCE>,
        src.ODS_UPDATE_TIMESTAMP AS UPDATE_TIMESTAMP_<ODS_PRIMARY_SOURCE>,
        enrichment.ODS_INSERT_TIMESTAMP AS INSERT_TIMESTAMP_<ODS_ENRICHMENT_TABLE>,
        enrichment.ODS_UPDATE_TIMESTAMP AS UPDATE_TIMESTAMP_<ODS_ENRICHMENT_TABLE>,

        -- Batch / audit columns
        CTE_audits.batch_id AS CDM_INS_BATCH_ID,
        CTE_audits.batch_id AS CDM_UPD_BATCH_ID,
        CTE_audits.batch_timestamp AS CDM_INSERT_TIMESTAMP,
        CTE_audits.batch_timestamp AS CDM_UPDATE_TIMESTAMP,

        -- Operation type for CDC tracking
        src.OPERATION_TYPE AS OPERATION_TYPE,

        -- Data quality fields
        CAST(NULL AS VARCHAR(20)) AS DQ_STATUS,
        CAST(NULL AS VARCHAR(50)) AS DQ_ERROR_CODE,
        CAST(NULL AS VARCHAR(500)) AS DQ_ERROR_MESSAGE

    FROM CTE_source_enriched src

    -- Enrichment join (LEFT JOIN — not all rows will match)
    LEFT JOIN CTE_<enrichment_table> enrichment
        ON enrichment.<enrichment_key> = src.<source_join_key>

    CROSS JOIN CTE_audits
)


-- ============================================
-- Step 8: Final SELECT — add hashes
-- ============================================
SELECT
    *,
    -- Hash columns (auto-generated from YAML metadata)
    {{ generate_hash_change(relation = this) }} AS HASH_CHANGE,
    {{ generate_hash_full(relation = this) }} AS HASH_FULL
FROM CTE_transformed
```

---

## 4. YAML Schema Template

```yaml
version: 2
models:
  - name: <ENTITY>
    description: >
      <SCHEMA>.<ENTITY>

      Purpose: <One-line description>

      Business Context: <What business process does this support?>

      Key Relationships: <Referenced by / references>

      Data Lineage: Sourced from <systems>

      Primary Use Cases: <List>

    config:
      contract:
        enforced: false

    constraints:
      - type: primary_key
        columns:
          - <BUSINESS_KEY>
        name: <BUSINESS_KEY>_PK
      # Optional: unique constraint on alternate key
      # - type: unique
      #   columns:
      #     - <ALTERNATE_KEY>
      #   name: CDM_<ENTITY>_AK

    columns:
      # ── Surrogate key ──
      - name: CDM_<ENTITY>_PK
        data_type: varchar
        description: SURROGATE KEY - MD5 HASH OF BUSINESS KEY COLUMNS
        constraints:
          - type: not_null
        meta:
          natural_key: true

      # ── Business key ──
      - name: <BUSINESS_KEY>
        data_type: varchar(15)
        description: <PRIMARY BUSINESS KEY DESCRIPTION>
        constraints:
          - type: not_null
        meta:
          natural_key: true

      # ── Alternate key (optional) ──
      - name: <ALTERNATE_KEY>
        data_type: varchar(40)
        description: <ALTERNATE KEY DESCRIPTION>
        meta:
          natural_key: true

      # ── Enrichment column (from lookup join) ──
      - name: <ENRICHED_COLUMN>
        data_type: integer
        description: <Enriched from ODS_ENRICHMENT_TABLE via join on KEY>

      # ── Business attributes ──
      - name: <COLUMN_NAME>
        data_type: varchar(50)
        description: <DESCRIPTION>

      # ── Flag columns ──
      - name: IS_<FLAG>
        data_type: integer
        description: <1/0 flag description>

      # ── Date columns ──
      - name: <DATE_COLUMN>
        data_type: date
        description: <DESCRIPTION>

      # ── Source system tracking ──
      - name: SOURCE_SYSTEM
        data_type: varchar
        description: Source system identifier
        meta:
          audit_column: true

      - name: SOURCE_TABLES
        data_type: varchar
        description: List of source ODS tables contributing to this row
        meta:
          audit_column: true

      - name: PRIMARY_DATA_SOURCE
        data_type: varchar
        description: Primary ODS table driving this entity
        meta:
          audit_column: true

      # ── ODS timestamps (one pair per source) ──
      - name: INSERT_TIMESTAMP_<ODS_PRIMARY_SOURCE>
        data_type: timestamp_ntz(9)
        description: ODS insert timestamp from primary source
        meta:
          audit_column: true

      - name: UPDATE_TIMESTAMP_<ODS_PRIMARY_SOURCE>
        data_type: timestamp_ntz(9)
        description: ODS update timestamp from primary source
        meta:
          audit_column: true

      - name: INSERT_TIMESTAMP_<ODS_ENRICHMENT_TABLE>
        data_type: timestamp_ntz(9)
        description: ODS insert timestamp from enrichment table
        meta:
          audit_column: true

      - name: UPDATE_TIMESTAMP_<ODS_ENRICHMENT_TABLE>
        data_type: timestamp_ntz(9)
        description: ODS update timestamp from enrichment table
        meta:
          audit_column: true

      # ── ODS primary keys ──
      - name: <ODS_PRIMARY_SOURCE>_PK
        data_type: varchar(32)
        description: Source record PK from primary source

      - name: <ODS_ENRICHMENT_TABLE>_PK
        data_type: varchar(32)
        description: Source record PK from enrichment table

      # ── Batch / audit columns ──
      - name: CDM_INS_BATCH_ID
        data_type: bigint
        description: Batch ID at insert time
        meta:
          audit_column: true

      - name: CDM_UPD_BATCH_ID
        data_type: bigint
        description: Batch ID at last update
        meta:
          audit_column: true

      - name: CDM_INSERT_TIMESTAMP
        data_type: timestamp_ntz(9)
        description: Row insert timestamp
        meta:
          audit_column: true

      - name: CDM_UPDATE_TIMESTAMP
        data_type: timestamp_ntz(9)
        description: Row last update timestamp
        meta:
          audit_column: true

      - name: OPERATION_TYPE
        data_type: varchar(10)
        description: Source operation type (INSERT/UPDATE/DELETE)
        meta:
          audit_column: true

      # ── Data quality ──
      - name: DQ_STATUS
        data_type: varchar(20)
        description: Data quality status
        meta:
          audit_column: true

      - name: DQ_ERROR_CODE
        data_type: varchar(50)
        description: Data quality error code
        meta:
          audit_column: true

      - name: DQ_ERROR_MESSAGE
        data_type: varchar(500)
        description: Data quality error message
        meta:
          audit_column: true

      # ── Hash columns ──
      - name: HASH_CHANGE
        data_type: varchar(250)
        description: MD5 hash of mutable business columns (auto-generated)
        meta:
          audit_column: true

      - name: HASH_FULL
        data_type: varchar(250)
        description: MD5 hash of all non-audit columns (auto-generated)
        meta:
          audit_column: true
```

---

## 5. dbt_project.yml Configuration

Same as Pattern 1:

```yaml
models:
  rs_medallion:
    silver:
      cdm:
        database: CDM
        schema: WORKFORCE
        tags: ["cdm", "workforce"]
        <domain>:
          materialized: incremental
          tags: ["cdm", "<domain>"]
```

---

## 6. Macro Reference

### `get_max_event_time(date_field, not_minus3)`
- **Location**: `macros/helpers/get_max_event_time.sql`
- **Input**: Column name (string), optional `not_minus3` flag
- **Behavior**: 
  - Executes `SELECT MAX(<date_field>) FROM {{ this }}` at **compile time** (Jinja)
  - Default: subtracts 3 minutes for overlap safety (`- interval '3 minutes'`)
  - Pass `not_minus3=true` for exact watermark (no overlap)
  - Returns `'19000101000000000'` if target is empty
- **Usage**: 
  ```sql
  {% if is_incremental() %}
      WHERE ODS_UPDATE_TIMESTAMP >= '{{ get_max_event_time('UPDATE_TIMESTAMP_<TABLE>') }}'
  {% endif %}
  ```
- **Key difference from Pattern 1**: This macro resolves at compile time (string interpolation), whereas Pattern 1 uses runtime subqueries (`SELECT MAX(...) FROM {{ this }}`).

### Other macros

Same as Pattern 1 — see [Pattern 1 Macro Reference](CDM_MODEL_TEMPLATE.md#6-macro-reference):
- `generate_surrogate_key(field_list)`
- `generate_hash_change(relation)`
- `generate_hash_full(relation)`

---

## 7. Pattern Rules & Conventions

### Incremental Logic

1. **Single watermark** on the primary source using `get_max_event_time()`:
   ```sql
   WHERE ODS_UPDATE_TIMESTAMP >= '{{ get_max_event_time('UPDATE_TIMESTAMP_<PRIMARY_SOURCE>') }}'
   ```

2. **Enrichment tables are NOT filtered incrementally** — they are scanned fully since they are small reference/lookup tables.

3. **Reprocessing pattern** (optional): If an enrichment column can be NULL at first load but may be resolvable later:
   ```sql
   {% if is_incremental() %}
   CTE_reprocess_null_<thing> AS (
       SELECT DISTINCT target.<KEY>
       FROM {{ this }} target
       WHERE target.<ENRICHED_COL> IS NULL
         AND target.<JOIN_KEY> IS NOT NULL
         AND EXISTS (
             SELECT 1 FROM {{ ref('<LOOKUP_TABLE>') }} lk
             WHERE lk.<KEY> = target.<JOIN_KEY>
         )
   ),
   {% endif %}
   ```

4. **Combined source** unions incremental + reprocess:
   ```sql
   CTE_combined_source AS (
       SELECT <KEY>, 'NEW' AS SOURCE_TYPE FROM CTE_source_data
       {% if is_incremental() %}
       UNION ALL
       SELECT ods.<KEY>, 'REPROCESS' FROM <full_source> ods
       INNER JOIN CTE_reprocess_null_<thing> rp ON ods.<KEY> = rp.<KEY>
       {% endif %}
   )
   ```

5. **`SELECT *` final pattern** — all CASTs happen in `CTE_transformed`; the final SELECT just adds hashes:
   ```sql
   SELECT *, {{ generate_hash_change(...) }}, {{ generate_hash_full(...) }}
   FROM CTE_transformed
   ```

### Simplified Variant (No Reprocessing)

If there's no enrichment that can arrive late, skip Steps 2-4 and simplify:

```sql
WITH CTE_source_data AS (
    SELECT *
    FROM {{ ref('<ODS_PRIMARY_SOURCE>') }}
    {% if is_incremental() %}
        WHERE ODS_UPDATE_TIMESTAMP >= '{{ get_max_event_time('UPDATE_TIMESTAMP_<ODS_PRIMARY_SOURCE>') }}'
    {% endif %}
),

CTE_<enrichment> AS (
    SELECT <cols> FROM {{ ref('<ODS_ENRICHMENT_TABLE>') }}
),

CTE_audits AS (
    SELECT TO_NUMBER(TO_VARCHAR(CURRENT_TIMESTAMP, 'YYYYMMDDHH24MISSFF3')) AS batch_id,
           CAST(CURRENT_TIMESTAMP AS TIMESTAMP_NTZ) AS batch_timestamp
),

CTE_transformed AS (
    SELECT
        {{ generate_surrogate_key(['<KEY>']) }} AS CDM_<ENTITY>_PK,
        -- all CASTs and business logic here
        ...
    FROM CTE_source_data src
    LEFT JOIN CTE_<enrichment> e ON e.<key> = src.<key>
    CROSS JOIN CTE_audits
)

SELECT *,
    {{ generate_hash_change(relation = this) }} AS HASH_CHANGE,
    {{ generate_hash_full(relation = this) }} AS HASH_FULL
FROM CTE_transformed
```

### Column Conventions

Same as Pattern 1. See [Pattern 1 Column Conventions](CDM_MODEL_TEMPLATE.md#column-conventions).

### Config Block Rules

| Config Key | Required | Value | Notes |
|------------|----------|-------|-------|
| `materialized` | ✅ | `'incremental'` | Always incremental for CDM |
| `unique_key` | ✅ | `'CDM_<ENTITY>_PK'` | The surrogate key |
| `on_schema_change` | Optional | `'append_new_columns'` | Include if schema may evolve |
| `merge_no_update_columns` | ✅ | `['CDM_INS_BATCH_ID', 'CDM_INSERT_TIMESTAMP']` | Preserve original insert audit |
| `merge_condition` | ✅ | `"DBT_INTERNAL_SOURCE.HASH_CHANGE != DBT_INTERNAL_DEST.HASH_CHANGE"` | Skip no-change rows |
| `tags` | ✅ | `["silver","cdm","<source>","<domain>","data-observe"]` | Scheduling + observability |

---

## 8. Real Example: EMPLOYEE

### Overview

| Aspect | Value |
|--------|-------|
| **File** | `models/silver/cdm/workforce/EMPLOYEE.sql` |
| **Primary source** | `ODS_WORKDAY_EMPLOYEE` |
| **Enrichment** | `ODS_ES_EQAI_USERS` (LEFT JOIN on `user_code = NETWORK_USER_ID`) |
| **Reprocessing** | Yes — re-enriches rows where `ES_USER_ID IS NULL` |
| **Business key** | `EMPLOYEE_EIN` (single column) |
| **Unique key** | `CDM_EMPLOYEE_PK` (MD5 of `EMPLOYEE_ID`) |

### Data Flow

```
ODS_WORKDAY_EMPLOYEE ─── watermark filter ──→ CTE_source_data
                                                    │
                    ┌───────────────────────────────┘
                    │
{{ this }} ────→ CTE_reprocess_null_es_users ──→ CTE_combined_source
                                                    │
ODS_WORKDAY_EMPLOYEE ────── INNER JOIN ────────→ CTE_source_enriched
                                                    │
ODS_ES_EQAI_USERS ─────── LEFT JOIN ──────────→ CTE_transformed
                                                    │
                                              SELECT *, HASH_CHANGE, HASH_FULL
```

### Source Table Mapping

| CTE | ODS Model | Join Type | Key | Purpose |
|-----|-----------|-----------|-----|---------|
| CTE_source_data | ODS_WORKDAY_EMPLOYEE | FROM (filtered) | — | Incremental delta |
| CTE_reprocess_null_es_users | {{ this }} + ODS_ES_EQAI_USERS | EXISTS check | `user_code = NETWORK_USER_ID` | Re-enrich nulls |
| CTE_combined_source | — | UNION ALL | `EMPLOYEE_ID` | Merge incremental + reprocess |
| CTE_source_enriched | ODS_WORKDAY_EMPLOYEE | INNER JOIN | `EMPLOYEE_ID` | Full row data for combined keys |
| CTE_es_users | ODS_ES_EQAI_USERS | LEFT JOIN | `user_code = USER_ID` | Enrichment: ES User ID |

### Key Design Decisions

1. **`get_max_event_time` with 3-min overlap**: The default `-3 minutes` safety window ensures no records are missed due to clock skew between Workday CDC and Snowflake load times.

2. **Reprocessing pattern**: When an employee record loads before the corresponding ES_EQAI_USERS record exists, `ES_USER_ID` is NULL. On the next run, the reprocessing CTE detects these rows and re-includes them so the LEFT JOIN can fill in the value.

3. **Dynamic SOURCE_TABLES**: The `SOURCE_TABLES` column conditionally includes the enrichment table name only when a match is found:
   ```sql
   'ODS_WORKDAY_EMPLOYEE' || CASE WHEN es.USER_ID IS NOT NULL
       THEN ', ODS_ES_EQAI_USERS' ELSE '' END AS SOURCE_TABLES
   ```

---

## Pattern Decision Guide

Use this to decide which template to apply:

```
Is there 1 primary source table that defines the entity?
├── YES → Does it need enrichment from lookup tables?
│         ├── YES → Pattern 2 (this template)
│         └── NO  → Pattern 2 simplified (skip reprocessing)
└── NO  → Are there 3+ source tables of similar importance?
          ├── YES → Pattern 1 (CDM_MODEL_TEMPLATE.md)
          └── NO  → Pattern 1 (even for 2 tables, if both drive changes)
```

---

## Quick Checklist for Pattern 2 Models

- [ ] Confirm this is a **single primary source** model. For multi-source, use [Pattern 1](CDM_MODEL_TEMPLATE.md)
- [ ] Identify the **primary source** ODS model (e.g., `ODS_WORKDAY_EMPLOYEE`)
- [ ] Identify any **enrichment/lookup** tables and their join keys
- [ ] Determine if **reprocessing** is needed (can enrichment columns be NULL at first load?)
- [ ] Create `<ENTITY>.sql` following the template above
- [ ] Create `<ENTITY>.yml` with `meta: natural_key` on keys and `meta: audit_column` on audit columns
- [ ] Include `INSERT/UPDATE_TIMESTAMP_<TABLE>` and `<TABLE>_PK` for **every** source (primary + enrichment)
- [ ] Add the model folder to `dbt_project.yml` under `silver.cdm.<domain>`
- [ ] Run `dbt run --select <ENTITY> --full-refresh` for initial load
- [ ] Verify incremental: update primary source, run `dbt run --select <ENTITY>`, confirm merge
- [ ] If reprocessing is used: verify NULL enrichment is filled on subsequent runs
