# CDM Incremental Model Template — Pattern 1 (Multi-Source Filter-Before-Join)

> **Purpose**: Reference template for AI agents generating Silver-layer CDM models in the RS_Medallion dbt project.  
> **When to use**: Model joins **multiple source tables** (3+) with independent change tracking per source.  
> **See also**: [Pattern 2 — Single-Source + Enrichment](CDM_MODEL_TEMPLATE_PATTERN2.md) for models with one primary source + optional lookup joins.  
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
8. [Real Examples](#8-real-examples)

---

## 1. Architecture Overview

### Pattern 1: Filter-Before-Join

```
┌─────────────────────────────────────────────────────────────────┐
│  CTE_CHANGED_KEYS (only on incremental)                         │
│  ┌─────────-──┐ ┌────────-───┐ ┌───────────-┐                   │
│  │ Source A   │ │ Source B   │ │ Source N   │  ← watermark per  │
│  │ delta keys │ │ delta keys │ │ delta keys │    source table   │
│  └─────┬───-──┘ └─────┬──-───┘ └────-─┬───--┘                   │
│        └───────---─ UNION ────────────┘                         │
│                       │                                         │
│            DISTINCT (key1, key2)  ← composite business key      │
└────────────────────┬────────────────────────────────────────────┘
                     │
    ┌────────────────┼─────-----───────────┐
    ▼                ▼                     ▼
┌─────────┐    ┌─────────┐            ┌─────────┐
│ CTE_A   │    │ CTE_B   │            │ CTE_N   │   ← each CTE filters
│ WHERE   │    │ WHERE   │            │ WHERE   │      to changed keys
│ key IN  │    │ key IN  │            │ key IN  │      on incremental
│ changed │    │ changed │            │ changed │
└────┬────┘    └────┬────┘            └────┬────┘
     │              │                      │
     └──── LEFT JOIN (on business key) ────┘
                     │
              CTE_TRANSFORMATION
                     │
              ┌──────┴───-───┐
              │ Final SELECT │
              │ • surrogate  │
              │ • casts      │
              │ • hash_change│
              │ • hash_full  │
              │ • audit cols │
              └──────┬───-───┘
                     │
               MERGE into {{ this }}
               (where HASH_CHANGE differs)
```

### Key Principles

| Principle | Description |
|-----------|-------------|
| **Independent watermarks** | Each source table has its own `UPDATE_TIMESTAMP_<table>` column in the target, compared against `ODS_UPDATE_TIMESTAMP` in the source |
| **Filter BEFORE join** | Source CTEs are filtered to only changed-key rows on incremental runs, avoiding full table scans |
| **Hash-based merge** | `merge_condition` uses `HASH_CHANGE` comparison so unchanged rows are skipped even if they appear in the delta |
| **Full-refresh safe** | All `{% if is_incremental() %}` blocks are skipped on full-refresh, resulting in complete table scan + rebuild |

---

## 2. File Structure

```
models/silver/cdm/<domain>/
├── <ENTITY>.sql                        # The model (e.g., ACCOUNT.sql)
├── <domain>_sources.yml                # Source definitions (if using {{ source() }})
└── cdm_<domain>_schema/
    └── <ENTITY>.yml                    # Column contracts, tests, meta tags
```

**Naming Conventions:**
- Model file name: `<ENTITY>.sql` (e.g., `ACCOUNT.sql`, `CUSTOMER.sql`, `SITE.sql`)
- Surrogate primary key: `CDM_<ENTITY>_PK` (retains `CDM_` prefix)
- YAML model name: `<ENTITY>` (matches file name without extension)
- Tags: `["cdm", "<domain>"]`

---

## 3. SQL Model Template

```sql
-- ==========================================================================
-- <ENTITY_NAME> Model (CDM Silver Layer)
-- Source: <list source tables> (<DATABASE>.<SCHEMA>)
-- Pattern 1: Multi-source incremental with filter-before-join
--            + hash-based change detection on merge
-- ==========================================================================

{{
    config(
        materialized='incremental',
        unique_key='CDM_<ENTITY>_PK',
        on_schema_change='append_new_columns',
        merge_no_update_columns = ['CDM_INSERT_TIMESTAMP','CDM_INS_BATCH_ID'],
        merge_condition="DBT_INTERNAL_SOURCE.HASH_CHANGE != DBT_INTERNAL_DEST.HASH_CHANGE",
        tags=["cdm","<domain>","scheduled-nightly","data-observe"]
    )
}}


-- ============================================
-- Batch ID for audit trail
-- ============================================
WITH CTE_BATCH_ID AS (
    SELECT
        TO_NUMBER(TO_VARCHAR(CURRENT_TIMESTAMP, 'YYYYMMDDHH24MISSFF3')) AS BATCH_ID,
        CURRENT_TIMESTAMP AS TIME_STAMP
)

-- ============================================
-- (Optional) Static reference data CTEs
-- ============================================
-- Use inline VALUES for small lookup tables that don't warrant their own model
-- , CTE_LOOKUP AS (
--     SELECT ID, DESCRIPTION
--     FROM (VALUES (1,'Value1'), (2,'Value2')) AS V (ID, DESCRIPTION)
-- )

-- ============================================
-- Pattern 1: Identify changed keys from ALL source tables BEFORE joining
-- ============================================
{% if is_incremental() %}
, CTE_CHANGED_KEYS AS (

    -- ── Source A changes ──
    SELECT DISTINCT
        <source_a_key1> AS <BUSINESS_KEY_1>,
        <source_a_key2> AS <BUSINESS_KEY_2>
    FROM {{ ref('<ODS_SOURCE_A>') }}
    WHERE ODS_UPDATE_TIMESTAMP > (
        SELECT MAX(t.UPDATE_TIMESTAMP_<ODS_SOURCE_A>)
        FROM {{ this }} t
    )

    UNION

    -- ── Source B changes ──
    SELECT DISTINCT
        <source_b_key1>,
        <source_b_key2>
    FROM {{ ref('<ODS_SOURCE_B>') }}
    WHERE ODS_UPDATE_TIMESTAMP > (
        SELECT MAX(t.UPDATE_TIMESTAMP_<ODS_SOURCE_B>)
        FROM {{ this }} t
    )

    -- ── (Repeat UNION for each source table) ──

    -- ── INDIRECT SOURCE: When source has no direct business key ──
    -- Resolve back to the business key via a join to a bridge table:
    --
    -- UNION
    -- SELECT DISTINCT bridge.<KEY1>, bridge.<KEY2>
    -- FROM {{ ref('<ODS_INDIRECT_SOURCE>') }} src
    -- INNER JOIN {{ ref('<ODS_BRIDGE_TABLE>') }} bridge
    --     ON bridge.<BRIDGE_KEY> = src.<SOURCE_KEY>
    -- WHERE src.ODS_UPDATE_TIMESTAMP > (
    --     SELECT MAX(t.UPDATE_TIMESTAMP_<ODS_INDIRECT_SOURCE>)
    --     FROM {{ this }} t
    -- )
)
{% endif %}


-- ============================================
-- Source CTEs – filtered to changed keys on incremental runs
-- ============================================

-- ── Source A: <description> ──
, CTE_<SOURCE_A> AS (
    SELECT
        <key_col> AS <BUSINESS_KEY_1>,
        <key_col> AS <BUSINESS_KEY_2>,
        <col1>,
        <col2>,
        ODS_INSERT_TIMESTAMP AS INSERT_TIMESTAMP_<SOURCE_A>,
        ODS_UPDATE_TIMESTAMP AS UPDATE_TIMESTAMP_<SOURCE_A>,
        <ODS_SOURCE_A>_PK
    FROM {{ ref('<ODS_SOURCE_A>') }}
    {% if is_incremental() %}
        WHERE (<key_col>, <key_col>) IN (
            SELECT <BUSINESS_KEY_1>, <BUSINESS_KEY_2>
            FROM CTE_CHANGED_KEYS
        )
    {% endif %}
)

-- ── Source B: <description> ──
, CTE_<SOURCE_B> AS (
    SELECT
        <key_col> AS <BUSINESS_KEY_1>,
        <key_col> AS <BUSINESS_KEY_2>,
        <col1>,
        <col2>,
        ODS_INSERT_TIMESTAMP AS INSERT_TIMESTAMP_<SOURCE_B>,
        ODS_UPDATE_TIMESTAMP AS UPDATE_TIMESTAMP_<SOURCE_B>,
        <ODS_SOURCE_B>_PK
    FROM {{ ref('<ODS_SOURCE_B>') }}
    {% if is_incremental() %}
        WHERE (<key_col>, <key_col>) IN (
            SELECT <BUSINESS_KEY_1>, <BUSINESS_KEY_2>
            FROM CTE_CHANGED_KEYS
        )
    {% endif %}
)

-- ── (Repeat for each source table) ──


-- ============================================
-- Transformation CTE – join all sources
-- ============================================
, CTE_TRANSFORMATION AS (

    SELECT
        -- ── Primary business keys ──
        A.<BUSINESS_KEY_1>,
        A.<BUSINESS_KEY_2>,

        -- ── Source A fields ──
        A.<col1>            AS <BUSINESS_COLUMN_NAME>,
        A.<col2>            AS <BUSINESS_COLUMN_NAME>,
        A.INSERT_TIMESTAMP_<SOURCE_A>,
        A.UPDATE_TIMESTAMP_<SOURCE_A>,
        A.<ODS_SOURCE_A>_PK,

        -- ── Source B fields ──
        B.<col1>            AS <BUSINESS_COLUMN_NAME>,
        B.INSERT_TIMESTAMP_<SOURCE_B>,
        B.UPDATE_TIMESTAMP_<SOURCE_B>,
        B.<ODS_SOURCE_B>_PK,

        -- ── Audit columns ──
        '<SYS1>,<SYS2>'    AS SOURCE_SYSTEM,
        '<ODS_SOURCE_A>,<ODS_SOURCE_B>' AS SOURCE_TABLES,
        A.OPERATION_TYPE    AS OPERATION_TYPE,
        '<SOURCE_A>'        AS PRIMARY_DATA_SOURCE,
        NULL                AS DQ_SCORE,
        'VALID'             AS DQ_STATUS,
        NULL                AS DQ_ERROR_CODE,
        NULL                AS DQ_ERROR_MESSAGE,
        NULL                AS DQ_VALIDATED_DATE

    FROM CTE_<SOURCE_A> A

    -- The driving table (usually the one with the most rows / the
    -- primary business entity) goes in FROM. All others LEFT JOIN.

    LEFT JOIN CTE_<SOURCE_B> B
        ON A.<BUSINESS_KEY_1> = B.<BUSINESS_KEY_1>
        AND A.<BUSINESS_KEY_2> = B.<BUSINESS_KEY_2>

    -- LEFT JOIN CTE_<SOURCE_N> N
    --     ON A.<BUSINESS_KEY_1> = N.<BUSINESS_KEY_1>
    --     AND A.<BUSINESS_KEY_2> = N.<BUSINESS_KEY_2>
)


-- ============================================
-- Final SELECT – surrogate key, casts, hashes, audit
-- ============================================
SELECT
    -- ── Surrogate primary key ──
    {{ generate_surrogate_key(['<BUSINESS_KEY_1>','<BUSINESS_KEY_2>']) }}
        AS CDM_<ENTITY>_PK,

    -- ── Business keys ──
    CAST(<BUSINESS_KEY_1> AS VARCHAR(<len>))    AS <BUSINESS_KEY_1>,
    CAST(<BUSINESS_KEY_2> AS VARCHAR(<len>))    AS <BUSINESS_KEY_2>,

    -- ── Business attributes (cast every column explicitly) ──
    CAST(<col> AS VARCHAR(<len>))               AS <COLUMN_NAME>,
    CAST(<col> AS INT)                          AS <COLUMN_NAME>,
    CAST(<col> AS DATE)                         AS <COLUMN_NAME>,
    CAST(<col> AS CHAR(1))                      AS <FLAG_COLUMN>,

    -- ── Source system audit ──
    COALESCE(SOURCE_SYSTEM, 'ETL')              AS SOURCE_SYSTEM,
    CAST(SOURCE_TABLES AS VARCHAR)              AS SOURCE_TABLES,
    CAST(PRIMARY_DATA_SOURCE AS VARCHAR)        AS PRIMARY_DATA_SOURCE,

    -- ── ODS timestamps (one pair per source table) ──
    CAST(INSERT_TIMESTAMP_<SOURCE_A> AS TIMESTAMP_NTZ(9))
        AS INSERT_TIMESTAMP_<ODS_SOURCE_A>,
    CAST(UPDATE_TIMESTAMP_<SOURCE_A> AS TIMESTAMP_NTZ(9))
        AS UPDATE_TIMESTAMP_<ODS_SOURCE_A>,
    CAST(INSERT_TIMESTAMP_<SOURCE_B> AS TIMESTAMP_NTZ(9))
        AS INSERT_TIMESTAMP_<ODS_SOURCE_B>,
    CAST(UPDATE_TIMESTAMP_<SOURCE_B> AS TIMESTAMP_NTZ(9))
        AS UPDATE_TIMESTAMP_<ODS_SOURCE_B>,

    -- ── ODS primary keys (one per source table) ──
    CAST(<ODS_SOURCE_A>_PK AS VARCHAR(32))      AS <ODS_SOURCE_A>_PK,
    CAST(<ODS_SOURCE_B>_PK AS VARCHAR(32))      AS <ODS_SOURCE_B>_PK,

    -- ── Data quality columns ──
    CAST(DQ_STATUS AS VARCHAR(20))              AS DQ_STATUS,
    CAST(DQ_ERROR_CODE AS VARCHAR(50))          AS DQ_ERROR_CODE,
    CAST(DQ_ERROR_MESSAGE AS VARCHAR(500))      AS DQ_ERROR_MESSAGE,

    -- ── Batch / audit columns ──
    CAST(BATCH_ID AS BIGINT)                    AS CDM_INS_BATCH_ID,
    CAST(BATCH_ID AS BIGINT)                    AS CDM_UPD_BATCH_ID,
    CAST(TIME_STAMP AS TIMESTAMP_NTZ(9))        AS CDM_INSERT_TIMESTAMP,
    CAST(TIME_STAMP AS TIMESTAMP_NTZ(9))        AS CDM_UPDATE_TIMESTAMP,

    -- ── Operation type ──
    CAST(OPERATION_TYPE AS VARCHAR(10))          AS OPERATION_TYPE,

    -- ── Hash columns (auto-generated from YAML metadata) ──
    {{ generate_hash_change(relation = this) }}  AS HASH_CHANGE,
    {{ generate_hash_full(relation = this) }}    AS HASH_FULL

FROM CTE_TRANSFORMATION
JOIN CTE_BATCH_ID ON 1=1
```

---

## 4. YAML Schema Template

The YAML schema drives `generate_hash_change` and `generate_hash_full` macros via `meta` tags.

```yaml
version: 2
models:
  - name: <ENTITY>
    description: >
      <DOMAIN>.<ENTITY>
      
      Purpose: <One-line description of the entity>
      
      Business Context: <What business process does this support?>
      
      Key Relationships: <Referenced by / references>
      
      Data Lineage: Sourced from <systems>
      
      Primary Use Cases: <List>

    config:
      contract:
        enforced: false         # Set true to enforce column-level contracts

    constraints:
      - type: primary_key
        columns:
          - <BUSINESS_KEY_1>    # NOT the surrogate key
          # - <BUSINESS_KEY_2>  # Add if composite
        name: <ENTITY>_PK

    columns:
      # ── Surrogate key ──
      - name: CDM_<ENTITY>_PK
        data_type: varchar
        description: SURROGATE KEY - MD5 HASH OF BUSINESS KEY COLUMNS
        constraints:
          - type: not_null
        meta:
          natural_key: true       # Excluded from HASH_CHANGE

      # ── Business keys ──
      - name: <BUSINESS_KEY_1>
        data_type: varchar(5)
        description: <DESCRIPTION>
        constraints:
          - type: not_null
        meta:
          natural_key: true       # Excluded from HASH_CHANGE

      # ── Business attributes ──
      - name: <COLUMN_NAME>
        data_type: varchar(30)
        description: <DESCRIPTION>

      # ── Flag columns (use CHAR(1), values 'Y'/'N') ──
      - name: IS_<FLAG>
        data_type: char(1)
        description: <Y/N flag description>

      # ── Date columns ──
      - name: <DATE_COLUMN>
        data_type: datetime
        description: <DESCRIPTION>

      # ── ODS timestamps (one pair per source — ALWAYS include) ──
      - name: INSERT_TIMESTAMP_<ODS_SOURCE_A>
        data_type: timestamp_ntz(9)
        description: ODS insert timestamp from <SOURCE_A>
        meta:
          audit_column: true      # Excluded from HASH_CHANGE and HASH_FULL

      - name: UPDATE_TIMESTAMP_<ODS_SOURCE_A>
        data_type: timestamp_ntz(9)
        description: ODS update timestamp from <SOURCE_A>
        meta:
          audit_column: true

      # ── ODS primary keys (one per source) ──
      - name: <ODS_SOURCE_A>_PK
        data_type: varchar(32)
        description: Source record primary key from <SOURCE_A>
        # Note: *_PK columns are auto-excluded from HASH_CHANGE by pattern

      # ── Data quality columns ──
      - name: DQ_STATUS
        data_type: varchar(20)
        description: Data quality validation status
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

      # ── Audit / batch columns ──
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

### Meta Tag Reference

| Meta Tag | Used By | Effect |
|----------|---------|--------|
| `natural_key: true` | `generate_hash_change` | Column excluded from HASH_CHANGE computation |
| `audit_column: true` | `generate_hash_change`, `generate_hash_full` | Column excluded from both hash computations |
| *(pattern)* `*_PK` | `generate_hash_change` | Auto-excluded from HASH_CHANGE by suffix pattern |

---

## 5. dbt_project.yml Configuration

```yaml
models:
  rs_medallion:
    silver:
      cdm:
        database: CDM                   # Target database
        schema: WORKFORCE               # Target schema
        tags: ["cdm", "workforce"]
        <domain>:                        # e.g., customer, site, container
          materialized: incremental
          tags: ["cdm", "<domain>"]
```

---

## 6. Macro Reference

### `generate_surrogate_key(field_list)`
- **Location**: `macros/helpers/generate_surrogate_key.sql`
- **Input**: List of column names (strings)
- **Output**: `MD5(COALESCE(col1,'_dbt_utils_surrogate_key_null_') || '-' || COALESCE(col2,...))`
- **Usage**: `{{ generate_surrogate_key(['KEY1','KEY2']) }} AS CDM_<ENTITY>_PK`

### `generate_hash_change(relation)`
- **Location**: `macros/helpers/generate_hash_change.sql`
- **Input**: `relation = this`
- **Output**: MD5 hash of all columns EXCEPT those tagged `natural_key`, `audit_column`, or ending in `_PK`
- **Usage**: `{{ generate_hash_change(relation = this) }} AS HASH_CHANGE`
- **Requirement**: Columns must be defined in the YAML schema with proper `meta` tags

### `generate_hash_full(relation)`
- **Location**: `macros/helpers/generate_hash_full.sql`
- **Input**: `relation = this`
- **Output**: MD5 hash of all columns EXCEPT those tagged `audit_column`
- **Usage**: `{{ generate_hash_full(relation = this) }} AS HASH_FULL`

---

## 7. Pattern Rules & Conventions

### Incremental Logic Rules

1. **Every source table MUST have its own watermark column pair** in the target:
   - `INSERT_TIMESTAMP_<ODS_TABLE_NAME>` (TIMESTAMP_NTZ(9))
   - `UPDATE_TIMESTAMP_<ODS_TABLE_NAME>` (TIMESTAMP_NTZ(9))

2. **CTE_CHANGED_KEYS watermark comparison** uses `>` (strict greater-than):
   ```sql
   WHERE ODS_UPDATE_TIMESTAMP > (SELECT MAX(t.UPDATE_TIMESTAMP_<TABLE>) FROM {{ this }} t)
   ```

3. **Source CTEs use `IN` filter** against CTE_CHANGED_KEYS:
   ```sql
   {% if is_incremental() %}
       WHERE (<key_col1>, <key_col2>) IN (
           SELECT <BUSINESS_KEY_1>, <BUSINESS_KEY_2> FROM CTE_CHANGED_KEYS
       )
   {% endif %}
   ```

4. **CTE_CHANGED_KEYS is wrapped** in `{% if is_incremental() %}...{% endif %}` at the CTE level.

5. **Source CTEs with existing WHERE clauses** append the incremental filter with `AND`:
   ```sql
   WHERE <existing_condition>
   {% if is_incremental() %}
       AND (<key1>, <key2>) IN (SELECT ... FROM CTE_CHANGED_KEYS)
   {% endif %}
   ```

### Handling Indirect Sources

When a source table does NOT have the business key columns directly:

**In CTE_CHANGED_KEYS** — resolve back to business key via a bridge join:
```sql
-- ARPCR has no division/account; resolve via ARPCC bridge
SELECT DISTINCT bridge.KEY1, bridge.KEY2
FROM {{ ref('ODS_INDIRECT_SOURCE') }} src
INNER JOIN {{ ref('ODS_BRIDGE_TABLE') }} bridge
    ON bridge.BRIDGE_COL = src.SOURCE_COL
WHERE src.ODS_UPDATE_TIMESTAMP > (SELECT MAX(t.UPDATE_TIMESTAMP_...) FROM {{ this }} t)
```

**In the Source CTE** — filter via subquery through the bridge:
```sql
{% if is_incremental() %}
    WHERE src.KEY IN (
        SELECT bridge.KEY FROM {{ ref('ODS_BRIDGE_TABLE') }} bridge
        WHERE (bridge.KEY1, bridge.KEY2) IN (
            SELECT BUSINESS_KEY_1, BUSINESS_KEY_2 FROM CTE_CHANGED_KEYS
        )
    )
{% endif %}
```

### Column Conventions

| Column Type | Data Type | Naming Convention | Example |
|-------------|-----------|-------------------|---------|
| Surrogate PK | `VARCHAR` | `CDM_<ENTITY>_PK` | `CDM_ACCOUNT_PK` |
| Business key | `VARCHAR(n)` | Domain-specific | `ACCOUNT_NUMBER` |
| Boolean flag | `CHAR(1)` | `IS_<THING>` | `IS_ACTIVE` |
| Integer flag | `INT` | `IS_<THING>` | `IS_MANUFACTURING` |
| Date | `DATE` or `DATETIME` | `<THING>_DATE` | `ACCOUNT_OPEN_DATE` |
| Source timestamp | `TIMESTAMP_NTZ(9)` | `INSERT/UPDATE_TIMESTAMP_<ODS_TABLE>` | `UPDATE_TIMESTAMP_ODS_IFP_ARPCU` |
| Source PK | `VARCHAR(32)` | `<ODS_TABLE>_PK` | `ODS_IFP_ARPCU_PK` |
| Batch ID | `BIGINT` | `CDM_INS/UPD_BATCH_ID` | `CDM_INS_BATCH_ID` |
| Hash | `VARCHAR(250)` | `HASH_CHANGE`, `HASH_FULL` | — |

### Config Block Rules

| Config Key | Required | Value | Notes |
|------------|----------|-------|-------|
| `materialized` | ✅ | `'incremental'` | Always incremental for CDM |
| `unique_key` | ✅ | `'CDM_<ENTITY>_PK'` | The surrogate key |
| `on_schema_change` | ✅ | `'append_new_columns'` | Safe schema evolution |
| `merge_no_update_columns` | ✅ | `['CDM_INSERT_TIMESTAMP','CDM_INS_BATCH_ID']` | Preserve original insert audit |
| `merge_condition` | ✅ | `"DBT_INTERNAL_SOURCE.HASH_CHANGE != DBT_INTERNAL_DEST.HASH_CHANGE"` | Skip no-change rows |
| `tags` | ✅ | `["cdm","<domain>","scheduled-nightly","data-observe"]` | Scheduling + observability |

### Driving Table Selection

The **driving table** (in `FROM`, not `LEFT JOIN`) should be:
- The table that defines the universe of entity records
- Usually the largest / most authoritative source
- All other tables LEFT JOIN to it
- Example: `CTE_ARPCU` is the driving table for CDM_ACCOUNT (every account exists in ARPCU)

---

## 8. Real Examples

### ACCOUNT.sql (12 source tables) — Pattern 1

| Source CTE | ODS Model | Join Type | Key | Special Handling |
|------------|-----------|-----------|-----|------------------|
| CTE_ARPCU | ODS_IFP_ARPCU | **FROM** (driving) | `(CUCO, CUCUNO)` | — |
| CTE_BIPCU | ODS_IFP_BIPCU | LEFT JOIN | `(CUCOMP, CUACCT)` | — |
| CTE_BIPIF | ODS_IFP_BIPIF | LEFT JOIN | `(IFCOMP, IFACCT)` | — |
| CTE_ARPCC | ODS_IFP_ARPCC | LEFT JOIN | `(CCCO, CCCUNO)` | — |
| CTE_CDPARTB | ODS_IFP_CDPARTB | LEFT JOIN | `(Company, Account)` | — |
| CTE_CUPCST01 | ODS_IFP_CUPCST01 | LEFT JOIN | `(CST_COMP, CST_ACCT)` | Extra filter: `CST_CNTR = ''` |
| CTE_BIPIFE | ODS_IFP_BIPIFE | LEFT JOIN | `(FECOMP, FEACCT)` | — |
| CTE_ARPCR | ODS_IFP_ARPCR | LEFT JOIN | via `ARPCC.CCCRPC` | Indirect: resolved via ARPCC bridge |
| CTE_BIPAS | ODS_IFP_BIPAS | LEFT JOIN | `(ASCOMP, ASACCT)` | Pre-filter: `ASCOMP='902'` |
| CTE_CUPCUG_CUPCUA | CUPCUA + CUPCUG | LEFT JOIN | `(UACONO, UAACCT)` | Two-table CTE with internal join |
| CTE_CONTAINER_CDM | CONTAINER | LEFT JOIN | `(div, acct)` | GROUP BY with aggregation |
| CTE_SFDC | ODS_SFDC_ACCOUNT | LEFT JOIN | LPAD join logic | Indirect: resolved via ARPCU in CTE_CHANGED_KEYS |

### CUSTOMER.sql (2 source tables) — Pattern 1

| Source CTE | Source | Join Type | Key |
|------------|--------|-----------|-----|
| CTE_customer | CUSTOMER (source) | **FROM** (driving) | `CUSTOMER_ID` |
| CTE_customer_address | CUSTOMER_ADDRESS (source) | INNER JOIN | `CUSTOMER_ID` |

---

## Quick Checklist for New Pattern 1 Models

- [ ] Confirm this is a **multi-source** model (3+ tables). For single-source, use [Pattern 2](CDM_MODEL_TEMPLATE_PATTERN2.md)
- [ ] Identify the **driving table** (defines the entity universe)
- [ ] Identify all **source tables** and their join keys to the driving table
- [ ] Identify any **indirect sources** that need bridge-table resolution
- [ ] Create `<ENTITY>.sql` following the template above
- [ ] Create `<ENTITY>.yml` with `meta: natural_key` on keys and `meta: audit_column` on audit columns
- [ ] Ensure every source table has `INSERT_TIMESTAMP_<TABLE>` and `UPDATE_TIMESTAMP_<TABLE>` in the final SELECT
- [ ] Ensure every source table has `<TABLE>_PK` in the final SELECT
- [ ] Add the model folder to `dbt_project.yml` under `silver.cdm.<domain>`
- [ ] Run `dbt run --select <ENTITY> --full-refresh` for initial load
- [ ] Verify incremental: update a source row, run `dbt run --select <ENTITY>`, confirm only changed rows merge
