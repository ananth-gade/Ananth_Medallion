{{
  config(
    materialized = 'incremental',
    unique_key = 'model_execution_id',
    on_schema_change = 'append_new_columns',
    schema = 'DATA_TRANSFORMATION'
  )
}}

-- This model creates the base table structure for dbt_run_results matching Elementary's schema
-- The actual data is populated via the on-run-end hook: capture_run_results()

SELECT
    CAST(NULL AS VARCHAR) AS model_execution_id,
    CAST(NULL AS VARCHAR) AS unique_id,
    CAST(NULL AS VARCHAR) AS invocation_id,
    CAST(NULL AS TIMESTAMP_NTZ) AS generated_at,
    CAST(NULL AS TIMESTAMP_NTZ) AS created_at,
    CAST(NULL AS VARCHAR) AS name,
    CAST(NULL AS VARCHAR) AS message,
    CAST(NULL AS VARCHAR) AS status,
    CAST(NULL AS VARCHAR) AS resource_type,
    CAST(NULL AS FLOAT) AS execution_time,
    CAST(NULL AS TIMESTAMP_NTZ) AS execute_started_at,
    CAST(NULL AS TIMESTAMP_NTZ) AS execute_completed_at,
    CAST(NULL AS TIMESTAMP_NTZ) AS compile_started_at,
    CAST(NULL AS TIMESTAMP_NTZ) AS compile_completed_at,
    CAST(NULL AS NUMBER) AS rows_affected,
    CAST(NULL AS NUMBER) AS rows_inserted,
    CAST(NULL AS NUMBER) AS rows_updated,
    CAST(NULL AS NUMBER) AS rows_deleted,
    CAST(NULL AS BOOLEAN) AS full_refresh,
    CAST(NULL AS VARCHAR) AS compiled_code,
    CAST(NULL AS NUMBER) AS failures,
    CAST(NULL AS VARCHAR) AS query_id,
    CAST(NULL AS VARCHAR) AS thread_id,
    CAST(NULL AS VARCHAR) AS materialization,
    CAST(NULL AS VARCHAR) AS adapter_response
WHERE FALSE  -- This ensures no rows are inserted during model build