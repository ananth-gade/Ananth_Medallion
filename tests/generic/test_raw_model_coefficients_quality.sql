-- Singular test: returns rows when failing.
-- Covers:
--  1) exactly one row exists
--  2) trained_at within 30 days
--  3) baseline keys exist and are zero in coef maps
-- test_raw_model_coefficients_quality.sql

WITH m AS (
    SELECT *
    FROM {{ ref('RAW_MODEL_COEFFICIENTS') }}
),
c AS (
    SELECT
        COUNT(*) AS row_count,
        MAX(TRAINED_AT) AS max_trained_at
    FROM m
),
failures AS (

    -- 1) enforce exactly one row
    SELECT
        'row_count_not_1' AS failure,
        TO_VARIANT(row_count) AS details
    FROM c
    WHERE row_count <> 1

    UNION ALL

    -- 2) staleness (freshness)
    SELECT
        'trained_at_stale_or_null' AS failure,
        TO_VARIANT(max_trained_at) AS details
    FROM c
    WHERE max_trained_at IS NULL
       OR max_trained_at < DATEADD('day', -30, CURRENT_TIMESTAMP())

    UNION ALL

    -- 3) baseline division key exists and equals 0
    SELECT
        'division_baseline_missing_or_nonzero' AS failure,
        TO_VARIANT(DIVISION_XLEVELS[0]) AS details
    FROM m
    WHERE TRY_TO_DOUBLE(GET(DIVISION_COEF, DIVISION_XLEVELS[0])) IS NULL
       OR TRY_TO_DOUBLE(GET(DIVISION_COEF, DIVISION_XLEVELS[0])) <> 0

    UNION ALL

    -- 4) baseline service key exists and equals 0
    SELECT
        'service_baseline_missing_or_nonzero' AS failure,
        TO_VARIANT(SERVICE_XLEVELS[0]) AS details
    FROM m
    WHERE TRY_TO_DOUBLE(GET(SERVICE_COEF, SERVICE_XLEVELS[0])) IS NULL
       OR TRY_TO_DOUBLE(GET(SERVICE_COEF, SERVICE_XLEVELS[0])) <> 0
)

SELECT * FROM failures;
