-- CDM CUSTOMER Model
-- Source: CUSTOMER & CUSTOMER_ADDRESS (WORKDAY.ODS)
-- Pattern: Incremental with independent delta filters on both source tables
--          + hash-based change detection on merge

{{
    config(
        materialized='incremental',
        unique_key='CUSTOMER_ADDRESS_PK',
        merge_no_update_columns=['CDM_INSERT_TIMESTAMP', 'CDM_INS_BATCH_ID'],
        merge_condition="DBT_INTERNAL_SOURCE.HASH_CHANGE != DBT_INTERNAL_DEST.HASH_CHANGE",
        tags=["cdm", "customer"]
    )
}}

WITH CTE_batch AS (
    SELECT
        TO_NUMBER(TO_VARCHAR(CURRENT_TIMESTAMP, 'YYYYMMDDHH24MISSFF3')) AS BATCH_ID,
        CAST(CURRENT_TIMESTAMP AS TIMESTAMP_NTZ)                        AS BATCH_TIMESTAMP
),

-- ============================================
-- Step 1: Identify changed CUSTOMER_IDs from BOTH tables
-- ============================================
{% if is_incremental() %}
CTE_changed_customer_ids AS (
    -- Customers with updated customer records
    SELECT DISTINCT c.CUSTOMER_ID
    FROM {{ source('workday_ods', 'CUSTOMER') }} c
    WHERE c.UPDATED_AT > (SELECT COALESCE(MAX(CUSTOMER_UPDATED_AT), '1900-01-01') FROM {{ this }})

    UNION

    -- Customers with updated address records
    SELECT DISTINCT ca.CUSTOMER_ID
    FROM {{ source('workday_ods', 'CUSTOMER_ADDRESS') }} ca
    WHERE ca.UPDATED_AT > (SELECT COALESCE(MAX(ADDRESS_UPDATED_AT), '1900-01-01') FROM {{ this }})
),
{% endif %}

-- ============================================
-- Step 2: Get customer data (filtered to changed IDs on incremental)
-- ============================================
CTE_customer AS (
    SELECT
        c.CUSTOMER_ID,
        c.FIRST_NAME,
        c.LAST_NAME,
        c.EMAIL,
        c.PHONE,
        c.DATE_OF_BIRTH,
        c.CREATED_AT        AS CUSTOMER_CREATED_AT,
        c.UPDATED_AT        AS CUSTOMER_UPDATED_AT
    FROM {{ source('workday_ods', 'CUSTOMER') }} c
    {% if is_incremental() %}
        WHERE c.CUSTOMER_ID IN (SELECT CUSTOMER_ID FROM CTE_changed_customer_ids)
    {% endif %}
),

-- ============================================
-- Step 3: Get address data (filtered to changed IDs on incremental)
-- ============================================
CTE_customer_address AS (
    SELECT
        ca.ADDRESS_ID,
        ca.CUSTOMER_ID,
        ca.ADDRESS_TYPE,
        ca.ADDRESS_LINE_1,
        ca.ADDRESS_LINE_2,
        ca.CITY,
        ca.STATE,
        ca.ZIP_CODE,
        ca.COUNTRY,
        ca.IS_PRIMARY,
        ca.CREATED_AT       AS ADDRESS_CREATED_AT,
        ca.UPDATED_AT       AS ADDRESS_UPDATED_AT
    FROM {{ source('workday_ods', 'CUSTOMER_ADDRESS') }} ca
    {% if is_incremental() %}
        WHERE ca.CUSTOMER_ID IN (SELECT CUSTOMER_ID FROM CTE_changed_customer_ids)
    {% endif %}
),

-- ============================================
-- Step 4: Join and compute keys + hashes
-- ============================================
CTE_joined AS (
    SELECT
        -- Surrogate key
        {{ dbt_utils.generate_surrogate_key(['c.CUSTOMER_ID', 'ca.ADDRESS_ID']) }}
                                            AS CUSTOMER_ADDRESS_PK,

        -- Customer fields
        c.CUSTOMER_ID,
        c.FIRST_NAME,
        c.LAST_NAME,
        c.EMAIL,
        c.PHONE,
        c.DATE_OF_BIRTH,
        c.CUSTOMER_CREATED_AT,
        c.CUSTOMER_UPDATED_AT,

        -- Address fields
        ca.ADDRESS_ID,
        ca.ADDRESS_TYPE,
        ca.ADDRESS_LINE_1,
        ca.ADDRESS_LINE_2,
        ca.CITY,
        ca.STATE,
        ca.ZIP_CODE,
        ca.COUNTRY,
        ca.IS_PRIMARY,
        ca.ADDRESS_CREATED_AT,
        ca.ADDRESS_UPDATED_AT,

        -- Hash for change detection (all mutable business fields)
        {{ dbt_utils.generate_surrogate_key([
            'c.FIRST_NAME',
            'c.LAST_NAME',
            'c.EMAIL',
            'c.PHONE',
            'c.DATE_OF_BIRTH',
            'ca.ADDRESS_TYPE',
            'ca.ADDRESS_LINE_1',
            'ca.ADDRESS_LINE_2',
            'ca.CITY',
            'ca.STATE',
            'ca.ZIP_CODE',
            'ca.COUNTRY',
            'ca.IS_PRIMARY'
        ]) }}                               AS HASH_CHANGE

    FROM CTE_customer c
    INNER JOIN CTE_customer_address ca
        ON c.CUSTOMER_ID = ca.CUSTOMER_ID
)

-- ============================================
-- Step 5: Final select with audit columns
-- ============================================
SELECT
    j.CUSTOMER_ADDRESS_PK,

    -- Customer fields
    j.CUSTOMER_ID,
    j.FIRST_NAME,
    j.LAST_NAME,
    j.EMAIL,
    j.PHONE,
    j.DATE_OF_BIRTH,
    j.CUSTOMER_CREATED_AT,
    j.CUSTOMER_UPDATED_AT,

    -- Address fields
    j.ADDRESS_ID,
    j.ADDRESS_TYPE,
    j.ADDRESS_LINE_1,
    j.ADDRESS_LINE_2,
    j.CITY,
    j.STATE,
    j.ZIP_CODE,
    j.COUNTRY,
    j.IS_PRIMARY,
    j.ADDRESS_CREATED_AT,
    j.ADDRESS_UPDATED_AT,

    -- Change detection
    j.HASH_CHANGE,

    -- Audit columns
    b.BATCH_ID                              AS CDM_INS_BATCH_ID,
    b.BATCH_ID                              AS CDM_UPD_BATCH_ID,
    b.BATCH_TIMESTAMP                       AS CDM_INSERT_TIMESTAMP,
    b.BATCH_TIMESTAMP                       AS CDM_UPDATE_TIMESTAMP

FROM CTE_joined j
CROSS JOIN CTE_batch b
WHERE j.IS_PRIMARY = TRUE
