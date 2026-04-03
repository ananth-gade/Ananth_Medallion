{% macro load_dim_employee_hist() %}
    {% do log("Step 1: Migrating historical employee dimension from on-prem DWCORE", info=True) %}

        {% set sql %}
            CREATE OR REPLACE TABLE CORE.SHARED_DIMS.DIM_EMPLOYEE AS

            WITH CTE_source AS (
                SELECT
                    src.*,
                    CAST(es.USER_ID AS INTEGER) AS _ES_USER_ID
                FROM WORKDAY.STAGING.DWCORE_DIM_EMPLOYEE src
                LEFT JOIN EQAI.ODS.ODS_ES_EQAI_USERS es
                    ON es.USER_CODE = src.NETWORK_USER_ID AND es.USER_ID <> 1602
            ),

            CTE_transformed AS (
                SELECT
                    -- Natural Key
                    CAST(EMPLOYEE_EIN AS VARCHAR(15))                    AS EMPLOYEE_EIN,

                    -- Identity
                    CAST(NETWORK_USER_ID AS VARCHAR(40))                 AS NETWORK_USER_ID,
                    _ES_USER_ID                                          AS ES_USER_ID,

                    -- Name Fields (SCD2)
                    CAST(LAST_NAME AS VARCHAR(50))                       AS LAST_NAME,
                    CAST(FIRST_NAME AS VARCHAR(50))                      AS FIRST_NAME,
                    CAST(MIDDLE_NAME AS VARCHAR(15))                     AS MIDDLE_NAME,
                    CAST(MIDDLE_INITIAL AS VARCHAR(1))                   AS MIDDLE_INITIAL,
                    CAST(COMPLETE_NAME AS VARCHAR(110))                  AS COMPLETE_NAME,
                    CAST(NICK_NAME AS VARCHAR(40))                       AS NICKNAME,
                    CAST(EMAIL_ADDR AS VARCHAR(60))                      AS EMAIL_ADDRESS,

                    -- Job Information (SCD2)
                    CAST(JOB_CD AS VARCHAR(9))                           AS JOB_CODE,
                    CAST(JOB_DESC AS VARCHAR(60))                        AS JOB_DESCRIPTION,
                    CAST(EMP_POSITION_CD AS VARCHAR(12))                 AS POSITION_CODE,
                    CAST(EMP_JOB_FAMILY_GRP AS VARCHAR(60))              AS JOB_FAMILY_GROUP,
                    CAST(EMP_JOB_FAMILY AS VARCHAR(60))                  AS JOB_FAMILY,

                    -- Employment Details (SCD2)
                    CAST(EMP_STATUS AS VARCHAR(5))                       AS EMPLOYEE_STATUS,
                    CAST(EMP_TYP AS VARCHAR(1))                          AS EMPLOYEE_TYPE,
                    CAST(DIV_SK AS INT)                                   AS DIVISION_NUMBER,
                    CAST(EMP_UNION_CD AS VARCHAR(10))                    AS UNION_CODE,
                    CASE WHEN EMP_UNION_CD IS NOT NULL
                              AND TRIM(EMP_UNION_CD) <> ''
                         THEN TRUE ELSE FALSE END                        AS IS_UNION_MEMBER,
                    CAST(IS_DELETED AS BOOLEAN)                          AS IS_DELETED,

                    -- Date Fields (SCD2)
                    CAST(HIRE_DT AS DATE)                                AS HIRE_DATE,
                    CASE WHEN ADJUSTED_HIRE_DT IS NULL THEN '1900-01-01'::DATE
                         ELSE CAST(ADJUSTED_HIRE_DT AS DATE) END         AS ADJUSTED_HIRE_DATE,
                    CASE WHEN TERM_DT IS NULL THEN '9999-12-31'::DATE
                         ELSE CAST(TERM_DT AS DATE) END                  AS TERMINATION_DATE,

                    -- Management Chain (SCD2)
                    CAST(SUPERVISOR AS VARCHAR(15))                      AS SUPERVISOR_EIN,
                    CAST(EMP_INDIRECT_REPORTS_TO AS VARCHAR(15))         AS INDIRECT_REPORTS_TO_EIN,
                    CAST(REGION_CONTROLLER AS VARCHAR(15))               AS REGION_CONTROLLER_EIN,
                    CAST(ASSISTANT_CONTROLLER AS VARCHAR(15))            AS ASSISTANT_CONTROLLER_EIN,
                    CAST(REGION_FINANCE_MGR AS VARCHAR(15))              AS REGION_FINANCE_MANAGER_EIN,
                    CAST(AREA_CONTROLLER AS VARCHAR(15))                 AS AREA_CONTROLLER_EIN,
                    CAST(ASST_AREA_CONTROLLER AS VARCHAR(15))            AS ASSISTANT_AREA_CONTROLLER_EIN,
                    CAST(AREA_SALES_MGR AS VARCHAR(15))                  AS AREA_SALES_MANAGER_EIN,
                    CAST(DIV_CONTROLLER AS VARCHAR(15))                  AS DIVISION_CONTROLLER_EIN,
                    CAST(DIV_GENERAL_MGR AS VARCHAR(15))                 AS DIVISION_GENERAL_MANAGER_EIN,
                    CAST(DIV_SALES_MGR AS VARCHAR(15))                   AS DIVISION_SALES_MANAGER_EIN,
                    CAST(HRPARTNEREIN AS VARCHAR(15))                    AS HR_PARTNER_EIN,
                    CAST(HR_PROFILE AS VARCHAR(255))                     AS HR_PROFILE,
                    CAST(INFOPRO_PROFILE AS VARCHAR(10))                 AS INFOPRO_PROFILE,

                    -- Address Fields (SCD1)
                    CAST(ADDR_LINE_1 AS VARCHAR(30))                     AS ADDRESS_LINE_1,
                    CAST(ADDR_LINE_2 AS VARCHAR(30))                     AS ADDRESS_LINE_2,
                    CAST(CITY AS VARCHAR(20))                            AS CITY,
                    CAST(STATE AS VARCHAR(2))                            AS STATE,
                    CAST(POSTAL_CD AS VARCHAR(10))                       AS POSTAL_CODE,
                    CAST(NULL AS VARCHAR(50))                            AS COUNTRY,

                    -- Phone Fields (SCD1)
                    CAST(EMP_PHONE_NBR AS VARCHAR(15))                   AS PHONE_NUMBER,
                    CAST(EMP_WORK_PHONE_NBR AS VARCHAR(10))              AS WORK_PHONE_NUMBER,
                    CAST(EMP_CELLPHONE_NBR AS VARCHAR(10))               AS CELL_PHONE_NUMBER,
                    CAST(EMP_FAX_NBR AS VARCHAR(15))                     AS FAX_NUMBER,

                    -- SCD Type 2 Fields
                    CAST(EFF_DT AS DATE)                                 AS EFF_DT,
                    CAST(DISC_DT AS DATE)                                AS DISC_DT,
                    IS_CURRENT,

                    -- ETL Batch Fields
                    INS_BATCH_ID,
                    UPD_BATCH_ID,

                    -- Audit / Lineage
                    SOURCE_SYSTEM,
                    'STG_DWCORE_DIM_EMPLOYEE'                            AS PRIMARY_DATA_SOURCE,
                    CURRENT_TIMESTAMP::TIMESTAMP_NTZ                     AS CDM_INSERT_TIMESTAMP,
                    CURRENT_TIMESTAMP::TIMESTAMP_NTZ                     AS CDM_UPDATE_TIMESTAMP,

                    -- Operation Type
                    'HISTORICAL'                                         AS OPERATION_TYPE

                FROM CTE_source
            ),

            CTE_keyed AS (
                SELECT
                    -- Surrogate key = MD5(EMPLOYEE_EIN || '-' || EFF_DT) — matches generate_surrogate_key macro
                    MD5(COALESCE(CAST(EMPLOYEE_EIN AS VARCHAR), '') || '-' || COALESCE(CAST(EFF_DT AS VARCHAR), '')) AS EMPLOYEE_SK,
                    *
                FROM CTE_transformed
            ),

            CTE_hashed AS (
                SELECT
                    *,

                    -- HASH_SCD2: matches generate_hash_scd2() — scd2_column: true columns in YAML order
                    CAST(MD5(
                        COALESCE(CAST(LAST_NAME AS VARCHAR), '')                    || '||' ||
                        COALESCE(CAST(FIRST_NAME AS VARCHAR), '')                   || '||' ||
                        COALESCE(CAST(MIDDLE_NAME AS VARCHAR), '')                  || '||' ||
                        COALESCE(CAST(MIDDLE_INITIAL AS VARCHAR), '')               || '||' ||
                        COALESCE(CAST(COMPLETE_NAME AS VARCHAR), '')                || '||' ||
                        COALESCE(CAST(NICKNAME AS VARCHAR), '')                     || '||' ||
                        COALESCE(CAST(EMAIL_ADDRESS AS VARCHAR), '')                || '||' ||
                        COALESCE(CAST(JOB_CODE AS VARCHAR), '')                     || '||' ||
                        COALESCE(CAST(JOB_DESCRIPTION AS VARCHAR), '')              || '||' ||
                        COALESCE(CAST(POSITION_CODE AS VARCHAR), '')                || '||' ||
                        COALESCE(CAST(JOB_FAMILY_GROUP AS VARCHAR), '')             || '||' ||
                        COALESCE(CAST(JOB_FAMILY AS VARCHAR), '')                   || '||' ||
                        COALESCE(CAST(EMPLOYEE_STATUS AS VARCHAR), '')              || '||' ||
                        COALESCE(CAST(EMPLOYEE_TYPE AS VARCHAR), '')                || '||' ||
                        COALESCE(CAST(DIVISION_NUMBER AS VARCHAR), '')              || '||' ||
                        COALESCE(CAST(UNION_CODE AS VARCHAR), '')                   || '||' ||
                        COALESCE(CAST(IS_UNION_MEMBER AS VARCHAR), '')              || '||' ||
                        COALESCE(CAST(IS_DELETED AS VARCHAR), '')                   || '||' ||
                        COALESCE(CAST(HIRE_DATE AS VARCHAR), '')                    || '||' ||
                        COALESCE(CAST(ADJUSTED_HIRE_DATE AS VARCHAR), '')           || '||' ||
                        COALESCE(CAST(TERMINATION_DATE AS VARCHAR), '')             || '||' ||
                        COALESCE(CAST(SUPERVISOR_EIN AS VARCHAR), '')               || '||' ||
                        COALESCE(CAST(INDIRECT_REPORTS_TO_EIN AS VARCHAR), '')      || '||' ||
                        COALESCE(CAST(REGION_CONTROLLER_EIN AS VARCHAR), '')        || '||' ||
                        COALESCE(CAST(ASSISTANT_CONTROLLER_EIN AS VARCHAR), '')     || '||' ||
                        COALESCE(CAST(REGION_FINANCE_MANAGER_EIN AS VARCHAR), '')   || '||' ||
                        COALESCE(CAST(AREA_CONTROLLER_EIN AS VARCHAR), '')          || '||' ||
                        COALESCE(CAST(ASSISTANT_AREA_CONTROLLER_EIN AS VARCHAR), '') || '||' ||
                        COALESCE(CAST(AREA_SALES_MANAGER_EIN AS VARCHAR), '')       || '||' ||
                        COALESCE(CAST(DIVISION_CONTROLLER_EIN AS VARCHAR), '')      || '||' ||
                        COALESCE(CAST(DIVISION_GENERAL_MANAGER_EIN AS VARCHAR), '') || '||' ||
                        COALESCE(CAST(DIVISION_SALES_MANAGER_EIN AS VARCHAR), '')   || '||' ||
                        COALESCE(CAST(HR_PARTNER_EIN AS VARCHAR), '')               || '||' ||
                        COALESCE(CAST(HR_PROFILE AS VARCHAR), '')                   || '||' ||
                        COALESCE(CAST(INFOPRO_PROFILE AS VARCHAR), '')
                    ) AS VARCHAR(32)) AS HASH_SCD2,

                    -- HASH_SCD1: matches generate_hash_scd1() — scd1_column: true columns in YAML order
                    CAST(MD5(
                        COALESCE(CAST(ADDRESS_LINE_1 AS VARCHAR), '')       || '||' ||
                        COALESCE(CAST(ADDRESS_LINE_2 AS VARCHAR), '')       || '||' ||
                        COALESCE(CAST(CITY AS VARCHAR), '')                 || '||' ||
                        COALESCE(CAST(STATE AS VARCHAR), '')                || '||' ||
                        COALESCE(CAST(POSTAL_CODE AS VARCHAR), '')          || '||' ||
                        COALESCE(CAST(COUNTRY AS VARCHAR), '')              || '||' ||
                        COALESCE(CAST(PHONE_NUMBER AS VARCHAR), '')         || '||' ||
                        COALESCE(CAST(WORK_PHONE_NUMBER AS VARCHAR), '')    || '||' ||
                        COALESCE(CAST(CELL_PHONE_NUMBER AS VARCHAR), '')    || '||' ||
                        COALESCE(CAST(FAX_NUMBER AS VARCHAR), '')
                    ) AS VARCHAR(32)) AS HASH_SCD1,

                    -- HASH_FULL: matches generate_hash_full() — all non-audit columns in YAML order
                    CAST(MD5(
                        COALESCE(CAST(EMPLOYEE_SK AS VARCHAR), '')                  || '|' ||
                        COALESCE(CAST(EMPLOYEE_EIN AS VARCHAR), '')                 || '|' ||
                        COALESCE(CAST(NETWORK_USER_ID AS VARCHAR), '')              || '|' ||
                        COALESCE(CAST(ES_USER_ID AS VARCHAR), '')                   || '|' ||
                        COALESCE(CAST(LAST_NAME AS VARCHAR), '')                    || '|' ||
                        COALESCE(CAST(FIRST_NAME AS VARCHAR), '')                   || '|' ||
                        COALESCE(CAST(MIDDLE_NAME AS VARCHAR), '')                  || '|' ||
                        COALESCE(CAST(MIDDLE_INITIAL AS VARCHAR), '')               || '|' ||
                        COALESCE(CAST(COMPLETE_NAME AS VARCHAR), '')                || '|' ||
                        COALESCE(CAST(NICKNAME AS VARCHAR), '')                     || '|' ||
                        COALESCE(CAST(EMAIL_ADDRESS AS VARCHAR), '')                || '|' ||
                        COALESCE(CAST(JOB_CODE AS VARCHAR), '')                     || '|' ||
                        COALESCE(CAST(JOB_DESCRIPTION AS VARCHAR), '')              || '|' ||
                        COALESCE(CAST(POSITION_CODE AS VARCHAR), '')                || '|' ||
                        COALESCE(CAST(JOB_FAMILY_GROUP AS VARCHAR), '')             || '|' ||
                        COALESCE(CAST(JOB_FAMILY AS VARCHAR), '')                   || '|' ||
                        COALESCE(CAST(EMPLOYEE_STATUS AS VARCHAR), '')              || '|' ||
                        COALESCE(CAST(EMPLOYEE_TYPE AS VARCHAR), '')                || '|' ||
                        COALESCE(CAST(DIVISION_NUMBER AS VARCHAR), '')              || '|' ||
                        COALESCE(CAST(UNION_CODE AS VARCHAR), '')                   || '|' ||
                        COALESCE(CAST(IS_UNION_MEMBER AS VARCHAR), '')              || '|' ||
                        COALESCE(CAST(IS_DELETED AS VARCHAR), '')                   || '|' ||
                        COALESCE(CAST(HIRE_DATE AS VARCHAR), '')                    || '|' ||
                        COALESCE(CAST(ADJUSTED_HIRE_DATE AS VARCHAR), '')           || '|' ||
                        COALESCE(CAST(TERMINATION_DATE AS VARCHAR), '')             || '|' ||
                        COALESCE(CAST(SUPERVISOR_EIN AS VARCHAR), '')               || '|' ||
                        COALESCE(CAST(INDIRECT_REPORTS_TO_EIN AS VARCHAR), '')      || '|' ||
                        COALESCE(CAST(REGION_CONTROLLER_EIN AS VARCHAR), '')        || '|' ||
                        COALESCE(CAST(ASSISTANT_CONTROLLER_EIN AS VARCHAR), '')     || '|' ||
                        COALESCE(CAST(REGION_FINANCE_MANAGER_EIN AS VARCHAR), '')   || '|' ||
                        COALESCE(CAST(AREA_CONTROLLER_EIN AS VARCHAR), '')          || '|' ||
                        COALESCE(CAST(ASSISTANT_AREA_CONTROLLER_EIN AS VARCHAR), '') || '|' ||
                        COALESCE(CAST(AREA_SALES_MANAGER_EIN AS VARCHAR), '')       || '|' ||
                        COALESCE(CAST(DIVISION_CONTROLLER_EIN AS VARCHAR), '')      || '|' ||
                        COALESCE(CAST(DIVISION_GENERAL_MANAGER_EIN AS VARCHAR), '') || '|' ||
                        COALESCE(CAST(DIVISION_SALES_MANAGER_EIN AS VARCHAR), '')   || '|' ||
                        COALESCE(CAST(HR_PARTNER_EIN AS VARCHAR), '')               || '|' ||
                        COALESCE(CAST(HR_PROFILE AS VARCHAR), '')                   || '|' ||
                        COALESCE(CAST(INFOPRO_PROFILE AS VARCHAR), '')              || '|' ||
                        COALESCE(CAST(ADDRESS_LINE_1 AS VARCHAR), '')               || '|' ||
                        COALESCE(CAST(ADDRESS_LINE_2 AS VARCHAR), '')               || '|' ||
                        COALESCE(CAST(CITY AS VARCHAR), '')                         || '|' ||
                        COALESCE(CAST(STATE AS VARCHAR), '')                        || '|' ||
                        COALESCE(CAST(POSTAL_CODE AS VARCHAR), '')                  || '|' ||
                        COALESCE(CAST(COUNTRY AS VARCHAR), '')                      || '|' ||
                        COALESCE(CAST(PHONE_NUMBER AS VARCHAR), '')                 || '|' ||
                        COALESCE(CAST(WORK_PHONE_NUMBER AS VARCHAR), '')            || '|' ||
                        COALESCE(CAST(CELL_PHONE_NUMBER AS VARCHAR), '')            || '|' ||
                        COALESCE(CAST(FAX_NUMBER AS VARCHAR), '')
                    ) AS VARCHAR(250)) AS HASH_FULL

                FROM CTE_keyed
            )

            SELECT
                EMPLOYEE_SK,
                EMPLOYEE_EIN,
                NETWORK_USER_ID,
                ES_USER_ID,
                LAST_NAME,
                FIRST_NAME,
                MIDDLE_NAME,
                MIDDLE_INITIAL,
                COMPLETE_NAME,
                NICKNAME,
                EMAIL_ADDRESS,
                JOB_CODE,
                JOB_DESCRIPTION,
                POSITION_CODE,
                JOB_FAMILY_GROUP,
                JOB_FAMILY,
                EMPLOYEE_STATUS,
                EMPLOYEE_TYPE,
                DIVISION_NUMBER,
                UNION_CODE,
                IS_UNION_MEMBER,
                IS_DELETED,
                HIRE_DATE,
                ADJUSTED_HIRE_DATE,
                TERMINATION_DATE,
                SUPERVISOR_EIN,
                INDIRECT_REPORTS_TO_EIN,
                REGION_CONTROLLER_EIN,
                ASSISTANT_CONTROLLER_EIN,
                REGION_FINANCE_MANAGER_EIN,
                AREA_CONTROLLER_EIN,
                ASSISTANT_AREA_CONTROLLER_EIN,
                AREA_SALES_MANAGER_EIN,
                DIVISION_CONTROLLER_EIN,
                DIVISION_GENERAL_MANAGER_EIN,
                DIVISION_SALES_MANAGER_EIN,
                HR_PARTNER_EIN,
                HR_PROFILE,
                INFOPRO_PROFILE,
                ADDRESS_LINE_1,
                ADDRESS_LINE_2,
                CITY,
                STATE,
                POSTAL_CODE,
                COUNTRY,
                PHONE_NUMBER,
                WORK_PHONE_NUMBER,
                CELL_PHONE_NUMBER,
                FAX_NUMBER,
                EFF_DT,
                DISC_DT,
                IS_CURRENT,
                INS_BATCH_ID,
                UPD_BATCH_ID,
                SOURCE_SYSTEM,
                PRIMARY_DATA_SOURCE,
                CDM_INSERT_TIMESTAMP,
                CDM_UPDATE_TIMESTAMP,
                OPERATION_TYPE,
                HASH_SCD2,
                HASH_SCD1,
                HASH_FULL
            FROM CTE_hashed

        {% endset %}
        
        {% do run_query(sql) %}

        {% do log("Step 1 Complete: Historical employee dimension migrated from on-prem DWCORE to CORE.SHARED_DIMS.DIM_EMPLOYEE", info=True) %}

{% endmacro %} 
