-- CORE DIM_EMPLOYEE Model - SCD Type 2
-- Source: WORKFORCE.EMPLOYEE (CDM Silver Layer)
-- Target: CORE.DIM_EMPLOYEE (Dimension Layer)
-- Pattern: Type 2 Slowly Changing Dimension with effective dating
-- Framework: Uses scd macros + YAML-driven hash generation (scd1/scd2/full)


{{
    config(
        materialized='scd_incremental',
        unique_key='EMPLOYEE_EIN',
        incremental_strategy='scd+merge',
        timestamp_column='INS_BATCH_ID',
        post_hook=[
            "update {{ this }} as tgt\nset\n    IS_DELETED = TRUE,\n    UPD_BATCH_ID = TO_NUMBER(TO_VARCHAR(CURRENT_TIMESTAMP, 'YYYYMMDDHH24MISSFF3'))\nwhere tgt.IS_DELETED = FALSE\n  and exists (\n    select 1\n    from {{ ref('EMPLOYEE') }} src\n    where src.EMPLOYEE_EIN = tgt.EMPLOYEE_EIN\n      and src.IS_DELETED = TRUE\n  )"
        ],
        tags=["gold","workday","workforce","core-dims","data-observe"]
    )
}}

with CTE_source as (
    select *
    from {{ ref('EMPLOYEE') }}
    {% if is_incremental() %}
        where CDM_UPDATE_TIMESTAMP >= '{{ get_max_event_time('CDM_UPDATE_TIMESTAMP') }}'
    {% endif %}
),
CTE_audits as (
    select
        TO_NUMBER(TO_VARCHAR(CURRENT_TIMESTAMP, 'YYYYMMDDHH24MISSFF3')) as batch_id,
        CAST(CURRENT_TIMESTAMP AS TIMESTAMP_NTZ) as batch_timestamp
),
CTE_transformed as (
    select
        -- Natural Key (passthrough, no rename)
        cast(src.EMPLOYEE_EIN as varchar(15)) as EMPLOYEE_EIN,

        -- Identity
        cast(src.NETWORK_USER_ID as varchar(40)) as NETWORK_USER_ID,
        cast(src.ES_USER_ID as integer) as ES_USER_ID,

        -- Name Fields (SCD2)
        cast(src.LAST_NAME as varchar(50)) as LAST_NAME,
        cast(src.FIRST_NAME as varchar(50)) as FIRST_NAME,
        cast(src.MIDDLE_NAME as varchar(15)) as MIDDLE_NAME,
        cast(src.MIDDLE_INITIAL as varchar(1)) as MIDDLE_INITIAL,
        cast(src.COMPLETE_NAME as varchar(110)) as COMPLETE_NAME,
        cast(src.NICKNAME as varchar(40)) as NICKNAME,
        cast(src.EMAIL_ADDRESS as varchar(60)) as EMAIL_ADDRESS,

        -- Job Information (SCD2)
        cast(src.JOB_CODE as varchar(9)) as JOB_CODE,
        cast(src.JOB_DESCRIPTION as varchar(60)) as JOB_DESCRIPTION,
        cast(src.POSITION_CODE as varchar(12)) as POSITION_CODE,
        cast(src.JOB_FAMILY_GROUP as varchar(60)) as JOB_FAMILY_GROUP,
        cast(src.JOB_FAMILY as varchar(60)) as JOB_FAMILY,

        -- Employment Details (SCD2)
        cast(src.EMPLOYEE_STATUS as varchar(5)) as EMPLOYEE_STATUS,
        cast(src.EMPLOYEE_TYPE as varchar(1)) as EMPLOYEE_TYPE,
        cast(src.DIVISION_NUMBER as int) as DIVISION_NUMBER,
        cast(src.UNION_CODE as varchar(10)) as UNION_CODE,
        cast(src.IS_UNION_MEMBER as boolean) as IS_UNION_MEMBER,
        cast(src.IS_DELETED as boolean) as IS_DELETED,

        -- Date Fields (SCD2)
        cast(src.HIRE_DATE as date) as HIRE_DATE,
        case when src.ADJUSTED_HIRE_DATE is null then '1900-01-01'::date
             else cast(src.ADJUSTED_HIRE_DATE as date) end as ADJUSTED_HIRE_DATE,
        case when src.TERMINATION_DATE is null then '9999-12-31'::date
             else cast(src.TERMINATION_DATE as date) end as TERMINATION_DATE,

        -- Management Chain (SCD2)
        cast(src.SUPERVISOR_EIN as varchar(15)) as SUPERVISOR_EIN,
        cast(src.INDIRECT_REPORTS_TO_EIN as varchar(15)) as INDIRECT_REPORTS_TO_EIN,
        cast(src.REGION_CONTROLLER_EIN as varchar(15)) as REGION_CONTROLLER_EIN,
        cast(src.ASSISTANT_CONTROLLER_EIN as varchar(15)) as ASSISTANT_CONTROLLER_EIN,
        cast(src.REGION_FINANCE_MANAGER_EIN as varchar(15)) as REGION_FINANCE_MANAGER_EIN,
        cast(src.AREA_CONTROLLER_EIN as varchar(15)) as AREA_CONTROLLER_EIN,
        cast(src.ASSISTANT_AREA_CONTROLLER_EIN as varchar(15)) as ASSISTANT_AREA_CONTROLLER_EIN,
        cast(src.AREA_SALES_MANAGER_EIN as varchar(15)) as AREA_SALES_MANAGER_EIN,
        cast(src.DIVISION_CONTROLLER_EIN as varchar(15)) as DIVISION_CONTROLLER_EIN,
        cast(src.DIVISION_GENERAL_MANAGER_EIN as varchar(15)) as DIVISION_GENERAL_MANAGER_EIN,
        cast(src.DIVISION_SALES_MANAGER_EIN as varchar(15)) as DIVISION_SALES_MANAGER_EIN,
        cast(src.HR_PARTNER_EIN as varchar(15)) as HR_PARTNER_EIN,
        cast(src.HR_PROFILE as varchar(255)) as HR_PROFILE,
        cast(src.INFOPRO_PROFILE as varchar(10)) as INFOPRO_PROFILE,

        -- Address Fields (SCD1 - overwrite in place)
        cast(src.ADDRESS_LINE_1 as varchar(30)) as ADDRESS_LINE_1,
        cast(src.ADDRESS_LINE_2 as varchar(30)) as ADDRESS_LINE_2,
        cast(src.CITY as varchar(20)) as CITY,
        cast(src.STATE as varchar(2)) as STATE,
        cast(src.POSTAL_CODE as varchar(10)) as POSTAL_CODE,
        cast(src.COUNTRY as varchar(50)) as COUNTRY,

        -- Phone Fields (SCD1 - overwrite in place)
        cast(src.PHONE_NUMBER as varchar(15)) as PHONE_NUMBER,
        cast(src.WORK_PHONE_NUMBER as varchar(10)) as WORK_PHONE_NUMBER,
        cast(src.CELL_PHONE_NUMBER as varchar(10)) as CELL_PHONE_NUMBER,
        cast(src.FAX_NUMBER as varchar(15)) as FAX_NUMBER,

        -- SCD Type 2 Fields
        CURRENT_DATE as EFF_DT,
        TO_DATE('9999-12-31') as DISC_DT,
        true as IS_CURRENT,

        -- ETL Batch Fields
        CTE_audits.batch_id as INS_BATCH_ID,
        CTE_audits.batch_id as UPD_BATCH_ID,

        -- Audit / Lineage
        cast(src.SOURCE_SYSTEM as varchar(20)) as SOURCE_SYSTEM,
        cast(src.PRIMARY_DATA_SOURCE as varchar(50)) as PRIMARY_DATA_SOURCE,
        cast(src.CDM_INSERT_TIMESTAMP as timestamp_ntz(9)) as CDM_INSERT_TIMESTAMP,
        cast(src.CDM_UPDATE_TIMESTAMP as timestamp_ntz(9)) as CDM_UPDATE_TIMESTAMP,

        -- Operation Type
        src.OPERATION_TYPE as OPERATION_TYPE

    from CTE_source src
    cross join CTE_audits
),
CTE_keyed as (
    select
        -- Surrogate key = MD5(EMPLOYEE_EIN || EFF_DT)
        {{ generate_surrogate_key(['EMPLOYEE_EIN', 'EFF_DT']) }} as EMPLOYEE_SK,
        *
    from CTE_transformed
),
CTE_hashed as (
    select
        *,
        -- Hash: SCD2 columns (change = new historical row)
        {{ generate_hash_scd2() }} as HASH_SCD2,
        -- Hash: SCD1 columns (change = in-place overwrite, no new row)
        {{ generate_hash_scd1() }} as HASH_SCD1,
        -- Hash: full record integrity
        {{ generate_hash_full(relation = this) }} as HASH_FULL
    from CTE_keyed
),
CTE_final as (
    select
        *
    from CTE_hashed
)
select * from CTE_final
