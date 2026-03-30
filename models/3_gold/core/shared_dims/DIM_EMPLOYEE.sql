-- CORE DIM_EMPLOYEE Model - SCD Type 2
-- Source: WORKFORCE.EMPLOYEE (CDM Silver Layer)
-- Target: CORE.DIM_EMPLOYEE (Dimension Layer)
-- Pattern: Type 2 Slowly Changing Dimension with effective dating
-- Framework: Uses scd macros to reduce code duplication


{{
        config(
        materialized='scd_incremental',
        unique_key ='EMPLOYEE_EIN',
        incremental_strategy='scd+merge',
        timestamp_column='INS_BATCH_ID',
    post_hook=[
        "update {{ this }} as tgt\nset\n    IS_DELETED = TRUE,\n    UPD_BATCH_ID = TO_NUMBER(TO_VARCHAR(CURRENT_TIMESTAMP, 'YYYYMMDDHH24MISS'))\nwhere tgt.IS_DELETED = FALSE\n  and exists (\n    select 1\n    from {{ ref('EMPLOYEE') }} src\n    where src.EMPLOYEE_EIN = tgt.EMPLOYEE_EIN\n      and src.IS_DELETED = TRUE\n  )"
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
        TO_NUMBER(TO_VARCHAR(CURRENT_TIMESTAMP, 'YYYYMMDDHH24MISS')) as batch_id,
        CAST(CURRENT_TIMESTAMP AS TIMESTAMP_NTZ) as batch_timestamp
),
CTE_transformed as (
    -- Create new records for changed employees or initial load
    select
        -- Natural Key
        cast(src.EMPLOYEE_EIN as varchar(15)) as EMPLOYEE_EIN,
        cast(src.ES_USER_ID as integer) as ES_USER_ID,
        -- Foreign Keys
        cast(src.DIVISION_NUMBER as integer) as DIV_SK,
        cast(src.SUPERVISOR_EIN as varchar(15)) as SUPERVISOR,
        
        -- Name Fields
        cast(src.LAST_NAME as varchar(50)) as LAST_NAME,
        cast(src.FIRST_NAME as varchar(50)) as FIRST_NAME,
        cast(src.MIDDLE_INITIAL as varchar(1)) as MIDDLE_INITIAL,
        cast(src.MIDDLE_NAME as varchar(15)) as MIDDLE_NAME,
        cast(src.COMPLETE_NAME as varchar(110)) as COMPLETE_NAME,
        cast(src.NICKNAME as varchar(40)) as NICK_NAME,
        
        -- Job Information
        cast(src.JOB_CODE as varchar(9)) as JOB_CD,
        cast(src.JOB_DESCRIPTION as varchar(60)) as JOB_DESC,
        cast(src.POSITION_CODE as varchar(12)) as EMP_POSITION_CD,
        cast(src.NETWORK_USER_ID as varchar(40)) as NETWORK_USER_ID,
        cast(src.EMAIL_ADDRESS as varchar(60)) as EMAIL_ADDR,
        
        -- Date Fields
        cast(src.HIRE_DATE as date) as HIRE_DT,
        case when src.TERMINATION_DATE is null then '9999-12-31'::date 
        else cast(src.TERMINATION_DATE as date) end as TERM_DT,
        case when src.ADJUSTED_HIRE_DATE is null then '1900-01-01'::date 
        else cast(src.ADJUSTED_HIRE_DATE as date) end as ADJUSTED_HIRE_DT,        
        -- Employment Details
        cast(src.EMPLOYEE_STATUS as varchar(5)) as EMP_STATUS,
        cast(src.UNION_CODE as varchar(10)) as EMP_UNION_CD,
        
        -- Phone Numbers
        cast(src.PHONE_NUMBER as varchar(15)) as EMP_PHONE_NBR,
        cast(src.FAX_NUMBER as varchar(15)) as EMP_FAX_NBR,
        cast(src.WORK_PHONE_NUMBER as varchar(10)) as EMP_WORK_PHONE_NBR,
        cast(src.CELL_PHONE_NUMBER as varchar(10)) as EMP_CELLPHONE_NBR,
        
        -- Address Fields
        cast(src.ADDRESS_LINE_1 as varchar(30)) as ADDR_LINE_1,
        cast(src.ADDRESS_LINE_2 as varchar(30)) as ADDR_LINE_2,
        cast(src.CITY as varchar(20)) as CITY,
        cast(src.STATE as varchar(2)) as STATE,
        cast(src.POSTAL_CODE as varchar(10)) as POSTAL_CD,
        
        -- Employee Type
        cast(src.EMPLOYEE_TYPE as varchar(1)) as EMP_TYP,
        
        -- System Profiles
        cast(src.HR_PROFILE as varchar(255)) as HR_PROFILE,
        cast(null as varchar(10)) as LAWSON_PROFILE,
        cast(null as varchar(10)) as KRONOS_PROFILE,
        cast(src.INFOPRO_PROFILE as varchar(10)) as INFOPRO_PROFILE,
        
        -- Financial Controllers - Region
        cast(src.REGION_CONTROLLER_EIN as varchar(15)) as REGION_CONTROLLER,
        cast(src.ASSISTANT_CONTROLLER_EIN as varchar(15)) as ASSISTANT_CONTROLLER,
        cast(src.REGION_FINANCE_MANAGER_EIN as varchar(15)) as REGION_FINANCE_MGR,
        
        -- Financial Controllers - Area
        cast(src.AREA_CONTROLLER_EIN as varchar(15)) as AREA_CONTROLLER,
        cast(src.ASSISTANT_AREA_CONTROLLER_EIN as varchar(15)) as ASST_AREA_CONTROLLER,
        cast(src.AREA_SALES_MANAGER_EIN as varchar(15)) as AREA_SALES_MGR,
        
        -- Financial Controllers - Division
        cast(src.DIVISION_CONTROLLER_EIN as varchar(15)) as DIV_CONTROLLER,
        cast(src.DIVISION_GENERAL_MANAGER_EIN as varchar(15)) as DIV_GENERAL_MGR,
        cast(src.DIVISION_SALES_MANAGER_EIN as varchar(15)) as DIV_SALES_MGR,
        
        -- Reporting Relationships
        cast(src.INDIRECT_REPORTS_TO_EIN as varchar(15)) as EMP_INDIRECT_REPORTS_TO,
        
        -- Job Family
        cast(src.JOB_FAMILY_GROUP as varchar(60)) as EMP_JOB_FAMILY_GRP,
        cast(src.JOB_FAMILY as varchar(60)) as EMP_JOB_FAMILY,
        
        -- HR Partner
        cast(src.HR_PARTNER_EIN as varchar(15)) as HRPARTNEREIN,
        
        -- SCD Type 2 Fields
        CURRENT_DATE as EFF_DT,
        TO_DATE('9999-12-31') as DISC_DT,
        true as IS_CURRENT,
        
        -- ETL Batch Fields
        CTE_audits.batch_id as INS_BATCH_ID,
        CTE_audits.batch_id as UPD_BATCH_ID,
        
        -- Audit Fields
        cast(src.IS_DELETED as boolean) as IS_DELETED,
        cast(src.SOURCE_SYSTEM as varchar(20)) as SOURCE_SYSTEM,
        cast(src.PRIMARY_DATA_SOURCE as varchar(50)) as PRIMARY_DATA_SOURCE,
        cast(src.CDM_INSERT_TIMESTAMP as timestamp_ntz(9)) as CDM_INSERT_TIMESTAMP,
        cast(src.CDM_UPDATE_TIMESTAMP as timestamp_ntz(9)) as CDM_UPDATE_TIMESTAMP,
        cast(src.HASH_FULL as varchar(32)) as DBT_HASH,
        
    from CTE_source src
    cross join CTE_audits
),
CTE_final as (
    select 
        {{ generate_surrogate_key(['EMPLOYEE_EIN','EFF_DT']) }} AS EMPLOYEE_SK,
        *
        from CTE_transformed
)
select * from CTE_final