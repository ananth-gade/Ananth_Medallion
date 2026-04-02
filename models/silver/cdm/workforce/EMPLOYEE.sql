-- CDM EMPLOYEE Model
-- Source: ODS_WORKDAY_EMPLOYEE (Bronze Layer)
-- Target: WORKFORCE.EMPLOYEE (Silver CDM Layer)
-- Pattern: Incremental with hash-based change detection

{{
    config(
        materialized='incremental',
        unique_key='CDM_EMPLOYEE_PK',
        merge_no_update_columns = ['CDM_INS_BATCH_ID', 'CDM_INSERT_TIMESTAMP'],
        merge_condition="DBT_INTERNAL_SOURCE.HASH_CHANGE != DBT_INTERNAL_DEST.HASH_CHANGE",
        tags=["silver","cdm","workday","workforce","data-observe"]
    )
}}

with
CTE_source_data as (
    select *
    from {{ ref('ODS_WORKDAY_EMPLOYEE') }}
    {% if is_incremental() %}
        where ODS_UPDATE_TIMESTAMP >= '{{ get_max_event_time('UPDATE_TIMESTAMP_ODS_WORKDAY_EMPLOYEE') }}' 
    {% endif %}
),
{% if is_incremental() %}
CTE_reprocess_null_es_users as (
    -- Reprocess records where ES_USER_ID is null to attempt re-enrichment
    select distinct
        emp.EMPLOYEE_EIN,
        emp.NETWORK_USER_ID
    from {{ this }} emp
    where emp.ES_USER_ID is null
      and emp.NETWORK_USER_ID is not null
        -- Only reprocess if there's a chance of new data in ES users table
        and exists (
            select 1 
            from {{ ref('ODS_ES_EQAI_USERS') }} es
            where es.user_code = emp.NETWORK_USER_ID
        )
),
{% endif %}
CTE_combined_source as (
    -- Regular incremental records
    select 
        EMPLOYEE_ID,
        'NEW' as SOURCE_TYPE
    from CTE_source_data
    
    {% if is_incremental() %}
    union all
    
    -- Reprocess records to pick up missing ES_USER_ID
    select
        ods.EMPLOYEE_ID,
        'REPROCESS' as SOURCE_TYPE
    from {{ ref('ODS_WORKDAY_EMPLOYEE') }} ods
    inner join CTE_reprocess_null_es_users rp
        on ods.EMPLOYEE_ID = rp.EMPLOYEE_EIN
    {% endif %}
),
CTE_source_enriched as (
    select 
        ods.*,
        cs.SOURCE_TYPE
    from {{ ref('ODS_WORKDAY_EMPLOYEE') }} ods
    inner join CTE_combined_source cs
        on ods.EMPLOYEE_ID = cs.EMPLOYEE_ID
),
CTE_es_users as (
    select
        USER_ID,
        user_code,
        ODS_ES_EQAI_USERS_PK,
        ODS_INSERT_TIMESTAMP,
        ODS_UPDATE_TIMESTAMP
    from {{ ref('ODS_ES_EQAI_USERS') }}
),
CTE_audits as (
    select
        TO_NUMBER(TO_VARCHAR(CURRENT_TIMESTAMP, 'YYYYMMDDHH24MISSFF3')) as batch_id,
        CAST(CURRENT_TIMESTAMP AS TIMESTAMP_NTZ) as batch_timestamp
),
CTE_transformed as (
    select
        -- Primary and Composite Keys
        {{ generate_surrogate_key(['EMPLOYEE_ID']) }} as CDM_EMPLOYEE_PK,
        cast(src.EMPLOYEE_ID as varchar(15)) as EMPLOYEE_EIN,
        cast(src.USER_ID as varchar(40)) as NETWORK_USER_ID,
        
        -- ES User ID - Join to ODS_ES_EQAI_USERS on USER_CODE = NETWORK_USER_ID
        cast(es.USER_ID as integer) as ES_USER_ID,


        -- Contact Information
        cast(src.EMAIL_ID as varchar(60)) as EMAIL_ADDRESS,

        -- Employee Name Fields
        cast(src.LEGAL_NAME_FIRST_NAME as varchar(50)) as FIRST_NAME,
        cast(src.LEGAL_NAME_MIDDLE_NAME as varchar(15)) as MIDDLE_NAME,
        cast(left(src.LEGAL_NAME_MIDDLE_INITIAL, 1) as varchar(1)) as MIDDLE_INITIAL,
        cast(src.LEGAL_NAME_LAST_NAME as varchar(50)) as LAST_NAME,
        cast(
            trim(
                concat_ws(
                    ' ', src.LEGAL_NAME_FIRST_NAME,
                    src.LEGAL_NAME_LAST_NAME
                )
            ) as varchar(110)
        ) as COMPLETE_NAME,
        cast(src.NICK_NAME as varchar(40)) as NICKNAME,

        -- Location / Address Fields
        cast(src.LOCATION_ADDRESS_LINE_1 as varchar(30)) as ADDRESS_LINE_1,
        cast(src.LOCATION_ADDRESS_LINE_2 as varchar(30)) as ADDRESS_LINE_2,
        cast(src.LOCATION_ADDRESS_CITY as varchar(20)) as CITY,
        cast(src.LOCATION_ADDRESS_STATE_ISO_CODE as varchar(2)) as STATE,
        cast(src.LOCATION_ADDRESS_POSTAL_CODE as varchar(10)) as POSTAL_CODE,
        cast(src.LOCATION_ADDRESS_COUNTRY as varchar(50)) as COUNTRY,

        -- Phone Numbers
        cast(src.LOC_PHONE as varchar(15)) as PHONE_NUMBER,
        cast(src.WORK_PHONE as varchar(10)) as WORK_PHONE_NUMBER,
        cast(src.CELL_PHONE as varchar(10)) as CELL_PHONE_NUMBER,
        cast(src.LOC_FAX as varchar(15)) as FAX_NUMBER,

        -- Employment Status & Type
        cast(src.EMPLOYEE_STATUS as varchar(5)) as EMPLOYEE_STATUS,
        cast(src.TYPE_E as varchar(1)) as EMPLOYEE_TYPE,

        -- Date Fields
        cast(src.DATE_OF_HIRE as date) as HIRE_DATE,
        cast(src.ADJUSTED_DATE_OF_HIRE as date) as ADJUSTED_HIRE_DATE,
        cast(src.TERMINATION_DATE as date) as TERMINATION_DATE,

        -- Soft Delete Indicator (convert Y/N to 1/0)
        case when src.SRC_DEL_IND = 'Y' then 1 else 0 end as IS_DELETED,

        -- Job Information
        cast(src.JOB_CODE as varchar(9)) as JOB_CODE,
        cast(src.JOB_DESCRIPTION as varchar(60)) as JOB_DESCRIPTION,
        cast(src.POSITION_CODE as varchar(12)) as POSITION_CODE,
        cast(src.JOB_FAMILY_GROUP as varchar(60)) as JOB_FAMILY_GROUP,
        cast(src.JOB_FAMILY as varchar(60)) as JOB_FAMILY,

        -- Division
        cast(try_to_number(src.INFOPRO_DIVISION) as int) as DIVISION_NUMBER,

        -- Reporting Hierarchy
        cast(src.SUPERVISOR_EMPLOYEE_ID as varchar(15)) as SUPERVISOR_EIN,
        cast(src.INDIRECT_REPORTS_TO as varchar(15)) as INDIRECT_REPORTS_TO_EIN,

        -- Financial Controllers - Region
        cast(src.REGION_CONTROLLER__EIN as varchar(15)) as REGION_CONTROLLER_EIN,
        cast(src.REGION_FINANCE_MANAGER__EIN as varchar(15)) as REGION_FINANCE_MANAGER_EIN,

        -- Financial Controllers - Area
        cast(src.AREA_CONTROLLER__EIN as varchar(15)) as AREA_CONTROLLER_EIN,
        cast(src.ASST_AREA_CONTROLLER__EIN as varchar(15)) as ASSISTANT_AREA_CONTROLLER_EIN,
        cast(src.AREA_SALES_MANAGER__EIN as varchar(15)) as AREA_SALES_MANAGER_EIN,

        -- Financial Controllers - Division
        cast(src.DIVISION_CONTROLLER__EIN as varchar(15)) as DIVISION_CONTROLLER_EIN,
        cast(src.DIVISION_GENERAL_MANAGER__EIN as varchar(15)) as DIVISION_GENERAL_MANAGER_EIN,
        cast(src.DIVISION_SALES_MANAGER__EIN as varchar(15)) as DIVISION_SALES_MANAGER_EIN,

        -- Assistant Controller (Regional level)
        cast(src.REGIONAL_ASST_CONTROLLER__EIN as varchar(15)) as ASSISTANT_CONTROLLER_EIN,

        -- HR Partner
        cast(src.HR_PARTNER_EIN as varchar(15)) as HR_PARTNER_EIN,

        -- Union Information
        cast(src.UNION_CODE as varchar(10)) as UNION_CODE,
        case
            when src.UNION_CODE is not null and trim(src.UNION_CODE) != ''
            then 1
            else 0
        end as IS_UNION_MEMBER,

        -- System Access Profiles
        cast(src.LAWSON_HR_PROFILE as varchar(255)) as HR_PROFILE,
        cast(src.INFOPRO_PROFILE as varchar(10)) as INFOPRO_PROFILE,

        -- Data Lineage & Source Tracking
        'WORKDAY' as SOURCE_SYSTEM,
        'ODS_WORKDAY_EMPLOYEE' || case when es.USER_ID is not null then ', ODS_ES_EQAI_USERS' else '' end as SOURCE_TABLES,
        'ODS_WORKDAY_EMPLOYEE' as PRIMARY_DATA_SOURCE,
        src.ODS_WORKDAY_EMPLOYEE_PK,
        es.ODS_ES_EQAI_USERS_PK,
        src.ODS_INSERT_TIMESTAMP as INSERT_TIMESTAMP_ODS_WORKDAY_EMPLOYEE,
        src.ODS_UPDATE_TIMESTAMP as UPDATE_TIMESTAMP_ODS_WORKDAY_EMPLOYEE,
        es.ODS_INSERT_TIMESTAMP as INSERT_TIMESTAMP_ODS_ES_EQAI_USERS,
        es.ODS_UPDATE_TIMESTAMP as UPDATE_TIMESTAMP_ODS_ES_EQAI_USERS,

        -- ETL Batch & Timestamps
        CTE_audits.batch_id as CDM_INS_BATCH_ID,
        CTE_audits.batch_id as CDM_UPD_BATCH_ID,
        CTE_audits.batch_timestamp as CDM_INSERT_TIMESTAMP,
        CTE_audits.batch_timestamp as CDM_UPDATE_TIMESTAMP,

        -- Operation Type for CDC tracking
        OPERATION_TYPE as OPERATION_TYPE,

        -- Data Quality Fields
        cast(null as varchar(20)) as DQ_STATUS,
        cast(null as varchar(50)) as DQ_ERROR_CODE,
        cast(null as varchar(500)) as DQ_ERROR_MESSAGE


    from CTE_source_enriched src
    left join CTE_es_users es
        on es.user_code = src.USER_ID
    cross join CTE_audits
)

select
    *,
    -- Hashed Columns for Change Detection
    {{ generate_hash_change( relation = this) }} as HASH_CHANGE,
    {{ generate_hash_full( relation = this) }} as HASH_FULL
from CTE_transformed