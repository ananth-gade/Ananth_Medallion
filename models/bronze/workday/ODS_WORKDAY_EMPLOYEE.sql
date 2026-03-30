-- ODS_WORKDAY_EMPLOYEE
-- Loads Workday employee data from STG_WORKDAY_EMPLOYEE into the ODS layer.
-- Pre-hook macro purge_old_batch_duplicates removes duplicate records from the staging table for EMPLOYEE_ID-based loads.
-- Post-hook macro mark_deleted_records flags deleted employees in the ODS by comparing ODS and staging tables.
-- Incremental materialization with deduplication by EMPLOYEE_ID on latest STG_INSERT_TIMESTAMP,
-- adding audit metadata and hash columns (HASH_CHANGE, HASH_FULL) for change tracking.

{{
    config(
        materialized='incremental',
        unique_key='ODS_WORKDAY_EMPLOYEE_PK',
        merge_no_update_columns = ['ODS_INS_BATCH_ID', 'ODS_INSERT_TIMESTAMP'],
        merge_condition="DBT_INTERNAL_SOURCE.HASH_CHANGE != DBT_INTERNAL_DEST.HASH_CHANGE",
        tags=["ods","workday","data-observe"],
        pre_hook = ["{{ purge_staging_duplicates_by_hash('WORKDAY', 'STAGING', 'STG_WORKDAY_EMPLOYEE', ['STG_INSERT_TIMESTAMP', 'EXEC_BATCH_ID', 'SOURCE_SYSTEM', 'STG_WORKDAY_EMPLOYEE_PK']) }}"],
        post_hook = ["{{ mark_deleted_records('WORKDAY','ODS','ODS_WORKDAY_EMPLOYEE','WORKDAY','STAGING','STG_WORKDAY_EMPLOYEE','EMPLOYEE_ID') }}"]   
        )
}}

with 
CTE_source_data as (
    select * 
    from {{ source('STAGING', 'STG_WORKDAY_EMPLOYEE') }}
    {% if is_incremental() %}
        -- Incremental logic: only pull records that are new or updated since last run
        where STG_INSERT_TIMESTAMP >= '{{ get_max_event_time('STG_INSERT_TIMESTAMP') }}'        
    {% endif %}
),
CTE_source_data_deduped as (
    select
        *,
        row_number() over (partition by EMPLOYEE_ID order by SYS_LAST_UPDATE_DTM desc) as rn
    from CTE_source_data
    qualify rn = 1 -- Keep only the latest record per EMPLOYEE_ID based on SYS_LAST_UPDATE_DTM
    ),
CTE_audits as (
    select
        TO_NUMBER(TO_VARCHAR(CURRENT_TIMESTAMP, 'YYYYMMDDHH24MISSFF3')) as batch_id,
        CAST(CURRENT_TIMESTAMP AS TIMESTAMP_NTZ)  AS batch_timestamp
),
CTE_transformed as (
    select
        -- Surrogate and Business Keys
        {{ generate_surrogate_key([trim_string('EMPLOYEE_ID')]) }} as ODS_WORKDAY_EMPLOYEE_PK ,
        CAST({{ trim_string('EMPLOYEE_ID') }} AS VARCHAR(32)) as ODS_WORKDAY_EMPLOYEE_COMPOSITE_KEY,
 
        -- Employee identification
        CAST({{ trim_string('EMPLOYEE_ID') }} AS VARCHAR(32)) as EMPLOYEE_ID,
        CAST({{ trim_string('USER_ID') }} AS VARCHAR(40)) as USER_ID,
        CAST({{ trim_string('POSITION_CODE') }} AS VARCHAR(32)) as POSITION_CODE,
        
        -- Employee name fields
        CAST({{ trim_string('LEGAL_NAME_LAST_NAME') }} AS VARCHAR(40)) as LEGAL_NAME_LAST_NAME,
        CAST({{ trim_string('LEGAL_NAME_FIRST_NAME') }} AS VARCHAR(40)) as LEGAL_NAME_FIRST_NAME,
        CAST({{ trim_string('LEGAL_NAME_MIDDLE_INITIAL') }} AS VARCHAR(32)) as LEGAL_NAME_MIDDLE_INITIAL,
        CAST({{ trim_string('LEGAL_NAME_MIDDLE_NAME') }} AS VARCHAR(32)) as LEGAL_NAME_MIDDLE_NAME,
        CAST({{ trim_string('NICK_NAME') }} AS VARCHAR(40)) as NICK_NAME,
        CAST({{ trim_string('COMPLETE_NAME') }} AS VARCHAR(60)) as COMPLETE_NAME,        
        -- Contact information
        CAST({{ trim_string('EMAIL_ID') }} AS VARCHAR(60)) as EMAIL_ID,
        
        -- Date fields - convert NUMBER(8,0) format (YYYYMMDD) to DATE
        case 
            when DATE_OF_HIRE is not null and DATE_OF_HIRE != 0
            then try_to_date({{ trim_string('CAST(DATE_OF_HIRE AS VARCHAR)') }}, 'YYYYMMDD')
            else null 
        end as DATE_OF_HIRE,
        case 
            when ADJUSTED_DATE_OF_HIRE is not null and ADJUSTED_DATE_OF_HIRE != 0
            then try_to_date({{ trim_string('CAST(ADJUSTED_DATE_OF_HIRE AS VARCHAR)') }}, 'YYYYMMDD')
            else null 
        end as ADJUSTED_DATE_OF_HIRE,
        case 
            when TERMINATION_DATE is not null and TERMINATION_DATE != 0
            then try_to_date({{ trim_string('CAST(TERMINATION_DATE AS VARCHAR)') }}, 'YYYYMMDD')
            else null 
        end as TERMINATION_DATE,
        
        -- Employment status and job information
        CAST({{ trim_string('EMPLOYEE_STATUS') }} AS VARCHAR(32)) as EMPLOYEE_STATUS,
        CAST({{ trim_string('JOB_CODE') }} AS VARCHAR(32)) as JOB_CODE,
        CAST({{ trim_string('JOB_DESCRIPTION') }} AS VARCHAR(60)) as JOB_DESCRIPTION,
        CAST({{ trim_string('JOB_FAMILY_GROUP') }} AS VARCHAR(60)) as JOB_FAMILY_GROUP,
        CAST({{ trim_string('JOB_FAMILY') }} AS VARCHAR(60)) as JOB_FAMILY,
        CAST({{ trim_string('PROCESS_LEVEL') }} AS VARCHAR(32)) as PROCESS_LEVEL,
        CAST({{ trim_string('ACCESS_HIERARCHY_LEVEL') }} AS VARCHAR(32)) as ACCESS_HIERARCHY_LEVEL,
        
        -- Organization hierarchy
        CAST({{ trim_string('REGION') }} AS VARCHAR(32)) as REGION,
        CAST({{ trim_string('REGION_DESCRIPTION') }} AS VARCHAR(32)) as REGION_DESCRIPTION,
        CAST({{ trim_string('AREA') }} AS VARCHAR(32)) as AREA,
        CAST({{ trim_string('AREA_DESCRIPTION') }} AS VARCHAR(32)) as AREA_DESCRIPTION,
        CAST({{ trim_string('BUSINESS_UNIT') }} AS VARCHAR(32)) as BUSINESS_UNIT,
        CAST({{ trim_string('BUSINESS_UNIT_DESCRIPTION') }} AS VARCHAR(100)) as BUSINESS_UNIT_DESCRIPTION,
        CAST({{ trim_string('DIVISION_ACCOUNTING_MANAGER') }} AS VARCHAR(32)) as DIVISION_ACCOUNTING_MANAGER,
        CAST({{ trim_string('MARKET_GROUP') }} AS VARCHAR(32)) as MARKET_GROUP,
        
        -- Location information
        CAST({{ trim_string('SITE') }} AS VARCHAR(32)) as SITE,
        CAST({{ trim_string('SITE_DESCRIPTION') }} AS VARCHAR(32)) as SITE_DESCRIPTION,
        CAST({{ trim_string('LOCATION_CODE') }} AS VARCHAR(32)) as LOCATION_CODE,
        CAST({{ trim_string('LOCATION_ADDRESS_LINE_1') }} AS VARCHAR(32)) as LOCATION_ADDRESS_LINE_1,
        CAST({{ trim_string('LOCATION_ADDRESS_LINE_2') }} AS VARCHAR(32)) as LOCATION_ADDRESS_LINE_2,
        CAST({{ trim_string('LOCATION_ADDRESS_CITY') }} AS VARCHAR(32)) as LOCATION_ADDRESS_CITY,
        CAST({{ trim_string('LOCATION_ADDRESS_STATE_ISO_CODE') }} AS VARCHAR(32)) as LOCATION_ADDRESS_STATE_ISO_CODE,
        CAST({{ trim_string('LOCATION_ADDRESS_POSTAL_CODE') }} AS VARCHAR(32)) as LOCATION_ADDRESS_POSTAL_CODE,
        CAST({{ trim_string('LOCATION_ADDRESS_COUNTRY') }} AS VARCHAR(50)) as LOCATION_ADDRESS_COUNTRY,
        CAST({{ trim_string('LOC_PHONE') }} AS VARCHAR(32)) as LOC_PHONE,
        CAST({{ trim_string('LOC_FAX') }} AS VARCHAR(32)) as LOC_FAX,
        CAST({{ trim_string('WORK_PHONE') }} AS VARCHAR(32)) as WORK_PHONE,
        CAST({{ trim_string('CELL_PHONE') }} AS VARCHAR(32)) as CELL_PHONE,
        
        -- Supervisor information
        CAST({{ trim_string('SUPERVISOR_EMPLOYEE_ID') }} AS VARCHAR(32)) as SUPERVISOR_EMPLOYEE_ID,
        CAST({{ trim_string('SUPERVISOR_LEGAL_LAST_NAME') }} AS VARCHAR(32)) as SUPERVISOR_LEGAL_LAST_NAME,
        CAST({{ trim_string('SUPERVISOR_LEGAL_FIRST_NAME') }} AS VARCHAR(32)) as SUPERVISOR_LEGAL_FIRST_NAME,
        CAST({{ trim_string('SUPERVISOR_USER_ID') }} AS VARCHAR(32)) as SUPERVISOR_USER_ID,
        CAST({{ trim_string('SUPERVISOR_EMAIL_ID') }} AS VARCHAR(60)) as SUPERVISOR_EMAIL_ID,
        
        -- Indirect reporting
        CAST({{ trim_string('INDIRECT_REPORTS_TO') }} AS VARCHAR(32)) as INDIRECT_REPORTS_TO,
        CAST({{ trim_string('INDIRECT_REPORT_LAST_NAME') }} AS VARCHAR(32)) as INDIRECT_REPORT_LAST_NAME,
        CAST({{ trim_string('INDIRECT_REPORT_FIRST_NAME') }} AS VARCHAR(32)) as INDIRECT_REPORT_FIRST_NAME,
        
        -- Financial controllers - Region level
        CAST({{ trim_string('REGION_CONTROLLER__EIN') }} AS VARCHAR(32)) as REGION_CONTROLLER__EIN,
        CAST({{ trim_string('REGION_CONTROLLER_LAST_NAME') }} AS VARCHAR(32)) as REGION_CONTROLLER_LAST_NAME,
        CAST({{ trim_string('REGION_CONTROLLER_FIRST_NAME') }} AS VARCHAR(32)) as REGION_CONTROLLER_FIRST_NAME,
        CAST({{ trim_string('REGIONAL_ASST_CONTROLLER__EIN') }} AS VARCHAR(32)) as REGIONAL_ASST_CONTROLLER__EIN,
        CAST({{ trim_string('REGIONAL_ASST_CONTROLLER_LAST_NAME') }} AS VARCHAR(32)) as REGIONAL_ASST_CONTROLLER_LAST_NAME,
        CAST({{ trim_string('REGIONAL_ASST_CONTROLLER_FIRST_NAME') }} AS VARCHAR(32)) as REGIONAL_ASST_CONTROLLER_FIRST_NAME,
        CAST({{ trim_string('REGION_FINANCE_MANAGER__EIN') }} AS VARCHAR(32)) as REGION_FINANCE_MANAGER__EIN,
        CAST({{ trim_string('REGION_FINANCE_MANAGER_LAST_NAME') }} AS VARCHAR(32)) as REGION_FINANCE_MANAGER_LAST_NAME,
        CAST({{ trim_string('REGION_FINANCE_MANAGER_FIRST_NAME') }} AS VARCHAR(32)) as REGION_FINANCE_MANAGER_FIRST_NAME,
        
        -- Financial controllers - Area level
        CAST({{ trim_string('AREA_CONTROLLER__EIN') }} AS VARCHAR(32)) as AREA_CONTROLLER__EIN,
        CAST({{ trim_string('AREA_CONTROLLER_LAST_NAME') }} AS VARCHAR(32)) as AREA_CONTROLLER_LAST_NAME,
        CAST({{ trim_string('AREA_CONTROLLER_FIRST_NAME') }} AS VARCHAR(32)) as AREA_CONTROLLER_FIRST_NAME,
        CAST({{ trim_string('ASST_AREA_CONTROLLER__EIN') }} AS VARCHAR(32)) as ASST_AREA_CONTROLLER__EIN,
        CAST({{ trim_string('ASST_AREA_CONTROLLER_LAST_NAME') }} AS VARCHAR(32)) as ASST_AREA_CONTROLLER_LAST_NAME,
        CAST({{ trim_string('ASST_AREA_CONTROLLER_FIRST_NAME') }} AS VARCHAR(32)) as ASST_AREA_CONTROLLER_FIRST_NAME,
        CAST({{ trim_string('AREA_SALES_MANAGER__EIN') }} AS VARCHAR(32)) as AREA_SALES_MANAGER__EIN,
        CAST({{ trim_string('AREA_SALES_MANAGER_LAST_NAME') }} AS VARCHAR(32)) as AREA_SALES_MANAGER_LAST_NAME,
        CAST({{ trim_string('AREA_SALES_MANAGER_FIRST_NAME') }} AS VARCHAR(32)) as AREA_SALES_MANAGER_FIRST_NAME,
        CAST({{ trim_string('AREA_DIRECTOR_OF_OPERATIONS__EIN') }} AS VARCHAR(32)) as AREA_DIRECTOR_OF_OPERATIONS__EIN,
        
        -- Financial controllers - Division level
        CAST({{ trim_string('DIVISION_CONTROLLER__EIN') }} AS VARCHAR(32)) as DIVISION_CONTROLLER__EIN,
        CAST({{ trim_string('DIVISION_CONTROLLER_LAST_NAME') }} AS VARCHAR(32)) as DIVISION_CONTROLLER_LAST_NAME,
        CAST({{ trim_string('DIVISION_CONTROLLER_FIRST_NAME') }} AS VARCHAR(32)) as DIVISION_CONTROLLER_FIRST_NAME,
        CAST({{ trim_string('DIVISION_GENERAL_MANAGER__EIN') }} AS VARCHAR(32)) as DIVISION_GENERAL_MANAGER__EIN,
        CAST({{ trim_string('DIVISION_GENERAL_MANAGER_LAST_NAME') }} AS VARCHAR(32)) as DIVISION_GENERAL_MANAGER_LAST_NAME,
        CAST({{ trim_string('DIVISION_GENERAL_MANAGER_FIRST_NAME') }} AS VARCHAR(32)) as DIVISION_GENERAL_MANAGER_FIRST_NAME,
        CAST({{ trim_string('DIVISION_SALES_MANAGER__EIN') }} AS VARCHAR(32)) as DIVISION_SALES_MANAGER__EIN,
        CAST({{ trim_string('DIVISION_SALES_MANAGER_LAST_NAME') }} AS VARCHAR(32)) as DIVISION_SALES_MANAGER_LAST_NAME,
        CAST({{ trim_string('DIVISION_SALES_MANAGER_FIRST_NAME') }} AS VARCHAR(32)) as DIVISION_SALES_MANAGER_FIRST_NAME,
        CAST({{ trim_string('DIVISION_MANAGER__EIN') }} AS VARCHAR(32)) as DIVISION_MANAGER__EIN,
        CAST({{ trim_string('OPERATIONS_MANAGER__EIN') }} AS VARCHAR(32)) as OPERATIONS_MANAGER__EIN,
        
        -- Other fields
        CAST({{ trim_string('HR_PARTNER_EIN') }} AS VARCHAR(15)) as HR_PARTNER_EIN,
        CAST({{ trim_string('UNION_CODE') }} AS VARCHAR(32)) as UNION_CODE,
        CAST({{ trim_string('INFOPRO_DIVISION') }} AS VARCHAR(32)) as INFOPRO_DIVISION,
        CAST({{ trim_string('LAWSON_HR_PROFILE') }} AS VARCHAR(255)) as LAWSON_HR_PROFILE,
        CAST({{ trim_string('LAWSON_FIN_PROFILE') }} AS VARCHAR(32)) as LAWSON_FIN_PROFILE,
        CAST({{ trim_string('KRONOS_PROFILE') }} AS VARCHAR(32)) as KRONOS_PROFILE,
        CAST({{ trim_string('INFOPRO_PROFILE') }} AS VARCHAR(32)) as INFOPRO_PROFILE,
        CAST({{ trim_string('TYPE_E') }} AS VARCHAR(32)) as TYPE_E,
        SYS_INSERT_DTM as SYS_INSERT_DTM,
        SYS_LAST_UPDATE_DTM as SYS_LAST_UPDATE_DTM,
        
        -- Source system metadata
        coalesce(SOURCE_SYSTEM, 'WORKDAY') as SOURCE_SYSTEM,
        cast('STG_WORKDAY_EMPLOYEE' as varchar(100)) as SOURCE_TABLE,
        
        -- staging primary key for reference
        STG_WORKDAY_EMPLOYEE_PK as STG_WORKDAY_EMPLOYEE_PK,

        -- Source system metadata fields
        SYS_INSERT_DTM as SRC_CREATE_DTM,
        SYS_LAST_UPDATE_DTM as SRC_LAST_UPDATE_DTM,
        'N' as SRC_DEL_IND,
        null as SRC_ACTION_CD,

        -- ODS ETL metadata
        CTE_audits.batch_id as ODS_INS_BATCH_ID,
        CTE_audits.batch_id as ODS_UPD_BATCH_ID,
        CTE_audits.batch_timestamp as ODS_INSERT_TIMESTAMP,
        CTE_audits.batch_timestamp as ODS_UPDATE_TIMESTAMP,
        STG_INSERT_TIMESTAMP as STG_INSERT_TIMESTAMP,
        'INSERT' as OPERATION_TYPE,
        
        -- Data quality fields
        cast(null as varchar(20)) as DQ_STATUS,
        cast(null as varchar(50)) as DQ_ERROR_CODE,
        cast(null as varchar(500)) as DQ_ERROR_MESSAGE

    from CTE_source_data_deduped
    cross join CTE_audits
    
)

    select
        *,
        --Hashed Columns
        {{ generate_hash_change( relation = this) }} AS HASH_CHANGE,
        {{ generate_hash_full( relation = this) }} AS HASH_FULL
    from CTE_transformed