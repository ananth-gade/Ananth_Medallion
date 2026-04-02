-- ODS_ES_EQAI_USERS
-- Loads ES EQAI Users data from STG_ES_EQAI_USERS into the ODS layer using
-- incremental materialization, deduplicating by USER_ID on latest STG_INSERT_TIMESTAMP,
-- and adding audit metadata and hash columns (HASH_CHANGE, HASH_FULL) for change tracking.

{{
    config(
        materialized='incremental',
        unique_key='ODS_ES_EQAI_USERS_PK',
        merge_no_update_columns = ['ODS_INS_BATCH_ID', 'ODS_INSERT_TIMESTAMP'],
        merge_condition="DBT_INTERNAL_SOURCE.HASH_CHANGE != DBT_INTERNAL_DEST.HASH_CHANGE",
        tags=["bronze","ods","es_eqai","data-observe"],
        pre_hook = ["{{ purge_staging_duplicates_by_hash('EQAI', 'STAGING', 'STG_ES_EQAI_USERS', ['STG_INSERT_TIMESTAMP', 'EXEC_BATCH_ID', 'SOURCE_SYSTEM', 'STG_ES_EQAI_USERS_PK']) }}"],
        post_hook = ["{{ mark_deleted_records('EQAI','ODS','ODS_ES_EQAI_USERS','EQAI','STAGING','STG_ES_EQAI_USERS','USER_ID') }}"]   
   )
}}

with 
CTE_source_data as (
    select * 
    from {{ source('STAGING','STG_ES_EQAI_USERS') }} 
    {% if is_incremental() %}
        -- Incremental logic: only pull records that are new or updated since last run
        where STG_INSERT_TIMESTAMP >= '{{ get_max_event_time('STG_INSERT_TIMESTAMP') }}'  
    {% endif %}
),
CTE_source_data_deduped as (
    select
        *,
        row_number() over (partition by USER_ID order by DATE_MODIFIED desc) as rn
    from CTE_source_data
    qualify rn = 1 -- Keep only the latest record per USER_ID based on DATE_MODIFIED
    ),
CTE_audits as (
    select
        TO_NUMBER(TO_VARCHAR(CURRENT_TIMESTAMP, 'YYYYMMDDHH24MISSFF3')) as batch_id,
        CAST(CURRENT_TIMESTAMP AS TIMESTAMP_NTZ)  AS batch_timestamp
),
CTE_transformed as (
    select
        -- Surrogate and Business Keys
        {{ generate_surrogate_key(['USER_ID']) }} as ODS_ES_EQAI_USERS_PK ,
        cast(USER_ID as varchar) as ODS_ES_EQAI_USERS_COMPOSITE_KEY,
 
        -- User identification
        cast(USER_ID as number(10,0)) as USER_ID,
        cast(USER_CODE as varchar(10)) as USER_CODE,
        cast(GROUP_ID as number(10,0)) as GROUP_ID,
        cast(USER_NAME as varchar(40)) as USER_NAME,
        cast(TITLE as varchar(100)) as TITLE,
        
        -- Address fields
        cast(ADDR1 as varchar(40)) as ADDR1,
        cast(ADDR2 as varchar(40)) as ADDR2,
        cast(ADDR3 as varchar(40)) as ADDR3,
        
        -- Contact information
        cast(PHONE as varchar(20)) as PHONE,
        cast(FAX as varchar(20)) as FAX,
        cast(PAGER as varchar(20)) as PAGER,
        cast(EMAIL as varchar(80)) as EMAIL,
        
        -- Territory and position
        cast(TERRITORY_CODE as varchar(8)) as TERRITORY_CODE,
        cast(POSITION as varchar(8)) as POSITION,
        
        -- Security and access
        cast(CHANGE_PASSWORD as varchar(1)) as CHANGE_PASSWORD,
        cast(DEFAULT_COMPANY_ID as number(10,0)) as DEFAULT_COMPANY_ID,
        cast(COMMENT as varchar(16777216)) as COMMENT,
        
        -- Printer preferences - Container labels
        cast(PRINTER_CONTAINER_LABEL as varchar(200)) as PRINTER_CONTAINER_LABEL,
        cast(PRINTER_CONTAINER_LABEL_MINI as varchar(200)) as PRINTER_CONTAINER_LABEL_MINI,
        
        -- Printer preferences - Lab and manifest
        cast(PRINTER_LAB_LABEL as varchar(200)) as PRINTER_LAB_LABEL,
        cast(PRINTER_MANIFEST as varchar(200)) as PRINTER_MANIFEST,
        cast(PRINTER_NONHAZ_MANIFEST as varchar(200)) as PRINTER_NONHAZ_MANIFEST,
        
        -- Printer preferences - Work orders
        cast(PRINTER_WO as varchar(200)) as PRINTER_WO,
        cast(PRINTER_WO_LABEL as varchar(200)) as PRINTER_WO_LABEL,
        
        -- Printer preferences - Hazard labels
        cast(PRINTER_HAZ_LABEL as varchar(200)) as PRINTER_HAZ_LABEL,
        cast(PRINTER_NONHAZ_LABEL as varchar(200)) as PRINTER_NONHAZ_LABEL,
        cast(PRINTER_NONRCRA_LABEL as varchar(200)) as PRINTER_NONRCRA_LABEL,
        cast(PRINTER_UNIVERSAL_LABEL as varchar(200)) as PRINTER_UNIVERSAL_LABEL,
        
        -- Printer preferences - Other
        cast(PRINTER_CONTINUATION as varchar(200)) as PRINTER_CONTINUATION,
        cast(PRINTER_PDF as varchar(200)) as PRINTER_PDF,
        cast(PRINTER_FAX as varchar(200)) as PRINTER_FAX,
        
        -- Audit fields from source
        cast(DATE_ADDED as timestamp_ntz(9)) as DATE_ADDED,
        cast(ADDED_BY as varchar(8)) as ADDED_BY,
        cast(DATE_MODIFIED as timestamp_ntz(9)) as DATE_MODIFIED,
        cast(MODIFIED_BY as varchar(8)) as MODIFIED_BY,
        cast(ROWGUID as varchar(16777216)) as ROWGUID,
        
        -- Access flags
        cast(B2B_ACCESS as varchar(1)) as B2B_ACCESS,
        cast(B2B_REMOTE_ACCESS as varchar(1)) as B2B_REMOTE_ACCESS,
        cast(LOGIN_UPDATED as varchar(1)) as LOGIN_UPDATED,
        
        -- Department and organizational
        cast(DEPARTMENT_ID as number(10,0)) as DEPARTMENT_ID,
        
        -- Employee information
        cast(FIRST_NAME as varchar(20)) as FIRST_NAME,
        cast(LAST_NAME as varchar(20)) as LAST_NAME,
        cast(CELL_PHONE as varchar(20)) as CELL_PHONE,
        
        -- Profile and location
        cast(PIC_URL as varchar(300)) as PIC_URL,
        cast(PHONE_LIST_FLAG as varchar(1)) as PHONE_LIST_FLAG,
        cast(PHONE_LIST_LOCATION_ID as number(10,0)) as PHONE_LIST_LOCATION_ID,
        cast(INTERNAL_PHONE as varchar(10)) as INTERNAL_PHONE,
        
        -- Alternative identification
        cast(ALIAS_NAME as varchar(40)) as ALIAS_NAME,
        cast(UPN as varchar(100)) as UPN,
        cast(EMPLOYEE_ID as varchar(20)) as EMPLOYEE_ID,
        
        -- Legacy fields
        cast(LEGACY_EMAIL_ADDRESS as varchar(80)) as LEGACY_EMAIL_ADDRESS,
        cast(LEGACY_UPN as varchar(100)) as LEGACY_UPN,
        
        -- Staging primary key for reference
        STG_ES_EQAI_USERS_PK as STG_ES_EQAI_USERS_PK,
        
        -- Source system metadata
        coalesce(SOURCE_SYSTEM, 'ES_EQAI') as SOURCE_SYSTEM,
        cast('STG_ES_EQAI_USERS' as varchar(100)) as SOURCE_TABLE,

        -- Source system metadata fields
        DATE_ADDED as SRC_CREATE_DTM,
        DATE_MODIFIED as SRC_LAST_UPDATE_DTM,
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