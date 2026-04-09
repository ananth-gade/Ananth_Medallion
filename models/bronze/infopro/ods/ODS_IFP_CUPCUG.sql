{{
    config(
        materialized='incremental',
        unique_key='ODS_IFP_CUPCUG_PK',
        merge_no_update_columns = ['ODS_INS_BATCH_ID','ODS_INSERT_TIMESTAMP','SRC_CREATE_DTM'],
        merge_condition="DBT_INTERNAL_SOURCE.HASH_CHANGE != DBT_INTERNAL_DEST.HASH_CHANGE",
        tags=["ods","infopro","scheduled-every30min","data-observe"],
	pre_hook=" {% if is_incremental() %}
                EXECUTE IMMEDIATE $$
                 BEGIN
                 IF (EXISTS(SELECT * FROM {{ this.database }}.information_schema.tables WHERE table_schema = UPPER('{{ this.schema }}') AND table_name = UPPER('{{ this.identifier }}'))) THEN
                 UPDATE {{ this }}
                 SET SRC_ACTION_CD='D',
                     SRC_DEL_IND='Y',
                     SRC_LAST_UPDATE_DTM=CURRENT_TIMESTAMP,
                     ODS_UPD_BATCH_ID=TO_NUMBER(TO_VARCHAR(CURRENT_TIMESTAMP,'YYYYMMDDHH24MISSFF3')),
                     ODS_UPDATE_TIMESTAMP=CURRENT_TIMESTAMP,
                     OPERATION_TYPE='DELETE'
                 WHERE ODS_IFP_CUPCUG_PK IN (
                     SELECT {{ generate_surrogate_key([trim_string('DTL__BI_ACCOUNT_GROUP_ID')]) }}
                     FROM {{ source('STAGING','STG_IFP_CUPCUG') }}
                     WHERE PRIMARY_KEY_CHANGE='Y'
                     AND to_timestamp_tz(SUBSTRING(DTL__CAPXTIMESTAMP,1,17),'YYYYMMDDHHMISSFF') >= '{{ get_max_event_time('SRC_LAST_UPDATE_DTM') }}'
                 );
                 COMMIT;
                 END IF;
                END;
                $$;
                {% endif %}"
        )
}}

WITH CTE_BATCH_ID AS (
    SELECT TO_NUMBER(TO_VARCHAR(CURRENT_TIMESTAMP, 'YYYYMMDDHH24MISSFF3')) AS BATCH_ID,
    CAST(CURRENT_TIMESTAMP AS TIMESTAMP_NTZ)  AS BATCH_TIMESTAMP
)

{% if is_incremental() %}

--> Grab all the records newer than last update
,CTE_SOURCE_RECORDS AS (
        SELECT * FROM {{ source('STAGING','STG_IFP_CUPCUG') }}
        where
        -- this filter will only be applied on an incremental run
        to_timestamp_tz(SUBSTRING(TRIM(DTL__CAPXTIMESTAMP),1,17),'YYYYMMDDHHMISSFF')  >= '{{ get_max_event_time('SRC_LAST_UPDATE_DTM') }}'
)

,CTE_DEDUP as (
  select *, ROW_NUMBER() OVER(PARTITION BY ACCOUNT_GROUP_ID ORDER BY DTL__CAPXTIMESTAMP DESC) AS ROW_NUM
  from CTE_SOURCE_RECORDS
  qualify ROW_NUM=1
)

,CTE_PRIMARY_KEY_CHANGE as (
select CTE_SOURCE_RECORDS.DTL__BI_ACCOUNT_GROUP_ID from 
CTE_SOURCE_RECORDS LEFT JOIN CTE_DEDUP 
 ON 1=1
 where CTE_SOURCE_RECORDS.PRIMARY_KEY_CHANGE  = 'Y'
AND (TO_NUMBER(CTE_SOURCE_RECORDS.PK_LZ_IFP_CUPCUG_ID) >=  TO_NUMBER(CTE_DEDUP.PK_LZ_IFP_CUPCUG_ID) OR TO_NUMBER(CTE_DEDUP.PK_LZ_IFP_CUPCUG_ID) IS NULL)
)

,CTE_FINAL as (
select * from CTE_DEDUP where CAST(ACCOUNT_GROUP_ID AS VARCHAR) in (
select CAST(ACCOUNT_GROUP_ID AS VARCHAR) from CTE_DEDUP
except 
select CAST(DTL__BI_ACCOUNT_GROUP_ID AS VARCHAR) from CTE_PRIMARY_KEY_CHANGE)
)

,--> Current Delta data capture from STG table
CTE_TRANSFORMED AS (
SELECT
        {{ generate_surrogate_key([trim_string('ACCOUNT_GROUP_ID')])}} as ODS_IFP_CUPCUG_PK ,
        CAST(ACCOUNT_GROUP_ID AS VARCHAR(50)) AS ODS_IFP_CUPCUG_COMPOSITE_KEY,
        STG_IFP_CUPCUG_PK,
        {{ trim_string('DTL__CAPXUSER') }} AS SRC_CDC_LIB,
        CAST(ACCOUNT_GROUP_ID AS NUMBER(10,0)) AS UGACGRPID,
        {{ trim_string('ACCOUNT_GROUP_CODE')}} AS UGACGPCD,
        {{ trim_string('ACCOUNT_GROUP_DESCRIPTION')}} AS UGACGDS,
        {{ trim_string('DEFAULT_VALUE')}} AS UGDFTVA,
        {{ trim_string('NATIONAL_ACCOUNT')}} AS UGNAFLAG,
        {{ trim_string('ACTIVE_STATUS')}} AS UGACTIVE,
        {{ trim_string('NON_CENTRAL_BILLED')}} AS UGREQCSTID,
        CAST(CUSTOMER_ID_SOURCE AS NUMBER(5,0)) AS UGCUSSRC,
        {{ trim_string('ADDED_BY_USER')}} AS UGADDBY,
        {{ trim_string('ADDED_BY_PROGRAM')}} AS UGADDPGM,
        ADDED_TIME_STAMP AS UGADDTIM,
        {{ trim_string('CHANGED_BY_USER')}} AS UGCHGBY,
        {{ trim_string('CHANGED_BY_PROGRAM')}} AS UGCHGPGM,
        CHANGED_TIME_STAMP AS UGCHGTIM,
        SOURCE_SYSTEM,
        CAST('STG_IFP_CUPCUG' AS VARCHAR(200)) AS SOURCE_TABLE,
        to_timestamp_ntz(to_timestamp_tz(SUBSTRING(TRIM(DTL__CAPXTIMESTAMP),1,17),'YYYYMMDDHHMISSFF')) AS SRC_CREATE_DTM,
        to_timestamp_ntz(to_timestamp_tz(SUBSTRING(TRIM(DTL__CAPXTIMESTAMP),1,17),'YYYYMMDDHHMISSFF')) AS SRC_LAST_UPDATE_DTM,
        TRIM(DTL__CAPXACTION) AS SRC_ACTION_CD,
        CASE WHEN TRIM(DTL__CAPXACTION) = 'D' THEN 'Y' ELSE 'N' END AS SRC_DEL_IND,
        CAST(BATCH_ID AS BIGINT) AS ODS_INS_BATCH_ID,
        CAST(BATCH_ID AS BIGINT) AS ODS_UPD_BATCH_ID,
        BATCH_TIMESTAMP AS ODS_INSERT_TIMESTAMP,
        BATCH_TIMESTAMP AS ODS_UPDATE_TIMESTAMP,
        CAST(STG_INSERT_TIMESTAMP AS TIMESTAMP_NTZ) AS STG_INSERT_TIMESTAMP,
        CASE WHEN SRC_DEL_IND = 'Y' THEN 'DELETE' ELSE 'INSERT' END AS OPERATION_TYPE, --overwrite operation type as UPDATE/DELETE in merge operation
        CAST(NULL AS VARCHAR(20)) AS DQ_STATUS,
        CAST(NULL AS VARCHAR(500)) AS DQ_ERROR_CODE,
        CAST(NULL AS VARCHAR(2000)) AS DQ_ERROR_MESSAGE
from CTE_FINAL
CROSS JOIN CTE_BATCH_ID
)

{% else %}

,CTE_HISTORICAL AS (
SELECT
        {{ generate_surrogate_key([trim_string('UGACGRPID')]) }} AS ODS_IFP_CUPCUG_PK,
        CAST(UGACGRPID AS VARCHAR(50)) AS ODS_IFP_CUPCUG_COMPOSITE_KEY,
        CAST(NULL AS NUMBER) AS STG_IFP_CUPCUG_PK,
        {{ trim_string('SYS_CDC_LIB') }} AS SRC_CDC_LIB,
        CAST(UGACGRPID AS NUMBER(10,0)) AS UGACGRPID,
        {{ trim_string('UGACGPCD') }} AS UGACGPCD,
        {{ trim_string('UGACGDS') }} AS UGACGDS,
        {{ trim_string('UGDFTVA') }} AS UGDFTVA,
        {{ trim_string('UGNAFLAG') }} AS UGNAFLAG,
        {{ trim_string('UGACTIVE') }} AS UGACTIVE,
        {{ trim_string('UGREQCSTID') }} AS UGREQCSTID,
        CAST(UGCUSSRC AS NUMBER(5,0)) AS UGCUSSRC,
        {{ trim_string('UGADDBY') }} AS UGADDBY,
        {{ trim_string('UGADDPGM') }} AS UGADDPGM,
        CAST(UGADDTIM AS TIMESTAMP_NTZ) AS UGADDTIM,
        {{ trim_string('UGCHGBY') }} AS UGCHGBY,
        {{ trim_string('UGCHGPGM') }} AS UGCHGPGM,
        CAST(UGCHGTIM AS TIMESTAMP_NTZ) AS UGCHGTIM,
        CAST('Infopro' AS VARCHAR(50)) AS SOURCE_SYSTEM,
        CAST('ODS_IFP_CUPCUG_HIST' AS VARCHAR(200)) AS SOURCE_TABLE,
        CAST(SYS_CDC_DTM AS TIMESTAMP_NTZ) AS SRC_CREATE_DTM,
        CAST(SYS_CDC_DTM AS TIMESTAMP_NTZ) AS SRC_LAST_UPDATE_DTM,
        CAST(SYS_ACTION_CD AS VARCHAR(1)) AS SRC_ACTION_CD,
        CAST(SYS_DEL_IND AS VARCHAR(1)) AS SRC_DEL_IND,
        CAST(BATCH_ID AS BIGINT) AS ODS_INS_BATCH_ID,
        CAST(BATCH_ID AS BIGINT) AS ODS_UPD_BATCH_ID,
        BATCH_TIMESTAMP AS ODS_INSERT_TIMESTAMP,
        BATCH_TIMESTAMP AS ODS_UPDATE_TIMESTAMP,
        CAST(STG_INSERT_TIMESTAMP AS TIMESTAMP_NTZ) AS STG_INSERT_TIMESTAMP,
        CASE WHEN SYS_DEL_IND = 'Y' THEN 'DELETE' ELSE 'INSERT' END AS OPERATION_TYPE,
        CAST(NULL AS VARCHAR(20)) AS DQ_STATUS,
        CAST(NULL AS VARCHAR(500)) AS DQ_ERROR_CODE,
        CAST(NULL AS VARCHAR(2000)) AS DQ_ERROR_MESSAGE
FROM INFOPRO.HISTORY.ODS_IFP_CUPCUG_HIST
CROSS JOIN CTE_BATCH_ID
)

{% endif %}

SELECT 
        *,
        {{ generate_hash_change( relation = this) }} AS HASH_CHANGE,
        {{ generate_hash_full( relation = this) }} AS HASH_FULL
FROM {% if is_incremental() %}CTE_TRANSFORMED{% else %}CTE_HISTORICAL{% endif %}





