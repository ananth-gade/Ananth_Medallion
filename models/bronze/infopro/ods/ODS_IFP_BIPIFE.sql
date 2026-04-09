{{
    config(
        materialized='incremental',
        unique_key='ODS_IFP_BIPIFE_PK',
        merge_no_update_columns = ['ODS_INS_BATCH_ID','ODS_INSERT_TIMESTAMP','SRC_CREATE_DTM'],
        merge_condition="DBT_INTERNAL_SOURCE.HASH_CHANGE != DBT_INTERNAL_DEST.HASH_CHANGE",
        tags=["ods","infopro","scheduled-every30min","data-observe"],
	pre_hook=" {% if is_incremental() %}
        EXECUTE IMMEDIATE $$
                 BEGIN
                 IF (EXISTS(SELECT  * FROM {{ this.database }}.information_schema.tables WHERE table_schema = UPPER('{{ this.schema }}') AND table_name = UPPER('{{ this.identifier }}'))) THEN
                 UPDATE {{ this }}
                 SET SRC_ACTION_CD='D',
                     SRC_DEL_IND='Y',
                     SRC_LAST_UPDATE_DTM=CURRENT_TIMESTAMP,
                     ODS_UPD_BATCH_ID=TO_NUMBER(TO_VARCHAR(CURRENT_TIMESTAMP,'YYYYMMDDHH24MISSFF3')),
                     ODS_UPDATE_TIMESTAMP=CURRENT_TIMESTAMP,
                     OPERATION_TYPE='DELETE'
                 WHERE ODS_IFP_BIPIFE_PK IN (
                     SELECT {{ generate_surrogate_key([trim_string('DTL__BI_FECOMP'),trim_string('DTL__BI_FEACCT')]) }}
                     FROM {{ source('STAGING','STG_IFP_BIPIFE') }}
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

--> Grab all the records newer than last update
WITH CTE_BATCH_ID AS (
    SELECT TO_NUMBER(TO_VARCHAR(CURRENT_TIMESTAMP, 'YYYYMMDDHH24MISSFF3')) AS BATCH_ID,
    CAST(CURRENT_TIMESTAMP AS TIMESTAMP_NTZ)  AS BATCH_TIMESTAMP
)

{% if is_incremental() %}
,CTE_SOURCE_RECORDS AS (
        SELECT * FROM {{ source('STAGING','STG_IFP_BIPIFE') }}
        where
        -- this filter will only be applied on an incremental run
        to_timestamp_tz(SUBSTRING(TRIM(DTL__CAPXTIMESTAMP),1,17),'YYYYMMDDHHMISSFF')  >= '{{ get_max_event_time('SRC_LAST_UPDATE_DTM') }}'
)

,CTE_DEDUP as (
  select *, ROW_NUMBER() OVER(PARTITION BY FECOMP,FEACCT ORDER BY DTL__CAPXTIMESTAMP DESC) AS ROW_NUM
  from CTE_SOURCE_RECORDS
  qualify ROW_NUM=1
)

,CTE_TEMP as (
select CTE_SOURCE_RECORDS.DTL__BI_FECOMP, CTE_SOURCE_RECORDS.DTL__BI_FEACCT from 
CTE_SOURCE_RECORDS LEFT JOIN CTE_DEDUP 
 ON CTE_SOURCE_RECORDS.DTL__BI_FECOMP=CTE_DEDUP.FECOMP
 AND CTE_SOURCE_RECORDS.DTL__BI_FEACCT=CTE_DEDUP.FEACCT
 where CTE_SOURCE_RECORDS.PRIMARY_KEY_CHANGE  = 'Y'
AND (TO_NUMBER(CTE_SOURCE_RECORDS.PK_LZ_IFP_BIPIFE_ID) >=  TO_NUMBER(CTE_DEDUP.PK_LZ_IFP_BIPIFE_ID) OR TO_NUMBER(CTE_DEDUP.PK_LZ_IFP_BIPIFE_ID) IS NULL)
)

,CTE_FINAL as (
select * from CTE_DEDUP where CONCAT_WS(',',FECOMP,FEACCT) in (
select CONCAT_WS(',',FECOMP,FEACCT) from CTE_DEDUP
except 
select CONCAT_WS(',',DTL__BI_FECOMP,DTL__BI_FEACCT) from CTE_TEMP)
)

,--> Current Delta data capture from STG table
CTE_TRANSFORMED AS (
SELECT
        {{ generate_surrogate_key([trim_string('FECOMP'),trim_string('FEACCT')])}} as ODS_IFP_BIPIFE_PK ,
        CAST(CONCAT({{ trim_string('FECOMP')}},'-',{{ trim_string('FEACCT')}}) AS VARCHAR(50)) AS ODS_IFP_BIPIFE_COMPOSITE_KEY,
        STG_IFP_BIPIFE_PK,
        {{ trim_string('DTL__CAPXUSER') }} AS SRC_CDC_LIB,
        {{ trim_string('FECOMP')}} AS FECOMP,
        {{ trim_string('FEACCT')}} AS FEACCT,
        {{ trim_string('FEAFLG')}} AS FEAFLG,
        {{ trim_string('FETXAP')}} AS FETXAP,
        {{ trim_string('FETXCD')}} AS FETXCD,
        {{ trim_string('FERVDC')}} AS FERVDC,
        {{ trim_string('FEUSER')}} AS FEUSER,
        CAST(FECDTE AS NUMBER(10,0)) AS FECDTE,
        CAST(FECTIM AS NUMBER(10,0)) AS FECTIM,
        CAST(SOURCE_SYSTEM AS VARCHAR(50)) AS SOURCE_SYSTEM,
        CAST('STG_IFP_BIPIFE' AS VARCHAR(200)) AS SOURCE_TABLE,
        to_timestamp_ntz(to_timestamp_tz(SUBSTRING(TRIM(DTL__CAPXTIMESTAMP),1,17),'YYYYMMDDHHMISSFF')) AS SRC_CREATE_DTM,
        to_timestamp_ntz(to_timestamp_tz(SUBSTRING(TRIM(DTL__CAPXTIMESTAMP),1,17),'YYYYMMDDHHMISSFF')) AS SRC_LAST_UPDATE_DTM,
        TRIM(DTL__CAPXACTION) AS SRC_ACTION_CD,
        CASE WHEN TRIM(DTL__CAPXACTION) = 'D' THEN 'Y' ELSE 'N' END AS SRC_DEL_IND,
        CAST(BATCH_ID AS BIGINT) AS ODS_INS_BATCH_ID,
        CAST(BATCH_ID AS BIGINT) AS ODS_UPD_BATCH_ID,
        CAST(BATCH_TIMESTAMP AS TIMESTAMP_NTZ) AS ODS_INSERT_TIMESTAMP,
        CAST(BATCH_TIMESTAMP AS TIMESTAMP_NTZ) AS ODS_UPDATE_TIMESTAMP,
        CAST(STG_INSERT_TIMESTAMP AS TIMESTAMP_NTZ) AS STG_INSERT_TIMESTAMP,
        CASE WHEN SRC_DEL_IND = 'Y' THEN 'DELETE' ELSE 'INSERT' END AS OPERATION_TYPE,
        CAST(NULL AS VARCHAR(20)) AS DQ_STATUS,
        CAST(NULL AS VARCHAR(50)) AS DQ_ERROR_CODE,
        CAST(NULL AS VARCHAR(500)) AS DQ_ERROR_MESSAGE
from CTE_FINAL
LEFT JOIN CTE_BATCH_ID ON 1=1
)

{% else %}
,CTE_HISTORICAL AS (
SELECT
        {{ generate_surrogate_key([trim_string('FECOMP'),trim_string('FEACCT')]) }} as ODS_IFP_BIPIFE_PK,
        CAST(CONCAT({{ trim_string('FECOMP') }},'-',{{ trim_string('FEACCT') }}) AS VARCHAR(50)) AS ODS_IFP_BIPIFE_COMPOSITE_KEY,
        CAST(NULL AS NUMBER) AS STG_IFP_BIPIFE_PK,
        {{ trim_string('SYS_CDC_LIB') }} AS SRC_CDC_LIB,
        {{ trim_string('FECOMP') }} AS FECOMP,
        {{ trim_string('FEACCT') }} AS FEACCT,
        {{ trim_string('FEAFLG') }} AS FEAFLG,
        {{ trim_string('FETXAP') }} AS FETXAP,
        {{ trim_string('FETXCD') }} AS FETXCD,
        {{ trim_string('FERVDC') }} AS FERVDC,
        {{ trim_string('FEUSER') }} AS FEUSER,
        CAST(FECDTE AS NUMBER(10,0)) AS FECDTE,
        CAST(FECTIM AS NUMBER(10,0)) AS FECTIM,
        CAST('Infopro' AS VARCHAR(50)) AS SOURCE_SYSTEM,
        CAST('ODS_IFP_BIPIFE_HIST' AS VARCHAR(200)) AS SOURCE_TABLE,
        CAST(SYS_CDC_DTM AS TIMESTAMP_NTZ) AS SRC_CREATE_DTM,
        CAST(SYS_CDC_DTM AS TIMESTAMP_NTZ) AS SRC_LAST_UPDATE_DTM,
        CAST(SYS_ACTION_CD AS VARCHAR(1)) AS SRC_ACTION_CD,
        CAST(SYS_DEL_IND AS VARCHAR(1)) AS SRC_DEL_IND,
        CAST(BATCH_ID AS BIGINT) AS ODS_INS_BATCH_ID,
        CAST(BATCH_ID AS BIGINT) AS ODS_UPD_BATCH_ID,
        CAST(BATCH_TIMESTAMP AS TIMESTAMP_NTZ) AS ODS_INSERT_TIMESTAMP,
        CAST(BATCH_TIMESTAMP AS TIMESTAMP_NTZ) AS ODS_UPDATE_TIMESTAMP,
        CAST(STG_INSERT_TIMESTAMP AS TIMESTAMP_NTZ) AS STG_INSERT_TIMESTAMP,
        CASE WHEN SYS_DEL_IND = 'Y' THEN 'DELETE' ELSE 'INSERT' END AS OPERATION_TYPE,
        CAST(NULL AS VARCHAR(20)) AS DQ_STATUS,
        CAST(NULL AS VARCHAR(50)) AS DQ_ERROR_CODE,
        CAST(NULL AS VARCHAR(500)) AS DQ_ERROR_MESSAGE
FROM INFOPRO.HISTORY.ODS_IFP_BIPIFE_HIST
CROSS JOIN CTE_BATCH_ID
)

{% endif %}

SELECT 
        *,
        {{ generate_hash_change( relation = this) }} AS HASH_CHANGE,
        {{ generate_hash_full( relation = this) }} AS HASH_FULL
FROM {% if is_incremental() %}CTE_TRANSFORMED{% else %}CTE_HISTORICAL{% endif %}






