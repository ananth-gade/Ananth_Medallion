{{
    config(
        materialized='incremental',
        unique_key='ODS_IFP_BIPCU_PK',
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
                 WHERE ODS_IFP_BIPCU_PK IN (
                     SELECT {{ generate_surrogate_key([trim_string('DTL__BI_CUCOMP'),trim_string('DTL__BI_CUACCT')]) }}
                     FROM {{ source('STAGING','STG_IFP_BIPCU') }}
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
        SELECT * FROM {{ source('STAGING','STG_IFP_BIPCU') }}
        where
        -- this filter will only be applied on an incremental run
        to_timestamp_tz(SUBSTRING(TRIM(DTL__CAPXTIMESTAMP),1,17),'YYYYMMDDHHMISSFF')  >= '{{ get_max_event_time('SRC_LAST_UPDATE_DTM') }}'
)

,CTE_DEDUP as (
  select *, ROW_NUMBER() OVER(PARTITION BY CUCOMP,CUACCT ORDER BY DTL__CAPXTIMESTAMP DESC) AS ROW_NUM
  from CTE_SOURCE_RECORDS
  qualify ROW_NUM=1
)

,CTE_TEMP as (
select CTE_SOURCE_RECORDS.DTL__BI_CUCOMP, CTE_SOURCE_RECORDS.DTL__BI_CUACCT from 
CTE_SOURCE_RECORDS LEFT JOIN CTE_DEDUP 
 ON CTE_SOURCE_RECORDS.DTL__BI_CUCOMP=CTE_DEDUP.CUCOMP
 AND CTE_SOURCE_RECORDS.DTL__BI_CUACCT=CTE_DEDUP.CUACCT
 where CTE_SOURCE_RECORDS.PRIMARY_KEY_CHANGE  = 'Y'
AND (TO_NUMBER(CTE_SOURCE_RECORDS.PK_LZ_IFP_BIPCU_ID) >=  TO_NUMBER(CTE_DEDUP.PK_LZ_IFP_BIPCU_ID) OR TO_NUMBER(CTE_DEDUP.PK_LZ_IFP_BIPCU_ID) IS NULL)
)

,CTE_FINAL as (
select * from CTE_DEDUP where CONCAT_WS(',',CUCOMP,CUACCT) in (
select CONCAT_WS(',',CUCOMP,CUACCT) from CTE_DEDUP
except 
select CONCAT_WS(',',DTL__BI_CUCOMP,DTL__BI_CUACCT) from CTE_TEMP)
)

,--> Current Delta data capture from STG table
CTE_TRANSFORMED AS (
SELECT
        {{ generate_surrogate_key([trim_string('CUCOMP'),trim_string('CUACCT')])}} as ODS_IFP_BIPCU_PK ,
        CAST(CONCAT({{ trim_string('CUCOMP')}},'-',{{ trim_string('CUACCT')}}) AS VARCHAR(50)) AS ODS_IFP_BIPCU_COMPOSITE_KEY,
        STG_IFP_BIPCU_PK,
        {{ trim_string('DTL__CAPXUSER') }} AS SRC_CDC_LIB,
        {{ trim_string('CUCOMP')}} AS CUCOMP,
        {{ trim_string('CUACCT')}} AS CUACCT,
        {{ trim_string('CUADNO')}} AS CUADNO,
        {{ trim_string('CUADNM')}} AS CUADNM,
        {{ trim_string('CUTYPE')}} AS CUTYPE,
        {{ trim_string('CUDIR')}} AS CUDIR,
        {{ trim_string('CUSUIT')}} AS CUSUIT,
        {{ trim_string('CUAQSN')}} AS CUAQSN,
        {{ trim_string('CUINGP')}} AS CUINGP,
        {{ trim_string('CULGCD')}} AS CULGCD,
        {{ trim_string('CUSUSD')}} AS CUSUSD,
        {{ trim_string('CUSUSF')}} AS CUSUSF,
        {{ trim_string('CUSUSB')}} AS CUSUSB,
        CAST(CUSTSP AS NUMBER(10,0)) AS CUSTSP_ORIG,
        {{ DateCheck_140_YYYYMMDD('CUSTSP') }} AS CUSTSP,
        CAST(CUEDSP AS NUMBER(10,0)) AS CUEDSP_ORIG,
        {{ DateCheck_1940_HighDate('CUEDSP') }} AS CUEDSP,
        {{ trim_string('CUCROF')}} AS CUCROF,
        {{ trim_string('CUCTFG')}} AS CUCTFG,
        CAST(CUEXT AS NUMBER(5,0)) AS CUEXT,
        {{ trim_string('CUINDS')}} AS CUINDS,
        {{ trim_string('CUREMT')}} AS CUREMT,
        {{ trim_string('CUAMFG')}} AS CUAMFG,
        {{ trim_string('CULDLS')}} AS CULDLS,
        CAST(CUDAT0 AS NUMBER(10,0)) AS CUDAT0_ORIG,
        {{ DateCheck_140_YYYYMMDD('CUDAT0') }} AS CUDAT0,
        CAST(CUDAT1 AS NUMBER(10,0)) AS CUDAT1_ORIG,
        {{ DateCheck_140_YYYYMMDD('CUDAT1') }} AS CUDAT1,
        {{ trim_string('CUFLG0')}} AS CUFLG0,
        {{ trim_string('CUFLG1')}} AS CUFLG1,
        {{ trim_string('CUFLG2')}} AS CUFLG2,
        CAST(CUCNT0 AS NUMBER(10,0)) AS CUCNT0,
        CAST(CUCNT1 AS NUMBER(10,0)) AS CUCNT1,
        CAST(CUCNT2 AS NUMBER(10,0)) AS CUCNT2,
        CAST(CUCNT3 AS NUMBER(10,0)) AS CUCNT3,
        {{ trim_string('CUTXT0')}} AS CUTXT0,
        {{ trim_string('CUTXT1')}} AS CUTXT1,
        {{ trim_string('CUTXT2')}} AS CUTXT2,
        {{ trim_string('CUISOF')}} AS CUISOF,
        {{ trim_string('CUIPBF')}} AS CUIPBF,
        {{ trim_string('CUICSF')}} AS CUICSF,
        {{ trim_string('CUICTF')}} AS CUICTF,
        {{ trim_string('CUISSF')}} AS CUISSF,
        {{ trim_string('CUISTF')}} AS CUISTF,
        {{ trim_string('CUINV1')}} AS CUINV1,
        {{ trim_string('CUINV2')}} AS CUINV2,
        {{ trim_string('CUINV3')}} AS CUINV3,
        {{ trim_string('CUINV4')}} AS CUINV4,
        {{ trim_string('CUUSER')}} AS CUUSER,
        CAST(CUUDAT AS NUMBER(10,0)) AS CUUDAT,
        CAST(CUUTIM AS NUMBER(10,0)) AS CUUTIM,
        CAST(CONCAT({{ trim_string('CUCOMP') }},'-',{{ trim_string('CUACCT') }}) AS VARCHAR(50)) AS COMP_ACCT_KEY,
        CAST(SOURCE_SYSTEM AS VARCHAR(50)) AS SOURCE_SYSTEM,
        CAST('STG_IFP_BIPCU' AS VARCHAR(200)) AS SOURCE_TABLE,
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
        {{ generate_surrogate_key([trim_string('CUCOMP'),trim_string('CUACCT')]) }} as ODS_IFP_BIPCU_PK,
        CAST(CONCAT({{ trim_string('CUCOMP') }},'-',{{ trim_string('CUACCT') }}) AS VARCHAR(50)) AS ODS_IFP_BIPCU_COMPOSITE_KEY,
        CAST(NULL AS NUMBER) AS STG_IFP_BIPCU_PK,
        {{ trim_string('SYS_CDC_LIB') }} AS SRC_CDC_LIB,
        {{ trim_string('CUCOMP') }} AS CUCOMP,
        {{ trim_string('CUACCT') }} AS CUACCT,
        {{ trim_string('CUADNO') }} AS CUADNO,
        {{ trim_string('CUADNM') }} AS CUADNM,
        {{ trim_string('CUTYPE') }} AS CUTYPE,
        {{ trim_string('CUDIR') }} AS CUDIR,
        {{ trim_string('CUSUIT') }} AS CUSUIT,
        {{ trim_string('CUAQSN') }} AS CUAQSN,
        {{ trim_string('CUINGP') }} AS CUINGP,
        {{ trim_string('CULGCD') }} AS CULGCD,
        {{ trim_string('CUSUSD') }} AS CUSUSD,
        {{ trim_string('CUSUSF') }} AS CUSUSF,
        {{ trim_string('CUSUSB') }} AS CUSUSB,
        CAST(CUSTSP_ORIG AS NUMBER(10,0)) AS CUSTSP_ORIG,
        CAST(CUSTSP AS TIMESTAMP_NTZ(7)) AS CUSTSP,
        CAST(CUEDSP_ORIG AS NUMBER(10,0)) AS CUEDSP_ORIG,
        CAST(CUEDSP AS TIMESTAMP_NTZ(7)) AS CUEDSP,
        {{ trim_string('CUCROF') }} AS CUCROF,
        {{ trim_string('CUCTFG') }} AS CUCTFG,
        CAST(CUEXT AS NUMBER(5,0)) AS CUEXT,
        {{ trim_string('CUINDS') }} AS CUINDS,
        {{ trim_string('CUREMT') }} AS CUREMT,
        {{ trim_string('CUAMFG') }} AS CUAMFG,
        {{ trim_string('CULDLS') }} AS CULDLS,
        CAST(CUDAT0_ORIG AS NUMBER(10,0)) AS CUDAT0_ORIG,
        CAST(CUDAT0 AS TIMESTAMP_NTZ(7)) AS CUDAT0,
        CAST(CUDAT1_ORIG AS NUMBER(10,0)) AS CUDAT1_ORIG,
        CAST(CUDAT1 AS TIMESTAMP_NTZ(7)) AS CUDAT1,
        {{ trim_string('CUFLG0') }} AS CUFLG0,
        {{ trim_string('CUFLG1') }} AS CUFLG1,
        {{ trim_string('CUFLG2') }} AS CUFLG2,
        CAST(CUCNT0 AS NUMBER(10,0)) AS CUCNT0,
        CAST(CUCNT1 AS NUMBER(10,0)) AS CUCNT1,
        CAST(CUCNT2 AS NUMBER(10,0)) AS CUCNT2,
        CAST(CUCNT3 AS NUMBER(10,0)) AS CUCNT3,
        {{ trim_string('CUTXT0') }} AS CUTXT0,
        {{ trim_string('CUTXT1') }} AS CUTXT1,
        {{ trim_string('CUTXT2') }} AS CUTXT2,
        {{ trim_string('CUISOF') }} AS CUISOF,
        {{ trim_string('CUIPBF') }} AS CUIPBF,
        {{ trim_string('CUICSF') }} AS CUICSF,
        {{ trim_string('CUICTF') }} AS CUICTF,
        {{ trim_string('CUISSF') }} AS CUISSF,
        {{ trim_string('CUISTF') }} AS CUISTF,
        {{ trim_string('CUINV1') }} AS CUINV1,
        {{ trim_string('CUINV2') }} AS CUINV2,
        {{ trim_string('CUINV3') }} AS CUINV3,
        {{ trim_string('CUINV4') }} AS CUINV4,
        {{ trim_string('CUUSER') }} AS CUUSER,
        CAST(CUUDAT AS NUMBER(10,0)) AS CUUDAT,
        CAST(CUUTIM AS NUMBER(10,0)) AS CUUTIM,
        CAST(CONCAT({{ trim_string('CUCOMP') }},'-',{{ trim_string('CUACCT') }}) AS VARCHAR(50)) AS COMP_ACCT_KEY,
        CAST('Infopro' AS VARCHAR(50)) AS SOURCE_SYSTEM,
        CAST('ODS_IFP_BIPCU_HIST' AS VARCHAR(200)) AS SOURCE_TABLE,
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
FROM INFOPRO.HISTORY.ODS_IFP_BIPCU_HIST
CROSS JOIN CTE_BATCH_ID
)

{% endif %}

SELECT 
        *,
        {{ generate_hash_change( relation = this) }} AS HASH_CHANGE,
        {{ generate_hash_full( relation = this) }} AS HASH_FULL
FROM {% if is_incremental() %}CTE_TRANSFORMED{% else %}CTE_HISTORICAL{% endif %}






