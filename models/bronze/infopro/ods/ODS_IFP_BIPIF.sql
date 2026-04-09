{{
    config(
        materialized='incremental',
        unique_key='ODS_IFP_BIPIF_PK',
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
                 WHERE ODS_IFP_BIPIF_PK IN (
                     SELECT {{ generate_surrogate_key([trim_string('DTL__BI_IFCOMP'),trim_string('DTL__BI_IFACCT')]) }}
                     FROM {{ source('STAGING','STG_IFP_BIPIF') }}
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
        SELECT * FROM {{ source('STAGING','STG_IFP_BIPIF') }}
        where
        -- this filter will only be applied on an incremental run
        to_timestamp_tz(SUBSTRING(TRIM(DTL__CAPXTIMESTAMP),1,17),'YYYYMMDDHHMISSFF')  >= '{{ get_max_event_time('SRC_LAST_UPDATE_DTM') }}'
)

,CTE_DEDUP as (
  select *, ROW_NUMBER() OVER(PARTITION BY IFCOMP,IFACCT ORDER BY DTL__CAPXTIMESTAMP DESC) AS ROW_NUM
  from CTE_SOURCE_RECORDS
  qualify ROW_NUM=1
)

,CTE_TEMP as (
select CTE_SOURCE_RECORDS.DTL__BI_IFCOMP, CTE_SOURCE_RECORDS.DTL__BI_IFACCT from 
CTE_SOURCE_RECORDS LEFT JOIN CTE_DEDUP 
 ON CTE_SOURCE_RECORDS.DTL__BI_IFCOMP=CTE_DEDUP.IFCOMP
 AND CTE_SOURCE_RECORDS.DTL__BI_IFACCT=CTE_DEDUP.IFACCT
 where CTE_SOURCE_RECORDS.PRIMARY_KEY_CHANGE  = 'Y'
AND (TO_NUMBER(CTE_SOURCE_RECORDS.PK_LZ_IFP_BIPIF_ID) >=  TO_NUMBER(CTE_DEDUP.PK_LZ_IFP_BIPIF_ID) OR TO_NUMBER(CTE_DEDUP.PK_LZ_IFP_BIPIF_ID) IS NULL)
)

,CTE_FINAL as (
select * from CTE_DEDUP where CONCAT_WS(',',IFCOMP,IFACCT) in (
select CONCAT_WS(',',IFCOMP,IFACCT) from CTE_DEDUP
except 
select CONCAT_WS(',',DTL__BI_IFCOMP,DTL__BI_IFACCT) from CTE_TEMP)
)

,--> Current Delta data capture from STG table
CTE_TRANSFORMED AS (
SELECT
        {{ generate_surrogate_key([trim_string('IFCOMP'),trim_string('IFACCT')])}} as ODS_IFP_BIPIF_PK ,
        CAST(CONCAT({{ trim_string('IFCOMP')}},'-',{{ trim_string('IFACCT')}}) AS VARCHAR(50)) AS ODS_IFP_BIPIF_COMPOSITE_KEY,
        STG_IFP_BIPIF_PK,
        {{ trim_string('DTL__CAPXUSER') }} AS SRC_CDC_LIB,
        {{ trim_string('IFCOMP')}} AS IFCOMP,
        {{ trim_string('IFACCT')}} AS IFACCT,
        {{ trim_string('IFRVCD')}} AS IFRVCD,
        {{ trim_string('IFIUSE')}} AS IFIUSE,
        {{ trim_string('IFIFEE')}} AS IFIFEE,
        {{ trim_string('IFIEXC')}} AS IFIEXC,
        CAST(IFICAM AS NUMBER(9,2)) AS IFICAM,
        {{ trim_string('IFSUSE')}} AS IFSUSE,
        {{ trim_string('IFSFEE')}} AS IFSFEE,
        {{ trim_string('IFSEXC')}} AS IFSEXC,
        CAST(IFSCAM AS NUMBER(9,2)) AS IFSCAM,
        {{ trim_string('IFLUSE')}} AS IFLUSE,
        {{ trim_string('IFLFEE')}} AS IFLFEE,
        {{ trim_string('IFLEXC')}} AS IFLEXC,
        {{ trim_string('IFLFPC')}} AS IFLFPC,
        CAST(IFDAT1 AS NUMBER(10,0)) AS IFDAT1_ORIG,
        {{ DateCheck_140_YYYYMMDD('IFDAT1') }} AS IFDAT1,
        CAST(IFDAT2 AS NUMBER(10,0)) AS IFDAT2_ORIG,
        {{ DateCheck_140_YYYYMMDD_HighDate('IFDAT2') }} AS IFDAT2,
        CAST(IFDAT3 AS NUMBER(10,0)) AS IFDAT3_ORIG,
        {{ DateCheck_140_YYYYMMDD_HighDate('IFDAT3') }} AS IFDAT3,
        {{ trim_string('IFFLG1')}} AS IFFLG1,
        {{ trim_string('IFFLG2')}} AS IFFLG2,
        {{ trim_string('IFFEXC')}} AS IFFEXC,
        CAST(IFOFDT AS NUMBER(10,0)) AS IFOFDT_ORIG,
        {{ DateCheck_140_YYYYMMDD_HighDate('IFOFDT') }} AS IFOFDT,
        {{ trim_string('IFEUSE')}} AS IFEUSE,
        {{ trim_string('IFEFEE')}} AS IFEFEE,
        {{ trim_string('IFEEXC')}} AS IFEEXC,
        CAST(IFOEDT AS NUMBER(10,0)) AS IFOEDT_ORIG,
        {{ DateCheck_140_YYYYMMDD_HighDate('IFOEDT') }} AS IFOEDT,
        {{ trim_string('IFAUSE')}} AS IFAUSE,
        {{ trim_string('IFAFEE')}} AS IFAFEE,
        {{ trim_string('IFAEXC')}} AS IFAEXC,
        CAST(IFOADT AS NUMBER(10,0)) AS IFOADT_ORIG,
        {{ DateCheck_140_YYYYMMDD_HighDate('IFOADT') }} AS IFOADT,
        {{ trim_string('IFFLG3')}} AS IFFLG3,
        {{ trim_string('IFFLG4')}} AS IFFLG4,
        {{ trim_string('IFXTRA')}} AS IFXTRA,
        CAST(IFXTRN AS NUMBER(9,2)) AS IFXTRN,
        {{ trim_string('IFBROK')}} AS IFBROK,
        {{ trim_string('IFUSER')}} AS IFUSER,
        CAST(IFUDAT AS NUMBER(10,0)) AS IFUDAT_ORIG,
        {{ DateCheck_140_YYYYMMDD('IFUDAT') }} AS IFUDAT,
        CAST(IFUTIM AS NUMBER(10,0)) AS IFUTIM,
        CAST(SOURCE_SYSTEM AS VARCHAR(50)) AS SOURCE_SYSTEM,
        CAST('STG_IFP_BIPIF' AS VARCHAR(200)) AS SOURCE_TABLE,
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
        {{ generate_surrogate_key([trim_string('IFCOMP'),trim_string('IFACCT')]) }} as ODS_IFP_BIPIF_PK,
        CAST(CONCAT({{ trim_string('IFCOMP') }},'-',{{ trim_string('IFACCT') }}) AS VARCHAR(50)) AS ODS_IFP_BIPIF_COMPOSITE_KEY,
        CAST(NULL AS NUMBER) AS STG_IFP_BIPIF_PK,
        {{ trim_string('SYS_CDC_LIB') }} AS SRC_CDC_LIB,
        {{ trim_string('IFCOMP') }} AS IFCOMP,
        {{ trim_string('IFACCT') }} AS IFACCT,
        {{ trim_string('IFRVCD') }} AS IFRVCD,
        {{ trim_string('IFIUSE') }} AS IFIUSE,
        {{ trim_string('IFIFEE') }} AS IFIFEE,
        {{ trim_string('IFIEXC') }} AS IFIEXC,
        CAST(IFICAM AS NUMBER(9,2)) AS IFICAM,
        {{ trim_string('IFSUSE') }} AS IFSUSE,
        {{ trim_string('IFSFEE') }} AS IFSFEE,
        {{ trim_string('IFSEXC') }} AS IFSEXC,
        CAST(IFSCAM AS NUMBER(9,2)) AS IFSCAM,
        {{ trim_string('IFLUSE') }} AS IFLUSE,
        {{ trim_string('IFLFEE') }} AS IFLFEE,
        {{ trim_string('IFLEXC') }} AS IFLEXC,
        {{ trim_string('IFLFPC') }} AS IFLFPC,
        CAST(IFDAT1_ORIG AS NUMBER(10,0)) AS IFDAT1_ORIG,
        CAST(IFDAT1 AS TIMESTAMP_NTZ(7)) AS IFDAT1,
        CAST(IFDAT2_ORIG AS NUMBER(10,0)) AS IFDAT2_ORIG,
        CAST(IFDAT2 AS TIMESTAMP_NTZ(7)) AS IFDAT2,
        CAST(IFDAT3_ORIG AS NUMBER(10,0)) AS IFDAT3_ORIG,
        CAST(IFDAT3 AS TIMESTAMP_NTZ(7)) AS IFDAT3,
        {{ trim_string('IFFLG1') }} AS IFFLG1,
        {{ trim_string('IFFLG2') }} AS IFFLG2,
        {{ trim_string('IFFEXC') }} AS IFFEXC,
        CAST(IFOFDT_ORIG AS NUMBER(10,0)) AS IFOFDT_ORIG,
        CAST(IFOFDT AS TIMESTAMP_NTZ(7)) AS IFOFDT,
        {{ trim_string('IFEUSE') }} AS IFEUSE,
        {{ trim_string('IFEFEE') }} AS IFEFEE,
        {{ trim_string('IFEEXC') }} AS IFEEXC,
        CAST(IFOEDT_ORIG AS NUMBER(10,0)) AS IFOEDT_ORIG,
        CAST(IFOEDT AS TIMESTAMP_NTZ(7)) AS IFOEDT,
        {{ trim_string('IFAUSE') }} AS IFAUSE,
        {{ trim_string('IFAFEE') }} AS IFAFEE,
        {{ trim_string('IFAEXC') }} AS IFAEXC,
        CAST(IFOADT_ORIG AS NUMBER(10,0)) AS IFOADT_ORIG,
        CAST(IFOADT AS TIMESTAMP_NTZ(7)) AS IFOADT,
        {{ trim_string('IFFLG3') }} AS IFFLG3,
        {{ trim_string('IFFLG4') }} AS IFFLG4,
        {{ trim_string('IFXTRA') }} AS IFXTRA,
        CAST(IFXTRN AS NUMBER(9,2)) AS IFXTRN,
        {{ trim_string('IFBROK') }} AS IFBROK,
        {{ trim_string('IFUSER') }} AS IFUSER,
        CAST(IFUDAT_ORIG AS NUMBER(10,0)) AS IFUDAT_ORIG,
        CAST(IFUDAT AS TIMESTAMP_NTZ(7)) AS IFUDAT,
        CAST(IFUTIM AS NUMBER(10,0)) AS IFUTIM,
        CAST('Infopro' AS VARCHAR(50)) AS SOURCE_SYSTEM,
        CAST('ODS_IFP_BIPIF_HIST' AS VARCHAR(200)) AS SOURCE_TABLE,
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
FROM INFOPRO.HISTORY.ODS_IFP_BIPIF_HIST
CROSS JOIN CTE_BATCH_ID
)

{% endif %}

SELECT
*,
{{ generate_hash_change( relation = this) }} AS HASH_CHANGE,
{{ generate_hash_full( relation = this) }} AS HASH_FULL
FROM {% if is_incremental() %}CTE_TRANSFORMED{% else %}CTE_HISTORICAL{% endif %}






