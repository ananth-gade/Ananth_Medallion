{% macro mark_deleted_records(
    ods_database,
    ods_schema,
    ods_table,
    stg_database,
    stg_schema,
    stg_table,
    pk_column,
    timestamp_column='STG_INSERT_TIMESTAMP'
) %}

    /*
      Marks records as deleted in the ODS table when they no longer exist in the latest staging data.

      Parameters:
        ods_database: Target ODS database name
        ods_schema:   Target ODS schema name
        ods_table:    Target ODS table name
        stg_database: Source staging database name
        stg_schema:   Source staging schema name
        stg_table:    Source staging table name
        pk_column:    Primary key column name for comparison (string) OR list of columns for composite PK
        timestamp_column: CDC timestamp column (default: STG_INSERT_TIMESTAMP)

      Example usage:
        dbt run-operation mark_deleted_records --args '{ods_database: WORKDAY, ods_schema: ODS, ods_table: ODS_WORKDAY_EMPLOYEE, stg_database: WORKDAY, stg_schema: STAGING, stg_table: STG_WORKDAY_EMPLOYEE, pk_column: EMPLOYEE_ID}'
        dbt run-operation mark_deleted_records --args '{ods_database: TRUX, ods_schema: ODS, ods_table: ODS_TRUX_USERCOMP, stg_database: TRUX, stg_schema: STAGING, stg_table: STG_TRUX_USERCOMP, pk_column: ["USERCOMP_USER","USERCOMP_COMPANY"]}'
    */

    {% if execute %}
      {{ log("Marking deleted records in " ~ ods_database ~ "." ~ ods_schema ~ "." ~ ods_table, info=True) }}

      /* -------------------- FIRST-RUN GUARD --------------------
         If the ODS table is not yet visible (typical on the very first run),
         skip gracefully so the post-hook doesn't fail. */
      {% set ods_relation = adapter.get_relation(
          database=ods_database,
          schema=ods_schema,
          identifier=ods_table
      ) %}

      {% if ods_relation is none %}
        {{ log("mark_deleted_records: ODS table not found/visible yet — skipping delete marking on this run.", info=True) }}
        {{ return('') }}
      {% endif %}

      /* Normalize pk_column to a list so composite PKs are supported */
      {% if pk_column is string %}
        {% set pk_list = [pk_column] %}
      {% else %}
        {% set pk_list = pk_column %}
      {% endif %}

      /* Resolve ODS column metadata (safe now that relation exists) */
      {% set ods_columns = adapter.get_columns_in_relation(ods_relation) %}

      /* Build predicate: TRIM only if the ODS column is string-typed */
      {% set pk_conditions = [] %}
      {% for pk in pk_list %}
        {% set col = (ods_columns | selectattr('name','equalto', pk) | list | first)
                    or (ods_columns | selectattr('name','equalto', pk|upper) | list | first)
                    or (ods_columns | selectattr('name','equalto', pk|lower) | list | first) %}
        {% if not col %}
          {% do pk_conditions.append("s." ~ pk ~ " = o." ~ pk) %}
        {% else %}
          {% set dtype = col.data_type | upper %}
          {% if 'CHAR' in dtype or 'TEXT' in dtype or 'STRING' in dtype or 'VARCHAR' in dtype %}
            {% do pk_conditions.append("TRIM(s." ~ pk ~ ") = TRIM(o." ~ pk ~ ")") %}
          {% else %}
            {% do pk_conditions.append("s." ~ pk ~ " = o." ~ pk) %}
          {% endif %}
        {% endif %}
      {% endfor %}
      {% set pk_predicate = pk_conditions | join(' AND ') %}

      /* Perform the delete-marking update using the latest staging snapshot */
      {% set update_query %}
      UPDATE {{ ods_database }}.{{ ods_schema }}.{{ ods_table }} AS o
      SET OPERATION_TYPE       = 'DELETE',
          SRC_DEL_IND          = 'Y',
          SRC_ACTION_CD        = 'D',
          ODS_UPDATE_TIMESTAMP = latest.max_ts,
          ODS_UPD_BATCH_ID     = latest.max_batch
      FROM (
          SELECT
              MAX(ODS_UPD_BATCH_ID)     AS max_batch,
              MAX(ODS_UPDATE_TIMESTAMP) AS max_ts
          FROM {{ ods_database }}.{{ ods_schema }}.{{ ods_table }}
      ) AS latest
      WHERE NOT EXISTS (
          SELECT 1
          FROM {{ stg_database }}.{{ stg_schema }}.{{ stg_table }} s
          WHERE {{ pk_predicate }}
            AND s.{{ timestamp_column }} = (
                SELECT MAX({{ timestamp_column }})
                FROM {{ stg_database }}.{{ stg_schema }}.{{ stg_table }}
            )
      )
      AND o.OPERATION_TYPE <> 'DELETE';
      {% endset %}

      {% do run_query(update_query) %}
      {{ log("Marking deleted records completed", info=True) }}
    {% endif %}

    {{ return('') }}
{% endmacro %}