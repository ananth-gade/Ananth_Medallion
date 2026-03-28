{% macro default__get_delete_insert_sql(target, source, unique_key, dest_columns, predicates) -%}
   {% if not unique_key %}
  
        insert into  {{ target }} 
          ({{ dest_cols_csv }})
        (Select  ({{ dest_cols_csv }}) from   {{ source }})
       
    {% endif %}

{% endmacro %}