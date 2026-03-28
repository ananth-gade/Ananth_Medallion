{%- macro generate_hash_full(relation) -%}

    {# 1. Get all columns from the model YAML metadata #}
    {%- set all_cols = model.columns.keys() | list -%}

    {# 2. Read audit columns from YAML metadata #}
    {%- set yaml_audit_columns = [] -%}

    {%- for col in model.columns.values() -%}
        {%- if col.meta.get('audit_column') -%}
            {%- do yaml_audit_columns.append(col.name) -%}
        {%- endif -%}
    {%- endfor -%}

    {# 3. Combine all exclusions #}
    {%- set excluded_cols = yaml_audit_columns-%}

    {# 4. Remove exclusions that do NOT exist in the actual relation #}
    {%- set filtered_excluded_cols = [] -%}
    {%- for col in excluded_cols -%}
        {%- if col in all_cols -%}
            {%- do filtered_excluded_cols.append(col) -%}
        {%- endif -%}
    {%- endfor -%}

    {# 5. Compute non-key, non-audit, non-excluded columns #}
    {%- set non_key_cols = [] -%}
    {%- for col in all_cols -%}
        {%- if col not in filtered_excluded_cols -%}
            {%- do non_key_cols.append(col) -%}
        {%- endif -%}
    {%- endfor -%}

    {# 6. Build concatenation expression manually #}
    {%- set concat_list = [] -%}
    {%- for col in non_key_cols -%}
        {%- do concat_list.append("coalesce(cast(" ~ col ~ " as varchar), '')") -%}
    {%- endfor -%}

    {%- set concat_expr = concat_list | join(" || '|' || ") -%}

    {# 7. Return fixed-length MD5 hash (single-line, whitespace trimmed) #}
    {{- "cast(md5(" ~ concat_expr ~ ") as varchar(250))" -}}

{%- endmacro -%}
