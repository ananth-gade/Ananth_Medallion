{%- macro generate_hash_scd2() -%}
{#
    Generates an MD5 hash of all columns tagged with meta.scd2_column: true
    in the model's YAML schema. Used for SCD Type 2 change detection —
    a change in this hash triggers a new historical row.

    Usage in model SQL:
        {{ generate_hash_scd2() }} as HASH_SCD2

    Requires YAML columns to have:
        meta:
          scd2_column: true
#}

    {# 1. Collect columns tagged as scd2_column from YAML metadata #}
    {%- set scd2_cols = [] -%}
    {%- for col in model.columns.values() -%}
        {%- if col.meta.get('scd2_column') -%}
            {%- do scd2_cols.append(col.name) -%}
        {%- endif -%}
    {%- endfor -%}

    {# 2. Guard: if no columns tagged, return a constant hash #}
    {%- if scd2_cols | length == 0 -%}
        {{- "cast(md5('NO_SCD2_COLUMNS_TAGGED') as varchar(32))" -}}
    {%- else -%}

        {# 3. Build concat expression with coalesce + cast for each column #}
        {%- set concat_list = [] -%}
        {%- for col in scd2_cols -%}
            {%- do concat_list.append("coalesce(cast(" ~ col ~ " as varchar), '')") -%}
        {%- endfor -%}

        {%- set concat_expr = concat_list | join(" || '||' || ") -%}

        {# 4. Return MD5 hash expression #}
        {{- "cast(md5(" ~ concat_expr ~ ") as varchar(32))" -}}

    {%- endif -%}

{%- endmacro -%}
