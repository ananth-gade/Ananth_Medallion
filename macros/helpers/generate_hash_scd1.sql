{%- macro generate_hash_scd1() -%}
{#
    Generates an MD5 hash of all columns tagged with meta.scd1_column: true
    in the model's YAML schema. Used for SCD Type 1 change detection —
    a change in this hash triggers an in-place overwrite (no new row).

    Usage in model SQL:
        {{ generate_hash_scd1() }} as HASH_SCD1

    Requires YAML columns to have:
        meta:
          scd1_column: true
#}

    {# 1. Collect columns tagged as scd1_column from YAML metadata #}
    {%- set scd1_cols = [] -%}
    {%- for col in model.columns.values() -%}
        {%- if col.meta.get('scd1_column') -%}
            {%- do scd1_cols.append(col.name) -%}
        {%- endif -%}
    {%- endfor -%}

    {# 2. Guard: if no columns tagged, return a constant hash #}
    {%- if scd1_cols | length == 0 -%}
        {{- "cast(md5('NO_SCD1_COLUMNS_TAGGED') as varchar(32))" -}}
    {%- else -%}

        {# 3. Build concat expression with coalesce + cast for each column #}
        {%- set concat_list = [] -%}
        {%- for col in scd1_cols -%}
            {%- do concat_list.append("coalesce(cast(" ~ col ~ " as varchar), '')") -%}
        {%- endfor -%}

        {%- set concat_expr = concat_list | join(" || '||' || ") -%}

        {# 4. Return MD5 hash expression #}
        {{- "cast(md5(" ~ concat_expr ~ ") as varchar(32))" -}}

    {%- endif -%}

{%- endmacro -%}
