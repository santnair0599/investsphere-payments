{#-
  Use the model's custom +schema name AS-IS (gold, gold_staging, gold_marts),
  instead of dbt's default `<target.schema>_<custom>` which produced gold_gold.
  This makes dbt write to the same Unity Catalog schemas the rest of the platform
  (governance masks, BI serving views, Terraform) expects.
-#}
{% macro generate_schema_name(custom_schema_name, node) -%}
    {%- if custom_schema_name is none -%}
        {{ target.schema }}
    {%- else -%}
        {{ custom_schema_name | trim }}
    {%- endif -%}
{%- endmacro %}
