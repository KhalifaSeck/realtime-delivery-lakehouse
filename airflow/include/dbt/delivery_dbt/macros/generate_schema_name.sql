{#
  Macro : generate_schema_name
  
  Override du comportement par défaut de dbt qui préfixe le schema
  du profile au schema du modèle (ex: STAGING_MARTS au lieu de MARTS).
  
  Comportement :
    - Si le modèle spécifie un schema custom (via +schema: dans dbt_project.yml
      ou config bloc dans le SQL), on utilise CE schema tel quel.
    - Sinon, on utilise le schema du profile (fallback).
  
  Résultat : DELIVERY_LAKEHOUSE.STAGING (pas STAGING_STAGING),
             DELIVERY_LAKEHOUSE.INTERMEDIATE, DELIVERY_LAKEHOUSE.MARTS.
#}

{% macro generate_schema_name(custom_schema_name, node) -%}

    {%- set default_schema = target.schema -%}

    {%- if custom_schema_name is none -%}
        {{ default_schema }}
    {%- else -%}
        {{ custom_schema_name | trim | upper }}
    {%- endif -%}

{%- endmacro %}