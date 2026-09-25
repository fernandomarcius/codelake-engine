{# Lists a relation's columns from the engine's metadata
   (`adapter.get_columns_in_relation`), widening smallint/tinyint to integer. #}
{% macro select_widened(relation) -%}
    {%- set cols = adapter.get_columns_in_relation(relation) -%}
    {%- for c in cols -%}
        {%- set t = c.dtype | lower -%}
        {%- if t in ('smallint', 'tinyint') -%}
            CAST({{ adapter.quote(c.name) }} AS integer) AS {{ adapter.quote(c.name) }}
        {%- else -%}
            {{ adapter.quote(c.name) }}
        {%- endif -%}
        {%- if not loop.last %},
        {% endif -%}
    {%- endfor -%}
{%- endmacro %}

{# The cycle's watermark as a varbinary literal: the harness passes the same
   hex value (`shadow_cutoff`) to both sides. #}
{% macro shadow_cutoff() -%}
X'{{ var("shadow_cutoff") }}'
{%- endmacro %}
