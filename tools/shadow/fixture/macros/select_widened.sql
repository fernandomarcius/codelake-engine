{# Lists a relation's columns from the engine's metadata
   (`adapter.get_columns_in_relation`), widening smallint/tinyint to integer,
   as the acceptance pipelines do.

   With `shadow_workarounds` (default true), it also restates what the
   candidate's SQL Server source loses (docs/shadow-run.md, "Findings"):
   F8-1, a datetime/datetime2(3) column arrives as timestamp(6), which a
   MILLISECONDS hive catalog refuses; F8-2, a varchar(n) column arrives
   unbounded, so the source's declared types (`declared`: column -> type)
   are restated. Each CAST is a no-op on the reference engine. Run the
   self-test with SHADOW_SELFTEST_STRICT=1 to see the findings. #}
{% macro select_widened(relation, declared={}) -%}
    {%- set cols = adapter.get_columns_in_relation(relation) -%}
    {%- set workarounds = var('shadow_workarounds', true) -%}
    {%- for c in cols -%}
        {%- set t = c.dtype | lower -%}
        {%- if t in ('smallint', 'tinyint') -%}
            CAST({{ adapter.quote(c.name) }} AS integer) AS {{ adapter.quote(c.name) }}
        {%- elif workarounds and t.startswith('timestamp') -%}
            CAST({{ adapter.quote(c.name) }} AS timestamp(3)) AS {{ adapter.quote(c.name) }}
        {%- elif workarounds and (c.name | lower) in declared -%}
            CAST({{ adapter.quote(c.name) }} AS {{ declared[c.name | lower] }}) AS {{ adapter.quote(c.name) }}
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

{# An ORDER BY key for a varbinary column. With `shadow_workarounds`, its hex
   text, which orders the same for values of one length: finding F8-3, the
   candidate refuses a window ORDER BY on varbinary ("Cannot run range queries
   on datatype: Binary"), which the acceptance pipelines use on __$seqval. #}
{% macro binary_order_key(expr) -%}
{%- if var('shadow_workarounds', true) -%}to_hex({{ expr }}){%- else -%}{{ expr }}{%- endif -%}
{%- endmacro %}

{# A SQL Server datetime column by name; with `shadow_workarounds`, restated
   as timestamp(3) (finding F8-1). #}
{% macro timestamp_column(name) -%}
{%- if var('shadow_workarounds', true) -%}CAST({{ name }} AS timestamp(3)) AS {{ name }}{%- else -%}{{ name }}{%- endif -%}
{%- endmacro %}
