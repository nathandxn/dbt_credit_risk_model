{% macro prefix_column_name(column_name, prefix, alias = none) %}

    {%if alias %}{{ alias }}.{% endif %}{{ column_name }} as {{ prefix }}_{{ column_name }}

{% endmacro %}