{% macro get_status_label(status_column) %}
    case
        when {{ status_column }} = 1 then 'HOẠT ĐỘNG'
        when {{ status_column }} = 0 then 'TẮT'
        else 'KHÔNG XÁC ĐỊNH'
    end
{% endmacro %}