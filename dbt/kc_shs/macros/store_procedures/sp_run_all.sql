{% macro run_all_sps() %}

    {% do log("=== BẮT ĐẦU TẠO/CẬP NHẬT TẤT CẢ STORED PROCEDURES ===", info=True) %}

    {# Các SP độc lập và SP được spGetOperationalMetrics sử dụng. #}
    {% do create_sp_get_connector_context() %}
    {% do create_sp_get_healing_attempt_metrics() %}
    {% do create_sp_get_healing_recovered_metrics() %}
    {% do create_sp_get_healing_escalated_metrics() %}
    {% do create_sp_get_monitored_topics() %}
    {% do create_sp_get_topic_lag_state_metrics() %}
    {% do create_sp_get_topic_lag_event_metrics() %}

    {# Các SP ghi/cập nhật trạng thái nghiệp vụ. #}
    {% do create_sp_insert_connector_healing_log() %}
    {% do create_sp_insert_topic_lag_log() %}
    {% do create_sp_update_connector() %}
    {% do create_sp_update_topic_lag_state() %}

    {# SP tổng hợp được tạo cuối cùng sau toàn bộ SP metric thành phần. #}
    {% do create_sp_get_operational_metrics() %}

    {% do log("=== ĐÃ TẠO/CẬP NHẬT XONG TẤT CẢ STORED PROCEDURES ===", info=True) %}

{% endmacro %}
