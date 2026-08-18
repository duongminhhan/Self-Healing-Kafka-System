from __future__ import annotations


class Phase:
    """
    Các phase mô tả trạng thái hiện tại của connector trong pipeline healing.

    Phần lớn phase được suy ra từ log healing mới nhất cộng với các bộ đếm lỗi
    trong bảng connectors. Phase không phải là raw status trả về từ Kafka
    Connect, mà là vị trí của connector trong quy trình tự phục hồi.
    """

    # Khi `failed_count` = 0. Connector được xem là khỏe, không có incident đang
    # mở, poller chỉ tiếp tục kiểm tra trạng thái định kỳ.
    HEALTHY = "HEALTHY"

    # Khi chương trình mới khởi động hoặc poll loop chưa có đủ dữ liệu status để
    # quyết định có cần bắt đầu healing hay không.
    INITIAL_WAITING = "INITIAL_WAITING"

    # Khi pre-flight check hoặc poll check không gọi được Kafka Connect REST API.
    # Đây là lỗi hạ tầng/kết nối tới Kafka Connect, không phải lỗi connector.
    KC_UNREACHABLE = "KC_UNREACHABLE"

    # Khi phát hiện connector/task unhealthy nhưng chưa đủ số lần confirm lỗi.
    # Hệ thống tiếp tục poll thêm để các lỗi tạm thời như DB/network timeout có
    # cơ hội tự phục hồi trước khi restart.
    FAILED_DEBOUNCE = "FAILED_DEBOUNCE"

    # Khi lỗi đã được confirm và hành động healing hiện tại là restart riêng các
    # task đang failed do Kafka Connect báo về.
    TASK_RESTARTING = "TASK_RESTARTING"

    # Khi restart task không phục hồi được connector và hệ thống escalate lên
    # restart toàn bộ connector.
    CONNECTOR_RESTARTING = "CONNECTOR_RESTARTING"

    # Khi hệ thống recreate connector với tên mới, giữ nguyên schema history
    # topic cũ, patch offset/SCN đã capture, rồi resume connector.
    RECREATE_WITH_OFFSET_VERIFYING = "RECREATE_WITH_OFFSET_VERIFYING"

    # Khi bước recreate-with-offset bị timeout hoặc failed. Hệ thống có thể retry
    # ngắn bước này, sau đó escalate sang recreate without offset nếu level cho phép.
    RECREATE_WITH_OFFSET_FAILED = "RECREATE_WITH_OFFSET_FAILED"

    # Khi hệ thống recreate connector với tên mới và schema history topic mới,
    # nhưng không patch offset/SCN nguồn.
    RECREATE_WITHOUT_OFFSET_VERIFYING = "RECREATE_WITHOUT_OFFSET_VERIFYING"

    # Khi recreate-without-offset failed. Sau bước này không còn action tự động
    # tiếp theo, incident sẽ chuyển sang manual escalation.
    RECREATE_WITHOUT_OFFSET_FAILED = "RECREATE_WITHOUT_OFFSET_FAILED"

    # Khi automated healing dừng vì connector chạm giới hạn level được cấu hình
    # hoặc toàn bộ action recovery được phép đều đã failed.
    ESCALATED = "ESCALATED"


class EventType:
    """
    Tên event ổn định được lưu trong bảng connector_healing_logs.

    Các giá trị này cũng được dùng bởi alert rule/Grafana/Loki, nên chỉ đổi khi
    đã cập nhật toàn bộ query và notification policy liên quan.
    """

    # Ghi khi connector/task đã unhealthy đủ số lần confirm. Đây là alert cảnh
    # báo đầu tiên rằng hệ thống bắt đầu quy trình healing.
    HEALTH_FAILED_CONFIRMED = "HEALTH_FAILED_CONFIRMED"

    # Ghi mỗi lần hệ thống gọi Kafka Connect restart với
    # `includeTasks=true&onlyFailed=true` để restart riêng failed tasks.
    TASK_RESTART = "TASK_RESTART"

    # Ghi khi hệ thống restart toàn bộ connector sau khi task restart không đủ
    # để phục hồi hoặc lỗi nằm ở connector level.
    CONNECTOR_RESTART = "CONNECTOR_RESTART"

    # Ghi trước bước recreate-with-offset, sau khi lấy được offsets từ Kafka
    # Connect để extract last_scn/last_commit_scn.
    OFFSET_CAPTURED = "OFFSET_CAPTURED"

    # Ghi khi không capture được offsets hợp lệ, nên không thể recreate an toàn
    # tại vị trí offset/SCN cũ.
    OFFSET_CAPTURE_FAILED = "OFFSET_CAPTURE_FAILED"

    # Ghi khi đã tạo replacement connector ở trạng thái STOPPED nhưng patch
    # offsets vào connector mới thất bại.
    OFFSET_PATCH_FAILED = "OFFSET_PATCH_FAILED"

    # Ghi khi patch offset thành công nhưng resume replacement connector thất bại.
    CONNECTOR_RESUME_FAILED = "CONNECTOR_RESUME_FAILED"

    # Ghi khi recreate-with-offset thành công: connector mới được tạo, offset đã
    # được patch, connector đã resume, và hệ thống bắt đầu verify.
    CONNECTOR_RECREATE_WITH_OFFSET = "CONNECTOR_RECREATE_WITH_OFFSET"

    # Ghi khi request tạo replacement connector bị timeout. Code sẽ chờ thêm rồi
    # retry bước create/patch/resume trước khi escalate.
    CONNECTOR_RECREATE_WITH_OFFSET_TIMEOUT = "CONNECTOR_RECREATE_WITH_OFFSET_TIMEOUT"

    # Ghi khi recreate-with-offset thất bại thật sự, ví dụ không có offsets, lỗi
    # tạo connector, lỗi patch offset hoặc lỗi resume.
    CONNECTOR_RECREATE_WITH_OFFSET_FAILED = "CONNECTOR_RECREATE_WITH_OFFSET_FAILED"

    # Ghi khi hệ thống recreate connector mới hoàn toàn, dùng schema history topic
    # mới và không restore offset/SCN.
    CONNECTOR_RECREATE_WITHOUT_OFFSET = "CONNECTOR_RECREATE_WITHOUT_OFFSET"

    # Ghi khi recreate-without-offset thất bại. Đây thường là bước cuối trước khi
    # yêu cầu cấu hình/xử lý thủ công.
    CONNECTOR_RECREATE_WITHOUT_OFFSET_FAILED = "CONNECTOR_RECREATE_WITHOUT_OFFSET_FAILED"

    # Ghi khi toàn bộ action tự động đã cạn hoặc bước cuối vẫn failed; connector
    # cần manual configuration.
    HEALING_ESCALATED = "HEALING_ESCALATED"

    # Ghi khi connector không được phép đi tới step tiếp theo vì field `level`
    # thấp hơn required_level. Code cũng tắt `is_active` để tránh loop healing.
    HEALING_LEVEL_LIMIT_REACHED = "HEALING_LEVEL_LIMIT_REACHED"

    # Ghi khi connector đã phục hồi sau một hoặc nhiều bước healing. Đây là event
    # success duy nhất cho healing, có kèm original connector và healing steps.
    HEALING_RECOVERED = "HEALING_RECOVERED"

    # Ghi khi chính state machine gặp exception trong lúc xử lý một connector.
    # Event này dùng để debug lỗi code/wiring thay vì lỗi Kafka Connect.
    STATE_MACHINE_ERROR = "STATE_MACHINE_ERROR"
