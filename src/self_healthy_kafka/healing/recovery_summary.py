from __future__ import annotations


def format_healing_steps(
    step_counts: dict[str, int],
    *,
    final_connector_name: str,
) -> str:
    task_restarts = step_counts["TASK_RESTART"]
    connector_restarts = step_counts["CONNECTOR_RESTART"]
    recreate_with_offset = step_counts["CONNECTOR_RECREATE_WITH_OFFSET"]
    recreate_without_offset = step_counts["CONNECTOR_RECREATE_WITHOUT_OFFSET"]
    recreate_timeouts = step_counts["CONNECTOR_RECREATE_WITH_OFFSET_TIMEOUT"]

    attempts: list[str] = []
    if task_restarts:
        attempts.append(f"khởi động lại task {task_restarts} lần")
    if connector_restarts:
        attempts.append(f"khởi động lại connector {connector_restarts} lần")
    if recreate_with_offset:
        attempts.append(
            f"tạo lại connector với offset cũ {recreate_with_offset} lần"
        )
    if recreate_timeouts:
        attempts.append(
            f"gặp timeout khi tạo lại với offset {recreate_timeouts} lần"
        )

    if recreate_without_offset:
        prefix = f"Đã trải qua {', '.join(attempts)}; " if attempts else ""
        return (
            f"{prefix}cuối cùng tạo connector mới {final_connector_name} "
            "không sử dụng offset cũ và đã hoạt động ổn định."
        )
    if recreate_with_offset:
        prefix = f"Đã trải qua {', '.join(attempts[:-1])}; " if len(attempts) > 1 else ""
        return (
            f"{prefix}phục hồi bằng connector mới {final_connector_name} "
            "với offset cũ."
        )
    if connector_restarts:
        return (
            f"Đã {', '.join(attempts)}; "
            "hệ thống phục hồi tại bước khởi động lại connector."
        )
    if task_restarts:
        return (
            f"Đã khởi động lại task {task_restarts} lần; "
            "hệ thống phục hồi tại bước khởi động lại task."
        )
    return "NO_AUTOMATED_STEP"
