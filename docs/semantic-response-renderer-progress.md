# Semantic response renderer progress

Mục đích: lưu ngắn gọn bối cảnh phát sinh goal, flaw hoặc thiếu sót cần khắc phục, nguyên tắc thiết kế và kỹ thuật đã chọn. Không dùng file này làm changelog, báo cáo test, danh sách file sửa hay nơi lưu payload/SQL/log nhạy cảm.

Format cho mỗi goal: bối cảnh → rủi ro → hướng triển khai/kỹ thuật → giới hạn hoặc quyết định còn mở. Các entry mới được append vào cuối file.

## 2026-09-14 — Semantic response renderer tổng quát

- Bối cảnh: response composer dựng câu theo `intent`; câu hỏi hay intent mới có nguy cơ kéo theo template riêng và hardcode tăng dần.
- Rủi ro: không có evidence có thể bị diễn đạt thành không có dữ liệu/kết quả; wording phụ thuộc intent làm khó mở rộng và kiểm soát grounding.
- Hướng triển khai: tách `PresentationFacts` trung lập từ semantic plan, canonical facts và outcome đã xác minh; renderer chỉ đọc contract này.
- Kỹ thuật: semantic catalog khai báo label, condition, metric, time scope và source; dùng outcome taxonomy cùng invariant `verified_empty` trước mọi kết luận phủ định.
- Giới hạn: cần fixture có verified results đa dạng để tiếp tục đánh giá chất lượng diễn đạt ngoài trường hợp dữ liệu rỗng.

## 2026-09-14 — Tối giản metadata câu trả lời

- Bối cảnh: câu trả lời chính đang hiển thị source, time range, row count và request ID; đây là chi tiết kỹ thuật không cần thiết cho người dùng chat thông thường.
- Rủi ro: metadata làm câu trả lời khó đọc, che khuất kết luận đã grounded và khiến UI mang cảm giác công cụ nội bộ.
- Hướng triển khai: bỏ metadata kỹ thuật khỏi phần câu trả lời chính, giữ thời gian phản hồi end-to-end và disclosure kỹ thuật an toàn.
- Kỹ thuật: presentation-only change ở frontend; loại bỏ quick action yêu cầu diễn đạt lại nhưng không thay đổi contract/backend.

## 2026-09-14 — Enforcement entity và time scope cho ranking

- Bối cảnh: planner có thể đổi câu hỏi xếp hạng connector thành xếp hạng mã lỗi, rồi tự thêm phạm vi “hôm nay”; renderer vẫn grounded nhưng diễn đạt đúng một truy vấn sai nghĩa.
- Rủi ro: SQL hợp lệ và kết quả rỗng có thể tạo kết luận tự nhiên nhưng không liên quan yêu cầu ban đầu, làm che khuất sai lệch semantic.
- Hướng triển khai: mở rộng plan bằng subject, metric, ranking, time scope và nguồn gốc time scope; không có time scope nghĩa là toàn bộ snapshot.
- Kỹ thuật: catalog khai báo business vocabulary; cue validator độc lập đối chiếu entity/ranking/time và giới hạn top-N trước compiler, có correction giới hạn và chỉ cho phép kế thừa time scope đã xác minh.
- Giới hạn: vocabulary là guardrail cho ràng buộc rõ ràng, không thay thế evaluation corpus mở rộng hoặc hiểu ngữ nghĩa tự do của model.

## 2026-09-15 — Ranking connector gốc, đồng hạng và truy vấn đã chạy

- Bối cảnh: ranking đã dùng current connector và aggregate Python, nên connector recreate bị tách và câu SQL hiển thị không thể là bằng chứng thực thi.
- Rủi ro: top-N bỏ qua đồng hạng ở ngưỡng; grounding failure làm che khuất facts đã xác minh; log-grain có thể bị nhầm thành incident-grain.
- Hướng triển khai: semantic subject mặc định là root connector, T-SQL compiler allowlist chạy aggregate/rank trực tiếp, top-N dùng dense rank và chỉ cắt deterministic khi yêu cầu chính xác N.
- Kỹ thuật: parameter binding, read-only CTE guard tại repository, canonical rank/tie coverage và deterministic renderer khi lớp diễn đạt không grounded.
- Quyết định: chỉ hiển thị disclosure truy vấn khi backend đã chạy đúng query đó; log-grain vẫn là capability riêng, không suy diễn từ incident snapshot.
