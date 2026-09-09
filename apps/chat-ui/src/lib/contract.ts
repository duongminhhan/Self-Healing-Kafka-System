import { z } from "zod";

export const questionSchema = z.object({ question: z.string().trim().min(1).max(4000) }).strict();
const label = z.string().max(240);
export const citationSchema = z.object({
  runbook_id: label.optional(), version: z.number().optional(), section: label.optional(),
  source: z.string().max(2000).optional(), url: z.string().max(2000).optional(), title: label.optional(),
});
const cell = z.union([z.string().max(8000), z.number().finite(), z.boolean(), z.null()]);
const rows = z.array(z.record(z.string().max(160), cell)).max(500);
const evidenceSchema = citationSchema.extend({
  match_basis: label.optional(), matched_error_codes: z.array(label).max(20).optional(),
  matched_config_keys: z.array(label).max(20).optional(), matched_exception_classes: z.array(label).max(20).optional(),
  matched_error_signatures: z.array(label).max(20).optional(),
});
export const backendSchema = z.object({
  answer: z.string().max(150000).nullish(), route: label.nullish(), source: label.nullish(),
  citations: z.array(citationSchema).max(100).nullish(), fallback_reason: label.nullish(),
  status: label.nullish(), reason: label.nullish(), row_count: z.number().int().nonnegative().nullish(),
  evidence: z.array(evidenceSchema).max(100).nullish(),
  recommended_runbooks: z.array(citationSchema).max(100).nullish(),
  candidates: z.array(citationSchema).max(100).nullish(),
  // Only explicitly verified result packets qualify for an answer fallback.
  verified_result: z.object({rows, columns:z.array(label).optional()}).nullish(),
  diagnostics: z.record(z.string(), z.unknown()).nullish(),
  query_plan: z.record(z.string(), z.unknown()).nullish(),
  sql_evidence: z.unknown().optional(), evidence_ids: z.array(label).max(500).nullish(),
});
export type ChatResponse = z.infer<typeof backendSchema> & {request_id?: string};
export const errors: Record<string, string> = {
  invalid_input: "Vui lòng nhập câu hỏi từ 1 đến 4.000 ký tự.",
  unauthorized: "Phiên truy cập chưa được xác thực. Vui lòng liên hệ quản trị viên.",
  rate_limit: "Dịch vụ đang giới hạn lượt gọi hoặc đã hết hạn mức. Hãy thử lại sau.",
  unavailable: "Hiện chưa kết nối được dịch vụ trả lời. Bạn có thể thử lại sau.",
  timeout: "Câu hỏi mất nhiều thời gian hơn dự kiến. Bạn có thể chủ động thử lại.",
  invalid_response: "Dịch vụ trả về dữ liệu không hợp lệ. Vui lòng thử lại.",
  empty_answer: "Dịch vụ chưa trả về câu trả lời hoặc kết quả đã xác minh. Bạn có thể thử lại.",
  cancelled: "Đã hủy chờ câu trả lời.",
  configuration: "Kết nối chatbot chưa được cấu hình. Vui lòng liên hệ quản trị viên.",
};
export function fallbackMessage(reason?: string | null) {
  if (!reason) return null;
  if (reason.includes("qdrant") || reason.includes("service")) return "Kho hướng dẫn tạm thời chưa truy cập được; câu trả lời có thể chưa đầy đủ.";
  if (reason.includes("ambigu") || reason.includes("clarification")) return "Cần thêm một thông tin để chọn kết quả phù hợp.";
  return "Câu trả lời sử dụng phần dữ liệu đã kiểm chứng. Một số nội dung diễn giải có thể chưa đầy đủ.";
}
export function statusMessage(status?: string | null) {
  if(status==="no_answer") return "Chưa có đủ thông tin phù hợp để đưa ra hướng xử lý.";
  if(status==="needs_clarification") return "Vui lòng gửi lại câu hỏi đầy đủ kèm thông tin được hỏi thêm; mỗi lượt hiện được xử lý độc lập.";
  if(status==="degraded") return "Một phần dịch vụ đang gián đoạn. Kết quả hiện tại có thể chưa đầy đủ.";
  return null;
}
export function safeLink(value?: string) {
  if (!value) return undefined;
  try { const url = new URL(value); return ["https:", "http:"].includes(url.protocol) && !url.username && !url.password ? url.href : undefined; }
  catch { return undefined; }
}
