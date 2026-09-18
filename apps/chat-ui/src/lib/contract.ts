import { z } from "zod";

const label = z.string().max(240);
export const conversationIdSchema = z.string().regex(/^[A-Za-z0-9._:-]{1,128}$/);
export const questionSchema = z.object({
  question: z.string().trim().min(1).max(4000),
  conversation_id: conversationIdSchema.optional(),
}).strict();
export const citationSchema = z.object({
  runbook_id: label.optional(), version: z.number().optional(), section: label.optional(),
  source: z.string().max(2000).optional(), url: z.string().max(2000).optional(), title: label.optional(),
});
const cell = z.union([z.string().max(8000), z.number().finite(), z.boolean(), z.null()]);
const rows = z.array(z.record(z.string().max(160), cell)).max(500);
const evidenceValue = z.union([cell, z.array(cell).max(10)]);
const evidenceSchema = citationSchema.extend({
  match_basis: label.optional(), matched_error_codes: z.array(label).max(20).optional(),
  matched_config_keys: z.array(label).max(20).optional(), matched_exception_classes: z.array(label).max(20).optional(),
  matched_error_signatures: z.array(label).max(20).optional(),
});
const analyticsMetricSchema = z.object({
  name: label, label, value: evidenceValue, unit: label, aggregation: label,
});
const analyticsEvidenceSchema = z.object({
  fact_id: label, rank: z.number().int().positive().nullish(), tie_count:z.number().int().positive().nullish(),
  coverage:z.enum(["complete","incomplete","boundary_tie_truncated"]).nullish(), dimension:z.array(label).max(4).nullish(),
  entity: z.record(label, evidenceValue), metrics: z.array(analyticsMetricSchema).min(1).max(4),
  details: z.record(label, evidenceValue).optional(), detail_values: z.record(label, evidenceValue).optional(),
  time_range: z.object({from_at:z.string().max(80).nullish(),to_at:z.string().max(80).nullish(),timestamp:label}),
  status: z.record(label, evidenceValue).nullish(), grain: z.string().max(240), source: z.string().max(2000),
  evidence_ids: z.array(label).max(500), complete: z.boolean(), semantic_catalog_version: label,
});
const analyticsClaimSchema = z.object({
  fact_id: label, entity: z.record(label, evidenceValue), metric: label, value: evidenceValue,
  time_range: z.object({from_at:z.string().max(80).nullish(),to_at:z.string().max(80).nullish(),timestamp:label}),
  status: z.record(label, evidenceValue).nullish(), text: z.string().max(1200),
  rank:z.number().int().positive().nullish(), tie_count:z.number().int().positive().nullish(),
});
const executedQuerySchema = z.object({
  kind:z.literal("tsql_select"), dialect:z.literal("tsql"),
  statement:z.string().min(1).max(30000), display_statement:z.string().min(1).max(40000),
  parameters:z.array(z.object({name:z.string().regex(/^@[A-Za-z][A-Za-z0-9_]{0,63}$/),type:label,value:z.union([z.string().max(1000),z.number().int(),z.null()])})).max(12),
  executed:z.literal(true), read_only:z.literal(true), result_shape:z.array(label).min(1).max(20),
});
const runbookClaimSchema = z.object({
  kind: z.literal("runbook"), citation: citationSchema, excerpt: z.string().max(1200), text: z.string().max(1200),
});
const modelCallSchema = z.object({
  model: label, http_status: z.number().int().min(100).max(599), finish_reason: label.nullish(),
  input_tokens: z.number().int().nonnegative().nullish(), output_tokens: z.number().int().nonnegative().nullish(),
  total_tokens: z.number().int().nonnegative().nullish(), latency_seconds: z.number().finite().nonnegative(),
  transport_attempts: z.number().int().positive(),
});
const outcomeSchema = z.enum([
  "verified_results", "verified_empty", "cannot_verify", "needs_clarification", "degraded",
]);
const timeRangeAppliedSchema = z.object({
  from_at: z.string().max(80).nullish(), to_at: z.string().max(80).nullish(),
  timezone: label, timestamp: label.optional(), kind: label.optional(), timestamp_field: label.optional(),
});
const presentationSchema = z.object({
  summary_item_limit:z.number().int().positive(), summary_detail_limit:z.number().int().positive(),
  result_total_count:z.number().int().nonnegative(), displayed_count:z.number().int().nonnegative(),
  remaining_count:z.number().int().nonnegative(), ranking:label.nullish(),
  tie_policy:z.enum(["exact_limit","include_ties"]).nullish(), boundary_tie_count:z.number().int().positive().nullish(),
  boundary_tie_truncated:z.boolean(), has_more_verified_results:z.boolean(), detail_accessible:z.boolean(),
});
export const backendSchema = z.object({
  answer: z.string().max(150000).nullish(), route: label.nullish(), source: label.nullish(),
  citations: z.array(citationSchema).max(100).nullish(), fallback_reason: label.nullish(),
  status: label.nullish(), reason: label.nullish(), row_count: z.number().int().nonnegative().nullish(),
  outcome: outcomeSchema.nullish(), query_executed: z.boolean().nullish(), evidence_complete: z.boolean().nullish(),
  source_kind: label.nullish(), snapshot_freshness: label.nullish(), time_range_applied: timeRangeAppliedSchema.nullish(),
  presentation: presentationSchema.nullish(),
  evidence: z.array(evidenceSchema).max(100).nullish(),
  analytics_evidence: z.array(analyticsEvidenceSchema).max(100).nullish(),
  claims: z.array(analyticsClaimSchema).max(400).nullish(),
  runbook_claims: z.array(runbookClaimSchema).max(100).nullish(),
  model_usage: z.object({planning:z.array(modelCallSchema).max(2),analytics_response:z.array(modelCallSchema).max(2),runbook:z.array(modelCallSchema).max(2)}).nullish(),
  recommended_runbooks: z.array(citationSchema).max(100).nullish(),
  candidates: z.array(citationSchema).max(100).nullish(),
  // Only explicitly verified result packets qualify for an answer fallback.
  verified_result: z.object({rows, columns:z.array(label).optional()}).nullish(),
  diagnostics: z.record(z.string(), z.unknown()).nullish(),
  query_plan: z.record(z.string(), z.unknown()).nullish(),
  semantic_plan: z.record(z.string(), z.unknown()).nullish(),
  executed_query: executedQuerySchema.nullish(),
  sql_evidence: z.unknown().optional(), evidence_ids: z.array(label).max(500).nullish(),
  conversation: z.object({
    id: conversationIdSchema,
    context_used: z.boolean(),
    action: label,
  }).strict().nullish(),
}).superRefine((value, context) => {
  // A browser must never receive an unproven negative result as
  // ``verified_empty``. The backend owns the query; the BFF enforces the
  // public cross-field invariant before it reaches the UI.
  if (value.outcome === "verified_empty" && !(
    value.query_executed === true && value.evidence_complete === true && value.row_count === 0
  )) context.addIssue({code:z.ZodIssueCode.custom,path:["outcome"],message:"verified_empty requires completed zero-row evidence"});
  if (value.outcome === "verified_results" && !(
    value.query_executed === true && value.evidence_complete === true && typeof value.row_count === "number" && value.row_count > 0
  )) context.addIssue({code:z.ZodIssueCode.custom,path:["outcome"],message:"verified_results requires complete evidence"});
  if (value.outcome === "needs_clarification" && (
    value.query_executed === true || value.evidence_complete === true
  )) context.addIssue({code:z.ZodIssueCode.custom,path:["outcome"],message:"clarification cannot claim executed evidence"});
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
  if(status==="cannot_verify") return null;
  if(status==="verified_empty") return null;
  if(status==="no_answer") return "Chưa có đủ thông tin phù hợp để đưa ra hướng xử lý.";
  if(status==="needs_clarification") return "Hãy bổ sung thông tin được hỏi; câu tiếp theo có thể kế thừa ngữ cảnh đã kiểm chứng trong cuộc trò chuyện này.";
  if(status==="degraded") return "Một phần dịch vụ đang gián đoạn. Kết quả hiện tại có thể chưa đầy đủ.";
  return null;
}
export function sourceKindLabel(value?: string | null) {
  if(value==="historical_incident_snapshot") return "Snapshot incident lịch sử";
  return value?.replaceAll("_", " ");
}
export function timeRangeLabel(value?: z.infer<typeof timeRangeAppliedSchema> | null) {
  if(!value?.from_at || !value.to_at) return undefined;
  return `Phạm vi: ${value.from_at} – ${value.to_at} (${value.timezone})`;
}
const routeLabels: Record<string,string> = {
  analytics: "Phân tích dữ liệu", runbook: "Hướng dẫn xử lý", combined: "Phân tích + hướng dẫn",
  clarification: "Cần làm rõ", fallback: "Dữ liệu kiểm chứng", no_answer: "Không có kết quả",
};
export function routeLabel(value?: string | null) {
  if (!value) return undefined;
  return routeLabels[value] ?? value;
}
export function safeLink(value?: string) {
  if (!value) return undefined;
  try { const url = new URL(value); return ["https:", "http:"].includes(url.protocol) && !url.username && !url.password ? url.href : undefined; }
  catch { return undefined; }
}
