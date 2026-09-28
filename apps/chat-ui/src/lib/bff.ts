import { randomUUID } from "node:crypto";
import { z } from "zod";
import { backendSchema, errors, questionSchema, upstreamBackendSchema, type ChatResponse } from "./contract";

type Settings = {url?:string; token?:string; timeoutMs:number};
type FailureKind = "configuration"|"request_validation"|"upstream_http"|"upstream_json_parse"|"schema_validation"|"empty_answer"|"timeout"|"cancelled"|"transport";
type Audit = {request_id:string; status:number; latency_ms:number; source?:string|null; route?:string|null; failure_kind?:FailureKind; upstream_status?:number; validation_issues?:Array<{path:string;code:string}>};
const safeLabel = (value?:string|null) => value && /^[a-z0-9_:-]{1,100}$/i.test(value) ? value : undefined;
const sensitiveObjectKey = (key:string) => /^(?:authorization|password|passwd|pwd|secret|api[_-]?key|apikey|access[_-]?token|refresh[_-]?token|id[_-]?token|token|prompt|raw[_-]?(?:prompt|question|log)|traceback|stack(?:trace)?)$/i.test(key) || /(?:^|[_-])token$/i.test(key);
function scrub(value: unknown, token: string, depth=0): unknown {
  if (depth > 8) return "[omitted]";
  if (typeof value === "string") return (token ? value.split(token).join("[REDACTED]") : value)
    .replace(/\b(Bearer|Basic)\s+[A-Za-z0-9+/_=.:\-]+/gi, "$1 [REDACTED]")
    .replace(/((?:password|passwd|pwd|secret|token|api[_-]?key)\s*[:=]\s*)[^\s,;]+/gi, "$1[REDACTED]");
  if (Array.isArray(value)) return value.slice(0,500).map(v=>scrub(v,token,depth+1));
  if (value && typeof value === "object") return Object.fromEntries(Object.entries(value).slice(0,100).map(([k,v])=>[k, sensitiveObjectKey(k) ? "[REDACTED]" : scrub(v,token,depth+1)]));
  return value;
}
async function boundedText(response: Response, limit:number) {
  if (!response.body) return "";
  const reader=response.body.getReader(); const chunks: Uint8Array[]=[]; let size=0;
  try { while(true) { const {done,value}=await reader.read(); if(done) break; size+=value.length; if(size>limit) { await reader.cancel(); throw new Error("oversize"); } chunks.push(value); } }
  finally { reader.releaseLock(); }
  return Buffer.concat(chunks).toString("utf8");
}

const publicRoutes = new Set(["analytics", "runbook", "combined", "clarification", "out_of_scope", "fallback", "no_answer", "conversation", "unsupported"]);
const publicStatuses = new Set(["ok", "verified_results", "verified_empty", "cannot_verify", "needs_clarification", "out_of_scope", "degraded", "no_answer"]);
const publicRowKeys = new Set([
  "label", "connector", "root_connector", "current_connector", "connector_name", "job_name", "error", "failure_code", "error_code", "error_signature", "final_outcome", "outcome", "error_message", "queue_status",
  "severity",
  "incident_count", "failure_count", "recovered_incident_count", "recovered_count", "open_incident_count", "open_count", "average_recovery_minutes", "recovery_rate_percent",
  "recovery_rate_numerator", "recovery_rate_denominator", "rank", "tie_count", "tie_truncated",
]);

function publicRows(value: ChatResponse["verified_result"]): ChatResponse["verified_result"] {
  if (!value) return value;
  return {
    columns: value.columns?.filter(column => publicRowKeys.has(column)).slice(0, 20),
    rows: value.rows.map(row => Object.fromEntries(
      Object.entries(row)
        .filter(([key]) => publicRowKeys.has(key))
        .map(([key, item]) => [key, typeof item === "string" ? item.slice(0, 2000) : item]),
    )),
  };
}

function publicCitations(value: Array<{runbook_id?:string;version?:number;section?:string;url?:string;title?:string}>|null|undefined) {
  return value?.flatMap(item => {
    const {runbook_id,version,section}=item;
    if (!runbook_id?.match(/^[A-Za-z0-9][A-Za-z0-9._-]{0,127}$/)
      || typeof version!=="number" || !Number.isInteger(version) || version < 1
      || !section?.match(/^[A-Za-z0-9_-]+$/)) return [];
    return [{
      runbook_id,
      version,
      section,
      url:`/runbooks/${runbook_id}?v=${version}#${section}`,
      ...(item.title === undefined ? {} : {title:item.title}),
    }];
  });
}

function publicNotice(data: z.infer<typeof upstreamBackendSchema>): string | undefined {
  if (data.outcome === "degraded") return "Một phần dịch vụ đang gián đoạn. Kết quả hiện tại có thể chưa đầy đủ.";
  if (data.outcome === "needs_clarification") return "Hãy bổ sung thông tin được hỏi để chọn đúng phạm vi truy vấn.";
  if (data.outcome === "out_of_scope") return undefined;
  if ((data.outcome === "verified_results" || data.verified_result?.rows.length) && data.fallback_reason) {
    return "Câu trả lời sử dụng phần dữ liệu đã kiểm chứng. Một số nội dung diễn giải có thể chưa đầy đủ.";
  }
  return undefined;
}

function projectPublicResponse(data: z.infer<typeof upstreamBackendSchema>): Omit<ChatResponse, "request_id"> {
  const route = data.route && publicRoutes.has(data.route) ? data.route as ChatResponse["route"] : undefined;
  const statusValue = data.status ?? data.outcome;
  const status = statusValue && publicStatuses.has(statusValue) ? statusValue as ChatResponse["status"] : undefined;
  return {
    answer: data.answer?.trim() || (data.verified_result?.rows.length ? "Đây là kết quả đã xác minh từ dữ liệu." : undefined),
    route,
    citations: publicCitations(data.citations),
    notice: publicNotice(data),
    status,
    row_count: data.row_count,
    outcome: data.outcome,
    query_executed: data.query_executed,
    evidence_complete: data.evidence_complete,
    source_kind: data.source_kind === "historical_incident_snapshot" ? data.source_kind : undefined,
    snapshot_freshness: data.snapshot_freshness,
    time_range_applied: data.time_range_applied,
    presentation: data.presentation,
    recommended_runbooks: publicCitations(data.recommended_runbooks),
    verified_result: publicRows(data.verified_result),
    conversation: data.conversation,
  };
}
export async function handleChat(request:Request, settings:Settings, fetcher:typeof fetch=fetch, audit:(entry:Audit)=>void=()=>{}) {
  const started=Date.now(); let id=randomUUID() as string;
  const respond=(body:object,status:number, meta:Partial<Audit>={})=>{
    audit({request_id:id,status,latency_ms:Date.now()-started,...meta});
    return Response.json({...body,request_id:id},{status,headers:{"Cache-Control":"no-store","X-Request-ID":id}});
  };
  const fail=(code:string,status:number,meta:Partial<Audit>={})=>respond({error:{code,message:errors[code]}},status,meta);
  // The default deployment is loopback-only. Reject cross-origin browser requests.
  const origin=request.headers.get("origin");
  // Next may normalize request.url to localhost despite a loopback Host header.
  const expectedHost=request.headers.get("host")??new URL(request.url).host;
  if (origin) {
    try { if(new URL(origin).host!==expectedHost || !["http:","https:"].includes(new URL(origin).protocol)) return fail("unauthorized",403); }
    catch { return fail("unauthorized",403,{failure_kind:"request_validation"}); }
  }
  if (!request.headers.get("content-type")?.includes("application/json")) return fail("invalid_input",400,{failure_kind:"request_validation"});
  let payload:{question:string;conversation_id?:string};
  try {
    const raw=await boundedText(new Response(request.body),20000);
    payload=questionSchema.parse(JSON.parse(raw));
  } catch { return fail("invalid_input",400,{failure_kind:"request_validation"}); }
  if (!settings.url || !settings.token) return fail("configuration",503,{failure_kind:"configuration"});
  let url:URL;
  try { url=new URL(settings.url); if(!["http:","https:"].includes(url.protocol)||url.username||url.password) throw new Error(); }
  catch { return fail("configuration",503,{failure_kind:"configuration"}); }
  const controller=new AbortController(); let timedOut=false;
  const cancel=()=>controller.abort();
  request.signal.addEventListener("abort",cancel,{once:true});
  if(request.signal.aborted) controller.abort();
  const timer=setTimeout(()=>{timedOut=true;controller.abort();},settings.timeoutMs);
  try {
    const upstream=await fetcher(url,{method:"POST",headers:{"Content-Type":"application/json","Authorization":`Bearer ${settings.token}`,"X-Request-ID":id},body:JSON.stringify(payload),signal:controller.signal,cache:"no-store",redirect:"error"});
    const forwarded=upstream.headers.get("x-request-id");
    if(forwarded && /^[\w.:-]{1,128}$/.test(forwarded) && !forwarded.includes(settings.token)) id=forwarded;
    if(!upstream.ok) { await upstream.body?.cancel(); const status=upstream.status;
      return fail(status===400||status===422?"invalid_input":status===401||status===403?"unauthorized":status===402||status===429?"rate_limit":status===502?"invalid_response":"unavailable",[400,401,422,429,502,503].includes(status)?status:status===402?429:503,{failure_kind:"upstream_http",upstream_status:status});
    }
    let raw:unknown;
    try { raw=JSON.parse(await boundedText(upstream,1000000)); }
    catch { if(controller.signal.aborted) throw new Error("aborted"); return fail("invalid_response",502,{failure_kind:"upstream_json_parse"}); }
    const parsed=upstreamBackendSchema.safeParse(scrub(raw,settings.token));
    if(!parsed.success) return fail("invalid_response",502,{failure_kind:"schema_validation",validation_issues:parsed.error.issues.slice(0,20).map(issue=>({path:issue.path.map(String).join("."),code:issue.code}))});
    const publicPayload=projectPublicResponse(parsed.data);
    const publicParsed=backendSchema.safeParse(publicPayload);
    if(!publicParsed.success) return fail("invalid_response",502,{failure_kind:"schema_validation",validation_issues:publicParsed.error.issues.slice(0,20).map(issue=>({path:issue.path.map(String).join("."),code:issue.code}))});
    const data=publicParsed.data;
    if(!data.answer?.trim() && !data.verified_result?.rows.length) return fail("empty_answer",502,{failure_kind:"empty_answer"});
    return respond({...data,answer:data.answer?.trim()||"Đây là kết quả đã xác minh từ dữ liệu.",citations:data.citations??[]},200,{route:safeLabel(data.route)});
  } catch { return fail(timedOut?"timeout":request.signal.aborted?"cancelled":"unavailable",timedOut?504:request.signal.aborted?499:503,{failure_kind:timedOut?"timeout":request.signal.aborted?"cancelled":"transport"}); }
  finally { clearTimeout(timer);request.signal.removeEventListener("abort",cancel); }
}
