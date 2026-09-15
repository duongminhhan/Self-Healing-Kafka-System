import { describe, expect, it, vi } from "vitest";
import { handleChat } from "../src/lib/bff";

const settings = { url: "http://127.0.0.1:8080/api/v1/chat", token: "test-server-secret", timeoutMs: 1000 };
function request(question: unknown = "  Connector nào lỗi?  ", signal?: AbortSignal, conversationId?: unknown) {
  return new Request("http://localhost:3000/api/chat", { method: "POST", headers: { "Content-Type": "application/json" }, body: JSON.stringify({ question, ...(conversationId===undefined?{}:{conversation_id:conversationId}) }), signal });
}
const pending = vi.fn<typeof fetch>((_, init) => new Promise((_, reject) => {
  const signal = init?.signal;
  if (signal?.aborted) reject(new DOMException("Aborted", "AbortError"));
  else signal?.addEventListener("abort", () => reject(new DOMException("Aborted", "AbortError")), { once: true });
}));

describe("chat BFF", () => {
  it("forwards only trimmed current question and injects server token, without exposing it", async () => {
    const fetcher = vi.fn<typeof fetch>().mockResolvedValue(Response.json({ answer: "Có dữ liệu.", route: "analytics" }, { headers: { "X-Request-ID": "backend-123" } }));
    const audit = vi.fn();
    const response = await handleChat(request(), settings, fetcher, audit);
    expect(response.status).toBe(200);
    expect(fetcher).toHaveBeenCalledTimes(1);
    const init = fetcher.mock.calls[0][1]!;
    expect(JSON.parse(init.body as string)).toEqual({ question: "Connector nào lỗi?" });
    expect(new Headers(init.headers).get("authorization")).toBe(`Bearer ${settings.token}`);
    expect(init.redirect).toBe("error");
    expect(response.headers.get("x-request-id")).toBe("backend-123");
    expect(await response.json()).toMatchObject({ answer: "Có dữ liệu.", citations: [] });
    expect(JSON.stringify(audit.mock.calls)).not.toContain(settings.token);
    expect(JSON.stringify(audit.mock.calls)).not.toContain("Connector nào");
  });
  it("forwards a validated conversation id and accepts structured context metadata", async () => {
    const fetcher=vi.fn<typeof fetch>().mockResolvedValue(Response.json({answer:"Có dữ liệu.",conversation:{id:"conversation-1",context_used:true,action:"continue"}}));
    const response=await handleChat(request("Tiếp theo thì sao?",undefined,"conversation-1"),settings,fetcher);
    expect(JSON.parse(fetcher.mock.calls[0][1]!.body as string)).toEqual({question:"Tiếp theo thì sao?",conversation_id:"conversation-1"});
    expect(await response.json()).toMatchObject({conversation:{id:"conversation-1",context_used:true,action:"continue"}});
  });
  it.each(["contains space","/unsafe","x".repeat(129),10])("rejects invalid conversation id %# before calling backend",async conversationId=>{
    const fetcher=vi.fn<typeof fetch>();
    expect((await handleChat(request("Câu hỏi",undefined,conversationId),settings,fetcher)).status).toBe(400);
    expect(fetcher).not.toHaveBeenCalled();
  });
  it.each(["", "   ", "x".repeat(4001), null, 10])("rejects invalid question %# before calling backend", async question => {
    const fetcher = vi.fn<typeof fetch>();
    expect((await handleChat(request(question), settings, fetcher)).status).toBe(400);
    expect(fetcher).not.toHaveBeenCalled();
  });
  it.each([400, 401, 429, 500, 502, 503])("sanitizes upstream HTTP %i without retries", async status => {
    const fetcher = vi.fn<typeof fetch>().mockResolvedValue(new Response("Traceback password=secret", { status }));
    const response = await handleChat(request(), settings, fetcher);
    expect(response.status).toBe(status === 500 ? 503 : status);
    if(status===502) expect((await response.clone().json()).error.code).toBe("invalid_response");
    expect(await response.text()).not.toMatch(/Traceback|password/);
    expect(fetcher).toHaveBeenCalledTimes(1);
  });
  it.each([{}, { answer: " " }])("rejects an absent answer without verified results", async payload => {
    const response = await handleChat(request(), settings, vi.fn<typeof fetch>().mockResolvedValue(Response.json(payload)));
    expect(response.status).toBe(502);
    expect((await response.json()).error.code).toBe("empty_answer");
  });
  it("preserves verified values without inventing a numeric answer", async () => {
    const response = await handleChat(request(), settings, vi.fn<typeof fetch>().mockResolvedValue(Response.json({ verified_result: { rows: [{ count: 75 }] } })));
    const data = await response.json();
    expect(data.verified_result.rows).toEqual([{ count: 75 }]);
    expect(data.answer).not.toMatch(/\d/);
  });
  it("keeps bounded structured analytics evidence and claims for technical details", async () => {
    const fact={fact_id:"analytics:orders:1",rank:1,entity:{connector:"orders"},metrics:[{name:"failure_count",label:"số incident",value:1,unit:"incident",aggregation:"count_distinct_incident"}],details:{},detail_values:{},time_range:{from_at:null,to_at:null,timestamp:"failure_at"},status:null,grain:"aggregated connector incident facts",source:"vConnectorIncidentFacts",evidence_ids:["one"],complete:true,semantic_catalog_version:"2026-09-11.1"};
    const response=await handleChat(request(),settings,vi.fn<typeof fetch>().mockResolvedValue(Response.json({answer:"orders có số incident là 1.",analytics_evidence:[fact],claims:[{fact_id:"analytics:orders:1",entity:{connector:"orders"},metric:"failure_count",value:1,time_range:fact.time_range,status:null,text:"orders có số incident là 1."}],model_usage:{planning:[],analytics_response:[],runbook:[]}})));
    const data=await response.json();
    expect(data.analytics_evidence[0]).toMatchObject({fact_id:"analytics:orders:1",entity:{connector:"orders"},metrics:[{value:1}]});
    expect(data.claims[0]).toMatchObject({metric:"failure_count",value:1});
    expect(data.model_usage).toEqual({planning:[],analytics_response:[],runbook:[]});
  });
  it("forwards only an executed, read-only T-SQL query packet for safe display", async () => {
    const executed_query={kind:"tsql_select",dialect:"tsql",statement:"SELECT ? AS [incident_count];",display_statement:"DECLARE @limit int = 3;\n\nSELECT @limit AS [incident_count];",parameters:[{name:"@limit",type:"int",value:3}],executed:true,read_only:true,result_shape:["incident_count"]};
    const response=await handleChat(request(),settings,vi.fn<typeof fetch>().mockResolvedValue(Response.json({answer:"Có 3 incident.",executed_query})));
    expect(response.status).toBe(200);
    expect((await response.json()).executed_query).toEqual(executed_query);
  });
  it("rejects a query packet that was not actually executed or is not read-only", async () => {
    const response=await handleChat(request(),settings,vi.fn<typeof fetch>().mockResolvedValue(Response.json({answer:"Có dữ liệu.",executed_query:{kind:"tsql_select",dialect:"tsql",statement:"SELECT 1",display_statement:"SELECT 1",parameters:[],executed:false,read_only:false,result_shape:["value"]}})));
    expect(response.status).toBe(502);
    expect((await response.json()).error.code).toBe("invalid_response");
  });
  it("forwards the public-safe execution outcome without downgrading verified empty evidence", async () => {
    const response=await handleChat(request(),settings,vi.fn<typeof fetch>().mockResolvedValue(Response.json({
      answer:"Trong snapshot hiện tại, chưa ghi nhận connector FAILED.",outcome:"verified_empty",query_executed:true,evidence_complete:true,row_count:0,
      source_kind:"historical_incident_snapshot",time_range_applied:{from_at:"2026-09-14T00:00:00+07:00",to_at:"2026-09-14T09:00:00+07:00",timezone:"Asia/Ho_Chi_Minh",timestamp:"failure_at"},
    })));
    expect(response.status).toBe(200);
    expect(await response.json()).toMatchObject({outcome:"verified_empty",query_executed:true,evidence_complete:true,row_count:0});
  });
  it("rejects an unproven verified-empty outcome before it reaches the browser", async () => {
    const response=await handleChat(request(),settings,vi.fn<typeof fetch>().mockResolvedValue(Response.json({
      answer:"Không có connector failed.",outcome:"verified_empty",query_executed:false,evidence_complete:false,row_count:0,
    })));
    expect(response.status).toBe(502);
    expect((await response.json()).error.code).toBe("invalid_response");
  });
  it("preserves numeric token accounting while redacting credentials in a successful payload", async () => {
    const usage={model:"qwen",http_status:200,input_tokens:123,output_tokens:45,total_tokens:168,latency_seconds:0.5,transport_attempts:1};
    const response=await handleChat(request(),settings,vi.fn<typeof fetch>().mockResolvedValue(Response.json({
      answer:"Có dữ liệu.",model_usage:{planning:[usage],analytics_response:[],runbook:[]},
      diagnostics:{access_token:"private",refresh_token:"private",api_key:"private",authorization:"private",token:"private",raw_log:"private",input_tokens:123},
    })));
    expect(response.status).toBe(200);
    const data=await response.json();
    expect(data.model_usage.planning[0]).toMatchObject({input_tokens:123,output_tokens:45,total_tokens:168});
    expect(data.diagnostics).toMatchObject({access_token:"[REDACTED]",refresh_token:"[REDACTED]",api_key:"[REDACTED]",authorization:"[REDACTED]",token:"[REDACTED]",raw_log:"[REDACTED]",input_tokens:123});
    expect(JSON.stringify(data)).not.toContain("private");
  });
  it("records only safe validation diagnostics when a post-redaction payload is invalid", async () => {
    const audit=vi.fn();
    const response=await handleChat(request(),settings,vi.fn<typeof fetch>().mockResolvedValue(Response.json({answer:"Có dữ liệu.",model_usage:{planning:[{model:"qwen",http_status:200,input_tokens:"not-a-number",output_tokens:45,total_tokens:168,latency_seconds:0.5,transport_attempts:1}],analytics_response:[],runbook:[]}})),audit);
    expect(response.status).toBe(502);
    expect(audit).toHaveBeenCalledWith(expect.objectContaining({failure_kind:"schema_validation",validation_issues:[expect.objectContaining({path:"model_usage.planning.0.input_tokens",code:"invalid_type"})]}));
    expect(JSON.stringify(audit.mock.calls)).not.toContain("Có dữ liệu");
  });
  it("classifies upstream HTTP, JSON and empty-response failures for server audit", async () => {
    const cases=[
      {upstream:new Response("not json"),kind:"upstream_json_parse",code:"invalid_response"},
      {upstream:new Response("unavailable",{status:502}),kind:"upstream_http",code:"invalid_response"},
      {upstream:Response.json({answer:" "}),kind:"empty_answer",code:"empty_answer"},
    ] as const;
    for(const item of cases){
      const audit=vi.fn();
      const response=await handleChat(request(),settings,vi.fn<typeof fetch>().mockResolvedValue(item.upstream),audit);
      expect((await response.json()).error.code).toBe(item.code);
      expect(audit).toHaveBeenCalledWith(expect.objectContaining({failure_kind:item.kind}));
    }
  });
  it("rejects invalid JSON", async () => {
    const response = await handleChat(request(), settings, vi.fn<typeof fetch>().mockResolvedValue(new Response("not json")));
    expect((await response.json()).error.code).toBe("invalid_response");
  });
  it("redacts configured credentials in successful backend payloads", async () => {
    const response = await handleChat(request(), settings, vi.fn<typeof fetch>().mockResolvedValue(Response.json({ answer: settings.token, diagnostics: { raw_log: "private", password: "private" } })));
    const text = await response.text();
    expect(text).not.toContain(settings.token);
    expect(text).not.toContain("private");
  });
  it("times out and aborts the upstream request", async () => {
    const response = await handleChat(request(), { ...settings, timeoutMs: 5 }, pending);
    expect(response.status).toBe(504);
    expect((await response.json()).error.code).toBe("timeout");
  });
  it("propagates user cancellation to the upstream signal", async () => {
    const controller = new AbortController();
    const result = handleChat(request("Cancel", controller.signal), settings, pending);
    setTimeout(() => controller.abort(), 5);
    const response = await result;
    expect(response.status).toBe(499);
    expect((await response.json()).error.code).toBe("cancelled");
  });
  it("fails closed when configuration is absent", async () => {
    const fetcher = vi.fn<typeof fetch>();
    expect((await handleChat(request(), { timeoutMs: 100 }, fetcher)).status).toBe(503);
    expect(fetcher).not.toHaveBeenCalled();
  });
  it("rejects cross-origin browser requests", async () => {
    const req = request(); req.headers.set("Origin", "https://untrusted.example");
    expect((await handleChat(req, settings)).status).toBe(403);
  });
});
