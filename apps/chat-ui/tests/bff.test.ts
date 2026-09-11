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
