import { afterEach, describe, expect, it, vi } from "vitest";
import type { ChatModelRunOptions } from "@assistant-ui/react";
import { chatAdapter, makeChatAdapter } from "../src/lib/adapter";
import { errors, safeLink, statusMessage } from "../src/lib/contract";
const options = (signal = new AbortController().signal) => ({
  messages: [{role:"user",content:[{type:"text",text:"Previous question"}]},{role:"assistant",content:[{type:"text",text:"Previous answer"}]},{role:"user",content:[{type:"text",text:"Current question"}]}], abortSignal: signal,
} as unknown as ChatModelRunOptions);
afterEach(()=>vi.unstubAllGlobals());
describe("browser adapter",()=>{
  it("sends only current question to BFF, without authorization", async()=>{
    const fetcher=vi.fn().mockResolvedValue(Response.json({answer:"Verified answer",route:"analytics",citations:[],request_id:"req-1"}));
    vi.stubGlobal("fetch",fetcher);
    const input=options();
    const result=await chatAdapter.run(input);
    expect(fetcher).toHaveBeenCalledTimes(1);
    const [url,init]=fetcher.mock.calls[0];
    expect(url).toBe("/api/chat");
    expect(JSON.parse(init.body)).toEqual({question:"Current question"});
    expect(init.signal).toBe(input.abortSignal);
    expect(new Headers(init.headers).has("authorization")).toBe(false);
    expect(result).toMatchObject({content:[{type:"text",text:"Verified answer"}],metadata:{custom:{response:{route:"analytics",request_id:"req-1"}}}});
  });
  it("sends a stable conversation id without sending prior message text", async()=>{
    const fetcher=vi.fn().mockResolvedValue(Response.json({answer:"Verified answer",conversation:{id:"conversation-1",context_used:true,action:"continue"}}));
    vi.stubGlobal("fetch",fetcher);
    await makeChatAdapter("conversation-1").run(options());
    const body=JSON.parse(fetcher.mock.calls[0][1].body);
    expect(body).toEqual({question:"Current question",conversation_id:"conversation-1"});
    expect(JSON.stringify(body)).not.toContain("Previous question");
    expect(JSON.stringify(body)).not.toContain("Previous answer");
  });
  it("records independent browser end-to-end timing for success and retry",async()=>{
    const fetcher=vi.fn().mockResolvedValue(Response.json({answer:"Verified answer"}));
    vi.stubGlobal("fetch",fetcher);
    const samples=[100,350,1_000,1_875];
    const adapter=makeChatAdapter("conversation-1",()=>samples.shift()??1_875);
    const first=await adapter.run(options());
    const retry=await adapter.run(options());
    expect(first).toMatchObject({metadata:{custom:{ui_timing:{elapsed_ms:250,measured_by:"browser"}}}});
    expect(retry).toMatchObject({metadata:{custom:{ui_timing:{elapsed_ms:875,measured_by:"browser"}}}});
  });
  it("records elapsed browser time for a failed request",async()=>{
    vi.stubGlobal("fetch",vi.fn().mockRejectedValue(new Error("network")));
    const samples=[20,520];
    const result=await makeChatAdapter(undefined,()=>samples.shift()??520).run(options());
    expect(result).toMatchObject({metadata:{custom:{errorCode:"unavailable",ui_timing:{elapsed_ms:500,measured_by:"browser"}}}});
  });
  it.each(["invalid_input","unauthorized","rate_limit","unavailable","timeout","invalid_response","empty_answer","configuration"])("renders distinct safe error %s",async code=>{
    const fetcher=vi.fn().mockResolvedValue(Response.json({error:{code,message:"private raw failure"}},{status:503}));
    vi.stubGlobal("fetch",fetcher);
    expect(await chatAdapter.run(options())).toMatchObject({content:[{text:errors[code]}]});
    expect(fetcher).toHaveBeenCalledTimes(1);
  });
  it.each(["bad JSON", JSON.stringify({answer:13})])("classifies malformed responses correctly",async body=>{
    vi.stubGlobal("fetch",vi.fn().mockResolvedValue(new Response(body)));
    expect(await chatAdapter.run(options())).toMatchObject({metadata:{custom:{errorCode:"invalid_response"}}});
  });
  it("rejects empty answers",async()=>{
    vi.stubGlobal("fetch",vi.fn().mockResolvedValue(Response.json({answer:" "})));
    expect(await chatAdapter.run(options())).toMatchObject({metadata:{custom:{errorCode:"empty_answer"}}});
  });
  it("does not turn cancellation into an ordinary assistant failure",async()=>{
    const controller=new AbortController();controller.abort();
    vi.stubGlobal("fetch",vi.fn().mockRejectedValue(new DOMException("Aborted","AbortError")));
    await expect(chatAdapter.run(options(controller.signal))).rejects.toThrow("Aborted");
  });
  it("rejects unsafe citation links and handles backend status distinctly",()=>{
    for(const url of ["javascript:alert(1)","file:///secret", "https://user:password@example.org", "runbooks/file.md"]) expect(safeLink(url)).toBeUndefined();
    expect(safeLink("https://example.org/runbook")).toBe("https://example.org/runbook");
    expect(statusMessage("no_answer")).not.toBe(statusMessage("degraded"));
    expect(statusMessage("ok")).toBeNull();
  });
});
