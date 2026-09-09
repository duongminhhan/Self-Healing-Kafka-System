import { afterEach, describe, expect, it, vi } from "vitest";
import type { ChatModelRunOptions } from "@assistant-ui/react";
import { chatAdapter } from "../src/lib/adapter";
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
