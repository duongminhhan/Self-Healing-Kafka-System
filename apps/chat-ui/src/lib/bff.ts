import { randomUUID } from "node:crypto";
import { backendSchema, errors, questionSchema } from "./contract";

type Settings = {url?:string; token?:string; timeoutMs:number};
type Audit = {request_id:string; status:number; latency_ms:number; source?:string|null; route?:string|null; fallback_reason?:string|null};
const safeLabel = (value?:string|null) => value && /^[a-z0-9_:-]{1,100}$/i.test(value) ? value : undefined;
function scrub(value: unknown, token: string, depth=0): unknown {
  if (depth > 8) return "[omitted]";
  if (typeof value === "string") return (token ? value.split(token).join("[REDACTED]") : value)
    .replace(/\b(Bearer|Basic)\s+[A-Za-z0-9+/_=.:\-]+/gi, "$1 [REDACTED]")
    .replace(/((?:password|passwd|pwd|secret|token|api[_-]?key)\s*[:=]\s*)[^\s,;]+/gi, "$1[REDACTED]");
  if (Array.isArray(value)) return value.slice(0,500).map(v=>scrub(v,token,depth+1));
  if (value && typeof value === "object") return Object.fromEntries(Object.entries(value).slice(0,100).map(([k,v])=>[k, /password|secret|token|authorization|raw.?log|traceback|stack/i.test(k) ? "[REDACTED]" : scrub(v,token,depth+1)]));
  return value;
}
async function boundedText(response: Response, limit:number) {
  if (!response.body) return "";
  const reader=response.body.getReader(); const chunks: Uint8Array[]=[]; let size=0;
  try { while(true) { const {done,value}=await reader.read(); if(done) break; size+=value.length; if(size>limit) { await reader.cancel(); throw new Error("oversize"); } chunks.push(value); } }
  finally { reader.releaseLock(); }
  return Buffer.concat(chunks).toString("utf8");
}
export async function handleChat(request:Request, settings:Settings, fetcher:typeof fetch=fetch, audit:(entry:Audit)=>void=()=>{}) {
  const started=Date.now(); let id=randomUUID() as string;
  const respond=(body:object,status:number, meta:Partial<Audit>={})=>{
    audit({request_id:id,status,latency_ms:Date.now()-started,...meta});
    return Response.json({...body,request_id:id},{status,headers:{"Cache-Control":"no-store","X-Request-ID":id}});
  };
  const fail=(code:string,status:number)=>respond({error:{code,message:errors[code]}},status);
  // The default deployment is loopback-only. Reject cross-origin browser requests.
  const origin=request.headers.get("origin");
  // Next may normalize request.url to localhost despite a loopback Host header.
  const expectedHost=request.headers.get("host")??new URL(request.url).host;
  if (origin) {
    try { if(new URL(origin).host!==expectedHost || !["http:","https:"].includes(new URL(origin).protocol)) return fail("unauthorized",403); }
    catch { return fail("unauthorized",403); }
  }
  if (!request.headers.get("content-type")?.includes("application/json")) return fail("invalid_input",400);
  let question:string;
  try {
    const raw=await boundedText(new Response(request.body),20000);
    question=questionSchema.parse(JSON.parse(raw)).question;
  } catch { return fail("invalid_input",400); }
  if (!settings.url || !settings.token) return fail("configuration",503);
  let url:URL;
  try { url=new URL(settings.url); if(!["http:","https:"].includes(url.protocol)||url.username||url.password) throw new Error(); }
  catch { return fail("configuration",503); }
  const controller=new AbortController(); let timedOut=false;
  const cancel=()=>controller.abort();
  request.signal.addEventListener("abort",cancel,{once:true});
  if(request.signal.aborted) controller.abort();
  const timer=setTimeout(()=>{timedOut=true;controller.abort();},settings.timeoutMs);
  try {
    const upstream=await fetcher(url,{method:"POST",headers:{"Content-Type":"application/json","Authorization":`Bearer ${settings.token}`,"X-Request-ID":id},body:JSON.stringify({question}),signal:controller.signal,cache:"no-store",redirect:"error"});
    const forwarded=upstream.headers.get("x-request-id");
    if(forwarded && /^[\w.:-]{1,128}$/.test(forwarded) && !forwarded.includes(settings.token)) id=forwarded;
    if(!upstream.ok) { await upstream.body?.cancel(); const status=upstream.status;
      return fail(status===400?"invalid_input":status===401||status===403?"unauthorized":status===402||status===429?"rate_limit":"unavailable",[400,401,429,503].includes(status)?status:status===402?429:503);
    }
    let data;
    try { data=backendSchema.parse(scrub(JSON.parse(await boundedText(upstream,1000000)),settings.token)); }
    catch { if(controller.signal.aborted) throw new Error("aborted"); return fail("invalid_response",502); }
    if(!data.answer?.trim() && !data.verified_result?.rows.length) return fail("empty_answer",502);
    return respond({...data,answer:data.answer?.trim()||"Đây là kết quả đã xác minh từ dữ liệu.",citations:data.citations??[]},200,{source:safeLabel(data.source),route:safeLabel(data.route),fallback_reason:safeLabel(data.fallback_reason)});
  } catch { return fail(timedOut?"timeout":request.signal.aborted?"cancelled":"unavailable",timedOut?504:request.signal.aborted?499:503); }
  finally { clearTimeout(timer);request.signal.removeEventListener("abort",cancel); }
}
