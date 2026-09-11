import type { ChatModelAdapter } from "@assistant-ui/react";
import { backendSchema, errors } from "./contract";
import type { UiTiming } from "./ui-utils";
function failure(code: string, timing: UiTiming) {
  return {content:[{type:"text" as const,text:errors[code]??errors.unavailable}],metadata:{custom:{errorCode:code,ui_timing:timing}}};
}
export function makeChatAdapter(conversationId?:string, now:()=>number=()=>performance.now()):ChatModelAdapter {
  return {
  async run({messages,abortSignal}) {
    const startedAt=now();
    const timing=():UiTiming=>({elapsed_ms:Math.max(0,now()-startedAt),measured_by:"browser"});
    const latest=messages.findLast(message=>message.role==="user");
    const question=latest?.content.filter(part=>part.type==="text").map(part=>part.text).join("\n")??"";
    try {
      const response=await fetch("/api/chat",{method:"POST",headers:{"Content-Type":"application/json"},body:JSON.stringify({question,...(conversationId?{conversation_id:conversationId}:{})}),signal:abortSignal});
      let raw;
      try { raw=await response.json(); } catch {
        if(abortSignal.aborted) throw new DOMException("Aborted", "AbortError");
        return failure("invalid_response",timing());
      }
      if(!response.ok) {
        const code=typeof raw?.error?.code==="string" ? raw.error.code : "unavailable";
        return failure(code,timing());
      }
      const result=backendSchema.safeParse(raw);
      if(!result.success) return failure("invalid_response",timing());
      if(!result.data.answer?.trim()) return failure("empty_answer",timing());
      return {content:[{type:"text",text:result.data.answer??errors.empty_answer}],metadata:{custom:{response:{...result.data,request_id:raw.request_id},ui_timing:timing()}}};
    } catch(error) {
      if(abortSignal.aborted) throw error;
      return failure("unavailable",timing());
    }
  },
  };
}
export const chatAdapter=makeChatAdapter();
