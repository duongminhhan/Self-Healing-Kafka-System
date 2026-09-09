import type { ChatModelAdapter } from "@assistant-ui/react";
import { backendSchema, errors } from "./contract";
function failure(code: string) {
  return {content:[{type:"text" as const,text:errors[code]??errors.unavailable}],metadata:{custom:{errorCode:code}}};
}
export const chatAdapter:ChatModelAdapter = {
  async run({messages,abortSignal}) {
    const latest=messages.findLast(message=>message.role==="user");
    const question=latest?.content.filter(part=>part.type==="text").map(part=>part.text).join("\n")??"";
    try {
      const response=await fetch("/api/chat",{method:"POST",headers:{"Content-Type":"application/json"},body:JSON.stringify({question}),signal:abortSignal});
      let raw;
      try { raw=await response.json(); } catch {
        if(abortSignal.aborted) throw new DOMException("Aborted", "AbortError");
        return failure("invalid_response");
      }
      if(!response.ok) {
        const code=typeof raw?.error?.code==="string" ? raw.error.code : "unavailable";
        return failure(code);
      }
      const result=backendSchema.safeParse(raw);
      if(!result.success) return failure("invalid_response");
      if(!result.data.answer?.trim()) return failure("empty_answer");
      return {content:[{type:"text",text:result.data.answer??errors.empty_answer}],metadata:{custom:{response:{...result.data,request_id:raw.request_id}}}};
    } catch(error) {
      if(abortSignal.aborted) throw error;
      return failure("unavailable");
    }
  },
};
