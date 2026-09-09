"use client";
import { ExternalLink, BookOpen, ChevronDown } from "lucide-react";
import { fallbackMessage, statusMessage, safeLink, type ChatResponse } from "@/lib/contract";

export function ResponseDetails({response}:{response:ChatResponse}) {
  const warning=statusMessage(response.status)??fallbackMessage(response.fallback_reason);
  const technical={status:response.status,reason:response.reason,fallback_reason:response.fallback_reason,row_count:response.row_count,query_plan:response.query_plan,sql_evidence:response.sql_evidence,evidence:response.evidence,evidence_ids:response.evidence_ids,diagnostics:response.diagnostics,request_id:response.request_id};
  const hasTechnical=Object.values(technical).some(v=>v!==undefined&&v!==null);
  const rows=response.verified_result?.rows??[];
  const columns=Array.from(new Set(rows.flatMap(row=>Object.keys(row))));
  return <div className="response-details">
    <div className="badges">{response.route&&<span>{response.route}</span>}{response.source&&<span>{response.source}</span>}</div>
    {warning&&<p className="notice">{warning}</p>}
    {!!response.recommended_runbooks?.length&&<details className="disclosure"><summary>Runbook phù hợp ({response.recommended_runbooks.length})</summary><ul>{response.recommended_runbooks.map((item,i)=><li key={i}>{item.title??item.runbook_id}<small>{item.version?`v${item.version}`:""}</small></li>)}</ul></details>}
    {!!rows.length&&<div className="table-scroll"><table><thead><tr>{columns.map(c=><th key={c}>{c}</th>)}</tr></thead><tbody>{rows.map((row,i)=><tr key={i}>{columns.map(c=><td key={c}>{row[c]===null?"NULL":String(row[c]??"")}</td>)}</tr>)}</tbody></table></div>}
    {!!response.citations?.length&&<details className="disclosure"><summary><BookOpen size={15}/> Nguồn tham khảo ({response.citations.length}) <ChevronDown size={14}/></summary><ul>{response.citations.map((citation,i)=>{
      const url=safeLink(citation.url??citation.source);
      const label=citation.title??citation.runbook_id??"Runbook";
      return <li key={i}>{url?<a href={url} target="_blank" rel="noopener noreferrer">{label} <ExternalLink size={12}/></a>:<strong>{label}</strong>}<small>{citation.section?.replaceAll("_"," ")}{citation.version?` · v${citation.version}`:""}</small>{!url&&<small>{citation.source}</small>}</li>;
    })}</ul></details>}
    {hasTechnical&&<details className="disclosure"><summary>Chi tiết kỹ thuật <ChevronDown size={14}/></summary><pre>{JSON.stringify(technical,null,2)}</pre></details>}
  </div>;
}
