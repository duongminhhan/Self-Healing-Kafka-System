"use client";
import { useEffect, useMemo, useRef, useState } from "react";
import { ExternalLink, BookOpen, ChevronDown, ChevronUp, ChevronsUpDown, Maximize2, RotateCcw, Search, X } from "lucide-react";
import { Button } from "@/components/ui/button";
import { fallbackMessage, statusMessage, safeLink, type ChatResponse } from "@/lib/contract";
import { filterAndSortRows, formatElapsedTime, type SortDirection, type UiTiming } from "@/lib/ui-utils";

type VerifiedRows = NonNullable<NonNullable<ChatResponse["verified_result"]>["rows"]>;

function ExecutedQuery({query}:{query:NonNullable<ChatResponse["executed_query"]>}) {
  const [copied,setCopied]=useState(false);
  const copy=()=>{
    const write=navigator.clipboard?.writeText(query.display_statement);
    write?.then(()=>{
      setCopied(true);
      window.setTimeout(()=>setCopied(false),1500);
    }).catch(()=>{});
  };
  return <details className="disclosure executed-query">
    <summary>Truy vấn đã chạy <span className="query-badge">T-SQL · chỉ đọc</span><ChevronDown size={14}/></summary>
    <p>Truy vấn này đã được backend thực thi với các tham số hiển thị bên dưới.</p>
    <div className="tech-copy"><button type="button" onClick={copy}>{copied?"Đã sao chép":"Sao chép truy vấn"}</button></div>
    <pre><code>{query.display_statement}</code></pre>
  </details>;
}

function VerifiedTable({rows,columns,resultId}:{rows:VerifiedRows;columns:string[];resultId:string}) {
  const [query,setQuery]=useState("");
  const [sort,setSort]=useState<{column:string;direction:SortDirection}|null>(null);
  const [fullscreen,setFullscreen]=useState(false);
  const closeRef=useRef<HTMLButtonElement|null>(null);
  const visibleRows=useMemo(()=>filterAndSortRows(rows,query,sort?.column,sort?.direction),[rows,query,sort]);

  useEffect(()=>{
    if(!fullscreen) return;
    const previousOverflow=document.body.style.overflow;
    document.body.style.overflow="hidden";
    closeRef.current?.focus();
    const close=(event:KeyboardEvent)=>{
      if(event.key==="Escape"){setFullscreen(false);return;}
      if(event.key!=="Tab")return;
      const dialog=document.getElementById(resultId)?.querySelector<HTMLElement>(".table-modal");
      const focusable=Array.from(dialog?.querySelectorAll<HTMLElement>('button:not([disabled]), input:not([disabled]), [tabindex]:not([tabindex="-1"])')??[]);
      if(!focusable.length)return;
      const first=focusable[0];const last=focusable[focusable.length-1];
      if(event.shiftKey&&document.activeElement===first){event.preventDefault();last.focus();}
      else if(!event.shiftKey&&document.activeElement===last){event.preventDefault();first.focus();}
    };
    window.addEventListener("keydown",close);
    return ()=>{document.body.style.overflow=previousOverflow;window.removeEventListener("keydown",close);window.requestAnimationFrame(()=>document.getElementById(resultId)?.querySelector<HTMLButtonElement>('[aria-label="Mở bảng toàn màn hình"]')?.focus());};
  },[fullscreen,resultId]);

  const toggleSort=(column:string)=>setSort(current=>current?.column===column
    ? {column,direction:current.direction==="ascending"?"descending":"ascending"}
    : {column,direction:"ascending"});
  const reset=()=>{setQuery("");setSort(null);};
  const table=<>
    <div className="table-toolbar">
      <label className="table-search"><Search size={15}/><span className="sr-only">Tìm trong bảng</span><input value={query} onChange={event=>setQuery(event.target.value)} placeholder="Tìm trong bảng…" aria-label="Tìm trong bảng"/></label>
      <span className="table-count" aria-live="polite">{visibleRows.length} / {rows.length} dòng</span>
      <Button variant="ghost" size="icon" aria-label="Đặt lại bộ lọc và sắp xếp" title="Đặt lại" onClick={reset}><RotateCcw size={15}/></Button>
      {!fullscreen&&<Button variant="ghost" size="icon" aria-label="Mở bảng toàn màn hình" title="Toàn màn hình" onClick={()=>setFullscreen(true)}><Maximize2 size={15}/></Button>}
      {fullscreen&&<Button ref={closeRef} variant="ghost" size="icon" aria-label="Đóng bảng toàn màn hình" title="Đóng" onClick={()=>setFullscreen(false)}><X size={17}/></Button>}
    </div>
    <div className="table-scroll" tabIndex={0} role="region" aria-label="Kết quả đã xác minh">
      <table><thead><tr>{columns.map(column=>{
        const active=sort?.column===column;
        return <th key={column} scope="col" aria-sort={active?sort.direction:"none"}><button className="table-sort" onClick={()=>toggleSort(column)}>{column}{!active?<ChevronsUpDown size={14}/>:sort.direction==="ascending"?<ChevronUp size={14}/>:<ChevronDown size={14}/>}</button></th>;
      })}</tr></thead><tbody>{visibleRows.map((row,index)=><tr key={index}>{columns.map(column=><td key={column}>{row[column]===null?"NULL":String(row[column]??"")}</td>)}</tr>)}</tbody></table>
      {!visibleRows.length&&<p className="table-empty">Không tìm thấy dòng phù hợp.</p>}
    </div>
  </>;

  return <div id={resultId} className="verified-result">{fullscreen?<div className="table-modal-backdrop" role="presentation" onMouseDown={event=>{if(event.target===event.currentTarget)setFullscreen(false);}}><section className="table-modal" role="dialog" aria-modal="true" aria-label="Bảng kết quả đã xác minh toàn màn hình">{table}</section></div>:table}</div>;
}

export function ResponseDetails({response,timing,resultId}:{response:ChatResponse;timing?:UiTiming;resultId:string}) {
  const outcomeHandled=response.outcome==="verified_empty"||response.outcome==="cannot_verify";
  const responseFallback=/^(?:grounding_failure|response_model_not_configured)/.test(response.fallback_reason??"");
  const warning=statusMessage(response.status)??(outcomeHandled||responseFallback?null:fallbackMessage(response.fallback_reason));
  const technical={outcome:response.outcome,query_executed:response.query_executed,evidence_complete:response.evidence_complete,status:response.status,reason:response.reason,fallback_reason:response.fallback_reason,row_count:response.row_count,time_range_applied:response.time_range_applied,semantic_plan:response.semantic_plan,query_plan:response.query_plan,sql_evidence:response.sql_evidence,evidence:response.evidence,analytics_evidence:response.analytics_evidence,claims:response.claims,runbook_claims:response.runbook_claims,model_usage:response.model_usage,evidence_ids:response.evidence_ids,diagnostics:response.diagnostics};
  const hasTechnical=Object.values(technical).some(value=>value!==undefined&&value!==null);
  const rows=response.verified_result?.rows??[];
  const columns=Array.from(new Set([...(response.verified_result?.columns??[]),...rows.flatMap(row=>Object.keys(row))]));
  const metadata=[
    timing&&`Phản hồi trong ${formatElapsedTime(timing.elapsed_ms)}`,
  ].filter(Boolean) as string[];
  const presentation=response.presentation;
  const showMore=Boolean(
    presentation?.has_more_verified_results
    && presentation.detail_accessible
    && rows.length
    && presentation.remaining_count>0
  );
  return <div className="response-details">
    {!!metadata.length&&<div className="response-meta badges" aria-label="Thông tin phản hồi" aria-live="polite">{metadata.map(item=><span key={item} title={item.startsWith("Phản hồi")?"Thời gian end-to-end quan sát từ trình duyệt":undefined}>{item}</span>)}</div>}
    {warning&&<p className="notice">{warning}</p>}
    {!!response.recommended_runbooks?.length&&<details className="disclosure"><summary>Runbook phù hợp ({response.recommended_runbooks.length})</summary><ul>{response.recommended_runbooks.map((item,index)=><li key={index}>{item.title??item.runbook_id}<small>{item.version?`v${item.version}`:""}</small></li>)}</ul></details>}
    {showMore&&<Button variant="outline" className="verified-more" onClick={()=>document.getElementById(resultId)?.scrollIntoView({behavior:"smooth",block:"center"})}>Xem {presentation?.remaining_count??0} kết quả đã xác minh khác</Button>}
    {!!rows.length&&<VerifiedTable rows={rows} columns={columns} resultId={resultId}/>}
    {response.executed_query?.executed&&response.executed_query.read_only&&<ExecutedQuery query={response.executed_query}/>}
    {!!response.citations?.length&&<details className="disclosure"><summary><BookOpen size={15}/> Nguồn tham khảo ({response.citations.length}) <ChevronDown size={14}/></summary><ul>{response.citations.map((citation,index)=>{
      const url=safeLink(citation.url??citation.source);
      const label=citation.title??citation.runbook_id??"Runbook";
      return <li key={index}>{url?<a href={url} target="_blank" rel="noopener noreferrer">{label} <ExternalLink size={12}/></a>:<strong>{label}</strong>}<small>{citation.section?.replaceAll("_"," ")}{citation.version?` · v${citation.version}`:""}</small>{!url&&<small>{citation.source}</small>}</li>;
    })}</ul></details>}
    {hasTechnical&&<details className="disclosure"><summary>Chi tiết kỹ thuật <ChevronDown size={14}/></summary><div className="tech-copy"><button type="button" onClick={(event)=>{const text=JSON.stringify(technical,null,2);navigator.clipboard?.writeText(text).catch(()=>{});const button=event.currentTarget;button.textContent="Đã sao chép";setTimeout(()=>{button.textContent="Sao chép";},1500);}}>Sao chép</button></div><pre>{JSON.stringify(technical,null,2)}</pre></details>}
  </div>;
}
