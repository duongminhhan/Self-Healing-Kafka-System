"use client";
import { useEffect, useMemo, useRef, useState } from "react";
import { ExternalLink, BookOpen, ChevronDown, ChevronUp, ChevronsUpDown, Maximize2, RotateCcw, Search, X } from "lucide-react";
import { Button } from "@/components/ui/button";
import { safeLink, type ChatResponse } from "@/lib/contract";
import { filterAndSortRows, formatElapsedTime, type SortDirection, type UiTiming } from "@/lib/ui-utils";

type VerifiedRows = NonNullable<NonNullable<ChatResponse["verified_result"]>["rows"]>;

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
  const runbooks=Array.from(new Map(
    [...(response.citations??[]), ...(response.recommended_runbooks??[])].map(item=>[
      `${item.runbook_id??"runbook"}:${item.version??""}:${item.section??""}`, item,
    ])
  ).values()).filter(item=>safeLink(item.url)?.startsWith("/runbooks/"));
  return <div className="response-details">
    {!!metadata.length&&<div className="response-meta badges" aria-label="Thông tin phản hồi" aria-live="polite">{metadata.map(item=><span key={item} title={item.startsWith("Phản hồi")?"Thời gian end-to-end quan sát từ trình duyệt":undefined}>{item}</span>)}</div>}
    {response.notice&&<p className="notice">{response.notice}</p>}
    {!!runbooks.length&&<div className="runbook-links" aria-label="Nguồn runbook"><BookOpen size={15}/><span>Nguồn runbook:</span>{runbooks.map((citation,index)=>{
      const url=safeLink(citation.url);
      const label=`${citation.runbook_id??"Runbook"}${citation.version?` v${citation.version}`:""}${citation.section?` · ${citation.section}`:""}`;
      return <a key={index} href={url}>{label} <ExternalLink size={12}/></a>;
    })}</div>}
    {showMore&&<Button variant="outline" className="verified-more" onClick={()=>document.getElementById(resultId)?.scrollIntoView({behavior:"smooth",block:"center"})}>Xem {presentation?.remaining_count??0} kết quả đã xác minh khác</Button>}
    {!!rows.length&&<VerifiedTable rows={rows} columns={columns} resultId={resultId}/>}
  </div>;
}
