"use client";
import { useEffect, useRef, useState } from "react";
import { AssistantRuntimeProvider, useLocalRuntime, ThreadPrimitive, ComposerPrimitive, MessagePrimitive, ActionBarPrimitive, useAuiState, useAui } from "@assistant-ui/react";
import { Activity, Plus, ArrowUp, Square, RotateCcw, Moon, Sun, PanelLeft, MessageSquare, ArrowRight, Trash2, Copy, ChevronsDown } from "lucide-react";
import ReactMarkdown from "react-markdown";
import remarkGfm from "remark-gfm";
import { Button } from "@/components/ui/button";
import { chatAdapter } from "@/lib/adapter";
import type { ChatResponse } from "@/lib/contract";
import { safeLink } from "@/lib/contract";
import { useDarkTheme, useMediaQuery, setDarkTheme } from "@/lib/theme";
import { ResponseDetails } from "./response-details";

const suggestions=[
  ["Phân tích sự cố","Connector nào gặp nhiều sự cố nhất trong 7 ngày qua?"],
  ["Tìm hướng xử lý","Làm sao xử lý lỗi ORA-01017 trên Oracle connector?"],
  ["Kiểm tra phục hồi","Tỷ lệ phục hồi thành công tuần này là bao nhiêu?"],
  ["Tra cứu runbook","Kafka Connect bị connection timeout, tôi nên kiểm tra gì?"],
];
function Markdown({text}:{text:string}) {
  return <div className="markdown"><ReactMarkdown remarkPlugins={[remarkGfm]} components={{img:({alt})=><span>{alt??"[Image omitted]"}</span>,a:({href,children})=>safeLink(href)?<a href={safeLink(href)} target="_blank" rel="noopener noreferrer">{children}</a>:<span>{children}</span>}}>{text}</ReactMarkdown></div>;
}
function UserMessage(){return <MessagePrimitive.Root className="user-message"><MessagePrimitive.Content components={{Text:Markdown}}/></MessagePrimitive.Root>;}
function AssistantMessage(){
  const message=useAuiState(s=>s.message);
  const running=message.status?.type==="running";
  const cancelled=message.status?.type==="incomplete"&&message.status.reason==="cancelled";
  const response=message.metadata.custom.response as ChatResponse|undefined;
  return <MessagePrimitive.Root className="assistant-message">
    <div className="assistant-label"><span className="mini-brand"><Activity size={15}/></span> Healing <span>Trợ lý vận hành</span></div>
    {running?<div role="status" className="loading"><span className="pulse"/> Đang tìm và kiểm tra thông tin…</div>:<MessagePrimitive.Content components={{Text:Markdown}}/>}
    {cancelled&&<p role="status" className="notice">Đã hủy chờ câu trả lời. Bạn có thể thử lại khi sẵn sàng.</p>}
    {response&&<ResponseDetails response={response}/>}
    {!running&&<ActionBarPrimitive.Root className="actions"><ActionBarPrimitive.Copy asChild copiedDuration={2000}><Button variant="ghost" size="default" title="Sao chép câu trả lời"><Copy size={14}/>Sao chép</Button></ActionBarPrimitive.Copy><ActionBarPrimitive.Reload asChild><Button variant="ghost" size="default" title="Mỗi lần thử lại sẽ gửi một yêu cầu mới tới mô hình"><RotateCcw size={14}/>Thử lại · lượt gọi mới</Button></ActionBarPrimitive.Reload></ActionBarPrimitive.Root>}
  </MessagePrimitive.Root>;
}
function CharCounter(){
  const text=useAuiState(s=>s.composer.text);
  const remaining=4000-text.length;
  if(remaining>200) return null;
  return <span aria-live="polite" className={remaining<0?"char-over":""}>{remaining}</span>;
}
function ScrollToBottom({viewportRef}:{viewportRef:React.RefObject<HTMLDivElement|null>}){
  const [atBottom,setAtBottom]=useState(true);
  useEffect(()=>{
    const el=viewportRef.current;
    if(!el) return;
    const check=()=>setAtBottom(el.scrollHeight-el.scrollTop-el.clientHeight<40);
    check();
    el.addEventListener("scroll",check,{passive:true});
    return ()=>el.removeEventListener("scroll",check);
  },[viewportRef]);
  if(atBottom) return null;
  return <Button variant="ghost" size="icon" className="scroll-to-bottom" aria-label="Cuộn xuống cuối" onClick={()=>viewportRef.current?.scrollTo({top:viewportRef.current.scrollHeight,behavior:"smooth"})}><ChevronsDown size={18}/></Button>;
}
function Conversation(){
  const running=useAuiState(s=>s.thread.isRunning);
  const aui=useAui();
  const viewportRef=useRef<HTMLDivElement|null>(null);
  return <ThreadPrimitive.Root className="thread">
    <ThreadPrimitive.Viewport ref={viewportRef} className="viewport">
      <ThreadPrimitive.Empty><div className="welcome"><span className="eyebrow"><span className="pulse"/> SELF HEALTHY KAFKA</span><h1>Cùng bạn giữ<br/><span>dữ liệu thông suốt.</span></h1><p>Phân tích sự cố, kiểm tra phục hồi và tìm hướng xử lý<br className="desktop-break"/> từ các runbook của hệ thống.</p><div className="suggestions">{suggestions.map(([title,question])=><button key={title} onClick={()=>aui.thread().append({role:"user",content:[{type:"text",text:question}]})}><span>{title}<ArrowRight size={15}/></span><small>{question}</small></button>)}</div></div></ThreadPrimitive.Empty>
      <div className="messages"><ThreadPrimitive.Messages components={{UserMessage,AssistantMessage}}/></div>
    </ThreadPrimitive.Viewport>
    <div className="composer-wrap"><ComposerPrimitive.Root className="composer"><ComposerPrimitive.Input aria-label="Câu hỏi" placeholder="Hỏi về sự cố hoặc cách xử lý…" rows={2} maxLength={4000} disabled={running}/><div className="composer-footer"><span><CharCounter/>Enter để gửi · Shift + Enter xuống dòng</span>{running?<ComposerPrimitive.Cancel asChild><Button size="icon" aria-label="Hủy yêu cầu"><Square size={16}/></Button></ComposerPrimitive.Cancel>:<ComposerPrimitive.Send asChild><Button size="icon" aria-label="Gửi câu hỏi"><ArrowUp size={18}/></Button></ComposerPrimitive.Send>}</div></ComposerPrimitive.Root><p className="footnote">Mỗi câu hỏi được xử lý độc lập. Kiểm tra nguồn trước khi thực hiện thao tác.</p></div>
    <ScrollToBottom viewportRef={viewportRef}/>
  </ThreadPrimitive.Root>;
}
function SessionHasMessages({onHasMessages}:{onHasMessages:(has:boolean)=>void}){
  const messageCount=useAuiState(s=>s.thread.messages.length);
  useEffect(()=>{onHasMessages(messageCount>0);},[messageCount,onHasMessages]);
  return null;
}
function Session({hidden,onHasMessages}:{hidden:boolean;onHasMessages:(has:boolean)=>void}){
  const runtime=useLocalRuntime(chatAdapter);
  return <div className="session" hidden={hidden}><AssistantRuntimeProvider runtime={runtime}><SessionHasMessages onHasMessages={onHasMessages}/><Conversation/></AssistantRuntimeProvider></div>;
}
type ChatSession={id:string;title:string};

export function Chat(){
  const [sessions,setSessions]=useState<ChatSession[]>([{id:"session-1",title:"Cuộc trò chuyện 1"}]);
  const [active,setActive]=useState("session-1");
  const [mobileSidebarOpen,setMobileSidebarOpen]=useState(false);
  const [desktopSidebarOpen,setDesktopSidebarOpen]=useState(true);
  const [deleteTarget,setDeleteTarget]=useState<string|null>(null);
  const [activeHasMessages,setActiveHasMessages]=useState(false);
  const dark=useDarkTheme();
  const isMobile=useMediaQuery("(max-width: 760px)");
  const nextSessionNumber=useRef(2);
  const deleteTriggers=useRef<Record<string,HTMLButtonElement|null>>({});
  const historyButtons=useRef<Record<string,HTMLButtonElement|null>>({});
  const confirmButton=useRef<HTMLButtonElement|null>(null);
  const focusAfterDelete=useRef<string|null>(null);
  // The sidebar collapses by width on desktop and slides off-canvas on mobile,
  // so the visible state depends on the current breakpoint.
  const sidebarOpen=isMobile?mobileSidebarOpen:desktopSidebarOpen;

  useEffect(()=>{ if(deleteTarget) confirmButton.current?.focus(); },[deleteTarget]);
  useEffect(()=>{
    const id=focusAfterDelete.current;
    if(!id) return;
    focusAfterDelete.current=null;
    historyButtons.current[id]?.focus();
  },[sessions]);
  useEffect(()=>{
    const onKeyDown=(event:KeyboardEvent)=>{
      if(event.key!=="Escape") return;
      if(deleteTarget){
        setDeleteTarget(null);
        deleteTriggers.current[deleteTarget]?.focus();
        return;
      }
      if(mobileSidebarOpen) setMobileSidebarOpen(false);
    };
    window.addEventListener("keydown",onKeyDown);
    return ()=>window.removeEventListener("keydown",onKeyDown);
  },[deleteTarget,mobileSidebarOpen]);

  const makeSession=():ChatSession=>{
    const number=nextSessionNumber.current++;
    return {id:`session-${Date.now()}-${number}`,title:`Cuộc trò chuyện ${number}`};
  };
  const closeMobileSidebar=()=>setMobileSidebarOpen(false);
  const createSession=()=>{
    const session=makeSession();
    setSessions(current=>[...current,session]);
    setActive(session.id);
    setDeleteTarget(null);
    closeMobileSidebar();
  };
  const selectSession=(id:string)=>{
    setActive(id);
    setDeleteTarget(null);
    closeMobileSidebar();
  };
  const deleteSession=(id:string)=>{
    const deletedIndex=sessions.findIndex(session=>session.id===id);
    if(deletedIndex<0)return;

    const remaining=sessions.filter(session=>session.id!==id);
    delete deleteTriggers.current[id];
    delete historyButtons.current[id];
    if(remaining.length===0){
      const replacement=makeSession();
      setSessions([replacement]);
      setActive(replacement.id);
      focusAfterDelete.current=replacement.id;
    }else{
      const neighbour=remaining[Math.min(deletedIndex,remaining.length-1)];
      setSessions(remaining);
      if(active===id){
        setActive(neighbour.id);
      }
      focusAfterDelete.current=neighbour.id;
    }
    setDeleteTarget(null);
  };
  const cancelDelete=(id:string)=>{
    setDeleteTarget(null);
    deleteTriggers.current[id]?.focus();
  };
  const toggleSidebar=()=>{
    if(isMobile){
      setMobileSidebarOpen(open=>!open);
    }else{
      setDesktopSidebarOpen(open=>!open);
    }
  };

  return <div className="app-shell">
    {mobileSidebarOpen&&<button className="backdrop" aria-label="Đóng lịch sử" onClick={closeMobileSidebar}/>}
    <header className="topbar">{!activeHasMessages&&<h1 className="sr-only">Trợ lý hệ thống Self Healthy Kafka</h1>}<div className="header-left"><Button variant="ghost" size="icon" aria-label="Ẩn hoặc mở lịch sử" aria-controls="session-sidebar" aria-expanded={sidebarOpen} onClick={toggleSidebar}><PanelLeft size={19}/></Button><span>Trợ lý hệ thống <small>Kafka Connect</small></span></div><Button variant="ghost" size="icon" aria-label={dark?"Giao diện sáng":"Giao diện tối"} onClick={()=>setDarkTheme(!dark)}>{dark?<Sun size={18}/>:<Moon size={18}/>}</Button></header>
    <div className="app-body">
      <aside id="session-sidebar" inert={!sidebarOpen} className={`sidebar ${mobileSidebarOpen?"mobile-open":""} ${desktopSidebarOpen?"":"desktop-closed"}`}>
        <div className="brand"><span><Activity size={22}/></span>healing<span className="brand-dot">.</span></div>
        <Button variant="outline" onClick={createSession}><Plus size={17}/>Cuộc trò chuyện mới</Button>
        <h2 className="sidebar-heading">TRONG PHIÊN NÀY</h2>
        <nav className="session-nav" aria-label="Lịch sử phiên">
          <ul className="session-list">
            {sessions.map(session=><li className="history-item" key={session.id}>
              <button ref={node=>{historyButtons.current[session.id]=node;}} className={active===session.id?"history active":"history"} onClick={()=>selectSession(session.id)}>
                <MessageSquare size={15}/><span>{session.title}</span>
              </button>
              <button ref={node=>{deleteTriggers.current[session.id]=node;}} className="history-delete" aria-label={`Xóa ${session.title}`} title={`Xóa ${session.title}`} aria-expanded={deleteTarget===session.id} onClick={()=>setDeleteTarget(session.id)}>
                <Trash2 size={15}/>
              </button>
              {deleteTarget===session.id&&<div className="history-confirm" role="group" aria-label={`Xác nhận xóa ${session.title}`}>
                <span>Xóa cuộc trò chuyện này?</span>
                <button ref={confirmButton} className="confirm-delete" onClick={()=>deleteSession(session.id)}>Xóa</button>
                <button onClick={()=>cancelDelete(session.id)}>Hủy</button>
              </div>}
            </li>)}
          </ul>
        </nav>
        <div className="sidebar-bottom"><div className="workspace-icon">SK</div><div><strong>Self Healthy Kafka</strong><small>Không gian vận hành</small></div></div>
      </aside>
      <main className="main">
        {sessions.map(session=><Session key={session.id} hidden={active!==session.id} onHasMessages={active===session.id?setActiveHasMessages:()=>{}}/>)}
      </main>
    </div>
  </div>;
}
