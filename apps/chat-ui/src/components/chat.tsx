"use client";
import { createContext, useContext, useEffect, useMemo, useRef, useState } from "react";
import { AssistantRuntimeProvider, useLocalRuntime, ThreadPrimitive, ComposerPrimitive, MessagePrimitive, ActionBarPrimitive, useAuiState, useAui } from "@assistant-ui/react";
import { Activity, Plus, ArrowUp, Square, RotateCcw, Moon, Sun, PanelLeft, MessageSquare, ArrowRight, Trash2, Copy, ChevronsDown, Search, ChevronUp, ChevronDown, X, Pencil, Check } from "lucide-react";
import ReactMarkdown from "react-markdown";
import remarkGfm from "remark-gfm";
import { Button } from "@/components/ui/button";
import { makeChatAdapter } from "@/lib/adapter";
import type { ChatResponse } from "@/lib/contract";
import { safeLink } from "@/lib/contract";
import { formatElapsedTime, normalizeSearchText, sessionTitleFromQuestion, type UiTiming } from "@/lib/ui-utils";
import { useDarkTheme, useMediaQuery, setDarkTheme } from "@/lib/theme";
import { ResponseDetails } from "./response-details";

const suggestions=[
  ["Phân tích sự cố","Connector nào gặp nhiều sự cố nhất trong 7 ngày qua?"],
  ["Tìm hướng xử lý","Làm sao xử lý lỗi ORA-01017 trên Oracle connector?"],
  ["Kiểm tra phục hồi","Tỷ lệ phục hồi thành công tuần này là bao nhiêu?"],
  ["Tra cứu runbook","Kafka Connect bị connection timeout, tôi nên kiểm tra gì?"],
];
const ActiveSearchMessageContext=createContext<string|null>(null);

function Markdown({text}:{text:string}) {
  return <div className="markdown"><ReactMarkdown remarkPlugins={[remarkGfm]} components={{img:({alt})=><span>{alt??"[Image omitted]"}</span>,a:({href,children})=>safeLink(href)?<a href={safeLink(href)} target="_blank" rel="noopener noreferrer">{children}</a>:<span>{children}</span>}}>{text}</ReactMarkdown></div>;
}
function UserMessage(){
  const message=useAuiState(state=>state.message);
  const active=useContext(ActiveSearchMessageContext)===message.id;
  return <MessagePrimitive.Root id={`message-${message.id}`} data-message-id={message.id} className={`user-message searchable-message${active?" search-selected":""}`}><MessagePrimitive.Content components={{Text:Markdown}}/></MessagePrimitive.Root>;
}
function AssistantMessage(){
  const message=useAuiState(state=>state.message);
  const aui=useAui();
  const active=useContext(ActiveSearchMessageContext)===message.id;
  const running=message.status?.type==="running";
  const cancelled=message.status?.type==="incomplete"&&message.status.reason==="cancelled";
  const custom=message.metadata.custom as {response?:ChatResponse;ui_timing?:UiTiming;errorCode?:string};
  const response=custom.response;
  const timing=custom.ui_timing;
  const [liveElapsed,setLiveElapsed]=useState(0);
  useEffect(()=>{
    if(!running)return;
    const start=performance.now();
    const frame=window.requestAnimationFrame(()=>setLiveElapsed(0));
    const update=()=>setLiveElapsed(Math.max(0,performance.now()-start));
    const interval=window.setInterval(update,100);
    return ()=>{update();window.cancelAnimationFrame(frame);window.clearInterval(interval);};
  },[running]);
  const resultId=`verified-result-${message.id}`;
  const followUp=(text:string)=>aui.thread().append({role:"user",content:[{type:"text",text}]});
  const showQuickActions=!running&&!cancelled&&!!response?.answer?.trim()&&response.status!=="no_answer";
  return <MessagePrimitive.Root id={`message-${message.id}`} data-message-id={message.id} className={`assistant-message searchable-message${active?" search-selected":""}`}>
    <div className="assistant-label"><span className="mini-brand"><Activity size={15}/></span> Healing <span>Trợ lý vận hành</span></div>
    {running?<div role="status" aria-live="polite" className="loading"><span className="pulse"/> Đang tìm và kiểm tra thông tin… <span className="elapsed">{formatElapsedTime(liveElapsed)}</span></div>:<MessagePrimitive.Content components={{Text:Markdown}}/>}
    {cancelled&&<p role="status" className="notice">Đã hủy chờ câu trả lời{liveElapsed>0?` sau ${formatElapsedTime(liveElapsed)}`:""}. Bạn có thể thử lại khi sẵn sàng.</p>}
    {response&&<ResponseDetails response={response} timing={timing} resultId={resultId}/>}
    {!response&&timing&&!cancelled&&<div className="response-meta" aria-label="Thông tin phản hồi" aria-live="polite"><span title="Thời gian end-to-end quan sát từ trình duyệt">Đã chờ {formatElapsedTime(timing.elapsed_ms)}</span></div>}
    {showQuickActions&&<div className="quick-actions" aria-label="Gợi ý thao tác">
      <button type="button" onClick={()=>followUp("Hãy giải thích câu trả lời vừa rồi ngắn gọn và dễ hiểu hơn.")}>Giải thích ngắn hơn</button>
      <button type="button" onClick={()=>followUp("Dựa trên câu trả lời vừa rồi, hãy đề xuất bước tiếp theo phù hợp.")}>Đề xuất bước tiếp theo</button>
      {!!response?.verified_result?.rows?.length&&<button type="button" onClick={()=>document.getElementById(resultId)?.scrollIntoView({behavior:"smooth",block:"center"})}>Xem dữ liệu xác minh</button>}
    </div>}
    {!running&&<ActionBarPrimitive.Root className="actions"><ActionBarPrimitive.Copy asChild copiedDuration={2000}><Button variant="ghost" size="default" title="Sao chép câu trả lời"><Copy size={14}/>Sao chép</Button></ActionBarPrimitive.Copy><ActionBarPrimitive.Reload asChild><Button variant="ghost" size="default" title="Mỗi lần thử lại sẽ gửi một yêu cầu mới tới mô hình"><RotateCcw size={14}/>Thử lại · lượt gọi mới</Button></ActionBarPrimitive.Reload></ActionBarPrimitive.Root>}
  </MessagePrimitive.Root>;
}
function CharCounter(){
  const text=useAuiState(state=>state.composer.text);
  const remaining=4000-text.length;
  if(remaining>200) return null;
  return <span aria-live="polite" className={remaining<0?"char-over":""}>{remaining}</span>;
}
function ScrollToBottom({viewportRef}:{viewportRef:React.RefObject<HTMLDivElement|null>}){
  const [atBottom,setAtBottom]=useState(true);
  useEffect(()=>{
    const element=viewportRef.current;
    if(!element) return;
    const check=()=>setAtBottom(element.scrollHeight-element.scrollTop-element.clientHeight<40);
    check();element.addEventListener("scroll",check,{passive:true});
    return ()=>element.removeEventListener("scroll",check);
  },[viewportRef]);
  if(atBottom) return null;
  return <Button variant="ghost" size="icon" className="scroll-to-bottom" aria-label="Cuộn xuống cuối" onClick={()=>viewportRef.current?.scrollTo({top:viewportRef.current.scrollHeight,behavior:"smooth"})}><ChevronsDown size={18}/></Button>;
}

function ConversationSearch({open,onClose,onActiveMessage}:{open:boolean;onClose:()=>void;onActiveMessage:(id:string|null)=>void}){
  const messages=useAuiState(state=>state.thread.messages);
  const [input,setInput]=useState("");
  const [query,setQuery]=useState("");
  const [current,setCurrent]=useState(-1);
  const inputRef=useRef<HTMLInputElement|null>(null);
  useEffect(()=>{const timeout=window.setTimeout(()=>setQuery(input),150);return()=>window.clearTimeout(timeout);},[input]);
  const matches=useMemo(()=>{
    const needle=normalizeSearchText(query);
    if(!needle)return [];
    return messages.flatMap(message=>{
      if(message.role!=="user"&&message.role!=="assistant")return [];
      const text=message.content.filter(part=>part.type==="text").map(part=>part.text).join("\n");
      const normalized=normalizeSearchText(text);
      const found:{id:string}[]=[];
      let offset=0;
      while((offset=normalized.indexOf(needle,offset))!==-1){found.push({id:message.id});offset+=Math.max(1,needle.length);}
      return found;
    });
  },[messages,query]);
  useEffect(()=>{if(open)inputRef.current?.focus();},[open]);
  useEffect(()=>{
    const boundedCurrent=matches.length?Math.min(Math.max(current,0),matches.length-1):-1;
    const id=boundedCurrent>=0?matches[boundedCurrent]?.id:null;
    onActiveMessage(id??null);
    if(id)document.getElementById(`message-${id}`)?.scrollIntoView({behavior:"smooth",block:"center"});
  },[current,matches,onActiveMessage]);
  const boundedCurrent=matches.length?Math.min(Math.max(current,0),matches.length-1):-1;
  const move=(delta:number)=>{if(matches.length)setCurrent((boundedCurrent+delta+matches.length)%matches.length);};
  if(!open)return null;
  return <div className="conversation-search" role="search" onKeyDown={event=>{
    if(event.key==="Escape"){event.preventDefault();onClose();}
    if(event.key==="Enter"){event.preventDefault();move(event.shiftKey?-1:1);}
  }}>
    <Search size={16}/><input ref={inputRef} value={input} onChange={event=>{setInput(event.target.value);setCurrent(0);}} aria-label="Tìm trong cuộc trò chuyện" placeholder="Tìm trong cuộc trò chuyện…"/>
    <span className="search-count" aria-live="polite">{query?(matches.length?`${boundedCurrent+1} / ${matches.length}`:"Không có kết quả"):"Nhập từ khóa"}</span>
    <Button variant="ghost" size="icon" aria-label="Kết quả trước" disabled={!matches.length} onClick={()=>move(-1)}><ChevronUp size={16}/></Button>
    <Button variant="ghost" size="icon" aria-label="Kết quả sau" disabled={!matches.length} onClick={()=>move(1)}><ChevronDown size={16}/></Button>
    <Button variant="ghost" size="icon" aria-label="Đóng tìm kiếm" onClick={onClose}><X size={17}/></Button>
  </div>;
}

function Conversation({searchOpen,onCloseSearch}:{searchOpen:boolean;onCloseSearch:()=>void}){
  const running=useAuiState(state=>state.thread.isRunning);
  const aui=useAui();
  const viewportRef=useRef<HTMLDivElement|null>(null);
  const [activeSearchMessage,setActiveSearchMessage]=useState<string|null>(null);
  return <ActiveSearchMessageContext.Provider value={searchOpen?activeSearchMessage:null}>
    <ThreadPrimitive.Root className="thread">
      <ConversationSearch open={searchOpen} onClose={onCloseSearch} onActiveMessage={setActiveSearchMessage}/>
      <ThreadPrimitive.Viewport ref={viewportRef} className="viewport">
        <ThreadPrimitive.Empty><div className="welcome"><span className="eyebrow"><span className="pulse"/> SELF HEALTHY KAFKA</span><h1>Cùng bạn giữ<br/><span>dữ liệu thông suốt.</span></h1><p>Phân tích sự cố, kiểm tra phục hồi và tìm hướng xử lý<br className="desktop-break"/> từ các runbook của hệ thống.</p><div className="suggestions">{suggestions.map(([title,question])=><button key={title} onClick={()=>aui.thread().append({role:"user",content:[{type:"text",text:question}]})}><span>{title}<ArrowRight size={15}/></span><small>{question}</small></button>)}</div></div></ThreadPrimitive.Empty>
        <div className="messages"><ThreadPrimitive.Messages components={{UserMessage,AssistantMessage}}/></div>
      </ThreadPrimitive.Viewport>
      <div className="composer-wrap"><ComposerPrimitive.Root className="composer"><ComposerPrimitive.Input aria-label="Câu hỏi" placeholder="Hỏi về sự cố hoặc cách xử lý…" rows={2} maxLength={4000} disabled={running}/><div className="composer-footer"><span><CharCounter/>Enter để gửi · Shift + Enter xuống dòng</span>{running?<ComposerPrimitive.Cancel asChild><Button size="icon" aria-label="Hủy yêu cầu"><Square size={16}/></Button></ComposerPrimitive.Cancel>:<ComposerPrimitive.Send asChild><Button size="icon" aria-label="Gửi câu hỏi"><ArrowUp size={18}/></Button></ComposerPrimitive.Send>}</div></ComposerPrimitive.Root><p className="footnote">Câu hỏi tiếp theo có thể kế thừa ngữ cảnh đã kiểm chứng trong cuộc trò chuyện này. Kiểm tra nguồn trước khi thực hiện thao tác.</p></div>
      <ScrollToBottom viewportRef={viewportRef}/>
    </ThreadPrimitive.Root>
  </ActiveSearchMessageContext.Provider>;
}
function SessionObserver({onHasMessages,onFirstQuestion}:{onHasMessages:(has:boolean)=>void;onFirstQuestion:(question:string)=>void}){
  const messages=useAuiState(state=>state.thread.messages);
  const reportedFirstQuestion=useRef(false);
  const firstQuestion=messages.find(message=>message.role==="user")?.content.filter(part=>part.type==="text").map(part=>part.text).join(" ")??"";
  useEffect(()=>{onHasMessages(messages.length>0);},[messages.length,onHasMessages]);
  useEffect(()=>{if(firstQuestion&&!reportedFirstQuestion.current){reportedFirstQuestion.current=true;onFirstQuestion(firstQuestion);}},[firstQuestion,onFirstQuestion]);
  return null;
}
function Session({id,hidden,onHasMessages,onFirstQuestion,searchOpen,onCloseSearch}:{id:string;hidden:boolean;onHasMessages:(has:boolean)=>void;onFirstQuestion:(question:string)=>void;searchOpen:boolean;onCloseSearch:()=>void}){
  const adapter=useMemo(()=>makeChatAdapter(id),[id]);
  const runtime=useLocalRuntime(adapter);
  return <div className="session" hidden={hidden}><AssistantRuntimeProvider runtime={runtime}><SessionObserver onHasMessages={onHasMessages} onFirstQuestion={onFirstQuestion}/><Conversation searchOpen={searchOpen} onCloseSearch={onCloseSearch}/></AssistantRuntimeProvider></div>;
}
type ChatSession={id:string;title:string;autoTitled:boolean};

export function Chat(){
  const [sessions,setSessions]=useState<ChatSession[]>([{id:"session-1",title:"Cuộc trò chuyện 1",autoTitled:false}]);
  const [active,setActive]=useState<string|null>("session-1");
  const [mobileSidebarOpen,setMobileSidebarOpen]=useState(false);
  const [desktopSidebarOpen,setDesktopSidebarOpen]=useState(true);
  const [deleteTarget,setDeleteTarget]=useState<string|null>(null);
  const [activeHasMessages,setActiveHasMessages]=useState(false);
  const [searchOpen,setSearchOpen]=useState(false);
  const [editingSession,setEditingSession]=useState<string|null>(null);
  const [renameDraft,setRenameDraft]=useState("");
  const dark=useDarkTheme();
  const isMobile=useMediaQuery("(max-width: 760px)");
  const nextSessionNumber=useRef(2);
  const deleteTriggers=useRef<Record<string,HTMLButtonElement|null>>({});
  const historyButtons=useRef<Record<string,HTMLButtonElement|null>>({});
  const renameInputs=useRef<Record<string,HTMLInputElement|null>>({});
  const confirmButton=useRef<HTMLButtonElement|null>(null);
  const searchButton=useRef<HTMLButtonElement|null>(null);
  const focusAfterDelete=useRef<string|null>(null);
  const sidebarOpen=isMobile?mobileSidebarOpen:desktopSidebarOpen;

  const closeSearch=()=>{setSearchOpen(false);requestAnimationFrame(()=>searchButton.current?.focus());};
  useEffect(()=>{if(deleteTarget)confirmButton.current?.focus();},[deleteTarget]);
  useEffect(()=>{if(editingSession)renameInputs.current[editingSession]?.focus();},[editingSession]);
  useEffect(()=>{
    const id=focusAfterDelete.current;if(!id)return;focusAfterDelete.current=null;historyButtons.current[id]?.focus();
  },[sessions]);
  useEffect(()=>{
    const onKeyDown=(event:KeyboardEvent)=>{
      if((event.ctrlKey||event.metaKey)&&event.key.toLocaleLowerCase()==="k"){
        event.preventDefault();if(active){setSearchOpen(true);}return;
      }
      if(event.key!=="Escape")return;
      if(searchOpen){closeSearch();return;}
      if(deleteTarget){setDeleteTarget(null);deleteTriggers.current[deleteTarget]?.focus();return;}
      if(mobileSidebarOpen)setMobileSidebarOpen(false);
    };
    window.addEventListener("keydown",onKeyDown);return()=>window.removeEventListener("keydown",onKeyDown);
  },[active,deleteTarget,mobileSidebarOpen,searchOpen]);

  const makeSession=():ChatSession=>{const number=nextSessionNumber.current++;return{id:`session-${Date.now()}-${number}`,title:`Cuộc trò chuyện ${number}`,autoTitled:false};};
  const closeMobileSidebar=()=>setMobileSidebarOpen(false);
  const createSession=()=>{const session=makeSession();setSessions(current=>[...current,session]);setActive(session.id);setDeleteTarget(null);setSearchOpen(false);closeMobileSidebar();};
  const selectSession=(id:string)=>{setActive(id);setDeleteTarget(null);setSearchOpen(false);closeMobileSidebar();};
  const autoTitle=(id:string,question:string)=>setSessions(current=>current.map(session=>session.id===id&&!session.autoTitled?{...session,title:sessionTitleFromQuestion(question),autoTitled:true}:session));
  const startRename=(session:ChatSession)=>{setEditingSession(session.id);setRenameDraft(session.title);};
  const cancelRename=()=>{const id=editingSession;setEditingSession(null);if(id)requestAnimationFrame(()=>historyButtons.current[id]?.focus());};
  const saveRename=(id:string)=>{const title=renameDraft.replace(/\s+/g," ").trim();if(title)setSessions(current=>current.map(session=>session.id===id?{...session,title:title.slice(0,80),autoTitled:true}:session));setEditingSession(null);requestAnimationFrame(()=>historyButtons.current[id]?.focus());};
  const deleteSession=(id:string)=>{
    const deletedIndex=sessions.findIndex(session=>session.id===id);if(deletedIndex<0)return;
    const remaining=sessions.filter(session=>session.id!==id);delete deleteTriggers.current[id];delete historyButtons.current[id];delete renameInputs.current[id];
    if(remaining.length===0){setSessions([]);setActive(null);setSearchOpen(false);}else{const neighbour=remaining[Math.min(deletedIndex,remaining.length-1)];setSessions(remaining);if(active===id)setActive(neighbour.id);focusAfterDelete.current=neighbour.id;}
    setDeleteTarget(null);
  };
  const cancelDelete=(id:string)=>{setDeleteTarget(null);deleteTriggers.current[id]?.focus();};
  const toggleSidebar=()=>{if(isMobile)setMobileSidebarOpen(open=>!open);else setDesktopSidebarOpen(open=>!open);};

  return <div className="app-shell">
    {mobileSidebarOpen&&<button className="backdrop" aria-label="Đóng lịch sử" onClick={closeMobileSidebar}/>}
    <header className="topbar">{!activeHasMessages&&<h1 className="sr-only">Trợ lý hệ thống Self Healthy Kafka</h1>}<div className="header-left"><Button variant="ghost" size="icon" aria-label="Ẩn hoặc mở lịch sử" aria-controls="session-sidebar" aria-expanded={sidebarOpen} onClick={toggleSidebar}><PanelLeft size={19}/></Button><span>Trợ lý hệ thống <small>Kafka Connect</small></span></div><div className="topbar-actions"><Button ref={searchButton} variant="ghost" size="icon" aria-label="Tìm trong cuộc trò chuyện" aria-keyshortcuts="Control+K Meta+K" disabled={!active} onClick={()=>setSearchOpen(true)}><Search size={18}/></Button><Button variant="ghost" size="icon" aria-label={dark?"Giao diện sáng":"Giao diện tối"} onClick={()=>setDarkTheme(!dark)}>{dark?<Sun size={18}/>:<Moon size={18}/>}</Button></div></header>
    <div className="app-body">
      <aside id="session-sidebar" inert={!sidebarOpen} className={`sidebar ${mobileSidebarOpen?"mobile-open":""} ${desktopSidebarOpen?"":"desktop-closed"}`}>
        <div className="brand"><span><Activity size={22}/></span>healing<span className="brand-dot">.</span></div>
        <Button variant="outline" onClick={createSession}><Plus size={17}/>Cuộc trò chuyện mới</Button>
        <h2 className="sidebar-heading">TRONG PHIÊN NÀY</h2>
        <nav className="session-nav" aria-label="Lịch sử phiên"><ul className="session-list">{sessions.map(session=><li className="history-item" key={session.id}>
          {editingSession===session.id?<form className="history-rename" onSubmit={event=>{event.preventDefault();saveRename(session.id);}}><input ref={node=>{renameInputs.current[session.id]=node;}} value={renameDraft} maxLength={80} aria-label={`Tên mới cho ${session.title}`} onChange={event=>setRenameDraft(event.target.value)} onKeyDown={event=>{if(event.key==="Escape"){event.preventDefault();cancelRename();}}}/><button type="submit" aria-label="Lưu tên" disabled={!renameDraft.trim()}><Check size={15}/></button><button type="button" aria-label="Hủy đổi tên" onClick={cancelRename}><X size={15}/></button></form>:<>
            <button ref={node=>{historyButtons.current[session.id]=node;}} className={active===session.id?"history active":"history"} onClick={()=>selectSession(session.id)}><MessageSquare size={15}/><span>{session.title}</span></button>
            <button className="history-edit" aria-label={`Đổi tên ${session.title}`} title={`Đổi tên ${session.title}`} onClick={()=>startRename(session)}><Pencil size={14}/></button>
            <button ref={node=>{deleteTriggers.current[session.id]=node;}} className="history-delete" aria-label={`Xóa ${session.title}`} title={`Xóa ${session.title}`} aria-expanded={deleteTarget===session.id} onClick={()=>setDeleteTarget(session.id)}><Trash2 size={15}/></button>
          </>}
          {deleteTarget===session.id&&<div className="history-confirm" role="group" aria-label={`Xác nhận xóa ${session.title}`}><span>Xóa cuộc trò chuyện này?</span><button ref={confirmButton} className="confirm-delete" onClick={()=>deleteSession(session.id)}>Xóa</button><button onClick={()=>cancelDelete(session.id)}>Hủy</button></div>}
        </li>)}</ul></nav>
        <div className="sidebar-bottom"><div className="workspace-icon">SK</div><div><strong>Self Healthy Kafka</strong><small>Không gian vận hành</small></div></div>
      </aside>
      <main className="main">{sessions.length===0?<div className="no-sessions"><p>Chưa có cuộc trò chuyện nào.</p><Button variant="outline" onClick={createSession}><Plus size={17}/>Cuộc trò chuyện mới</Button></div>:sessions.map(session=><Session key={session.id} id={session.id} hidden={active!==session.id} onHasMessages={active===session.id?setActiveHasMessages:()=>{}} onFirstQuestion={question=>autoTitle(session.id,question)} searchOpen={active===session.id&&searchOpen} onCloseSearch={closeSearch}/>)}</main>
    </div>
  </div>;
}
