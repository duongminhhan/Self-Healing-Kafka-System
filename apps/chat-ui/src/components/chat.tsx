"use client";
import { useState } from "react";
import { AssistantRuntimeProvider, useLocalRuntime, ThreadPrimitive, ComposerPrimitive, MessagePrimitive, ActionBarPrimitive, useAuiState, useAui } from "@assistant-ui/react";
import { Activity, Plus, ArrowUp, Square, RotateCcw, Moon, Sun, PanelLeft, MessageSquare, ArrowRight } from "lucide-react";
import ReactMarkdown from "react-markdown";
import remarkGfm from "remark-gfm";
import { Button } from "@/components/ui/button";
import { chatAdapter } from "@/lib/adapter";
import type { ChatResponse } from "@/lib/contract";
import { safeLink } from "@/lib/contract";
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
    {!running&&<ActionBarPrimitive.Root className="actions"><ActionBarPrimitive.Reload asChild><Button variant="ghost" size="default" title="Mỗi lần thử lại sẽ gửi một yêu cầu mới tới mô hình"><RotateCcw size={14}/>Thử lại · lượt gọi mới</Button></ActionBarPrimitive.Reload></ActionBarPrimitive.Root>}
  </MessagePrimitive.Root>;
}
function Conversation(){
  const running=useAuiState(s=>s.thread.isRunning);
  const aui=useAui();
  return <ThreadPrimitive.Root className="thread">
    <ThreadPrimitive.Viewport className="viewport">
      <ThreadPrimitive.Empty><div className="welcome"><span className="eyebrow"><span className="pulse"/> SELF HEALTHY KAFKA</span><h1>Cùng bạn giữ<br/><span>dữ liệu thông suốt.</span></h1><p>Phân tích sự cố, kiểm tra phục hồi và tìm hướng xử lý<br className="desktop-break"/> từ các runbook của hệ thống.</p><div className="suggestions">{suggestions.map(([title,question])=><button key={title} onClick={()=>aui.thread().append({role:"user",content:[{type:"text",text:question}]})}><span>{title}<ArrowRight size={15}/></span><small>{question}</small></button>)}</div></div></ThreadPrimitive.Empty>
      <div className="messages"><ThreadPrimitive.Messages components={{UserMessage,AssistantMessage}}/></div>
    </ThreadPrimitive.Viewport>
    <div className="composer-wrap"><ComposerPrimitive.Root className="composer"><ComposerPrimitive.Input aria-label="Câu hỏi" placeholder="Hỏi về sự cố hoặc cách xử lý…" rows={2} maxLength={4000} disabled={running}/><div className="composer-footer"><span>Enter để gửi · Shift + Enter xuống dòng</span>{running?<ComposerPrimitive.Cancel asChild><Button size="icon" aria-label="Hủy yêu cầu"><Square size={16}/></Button></ComposerPrimitive.Cancel>:<ComposerPrimitive.Send asChild><Button size="icon" aria-label="Gửi câu hỏi"><ArrowUp size={18}/></Button></ComposerPrimitive.Send>}</div></ComposerPrimitive.Root><p className="footnote">Mỗi câu hỏi được xử lý độc lập. Kiểm tra nguồn trước khi thực hiện thao tác.</p></div>
  </ThreadPrimitive.Root>;
}
function Session({hidden}:{hidden:boolean}){
  const runtime=useLocalRuntime(chatAdapter);
  return <div className="session" hidden={hidden}><AssistantRuntimeProvider runtime={runtime}><Conversation/></AssistantRuntimeProvider></div>;
}
export function Chat(){
  const [sessions,setSessions]=useState(["1"]); const [active,setActive]=useState("1");
  const [dark,setDark]=useState(false);const [sidebar,setSidebar]=useState(false);
  return <div className={`app-shell ${dark?"dark":""}`}>
    {sidebar&&<button className="backdrop" aria-label="Đóng lịch sử" onClick={()=>setSidebar(false)}/>}
    <aside className={sidebar?"sidebar open":"sidebar"}><div className="brand"><span><Activity size={22}/></span>healing<span className="brand-dot">.</span></div><Button variant="outline" onClick={()=>{const id=String(Date.now());setSessions(s=>[...s,id]);setActive(id);setSidebar(false);}}><Plus size={17}/>Cuộc trò chuyện mới</Button><div className="sidebar-heading">TRONG PHIÊN NÀY</div><nav aria-label="Lịch sử phiên">{sessions.map((id,i)=><button className={active===id?"history active":"history"} key={id} onClick={()=>{setActive(id);setSidebar(false);}}><MessageSquare size={15}/><span>Cuộc trò chuyện {i+1}</span></button>)}</nav><div className="sidebar-bottom"><div className="workspace-icon">SK</div><div><strong>Self Healthy Kafka</strong><small>Không gian vận hành</small></div></div></aside>
    <main className="main"><header><div className="header-left"><Button variant="ghost" size="icon" aria-label="Mở lịch sử" onClick={()=>setSidebar(!sidebar)}><PanelLeft size={19}/></Button><span>Trợ lý hệ thống <small>Kafka Connect</small></span></div><Button variant="ghost" size="icon" aria-label={dark?"Giao diện sáng":"Giao diện tối"} onClick={()=>setDark(!dark)}>{dark?<Sun size={18}/>:<Moon size={18}/>}</Button></header>{sessions.map(id=><Session key={id} hidden={active!==id}/>)}</main>
  </div>;
}
