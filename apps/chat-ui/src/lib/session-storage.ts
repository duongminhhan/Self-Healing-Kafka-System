import type { ThreadMessageLike } from "@assistant-ui/react";

const KEY="self-healthy-kafka:chat-state:v1";
const ID=/^[A-Za-z0-9._:-]{1,128}$/;
const MAX_SESSIONS=20;
const MAX_MESSAGES=40;
const MAX_TEXT=12_000;

export type StoredMessage={id:string;role:"user"|"assistant";text:string};
export type StoredSession={id:string;title:string;autoTitled:boolean;messages:StoredMessage[]};
export type StoredChatState={sessions:StoredSession[];active:string|null;nextSessionNumber:number};

export const defaultChatState=():StoredChatState=>({
  sessions:[{id:"session-1",title:"Cuộc trò chuyện 1",autoTitled:false,messages:[]}],
  active:"session-1",
  nextSessionNumber:2,
});

export function loadChatState():StoredChatState {
  if(typeof window==="undefined")return defaultChatState();
  try{
    const value=JSON.parse(localStorage.getItem(KEY)??"") as Partial<StoredChatState>;
    if(!Array.isArray(value.sessions))return defaultChatState();
    const sessions=value.sessions.slice(0,MAX_SESSIONS).flatMap(parseSession);
    const active=typeof value.active==="string"&&sessions.some(item=>item.id===value.active)?value.active:sessions[0]?.id??null;
    const next=Number.isInteger(value.nextSessionNumber)&&Number(value.nextSessionNumber)>0?Number(value.nextSessionNumber):sessions.length+1;
    return {sessions,active,nextSessionNumber:next};
  }catch{return defaultChatState();}
}

export function saveChatState(value:StoredChatState) {
  if(typeof window==="undefined")return;
  try{localStorage.setItem(KEY,JSON.stringify({...value,sessions:value.sessions.slice(-MAX_SESSIONS)}));}catch{}
}

export function storeMessages(messages:readonly {id:string;role:string;content:readonly {type:string;text?:string}[]}[]):StoredMessage[] {
  return messages.flatMap(message=>{
    if(message.role!=="user"&&message.role!=="assistant")return [];
    const role:StoredMessage["role"] = message.role === "user" ? "user" : "assistant";
    const text=message.content.filter(part=>part.type==="text"&&typeof part.text==="string").map(part=>part.text).join("\n").trim();
    return text?[{id:ID.test(message.id)?message.id:crypto.randomUUID(),role,text:text.slice(0,MAX_TEXT)}]:[];
  }).slice(-MAX_MESSAGES);
}

export function initialMessages(messages:StoredMessage[]):ThreadMessageLike[] {
  return messages.map(message=>({id:message.id,role:message.role,content:[{type:"text",text:message.text}]}));
}

function parseSession(value:unknown):StoredSession[] {
  if(!value||typeof value!=="object")return [];
  const item=value as Partial<StoredSession>;
  if(typeof item.id!=="string"||!ID.test(item.id)||typeof item.title!=="string")return [];
  const messages=Array.isArray(item.messages)?item.messages.slice(-MAX_MESSAGES).flatMap(parseMessage):[];
  return [{id:item.id,title:item.title.slice(0,80),autoTitled:item.autoTitled===true,messages}];
}

function parseMessage(value:unknown):StoredMessage[] {
  if(!value||typeof value!=="object")return [];
  const item=value as Partial<StoredMessage>;
  if(typeof item.id!=="string"||!ID.test(item.id)||!item.text||typeof item.text!=="string")return [];
  if(item.role!=="user"&&item.role!=="assistant")return [];
  return [{id:item.id,role:item.role,text:item.text.slice(0,MAX_TEXT)}];
}
