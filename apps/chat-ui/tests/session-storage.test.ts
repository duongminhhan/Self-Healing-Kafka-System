import { afterEach, describe, expect, it, vi } from "vitest";
import { defaultChatState, initialMessages, loadChatState, saveChatState, storeMessages, type StoredChatState } from "../src/lib/session-storage";

function storage(){
  const values=new Map<string,string>();
  return {
    getItem:(key:string)=>values.get(key)??null,
    setItem:(key:string,value:string)=>{values.set(key,value);},
    removeItem:(key:string)=>{values.delete(key);},
  };
}

afterEach(()=>{
  vi.unstubAllGlobals();
});

describe("chat session storage",()=>{
  it("round-trips bounded public sessions and stable ids",()=>{
    const local=storage();
    vi.stubGlobal("window",{});vi.stubGlobal("localStorage",local);
    const value:StoredChatState={sessions:[{id:"conversation-1",title:"Theo dõi orders",autoTitled:true,messages:[{id:"m-1",role:"user",text:"Connector orders gặp lỗi gì?"},{id:"m-2",role:"assistant",text:"Đã kiểm chứng."}]}],active:"conversation-1",nextSessionNumber:2};
    saveChatState(value);
    expect(loadChatState()).toEqual(value);
  });

  it("drops unsupported roles and stores public text only",()=>{
    const local=storage();
    vi.stubGlobal("window",{});vi.stubGlobal("localStorage",local);
    saveChatState({sessions:[{id:"conversation-1",title:"Chat",autoTitled:false,messages:[]}],active:"conversation-1",nextSessionNumber:2});
    const messages=storeMessages([
      {id:"m-1",role:"user",content:[{type:"text",text:"Question"}]},
      {id:"m-2",role:"system",content:[{type:"text",text:"ignored"}]},
    ]);
    expect(messages).toEqual([{id:"m-1",role:"user",text:"Question"}]);
    expect(JSON.stringify(messages)).not.toContain("secret");
  });

  it("hydrates transcript as assistant-ui messages only",()=>{
    expect(initialMessages([{id:"m-1",role:"user",text:"Question"}])).toEqual([
      {id:"m-1",role:"user",content:[{type:"text",text:"Question"}]},
    ]);
  });

  it("falls back safely when persisted JSON is invalid",()=>{
    const local=storage();
    vi.stubGlobal("window",{});vi.stubGlobal("localStorage",local);
    local.setItem("self-healthy-kafka:chat-state:v1","not-json");
    expect(loadChatState()).toEqual(defaultChatState());
  });
});
