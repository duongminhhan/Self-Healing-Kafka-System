from __future__ import annotations

"""Static, same-origin browser UI for the read-only local chat API."""


def page() -> bytes:
    """Return the chat UI without requiring a separate frontend runtime."""
    return _PAGE.encode("utf-8")


_PAGE = r'''<!doctype html>
<html lang="vi">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <title>Self-Healing Kafka Assistant</title>
  <style>
    :root { color-scheme: dark; --bg:#0b1220; --panel:#111c2e; --line:#263650; --text:#e6edf7; --muted:#98a8be; --cyan:#22d3ee; --green:#34d399; --red:#fb7185; }
    * { box-sizing:border-box; } body { margin:0; background:linear-gradient(145deg,#07101e,#111827); color:var(--text); font:15px/1.5 system-ui,-apple-system,Segoe UI,sans-serif; }
    main { max-width:980px; min-height:100vh; margin:auto; padding:28px 18px; display:flex; flex-direction:column; gap:18px; }
    header,.composer,.message,.suggestions { border:1px solid var(--line); background:rgba(17,28,46,.94); border-radius:14px; }
    header { padding:18px 20px; display:flex; justify-content:space-between; align-items:center; gap:16px; } h1 { font-size:20px; margin:0; } .subtle { color:var(--muted); font-size:13px; }
    #health { white-space:nowrap; font-weight:650; } #health::before { content:""; display:inline-block; width:9px; height:9px; margin-right:7px; border-radius:50%; background:var(--red); } #health.online::before { background:var(--green); }
    #messages { flex:1; display:flex; flex-direction:column; gap:12px; min-height:360px; } .message { padding:15px 17px; max-width:88%; white-space:pre-wrap; overflow-wrap:anywhere; } .message.user { align-self:flex-end; background:#12334a; border-color:#1b5c78; } .message.assistant { align-self:flex-start; } .message.error { border-color:#9f2741; color:#fecdd3; }
    .role { color:var(--cyan); font-size:12px; font-weight:750; letter-spacing:.06em; text-transform:uppercase; margin-bottom:6px; } .user .role { color:#a5f3fc; }
    details { margin-top:12px; border-top:1px solid var(--line); padding-top:9px; } summary { cursor:pointer; color:#bceff7; } .evidence { margin-top:10px; display:grid; gap:8px; } .log { border:1px solid var(--line); background:#0a1424; border-radius:8px; padding:10px; font-size:13px; } .log strong { color:var(--cyan); } .log p { margin:5px 0 0; color:#cbd5e1; white-space:pre-wrap; overflow-wrap:anywhere; }
    .suggestions { padding:12px; display:flex; gap:8px; flex-wrap:wrap; } button { font:inherit; cursor:pointer; } .suggestion { color:#c9f7ff; background:#113249; border:1px solid #22617b; padding:7px 10px; border-radius:99px; font-size:13px; } .composer { padding:12px; } textarea { width:100%; resize:vertical; min-height:58px; border:1px solid var(--line); border-radius:9px; padding:11px; color:var(--text); background:#091322; font:inherit; } .actions { margin-top:9px; display:flex; justify-content:space-between; align-items:center; gap:12px; } #send { background:var(--cyan); color:#06202a; border:0; border-radius:8px; padding:9px 16px; font-weight:750; } #send:disabled { opacity:.55; cursor:wait; } #token { width:220px; max-width:52vw; border:1px solid var(--line); border-radius:7px; background:#091322; color:var(--text); padding:7px 9px; }
    @media (max-width:560px) { main { padding:14px 10px; } header { align-items:flex-start; flex-direction:column; } .message { max-width:96%; } .actions { align-items:flex-end; } }
  </style>
</head>
<body><main>
  <header><div><h1>Self-Healing Kafka Assistant</h1><div class="subtle">Tra cứu read-only từ ConnectorHealingLogs</div></div><div id="health" aria-live="polite">Backend offline</div></header>
  <section id="messages" aria-live="polite"><article class="message assistant"><div class="role">Assistant</div>Chào bạn. Hãy đặt câu hỏi về Kafka Connect, Debezium hoặc Oracle.</article></section>
  <section class="suggestions" aria-label="Câu hỏi gợi ý">
    <button class="suggestion" type="button">ORA-01291 xảy ra ở connector nào?</button><button class="suggestion" type="button">Các lỗi gần đây của connector là gì?</button><button class="suggestion" type="button">Connector nào đang có nhiều sự cố?</button>
  </section>
  <form class="composer" id="chat-form"><textarea id="question" placeholder="Nhập câu hỏi… (Enter để gửi, Shift+Enter để xuống dòng)" aria-label="Câu hỏi"></textarea><div class="actions"><label class="subtle">API token <input id="token" type="password" autocomplete="off" placeholder="CHAT_API_TOKEN"></label><button id="send" type="submit">Gửi</button></div></form>
</main>
<script>
(() => {
  const messages = document.querySelector('#messages'), form = document.querySelector('#chat-form'), question = document.querySelector('#question'), token = document.querySelector('#token'), send = document.querySelector('#send'), health = document.querySelector('#health');
  token.value = sessionStorage.getItem('self-healthy-kafka-chat-token') || '';
  token.addEventListener('change', () => sessionStorage.setItem('self-healthy-kafka-chat-token', token.value));
  const add = (role, content, cls = '') => { const box = document.createElement('article'); box.className = `message ${role} ${cls}`; const label = document.createElement('div'); label.className='role'; label.textContent = role === 'user' ? 'Bạn' : 'Assistant'; const body = document.createElement('div'); body.textContent = content; box.append(label, body); messages.append(box); box.scrollIntoView({block:'end', behavior:'smooth'}); return box; };
  const value = (item, ...keys) => { for (const key of keys) if (item[key] !== undefined && item[key] !== null && item[key] !== '') return String(item[key]); return '—'; };
  const evidence = (sources) => { const source = Array.isArray(sources) ? sources.find(s => s && Array.isArray(s.items)) : null; if (!source) return null; const details = document.createElement('details'); const summary = document.createElement('summary'); const count = Number.isFinite(source.count) ? source.count : source.items.length; summary.textContent = `Bằng chứng từ ${source.source} (${count} dòng)`; details.append(summary); const list = document.createElement('div'); list.className='evidence'; source.items.forEach(item => { const log = document.createElement('div'); log.className='log'; const title = document.createElement('strong'); title.textContent = `#${value(item,'incident_id','id','Id')} · ${value(item,'job_name','connector_name','ConnectorName')} · ${value(item,'event_type','EventType')}`; const meta = document.createElement('div'); meta.className='subtle'; meta.textContent = value(item,'failure_at','created_at','CreatedAt','timestamp'); const text = document.createElement('p'); text.textContent = value(item,'error_code','message','Message'); log.append(title,meta,text); list.append(log); }); details.append(list); return details; };
  const queryPlan = (plan) => { if (!plan || typeof plan !== 'object') return null; const details = document.createElement('details'); const summary = document.createElement('summary'); summary.textContent='QueryPlan đã kiểm duyệt'; const body = document.createElement('pre'); body.className='log'; body.textContent=JSON.stringify(plan,null,2); details.append(summary,body); return details; };
  async function checkHealth() { try { const res = await fetch('/health', {cache:'no-store'}); if (!res.ok) throw new Error(); health.textContent='Backend online'; health.classList.add('online'); } catch { health.textContent='Backend offline'; health.classList.remove('online'); } }
  async function ask() { const text = question.value.trim(); if (!text || send.disabled) return; const apiToken = token.value.trim(); if (!apiToken) { add('assistant','Nhập CHAT_API_TOKEN để gửi câu hỏi.', 'error'); token.focus(); return; } sessionStorage.setItem('self-healthy-kafka-chat-token', apiToken); add('user',text); question.value=''; send.disabled=true; send.textContent='Đang gửi…'; const loading=add('assistant','Đang phân tích dữ liệu incident…'); try { const response=await fetch('/api/v1/chat',{method:'POST',headers:{'Content-Type':'application/json','Authorization':`Bearer ${apiToken}`},body:JSON.stringify({question:text})}); const payload=await response.json().catch(()=>({})); loading.remove(); if (!response.ok) throw new Error(payload.error || `HTTP ${response.status}`); const answer=add('assistant', typeof payload.answer === 'string' ? payload.answer : 'Không nhận được câu trả lời hợp lệ.'); const plan=queryPlan(payload.query_plan); if(plan) answer.append(plan); const proof=evidence(payload.sources); if (proof) answer.append(proof); } catch (error) { loading.remove(); add('assistant',`Không thể gửi câu hỏi: ${error.message}`, 'error'); } finally { send.disabled=false; send.textContent='Gửi'; checkHealth(); } }
  form.addEventListener('submit', event => { event.preventDefault(); ask(); }); question.addEventListener('keydown', event => { if (event.key === 'Enter' && !event.shiftKey) { event.preventDefault(); ask(); } }); document.querySelectorAll('.suggestion').forEach(button => button.addEventListener('click', () => { question.value=button.textContent; question.focus(); })); checkHealth(); setInterval(checkHealth, 30000);
})();
</script></body></html>'''
