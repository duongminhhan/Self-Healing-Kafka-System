import http from "node:http";
const attempts = new Map();
http.createServer(async (req, res) => {
  if (req.url === "/health") { res.end("ok"); return; }
  if (req.headers.authorization !== "Bearer ui-e2e-server-only-secret") { res.writeHead(401); res.end(); return; }
  let body = "";
  for await (const chunk of req) body += chunk;
  const { question } = JSON.parse(body);
  if(question === "Empty test") { res.setHeader("Content-Type","application/json"); res.end(JSON.stringify({answer:""})); return; }
  if(question === "Invalid JSON test") { res.end("private invalid JSON"); return; }
  if(question === "Fallback test") {
    res.setHeader("Content-Type","application/json");
    res.end(JSON.stringify({answer:"",verified_result:{rows:[{incident_count:75,healing_log_count:75}]},fallback_reason:"unsupported_numeric_claim"})); return;
  }
  if(question === "Long test") {
    res.setHeader("Content-Type","application/json");
    res.end(JSON.stringify({answer:"Đây là bản giải thích dài.\n\n".repeat(30)+"| Connector | Ghi chú |\n| --- | --- |\n| orders | "+"sample-connector-".repeat(100)+" |",verified_result:{rows:Array.from({length:30},(_,i)=>({connector:`connector-${i}`,note:"large-field-".repeat(80),incident_count:i}))}})); return;
  }
  const count = (attempts.get(question) ?? 0) + 1; attempts.set(question, count);
  if (question === "Retry test" && count === 1) { res.writeHead(503); res.end("private backend failure"); return; }
  const timer = setTimeout(() => {
    res.setHeader("Content-Type", "application/json");
    res.end(JSON.stringify({ answer: "Connector orders có 2 incident trong dữ liệu thử nghiệm.", route: "analytics", source: "verified_sql", citations: [{ runbook_id: "connection-failure", version: 1, section: "recovery", source: "https://example.org/runbook" }], diagnostics: { row_count: 1 } }));
  }, question === "Cancel test" ? 8000 : 700);
  res.on("close", () => clearTimeout(timer));
}).listen(18080, "127.0.0.1");
