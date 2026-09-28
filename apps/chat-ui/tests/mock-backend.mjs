import http from "node:http";
const attempts = new Map();
http.createServer(async (req, res) => {
  if (req.url === "/health") { res.end("ok"); return; }
  if (req.headers.authorization !== "Bearer ui-e2e-server-only-secret") { res.writeHead(401); res.end(); return; }
  let body = "";
  for await (const chunk of req) body += chunk;
  const { question, conversation_id } = JSON.parse(body);
  if(typeof conversation_id!=="string" || !/^[A-Za-z0-9._:-]{1,128}$/.test(conversation_id)) { res.writeHead(400); res.end(); return; }
  if(question === "Empty test") { res.setHeader("Content-Type","application/json"); res.end(JSON.stringify({answer:""})); return; }
  if(question === "Invalid JSON test") { res.end("private invalid JSON"); return; }
  if(question === "Out of scope test") {
    res.setHeader("Content-Type","application/json");
    res.end(JSON.stringify({
      answer:"Câu hỏi này nằm ngoài phạm vi hỗ trợ của chatbot Self Healthy Kafka.",
      route:"out_of_scope",source:"jev_relevance_gate",outcome:"out_of_scope",
      query_executed:false,evidence_complete:false,row_count:null,
      conversation:{id:conversation_id,context_used:false,action:"out_of_scope"},
    })); return;
  }
  if(question === "Tuần này có connector nào gặp sự cố không?") {
    res.setHeader("Content-Type","application/json");
    res.end(JSON.stringify({
      answer:"Có. Tuần này ghi nhận 1 connector gặp sự cố: orders với 1 incident.",
      route:"analytics",source:"deterministic_evidence_renderer",outcome:"verified_results",
      query_executed:true,evidence_complete:true,row_count:1,
      conversation:{id:conversation_id,context_used:false,action:"start"},
    })); return;
  }
  if(question === "Lỗi này có nghiêm trọng không?") {
    res.setHeader("Content-Type","application/json");
    res.end(JSON.stringify({
      answer:"Lỗi của connector orders có mức độ nghiêm trọng đã ghi nhận là CRITICAL.",
      route:"analytics",source:"deterministic_evidence_renderer",outcome:"verified_results",
      query_executed:true,evidence_complete:true,row_count:1,
      conversation:{id:conversation_id,context_used:true,action:"continue"},
    })); return;
  }
  if(question === "Verified empty outcome test") {
    res.setHeader("Content-Type","application/json");
    res.end(JSON.stringify({
      answer:"Không. Chưa ghi nhận connector nào có trạng thái FAILED trong ngày hôm nay theo múi giờ Asia/Ho_Chi_Minh.",
      route:"analytics",source:"deterministic_outcome_renderer",outcome:"verified_empty",query_executed:true,evidence_complete:true,row_count:0,
      source_kind:"historical_incident_snapshot",snapshot_freshness:"historical_incident_snapshot",
      time_range_applied:{from_at:"2026-09-14T00:00:00+07:00",to_at:"2026-09-14T09:00:00+07:00",timezone:"Asia/Ho_Chi_Minh",timestamp:"failure_at"},
      conversation:{id:conversation_id,context_used:false,action:"start"},
    })); return;
  }
  if(question === "Cannot verify outcome test") {
    res.setHeader("Content-Type","application/json");
    res.end(JSON.stringify({
      answer:"Mình chưa thể xác minh đủ dữ liệu cho câu hỏi này, nên không thể kết luận là không có connector gặp lỗi.",
      route:"analytics",source:"deterministic_outcome_renderer",outcome:"cannot_verify",query_executed:true,evidence_complete:false,row_count:1,
      source_kind:"historical_incident_snapshot",fallback_reason:"incomplete_result_coverage",
      time_range_applied:{from_at:"2026-09-14T00:00:00+07:00",to_at:"2026-09-14T09:00:00+07:00",timezone:"Asia/Ho_Chi_Minh",timestamp:"failure_at"},
      conversation:{id:conversation_id,context_used:false,action:"start"},
    })); return;
  }
  // Deterministic semantic-enforcement fixtures. These exercise the browser
  // contract only; production planning remains backend-owned.
  if(question === "Connector ranking response test") {
    res.setHeader("Content-Type","application/json");
    res.end(JSON.stringify({
      answer:"Trong toàn bộ dữ liệu incident hiện có, connector sample-oracle-orders có số incident là 4.",
      route:"analytics",source:"deterministic_evidence_renderer",outcome:"verified_results",query_executed:true,evidence_complete:true,row_count:1,
      conversation:{id:conversation_id,context_used:false,action:"start"},
    })); return;
  }
  if(question === "Error ranking response test") {
    res.setHeader("Content-Type","application/json");
    res.end(JSON.stringify({
      answer:"Trong toàn bộ dữ liệu incident hiện có, mã lỗi ORA-01013 có số incident là 6.",
      route:"analytics",source:"deterministic_evidence_renderer",outcome:"verified_results",query_executed:true,evidence_complete:true,row_count:1,
      conversation:{id:conversation_id,context_used:false,action:"start"},
    })); return;
  }
  if(question === "Rejected semantic plan test") {
    res.setHeader("Content-Type","application/json");
    res.end(JSON.stringify({
      answer:"Mình chưa thể xác minh đủ dữ liệu cho yêu cầu này, nên chưa thể đưa ra kết luận.",
      route:"analytics",source:"deterministic_outcome_renderer",outcome:"cannot_verify",query_executed:false,evidence_complete:false,row_count:0,
      conversation:{id:conversation_id,context_used:false,action:"start"},
    })); return;
  }
  if(question === "Fallback test") {
    res.setHeader("Content-Type","application/json");
    res.end(JSON.stringify({answer:"",verified_result:{rows:[{incident_count:75}]},fallback_reason:"grounding_failure:ValueError",diagnostics:{raw_prompt:"private prompt",provider:"internal-provider"}})); return;
  }
  if(question === "Long test") {
    res.setHeader("Content-Type","application/json");
    res.end(JSON.stringify({answer:"Đây là bản giải thích dài.\n\n".repeat(30)+"| Connector | Ghi chú |\n| --- | --- |\n| orders | "+"sample-connector-".repeat(100)+" |",verified_result:{rows:Array.from({length:30},(_,i)=>({connector:`connector-${i}`,note:"large-field-".repeat(80),incident_count:i}))}})); return;
  }
  if(question === "Table tools test") {
    res.setHeader("Content-Type","application/json");
    res.end(JSON.stringify({answer:"Đã tìm thấy 3 connector để kiểm tra.",route:"analytics",source:"verified_sql",row_count:3,request_id:"request-table-tools-123",verified_result:{rows:[
      {connector:"jdbc-orders",incident_count:10,last_seen:"2026-09-09T08:00:00Z"},
      {connector:"oracle-cdc",incident_count:2,last_seen:"2026-09-10T09:00:00Z"},
      {connector:"s3-sink",incident_count:null,last_seen:null},
    ]},conversation:{id:conversation_id,context_used:false,action:"start"}})); return;
  }
  if(question === "Presentation summary test") {
    res.setHeader("Content-Type","application/json");
    res.end(JSON.stringify({answer:"Ba kết quả chính đã được xác minh.\n\nCó thêm 1 kết quả đồng hạng với vị trí thứ 3; bảng chi tiết chứa đầy đủ danh sách.",route:"analytics",source:"deterministic_evidence_renderer",outcome:"verified_results",query_executed:true,evidence_complete:true,row_count:4,presentation:{summary_item_limit:3,summary_detail_limit:1,result_total_count:4,displayed_count:3,remaining_count:1,ranking:"descending",tie_policy:"include_ties",boundary_tie_count:2,boundary_tie_truncated:true,has_more_verified_results:true,detail_accessible:true},verified_result:{rows:[
      {connector:"orders",incident_count:4},{connector:"payments",incident_count:3},{connector:"auth",incident_count:1},{connector:"oracle",incident_count:1},
    ]},conversation:{id:conversation_id,context_used:false,action:"start"}})); return;
  }
  if(question === "Executed query disclosure test") {
    res.setHeader("Content-Type","application/json");
    res.end(JSON.stringify({answer:"Đã xác minh 3 incident.",route:"analytics",source:"deterministic_evidence_renderer",outcome:"verified_results",query_executed:true,evidence_complete:true,row_count:1,executed_query:{kind:"tsql_select",dialect:"tsql",statement:"SELECT ? AS [incident_count];",display_statement:"DECLARE @rank_limit int = 3;\n\nSELECT @rank_limit AS [incident_count];",parameters:[{name:"@rank_limit",type:"int",value:3}],executed:true,read_only:true,result_shape:["incident_count"]},conversation:{id:conversation_id,context_used:false,action:"start"}})); return;
  }
  if(question === "Hãy giải thích câu trả lời vừa rồi ngắn gọn và dễ hiểu hơn." || question === "Dựa trên câu trả lời vừa rồi, hãy đề xuất bước tiếp theo phù hợp.") {
    res.setHeader("Content-Type","application/json");
    res.end(JSON.stringify({answer:`Follow-up dùng đúng phiên ${conversation_id}.`,route:"analytics",source:"verified_sql",conversation:{id:conversation_id,context_used:true,action:"continue"}})); return;
  }
  const count = (attempts.get(question) ?? 0) + 1; attempts.set(question, count);
  if (question === "Retry test" && count === 1) { res.writeHead(503); res.end("private backend failure"); return; }
  const timer = setTimeout(() => {
    res.setHeader("Content-Type", "application/json");
    res.end(JSON.stringify({ answer: "Connector orders có 2 incident trong dữ liệu thử nghiệm.", route: "analytics", source: "verified_sql", row_count:1, request_id:"request-e2e-123456", citations: [{ runbook_id: "RB-ORACLE-003", version: 1, section: "diagnostic_steps", url: "/runbooks/RB-ORACLE-003?v=1#diagnostic_steps" }], diagnostics: { row_count: 1 }, conversation:{id:conversation_id,context_used:false,action:"start"} }));
  }, question === "Cancel test" ? 8000 : question === "Connector nào lỗi?" ? 1500 : 700);
  res.on("close", () => clearTimeout(timer));
}).listen(Number(process.env.MOCK_BACKEND_PORT??"18080"), "127.0.0.1");
