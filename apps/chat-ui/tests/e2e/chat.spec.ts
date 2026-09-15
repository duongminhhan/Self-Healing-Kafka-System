import { test, expect, type Page } from "@playwright/test";
import { readdir, readFile } from "node:fs/promises";
import path from "node:path";

async function sendQuestion(page: Page, question: string) {
  const input = page.getByRole("textbox", { name: "Câu hỏi" });
  const send = page.getByRole("button", { name: "Gửi câu hỏi" });
  await input.fill(question);
  await expect(send).toBeEnabled();
  await send.click();
}

test("send, loading, answer, citations, details, retry and cancellation", async ({ page }) => {
  await page.goto("/");
  const input = page.getByRole("textbox", { name: "Câu hỏi" });
  await sendQuestion(page, "Connector nào lỗi?");
  await expect(page.getByRole("status")).toContainText(/Đang tìm.*(?:ms|giây)/);
  await page.screenshot({path:"test-results/desktop-loading.png"});
  await expect(page.getByText("Connector orders có 2 incident trong dữ liệu thử nghiệm.", { exact: true })).toBeVisible();
  await expect(page.getByLabel("Thông tin phản hồi")).toContainText(/Phản hồi trong (?:\d+ ms|\d+(?:,\d)? giây)/);
  await expect(page.locator(".badges")).not.toContainText("Phân tích dữ liệu");
  await expect(page.locator("details[open]")).toHaveCount(0);
  await page.getByText("Nguồn tham khảo (1)").click();
  await expect(page.getByRole("link", { name: "connection-failure" })).toBeVisible();
  await page.getByText("Chi tiết kỹ thuật").click();
  await expect(page.locator("details pre")).toContainText("row_count");
  await page.screenshot({path:"test-results/desktop-answer.png"});
  await expect(page.locator("body")).not.toContainText("ui-e2e-server-only-secret");
  await sendQuestion(page, "Retry test");
  await expect(page.getByText("Hiện chưa kết nối được dịch vụ trả lời. Bạn có thể thử lại sau.", { exact: true })).toBeVisible();
  await page.getByRole("button", { name: "Thử lại · lượt gọi mới" }).last().click();
  await expect(page.getByText("Connector orders có 2 incident trong dữ liệu thử nghiệm.", { exact: true })).toHaveCount(2);
  await sendQuestion(page, "Cancel test");
  await page.getByRole("button", { name: "Hủy yêu cầu" }).click();
  await expect(page.getByText(/Đã hủy chờ câu trả lời(?: sau .+)?\. Bạn có thể thử lại khi sẵn sàng\./)).toBeVisible();
  await expect(input).toBeEnabled();
  await page.screenshot({path:"test-results/desktop-cancel.png"});
});

test("empty state, multiline, local visual history and mobile long content",async({page})=>{
  await page.setViewportSize({width:390,height:844});
  await page.goto("/");
  await expect(page.getByRole("heading",{name:"Cùng bạn giữ dữ liệu thông suốt.",exact:false})).toBeVisible();
  await page.screenshot({path:"test-results/mobile-empty.png"});
  const input=page.getByRole("textbox",{name:"Câu hỏi"});
  await input.fill("Line one");await input.press("Shift+Enter");await input.press("a");
  await expect(input).toHaveValue("Line one\na");
  await expect(page.locator(".assistant-message")).toHaveCount(0);
  await sendQuestion(page, "Fallback test");
  await expect(page.locator(".table-scroll tbody tr")).toHaveCount(1);
  await expect(page.locator(".notice")).toContainText("dữ liệu đã kiểm chứng");
  await expect(page.locator(".table-scroll")).toContainText("75");
  await page.screenshot({path:"test-results/mobile-fallback.png"});
  await page.getByRole("button",{name:"Ẩn hoặc mở lịch sử"}).click();
  await page.getByRole("button",{name:"Cuộc trò chuyện mới",exact:true}).click();
  await expect(page.getByRole("heading",{name:"Cùng bạn giữ dữ liệu thông suốt.",exact:false})).toBeVisible();
  await sendQuestion(page, "Long test");
  await expect(page.locator(".session:not([hidden]) .table-scroll tbody tr")).toHaveCount(30);
  expect(await page.evaluate(()=>document.documentElement.scrollWidth<=window.innerWidth)).toBe(true);
  await page.screenshot({path:"test-results/mobile-long.png"});
  await page.getByRole("button",{name:"Giao diện tối"}).click();
  await expect(page.locator("html")).toHaveClass(/dark/);
  await page.screenshot({path:"test-results/mobile-dark.png"});
  await page.getByRole("button",{name:"Ẩn hoặc mở lịch sử"}).click();
  await page.getByRole("button",{name:"Fallback test",exact:true}).click();
  await expect(page.locator(".session:not([hidden]) .table-scroll")).toContainText("75");
});

test("desktop sidebar toggle and session deletion",async({page})=>{
  await page.goto("/");
  const sidebar=page.locator("#session-sidebar");
  const toggle=page.getByRole("button",{name:"Ẩn hoặc mở lịch sử"});

  await expect(sidebar).not.toHaveClass(/desktop-closed/);
  await toggle.click();
  await expect(sidebar).toHaveClass(/desktop-closed/);
  await toggle.click();
  await expect(sidebar).not.toHaveClass(/desktop-closed/);

  await page.getByRole("button",{name:"Cuộc trò chuyện mới",exact:true}).click();
  await expect(page.getByRole("button",{name:"Cuộc trò chuyện 2",exact:true})).toBeVisible();
  await page.getByRole("button",{name:"Xóa Cuộc trò chuyện 2"}).click();
  await page.getByRole("button",{name:"Hủy",exact:true}).click();
  await expect(page.getByRole("button",{name:"Cuộc trò chuyện 2",exact:true})).toBeVisible();

  await page.getByRole("button",{name:"Xóa Cuộc trò chuyện 2"}).click();
  await page.getByRole("button",{name:"Xóa",exact:true}).click();
  await expect(page.getByRole("button",{name:"Cuộc trò chuyện 2",exact:true})).toHaveCount(0);
  await expect(page.getByRole("button",{name:"Cuộc trò chuyện 1",exact:true})).toBeVisible();

  await page.getByRole("button",{name:"Xóa Cuộc trò chuyện 1"}).click();
  await page.getByRole("button",{name:"Xóa",exact:true}).click();
  await expect(page.locator(".no-sessions")).toBeVisible();
  await page.locator(".no-sessions button").click();
  await expect(page.getByRole("button",{name:"Cuộc trò chuyện 3",exact:true})).toBeVisible();
});

test("theme choice survives reload and applies before paint",async({page})=>{
  const hydrationErrors:string[]=[];
  page.on("console",message=>{if(message.type()==="error"&&message.text().toLowerCase().includes("hydration"))hydrationErrors.push(message.text());});
  await page.goto("/");
  await expect(page.locator("html")).not.toHaveClass(/dark/);
  await page.getByRole("button",{name:"Giao diện tối"}).click();
  await expect(page.locator("html")).toHaveClass(/dark/);
  await page.reload();
  await expect(page.locator("html")).toHaveClass(/dark/);
  await expect(page.getByRole("button",{name:"Giao diện sáng"})).toBeVisible();
  await page.getByRole("button",{name:"Giao diện sáng"}).click();
  await page.reload();
  await expect(page.locator("html")).not.toHaveClass(/dark/);
  expect(hydrationErrors).toEqual([]);
});

test("empty and invalid responses remain distinct and preserve user question",async({page})=>{
  await page.goto("/");
  await sendQuestion(page, "Empty test");
  await expect(page.getByText("Dịch vụ chưa trả về câu trả lời hoặc kết quả đã xác minh. Bạn có thể thử lại.",{exact:true})).toBeVisible();
  await expect(page.locator(".user-message")).toContainText("Empty test");
  await sendQuestion(page, "Invalid JSON test");
  await expect(page.getByText("Dịch vụ trả về dữ liệu không hợp lệ. Vui lòng thử lại.",{exact:true})).toBeVisible();
  await page.screenshot({path:"test-results/desktop-errors.png"});
});

test("outcome contract only renders a negative conclusion after verified empty evidence",async({page})=>{
  await page.goto("/");
  await sendQuestion(page, "Verified empty outcome test");
  await expect(page.getByText(/chưa ghi nhận connector nào có trạng thái FAILED/i)).toBeVisible();
  await expect(page.getByLabel("Thông tin phản hồi")).toContainText(/Phản hồi trong/);
  await expect(page.getByLabel("Thông tin phản hồi")).not.toContainText("Nguồn:");
  await expect(page.getByLabel("Thông tin phản hồi")).not.toContainText("Phạm vi:");
  await expect(page.getByLabel("Thông tin phản hồi")).not.toContainText("Số dòng truy vấn:");
  await expect(page.getByLabel("Thông tin phản hồi")).not.toContainText("Request:");
  await expect(page.locator(".notice")).toHaveCount(0);
  await sendQuestion(page, "Cannot verify outcome test");
  const latest=page.locator(".assistant-message").last();
  await expect(latest).toContainText("chưa thể xác minh đủ dữ liệu");
  await expect(latest).not.toContainText("chưa ghi nhận connector nào có trạng thái FAILED");
});

test("semantic ranking responses retain the verified entity and unspecified time scope",async({page})=>{
  await page.goto("/");
  await sendQuestion(page, "Connector ranking response test");
  const connector=page.locator(".assistant-message").last();
  await expect(connector).toContainText("connector sample-oracle-orders");
  await expect(connector).toContainText("toàn bộ snapshot hiện có");
  await expect(connector).not.toContainText("hôm nay");
  await sendQuestion(page, "Error ranking response test");
  const error=page.locator(".assistant-message").last();
  await expect(error).toContainText("mã lỗi ORA-01013");
  await expect(error).toContainText("toàn bộ snapshot hiện có");
  await sendQuestion(page, "Rejected semantic plan test");
  const rejected=page.locator(".assistant-message").last();
  await expect(rejected).toContainText("chưa thể xác minh");
  await expect(rejected).not.toContainText("chưa ghi nhận");
});

test("live backend never turns an unverified result into a no-failed conclusion",async({page})=>{
  test.skip(!process.env.PLAYWRIGHT_LIVE_BACKEND_URL,"requires an explicitly configured local backend");
  test.setTimeout(90_000);
  await page.goto("/");
  const input=page.getByRole("textbox",{name:"Câu hỏi"});
  for(let attempt=0;attempt<2;attempt+=1){
    const responsePromise=page.waitForResponse(response=>response.url().endsWith("/api/chat")&&response.request().method()==="POST");
    await input.fill("hôm nay có connector nào failed không?");
    await expect(page.getByRole("button",{name:"Gửi câu hỏi"})).toBeEnabled();
    await page.getByRole("button",{name:"Gửi câu hỏi"}).click();
    const response=await responsePromise;
    expect(response.status()).toBe(200);
    const payload=await response.json() as {outcome?:string;query_executed?:boolean;evidence_complete?:boolean;row_count?:number;answer?:string};
    const latest=page.locator(".assistant-message").last();
    if(payload.outcome==="verified_empty"){
      expect(payload.query_executed).toBe(true);
      expect(payload.evidence_complete).toBe(true);
      expect(payload.row_count).toBe(0);
      await expect(latest).toContainText(/chưa ghi nhận connector nào có trạng thái FAILED/i);
    }else{
      expect(["verified_results","cannot_verify","degraded","needs_clarification"]).toContain(payload.outcome);
      await expect(latest).not.toContainText(/chưa ghi nhận connector nào có trạng thái FAILED/i);
    }
  }
});

test("conversation search is accent-insensitive, keyboard accessible and session-local",async({page})=>{
  await page.goto("/");
  await sendQuestion(page, "Sự cố Đà Nẵng lần một");
  await expect(page.getByText("Connector orders có 2 incident trong dữ liệu thử nghiệm.",{exact:true})).toBeVisible();
  await sendQuestion(page, "Sự cố Đà Nẵng lần hai");
  await expect(page.getByText("Connector orders có 2 incident trong dữ liệu thử nghiệm.",{exact:true})).toHaveCount(2);
  await page.keyboard.press("Control+K");
  const search=page.getByRole("search").getByRole("textbox",{name:"Tìm trong cuộc trò chuyện"});
  await expect(search).toBeFocused();
  await search.fill("su co da nang");
  await expect(page.locator(".search-count")).toHaveText("1 / 2");
  await expect(page.locator(".search-selected")).toContainText("Sự cố Đà Nẵng lần một");
  await search.press("Enter");
  await expect(page.locator(".search-count")).toHaveText("2 / 2");
  await expect(page.locator(".search-selected")).toContainText("Sự cố Đà Nẵng lần hai");
  await search.press("Shift+Enter");
  await expect(page.locator(".search-count")).toHaveText("1 / 2");
  await search.fill("không tồn tại");
  await expect(page.locator(".search-count")).toHaveText("Không có kết quả");
  await search.press("Escape");
  await expect(page.getByRole("search")).toHaveCount(0);
  await expect(page.getByRole("button",{name:"Tìm trong cuộc trò chuyện"})).toBeFocused();
  await page.getByRole("button",{name:"Cuộc trò chuyện mới",exact:true}).click();
  await page.keyboard.press("Control+K");
  await page.getByRole("search").getByRole("textbox").fill("da nang");
  await expect(page.locator(".search-count")).toHaveText("Không có kết quả");
});

test("auto-title, in-memory rename and quick actions keep the active conversation",async({page})=>{
  await page.goto("/");
  await sendQuestion(page, "Connector nào đang gặp sự cố cần xử lý ngay bây giờ?");
  await expect(page.getByText("Connector orders có 2 incident trong dữ liệu thử nghiệm.",{exact:true})).toBeVisible();
  const activeTitle=page.locator(".history.active");
  await expect(activeTitle).not.toHaveText("Cuộc trò chuyện 1");
  const generatedTitle=(await activeTitle.innerText()).trim();
  expect(generatedTitle.length).toBeLessThanOrEqual(42);
  await page.getByRole("button",{name:`Đổi tên ${generatedTitle}`}).click();
  const rename=page.getByRole("textbox",{name:new RegExp("Tên mới cho")});
  await rename.fill("Theo dõi connector khẩn cấp");await rename.press("Enter");
  await expect(page.getByRole("button",{name:"Theo dõi connector khẩn cấp",exact:true})).toBeVisible();
  await page.getByRole("button",{name:"Đề xuất bước tiếp theo"}).click();
  await expect(page.getByText(/Follow-up dùng đúng phiên session-1\./)).toBeVisible();
  await page.reload();
  await expect(page.getByRole("button",{name:"Cuộc trò chuyện 1",exact:true})).toBeVisible();
});

test("verified table can filter, stably sort, reset and open fullscreen",async({page})=>{
  await page.goto("/");
  await sendQuestion(page, "Table tools test");
  const table=page.getByRole("region",{name:"Kết quả đã xác minh"});
  await expect(table.locator("tbody tr")).toHaveCount(3);
  await page.getByRole("button",{name:"incident_count"}).click();
  await expect(table.locator("tbody tr").first()).toContainText("oracle-cdc");
  await page.getByRole("button",{name:"incident_count"}).click();
  await expect(table.locator("tbody tr").first()).toContainText("jdbc-orders");
  await page.getByRole("textbox",{name:"Tìm trong bảng"}).fill("ORACLE");
  await expect(table.locator("tbody tr")).toHaveCount(1);
  await expect(page.locator(".table-count")).toHaveText("1 / 3 dòng");
  await page.getByRole("button",{name:"Đặt lại bộ lọc và sắp xếp"}).click();
  await expect(table.locator("tbody tr")).toHaveCount(3);
  const fullscreenButton=page.getByRole("button",{name:"Mở bảng toàn màn hình"});
  await fullscreenButton.click();
  await expect(page.getByRole("dialog",{name:"Bảng kết quả đã xác minh toàn màn hình"})).toBeVisible();
  await expect(page.getByRole("button",{name:"Đóng bảng toàn màn hình"})).toBeFocused();
  await page.keyboard.press("Escape");
  await expect(page.getByRole("dialog")).toHaveCount(0);
  await expect(fullscreenButton).toBeFocused();
});

test("shows only a verified read-only executed T-SQL query and copies its display form",async({page})=>{
  await page.goto("/");
  await sendQuestion(page,"Executed query disclosure test");
  const disclosure=page.getByText("Truy vấn đã chạy");
  await expect(disclosure).toBeVisible();
  await disclosure.click();
  await expect(page.getByText("T-SQL · chỉ đọc",{exact:true})).toBeVisible();
  await expect(page.locator(".executed-query pre")).toContainText("DECLARE @rank_limit int = 3");
  await expect(page.getByRole("button",{name:"Sao chép truy vấn"})).toBeVisible();
});

test("HTML and all client assets contain no server credential",async({request})=>{
  const secret="ui-e2e-server-only-secret";
  const response=await request.get("/");
  expect(await response.text()).not.toContain(secret);
  async function scan(dir:string):Promise<number>{
    let count=0;
    for(const item of await readdir(dir,{withFileTypes:true})){
      const file=path.join(dir,item.name);
      if(item.isDirectory()) count+=await scan(file);
      else {expect((await readFile(file)).includes(Buffer.from(secret))).toBe(false);count++;}
    }
    return count;
  }
  expect(await scan(".next/static")).toBeGreaterThan(0);
});
