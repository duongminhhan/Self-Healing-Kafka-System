import { test, expect } from "@playwright/test";
import { readdir, readFile } from "node:fs/promises";
import path from "node:path";
test("send, loading, answer, citations, details, retry and cancellation", async ({ page }) => {
  await page.goto("/");
  const input = page.getByRole("textbox", { name: "Câu hỏi" });
  await input.fill("Connector nào lỗi?"); await input.press("Enter");
  await expect(page.getByRole("status")).toContainText("Đang tìm");
  await page.screenshot({path:"test-results/desktop-loading.png"});
  await expect(page.getByText("Connector orders có 2 incident trong dữ liệu thử nghiệm.", { exact: true })).toBeVisible();
  await expect(page.locator(".badges")).toContainText("analytics");
  await expect(page.locator("details[open]")).toHaveCount(0);
  await page.getByText("Nguồn tham khảo (1)").click();
  await expect(page.getByRole("link", { name: "connection-failure" })).toBeVisible();
  await page.getByText("Chi tiết kỹ thuật").click();
  await expect(page.locator("details pre")).toContainText("row_count");
  await page.screenshot({path:"test-results/desktop-answer.png"});
  await expect(page.locator("body")).not.toContainText("ui-e2e-server-only-secret");
  await input.fill("Retry test"); await input.press("Enter");
  await expect(page.getByText("Hiện chưa kết nối được dịch vụ trả lời. Bạn có thể thử lại sau.", { exact: true })).toBeVisible();
  await page.getByRole("button", { name: "Thử lại · lượt gọi mới" }).last().click();
  await expect(page.getByText("Connector orders có 2 incident trong dữ liệu thử nghiệm.", { exact: true })).toHaveCount(2);
  await input.fill("Cancel test"); await input.press("Enter");
  await page.getByRole("button", { name: "Hủy yêu cầu" }).click();
  await expect(page.getByText("Đã hủy chờ câu trả lời. Bạn có thể thử lại khi sẵn sàng.")).toBeVisible();
  await expect(input).toBeEnabled();
  await page.screenshot({path:"test-results/desktop-cancel.png"});
});

test("empty state, multiline, local visual history and mobile long content",async({page})=>{
  await page.setViewportSize({width:390,height:844});
  await page.goto("/");
  await expect(page.getByRole("heading",{level:1})).toBeVisible();
  await page.screenshot({path:"test-results/mobile-empty.png"});
  const input=page.getByRole("textbox",{name:"Câu hỏi"});
  await input.fill("Line one");await input.press("Shift+Enter");await input.press("a");
  await expect(input).toHaveValue("Line one\na");
  await expect(page.locator(".assistant-message")).toHaveCount(0);
  await input.fill("Fallback test");await input.press("Enter");
  await expect(page.locator(".table-scroll tbody tr")).toHaveCount(1);
  await expect(page.locator(".notice")).toContainText("dữ liệu đã kiểm chứng");
  await expect(page.locator(".table-scroll")).toContainText("75");
  await page.screenshot({path:"test-results/mobile-fallback.png"});
  await page.getByRole("button",{name:"Mở lịch sử"}).click();
  await page.getByRole("button",{name:"Cuộc trò chuyện mới",exact:true}).click();
  await expect(page.getByRole("heading",{level:1})).toBeVisible();
  await input.fill("Long test");await input.press("Enter");
  await expect(page.locator(".session:not([hidden]) .table-scroll tbody tr")).toHaveCount(30);
  expect(await page.evaluate(()=>document.documentElement.scrollWidth<=window.innerWidth)).toBe(true);
  await page.screenshot({path:"test-results/mobile-long.png"});
  await page.getByRole("button",{name:"Giao diện tối"}).click();
  await expect(page.locator(".app-shell")).toHaveClass(/dark/);
  await page.screenshot({path:"test-results/mobile-dark.png"});
  await page.getByRole("button",{name:"Mở lịch sử"}).click();
  await page.getByRole("button",{name:"Cuộc trò chuyện 1",exact:true}).click();
  await expect(page.locator(".session:not([hidden]) .table-scroll")).toContainText("75");
});

test("empty and invalid responses remain distinct and preserve user question",async({page})=>{
  await page.goto("/");
  const input=page.getByRole("textbox",{name:"Câu hỏi"});
  await input.fill("Empty test");await input.press("Enter");
  await expect(page.getByText("Dịch vụ chưa trả về câu trả lời hoặc kết quả đã xác minh. Bạn có thể thử lại.",{exact:true})).toBeVisible();
  await expect(page.locator(".user-message")).toContainText("Empty test");
  await input.fill("Invalid JSON test");await input.press("Enter");
  await expect(page.getByText("Dịch vụ trả về dữ liệu không hợp lệ. Vui lòng thử lại.",{exact:true})).toBeVisible();
  await page.screenshot({path:"test-results/desktop-errors.png"});
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
