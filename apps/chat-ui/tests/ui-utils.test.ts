import { describe, expect, it } from "vitest";
import { filterAndSortRows, formatElapsedTime, normalizeSearchText, sessionTitleFromQuestion } from "../src/lib/ui-utils";

describe("in-memory UI helpers",()=>{
  it("matches Vietnamese text without accents or case",()=>{
    expect(normalizeSearchText("  SỰ CỐ   kết nối  ")).toBe("su co ket noi");
  });
  it("formats browser elapsed time without inventing backend stages",()=>{
    expect(formatElapsedTime(245)).toBe("245 ms");
    expect(formatElapsedTime(1_250)).toBe("1,3 giây");
  });
  it("creates a normalized, bounded title from the first question",()=>{
    expect(sessionTitleFromQuestion("  Connector   nào lỗi? ")).toBe("Connector nào lỗi?");
    const title=sessionTitleFromQuestion("Một câu hỏi rất dài cần được cắt gọn để vừa trong thanh lịch sử");
    expect(title.length).toBeLessThanOrEqual(42);
    expect(title.endsWith("…")).toBe(true);
  });
  it("filters accent-insensitively and sorts numbers stably with NULL last",()=>{
    const rows=[
      {name:"Sự cố B",count:10},
      {name:"Sự cố A",count:2},
      {name:"Khỏe",count:null},
      {name:"Sự cố C",count:2},
    ];
    expect(filterAndSortRows(rows,"su co","count","ascending").map(row=>row.name)).toEqual(["Sự cố A","Sự cố C","Sự cố B"]);
    expect(filterAndSortRows(rows,"","count","ascending").at(-1)?.name).toBe("Khỏe");
  });
});
