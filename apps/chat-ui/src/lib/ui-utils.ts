export type UiTiming = {
  elapsed_ms: number;
  measured_by: "browser";
};

export function normalizeSearchText(value: string) {
  return value
    .normalize("NFD")
    .replace(/[\u0300-\u036f]/g, "")
    .toLocaleLowerCase("vi")
    .replace(/đ/g, "d")
    .replace(/\s+/g, " ")
    .trim();
}

export function formatElapsedTime(milliseconds: number) {
  const safe = Math.max(0, milliseconds);
  if (safe < 1000) return `${Math.round(safe)} ms`;
  const seconds = Math.round((safe / 1000) * 10) / 10;
  return `${seconds.toLocaleString("vi-VN", { maximumFractionDigits: 1 })} giây`;
}

export function sessionTitleFromQuestion(question: string, maxLength = 42) {
  const normalized = question.replace(/\s+/g, " ").trim();
  if (normalized.length <= maxLength) return normalized;
  return `${normalized.slice(0, Math.max(1, maxLength - 1)).trimEnd()}…`;
}

export type SortDirection = "ascending" | "descending";

function comparable(value: unknown) {
  if (value === null || value === undefined || value === "") return { type: "null" as const, value: "" };
  if (typeof value === "number" && Number.isFinite(value)) return { type: "number" as const, value };
  if (typeof value === "boolean") return { type: "boolean" as const, value: value ? 1 : 0 };
  const text = String(value).trim();
  const numeric = Number(text);
  if (text !== "" && Number.isFinite(numeric)) return { type: "number" as const, value: numeric };
  if (/^\d{4}-\d{2}-\d{2}(?:[T ][0-9:.+-]+Z?)?$/.test(text)) {
    const timestamp = Date.parse(text);
    if (!Number.isNaN(timestamp)) return { type: "date" as const, value: timestamp };
  }
  return { type: "string" as const, value: text };
}

export function compareCellValues(left: unknown, right: unknown) {
  const a = comparable(left);
  const b = comparable(right);
  if (a.type === "null" && b.type === "null") return 0;
  if (a.type === "null") return 1;
  if (b.type === "null") return -1;
  if (a.type === b.type && a.type === "string" && b.type === "string") {
    return a.value.localeCompare(b.value, "vi", { numeric: true, sensitivity: "base" });
  }
  if (a.type === b.type && typeof a.value === "number" && typeof b.value === "number") {
    return a.value - b.value;
  }
  return String(a.value).localeCompare(String(b.value), "vi", { numeric: true, sensitivity: "base" });
}

export function filterAndSortRows<T extends Record<string, unknown>>(
  rows: readonly T[],
  query: string,
  column?: string,
  direction: SortDirection = "ascending",
) {
  const needle = normalizeSearchText(query);
  const filtered = rows
    .map((row, index) => ({ row, index }))
    .filter(({ row }) => !needle || Object.values(row).some(value => normalizeSearchText(value === null ? "NULL" : String(value ?? "")).includes(needle)));
  if (!column) return filtered.map(item => item.row);
  return filtered
    .sort((left, right) => {
      const leftNull=left.row[column]===null||left.row[column]===undefined||left.row[column]==="";
      const rightNull=right.row[column]===null||right.row[column]===undefined||right.row[column]==="";
      if(leftNull&&rightNull)return left.index-right.index;
      if(leftNull)return 1;
      if(rightNull)return -1;
      const comparison = compareCellValues(left.row[column], right.row[column]);
      return comparison === 0 ? left.index - right.index : direction === "ascending" ? comparison : -comparison;
    })
    .map(item => item.row);
}
