import "server-only";

import { existsSync, readFileSync, readdirSync } from "node:fs";
import path from "node:path";

export type RunbookSection = { id: string; title: string; markdown: string };
export type Runbook = {
  id: string;
  title: string;
  version: number;
  status: string;
  connectorClass: string;
  source: string;
  sections: RunbookSection[];
};

const runbooksRoot = [
  path.resolve(process.cwd(), "runbooks"),
  path.resolve(process.cwd(), "../../runbooks"),
].find((candidate) => existsSync(candidate)) ?? path.resolve(process.cwd(), "runbooks");
const idPattern = /^[A-Za-z0-9][A-Za-z0-9._-]{0,127}$/;

function slug(value: string) {
  return value.normalize("NFD").replace(/[\u0300-\u036f]/g, "").toLowerCase()
    .replace(/[^a-z0-9]+/g, "_").replace(/^_+|_+$/g, "");
}

function frontMatter(source: string) {
  const match = source.match(/^---\s*\n([\s\S]*?)\n---\s*\n([\s\S]*)$/);
  if (!match) return undefined;
  const fields = new Map<string, string>();
  for (const line of match[1].split(/\r?\n/)) {
    const field = line.match(/^([A-Za-z_][A-Za-z0-9_]*):\s*(.*?)\s*$/);
    if (field) fields.set(field[1], field[2].replace(/^['"]|['"]$/g, ""));
  }
  const id = fields.get("runbook_id");
  const title = fields.get("title");
  const version = Number(fields.get("version"));
  if (!id || !idPattern.test(id) || !title || !Number.isInteger(version) || version < 1) return undefined;
  return {
    id,
    title,
    version,
    status: fields.get("status") ?? "",
    connectorClass: fields.get("connector_class") ?? "",
    body: match[2],
  };
}

function sections(body: string): RunbookSection[] {
  const headings = [...body.matchAll(/^##\s+(.+?)\s*$/gm)];
  return headings.map((heading, index) => ({
    id: slug(heading[1]),
    title: heading[1].trim(),
    markdown: body.slice(heading.index! + heading[0].length, headings[index + 1]?.index ?? body.length).trim(),
  }));
}

function markdownFiles(directory: string): string[] {
  try {
    return readdirSync(directory, { withFileTypes: true }).flatMap((entry) => {
      const file = path.join(directory, entry.name);
      return entry.isDirectory() ? markdownFiles(file) : entry.name.endsWith(".md") ? [file] : [];
    });
  } catch {
    return [];
  }
}

function loadAll(): Runbook[] {
  return markdownFiles(runbooksRoot).flatMap((file) => {
    const raw = frontMatter(readFileSync(file, "utf8"));
    if (!raw || raw.status !== "approved") return [];
    return [{
      id: raw.id,
      title: raw.title,
      version: raw.version,
      status: raw.status,
      connectorClass: raw.connectorClass,
      source: path.relative(process.cwd(), file).replaceAll("\\", "/"),
      sections: sections(raw.body),
    }];
  }).sort((left, right) => left.id.localeCompare(right.id) || right.version - left.version);
}

export function listRunbooks() {
  return loadAll();
}

export function getRunbook(id: string, version?: number) {
  if (!idPattern.test(id)) return undefined;
  return loadAll().find((item) => item.id === id && (version === undefined || item.version === version));
}
