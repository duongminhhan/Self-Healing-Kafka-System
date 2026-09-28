import Link from "next/link";
import { notFound } from "next/navigation";
import ReactMarkdown from "react-markdown";
import remarkGfm from "remark-gfm";
import { getRunbook, listRunbooks } from "@/lib/runbooks";

export function generateStaticParams() {
  return listRunbooks().map((runbook) => ({ runbookId: runbook.id }));
}

export default async function RunbookPage({
  params,
  searchParams,
}: {
  params: Promise<{ runbookId: string }>;
  searchParams?: Promise<{ v?: string }>;
}) {
  const { runbookId } = await params;
  const query = searchParams ? await searchParams : {};
  const version = query.v ? Number(query.v) : undefined;
  if (query.v && (!Number.isInteger(version) || version! < 1)) notFound();
  const runbook = getRunbook(runbookId, version);
  if (!runbook) notFound();
  return <main className="runbook-page runbook-detail">
    <Link className="runbook-back" href="/runbooks">← Tất cả runbook</Link>
    <p className="eyebrow">{runbook.id} · v{runbook.version}</p>
    <h1>{runbook.title}</h1>
    <p className="runbook-source">Nhóm: {runbook.connectorClass || "Kafka Connect"} · Trạng thái: {runbook.status} · Phiên bản: {runbook.version}</p>
    <nav className="runbook-sections" aria-label="Mục trong runbook">
      {runbook.sections.map((section) => <a key={section.id} href={`#${section.id}`}>{section.title}</a>)}
    </nav>
    <article className="runbook-content">
      {runbook.sections.map((section) => <section id={section.id} key={section.id}>
        <h2>{section.title}</h2>
        <ReactMarkdown remarkPlugins={[remarkGfm]}>{section.markdown}</ReactMarkdown>
      </section>)}
    </article>
  </main>;
}
