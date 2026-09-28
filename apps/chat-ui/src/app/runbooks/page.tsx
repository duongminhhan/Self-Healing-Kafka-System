import Link from "next/link";
import { listRunbooks } from "@/lib/runbooks";

export default async function RunbooksPage({
  searchParams,
}: {
  searchParams?: Promise<{ q?: string }>;
}) {
  const query = searchParams ? await searchParams : {};
  const term = query.q?.trim() ?? "";
  const normalized = term.toLocaleLowerCase();
  const runbooks = listRunbooks().filter((runbook) => !normalized || [
    runbook.id,
    runbook.title,
    runbook.connectorClass,
    runbook.status,
  ].some((value) => value.toLocaleLowerCase().includes(normalized)));
  return <main className="runbook-page">
    <p className="eyebrow">SELF HEALTHY KAFKA · RUNBOOKS</p>
    <h1>Runbook vận hành</h1>
    <p className="runbook-intro">Các hướng dẫn đã được phê duyệt cho chẩn đoán và phục hồi Kafka Connect.</p>
    <form className="runbook-filter" method="get">
      <label htmlFor="runbook-search">Tìm runbook</label>
      <div>
        <input id="runbook-search" name="q" type="search" defaultValue={term} placeholder="Mã, tiêu đề hoặc nhóm connector" />
        <button type="submit">Tìm</button>
      </div>
    </form>
    {runbooks.length ? <div className="runbook-list">
      {runbooks.map((runbook) => <Link className="runbook-card" key={`${runbook.id}-${runbook.version}`} href={`/runbooks/${runbook.id}?v=${runbook.version}`}>
        <span className="runbook-card-id">{runbook.id} · v{runbook.version}</span>
        <strong>{runbook.title}</strong>
        <small>{runbook.connectorClass || "Kafka Connect"} · {runbook.sections.length} mục · {runbook.status}</small>
      </Link>)}
    </div> : <p className="runbook-empty" role="status">Không tìm thấy runbook phù hợp.</p>}
  </main>;
}
