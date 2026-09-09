# Healing Chat UI

Next.js App Router + TypeScript strict + assistant-ui LocalRuntime, with a
source-owned shadcn-style Button (Radix Slot/CVA), Tailwind CSS and Zod.

## Architecture and boundaries

Browser `POST /api/chat` → Next.js server-side BFF → Python `POST /api/v1/chat`.
Only `{ "question": "..." }` for the current turn is sent. Qwen, SQL validation,
semantic planning, retrieval and grounding remain in the existing Python service.
There is no browser-to-model call, streaming, Assistant Cloud service, question
cache, conversation database or semantic multi-turn. Sidebar sessions are held
in memory in this browser tab; refreshing the page clears them. Follow-up questions
must include their own context. A failed question remains visible in the thread.

Answers render separately from source/route badges, citations and a closed-by-default
technical panel. Relative runbook paths are labels, not invented hyperlinks.
Raw HTML and remote Markdown images are not rendered. A missing answer produces a
friendly error unless the backend explicitly supplies `verified_result.rows`;
then the UI shows those exact values without generating a numeric summary.
Current Python analytics responses do not supply that optional packet: the UI
does not relabel arbitrary `sources.items` as verified aggregates.

## Configure and run locally

Prerequisites: Node.js 22.12+ (validated with 24.19), pnpm 11, and the configured
Python Chat API described in the repository's `RUNNING.md`.

From this directory:

```powershell
pnpm install --frozen-lockfile
Copy-Item .env.example .env.local
```

Edit the ignored `.env.local` locally:

```dotenv
SELF_HEALTHY_KAFKA_CHAT_API_URL=http://127.0.0.1:8080/api/v1/chat
CHAT_API_TOKEN=
CHAT_API_TIMEOUT_MS=60000
```

Set `CHAT_API_TOKEN` to the private token already configured in Python. Never use
`NEXT_PUBLIC_*` for credentials. Do not paste secrets into screenshots, source files,
test fixtures or commits. The URL must be the full Chat API endpoint; this UI does
not start the healing worker or configure SQL Server/Qdrant. Missing settings fail
closed with a configuration message. Restart Next.js after changing settings.

```powershell
pnpm dev
```

Open <http://127.0.0.1:3000>. Development and production scripts bind to loopback.
Enter sends; Shift+Enter inserts a newline. While pending, sending is disabled.
Cancel aborts the browser request and signals cancellation to the BFF's upstream
fetch. It cannot guarantee termination of an already-running Python/model operation
or reverse incurred provider costs. Retry is explicit and can incur a new model call;
there is no automatic POST retry.

## Production build

```powershell
pnpm build
pnpm start
```

**Do not expose this default deployment publicly.** End-user authentication and
authorization are production blockers: the backend Bearer token authenticates the
BFF, not the browser user. Before shared deployment, place the UI behind an
authenticated reverse proxy/SSO, enforce access to the relevant environment/tenant,
apply rate limits and HTTPS, and restrict network access to Python. Preserve the
original Host header and configure proxy timeouts consistently. The same-origin
check is defense in depth, not authentication. Keep `RAG_DIAGNOSTICS_ENABLED=false`
for ordinary users; a collapsed panel is not an access-control boundary.

The BFF bounds input to 4,000 characters / 20 KB and backend JSON to 1 MB. Timeout
is clamped to 1–180 seconds. HTTP errors are sanitized; audit logs contain only
request ID, status, latency, source, route and fallback reason. Upstream request IDs
are forwarded when safe. Unknown top-level response fields (including raw source
rows) are not forwarded. Retain backend redaction: this BFF is not a replacement
for source-side data classification or permission enforcement.

## Automated validation

```powershell
pnpm typecheck
pnpm lint
pnpm test
pnpm build
pnpm exec playwright install chromium
pnpm test:e2e
```

If an installed Edge browser is available and Chromium download is unavailable:

```powershell
$env:PLAYWRIGHT_CHANNEL = "msedge"
pnpm test:e2e
```

E2E starts an isolated HTTP fixture on `127.0.0.1:18080` and the production UI on
`127.0.0.1:3000`. These ports must be free. It uses a fake test-only credential and
does not call HF, Qdrant or MSSQL. Screenshots under ignored `test-results/` contain
fixture data only. Build before E2E after changing app code. Unit tests cover BFF
input/auth/error/timeout/cancellation/redaction and the single-question adapter.
E2E covers interaction, citations, technical disclosures, fallback, local visual
history, mobile/long content, and scans rendered HTML and client assets for the
test credential. For a stronger release check, build with the fake token set too:

```powershell
$env:CHAT_API_TOKEN = "ui-e2e-server-only-secret"
pnpm build
pnpm test:e2e
Remove-Item Env:CHAT_API_TOKEN
```

Run related Python regression checks from the repository root:

```powershell
python -m pytest tests/unit/test_analytics_chat.py tests/unit/test_grafana_webhook.py tests/unit/test_runbook_workflow.py tests/unit/test_runbook_retrieval.py -q
```

## Live smoke checklist (separate from fixture tests)

With the existing Python backend running and valid local configuration, start the
UI normally and test these routes. Verify SQL-derived numbers independently and
runbook citations against approved documents; a rendered response is not proof of
semantic correctness.

| Case | Example / expected UI behavior |
| --- | --- |
| Analytics | `Connector nào gặp nhiều incident nhất trong 7 ngày qua?` — natural answer and analytics badge |
| Runbook | `ORA-01017 trên Oracle connector cần xử lý thế nào?` — approved citations separately expandable |
| Combined | `Connector nào bị ORA-01017 tuần này và nên xử lý thế nào?` — analytics plus cited guidance |
| Clarification | An ambiguous query from the router's current evaluation set — clarification question, no invented answer |
| Deterministic fallback | An unsupported issue from the current evaluation set — safe fallback/no-answer, not a success claim |

Routes depend on the actual router, available evidence and configuration; do not
force or label a case as passed when it took another route. Record status, route,
source and duration without exporting full questions/results containing sensitive
data. Manually inspect desktop/mobile, long tables, error states, cancel and retry.
Mocked tests are not a live model/database verification.
