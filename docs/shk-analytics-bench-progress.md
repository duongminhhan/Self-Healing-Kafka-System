# SHK-AnalyticsBench Progress

This append-only journal records the need, architecture decisions, and open
questions behind each benchmark-related goal. It does not contain raw SQL,
production payloads, secrets, or lengthy test output.

## 2026-09-15 - Benchmark and LoRA preparation

- Need: model/model-adapter changes lacked an independent measure of semantic accuracy and safety.
- Risk: changing a model without reviewed labels can hide planning, data, or grounding regressions.
- Technique: versioned family-isolated benchmark, deterministic compiler/evidence gates, and plan-only LoRA records.
- Decision: seed labels remain review-gated; no training or runtime model replacement occurs in this phase.
- Open: business review must approve labels and a redacted immutable snapshot before a baseline or LoRA decision.

## 2026-09-16 - Initial training-label approval

- Need: LoRA export had to remain blocked until a named business reviewer approved the candidate plans.
- Technique: review-gated train split; validation and hidden cases remain excluded from export.
- Decision: Minh An approved the 10 initial train labels.
- Open: reference facts remain sanitized fixtures; immutable redacted snapshot evidence is still required before baseline scoring.

## 2026-09-16 - Immutable baseline snapshot workflow

- Need: live outcome scoring could still be mistaken for a comparison against changing source data.
- Technique: opt-in compiler-only capture, allowlisted canonical facts, SHA-256 integrity, and snapshot-identity matching.
- Decision: synthetic fixtures remain offline-only; live plan-only scores exclude outcome and execution accuracy.
- Open: a reviewer must approve eligible validation/hidden labels and a captured manifest before official execution baselines exist.

## 2026-09-16 - Snapshot baseline implementation

- Need: prevent mutable MSSQL state from being treated as model execution ground truth.
- Technique: compiler-only capture, canonical time boundaries, pseudonymized connector facts, and integrity-bound score matching.
- Decision: execution and outcome scores remain unavailable unless the source declares the approved snapshot ID and hash.
- Open: no immutable source adapter or reviewer-approved validation/hidden snapshot has been supplied yet.

## 2026-09-16 - Source identity binding

- Need: a snapshot classification alone could not prove that an adapter read the same source.
- Technique: bind reference, manifest, and adapter to a SHA-256 hash of a non-sensitive logical source alias.
- Decision: raw host, DSN, credential, and source alias are never persisted; identity mismatch disables execution scoring.
- Open: an immutable source adapter must supply the matching identity hash after a reviewer approves a real snapshot.

## 2026-09-16 - Plan-to-snapshot binding

- Need: a correct snapshot result must not score a different semantic plan as correct.
- Technique: compare the adapter's canonical plan hash to the reviewed snapshot plan hash before scoring execution.
- Decision: any plan mismatch is plan-level evidence only and disables outcome/execution accuracy.
- Open: a real immutable source adapter still needs to provide snapshot identity metadata.

## 2026-09-17 - Independent validation-label review

- Need: validation labels had not been independently checked before any model-selection or LoRA decision.
- Finding: the planner-failure case was a fault injection and could not validly score a model.
- Technique: validate each plan against catalog/compiler/outcome rules and enforce scalar aggregate row shape.
- Decision: marked the injected planner-failure case offline-contract-only; labels remain `seed` pending a named business reviewer.
- Boundary: synthetic fixtures validate contracts only; immutable snapshot review is still required for outcome accuracy.

## 2026-09-17 - Validation-label approval

- Need: plan-only model selection required reviewed validation labels before any LoRA decision.
- Decision: Minh An approved all eight independently reviewed validation labels.
- Boundary: validation remains excluded from training; synthetic references cannot measure live outcome accuracy.
