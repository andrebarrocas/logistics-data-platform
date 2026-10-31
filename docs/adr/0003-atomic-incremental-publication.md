# ADR 0003: manifest-controlled atomic publication

Status: accepted at Phase 0.

## Context
An apparently successful load may hide a missing file, duplicate delivery, late event or partial financial ledger. Event-time watermarks drop valid late data.

## Decision
Use batch ingestion sequence as a publication cursor. Require manifests for all five sources, including explicit zero-row feeds. Verify SHA256, expected file and row totals, and currency-level integer cost controls. Deduplicate physical content and source revisions separately. Quarantine invalid records and persist structured quality results. Block publication for critical failures. Commit evidence acceptance, canonical history, fact updates and cursor together, with a serialized writer. Retry the same batch safely after interruption; identical committed batches are no-ops. Different content under an already committed batch identity is rejected.

Allow contiguous incremental batches and bounded replay ranges. Backfills process source revisions, never blindly overwrite current data with older snapshots. Refresh dependent facts for reference changes. Keep raw files and publication/audit metadata for recovery.

## Consequences
A missing financial source delays publication of the whole consistent product release. Availability is traded for explicit trust. Fine-grained per-product release schedules can be added only with separate dependency/quality contracts. The local profile may rebuild relational projections from accepted evidence for simplicity; this limitation and measured cost must be explicit, while ingestion itself remains incremental.
