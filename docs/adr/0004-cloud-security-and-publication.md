# ADR 0004 — Private cloud deployment and manifest-driven publication

Status: Accepted for implementation. Date: 2026-10-04.

## Context

The project must demonstrate Glue, S3, Catalog, IAM, CloudWatch, and Redshift without claiming an unexecuted deployment is production evidence. Object stores and a warehouse do not share a transaction manager. Partial uploads, duplicate deliveries, interrupted SQL loads, and retries must not create partly published products.

## Decision

Use a private Redshift Serverless workgroup in an operator-supplied VPC, separate versioned encrypted S3 buckets, explicit Glue 5.0 schemas, and scoped service roles. Do not execute paid infrastructure deployment automatically during development or CI.

Treat producer manifests as completeness contracts and immutable release manifests as the boundary between canonical processing and warehouse loading. Verify file hashes before publishing a release marker. Load exact mandatory file lists into typed Redshift staging tables. Recheck reconciliation inside the warehouse transaction and publish warehouse changes, the run ledger, and the release sequence together. Retain the structured quality results in the immutable release manifest. Serialize publishers and use the committed manifest identity to make long-term reruns idempotent. S3-ready and warehouse-committed are separate states with separate recovery actions.

Use explicit SQL for deduplication, temporal rebuilding, `MERGE`, and quality checks. Preserve conflict candidates and source provenance. Keep optimization choices measurable: no claimed improvement without captured EXPLAIN plans, equal query results, dataset identity, and actual repeated timings.

## Alternatives considered

- Loading by S3 prefix is simpler but can admit unexpected files and incomplete deliveries.
- Treating a Glue success flag as warehouse completion hides the second commit boundary.
- Public database endpoints reduce setup work but unnecessarily expose the warehouse network surface.
- Inferring all schemas with crawlers turns upstream drift into an accidental contract change.
- API idempotency tokens alone do not protect reruns beyond their retention period.

## Consequences

Operators supply a working VPC/S3 route and explicitly provision paid resources. Data API authentication avoids committed database passwords. The implementation needs release validation, a durable ledger, and recovery documentation. Catalog publication and S3 object writes remain outside the warehouse transaction and must be reconciled using explicit release state. Cloud performance and deployment status remain unverified until an actual cloud run produces evidence.

Related design: [AWS deployment and publication](../aws-deployment.md).

## Implementation boundary

The delivered publisher accepts a validated typed product snapshot from either the local correctness profile or the integrated Spark profile. The Spark path connects manifested raw inputs, prior accepted evidence, deterministic reference resolution, typed canonical staging, historical interval reconstruction, dimensional facts, SSOT products, and quality gates. It writes a version-2 release containing all 24 products as explicitly listed shards. The local profile remains a second supported export path.

The Redshift loader verifies the release files and controls, locks the durable release ledger, applies fact MERGEs and complete dimension/reference replacements, and commits the release sequence atomically. The integrated Spark full/incremental flow and all-product model parity have passed locally. Terraform validation, offline API tests, and SQL parsing remain separate evidence from live AWS execution, which has not been performed.
