# AWS deployment and publication design

This is a public portfolio system for independently generated synthetic data. AWS is a deployment target; this project does not claim affiliation with Amazon or any logistics company. Local execution and AWS execution are distinct verification levels. A successful local test is not an AWS benchmark or evidence of a successful cloud deployment.

## Implemented execution profiles

Two implemented profiles share one typed publication contract. The local `logistics demo` / `run` path uses Python and DuckDB SQL as a correctness reference. The distributed `jobs/normalize.py --build_products` path uses PySpark and Spark SQL for ingestion, effective alias resolution, canonical entities, SCD history, temporal fact attribution, all 24 product tables, and blocking quality checks. It writes bounded CSV shards from executors, then a release manifest and completion marker. The same `logistics load` command verifies either profile's S3 release and publishes it to Redshift.

Cross-batch Spark evidence is persisted as Parquet. An incremental job requires the exact preceding accepted attempt's `evidence/` URI; no event-time watermark discards late arrivals. Both profiles currently rebuild full projections from accepted source revisions. Distributed row processing is implemented; impacted-key-only projection, automatic scheduling, and automatic release discovery are not claimed.

The 24 products include canonical, dimension, fact, reference, and SSOT tables. Spark evidence, resolution decisions, staging boundaries, and quality artifacts remain under the attempt prefix; the release manifest links their locations and the source manifest hash. Local evidence and publication snapshots remain in the local warehouse. Retain raw bytes and quality evidence alongside published products. Local Spark full/incremental integration and all-product SQL parity run against real Spark. The Data API loader is tested with mocks and its generated standard SQL is parsed; the stored procedure and infrastructure have **not been executed on AWS** in the included evidence.

## Runtime and infrastructure contract

The deployment pins AWS Glue **5.0**: Apache Spark **3.5.4**, Python **3.11**, and Java **17**. The local Spark test environment uses the same versions. Glue versions must be explicit because service defaults change. The job also pins `boto3==1.40.40,botocore==1.40.76` through `--additional-python-modules`: its conditional S3 writes require SDK support newer than the Glue 5.0 bundled SDK. Runtime package installation requires access to the package index; for an isolated environment mirror these exact wheels to an approved repository. See the [AWS Glue runtime matrix](https://docs.aws.amazon.com/glue/latest/dg/release-notes.html).

Terraform creates three private, versioned, encrypted S3 buckets (raw, canonical, artifacts), a Glue Catalog database and typed `normalized_evidence` staging table, one Glue job, a private Redshift Serverless namespace/workgroup, narrowly scoped service roles, and CloudWatch logs. It consumes an existing VPC and at least three private subnets across three availability zones. The project does not provision a new corporate network, broad administrator credentials, or automatic paid benchmark workloads. Infrastructure provisioning is an explicit operator command.

| Component | Responsibility | Failure boundary |
|---|---|---|
| S3 raw | Immutable source bytes plus producer delivery manifests | A missing or mismatched part blocks that source delivery |
| Glue / Spark | Schema checks, deterministic resolution, Spark SQL canonical/SCD/marts, quality gates, bounded release shards | Failed critical checks produce evidence but no publishable release |
| S3 canonical | Immutable release-scoped typed exports and hash manifest | Staged objects are invisible to product consumers until committed |
| Glue Catalog | Explicit types and run-partition locations for normalized evidence | Crawlers cannot silently approve an incompatible schema |
| Redshift | Typed integration tables, SCD history, facts, products, run ledger | One transaction publishes a complete warehouse release |
| CloudWatch | Job logs and failure/quality signals | Logs support investigation; durable quality results remain in S3 |

## Network, encryption, and identity

Redshift has `publicly_accessible = false` and enhanced VPC routing. No public database ingress is opened. Any optional JDBC access must come from an explicit trusted security group. The supplied route tables must support S3 access through an S3 gateway endpoint or an approved NAT path, with compatible network ACLs. Enhanced routing does not itself create connectivity. See [AWS Redshift network isolation](https://docs.aws.amazon.com/redshift/latest/mgmt/security-network-isolation.html).

The Glue job reads S3 directly and does not require direct JDBC access to Redshift. The operator loads Redshift through its authenticated Data API; that API is separate from the private database socket. A fully private runner requires interface endpoints for the AWS APIs it calls. Public GitHub runners can run tests without AWS credentials; cloud deployment requires a separately configured OIDC role or an operator's authenticated AWS session.

S3 public access is blocked, bucket ownership is enforced, TLS is required, and default server-side encryption and versioning are enabled. SSE-S3 is the baseline for synthetic data; customer-managed KMS is a documented extension requiring corresponding service key grants. Lifecycle cleanup may remove abandoned staging objects after a retention period, but must never purge currently referenced release objects. Terraform state belongs in an encrypted access-controlled backend when deployed; local state and credentials are never committed.

Glue can read raw input and job artifacts and write only canonical/log outputs. Redshift can read only canonical release objects required by `COPY`. Neither role can create IAM roles or change bucket policies. An operator supplies a separately managed authenticated role; restrict it to the intended service jobs and Data API statements. Terraform does not create an administrator or GitHub deployment role. Database grants are separate from IAM grants: deployment bootstrap creates schemas; the pipeline database user receives only the required schema/table permissions.

## End-to-end design and implemented publication protocol

1. A producer writes a delivery manifest containing source, delivery ID, schema version, expected part names, byte counts, SHA-256 hashes, row counts, and control totals (amount plus currency where relevant). Independent producer controls make a partial feed detectable. Listing a folder and counting what happened to arrive is insufficient.
2. Cloud raw objects are immutable under a batch namespace; the producer manifest identifies content hashes. Local raw evidence is retained under its content hash. Ingestion verifies bytes and manifest membership, records source object identity and hash, and rejects incompatible contracts. Duplicate content under another filename is recorded as a replay, not silently counted as new business data.
3. Glue writes normalized evidence into a unique attempt prefix, unions the prior accepted evidence, applies revision selection, and constructs typed stages. Spark SQL derives canonical entities, effective-dated reference dimensions, facts, and products. Parquet materialization boundaries limit query-plan expansion. Quality gates check identity, temporal, foreign-key, transition, count, and exact signed financial invariants before release export. Source change positions and business event time are separate: late business events are still new changes.
4. Both exporters require passing critical quality checks. The local export uses a consistent database read transaction and one CSV per table (manifest v1). Spark uses executor-side bounded CSV shards, deterministic key partitioning/order, and streaming hashes (manifest v2). Both manifests declare byte lengths, SHA-256 hashes, row counts, columns, primary/foreign keys, currency totals, sequence, and quality evidence. The S3 publisher writes exact per-table COPY object manifests, then `COMMITTED.json`. Source run IDs and raw hashes remain in exported rows. Consumers never discover releases by listing unfinished prefixes. The final marker uses an S3 conditional write to prevent accidental overwrite; S3 does not provide a multi-object transaction. See [conditional writes](https://docs.aws.amazon.com/AmazonS3/latest/userguide/conditional-writes.html).
5. The Redshift loader reads only the selected completed manifest, validates it, creates temporary typed staging tables, and issues `COPY` with exact mandatory object entries. The implemented CSV exports specify column order, a reserved null token, and automatic timestamp parsing. An eventual Parquet COPY implementation must additionally supply `meta.content_length`; the current warehouse loader does not use Parquet. `COPY` does not validate an application SHA-256 manifest itself; publisher verification is mandatory. See [Redshift manifest loading](https://docs.aws.amazon.com/redshift/latest/dg/loading-data-files-using-manifest.html).
6. One warehouse transaction checks uniqueness and reconciliation, applies deduplicated fact `MERGE` statements, replaces complete dimension/reference snapshots including corrected SCD timelines, restates dependent facts, and records the release hash/schema hash/sequence in `ssot.publication_runs`. Fact MERGE sources are unique full snapshots; absent fact keys are synchronized only because upstream completeness checks guard the full snapshot. Ingestion is incremental; cloud publication uses complete product snapshots. The S3 manifest holds quality evidence and the ingestion sequence; local per-source cursors or Spark retained evidence preserve source change positions. A failed statement rolls everything back. The Data API batch runs serially in a single transaction by default; the loader must not select auto-commit mode. See [BatchExecuteStatement](https://docs.aws.amazon.com/redshift-data/latest/APIReference/API_BatchExecuteStatement.html).
7. A durable warehouse run ledger guards reruns after the Data API token retention window. A serial publisher and transactional ledger lock prevent concurrent overlapping releases. A committed run is a no-op only when its release manifest identity matches exactly; reusing a run ID with different content is an error. After an uncertain API response, inspect the statement ID and ledger before retrying.

S3 completion means **canonical release ready**; it does not mean **warehouse published**. The warehouse ledger is authoritative for warehouse visibility. There is deliberately no claim of an atomic distributed transaction across S3, Glue Catalog, and Redshift. Catalog consumers select immutable release locations; product consumers use a committed warehouse release. A failure after S3 publication is recoverable by loading that same release.

`MERGE` source keys must be unique. Redshift can reject multiple source matches; duplicate removal must be an explicit deterministic upstream operation that preserves evidence, not an unexplained `REMOVE DUPLICATES`. See [Redshift MERGE semantics](https://docs.aws.amazon.com/redshift/latest/dg/r_MERGE.html).

## Operations and acceptance

Local runs emit structured quality and reconciliation results. Glue writes logs to configured CloudWatch log groups with retention, while durable stage results remain in S3. No SNS recipient or operational paging subscription is provisioned. The operator should configure routing for job failure, missing delivery, critical quality failure, late backlog, and freshness breach. These conditions are design targets, not observed production service levels. A monthly Serverless usage limit deactivates queries at the configured RPU-hour boundary; this is a usage guardrail, not a monetary budget guarantee.

Deployment acceptance requires Terraform validation, review of the plan, a synthetic full load, an incremental load, an identical rerun, an intentionally incomplete feed, a historical correction, and reconciliation of exact published totals. Record account-independent runtime details, query IDs, EXPLAIN output, actual timings, and data hashes when cloud tests run. Do not publish account IDs, credentials, private addresses, or sensitive Terraform state.

Benchmarks compare fixed synthetic datasets and equivalent result sets, disable the result cache, record distribution/sort choices and capacity, warm-up policy, repetitions, and the complete timing series. Local timings are useful development measurements but cannot substantiate a Redshift improvement. Without an available deployed warehouse, the honest outcome is **Redshift benchmark not run** plus an executable benchmark harness.


## Reproduce the validated product publication path

Run these from the repository root after installing `.[dev,aws]`. The first three commands are local and do not need AWS credentials:

```bash
logistics demo --output work/demo --shipments 1000
logistics export --database work/demo/warehouse.duckdb --output work/release-002 --release-id release_002
python scripts/build_glue_zip.py
```

Prepare `infra/terraform.tfvars` from the checked-in example using an existing private VPC/subnets. Review the route tables, S3 endpoint policy, free IP capacity, and database/network administration permissions. Terraform validation alone cannot verify these account-specific prerequisites.

```bash
terraform -chdir=infra init
terraform -chdir=infra fmt -check
terraform -chdir=infra validate
terraform -chdir=infra plan -out=work.tfplan
# Explicit operator action after reviewing the plan; creates paid AWS resources:
terraform -chdir=infra apply work.tfplan
```

Set the standard AWS region/profile environment for the target account. Use the actual canonical bucket, workgroup, and COPY role from Terraform outputs. The following commands write to AWS only when explicitly invoked:

```bash
logistics upload --input work/release-002 --bucket YOUR_CANONICAL_BUCKET --prefix releases
logistics load --manifest-uri s3://YOUR_CANONICAL_BUCKET/releases/release_002/release.json --workgroup YOUR_WORKGROUP --database logistics --copy-role-arn YOUR_COPY_ROLE_ARN
```

The first load bootstraps `ssot` tables and a durable publication ledger through the Data API. Its IAM-derived database user needs schema creation, procedure creation, table DDL/DML, and permission to use the COPY role. Production separation of bootstrap DDL and routine DML identities is an operator extension. AWS permissions required by the invoking identity include scoped S3 release read/write, Redshift Data API submit/describe/result access, and Serverless temporary credentials for the chosen workgroup. Benchmarking also needs `redshift-serverless:GetWorkgroup` and database access to its own SYS/SVV/PG views.

Do not run overlapping operators; the transactional publication ledger serializes committed changes, while bootstrap is deliberately simple. New schema hashes are rejected until an explicit migration aligns all tables and the ledger. A timed-out API call produces an unknown-outcome error containing the statement ID: inspect that statement and `ssot.publication_runs` before retrying. A failed transaction leaves the previous products visible. After repairing a confirmed failed execution, invoke `logistics load` again with the same immutable manifest. The SDK generates a fresh request token for that invocation and preserves it for transport retries; the locked warehouse ledger supplies durable publication idempotency. A manifest-derived token would otherwise keep returning the previous execution during the service's token retention window. An exact same-manifest rerun after a successful commit is a no-op; an older sequence or changed release identity fails. See the [Data API idempotency guidance](https://docs.aws.amazon.com/redshift/latest/mgmt/data-api.html).

The local export writes one object per table, and its upload helper reads one file into memory. Use the Spark profile for distributed exports: CSV shards are bounded to 100,000 rows / 64 MiB, with 8 MiB spillable buffers. The driver collects shard metadata, not fact rows. Read-back checksum verification streams. These bounds do not establish a measured Glue or Redshift throughput limit.

## Run the distributed Glue-to-Redshift path

The Terraform job points at the generated `dist/logistics_platform.zip` and `jobs/normalize.py` artifacts. Build the zip before `terraform plan`. Generate the source data locally, then publish raw objects with the supplied conditional uploader; ordinary `aws s3 cp` does not supply the required immutable-create header.

```bash
logistics generate --output work/cloud-input --shipments 10000 --seed 42
logistics upload-inputs --manifest work/cloud-input/input/batch001/manifest.json \
  --bucket YOUR_RAW_BUCKET --prefix deliveries

aws glue start-job-run --job-name YOUR_GLUE_JOB --arguments '{
  "--manifest_uri":"s3://YOUR_RAW_BUCKET/deliveries/batch001/manifest.json",
  "--output_uri":"s3://YOUR_CANONICAL_BUCKET/normalized/run_id=attempt001",
  "--build_products":"true",
  "--release_uri":"s3://YOUR_CANONICAL_BUCKET/releases/release_001",
  "--release_id":"release_001"
}'
```

Wait for the job to succeed and inspect the attempt's `quality/` JSON and release `COMMITTED.json`. Invoke the loader on that exact manifest:

```bash
logistics load --manifest-uri s3://YOUR_CANONICAL_BUCKET/releases/release_001/release.json \
  --workgroup YOUR_WORKGROUP --database logistics --copy-role-arn YOUR_COPY_ROLE_ARN

logistics upload-inputs --manifest work/cloud-input/input/batch002/manifest.json \
  --bucket YOUR_RAW_BUCKET --prefix deliveries

aws glue start-job-run --job-name YOUR_GLUE_JOB --arguments '{
  "--manifest_uri":"s3://YOUR_RAW_BUCKET/deliveries/batch002/manifest.json",
  "--history_uri":"s3://YOUR_CANONICAL_BUCKET/normalized/run_id=attempt001/evidence",
  "--output_uri":"s3://YOUR_CANONICAL_BUCKET/normalized/run_id=attempt002",
  "--build_products":"true",
  "--release_uri":"s3://YOUR_CANONICAL_BUCKET/releases/release_002",
  "--release_id":"release_002"
}'
```

After success, load `release_002/release.json` using the same command. An incomplete feed, ambiguous mapping, missing predecessor, or failing critical gate cannot advance accepted history. Retry a failed computation with fresh attempt/release paths and the last accepted evidence. If a release already completed and only warehouse delivery failed, reuse that release directly; do not regenerate it. Rerunning the exact validated release through the loader is idempotent. A newly generated release must never overwrite an existing release ID, even if the intended source batch is the same.

For local execution of the identical Spark job, use the `spark-submit` command in the README; add `--history_uri` pointing to the previous successful attempt's `evidence/` for batch 2. Local v2 release directories can be published with `logistics upload`, which creates S3 COPY manifests and the completion marker.

## Glue Catalog and retention

The Catalog table `normalized_evidence` is partitioned by `run_id`. After a successful product-building run, register the exact `.../normalized/run_id=RUN_ID/evidence/` location using the table's existing Parquet storage descriptor. Failed or incomplete attempts must not be registered. This is an explicit operator administration step; the typed staging table is distinct from curated warehouse products.

The Glue role can read raw/history/artifacts and write `normalized/`, `glue-temp/`, and immutable `releases/`. The raw bucket and canonical `releases/` prefix enforce `If-None-Match: *` on object creation. The raw uploader writes the original manifest last and accepts exact-byte replays. S3 versioning and hashes support recovery; manifest controls detect partial deliveries. Cleanup may remove abandoned attempt prefixes only after confirming they are not linked by any accepted release/history chain. Terraform does not force bucket deletion.

## Real Redshift experiment

After a successful real load, this command creates a disposable benchmark schema, makes equivalent full snapshots, and measures three alternatives. It performs paid warehouse work only when invoked:

```bash
python scripts/benchmark_redshift.py --workgroup YOUR_WORKGROUP --database logistics --region eu-west-1 --prepare --repetitions 5 --warmups 1 --date-from 2026-01-01 --date-to 2027-01-01 --output work/redshift-benchmark.json
```

The alternatives are EVEN distribution with shipment-key sorting, carrier-key distribution with date sorting, and a daily carrier materialized view. The harness verifies exact clone equivalence, disables result caching and automatic materialized-view rewriting for every query, captures EXPLAIN and `SYS_QUERY_HISTORY` server timings, checks analytical result equality, interleaves repetitions, and verifies the source release stayed unchanged. It preserves failed-run evidence instead of manufacturing missing timings. Materialized-view preparation cost is outside the timed read workload; the report must not be interpreted as total pipeline savings. Distribution/sort changes are a combined treatment; use the captured plan and follow-up controlled experiments before attributing an effect to one setting. See [SYS_QUERY_HISTORY field definitions](https://docs.aws.amazon.com/redshift/latest/dg/SYS_QUERY_HISTORY.html).

Automatic rewriting is disabled with the documented [mv_enable_aqmv_for_session setting](https://docs.aws.amazon.com/redshift/latest/dg/r_mv_enable_aqmv_for_session.html), so the base-table treatment cannot silently use the explicit materialized-view alternative. The report also captures actual column encodings; no compression benefit is presumed.
