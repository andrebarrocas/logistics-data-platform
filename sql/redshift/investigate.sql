-- Operational read-only inspection. Query availability depends on database grants.
SELECT query_id,status,elapsed_time,queue_time,execution_time,result_cache_hit,
    returned_rows,redshift_version
FROM sys_query_history
WHERE start_time >= DATEADD(hour,-1,GETDATE())
ORDER BY start_time DESC;

-- Compare distribution skew, unsorted percentage, size, and compression choices.
SELECT "schema","table",diststyle,sortkey1,skew_rows,unsorted,size,encoded,stats_off
FROM svv_table_info
WHERE "schema" IN ('ssot','benchmark_logistics')
ORDER BY "schema","table";

SELECT schemaname,tablename,"column",type,encoding,distkey,sortkey
FROM pg_table_def
WHERE schemaname='benchmark_logistics'
ORDER BY tablename,sortkey,"column";

-- Audit product publication independently of S3 readiness.
SELECT release_id,manifest_sha256,sequence,schema_sha256,committed_at
FROM ssot.publication_runs ORDER BY sequence DESC;
