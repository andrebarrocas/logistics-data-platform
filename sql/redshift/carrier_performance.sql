-- Template replacements are controlled by benchmark_redshift.py.
SELECT d.carrier_name,f.carrier_sk,COUNT(*)::BIGINT AS shipments,
    SUM(CASE WHEN f.state='delivered' THEN 1 ELSE 0 END)::BIGINT AS delivered_shipments,
    SUM(f.package_count)::BIGINT AS packages
FROM benchmark_logistics.{{table}} f
JOIN benchmark_logistics.carrier_all d ON f.carrier_sk=d.carrier_sk
WHERE f.created_at >= TIMESTAMP '{{date_from}}'
  AND f.created_at < TIMESTAMP '{{date_to}}'
  AND NOT f.is_deleted
GROUP BY d.carrier_name,f.carrier_sk
ORDER BY d.carrier_name,f.carrier_sk;
