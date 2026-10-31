SELECT d.carrier_name,f.carrier_sk,SUM(f.shipments)::BIGINT AS shipments,
    SUM(f.delivered_shipments)::BIGINT AS delivered_shipments,SUM(f.packages)::BIGINT AS packages
FROM benchmark_logistics.daily_carrier f
JOIN benchmark_logistics.carrier_all d ON f.carrier_sk=d.carrier_sk
WHERE f.created_date >= DATE '{{date_from}}'
  AND f.created_date < DATE '{{date_to}}'
GROUP BY d.carrier_name,f.carrier_sk
ORDER BY d.carrier_name,f.carrier_sk;
