-- Run only in a disposable benchmark schema. This creates paid warehouse work.
-- Neither layout is declared faster; actual plans and repeated runs decide.
CREATE SCHEMA IF NOT EXISTS benchmark_logistics;
DROP MATERIALIZED VIEW IF EXISTS benchmark_logistics.daily_carrier;
DROP TABLE IF EXISTS benchmark_logistics.shipment_even;
DROP TABLE IF EXISTS benchmark_logistics.shipment_key;
DROP TABLE IF EXISTS benchmark_logistics.carrier_all;
CREATE TABLE benchmark_logistics.shipment_even DISTSTYLE EVEN SORTKEY (shipment_id)
AS SELECT * FROM ssot.fact_shipment;
CREATE TABLE benchmark_logistics.shipment_key DISTSTYLE KEY DISTKEY (carrier_sk)
SORTKEY (created_at,carrier_sk) AS SELECT * FROM ssot.fact_shipment;
CREATE TABLE benchmark_logistics.carrier_all DISTSTYLE ALL SORTKEY (carrier_sk)
AS SELECT * FROM ssot.dim_carrier;
ANALYZE benchmark_logistics.shipment_even;
ANALYZE benchmark_logistics.shipment_key;
ANALYZE benchmark_logistics.carrier_all;
CREATE MATERIALIZED VIEW benchmark_logistics.daily_carrier
DISTSTYLE KEY DISTKEY (carrier_sk) SORTKEY (created_date,carrier_sk)
AS SELECT carrier_sk,CAST(created_at AS DATE) AS created_date,
    COUNT(*) AS shipments,
    SUM(CASE WHEN state='delivered' THEN 1 ELSE 0 END) AS delivered_shipments,
    SUM(package_count) AS packages
FROM benchmark_logistics.shipment_key
WHERE NOT is_deleted
GROUP BY carrier_sk,CAST(created_at AS DATE);
