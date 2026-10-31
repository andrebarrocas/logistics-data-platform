-- Consumers receive readable names, explicit quality status and traceable IDs.
CREATE OR REPLACE VIEW ssot_shipments AS
SELECT f.*,c.carrier_name,ofac.facility_name AS origin_facility,
    dfac.facility_name AS destination_facility,r.route_name,co.country_name,
    re.region_name AS origin_region,sl.service_level_name,
    CASE WHEN EXISTS (SELECT 1 FROM pending_fact_reference p
        WHERE p.fact_type='shipment' AND p.fact_id=f.shipment_id)
        THEN 'DEGRADED_PENDING_REFERENCE' ELSE 'PASSED' END AS quality_status,
    'Logistics Data Platform' AS product_owner
FROM fact_shipment f
JOIN dim_carrier c USING(carrier_sk)
JOIN dim_facility ofac ON f.origin_facility_sk=ofac.facility_sk
JOIN dim_facility dfac ON f.destination_facility_sk=dfac.facility_sk
JOIN dim_route r USING(route_sk)
JOIN dim_country co USING(country_sk)
JOIN dim_region re USING(region_sk)
JOIN dim_service_level sl USING(service_level_sk)
WHERE NOT f.is_deleted;

CREATE OR REPLACE VIEW ssot_delivery_events AS
SELECT f.*,c.carrier_name,
    CASE WHEN EXISTS (SELECT 1 FROM pending_fact_reference p
        WHERE p.fact_type='event' AND p.fact_id=f.event_canonical_id)
        THEN 'DEGRADED_PENDING_REFERENCE' ELSE 'PASSED' END AS quality_status,
    'Logistics Data Platform' AS product_owner
FROM fact_delivery_event f JOIN dim_carrier c USING(carrier_sk);

CREATE OR REPLACE VIEW ssot_transportation_costs AS
SELECT f.*,c.carrier_name,r.route_name,sl.service_level_name,
    CASE WHEN EXISTS (SELECT 1 FROM pending_fact_reference p
        WHERE p.fact_type='cost' AND p.fact_id=f.cost_canonical_id)
        THEN 'DEGRADED_PENDING_REFERENCE' ELSE 'PASSED' END AS quality_status,
    'Logistics Data Platform' AS product_owner
FROM fact_transportation_cost f JOIN dim_carrier c USING(carrier_sk)
JOIN dim_route r USING(route_sk) JOIN dim_service_level sl USING(service_level_sk);
