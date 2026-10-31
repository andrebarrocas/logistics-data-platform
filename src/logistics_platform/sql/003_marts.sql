-- One reusable temporal attribution relation for all declared fact grains.
CREATE OR REPLACE TABLE fact_attribution AS
WITH facts AS (
    SELECT 'shipment' AS fact_type,canonical_id AS fact_id,canonical_id AS shipment_id,
        created_at AS business_at,carrier_id,origin_facility_id,destination_facility_id,
        route_id,country_id,service_level_id FROM canonical_shipment
    UNION ALL
    SELECT 'event',e.event_canonical_id,e.shipment_id,e.occurred_at,
        coalesce(e.carrier_id,s.carrier_id),s.origin_facility_id,s.destination_facility_id,
        s.route_id,s.country_id,s.service_level_id
    FROM canonical_delivery_event e LEFT JOIN canonical_shipment s ON e.shipment_id=s.canonical_id
    UNION ALL
    SELECT 'cost',c.cost_canonical_id,c.shipment_id,c.incurred_at,
        coalesce(c.carrier_id,s.carrier_id),s.origin_facility_id,s.destination_facility_id,
        s.route_id,s.country_id,s.service_level_id
    FROM canonical_transportation_cost c LEFT JOIN canonical_shipment s ON c.shipment_id=s.canonical_id
    WHERE NOT c.is_deleted
)
SELECT f.*,coalesce(c.carrier_sk,0) AS carrier_sk,
    coalesce(o.facility_sk,0) AS origin_facility_sk,
    coalesce(d.facility_sk,0) AS destination_facility_sk,
    coalesce(r.route_sk,0) AS route_sk,
    coalesce(co.country_sk,0) AS country_sk,
    coalesce(re.region_sk,0) AS region_sk,
    coalesce(sl.service_level_sk,0) AS service_level_sk,
    o.country_id AS origin_country_id,
    d.country_id AS destination_country_id,
    o.region_id AS origin_region_id,
    d.region_id AS destination_region_id,
    coalesce(dr.region_sk,0) AS destination_region_sk,
    coalesce(oc.country_sk,0) AS origin_country_sk,
    coalesce(dc.country_sk,0) AS destination_country_sk
FROM facts f
LEFT JOIN dim_carrier c ON f.carrier_id=c.canonical_id AND f.business_at>=c.effective_from AND f.business_at<c.effective_to
LEFT JOIN dim_facility o ON f.origin_facility_id=o.canonical_id AND f.business_at>=o.effective_from AND f.business_at<o.effective_to
LEFT JOIN dim_facility d ON f.destination_facility_id=d.canonical_id AND f.business_at>=d.effective_from AND f.business_at<d.effective_to
LEFT JOIN dim_route r ON f.route_id=r.canonical_id AND f.business_at>=r.effective_from AND f.business_at<r.effective_to
LEFT JOIN dim_country co ON f.country_id=co.canonical_id AND f.business_at>=co.effective_from AND f.business_at<co.effective_to
LEFT JOIN dim_country oc ON o.country_id=oc.canonical_id AND f.business_at>=oc.effective_from AND f.business_at<oc.effective_to
LEFT JOIN dim_country dc ON d.country_id=dc.canonical_id AND f.business_at>=dc.effective_from AND f.business_at<dc.effective_to
LEFT JOIN dim_region re ON o.region_id=re.canonical_id AND f.business_at>=re.effective_from AND f.business_at<re.effective_to
LEFT JOIN dim_region dr ON d.region_id=dr.canonical_id AND f.business_at>=dr.effective_from AND f.business_at<dr.effective_to
LEFT JOIN dim_service_level sl ON f.service_level_id=sl.canonical_id AND f.business_at>=sl.effective_from AND f.business_at<sl.effective_to;

CREATE OR REPLACE TABLE fact_shipment AS
SELECT s.canonical_id AS shipment_id,s.business_key,s.order_id,s.created_at,s.promised_at,
    s.actual_delivery_at,s.state,s.is_deleted,s.package_count,1::BIGINT AS shipment_count,
    s.state_source_system,s.state_observation_id,s.state_authority,s.state_rule_id,
    a.carrier_id,a.origin_facility_id,a.destination_facility_id,a.route_id,a.country_id,a.service_level_id,
    a.carrier_sk,a.origin_facility_sk,a.destination_facility_sk,a.route_sk,a.country_sk,
    a.region_sk,a.destination_region_sk,a.origin_country_sk,a.destination_country_sk,a.service_level_sk,
    coalesce(try_cast(strftime(s.created_at,'%Y%m%d') AS INTEGER),0) AS created_date_sk,
    coalesce(try_cast(strftime(s.promised_at,'%Y%m%d') AS INTEGER),0) AS promised_date_sk,
    coalesce(try_cast(strftime(s.actual_delivery_at,'%Y%m%d') AS INTEGER),0) AS delivered_date_sk,
    s.raw_hash,(SELECT run_id FROM projection_context) AS run_id
FROM canonical_shipment s JOIN fact_attribution a ON a.fact_type='shipment' AND a.fact_id=s.canonical_id;

CREATE OR REPLACE TABLE fact_delivery_event AS
SELECT e.* EXCLUDE(run_id),e.run_id AS source_run_id,
    (SELECT run_id FROM projection_context) AS run_id,1::BIGINT AS event_count,
    date_diff('second',e.occurred_at,e.source_updated_at) AS source_recording_delay_seconds,
    a.carrier_sk,a.origin_facility_sk,a.destination_facility_sk,a.route_sk,a.country_sk,
    a.region_sk,a.destination_region_sk,a.origin_country_sk,a.destination_country_sk,a.service_level_sk,
    coalesce(try_cast(strftime(e.occurred_at,'%Y%m%d') AS INTEGER),0) AS event_date_sk
FROM canonical_delivery_event e JOIN fact_attribution a ON a.fact_type='event' AND a.fact_id=e.event_canonical_id;

CREATE OR REPLACE TABLE fact_transportation_cost AS
SELECT c.* EXCLUDE(run_id),c.run_id AS source_run_id,
    (SELECT run_id FROM projection_context) AS run_id,a.carrier_sk,a.origin_facility_sk,a.destination_facility_sk,a.route_sk,a.country_sk,
    a.region_sk,a.destination_region_sk,a.origin_country_sk,a.destination_country_sk,a.service_level_sk,
    coalesce(try_cast(strftime(c.incurred_at,'%Y%m%d') AS INTEGER),0) AS incurred_date_sk
FROM canonical_transportation_cost c JOIN fact_attribution a ON a.fact_type='cost' AND a.fact_id=c.cost_canonical_id
WHERE NOT c.is_deleted;

CREATE OR REPLACE TABLE dim_date AS
WITH dates AS (
    SELECT created_at::DATE AS full_date FROM canonical_shipment
    UNION SELECT promised_at::DATE FROM canonical_shipment
    UNION SELECT actual_delivery_at::DATE FROM canonical_shipment
    UNION SELECT occurred_at::DATE FROM canonical_delivery_event
    UNION SELECT incurred_at::DATE FROM canonical_transportation_cost
), valid_dates AS (SELECT full_date FROM dates WHERE full_date IS NOT NULL)
SELECT cast(strftime(full_date,'%Y%m%d') AS INTEGER) AS date_sk,full_date,
    year(full_date)::INTEGER AS year_number,quarter(full_date)::INTEGER AS quarter_number,
    month(full_date)::INTEGER AS month_number,isodow(full_date)::INTEGER AS iso_weekday,
    isodow(full_date) IN (6,7) AS is_weekend
FROM valid_dates
UNION ALL SELECT 0,NULL,NULL,NULL,NULL,NULL,NULL;

CREATE OR REPLACE TABLE pending_fact_reference AS
SELECT a.fact_type,a.fact_id,a.shipment_id,a.business_at,x.dimension_name,x.canonical_reference,
    'PENDING_REFERENCE' AS reason,(SELECT run_id FROM projection_context) AS run_id
FROM fact_attribution a,
LATERAL (VALUES
    ('carrier',a.carrier_id,a.carrier_sk),
    ('origin_facility',a.origin_facility_id,a.origin_facility_sk),
    ('destination_facility',a.destination_facility_id,a.destination_facility_sk),
    ('route',a.route_id,a.route_sk),
    ('country',a.country_id,a.country_sk),
    ('origin_region',a.origin_region_id,a.region_sk),
    ('destination_region',a.destination_region_id,a.destination_region_sk),
    ('service_level',a.service_level_id,a.service_level_sk)
) x(dimension_name,canonical_reference,resolved_sk)
WHERE x.resolved_sk=0;
