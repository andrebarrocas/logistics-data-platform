-- Reference versions: correct boundaries, compare tracked attributes, find islands,
-- collapse unchanged adjacent assertions, then calculate exclusive end times.
CREATE OR REPLACE TABLE dim_reference AS
WITH corrected AS (
    SELECT *, row_number() OVER (
        PARTITION BY entity_type, canonical_id, effective_from
        ORDER BY revision DESC, source_updated_at DESC, source_id DESC
    ) AS boundary_rank,
    first_value(canonical_value) OVER (
        PARTITION BY entity_type, canonical_id
        ORDER BY source_updated_at DESC, revision DESC, effective_from DESC, source_id DESC
    ) AS current_label,
    max(source_updated_at) OVER (PARTITION BY entity_type, canonical_id) AS modified_at
    FROM stg_reference
    WHERE entity_type IN ('carrier','facility','route','country','region','service_level')
), tracked AS (
    SELECT *, CASE
        WHEN entity_type='carrier' THEN json_object(
            'default_service_key',attributes->>'default_service_key',
            'operating_status',attributes->>'operating_status','status',status)
        WHEN entity_type='facility' THEN json_object(
            'country_key',attributes->>'country_key','region_key',attributes->>'region_key',
            'operating_status',attributes->>'operating_status','status',status)
        WHEN entity_type='route' THEN json_object(
            'origin_facility_key',attributes->>'origin_facility_key',
            'destination_facility_key',attributes->>'destination_facility_key',
            'distance_km',attributes->>'distance_km',
            'operating_status',attributes->>'operating_status','status',status)
        WHEN entity_type='service_level' THEN json_object(
            'target_business_days',attributes->>'target_business_days',
            'operating_status',attributes->>'operating_status','status',status)
        ELSE json_object('status',status) END AS tracked_attributes
    FROM corrected WHERE boundary_rank=1
), previous AS (
    SELECT *, lag(tracked_attributes::VARCHAR) OVER (
        PARTITION BY entity_type,canonical_id ORDER BY effective_from
    ) AS previous_attributes FROM tracked
), boundaries AS (
    SELECT *, CASE WHEN previous_attributes IS DISTINCT FROM tracked_attributes::VARCHAR
        THEN 1 ELSE 0 END AS starts_island FROM previous
), islands AS (
    SELECT *, sum(starts_island) OVER (
        PARTITION BY entity_type,canonical_id ORDER BY effective_from
        ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
    ) AS island_id FROM boundaries
), island_rows AS (
    SELECT *, row_number() OVER (
        PARTITION BY entity_type,canonical_id,island_id ORDER BY effective_from
    ) AS island_row FROM islands
), periods AS (
    SELECT *, lead(effective_from,1,TIMESTAMP '9999-12-31 00:00:00') OVER (
        PARTITION BY entity_type,canonical_id ORDER BY effective_from
    ) AS effective_to FROM island_rows WHERE island_row=1
)
SELECT entity_type, business_key, canonical_id, current_label AS canonical_value,
    tracked_attributes AS attributes, effective_from, effective_to,
    surrogate_key(entity_type,canonical_id,strftime(effective_from,'%Y-%m-%dT%H:%M:%S.%fZ')) AS surrogate_key,
    effective_to=TIMESTAMP '9999-12-31 00:00:00' AS is_current,
    status,source_id,'reference_master' AS source_system,source_updated_at,modified_at,
    (SELECT recorded_at FROM projection_context) AS recorded_at,raw_hash,(SELECT run_id FROM projection_context) AS run_id
FROM periods
UNION ALL
SELECT entity_type,'__UNKNOWN__',NULL,'Unknown','{}'::JSON,
    TIMESTAMP '1900-01-01',TIMESTAMP '9999-12-31',0,TRUE,'UNKNOWN',NULL,'reference_master',NULL::TIMESTAMP,NULL::TIMESTAMP,
    (SELECT recorded_at FROM projection_context),NULL,
    (SELECT run_id FROM projection_context)
FROM (VALUES ('carrier'),('facility'),('route'),('country'),('region'),('service_level')) t(entity_type);

CREATE OR REPLACE TABLE dim_carrier AS
SELECT surrogate_key AS carrier_sk,canonical_id, business_key, canonical_value AS carrier_name,
    attributes->>'operating_status' AS operating_status,
    canonical_id('service_level',attributes->>'default_service_key') AS default_service_id,
    effective_from,effective_to,is_current,status,raw_hash,run_id
FROM dim_reference WHERE entity_type='carrier';
CREATE OR REPLACE TABLE dim_facility AS
SELECT surrogate_key AS facility_sk,canonical_id,business_key,canonical_value AS facility_name,
    canonical_id('country',attributes->>'country_key') AS country_id,
    canonical_id('region',attributes->>'region_key') AS region_id,
    attributes->>'operating_status' AS operating_status,
    effective_from,effective_to,is_current,status,raw_hash,run_id
FROM dim_reference WHERE entity_type='facility';
CREATE OR REPLACE TABLE dim_route AS
SELECT surrogate_key AS route_sk,canonical_id,business_key,canonical_value AS route_name,
    canonical_id('facility',attributes->>'origin_facility_key') AS origin_facility_id,
    canonical_id('facility',attributes->>'destination_facility_key') AS destination_facility_id,
    try_cast(attributes->>'distance_km' AS DECIMAL(12,3)) AS distance_km,
    attributes->>'operating_status' AS operating_status,
    effective_from,effective_to,is_current,status,raw_hash,run_id
FROM dim_reference WHERE entity_type='route';
CREATE OR REPLACE TABLE dim_country AS
SELECT surrogate_key AS country_sk,canonical_id,business_key,canonical_value AS country_name,
    effective_from,effective_to,is_current,status,raw_hash,run_id
FROM dim_reference WHERE entity_type='country';
CREATE OR REPLACE TABLE dim_region AS
SELECT surrogate_key AS region_sk,canonical_id,business_key,canonical_value AS region_name,
    effective_from,effective_to,is_current,status,raw_hash,run_id
FROM dim_reference WHERE entity_type='region';
CREATE OR REPLACE TABLE dim_service_level AS
SELECT surrogate_key AS service_level_sk,canonical_id,business_key,canonical_value AS service_level_name,
    try_cast(attributes->>'target_business_days' AS INTEGER) AS target_business_days,
    attributes->>'operating_status' AS operating_status,
    effective_from,effective_to,is_current,status,raw_hash,run_id
FROM dim_reference WHERE entity_type='service_level';

CREATE OR REPLACE VIEW carrier_reference AS SELECT * FROM dim_reference WHERE entity_type='carrier' AND surrogate_key<>0;
CREATE OR REPLACE VIEW facility_reference AS SELECT * FROM dim_reference WHERE entity_type='facility' AND surrogate_key<>0;
CREATE OR REPLACE VIEW route_reference AS SELECT * FROM dim_reference WHERE entity_type='route' AND surrogate_key<>0;
CREATE OR REPLACE VIEW country_reference AS SELECT * FROM dim_reference WHERE entity_type='country' AND surrogate_key<>0;
CREATE OR REPLACE VIEW region_reference AS SELECT * FROM dim_reference WHERE entity_type='region' AND surrogate_key<>0;
CREATE OR REPLACE VIEW service_level_reference AS SELECT * FROM dim_reference WHERE entity_type='service_level' AND surrogate_key<>0;
