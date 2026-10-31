-- Spark 3.5 native projection, parity-tested against the local SQL contract.
-- Reference versions: correct boundaries, compare tracked attributes, find islands,
-- collapse unchanged adjacent assertions, then calculate exclusive end times.
CREATE OR REPLACE TEMP VIEW dim_reference AS
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
        WHEN entity_type='carrier' THEN to_json(named_struct(
            'default_service_key',get_json_object(attributes,'$.default_service_key'),
            'operating_status',get_json_object(attributes,'$.operating_status'),'status',status),map('ignoreNullFields','false'))
        WHEN entity_type='facility' THEN to_json(named_struct(
            'country_key',get_json_object(attributes,'$.country_key'),'region_key',get_json_object(attributes,'$.region_key'),
            'operating_status',get_json_object(attributes,'$.operating_status'),'status',status),map('ignoreNullFields','false'))
        WHEN entity_type='route' THEN to_json(named_struct(
            'origin_facility_key',get_json_object(attributes,'$.origin_facility_key'),
            'destination_facility_key',get_json_object(attributes,'$.destination_facility_key'),
            'distance_km',get_json_object(attributes,'$.distance_km'),
            'operating_status',get_json_object(attributes,'$.operating_status'),'status',status),map('ignoreNullFields','false'))
        WHEN entity_type='service_level' THEN to_json(named_struct(
            'target_business_days',get_json_object(attributes,'$.target_business_days'),
            'operating_status',get_json_object(attributes,'$.operating_status'),'status',status),map('ignoreNullFields','false'))
        ELSE to_json(named_struct('status',status),map('ignoreNullFields','false')) END AS tracked_attributes
    FROM corrected WHERE boundary_rank=1
), previous AS (
    SELECT *, lag(CAST(tracked_attributes AS STRING)) OVER (
        PARTITION BY entity_type,canonical_id ORDER BY effective_from
    ) AS previous_attributes FROM tracked
), boundaries AS (
    SELECT *, CASE WHEN NOT (previous_attributes <=> CAST(tracked_attributes AS STRING))
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
    surrogate_key(entity_type,canonical_id,concat(date_format(effective_from,'yyyy-MM-dd'),'T',date_format(effective_from,'HH:mm:ss.SSSSSS'),'Z')) AS surrogate_key,
    effective_to=TIMESTAMP '9999-12-31 00:00:00' AS is_current,
    status,source_id,'reference_master' AS source_system,source_updated_at,modified_at,
    (SELECT recorded_at FROM projection_context) AS recorded_at,raw_hash,(SELECT run_id FROM projection_context) AS run_id
FROM periods
UNION ALL
SELECT entity_type,'__UNKNOWN__',NULL,'Unknown','{}',
    TIMESTAMP '1900-01-01',TIMESTAMP '9999-12-31',0,TRUE,'UNKNOWN',NULL,'reference_master',CAST(NULL AS TIMESTAMP),CAST(NULL AS TIMESTAMP),
    (SELECT recorded_at FROM projection_context),NULL,
    (SELECT run_id FROM projection_context)
FROM (VALUES ('carrier'),('facility'),('route'),('country'),('region'),('service_level')) t(entity_type);

CREATE OR REPLACE TEMP VIEW dim_carrier AS
SELECT surrogate_key AS carrier_sk,canonical_id, business_key, canonical_value AS carrier_name,
    get_json_object(attributes,'$.operating_status') AS operating_status,
    canonical_id('service_level',get_json_object(attributes,'$.default_service_key')) AS default_service_id,
    effective_from,effective_to,is_current,status,raw_hash,run_id
FROM dim_reference WHERE entity_type='carrier';
CREATE OR REPLACE TEMP VIEW dim_facility AS
SELECT surrogate_key AS facility_sk,canonical_id,business_key,canonical_value AS facility_name,
    canonical_id('country',get_json_object(attributes,'$.country_key')) AS country_id,
    canonical_id('region',get_json_object(attributes,'$.region_key')) AS region_id,
    get_json_object(attributes,'$.operating_status') AS operating_status,
    effective_from,effective_to,is_current,status,raw_hash,run_id
FROM dim_reference WHERE entity_type='facility';
CREATE OR REPLACE TEMP VIEW dim_route AS
SELECT surrogate_key AS route_sk,canonical_id,business_key,canonical_value AS route_name,
    canonical_id('facility',get_json_object(attributes,'$.origin_facility_key')) AS origin_facility_id,
    canonical_id('facility',get_json_object(attributes,'$.destination_facility_key')) AS destination_facility_id,
    try_cast(get_json_object(attributes,'$.distance_km') AS DECIMAL(12,3)) AS distance_km,
    get_json_object(attributes,'$.operating_status') AS operating_status,
    effective_from,effective_to,is_current,status,raw_hash,run_id
FROM dim_reference WHERE entity_type='route';
CREATE OR REPLACE TEMP VIEW dim_country AS
SELECT surrogate_key AS country_sk,canonical_id,business_key,canonical_value AS country_name,
    effective_from,effective_to,is_current,status,raw_hash,run_id
FROM dim_reference WHERE entity_type='country';
CREATE OR REPLACE TEMP VIEW dim_region AS
SELECT surrogate_key AS region_sk,canonical_id,business_key,canonical_value AS region_name,
    effective_from,effective_to,is_current,status,raw_hash,run_id
FROM dim_reference WHERE entity_type='region';
CREATE OR REPLACE TEMP VIEW dim_service_level AS
SELECT surrogate_key AS service_level_sk,canonical_id,business_key,canonical_value AS service_level_name,
    try_cast(get_json_object(attributes,'$.target_business_days') AS INTEGER) AS target_business_days,
    get_json_object(attributes,'$.operating_status') AS operating_status,
    effective_from,effective_to,is_current,status,raw_hash,run_id
FROM dim_reference WHERE entity_type='service_level';

CREATE OR REPLACE TEMP VIEW carrier_reference AS SELECT * FROM dim_reference WHERE entity_type='carrier' AND surrogate_key<>0;
CREATE OR REPLACE TEMP VIEW facility_reference AS SELECT * FROM dim_reference WHERE entity_type='facility' AND surrogate_key<>0;
CREATE OR REPLACE TEMP VIEW route_reference AS SELECT * FROM dim_reference WHERE entity_type='route' AND surrogate_key<>0;
CREATE OR REPLACE TEMP VIEW country_reference AS SELECT * FROM dim_reference WHERE entity_type='country' AND surrogate_key<>0;
CREATE OR REPLACE TEMP VIEW region_reference AS SELECT * FROM dim_reference WHERE entity_type='region' AND surrogate_key<>0;
CREATE OR REPLACE TEMP VIEW service_level_reference AS SELECT * FROM dim_reference WHERE entity_type='service_level' AND surrogate_key<>0;
