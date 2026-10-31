CREATE OR REPLACE TABLE canonical_order AS SELECT * FROM stg_order;
CREATE OR REPLACE TABLE canonical_package AS SELECT * FROM stg_package;
CREATE OR REPLACE TABLE canonical_delivery_event AS SELECT * FROM stg_event;
CREATE OR REPLACE TABLE canonical_transportation_cost AS SELECT * FROM stg_cost;

-- Package deliveries do not assert complete shipment delivery. A carrier's
-- shipment-level confirmation has package_id NULL. State authority is explicit.
CREATE OR REPLACE TABLE shipment_state_evidence AS
SELECT canonical_id AS shipment_id, 'shipment_ops' AS source_system,
    source_id AS observation_id, revision, state, coalesce(state_occurred_at,created_at) AS occurred_at,
    updated_at AS source_updated_at,200 AS authority,raw_hash,run_id,
    'state-precedence-v1:operational' AS rule_id
FROM stg_shipment WHERE NOT is_deleted
UNION ALL
SELECT shipment_id,source_system,event_id,revision,state,occurred_at,
    source_updated_at,
    CASE WHEN source_system='carrier_delivery' AND verified THEN 300 ELSE 100 END,
    raw_hash,run_id,
    CASE WHEN source_system='carrier_delivery' AND verified
        THEN 'state-precedence-v1:verified-carrier' ELSE 'state-precedence-v1:observation' END
FROM stg_event WHERE state<>'delivered' OR package_id IS NULL;

CREATE OR REPLACE TABLE shipment_state_conflict AS
WITH tiers AS (
    SELECT *, dense_rank() OVER (
        PARTITION BY shipment_id ORDER BY authority DESC,occurred_at DESC
    ) AS precedence_rank FROM shipment_state_evidence
)
SELECT shipment_id,authority,occurred_at,count(DISTINCT state) AS conflicting_states,
    list(DISTINCT state ORDER BY state) AS states
FROM tiers WHERE precedence_rank=1
GROUP BY shipment_id,authority,occurred_at HAVING count(DISTINCT state)>1;

CREATE OR REPLACE TABLE shipment_state_selection AS
SELECT * EXCLUDE(selection_rank) FROM (
    SELECT *,row_number() OVER (
        PARTITION BY shipment_id ORDER BY authority DESC,occurred_at DESC,
        source_updated_at DESC,source_system,event_id_sort
    ) AS selection_rank
    FROM (SELECT *,observation_id AS event_id_sort FROM shipment_state_evidence)
) WHERE selection_rank=1;

CREATE OR REPLACE TABLE canonical_shipment AS
SELECT s.* EXCLUDE(state,state_occurred_at),
    s.state_occurred_at AS operational_state_occurred_at, coalesce(w.state,s.state) AS state,
    w.source_system AS state_source_system,w.observation_id AS state_observation_id,
    w.revision AS state_revision,w.authority AS state_authority,
    w.occurred_at AS state_occurred_at,w.rule_id AS state_rule_id,
    CASE WHEN w.state='delivered' THEN w.occurred_at END AS actual_delivery_at,
    coalesce(p.package_count,0)::BIGINT AS package_count
FROM stg_shipment s
LEFT JOIN shipment_state_selection w ON s.canonical_id=w.shipment_id
LEFT JOIN (
    SELECT shipment_id,count(*) AS package_count FROM stg_package
    WHERE NOT is_deleted GROUP BY shipment_id
) p ON s.canonical_id=p.shipment_id;

-- Source chronology, not arrival order. Multiple corroborating feeds must not
-- create a fabricated state regression when one observer missed a milestone.
CREATE OR REPLACE TABLE invalid_state_transition AS
WITH chronology AS (
    SELECT source_system,shipment_id,event_id,state,occurred_at,
        lag(state) OVER (
            PARTITION BY source_system,shipment_id,coalesce(package_id,'__SHIPMENT__')
            ORDER BY occurred_at,event_id
        ) AS previous_state
    FROM stg_event
)
SELECT * FROM chronology
WHERE (previous_state='delivered' AND state NOT IN ('delivered','returned'))
   OR (previous_state='cancelled' AND state<>'cancelled')
   OR (previous_state='returned' AND state NOT IN ('returned'));
